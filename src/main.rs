mod banner;
mod budget;
mod capacity;
mod config;
mod connection;
mod credentials;
mod executor;
mod heartbeat;
mod identity;
mod retry;
mod service;
mod sys;

use budget::{Budget, Resources, Shared};
use config::Config;
use executor::Podman;
use identity::Identity;
use retry::retrying;
use service::{Hosts, RegisterHost};
use std::sync::Arc;
use std::time::Duration;
use std::{env, path::PathBuf, process::ExitCode};

/// The release this binary is, as registration and `--version` say: the
/// release tag (else commit) the install script built it from, given in `JOBBER_HOST_RELEASE`,
/// or Cargo.toml's version for any other build.
const VERSION: &str = match option_env!("JOBBER_HOST_RELEASE") {
    Some(release) if !release.is_empty() => release,
    _ => env!("CARGO_PKG_VERSION"),
};

const USAGE: &str = "usage: jobber-host [--config PATH] [--slice-unit]

  --config PATH   config file (default $XDG_CONFIG_HOME/jobber-host/config.toml,
                  or ~/.config/jobber-host/config.toml); JOBBER_HOST_CONFIG also names it
  --slice-unit    print the systemd unit for the config's slice, and exit
  --version       print the release, and exit
  -h, --help      print this, and exit

Every setting is in the file; see the README.";

/// What the command line asks for.
#[derive(Debug, PartialEq)]
enum Asked {
    Run {
        config: Option<PathBuf>,
        slice_unit: bool,
    },
    Help,
    Version,
}

fn parse(mut args: impl Iterator<Item = String>, config: Option<PathBuf>) -> Result<Asked, String> {
    let mut config = config;
    let mut slice_unit = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => config = Some(args.next().ok_or("--config needs a path")?.into()),
            "--slice-unit" => slice_unit = true,
            "--version" => return Ok(Asked::Version),
            "--help" | "-h" => return Ok(Asked::Help),
            other => return Err(format!("unknown argument {other}\n\n{USAGE}")),
        }
    }
    Ok(Asked::Run { config, slice_unit })
}

/// The banner, then the options: `--help`.
fn help(paint: &banner::Palette) -> String {
    format!(
        "{}\n{}\n\n{USAGE}",
        banner::banner(paint),
        paint.dim(&format!("jobber-host {VERSION}"))
    )
}

fn main() -> ExitCode {
    let asked = parse(
        env::args().skip(1),
        env::var_os("JOBBER_HOST_CONFIG").map(PathBuf::from),
    );
    let (path, slice_unit) = match asked {
        Ok(Asked::Run { config, slice_unit }) => (config, slice_unit),
        Ok(Asked::Help) => {
            println!("{}", help(&banner::Palette::for_stdout()));
            return ExitCode::SUCCESS;
        }
        Ok(Asked::Version) => {
            // The banner, ending in the version's line, which the install script reads.
            let paint = banner::Palette::for_stdout();
            println!("{}", banner::banner(&paint));
            println!("jobber-host {VERSION}");
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            log!("{e}");
            return ExitCode::from(2);
        }
    };
    let config = match config::load(path) {
        Ok(config) => config,
        Err(e) => {
            log!("jobber-host: {e}");
            return ExitCode::from(2);
        }
    };
    // What the machine has less what is kept back, measured now: each start registers it again.
    let (measured, offered) = match capacity::measure().and_then(|m| {
        capacity::allocatable(m, &config.reserve, config.job_memory_overhead).map(|a| (m, a))
    }) {
        Ok((measured, offered)) => (
            measured,
            Resources {
                gpu: config.gpus.len() as u32,
                ..offered
            },
        ),
        Err(e) => {
            log!("jobber-host: {e}");
            return ExitCode::from(2);
        }
    };
    if slice_unit {
        print!("{}", config.slice.unit(offered));
        return ExitCode::SUCCESS;
    }
    log!(
        "measured {} CPU and {} MiB; keeping back {} CPU and {} MiB, it offers {} CPU and {} MiB",
        measured.cpu,
        measured.memory,
        config.reserve.cpu,
        config.reserve.memory,
        offered.cpu,
        offered.memory
    );
    // Two hosts on one state file would be one host to jobber, each taking
    // runs against a budget that is only half its own.
    let _lock = match hold(&config) {
        Ok(lock) => lock,
        Err(e) => {
            log!("jobber-host: {e}");
            return ExitCode::from(3);
        }
    };
    sys::handle_signals();
    log!(
        "jobber-host {VERSION}: {} for {}, {} CPU and {} MiB ({} MiB per image build), {}, labels {:?}, containers in {}",
        config.name,
        config.service,
        offered.cpu,
        offered.memory,
        build_memory(&config, offered),
        if config.gpus.is_empty() {
            "no GPUs".to_string()
        } else {
            format!("GPUs {}", config.gpus.join(" "))
        },
        config.labels,
        config.cgroup_parent.as_deref().unwrap_or("podman's default cgroup")
    );
    // Runs are checked out under it, and its free disk is offered: made now,
    // so a fresh host does not offer 0 MiB of disk and is never given a run.
    if let Err(e) = std::fs::create_dir_all(&config.work_dir) {
        log!("cannot make work_dir {}: {e}", config.work_dir.display());
        return ExitCode::FAILURE;
    }
    executor::sweep(&config.work_dir);
    let Some(identity) = identity(&config, offered) else {
        log!("stopped before registering");
        return ExitCode::SUCCESS;
    };
    let hosts = Arc::new(Hosts::new(&config.service, &identity.id, &identity.token));
    connection::Host {
        hosts: hosts.clone(),
        executor: Arc::new(podman(&config, hosts)),
        budget: budget(&config, offered),
        work_dir: config.work_dir.clone(),
        every: heartbeat::EVERY,
    }
    .run(&sys::stopping);
    log!("stopped");
    ExitCode::SUCCESS
}

fn podman(config: &Config, hosts: Arc<Hosts>) -> Podman<Hosts> {
    Podman {
        service: hosts,
        work_dir: config.work_dir.clone(),
        git_credential_helper: config.git_credential_helper.clone(),
        base_owner: config.base_owner.clone(),
        grace: Duration::from_secs(config.stop_grace_seconds),
        cgroup_parent: config.cgroup_parent.clone(),
        platform: config.platform.clone(),
        memory_overhead: config.job_memory_overhead,
        credentials: Arc::new(credentials::Host),
        git_base: executor::GITHUB.into(),
        building: std::sync::Mutex::new(()),
        stopping: sys::stopping,
        host_runs: config.host_runs.clone(),
    }
}

/// MiB an image build may use: as configured, and no more than the host offers.
fn build_memory(config: &Config, offered: Resources) -> u64 {
    config.build_memory.min(offered.memory)
}

fn budget(config: &Config, offered: Resources) -> Shared {
    Shared::new(
        Budget::new(offered, build_memory(config, offered))
            .with_overhead(config.job_memory_overhead)
            .with_gpus(&config.gpus),
    )
}

/// The state file's lock, held for as long as this host runs; the system
/// lets it go when the process ends, however it ends.
fn hold(config: &Config) -> Result<std::fs::File, String> {
    let path = config.state_file.with_extension("lock");
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    let file = std::fs::File::create(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(format!(
            "another jobber-host is already running with {}: stop it first",
            config.state_file.display()
        )),
        Err(std::fs::TryLockError::Error(e)) => Err(format!("{}: {e}", path.display())),
    }
}

/// Registers with jobber, retrying with backoff until it answers, and keeps
/// the identity in the state file. A host registers on every start, so
/// jobber has its current capacity and version; its name keeps its id. A
/// state file that cannot be read is retried too: it is the person's to fix,
/// and the host stays up saying why. None only when told to stop first.
fn identity(config: &Config, offered: Resources) -> Option<Identity> {
    // Registered but not yet saved: saved again rather than registered again.
    let mut unsaved: Option<Identity> = None;
    retrying("registering", &sys::stopping, || {
        let transient = service::Failure::Transient;
        let identity = match unsaved.take() {
            Some(identity) => identity,
            None => {
                if let Some(known) =
                    identity::load(&config.state_file, &config.service).map_err(transient)?
                {
                    log!(
                        "host {}: registering again with what it offers now and version {VERSION}",
                        known.id
                    );
                }
                let token = config.registration_token().map_err(transient)?;
                let registered = service::register_host(
                    &config.service,
                    &token,
                    &RegisterHost {
                        name: &config.name,
                        os: env::consts::OS,
                        arch: env::consts::ARCH,
                        capacity: offered,
                        labels: &config.labels,
                        version: VERSION,
                        features: service::FEATURES,
                    },
                )?;
                Identity {
                    service: config.service.clone(),
                    id: registered.host.id,
                    token: registered.token,
                }
            }
        };
        if let Err(e) = identity::save(&config.state_file, &identity) {
            unsaved = Some(identity);
            return Err(transient(e));
        }
        log!(
            "registered as host {}; its token is in {}",
            identity.id,
            config.state_file.display()
        );
        Ok(identity)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> impl Iterator<Item = String> {
        list.iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn help_opens_with_the_banner_and_tagline_and_gives_the_options() {
        let help = help(&banner::Palette::new(false, false));
        assert!(
            help.starts_with(&format!("\n{}", banner::wordmark())),
            "{help}"
        );
        assert!(help.contains(banner::TAGLINE), "{help}");
        assert!(!help.contains('\x1b'), "{help}");
        for option in [
            "--config PATH",
            "--slice-unit",
            "--version",
            "JOBBER_HOST_CONFIG",
        ] {
            assert!(help.contains(option), "{option}");
        }
        assert_eq!(parse(args(&["-h"]), None), Ok(Asked::Help));
        assert_eq!(parse(args(&["--version"]), None), Ok(Asked::Version));
    }

    #[test]
    fn the_config_is_named_by_its_flag_over_its_variable() {
        let from_env = Some(PathBuf::from("/env.toml"));
        assert_eq!(
            parse(args(&[]), from_env.clone()),
            Ok(Asked::Run {
                config: from_env.clone(),
                slice_unit: false
            })
        );
        assert_eq!(
            parse(args(&["--config", "/flag.toml", "--slice-unit"]), from_env),
            Ok(Asked::Run {
                config: Some("/flag.toml".into()),
                slice_unit: true
            })
        );
        assert!(parse(args(&["--config"]), None).is_err());
        let err = parse(args(&["--api"]), None).unwrap_err();
        assert!(err.contains("unknown argument --api"), "{err}");
    }
}
