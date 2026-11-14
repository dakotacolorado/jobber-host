use serde::Deserialize;

use crate::budget::Resources;
use std::collections::BTreeMap;
use std::{env, fs, path::Path, path::PathBuf};

/// The smallest run jobber-host takes, in MiB: with less free than this
/// there is nothing to take.
pub const MIN_JOB_MEMORY: u64 = 16;

/// The systemd user slice every container runs in unless the config says:
/// jobber-host's own, beside any `jobs.slice` a job-runner on the same
/// computer has.
pub const SLICE: &str = "jobber.slice";

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    /// jobber's address, without a trailing `/`.
    pub service: String,
    /// jobber's registration token (its `RUNNER_TOKEN`), read only to
    /// register; after that the host calls with its own.
    pub registration_token_file: PathBuf,
    /// The host's id and own token, written once it registers, mode 600.
    pub state_file: PathBuf,
    /// The host's name in jobber, which keeps its id from one registration to the next.
    pub name: String,
    pub labels: BTreeMap<String, String>,
    /// CPUs and MiB kept back from runs: it offers the rest of what it measures.
    pub reserve: Reserve,
    /// MiB an image build may use, reserved while it runs; at most what the host offers.
    pub build_memory: u64,
    /// MiB each run's container gets, and reserves, beyond its admitted memory.
    pub job_memory_overhead: u64,
    /// Where each run's checkout is made, and removed after.
    pub work_dir: PathBuf,
    /// A git credential helper for cloning run repos, e.g. `!gh auth git-credential`;
    /// none: whatever the user's own git config gives.
    pub git_credential_helper: Option<String>,
    /// The GitHub owner whose `base-<name>` repo `localhost/base-<name>:<n>` is built from, at tag `v<n>`, when a host lacks it.
    pub base_owner: String,
    /// Seconds a stopped container gets between SIGTERM and SIGKILL.
    pub stop_grace_seconds: u64,
    /// The systemd user slice every container runs in; none: podman's default.
    pub cgroup_parent: Option<String>,
    /// The platform every image is pulled, built and run as, e.g. `linux/amd64`
    /// on an arm64 mac whose podman machine runs amd64 through Rosetta; none: the host's own.
    pub platform: Option<String>,
    pub slice: Slice,
    /// Repos (`owner/name`) whose runs `onHost` this host runs on itself, as
    /// its user, outside a container; any other repo's is failed unrun.
    pub host_runs: Vec<String>,
    /// The GPUs it offers runs, as podman CDI device names (`nvidia.com/gpu=0`),
    /// each given whole to one run's container at a time.
    pub gpus: Vec<String>,
}

/// CPUs and MiB kept back from runs, as Kubernetes' `system-reserved`.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reserve {
    pub cpu: f64,
    /// MiB.
    pub memory: u64,
}

/// The slice's caps, written into its unit by `--slice-unit`, which the install script runs.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Slice {
    /// systemd's `MemoryMax`: everything in the slice together, e.g. `3G`.
    /// None: what the host offers.
    #[serde(default)]
    pub memory_max: Option<String>,
    /// systemd's `CPUWeight`, against 100 for the sessions beside it.
    #[serde(default = "Slice::default_cpu_weight")]
    pub cpu_weight: u32,
}

impl Slice {
    fn default_cpu_weight() -> u32 {
        20
    }

    /// Its unit, sized to what the host offers runs, in memory and in CPU.
    pub fn unit(&self, offered: Resources) -> String {
        let memory_max = match &self.memory_max {
            Some(max) => max.clone(),
            None => format!("{}M", offered.memory),
        };
        format!(
            "[Unit]\nDescription=Runs jobber-host runs: capped in memory, below agents in CPU\n\n\
             [Slice]\nMemoryMax={memory_max}\nMemorySwapMax=0\nCPUQuota={}%\nCPUWeight={}\n",
            (offered.cpu.cpus() * 100.0).round() as u64,
            self.cpu_weight
        )
    }
}

impl Default for Slice {
    fn default() -> Slice {
        Slice {
            memory_max: None,
            cpu_weight: Slice::default_cpu_weight(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    service: Option<String>,
    registration_token_file: Option<PathBuf>,
    state_file: Option<PathBuf>,
    name: Option<String>,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    reserve: Option<Reserve>,
    build_memory: Option<u64>,
    job_memory_overhead: Option<u64>,
    work_dir: Option<PathBuf>,
    git_credential_helper: Option<String>,
    base_owner: Option<String>,
    stop_grace_seconds: Option<u64>,
    cgroup_parent: Option<String>,
    platform: Option<String>,
    #[serde(default)]
    slice: Slice,
    #[serde(default)]
    host_runs: Vec<String>,
    #[serde(default)]
    gpus: Vec<String>,
}

/// Where a setting's file lives by default, under XDG's directories.
struct Dirs {
    config: PathBuf,
    state: PathBuf,
    data: PathBuf,
}

impl Dirs {
    fn from_env(var: impl Fn(&str) -> Option<String>) -> Option<Dirs> {
        let home = var("HOME").filter(|h| !h.is_empty()).map(PathBuf::from);
        let dir = |xdg: &str, under_home: &str| {
            var(xdg)
                .filter(|d| !d.is_empty())
                .map(PathBuf::from)
                .or_else(|| home.as_ref().map(|h| h.join(under_home)))
                .map(|d| d.join("jobber-host"))
        };
        Some(Dirs {
            config: dir("XDG_CONFIG_HOME", ".config")?,
            state: dir("XDG_STATE_HOME", ".local/state")?,
            data: dir("XDG_DATA_HOME", ".local/share")?,
        })
    }
}

pub fn default_path() -> Option<PathBuf> {
    Dirs::from_env(|k| env::var(k).ok()).map(|d| d.config.join("config.toml"))
}

/// Reads the file, which is the only place settings come from.
pub fn load(path: Option<PathBuf>) -> Result<Config, String> {
    let path = path
        .or_else(default_path)
        .ok_or("no config file: pass --config PATH")?;
    let text = fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let dirs = Dirs::from_env(|k| env::var(k).ok()).ok_or("no HOME")?;
    parse(&text, &dirs).map_err(|e| format!("{}: {e}", path.display()))
}

/// Keys job-runner's config has that jobber-host's does not, each with what to do instead.
const NOT_HERE: [(&str, &str); 6] = [
    ("api", "jobber-host speaks only jobber's protocol; leave api out"),
    ("slots", "a host measures its CPUs and memory each time it starts; leave slots out and say what to keep back with reserve = { cpu = .., memory = .. }"),
    ("memory", "a host measures its CPUs and memory each time it starts; leave memory out and say what to keep back with reserve = { cpu = .., memory = .. }"),
    ("host_id", "a host is its name in jobber; leave host_id out"),
    ("poll_seconds", "jobber pushes runs to its hosts; leave poll_seconds out"),
    ("host_jobs", "jobber runs every run in a container; leave [host_jobs] out"),
];

fn parse(text: &str, dirs: &Dirs) -> Result<Config, String> {
    if let Ok(table) = toml::from_str::<toml::Table>(text) {
        if let Some((key, why)) = NOT_HERE.iter().find(|(k, _)| table.contains_key(*k)) {
            return Err(format!("{key}: {why}"));
        }
    }
    let file: File = toml::from_str(text).map_err(|e| e.to_string())?;
    let service = file.service.ok_or("no service: jobber's address")?;
    if !(service.starts_with("https://") || service.starts_with("http://")) {
        return Err(format!("service {service} is not an http(s) address"));
    }
    let job_memory_overhead = file.job_memory_overhead.unwrap_or(0);
    let reserve = file.reserve.ok_or(
        "no reserve: the CPUs and MiB to keep back for the rest of the machine, e.g. reserve = { cpu = 2, memory = 4096 }",
    )?;
    if !reserve.cpu.is_finite() || reserve.cpu < 0.0 {
        return Err(format!(
            "reserve.cpu is {}: it must be 0 or more",
            reserve.cpu
        ));
    }
    let build_memory = file.build_memory.unwrap_or(1024);
    if build_memory == 0 {
        return Err("build_memory is 0 MiB: it must be at least 1".into());
    }
    let name = file
        .name
        .or_else(crate::sys::hostname)
        .ok_or("no name, and no hostname to use instead")?;
    let absolute = |key: &str, path: PathBuf| {
        if path.is_absolute() {
            Ok(path)
        } else {
            Err(format!("{key} {} is not an absolute path", path.display()))
        }
    };
    Ok(Config {
        service: service.trim_end_matches('/').to_string(),
        registration_token_file: absolute(
            "registration_token_file",
            file.registration_token_file
                .unwrap_or_else(|| dirs.config.join("registration-token")),
        )?,
        state_file: absolute(
            "state_file",
            file.state_file
                .unwrap_or_else(|| dirs.state.join("host.json")),
        )?,
        name,
        labels: file.labels,
        reserve,
        build_memory,
        job_memory_overhead,
        work_dir: absolute(
            "work_dir",
            file.work_dir.unwrap_or_else(|| dirs.data.clone()),
        )?,
        git_credential_helper: file.git_credential_helper.filter(|h| !h.is_empty()),
        base_owner: file.base_owner.unwrap_or_else(|| "dakotacolorado".into()),
        stop_grace_seconds: file.stop_grace_seconds.unwrap_or(10),
        cgroup_parent: match file.cgroup_parent {
            None => Some(SLICE.into()),
            Some(p) if p.is_empty() => None,
            Some(p) if p.ends_with(".slice") && !p.contains('-') && !p.contains('/') => Some(p),
            // systemd reads a `-` in a slice's name as nesting: `a-b.slice` is inside `a.slice`.
            Some(p) => {
                return Err(format!(
                    "cgroup_parent {p} must be a slice name without - or /, e.g. {SLICE}"
                ))
            }
        },
        platform: match file.platform {
            Some(p) if p.is_empty() => None,
            Some(p) if p.starts_with("linux/") && !p.contains(char::is_whitespace) => Some(p),
            Some(p) => {
                return Err(format!(
                    "platform {p} must be a podman platform, e.g. linux/amd64"
                ))
            }
            None => None,
        },
        slice: file.slice,
        host_runs: file
            .host_runs
            .into_iter()
            .map(|repo| {
                let mut parts = repo.split('/');
                let ok = |p: Option<&str>| {
                    p.is_some_and(|p| {
                        !p.is_empty()
                            && p.chars()
                                .all(|c| c.is_ascii_alphanumeric() || "_.-".contains(c))
                    })
                };
                if ok(parts.next()) && ok(parts.next()) && parts.next().is_none() {
                    Ok(repo)
                } else {
                    Err(format!("host_runs {repo:?} must be a repo, owner/name"))
                }
            })
            .collect::<Result<_, _>>()?,
        gpus: gpus(file.gpus)?,
    })
}

/// CDI device names, each once: `vendor/class=name`, as `nvidia-ctk cdi list` prints them.
fn gpus(devices: Vec<String>) -> Result<Vec<String>, String> {
    for (i, d) in devices.iter().enumerate() {
        let cdi = d.split_once('=').is_some_and(|(kind, name)| {
            kind.contains('/') && !name.is_empty() && !d.contains(char::is_whitespace)
        });
        if !cdi {
            return Err(format!(
                "gpus {d:?} must be a CDI device name, e.g. nvidia.com/gpu=0 (see nvidia-ctk cdi list)"
            ));
        }
        if devices[..i].contains(d) {
            return Err(format!("gpus names {d:?} twice"));
        }
    }
    if devices.len() > 64 {
        return Err("gpus names more than 64".into());
    }
    Ok(devices)
}

impl Config {
    pub fn registration_token(&self) -> Result<String, String> {
        read_secret(&self.registration_token_file)
    }
}

fn read_secret(path: &Path) -> Result<String, String> {
    let text = fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let token = text.trim();
    if token.is_empty() {
        return Err(format!("{} is empty", path.display()));
    }
    Ok(token.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "service = \"https://jobber.example/\"\nname = \"box\"\nreserve = { cpu = 2, memory = 4096 }\n";

    fn dirs() -> Dirs {
        Dirs::from_env(|k| (k == "HOME").then(|| "/home/me".to_string())).unwrap()
    }

    fn read(text: &str) -> Result<Config, String> {
        parse(text, &dirs())
    }

    #[test]
    fn reads_the_file_with_defaults_under_xdg_dirs() {
        assert_eq!(
            read(BASE).unwrap(),
            Config {
                service: "https://jobber.example".into(),
                registration_token_file: "/home/me/.config/jobber-host/registration-token".into(),
                state_file: "/home/me/.local/state/jobber-host/host.json".into(),
                name: "box".into(),
                labels: BTreeMap::new(),
                reserve: Reserve {
                    cpu: 2.0,
                    memory: 4096
                },
                build_memory: 1024,
                job_memory_overhead: 0,
                work_dir: "/home/me/.local/share/jobber-host".into(),
                git_credential_helper: None,
                base_owner: "dakotacolorado".into(),
                stop_grace_seconds: 10,
                cgroup_parent: Some("jobber.slice".into()),
                platform: None,
                slice: Slice::default(),
                host_runs: vec![],
                gpus: vec![],
            }
        );
    }

    #[test]
    fn gpus_are_cdi_device_names_each_once() {
        let with = |list: &str| read(&format!("{BASE}gpus = {list}\n"));
        assert_eq!(
            with("[\"nvidia.com/gpu=0\", \"nvidia.com/gpu=1\"]")
                .unwrap()
                .gpus,
            ["nvidia.com/gpu=0", "nvidia.com/gpu=1"]
        );
        for bad in [
            "[\"0\"]",
            "[\"nvidia.com/gpu=\"]",
            "[\"gpu=0\"]",
            "[\"a/b=0\", \"a/b=0\"]",
        ] {
            assert!(with(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_config_is_at_its_own_path_under_xdg_config() {
        assert_eq!(
            dirs().config.join("config.toml"),
            PathBuf::from("/home/me/.config/jobber-host/config.toml")
        );
    }

    #[test]
    fn a_host_says_only_what_to_keep_back_and_no_slot_count_or_memory_figure() {
        let err = read(&format!("{BASE}slots = 2\n")).unwrap_err();
        assert!(err.contains("measures its CPUs and memory"), "{err}");
        let err = read(&format!("{BASE}memory = 2048\n")).unwrap_err();
        assert!(err.contains("leave memory out"), "{err}");
        let err = read(&BASE.replace("reserve = { cpu = 2, memory = 4096 }\n", "")).unwrap_err();
        assert!(err.contains("no reserve"), "{err}");
        let err = read(&BASE.replace("cpu = 2", "cpu = -1")).unwrap_err();
        assert!(err.contains("0 or more"), "{err}");
    }

    #[test]
    fn job_runners_protocols_and_their_settings_are_refused_saying_what_to_do() {
        for (key, line) in [
            ("api", "api = \"hosts\"\n"),
            ("host_id", "host_id = \"box\"\n"),
            ("poll_seconds", "poll_seconds = 5\n"),
            ("host_jobs", "[host_jobs]\nrepos = [\"o/r\"]\n"),
        ] {
            let err = read(&format!("{BASE}{line}")).unwrap_err();
            assert!(err.starts_with(&format!("{key}: ")), "{err}");
            assert!(err.contains("leave"), "{err}");
        }
    }

    #[test]
    fn its_slice_is_sized_to_what_it_offers_unless_its_config_says() {
        let offered = Resources {
            cpu: crate::budget::Cpu::of(13.5),
            memory: 9216,
            gpu: 0,
        };
        let unit = Slice::default().unit(offered);
        assert!(unit.contains("MemoryMax=9216M\n"), "{unit}");
        assert!(unit.contains("CPUQuota=1350%\n"), "{unit}");
        assert!(unit.contains("CPUWeight=20\n"), "{unit}");
        let given = Slice {
            memory_max: Some("2G".into()),
            cpu_weight: 20,
        };
        assert!(given.unit(offered).contains("MemoryMax=2G\n"));
    }

    #[test]
    fn host_runs_names_repos_and_nothing_else() {
        let with = |list: &str| read(&format!("{BASE}host_runs = {list}\n"));
        assert_eq!(
            with("[\"a/b\", \"c-d/e.f\"]").unwrap().host_runs,
            ["a/b", "c-d/e.f"]
        );
        for bad in ["[\"a\"]", "[\"a/b/c\"]", "[\"/b\"]", "[\"a/b c\"]"] {
            assert!(with(bad).unwrap_err().contains("must be a repo"), "{bad}");
        }
    }

    #[test]
    fn every_setting_can_be_given() {
        let config = read(
            "service = \"http://localhost:4500\"\nregistration_token_file = \"/etc/jh/reg\"\n\
             state_file = \"/var/jh/state.json\"\nname = \"n\"\nlabels = { arch = \"x86_64\" }\n\
             reserve = { cpu = 0.5, memory = 512 }\nbuild_memory = 256\njob_memory_overhead = 64\n\
             work_dir = \"/srv/jh\"\ngit_credential_helper = \"!gh auth git-credential\"\n\
             base_owner = \"me\"\nstop_grace_seconds = 3\ncgroup_parent = \"mine.slice\"\n\
             platform = \"linux/amd64\"\nhost_runs = [\"o/deploy-routines\"]\n[slice]\nmemory_max = \"1G\"\ncpu_weight = 50\n",
        )
        .unwrap();
        assert_eq!(config.registration_token_file, PathBuf::from("/etc/jh/reg"));
        assert_eq!(config.state_file, PathBuf::from("/var/jh/state.json"));
        assert_eq!(config.labels["arch"], "x86_64");
        assert_eq!(
            config.reserve,
            Reserve {
                cpu: 0.5,
                memory: 512
            }
        );
        assert_eq!((config.build_memory, config.job_memory_overhead), (256, 64));
        assert_eq!(config.work_dir, PathBuf::from("/srv/jh"));
        assert_eq!(
            config.git_credential_helper.as_deref(),
            Some("!gh auth git-credential")
        );
        assert_eq!(
            (config.base_owner.as_str(), config.stop_grace_seconds),
            ("me", 3)
        );
        assert_eq!(config.cgroup_parent.as_deref(), Some("mine.slice"));
        assert_eq!(config.platform.as_deref(), Some("linux/amd64"));
        assert_eq!(config.host_runs, ["o/deploy-routines"]);
        assert_eq!(
            config.slice,
            Slice {
                memory_max: Some("1G".into()),
                cpu_weight: 50
            }
        );
    }

    #[test]
    fn xdg_dirs_are_followed_when_set() {
        let dirs = Dirs::from_env(|k| match k {
            "HOME" => Some("/home/me".into()),
            "XDG_STATE_HOME" => Some("/state".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(
            parse(BASE, &dirs).unwrap().state_file,
            PathBuf::from("/state/jobber-host/host.json")
        );
    }

    #[test]
    fn paths_must_be_absolute_and_the_service_an_address() {
        let err = read(&format!("{BASE}work_dir = \"runs\"")).unwrap_err();
        assert!(err.contains("not an absolute path"), "{err}");
        let err = read(&BASE.replace("https://jobber.example/", "jobber.example")).unwrap_err();
        assert!(err.contains("not an http(s) address"), "{err}");
    }

    #[test]
    fn a_slice_is_one_systemd_will_not_nest_and_empty_is_podmans_default() {
        let parent =
            |p: &str| read(&format!("{BASE}cgroup_parent = \"{p}\"")).map(|c| c.cgroup_parent);
        assert_eq!(parent("").unwrap(), None);
        let err = parent("jobber-host.slice").unwrap_err();
        assert!(err.contains("without - or /"), "{err}");
    }

    #[test]
    fn a_platform_is_one_podman_takes_and_empty_is_the_hosts_own() {
        let platform = |p: &str| read(&format!("{BASE}platform = \"{p}\"")).map(|c| c.platform);
        assert_eq!(platform("linux/amd64").unwrap(), Some("linux/amd64".into()));
        assert_eq!(platform("").unwrap(), None);
        let err = platform("amd64").unwrap_err();
        assert!(err.contains("linux/amd64"), "{err}");
    }

    #[test]
    fn an_unknown_key_is_refused() {
        assert!(read(&format!("{BASE}capacity = 2")).is_err());
    }

    #[test]
    fn the_file_must_exist() {
        let err = load(Some("/nonexistent/jobber-host.toml".into())).unwrap_err();
        assert!(err.contains("/nonexistent/jobber-host.toml"), "{err}");
    }

    #[test]
    fn a_secret_file_is_read_trimmed_and_must_not_be_empty() {
        let dir = std::env::temp_dir().join(format!("jh-secret-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("t"), "reg\n").unwrap();
        assert_eq!(read_secret(&dir.join("t")).unwrap(), "reg");
        fs::write(dir.join("e"), "\n").unwrap();
        assert!(read_secret(&dir.join("e")).unwrap_err().contains("empty"));
        fs::remove_dir_all(dir).unwrap();
    }
}
