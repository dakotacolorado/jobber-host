//! The drive cellar-host mounts on this host, given to every run: its own
//! folder on it, the drive and the `cellar` CLI in its container, and, once it
//! ends, a bounded wait for what it wrote to upload, so a later run on another
//! host reads it.

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::credentials::Var;

/// Where the CLI is in a run's container.
pub const CONTAINER_CLI: &str = "/usr/local/bin/cellar";

/// The config's `[cellar]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cellar {
    /// Where cellar-host mounts the drive, and where a container sees it.
    pub mount: PathBuf,
    /// Seconds a run's end waits for its writes to upload.
    pub sync_timeout: u64,
    /// The `cellar` CLI (a link to cellar-host is followed).
    pub cli: PathBuf,
    /// cellar-host's `config.toml`, which names its control socket's folder.
    pub config: PathBuf,
}

impl Cellar {
    /// The run's own folder on the drive, `<mount>/jobber/runs/<id>`.
    pub fn run_dir(&self, id: &str) -> PathBuf {
        self.mount.join("jobber/runs").join(id)
    }

    /// What every run is sent: its folder, and the config the CLI reads.
    pub fn env(&self, id: &str) -> Vec<Var> {
        [
            ("CELLAR_RUN_DIR", self.run_dir(id).display().to_string()),
            ("CELLAR_HOST_CONFIG", self.config.display().to_string()),
        ]
        .into_iter()
        .map(|(name, value)| Var {
            name: name.into(),
            value,
            secret: false,
        })
        .collect()
    }

    /// `podman run`'s mounts: the drive, `rslave` so mounts made beneath it
    /// reach a running container, and, where they are on this host, the CLI,
    /// cellar-host's config and its control socket's folder, each read-only.
    /// The folder, not the socket, as a restarted daemon makes a new socket.
    pub fn podman_args(&self, cli: Option<&Path>, socket_dir: Option<&Path>) -> Vec<String> {
        let bind = |src: &Path, dst: &Path, how: &str| {
            format!(
                "--mount=type=bind,src={},dst={},{how}",
                src.display(),
                dst.display()
            )
        };
        let mut args = vec![bind(&self.mount, &self.mount, "bind-propagation=rslave")];
        if let Some(cli) = cli {
            args.push(bind(cli, Path::new(CONTAINER_CLI), "ro"));
        }
        if self.config.is_file() {
            args.push(bind(&self.config, &self.config, "ro"));
        }
        if let Some(dir) = socket_dir {
            args.push(bind(dir, dir, "ro"));
        }
        args
    }

    /// The CLI's own file, if it is there to be mounted.
    pub fn cli_file(&self) -> Option<PathBuf> {
        std::fs::canonicalize(&self.cli)
            .ok()
            .filter(|p| p.is_file() && crate::config::is_plain_absolute(p))
    }

    /// The control socket's folder, as cellar-host's config names it, if it is there.
    pub fn socket_dir(&self, home: &Path) -> Option<PathBuf> {
        let text = std::fs::read_to_string(&self.config).ok()?;
        let dir = state_dir(&text, home);
        (dir.is_dir() && crate::config::is_plain_absolute(&dir)).then_some(dir)
    }

    /// The drive's device now: a new one means cellar-host mounted it again.
    pub fn device(&self) -> Option<u64> {
        std::fs::metadata(&self.mount).ok().map(|m| m.dev())
    }

    /// Waits up to `sync_timeout` for what was written here since `since` to
    /// upload; answers the line its log says. It never fails the run.
    pub fn sync(&self, since: SystemTime) -> String {
        let mut cmd = Command::new(&self.cli);
        cmd.arg("--config")
            .arg(&self.config)
            .args(["sync", "--since", &rfc3339(since), "--timeout"])
            .arg(self.sync_timeout.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let started = Instant::now();
        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => return format!("cellar: sync could not start {}: {e}", self.cli.display()),
        };
        let mut stderr = child.stderr.take();
        let reader = thread::spawn(move || {
            let mut text = String::new();
            if let Some(s) = stderr.as_mut() {
                let _ = std::io::Read::read_to_string(s, &mut text);
            }
            text
        });
        // The CLI bounds its own wait; this bounds a daemon that never answers.
        let deadline = Duration::from_secs(self.sync_timeout + 30);
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break Some(status),
                Ok(None) if started.elapsed() < deadline => {
                    thread::sleep(Duration::from_millis(200))
                }
                _ => {
                    let _ = child.kill();
                    let _ = child.wait();
                    break None;
                }
            }
        };
        let stderr = reader.join().unwrap_or_default();
        said(
            status.and_then(|s| s.code()),
            &stderr,
            self.sync_timeout,
            started.elapsed(),
        )
    }
}

/// The log's line for a `cellar sync` that exited `code` saying `stderr`.
pub fn said(code: Option<i32>, stderr: &str, timeout: u64, took: Duration) -> String {
    match code {
        Some(0) => format!("cellar: what it wrote is uploaded ({}s)", took.as_secs()),
        Some(1) if stderr.starts_with("timed out with ") => {
            let n = stderr["timed out with ".len()..]
                .split_whitespace()
                .next()
                .unwrap_or("?");
            format!(
                "cellar: {n} files still uploading after {timeout}s; they upload after the run ends"
            )
        }
        None => format!(
            "cellar: sync gave no answer within {}s; stopped it",
            timeout + 30
        ),
        Some(code) => format!(
            "cellar: sync failed (exit {code}): {}",
            stderr.lines().next().unwrap_or("").trim()
        ),
    }
}

/// cellar-host's `state_dir`, where its control socket is, as it reads it.
fn state_dir(config: &str, home: &Path) -> PathBuf {
    let named = toml::from_str::<toml::Table>(config)
        .ok()
        .and_then(|t| t.get("state_dir")?.as_str().map(String::from));
    match named {
        Some(p) => match p.strip_prefix("~/") {
            Some(rest) => home.join(rest),
            None => PathBuf::from(p),
        },
        None => home.join(".local/state/cellar-host"),
    }
}

/// `t` to the second, as RFC 3339 in UTC: `2026-11-16T12:00:00Z`.
pub fn rfc3339(t: SystemTime) -> String {
    let secs = t
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (days, rem) = (secs / 86400, secs % 86400);
    // Howard Hinnant's civil_from_days.
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem / 60 % 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn cellar(root: &Path) -> Cellar {
        Cellar {
            mount: root.join("drive"),
            sync_timeout: 2,
            cli: root.join("cellar"),
            config: root.join("cellar-host.toml"),
        }
    }

    fn scratch(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("jobber-host-cellar-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn a_container_gets_the_drive_rslave_and_the_cli_config_and_socket_read_only() {
        let root = scratch("args");
        let c = cellar(&root);
        fs::write(&c.config, "state_dir = \"~/state\"\n").unwrap();
        fs::create_dir_all(root.join("home/state")).unwrap();
        let socket = c.socket_dir(&root.join("home")).unwrap();
        assert_eq!(socket, root.join("home/state"));
        let r = root.display();
        assert_eq!(
            c.podman_args(Some(Path::new("/opt/cellar-host")), Some(&socket)),
            [
                format!("--mount=type=bind,src={r}/drive,dst={r}/drive,bind-propagation=rslave"),
                "--mount=type=bind,src=/opt/cellar-host,dst=/usr/local/bin/cellar,ro".to_string(),
                format!("--mount=type=bind,src={r}/cellar-host.toml,dst={r}/cellar-host.toml,ro"),
                format!("--mount=type=bind,src={r}/home/state,dst={r}/home/state,ro"),
            ]
        );
        // What is not on this host is not mounted; the drive always is.
        fs::remove_file(&c.config).unwrap();
        assert_eq!(c.socket_dir(&root.join("home")), None);
        assert_eq!(c.cli_file(), None);
        assert_eq!(
            c.podman_args(None, None),
            [format!(
                "--mount=type=bind,src={r}/drive,dst={r}/drive,bind-propagation=rslave"
            )]
        );
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_run_is_sent_its_own_folder_on_the_drive_and_the_clis_config() {
        let c = cellar(Path::new("/x"));
        let env: Vec<(String, String)> = c
            .env("r1")
            .into_iter()
            .map(|v| {
                assert!(!v.secret);
                (v.name.into_owned(), v.value)
            })
            .collect();
        assert_eq!(
            env,
            [
                ("CELLAR_RUN_DIR".into(), "/x/drive/jobber/runs/r1".into()),
                ("CELLAR_HOST_CONFIG".into(), "/x/cellar-host.toml".into())
            ]
        );
    }

    #[test]
    fn the_socket_folder_is_cellar_hosts_state_dir_or_its_default() {
        let home = Path::new("/home/me");
        assert_eq!(state_dir("state_dir = \"/s\"\n", home), PathBuf::from("/s"));
        assert_eq!(
            state_dir("state_dir = \"~/s\"\n", home),
            PathBuf::from("/home/me/s")
        );
        assert_eq!(
            state_dir("service = \"https://c\"\n", home),
            PathBuf::from("/home/me/.local/state/cellar-host")
        );
    }

    #[test]
    fn since_is_rfc3339_in_utc_to_the_second() {
        assert_eq!(rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        let t = UNIX_EPOCH + Duration::from_millis(1_795_392_000_900);
        assert_eq!(rfc3339(t), "2026-11-23T00:00:00Z");
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::from_secs(951_782_400 + 3_723)),
            "2000-02-29T01:02:03Z"
        );
    }

    #[test]
    fn a_sync_says_it_uploaded_or_how_many_are_left_and_never_fails() {
        let s = Duration::from_secs(3);
        assert_eq!(
            said(Some(0), "", 600, s),
            "cellar: what it wrote is uploaded (3s)"
        );
        assert_eq!(
            said(
                Some(1),
                "timed out with 4 changes not yet uploaded:\n  put /jobber/runs/r/a",
                600,
                s
            ),
            "cellar: 4 files still uploading after 600s; they upload after the run ends"
        );
        assert_eq!(
            said(
                Some(1),
                "cannot reach the daemon at /s/control.sock: refused\n",
                600,
                s
            ),
            "cellar: sync failed (exit 1): cannot reach the daemon at /s/control.sock: refused"
        );
        assert_eq!(
            said(None, "", 600, s),
            "cellar: sync gave no answer within 630s; stopped it"
        );
    }

    #[test]
    fn the_sync_runs_the_cli_with_since_and_timeout_and_reads_its_answer() {
        use std::os::unix::fs::PermissionsExt;
        let root = scratch("sync");
        let mut c = cellar(&root);
        let script = |body: &str| {
            fs::write(&c.cli, format!("#!/bin/sh\n{body}\n")).unwrap();
            fs::set_permissions(&c.cli, fs::Permissions::from_mode(0o755)).unwrap();
        };
        let args = root.join("args");
        script(&format!("echo \"$@\" > {}", args.display()));
        let line = c.sync(UNIX_EPOCH + Duration::from_secs(60));
        assert!(
            line.starts_with("cellar: what it wrote is uploaded"),
            "{line}"
        );
        assert_eq!(
            fs::read_to_string(&args).unwrap().trim(),
            format!(
                "--config {} sync --since 1970-01-01T00:01:00Z --timeout 2",
                c.config.display()
            )
        );
        script("echo 'timed out with 2 changes not yet uploaded:' >&2; exit 1");
        assert_eq!(
            c.sync(SystemTime::now()),
            "cellar: 2 files still uploading after 2s; they upload after the run ends"
        );
        c.cli = root.join("missing");
        assert!(c
            .sync(SystemTime::now())
            .starts_with("cellar: sync could not start"));
        fs::remove_dir_all(&root).unwrap();
    }
}
