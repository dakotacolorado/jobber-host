//! The heartbeat is what keeps a host's runs alive. It
//! renews every run the host has started and not yet ended, says what is free
//! and how the machine is, and brings back which runs to cancel, which each
//! run's log then reads. `src/connection.rs` sends it, over the connection
//! while there is one and by HTTP while there is not.

use std::path::Path;
use std::time::Duration;

use crate::service::Metrics;

/// How often a heartbeat is sent: well inside jobber's 60s lease, and
/// its 15s at most.
pub const EVERY: Duration = Duration::from_secs(10);

/// MiB in use and in all, from `/proc/meminfo`'s `MemTotal` and `MemAvailable`.
pub fn parse_meminfo(text: &str) -> Option<(u64, u64)> {
    let kib = |key: &str| -> Option<u64> {
        let line = text.lines().find(|l| l.starts_with(key))?;
        line[key.len()..]
            .trim()
            .trim_end_matches("kB")
            .trim()
            .parse()
            .ok()
    };
    let total = kib("MemTotal:")?;
    let available = kib("MemAvailable:")?;
    Some((total.saturating_sub(available) / 1024, total / 1024))
}

/// `path`, or its nearest ancestor that exists: the disk it will be on.
fn existing(path: &Path) -> &Path {
    let mut p = path;
    while !p.exists() {
        match p.parent() {
            Some(parent) => p = parent,
            None => break,
        }
    }
    p
}

/// How the machine is now; what cannot be read here is 0.
pub fn metrics(work_dir: &Path) -> Metrics {
    // Without /proc (macOS), its total is still known, if not what is in use.
    let (memory_used, memory_total) = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|t| parse_meminfo(&t))
        .or_else(|| crate::capacity::memory().map(|total| (0, total)))
        .unwrap_or((0, 0));
    Metrics {
        cpu_load: crate::sys::load_average().unwrap_or(0.0),
        memory_used,
        memory_total,
        disk_free: crate::sys::disk_free(existing(work_dir)).unwrap_or(0),
        cpus: std::thread::available_parallelism().map_or(0, |n| n.get() as u64),
    }
}

/// The wait before the next heartbeat: `every`, or a third of the lease
/// the last answer gave if less.
pub fn wait(every: Duration, lease: u64) -> Duration {
    every
        .min(Duration::from_secs(lease) / 3)
        .max(Duration::from_secs(1).min(every))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::Budget;
    use crate::executor::Log;
    use crate::service::fake_http::route;
    use crate::service::{Heartbeat, Hosts, Service};
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    #[test]
    fn memory_in_use_is_all_but_what_is_available() {
        let text = "MemTotal:       16384000 kB\nMemFree:  1000 kB\nMemAvailable:    4096000 kB\n";
        assert_eq!(parse_meminfo(text), Some((12000, 16000)));
        assert_eq!(parse_meminfo("MemTotal: 1 kB\n"), None);
    }

    #[test]
    fn metrics_are_read_or_zero_and_never_fail() {
        let m = metrics(Path::new("/"));
        assert!(m.cpu_load >= 0.0);
        if cfg!(target_os = "linux") {
            assert!(m.memory_total > 0 && m.memory_used <= m.memory_total);
            assert!(m.disk_free > 0);
        }
        assert!(m.cpus >= 1);
    }

    #[test]
    fn disk_free_is_read_before_the_work_dir_exists() {
        let missing = std::env::temp_dir().join("jobber-host-no-such-dir/not/yet");
        assert!(!missing.exists());
        assert_eq!(existing(&missing), std::env::temp_dir().as_path());
        if cfg!(target_os = "linux") {
            assert!(metrics(&missing).disk_free > 0);
        }
    }

    fn wait_for(what: &str, f: impl Fn() -> bool) {
        let until = Instant::now() + Duration::from_secs(10);
        while !f() {
            assert!(Instant::now() < until, "never: {what}");
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn a_heartbeat_comes_every_ten_seconds_or_a_third_of_a_shorter_lease() {
        assert_eq!(wait(EVERY, 60), EVERY);
        assert_eq!(wait(EVERY, 15), Duration::from_secs(5));
        assert_eq!(wait(EVERY, 0), Duration::from_secs(1), "never a busy loop");
    }

    #[test]
    fn a_cancel_in_a_heartbeats_answer_reaches_the_runs_log_without_a_call_of_its_own() {
        let (url, sent) = route(|s| match s.request() {
            "POST /hosts/h1/heartbeat" => {
                ("200 OK".into(), r#"{"cancel":["r1"],"lease":60}"#.into())
            }
            _ => ("500 Internal Server Error".into(), "{}".into()),
        });
        let hosts = Arc::new(Hosts::new(&url, "h1", "t"));
        hosts.begin("r1", 1);
        hosts.begin("r2", 1);
        let log = Log::start(hosts.clone(), "r1".into());
        let other = Log::start(hosts.clone(), "r2".into());
        assert!(!log.canceled());
        let answer = hosts
            .heartbeat(&Heartbeat {
                free: Budget::new(
                    crate::budget::Resources {
                        cpu: crate::budget::Cpu::of(1.0),
                        memory: 1024,
                    },
                    512,
                )
                .free(),
                metrics: metrics(Path::new("/")),
                running: &["r1".into(), "r2".into()],
            })
            .unwrap();
        assert_eq!(answer.cancel, ["r1"]);
        wait_for("the log hears of the cancel", || log.canceled());
        assert!(!other.canceled());
        log.close(Duration::from_secs(1));
        other.close(Duration::from_secs(1));
        assert_eq!(
            sent.lock().unwrap().len(),
            1,
            "a silent run's renewal is the heartbeat's, not a log append"
        );
        assert_eq!(hosts.renew_every(), Duration::from_secs(1));
    }
}
