//! What the machine has, measured each time jobber-host starts, and what it
//! offers runs: that less what its config keeps back
//! (`reserve`), as a Kubernetes node's allocatable is its capacity less
//! `system-reserved`.

use crate::budget::{Cpu, Resources};
use crate::config::{Reserve, MIN_JOB_MEMORY};

/// What the machine has: CPUs this process may use and MiB of memory, the
/// least of the machine's and any cgroup limit above this process.
pub fn measure() -> Result<Resources, String> {
    let cpus = std::thread::available_parallelism()
        .map_err(|e| format!("counting CPUs: {e}"))?
        .get();
    let memory = memory().ok_or("reading the machine's memory")?;
    Ok(Resources {
        cpu: Cpu::of(cpus as f64),
        memory,
    })
}

/// What the host offers jobs: `measured` less `reserve`, refused when that
/// leaves no CPU, or too little memory for one job with its overhead.
pub fn allocatable(
    measured: Resources,
    reserve: &Reserve,
    overhead: u64,
) -> Result<Resources, String> {
    let offered = Resources {
        cpu: measured.cpu.saturating_sub(Cpu::of(reserve.cpu)),
        memory: measured.memory.saturating_sub(reserve.memory),
    };
    if offered.cpu == Cpu::default() {
        return Err(format!(
            "reserve keeps back {} CPU of the {} this machine has: nothing is left for jobs",
            reserve.cpu, measured.cpu
        ));
    }
    if offered.memory < overhead + MIN_JOB_MEMORY {
        return Err(format!(
            "reserve keeps back {} MiB of the {} MiB this machine has: {} MiB is left, less than a job ({MIN_JOB_MEMORY} MiB) and its overhead ({overhead} MiB)",
            reserve.memory, measured.memory, offered.memory
        ));
    }
    Ok(offered)
}

#[cfg(target_os = "linux")]
pub fn memory() -> Option<u64> {
    let total = crate::heartbeat::parse_meminfo(&std::fs::read_to_string("/proc/meminfo").ok()?)?.1;
    let limit = std::fs::read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|text| cgroup_path(&text))
        .and_then(|path| cgroup_limit(&path, |p| std::fs::read_to_string(p).ok()));
    Some(limit.map_or(total, |l| l.min(total)))
}

#[cfg(target_os = "macos")]
pub fn memory() -> Option<u64> {
    let out = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    let bytes: u64 = String::from_utf8_lossy(&out.stdout).trim().parse().ok()?;
    Some(bytes / (1024 * 1024))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn memory() -> Option<u64> {
    None
}

/// This process's cgroup v2 path, from `/proc/self/cgroup`'s `0::<path>`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn cgroup_path(text: &str) -> Option<String> {
    text.lines()
        .find_map(|l| l.strip_prefix("0::"))
        .map(|p| p.trim().to_string())
}

/// The least `memory.max`, in MiB, of the cgroup at `path` and each above it;
/// None when none sets one. `read` reads a file under `/sys/fs/cgroup`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn cgroup_limit(path: &str, read: impl Fn(&str) -> Option<String>) -> Option<u64> {
    let mut least: Option<u64> = None;
    let mut at = path.trim_end_matches('/').to_string();
    loop {
        if let Some(text) = read(&format!("/sys/fs/cgroup{at}/memory.max")) {
            if let Ok(bytes) = text.trim().parse::<u64>() {
                let mib = bytes / (1024 * 1024);
                least = Some(least.map_or(mib, |l| l.min(mib)));
            }
        }
        match at.rfind('/') {
            Some(i) if !at.is_empty() => at.truncate(i),
            _ => break,
        }
    }
    least
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(cpu: f64, memory: u64) -> Resources {
        Resources {
            cpu: Cpu::of(cpu),
            memory,
        }
    }

    #[test]
    fn a_host_offers_what_it_measured_less_what_it_keeps_back() {
        let reserve = Reserve {
            cpu: 2.5,
            memory: 4096,
        };
        assert_eq!(allocatable(r(16.0, 13_000), &reserve, 0), Ok(r(13.5, 8904)));
        let err = allocatable(r(2.0, 13_000), &reserve, 0).unwrap_err();
        assert!(err.contains("nothing is left for jobs"), "{err}");
        let err = allocatable(r(16.0, 4200), &reserve, 512).unwrap_err();
        assert!(err.contains("104 MiB is left"), "{err}");
    }

    #[test]
    fn the_machine_is_measured() {
        let m = measure().unwrap();
        assert!(m.cpu >= Cpu::of(1.0));
        if cfg!(target_os = "linux") {
            assert!(m.memory > 0);
        }
    }

    #[test]
    fn memory_is_the_least_of_the_cgroups_above_this_process() {
        assert_eq!(
            cgroup_path("0::/user.slice/user-1000.slice/user@1000.service/app.slice/x.service\n"),
            Some("/user.slice/user-1000.slice/user@1000.service/app.slice/x.service".into())
        );
        assert_eq!(cgroup_path("1:name=systemd:/\n"), None);
        let files = |p: &str| match p {
            "/sys/fs/cgroup/a/b/c/memory.max" => Some("max\n".to_string()),
            "/sys/fs/cgroup/a/b/memory.max" => Some(format!("{}\n", 6u64 << 30)),
            "/sys/fs/cgroup/a/memory.max" => Some(format!("{}\n", 8u64 << 30)),
            _ => None,
        };
        assert_eq!(cgroup_limit("/a/b/c", files), Some(6144));
        assert_eq!(cgroup_limit("/a/b/c", |_| Some("max".into())), None);
        assert_eq!(cgroup_limit("/", files), None);
    }
}
