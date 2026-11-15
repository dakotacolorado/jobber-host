//! What std does not offer: a few calls declared directly rather than pulling
//! in libc for them, and the log line.

use std::ffi::{c_char, c_int, CStr};
use std::sync::atomic::{AtomicBool, Ordering};

const SIGHUP: c_int = 1;
const SIGINT: c_int = 2;
const SIGTERM: c_int = 15;
#[cfg(target_os = "linux")]
const SIGUSR2: c_int = 12;
#[cfg(not(target_os = "linux"))]
const SIGUSR2: c_int = 31;
const SIG_IGN: usize = 1;

extern "C" {
    fn signal(signum: c_int, handler: usize) -> usize;
    fn gethostname(name: *mut c_char, len: usize) -> c_int;
    fn getloadavg(loadavg: *mut f64, nelem: c_int) -> c_int;
}

static STOPPING: AtomicBool = AtomicBool::new(false);
static DRAINING: AtomicBool = AtomicBool::new(false);
static SAID_DRAINING: AtomicBool = AtomicBool::new(false);

extern "C" fn on_stop(_: c_int) {
    STOPPING.store(true, Ordering::SeqCst);
}

extern "C" fn on_drain(_: c_int) {
    DRAINING.store(true, Ordering::SeqCst);
}

/// SIGTERM (systemd stopping the unit) and SIGINT only set a flag: the
/// connection declines what it is sent and each running run is stopped and
/// reported before the process exits. SIGUSR2 sets another: the connection
/// declines what it is sent as when stopping, but running runs are left to
/// end, and the process exits once none is left, so a service manager that
/// restarts it puts a new release in its place. It is caught from here on, so
/// a deploy can tell (by `SigCgt`) that this release drains. SIGHUP is
/// ignored so a host started from a terminal outlives it.
pub fn handle_signals() {
    unsafe {
        signal(SIGTERM, on_stop as extern "C" fn(c_int) as usize);
        signal(SIGINT, on_stop as extern "C" fn(c_int) as usize);
        signal(SIGUSR2, on_drain as extern "C" fn(c_int) as usize);
        signal(SIGHUP, SIG_IGN);
    }
}

/// Told to stop now: running runs are stopped too.
pub fn stopping() -> bool {
    STOPPING.load(Ordering::SeqCst)
}

/// Told to drain: no new run is taken, and those running end on their own.
/// Said in the log the first time it is seen, since a handler cannot log.
pub fn draining() -> bool {
    let draining = DRAINING.load(Ordering::SeqCst);
    if draining && !SAID_DRAINING.swap(true, Ordering::SeqCst) {
        crate::log!("draining: declining new runs, and exiting once those running have ended");
    }
    draining
}

/// Taking no new run, for either reason.
pub fn winding_down() -> bool {
    stopping() || draining()
}

pub fn hostname() -> Option<String> {
    let mut buf = [0 as c_char; 256];
    if unsafe { gethostname(buf.as_mut_ptr(), buf.len() - 1) } != 0 {
        return None;
    }
    let name = unsafe { CStr::from_ptr(buf.as_ptr()) }.to_string_lossy();
    Some(name.split('.').next()?.to_string()).filter(|n| !n.is_empty())
}

/// The 1-minute load average.
pub fn load_average() -> Option<f64> {
    let mut load = [0f64; 1];
    (unsafe { getloadavg(load.as_mut_ptr(), 1) } == 1).then_some(load[0])
}

/// MiB free to an unprivileged user on the filesystem holding `path`.
#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
pub fn disk_free(path: &std::path::Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    // glibc's and musl's `struct statvfs` on 64-bit Linux begin with these;
    // `_rest` is room to spare for what follows.
    #[repr(C)]
    #[allow(dead_code)]
    struct Statvfs {
        f_bsize: u64,
        f_frsize: u64,
        f_blocks: u64,
        f_bfree: u64,
        f_bavail: u64,
        _rest: [u64; 32],
    }
    extern "C" {
        fn statvfs(path: *const c_char, buf: *mut Statvfs) -> c_int;
    }
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut buf = std::mem::MaybeUninit::<Statvfs>::zeroed();
    if unsafe { statvfs(path.as_ptr(), buf.as_mut_ptr()) } != 0 {
        return None;
    }
    let s = unsafe { buf.assume_init() };
    Some(s.f_bavail * s.f_frsize / (1024 * 1024))
}

/// Elsewhere `struct statvfs` differs, so POSIX `df` is asked rather than read it wrong.
#[cfg(not(all(target_os = "linux", target_pointer_width = "64")))]
pub fn disk_free(path: &std::path::Path) -> Option<u64> {
    let out = std::process::Command::new("df")
        .arg("-Pk")
        .arg(path)
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let kib: u64 = text
        .lines()
        .nth(1)?
        .split_whitespace()
        .nth(3)?
        .parse()
        .ok()?;
    Some(kib / 1024)
}

/// stderr may be closed once a terminal is gone, and `eprintln!` panics on a
/// failed write. Losing a log line is fine; losing the host over one is not.
/// Under systemd the journal timestamps each line.
#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {{
        use std::io::Write;
        let _ = writeln!(std::io::stderr(), $($arg)*);
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    extern "C" {
        fn raise(sig: c_int) -> c_int;
    }

    // The only test that signals the process: the flags are its own.
    #[test]
    fn sigusr2_is_caught_and_drains_without_stopping_and_twice_is_harmless() {
        handle_signals();
        #[cfg(target_os = "linux")]
        {
            let status = std::fs::read_to_string("/proc/self/status").unwrap();
            let caught = status
                .lines()
                .find_map(|l| l.strip_prefix("SigCgt:"))
                .map(|hex| u64::from_str_radix(hex.trim(), 16).unwrap())
                .unwrap();
            assert_ne!(caught & 1 << (SIGUSR2 - 1), 0, "SigCgt {caught:x}");
        }
        assert!(!draining() && !winding_down());
        for _ in 0..2 {
            assert_eq!(unsafe { raise(SIGUSR2) }, 0);
            assert!(draining() && winding_down());
            assert!(!stopping(), "draining leaves running runs be");
        }
    }
}
