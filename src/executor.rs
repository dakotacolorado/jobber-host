//! Runs a run jobber pushed here, after Nomad's podman driver and GitLab's
//! docker executor: a fresh clone at the run's commit, the repo's image (built
//! once per hash of what goes into it), `podman run` with the checkout
//! mounted writable as its workspace, the log streamed to jobber, the end
//! reported, and the workspace deleted.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::budget::{JobReservation, Shared};
use crate::credentials::{self, Kind, Var};
use crate::log;
use crate::retry::Executor;
use crate::service::{Appended, Assignment, End, Failure, Report, Service};

/// Where a job's repos are cloned from.
pub const GITHUB: &str = "https://github.com/";
/// Most one append carries; jobber takes up to 256 KiB.
const CHUNK: usize = 64 * 1024;
/// Used when neither the submitter nor `jobs.toml` gives a timeout.
const DEFAULT_TIMEOUT: u64 = 3600;
/// Seconds without progress before a job is ended as stalled, when `jobs.toml` gives none.
pub const DEFAULT_STALL: u64 = 600;
/// How often a running job's CPU and network are read.
const SAMPLE: Duration = Duration::from_secs(15);

pub struct Podman<S: Service> {
    pub service: Arc<S>,
    pub work_dir: PathBuf,
    pub git_credential_helper: Option<String>,
    /// The GitHub owner of the `base-<name>` repos local base images are built from.
    pub base_owner: String,
    pub grace: Duration,
    /// The systemd slice every container runs under, e.g. `jobber.slice`.
    pub cgroup_parent: Option<String>,
    /// `--platform` for every pull, build and run; none: the host's own.
    pub platform: Option<String>,
    /// MiB a container gets beyond the job's memory, which the budget reserves with it.
    pub memory_overhead: u64,
    /// Where the credentials a job names come from.
    pub credentials: Arc<dyn credentials::Source>,
    /// Where `owner/name` is cloned from: `https://github.com/` but in tests.
    pub git_base: String,
    /// One image build at a time.
    pub building: Mutex<()>,
    pub stopping: fn() -> bool,
}

// --- jobs.toml ---------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Defaults {
    memory: Option<u64>,
    timeout: Option<u64>,
    stall: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JobDef {
    run: Vec<String>,
    memory: Option<u64>,
    timeout: Option<u64>,
    /// Seconds without progress before it is ended as stalled; 0 for never.
    stall: Option<u64>,
    /// What it is sent for its run, e.g. `["aws", "github"]`; none if absent.
    #[serde(default)]
    credentials: Vec<Kind>,
    #[serde(default)]
    #[allow(dead_code)]
    labels: BTreeMap<String, String>,
    /// The machine a job-runner runs it on, on the host itself: jobber has no such
    /// runs, so a job that names one is refused rather than run in a container.
    host: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JobsToml {
    #[serde(default)]
    defaults: Defaults,
    #[serde(default)]
    jobs: BTreeMap<String, JobDef>,
}

/// What a job runs, and within what: the submitter's requirements, else the
/// job's own in `jobs.toml`, else its `[defaults]`.
#[derive(Debug, PartialEq, Eq)]
pub struct Plan {
    pub run: Vec<String>,
    pub memory: u64,
    /// CPUs its container may use, as admitted.
    pub cpu: crate::budget::Cpu,
    pub timeout: u64,
    /// Seconds without progress before it is ended as stalled; None for never.
    pub stall: Option<u64>,
    pub credentials: Vec<Kind>,
}

pub fn plan(text: &str, run: &Assignment) -> Result<Plan, String> {
    let file: JobsToml = toml::from_str(text).map_err(|e| format!("jobs.toml: {e}"))?;
    let name = &run.run.job;
    let def = file
        .jobs
        .get(name)
        .ok_or_else(|| format!("jobs.toml has no [jobs.{name}]"))?;
    if def.run.is_empty() {
        return Err(format!("[jobs.{name}] run is empty"));
    }
    if let Some(host) = &def.host {
        return Err(format!(
            "[jobs.{name}] runs on host {host} itself, which jobber does not do: jobber-host runs every run in its repo's container"
        ));
    }
    let requirements = &run.run.requirements;
    let memory = requirements
        .memory
        .or(def.memory)
        .or(file.defaults.memory)
        .unwrap_or(run.memory);
    if memory > run.memory {
        return Err(format!(
            "jobs.toml gives it {memory} MiB, over the {} MiB it was admitted with: submit it with requirements.memory = {memory}",
            run.memory
        ));
    }
    let timeout = requirements
        .timeout
        .or(def.timeout)
        .or(file.defaults.timeout)
        .unwrap_or(DEFAULT_TIMEOUT);
    let stall = def.stall.or(file.defaults.stall).unwrap_or(DEFAULT_STALL);
    let stall = (stall > 0).then_some(stall);
    let mut command = def.run.clone();
    command.extend(run.run.args.iter().cloned());
    let mut credentials = def.credentials.clone();
    credentials.sort();
    credentials.dedup();
    Ok(Plan {
        run: command,
        memory,
        cpu: run.cpu,
        timeout,
        stall,
        credentials,
    })
}

/// The log's line for a job's start: its limits first, so a long command
/// cut off where the line is shown cannot hide them.
pub fn launch_line(plan: &Plan) -> String {
    format!(
        "running with {} CPU, {} MiB, a {}s timeout and {}{}: {}",
        plan.cpu,
        plan.memory,
        plan.timeout,
        plan.stall.map_or("no stall limit".to_string(), |s| format!(
            "a {s}s stall limit"
        )),
        if plan.credentials.is_empty() {
            String::new()
        } else {
            let kinds: Vec<_> = plan.credentials.iter().map(|k| k.name()).collect();
            format!(", sent the host's {} credentials", kinds.join(" and "))
        },
        plan.run.join(" ")
    )
}

// --- stalls ------------------------------------------------------------------

/// What a running job has done so far, each a running total.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Sample {
    /// Bytes of output.
    pub log: u64,
    /// CPU time, its whole container's.
    pub cpu_ns: u64,
    /// Network bytes in and out of its container.
    pub net: u64,
}

/// A share of one CPU, as a fraction of 1000, that counts as working: an idle
/// process waiting on a dead socket uses far less, a silent compile far more.
const BUSY_PER_MILLE: u64 = 100;
/// Network bytes in a sample that count as moving; a dead socket moves none, and
/// stray packets on the container's interface are well under it.
const NET_BYTES: u64 = 1024;

/// Whether a job did anything between two samples `elapsed` apart: wrote any
/// output, used 10% of a CPU, or moved 1 KiB on the network. A `cdk deploy`
/// waiting on CloudFormation polls it, so it moves bytes; a stalled download does not.
pub fn progressed(before: &Sample, after: &Sample, elapsed: Duration) -> bool {
    let busy = elapsed.as_nanos() as u64 / 1000 * BUSY_PER_MILLE;
    after.log > before.log
        || after.cpu_ns.saturating_sub(before.cpu_ns) >= busy.max(1)
        || after.net.saturating_sub(before.net) >= NET_BYTES
}

/// Watches a job for a stall: no progress for `limit`.
pub struct Stall {
    limit: Duration,
    last: Option<(Duration, Sample)>,
    since: Duration,
}

impl Stall {
    pub fn new(limit: Duration) -> Stall {
        Stall {
            limit,
            last: None,
            since: Duration::ZERO,
        }
    }

    /// Takes a sample at `at` since the job started, None when it could not be
    /// read, which counts as progress: a job is never ended on what is not known.
    /// Answers how long it has gone without progress once that passes the limit.
    pub fn observe(&mut self, at: Duration, sample: Option<Sample>) -> Option<Duration> {
        let Some(sample) = sample else {
            self.since = at;
            self.last = None;
            return None;
        };
        match self.last {
            Some((then, before)) if !progressed(&before, &sample, at.saturating_sub(then)) => {}
            _ => self.since = at,
        }
        self.last = Some((at, sample));
        let idle = at.saturating_sub(self.since);
        (idle >= self.limit).then_some(idle)
    }
}

/// The container's CPU time and network bytes in and out, as podman reads them
/// from its cgroup and network namespace.
fn container_usage(name: &str) -> Result<(u64, u64), String> {
    let out = clean("podman")
        .args([
            "stats",
            "--no-stream",
            "--no-reset",
            "--format",
            "{{json .}}",
            name,
        ])
        .output()
        .map_err(|e| format!("podman stats: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "podman stats: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    parse_usage(&String::from_utf8_lossy(&out.stdout))
}

/// Network bytes are `NetInput` and `NetOutput` up to podman 5, and summed
/// over `Network`'s interfaces from podman 6, which has no `NetInput`.
fn parse_usage(text: &str) -> Result<(u64, u64), String> {
    let said = || format!("podman stats said {:?}", text.trim());
    let v: serde_json::Value =
        serde_json::from_str(text.trim()).map_err(|e| format!("{}: {e}", said()))?;
    let v = v.as_array().and_then(|a| a.first()).unwrap_or(&v);
    let n = |key: &str| v.get(key).and_then(serde_json::Value::as_u64);
    let cpu = n("CPUNano").ok_or_else(said)?;
    let net = match (n("NetInput"), n("NetOutput")) {
        (Some(rx), Some(tx)) => rx.saturating_add(tx),
        _ => v
            .get("Network")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(said)?
            .values()
            .map(|i| {
                let b = |k| i.get(k).and_then(serde_json::Value::as_u64).unwrap_or(0);
                b("RxBytes").saturating_add(b("TxBytes"))
            })
            .fold(0, u64::saturating_add),
    };
    Ok((cpu, net))
}

/// The image a Containerfile builds on: its `ARG BASE=` default if it has
/// one (the template's form), else its first `FROM`.
pub fn base_image(containerfile: &str) -> Option<String> {
    let mut arg = None;
    for line in containerfile.lines().map(str::trim) {
        let mut words = line.split_whitespace();
        match words.next().map(str::to_ascii_uppercase).as_deref() {
            Some("ARG") => {
                if let Some(v) = words.next().and_then(|w| w.strip_prefix("BASE=")) {
                    arg = Some(v.to_string());
                }
            }
            Some("FROM") => {
                let from = words.next()?;
                return Some(if from == "${BASE}" || from == "$BASE" {
                    arg?
                } else {
                    from.to_string()
                });
            }
            _ => {}
        }
    }
    None
}

/// The files an image is built from, for every toolchain a base serves; a
/// change to any of them is a new image. One a repo lacks hashes as absent.
pub const IMAGE_INPUTS: [&str; 12] = [
    "Containerfile",
    // Python
    ".python-version",
    "pyproject.toml",
    "uv.lock",
    // Node
    ".nvmrc",
    ".node-version",
    "package.json",
    "package-lock.json",
    // Rust
    "rust-toolchain",
    "rust-toolchain.toml",
    "Cargo.toml",
    "Cargo.lock",
];

/// A base this host builds itself: `localhost/base-<name>:<n>` is the repo
/// `base-<name>` at tag `v<n>`, as base-python, base-node and base-rust are
/// released. Any other image is pulled.
pub fn local_base(image: &str) -> Option<(&str, &str)> {
    let (name, version) = image.strip_prefix("localhost/base-")?.split_once(':')?;
    let name_ok = !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    let version_ok = !version.is_empty() && version.bytes().all(|b| b.is_ascii_digit());
    (name_ok && version_ok).then_some((name, version))
}

/// `localhost/job-<repo>:<hash>`, the hash over the base image's id and each input file.
pub fn image_tag(repo: &str, base_id: &str, inputs: &[(&str, Vec<u8>)]) -> String {
    let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
    ctx.update(base_id.as_bytes());
    for (name, bytes) in inputs {
        ctx.update(&[0]);
        ctx.update(name.as_bytes());
        ctx.update(&(bytes.len() as u64).to_be_bytes());
        ctx.update(bytes);
    }
    let hex: String = ctx.finish().as_ref()[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let slug: String = repo
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    format!("localhost/job-{slug}:{hex}")
}

// --- the log -----------------------------------------------------------------

/// What a masked value reads as in a job's log.
const MASK: &[u8] = b"***";
/// How much of the end of a job's output is kept for its last line.
const TAIL: usize = 4096;
/// Most of a last line a failure's reason carries.
const LAST_LINE: usize = 500;

#[derive(Default)]
struct LogState {
    /// Written, not yet masked: the tail of it that could be the start of a
    /// secret is kept back until more comes or the log closes.
    raw: Vec<u8>,
    /// Values never stored as written, longest first.
    secrets: Vec<Vec<u8>>,
    /// Masked, not yet stored.
    pending: Vec<u8>,
    offset: u64,
    /// Bytes written by the job and jobber-host so far, kept or not.
    written: u64,
    /// The last `TAIL` bytes written, unmasked.
    tail: Vec<u8>,
    canceled: bool,
    ended: Option<String>,
    too_large: bool,
    closing: bool,
    done: bool,
}

/// The run's log on its way to jobber: written here, appended about every
/// second, and renewed at least every `Service::renew_every` while nothing is written.
pub struct Log {
    state: Arc<(Mutex<LogState>, Condvar)>,
    sender: Option<thread::JoinHandle<()>>,
}

impl LogState {
    fn push(&mut self, bytes: &[u8]) {
        self.written += bytes.len() as u64;
        self.tail.extend_from_slice(bytes);
        if self.tail.len() > TAIL {
            self.tail.drain(..self.tail.len() - TAIL);
        }
        if !self.too_large && self.ended.is_none() {
            self.raw.extend_from_slice(bytes);
        }
    }

    /// Moves what is written to `pending`, each secret in it masked; all of
    /// it when `all`, else all but what a secret may still be starting in.
    fn mask(&mut self, all: bool) {
        if self.raw.is_empty() {
            return;
        }
        let masked = mask(&self.raw, &self.secrets);
        let keep = if all {
            0
        } else {
            self.secrets.first().map_or(0, |s| s.len() - 1)
        };
        let split = masked.len() - keep.min(masked.len());
        self.pending.extend_from_slice(&masked[..split]);
        self.raw = masked[split..].to_vec();
    }
}

/// `text` with every occurrence of each of `secrets`, in order, replaced by `***`.
fn mask(text: &[u8], secrets: &[Vec<u8>]) -> Vec<u8> {
    let mut text = text.to_vec();
    for secret in secrets {
        let mut out = Vec::with_capacity(text.len());
        let mut i = 0;
        while i < text.len() {
            if text[i..].starts_with(secret) {
                out.extend_from_slice(MASK);
                i += secret.len();
            } else {
                out.push(text[i]);
                i += 1;
            }
        }
        text = out;
    }
    text
}

impl Log {
    pub fn start<S: Service>(service: Arc<S>, job: String) -> Log {
        let state: Arc<(Mutex<LogState>, Condvar)> = Arc::default();
        let shared = state.clone();
        let sender = thread::spawn(move || send(&*service, &job, &shared));
        Log {
            state,
            sender: Some(sender),
        }
    }

    pub fn write(&self, bytes: &[u8]) {
        let mut s = self.state.0.lock().unwrap_or_else(|e| e.into_inner());
        s.push(bytes);
    }

    /// From now on each of `values` reads `***` in the log: a backstop for a
    /// job that prints a credential it was sent.
    pub fn mask(&self, values: impl IntoIterator<Item = String>) {
        let mut s = self.state.0.lock().unwrap_or_else(|e| e.into_inner());
        // A value this short would mask ordinary output, and is no credential.
        s.secrets.extend(
            values
                .into_iter()
                .filter(|v| v.len() >= 4)
                .map(String::into_bytes),
        );
        // Longest first: a shorter value inside a longer one would leave the rest of it showing.
        s.secrets.sort_by_key(|v| std::cmp::Reverse(v.len()));
        s.secrets.dedup();
    }

    /// A line of jobber-host's own, marked so it reads apart from the job's.
    pub fn say(&self, line: &str) {
        self.write(format!("[jobber-host] {line}\n").as_bytes());
    }

    /// The last line the job itself wrote, masked, without jobber-host's own
    /// lines; None if there is none, or it began before what is kept of the end.
    pub fn last_line(&self) -> Option<String> {
        let s = self.state.0.lock().unwrap_or_else(|e| e.into_inner());
        let whole = s.written as usize <= s.tail.len();
        let text = String::from_utf8_lossy(&s.tail).into_owned();
        let mut lines: Vec<&str> = text.split('\n').collect();
        if !whole {
            // The first may be the end of a longer line, or of a secret, cut off.
            lines.remove(0);
        }
        let line = lines
            .into_iter()
            .map(|l| l.rsplit('\r').next().unwrap_or("").trim())
            .rfind(|l| !l.is_empty() && !l.starts_with("[jobber-host] "))?;
        let masked = String::from_utf8_lossy(&mask(line.as_bytes(), &s.secrets)).into_owned();
        Some(if masked.chars().count() > LAST_LINE {
            let cut: String = masked.chars().take(LAST_LINE).collect();
            format!("{cut}…")
        } else {
            masked
        })
    }

    /// Bytes written to it so far.
    pub fn written(&self) -> u64 {
        self.state
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .written
    }

    pub fn canceled(&self) -> bool {
        self.state
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .canceled
    }

    /// Why jobber no longer takes this run's log (it ended there), if it does not.
    pub fn ended(&self) -> Option<String> {
        self.state
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .ended
            .clone()
    }

    /// Sends what is left, giving up after `within`.
    pub fn close(mut self, within: Duration) {
        {
            let mut s = self.state.0.lock().unwrap_or_else(|e| e.into_inner());
            s.closing = true;
        }
        self.state.1.notify_all();
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if self.state.0.lock().unwrap_or_else(|e| e.into_inner()).done {
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        self.state.0.lock().unwrap_or_else(|e| e.into_inner()).done = true;
        self.state.1.notify_all();
        if let Some(t) = self.sender.take() {
            let _ = t.join();
        }
    }
}

fn send<S: Service + ?Sized>(service: &S, job: &str, state: &(Mutex<LogState>, Condvar)) {
    let mut last = Instant::now();
    loop {
        let (chunk, offset) = {
            let s = state.0.lock().unwrap_or_else(|e| e.into_inner());
            let (mut s, _) = state
                .1
                .wait_timeout_while(s, Duration::from_secs(1), |s| !s.closing && !s.done)
                .unwrap_or_else(|e| e.into_inner());
            let closing = s.closing;
            s.mask(closing);
            if s.done || s.ended.is_some() || (s.closing && s.pending.is_empty()) {
                s.done = true;
                return;
            }
            if s.pending.is_empty() && last.elapsed() < service.renew_every() {
                continue;
            }
            let n = s.pending.len().min(CHUNK);
            (s.pending[..n].to_vec(), s.offset)
        };
        let result = service.append(job, offset, &chunk);
        let mut s = state.0.lock().unwrap_or_else(|e| e.into_inner());
        match result {
            Ok(Appended::Stored { size, canceled }) => {
                last = Instant::now();
                s.pending.drain(..chunk.len());
                s.offset = size;
                s.canceled |= canceled;
            }
            Ok(Appended::Resend { size }) => {
                // Stored before a reply was lost: skip what jobber already has.
                if size > s.offset {
                    let skip = ((size - s.offset) as usize).min(s.pending.len());
                    s.pending.drain(..skip);
                }
                s.offset = size;
            }
            Ok(Appended::TooLarge(why)) => {
                log!("run {job}: log not stored past {} bytes: {why}", s.offset);
                s.too_large = true;
                s.pending.clear();
            }
            Ok(Appended::Ended(why)) => {
                log!("run {job}: jobber no longer takes its log: {why}");
                s.ended = Some(why);
            }
            Err(e) => {
                log!("run {job}: append failed, retrying: {e}");
                drop(s);
                thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

// --- running it --------------------------------------------------------------

/// How a job ended, before it is reported.
#[derive(Debug)]
struct Outcome {
    state: End,
    exit: Option<i32>,
    reason: Option<String>,
}

impl Outcome {
    fn failed(reason: impl Into<String>) -> Outcome {
        Outcome {
            state: End::Failed,
            exit: None,
            reason: Some(reason.into()),
        }
    }
}

/// `program` with nothing of jobber-host's environment but what podman and
/// git need to find the user's own storage, runtime dir and credentials.
fn clean(program: &str) -> Command {
    let mut cmd = Command::new(program);
    cmd.env_clear();
    for key in [
        "PATH",
        "HOME",
        "USER",
        "XDG_RUNTIME_DIR",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "DBUS_SESSION_BUS_ADDRESS",
    ] {
        if let Ok(v) = std::env::var(key) {
            cmd.env(key, v);
        }
    }
    cmd.stdin(Stdio::null());
    cmd
}

/// Runs `cmd` to its end with its output in the log; false, with the reason
/// said, if it could not start or exited non-zero.
fn logged(log: &Log, mut cmd: Command, what: &str) -> Result<(), String> {
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| format!("{what}: {e}"))?;
    let readers = pipe(log, &mut child);
    let status = child.wait().map_err(|e| format!("{what}: {e}"))?;
    for r in readers {
        let _ = r.join();
    }
    if status.success() {
        Ok(())
    } else {
        Err(format!("{what} failed ({status})"))
    }
}

fn pipe(log: &Log, child: &mut std::process::Child) -> Vec<thread::JoinHandle<()>> {
    let state = log.state.clone();
    let mut readers = Vec::new();
    let streams: Vec<Box<dyn Read + Send>> = [
        child
            .stdout
            .take()
            .map(|s| Box::new(s) as Box<dyn Read + Send>),
        child
            .stderr
            .take()
            .map(|s| Box::new(s) as Box<dyn Read + Send>),
    ]
    .into_iter()
    .flatten()
    .collect();
    for mut stream in streams {
        let state = state.clone();
        readers.push(thread::spawn(move || {
            let mut buf = [0u8; 8192];
            while let Ok(n) = stream.read(&mut buf) {
                if n == 0 {
                    break;
                }
                let mut s = state.0.lock().unwrap_or_else(|e| e.into_inner());
                s.push(&buf[..n]);
            }
        }));
    }
    readers
}

/// Deletes `dir` and everything in it, even what a job made read-only.
pub fn remove(dir: &Path) {
    if std::fs::remove_dir_all(dir).is_ok() || !dir.exists() {
        return;
    }
    // A directory without write permission keeps its entries: give it back.
    fn writable(path: &Path) {
        use std::os::unix::fs::PermissionsExt;
        let Ok(meta) = std::fs::symlink_metadata(path) else {
            return;
        };
        if !meta.is_dir() {
            return;
        }
        let mut perms = meta.permissions();
        perms.set_mode(perms.mode() | 0o700);
        let _ = std::fs::set_permissions(path, perms);
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                writable(&entry.path());
            }
        }
    }
    writable(dir);
    if let Err(e) = std::fs::remove_dir_all(dir) {
        log!("{}: not removed: {e}", dir.display());
    }
}

/// Whether `name` is a directory jobber-host makes in `work_dir`: a run's id
/// (jobber's are UUIDs), or that with `-base` for a base image's checkout.
fn is_job_dir(name: &str) -> bool {
    let id = name.strip_suffix("-base").unwrap_or(name);
    id.len() == 36
        && id.bytes().enumerate().all(|(i, b)| match i {
            8 | 13 | 18 | 23 => b == b'-',
            _ => b.is_ascii_hexdigit(),
        })
}

/// Deletes what runs left in `work_dir` when jobber-host stopped mid-run (the
/// host suspended or unplugged), and only that, should `work_dir` hold anything else.
pub fn sweep(work_dir: &Path) {
    let Ok(entries) = std::fs::read_dir(work_dir) else {
        return;
    };
    for entry in entries.flatten() {
        if entry.file_name().to_str().is_some_and(is_job_dir) {
            log!(
                "removing {}, left by a run that did not finish here",
                entry.path().display()
            );
            remove(&entry.path());
        }
    }
}

fn image_id(image: &str) -> Option<String> {
    let out = clean("podman")
        .args(["image", "inspect", "--format", "{{.Id}}", image])
        .stderr(Stdio::null())
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

impl<S: Service> Podman<S> {
    /// An image a multi-arch digest pins is pulled for the host's own arch
    /// unless told: without this an arm64 mac mixes arm64 and amd64 stages.
    fn platform_arg(&self) -> Option<String> {
        self.platform.as_ref().map(|p| format!("--platform={p}"))
    }

    fn git(&self, dir: &Path) -> Command {
        let mut cmd = clean("git");
        cmd.current_dir(dir).env("GIT_TERMINAL_PROMPT", "0");
        if let Some(helper) = &self.git_credential_helper {
            // An empty helper first clears any the user's config adds.
            cmd.args(["-c", "credential.helper=", "-c"])
                .arg(format!("credential.helper={helper}"));
        }
        cmd
    }

    /// A fresh checkout of `repo` at `rev` (a commit or a tag) in `dir`.
    fn checkout(&self, log: &Log, repo: &str, rev: &str, dir: &Path) -> Result<(), String> {
        self.checkout_from(log, &format!("{}{repo}.git", self.git_base), rev, dir)
    }

    /// With all of `rev`'s history, not `--depth 1`: packages number their
    /// version by `git rev-list --count HEAD`, which a shallow checkout reads as 1.
    fn checkout_from(&self, log: &Log, url: &str, rev: &str, dir: &Path) -> Result<(), String> {
        remove(dir);
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        let steps: [&[&str]; 4] = [
            &["init", "-q"],
            &["remote", "add", "origin", url],
            &["fetch", "-q", "origin", rev],
            &["checkout", "-q", "FETCH_HEAD"],
        ];
        for args in steps {
            let mut cmd = self.git(dir);
            cmd.args(args);
            logged(log, cmd, &format!("git {}", args[0]))?;
        }
        Ok(())
    }

    /// The base image's id: built from `<base_owner>/base-<name>` at `v<n>`
    /// when it is `localhost/base-<name>:<n>` and this host lacks it, else pulled.
    fn base(&self, log: &Log, image: &str, job: &str) -> Result<String, String> {
        if let Some(id) = image_id(image) {
            return Ok(id);
        }
        let Some((name, version)) = local_base(image) else {
            log.say(&format!("pulling {image}"));
            let mut cmd = clean("podman");
            cmd.args(["pull", "-q"])
                .args(self.platform_arg())
                .arg(image);
            logged(log, cmd, "podman pull of the base image")?;
            return image_id(image).ok_or_else(|| format!("{image} was pulled but cannot be read"));
        };
        let repo = format!("{}/base-{name}", self.base_owner);
        log.say(&format!("building {image} from {repo} at v{version}"));
        let dir = self.work_dir.join(format!("{job}-base"));
        let result = self
            .checkout(log, &repo, &format!("refs/tags/v{version}"), &dir)
            .and_then(|()| {
                let mut cmd = clean("podman");
                cmd.args(["build", "--pull=missing"])
                    .args(self.platform_arg())
                    .args(["-t", image])
                    .arg(&dir);
                logged(log, cmd, "podman build of the base image")
            });
        remove(&dir);
        result?;
        image_id(image).ok_or_else(|| format!("{image} was built but cannot be read"))
    }

    /// The job's image, built unless one with its hash is already here, from the
    /// `Containerfile` in `dir`, with `dir` as its context; `name` names it.
    fn image(
        &self,
        log: &Log,
        run: &Assignment,
        dir: &Path,
        name: &str,
        job: &JobReservation,
        budget: &Shared,
    ) -> Result<String, Outcome> {
        let read = |name: &str| std::fs::read(dir.join(name));
        let containerfile =
            read("Containerfile").map_err(|e| Outcome::failed(format!("no Containerfile: {e}")))?;
        let base = base_image(&String::from_utf8_lossy(&containerfile))
            .ok_or_else(|| Outcome::failed("the Containerfile names no base image"))?;
        // One build at a time, the base's included: two jobs on a new host build it once.
        let _one = self.building.lock().unwrap_or_else(|e| e.into_inner());
        let base_id = self
            .base(log, &base, &run.run.id)
            .map_err(|e| Outcome::failed(format!("image build failed: {e}")))?;
        let inputs: Vec<(&str, Vec<u8>)> = IMAGE_INPUTS
            .iter()
            .map(|n| (*n, read(n).unwrap_or_default()))
            .collect();
        let tag = image_tag(name, &base_id, &inputs);
        if image_id(&tag).is_some() {
            log.say(&format!("using image {tag}"));
            return Ok(tag);
        }
        let give_up = || log.canceled() || log.ended().is_some() || (self.stopping)();
        let Some(_build) = budget.reserve_build(job, give_up) else {
            return Err(Outcome::failed(
                "gave up waiting for memory to build its image",
            ));
        };
        log.say(&format!("building image {tag}"));
        let mut cmd = clean("podman");
        cmd.args(["build", "--pull=missing"])
            .args(self.platform_arg())
            .args(["--build-arg", &format!("BASE={base}"), "-t", &tag])
            .arg(dir);
        logged(log, cmd, "podman build")
            .map_err(|e| Outcome::failed(format!("image build failed: {e}")))?;
        Ok(tag)
    }

    fn run_job(
        &self,
        log: &Log,
        run: &Assignment,
        job: &JobReservation,
        budget: &Shared,
    ) -> Outcome {
        let id = &run.run.id;
        let dir = self.work_dir.join(id);
        log.say(&format!(
            "checking out {} at {}",
            run.run.repo, run.run.commit
        ));
        if let Err(e) = self.checkout(log, &run.run.repo, &run.run.commit, &dir) {
            return Outcome::failed(format!("checkout failed: {e}"));
        }
        let plan = match std::fs::read_to_string(dir.join("jobs.toml")) {
            Err(e) => Err(format!("no jobs.toml: {e}")),
            Ok(text) => plan(&text, run),
        };
        let plan = match plan {
            Ok(plan) => plan,
            Err(e) => {
                log.say(&e);
                return Outcome::failed(e);
            }
        };
        let tag = match self.image(log, run, &dir, &run.run.repo, job, budget) {
            Ok(tag) => tag,
            Err(outcome) => {
                if let Some(why) = &outcome.reason {
                    log.say(why);
                }
                return outcome;
            }
        };
        let env = match credentials::gather(&*self.credentials, run, &plan.credentials) {
            Ok(env) => env,
            Err(e) => {
                log.say(&e);
                return Outcome::failed(e);
            }
        };
        log.mask(env.iter().filter(|v| v.secret).map(|v| v.value.clone()));
        if log.canceled() {
            return Outcome {
                state: End::Canceled,
                exit: None,
                reason: Some("canceled before it started".into()),
            };
        }
        self.contain(log, &tag, &plan, &env, &dir, &format!("job-{id}"))
    }

    /// Runs the job's container, `name`, to its end, stopping it for a
    /// cancel, its timeout, jobber ending it, or jobber-host stopping.
    /// `env` is what it is sent beyond its image's environment: named to
    /// podman by `--env NAME` alone, the value in podman's own environment.
    fn contain(
        &self,
        log: &Log,
        tag: &str,
        plan: &Plan,
        env: &[Var],
        dir: &Path,
        name: &str,
    ) -> Outcome {
        let limit = plan.memory + self.memory_overhead;
        let mut cmd = {
            let mut cmd = clean("podman");
            // `--init`: a job is not PID 1, so SIGTERM reaches it rather than being ignored.
            // No `--rm`: how the container ended is read after `podman run` returns (`ended`), then it is removed.
            cmd.args(["run", "--init", "--name", name, "--pull=never"])
                .args(self.platform_arg())
                .arg(format!("--memory={limit}m"))
                .arg(format!("--memory-swap={limit}m"))
                .args(
                    (plan.cpu > crate::budget::Cpu::default())
                        .then(|| format!("--cpus={}", plan.cpu)),
                )
                .args(["--userns=keep-id:uid=1000,gid=1000", "-w", "/work"])
                .arg("-v")
                // Writable, so installs and build output land beside the sources; the
                // checkout is the job's own and deleted after it, with the container.
                .arg(format!("{}:/work:Z", dir.display()));
            if let Some(parent) = &self.cgroup_parent {
                cmd.arg(format!("--cgroup-parent={parent}"));
            }
            for var in env {
                cmd.arg("--env").arg(var.name).env(var.name, &var.value);
            }
            cmd.arg(tag).args(&plan.run);
            cmd
        };
        log.say(&launch_line(plan));
        if self.memory_overhead > 0 {
            log.say(&format!(
                "its container may use {} MiB: this host gives each job {} MiB more than its memory",
                plan.memory + self.memory_overhead,
                self.memory_overhead
            ));
        }
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => return Outcome::failed(format!("podman run: {e}")),
        };
        let readers = pipe(log, &mut child);
        let started = Instant::now();
        let mut stall = plan.stall.map(|s| Stall::new(Duration::from_secs(s)));
        let mut sampled: Option<Instant> = None;
        let mut unread = false;
        let mut stopped: Option<Outcome> = None;
        let status: std::io::Result<ExitStatus> = loop {
            match child.try_wait() {
                Ok(Some(status)) => break Ok(status),
                Err(e) => break Err(e),
                Ok(None) => {}
            }
            let mut stalled = None;
            if let Some(watch) = stall.as_mut().filter(|_| stopped.is_none()) {
                if sampled.is_none_or(|t| t.elapsed() >= SAMPLE) {
                    sampled = Some(Instant::now());
                    let sample = match container_usage(name) {
                        Ok((cpu_ns, net)) => Some(Sample {
                            log: log.written(),
                            cpu_ns,
                            net,
                        }),
                        Err(e) => {
                            // Before podman has the container up, or on a host it cannot read.
                            if !unread && started.elapsed() > SAMPLE {
                                unread = true;
                                log!("{name}: its progress cannot be read, so it is not watched for a stall meanwhile: {e}");
                            }
                            None
                        }
                    };
                    stalled = watch.observe(started.elapsed(), sample);
                }
            }
            if stopped.is_none() {
                let why = if log.canceled() {
                    Some(Outcome {
                        state: End::Canceled,
                        exit: None,
                        reason: None,
                    })
                } else if started.elapsed() > Duration::from_secs(plan.timeout) {
                    Some(Outcome::failed(format!(
                        "timed out after {}s",
                        plan.timeout
                    )))
                } else if let Some(idle) = stalled {
                    Some(Outcome::failed(format!(
                        "stalled: no output, under 10% of a CPU and under 1 KiB of network every {}s, for {}s (its stall limit is {}s)",
                        SAMPLE.as_secs(),
                        idle.as_secs(),
                        plan.stall.unwrap_or_default()
                    )))
                } else if let Some(why) = log.ended() {
                    Some(Outcome::failed(format!("jobber ended it: {why}")))
                } else if (self.stopping)() {
                    Some(Outcome::failed("jobber-host was stopped"))
                } else {
                    None
                };
                if let Some(outcome) = why {
                    log.say(&format!(
                        "stopping it: {}",
                        outcome.reason.as_deref().unwrap_or("canceled")
                    ));
                    let mut stop = clean("podman");
                    stop.args(["stop", "-t", &self.grace.as_secs().to_string(), name]);
                    let _ = stop.stdout(Stdio::null()).stderr(Stdio::null()).status();
                    stopped = Some(outcome);
                }
            }
            thread::sleep(Duration::from_millis(200));
        };
        for r in readers {
            let _ = r.join();
        }
        let container = ended(name);
        remove_container(name);
        let exit = status.ok().and_then(|s| s.code());
        if let Some(mut outcome) = stopped {
            outcome.exit = exit;
            return outcome;
        }
        // The memory cgroup's OOM killer may take conmon, not the job: `podman run` then answers 0,
        // so how the container ended is what says it ran out of memory.
        if let Some(why) = container.as_ref().and_then(out_of_memory) {
            let why = format!(
                "killed (exit 137): {why}, past its {} MiB memory limit",
                plan.memory + self.memory_overhead
            );
            return Outcome {
                state: End::Failed,
                exit: Some(137),
                reason: Some(match log.last_line() {
                    Some(line) => format!("{why}; its last line: {line}"),
                    None => why,
                }),
            };
        }
        match exit {
            Some(0) => Outcome {
                state: End::Succeeded,
                exit,
                reason: None,
            },
            Some(code) => {
                let why = if code == 137 {
                    format!(
                        "killed (exit 137): most likely past its {} MiB memory limit",
                        plan.memory + self.memory_overhead
                    )
                } else {
                    format!("exited {code}")
                };
                Outcome {
                    state: End::Failed,
                    exit,
                    reason: Some(match log.last_line() {
                        Some(line) => format!("{why}; its last line: {line}"),
                        None => why,
                    }),
                }
            }
            None => Outcome::failed("podman run ended without an exit code"),
        }
    }
}

/// How a job's container stood once `podman run` returned, as `podman inspect` says.
#[derive(Debug, PartialEq)]
struct Ended {
    oom_killed: bool,
    running: bool,
}

/// `podman inspect`'s `{{.State.OOMKilled}} {{.State.Running}}`, read.
fn parse_ended(text: &str) -> Option<Ended> {
    let mut words = text.split_whitespace();
    let flag = |word: Option<&str>| match word {
        Some("true") => Some(true),
        Some("false") => Some(false),
        _ => None,
    };
    Some(Ended {
        oom_killed: flag(words.next())?,
        running: flag(words.next())?,
    })
}

/// None when podman cannot say, as for a container that never started.
fn ended(name: &str) -> Option<Ended> {
    let out = clean("podman")
        .args([
            "inspect",
            "--format",
            "{{.State.OOMKilled}} {{.State.Running}}",
            name,
        ])
        .stderr(Stdio::null())
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| parse_ended(&String::from_utf8_lossy(&out.stdout)))
        .flatten()
}

/// What says a job ran out of memory: its container was OOM-killed, or it outlived its monitor, as when the
/// OOM killer takes conmon in the job's cgroup.
fn out_of_memory(container: &Ended) -> Option<&'static str> {
    if container.oom_killed {
        Some("out of memory")
    } else if container.running {
        Some("its container monitor was killed while it ran, as the OOM killer does")
    } else {
        None
    }
}

/// The container gone, stopped first if it still runs.
fn remove_container(name: &str) {
    let _ = clean("podman")
        .args(["rm", "--force", "--time", "0", name])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

impl<S: Service> Executor for Podman<S> {
    fn run(&self, run: Assignment, reservation: JobReservation, budget: &Shared) {
        let id = run.run.id.clone();
        let started = Instant::now();
        let log = Log::start(self.service.clone(), id.clone());
        let outcome = self.run_job(&log, &run, &reservation, budget);
        log.say(&format!(
            "{}{}{}",
            match outcome.state {
                End::Succeeded => "succeeded",
                End::Failed => "failed",
                End::Canceled => "canceled",
            },
            outcome
                .exit
                .map(|e| format!(" with exit {e}"))
                .unwrap_or_default(),
            outcome
                .reason
                .as_deref()
                .map(|r| format!(": {r}"))
                .unwrap_or_default()
        ));
        log.close(Duration::from_secs(30));
        remove(&self.work_dir.join(&id));
        drop(reservation);
        let report = Report {
            state: outcome.state,
            exit: outcome.exit,
            duration: started.elapsed().as_secs_f64(),
            reason: outcome.reason,
        };
        log!("run {id}: {:?}", report);
        for attempt in 1..=10 {
            match self.service.report(&id, &report) {
                Ok(()) => return,
                Err(Failure::Refused(code, body)) => {
                    log!("run {id}: report refused {code}: {body}");
                    return;
                }
                Err(e) => {
                    log!("run {id}: report failed (try {attempt}): {e}");
                    thread::sleep(Duration::from_secs(attempt * 2));
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    #[test]
    fn a_container_oom_killed_or_outliving_its_monitor_ran_out_of_memory() {
        let ended = |text| parse_ended(text);
        assert_eq!(
            ended("true false\n"),
            Some(Ended {
                oom_killed: true,
                running: false
            })
        );
        assert_eq!(
            ended("false true"),
            Some(Ended {
                oom_killed: false,
                running: true
            })
        );
        assert_eq!(ended(""), None);
        assert_eq!(ended("<no value> false"), None);
        assert!(out_of_memory(&ended("true false").unwrap()).is_some());
        assert!(out_of_memory(&ended("false true").unwrap()).is_some());
        assert!(out_of_memory(&ended("false false").unwrap()).is_none());
    }

    use super::*;
    use crate::budget::{Budget, Cpu, Resources};
    use crate::service::{HostRun, Requirements};

    /// A run of `job` requesting `memory` and `timeout`, admitted with half a CPU and `admitted` MiB.
    pub fn assignment(
        job: &str,
        memory: Option<u64>,
        timeout: Option<u64>,
        admitted: u64,
    ) -> Assignment {
        Assignment {
            run: HostRun {
                id: "j".into(),
                repo: "o/r".into(),
                commit: "0".repeat(40),
                job: job.into(),
                args: vec!["x".into()],
                requirements: Requirements {
                    cpu: None,
                    memory,
                    timeout,
                },
                labels: BTreeMap::new(),
            },
            cpu: Cpu::of(0.5),
            memory: admitted,
            lease: 60,
        }
    }

    fn budget(cpu: f64, memory: u64, build_memory: u64) -> Shared {
        Shared::new(Budget::new(
            Resources {
                cpu: Cpu::of(cpu),
                memory,
            },
            build_memory,
        ))
    }

    const TOML: &str = r#"
[defaults]
memory = 512
timeout = 600

[jobs.hello]
run = ["python", "jobs/hello.py"]
memory = 256
timeout = 60

[jobs.plain]
run = ["python", "jobs/plain.py"]
"#;

    #[test]
    fn limits_come_from_the_submitter_then_the_job_then_the_defaults() {
        let p = plan(TOML, &assignment("hello", None, None, 1024)).unwrap();
        assert_eq!(
            p,
            Plan {
                run: vec!["python".into(), "jobs/hello.py".into(), "x".into()],
                memory: 256,
                cpu: Cpu::of(0.5),
                timeout: 60,
                stall: Some(DEFAULT_STALL),
                credentials: vec![],
            }
        );
        let p = plan(TOML, &assignment("plain", None, None, 1024)).unwrap();
        assert_eq!((p.memory, p.timeout), (512, 600));
        let p = plan(TOML, &assignment("hello", Some(128), Some(5), 128)).unwrap();
        assert_eq!((p.memory, p.timeout), (128, 5));
    }

    #[test]
    fn the_stall_limit_is_the_jobs_then_the_defaults_then_ten_minutes_and_0_is_never() {
        let toml = "[defaults]\nstall = 120\n[jobs.a]\nrun = [\"x\"]\nstall = 30\n\
                    [jobs.b]\nrun = [\"x\"]\n[jobs.c]\nrun = [\"x\"]\nstall = 0\n";
        let stall = |toml: &str, job: &str| {
            plan(toml, &assignment(job, None, None, 1024))
                .unwrap()
                .stall
        };
        assert_eq!(stall(toml, "a"), Some(30));
        assert_eq!(stall(toml, "b"), Some(120));
        assert_eq!(stall(toml, "c"), None);
        assert_eq!(stall(TOML, "plain"), Some(DEFAULT_STALL));
        assert_eq!(DEFAULT_STALL, 600);
    }

    #[test]
    fn the_launch_line_gives_every_limit_before_the_command() {
        let mut p = plan(
            "[defaults]\nmemory = 1024\ntimeout = 1800\n[jobs.deploy-beta]\n\
             run = [\"sh\", \"-c\", \"npm ci && npm run deploy:beta\"]\ncredentials = [\"aws\"]\n",
            &assignment("deploy-beta", None, None, 1024),
        )
        .unwrap();
        p.run.pop();
        assert_eq!(
            launch_line(&p),
            "running with 0.5 CPU, 1024 MiB, a 1800s timeout and a 600s stall limit, sent the host's aws credentials: sh -c npm ci && npm run deploy:beta"
        );
        p.stall = None;
        p.credentials.clear();
        assert_eq!(
            launch_line(&p),
            "running with 0.5 CPU, 1024 MiB, a 1800s timeout and no stall limit: sh -c npm ci && npm run deploy:beta"
        );
    }

    #[test]
    fn output_cpu_or_network_is_progress_and_an_idle_socket_is_not() {
        let at = |log, cpu_ns, net| Sample { log, cpu_ns, net };
        let window = Duration::from_secs(15);
        let before = at(100, 5_000_000_000, 106_100_000);
        assert!(progressed(
            &before,
            &at(101, 5_000_000_000, 106_100_000),
            window
        ));
        assert!(progressed(
            &before,
            &at(100, 6_500_000_000, 106_100_000),
            window
        ));
        assert!(progressed(
            &before,
            &at(100, 5_000_000_000, 106_101_024),
            window
        ));
        // npm on a dead socket in batch c127cce9: a few % of a CPU, no bytes.
        assert!(!progressed(
            &before,
            &at(100, 5_450_000_000, 106_100_000),
            window
        ));
        assert!(!progressed(
            &before,
            &at(100, 5_000_000_000, 106_100_200),
            window
        ));
        assert!(!progressed(&before, &before, window));
    }

    #[test]
    fn a_stall_is_the_time_since_the_last_progress_once_past_the_limit() {
        let s = |t: u64| Duration::from_secs(t);
        let idle = |log| {
            Some(Sample {
                log,
                cpu_ns: 0,
                net: 0,
            })
        };
        let mut w = Stall::new(s(60));
        assert_eq!(w.observe(s(0), idle(0)), None);
        assert_eq!(w.observe(s(30), idle(10)), None);
        assert_eq!(w.observe(s(60), idle(10)), None);
        assert_eq!(w.observe(s(89), idle(10)), None);
        assert_eq!(w.observe(s(90), idle(10)), Some(s(60)));
        // An unread sample counts as progress, and the next read one is the new baseline.
        let mut w = Stall::new(s(60));
        w.observe(s(0), idle(0));
        assert_eq!(w.observe(s(45), None), None);
        assert_eq!(w.observe(s(60), idle(0)), None);
        assert_eq!(w.observe(s(119), idle(0)), None);
        assert_eq!(w.observe(s(120), idle(0)), Some(s(60)));
    }

    #[test]
    fn podman_stats_reads_as_cpu_and_network_bytes() {
        // podman 4 and 5
        assert_eq!(
            parse_usage("{\"CPUNano\":97317000,\"NetInput\":5194,\"NetOutput\":3231}\n"),
            Ok((97_317_000, 8425))
        );
        // podman 6: per interface, and no NetInput
        assert_eq!(
            parse_usage(
                "{\"CPUNano\":2284000,\"Network\":{\"eth0\":{\"RxBytes\":254,\"TxBytes\":132},\
                 \"eth1\":{\"RxBytes\":10,\"TxBytes\":4}}}\n"
            ),
            Ok((2_284_000, 400))
        );
        assert!(parse_usage("").is_err());
        assert!(parse_usage("{\"CPUNano\":1}").is_err());
        assert!(parse_usage("{\"NetInput\":1,\"NetOutput\":2}").is_err());
        assert!(parse_usage("--").is_err());
    }

    #[test]
    fn a_job_missing_from_jobs_toml_or_over_its_admitted_memory_is_refused() {
        assert!(plan(TOML, &assignment("nope", None, None, 1024))
            .unwrap_err()
            .contains("no [jobs.nope]"));
        assert!(plan(TOML, &assignment("plain", None, None, 256))
            .unwrap_err()
            .contains("submit it with requirements.memory = 512"));
        assert!(plan("[jobs.a]\nrun = []\n", &assignment("a", None, None, 1024)).is_err());
    }

    #[test]
    fn a_job_a_job_runner_runs_on_its_host_itself_is_refused_not_run_in_a_container() {
        let e = plan(
            "[jobs.restart]\nrun = [\"x\"]\nhost = \"laptop\"\n",
            &assignment("restart", None, None, 1024),
        )
        .unwrap_err();
        assert!(e.contains("runs on host laptop itself"), "{e}");
    }

    #[test]
    fn the_base_image_is_the_arg_default_or_the_first_from() {
        assert_eq!(
            base_image("ARG BASE=localhost/base-python:1\nFROM ${BASE}\n").as_deref(),
            Some("localhost/base-python:1")
        );
        assert_eq!(
            base_image("# x\nFROM docker.io/library/debian:12 AS b\nFROM y\n").as_deref(),
            Some("docker.io/library/debian:12")
        );
        assert_eq!(base_image("RUN true\n"), None);
    }

    #[test]
    fn a_local_base_is_any_base_repo_at_a_numbered_version() {
        assert_eq!(local_base("localhost/base-python:1"), Some(("python", "1")));
        assert_eq!(local_base("localhost/base-node:12"), Some(("node", "12")));
        assert_eq!(local_base("localhost/base-rust:3"), Some(("rust", "3")));
        assert_eq!(
            local_base("localhost/base-python-cuda:2"),
            Some(("python-cuda", "2"))
        );
        for pulled in [
            "docker.io/library/node:24",
            "localhost/base-node:latest",
            "localhost/base-node:",
            "localhost/base-:1",
            "localhost/base-Node:1",
            "localhost/base-node/x:1",
            "localhost/other:1",
        ] {
            assert_eq!(local_base(pulled), None, "{pulled}");
        }
    }

    #[test]
    fn every_toolchains_lockfile_is_an_image_input() {
        for lock in ["uv.lock", "package-lock.json", "Cargo.lock"] {
            assert!(IMAGE_INPUTS.contains(&lock), "{lock}");
        }
    }

    #[test]
    fn the_image_tag_changes_with_the_base_and_each_input_and_only_with_them() {
        let inputs = |lock: &[u8]| {
            vec![
                ("Containerfile", b"FROM b".to_vec()),
                ("uv.lock", lock.to_vec()),
            ]
        };
        let a = image_tag("dakotacolorado/Python_Template", "base1", &inputs(b"l1"));
        assert!(a.starts_with("localhost/job-dakotacolorado-python-template:"));
        assert_eq!(
            a,
            image_tag("dakotacolorado/Python_Template", "base1", &inputs(b"l1"))
        );
        assert_ne!(
            a,
            image_tag("dakotacolorado/Python_Template", "base2", &inputs(b"l1"))
        );
        assert_ne!(
            a,
            image_tag("dakotacolorado/Python_Template", "base1", &inputs(b"l2"))
        );
    }

    #[test]
    fn a_workspace_is_removed_even_where_a_job_made_it_read_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("jobber-host-rm-{}", std::process::id()));
        let locked = dir.join("node_modules/pkg");
        std::fs::create_dir_all(&locked).unwrap();
        std::fs::write(locked.join("f"), "x").unwrap();
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o500)).unwrap();
        std::fs::set_permissions(
            dir.join("node_modules"),
            std::fs::Permissions::from_mode(0o500),
        )
        .unwrap();
        remove(&dir);
        assert!(!dir.exists());
    }

    #[test]
    fn the_sweep_removes_only_what_jobs_left() {
        let dir = std::env::temp_dir().join(format!("jobber-host-sweep-{}", std::process::id()));
        let job = "0b5f8a1e-2c3d-4e5f-8a9b-0c1d2e3f4a5b";
        for name in [job, &format!("{job}-base"), "keep", "0b5f8a1e"] {
            std::fs::create_dir_all(dir.join(name).join("sub")).unwrap();
        }
        sweep(&dir);
        let mut left: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(left, ["0b5f8a1e", "keep"]);
    }

    #[test]
    fn a_job_names_the_credentials_it_needs_and_gets_none_otherwise() {
        let toml = "[jobs.deploy]\nrun = [\"x\"]\ncredentials = [\"github\", \"aws\", \"aws\"]\n\
                    [jobs.test]\nrun = [\"y\"]\n";
        let p = plan(toml, &assignment("deploy", None, None, 1024)).unwrap();
        assert_eq!(p.credentials, [Kind::Aws, Kind::Github]);
        let p = plan(toml, &assignment("test", None, None, 1024)).unwrap();
        assert!(p.credentials.is_empty());
        let e = plan(
            "[jobs.a]\nrun = [\"x\"]\ncredentials = [\"gcp\"]\n",
            &assignment("a", None, None, 1024),
        )
        .unwrap_err();
        assert!(e.contains("gcp"), "{e}");
    }

    #[test]
    fn a_secret_reads_masked_in_the_log_even_split_across_writes() {
        let service = Arc::new(Recorder::default());
        let log = Log::start(service.clone(), "j".into());
        log.mask([
            "s3cr3t-value".to_string(),
            "abc".to_string(),
            "s3cr3t".to_string(),
        ]);
        log.write(b"key=s3cr");
        thread::sleep(Duration::from_millis(1200));
        log.write(b"3t-value and s3cr3t, abc\nend s3c");
        log.close(Duration::from_secs(5));
        assert_eq!(
            String::from_utf8_lossy(&service.0.lock().unwrap()),
            "key=*** and ***, abc\nend s3c"
        );
    }

    /// Records appends; a jobber that takes everything.
    #[derive(Default)]
    struct Recorder(Mutex<Vec<u8>>);

    impl Service for Recorder {
        fn append(&self, _: &str, offset: u64, bytes: &[u8]) -> Result<Appended, Failure> {
            let mut log = self.0.lock().unwrap();
            assert_eq!(offset, log.len() as u64);
            log.extend_from_slice(bytes);
            Ok(Appended::Stored {
                size: log.len() as u64,
                canceled: false,
            })
        }
        fn report(&self, _: &str, _: &Report) -> Result<(), Failure> {
            Ok(())
        }
    }

    #[test]
    fn a_checkout_of_a_commit_has_its_whole_history() {
        let root = std::env::temp_dir().join(format!("jobber-host-git-{}", std::process::id()));
        let (src, dir) = (root.join("src"), root.join("checkout"));
        std::fs::create_dir_all(&src).unwrap();
        let git = |dir: &Path, args: &[&str]| {
            let out = clean("git")
                .current_dir(dir)
                .args(["-c", "user.name=t", "-c", "user.email=t@t", "-c"])
                .arg("init.defaultBranch=main")
                .args(args)
                .output()
                .unwrap();
            assert!(out.status.success(), "git {args:?}: {out:?}");
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        git(&src, &["init", "-q"]);
        for n in 1..=3 {
            git(
                &src,
                &["commit", "-q", "--allow-empty", "-m", &n.to_string()],
            );
        }
        let third = git(&src, &["rev-parse", "HEAD"]);
        git(&src, &["commit", "-q", "--allow-empty", "-m", "4"]);
        let podman = Podman {
            service: Arc::new(Recorder::default()),
            work_dir: root.clone(),
            git_credential_helper: None,
            base_owner: String::new(),
            grace: Duration::from_secs(1),
            cgroup_parent: None,
            platform: None,
            memory_overhead: 0,
            credentials: Arc::new(credentials::Host),
            git_base: GITHUB.into(),
            building: Mutex::new(()),
            stopping: || false,
        };
        let log = Log::start(Arc::new(Recorder::default()), "j".into());
        let url = format!("file://{}", src.display());
        let result = podman.checkout_from(&log, &url, &third, &dir);
        log.close(Duration::from_secs(5));
        assert_eq!(result, Ok(()));
        let (count, head) = (
            git(&dir, &["rev-list", "--count", "HEAD"]),
            git(&dir, &["rev-parse", "HEAD"]),
        );
        remove(&root);
        assert_eq!((count.as_str(), head), ("3", third));
    }

    #[test]
    fn the_last_line_is_the_jobs_own_masked_and_whole() {
        let log = Log::start(Arc::new(Recorder::default()), "j".into());
        assert_eq!(log.last_line(), None);
        log.mask(["s3cr3t-value".to_string()]);
        log.say("running with 1024 MiB: sh -c npm-install");
        log.write(b"npm http fetch GET 200 x 5ms\nnpm-install: npm ci did not finish in 180s, on each of 3 tries: giving up\n\n");
        log.say("stopping it");
        assert_eq!(
            log.last_line().as_deref(),
            Some("npm-install: npm ci did not finish in 180s, on each of 3 tries: giving up")
        );
        log.write(b"progress 10%\rprogress 99%\rtoken s3cr3t-value refused  \n");
        assert_eq!(log.last_line().as_deref(), Some("token *** refused"));
        log.write(&[b'x'; 600]);
        let long = log.last_line().unwrap();
        assert!(long.starts_with("xxx") && long.ends_with('…'), "{long}");
        assert_eq!(long.chars().count(), LAST_LINE + 1);
        // A line longer than what is kept may start with the end of a secret: it is not used.
        log.write(&[b'y'; TAIL + 10]);
        assert_eq!(log.last_line(), None);
        log.write(b"\nend\n");
        assert_eq!(log.last_line().as_deref(), Some("end"));
        log.close(Duration::from_secs(5));
    }

    #[test]
    fn the_log_reaches_jobber_in_order_and_skips_what_it_already_has() {
        let service = Arc::new(Recorder::default());
        let log = Log::start(service.clone(), "j".into());
        log.say("one");
        log.write(b"two\n");
        log.close(Duration::from_secs(5));
        assert_eq!(
            String::from_utf8_lossy(&service.0.lock().unwrap()),
            "[jobber-host] one\ntwo\n"
        );
    }

    /// Needs rootless podman: `cargo test -- --ignored`.
    #[test]
    #[ignore]
    fn an_image_that_will_not_build_fails_the_job_with_the_build_output_in_its_log() {
        let dir = std::env::temp_dir().join(format!("jobber-host-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("Containerfile"),
            "FROM docker.io/library/busybox:latest\nRUN echo building-it && exit 3\n",
        )
        .unwrap();
        let service = Arc::new(Recorder::default());
        let podman = Podman {
            service: service.clone(),
            work_dir: dir.clone(),
            git_credential_helper: None,
            base_owner: "dakotacolorado".into(),
            grace: Duration::from_secs(1),
            cgroup_parent: None,
            platform: None,
            memory_overhead: 0,
            credentials: Arc::new(credentials::Host),
            git_base: GITHUB.into(),
            building: Mutex::new(()),
            stopping: || false,
        };
        let budget = budget(1.0, 2048, 512);
        let job = budget
            .reserve_job(crate::budget::Resources {
                cpu: Cpu::of(1.0),
                memory: 256,
            })
            .unwrap();
        let log = Log::start(service.clone(), "j".into());
        let outcome = podman
            .image(
                &log,
                &assignment("hello", None, None, 256),
                &dir,
                "o/r",
                &job,
                &budget,
            )
            .err()
            .unwrap();
        log.close(Duration::from_secs(5));
        let _ = std::fs::remove_dir_all(&dir);
        assert!(matches!(outcome.state, End::Failed));
        assert!(outcome.reason.unwrap().starts_with("image build failed"));
        let text = String::from_utf8_lossy(&service.0.lock().unwrap()).to_string();
        assert!(text.contains("building-it"), "{text}");
    }

    /// Needs rootless podman: `cargo test -- --ignored`.
    #[test]
    #[ignore]
    fn a_job_writes_in_its_workspace_as_the_hosts_user_and_it_is_removed_after() {
        let dir = std::env::temp_dir().join(format!("jobber-host-ws-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("src.txt"), "source\n").unwrap();
        let service = Arc::new(Recorder::default());
        let podman = Podman {
            service: service.clone(),
            work_dir: std::env::temp_dir(),
            git_credential_helper: None,
            base_owner: String::new(),
            grace: Duration::from_secs(1),
            cgroup_parent: None,
            platform: None,
            memory_overhead: 0,
            credentials: Arc::new(credentials::Host),
            git_base: GITHUB.into(),
            building: Mutex::new(()),
            stopping: || false,
        };
        assert!(Command::new("podman")
            .args(["pull", "-q", "docker.io/library/busybox:latest"])
            .status()
            .unwrap()
            .success());
        let log = Log::start(service.clone(), "j".into());
        let plan = Plan {
            run: vec![
                "sh".into(),
                "-c".into(),
                "mkdir -p node_modules/a target && cat src.txt > target/out && chmod 500 node_modules/a"
                    .into(),
            ],
            memory: 64,
            cpu: Cpu::of(0.5),
            timeout: 60,
            stall: None,
            credentials: vec![],
        };
        let name = format!("job-ws-{}", std::process::id());
        let outcome = podman.contain(
            &log,
            "docker.io/library/busybox:latest",
            &plan,
            &[],
            &dir,
            &name,
        );
        log.close(Duration::from_secs(5));
        let text = String::from_utf8_lossy(&service.0.lock().unwrap()).to_string();
        assert!(matches!(outcome.state, End::Succeeded), "{text}");
        assert_eq!(
            std::fs::read_to_string(dir.join("target/out")).unwrap(),
            "source\n"
        );
        remove(&dir);
        assert!(!dir.exists());
    }

    /// Needs rootless podman: `cargo test -- --ignored`.
    #[test]
    #[ignore]
    fn a_job_is_sent_only_the_credentials_given_it_and_reads_them_masked() {
        let podman = Podman {
            service: Arc::new(Recorder::default()),
            work_dir: std::env::temp_dir(),
            git_credential_helper: None,
            base_owner: String::new(),
            grace: Duration::from_secs(1),
            cgroup_parent: None,
            platform: None,
            memory_overhead: 0,
            credentials: Arc::new(credentials::Host),
            git_base: GITHUB.into(),
            building: Mutex::new(()),
            stopping: || false,
        };
        assert!(Command::new("podman")
            .args(["pull", "-q", "docker.io/library/busybox:latest"])
            .status()
            .unwrap()
            .success());
        let run = |env: &[Var]| {
            let service = Arc::new(Recorder::default());
            let log = Log::start(service.clone(), "j".into());
            log.mask(env.iter().map(|v| v.value.clone()));
            let plan = Plan {
                run: vec![
                    "sh".into(),
                    "-c".into(),
                    "echo token=${GH_TOKEN:-none}; env | grep -c ^AWS_ || true".into(),
                ],
                memory: 64,
                cpu: Cpu::of(0.5),
                timeout: 60,
                stall: None,
                credentials: vec![],
            };
            let name = format!("job-cred-{}-{}", std::process::id(), env.len());
            let outcome = podman.contain(
                &log,
                "docker.io/library/busybox:latest",
                &plan,
                env,
                &std::env::temp_dir(),
                &name,
            );
            log.close(Duration::from_secs(5));
            assert!(matches!(outcome.state, End::Succeeded));
            let text = String::from_utf8_lossy(&service.0.lock().unwrap()).to_string();
            text
        };
        let sent = run(&[Var {
            name: "GH_TOKEN",
            value: "ghp_not-a-real-token-1234".into(),
            secret: true,
        }]);
        assert!(sent.contains("token=***\n0\n"), "{sent}");
        assert!(!sent.contains("not-a-real"), "{sent}");
        let none = run(&[]);
        assert!(none.contains("token=none\n0\n"), "{none}");
    }

    fn busybox_podman() -> Podman<Recorder> {
        assert!(Command::new("podman")
            .args(["pull", "-q", "docker.io/library/busybox:latest"])
            .status()
            .unwrap()
            .success());
        Podman {
            service: Arc::new(Recorder::default()),
            work_dir: std::env::temp_dir(),
            git_credential_helper: None,
            base_owner: String::new(),
            grace: Duration::from_secs(1),
            cgroup_parent: None,
            platform: None,
            memory_overhead: 0,
            credentials: Arc::new(credentials::Host),
            git_base: GITHUB.into(),
            building: Mutex::new(()),
            stopping: || false,
        }
    }

    fn run_busybox(
        podman: &Podman<Recorder>,
        script: &str,
        stall: u64,
        name: &str,
    ) -> (Outcome, Duration) {
        let log = Log::start(podman.service.clone(), "j".into());
        let plan = Plan {
            run: vec!["sh".into(), "-c".into(), script.into()],
            memory: 64,
            cpu: Cpu::of(0.5),
            timeout: 600,
            stall: Some(stall),
            credentials: vec![],
        };
        let started = Instant::now();
        let outcome = podman.contain(
            &log,
            "docker.io/library/busybox:latest",
            &plan,
            &[],
            &std::env::temp_dir(),
            &format!("job-{name}-{}", std::process::id()),
        );
        log.close(Duration::from_secs(5));
        (outcome, started.elapsed())
    }

    /// Needs rootless podman: `cargo test -- --ignored`.
    #[test]
    #[ignore]
    fn a_job_with_no_output_cpu_or_network_is_ended_as_stalled_at_its_limit() {
        let podman = busybox_podman();
        let (outcome, took) = run_busybox(&podman, "echo started; sleep 300", 20, "stall");
        assert!(matches!(outcome.state, End::Failed));
        let reason = outcome.reason.unwrap();
        assert!(reason.starts_with("stalled: "), "{reason}");
        assert!(reason.contains("(its stall limit is 20s)"), "{reason}");
        assert!(took < Duration::from_secs(60), "took {took:?}");
    }

    /// Needs rootless podman: `cargo test -- --ignored`.
    #[test]
    #[ignore]
    // stdout and stderr are read apart, so lines on the two only keep their order a moment apart.
    fn a_failed_jobs_reason_carries_its_last_line() {
        let podman = busybox_podman();
        let (outcome, _) = run_busybox(
            &podman,
            "echo fetching; sleep 1; echo 'npm-install: npm ci did not finish in 5s, on each of 2 tries: giving up' >&2; exit 124",
            60,
            "last",
        );
        assert_eq!(outcome.exit, Some(124));
        assert_eq!(
            outcome.reason.as_deref(),
            Some("exited 124; its last line: npm-install: npm ci did not finish in 5s, on each of 2 tries: giving up")
        );
    }

    /// Needs rootless podman: `cargo test -- --ignored`.
    #[test]
    #[ignore]
    fn a_job_past_its_stall_limit_that_keeps_making_progress_is_not_ended() {
        let podman = busybox_podman();
        // Silent, but busy on a CPU, as a compile is.
        let (busy, _) = run_busybox(
            &podman,
            "end=$(($(date +%s)+40)); while [ $(date +%s) -lt $end ]; do :; done",
            15,
            "busy",
        );
        assert!(matches!(busy.state, End::Succeeded), "{:?}", busy.reason);
        // Quiet on the CPU, but writing, as `cdk deploy` waiting on CloudFormation does.
        let (talking, _) = run_busybox(
            &podman,
            "for i in $(seq 8); do echo waiting $i; sleep 5; done",
            15,
            "talk",
        );
        assert!(
            matches!(talking.state, End::Succeeded),
            "{:?}",
            talking.reason
        );
    }

    #[test]
    #[ignore]
    fn a_job_whose_container_monitor_is_killed_is_failed_as_out_of_memory() {
        // As the memory cgroup's OOM killer does on a podman machine, taking conmon rather than the job.
        let service = Arc::new(Recorder::default());
        let podman = Podman {
            service: service.clone(),
            work_dir: std::env::temp_dir(),
            git_credential_helper: None,
            base_owner: String::new(),
            grace: Duration::from_secs(1),
            cgroup_parent: None,
            platform: None,
            memory_overhead: 0,
            credentials: Arc::new(credentials::Host),
            git_base: GITHUB.into(),
            building: Mutex::new(()),
            stopping: || false,
        };
        assert!(Command::new("podman")
            .args(["pull", "-q", "docker.io/library/busybox:latest"])
            .status()
            .unwrap()
            .success());
        let log = Log::start(service.clone(), "j".into());
        let name = format!("job-test-conmon-{}", std::process::id());
        let killer = {
            let name = name.clone();
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(2500));
                let out = Command::new("podman")
                    .args(["inspect", "--format", "{{.State.ConmonPid}}", &name])
                    .output()
                    .unwrap();
                let pid = String::from_utf8_lossy(&out.stdout).trim().to_string();
                // Where podman is remote, as on a Mac, conmon runs in its machine, under that machine's PIDs.
                let remote = Command::new("podman")
                    .args(["info", "--format", "{{.Host.ServiceIsRemote}}"])
                    .output()
                    .unwrap();
                let killed = if String::from_utf8_lossy(&remote.stdout).trim() == "true" {
                    Command::new("podman")
                        .args(["machine", "ssh", &format!("kill -KILL {pid}")])
                        .status()
                } else {
                    Command::new("kill").args(["-KILL", &pid]).status()
                };
                assert!(killed.unwrap().success(), "conmon {pid}");
            })
        };
        let plan = Plan {
            run: vec!["sleep".into(), "30".into()],
            memory: 64,
            cpu: Cpu::of(0.5),
            timeout: 120,
            stall: None,
            credentials: vec![],
        };
        let started = Instant::now();
        let outcome = podman.contain(
            &log,
            "docker.io/library/busybox:latest",
            &plan,
            &[],
            &std::env::temp_dir(),
            &name,
        );
        log.close(Duration::from_secs(5));
        killer.join().unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(25),
            "it waited for the job to end"
        );
        assert!(matches!(outcome.state, End::Failed), "{outcome:?}");
        assert_eq!(outcome.exit, Some(137));
        assert!(!Command::new("podman")
            .args(["container", "exists", &name])
            .status()
            .unwrap()
            .success());
    }

    /// Needs rootless podman and a user slice capped below the job's own
    /// limit: `JOBBER_HOST_TEST_SLICE=jobbertest.slice cargo test -- --ignored`
    /// (the test skill makes it). Skipped without the variable.
    #[test]
    #[ignore]
    fn a_job_past_its_slices_memory_is_killed_and_failed_in_that_slice() {
        let Ok(slice) = std::env::var("JOBBER_HOST_TEST_SLICE") else {
            return;
        };
        let service = Arc::new(Recorder::default());
        let podman = Podman {
            service: service.clone(),
            work_dir: std::env::temp_dir(),
            git_credential_helper: None,
            base_owner: String::new(),
            grace: Duration::from_secs(1),
            cgroup_parent: Some(slice.clone()),
            platform: None,
            memory_overhead: 0,
            credentials: Arc::new(credentials::Host),
            git_base: GITHUB.into(),
            building: Mutex::new(()),
            stopping: || false,
        };
        assert!(Command::new("podman")
            .args(["pull", "-q", "docker.io/library/busybox:latest"])
            .status()
            .unwrap()
            .success());
        let log = Log::start(service.clone(), "j".into());
        let name = format!("job-test-{}", std::process::id());
        let probe = {
            let name = name.clone();
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(1500));
                let out = Command::new("podman")
                    .args(["inspect", "--format", "{{.State.CgroupPath}}", &name])
                    .output()
                    .unwrap();
                String::from_utf8_lossy(&out.stdout).trim().to_string()
            })
        };
        // `tail` keeps a line with no end in memory: it grows until it is killed.
        let plan = Plan {
            run: vec!["sh".into(), "-c".into(), "sleep 2; tail /dev/zero".into()],
            memory: 4096,
            cpu: Cpu::of(0.5),
            timeout: 120,
            stall: None,
            credentials: vec![],
        };
        let outcome = podman.contain(
            &log,
            "docker.io/library/busybox:latest",
            &plan,
            &[],
            &std::env::temp_dir(),
            &name,
        );
        log.close(Duration::from_secs(5));
        let cgroup = probe.join().unwrap();
        assert!(cgroup.contains(&format!("/{slice}/")), "cgroup {cgroup}");
        assert!(matches!(outcome.state, End::Failed));
        assert_eq!(outcome.exit, Some(137));
    }

    /// A run of a busybox `sleep 300` against a fake jobber whose
    /// heartbeat answers name it in `key` once it is up: what the service was
    /// sent, the run's log there, and how long it took.
    fn hosts_run_named_in_a_heartbeat(
        key: &'static str,
        n: u8,
    ) -> (Vec<(String, serde_json::Value)>, String, Duration) {
        use crate::service::{fake_http, Heartbeat, Hosts};
        use std::sync::atomic::{AtomicBool, Ordering};
        let root = std::env::temp_dir().join(format!("jh-hosts-{key}-{}", std::process::id()));
        let app = root.join("src/o/app.git");
        std::fs::create_dir_all(&app).unwrap();
        std::fs::write(
            app.join("Containerfile"),
            "FROM docker.io/library/busybox:latest\n",
        )
        .unwrap();
        std::fs::write(
            app.join("jobs.toml"),
            "[jobs.wait]\nrun = [\"sh\", \"-c\", \"echo up; sleep 300\"]\n",
        )
        .unwrap();
        let git = |args: &[&str]| {
            let out = clean("git")
                .current_dir(&app)
                .args(["-c", "user.name=t", "-c", "user.email=t@t"])
                .args(args)
                .output()
                .unwrap();
            assert!(out.status.success(), "git {args:?}: {out:?}");
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        git(&["init", "-q"]);
        git(&["add", "-A"]);
        git(&["commit", "-q", "-m", "1"]);
        let commit = git(&["rev-parse", "HEAD"]);
        let id = format!(
            "{:08x}-0000-4000-8000-0000000000{n:02x}",
            std::process::id()
        );

        let text = Arc::new(Mutex::new(Vec::<u8>::new()));
        let (url, sent) = {
            let (text, id) = (text.clone(), id.clone());
            fake_http::route(move |s| {
                let r = s.request().to_string();
                if r == "POST /hosts/h1/heartbeat" {
                    let up = String::from_utf8_lossy(&text.lock().unwrap()).contains("\nup\n");
                    let listed = s.json()["running"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|v| v == &*id);
                    let other = if key == "cancel" { "ended" } else { "cancel" };
                    let named = if up && listed {
                        format!("[\"{id}\"]")
                    } else {
                        "[]".into()
                    };
                    (
                        "200 OK".into(),
                        format!("{{\"{other}\":[],\"{key}\":{named},\"lease\":60}}"),
                    )
                } else if r == format!("PATCH /runs/{id}/log?attempt=1") {
                    let mut t = text.lock().unwrap();
                    t.extend_from_slice(&s.body);
                    ("200 OK".into(), format!("{{\"size\":{}}}", t.len()))
                } else {
                    ("200 OK".into(), "{}".into())
                }
            })
        };
        let hosts = Arc::new(Hosts::new(&url, "h1", "h1.t"));
        let podman = Podman {
            service: hosts.clone(),
            work_dir: root.join("work"),
            git_credential_helper: None,
            base_owner: String::new(),
            grace: Duration::from_secs(1),
            cgroup_parent: None,
            platform: None,
            memory_overhead: 0,
            credentials: Arc::new(credentials::Host),
            git_base: format!("file://{}/src/", root.display()),
            building: Mutex::new(()),
            stopping: || false,
        };
        let mut c = assignment("wait", None, Some(600), 256);
        c.run.id = id.clone();
        c.run.repo = "o/app".into();
        c.run.commit = commit;
        c.run.args = vec![];
        let budget = budget(1.0, 1024, 256);
        let reservation = budget
            .reserve_job(crate::budget::Resources {
                cpu: Cpu::of(1.0),
                memory: 256,
            })
            .unwrap();
        hosts.begin(&id, 1);
        let done = AtomicBool::new(false);
        let started = Instant::now();
        thread::scope(|s| {
            s.spawn(|| {
                while !done.load(Ordering::SeqCst) {
                    let _ = hosts.heartbeat(&Heartbeat {
                        free: budget.free(),
                        metrics: crate::heartbeat::metrics(&root),
                        running: std::slice::from_ref(&id),
                    });
                    thread::sleep(Duration::from_secs(1));
                }
            });
            podman.run(c, reservation, &budget);
            done.store(true, Ordering::SeqCst);
        });
        hosts.finish(&id);
        remove(&root);
        let log = String::from_utf8_lossy(&text.lock().unwrap()).to_string();
        let sent = sent
            .lock()
            .unwrap()
            .iter()
            .filter(|s| !s.request().ends_with("/heartbeat") && !s.request().contains("/log"))
            .map(|s| {
                (
                    s.request().to_string(),
                    if s.body.is_empty() {
                        serde_json::Value::Null
                    } else {
                        s.json()
                    },
                )
            })
            .collect();
        (sent, log, started.elapsed())
    }

    /// Needs rootless podman and the network (busybox): `cargo test -- --ignored`.
    #[test]
    #[ignore]
    fn a_run_canceled_in_a_heartbeats_answer_is_stopped_and_completed_canceled() {
        let (sent, log, took) = hosts_run_named_in_a_heartbeat("cancel", 0x0a);
        assert_eq!(sent.len(), 1, "only its completion: {sent:?}");
        assert!(sent[0].0.starts_with("PUT /runs/"), "{sent:?}");
        assert_eq!(sent[0].1["state"], "canceled", "{log}");
        assert!(log.contains("stopping it: canceled"), "{log}");
        assert!(took < Duration::from_secs(240), "{log}");
    }

    /// Needs rootless podman and the network (busybox): `cargo test -- --ignored`.
    #[test]
    #[ignore]
    fn a_run_a_heartbeat_says_has_ended_is_stopped_and_not_completed() {
        let (sent, log, took) = hosts_run_named_in_a_heartbeat("ended", 0x0b);
        assert_eq!(sent, [], "nothing beyond heartbeats and log: {log}");
        assert!(log.contains("\nup\n"), "{log}");
        assert!(
            took < Duration::from_secs(240),
            "stopped, not slept out: {log}"
        );
    }
}
