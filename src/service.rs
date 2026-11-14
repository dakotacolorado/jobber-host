//! jobber's host half, as its `openapi.json` and `docs/PROTOCOL.md` give it:
//! a host registers (`POST /hosts`) with what it offers, holds one connection
//! over which it is sent its runs (`src/connection.rs`), and a heartbeat renews
//! every run it lists and answers which to cancel. Logs and completions are
//! HTTP, each naming the attempt the run was started on. jobber's shapes are
//! the authority; jobber-host follows them.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use crate::budget::{Cpu, Resources};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum End {
    Succeeded,
    Failed,
    Canceled,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Report {
    pub state: End,
    pub exit: Option<i32>,
    pub duration: f64,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Appended {
    /// Stored (or, empty, nothing sent): the log is now `size` bytes.
    Stored { size: u64, canceled: bool },
    /// Not at the log's end: resend from `size`.
    Resend { size: u64 },
    /// Over an append's or the run's limit: send no more log.
    TooLarge(String),
    /// The run is not running here any more, e.g. its lease lapsed: stop it.
    Ended(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    /// Refused for what was sent or who sent it; the same request will be
    /// refused again.
    Refused(u16, String),
    /// Might be different on the next try: no network, a service mid-deploy.
    Transient(String),
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Failure::Refused(code, body) => write!(f, "refused {code}: {body}"),
            Failure::Transient(why) => write!(f, "{why}"),
        }
    }
}

/// What a run, once started, asks of jobber. A trait so each run can be
/// tested without a live service.
pub trait Service: Send + Sync + 'static {
    /// `bytes` from `offset`; empty, only says whether jobber said to cancel.
    fn append(&self, run: &str, offset: u64, bytes: &[u8]) -> Result<Appended, Failure>;
    fn report(&self, run: &str, report: &Report) -> Result<(), Failure>;
    /// How often a silent run's log asks `append` with no bytes, to hear of a cancel.
    fn renew_every(&self) -> Duration {
        Duration::from_secs(1)
    }
}

fn agent() -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(30))
        .build()
}

fn failure(e: ureq::Error) -> Failure {
    match e {
        ureq::Error::Status(code, response) => {
            let body = response.into_string().unwrap_or_default();
            // 408, 429 and 5xx are the service's to recover from.
            if code == 408 || code == 429 || code >= 500 {
                Failure::Transient(format!("{code} {body}"))
            } else {
                Failure::Refused(code, body)
            }
        }
        e => Failure::Transient(e.to_string()),
    }
}

fn json<T: serde::de::DeserializeOwned>(response: ureq::Response) -> Result<T, Failure> {
    response
        .into_json()
        .map_err(|e| Failure::Transient(format!("unreadable reply: {e}")))
}

#[derive(Debug, Deserialize)]
pub struct RegisteredId {
    pub id: String,
}

#[derive(Debug, Serialize)]
pub struct RegisterHost<'a> {
    pub name: &'a str,
    pub os: &'a str,
    pub arch: &'a str,
    /// What it offers: measured, less what it keeps back.
    pub capacity: Resources,
    pub labels: &'a BTreeMap<String, String>,
    pub version: &'a str,
    /// What it can do beyond running a run in its repo's container: jobber
    /// sends a run needing one only to a host that names it.
    pub features: &'a [&'a str],
}

/// What every jobber-host of this version can do, as it registers: give a run
/// the secrets jobber sends with its start, and run a run on the host itself.
pub const FEATURES: &[&str] = &["secrets", "hostRuns"];

#[derive(Debug, Deserialize)]
pub struct RegisteredHost {
    pub host: RegisteredId,
    pub token: String,
}

/// `POST /hosts` with the registration token.
pub fn register_host(
    service: &str,
    token: &str,
    body: &RegisterHost,
) -> Result<RegisteredHost, Failure> {
    let response = agent()
        .post(&format!("{service}/hosts"))
        .set("Authorization", &format!("Bearer {token}"))
        .send_json(body)
        .map_err(failure)?;
    json(response)
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Metrics {
    pub cpu_load: f64,
    pub memory_used: u64,
    pub memory_total: u64,
    pub disk_free: u64,
    /// CPUs the machine has, which `cpu_load` is measured against; 0 if unknown.
    pub cpus: u64,
}

/// `SendHeartbeat`'s body, the HTTP heartbeat sent while there is no connection.
#[derive(Debug, Serialize)]
pub struct Heartbeat<'a> {
    pub free: Resources,
    pub metrics: Metrics,
    pub running: &'a [String],
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Beat {
    pub cancel: Vec<String>,
    /// Of the runs listed, those no longer running here; none from a service before it.
    #[serde(default)]
    pub ended: Vec<String>,
    pub lease: u64,
}

/// What a run asks for. Its CPUs and memory as jobber admitted them come
/// with its `assign`, its defaults filled in.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Requirements {
    #[serde(default)]
    pub cpu: Option<f64>,
    pub memory: Option<u64>,
    /// Whole GPUs; absent from a jobber before GPUs.
    #[serde(default)]
    pub gpu: Option<u32>,
    pub timeout: Option<u64>,
}

/// A run as jobber sends it; fields jobber-host does not use are left out.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct HostRun {
    pub id: String,
    pub repo: String,
    pub commit: String,
    pub job: String,
    pub args: Vec<String>,
    pub requirements: Requirements,
    pub labels: BTreeMap<String, String>,
    /// Runs on this machine itself, as its user, outside a container: only
    /// for a repo on the config's `host_runs`.
    #[serde(default, rename = "onHost")]
    pub on_host: bool,
}

/// A run's secrets as jobber sends them with its `start`, by name: kept only
/// in memory and given to the run as environment variables. Never printed:
/// its `Debug` shows names alone.
#[derive(Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct Secrets(pub BTreeMap<String, String>);

impl std::fmt::Debug for Secrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.0.keys()).finish()
    }
}

/// A run accepted and started here: what the executor runs.
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub run: HostRun,
    /// CPUs it was admitted with: reserved, and its container's `--cpus`.
    pub cpu: Cpu,
    /// MiB it was admitted with: reserved, and its ceiling.
    pub memory: u64,
    /// The GPUs it holds, as podman CDI device names: its container's `--device`s.
    pub gpus: Vec<String>,
    /// Seconds it lives without a heartbeat listing it.
    pub lease: u64,
    pub secrets: Secrets,
}

/// A host's own calls, under its own token `<id>.<secret>`, and what it has
/// heard of the runs it has started.
pub struct Hosts {
    agent: ureq::Agent,
    service: String,
    id: String,
    authorization: String,
    /// Each run started here and not yet ended here, by the attempt it was
    /// started on: what a heartbeat renews and every write names.
    attempts: std::sync::Mutex<BTreeMap<String, u64>>,
    /// Runs jobber said to cancel; kept until each ends here, so a
    /// cancel is heard however soon a later answer leaves it out.
    cancel: std::sync::Mutex<std::collections::BTreeSet<String>>,
    /// Runs jobber has already ended, until each is reported: kept,
    /// since a stopped run is no longer listed.
    ended: std::sync::Mutex<std::collections::BTreeSet<String>>,
}

fn locked<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Hosts {
    pub fn new(service: &str, id: &str, token: &str) -> Hosts {
        Hosts {
            agent: agent(),
            service: service.to_string(),
            id: id.to_string(),
            authorization: format!("Bearer {token}"),
            attempts: Default::default(),
            cancel: Default::default(),
            ended: Default::default(),
        }
    }

    /// `GET /hosts/{id}/connect`, as a WebSocket URL.
    pub fn connect_url(&self) -> String {
        let service = match self.service.split_once("://") {
            Some(("https", rest)) => format!("wss://{rest}"),
            Some(("http", rest)) => format!("ws://{rest}"),
            _ => self.service.clone(),
        };
        format!("{service}/hosts/{}/connect", self.id)
    }

    pub fn authorization(&self) -> &str {
        &self.authorization
    }

    /// Notes `run` as started here on `attempt`.
    pub fn begin(&self, run: &str, attempt: u64) {
        locked(&self.attempts).insert(run.to_string(), attempt);
    }

    /// Forgets `run`, once its thread has ended.
    pub fn finish(&self, run: &str) {
        locked(&self.attempts).remove(run);
        locked(&self.cancel).remove(run);
        locked(&self.ended).remove(run);
    }

    /// The attempt `run` was started on here, if it runs here.
    pub fn attempt(&self, run: &str) -> Option<u64> {
        locked(&self.attempts).get(run).copied()
    }

    /// Every run started here and not yet ended here, with its attempt.
    pub fn running(&self) -> Vec<(String, u64)> {
        locked(&self.attempts)
            .iter()
            .map(|(r, a)| (r.clone(), *a))
            .collect()
    }

    /// Stops `run`, and completes it `canceled`.
    pub fn cancel(&self, run: &str) {
        if self.attempt(run).is_some() && locked(&self.cancel).insert(run.to_string()) {
            crate::log!("run {run}: canceled on jobber: stopping it");
        }
    }

    /// Stops `run` without completing it: jobber has its end already.
    pub fn end(&self, run: &str) {
        if self.attempt(run).is_some() && locked(&self.ended).insert(run.to_string()) {
            crate::log!("run {run} ended on jobber: stopping it");
        }
    }

    /// Takes in a heartbeat's answer, however it came; of runs not running
    /// here any more it takes nothing.
    pub fn heard(&self, beat: &Beat) {
        for run in &beat.cancel {
            self.cancel(run);
        }
        for run in &beat.ended {
            self.end(run);
        }
    }

    /// `POST /hosts/{id}/heartbeat`: renews each of `running` and takes in the answer.
    pub fn heartbeat(&self, beat: &Heartbeat) -> Result<Beat, Failure> {
        let response = self
            .agent
            .post(&format!("{}/hosts/{}/heartbeat", self.service, self.id))
            .set("Authorization", &self.authorization)
            .send_json(beat)
            .map_err(failure)?;
        let answer: Beat = json(response)?;
        self.heard(&answer);
        Ok(answer)
    }

    fn ended(&self, run: &str) -> bool {
        locked(&self.ended).contains(run)
    }

    fn canceled(&self, run: &str) -> bool {
        locked(&self.cancel).contains(run)
    }

    /// `path` under the service, naming the attempt `run` was started on.
    fn url(&self, path: &str, run: &str) -> String {
        match self.attempt(run) {
            Some(attempt) => format!("{}{path}?attempt={attempt}", self.service),
            None => format!("{}{path}", self.service),
        }
    }
}

impl Service for Hosts {
    /// Empty, it calls nothing: the heartbeat renews, so it only says whether
    /// jobber said to cancel. A run it said has ended is stopped as
    /// one whose append is refused.
    fn append(&self, run: &str, offset: u64, bytes: &[u8]) -> Result<Appended, Failure> {
        if self.ended(run) {
            return Ok(Appended::Ended("ended on jobber".into()));
        }
        if bytes.is_empty() {
            return Ok(Appended::Stored {
                size: offset,
                canceled: self.canceled(run),
            });
        }
        let end = offset + bytes.len() as u64 - 1;
        #[derive(Deserialize)]
        struct Size {
            size: u64,
        }
        let sent = self
            .agent
            .request("PATCH", &self.url(&format!("/runs/{run}/log"), run))
            .set("Authorization", &self.authorization)
            .set("Content-Type", "application/octet-stream")
            .set("Content-Range", &format!("bytes {offset}-{end}/*"))
            .send_bytes(bytes);
        match sent {
            Ok(response) => {
                let s: Size = json(response)?;
                Ok(Appended::Stored {
                    size: s.size,
                    canceled: self.canceled(run),
                })
            }
            Err(ureq::Error::Status(416, response)) => {
                let s: Size = json(response)?;
                Ok(Appended::Resend { size: s.size })
            }
            Err(ureq::Error::Status(413, response)) => Ok(Appended::TooLarge(
                response.into_string().unwrap_or_default(),
            )),
            // Ended, or no longer on the attempt it was started on.
            Err(ureq::Error::Status(409, response)) => {
                Ok(Appended::Ended(response.into_string().unwrap_or_default()))
            }
            Err(e) => Err(failure(e)),
        }
    }

    /// Not for a run jobber has already ended: it has its end.
    fn report(&self, run: &str, report: &Report) -> Result<(), Failure> {
        if locked(&self.ended).remove(run) {
            return Ok(());
        }
        self.agent
            .put(&self.url(&format!("/runs/{run}"), run))
            .set("Authorization", &self.authorization)
            .send_json(report)
            .map_err(failure)
            .map(drop)
    }
}

#[cfg(test)]
pub mod fake_http {
    //! A server that answers each request it is sent with the next of a list
    //! of replies, and hands back what it was sent.
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    pub struct Sent {
        pub head: String,
        pub body: Vec<u8>,
    }

    impl Sent {
        pub fn json(&self) -> serde_json::Value {
            serde_json::from_slice(&self.body).unwrap()
        }

        /// Its method and path, e.g. `POST /hosts`.
        pub fn request(&self) -> &str {
            let line = self.head.lines().next().unwrap_or("");
            line.rsplit_once(' ').map_or(line, |(r, _)| r)
        }
    }

    pub fn serve(
        replies: Vec<(&'static str, &'static str)>,
    ) -> (String, thread::JoinHandle<Vec<Sent>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            let mut sent = vec![];
            for (status, body) in replies {
                let (stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream);
                let mut head = String::new();
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = v.trim().parse().unwrap();
                    }
                    if line == "\r\n" {
                        break;
                    }
                    head.push_str(&line);
                }
                let mut bytes = vec![0; length];
                reader.read_exact(&mut bytes).unwrap();
                let reply = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                reader.get_mut().write_all(reply.as_bytes()).unwrap();
                sent.push(Sent { head, body: bytes });
            }
            sent
        });
        (url, handle)
    }

    /// Everything a `route` server has been sent, in order.
    pub type Log = std::sync::Arc<std::sync::Mutex<Vec<Sent>>>;

    /// A server that answers every request it is sent, for as long as the
    /// test runs, with what `answer` makes of it: a status line and a body.
    pub fn route(answer: impl Fn(&Sent) -> (String, String) + Send + 'static) -> (String, Log) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let log: Log = Default::default();
        let kept = log.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                answer_one(stream, &answer, &kept);
            }
        });
        (url, log)
    }

    /// Reads one request from `stream`, keeps it in `log` and answers it as `answer` says.
    pub fn answer_one(stream: TcpStream, answer: &dyn Fn(&Sent) -> (String, String), log: &Log) {
        let mut reader = BufReader::new(stream);
        let mut head = String::new();
        let mut length = 0;
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).unwrap_or(0) == 0 {
                break;
            }
            if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                length = v.trim().parse().unwrap();
            }
            if line == "\r\n" {
                break;
            }
            head.push_str(&line);
        }
        let mut bytes = vec![0; length];
        if reader.read_exact(&mut bytes).is_err() {
            return;
        }
        let sent = Sent { head, body: bytes };
        let (status, body) = answer(&sent);
        log.lock().unwrap().push(sent);
        let reply = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = reader.get_mut().write_all(reply.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::fake_http::serve;
    use super::*;
    use serde_json::json;

    const RUN: &str = r#"{"run":{"id":"j1","repo":"o/r","commit":"862d32fbe2a988fb533215c156cf058191ec54b9","job":"hello","args":["you"],"requirements":{"os":"linux","arch":null,"cpu":0.25,"memory":512,"timeout":null},"labels":{"item":"616376ae"},"state":"running","reason":null,"cancelRequested":false,"host":"h1","exit":null,"duration":null,"logSize":0,"createdAt":"","updatedAt":"","startedAt":"","endedAt":null,"leaseExpiresAt":""},"cpu":0.25,"memory":512,"lease":60}"#;

    #[test]
    fn a_host_registers_with_what_it_is_and_has() {
        let (url, server) = serve(vec![(
            "201 Created",
            r#"{"host":{"id":"h1","name":"box","os":"linux","arch":"x86_64","capacity":{"cpu":13.5,"memory":9216,"gpu":0},"labels":{},"version":"v","free":null,"metrics":null,"createdAt":"","lastSeenAt":""},"token":"h1.secret"}"#,
        )]);
        let labels = BTreeMap::from([("gpu".to_string(), "none".to_string())]);
        let registered = register_host(
            &url,
            "reg",
            &RegisterHost {
                name: "box",
                os: "linux",
                arch: "x86_64",
                capacity: Resources {
                    cpu: Cpu::of(13.5),
                    memory: 9216,
                    gpu: 0,
                },
                labels: &labels,
                version: "0.1.0",
                features: FEATURES,
            },
        )
        .unwrap();
        assert_eq!(
            (registered.host.id.as_str(), registered.token.as_str()),
            ("h1", "h1.secret")
        );
        let sent = server.join().unwrap();
        assert_eq!(sent[0].request(), "POST /hosts");
        assert!(sent[0].head.contains("Authorization: Bearer reg"));
        assert_eq!(
            sent[0].json(),
            json!({"name":"box","os":"linux","arch":"x86_64","capacity":{"cpu":13.5,"memory":9216,"gpu":0},"labels":{"gpu":"none"},"version":"0.1.0","features":["secrets","hostRuns"]})
        );
    }

    #[test]
    fn a_pushed_run_reads_its_requirements_and_ignores_what_it_does_not_use() {
        let pushed: serde_json::Value = serde_json::from_str(RUN).unwrap();
        let mut run = pushed["run"].clone();
        run["attempt"] = json!(2);
        let run: HostRun = serde_json::from_value(run).unwrap();
        assert_eq!(run.id, "j1");
        assert_eq!(run.args, ["you"]);
        assert_eq!(
            run.requirements,
            Requirements {
                cpu: Some(0.25),
                memory: Some(512),
                gpu: None,
                timeout: None
            }
        );
        assert_eq!(run.labels["item"], "616376ae");
    }

    #[test]
    fn a_hosts_connection_is_its_service_as_a_websocket() {
        let at = |service: &str| Hosts::new(service, "h1", "t").connect_url();
        assert_eq!(
            at("https://js.example/prod"),
            "wss://js.example/prod/hosts/h1/connect"
        );
        assert_eq!(
            at("http://127.0.0.1:8080"),
            "ws://127.0.0.1:8080/hosts/h1/connect"
        );
    }

    #[test]
    fn a_hosts_log_and_completion_name_the_attempt_it_was_started_on() {
        let (url, server) = serve(vec![
            ("200 OK", r#"{"size":2}"#),
            (
                "409 Conflict",
                r#"{"code":"ConflictException","message":"attempt 3 is current"}"#,
            ),
            ("200 OK", "{}"),
        ]);
        let hosts = Hosts::new(&url, "h1", "t");
        hosts.begin("j1", 2);
        hosts.append("j1", 0, b"hi").unwrap();
        assert!(
            matches!(hosts.append("j1", 2, b"x").unwrap(), Appended::Ended(e) if e.contains("attempt 3")),
            "a write to an attempt no longer current ends the run here"
        );
        hosts
            .report(
                "j1",
                &Report {
                    state: End::Succeeded,
                    exit: Some(0),
                    duration: 1.0,
                    reason: None,
                },
            )
            .unwrap();
        let sent = server.join().unwrap();
        let requests: Vec<&str> = sent.iter().map(|s| s.request()).collect();
        assert_eq!(
            requests,
            [
                "PATCH /runs/j1/log?attempt=2",
                "PATCH /runs/j1/log?attempt=2",
                "PUT /runs/j1?attempt=2"
            ]
        );
    }

    #[test]
    fn a_hosts_append_always_carries_its_range_and_an_empty_one_calls_nothing() {
        let (url, server) = serve(vec![
            ("200 OK", r#"{"size":5}"#),
            (
                "416 Range Not Satisfiable",
                r#"{"code":"InvalidRangeException","message":"e","size":3}"#,
            ),
            (
                "413 Payload Too Large",
                r#"{"code":"PayloadTooLargeException","message":"too big"}"#,
            ),
            (
                "409 Conflict",
                r#"{"code":"ConflictException","message":"lease lapsed"}"#,
            ),
        ]);
        let hosts = Hosts::new(&url, "h1", "t");
        assert_eq!(
            hosts.append("j1", 0, b"hello").unwrap(),
            Appended::Stored {
                size: 5,
                canceled: false
            }
        );
        assert_eq!(
            hosts.append("j1", 5, b"").unwrap(),
            Appended::Stored {
                size: 5,
                canceled: false
            },
            "answered here: the heartbeat renews"
        );
        assert_eq!(
            hosts.append("j1", 7, b"xy").unwrap(),
            Appended::Resend { size: 3 }
        );
        assert!(
            matches!(hosts.append("j1", 3, b"z").unwrap(), Appended::TooLarge(e) if e.contains("too big"))
        );
        assert!(
            matches!(hosts.append("j1", 3, b"z").unwrap(), Appended::Ended(e) if e.contains("lapsed"))
        );
        let sent = server.join().unwrap();
        assert_eq!(sent.len(), 4);
        assert_eq!(sent[0].request(), "PATCH /runs/j1/log");
        assert!(sent[0].head.contains("Content-Range: bytes 0-4/*"));
        assert!(sent[0]
            .head
            .contains("Content-Type: application/octet-stream"));
        assert_eq!(sent[0].body, b"hello");
        assert!(sent[1].head.contains("Content-Range: bytes 7-8/*"));
    }

    #[test]
    fn a_heartbeat_sends_free_metrics_and_runs_and_its_cancels_reach_their_appends() {
        let (url, server) = serve(vec![
            ("200 OK", r#"{"cancel":["j1"],"lease":60}"#),
            ("200 OK", r#"{"size":3}"#),
            ("200 OK", r#"{"cancel":[],"lease":60}"#),
        ]);
        let hosts = Hosts::new(&url, "h1", "h1.secret");
        hosts.begin("j1", 1);
        hosts.begin("j2", 1);
        let running = ["j1".to_string(), "j2".to_string()];
        let beat = Heartbeat {
            free: Resources {
                cpu: Cpu::of(0.0),
                memory: 512,
                gpu: 0,
            },
            metrics: Metrics {
                cpu_load: 1.5,
                memory_used: 1000,
                memory_total: 4000,
                disk_free: 9000,
                cpus: 4,
            },
            running: &running,
        };
        assert_eq!(
            hosts.heartbeat(&beat).unwrap(),
            Beat {
                cancel: vec!["j1".into()],
                ended: vec![],
                lease: 60
            }
        );
        assert_eq!(
            hosts.append("j1", 0, b"abc").unwrap(),
            Appended::Stored {
                size: 3,
                canceled: true
            }
        );
        assert_eq!(
            hosts.append("j2", 0, b"").unwrap(),
            Appended::Stored {
                size: 0,
                canceled: false
            }
        );
        hosts.heartbeat(&beat).unwrap();
        assert_eq!(
            hosts.append("j1", 3, b"").unwrap(),
            Appended::Stored {
                size: 3,
                canceled: true
            },
            "a cancel holds though the next answer leaves it out"
        );
        hosts.finish("j1");
        hosts.begin("j1", 2);
        assert_eq!(
            hosts.append("j1", 3, b"").unwrap(),
            Appended::Stored {
                size: 3,
                canceled: false
            },
            "until the run ends here"
        );
        let sent = server.join().unwrap();
        assert_eq!(sent[0].request(), "POST /hosts/h1/heartbeat");
        assert!(sent[0].head.contains("Authorization: Bearer h1.secret"));
        assert_eq!(
            sent[0].json(),
            json!({"free":{"cpu":0.0,"memory":512,"gpu":0},"metrics":{"cpuLoad":1.5,"memoryUsed":1000,"memoryTotal":4000,"diskFree":9000,"cpus":4},"running":["j1","j2"]})
        );
    }

    #[test]
    fn a_host_completes_a_run_under_runs_and_a_refusal_is_one() {
        let (url, server) = serve(vec![
            ("200 OK", "{}"),
            (
                "409 Conflict",
                r#"{"code":"ConflictException","message":"ended"}"#,
            ),
            (
                "401 Unauthorized",
                r#"{"code":"UnauthorizedException","message":"no"}"#,
            ),
        ]);
        let hosts = Hosts::new(&url, "h1", "t");
        let report = Report {
            state: End::Canceled,
            exit: None,
            duration: 2.5,
            reason: None,
        };
        hosts.report("j1", &report).unwrap();
        assert!(matches!(
            hosts.report("j1", &report),
            Err(Failure::Refused(409, _))
        ));
        assert!(matches!(
            hosts.heartbeat(&Heartbeat {
                free: Resources {
                    cpu: Cpu::of(1.0),
                    memory: 16,
                    gpu: 0,
                },
                metrics: Metrics {
                    cpu_load: 0.0,
                    memory_used: 0,
                    memory_total: 0,
                    disk_free: 0,
                    cpus: 4,
                },
                running: &[],
            }),
            Err(Failure::Refused(401, _))
        ));
        let sent = server.join().unwrap();
        assert_eq!(sent[0].request(), "PUT /runs/j1");
        assert_eq!(
            sent[0].json(),
            json!({"state":"canceled","exit":null,"duration":2.5,"reason":null})
        );
    }

    #[test]
    fn a_run_a_heartbeat_says_has_ended_is_stopped_and_never_completed() {
        let (url, sent) = fake_http::route(|s| match s.request() {
            "POST /hosts/h1/heartbeat" => (
                "200 OK".into(),
                r#"{"cancel":[],"ended":["j1"],"lease":60}"#.into(),
            ),
            _ => ("200 OK".into(), r#"{"size":1}"#.into()),
        });
        let hosts = Hosts::new(&url, "h1", "t");
        hosts.begin("j1", 1);
        hosts.begin("j2", 1);
        let none = Metrics {
            cpu_load: 0.0,
            memory_used: 0,
            memory_total: 0,
            disk_free: 0,
            cpus: 4,
        };
        let running = ["j1".to_string(), "j2".to_string()];
        let beat = Heartbeat {
            free: Resources {
                cpu: Cpu::of(0.0),
                memory: 0,
                gpu: 0,
            },
            metrics: none,
            running: &running,
        };
        assert_eq!(hosts.heartbeat(&beat).unwrap().ended, ["j1"]);
        hosts.heartbeat(&beat).unwrap();
        assert!(matches!(
            hosts.append("j1", 0, b"").unwrap(),
            Appended::Ended(_)
        ));
        assert!(matches!(
            hosts.append("j1", 0, b"x").unwrap(),
            Appended::Ended(_)
        ));
        assert!(matches!(
            hosts.append("j2", 0, b"x").unwrap(),
            Appended::Stored { size: 1, .. }
        ));
        let report = Report {
            state: End::Failed,
            exit: None,
            duration: 1.0,
            reason: Some("jobber ended it".into()),
        };
        hosts.report("j1", &report).unwrap();
        hosts.report("j2", &report).unwrap();
        let requests: Vec<String> = sent
            .lock()
            .unwrap()
            .iter()
            .map(|s| s.request().to_string())
            .collect();
        assert_eq!(
            requests,
            [
                "POST /hosts/h1/heartbeat",
                "POST /hosts/h1/heartbeat",
                "PATCH /runs/j2/log?attempt=1",
                "PUT /runs/j2?attempt=1",
            ],
            "nothing sent for the ended run, its completion included"
        );
    }

    #[test]
    fn a_heartbeat_answer_without_ended_ends_nothing() {
        let beat: Beat = serde_json::from_str(r#"{"cancel":["a"],"lease":60}"#).unwrap();
        assert_eq!(
            beat,
            Beat {
                cancel: vec!["a".into()],
                ended: vec![],
                lease: 60
            }
        );
    }
}
