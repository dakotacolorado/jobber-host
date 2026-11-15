//! The host's one connection to jobber, over which it is sent its runs
//! (jobber's `docs/PROTOCOL.md`). This thread holds it: it reads what jobber
//! sends, answers it, and sends the heartbeat, by HTTP while there is no
//! connection. Each run started gets a thread of its own, and keeps going
//! whatever the connection does.
//!
//! A run is reserved from the budget when it is accepted and started only on
//! `start`; what is started is keyed by run and attempt, so an `assign` or
//! `start` sent again after a reconnect runs nothing twice.

use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hasher};
use std::io::ErrorKind;
use std::net::{TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tungstenite::client::IntoClientRequest;
use tungstenite::handshake::HandshakeError;
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

use crate::budget::{Cpu, JobReservation, Resources, Shared};
use crate::heartbeat::{self, metrics};
use crate::log;
use crate::retry::{Backoff, Executor};
use crate::service::{Assignment, Beat, Failure, Heartbeat, HostRun, Hosts, Metrics, Secrets};

/// The most between tries to connect.
const MOST: Duration = Duration::from_secs(30);
/// How long dialing and upgrading may take, and a send.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How often the loop looks up from reading: for a heartbeat due, or a stop.
const TICK: Duration = Duration::from_millis(250);
/// An accepted run neither started nor dropped in this long is given up, so
/// its reservation does not wait on an answer that is never coming.
const ACCEPTED_FOR: Duration = Duration::from_secs(60);
/// The close code jobber ends a connection with when a newer one replaces it.
const REPLACED: u16 = 4000;

type Socket = WebSocket<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Incoming {
    Assign {
        run: HostRun,
        attempt: u64,
        /// What it requests, as jobber admitted it; absent from one
        /// before CPU requests, whose runs are taken at its default.
        #[serde(default = "default_cpu")]
        cpu: f64,
        memory: u64,
        /// Absent from a jobber before GPUs, which sends no run that needs one.
        #[serde(default)]
        gpu: u32,
        lease: u64,
    },
    Start {
        run: String,
        attempt: u64,
        #[serde(default)]
        lease: Option<u64>,
        /// The run's secrets, sent with its start alone; none from a jobber before them.
        #[serde(default)]
        secrets: Secrets,
    },
    Drop {
        run: String,
        attempt: u64,
        #[serde(default)]
        reason: String,
    },
    Cancel {
        run: String,
        attempt: u64,
    },
    Heartbeat(Beat),
    Error {
        message: String,
    },
}

#[derive(Debug, Serialize)]
struct Attempt {
    id: String,
    attempt: u64,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Outgoing {
    Heartbeat {
        free: Resources,
        metrics: Metrics,
        running: Vec<Attempt>,
    },
    Accept {
        run: String,
        attempt: u64,
    },
    Decline {
        run: String,
        attempt: u64,
        reason: String,
    },
}

/// A run accepted and not yet started: its CPU and memory held for it.
struct Accepted {
    run: HostRun,
    admitted: Resources,
    lease: u64,
    _reservation: JobReservation,
    at: Instant,
}

/// How a connection ended.
enum Closed {
    /// Told to stop, with nothing left running.
    Stopped,
    /// A newer connection for this host took its place.
    Replaced,
    Lost(String),
}

/// What the loop keeps from one connection to the next.
struct State {
    /// Accepted, by run and attempt. Given back when a connection ends:
    /// jobber sends again, on the next, each it still assigns here.
    accepted: BTreeMap<(String, u64), Accepted>,
    /// When the next heartbeat is due, over the connection or not.
    beat: Instant,
    /// Between heartbeats, as the last answer's lease allows.
    every: Duration,
    /// Whether the last HTTP heartbeat failed, so a failure is logged once.
    failing: bool,
}

pub struct Host<E: Executor> {
    pub hosts: Arc<Hosts>,
    pub executor: Arc<E>,
    pub budget: Shared,
    pub work_dir: PathBuf,
    /// Between heartbeats, at most.
    pub every: Duration,
}

impl<E: Executor> Host<E> {
    /// Holds the connection until told to stop: reconnecting with backoff
    /// whenever it drops, and declining what is assigned once stopping. Then
    /// waits for the runs it started, which see the same stop and end theirs.
    pub fn run(&self, stop: &dyn Fn() -> bool) {
        let done = || stop() && self.hosts.running().is_empty();
        let mut backoff = Backoff::upto(MOST);
        let mut state = State {
            accepted: BTreeMap::new(),
            beat: Instant::now(),
            every: self.every,
            failing: false,
        };
        while !done() {
            let mut wait = None;
            match self.connect() {
                Ok(mut socket) => {
                    log!("connected to jobber");
                    backoff.reset();
                    let closed = self.converse(&mut socket, &mut state, stop);
                    if !state.accepted.is_empty() {
                        log!(
                            "giving back {} accepted and not started: jobber sends them again",
                            state.accepted.len()
                        );
                        state.accepted.clear();
                    }
                    match closed {
                        Closed::Stopped => {
                            let _ = socket.close(None);
                            let _ = socket.flush();
                            return;
                        }
                        Closed::Replaced => {
                            log!("connection replaced by a newer one for this host: is another jobber-host running with its token?");
                            wait = Some(MOST);
                        }
                        Closed::Lost(why) => log!("connection lost: {why}"),
                    }
                }
                Err(e) => {
                    let hint = match e {
                        Failure::Refused(401, _) => {
                            ": another jobber-host registered under this name has its token now"
                        }
                        _ => "",
                    };
                    log!("connecting failed: {e}{hint}");
                    if matches!(e, Failure::Refused(..)) {
                        wait = Some(MOST);
                    }
                }
            }
            let wait = jitter(wait.unwrap_or_else(|| backoff.next()));
            log!("connecting again in {:.1}s", wait.as_secs_f64());
            self.disconnected(wait, &mut state, &done);
        }
    }

    /// `GET /hosts/{id}/connect`, upgraded, with the host's token.
    fn connect(&self) -> Result<Socket, Failure> {
        let transient = |e: &dyn std::fmt::Display| Failure::Transient(e.to_string());
        let mut request = self
            .hosts
            .connect_url()
            .into_client_request()
            .map_err(|e| transient(&e))?;
        request.headers_mut().insert(
            "Authorization",
            self.hosts
                .authorization()
                .parse()
                .map_err(|e| transient(&e))?,
        );
        let uri = request.uri();
        let host = uri
            .host()
            .ok_or_else(|| transient(&"no host in the service's URL"))?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        let port = uri
            .port_u16()
            .unwrap_or(if uri.scheme_str() == Some("wss") {
                443
            } else {
                80
            });
        let stream = dial(&host, port)?;
        stream
            .set_read_timeout(Some(CONNECT_TIMEOUT))
            .and_then(|()| stream.set_write_timeout(Some(CONNECT_TIMEOUT)))
            .map_err(|e| transient(&e))?;
        let tcp = stream.try_clone().map_err(|e| transient(&e))?;
        let (socket, _) = tungstenite::client_tls_with_config(request, stream, None, None)
            .map_err(|e| match e {
                HandshakeError::Failure(tungstenite::Error::Http(response)) => {
                    let code = response.status().as_u16();
                    let body = response
                        .body()
                        .as_deref()
                        .map(|b| String::from_utf8_lossy(b).into_owned())
                        .unwrap_or_default();
                    if code == 408 || code == 429 || code >= 500 {
                        Failure::Transient(format!("{code} {body}"))
                    } else {
                        Failure::Refused(code, body)
                    }
                }
                HandshakeError::Failure(e) => transient(&e),
                HandshakeError::Interrupted(_) => transient(&"timed out upgrading"),
            })?;
        // Reads time out, so the loop looks up for heartbeats and stops however quiet jobber is.
        tcp.set_read_timeout(Some(self.tick()))
            .map_err(|e| transient(&e))?;
        Ok(socket)
    }

    fn tick(&self) -> Duration {
        TICK.min(self.every)
    }

    /// Reads and answers until the connection ends, sending a heartbeat as
    /// soon as it opens and every `state.every` after.
    fn converse(&self, socket: &mut Socket, state: &mut State, stop: &dyn Fn() -> bool) -> Closed {
        state.beat = Instant::now();
        loop {
            if stop() && self.hosts.running().is_empty() && state.accepted.is_empty() {
                return Closed::Stopped;
            }
            if Instant::now() >= state.beat {
                state.beat = Instant::now() + state.every;
                if let Err(e) = send(socket, &self.heartbeat()) {
                    return Closed::Lost(e);
                }
            }
            self.expire(state);
            match socket.read() {
                Ok(Message::Text(text)) => {
                    if let Some(reply) = self.handle(&text, state, stop) {
                        if let Err(e) = send(socket, &reply) {
                            return Closed::Lost(e);
                        }
                    }
                }
                Ok(Message::Close(frame)) => {
                    return match frame {
                        Some(f) if f.code == CloseCode::from(REPLACED) => Closed::Replaced,
                        Some(f) => {
                            Closed::Lost(format!("closed {}: {}", u16::from(f.code), f.reason))
                        }
                        None => Closed::Lost("closed".into()),
                    };
                }
                Ok(_) => {}
                // A signal (SIGTERM, to stop) interrupts a read: the loop looks up for it as for a timeout.
                Err(tungstenite::Error::Io(e))
                    if matches!(
                        e.kind(),
                        ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
                    ) => {}
                Err(e) => return Closed::Lost(e.to_string()),
            }
        }
    }

    /// Waits `wait` with no connection, sending heartbeats by HTTP when due, so
    /// the runs' leases hold.
    fn disconnected(&self, wait: Duration, state: &mut State, done: &dyn Fn() -> bool) {
        let until = Instant::now() + wait;
        while !done() {
            let now = Instant::now();
            if now >= until {
                return;
            }
            if now >= state.beat {
                self.beat_over_http(state);
                state.beat = Instant::now() + state.every;
            }
            let next = until.min(state.beat);
            thread::sleep(next.saturating_duration_since(now).min(self.tick()));
        }
    }

    fn beat_over_http(&self, state: &mut State) {
        let ids: Vec<String> = self.hosts.running().into_iter().map(|(id, _)| id).collect();
        let sent = self.hosts.heartbeat(&Heartbeat {
            free: self.budget.free(),
            metrics: metrics(&self.work_dir),
            running: &ids,
        });
        match sent {
            Ok(answer) => {
                if state.failing {
                    log!("heartbeat answered again");
                    state.failing = false;
                }
                state.every = heartbeat::wait(self.every, answer.lease);
            }
            Err(e) => {
                if !state.failing {
                    log!("heartbeat failed: {e}; trying again with each beat");
                    state.failing = true;
                }
            }
        }
    }

    fn heartbeat(&self) -> Outgoing {
        Outgoing::Heartbeat {
            free: self.budget.free(),
            metrics: metrics(&self.work_dir),
            running: self
                .hosts
                .running()
                .into_iter()
                .map(|(id, attempt)| Attempt { id, attempt })
                .collect(),
        }
    }

    /// Gives back what was accepted too long ago to be started.
    fn expire(&self, state: &mut State) {
        state.accepted.retain(|(run, attempt), a| {
            let keep = a.at.elapsed() < ACCEPTED_FOR;
            if !keep {
                log!(
                    "run {run}: attempt {attempt} accepted {}s ago and neither started nor dropped: giving it back",
                    ACCEPTED_FOR.as_secs()
                );
            }
            keep
        });
    }

    /// Takes in one message, answering what it says to.
    fn handle(&self, text: &str, state: &mut State, stop: &dyn Fn() -> bool) -> Option<Outgoing> {
        let message = match serde_json::from_str::<Incoming>(text) {
            Ok(m) => m,
            Err(e) => {
                let shown: String = text.chars().take(200).collect();
                log!("unreadable message from jobber ({e}): {shown}");
                return None;
            }
        };
        match message {
            Incoming::Assign {
                run,
                attempt,
                cpu,
                memory,
                gpu,
                lease,
            } => {
                let admitted = Resources {
                    cpu: Cpu::of(cpu),
                    memory,
                    gpu,
                };
                Some(self.assign(run, attempt, admitted, lease, state, stop))
            }
            Incoming::Start {
                run,
                attempt,
                lease,
                secrets,
            } => {
                if self.hosts.attempt(&run) == Some(attempt) {
                    return None;
                }
                match state.accepted.remove(&(run.clone(), attempt)) {
                    Some(accepted) => self.start(accepted, attempt, lease, secrets),
                    None => {
                        log!("run {run}: start for attempt {attempt}, not accepted here: ignored")
                    }
                }
                None
            }
            Incoming::Drop {
                run,
                attempt,
                reason,
            } => {
                if state.accepted.remove(&(run.clone(), attempt)).is_some() {
                    log!("run {run}: attempt {attempt} dropped: {reason}");
                } else if self.hosts.attempt(&run) == Some(attempt) {
                    // Not this host's any more, though started: stopped as one ended there.
                    log!("run {run}: attempt {attempt} dropped while running: {reason}");
                    self.hosts.end(&run);
                }
                None
            }
            Incoming::Cancel { run, attempt } => {
                if self.hosts.attempt(&run) == Some(attempt) {
                    self.hosts.cancel(&run);
                }
                None
            }
            Incoming::Heartbeat(beat) => {
                self.hosts.heard(&beat);
                state.every = heartbeat::wait(self.every, beat.lease);
                None
            }
            Incoming::Error { message } => {
                log!("jobber could not read a message: {message}");
                None
            }
        }
    }

    fn assign(
        &self,
        run: HostRun,
        attempt: u64,
        admitted: Resources,
        lease: u64,
        state: &mut State,
        stop: &dyn Fn() -> bool,
    ) -> Outgoing {
        let id = run.id.clone();
        let decline = |reason: String| {
            log!("run {id}: attempt {attempt} declined: {reason}");
            Outgoing::Decline {
                run: id.clone(),
                attempt,
                reason,
            }
        };
        let accept = || Outgoing::Accept {
            run: id.clone(),
            attempt,
        };
        // Sent again after a reconnect: what it was accepted with still holds.
        if self.hosts.attempt(&id) == Some(attempt)
            || state.accepted.contains_key(&(id.clone(), attempt))
        {
            return accept();
        }
        if let Some(other) = self.hosts.attempt(&id) {
            return decline(format!("attempt {other} of it is still running here"));
        }
        if stop() {
            return decline("jobber-host is stopping".into());
        }
        let free = self.budget.free();
        match self.budget.reserve_job(admitted) {
            Ok(reservation) => {
                log!(
                    "run {id}: attempt {attempt} assigned and accepted: {} {} at {}, {} CPU of {} free and {} MiB of {} MiB free{}",
                    run.repo,
                    run.job,
                    run.commit,
                    admitted.cpu,
                    free.cpu,
                    admitted.memory,
                    free.memory,
                    if reservation.gpus.is_empty() {
                        String::new()
                    } else {
                        format!(", GPUs {}", reservation.gpus.join(" "))
                    }
                );
                state.accepted.insert(
                    (id.clone(), attempt),
                    Accepted {
                        run,
                        admitted,
                        lease,
                        _reservation: reservation,
                        at: Instant::now(),
                    },
                );
                accept()
            }
            Err(why) => decline(why),
        }
    }

    fn start(&self, accepted: Accepted, attempt: u64, lease: Option<u64>, secrets: Secrets) {
        let Accepted {
            run,
            admitted,
            lease: assigned,
            _reservation: reservation,
            ..
        } = accepted;
        let id = run.id.clone();
        let assignment = Assignment {
            run,
            cpu: admitted.cpu,
            memory: admitted.memory,
            gpus: reservation.gpus.clone(),
            lease: lease.unwrap_or(assigned),
            secrets,
        };
        log!("run {id}: attempt {attempt} started");
        self.hosts.begin(&id, attempt);
        let (executor, budget, hosts) = (
            self.executor.clone(),
            self.budget.clone(),
            self.hosts.clone(),
        );
        thread::Builder::new()
            .name(format!("run-{id}"))
            .spawn(move || {
                struct Done(Arc<Hosts>, String);
                impl Drop for Done {
                    fn drop(&mut self) {
                        self.0.finish(&self.1);
                    }
                }
                let _done = Done(hosts, id);
                executor.run(assignment, reservation, &budget);
            })
            .expect("spawning a run thread");
    }
}

/// jobber's own default request, for an `assign` from one that sends none.
fn default_cpu() -> f64 {
    0.5
}

fn send(socket: &mut Socket, message: &Outgoing) -> Result<(), String> {
    let text = serde_json::to_string(message).map_err(|e| e.to_string())?;
    socket.send(Message::text(text)).map_err(|e| e.to_string())
}

/// The first of `host`'s addresses that answers within the timeout.
fn dial(host: &str, port: u16) -> Result<TcpStream, Failure> {
    let addrs = (host, port)
        .to_socket_addrs()
        .map_err(|e| Failure::Transient(format!("{host}: {e}")))?;
    let mut last = Failure::Transient(format!("{host}: no address"));
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
            Ok(stream) => return Ok(stream),
            Err(e) => last = Failure::Transient(format!("{addr}: {e}")),
        }
    }
    Err(last)
}

/// Between half and all of `d`, so hosts that lost jobber together do
/// not all come back at once.
fn jitter(d: Duration) -> Duration {
    let random = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    d.mul_f64(0.5 + (random % 1000) as f64 / 2000.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::Budget;
    use crate::service::fake_http::{answer_one, Log, Sent};
    use crate::service::{End, Report, Service};
    use serde_json::{json, Value};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{mpsc, Mutex};
    use tungstenite::handshake::server::{Request, Response};

    /// One connection the fake jobber has taken, and what it was opened with.
    struct Peer {
        ws: WebSocket<TcpStream>,
        path: String,
        authorization: String,
    }

    impl Peer {
        fn send(&mut self, message: Value) {
            self.ws.send(Message::text(message.to_string())).unwrap();
        }

        /// The next message the host sends.
        fn next(&mut self) -> Value {
            loop {
                match self.ws.read().expect("a message from the host") {
                    Message::Text(t) => return serde_json::from_str(&t).unwrap(),
                    _ => continue,
                }
            }
        }

        /// The next message the host sends that is not a heartbeat.
        fn answer(&mut self) -> Value {
            loop {
                let m = self.next();
                if m["type"] != "heartbeat" {
                    return m;
                }
            }
        }

        /// The next heartbeat for which `f` holds.
        fn heartbeat_where(&mut self, f: impl Fn(&Value) -> bool) -> Value {
            loop {
                let m = self.next();
                if m["type"] == "heartbeat" && f(&m) {
                    return m;
                }
            }
        }
    }

    /// jobber as a host sees it: WebSocket upgrades on `/connect`, each
    /// handed to the test, and every other request answered by HTTP.
    struct Fake {
        url: String,
        peers: mpsc::Receiver<Peer>,
        http: Log,
        /// Upgrades are answered 503 while it is set.
        refuse: Arc<AtomicBool>,
    }

    impl Fake {
        fn peer(&self) -> Peer {
            self.peers
                .recv_timeout(Duration::from_secs(10))
                .expect("the host connects")
        }

        fn requests(&self) -> Vec<String> {
            self.http
                .lock()
                .unwrap()
                .iter()
                .map(|s| s.request().to_string())
                .collect()
        }

        fn http_heartbeats(&self) -> Vec<Value> {
            self.http
                .lock()
                .unwrap()
                .iter()
                .filter(|s| s.request() == "POST /hosts/h1/heartbeat")
                .map(Sent::json)
                .collect()
        }
    }

    // tungstenite's handshake callback answers its own Result, large or not.
    #[allow(clippy::result_large_err)]
    fn fake() -> Fake {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (tx, peers) = mpsc::channel();
        let http: Log = Default::default();
        let refuse = Arc::new(AtomicBool::new(false));
        let (kept, refusing) = (http.clone(), refuse.clone());
        thread::spawn(move || {
            let answer = |s: &Sent| -> (String, String) {
                let ok = |b: &str| ("200 OK".to_string(), b.to_string());
                match s.request() {
                    "POST /hosts/h1/heartbeat" => ok(r#"{"cancel":[],"lease":60}"#),
                    r if r.starts_with("PATCH ") => ok(r#"{"size":6}"#),
                    _ => ok("{}"),
                }
            };
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                if !upgrade(&stream) {
                    answer_one(stream, &answer, &kept);
                    continue;
                }
                if refusing.load(Ordering::SeqCst) {
                    let unavailable = |_: &Sent| {
                        (
                            "503 Service Unavailable".to_string(),
                            r#"{"code":"InternalServerException","message":"down"}"#.to_string(),
                        )
                    };
                    answer_one(stream, &unavailable, &Log::default());
                    continue;
                }
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let (mut path, mut authorization) = (String::new(), String::new());
                let ws = tungstenite::accept_hdr(stream, |req: &Request, res: Response| {
                    path = req.uri().path().to_string();
                    authorization = req.headers()["authorization"].to_str().unwrap().into();
                    Ok(res)
                })
                .unwrap();
                let _ = tx.send(Peer {
                    ws,
                    path,
                    authorization,
                });
            }
        });
        Fake {
            url,
            peers,
            http,
            refuse,
        }
    }

    /// Whether the request coming on `stream` asks to connect.
    fn upgrade(stream: &TcpStream) -> bool {
        let mut buf = [0; 512];
        loop {
            let n = stream.peek(&mut buf).unwrap_or(0);
            let head = String::from_utf8_lossy(&buf[..n]);
            if let Some((line, _)) = head.split_once("\r\n") {
                return line.starts_with("GET ") && line.contains("/connect");
            }
            if n == 0 || n == buf.len() {
                return false;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// Runs each run it is started on, held while `hold` is set, then logs
    /// a line and completes it, as the podman executor does.
    struct Recording {
        hosts: Arc<Hosts>,
        ran: Mutex<Vec<(String, u64, u64)>>,
        hold: AtomicBool,
    }

    impl Executor for Recording {
        fn run(&self, run: Assignment, reservation: JobReservation, _: &Shared) {
            let id = run.run.id.clone();
            self.ran
                .lock()
                .unwrap()
                .push((id.clone(), run.memory, run.lease));
            while self.hold.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(5));
            }
            let _ = self.hosts.append(&id, 0, b"hello\n");
            drop(reservation);
            let _ = self.hosts.report(
                &id,
                &Report {
                    state: End::Succeeded,
                    exit: Some(0),
                    duration: 0.1,
                    reason: None,
                },
            );
        }
    }

    struct Running {
        host: Arc<Host<Recording>>,
        stop: Arc<AtomicBool>,
        thread: thread::JoinHandle<()>,
    }

    impl Running {
        fn ran(&self) -> Vec<(String, u64, u64)> {
            self.host.executor.ran.lock().unwrap().clone()
        }

        fn hold(&self, hold: bool) {
            self.host.executor.hold.store(hold, Ordering::SeqCst);
        }

        fn free(&self) -> Resources {
            self.host.budget.free()
        }

        fn stop(self) {
            self.hold(false);
            self.stop.store(true, Ordering::SeqCst);
            self.thread.join().unwrap();
        }
    }

    /// A host offering `cpus` and `memory`; each run `assign` sends requests one CPU.
    fn start(fake: &Fake, cpus: u32, memory: u64) -> Running {
        let hosts = Arc::new(Hosts::new(&fake.url, "h1", "h1.secret"));
        let host = Arc::new(Host {
            hosts: hosts.clone(),
            executor: Arc::new(Recording {
                hosts,
                ran: Mutex::new(vec![]),
                hold: AtomicBool::new(false),
            }),
            budget: Shared::new(Budget::new(offer(cpus, memory), 256)),
            work_dir: "/".into(),
            every: Duration::from_millis(50),
        });
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let (host, stop) = (host.clone(), stop.clone());
            thread::spawn(move || host.run(&|| stop.load(Ordering::SeqCst)))
        };
        Running { host, stop, thread }
    }

    fn offer(cpus: u32, memory: u64) -> Resources {
        Resources {
            cpu: Cpu::of(cpus as f64),
            memory,
            gpu: 0,
        }
    }

    fn wait_for(what: &str, f: impl Fn() -> bool) {
        let until = Instant::now() + Duration::from_secs(10);
        while !f() {
            assert!(Instant::now() < until, "never: {what}");
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn assign(run: &str, attempt: u64, memory: u64) -> Value {
        json!({
            "type": "assign",
            "run": {
                "id": run, "repo": "o/r", "commit": "0".repeat(40), "job": "hello",
                "args": [], "requirements": {"os": "linux", "arch": null, "memory": memory, "timeout": null},
                "labels": {}, "state": "assigned", "attempt": attempt, "host": "h1"
            },
            "attempt": attempt,
            "cpu": 1.0,
            "memory": memory,
            "lease": 60
        })
    }

    #[test]
    fn a_start_carries_its_runs_secrets_and_never_shows_them() {
        let with: Incoming = serde_json::from_value(json!({"type": "start", "run": "r1", "attempt": 1, "lease": 45, "secrets": {"NPM_TOKEN": "npm_value"}})).unwrap();
        let Incoming::Start { secrets, .. } = &with else {
            panic!("{with:?}")
        };
        assert_eq!(secrets.0["NPM_TOKEN"], "npm_value");
        assert!(!format!("{with:?}").contains("npm_value"), "{with:?}");
        let before: Incoming = serde_json::from_value(start_message("r1", 1)).unwrap();
        let Incoming::Start { secrets, .. } = before else {
            panic!()
        };
        assert!(secrets.0.is_empty(), "none from a jobber before secrets");
    }

    fn start_message(run: &str, attempt: u64) -> Value {
        json!({"type": "start", "run": run, "attempt": attempt, "lease": 45})
    }

    #[test]
    fn a_pushed_run_is_accepted_started_only_on_start_and_completed_on_its_attempt() {
        let fake = fake();
        let host = start(&fake, 2, 2048);
        let mut peer = fake.peer();
        assert_eq!(peer.path, "/hosts/h1/connect");
        assert_eq!(peer.authorization, "Bearer h1.secret");
        let first = peer.next();
        assert_eq!(
            first["type"], "heartbeat",
            "a heartbeat as soon as it connects"
        );
        assert_eq!(first["free"], json!({"cpu": 2.0, "memory": 2048, "gpu": 0}));
        assert_eq!(first["running"], json!([]));
        for key in ["cpuLoad", "memoryUsed", "memoryTotal", "diskFree", "cpus"] {
            assert!(first["metrics"][key].is_number(), "{key}: {first}");
        }

        peer.send(assign("r1", 3, 512));
        assert_eq!(
            peer.answer(),
            json!({"type": "accept", "run": "r1", "attempt": 3})
        );
        assert_eq!(host.free(), offer(1, 1536), "reserved once accepted");
        assert!(host.ran().is_empty(), "not run before its start");

        peer.send(start_message("r1", 3));
        wait_for("its completion", || {
            fake.requests()
                .contains(&"PUT /runs/r1?attempt=3".to_string())
        });
        assert_eq!(
            host.ran(),
            [("r1".to_string(), 512, 45)],
            "its lease from its start"
        );
        assert!(fake
            .requests()
            .contains(&"PATCH /runs/r1/log?attempt=3".to_string()));
        assert!(
            fake.requests().iter().all(|r| r.contains("/runs/r1")),
            "only its log and completion go by HTTP while connected"
        );
        wait_for("its reservation given back", || {
            host.free() == offer(2, 2048)
        });
        host.stop();
    }

    #[test]
    fn an_assignment_accepted_and_lost_with_the_connection_is_run_once_when_sent_again() {
        let fake = fake();
        let host = start(&fake, 1, 1024);
        let mut peer = fake.peer();
        peer.send(assign("r1", 1, 512));
        assert_eq!(peer.answer()["type"], "accept");
        drop(peer);

        let mut peer = fake.peer();
        assert_eq!(peer.next()["type"], "heartbeat");
        assert_eq!(
            host.free().cpu,
            Cpu::of(1.0),
            "given back with the connection"
        );
        peer.send(assign("r1", 1, 512));
        assert_eq!(
            peer.answer(),
            json!({"type": "accept", "run": "r1", "attempt": 1})
        );
        peer.send(start_message("r1", 1));
        wait_for("its completion", || {
            fake.requests()
                .contains(&"PUT /runs/r1?attempt=1".to_string())
        });
        assert_eq!(host.ran().len(), 1);
        host.stop();
    }

    #[test]
    fn a_run_started_before_the_connection_dropped_is_accepted_again_and_not_run_twice() {
        let fake = fake();
        let host = start(&fake, 2, 2048);
        host.hold(true);
        let mut peer = fake.peer();
        peer.send(assign("r1", 2, 512));
        assert_eq!(peer.answer()["type"], "accept");
        peer.send(start_message("r1", 2));
        wait_for("it runs", || host.ran().len() == 1);
        peer.ws.close(None).unwrap();
        drop(peer);

        let mut peer = fake.peer();
        let first = peer.next();
        assert_eq!(first["running"], json!([{"id": "r1", "attempt": 2}]));
        peer.send(assign("r1", 2, 512));
        assert_eq!(
            peer.answer(),
            json!({"type": "accept", "run": "r1", "attempt": 2}),
            "an assign for the attempt it runs is accepted again"
        );
        peer.send(start_message("r1", 2));
        // Ordered after the start: once answered, the start has been read.
        peer.send(assign("r2", 1, 4096));
        assert_eq!(peer.answer()["type"], "decline");
        assert_eq!(host.ran().len(), 1, "the start sent again is ignored");
        assert_eq!(host.free(), offer(1, 1536), "reserved once");
        host.hold(false);
        wait_for("its completion", || {
            fake.requests()
                .contains(&"PUT /runs/r1?attempt=2".to_string())
        });
        assert_eq!(host.ran().len(), 1);
        host.stop();
    }

    #[test]
    fn a_dropped_assignment_gives_its_reservation_back_and_never_runs() {
        let fake = fake();
        let host = start(&fake, 1, 1024);
        let mut peer = fake.peer();
        peer.send(assign("r1", 1, 512));
        assert_eq!(peer.answer()["type"], "accept");
        assert_eq!(host.free().cpu, Cpu::of(0.0));
        peer.send(json!({"type": "drop", "run": "r1", "attempt": 1, "reason": "canceled"}));
        wait_for("its reservation given back", || {
            host.free() == offer(1, 1024)
        });
        peer.send(start_message("r1", 1));
        peer.send(assign("r2", 1, 2048));
        assert_eq!(peer.answer()["type"], "decline");
        assert!(host.ran().is_empty(), "a start after its drop runs nothing");
        host.stop();
    }

    #[test]
    fn an_assignment_over_the_budget_is_declined_saying_why() {
        let fake = fake();
        let host = start(&fake, 1, 1024);
        let mut peer = fake.peer();
        peer.send(assign("big", 1, 2048));
        let declined = peer.answer();
        assert_eq!(
            (&declined["type"], &declined["run"], &declined["attempt"]),
            (&json!("decline"), &json!("big"), &json!(1))
        );
        assert!(
            declined["reason"]
                .as_str()
                .unwrap()
                .contains("does not fit"),
            "{declined}"
        );
        peer.send(assign("r1", 1, 512));
        assert_eq!(peer.answer()["type"], "accept");
        peer.send(assign("r2", 1, 256));
        let declined = peer.answer();
        assert!(
            declined["reason"]
                .as_str()
                .unwrap()
                .contains("1 CPU does not fit the 0 CPU free of 1"),
            "{declined}"
        );
        assert_eq!(host.free().cpu, Cpu::of(0.0), "a decline reserves nothing");
        assert!(host.ran().is_empty());
        // Else stopping waits on its start.
        peer.send(json!({"type": "drop", "run": "r1", "attempt": 1, "reason": "x"}));
        host.stop();
    }

    #[test]
    fn heartbeats_name_each_runs_attempt_and_go_by_http_while_disconnected() {
        let fake = fake();
        let host = start(&fake, 2, 2048);
        host.hold(true);
        let mut peer = fake.peer();
        peer.send(assign("r1", 2, 512));
        assert_eq!(peer.answer()["type"], "accept");
        peer.send(start_message("r1", 2));
        peer.heartbeat_where(|b| b["running"] == json!([{"id": "r1", "attempt": 2}]));
        assert!(
            fake.http_heartbeats().is_empty(),
            "none by HTTP while connected"
        );

        fake.refuse.store(true, Ordering::SeqCst);
        drop(peer);
        wait_for("a heartbeat by HTTP", || !fake.http_heartbeats().is_empty());
        let beat = &fake.http_heartbeats()[0];
        assert_eq!(
            beat["running"],
            json!(["r1"]),
            "ids, as SendHeartbeat takes them"
        );
        assert_eq!(beat["free"], json!({"cpu": 1.0, "memory": 1536, "gpu": 0}));

        fake.refuse.store(false, Ordering::SeqCst);
        let mut peer = fake.peer();
        assert_eq!(peer.next()["running"], json!([{"id": "r1", "attempt": 2}]));
        let by_http = fake.http_heartbeats().len();
        peer.heartbeat_where(|_| true);
        peer.heartbeat_where(|_| true);
        assert_eq!(
            fake.http_heartbeats().len(),
            by_http,
            "none by HTTP once connected again"
        );
        host.stop();
    }

    #[test]
    fn a_cancel_or_a_heartbeats_answer_reaches_the_run_it_names() {
        let fake = fake();
        let host = start(&fake, 2, 2048);
        host.hold(true);
        let mut peer = fake.peer();
        for run in ["r1", "r2"] {
            peer.send(assign(run, 1, 256));
            assert_eq!(peer.answer()["type"], "accept");
            peer.send(start_message(run, 1));
        }
        wait_for("both run", || host.ran().len() == 2);
        let hosts = &host.host.hosts;
        let canceled = |run: &str| {
            matches!(
                hosts.append(run, 0, b"").unwrap(),
                crate::service::Appended::Stored { canceled: true, .. }
            )
        };
        peer.send(json!({"type": "cancel", "run": "r1", "attempt": 7}));
        peer.send(json!({"type": "cancel", "run": "r1", "attempt": 1}));
        wait_for("r1 canceled", || canceled("r1"));
        assert!(!canceled("r2"));
        peer.send(json!({"type": "heartbeat", "cancel": [], "ended": ["r2"], "lease": 60}));
        wait_for("r2 ended", || {
            matches!(
                hosts.append("r2", 0, b"").unwrap(),
                crate::service::Appended::Ended(_)
            )
        });
        peer.send(json!({"type": "nonsense"}));
        peer.send(assign("r3", 1, 4096));
        assert_eq!(
            peer.answer()["type"],
            "decline",
            "an unreadable message is passed over"
        );
        host.hold(false);
        wait_for("r1 completed, r2 not", || {
            fake.requests()
                .contains(&"PUT /runs/r1?attempt=1".to_string())
        });
        host.stop();
        assert!(!fake
            .requests()
            .contains(&"PUT /runs/r2?attempt=1".to_string()));
    }

    #[test]
    fn once_stopping_it_declines_what_is_assigned_and_waits_for_its_runs() {
        let fake = fake();
        let host = start(&fake, 2, 2048);
        host.hold(true);
        let mut peer = fake.peer();
        peer.send(assign("r1", 1, 256));
        assert_eq!(peer.answer()["type"], "accept");
        peer.send(start_message("r1", 1));
        wait_for("it runs", || host.ran().len() == 1);
        host.stop.store(true, Ordering::SeqCst);
        peer.send(assign("r2", 1, 256));
        let declined = peer.answer();
        assert!(
            declined["reason"].as_str().unwrap().contains("stopping"),
            "{declined}"
        );
        assert!(!host.thread.is_finished(), "waiting for r1");
        host.hold(false);
        host.thread.join().unwrap();
        assert!(fake
            .requests()
            .contains(&"PUT /runs/r1?attempt=1".to_string()));
    }

    #[test]
    fn once_draining_it_declines_what_is_assigned_and_runs_what_it_took_to_its_end() {
        let fake = fake();
        let host = start(&fake, 2, 2048);
        host.hold(true);
        let mut peer = fake.peer();
        peer.send(assign("r1", 1, 256));
        assert_eq!(peer.answer()["type"], "accept");
        peer.send(start_message("r1", 1));
        wait_for("it runs", || host.ran().len() == 1);
        peer.send(assign("r2", 1, 256));
        assert_eq!(peer.answer()["type"], "accept");
        // Draining is winding down as stopping is: the executor alone tells them apart.
        host.stop.store(true, Ordering::SeqCst);
        peer.send(assign("r3", 1, 256));
        let declined = peer.answer();
        assert_eq!(declined["run"], "r3");
        assert!(
            declined["reason"].as_str().unwrap().contains("stopping"),
            "{declined}"
        );
        peer.heartbeat_where(|b| b["running"] == json!([{"id": "r1", "attempt": 1}]));
        peer.send(start_message("r2", 1));
        wait_for("what it accepted runs too", || host.ran().len() == 2);
        thread::sleep(Duration::from_millis(300));
        assert!(!host.thread.is_finished(), "waiting for r1 and r2");
        assert!(!host.ran().iter().any(|(id, ..)| id == "r3"));
        host.hold(false);
        host.thread.join().unwrap();
        for run in ["r1", "r2"] {
            assert!(fake
                .requests()
                .contains(&format!("PUT /runs/{run}?attempt=1")));
        }
    }

    #[test]
    fn a_refused_connection_says_its_status() {
        let fake = fake();
        fake.refuse.store(true, Ordering::SeqCst);
        let hosts = Arc::new(Hosts::new(&fake.url, "h1", "t"));
        let host = Host {
            hosts: hosts.clone(),
            executor: Arc::new(Recording {
                hosts,
                ran: Mutex::new(vec![]),
                hold: AtomicBool::new(false),
            }),
            budget: Shared::new(Budget::new(offer(1, 1024), 256)),
            work_dir: "/".into(),
            every: Duration::from_millis(50),
        };
        assert!(matches!(host.connect(), Err(Failure::Transient(e)) if e.starts_with("503")));
    }

    #[test]
    fn jitter_is_between_half_and_all_of_the_wait() {
        for _ in 0..100 {
            let j = jitter(Duration::from_secs(30));
            assert!(
                j >= Duration::from_secs(15) && j <= Duration::from_secs(30),
                "{j:?}"
            );
        }
    }
}
