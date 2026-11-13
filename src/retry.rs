//! What runs a run, and how jobber-host keeps trying through jobber being away.

use std::thread;
use std::time::{Duration, Instant};

use crate::budget::{JobReservation, Shared};
use crate::log;
use crate::service::{Assignment, Failure};

/// Runs one run to its end and reports it. Holds `reservation` for
/// as long as the job does; dropping it gives its CPU and memory back.
pub trait Executor: Send + Sync + 'static {
    fn run(&self, run: Assignment, reservation: JobReservation, budget: &Shared);
}

/// Doubling waits between failed tries, so a service that is down is not
/// hammered and one that is back is found within a minute.
pub struct Backoff {
    next: Duration,
    most: Duration,
}

impl Backoff {
    const FIRST: Duration = Duration::from_secs(1);
    const MOST: Duration = Duration::from_secs(60);

    pub fn new() -> Backoff {
        Backoff::upto(Self::MOST)
    }

    pub fn upto(most: Duration) -> Backoff {
        Backoff {
            next: Self::FIRST,
            most,
        }
    }

    pub fn next(&mut self) -> Duration {
        let wait = self.next;
        self.next = (self.next * 2).min(self.most);
        wait
    }

    pub fn reset(&mut self) {
        self.next = Self::FIRST;
    }
}

/// Sleeps for `d`, or less if jobber-host is told to stop.
pub fn sleep(d: Duration, stop: &dyn Fn() -> bool) {
    let until = Instant::now() + d;
    while !stop() {
        let left = until.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return;
        }
        thread::sleep(left.min(Duration::from_millis(250)));
    }
}

/// Tries `f` until it answers or jobber-host is told to stop, logging each
/// failure and the wait before the next try. A refusal is retried too, more
/// slowly, since it may be fixed without restarting jobber-host (a token
/// file rewritten, jobber redeployed).
pub fn retrying<T>(
    what: &str,
    stop: &dyn Fn() -> bool,
    mut f: impl FnMut() -> Result<T, Failure>,
) -> Option<T> {
    let mut backoff = Backoff::new();
    while !stop() {
        match f() {
            Ok(t) => return Some(t),
            Err(e) => {
                let mut wait = backoff.next();
                if matches!(e, Failure::Refused(..)) {
                    wait = Backoff::MOST;
                }
                log!("{what} failed: {e}; trying again in {}s", wait.as_secs());
                sleep(wait, stop);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_to_a_minute_and_starts_again_after_a_success() {
        let mut b = Backoff::new();
        let waits: Vec<u64> = (0..8).map(|_| b.next().as_secs()).collect();
        assert_eq!(waits, [1, 2, 4, 8, 16, 32, 60, 60]);
        b.reset();
        assert_eq!(b.next().as_secs(), 1);
    }

    #[test]
    fn retrying_answers_once_a_try_succeeds() {
        let mut tries = 0;
        let got = retrying("t", &|| false, || {
            tries += 1;
            if tries < 2 {
                Err(Failure::Transient("down".into()))
            } else {
                Ok(tries)
            }
        });
        assert_eq!(got, Some(2));
        assert_eq!(
            retrying("t", &|| true, || Ok(1)),
            None,
            "a stopping host tries nothing"
        );
    }
}
