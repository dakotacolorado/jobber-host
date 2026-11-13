//! Admission, as Kubernetes admits pods to a node's allocatable: a job is
//! taken only while its CPU and memory requests fit what running jobs and
//! image builds have not reserved. There is no count of jobs.

use serde::{Serialize, Serializer};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use crate::config::MIN_JOB_MEMORY;

/// CPUs, kept in thousandths so requests add and subtract exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Cpu(u64);

impl Cpu {
    /// `cpus` to the nearest thousandth; a negative or unreadable figure is none.
    pub fn of(cpus: f64) -> Cpu {
        Cpu(if cpus.is_finite() && cpus > 0.0 {
            (cpus * 1000.0).round() as u64
        } else {
            0
        })
    }

    pub fn cpus(self) -> f64 {
        self.0 as f64 / 1000.0
    }

    pub fn saturating_sub(self, other: Cpu) -> Cpu {
        Cpu(self.0.saturating_sub(other.0))
    }
}

impl std::fmt::Display for Cpu {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.cpus())
    }
}

impl Serialize for Cpu {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_f64(self.cpus())
    }
}

/// CPUs and MiB: what a host offers, what it has free, or what a job requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Resources {
    pub cpu: Cpu,
    pub memory: u64,
}

/// The pure arithmetic: no locks, no clock, every rule tested.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Budget {
    capacity: Resources,
    build_memory: u64,
    /// MiB each job's container gets beyond its admitted memory, and reserves
    /// with it: a host whose jobs need more than elsewhere for the same work
    /// (an arm64 mac running amd64 node through Rosetta).
    overhead: u64,
    /// CPUs the jobs running hold.
    cpu: Cpu,
    /// MiB the jobs running and the builds hold.
    reserved: u64,
    /// Builds waiting for memory. While one waits, nothing more is taken,
    /// so the memory it waits for is freed by jobs ending rather than taken
    /// by new ones.
    waiting: u32,
}

impl Budget {
    pub fn new(capacity: Resources, build_memory: u64) -> Budget {
        Budget {
            capacity,
            build_memory,
            overhead: 0,
            cpu: Cpu::default(),
            reserved: 0,
            waiting: 0,
        }
    }

    pub fn with_overhead(self, overhead: u64) -> Budget {
        Budget { overhead, ..self }
    }

    /// Unreserved CPU, and unreserved memory less one job's overhead, so a
    /// run jobber admits within it fits with its overhead.
    pub fn free(&self) -> Resources {
        Resources {
            cpu: self.capacity.cpu.saturating_sub(self.cpu),
            memory: self
                .capacity
                .memory
                .saturating_sub(self.reserved)
                .saturating_sub(self.overhead),
        }
    }

    /// Takes what `request` asks for, or refuses and changes nothing.
    pub fn reserve_job(&mut self, request: Resources) -> Result<(), String> {
        if self.waiting > 0 {
            return Err(
                "an image build is waiting for memory: nothing more is taken until it has it"
                    .into(),
            );
        }
        let free = self.free();
        if free.memory < MIN_JOB_MEMORY {
            return Err(format!(
                "{} MiB free is less than any run ({MIN_JOB_MEMORY} MiB)",
                free.memory
            ));
        }
        if request.cpu > free.cpu {
            return Err(format!(
                "{} CPU does not fit the {} CPU free of {}",
                request.cpu, free.cpu, self.capacity.cpu
            ));
        }
        if request.memory > free.memory {
            return Err(format!(
                "{} MiB does not fit the {} MiB free",
                request.memory, free.memory
            ));
        }
        self.cpu = Cpu(self.cpu.0 + request.cpu.0);
        self.reserved += request.memory + self.overhead;
        Ok(())
    }

    pub fn release_job(&mut self, request: Resources) {
        self.cpu = self.cpu.saturating_sub(request.cpu);
        self.reserved = self.reserved.saturating_sub(request.memory + self.overhead);
    }

    /// MiB a build for a job holding `job_memory` reserves beyond it. The job
    /// is not running while its image builds, so the build uses the job's own
    /// reservation first: this keeps a build from waiting on memory its own
    /// job holds, which would never come free.
    pub fn build_extra(&self, job_memory: u64) -> u64 {
        self.build_memory.saturating_sub(job_memory + self.overhead)
    }

    /// Reserves a build for a job holding `job_memory`, answering the MiB
    /// reserved, or None when it does not fit yet.
    pub fn reserve_build(&mut self, job_memory: u64) -> Option<u64> {
        let extra = self.build_extra(job_memory);
        if extra > self.capacity.memory.saturating_sub(self.reserved) {
            return None;
        }
        self.reserved += extra;
        Some(extra)
    }

    pub fn release_build(&mut self, extra: u64) {
        self.reserved = self.reserved.saturating_sub(extra);
    }
}

/// The budget shared by the connection and each run's thread. Reservations
/// are guards, so a job that ends any way, a panic included, gives its
/// memory back.
#[derive(Clone)]
pub struct Shared(Arc<(Mutex<Budget>, Condvar)>);

impl Shared {
    pub fn new(budget: Budget) -> Shared {
        Shared(Arc::new((Mutex::new(budget), Condvar::new())))
    }

    fn lock(&self) -> MutexGuard<'_, Budget> {
        self.0 .0.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[cfg(test)]
    pub fn snapshot(&self) -> Budget {
        self.lock().clone()
    }

    pub fn free(&self) -> Resources {
        self.lock().free()
    }

    pub fn reserve_job(&self, request: Resources) -> Result<JobReservation, String> {
        self.lock().reserve_job(request)?;
        Ok(JobReservation {
            budget: self.clone(),
            memory: request.memory,
            cpu: request.cpu,
        })
    }

    /// Waits until the build fits, checking `give_up` about once a second
    /// (a cancel, a lapsed lease, jobber-host stopping); None if it gave up.
    pub fn reserve_build(
        &self,
        job: &JobReservation,
        give_up: impl Fn() -> bool,
    ) -> Option<BuildReservation> {
        let mut budget = self.lock();
        budget.waiting += 1;
        let reserved = loop {
            if let Some(extra) = budget.reserve_build(job.memory) {
                break Some(extra);
            }
            if give_up() {
                break None;
            }
            budget = self
                .0
                 .1
                .wait_timeout(budget, Duration::from_secs(1))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        };
        budget.waiting -= 1;
        drop(budget);
        self.0 .1.notify_all();
        reserved.map(|extra| BuildReservation {
            budget: self.clone(),
            extra,
        })
    }
}

pub struct JobReservation {
    budget: Shared,
    pub memory: u64,
    pub cpu: Cpu,
}

impl Drop for JobReservation {
    fn drop(&mut self) {
        self.budget.lock().release_job(Resources {
            cpu: self.cpu,
            memory: self.memory,
        });
        self.budget.0 .1.notify_all();
    }
}

pub struct BuildReservation {
    budget: Shared,
    pub extra: u64,
}

impl Drop for BuildReservation {
    fn drop(&mut self) {
        self.budget.lock().release_build(self.extra);
        self.budget.0 .1.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn of(cpu: f64, memory: u64) -> Resources {
        Resources {
            cpu: Cpu::of(cpu),
            memory,
        }
    }

    #[test]
    fn cpus_are_kept_to_the_thousandth_and_serialized_as_a_number() {
        assert_eq!(Cpu::of(0.1).0 + Cpu::of(0.2).0, Cpu::of(0.3).0);
        assert_eq!(Cpu::of(-1.0), Cpu::default());
        assert_eq!(Cpu::of(f64::NAN), Cpu::default());
        assert_eq!(
            serde_json::to_value(of(13.5, 9216)).unwrap(),
            serde_json::json!({"cpu": 13.5, "memory": 9216})
        );
    }

    #[test]
    fn each_job_reserves_its_overhead_and_free_leaves_room_for_the_next_ones() {
        let mut b = Budget::new(of(4.0, 5120), 1024).with_overhead(512);
        assert_eq!(b.free().memory, 4608);
        b.reserve_job(of(0.5, 1024)).unwrap();
        assert_eq!(
            b.free().memory,
            3072,
            "1024 + 512 reserved, and 512 for the next"
        );
        b.reserve_job(of(0.5, 1024)).unwrap();
        b.reserve_job(of(0.5, 1024)).unwrap();
        assert_eq!(b.free(), of(2.5, 0));
        assert!(b
            .reserve_job(of(0.5, 16))
            .unwrap_err()
            .contains("less than any run"));
        b.release_job(of(0.5, 1024));
        assert_eq!(b.free(), of(3.0, 1536));
        assert_eq!(
            b.build_extra(256),
            256,
            "a build uses its job's 256 + 512 first"
        );
    }

    #[test]
    fn jobs_are_admitted_while_their_cpu_and_memory_requests_fit_with_no_count_of_them() {
        let mut b = Budget::new(of(2.0, 8192), 512);
        for _ in 0..8 {
            b.reserve_job(of(0.25, 256)).unwrap();
        }
        assert_eq!(b.free(), of(0.0, 6144));
        let err = b.reserve_job(of(0.001, 16)).unwrap_err();
        assert!(
            err.contains("CPU does not fit the 0 CPU free of 2"),
            "{err}"
        );
        assert_eq!(b.free(), of(0.0, 6144), "a refusal changes nothing");
        b.release_job(of(0.25, 256));
        assert!(b.clone().reserve_job(of(0.25, 256)).is_ok());
        assert!(b.reserve_job(of(0.3, 256)).is_err());
    }

    #[test]
    fn a_job_whose_memory_does_not_fit_is_declined_with_cpu_to_spare() {
        let mut b = Budget::new(of(16.0, 1024), 512);
        b.reserve_job(of(1.0, 1010)).unwrap();
        assert_eq!(b.free(), of(15.0, 14));
        assert!(b
            .reserve_job(of(1.0, 16))
            .unwrap_err()
            .contains("14 MiB free is less than any run"));
        b.release_job(of(1.0, 1010));
        assert_eq!(b.free(), of(16.0, 1024));
    }

    #[test]
    fn a_build_counts_against_the_same_memory() {
        let mut b = Budget::new(of(4.0, 2048), 1024);
        b.reserve_job(of(1.0, 256)).unwrap();
        assert_eq!(b.reserve_build(256), Some(768), "beyond the job's own 256");
        assert_eq!(b.free(), of(3.0, 1024));
        b.reserve_job(of(1.0, 1024)).unwrap();
        assert!(
            b.reserve_job(of(1.0, 16)).is_err(),
            "the build holds the rest"
        );
        b.release_build(768);
        assert_eq!(b.free().memory, 768);
    }

    #[test]
    fn a_job_at_least_as_big_as_a_build_builds_inside_its_own_reservation() {
        let mut b = Budget::new(of(1.0, 1024), 512);
        b.reserve_job(of(1.0, 1024)).unwrap();
        assert_eq!(b.reserve_build(1024), Some(0));
        assert_eq!(b.free().memory, 0);
    }

    #[test]
    fn a_build_that_does_not_fit_waits_and_nothing_is_taken_meanwhile() {
        let shared = Shared::new(Budget::new(of(4.0, 2048), 1024));
        let small = shared.reserve_job(of(1.0, 128)).unwrap();
        let big = shared.reserve_job(of(1.0, 1500)).unwrap();
        assert_eq!(shared.free().memory, 420);
        let waiter = {
            let shared = shared.clone();
            std::thread::spawn(move || shared.reserve_build(&small, || false).map(|b| b.extra))
        };
        while shared.snapshot().waiting == 0 {
            std::thread::yield_now();
        }
        let held = shared.reserve_job(of(1.0, 16)).map(drop).unwrap_err();
        assert!(held.contains("build is waiting"), "{held}");
        drop(big);
        assert_eq!(waiter.join().unwrap(), Some(896));
        drop(shared.reserve_job(of(1.0, 16)).unwrap());
        assert_eq!(shared.free(), of(4.0, 2048), "both guards gave back");
    }

    #[test]
    fn a_build_that_gives_up_reserves_nothing() {
        let shared = Shared::new(Budget::new(of(2.0, 1024), 1024));
        let a = shared.reserve_job(of(1.0, 16)).unwrap();
        let _b = shared.reserve_job(of(1.0, 1000)).unwrap();
        assert!(shared.reserve_build(&a, || true).is_none());
        assert!(shared.snapshot().waiting == 0 && shared.free().memory == 8);
    }
}
