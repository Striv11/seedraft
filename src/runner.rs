//! Fault schedule generation and whole-run execution: one seed in, one verdict out.
//!
//! The schedule is drawn from its own RNG stream (derived from the seed), so it does not
//! depend on how the simulation consumes randomness. The same seed always yields the same
//! schedule, the same history and the same verdict.

use crate::kv::{KvHarness, Workload};
use crate::lin;
use crate::raft::{Bug, RaftConfig};
use crate::rng::Rng;
use crate::sim::{NetConfig, NodeId};
use std::fmt;

/// A fault injected at a virtual time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    Crash(NodeId),
    Restart(NodeId),
    Partition(Vec<Vec<NodeId>>),
    Heal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scheduled {
    pub at: u64,
    pub fault: Fault,
}

impl fmt::Display for Scheduled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.fault {
            Fault::Crash(n) => write!(f, "t={:<5} crash n{n}", self.at),
            Fault::Restart(n) => write!(f, "t={:<5} restart n{n}", self.at),
            Fault::Partition(g) => write!(f, "t={:<5} partition {g:?}", self.at),
            Fault::Heal => write!(f, "t={:<5} heal", self.at),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RunConfig {
    pub nodes: usize,
    /// Faults are injected in `0..fault_window`; afterwards everything is healed and drained.
    pub fault_window: u64,
    /// Extra time after the window for the cluster to settle and clients to finish.
    pub settle: u64,
    pub net: NetConfig,
    pub workload: Workload,
    /// Raft log compaction threshold (0 disables snapshots).
    pub snapshot_every: u64,
    /// Inject a known protocol bug (for validating the harness).
    pub bug: Option<Bug>,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            nodes: 5,
            fault_window: 3000,
            settle: 3000,
            net: NetConfig {
                drop_pct: 10,
                dup_pct: 5,
                reorder_pct: 5,
                ..NetConfig::default()
            },
            workload: Workload::default(),
            snapshot_every: 0,
            bug: None,
        }
    }
}

/// Generate a fault schedule. At most a minority of nodes is down at once, and all faults
/// are undone by `fault_window` (final restarts and a heal are part of the schedule).
pub fn schedule(seed: u64, cfg: &RunConfig) -> Vec<Scheduled> {
    let mut rng = Rng::new(seed ^ 0x5eed_fa17_5eed_fa17);
    let max_down = (cfg.nodes - 1) / 2;
    let mut down: Vec<NodeId> = Vec::new();
    let mut partitioned = false;
    let mut out = Vec::new();
    let mut t = rng.range(200, 500);
    while t < cfg.fault_window {
        match rng.range(0, 3) {
            0 if down.len() < max_down => {
                let up: Vec<NodeId> = (0..cfg.nodes).filter(|n| !down.contains(n)).collect();
                let n = up[rng.range(0, up.len() as u64 - 1) as usize];
                down.push(n);
                out.push(Scheduled {
                    at: t,
                    fault: Fault::Crash(n),
                });
            }
            1 if !down.is_empty() => {
                let n = down.remove(rng.range(0, down.len() as u64 - 1) as usize);
                out.push(Scheduled {
                    at: t,
                    fault: Fault::Restart(n),
                });
            }
            2 if !partitioned => {
                // Isolate a random non-empty strict subset on one side.
                let size = rng.range(1, (cfg.nodes as u64 - 1) / 2 + 1) as usize;
                let mut pool: Vec<NodeId> = (0..cfg.nodes).collect();
                let mut side = Vec::new();
                for _ in 0..size {
                    side.push(pool.remove(rng.range(0, pool.len() as u64 - 1) as usize));
                }
                side.sort_unstable();
                partitioned = true;
                out.push(Scheduled {
                    at: t,
                    fault: Fault::Partition(vec![side]),
                });
            }
            3 if partitioned => {
                partitioned = false;
                out.push(Scheduled {
                    at: t,
                    fault: Fault::Heal,
                });
            }
            _ => {}
        }
        t += rng.range(100, 400);
    }
    for n in down {
        out.push(Scheduled {
            at: cfg.fault_window,
            fault: Fault::Restart(n),
        });
    }
    if partitioned {
        out.push(Scheduled {
            at: cfg.fault_window,
            fault: Fault::Heal,
        });
    }
    out
}

/// What went wrong in a run, if anything.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Failure {
    /// Raft safety invariants violated.
    Invariant(Vec<String>),
    /// History is not linearizable on `key`.
    NotLinearizable { key: u8, ops: Vec<usize> },
    /// The implementation panicked (corrupt state reached an assertion or bad index).
    Panic(String),
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failure::Invariant(v) => {
                let more = v.len().saturating_sub(1);
                write!(f, "raft invariant violated: {}", v[0])?;
                if more > 0 {
                    write!(f, " (+{more} more)")?;
                }
                Ok(())
            }
            Failure::Panic(m) => write!(f, "panic: {m}"),
            Failure::NotLinearizable { key, ops } => {
                write!(f, "history not linearizable on key {key} (ops {ops:?})")
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct Outcome {
    pub seed: u64,
    pub schedule: Vec<Scheduled>,
    pub ops: usize,
    pub completed: usize,
    pub committed: usize,
    pub trace_hash: u64,
    /// Distinct terms that had a leader (a proxy for how much election churn the run saw).
    pub elections: usize,
    pub snapshots_installed: u64,
    pub failure: Option<Failure>,
}

/// How much of the fault space a set of runs exercised.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Coverage {
    pub seeds: u64,
    pub crashes: u64,
    pub restarts: u64,
    pub partitions: u64,
    pub heals: u64,
    /// Seeds where a node was down at the moment a partition started.
    pub crash_during_partition: u64,
    /// Seeds that saw two or more leader terms (i.e. at least one re-election).
    pub reelections: u64,
    pub snapshots_installed: u64,
    pub ops: u64,
    pub completed: u64,
}

impl Coverage {
    pub fn add(&mut self, o: &Outcome) {
        self.seeds += 1;
        let mut down = 0i64;
        for s in &o.schedule {
            match &s.fault {
                Fault::Crash(_) => {
                    self.crashes += 1;
                    down += 1;
                }
                Fault::Restart(_) => {
                    self.restarts += 1;
                    down -= 1;
                }
                Fault::Partition(_) => {
                    self.partitions += 1;
                    if down > 0 {
                        self.crash_during_partition += 1;
                    }
                }
                Fault::Heal => self.heals += 1,
            }
        }
        if o.elections >= 2 {
            self.reelections += 1;
        }
        self.snapshots_installed += o.snapshots_installed;
        self.ops += o.ops as u64;
        self.completed += o.completed as u64;
    }

    pub fn merge(&mut self, other: &Coverage) {
        self.seeds += other.seeds;
        self.crashes += other.crashes;
        self.restarts += other.restarts;
        self.partitions += other.partitions;
        self.heals += other.heals;
        self.crash_during_partition += other.crash_during_partition;
        self.reelections += other.reelections;
        self.snapshots_installed += other.snapshots_installed;
        self.ops += other.ops;
        self.completed += other.completed;
    }
}

/// Run seeds `lo..hi` on `jobs` threads. Every seed still runs in its own single-threaded
/// simulator, so each outcome is identical to what `run_seed` gives; results come back in
/// seed order regardless of `jobs`. The failing outcomes are returned in full, passing runs
/// are only folded into the coverage numbers.
pub fn sweep(lo: u64, hi: u64, cfg: &RunConfig, jobs: usize) -> (Coverage, Vec<Outcome>) {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;
    let next = AtomicU64::new(lo);
    let acc = Mutex::new((Coverage::default(), Vec::new()));
    std::thread::scope(|sc| {
        for _ in 0..jobs.max(1) {
            sc.spawn(|| {
                let mut cov = Coverage::default();
                let mut bad = Vec::new();
                loop {
                    let seed = next.fetch_add(1, Ordering::Relaxed);
                    if seed >= hi {
                        break;
                    }
                    let o = run_seed(seed, cfg);
                    cov.add(&o);
                    if !o.ok() {
                        bad.push(o);
                    }
                }
                let mut g = acc.lock().unwrap();
                g.0.merge(&cov);
                g.1.extend(bad);
            });
        }
    });
    let (cov, mut bad) = acc.into_inner().unwrap();
    bad.sort_by_key(|o| o.seed);
    (cov, bad)
}

impl Outcome {
    pub fn ok(&self) -> bool {
        self.failure.is_none()
    }
}

/// Run one seed end to end: workload + faults, then heal, drain, and check everything.
pub fn run_seed(seed: u64, cfg: &RunConfig) -> Outcome {
    run_schedule(seed, cfg, schedule(seed, cfg))
}

/// Like [`run_seed`] but with an explicit fault schedule (used when shrinking a failure).
/// The schedule must be time-ordered; faults that make no sense (restarting a node that is
/// up) are no-ops, so any subsequence of a valid schedule is runnable.
pub fn run_schedule(seed: u64, cfg: &RunConfig, sched: Vec<Scheduled>) -> Outcome {
    let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| execute(seed, cfg, &sched)));
    attempt.unwrap_or_else(|e| {
        let msg = e
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_else(|| "unknown".into());
        Outcome {
            seed,
            schedule: sched,
            ops: 0,
            completed: 0,
            committed: 0,
            trace_hash: 0,
            elections: 0,
            snapshots_installed: 0,
            failure: Some(Failure::Panic(msg)),
        }
    })
}

fn execute(seed: u64, cfg: &RunConfig, sched: &[Scheduled]) -> Outcome {
    let mut h = KvHarness::with_raft(
        seed,
        cfg.nodes,
        cfg.net.clone(),
        cfg.workload,
        RaftConfig {
            snapshot_every: cfg.snapshot_every,
            bug: cfg.bug,
            ..RaftConfig::default()
        },
    );
    h.cluster.sim.set_trace_recording(false);
    for s in sched {
        h.run_until(s.at);
        match &s.fault {
            Fault::Crash(n) => h.cluster.crash(*n),
            Fault::Restart(n) => h.cluster.restart(*n),
            Fault::Partition(g) => h.cluster.sim.partition(g),
            Fault::Heal => h.cluster.sim.heal(),
        }
    }
    h.run_until(cfg.fault_window);
    h.quiesce();
    h.run_until(cfg.fault_window + cfg.settle);

    let ops = h.history.ops();
    let completed = ops.iter().filter(|o| o.ret.is_some()).count();
    let failure = if !h.cluster.violations().is_empty() {
        Some(Failure::Invariant(h.cluster.violations().to_vec()))
    } else {
        lin::check(ops).err().map(|v| Failure::NotLinearizable {
            key: v.key,
            ops: v.ops,
        })
    };
    Outcome {
        seed,
        schedule: sched.to_vec(),
        ops: ops.len(),
        completed,
        committed: h.cluster.committed().len(),
        trace_hash: h.cluster.sim.trace_hash(),
        elections: h.cluster.leaders_by_term().len(),
        snapshots_installed: h.cluster.snapshots_installed(),
        failure,
    }
}
