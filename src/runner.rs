//! Fault schedule generation and whole-run execution: one seed in, one verdict out.
//!
//! The schedule is drawn from its own RNG stream (derived from the seed), so it does not
//! depend on how the simulation consumes randomness. The same seed always yields the same
//! schedule, the same history and the same verdict.

use crate::kv::{KvHarness, Workload};
use crate::lin;
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
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failure::Invariant(v) => write!(f, "raft invariant violated: {}", v.join("; ")),
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
    pub failure: Option<Failure>,
}

impl Outcome {
    pub fn ok(&self) -> bool {
        self.failure.is_none()
    }
}

/// Run one seed end to end: workload + faults, then heal, drain, and check everything.
pub fn run_seed(seed: u64, cfg: &RunConfig) -> Outcome {
    let sched = schedule(seed, cfg);
    let mut h = KvHarness::new(seed, cfg.nodes, cfg.net.clone(), cfg.workload);
    for s in &sched {
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
        schedule: sched,
        ops: ops.len(),
        completed,
        committed: h.cluster.committed().len(),
        trace_hash: h.cluster.sim.trace_hash(),
        failure,
    }
}
