//! ASCII timeline: one lane per node, one column per time bucket, plus a fault row.
//!
//! Lane glyphs: `L` leader, `c` candidate, `.` follower, `X` node down. The fault row marks
//! `x` crash, `r` restart, `P` partition, `H` heal ; `*` when one column holds several different faults.
//! The `term` row shows the highest term (mod 10) among up nodes. The render replays the seed
//! in column-sized steps, so it is deterministic: same inputs, same text.

use crate::kv::KvHarness;
use crate::raft::{RaftConfig, Role};
use crate::runner::{Fault, RunConfig, Scheduled};

/// Render the run of `seed` under `sched` as text, using `cols` columns over the fault window
/// plus settle time.
pub fn render(seed: u64, cfg: &RunConfig, sched: &[Scheduled], cols: usize) -> String {
    let cols = cols.max(1);
    let total = cfg.fault_window + cfg.settle;
    let bucket = total.div_ceil(cols as u64).max(1);
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
    let mut lanes = vec![String::new(); cfg.nodes];
    let mut faults = String::new();
    let mut terms = String::new();
    let mut next = 0;
    for c in 0..cols as u64 {
        let end = (c + 1) * bucket;
        let mut mark = ' ';
        while next < sched.len() && sched[next].at <= end {
            let s = &sched[next];
            h.run_until(s.at);
            let m = match &s.fault {
                Fault::Crash(n) => {
                    h.cluster.crash(*n);
                    'x'
                }
                Fault::Restart(n) => {
                    h.cluster.restart(*n);
                    'r'
                }
                Fault::Partition(g) => {
                    h.cluster.sim.partition(g);
                    'P'
                }
                Fault::Heal => {
                    h.cluster.sim.heal();
                    'H'
                }
                Fault::AddLearner(n) => {
                    h.cluster.add_learner(*n);
                    'm'
                }
                Fault::RemoveLearner(n) => {
                    h.cluster.remove_learner(*n);
                    'm'
                }
                Fault::SetVoters(v) => {
                    h.cluster.change_membership(v);
                    'm'
                }
            };
            mark = if mark == ' ' || mark == m { m } else { '*' };
            next += 1;
        }
        // Mirror the real run: quiesce clients once the fault window is over.
        if end >= cfg.fault_window && h.cluster.sim.now() < cfg.fault_window {
            h.run_until(cfg.fault_window);
            h.quiesce();
        }
        h.run_until(end);
        let mut max_term = 0;
        for (n, lane) in lanes.iter_mut().enumerate() {
            if !h.cluster.sim.is_up(n) {
                lane.push('X');
                continue;
            }
            let node = h.cluster.node(n);
            max_term = max_term.max(node.term());
            lane.push(match node.role() {
                Role::Leader => 'L',
                Role::Candidate => 'c',
                Role::Follower => '.',
            });
        }
        faults.push(mark);
        terms.push(char::from_digit((max_term % 10) as u32, 10).unwrap());
    }
    let mut out = format!(
        "seed {seed}, {bucket} ticks/column, t=0..{}\n",
        bucket * cols as u64
    );
    out.push_str(&format!("{:<6}{faults}\n", "fault"));
    for (n, lane) in lanes.iter().enumerate() {
        out.push_str(&format!("{:<6}{lane}\n", format!("n{n}")));
    }
    out.push_str(&format!("{:<6}{terms}\n", "term"));
    out.push_str("L leader  c candidate  . follower  X down | x crash  r restart  P partition  H heal\n");
    out
}
