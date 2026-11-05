//! Trace minimization: shrink a failing run's fault schedule with delta debugging (ddmin).
//!
//! The simulation is deterministic given (seed, schedule), so "does this smaller schedule still
//! fail the same way?" is a pure function we can query as often as we like. We keep the seed
//! (and therefore the workload and network randomness) fixed and search for a 1-minimal
//! subsequence of faults: removing any single remaining fault makes the failure go away.

use crate::runner::{run_schedule, Failure, Outcome, RunConfig, Scheduled};

/// Coarse failure class: shrinking must not drift from, say, a safety violation to a
/// linearizability failure, or the reproducer would explain a different bug.
fn class(f: &Failure) -> u8 {
    match f {
        Failure::Invariant(_) => 0,
        Failure::NotLinearizable { .. } => 1,
        Failure::Panic(_) => 2,
    }
}

#[derive(Debug)]
pub struct Shrunk {
    pub original_faults: usize,
    pub schedule: Vec<Scheduled>,
    /// Number of simulation runs spent shrinking.
    pub runs: usize,
    pub outcome: Outcome,
}

/// Minimize `failing`'s schedule. Returns `None` if the outcome did not fail (or does not
/// reproduce from its own schedule, which would indicate nondeterminism).
pub fn shrink(failing: &Outcome, cfg: &RunConfig) -> Option<Shrunk> {
    let want = class(failing.failure.as_ref()?);
    let seed = failing.seed;
    let mut runs = 0;
    let mut fails = |s: &[Scheduled]| -> bool {
        runs += 1;
        let o = run_schedule(seed, cfg, s.to_vec());
        o.failure.as_ref().is_some_and(|f| class(f) == want)
    };
    let mut cur = failing.schedule.clone();
    if !fails(&cur) {
        return None;
    }
    // ddmin: try dropping chunks of size len/n, refining the granularity when stuck.
    let mut n = 2;
    while cur.len() >= 2 {
        let chunk = cur.len().div_ceil(n);
        let mut reduced = false;
        let mut start = 0;
        while start < cur.len() {
            let end = (start + chunk).min(cur.len());
            let cand: Vec<Scheduled> = cur[..start].iter().chain(&cur[end..]).cloned().collect();
            if fails(&cand) {
                cur = cand;
                n = (n - 1).max(2);
                reduced = true;
                break;
            }
            start = end;
        }
        if !reduced {
            if chunk == 1 {
                break;
            }
            n = (n * 2).min(cur.len());
        }
    }
    // The loop above stops at single-element granularity only when len >= 2; check the
    // last remaining fault (and the empty schedule) explicitly.
    if cur.len() == 1 && fails(&[]) {
        cur.clear();
    }
    let outcome = if cur == failing.schedule {
        failing.clone()
    } else {
        // Re-run for the final outcome so callers see the exact failure of the minimal schedule.
        runs += 1;
        run_schedule(seed, cfg, cur.clone())
    };
    Some(Shrunk {
        original_faults: failing.schedule.len(),
        schedule: cur,
        runs,
        outcome,
    })
}
