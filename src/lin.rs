//! Linearizability checker for KV histories (Wing & Gong / Lowe "WGL" style).
//!
//! Operations on different keys are independent, so the history is split per key and each
//! part is searched separately. Within a key we do a depth-first search for a total order
//! that (a) respects real time (if `a` returned before `b` was invoked, `a` comes first) and
//! (b) replays correctly against the sequential register model. Visited
//! `(linearized set, register value)` pairs are memoised, which is what makes the search
//! practical (the "L" in WGL).
//!
//! Indeterminate ops (`ret == None`, the client never saw a reply) may take effect at any
//! point after their invocation, or never; whatever they would have returned is unconstrained.

use std::collections::{BTreeMap, HashSet};

use crate::kv::{HistOp, Op, Reply};

/// A key whose sub-history admits no linearization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub key: u8,
    /// Ids (`HistOp::id`) of the ops on that key, in invocation order.
    pub ops: Vec<usize>,
}

/// `Ok(())` if the whole history is linearizable, else the first offending key.
pub fn check(history: &[HistOp]) -> Result<(), Violation> {
    let mut by_key: BTreeMap<u8, Vec<&HistOp>> = BTreeMap::new();
    for o in history {
        by_key.entry(o.op.key()).or_default().push(o);
    }
    for (key, mut ops) in by_key {
        ops.sort_by_key(|o| (o.invoke, o.id));
        if !check_register(&ops) {
            return Err(Violation {
                key,
                ops: ops.iter().map(|o| o.id).collect(),
            });
        }
    }
    Ok(())
}

/// Apply `op` to the register; returns the new value and the reply.
fn step(state: Option<u16>, op: Op) -> (Option<u16>, Reply) {
    match op {
        Op::Put { val, .. } => (Some(val), Reply::Ok),
        Op::Get { .. } => (state, Reply::Value(state)),
        Op::Cas { expect, new, .. } => {
            if state == Some(expect) {
                (Some(new), Reply::Cas(true))
            } else {
                (state, Reply::Cas(false))
            }
        }
    }
}

struct Search<'a> {
    ops: &'a [&'a HistOp],
    done: Vec<u64>,
    /// Count of completed ops not yet linearized; pending ones can be dropped.
    remaining_completed: usize,
    failed: HashSet<(Vec<u64>, Option<u16>)>,
}

impl Search<'_> {
    fn is_done(&self, i: usize) -> bool {
        self.done[i / 64] >> (i % 64) & 1 == 1
    }

    fn flip(&mut self, i: usize) {
        self.done[i / 64] ^= 1 << (i % 64);
    }

    fn run(&mut self, state: Option<u16>) -> bool {
        if self.remaining_completed == 0 {
            return true;
        }
        let memo = (self.done.clone(), state);
        if self.failed.contains(&memo) {
            return false;
        }
        // An op can go next only if no other unlinearized op returned before it was invoked.
        let horizon = (0..self.ops.len())
            .filter(|&i| !self.is_done(i))
            .filter_map(|i| self.ops[i].ret.map(|(t, _)| t))
            .min()
            .unwrap_or(u64::MAX);
        for i in 0..self.ops.len() {
            if self.is_done(i) {
                continue;
            }
            let o = self.ops[i];
            if o.invoke > horizon {
                break; // sorted by invoke time: nothing later qualifies either
            }
            let (next, reply) = step(state, o.op);
            if let Some((_, want)) = o.ret {
                if want != reply {
                    continue;
                }
                self.remaining_completed -= 1;
            }
            self.flip(i);
            let ok = self.run(next);
            self.flip(i);
            if o.ret.is_some() {
                self.remaining_completed += 1;
            }
            if ok {
                return true;
            }
        }
        self.failed.insert(memo);
        false
    }
}

fn check_register(ops: &[&HistOp]) -> bool {
    let mut s = Search {
        ops,
        done: vec![0; ops.len().div_ceil(64)],
        remaining_completed: ops.iter().filter(|o| o.ret.is_some()).count(),
        failed: HashSet::new(),
    };
    s.run(None)
}

/// A bounded-staleness read that returned a value which had certainly been overwritten for
/// longer than the bound.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StaleViolation {
    pub key: u8,
    /// `HistOp::id` of the offending read in the stale-read recorder.
    pub read: usize,
}

/// Check bounded-staleness reads against the linearizable `history`.
///
/// A stale `Get` invoked at `t` may return any value the register held at some instant in
/// `[t - bound, t]`. Without knowing the linearization we apply a sound (never falsely
/// accusing) necessary condition: some write of the returned value, invoked by `t`, must not
/// be *certainly* overwritten before `t - bound`. A write `w` is certainly overwritten by a
/// definite write `w2` when `w2` was invoked after `w` returned; `w2` is then in effect by the
/// time it returns. The initial (absent) value counts as overwritten by any definite write.
/// Definite writes are Puts and Cas ops that reported success; any Put, or Cas that did not
/// report failure, can be the source of a value.
pub fn check_bounded_staleness(
    history: &[HistOp],
    stale: &[HistOp],
    bound: u64,
) -> Result<(), StaleViolation> {
    for r in stale {
        let Some((t, Reply::Value(v))) = r.ret else {
            continue;
        };
        let key = r.op.key();
        let on_key: Vec<&HistOp> = history.iter().filter(|o| o.op.key() == key).collect();
        let definite = |o: &HistOp| {
            matches!(
                (o.op, o.ret),
                (Op::Put { .. }, Some(_)) | (Op::Cas { .. }, Some((_, Reply::Cas(true))))
            )
        };
        // Has some definite write invoked after `after` landed before `t - bound`?
        let overwritten = |after: Option<u64>| {
            on_key.iter().any(|w2| {
                definite(w2)
                    && after.is_none_or(|a| w2.invoke > a)
                    && w2.ret.is_some_and(|(rt, _)| rt + bound < t)
            })
        };
        let live = match v {
            None => !overwritten(None),
            Some(val) => on_key.iter().any(|w| {
                let writes_v = match (w.op, w.ret) {
                    (_, Some((_, Reply::Cas(false)))) => false,
                    (Op::Put { val: x, .. }, _) => x == val,
                    (Op::Cas { new, .. }, _) => new == val,
                    _ => false,
                };
                // A write still pending can never be certainly overwritten.
                let done = Some(w.ret.map_or(u64::MAX, |(rt, _)| rt));
                writes_v && w.invoke <= t && !overwritten(done)
            }),
        };
        if !live {
            return Err(StaleViolation { key, read: r.id });
        }
    }
    Ok(())
}
