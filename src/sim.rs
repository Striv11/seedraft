//! Single-threaded simulator core: virtual clock, event queue, simulated network.
//!
//! The simulator owns time. Nothing here reads the wall clock or spawns threads; the only
//! source of randomness is the seeded [`Rng`]. Events are ordered by `(time, sequence)`, so
//! ties are broken deterministically in insertion order.

use crate::disk::{Disk, DiskConfig};
use crate::rng::Rng;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt::Debug;

pub type NodeId = usize;

/// Network fault model. Delays are in virtual ticks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetConfig {
    pub min_delay: u64,
    pub max_delay: u64,
    pub drop_pct: u8,
    pub dup_pct: u8,
    /// Chance a message gets extra delay, which reorders it behind later messages.
    pub reorder_pct: u8,
    pub reorder_extra: u64,
}

impl Default for NetConfig {
    fn default() -> Self {
        NetConfig {
            min_delay: 1,
            max_delay: 10,
            drop_pct: 0,
            dup_pct: 0,
            reorder_pct: 0,
            reorder_extra: 50,
        }
    }
}

/// Something the simulator hands back to the driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event<M> {
    Deliver { from: NodeId, to: NodeId, msg: M },
    Timer { node: NodeId, id: u64 },
}

/// One line of the replayable trace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceEntry {
    Send {
        at: u64,
        from: NodeId,
        to: NodeId,
        msg: String,
    },
    Drop {
        at: u64,
        from: NodeId,
        to: NodeId,
        why: &'static str,
    },
    Dup {
        at: u64,
        from: NodeId,
        to: NodeId,
    },
    Deliver {
        at: u64,
        from: NodeId,
        to: NodeId,
        msg: String,
    },
    Timer {
        at: u64,
        node: NodeId,
        id: u64,
    },
    Partition {
        at: u64,
        groups: Vec<Vec<NodeId>>,
    },
    Heal {
        at: u64,
    },
    Crash {
        at: u64,
        node: NodeId,
        /// Unsynced disk bytes that survived (torn write).
        torn_bytes: usize,
    },
    Restart {
        at: u64,
        node: NodeId,
    },
    Fsync {
        at: u64,
        node: NodeId,
        ok: bool,
    },
}

struct Queued<M> {
    at: u64,
    seq: u64,
    /// For timers: the node's incarnation when it was set; stale timers die on crash.
    epoch: u64,
    ev: Event<M>,
}

impl<M> PartialEq for Queued<M> {
    fn eq(&self, o: &Self) -> bool {
        (self.at, self.seq) == (o.at, o.seq)
    }
}
impl<M> Eq for Queued<M> {}
impl<M> PartialOrd for Queued<M> {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl<M> Ord for Queued<M> {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        (self.at, self.seq).cmp(&(o.at, o.seq))
    }
}

pub struct Sim<M> {
    now: u64,
    seq: u64,
    queue: BinaryHeap<Reverse<Queued<M>>>,
    rng: Rng,
    net: NetConfig,
    /// Partition group of each node; nodes in different groups cannot talk.
    group: Vec<usize>,
    up: Vec<bool>,
    epoch: Vec<u64>,
    disks: Vec<Disk>,
    disk_cfg: DiskConfig,
    trace: Vec<TraceEntry>,
}

impl<M: Clone + Debug> Sim<M> {
    pub fn new(seed: u64, nodes: usize, net: NetConfig) -> Self {
        assert!(net.min_delay <= net.max_delay, "min_delay > max_delay");
        Sim {
            now: 0,
            seq: 0,
            queue: BinaryHeap::new(),
            rng: Rng::new(seed),
            net,
            group: vec![0; nodes],
            up: vec![true; nodes],
            epoch: vec![0; nodes],
            disks: vec![Disk::new(); nodes],
            disk_cfg: DiskConfig::default(),
            trace: Vec::new(),
        }
    }

    pub fn now(&self) -> u64 {
        self.now
    }

    /// Time of the next queued event, if any (it may still turn out to be discarded).
    pub fn peek_time(&self) -> Option<u64> {
        self.queue.peek().map(|Reverse(q)| q.at)
    }

    pub fn nodes(&self) -> usize {
        self.group.len()
    }

    pub fn trace(&self) -> &[TraceEntry] {
        &self.trace
    }

    pub fn pending(&self) -> usize {
        self.queue.len()
    }

    /// Access to the simulation RNG for drivers (workloads, fault schedules).
    pub fn rng(&mut self) -> &mut Rng {
        &mut self.rng
    }

    /// FNV-1a hash of the trace: a cheap fingerprint to compare runs.
    pub fn trace_hash(&self) -> u64 {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in format!("{:?}", self.trace).bytes() {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        h
    }

    fn push(&mut self, at: u64, ev: Event<M>) {
        self.seq += 1;
        let epoch = match &ev {
            Event::Timer { node, .. } => self.epoch[*node],
            Event::Deliver { .. } => 0,
        };
        self.queue.push(Reverse(Queued {
            at,
            seq: self.seq,
            epoch,
            ev,
        }));
    }

    pub fn set_disk_config(&mut self, cfg: DiskConfig) {
        self.disk_cfg = cfg;
    }

    pub fn is_up(&self, node: NodeId) -> bool {
        self.up[node]
    }

    /// Direct access to a node's disk (only meaningful while the node is up).
    pub fn disk(&mut self, node: NodeId) -> &mut Disk {
        &mut self.disks[node]
    }

    /// Fsync a node's disk through the fault model; the outcome is traced.
    pub fn fsync(&mut self, node: NodeId) -> bool {
        let ok = self.disks[node].fsync(&self.disk_cfg, &mut self.rng);
        self.trace.push(TraceEntry::Fsync {
            at: self.now,
            node,
            ok,
        });
        ok
    }

    /// Crash a node: it stops receiving messages and timers, its pending timers die, and its
    /// disk loses (or tears) unsynced writes. No-op if already down.
    pub fn crash(&mut self, node: NodeId) {
        if !self.up[node] {
            return;
        }
        self.up[node] = false;
        self.epoch[node] += 1;
        let torn_bytes = self.disks[node].crash(&self.disk_cfg, &mut self.rng);
        self.trace.push(TraceEntry::Crash {
            at: self.now,
            node,
            torn_bytes,
        });
    }

    /// Bring a crashed node back. Volatile state is the driver's job to rebuild from the disk.
    pub fn restart(&mut self, node: NodeId) {
        if self.up[node] {
            return;
        }
        self.up[node] = true;
        self.trace.push(TraceEntry::Restart { at: self.now, node });
    }

    /// Schedule a timer `after` ticks from now.
    pub fn set_timer(&mut self, node: NodeId, id: u64, after: u64) {
        self.push(self.now + after, Event::Timer { node, id });
    }

    fn connected(&self, a: NodeId, b: NodeId) -> bool {
        self.group[a] == self.group[b]
    }

    /// Split the cluster into the given groups. Nodes not listed form one extra group.
    pub fn partition(&mut self, groups: &[Vec<NodeId>]) {
        let rest = groups.len() + 1;
        self.group.iter_mut().for_each(|g| *g = rest);
        for (i, grp) in groups.iter().enumerate() {
            for &n in grp {
                self.group[n] = i + 1;
            }
        }
        self.trace.push(TraceEntry::Partition {
            at: self.now,
            groups: groups.to_vec(),
        });
    }

    pub fn heal(&mut self) {
        self.group.iter_mut().for_each(|g| *g = 0);
        self.trace.push(TraceEntry::Heal { at: self.now });
    }

    /// Send through the faulty network: may drop, delay, reorder or duplicate.
    pub fn send(&mut self, from: NodeId, to: NodeId, msg: M) {
        let at = self.now;
        self.trace.push(TraceEntry::Send {
            at,
            from,
            to,
            msg: format!("{msg:?}"),
        });
        if !self.up[from] {
            self.trace.push(TraceEntry::Drop {
                at,
                from,
                to,
                why: "sender-down",
            });
            return;
        }
        if !self.connected(from, to) {
            self.trace.push(TraceEntry::Drop {
                at,
                from,
                to,
                why: "partition",
            });
            return;
        }
        if self.rng.chance_pct(self.net.drop_pct) {
            self.trace.push(TraceEntry::Drop {
                at,
                from,
                to,
                why: "loss",
            });
            return;
        }
        let copies = if self.rng.chance_pct(self.net.dup_pct) {
            self.trace.push(TraceEntry::Dup { at, from, to });
            2
        } else {
            1
        };
        for _ in 0..copies {
            let mut d = self.rng.range(self.net.min_delay, self.net.max_delay);
            if self.rng.chance_pct(self.net.reorder_pct) {
                d += self.rng.range(1, self.net.reorder_extra.max(1));
            }
            self.push(
                at + d,
                Event::Deliver {
                    from,
                    to,
                    msg: msg.clone(),
                },
            );
        }
    }

    /// Advance the clock to the next event and return it. Messages in flight across a
    /// partition that appeared after they were sent are dropped here.
    pub fn step(&mut self) -> Option<Event<M>> {
        while let Some(Reverse(q)) = self.queue.pop() {
            self.now = q.at;
            match q.ev {
                Event::Deliver { from, to, msg } => {
                    if !self.connected(from, to) {
                        self.trace.push(TraceEntry::Drop {
                            at: self.now,
                            from,
                            to,
                            why: "partition-in-flight",
                        });
                        continue;
                    }
                    if !self.up[to] {
                        self.trace.push(TraceEntry::Drop {
                            at: self.now,
                            from,
                            to,
                            why: "receiver-down",
                        });
                        continue;
                    }
                    self.trace.push(TraceEntry::Deliver {
                        at: self.now,
                        from,
                        to,
                        msg: format!("{msg:?}"),
                    });
                    return Some(Event::Deliver { from, to, msg });
                }
                Event::Timer { node, id } => {
                    if !self.up[node] || q.epoch != self.epoch[node] {
                        continue; // timer belonged to a previous incarnation
                    }
                    self.trace.push(TraceEntry::Timer {
                        at: self.now,
                        node,
                        id,
                    });
                    return Some(Event::Timer { node, id });
                }
            }
        }
        None
    }
}
