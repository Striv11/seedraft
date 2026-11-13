//! Replicated key-value state machine with client sessions (exactly-once), a history
//! recorder, and a deterministic client harness that drives a Raft [`Cluster`].
//!
//! Raft log commands are `u64`, so a request is bit-packed:
//! `client:8 | seq:16 | kind:2 | key:6 | a:16 | b:16`.
//! Clients issue one request at a time and number them `1, 2, 3, ...`; the store keeps, per
//! client, the last applied sequence number and its reply. A retried or duplicated request
//! (same client and seq) is answered from that table and never applied twice.

use std::collections::BTreeMap;

use crate::raft::{Cluster, RaftConfig};
use crate::sim::NetConfig;

pub const KEYS: u8 = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Put {
        key: u8,
        val: u16,
    },
    Get {
        key: u8,
    },
    /// Set `key` to `new` only if it currently holds `expect` (absent never matches).
    Cas {
        key: u8,
        expect: u16,
        new: u16,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reply {
    Ok,
    Value(Option<u16>),
    Cas(bool),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Request {
    pub client: u8,
    pub seq: u16,
    pub op: Op,
}

impl Request {
    pub fn encode(self) -> u64 {
        assert!(self.op.key() < KEYS, "key out of range");
        let (kind, a, b) = match self.op {
            Op::Put { val, .. } => (0u64, val, 0),
            Op::Get { .. } => (1, 0, 0),
            Op::Cas { expect, new, .. } => (2, expect, new),
        };
        (self.client as u64) << 56
            | (self.seq as u64) << 40
            | kind << 38
            | (self.op.key() as u64) << 32
            | (a as u64) << 16
            | b as u64
    }

    /// Inverse of [`Request::encode`]; `None` for the unused kind value and for Raft config
    /// entries (top byte 0xFF, so client id 255 is reserved), which are not KV commands.
    pub fn decode(w: u64) -> Option<Self> {
        if w >> 56 == 0xFF {
            return None;
        }
        let key = ((w >> 32) & 0x3f) as u8;
        let a = (w >> 16) as u16;
        let b = w as u16;
        let op = match (w >> 38) & 3 {
            0 => Op::Put { key, val: a },
            1 => Op::Get { key },
            2 => Op::Cas {
                key,
                expect: a,
                new: b,
            },
            _ => return None,
        };
        Some(Self {
            client: (w >> 56) as u8,
            seq: (w >> 40) as u16,
            op,
        })
    }
}

impl Op {
    pub fn key(&self) -> u8 {
        match *self {
            Op::Put { key, .. } | Op::Get { key } | Op::Cas { key, .. } => key,
        }
    }
}

/// The sequential specification: a map plus the per-client session table.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KvStore {
    data: BTreeMap<u8, u16>,
    sessions: BTreeMap<u8, (u16, Reply)>,
    effects: u64,
}

impl KvStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one request. Returns its reply, or `None` if it is older than the client's
    /// current request (a stale duplicate nobody is waiting for).
    pub fn apply(&mut self, r: Request) -> Option<Reply> {
        if let Some(&(last, reply)) = self.sessions.get(&r.client) {
            if r.seq == last {
                return Some(reply); // retry of the latest request: replay the cached reply
            }
            if r.seq < last {
                return None;
            }
        }
        let reply = match r.op {
            Op::Put { key, val } => {
                self.data.insert(key, val);
                Reply::Ok
            }
            Op::Get { key } => Reply::Value(self.data.get(&key).copied()),
            Op::Cas { key, expect, new } => {
                if self.data.get(&key) == Some(&expect) {
                    self.data.insert(key, new);
                    Reply::Cas(true)
                } else {
                    Reply::Cas(false)
                }
            }
        };
        self.sessions.insert(r.client, (r.seq, reply));
        self.effects += 1;
        Some(reply)
    }

    /// Serialise the whole state (data, sessions, effect count) for a Raft snapshot.
    pub fn snapshot(&self) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&self.effects.to_le_bytes());
        b.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        for (&k, &v) in &self.data {
            b.push(k);
            b.extend_from_slice(&v.to_le_bytes());
        }
        b.extend_from_slice(&(self.sessions.len() as u32).to_le_bytes());
        for (&c, &(seq, reply)) in &self.sessions {
            b.push(c);
            b.extend_from_slice(&seq.to_le_bytes());
            match reply {
                Reply::Ok => b.push(0),
                Reply::Value(None) => b.push(1),
                Reply::Value(Some(v)) => {
                    b.push(2);
                    b.extend_from_slice(&v.to_le_bytes());
                }
                Reply::Cas(ok) => b.extend_from_slice(&[3, u8::from(ok)]),
            }
        }
        b
    }

    /// Inverse of [`KvStore::snapshot`]; `None` if the bytes are malformed.
    pub fn restore(bytes: &[u8]) -> Option<Self> {
        let mut r = Reader(bytes);
        let mut s = Self {
            effects: u64::from_le_bytes(r.take()?),
            ..Self::default()
        };
        for _ in 0..u32::from_le_bytes(r.take()?) {
            let k = r.take::<1>()?[0];
            s.data.insert(k, u16::from_le_bytes(r.take()?));
        }
        for _ in 0..u32::from_le_bytes(r.take()?) {
            let c = r.take::<1>()?[0];
            let seq = u16::from_le_bytes(r.take()?);
            let reply = match r.take::<1>()?[0] {
                0 => Reply::Ok,
                1 => Reply::Value(None),
                2 => Reply::Value(Some(u16::from_le_bytes(r.take()?))),
                3 => Reply::Cas(r.take::<1>()?[0] != 0),
                _ => return None,
            };
            s.sessions.insert(c, (seq, reply));
        }
        r.0.is_empty().then_some(s)
    }

    pub fn get(&self, key: u8) -> Option<u16> {
        self.data.get(&key).copied()
    }

    /// Last applied `(seq, reply)` for a client.
    pub fn session(&self, client: u8) -> Option<(u16, Reply)> {
        self.sessions.get(&client).copied()
    }

    /// Number of requests that actually changed or read state (duplicates excluded).
    pub fn effects(&self) -> u64 {
        self.effects
    }
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, rest) = self.0.split_first_chunk::<N>()?;
        self.0 = rest;
        Some(*head)
    }
}

/// One operation in a recorded history. `ret == None` means the client never saw a response
/// (indeterminate: the op may or may not have taken effect).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistOp {
    pub id: usize,
    pub client: u8,
    pub op: Op,
    pub invoke: u64,
    pub ret: Option<(u64, Reply)>,
}

/// Records invoke/return events as a list of operations.
#[derive(Clone, Debug, Default)]
pub struct Recorder {
    ops: Vec<HistOp>,
}

impl Recorder {
    pub fn invoke(&mut self, client: u8, op: Op, time: u64) -> usize {
        let id = self.ops.len();
        self.ops.push(HistOp {
            id,
            client,
            op,
            invoke: time,
            ret: None,
        });
        id
    }

    pub fn complete(&mut self, id: usize, reply: Reply, time: u64) {
        let o = &mut self.ops[id];
        assert!(o.ret.is_none(), "op {id} returned twice");
        assert!(time >= o.invoke, "op {id} returned before it was invoked");
        o.ret = Some((time, reply));
    }

    pub fn ops(&self) -> &[HistOp] {
        &self.ops
    }
}

struct Pending {
    req: Request,
    hist: usize,
    last_sent: u64,
}

struct Client {
    next_seq: u16,
    pending: Option<Pending>,
}

/// Workload shape for [`KvHarness`].
#[derive(Clone, Copy, Debug)]
pub struct Workload {
    pub clients: u8,
    /// Keys are drawn from `0..keys`.
    pub keys: u8,
    /// A client with nothing in flight starts a request with this probability per poll (percent).
    pub start_pct: u8,
    /// Re-send an unanswered request to the leader after this many ticks.
    pub retry_after: u64,
    /// Virtual ticks between polls of the cluster.
    pub poll: u64,
}

impl Default for Workload {
    fn default() -> Self {
        Self {
            clients: 3,
            keys: 4,
            start_pct: 30,
            retry_after: 120,
            poll: 5,
        }
    }
}

/// Clients + replicated store on top of a Raft cluster. Requests enter the log through the
/// leader, are applied in commit order to a single [`KvStore`] (the replicated state machine
/// as seen by the global committed prefix), and the client observes the reply once its
/// request is committed. All choices come from the simulator's RNG, so runs replay per seed.
pub struct KvHarness {
    pub cluster: Cluster,
    pub store: KvStore,
    pub history: Recorder,
    wl: Workload,
    clients: Vec<Client>,
    applied: usize,
    clock: u64,
    lease_reads: u64,
}

impl KvHarness {
    pub fn new(seed: u64, nodes: usize, net: NetConfig, wl: Workload) -> Self {
        Self::with_raft(seed, nodes, net, wl, RaftConfig::default())
    }

    pub fn with_raft(seed: u64, nodes: usize, net: NetConfig, wl: Workload, raft: RaftConfig) -> Self {
        let clients = (0..wl.clients)
            .map(|_| Client {
                next_seq: 1,
                pending: None,
            })
            .collect();
        Self {
            cluster: Cluster::new(seed, nodes, net, raft),
            store: KvStore::new(),
            history: Recorder::default(),
            wl,
            clients,
            applied: 0,
            clock: 0,
            lease_reads: 0,
        }
    }

    /// Advance virtual time to `t`, polling clients every `poll` ticks. Faults can be
    /// injected between calls through `self.cluster`.
    pub fn run_until(&mut self, t: u64) {
        // The sim clock only moves when an event fires, so keep our own poll clock.
        while self.clock < t {
            let next = (self.clock + self.wl.poll).min(t);
            self.cluster.run_until(next);
            self.clock = next;
            self.poll(next);
        }
    }

    /// Reads answered locally by a lease-holding leader instead of through the log.
    pub fn lease_reads(&self) -> u64 {
        self.lease_reads
    }

    /// Requests still waiting for a reply.
    pub fn in_flight(&self) -> usize {
        self.clients.iter().filter(|c| c.pending.is_some()).count()
    }

    /// Stop starting new requests (used to let the system drain at the end of a run).
    pub fn quiesce(&mut self) {
        self.wl.start_pct = 0;
    }

    fn poll(&mut self, now: u64) {
        // Apply newly committed entries in order.
        while self.applied < self.cluster.committed().len() {
            let e = self.cluster.committed()[self.applied];
            self.applied += 1;
            if let Some(r) = e.cmd.and_then(Request::decode) {
                self.store.apply(r);
            }
        }
        for c in 0..self.clients.len() {
            let id = c as u8;
            if let Some(p) = &self.clients[c].pending {
                match self.store.session(id) {
                    Some((seq, reply)) if seq == p.req.seq => {
                        self.history.complete(p.hist, reply, now);
                        self.clients[c].pending = None;
                    }
                    _ if now - p.last_sent >= self.wl.retry_after => {
                        let req = p.req;
                        self.cluster.propose(req.encode());
                        self.clients[c].pending.as_mut().unwrap().last_sent = now;
                    }
                    _ => {}
                }
            } else if self.cluster.sim.rng().chance_pct(self.wl.start_pct) {
                let op = self.random_op(id);
                let seq = self.clients[c].next_seq;
                self.clients[c].next_seq += 1;
                let req = Request { client: id, seq, op };
                let hist = self.history.invoke(id, op, now);
                if let Some(reply) = self.try_lease_read(req, now) {
                    self.history.complete(hist, reply, now);
                    self.lease_reads += 1;
                    continue;
                }
                self.cluster.propose(req.encode());
                self.clients[c].pending = Some(Pending {
                    req,
                    hist,
                    last_sent: now,
                });
            }
        }
    }

    /// Serve a Get from some node that believes it holds a lease (picked at random, so a
    /// deposed leader with a stale lease gets its turn). `None`: go through the log.
    fn try_lease_read(&mut self, req: Request, now: u64) -> Option<Reply> {
        if !matches!(req.op, Op::Get { .. }) {
            return None;
        }
        let holders = self.cluster.lease_holders(now);
        if holders.is_empty() {
            return None;
        }
        let pick = holders[self.cluster.sim.rng().range(0, holders.len() as u64 - 1) as usize];
        self.cluster.read_view(pick).apply(req)
    }

    fn random_op(&mut self, _client: u8) -> Op {
        let keys = self.wl.keys as u64;
        let rng = self.cluster.sim.rng();
        let key = rng.range(0, keys - 1) as u8;
        match rng.range(0, 2) {
            0 => Op::Put {
                key,
                val: rng.range(0, 4) as u16,
            },
            1 => Op::Get { key },
            _ => Op::Cas {
                key,
                expect: rng.range(0, 4) as u16,
                new: rng.range(0, 4) as u16,
            },
        }
    }
}
