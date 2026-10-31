//! Raft leader election (Day 3). Log replication arrives on Day 4.
//!
//! [`Node`] is a pure state machine: handlers take an input and return a list of [`Out`]
//! actions, and never touch the clock, network or disk themselves. [`Cluster`] wires nodes to
//! the deterministic [`Sim`], executes the actions (persisting `currentTerm`/`votedFor` and
//! fsyncing *before* any message that depends on them is sent) and records which node won
//! which term so tests can check election safety.

use crate::sim::{Event, NetConfig, NodeId, Sim};
use std::collections::{BTreeMap, BTreeSet};

pub type Term = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Msg {
    RequestVote {
        term: Term,
    },
    VoteReply {
        term: Term,
        granted: bool,
    },
    /// Heartbeat (entries are added on Day 4).
    AppendEntries {
        term: Term,
    },
    AppendReply {
        term: Term,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Timing knobs, in virtual ticks.
#[derive(Debug, Clone, Copy)]
pub struct RaftConfig {
    pub election_min: u64,
    pub election_max: u64,
    pub heartbeat: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_min: 150,
            election_max: 300,
            heartbeat: 50,
        }
    }
}

/// An action a node asks its environment to perform, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Out {
    Send(NodeId, Msg),
    /// Make `(term, voted_for)` durable before executing later actions.
    Persist {
        term: Term,
        voted_for: Option<NodeId>,
    },
    /// Arm a timer; `after` is a fixed delay for heartbeats, or a random election timeout.
    SetTimer {
        id: u64,
        after: TimerKind,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerKind {
    Election,
    Heartbeat,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    n: usize,
    term: Term,
    voted_for: Option<NodeId>,
    role: Role,
    votes: BTreeSet<NodeId>,
    /// Bumped whenever timers are re-armed; timers carrying an older generation are stale.
    gen: u64,
}

fn timer_id(gen: u64, kind: TimerKind) -> u64 {
    (gen << 1) | u64::from(kind == TimerKind::Heartbeat)
}

impl Node {
    /// A node (re)starting with the state recovered from disk. Returns its first actions.
    pub fn new(id: NodeId, n: usize, term: Term, voted_for: Option<NodeId>) -> (Self, Vec<Out>) {
        let mut node = Self {
            id,
            n,
            term,
            voted_for,
            role: Role::Follower,
            votes: BTreeSet::new(),
            gen: 0,
        };
        let outs = vec![node.arm(TimerKind::Election)];
        (node, outs)
    }

    pub fn term(&self) -> Term {
        self.term
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    fn majority(&self) -> usize {
        self.n / 2 + 1
    }

    fn arm(&mut self, kind: TimerKind) -> Out {
        self.gen += 1;
        Out::SetTimer {
            id: timer_id(self.gen, kind),
            after: kind,
        }
    }

    fn observe_term(&mut self, term: Term, outs: &mut Vec<Out>) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
            if self.role != Role::Follower {
                self.role = Role::Follower;
                outs.push(self.arm(TimerKind::Election));
            }
        }
    }

    fn broadcast(&self, msg: &Msg, outs: &mut Vec<Out>) {
        for peer in (0..self.n).filter(|&p| p != self.id) {
            outs.push(Out::Send(peer, msg.clone()));
        }
    }

    /// Run `f`, then prepend a `Persist` if the durable state changed (so it precedes sends).
    fn persisting(&mut self, f: impl FnOnce(&mut Self, &mut Vec<Out>)) -> Vec<Out> {
        let before = (self.term, self.voted_for);
        let mut outs = Vec::new();
        f(self, &mut outs);
        if (self.term, self.voted_for) != before {
            outs.insert(
                0,
                Out::Persist {
                    term: self.term,
                    voted_for: self.voted_for,
                },
            );
        }
        outs
    }

    pub fn on_timer(&mut self, id: u64) -> Vec<Out> {
        if id >> 1 != self.gen {
            return Vec::new(); // stale timer
        }
        let heartbeat = id & 1 == 1;
        self.persisting(|s, outs| {
            if heartbeat {
                if s.role == Role::Leader {
                    s.broadcast(&Msg::AppendEntries { term: s.term }, outs);
                    outs.push(s.arm(TimerKind::Heartbeat));
                }
            } else if s.role != Role::Leader {
                s.role = Role::Candidate;
                s.term += 1;
                s.voted_for = Some(s.id);
                s.votes = BTreeSet::from([s.id]);
                outs.push(s.arm(TimerKind::Election));
                s.broadcast(&Msg::RequestVote { term: s.term }, outs);
                s.maybe_win(outs); // single-node cluster
            }
        })
    }

    fn maybe_win(&mut self, outs: &mut Vec<Out>) {
        if self.role == Role::Candidate && self.votes.len() >= self.majority() {
            self.role = Role::Leader;
            self.broadcast(&Msg::AppendEntries { term: self.term }, outs);
            outs.push(self.arm(TimerKind::Heartbeat));
        }
    }

    pub fn on_message(&mut self, from: NodeId, msg: Msg) -> Vec<Out> {
        self.persisting(|s, outs| match msg {
            Msg::RequestVote { term } => {
                s.observe_term(term, outs);
                let granted = term == s.term && s.voted_for.is_none_or(|v| v == from);
                if granted {
                    s.voted_for = Some(from);
                    if s.role == Role::Follower {
                        outs.push(s.arm(TimerKind::Election));
                    }
                }
                outs.push(Out::Send(
                    from,
                    Msg::VoteReply {
                        term: s.term,
                        granted,
                    },
                ));
            }
            Msg::VoteReply { term, granted } => {
                s.observe_term(term, outs);
                if granted && term == s.term && s.role == Role::Candidate {
                    s.votes.insert(from);
                    s.maybe_win(outs);
                }
            }
            Msg::AppendEntries { term } => {
                s.observe_term(term, outs);
                if term == s.term {
                    if s.role == Role::Candidate {
                        s.role = Role::Follower;
                    }
                    if s.role == Role::Follower {
                        outs.push(s.arm(TimerKind::Election));
                    }
                }
                outs.push(Out::Send(from, Msg::AppendReply { term: s.term }));
            }
            Msg::AppendReply { term } => s.observe_term(term, outs),
        })
    }
}

fn encode(term: Term, voted_for: Option<NodeId>) -> Vec<u8> {
    let mut b = term.to_le_bytes().to_vec();
    b.extend_from_slice(&voted_for.map_or(0, |v| v as u64 + 1).to_le_bytes());
    b
}

fn decode(rec: &[u8]) -> Option<(Term, Option<NodeId>)> {
    let term = Term::from_le_bytes(rec.get(..8)?.try_into().ok()?);
    let v = u64::from_le_bytes(rec.get(8..16)?.try_into().ok()?);
    Some((term, v.checked_sub(1).map(|v| v as usize)))
}

/// Raft nodes running inside a [`Sim`].
pub struct Cluster {
    pub sim: Sim<Msg>,
    pub cfg: RaftConfig,
    nodes: Vec<Node>,
    leaders: BTreeMap<Term, BTreeSet<NodeId>>,
}

impl Cluster {
    pub fn new(seed: u64, n: usize, net: NetConfig, cfg: RaftConfig) -> Self {
        let mut c = Self {
            sim: Sim::new(seed, n, net),
            cfg,
            nodes: Vec::new(),
            leaders: BTreeMap::new(),
        };
        for id in 0..n {
            let (node, outs) = Node::new(id, n, 0, None);
            c.nodes.push(node);
            c.exec(id, outs);
        }
        c
    }

    pub fn node(&self, id: NodeId) -> &Node {
        &self.nodes[id]
    }

    /// Every node that has ever won an election, by term.
    pub fn leaders_by_term(&self) -> &BTreeMap<Term, BTreeSet<NodeId>> {
        &self.leaders
    }

    /// Currently-up nodes that believe they are leader, as `(node, term)`.
    pub fn current_leaders(&self) -> Vec<(NodeId, Term)> {
        (0..self.nodes.len())
            .filter(|&i| self.sim.is_up(i) && self.nodes[i].role == Role::Leader)
            .map(|i| (i, self.nodes[i].term))
            .collect()
    }

    pub fn crash(&mut self, id: NodeId) {
        self.sim.crash(id);
    }

    /// Restart a node, rebuilding its state from whatever survived on disk.
    pub fn restart(&mut self, id: NodeId) {
        if self.sim.is_up(id) {
            return;
        }
        self.sim.restart(id);
        self.sim.disk(id).repair();
        let (term, vote) = self
            .sim
            .disk(id)
            .read_records()
            .iter()
            .rev()
            .find_map(|r| decode(r))
            .unwrap_or((0, None));
        let (node, outs) = Node::new(id, self.nodes.len(), term, vote);
        self.nodes[id] = node;
        self.exec(id, outs);
    }

    fn exec(&mut self, id: NodeId, outs: Vec<Out>) {
        for out in outs {
            match out {
                Out::Persist { term, voted_for } => {
                    self.sim.disk(id).append(&encode(term, voted_for));
                    self.sim.fsync(id);
                }
                Out::Send(to, msg) => self.sim.send(id, to, msg),
                Out::SetTimer { id: tid, after } => {
                    let d = match after {
                        TimerKind::Heartbeat => self.cfg.heartbeat,
                        TimerKind::Election => {
                            self.sim.rng().range(self.cfg.election_min, self.cfg.election_max)
                        }
                    };
                    self.sim.set_timer(id, tid, d);
                }
            }
        }
    }

    fn note_leader(&mut self, id: NodeId, before: Role) {
        if before != Role::Leader && self.nodes[id].role == Role::Leader {
            let term = self.nodes[id].term;
            self.leaders.entry(term).or_default().insert(id);
        }
    }

    /// Process one simulator event. Returns false once the queue is empty.
    pub fn step(&mut self) -> bool {
        match self.sim.step() {
            None => false,
            Some(Event::Timer { node, id }) => {
                let before = self.nodes[node].role;
                let outs = self.nodes[node].on_timer(id);
                self.exec(node, outs);
                self.note_leader(node, before);
                true
            }
            Some(Event::Deliver { from, to, msg }) => {
                let before = self.nodes[to].role;
                let outs = self.nodes[to].on_message(from, msg);
                self.exec(to, outs);
                self.note_leader(to, before);
                true
            }
        }
    }

    /// Run until virtual time `t` (or the queue drains).
    pub fn run_until(&mut self, t: u64) {
        while self.sim.peek_time().is_some_and(|at| at <= t) {
            self.step();
        }
    }
}
