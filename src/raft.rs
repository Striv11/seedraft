//! Raft leader election (Day 3) and log replication with commit (Day 4).
//!
//! Day 5: `currentTerm`, `votedFor` and the log are write-ahead logged to the node's simulated
//! disk (records: hard state, appended entry, truncate-to-length) and fsynced *before* any
//! message that depends on them is sent, so a restarted node replays its disk and loses no
//! acknowledged entry (assuming fsync is honest; a lying drive voids every WAL protocol).
//!
//! [`Node`] is a pure state machine: handlers take an input and return a list of [`Out`]
//! actions, and never touch the clock, network or disk themselves. [`Cluster`] wires nodes to
//! the deterministic [`Sim`], executes the actions (persisting `currentTerm`/`votedFor` and
//! fsyncing *before* any message that depends on them is sent) and records which node won
//! which term so tests can check election safety.

use crate::kv::{KvStore, Request};
use crate::sim::{Event, NetConfig, NodeId, Sim};
use std::collections::{BTreeMap, BTreeSet};

pub type Term = u64;
/// 1-based log index; 0 means "before the first entry".
pub type Index = u64;

/// A log entry. `cmd == None` is the no-op a new leader appends to commit older entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    pub term: Term,
    pub cmd: Option<u64>,
}

/// Cluster membership as a pair of voter bitmasks (node ids < 32). `prev != 0` means the
/// cluster is in the joint configuration C(old,new): every decision needs a majority of
/// `prev` *and* a majority of `cur`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    pub cur: u32,
    pub prev: u32,
}

/// Config entries are log commands with this tag in the top byte (client id 0xFF is reserved).
const CONF_TAG: u64 = 0xFF << 56;

impl Config {
    pub fn stable(members: &[NodeId]) -> Self {
        Self {
            cur: mask_of(members),
            prev: 0,
        }
    }
    pub fn is_joint(&self) -> bool {
        self.prev != 0
    }
    /// Every node that takes part in decisions under this configuration.
    pub fn voters(&self) -> u32 {
        self.cur | self.prev
    }
    pub fn contains(&self, id: NodeId) -> bool {
        self.voters() >> id & 1 == 1
    }
    pub fn members(&self) -> Vec<NodeId> {
        (0..32).filter(|&i| self.contains(i)).collect()
    }
    /// Does `set` hold a majority of every active member set?
    pub fn has_quorum(&self, set: &BTreeSet<NodeId>) -> bool {
        let ok = |m: u32| {
            let size = m.count_ones() as usize;
            size > 0 && set.iter().filter(|&&i| m >> i & 1 == 1).count() > size / 2
        };
        ok(self.cur) && (self.prev == 0 || ok(self.prev))
    }
    /// Highest index replicated on a majority of every active member set.
    fn quorum_match(&self, matched: &[Index]) -> Index {
        let one = |m: u32| {
            let mut v: Vec<Index> = (0..matched.len())
                .filter(|&i| m >> i & 1 == 1)
                .map(|i| matched[i])
                .collect();
            if v.is_empty() {
                return 0;
            }
            v.sort_unstable();
            v[v.len() - (v.len() / 2 + 1)]
        };
        let c = one(self.cur);
        if self.prev == 0 {
            c
        } else {
            c.min(one(self.prev))
        }
    }
    pub fn encode(&self) -> u64 {
        CONF_TAG | u64::from(self.prev) << 24 | u64::from(self.cur)
    }
    /// `Some` if `cmd` is a config entry.
    pub fn decode(cmd: u64) -> Option<Self> {
        (cmd >> 56 == 0xFF).then_some(Self {
            cur: (cmd & 0xFF_FFFF) as u32,
            prev: (cmd >> 24 & 0xFF_FFFF) as u32,
        })
    }
}

fn mask_of(ids: &[NodeId]) -> u32 {
    ids.iter().fold(0, |m, &i| m | 1 << i)
}

/// Max entries per AppendEntries message.
const BATCH: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Msg {
    RequestVote {
        term: Term,
        last_index: Index,
        last_term: Term,
    },
    VoteReply {
        term: Term,
        granted: bool,
    },
    AppendEntries {
        term: Term,
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
    },
    /// `index` is the follower's last matching index on success, or its log length as a hint
    /// for backing up `next_index` on failure.
    AppendReply {
        term: Term,
        success: bool,
        index: Index,
    },
    /// Leader -> far-behind follower: the state machine as of `last_index` (whole snapshot in
    /// one message; chunking is not modelled). Answered with an `AppendReply`.
    InstallSnapshot {
        term: Term,
        last_index: Index,
        last_term: Term,
        /// Configuration in force as of `last_index`.
        conf: Config,
        data: Vec<u8>,
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
    /// Compact the log once this many applied entries sit above the last snapshot (0 = never).
    pub snapshot_every: u64,
    /// Deliberately broken behaviour, for checking that the harness catches real bugs.
    pub bug: Option<Bug>,
}

/// Known protocol bugs that can be switched on to validate the test harness itself.
/// Production runs never set one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Bug {
    /// Grant a vote even if already voted for someone else this term (two leaders per term).
    DoubleVote,
    /// Grant votes to candidates with stale logs (committed entries can be lost).
    SkipLogRecencyCheck,
    /// Process AppendEntries from a leader of an older term (the "skipped term check").
    AcceptStaleAppend,
}

impl Bug {
    pub const ALL: [Bug; 3] = [Bug::DoubleVote, Bug::SkipLogRecencyCheck, Bug::AcceptStaleAppend];

    pub fn name(self) -> &'static str {
        match self {
            Bug::DoubleVote => "double-vote",
            Bug::SkipLogRecencyCheck => "skip-log-recency",
            Bug::AcceptStaleAppend => "accept-stale-append",
        }
    }

    pub fn parse(s: &str) -> Option<Bug> {
        Bug::ALL.into_iter().find(|b| b.name() == s)
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_min: 150,
            election_max: 300,
            heartbeat: 50,
            snapshot_every: 0,
            bug: None,
        }
    }
}

/// An action a node asks its environment to perform, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Out {
    Send(NodeId, Msg),
    /// Make hard state and/or log changes durable (append + fsync) before later actions.
    Persist {
        hard: Option<(Term, Option<NodeId>)>,
        log: Vec<LogOp>,
    },
    /// Arm a timer; `after` is a fixed delay for heartbeats, or a random election timeout.
    SetTimer {
        id: u64,
        after: TimerKind,
    },
    /// The node adopted a leader's snapshot: replace the state machine with `data`, which
    /// covers everything through `index`.
    Restore {
        index: Index,
        data: Vec<u8>,
    },
}

/// A mutation of the durable log, replayed in order on recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogOp {
    Append(Entry),
    /// Keep only entries with index `<= n` (absolute index).
    TruncateTo(Index),
    /// Everything through `index` (whose entry had `term`) is summarised by `data`.
    Snapshot {
        index: Index,
        term: Term,
        conf: Config,
        data: Vec<u8>,
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
    /// Injected bug (testing the harness only).
    pub bug: Option<Bug>,
    n: usize,
    term: Term,
    voted_for: Option<NodeId>,
    role: Role,
    votes: BTreeSet<NodeId>,
    /// Entries after the snapshot: `log[0]` has index `snap_index + 1`.
    log: Vec<Entry>,
    /// Bumped whenever existing log content is discarded or replaced (truncation, snapshot
    /// install). Appends and compaction keep earlier indexes intact and do not count. Lets the
    /// invariant checker reuse earlier comparisons of two logs.
    rewrites: u64,
    snap_index: Index,
    snap_term: Term,
    snap_data: Vec<u8>,
    snap_conf: Config,
    /// Configuration in force: the last config entry in the log (committed or not), else the
    /// snapshot's, else genesis. `conf_index` is that entry's index (0 if not from the log).
    conf: Config,
    conf_index: Index,
    /// Log mutations not yet handed to the environment for persisting.
    wal: Vec<LogOp>,
    commit: Index,
    /// Leader state, indexed by peer id.
    next: Vec<Index>,
    matched: Vec<Index>,
    /// Bumped whenever timers are re-armed; timers carrying an older generation are stale.
    gen: u64,
}

fn timer_id(gen: u64, kind: TimerKind) -> u64 {
    (gen << 1) | u64::from(kind == TimerKind::Heartbeat)
}

impl Node {
    /// A node (re)starting with the state recovered from disk. Returns its first actions.
    pub fn new(
        id: NodeId,
        n: usize,
        term: Term,
        voted_for: Option<NodeId>,
        log: Vec<Entry>,
    ) -> (Self, Vec<Out>) {
        Self::recovered(
            id,
            n,
            Recovered {
                term,
                voted_for,
                log,
                ..Recovered::default()
            },
        )
    }

    /// Like [`Node::new`] but from everything recovered from disk, snapshot included.
    pub fn recovered(id: NodeId, n: usize, rec: Recovered) -> (Self, Vec<Out>) {
        let all: Vec<NodeId> = (0..n).collect();
        Self::recovered_with(id, n, Config::stable(&all), rec)
    }

    /// Like [`Node::recovered`] for a cluster whose initial membership is `genesis`.
    pub fn recovered_with(id: NodeId, n: usize, genesis: Config, rec: Recovered) -> (Self, Vec<Out>) {
        let snap_conf = if rec.snap_index > 0 {
            rec.snap_conf.unwrap_or(genesis)
        } else {
            genesis
        };
        let mut node = Self {
            id,
            bug: None,
            n,
            term: rec.term,
            voted_for: rec.voted_for,
            role: Role::Follower,
            votes: BTreeSet::new(),
            log: rec.log,
            rewrites: 0,
            snap_index: rec.snap_index,
            snap_term: rec.snap_term,
            snap_data: rec.snap_data,
            snap_conf,
            conf: snap_conf,
            conf_index: 0,
            wal: Vec::new(),
            commit: rec.snap_index,
            next: vec![1; n],
            matched: vec![0; n],
            gen: 0,
        };
        node.recompute_conf();
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

    /// The configuration this node currently acts under.
    pub fn config(&self) -> Config {
        self.conf
    }

    /// Rebuild `conf` from the snapshot and the log.
    fn recompute_conf(&mut self) {
        self.conf = self.snap_conf;
        self.conf_index = 0;
        for (k, e) in self.log.iter().enumerate() {
            if let Some(c) = e.cmd.and_then(Config::decode) {
                self.conf = c;
                self.conf_index = self.snap_index + k as Index + 1;
            }
        }
    }

    pub fn log(&self) -> &[Entry] {
        &self.log
    }
    pub fn commit_index(&self) -> Index {
        self.commit
    }
    /// Index of the last entry covered by the snapshot (0 if none).
    pub fn snapshot_index(&self) -> Index {
        self.snap_index
    }
    fn last_index(&self) -> Index {
        self.snap_index + self.log.len() as Index
    }
    /// The entry at absolute index `i`, if it is still in the (uncompacted) log.
    pub fn entry(&self, i: Index) -> Option<Entry> {
        i.checked_sub(self.snap_index + 1)
            .and_then(|k| self.log.get(k as usize))
            .copied()
    }
    fn term_at(&self, i: Index) -> Term {
        if i == self.snap_index {
            self.snap_term
        } else {
            self.entry(i)
                .expect("term_at: index compacted or beyond log")
                .term
        }
    }

    fn push_entry(&mut self, e: Entry) {
        if let Some(c) = e.cmd.and_then(Config::decode) {
            self.conf = c;
            self.conf_index = self.snap_index + self.log.len() as Index + 1;
        }
        self.log.push(e);
        self.wal.push(LogOp::Append(e));
    }

    /// Keep only entries with absolute index `<= last`.
    fn truncate_to(&mut self, last: Index) {
        debug_assert!(last >= self.snap_index);
        let cut = self.conf_index > last;
        self.log.truncate((last - self.snap_index) as usize);
        self.rewrites += 1;
        self.wal.push(LogOp::TruncateTo(last));
        if cut {
            self.recompute_conf();
        }
    }

    /// Local compaction: the state machine has applied everything through `upto` (which must
    /// be committed) and produced `data`. Drops the covered log prefix.
    pub fn compact(&mut self, upto: Index, data: Vec<u8>) -> Vec<Out> {
        if upto <= self.snap_index || upto > self.commit {
            return Vec::new();
        }
        let term = self.term_at(upto);
        // The configuration as of `upto`: the last config entry at or before it.
        let k = (upto - self.snap_index) as usize;
        if let Some(c) = self.log[..k]
            .iter()
            .rev()
            .find_map(|e| e.cmd.and_then(Config::decode))
        {
            self.snap_conf = c;
        }
        self.log.drain(..k);
        self.snap_index = upto;
        self.snap_term = term;
        self.snap_data = data.clone();
        self.recompute_conf();
        self.wal.push(LogOp::Snapshot {
            index: upto,
            term,
            conf: self.snap_conf,
            data,
        });
        let mut outs = Vec::new();
        self.flush_persist(false, &mut outs);
        outs
    }

    /// Hand pending durable changes to the environment, to be executed before any send.
    fn flush_persist(&mut self, hard_changed: bool, outs: &mut Vec<Out>) {
        if hard_changed || !self.wal.is_empty() {
            outs.insert(
                0,
                Out::Persist {
                    hard: hard_changed.then_some((self.term, self.voted_for)),
                    log: std::mem::take(&mut self.wal),
                },
            );
        }
    }

    /// Every other node we exchange votes and log entries with.
    fn peers(&self) -> Vec<NodeId> {
        (0..self.n)
            .filter(|&p| p != self.id && self.conf.contains(p))
            .collect()
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
        for peer in self.peers() {
            outs.push(Out::Send(peer, msg.clone()));
        }
    }

    /// Run `f`, then prepend a `Persist` if the durable state changed (so it precedes sends).
    fn persisting(&mut self, f: impl FnOnce(&mut Self, &mut Vec<Out>)) -> Vec<Out> {
        let before = (self.term, self.voted_for);
        let mut outs = Vec::new();
        f(self, &mut outs);
        self.flush_persist((self.term, self.voted_for) != before, &mut outs);
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
                    s.config_progress(outs);
                    s.replicate_all(outs);
                    outs.push(s.arm(TimerKind::Heartbeat));
                }
            } else if s.role != Role::Leader {
                if !s.conf.contains(s.id) {
                    // Not a voter (new, or removed): never campaign, just keep listening.
                    outs.push(s.arm(TimerKind::Election));
                    return;
                }
                s.role = Role::Candidate;
                s.term += 1;
                s.voted_for = Some(s.id);
                s.votes = BTreeSet::from([s.id]);
                outs.push(s.arm(TimerKind::Election));
                let (last_index, last_term) = (s.last_index(), s.term_at(s.last_index()));
                let rv = Msg::RequestVote {
                    term: s.term,
                    last_index,
                    last_term,
                };
                s.broadcast(&rv, outs);
                s.maybe_win(outs); // single-node cluster
            }
        })
    }

    fn maybe_win(&mut self, outs: &mut Vec<Out>) {
        if self.role == Role::Candidate && self.conf.has_quorum(&self.votes) {
            self.role = Role::Leader;
            let next = self.last_index() + 1;
            self.next = vec![next; self.n];
            self.matched = vec![0; self.n];
            self.push_entry(Entry {
                term: self.term,
                cmd: None,
            });
            self.advance_commit();
            self.config_progress(outs);
            self.replicate_all(outs);
            outs.push(self.arm(TimerKind::Heartbeat));
        }
    }

    /// Leader: start moving the cluster to voter set `members` through the joint
    /// configuration. Refused unless this leader is in a stable configuration whose last
    /// config entry is committed (one change at a time).
    pub fn change_membership(&mut self, members: &[NodeId]) -> Option<(Index, Vec<Out>)> {
        let target = mask_of(members);
        if self.role != Role::Leader
            || self.conf.is_joint()
            || self.conf_index > self.commit
            || target == 0
            || target == self.conf.cur
            || members.iter().any(|&m| m >= self.n)
        {
            return None;
        }
        let joint = Config {
            cur: target,
            prev: self.conf.cur,
        };
        self.propose(joint.encode())
    }

    /// Leader: once the joint configuration is committed, append the final one; once a final
    /// configuration without us is committed, step down.
    fn config_progress(&mut self, outs: &mut Vec<Out>) {
        if self.role != Role::Leader {
            return;
        }
        if self.conf.is_joint() && self.conf_index <= self.commit {
            let fin = Config {
                cur: self.conf.cur,
                prev: 0,
            };
            self.push_entry(Entry {
                term: self.term,
                cmd: Some(fin.encode()),
            });
            self.advance_commit();
        } else if !self.conf.contains(self.id) && self.conf_index <= self.commit {
            self.role = Role::Follower;
            outs.push(self.arm(TimerKind::Election));
        }
    }

    /// Client entry point: append `cmd` if leader and start replicating it.
    pub fn propose(&mut self, cmd: u64) -> Option<(Index, Vec<Out>)> {
        if self.role != Role::Leader {
            return None;
        }
        self.push_entry(Entry {
            term: self.term,
            cmd: Some(cmd),
        });
        let mut outs = Vec::new();
        let index = self.last_index();
        self.advance_commit();
        self.config_progress(&mut outs);
        self.replicate_all(&mut outs);
        self.flush_persist(false, &mut outs);
        Some((index, outs))
    }

    fn replicate_all(&self, outs: &mut Vec<Out>) {
        for p in self.peers() {
            self.replicate_to(p, outs);
        }
    }

    fn replicate_to(&self, peer: NodeId, outs: &mut Vec<Out>) {
        let prev_index = self.next[peer] - 1;
        if prev_index < self.snap_index {
            outs.push(Out::Send(
                peer,
                Msg::InstallSnapshot {
                    term: self.term,
                    last_index: self.snap_index,
                    last_term: self.snap_term,
                    conf: self.snap_conf,
                    data: self.snap_data.clone(),
                },
            ));
            return;
        }
        let entries = self.log[(prev_index - self.snap_index) as usize..]
            .iter()
            .take(BATCH)
            .copied()
            .collect();
        outs.push(Out::Send(
            peer,
            Msg::AppendEntries {
                term: self.term,
                prev_index,
                prev_term: self.term_at(prev_index),
                entries,
                leader_commit: self.commit,
            },
        ));
    }

    /// Leader: commit the highest index replicated on a majority *in the current term*
    /// (older-term entries commit only indirectly, Raft paper figure 8).
    fn advance_commit(&mut self) {
        self.matched[self.id] = self.last_index();
        let candidate = self.conf.quorum_match(&self.matched);
        if candidate > self.commit && self.term_at(candidate) == self.term {
            self.commit = candidate;
        }
    }

    /// Adopt a leader's snapshot ahead of our commit point. Keep the log suffix only if it
    /// agrees with the snapshot at its last index.
    fn install_snapshot(&mut self, index: Index, term: Term, conf: Config, data: Vec<u8>) {
        let keep_suffix = index <= self.last_index() && self.term_at(index) == term;
        if keep_suffix {
            self.log.drain(..(index - self.snap_index) as usize);
        } else {
            self.log.clear();
            self.rewrites += 1;
        }
        self.snap_index = index;
        self.snap_term = term;
        self.snap_data = data.clone();
        self.snap_conf = conf;
        self.recompute_conf();
        self.commit = self.commit.max(index);
        self.wal.push(LogOp::Snapshot {
            index,
            term,
            conf,
            data,
        });
    }

    pub fn on_message(&mut self, from: NodeId, msg: Msg) -> Vec<Out> {
        self.persisting(|s, outs| match msg {
            // A node outside our configuration (e.g. removed but unaware) must not be able to
            // depose us by inflating terms: ignore it entirely.
            Msg::RequestVote { .. } if !s.conf.contains(from) => {}
            Msg::RequestVote {
                term,
                last_index,
                last_term,
            } => {
                s.observe_term(term, outs);
                let mine = (s.term_at(s.last_index()), s.last_index());
                let up_to_date = (last_term, last_index) >= mine || s.bug == Some(Bug::SkipLogRecencyCheck);
                let free = s.voted_for.is_none_or(|v| v == from) || s.bug == Some(Bug::DoubleVote);
                let granted = term == s.term && up_to_date && free;
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
            Msg::AppendEntries {
                term,
                prev_index,
                prev_term,
                entries,
                leader_commit,
            } => {
                s.observe_term(term, outs);
                let mut reply = Msg::AppendReply {
                    term: s.term,
                    success: false,
                    index: s.last_index(),
                };
                if term == s.term || (s.bug == Some(Bug::AcceptStaleAppend) && term < s.term) {
                    if s.role == Role::Candidate {
                        s.role = Role::Follower;
                    }
                    if s.role == Role::Follower {
                        outs.push(s.arm(TimerKind::Election));
                    }
                    // A prev_index inside our snapshot is committed, hence matches the leader's.
                    if prev_index <= s.last_index()
                        && (prev_index < s.snap_index || s.term_at(prev_index) == prev_term)
                    {
                        let mut at = prev_index;
                        for e in entries {
                            at += 1;
                            if at <= s.snap_index {
                                continue; // covered by our snapshot
                            }
                            if at <= s.last_index() {
                                if s.term_at(at) == e.term {
                                    continue; // already have it (duplicate or reordered)
                                }
                                s.truncate_to(at - 1);
                            }
                            s.push_entry(e);
                        }
                        // `at` is the last index this message vouches for.
                        s.commit = s.commit.max(leader_commit.min(at));
                        reply = Msg::AppendReply {
                            term: s.term,
                            success: true,
                            index: at,
                        };
                    }
                }
                outs.push(Out::Send(from, reply));
            }
            Msg::InstallSnapshot {
                term,
                last_index,
                last_term,
                conf,
                data,
            } => {
                s.observe_term(term, outs);
                let mut reply = Msg::AppendReply {
                    term: s.term,
                    success: false,
                    index: s.last_index(),
                };
                if term == s.term {
                    if s.role == Role::Candidate {
                        s.role = Role::Follower;
                    }
                    if s.role == Role::Follower {
                        outs.push(s.arm(TimerKind::Election));
                    }
                    if last_index <= s.commit {
                        // Already committed (hence identical to the leader's) through there.
                        reply = Msg::AppendReply {
                            term: s.term,
                            success: true,
                            index: last_index,
                        };
                    } else {
                        s.install_snapshot(last_index, last_term, conf, data.clone());
                        outs.push(Out::Restore {
                            index: last_index,
                            data,
                        });
                        reply = Msg::AppendReply {
                            term: s.term,
                            success: true,
                            index: last_index,
                        };
                    }
                }
                outs.push(Out::Send(from, reply));
            }
            Msg::AppendReply { term, success, index } => {
                s.observe_term(term, outs);
                if term == s.term && s.role == Role::Leader {
                    if success {
                        s.matched[from] = s.matched[from].max(index);
                        s.next[from] = s.next[from].max(s.matched[from] + 1);
                        s.advance_commit();
                        s.config_progress(outs);
                        if s.matched[from] < s.last_index() {
                            s.replicate_to(from, outs);
                        }
                    } else {
                        // Back up, never below what is known to match.
                        let backed = (s.next[from] - 1).min(index + 1);
                        s.next[from] = backed.max(s.matched[from] + 1).max(1);
                        s.replicate_to(from, outs);
                    }
                }
            }
        })
    }
}

const REC_HARD: u8 = 0;
const REC_APPEND: u8 = 1;
const REC_TRUNC: u8 = 2;
const REC_SNAP: u8 = 3;

fn encode_hard(term: Term, voted_for: Option<NodeId>) -> Vec<u8> {
    let mut b = vec![REC_HARD];
    b.extend_from_slice(&term.to_le_bytes());
    b.extend_from_slice(&voted_for.map_or(0, |v| v as u64 + 1).to_le_bytes());
    b
}

fn encode_op(op: &LogOp) -> Vec<u8> {
    match op {
        LogOp::Append(e) => {
            let mut b = vec![REC_APPEND];
            b.extend_from_slice(&e.term.to_le_bytes());
            b.extend_from_slice(&e.cmd.unwrap_or(0).to_le_bytes());
            b.push(u8::from(e.cmd.is_some()));
            b
        }
        LogOp::TruncateTo(n) => {
            let mut b = vec![REC_TRUNC];
            b.extend_from_slice(&n.to_le_bytes());
            b
        }
        LogOp::Snapshot {
            index,
            term,
            conf,
            data,
        } => {
            let mut b = vec![REC_SNAP];
            b.extend_from_slice(&index.to_le_bytes());
            b.extend_from_slice(&term.to_le_bytes());
            b.extend_from_slice(&conf.encode().to_le_bytes());
            b.extend_from_slice(data);
            b
        }
    }
}

fn u64_at(rec: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(rec.get(at..at + 8)?.try_into().ok()?))
}

/// Everything a node recovers from its disk.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Recovered {
    pub term: Term,
    pub voted_for: Option<NodeId>,
    /// Entries after the snapshot.
    pub log: Vec<Entry>,
    pub snap_index: Index,
    pub snap_term: Term,
    pub snap_data: Vec<u8>,
    /// Configuration as of the snapshot (`None`: the cluster's genesis configuration).
    pub snap_conf: Option<Config>,
}

/// Replay intact disk records in order. Unknown or malformed records are skipped.
pub fn recover(records: &[Vec<u8>]) -> Recovered {
    let mut r = Recovered::default();
    for rec in records {
        match rec.first() {
            Some(&REC_HARD) => {
                if let (Some(t), Some(v)) = (u64_at(rec, 1), u64_at(rec, 9)) {
                    r.term = t;
                    r.voted_for = v.checked_sub(1).map(|v| v as usize);
                }
            }
            Some(&REC_APPEND) => {
                if let (Some(term), Some(c), Some(&has)) = (u64_at(rec, 1), u64_at(rec, 9), rec.get(17)) {
                    r.log.push(Entry {
                        term,
                        cmd: (has == 1).then_some(c),
                    });
                }
            }
            Some(&REC_TRUNC) => {
                if let Some(n) = u64_at(rec, 1) {
                    r.log.truncate(n.saturating_sub(r.snap_index) as usize);
                }
            }
            Some(&REC_SNAP) => {
                if let (Some(index), Some(term), Some(conf)) = (
                    u64_at(rec, 1),
                    u64_at(rec, 9),
                    u64_at(rec, 17).and_then(Config::decode),
                ) {
                    let k = index.wrapping_sub(r.snap_index) as usize;
                    let agrees =
                        index > r.snap_index && k <= r.log.len() && (k == 0 || r.log[k - 1].term == term);
                    if index > r.snap_index {
                        if agrees {
                            r.log.drain(..k);
                        } else {
                            r.log.clear();
                        }
                        r.snap_index = index;
                        r.snap_term = term;
                        r.snap_conf = Some(conf);
                        r.snap_data = rec[25..].to_vec();
                    }
                }
            }
            _ => {}
        }
    }
    r
}

/// Raft nodes running inside a [`Sim`].
pub struct Cluster {
    pub sim: Sim<Msg>,
    pub cfg: RaftConfig,
    genesis: Config,
    /// The latest committed configuration, for checking the chain of changes.
    conf_chain: Config,
    nodes: Vec<Node>,
    leaders: BTreeMap<Term, BTreeSet<NodeId>>,
    /// Every entry any node has ever committed, by index (the global committed prefix).
    committed: Vec<Entry>,
    violations: Vec<String>,
    /// Per-node replicated state machine and the index it has applied through.
    machines: Vec<KvStore>,
    applied: Vec<Index>,
    snapshots_installed: u64,
    /// For each pair `(lo, hi)` of nodes: `(rewrites of lo, rewrites of hi, n)` meaning their
    /// logs were already compared and found identical through index `n`.
    agreed: Vec<Vec<(u64, u64, Index)>>,
}

impl Cluster {
    pub fn new(seed: u64, n: usize, net: NetConfig, cfg: RaftConfig) -> Self {
        let all: Vec<NodeId> = (0..n).collect();
        Self::with_members(seed, n, &all, net, cfg)
    }

    /// A universe of `n` simulated machines of which only `members` start as voters; the
    /// rest idle until a membership change adds them.
    pub fn with_members(seed: u64, n: usize, members: &[NodeId], net: NetConfig, cfg: RaftConfig) -> Self {
        let genesis = Config::stable(members);
        let mut c = Self {
            genesis,
            conf_chain: genesis,
            sim: Sim::new(seed, n, net),
            cfg,
            nodes: Vec::new(),
            leaders: BTreeMap::new(),
            committed: Vec::new(),
            violations: Vec::new(),
            machines: vec![KvStore::new(); n],
            applied: vec![0; n],
            snapshots_installed: 0,
            agreed: vec![vec![(0, 0, 0); n]; n],
        };
        for id in 0..n {
            let (mut node, outs) = Node::recovered_with(id, n, genesis, Recovered::default());
            node.bug = c.cfg.bug;
            c.nodes.push(node);
            c.exec(id, outs);
        }
        c
    }

    pub fn node(&self, id: NodeId) -> &Node {
        &self.nodes[id]
    }

    /// Node `id`'s own copy of the state machine and the log index it reflects.
    pub fn machine(&self, id: NodeId) -> (&KvStore, Index) {
        (&self.machines[id], self.applied[id])
    }

    /// How many times a node replaced its state with a leader's snapshot.
    pub fn snapshots_installed(&self) -> u64 {
        self.snapshots_installed
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

    /// Invariant violations seen so far (empty means all of Raft's safety properties held).
    pub fn violations(&self) -> &[String] {
        &self.violations
    }

    /// The latest committed configuration.
    pub fn committed_config(&self) -> Config {
        self.conf_chain
    }

    /// The globally committed prefix of the log.
    pub fn committed(&self) -> &[Entry] {
        &self.committed
    }

    /// Ask the current leader to change the voter set to `members`. Returns `(leader, index)`
    /// of the joint-configuration entry, or `None` if there is no leader or it refuses.
    pub fn change_membership(&mut self, members: &[NodeId]) -> Option<(NodeId, Index)> {
        let (leader, _) = self.current_leaders().into_iter().max_by_key(|&(_, t)| t)?;
        let (index, outs) = self.nodes[leader].change_membership(members)?;
        self.exec(leader, outs);
        self.check_node(leader, false);
        Some((leader, index))
    }

    /// Submit `cmd` to the current leader (highest term if several), returning
    /// `(leader, index)`. `None` if no live leader exists.
    pub fn propose(&mut self, cmd: u64) -> Option<(NodeId, Index)> {
        let (leader, _) = self.current_leaders().into_iter().max_by_key(|&(_, t)| t)?;
        let (index, outs) = self.nodes[leader].propose(cmd)?;
        self.exec(leader, outs);
        self.check_node(leader, false);
        Some((leader, index))
    }

    /// Check the safety invariants that involve node `id`, which just changed:
    /// log matching, state-machine safety (committed entries never differ) and
    /// leader completeness (a leader holds every entry committed before its term).
    fn check_node(&mut self, id: NodeId, just_elected: bool) {
        let me = &self.nodes[id];
        let mut found = Vec::new();
        let now = self.sim.now();
        for (j, other) in self.nodes.iter().enumerate().filter(|&(j, _)| j != id) {
            // Log matching: the highest index with equal terms implies identical prefixes
            // (compared over the range both nodes still hold).
            let lo = me.snap_index.max(other.snap_index);
            let hi = me.last_index().min(other.last_index());
            if let Some(i) = (lo + 1..=hi)
                .rev()
                .find(|&i| me.entry(i).map(|e| e.term) == other.entry(i).map(|e| e.term))
            {
                // Entries below `from` were already compared and neither log has been
                // rewritten since, so only the new part needs checking.
                let (a, b) = (id.min(j), id.max(j));
                let (ra, rb) = if a == id {
                    (me.rewrites, other.rewrites)
                } else {
                    (other.rewrites, me.rewrites)
                };
                let (sa, sb, upto) = self.agreed[a][b];
                let from = if (sa, sb) == (ra, rb) { upto.max(lo) } else { lo };
                if (from + 1..=i).any(|k| me.entry(k) != other.entry(k)) {
                    found.push(format!(
                        "t={now}: log matching broken between {id} and {j} at {i}"
                    ));
                } else {
                    self.agreed[a][b] = (ra, rb, i);
                }
            }
        }
        // State machine safety / committed-prefix agreement.
        let upto = me.commit;
        if upto > me.last_index() {
            found.push(format!("t={now}: node {id} commit {upto} beyond its log"));
        } else {
            for i in me.snap_index + 1..=upto {
                let e = me.entry(i).expect("within log");
                match self.committed.get(i as usize - 1) {
                    Some(c) if *c != e => found.push(format!(
                        "t={now}: node {id} committed {e:?} at {i} but {c:?} was committed before"
                    )),
                    Some(_) => {}
                    None if self.committed.len() as Index == i - 1 => {
                        self.committed.push(e);
                        if let Some(c) = e.cmd.and_then(Config::decode) {
                            // Committed configs must walk stable -> joint -> stable, each step
                            // anchored at the previous one, so no two disjoint majorities exist.
                            let ok = if self.conf_chain.is_joint() {
                                !c.is_joint() && c.cur == self.conf_chain.cur
                            } else {
                                c.is_joint() && c.prev == self.conf_chain.cur
                            };
                            if !ok {
                                found.push(format!(
                                    "t={now}: committed config {c:?} at {i} does not follow {:?}",
                                    self.conf_chain
                                ));
                            }
                            self.conf_chain = c;
                        }
                    }
                    None => found.push(format!("t={now}: node {id} commits {i} past a gap")),
                }
            }
        }
        // A snapshot may only cover entries that were committed, with the committed term.
        if me.snap_index > 0 {
            match self.committed.get(me.snap_index as usize - 1) {
                Some(c) if c.term == me.snap_term => {}
                _ => found.push(format!(
                    "t={now}: node {id} snapshot at {} is not a committed prefix",
                    me.snap_index
                )),
            }
        }
        // Leader completeness, checked at the moment of election: everything committed so far
        // from an earlier term must be in the new leader's log. (Later commits by a newer
        // leader legitimately do not reach a stale, partitioned old leader.)
        if just_elected {
            for (i, c) in self.committed.iter().enumerate() {
                let idx = i as Index + 1;
                if c.term < me.term && idx > me.snap_index && me.entry(idx) != Some(*c) {
                    found.push(format!(
                        "t={now}: leader {id} (term {}) lacks committed entry {idx} {c:?}",
                        me.term
                    ));
                }
            }
        }
        self.violations.extend(found);
        self.apply_and_compact(id);
    }

    /// Feed newly committed entries to node `id`'s state machine and compact its log when
    /// enough has been applied since the last snapshot.
    fn apply_and_compact(&mut self, id: NodeId) {
        while self.applied[id] < self.nodes[id].commit {
            let i = self.applied[id] + 1;
            let Some(e) = self.nodes[id].entry(i) else { break };
            if let Some(r) = e
                .cmd
                .filter(|&c| Config::decode(c).is_none())
                .and_then(Request::decode)
            {
                self.machines[id].apply(r);
            }
            self.applied[id] = i;
        }
        let every = self.cfg.snapshot_every;
        if every > 0 && self.applied[id] - self.nodes[id].snap_index >= every {
            let data = self.machines[id].snapshot();
            let outs = self.nodes[id].compact(self.applied[id], data);
            self.exec(id, outs);
        }
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
        for k in 0..self.nodes.len() {
            self.agreed[id.min(k)][id.max(k)] = (0, 0, 0);
        }
        self.sim.disk(id).repair();
        let rec = recover(&self.sim.disk(id).read_records());
        self.machines[id] = if rec.snap_index > 0 {
            KvStore::restore(&rec.snap_data).expect("durable snapshot decodes")
        } else {
            KvStore::new()
        };
        self.applied[id] = rec.snap_index;
        let (mut node, outs) = Node::recovered_with(id, self.nodes.len(), self.genesis, rec);
        node.bug = self.cfg.bug;
        self.nodes[id] = node;
        self.exec(id, outs);
    }

    fn exec(&mut self, id: NodeId, outs: Vec<Out>) {
        for out in outs {
            match out {
                Out::Persist { hard, log } => {
                    if let Some((term, voted_for)) = hard {
                        self.sim.disk(id).append(&encode_hard(term, voted_for));
                    }
                    for op in log {
                        self.sim.disk(id).append(&encode_op(&op));
                    }
                    self.sim.fsync(id);
                }
                Out::Send(to, msg) => self.sim.send(id, to, msg),
                Out::Restore { index, data } => {
                    self.machines[id] = KvStore::restore(&data).expect("leader sent a decodable snapshot");
                    self.applied[id] = index;
                    self.snapshots_installed += 1;
                }
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
                let won = before != Role::Leader && self.nodes[node].role == Role::Leader;
                self.check_node(node, won);
                true
            }
            Some(Event::Deliver { from, to, msg }) => {
                let before = self.nodes[to].role;
                let outs = self.nodes[to].on_message(from, msg);
                self.exec(to, outs);
                self.note_leader(to, before);
                let won = before != Role::Leader && self.nodes[to].role == Role::Leader;
                self.check_node(to, won);
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
