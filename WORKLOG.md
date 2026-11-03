# Work log

Newest entries at the bottom. Each entry records what actually changed, what was learned, and what comes next.

## 2026-10-30 — Day 1: simulator core
- Chose the seeded flagship: Raft + deterministic simulation testing, repo `seedraft`, 14-day plan in PLAN.md.
- Built `Rng` (SplitMix64, checked against the reference vector), and `Sim<M>`: virtual clock, `(time, seq)` ordered event queue, timers, network with delay/reorder/drop/duplicate, partitions that also kill in-flight messages, a trace and an FNV trace hash.
- Tests prove same seed => identical trace hash, deliveries and end time (including with a mid-run partition); different seeds diverge; faults really occur.
- Learned: reordering falls out of random delays plus an explicit extra-delay knob; tie-breaking by insertion sequence is what keeps equal-time events deterministic.
- Next (Day 2): simulated disk with fsync loss, torn writes, crash-restart.

## 2026-10-31 — Day 2: simulated disk and crash-restart
- Added `disk.rs`: per-node append-only log with checksummed framing, `fsync` that can silently lose data, and `crash` that drops unsynced bytes or keeps a random prefix (torn write). Reads stop at the first torn record; `repair` truncates the tail.
- `Sim` now owns each node's disk and node lifecycle: `crash`/`restart`/`fsync`, traced. Down nodes neither send nor receive; timers carry an incarnation epoch so pre-crash timers never fire after restart.
- Tests: unsynced loss, lost fsync, torn write is always a record prefix (200 seeds), repair, same-seed same disk state, and a crash-heavy sim workload that is trace-hash identical per seed.
- Learned: a per-node epoch on timers is simpler than purging the event queue on crash.
- Next (Day 3): Raft leader election on top of the sim; at most one leader per term over 1000 seeds.

## 2026-10-31 — Day 3: Raft leader election
- Added `raft.rs`: pure `Node` state machine (RequestVote, heartbeats, term handling, randomized election timeouts) returning ordered `Out` actions, and `Cluster` that runs it in the sim, persisting `(term, votedFor)` with fsync before dependent sends and recovering from disk on restart.
- Tests (`tests/election.rs`, 1000 seeds each): leader within 1500 ticks on a healthy net; re-election after leader crash with a higher term; at most one leader per term under drops/dup/reorder plus random crashes and partitions; term/vote survive restart; same seed => same history.
- Mutation check: making every node grant every vote immediately produces two leaders in one term, so the safety test has teeth.
- Learned: stale timers are handled with a generation number in the timer id, same idea as the sim's crash epochs. Leader-per-term must be recorded on the role transition, not after actions run.
- Caveat: persistence assumes fsync works; with fsync loss enabled votes can be forgotten (Day 5 addresses recovery under disk faults).
- Next (Day 4): log replication and commit, log matching invariant over 1000 seeds.

## 2026-11-01 — Day 4: log replication and commit
- `raft.rs`: log of `(term, cmd)` entries, AppendEntries with consistency check, idempotent append (duplicates/reordering never truncate), batching, next_index backoff with follower length hint, election restriction in RequestVote, leader no-op on election, commit by majority match restricted to current-term entries. `Cluster::propose` submits to the leader.
- `Cluster` checks invariants after every step: log matching, committed-prefix agreement across all nodes ever, leader completeness at election. Results in `violations()`.
- Tests (`tests/replication.rs`): 1000 seeds under drop/dup/reorder + partitions with proposals, no violations and >20k commits overall; healthy-net convergence; minority leader cannot commit and its stale entries get overwritten after heal; same-seed determinism.
- Mutation check: dropping the election restriction is caught by the invariants. Dropping the current-term-only commit rule is NOT caught yet: the figure-8 scenario needs crashes with a durable log, so that belongs with Day 5.
- Learned: my first leader-completeness check was wrong (flagged a stale partitioned leader for entries committed after its term); the property only holds at election time.
- Caveat: log is in-memory, so tests do not crash nodes yet.
- Next (Day 5): persist the log with fsync before acking, recover on restart, add a figure-8 crash test, no committed entry lost over 1000 seeds with crashes and fsync loss.

## 2026-11-01 — Day 5: persistence and crash recovery
- Log is now durable: `Node` records `LogOp`s (append, truncate-to) as it mutates the log, and every handler (and `propose`) emits one `Out::Persist { hard, log }` ahead of any send. `Cluster` appends WAL records, fsyncs, and `raft::recover` replays them on restart (term, vote, log).
- Tests (`tests/recovery.rs`): recovery round-trip equals live state and tolerates garbage records; crashed follower restarts with its exact log; 1000 seeds of random leader/follower crashes, partitions, hostile network and torn writes with per-step invariants and no violations; whole-cluster power loss keeps all committed entries; same-seed determinism with crashes.
- Mutation check: not persisting log ops makes 4 of 5 recovery tests fail.
- Learned: with a lying fsync (`fsync_loss_pct > 0`) no WAL protocol is safe, so the Day 5 criterion is tested with honest fsync and torn writes; documented as a limitation.
- Gotcha: reverting a mutation with `git checkout` wiped uncommitted work; I reapplied it. Commit before mutating.
- Still open: the figure-8 mutation (commit older-term entries by counting) is not yet shown to be caught; worth a targeted scenario test.
- Next (Day 6): replicated KV state machine with client sessions for exactly-once, history recorder.

## 2026-11-02 — Day 6: replicated KV with sessions
- Added `kv.rs`: `Request`/`Op` (put, get, cas) packed into the `u64` Raft command, `KvStore` with a per-client session table (retry of the latest seq returns the cached reply, older seq is ignored), `Recorder` producing invoke/return `HistOp`s, and `KvHarness` that drives retrying clients against a `Cluster` and applies the committed prefix in order.
- Tests (`tests/kv.rs`): encoding round-trip, store semantics, duplicates applied once (directly and through the Raft log), 300 seeds of hostile network plus leader crashes with no invariant violations and effects <= distinct requests, single-client history equals a sequential model, same seed => same history.
- Learned: `Sim::now()` only advances when an event fires, so a poll loop based on it can spin forever once the queue has no event before the target; the harness keeps its own clock. Also: `pkill -f` can match and kill your own shell.
- Reads go through the log (no read-index/lease optimisation); that is the stretch item.
- Next (Day 7): WGL-style linearizability checker over `HistOp`s, including indeterminate ops; reject stale read and lost write.

## 2026-11-03 — Day 7: linearizability checker
- Added `lin.rs`: per-key WGL search (real-time horizon, bitset of linearized ops, memo of failed `(set, value)` states). Pending ops are optional and unconstrained in reply; the search succeeds once every completed op is placed.
- Tests (`tests/lin.rs`): accepts sequential and concurrent histories; rejects stale read, lost write, time-travelling reads, a pending put observed then vanishing, effect before invocation, phantom values; 200 simulated seeds with leader crashes are linearizable; corrupting a real history's get reply is detected.
- Learned: my first overlap stress test used 40 concurrent puts; rejecting is exponential (2^n subsets) and ate 2GB. Memoisation bounds it, not eliminates it. Shrunk to 14 and documented. Also a test bug: expected a value no put wrote.
- Gotcha: `pkill -f` with a pattern that appears in my own command line kills the shell (again).
- Next (Day 8): fault schedule generator and `seedraft run` / `sweep` CLI that also runs the checker per seed.
