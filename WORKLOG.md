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

## 2026-11-03 — Day 8: fault schedules and CLI
- Added `runner.rs`: `schedule(seed)` generates crashes/restarts/partitions/heals from its own RNG stream (never more than a minority down, everything undone by the end of the fault window); `run_seed` runs workload + faults, drains, then checks Raft invariants and linearizability. Added `src/bin/seedraft.rs` with `run --seed N` and `sweep --seeds A..B` (exit 1 and a reproduce hint on failure).
- Tests (`tests/runner.rs`): schedule determinism, majority-up/heal-all properties over 500 seeds, run reproducibility, 60 seeds pass with real progress, CLI sweep/run/bad usage.
- Result: `sweep --seeds 0..2000` in release takes ~20s, 0 failures, ~400k ops checked.
- Learned: separate RNG stream for the schedule keeps schedules stable even if the simulation's RNG consumption changes. Failure reporting path is untested end to end until Day 11 bug injection provides a real failing seed.
- Next (Day 9): snapshots / log compaction and InstallSnapshot.

## 2026-11-04 — Day 9: snapshots and InstallSnapshot
- Raft log is now offset by `snap_index`; all index math goes through `entry(i)`/`term_at(i)`. `RaftConfig::snapshot_every` (0 = off, the default, so existing traces are unchanged) makes the cluster snapshot a node's KV state machine once that many applied entries accumulate and drop the covered log prefix (`Node::compact`).
- Leaders send `InstallSnapshot` when a follower's `next_index` falls inside the compacted prefix; followers keep their log suffix only if it agrees with the snapshot's last term, and emit `Out::Restore` so the cluster swaps the state machine. Snapshots are a WAL record (`REC_SNAP`) replayed by `recover`; torn snapshot records fall back to the earlier full log.
- Each node now owns a `KvStore` fed by its own commits (`Cluster::machine`); invariants were rewritten for absolute indices, plus a new check that every snapshot covers a committed prefix with the committed term.
- Tests (`tests/snapshot.rs`): KvStore snapshot round-trip/garbage rejection, lagging follower catches up via snapshot over 40 seeds, every node's state equals replay of the committed prefix over 60 faulty seeds, restart from durable snapshot with torn writes, 200 full linearizability runs with compaction. `sweep --seeds 0..2000 --snapshot-every 8`: 0 failures.
- Limits: the on-disk WAL is not physically truncated (the sim disk has no rewrite op), so compaction bounds memory and message size but not simulated disk records; snapshots go in one message, no chunking.
- Next (Day 10): membership change via joint consensus.

## 2026-11-04 — Day 10: membership change via joint consensus
- `Config { cur, prev }` (voter bitmasks; `prev != 0` = joint) lives in the log as tagged commands, so WAL and AppendEntries are unchanged. A node acts under the last config entry in its log (committed or not), else its snapshot's, else genesis. Quorums (votes and commit index) need a majority of every active set. `Node::change_membership` appends C(old,new) (one change at a time); the leader appends C(new) when the joint entry commits and steps down if C(new) excludes it.
- Snapshots (`LogOp::Snapshot`, `REC_SNAP`, `InstallSnapshot`) now carry the config, so compaction and late joiners keep it. Non-voters never campaign; `RequestVote` from outside our config is ignored, which stops removed-but-unaware nodes from inflating terms.
- Cluster: `with_members`, `change_membership`, `committed_config`, plus an invariant that committed configs form stable -> joint -> stable with matching anchors.
- Tests (`tests/membership.rs`): quorum maths, grow 3->5, shrink 5->3 incl. removing the leader, removed node partitioned away can't disrupt, 1000 seeds of changes mixed with crashes/partitions/hostile net (all invariants + one leader per term), config survives compaction/restart/snapshot install. Mutation check: ignoring the old majority in joint mode makes the quorum and fault tests fail.
- Learned: the fault test initially passed vacuously because no leader existed at the first step (changes were never accepted); asserting a minimum number of completed changes caught it.
- Limits: no learner phase; the sweep CLI does not schedule membership changes yet.
- Next (Day 11): bug injection and trace minimization.

## 2026-11-05 — Day 11: bug injection and trace minimization
- `Bug` enum (`double-vote`, `skip-log-recency`, `accept-stale-append`) wired into `Node` via `RaftConfig::bug`; CLI `--bug NAME`. `run_schedule` runs an explicit fault schedule and turns panics into `Failure::Panic`, since broken nodes hit bad indices.
- `shrink::shrink`: ddmin over the fault schedule with seed fixed and failure class preserved. Typical result: 8 faults -> 2-3. `--shrink` on `run`/`sweep` prints it. `Failure` display now truncates the (tens of thousands of) repeated invariant violations to the first plus a count.
- Tests (`tests/bugs.rs`): each bug caught within a small seed budget, same seeds pass unbugged, shrunk schedule is smaller, still fails, 1-minimal and deterministic, CLI flags.
- Learned: a figure-8 "commit old terms" bug was not caught by 4000 random seeds, so I removed it rather than ship an undetected one; it needs a targeted scenario. Restarting a node that is up is a no-op yet can still be kept by ddmin, because extra `run_until` stops perturb scheduling.
- Limits: only the schedule is shrunk (not workload, node count or time).
- Next (Day 12): ASCII timeline renderer.

## 2026-11-06 — Day 12: ASCII timeline renderer
- `timeline::render(seed, cfg, schedule, cols)`: per-node lanes (leader/candidate/follower/down), fault row, max-term row; `--timeline` on `seedraft run`. Columns with several different faults show `*`.
- Tests (`tests/timeline.rs`): determinism, shape and markers, leader change visible after a leader crash, golden snapshot in `tests/golden/timeline_seed7.txt`.
- Learned: sampling by `run_until` per column perturbs scheduling relative to a plain run (same effect as the ddmin note on Day 11), so the timeline is a faithful picture of its own replay, not byte-identical to the sweep's trace; documented in the README.
- Limits: sampled view, so short elections between samples can be missed; no message-level arrows.
- Note: the debug-mode suite takes ~13 min (kv and lin dominate); worth speeding up on Day 13.
- Next (Day 13): performance and fault-space coverage stats, seed corpus.

## 2026-11-07 — Day 13: performance, coverage stats, seed corpus
- Speed: 10k seeds now take ~43s in release on 2 cores (was ~230s extrapolated). Two causes found by timing experiments (no `perf` here): `trace_hash` formatted the entire trace into one string, and `check_node` recompared whole log prefixes for every node pair on every step. Trace is now hashed incrementally per entry (same fingerprint whether or not entries are stored; sweeps do not store them), and the log-matching check keeps a per-pair "verified through index N" watermark, invalidated by a per-node `rewrites` counter bumped on truncate / snapshot-install and reset on restart. ~23 ms -> ~5.4 ms per seed single-threaded.
- `runner::sweep` + `--jobs`: per-thread simulators over an atomic seed counter, results in seed order; `Coverage` stats (crashes, partitions, crashes-during-partition, re-elections, snapshots installed, ops) printed after every sweep.
- Seed corpus: `corpus/seeds.txt`, `corpus::{parse, replay}`, `seedraft corpus`. Seeds found by sweeping each injected bug; plus the same seeds on the real protocol.
- Tests (`tests/coverage.rs`): hash independent of recording, parallel == sequential (including failing seeds), coverage arithmetic, corpus parsing/replay/not-caught detection, CLI. The existing bug tests still catch the injected log-matching bug, which exercises the incremental check.
- Learned: 2 threads gave only ~1.25x over 1 here (shared cores), so the single-thread wins mattered more than parallelism. Corpus entries are shape-sensitive: seed 6 fails under skip-log-recency with 5 nodes but not 3.
- Limits: the 10k < 60s target is not enforced by a test (debug-mode timing is meaningless); no seed-time histogram or message-level coverage. Debug-mode test suite is still slow.
- Next (Day 14): README demo verified from a fresh clone, docs polish, release tag.
