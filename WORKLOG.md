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
