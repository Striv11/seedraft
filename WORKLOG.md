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
