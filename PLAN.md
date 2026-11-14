# Plan

Fourteen-day flagship arc. Every milestone has acceptance criteria that can be checked by a test or a command.

| Day | Milestone | Acceptance criteria | Status |
|-----|-----------|---------------------|--------|
| 1 | Simulator core: virtual clock, event queue, seeded RNG, faulty network | Identical seeds give identical traces (hash + deliveries); different seeds differ; time monotonic; drop/dup/reorder/partition tests; `bash check.sh` green | [x] |
| 2 | Simulated disk: write/fsync, fsync loss, torn writes, crash-restart of nodes | Unsynced writes vanish on crash; torn write yields prefix of a record; same seed => same disk state; node lifecycle (up/down) in sim | [x] |
| 3 | Raft leader election | Property test over 1000 seeds: at most one leader per term; leader elected within bound on healthy net; re-election after leader crash | [x] |
| 4 | Log replication + commit | Log matching and leader completeness invariants checked each step over 1000 seeds under drops/reorder/dup | [x] |
| 5 | Persistence and crash recovery | currentTerm/votedFor/log survive crash-restart with fsync loss; no committed entry lost across 1000 seeds with crashes (fsync honest; torn writes on) | [x] |
| 6 | Replicated KV state machine + client sessions (exactly-once) | Retried/duplicated client requests applied once; session dedupe table test; history recorder emits invoke/return ops | [x] |
| 7 | Linearizability checker (Porcupine/WGL-style) | Accepts hand-built linearizable histories, rejects known-bad ones (stale read, lost write); handles timeouts as indeterminate ops | [x] |
| 8 | Fault schedule generator + CLI `run`/`sweep` | `seedraft sweep --seeds 0..2000` runs all seeds, reports failing seed; reproducible with `seedraft run --seed N` | [x] |
| 9 | Snapshots / log compaction + InstallSnapshot | Followers far behind catch up via snapshot; state machine equals uncompacted run; checker still passes | [x] |
| 10 | Membership change via joint consensus | Add/remove node under partitions with no two disjoint majorities; invariants over 1000 seeds | [x] |
| 11 | Bug injection and trace minimization | Seeded known bugs (e.g. skip term check) are caught; failing trace shrunk by delta-debugging to a smaller reproducer | [x] |
| 12 | ASCII timeline renderer | Partition + leader change rendered as per-node lanes; snapshot test of output | [x] |
| 13 | Performance and coverage of the fault space | 10k seeds in under 60s release; fault-injection stats reported; fuzz-style seed corpus of past failures | [x] |
| 14 | Polish: README demo with real output, docs, release tag | Demo section verified from a fresh clone in under two minutes; roadmap updated | [x] |

## Stretch
- [x] Pre-vote (Day 16, `tests/prevote.rs`).
- [x] Leader leases (checker catches stale lease reads under clock skew; Day 17, `tests/lease.rs`).
- [x] Parallel sweep via per-thread simulators (each still single-threaded and deterministic) (Day 13).
- [x] Non-voting learners with catch-up-gated promotion (Day 18, `tests/learners.rs`).
- [x] CLI `--membership` flag: membership changes inside fault schedules (Day 19, `tests/membership_sweep.rs`).
- [x] Membership + leases + skew in one sweep (Day 20, `tests/membership_sweep.rs`).
- Done earlier: a targeted scenario for the Raft figure-8 bug (done, Day 15: `tests/figure8.rs`, `commit-old-term` bug).
