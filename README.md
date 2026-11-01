# seedraft

> Raft consensus in Rust, torn apart by a FoundationDB-style deterministic simulator: one `u64` seed replays any failure exactly.

![CI](https://github.com/Striv11/seedraft/actions/workflows/ci.yml/badge.svg)

## Problem
Distributed consensus bugs hide in rare interleavings: a dropped message plus a crash plus a partition at the wrong tick. Ordinary tests and real-network chaos tests rarely reproduce them, so the bugs ship.

## Motivation
Deterministic simulation testing (FoundationDB, TigerBeetle) fixes this by making the whole world a pure function of a seed. This project builds that harness first, then a Raft implementation on top of it, then a linearizability checker that judges every simulated history. Ideas explored: virtual time, fault injection for network and disk, replayable traces, trace minimization, WGL-style linearizability checking.

## Architecture
```
            seed (u64)
               |
          +----v-----+   events    +-----------------+
          |   Sim    +------------>| Raft nodes      |
          | clock    |<------------+ (pure state     |
          | queue    |  send/timer |  machines)      |
          | rng      |             +--------+--------+
          | network  |                      | applies
          | disk     |             +--------v--------+
          +----+-----+             | KV + sessions*  |
               | trace             +--------+--------+
               v                            | history
        replayable trace *         +--------v--------+
                                   | linearizability |
                                   | checker *       |
                                   +-----------------+
   (* = planned, see PLAN.md)
```
Key invariant: the simulator is single-threaded, never reads the wall clock, and all randomness comes from one seeded generator; events are ordered by `(time, insertion sequence)`.

Implemented so far: `rng` (SplitMix64), `sim` (virtual clock, event queue, network with delay, reorder, drop, duplicate, partition and heal, trace and trace hash), and (Day 2) `disk` (append-only record log with fsync, lost fsyncs, torn writes) plus node crash/restart in the sim (down nodes get no messages or timers; timers from before a crash never fire).

Day 3 adds `raft`: `Node` is a pure state machine (input -> list of `Out` actions) and `Cluster` runs nodes in the sim, fsyncing `currentTerm`/`votedFor` before any dependent message is sent. The cluster records every leader per term, which the tests use to check election safety.

Day 4 adds log replication: AppendEntries with prev-index/term consistency checks, conflict truncation, batching, `next_index` backoff, the election restriction (up-to-date log), commit only of current-term entries by counting, and a no-op on election. After every simulator step `Cluster` checks log matching, committed-prefix agreement (state machine safety) and leader completeness at election time, collecting `violations()`. The log is in memory only until Day 5.

## Setup
Requires a stable Rust toolchain.
```
cargo test          # unit and determinism tests
bash check.sh       # fmt + clippy + tests (same as CI)
```

## Demo
Planned. Target: `seedraft sweep --seeds 0..5000` prints the first failing seed with a minimized event trace, and `seedraft timeline --seed N` draws an ASCII timeline of a partition and leader change.

## Design decisions & trade-offs
- Own SplitMix64 instead of the `rand` crate: zero dependencies and a stream that can never change under us, which matters for seed replay.
- Messages are formatted to strings in the trace so traces are comparable and printable; costs speed, simplifies debugging.
- Disk records are framed with length + checksum; crash keeps a random prefix of unsynced bytes, so recovery must tolerate a torn tail (`Disk::repair`).
- Partitions also drop messages already in flight, modelling a cable cut rather than a polite filter.

## Roadmap
See [PLAN.md](PLAN.md) for the 14-day arc.
- [x] Day 1: simulator core
- [x] Disk faults and crash-restart
- [x] Raft leader election (term/vote persisted)
- [x] Raft log replication and commit (Day 4)
- [ ] Raft crash recovery (durable log)
- [ ] KV with sessions, linearizability checker
- [ ] CLI sweep, snapshots, joint consensus, minimizer, timeline

## License
MIT. See [LICENSE](LICENSE).
