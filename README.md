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

Day 4 adds log replication: AppendEntries with prev-index/term consistency checks, conflict truncation, batching, `next_index` backoff, the election restriction (up-to-date log), commit only of current-term entries by counting, and a no-op on election. After every simulator step `Cluster` checks log matching, committed-prefix agreement (state machine safety) and leader completeness at election time, collecting `violations()`. 

Day 5 makes the log durable: `currentTerm`/`votedFor`, appended entries and truncations are write-ahead logged and fsynced before any dependent message is sent; a restarted node replays its disk (`raft::recover`). Tests crash leaders, followers and the whole cluster with torn writes over 1000 seeds. Limitation: fsync is assumed honest; a drive that lies about fsync can lose acknowledged entries and no WAL protocol survives that.

Day 6 adds `kv`: a replicated key-value store (put/get/cas) whose requests are bit-packed into Raft commands. A per-client session table (last seq + cached reply) makes retried and duplicated requests apply exactly once. `KvHarness` runs retrying clients against the cluster and a `Recorder` emits invoke/return operations (unanswered ones stay indeterminate) for the Day 7 checker. Reads go through the log, so they are linearizable by construction.

Day 7 adds `lin`: a WGL-style linearizability checker (`lin::check(&history)`). It splits the history per key, searches for a real-time-respecting order that replays against a register model, and memoises visited (linearized set, value) states. Unanswered ops are indeterminate: they may take effect any time after invocation or never. Tests hand-build good histories and reject stale reads, lost writes and vanishing pending writes, then check 200 simulated seeds with leader crashes and show that corrupting a real history is caught. Worst case is exponential in the number of mutually concurrent ops on one key.

## Setup
Requires a stable Rust toolchain.
```
cargo test          # unit and determinism tests
bash check.sh       # fmt + clippy + tests (same as CI)
```

## Demo
Available now (Day 8):

```
cargo run --release --bin seedraft -- sweep --seeds 0..2000   # 2000 seeds, 0 failed (~20s)
cargo run --release --bin seedraft -- sweep --seeds 0..2000 --snapshot-every 8   # with log compaction, 0 failed
cargo run --release --bin seedraft -- run --seed 5            # fault schedule + verdict for one seed
```

Each seed gets a generated fault schedule (crashes keeping a majority up, partitions, restarts, heal), a client workload, then the Raft invariants and the linearizability checker run over the result. A failing seed is printed with `reproduce: seedraft run --seed N`.

Planned. Target: `seedraft sweep --seeds 0..5000` prints the first failing seed with a minimized event trace, and `seedraft timeline --seed N` draws an ASCII timeline of a partition and leader change.

## Membership changes
`Cluster::with_members(seed, n, &[0,1,2], ..)` starts a universe of `n` machines of which only some are voters; `Cluster::change_membership(&[..])` asks the leader to move to a new voter set through the joint configuration C(old,new), as in the Raft thesis: the joint entry takes effect as soon as it is appended, needs a majority of *both* sets, and once it commits the leader appends the final C(new) entry. Config entries are ordinary log commands (top byte `0xFF`, so KV client id 255 is reserved), snapshots carry the configuration, and nodes outside their configuration never campaign and have their `RequestVote`s ignored, so a removed node cannot depose the cluster. A leader removed by the change steps down once C(new) commits. The simulator checks that committed configs form a valid stable -> joint -> stable chain. Not modelled: non-voting learners (a freshly added node must catch up before the joint entry can commit) and a CLI flag for membership schedules.

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
- [x] Raft crash recovery (durable log, Day 5)
- [x] Replicated KV with client sessions and history recorder (Day 6)
- [x] Linearizability checker (Day 7)
- [x] Fault schedule generator and `run`/`sweep` CLI (Day 8)
- [x] Snapshots / log compaction and InstallSnapshot (Day 9)
- [x] Membership change via joint consensus (Day 10)
- [ ] Bug injection and minimizer, timeline

## License
MIT. See [LICENSE](LICENSE).
