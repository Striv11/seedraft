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
   (* = implemented after the first diagram; every box now exists, see PLAN.md)
```
Key invariant: the simulator is single-threaded, never reads the wall clock, and all randomness comes from one seeded generator; events are ordered by `(time, insertion sequence)`.

Implemented so far: `rng` (SplitMix64), `sim` (virtual clock, event queue, network with delay, reorder, drop, duplicate, partition and heal, trace and trace hash), and (Day 2) `disk` (append-only record log with fsync, lost fsyncs, torn writes) plus node crash/restart in the sim (down nodes get no messages or timers; timers from before a crash never fire).

Day 3 adds `raft`: `Node` is a pure state machine (input -> list of `Out` actions) and `Cluster` runs nodes in the sim, fsyncing `currentTerm`/`votedFor` before any dependent message is sent. The cluster records every leader per term, which the tests use to check election safety.

Day 4 adds log replication: AppendEntries with prev-index/term consistency checks, conflict truncation, batching, `next_index` backoff, the election restriction (up-to-date log), commit only of current-term entries by counting, and a no-op on election. After every simulator step `Cluster` checks log matching, committed-prefix agreement (state machine safety) and leader completeness at election time, collecting `violations()`. 

Day 5 makes the log durable: `currentTerm`/`votedFor`, appended entries and truncations are write-ahead logged and fsynced before any dependent message is sent; a restarted node replays its disk (`raft::recover`). Tests crash leaders, followers and the whole cluster with torn writes over 1000 seeds. Limitation: fsync is assumed honest; a drive that lies about fsync can lose acknowledged entries and no WAL protocol survives that.

Day 6 adds `kv`: a replicated key-value store (put/get/cas) whose requests are bit-packed into Raft commands. A per-client session table (last seq + cached reply) makes retried and duplicated requests apply exactly once. `KvHarness` runs retrying clients against the cluster and a `Recorder` emits invoke/return operations (unanswered ones stay indeterminate) for the Day 7 checker. Reads go through the log, so they are linearizable by construction.

Day 7 adds `lin`: a WGL-style linearizability checker (`lin::check(&history)`). It splits the history per key, searches for a real-time-respecting order that replays against a register model, and memoises visited (linearized set, value) states. Unanswered ops are indeterminate: they may take effect any time after invocation or never. Tests hand-build good histories and reject stale reads, lost writes and vanishing pending writes, then check 200 simulated seeds with leader crashes and show that corrupting a real history is caught. Worst case is exponential in the number of mutually concurrent ops on one key.

Day 12 adds `timeline`: `seedraft run --seed N --timeline` replays the seed and draws one lane per node (`L` leader, `c` candidate, `.` follower, `X` down) with a fault row (`x` crash, `r` restart, `P` partition, `H` heal) and the highest term. Example (3 nodes, leader crashed at t=1500, node 1 partitioned at 3000):
```
fault          x         P     H   r
n0    .........XXXXXXXXXXXXXXXXXXXX...........
n1    .LLLLLLLLLLLLLLLLLLLLLLLL...............
n2    .....................cccccLLLLLLLLLLLLLL
term  0111111111111111111112334455555555555555
```
The render steps the sim in column-sized chunks, so its scheduling can differ slightly from the plain `run` of the same seed; it is deterministic on its own.

## Setup
Requires a stable Rust toolchain.
```
cargo test          # unit and determinism tests
bash check.sh       # fmt + clippy + tests (same as CI)
```

## Demo
Verified from a fresh `git clone` (release build ~5s, then every command below runs in seconds except the 10k sweep):

```
cargo run --release --bin seedraft -- sweep --seeds 0..10000  # 10k seeds, 0 failed (~43s on 2 cores)
cargo run --release --bin seedraft -- sweep --seeds 0..2000 --snapshot-every 8   # with log compaction, 0 failed
cargo run --release --bin seedraft -- run --seed 5            # fault schedule + verdict for one seed
cargo run --release --bin seedraft -- run --seed 5 --timeline # per-node lanes
cargo run --release --bin seedraft -- corpus                  # replay the seed corpus: 16 entries, 0 unexpected
cargo run --release --bin seedraft -- --help                  # all flags
```

Each seed gets a generated fault schedule (crashes keeping a majority up, partitions, restarts, heal), a client workload, then the Raft invariants and the linearizability checker run over the result. A failing seed is printed with `reproduce: seedraft run --seed N`.

### Speed, coverage and the seed corpus (Day 13)
`sweep` runs seeds on all cores (`--jobs J` to override). Each seed is still its own single-threaded simulator, so outcomes and failing-seed order do not depend on the thread count. It ends with a coverage line, e.g. for `0..10000`:

```
10000 seeds (0..10000), 1983195 ops checked, 0 failed
coverage: 22794 crashes, 22794 restarts, 16473 partitions, 16473 heals; 7259 seeds crashed during a partition, 9587 saw a re-election; 0 snapshots installed; 1983195/1983195 ops completed
```

`--snapshot-every 8` makes the snapshot count non-zero. Where the time went: the simulator rendered the whole trace to one `Debug` string to hash it (now hashed incrementally, and not stored during sweeps), and the log-matching invariant re-compared whole log prefixes on every step (now only the part not yet verified, reset whenever either log is truncated or replaced). Single-thread cost dropped from ~23 ms to ~5.4 ms per seed.

`corpus/seeds.txt` lists seeds worth keeping: ones that expose each injected bug, and the same seeds on the real protocol. `seedraft corpus` replays them (a bugged line must fail, a `none` line must pass); a test does the same in `cargo test`.

## Membership changes
`Cluster::with_members(seed, n, &[0,1,2], ..)` starts a universe of `n` machines of which only some are voters; `Cluster::change_membership(&[..])` asks the leader to move to a new voter set through the joint configuration C(old,new), as in the Raft thesis: the joint entry takes effect as soon as it is appended, needs a majority of *both* sets, and once it commits the leader appends the final C(new) entry. Config entries are ordinary log commands (top byte `0xFF`, so KV client id 255 is reserved), snapshots carry the configuration, and nodes outside their configuration never campaign and have their `RequestVote`s ignored, so a removed node cannot depose the cluster. A leader removed by the change steps down once C(new) commits. The simulator checks that committed configs form a valid stable -> joint -> stable chain. 

### Learners
`Cluster::add_learner(id)` / `remove_learner(id)` add or drop a non-voting learner with a single config entry (the config carries a third bitmask, so ids must be < 18). Learners receive the log and snapshots but count toward no quorum or lease, never campaign and have their votes ignored. A learner can only be promoted (`change_membership` including it) once it has caught up to the leader's commit point, so a cold node cannot stall the joint phase. The simulator's config-chain invariant also accepts stable -> stable learner changes and requires voters and learners to stay disjoint. Not modelled: a CLI flag for membership schedules.

## Bug injection and trace minimization
To prove the harness can find bugs, `RaftConfig::bug` (CLI `--bug NAME`) switches on a known protocol defect: `double-vote` (vote twice in a term), `skip-log-recency` (grant votes to candidates with stale logs) or `accept-stale-append` (handle AppendEntries from an older term) or `commit-old-term` (commit older-term entries by counting replicas, the Raft figure-8 bug; random fault sweeps do not reach it in 3000 seeds, so `tests/figure8.rs` pins it with a scripted message order). Panics inside a broken node are reported as failures too. `--shrink` then delta-debugs (ddmin) the failing seed's fault schedule, keeping the seed fixed and the failure class the same, down to a 1-minimal reproducer:

```
$ seedraft sweep --seeds 0..300 --bug skip-log-recency --shrink
  shrunk fault schedule 8 -> 2 faults in 15 runs:
    t=1205  partition [[0, 3, 4]]
    t=3000  heal
  still fails: raft invariant violated: ... leader 2 (term 10) lacks committed entry 112 ...
```
A fourth candidate, committing old-term entries by replica count (Raft figure 8), was dropped: random schedules did not trigger it in 4000 seeds, so it needs a targeted scenario rather than a sweep. Only the fault schedule is shrunk, not the workload.

## Design decisions & trade-offs
- Own SplitMix64 instead of the `rand` crate: zero dependencies and a stream that can never change under us, which matters for seed replay.
- Messages are formatted to strings in the trace so traces are comparable and printable; costs speed, simplifies debugging.
- Disk records are framed with length + checksum; crash keeps a random prefix of unsynced bytes, so recovery must tolerate a torn tail (`Disk::repair`).
- Partitions also drop messages already in flight, modelling a cable cut rather than a polite filter.

### Pre-vote
`RaftConfig::pre_vote` (CLI `--pre-vote`) makes a timed-out follower first ask peers whether they *would* vote for it (`PreVote`/`PreVoteReply`, no state change on the receiver), and only bump its term and campaign once a quorum agrees. Peers refuse while they have recently heard from a leader or have a newer log. An isolated node therefore stays at its old term instead of inflating it, and rejoining no longer deposes a healthy leader (`tests/prevote.rs` shows both behaviours, with a without-pre-vote control).

### Leader leases
`RaftConfig::lease` (CLI `--lease TICKS`) lets a leader answer `Get`s from its own state. Each `AppendEntries` carries the leader's local send time and the reply echoes it; the lease runs until the newest send time acknowledged by a quorum plus the lease length. A leader serves a read only if the lease is unexpired and it has committed an entry of its own term. Followers ignore `RequestVote` for one lease length after hearing from a leader, and a restarted node holds its vote for a full lease. Nodes get their clock pushed in by the cluster, so each can run fast or slow (`--clock-skew PCT`, a per-node rate error drawn from the seed). With sane clocks 2000 seeds pass; at `--lease 140 --clock-skew 90` the linearizability checker reports stale reads (e.g. `seedraft run --seed 34 --lease 140 --clock-skew 90`), and `tests/lease.rs` shows the same seed passing with perfect clocks. Reads that find no lease holder fall back to the log.

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
- [x] Bug injection and schedule minimizer (Day 11)
- [x] ASCII timeline (Day 12)
- [x] Parallel sweep, coverage stats, seed corpus (Day 13)
- [x] README demo verified from a fresh clone, `--help` (Day 14)

## License
MIT. See [LICENSE](LICENSE).
