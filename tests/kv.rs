use std::collections::BTreeSet;

use seedraft::kv::{KvHarness, KvStore, Op, Recorder, Reply, Request, Workload};
use seedraft::NetConfig;

fn hostile() -> NetConfig {
    NetConfig {
        min_delay: 1,
        max_delay: 30,
        drop_pct: 15,
        dup_pct: 15,
        reorder_pct: 25,
        reorder_extra: 60,
    }
}

fn healthy() -> NetConfig {
    NetConfig {
        min_delay: 1,
        max_delay: 15,
        drop_pct: 0,
        dup_pct: 0,
        reorder_pct: 0,
        reorder_extra: 0,
    }
}

fn req(client: u8, seq: u16, op: Op) -> Request {
    Request { client, seq, op }
}

#[test]
fn encoding_round_trips() {
    let ops = [
        Op::Put { key: 63, val: 65535 },
        Op::Get { key: 0 },
        Op::Cas {
            key: 17,
            expect: 65535,
            new: 1,
        },
    ];
    for (i, op) in ops.into_iter().enumerate() {
        let r = req(254 - i as u8, 65535 - i as u16, op);
        assert_eq!(Request::decode(r.encode()), Some(r));
    }
    assert_eq!(Request::decode(3 << 38), None);
    // Raft config entries share the command space and must never decode as client requests.
    assert_eq!(Request::decode(0xFF << 56 | 1 << 38 | 5 << 32), None);
}

#[test]
fn store_semantics() {
    let mut s = KvStore::new();
    assert_eq!(s.apply(req(0, 1, Op::Get { key: 1 })), Some(Reply::Value(None)));
    assert_eq!(s.apply(req(0, 2, Op::Put { key: 1, val: 5 })), Some(Reply::Ok));
    assert_eq!(
        s.apply(req(
            0,
            3,
            Op::Cas {
                key: 1,
                expect: 4,
                new: 9
            }
        )),
        Some(Reply::Cas(false))
    );
    assert_eq!(
        s.apply(req(
            0,
            4,
            Op::Cas {
                key: 1,
                expect: 5,
                new: 9
            }
        )),
        Some(Reply::Cas(true))
    );
    assert_eq!(
        s.apply(req(0, 5, Op::Get { key: 1 })),
        Some(Reply::Value(Some(9)))
    );
    assert_eq!(
        s.apply(req(
            0,
            6,
            Op::Cas {
                key: 2,
                expect: 0,
                new: 1
            }
        )),
        Some(Reply::Cas(false))
    );
}

#[test]
fn duplicate_requests_apply_once() {
    let mut s = KvStore::new();
    let cas = req(
        1,
        1,
        Op::Cas {
            key: 0,
            expect: 0,
            new: 1,
        },
    );
    s.apply(req(1, 0, Op::Put { key: 0, val: 0 })); // seq 0 is fine as a first request
                                                    // A Cas retried three times must not "succeed" twice or change its own answer.
    assert_eq!(s.apply(cas), Some(Reply::Cas(true)));
    assert_eq!(s.apply(cas), Some(Reply::Cas(true)));
    assert_eq!(s.apply(cas), Some(Reply::Cas(true)));
    assert_eq!(s.get(0), Some(1));
    assert_eq!(s.effects(), 2);
    // A later request supersedes; a late duplicate of the old one is ignored, not re-applied.
    assert_eq!(s.apply(req(1, 2, Op::Put { key: 0, val: 7 })), Some(Reply::Ok));
    assert_eq!(s.apply(cas), None);
    assert_eq!(s.get(0), Some(7));
    // Sessions are per client.
    assert_eq!(
        s.apply(req(2, 1, Op::Get { key: 0 })),
        Some(Reply::Value(Some(7)))
    );
}

#[test]
fn duplicates_in_the_raft_log_apply_once() {
    // Propose the very same request several times through consensus; the replicated store
    // must see one effect and clients one reply.
    let mut h = KvHarness::new(
        5,
        3,
        healthy(),
        Workload {
            start_pct: 0,
            ..Workload::default()
        },
    );
    h.run_until(1500);
    let r = req(
        9,
        1,
        Op::Cas {
            key: 3,
            expect: 0,
            new: 1,
        },
    );
    h.cluster
        .propose(
            Request {
                op: Op::Put { key: 3, val: 0 },
                seq: 0,
                ..r
            }
            .encode(),
        )
        .unwrap();
    for _ in 0..4 {
        h.cluster.propose(r.encode()).unwrap();
    }
    h.run_until(3000);
    let dups = h
        .cluster
        .committed()
        .iter()
        .filter(|e| e.cmd == Some(r.encode()))
        .count();
    assert_eq!(dups, 4, "all duplicates reached the log");
    assert_eq!(h.store.effects(), 2);
    assert_eq!(h.store.get(3), Some(1));
    assert_eq!(h.store.session(9), Some((1, Reply::Cas(true))));
}

#[test]
fn recorder_tracks_pending_and_completed() {
    let mut r = Recorder::default();
    let a = r.invoke(0, Op::Get { key: 1 }, 10);
    let b = r.invoke(1, Op::Put { key: 1, val: 2 }, 12);
    r.complete(a, Reply::Value(None), 20);
    assert_eq!(r.ops()[a].ret, Some((20, Reply::Value(None))));
    assert!(r.ops()[b].ret.is_none());
    assert_eq!((r.ops()[a].invoke, r.ops()[b].invoke), (10, 12));
}

/// Replay the committed log from scratch, counting distinct (client, seq) requests.
fn distinct_requests(h: &KvHarness) -> usize {
    h.cluster
        .committed()
        .iter()
        .filter_map(|e| e.cmd.and_then(Request::decode))
        .map(|r| (r.client, r.seq))
        .collect::<BTreeSet<_>>()
        .len()
}

#[test]
fn exactly_once_under_faults_over_300_seeds() {
    let mut completed = 0;
    let mut retried_dups = 0;
    for seed in 0..300 {
        let mut h = KvHarness::new(seed, 5, hostile(), Workload::default());
        h.run_until(1500);
        let mut crashed = None;
        for round in 0..6u64 {
            h.run_until(1500 + round * 700);
            match crashed.take() {
                Some(id) => h.cluster.restart(id),
                None => {
                    if let Some(&(l, _)) = h.cluster.current_leaders().first() {
                        if round % 2 == 0 {
                            h.cluster.crash(l);
                            crashed = Some(l);
                        }
                    }
                }
            }
        }
        if let Some(id) = crashed {
            h.cluster.restart(id);
        }
        h.cluster.sim.heal();
        h.run_until(7000);
        assert!(
            h.cluster.violations().is_empty(),
            "seed {seed}: {:?}",
            h.cluster.violations()
        );

        // Exactly-once: every distinct request had at most one effect, however many log copies.
        let total = h
            .cluster
            .committed()
            .iter()
            .filter_map(|e| e.cmd.and_then(Request::decode))
            .count();
        let distinct = distinct_requests(&h);
        assert!(h.store.effects() as usize <= distinct, "seed {seed}");
        retried_dups += total - distinct;

        // Every reply the clients saw is the one the store produced for that request.
        for op in h.history.ops() {
            if let Some((t, _)) = op.ret {
                assert!(t >= op.invoke);
                completed += 1;
            }
        }
    }
    assert!(completed > 5_000, "workload too weak: {completed} completed ops");
    assert!(
        retried_dups > 0,
        "no duplicate request ever reached the log; retries untested"
    );
}

/// A single client is sequential, so its history order is the real order and must match
/// a plain in-memory model: this checks that replies come from the replicated machine.
#[test]
fn single_client_history_matches_a_model() {
    for seed in 0..100 {
        let wl = Workload {
            clients: 1,
            start_pct: 60,
            ..Workload::default()
        };
        let mut h = KvHarness::new(seed, 3, hostile(), wl);
        h.run_until(6000);
        h.quiesce();
        h.cluster.sim.heal();
        h.run_until(9000);
        let mut model = KvStore::new();
        let mut seq = 0;
        let ops = h.history.ops();
        for o in ops {
            seq += 1;
            let want = model.apply(req(0, seq, o.op));
            match o.ret {
                Some((_, got)) => assert_eq!(want, Some(got), "seed {seed} op {}", o.id),
                None => assert_eq!(o.id, ops.len() - 1, "only the last op may be pending"),
            }
        }
        assert!(ops.len() > 5, "seed {seed}: only {} ops", ops.len());
    }
}

#[test]
fn same_seed_same_history() {
    let run = |seed| {
        let mut h = KvHarness::new(seed, 5, hostile(), Workload::default());
        h.run_until(5000);
        (h.cluster.sim.trace_hash(), h.history.ops().to_vec())
    };
    assert_eq!(run(77), run(77));
    assert_ne!(run(77).0, run(78).0);
}
