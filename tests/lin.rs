use seedraft::kv::{HistOp, KvHarness, Op, Reply, Workload};
use seedraft::lin::check;
use seedraft::sim::NetConfig;

fn op(id: usize, client: u8, op: Op, invoke: u64, ret: Option<(u64, Reply)>) -> HistOp {
    HistOp {
        id,
        client,
        op,
        invoke,
        ret,
    }
}
const PUT1: Op = Op::Put { key: 1, val: 1 };
const PUT2: Op = Op::Put { key: 1, val: 2 };
const GET: Op = Op::Get { key: 1 };

#[test]
fn accepts_sequential_history() {
    let h = [
        op(0, 0, GET, 0, Some((1, Reply::Value(None)))),
        op(1, 0, PUT1, 2, Some((3, Reply::Ok))),
        op(2, 0, GET, 4, Some((5, Reply::Value(Some(1))))),
        op(
            3,
            0,
            Op::Cas {
                key: 1,
                expect: 1,
                new: 9,
            },
            6,
            Some((7, Reply::Cas(true))),
        ),
        op(
            4,
            0,
            Op::Cas {
                key: 1,
                expect: 1,
                new: 5,
            },
            8,
            Some((9, Reply::Cas(false))),
        ),
    ];
    assert!(check(&h).is_ok());
}

#[test]
fn accepts_concurrent_reads_either_way() {
    // get overlaps the put, so both None and Some(1) are fine.
    for seen in [None, Some(1)] {
        let h = [
            op(0, 0, PUT1, 0, Some((10, Reply::Ok))),
            op(1, 1, GET, 5, Some((6, Reply::Value(seen)))),
        ];
        assert!(check(&h).is_ok(), "{seen:?}");
    }
}

#[test]
fn rejects_stale_read() {
    let h = [
        op(0, 0, PUT1, 0, Some((1, Reply::Ok))),
        op(1, 0, PUT2, 2, Some((3, Reply::Ok))),
        op(2, 1, GET, 4, Some((5, Reply::Value(Some(1))))),
    ];
    let v = check(&h).unwrap_err();
    assert_eq!(v.key, 1);
}

#[test]
fn rejects_lost_write() {
    let h = [
        op(0, 0, PUT1, 0, Some((1, Reply::Ok))),
        op(1, 1, GET, 2, Some((3, Reply::Value(None)))),
    ];
    assert!(check(&h).is_err());
}

#[test]
fn rejects_reads_that_go_back_in_time() {
    // Two overlapping puts; one client sees 2 then 1 while another sees 1 then 2 afterwards.
    let h = [
        op(0, 0, PUT1, 0, Some((20, Reply::Ok))),
        op(1, 1, PUT2, 0, Some((20, Reply::Ok))),
        op(2, 2, GET, 21, Some((22, Reply::Value(Some(2))))),
        op(3, 2, GET, 23, Some((24, Reply::Value(Some(1))))),
    ];
    assert!(check(&h).is_err());
}

#[test]
fn pending_ops_are_indeterminate() {
    // A put that never returned may have taken effect...
    let seen = [
        op(0, 0, PUT1, 0, None),
        op(1, 1, GET, 5, Some((6, Reply::Value(Some(1))))),
    ];
    assert!(check(&seen).is_ok());
    // ...or not...
    let unseen = [
        op(0, 0, PUT1, 0, None),
        op(1, 1, GET, 5, Some((6, Reply::Value(None)))),
    ];
    assert!(check(&unseen).is_ok());
    // ...but it cannot take effect before it was invoked.
    let early = [
        op(0, 1, GET, 0, Some((1, Reply::Value(Some(1))))),
        op(1, 0, PUT1, 5, None),
    ];
    assert!(check(&early).is_err());
    // A pending put cannot explain a value nobody wrote.
    let phantom = [
        op(0, 0, PUT1, 0, None),
        op(1, 1, GET, 5, Some((6, Reply::Value(Some(7))))),
    ];
    assert!(check(&phantom).is_err());
}

#[test]
fn pending_effect_is_all_or_nothing_across_readers() {
    // Once a reader saw the pending put's value, a later reader cannot see it vanish.
    let h = [
        op(0, 0, PUT1, 0, None),
        op(1, 1, GET, 5, Some((6, Reply::Value(Some(1))))),
        op(2, 1, GET, 7, Some((8, Reply::Value(None)))),
    ];
    assert!(check(&h).is_err());
}

#[test]
fn keys_are_independent() {
    let h = [
        op(0, 0, Op::Put { key: 1, val: 1 }, 0, Some((1, Reply::Ok))),
        op(1, 0, Op::Get { key: 2 }, 2, Some((3, Reply::Value(None)))),
    ];
    assert!(check(&h).is_ok());
}

#[test]
fn many_concurrent_ops_stay_tractable() {
    // 14 fully-overlapping puts then a read. Rejection must exhaust all 2^n subsets, so n stays small.
    let mut h = Vec::new();
    for i in 0..14usize {
        h.push(op(
            i,
            i as u8,
            Op::Put {
                key: 1,
                val: i as u16,
            },
            0,
            Some((100, Reply::Ok)),
        ));
    }
    h.push(op(14, 99, GET, 101, Some((102, Reply::Value(Some(7))))));
    assert!(check(&h).is_ok());
    h[14] = op(14, 99, GET, 101, Some((102, Reply::Value(Some(500)))));
    assert!(check(&h).is_err());
}

fn hostile() -> NetConfig {
    NetConfig {
        drop_pct: 10,
        dup_pct: 5,
        ..NetConfig::default()
    }
}

#[test]
fn simulated_histories_are_linearizable_over_200_seeds() {
    let mut total = 0;
    for seed in 0..200 {
        let mut h = KvHarness::new(seed, 5, hostile(), Workload::default());
        h.run_until(1500);
        for round in 0..4u64 {
            h.run_until(1500 + round * 700);
            if round % 2 == 0 {
                if let Some(&(l, _)) = h.cluster.current_leaders().first() {
                    h.cluster.crash(l);
                    h.run_until(1500 + round * 700 + 300);
                    h.cluster.restart(l);
                }
            }
        }
        h.cluster.sim.heal();
        h.run_until(6000);
        let ops = h.history.ops();
        total += ops.len();
        if let Err(v) = check(ops) {
            panic!("seed {seed}: not linearizable on key {}: ops {:?}", v.key, v.ops);
        }
    }
    assert!(total > 3000, "workload too weak: {total} ops");
}

#[test]
fn corrupting_a_simulated_history_is_detected() {
    let mut h = KvHarness::new(5, 3, NetConfig::default(), Workload::default());
    h.run_until(4000);
    let mut ops = h.history.ops().to_vec();
    assert!(check(&ops).is_ok());
    // Make a completed get claim a value no one ever wrote.
    let i = ops
        .iter()
        .position(|o| matches!(o.op, Op::Get { .. }) && o.ret.is_some())
        .expect("a completed get");
    let t = ops[i].ret.unwrap().0;
    ops[i].ret = Some((t, Reply::Value(Some(u16::MAX))));
    assert!(check(&ops).is_err());
}
