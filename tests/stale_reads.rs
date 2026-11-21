//! Bounded-staleness reads: followers that recently heard from their leader answer Gets
//! locally; the checker enforces that a value was never overwritten for longer than the bound.

use seedraft::kv::{HistOp, KvHarness, Op, Reply, Workload};
use seedraft::lin::check_bounded_staleness;
use seedraft::runner::{sweep, Failure, RunConfig};
use seedraft::NetConfig;

fn op(id: usize, op: Op, invoke: u64, ret: Option<(u64, Reply)>) -> HistOp {
    HistOp {
        id,
        client: 0,
        op,
        invoke,
        ret,
    }
}

fn put(id: usize, val: u16, inv: u64, ret: u64) -> HistOp {
    op(id, Op::Put { key: 0, val }, inv, Some((ret, Reply::Ok)))
}

fn read(id: usize, at: u64, v: Option<u16>) -> HistOp {
    op(id, Op::Get { key: 0 }, at, Some((at, Reply::Value(v))))
}

#[test]
fn checker_accepts_values_within_the_bound() {
    // 1 was overwritten by 2 (returned at 30); a read at 100 with bound 100 may still see 1.
    let h = [put(0, 1, 0, 10), put(1, 2, 20, 30)];
    assert!(check_bounded_staleness(&h, &[read(0, 100, Some(1))], 100).is_ok());
    assert!(check_bounded_staleness(&h, &[read(1, 100, Some(2))], 100).is_ok());
    // The initial value is acceptable until a write has been in effect for the bound.
    assert!(check_bounded_staleness(&h, &[read(2, 15, None)], 10).is_ok());
}

#[test]
fn checker_rejects_values_overwritten_for_longer_than_the_bound() {
    let h = [put(0, 1, 0, 10), put(1, 2, 20, 30)];
    let v = check_bounded_staleness(&h, &[read(0, 100, Some(1))], 50).unwrap_err();
    assert_eq!((v.key, v.read), (0, 0));
    // The initial value, long gone.
    assert!(check_bounded_staleness(&h, &[read(0, 100, None)], 50).is_err());
    // A value nobody ever wrote.
    assert!(check_bounded_staleness(&h, &[read(0, 100, Some(9))], 1000).is_err());
}

#[test]
fn checker_does_not_accuse_on_overlapping_or_pending_writes() {
    // Writes of 1 and 2 overlap, so 1 may be the later one: not certainly overwritten.
    let h = [put(0, 1, 0, 40), put(1, 2, 10, 20)];
    assert!(check_bounded_staleness(&h, &[read(0, 500, Some(1))], 10).is_ok());
    // A write that never returned may take effect at any time.
    let h = [put(0, 1, 0, 10), op(1, Op::Put { key: 0, val: 2 }, 20, None)];
    assert!(check_bounded_staleness(&h, &[read(0, 500, Some(2))], 10).is_ok());
    // A failed Cas wrote nothing and does not count as an overwrite either.
    let h = [
        put(0, 1, 0, 10),
        op(
            1,
            Op::Cas {
                key: 0,
                expect: 7,
                new: 3,
            },
            20,
            Some((30, Reply::Cas(false))),
        ),
    ];
    assert!(check_bounded_staleness(&h, &[read(0, 500, Some(1))], 10).is_ok());
    assert!(check_bounded_staleness(&h, &[read(0, 500, Some(3))], 10).is_err());
}

fn cfg(bound: u64) -> RunConfig {
    RunConfig {
        stale_bound: bound,
        ..RunConfig::default()
    }
}

#[test]
fn followers_serve_a_share_of_reads() {
    let wl = Workload {
        stale_bound: 400,
        ..Workload::default()
    };
    let mut h = KvHarness::new(5, 5, NetConfig::default(), wl);
    h.run_until(4000);
    assert!(h.stale_reads() > 20, "stale reads: {}", h.stale_reads());
    assert_eq!(h.stale_history.ops().len() as u64, h.stale_reads());
    // They stay out of the linearizable history.
    assert!(h.history.ops().len() > 20);
}

#[test]
fn off_by_default() {
    let mut h = KvHarness::new(5, 5, NetConfig::default(), Workload::default());
    h.run_until(4000);
    assert_eq!(h.stale_reads(), 0);
}

#[test]
fn bound_holds_under_faults_at_several_sizes() {
    for bound in [20, 80, 600] {
        let (cov, bad) = sweep(0, 300, &cfg(bound), 2);
        assert!(
            bad.is_empty(),
            "bound {bound}: {:?}",
            bad.first().map(|o| (o.seed, &o.failure))
        );
        assert!(cov.ops > 0);
    }
}

#[test]
fn followers_do_lag_so_a_stricter_bound_is_caught() {
    // Serve with a 400-tick bound but audit against 5 ticks: followers really are behind
    // by more than that sometimes, and the checker must notice.
    let mut caught = 0;
    for seed in 0..20 {
        let wl = Workload {
            stale_bound: 400,
            ..Workload::default()
        };
        let mut h = KvHarness::new(seed, 5, NetConfig::default(), wl);
        h.run_until(4000);
        assert!(check_bounded_staleness(h.history.ops(), h.stale_history.ops(), 400).is_ok());
        if check_bounded_staleness(h.history.ops(), h.stale_history.ops(), 5).is_err() {
            caught += 1;
        }
    }
    assert!(caught > 0, "a 5-tick audit should flag some 400-tick-bound reads");
}

#[test]
fn failure_is_reported_as_stale_read() {
    use seedraft::runner::run_seed;
    // Display of the failure class (no real run produces one now that the rule is sound).
    let f = Failure::StaleRead {
        key: 1,
        read: 2,
        bound: 30,
    };
    assert_eq!(
        f.to_string(),
        "stale read 2 on key 1 older than the 30-tick bound"
    );
    assert!(run_seed(1, &cfg(100)).ok());
}

#[test]
fn sweep_reports_stale_read_counts() {
    let cfg = RunConfig {
        stale_bound: 80,
        ..RunConfig::default()
    };
    let (cov, bad) = sweep(0, 20, &cfg, 2);
    assert!(bad.is_empty());
    assert!(cov.stale_reads > 0 && cov.stale_reads < cov.ops);
    assert_eq!(cov.lease_reads, 0);

    let run = |extra: &[&str]| {
        let o = std::process::Command::new(env!("CARGO_BIN_EXE_seedraft"))
            .args(["sweep", "--seeds", "0..8", "--jobs", "2"])
            .args(extra)
            .output()
            .unwrap();
        String::from_utf8_lossy(&o.stdout).into_owned()
    };
    let out = run(&["--stale-reads", "80"]);
    assert!(out.contains("bounded-staleness reads by followers"), "{out}");
    assert!(!out.contains("served under a lease"), "{out}");
    assert!(!run(&[]).contains("reads: "));
}

#[test]
fn stale_reads_compose_with_membership() {
    let cfg = RunConfig {
        stale_bound: 80,
        membership: true,
        ..RunConfig::default()
    };
    let (cov, bad) = sweep(0, 300, &cfg, 2);
    assert!(bad.is_empty(), "{:?}", bad.first().map(|o| (o.seed, &o.failure)));
    assert!(cov.membership_ops > 0 && cov.stale_reads > 0);
}

#[test]
fn stale_reads_compose_with_leases_follower_reads_and_mild_skew() {
    let cfg = RunConfig {
        stale_bound: 80,
        lease: 140,
        clock_skew: 10,
        follower_reads: true,
        membership: true,
        ..RunConfig::default()
    };
    let (cov, bad) = sweep(0, 200, &cfg, 2);
    assert!(bad.is_empty(), "{:?}", bad.first().map(|o| (o.seed, &o.failure)));
    assert!(
        cov.stale_reads > 0 && cov.lease_reads > 0,
        "both read paths must be exercised"
    );
}

#[test]
fn stale_reads_under_heavy_skew_do_not_break_the_staleness_audit_or_hide_lease_bugs() {
    // Staleness is measured in ticks of the global sim clock, so node clock skew must not
    // produce false StaleRead failures; any failure under heavy skew must be a lease one.
    let cfg = RunConfig {
        stale_bound: 80,
        lease: 140,
        clock_skew: 90,
        ..RunConfig::default()
    };
    let (_, bad) = sweep(0, 100, &cfg, 2);
    for o in &bad {
        assert!(
            !matches!(o.failure, Some(Failure::StaleRead { .. })),
            "seed {}: {:?}",
            o.seed,
            o.failure
        );
    }
}

/// Reads kept off the leader (answered by followers/learners) in each read mode.
fn offloaded(cfg: &RunConfig) -> (u64, u64) {
    let (cov, bad) = sweep(0, 100, cfg, 2);
    assert!(bad.is_empty(), "{:?}", bad.first().map(|o| (o.seed, &o.failure)));
    (cov.follower_reads + cov.stale_reads, cov.lease_reads)
}

#[test]
fn read_modes_offload_progressively_more_reads_from_the_leader() {
    let base = RunConfig::default();
    let lease = RunConfig {
        lease: 140,
        ..base.clone()
    };
    let follower = RunConfig {
        follower_reads: true,
        ..lease.clone()
    };
    let stale = RunConfig {
        stale_bound: 80,
        ..base.clone()
    };

    assert_eq!(
        offloaded(&base),
        (0, 0),
        "plain mode serves every read through the log"
    );
    let (off, leased) = offloaded(&lease);
    assert!(off == 0 && leased > 0, "a lease alone keeps reads on the leader");
    let (off_f, _) = offloaded(&follower);
    assert!(off_f > 0, "follower reads move some leased reads off the leader");
    let (off_s, _) = offloaded(&stale);
    assert!(off_s > 0, "stale reads are served by followers");
}
