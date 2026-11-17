//! Follower/learner reads at the lease holder's read index: linearizable with sane clocks,
//! actually served by non-leaders, and still caught by the checker under heavy clock skew.

use seedraft::kv::{KvHarness, Workload};
use seedraft::raft::RaftConfig;
use seedraft::runner::{sweep, Failure, RunConfig};
use seedraft::NetConfig;

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

fn cfg(skew: i64, membership: bool) -> RunConfig {
    RunConfig {
        lease: 140,
        follower_reads: true,
        clock_skew: skew,
        membership,
        ..RunConfig::default()
    }
}

#[test]
fn non_leaders_serve_a_share_of_reads() {
    let wl = Workload {
        follower_reads: true,
        ..Workload::default()
    };
    let raft = RaftConfig {
        lease: 140,
        ..RaftConfig::default()
    };
    let mut h = KvHarness::with_raft(3, 5, healthy(), wl, raft);
    h.run_until(4000);
    assert!(h.lease_reads() > 20, "lease reads: {}", h.lease_reads());
    assert!(
        h.follower_reads() * 4 > h.lease_reads(),
        "a good share of reads should find a caught-up non-leader: {} of {}",
        h.follower_reads(),
        h.lease_reads()
    );
}

#[test]
fn without_the_flag_no_follower_serves_reads() {
    let raft = RaftConfig {
        lease: 140,
        ..RaftConfig::default()
    };
    let mut h = KvHarness::with_raft(3, 5, healthy(), Workload::default(), raft);
    h.run_until(4000);
    assert!(h.lease_reads() > 0);
    assert_eq!(h.follower_reads(), 0);
}

#[test]
fn follower_reads_are_linearizable_under_faults() {
    let (_, bad) = sweep(0, 300, &cfg(0, false), 2);
    assert!(bad.is_empty(), "{:?}", bad.first().map(|o| (o.seed, &o.failure)));
}

#[test]
fn follower_reads_compose_with_membership_and_mild_skew() {
    let (_, bad) = sweep(0, 300, &cfg(10, true), 2);
    assert!(bad.is_empty(), "{:?}", bad.first().map(|o| (o.seed, &o.failure)));
}

#[test]
fn heavy_skew_is_still_caught_as_a_stale_read() {
    let (_, bad) = sweep(0, 300, &cfg(90, false), 2);
    assert!(!bad.is_empty(), "skew 90 should break leases");
    assert!(bad
        .iter()
        .all(|o| matches!(o.failure, Some(Failure::NotLinearizable { .. }))));
}

#[test]
fn sweep_reports_read_serving_breakdown() {
    let (cov, bad) = sweep(0, 20, &cfg(0, false), 2);
    assert!(bad.is_empty());
    assert!(cov.follower_reads > 0 && cov.follower_reads <= cov.lease_reads);
    assert!(cov.lease_reads < cov.ops);

    let run = |extra: &[&str]| {
        let o = std::process::Command::new(env!("CARGO_BIN_EXE_seedraft"))
            .args(["sweep", "--seeds", "0..8", "--jobs", "2"])
            .args(extra)
            .output()
            .unwrap();
        String::from_utf8_lossy(&o.stdout).into_owned()
    };
    let out = run(&["--lease", "140", "--follower-reads"]);
    assert!(
        out.contains("reads: ") && out.contains("by followers/learners"),
        "{out}"
    );
    assert!(!run(&[]).contains("reads: "));
}
