//! Leader leases: local reads are linearizable with sane clocks, and the checker catches the
//! stale reads that clock skew produces.

use seedraft::kv::{KvHarness, Workload};
use seedraft::raft::{Cluster, RaftConfig};
use seedraft::runner::{run_seed, sweep, Failure, RunConfig};
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

fn leased(lease: u64) -> RaftConfig {
    RaftConfig {
        lease,
        ..RaftConfig::default()
    }
}

#[test]
fn only_a_current_leader_holds_a_lease_and_it_expires_when_cut_off() {
    let mut c = Cluster::new(7, 5, healthy(), leased(100));
    c.run_until(1500);
    let leader = c.current_leaders()[0].0;
    assert_eq!(
        c.lease_holders(1500),
        vec![leader],
        "exactly the leader holds a lease"
    );

    // Isolate the leader: without acks its lease must lapse within `lease` ticks of the last
    // acked send, even though it still believes it is leader.
    let rest: Vec<usize> = (0..5).filter(|&n| n != leader).collect();
    c.sim.partition(&[vec![leader], rest]);
    c.run_until(1500 + 20);
    c.run_until(1500 + 130);
    assert!(
        !c.lease_holders(1500 + 130).contains(&leader),
        "isolated leader still serving reads after the lease ran out"
    );
}

#[test]
fn followers_withhold_votes_until_the_old_leaders_lease_runs_out() {
    let mut c = Cluster::new(3, 5, healthy(), leased(3000));
    c.run_until(1500);
    let leader = c.current_leaders()[0].0;
    c.crash(leader);
    // Everyone just heard the leader, so for ~3000 ticks nobody may grant a vote.
    c.run_until(1500 + 2500);
    assert!(
        c.current_leaders().is_empty(),
        "elected a leader inside the old lease"
    );
    c.run_until(1500 + 3000 + 1500);
    assert_eq!(
        c.current_leaders().len(),
        1,
        "no new leader after the lease ended"
    );
    assert!(c.violations().is_empty());
}

#[test]
fn lease_reads_are_served_locally_and_stay_linearizable() {
    let mut h = KvHarness::with_raft(11, 5, healthy(), Workload::default(), leased(100));
    h.run_until(3000);
    h.quiesce();
    h.run_until(4000);
    assert!(
        h.lease_reads() > 50,
        "lease path unused: {} reads",
        h.lease_reads()
    );
    assert!(seedraft::lin::check(h.history.ops()).is_ok());
}

#[test]
fn perfect_clocks_survive_the_full_fault_sweep() {
    let cfg = RunConfig {
        lease: 140,
        ..RunConfig::default()
    };
    let (_, bad) = sweep(0, 300, &cfg, 2);
    assert!(
        bad.is_empty(),
        "failing seeds: {:?}",
        bad.iter().map(|o| o.seed).collect::<Vec<_>>()
    );
}

#[test]
fn small_skew_within_the_election_margin_is_tolerated() {
    let cfg = RunConfig {
        lease: 100,
        clock_skew: 20,
        ..RunConfig::default()
    };
    let (_, bad) = sweep(0, 300, &cfg, 2);
    assert!(
        bad.is_empty(),
        "failing seeds: {:?}",
        bad.iter().map(|o| o.seed).collect::<Vec<_>>()
    );
}

#[test]
fn checker_catches_stale_lease_reads_under_large_clock_skew() {
    let skewed = RunConfig {
        lease: 140,
        clock_skew: 90,
        ..RunConfig::default()
    };
    let (_, bad) = sweep(0, 300, &skewed, 2);
    assert!(!bad.is_empty(), "no stale read found in 300 skewed seeds");
    assert!(bad
        .iter()
        .all(|o| matches!(o.failure, Some(Failure::NotLinearizable { .. }))));
    // Same seed, same schedule, perfect clocks: fine. The skew is the culprit.
    let sane = RunConfig {
        clock_skew: 0,
        ..skewed.clone()
    };
    let seed = bad[0].seed;
    assert!(
        run_seed(seed, &sane).ok(),
        "seed {seed} fails even with perfect clocks"
    );
    assert!(!run_seed(seed, &skewed).ok());
}
