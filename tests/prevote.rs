use seedraft::raft::{Cluster, RaftConfig, Role};
use seedraft::runner::{run_seed, RunConfig};
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

fn cfg(pre_vote: bool) -> RaftConfig {
    RaftConfig {
        pre_vote,
        ..RaftConfig::default()
    }
}

type Leaders = Vec<(usize, u64)>;

/// Isolate one follower for a long time, heal, and report (max term seen, leader before/after).
fn isolate_follower(seed: u64, pre_vote: bool) -> (u64, u64, Leaders, Leaders) {
    let mut c = Cluster::new(seed, 5, healthy(), cfg(pre_vote));
    c.run_until(1500);
    let before = c.current_leaders();
    assert_eq!(before.len(), 1, "seed {seed}: no stable leader");
    let victim = (before[0].0 + 1) % 5;
    let rest: Vec<usize> = (0..5).filter(|&i| i != victim).collect();
    c.sim.partition(&[vec![victim], rest]);
    c.run_until(6000);
    let isolated_term = c.node(victim).term();
    c.sim.heal();
    c.run_until(9000);
    let after = c.current_leaders();
    assert!(c.violations().is_empty(), "seed {seed}: {:?}", c.violations());
    let final_term = (0..5).map(|i| c.node(i).term()).max().unwrap();
    (isolated_term, final_term, before, after)
}

#[test]
fn isolated_node_inflates_term_without_prevote() {
    // Sanity check that the scenario exercises the problem pre-vote solves.
    let (iso, _, _, _) = isolate_follower(1, false);
    assert!(iso > 5, "isolated node only reached term {iso}");
}

#[test]
fn isolated_node_does_not_inflate_term_or_depose_leader_with_prevote() {
    for seed in 0..100 {
        let (iso, fin, before, after) = isolate_follower(seed, true);
        assert_eq!(iso, before[0].1, "seed {seed}: isolated node bumped its term");
        assert_eq!(
            after, before,
            "seed {seed}: leader disturbed on heal (term {fin})"
        );
    }
}

#[test]
fn without_prevote_rejoining_node_disturbs_leader() {
    let disturbed = (0..20)
        .filter(|&s| {
            let (_, _, before, after) = isolate_follower(s, false);
            before != after
        })
        .count();
    assert!(
        disturbed > 0,
        "expected term inflation to depose the leader at least once"
    );
}

#[test]
fn election_and_reelection_still_work_with_prevote() {
    for seed in 0..300 {
        let mut c = Cluster::new(seed, 5, healthy(), cfg(true));
        c.run_until(1500);
        let l = c.current_leaders();
        assert_eq!(l.len(), 1, "seed {seed}: no leader");
        c.crash(l[0].0);
        c.run_until(4000);
        let l2 = c.current_leaders();
        assert!(
            l2.iter()
                .any(|&(id, _)| id != l[0].0 && c.node(id).role() == Role::Leader),
            "seed {seed}: no re-election"
        );
        for (term, set) in c.leaders_by_term() {
            assert!(set.len() <= 1, "seed {seed}: term {term} leaders {set:?}");
        }
    }
}

#[test]
fn full_sweep_passes_with_prevote() {
    let rc = RunConfig {
        pre_vote: true,
        ..RunConfig::default()
    };
    for seed in 0..150 {
        let o = run_seed(seed, &rc);
        assert!(o.failure.is_none(), "seed {seed}: {:?}", o.failure);
    }
}
