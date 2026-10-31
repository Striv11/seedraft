use seedraft::raft::{Cluster, RaftConfig, Role};
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

fn hostile() -> NetConfig {
    NetConfig {
        min_delay: 1,
        max_delay: 30,
        drop_pct: 20,
        dup_pct: 10,
        reorder_pct: 25,
        reorder_extra: 60,
    }
}

fn assert_one_leader_per_term(c: &Cluster, seed: u64) {
    for (term, set) in c.leaders_by_term() {
        assert!(set.len() <= 1, "seed {seed}: term {term} had leaders {set:?}");
    }
}

#[test]
fn leader_elected_within_bound_on_healthy_net() {
    for seed in 0..1000 {
        let mut c = Cluster::new(seed, 5, healthy(), RaftConfig::default());
        c.run_until(1500);
        assert!(
            !c.current_leaders().is_empty(),
            "seed {seed}: no leader by t=1500"
        );
        assert_one_leader_per_term(&c, seed);
    }
}

#[test]
fn stable_leader_on_healthy_net() {
    for seed in 0..200 {
        let mut c = Cluster::new(seed, 5, healthy(), RaftConfig::default());
        c.run_until(1500);
        let first = c.current_leaders();
        assert_eq!(first.len(), 1, "seed {seed}");
        c.run_until(6000);
        assert_eq!(c.current_leaders(), first, "seed {seed}: leadership flapped");
    }
}

#[test]
fn reelection_after_leader_crash() {
    for seed in 0..1000 {
        let mut c = Cluster::new(seed, 5, healthy(), RaftConfig::default());
        c.run_until(1500);
        let (old, old_term) = c.current_leaders()[0];
        c.crash(old);
        c.run_until(4000);
        let leaders = c.current_leaders();
        assert_eq!(leaders.len(), 1, "seed {seed}: {leaders:?}");
        assert_ne!(leaders[0].0, old);
        assert!(leaders[0].1 > old_term, "seed {seed}: term did not advance");
        assert_one_leader_per_term(&c, seed);
    }
}

/// Chaos: lossy network, random crashes/restarts and partitions. Safety must always hold.
#[test]
fn at_most_one_leader_per_term_under_chaos() {
    let mut total_leaders = 0;
    for seed in 0..1000 {
        let mut c = Cluster::new(seed, 5, hostile(), RaftConfig::default());
        for round in 1..=40u64 {
            c.run_until(round * 100);
            let node = c.sim.rng().range(0, 4) as usize;
            match c.sim.rng().range(0, 4) {
                0 => c.crash(node),
                1 | 2 => c.restart(node),
                3 => {
                    let k = c.sim.rng().range(1, 2) as usize;
                    let a: Vec<usize> = (0..k).collect();
                    let b: Vec<usize> = (k..5).collect();
                    c.sim.partition(&[a, b]);
                }
                _ => c.sim.heal(),
            }
        }
        assert_one_leader_per_term(&c, seed);
        total_leaders += c.leaders_by_term().len();
    }
    assert!(
        total_leaders > 1000,
        "chaos barely elected anyone: {total_leaders}"
    );
}

#[test]
fn term_and_vote_survive_crash_restart() {
    let mut c = Cluster::new(7, 3, healthy(), RaftConfig::default());
    c.run_until(1000);
    let id = 1;
    let (term, vote) = (c.node(id).term(), c.node(id).voted_for());
    assert!(term > 0);
    c.crash(id);
    c.restart(id);
    assert_eq!((c.node(id).term(), c.node(id).voted_for()), (term, vote));
    assert_eq!(c.node(id).role(), Role::Follower);
}

#[test]
fn same_seed_same_election_history() {
    let run = |seed| {
        let mut c = Cluster::new(seed, 5, hostile(), RaftConfig::default());
        c.run_until(3000);
        (c.sim.trace_hash(), c.leaders_by_term().clone())
    };
    assert_eq!(run(5), run(5));
    assert_ne!(run(5).0, run(6).0);
}
