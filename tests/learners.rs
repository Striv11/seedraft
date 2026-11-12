//! Day 18: non-voting learners.

use seedraft::raft::{Cluster, Config, RaftConfig, Role};
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
        drop_pct: 15,
        dup_pct: 10,
        reorder_pct: 25,
        reorder_extra: 60,
    }
}

fn assert_ok(c: &Cluster, seed: u64) {
    assert!(c.violations().is_empty(), "seed {seed}: {:?}", c.violations());
}

fn settle(c: &mut Cluster, ticks: u64) {
    let t = c.sim.now() + ticks;
    c.run_until(t);
}

/// Retry `f` (e.g. waiting out an election or a pending config change) until it is accepted.
fn until_accepted(c: &mut Cluster, deadline: u64, mut f: impl FnMut(&mut Cluster) -> bool) -> bool {
    while c.sim.now() < deadline {
        if f(c) {
            return true;
        }
        settle(c, 25);
    }
    false
}

#[test]
fn learner_encoding_roundtrip() {
    let c = Config {
        cur: 0b111,
        prev: 0b11100,
        learners: 0b110_0000,
    };
    assert_eq!(Config::decode(c.encode()), Some(c));
    assert!(c.replicates(5) && c.is_learner(5) && !c.contains(5));
    assert!(!c.replicates(7));
    // Learners never help a quorum.
    let set = [5usize, 6].into_iter().collect();
    assert!(!c.has_quorum(&set));
}

#[test]
fn learner_catches_up_but_never_campaigns_or_leads() {
    for seed in 0..60 {
        let mut c = Cluster::with_members(seed, 4, &[0, 1, 2], healthy(), RaftConfig::default());
        c.run_until(1500);
        for cmd in 1..=6 {
            c.propose(cmd).expect("leader");
            settle(&mut c, 30);
        }
        assert!(
            until_accepted(&mut c, 5000, |c| c.add_learner(3).is_some()),
            "seed {seed}"
        );
        settle(&mut c, 1500);
        assert_ok(&c, seed);
        let total = c.committed().len() as u64;
        assert_eq!(c.node(3).commit_index(), total, "seed {seed}: learner caught up");
        assert!(c.node(3).config().is_learner(3));
        assert_eq!(c.node(3).role(), Role::Follower);
        // Even with every voter's heartbeats lost for a long time it must not start elections.
        c.sim.partition(&[vec![3], vec![0, 1, 2]]);
        settle(&mut c, 3000);
        assert_ok(&c, seed);
        assert_eq!(c.node(3).role(), Role::Follower);
        assert!(
            c.leaders_by_term().values().all(|l| !l.contains(&3)),
            "seed {seed}"
        );
        assert!(
            c.node(3).term() <= c.node(0).term(),
            "seed {seed}: learner inflated its term"
        );
    }
}

#[test]
fn learners_do_not_count_toward_quorum() {
    for seed in 0..40 {
        let mut c = Cluster::with_members(seed, 5, &[0, 1, 2], healthy(), RaftConfig::default());
        c.run_until(1500);
        assert!(until_accepted(&mut c, 4000, |c| c.add_learner(3).is_some()));
        assert!(until_accepted(&mut c, 6000, |c| c.add_learner(4).is_some()));
        settle(&mut c, 800);
        let (leader, _) = c.current_leaders()[0];
        // Take both other voters away: the leader plus two learners is 3 of 5 nodes, but only
        // 1 of 3 voters, so nothing may commit.
        let others: Vec<usize> = (0..3).filter(|&i| i != leader).collect();
        for &o in &others {
            c.crash(o);
        }
        let before = c.committed().len();
        let _ = c.propose(77);
        settle(&mut c, 1500);
        assert_ok(&c, seed);
        assert_eq!(
            c.committed().len(),
            before,
            "seed {seed}: committed without a voter quorum"
        );
        // Voters return: progress resumes.
        for &o in &others {
            c.restart(o);
        }
        settle(&mut c, 3000);
        assert_ok(&c, seed);
        assert!(c.committed().len() > before, "seed {seed}");
    }
}

#[test]
fn promotion_waits_for_catch_up_then_succeeds() {
    let mut refused = 0;
    for seed in 0..60 {
        let mut c = Cluster::with_members(seed, 4, &[0, 1, 2], healthy(), RaftConfig::default());
        c.run_until(1500);
        assert!(until_accepted(&mut c, 4000, |c| c.add_learner(3).is_some()));
        settle(&mut c, 600);
        // Cut the learner off and commit more, so it lags.
        c.sim.partition(&[vec![3], vec![0, 1, 2]]);
        for cmd in 1..=5 {
            c.propose(cmd).expect("leader");
            settle(&mut c, 40);
        }
        settle(&mut c, 200);
        if c.change_membership(&[0, 1, 2, 3]).is_none() {
            refused += 1;
        } else {
            panic!("seed {seed}: promoted a lagging learner");
        }
        c.sim.heal();
        settle(&mut c, 1000);
        assert!(
            until_accepted(&mut c, 8000, |c| c.change_membership(&[0, 1, 2, 3]).is_some()),
            "seed {seed}"
        );
        settle(&mut c, 2000);
        assert_ok(&c, seed);
        assert_eq!(c.committed_config(), Config::stable(&[0, 1, 2, 3]), "seed {seed}");
        for n in 0..4 {
            assert_eq!(
                c.node(n).config(),
                Config::stable(&[0, 1, 2, 3]),
                "seed {seed} node {n}"
            );
        }
    }
    assert_eq!(refused, 60);
}

#[test]
fn learner_removal() {
    for seed in 0..30 {
        let mut c = Cluster::with_members(seed, 4, &[0, 1, 2], healthy(), RaftConfig::default());
        c.run_until(1500);
        assert!(until_accepted(&mut c, 4000, |c| c.add_learner(3).is_some()));
        settle(&mut c, 500);
        assert!(until_accepted(&mut c, 6000, |c| c.remove_learner(3).is_some()));
        settle(&mut c, 800);
        assert_ok(&c, seed);
        assert_eq!(c.committed_config(), Config::stable(&[0, 1, 2]));
        // Removing a non-learner or adding an existing voter is refused.
        assert!(c.remove_learner(3).is_none());
        assert!(c.add_learner(1).is_none());
    }
}

/// Learner adds, promotions and removals interleaved with crashes, partitions and a hostile
/// network; every invariant (including the config-chain check) must hold.
#[test]
fn learner_lifecycle_under_faults() {
    let mut promoted = 0;
    for seed in 0..400u64 {
        let mut c = Cluster::with_members(seed, 5, &[0, 1, 2], hostile(), RaftConfig::default());
        let mut st = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut roll = |m: u64| {
            st ^= st << 13;
            st ^= st >> 7;
            st ^= st << 17;
            st % m
        };
        c.run_until(1500);
        let mut cmd = 1;
        for _ in 0..50 {
            let t = c.sim.now() + 60 + roll(80);
            c.run_until(t);
            match roll(12) {
                0 => c.crash(roll(5) as usize),
                1 => (0..5).for_each(|n| c.restart(n)),
                2 => c.sim.partition(&[vec![roll(5) as usize], (0..5).collect()]),
                3 => c.sim.heal(),
                4 | 5 => {
                    let _ = c.add_learner(3 + roll(2) as usize);
                }
                6 => {
                    let _ = c.remove_learner(3 + roll(2) as usize);
                }
                7 => {
                    let cur = c.committed_config();
                    if cur.learners & 0b1000 != 0 && c.change_membership(&[0, 1, 2, 3]).is_some() {
                        promoted += 1;
                    }
                }
                _ => {
                    let _ = c.propose(cmd);
                    cmd += 1;
                }
            }
            assert_ok(&c, seed);
        }
        c.sim.heal();
        (0..5).for_each(|n| c.restart(n));
        settle(&mut c, 3000);
        assert_ok(&c, seed);
    }
    assert!(promoted > 0, "promotion path never exercised");
}
