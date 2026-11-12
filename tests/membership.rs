//! Day 10: membership change via joint consensus.

use seedraft::raft::{recover, Cluster, Config, RaftConfig, Role};
use seedraft::NetConfig;
use std::collections::BTreeSet;

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

/// Keep asking for `members` until a leader accepts, then run until the change is committed
/// (or `deadline` passes). Returns whether the final configuration committed.
fn change_and_wait(c: &mut Cluster, members: &[usize], deadline: u64) -> bool {
    let target = Config::stable(members);
    let mut asked = false;
    while c.sim.now() < deadline {
        if !asked && c.change_membership(members).is_some() {
            asked = true;
        }
        let t = c.sim.now() + 25;
        c.run_until(t);
        if asked && c.committed_config() == target {
            return true;
        }
    }
    false
}

#[test]
fn config_encoding_and_quorums() {
    let joint = Config {
        cur: 0b11100,
        prev: 0b00111,
        learners: 0b1000000,
    };
    assert_eq!(Config::decode(joint.encode()), Some(joint));
    assert_eq!(Config::decode(7), None);
    assert_eq!(joint.members(), vec![0, 1, 2, 3, 4]);
    let set = |v: &[usize]| v.iter().copied().collect::<BTreeSet<_>>();
    // Needs 2 of {0,1,2} AND 2 of {2,3,4}.
    assert!(joint.has_quorum(&set(&[1, 2, 3])));
    assert!(!joint.has_quorum(&set(&[0, 1, 3])));
    assert!(!joint.has_quorum(&set(&[0, 1, 2])));
    assert!(!joint.has_quorum(&set(&[2, 3, 4])));
    let stable = Config::stable(&[0, 1, 2]);
    assert!(stable.has_quorum(&set(&[0, 2])));
    assert!(!stable.has_quorum(&set(&[0, 3, 4])));
}

#[test]
fn idle_nodes_do_not_campaign() {
    let mut c = Cluster::with_members(3, 5, &[0, 1, 2], healthy(), RaftConfig::default());
    c.run_until(3000);
    assert_ok(&c, 3);
    for leaders in c.leaders_by_term().values() {
        assert!(leaders.iter().all(|&l| l < 3));
    }
    assert!(c.node(3).role() == Role::Follower && c.node(4).role() == Role::Follower);
}

#[test]
fn grow_three_to_five_and_serve_requests() {
    for seed in 0..60 {
        let mut c = Cluster::with_members(seed, 5, &[0, 1, 2], healthy(), RaftConfig::default());
        c.run_until(1500);
        for cmd in 1..=5 {
            c.propose(cmd).expect("leader");
            c.run_until(1500 + cmd * 30);
        }
        assert!(change_and_wait(&mut c, &[0, 1, 2, 3, 4], 8000), "seed {seed}");
        for cmd in 6..=10 {
            c.propose(cmd).expect("leader");
            c.run_until(c.sim.now() + 40);
        }
        let t = c.sim.now() + 1500;
        c.run_until(t);
        assert_ok(&c, seed);
        let total = c.committed().len() as u64;
        for n in 0..5 {
            assert_eq!(
                c.node(n).config(),
                Config::stable(&[0, 1, 2, 3, 4]),
                "seed {seed} node {n}"
            );
            assert_eq!(c.node(n).commit_index(), total, "seed {seed} node {n} caught up");
        }
    }
}

#[test]
fn shrink_five_to_three_including_removing_the_leader() {
    let mut removed_leader = 0;
    for seed in 0..80 {
        let mut c = Cluster::new(seed, 5, healthy(), RaftConfig::default());
        c.run_until(1500);
        let leader = c.current_leaders()[0].0;
        // Even seeds drop the current leader, odd seeds drop two followers.
        let keep: Vec<usize> = if seed % 2 == 0 {
            removed_leader += 1;
            (0..5).filter(|&i| i != leader).take(3).collect()
        } else {
            vec![leader, (leader + 1) % 5, (leader + 2) % 5]
        };
        assert!(change_and_wait(&mut c, &keep, 8000), "seed {seed}");
        let t = c.sim.now() + 2000;
        c.run_until(t);
        assert_ok(&c, seed);
        let leaders = c.current_leaders();
        assert_eq!(leaders.len(), 1, "seed {seed}: {leaders:?}");
        assert!(
            keep.contains(&leaders[0].0),
            "seed {seed}: leader outside the new config"
        );
        // The cluster keeps committing with the smaller membership.
        let before = c.committed().len();
        c.propose(99).expect("leader");
        let t = c.sim.now() + 300;
        c.run_until(t);
        assert_eq!(c.committed().len(), before + 1, "seed {seed}");
    }
    assert!(removed_leader > 0);
}

#[test]
fn removed_nodes_cannot_disrupt_the_cluster() {
    for seed in 0..40 {
        let mut c = Cluster::new(seed, 5, healthy(), RaftConfig::default());
        c.run_until(1500);
        let leader = c.current_leaders()[0].0;
        let keep = vec![leader, (leader + 1) % 5, (leader + 2) % 5];
        let dropped: Vec<usize> = (0..5).filter(|i| !keep.contains(i)).collect();
        // Cut a removed node off so it never learns about its removal, then let it time out.
        c.sim.partition(&[dropped.clone(), keep.clone()]);
        assert!(change_and_wait(&mut c, &keep, 8000), "seed {seed}");
        c.sim.heal();
        let term = c.node(leader).term();
        let t = c.sim.now() + 3000;
        c.run_until(t);
        assert_ok(&c, seed);
        let now = c.current_leaders();
        assert!(now.iter().any(|&(l, _)| keep.contains(&l)));
        assert!(
            c.node(now[0].0).term() <= term + 1,
            "seed {seed}: stale nodes inflated the term"
        );
    }
}

/// Membership changes interleaved with crashes, partitions and a hostile network.
#[test]
fn membership_changes_under_faults_keep_all_invariants() {
    let mut finished = 0;
    for seed in 0..1000u64 {
        let mut c = Cluster::with_members(seed, 5, &[0, 1, 2], hostile(), RaftConfig::default());
        let targets: [&[usize]; 4] = [&[0, 1, 2, 3], &[1, 2, 3, 4], &[2, 3, 4], &[0, 2, 4]];
        let mut next_target = 0;
        let mut rng_state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut roll = |m: u64| {
            rng_state ^= rng_state << 13;
            rng_state ^= rng_state >> 7;
            rng_state ^= rng_state << 17;
            rng_state % m
        };
        c.run_until(1500);
        let mut cmd = 1;
        for step in 0..60 {
            let t = c.sim.now() + 60 + roll(80);
            c.run_until(t);
            match roll(12) {
                0 => {
                    let n = roll(5) as usize;
                    c.crash(n);
                }
                1 => {
                    for n in 0..5 {
                        c.restart(n);
                    }
                }
                2 => {
                    let a = roll(5) as usize;
                    let rest: Vec<usize> = (0..5).filter(|&i| i != a).collect();
                    c.sim.partition(&[vec![a], rest]);
                }
                3 => c.sim.heal(),
                4..=7 if next_target < targets.len() => {
                    if c.change_membership(targets[next_target]).is_some() {
                        next_target += 1;
                    }
                }
                _ => {
                    c.propose(cmd);
                    cmd += 1;
                }
            }
            assert_ok(&c, seed * 100 + step);
        }
        // Heal everything and let the cluster settle.
        c.sim.heal();
        for n in 0..5 {
            c.restart(n);
        }
        let t = c.sim.now() + 5000;
        c.run_until(t);
        assert_ok(&c, seed);
        if next_target >= 3 && c.committed_config().cur != 0b111 {
            finished += 1;
        }
        // Election safety across every term.
        for (term, ls) in c.leaders_by_term() {
            assert!(ls.len() <= 1, "seed {seed}: two leaders in term {term}");
        }
    }
    assert!(finished > 100, "only {finished} seeds completed several changes");
}

#[test]
fn config_survives_compaction_restart_and_snapshot_install() {
    let cfg = RaftConfig {
        snapshot_every: 6,
        ..RaftConfig::default()
    };
    let mut installs = 0;
    for seed in 0..40 {
        let mut c = Cluster::with_members(seed, 5, &[0, 1, 2], healthy(), cfg);
        c.run_until(1500);
        for cmd in 1..=12 {
            c.propose(cmd);
            c.run_until(c.sim.now() + 30);
        }
        // Node 3 joins long after the config entries were compacted away on the others.
        assert!(change_and_wait(&mut c, &[0, 1, 2, 3], 9000), "seed {seed}");
        for cmd in 13..=40 {
            c.propose(cmd);
            c.run_until(c.sim.now() + 30);
        }
        let t = c.sim.now() + 2000;
        c.run_until(t);
        assert_ok(&c, seed);
        installs += c.snapshots_installed();
        let want = Config::stable(&[0, 1, 2, 3]);
        for n in 0..4 {
            assert_eq!(c.node(n).config(), want, "seed {seed} node {n}");
            assert!(c.node(n).snapshot_index() > 0, "seed {seed} node {n} compacted");
        }
        // Restarting every voter restores the configuration from disk (snapshot + log).
        for n in 0..4 {
            c.crash(n);
        }
        for n in 0..4 {
            c.restart(n);
        }
        for n in 0..4 {
            assert_eq!(c.node(n).config(), want, "seed {seed} node {n} after restart");
        }
        let t = c.sim.now() + 3000;
        c.run_until(t);
        assert_ok(&c, seed);
        assert_eq!(c.current_leaders().len(), 1, "seed {seed}");
    }
    assert!(installs > 0, "no seed installed a snapshot carrying the config");
}

#[test]
fn recover_reads_config_from_snapshot_record() {
    let conf = Config::stable(&[1, 3]);
    // A hand-built REC_SNAP record: index, term, config, then the state machine bytes.
    let mut rec = vec![3u8];
    rec.extend_from_slice(&4u64.to_le_bytes());
    rec.extend_from_slice(&2u64.to_le_bytes());
    rec.extend_from_slice(&conf.encode().to_le_bytes());
    rec.extend_from_slice(&[9, 9]);
    let r = recover(&[rec]);
    assert_eq!((r.snap_index, r.snap_term, r.snap_conf), (4, 2, Some(conf)));
    assert_eq!(r.snap_data, vec![9, 9]);
}
