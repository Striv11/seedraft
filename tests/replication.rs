use seedraft::raft::{Cluster, RaftConfig};
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

fn assert_ok(c: &Cluster, seed: u64) {
    assert!(c.violations().is_empty(), "seed {seed}: {:?}", c.violations());
}

#[test]
fn proposals_commit_on_every_node_on_healthy_net() {
    for seed in 0..200 {
        let mut c = Cluster::new(seed, 5, healthy(), RaftConfig::default());
        c.run_until(1500);
        for cmd in 1..=10 {
            c.propose(cmd).expect("leader exists");
            c.run_until(1500 + cmd * 40);
        }
        c.run_until(4000);
        assert_ok(&c, seed);
        let cmds: Vec<u64> = c.committed().iter().filter_map(|e| e.cmd).collect();
        assert_eq!(cmds, (1..=10).collect::<Vec<_>>(), "seed {seed}");
        for n in 0..5 {
            assert_eq!(
                c.node(n).commit_index() as usize,
                c.committed().len(),
                "seed {seed} node {n}"
            );
            assert_eq!(c.node(n).log(), c.node(0).log(), "seed {seed} node {n}");
        }
    }
}

/// Log matching, state machine safety and leader completeness are checked after every step
/// while the network drops, duplicates and reorders, and partitions come and go.
#[test]
fn replication_invariants_under_faults() {
    let mut committed_total = 0;
    for seed in 0..1000 {
        let mut c = Cluster::new(seed, 5, hostile(), RaftConfig::default());
        for round in 1..=40u64 {
            c.run_until(round * 100);
            c.propose(round * 1000 + seed % 1000);
            c.propose(round * 1000 + 500);
            match c.sim.rng().range(0, 5) {
                0 => {
                    let k = c.sim.rng().range(1, 2) as usize;
                    let a: Vec<usize> = (0..k).collect();
                    let b: Vec<usize> = (k..5).collect();
                    c.sim.partition(&[a, b]);
                }
                1 | 2 => c.sim.heal(),
                _ => {}
            }
        }
        assert_ok(&c, seed);
        committed_total += c.committed().len();
    }
    assert!(committed_total > 20_000, "too little progress: {committed_total}");
}

#[test]
fn minority_leader_cannot_commit_and_old_entries_are_overwritten() {
    for seed in 0..200 {
        let mut c = Cluster::new(seed, 5, healthy(), RaftConfig::default());
        c.run_until(1500);
        let (old, _) = c.propose(1).unwrap();
        c.run_until(1700);
        assert_eq!(c.committed().iter().filter(|e| e.cmd == Some(1)).count(), 1);
        // Isolate the leader with one follower; it keeps accepting but cannot commit.
        let buddy = (old + 1) % 5;
        let rest: Vec<usize> = (0..5).filter(|&i| i != old && i != buddy).collect();
        c.sim.partition(&[vec![old, buddy], rest]);
        c.propose(666).unwrap();
        c.run_until(4000);
        assert!(!c.committed().iter().any(|e| e.cmd == Some(666)), "seed {seed}");
        // Majority side elects a new leader and commits other work.
        let (newl, _) = c.propose(2).expect("majority side elected a leader");
        assert_ne!(newl, old);
        c.run_until(5000);
        c.sim.heal();
        c.run_until(8000);
        assert_ok(&c, seed);
        let cmds: Vec<u64> = c.committed().iter().filter_map(|e| e.cmd).collect();
        assert_eq!(cmds, vec![1, 2], "seed {seed}");
        assert!(
            !c.node(old).log().iter().any(|e| e.cmd == Some(666)),
            "seed {seed}: stale entry survived"
        );
        assert_eq!(c.node(old).log(), c.node(newl).log(), "seed {seed}");
    }
}

#[test]
fn same_seed_same_replicated_history() {
    let run = |seed| {
        let mut c = Cluster::new(seed, 5, hostile(), RaftConfig::default());
        for r in 1..=20u64 {
            c.run_until(r * 150);
            c.propose(r);
        }
        (c.sim.trace_hash(), c.committed().to_vec())
    };
    assert_eq!(run(3), run(3));
    assert_ne!(run(3).0, run(4).0);
}
