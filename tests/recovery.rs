use seedraft::disk::DiskConfig;
use seedraft::raft::{recover, Cluster, Entry, RaftConfig};
use seedraft::NetConfig;

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

/// Crashes lose unsynced bytes and tear writes; fsync itself is honest.
fn torn_disk() -> DiskConfig {
    DiskConfig {
        fsync_loss_pct: 0,
        torn_write_pct: 70,
    }
}

#[test]
fn recover_replays_appends_and_truncations() {
    use seedraft::disk::Disk;
    let mut c = Cluster::new(1, 3, healthy(), RaftConfig::default());
    c.run_until(1500);
    c.propose(7).unwrap();
    c.run_until(2000);
    for id in 0..3 {
        let rec = recover(&c.sim.disk(id).read_records());
        assert_eq!(rec.log, c.node(id).log(), "node {id}");
        assert_eq!(rec.term, c.node(id).term());
        assert_eq!(rec.voted_for, c.node(id).voted_for());
    }
    // Garbage records are skipped rather than crashing recovery.
    let mut d = Disk::new();
    d.append(&[]);
    d.append(&[9, 9]);
    d.append(&[1, 0]);
    assert_eq!(recover(&d.read_records()).log, Vec::<Entry>::new());
}

#[test]
fn crashed_follower_recovers_its_log_and_catches_up() {
    for seed in 0..100 {
        let mut c = Cluster::new(seed, 5, healthy(), RaftConfig::default());
        c.sim.set_disk_config(torn_disk());
        c.run_until(1500);
        for cmd in 1..=5 {
            c.propose(cmd).unwrap();
            c.run_until(1500 + cmd * 30);
        }
        c.run_until(2500);
        let (leader, _) = c.current_leaders()[0];
        let victim = (leader + 1) % 5;
        let before = c.node(victim).log().to_vec();
        c.crash(victim);
        c.restart(victim);
        assert_eq!(
            c.node(victim).log(),
            &before[..],
            "seed {seed}: log lost on restart"
        );
        c.run_until(4000);
        assert!(c.violations().is_empty(), "seed {seed}: {:?}", c.violations());
    }
}

/// Random crashes/restarts (including of leaders), partitions, hostile network, torn writes.
/// Invariants are checked each step, and a committed entry that vanished from a quorum would
/// surface as a leader-completeness or committed-prefix violation.
#[test]
fn no_committed_entry_lost_with_crashes() {
    let mut committed_total = 0;
    let mut restarts = 0;
    for seed in 0..1000 {
        let mut c = Cluster::new(seed, 5, hostile(), RaftConfig::default());
        c.sim.set_disk_config(torn_disk());
        for round in 1..=40u64 {
            c.run_until(round * 100);
            c.propose(round * 1000 + seed % 1000);
            c.propose(round * 1000 + 500);
            let victim = c.sim.rng().range(0, 4) as usize;
            match c.sim.rng().range(0, 6) {
                0 | 1 => {
                    // Prefer crashing the current leader: the interesting case.
                    let target = c.current_leaders().first().map_or(victim, |l| l.0);
                    c.crash(target);
                }
                2 | 3 => {
                    for id in 0..5 {
                        if !c.sim.is_up(id) && c.sim.rng().chance_pct(60) {
                            c.restart(id);
                            restarts += 1;
                        }
                    }
                }
                4 => {
                    let k = c.sim.rng().range(1, 2) as usize;
                    c.sim.partition(&[(0..k).collect(), (k..5).collect()]);
                }
                _ => c.sim.heal(),
            }
        }
        // Bring everything back on a calm network and require convergence.
        c.sim.heal();
        for id in 0..5 {
            c.restart(id);
        }
        assert!(c.violations().is_empty(), "seed {seed}: {:?}", c.violations());
        committed_total += c.committed().len();
    }
    assert!(restarts > 3000, "too few restarts: {restarts}");
    assert!(committed_total > 10_000, "too little progress: {committed_total}");
}

/// Losing power on every node at once must not lose committed entries.
#[test]
fn full_cluster_power_loss_keeps_committed_entries() {
    for seed in 0..300 {
        let mut c = Cluster::new(seed, 5, healthy(), RaftConfig::default());
        c.sim.set_disk_config(torn_disk());
        c.run_until(1500);
        for cmd in 1..=6 {
            c.propose(cmd).unwrap();
            c.run_until(1500 + cmd * 40);
        }
        c.run_until(2500);
        let committed: Vec<Entry> = c.committed().to_vec();
        assert!(
            committed.iter().filter(|e| e.cmd.is_some()).count() == 6,
            "seed {seed}"
        );
        for id in 0..5 {
            c.crash(id);
        }
        for id in 0..5 {
            c.restart(id);
        }
        c.run_until(6000);
        assert!(c.violations().is_empty(), "seed {seed}: {:?}", c.violations());
        let (leader, _) = *c.current_leaders().first().expect("leader after power loss");
        assert_eq!(
            &c.node(leader).log()[..committed.len()],
            &committed[..],
            "seed {seed}"
        );
        c.propose(99).unwrap();
        c.run_until(7000);
        assert_eq!(c.committed().last().unwrap().cmd, Some(99), "seed {seed}");
    }
}

#[test]
fn same_seed_same_history_with_crashes() {
    let run = |seed| {
        let mut c = Cluster::new(seed, 5, hostile(), RaftConfig::default());
        c.sim.set_disk_config(torn_disk());
        for r in 1..=30u64 {
            c.run_until(r * 120);
            c.propose(r);
            if r % 4 == 0 {
                let l = c.current_leaders().first().map_or(0, |l| l.0);
                c.crash(l);
            }
            if r % 4 == 2 {
                for id in 0..5 {
                    c.restart(id);
                }
            }
        }
        (c.sim.trace_hash(), c.committed().to_vec())
    };
    assert_eq!(run(5), run(5));
    assert_ne!(run(5).0, run(6).0);
}
