use seedraft::{Event, NetConfig, Sim};

fn lossy() -> NetConfig {
    NetConfig {
        min_delay: 1,
        max_delay: 20,
        drop_pct: 15,
        dup_pct: 10,
        reorder_pct: 20,
        reorder_extra: 40,
    }
}

/// A toy workload: nodes gossip a counter around, with timers and a partition mid-run.
fn run(seed: u64) -> (u64, Vec<(u64, usize, u32)>, u64) {
    let mut sim: Sim<u32> = Sim::new(seed, 5, lossy());
    for n in 0..5 {
        sim.set_timer(n, 0, 5 + n as u64);
    }
    let mut seen = Vec::new();
    let mut steps = 0;
    while let Some(ev) = sim.step() {
        steps += 1;
        if steps > 2000 {
            break;
        }
        if steps == 300 {
            sim.partition(&[vec![0, 1], vec![2, 3]]);
        }
        if steps == 600 {
            sim.heal();
        }
        match ev {
            Event::Timer { node, .. } => {
                let to = sim.rng().range(0, 4) as usize;
                let v = sim.rng().range(0, 1000) as u32;
                sim.send(node, to, v);
                sim.set_timer(node, 0, 10);
            }
            Event::Deliver { to, msg, .. } => {
                seen.push((sim.now(), to, msg));
                if msg % 3 == 0 {
                    sim.send(to, (to + 1) % 5, msg + 1);
                }
            }
        }
    }
    (sim.trace_hash(), seen, sim.now())
}

#[test]
fn same_seed_same_trace() {
    for seed in [0, 1, 42, u64::MAX] {
        assert_eq!(run(seed), run(seed), "seed {seed} diverged");
    }
}

#[test]
fn different_seeds_differ() {
    let hashes: std::collections::HashSet<u64> = (0..20).map(|s| run(s).0).collect();
    assert!(hashes.len() > 15);
}

#[test]
fn time_is_monotonic_and_ordered() {
    let mut sim: Sim<u8> = Sim::new(9, 2, lossy());
    for i in 0..200 {
        sim.send(0, 1, i);
    }
    let mut last = 0;
    while sim.step().is_some() {
        assert!(sim.now() >= last);
        last = sim.now();
    }
}

#[test]
fn partition_blocks_and_heal_restores() {
    let mut sim: Sim<u8> = Sim::new(1, 3, NetConfig::default());
    sim.partition(&[vec![0], vec![1, 2]]);
    sim.send(0, 1, 1);
    sim.send(1, 2, 2);
    let mut got = vec![];
    while let Some(Event::Deliver { to, msg, .. }) = sim.step() {
        got.push((to, msg));
    }
    assert_eq!(got, vec![(2, 2)]);
    sim.heal();
    sim.send(0, 1, 3);
    assert!(matches!(sim.step(), Some(Event::Deliver { to: 1, msg: 3, .. })));
}

#[test]
fn in_flight_messages_dropped_by_new_partition() {
    let mut sim: Sim<u8> = Sim::new(1, 2, NetConfig::default());
    sim.send(0, 1, 7);
    sim.partition(&[vec![0], vec![1]]);
    assert!(sim.step().is_none());
}

#[test]
fn faults_actually_happen() {
    let mut sim: Sim<u32> = Sim::new(
        5,
        2,
        NetConfig {
            drop_pct: 50,
            dup_pct: 50,
            ..lossy()
        },
    );
    for i in 0..400 {
        sim.send(0, 1, i);
    }
    let mut delivered = 0;
    let mut order = vec![];
    while let Some(Event::Deliver { msg, .. }) = sim.step() {
        delivered += 1;
        order.push(msg);
    }
    assert!(delivered > 0 && delivered != 400);
    assert!(order.windows(2).any(|w| w[0] > w[1]), "expected reordering");
    let mut s = order.clone();
    s.sort();
    s.dedup();
    assert!(s.len() < order.len(), "expected duplicates");
}

/// Crash/restart workload with disk writes; returns (trace hash, recovered records per node).
fn crashy(seed: u64) -> (u64, Vec<Vec<Vec<u8>>>) {
    let mut sim: Sim<u32> = Sim::new(seed, 3, lossy());
    sim.set_disk_config(seedraft::DiskConfig {
        fsync_loss_pct: 20,
        torn_write_pct: 70,
    });
    for n in 0..3 {
        sim.set_timer(n, 0, 3);
    }
    let mut steps = 0u32;
    while let Some(ev) = sim.step() {
        steps += 1;
        if steps > 1500 {
            break;
        }
        if steps.is_multiple_of(97) {
            let victim = sim.rng().range(0, 2) as usize;
            sim.crash(victim);
            sim.disk(victim).repair();
            sim.restart(victim);
            sim.set_timer(victim, 0, 4);
        }
        if let Event::Timer { node, .. } = ev {
            let v = sim.rng().range(0, 255) as u8;
            sim.disk(node).append(&[v; 4]);
            if v.is_multiple_of(2) {
                sim.fsync(node);
            }
            sim.send(node, (node + 1) % 3, u32::from(v));
            sim.set_timer(node, 0, 6);
        }
    }
    let recs = (0..3).map(|n| sim.disk(n).read_records()).collect();
    (sim.trace_hash(), recs)
}

#[test]
fn crash_restart_is_deterministic() {
    assert_eq!(crashy(11), crashy(11));
    assert_ne!(crashy(11).0, crashy(12).0);
}

#[test]
fn down_node_gets_no_messages_or_timers_and_old_timers_die() {
    let mut sim: Sim<u32> = Sim::new(1, 2, NetConfig::default());
    sim.set_timer(1, 7, 50);
    sim.send(0, 1, 42);
    sim.crash(1);
    assert!(!sim.is_up(1));
    // Everything addressed to node 1 is discarded while it is down.
    assert!(sim.step().is_none());
    sim.restart(1);
    assert!(sim.is_up(1));
    // The pre-crash timer must not fire after restart (it was set in an old incarnation).
    assert!(sim.step().is_none());
    sim.set_timer(1, 8, 5);
    assert_eq!(sim.step(), Some(Event::Timer { node: 1, id: 8 }));
    // A down node cannot send.
    sim.crash(0);
    sim.send(0, 1, 1);
    assert!(sim.step().is_none());
}

#[test]
fn crash_loses_unsynced_disk_data_in_sim() {
    let mut sim: Sim<u32> = Sim::new(2, 1, NetConfig::default());
    sim.disk(0).append(b"durable");
    assert!(sim.fsync(0));
    sim.disk(0).append(b"volatile");
    sim.crash(0);
    sim.restart(0);
    assert_eq!(sim.disk(0).read_records(), vec![b"durable".to_vec()]);
}
