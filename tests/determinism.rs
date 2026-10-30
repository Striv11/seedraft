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
