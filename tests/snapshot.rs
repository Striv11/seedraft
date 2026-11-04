//! Day 9: log compaction and InstallSnapshot.

use seedraft::disk::DiskConfig;
use seedraft::kv::{KvHarness, KvStore, Op, Reply, Request};
use seedraft::raft::{recover, Cluster, RaftConfig};
use seedraft::runner::{run_seed, schedule, Fault, RunConfig};
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

fn snap_cfg(every: u64) -> RaftConfig {
    RaftConfig {
        snapshot_every: every,
        ..RaftConfig::default()
    }
}

fn put(client: u8, seq: u16, key: u8, val: u16) -> u64 {
    Request {
        client,
        seq,
        op: Op::Put { key, val },
    }
    .encode()
}

#[test]
fn kv_snapshot_roundtrips_and_rejects_garbage() {
    let mut s = KvStore::new();
    let ops = [
        Op::Put { key: 3, val: 9 },
        Op::Get { key: 3 },
        Op::Get { key: 4 },
        Op::Cas {
            key: 3,
            expect: 9,
            new: 1,
        },
        Op::Cas {
            key: 3,
            expect: 9,
            new: 2,
        },
    ];
    for (i, op) in ops.iter().enumerate() {
        s.apply(Request {
            client: i as u8,
            seq: 1,
            op: *op,
        });
    }
    assert_eq!(s.session(2).unwrap().1, Reply::Value(None));
    let bytes = s.snapshot();
    assert_eq!(KvStore::restore(&bytes), Some(s));
    assert_eq!(KvStore::restore(&bytes[..bytes.len() - 1]), None);
    let mut extra = bytes.clone();
    extra.push(0);
    assert_eq!(KvStore::restore(&extra), None);
    assert_eq!(KvStore::restore(&[]), None);
    assert_eq!(KvStore::restore(&KvStore::new().snapshot()), Some(KvStore::new()));
}

#[test]
fn lagging_follower_catches_up_via_install_snapshot() {
    for seed in 0..40 {
        let mut c = Cluster::new(seed, 5, healthy(), snap_cfg(10));
        c.run_until(1500);
        let (leader, _) = c.propose(put(0, 1, 0, 1)).unwrap();
        let victim = (leader + 1) % 5;
        c.crash(victim);
        for i in 2..=80u16 {
            c.propose(put(0, i, (i % 4) as u8, i)).unwrap();
            c.run_until(c.sim.now() + 40);
        }
        c.run_until(c.sim.now() + 500);
        let leader = c.current_leaders()[0].0;
        assert!(
            c.node(leader).snapshot_index() > 0,
            "seed {seed}: leader never compacted"
        );
        assert!(
            c.node(leader).log().len() < 40,
            "seed {seed}: log not compacted ({})",
            c.node(leader).log().len()
        );
        c.restart(victim);
        c.run_until(c.sim.now() + 2000);
        assert!(c.violations().is_empty(), "seed {seed}: {:?}", c.violations());
        assert!(
            c.snapshots_installed() > 0,
            "seed {seed}: no snapshot was installed"
        );
        let (lm, li) = c.machine(leader);
        let (vm, vi) = c.machine(victim);
        assert_eq!(vi, li, "seed {seed}: victim not caught up");
        assert_eq!(vm, lm, "seed {seed}: state machines differ");
        assert_eq!(c.committed().len() as u64, li, "seed {seed}");
    }
}

/// Replay of the globally committed prefix: the uncompacted reference state machine.
fn replay(c: &Cluster, upto: u64) -> KvStore {
    let mut s = KvStore::new();
    for e in &c.committed()[..upto as usize] {
        if let Some(r) = e.cmd.and_then(Request::decode) {
            s.apply(r);
        }
    }
    s
}

#[test]
fn compacted_nodes_match_uncompacted_replay_under_faults() {
    let cfg = RunConfig::default();
    let mut installs = 0;
    let mut compacted = 0;
    for seed in 0..60u64 {
        let mut h = KvHarness::with_raft(seed, cfg.nodes, cfg.net.clone(), cfg.workload, snap_cfg(6));
        for s in schedule(seed, &cfg) {
            h.run_until(s.at);
            match &s.fault {
                Fault::Crash(n) => h.cluster.crash(*n),
                Fault::Restart(n) => h.cluster.restart(*n),
                Fault::Partition(g) => h.cluster.sim.partition(g),
                Fault::Heal => h.cluster.sim.heal(),
            }
        }
        h.quiesce();
        h.run_until(cfg.fault_window + cfg.settle);
        let c = &h.cluster;
        assert!(c.violations().is_empty(), "seed {seed}: {:?}", c.violations());
        for n in 0..cfg.nodes {
            let (m, applied) = c.machine(n);
            assert!(applied <= c.committed().len() as u64);
            assert_eq!(m, &replay(c, applied), "seed {seed} node {n} applied {applied}");
            compacted += u64::from(c.node(n).snapshot_index() > 0);
        }
        installs += c.snapshots_installed();
    }
    assert!(compacted > 100, "compaction barely happened: {compacted}");
    assert!(installs > 0, "no seed ever exercised InstallSnapshot");
}

#[test]
fn restart_recovers_from_durable_snapshot() {
    let mut c = Cluster::new(5, 3, healthy(), snap_cfg(8));
    c.sim.set_disk_config(DiskConfig {
        fsync_loss_pct: 0,
        torn_write_pct: 70,
    });
    c.run_until(1500);
    for i in 1..=40u16 {
        c.propose(put(1, i, (i % 3) as u8, i)).unwrap();
        c.run_until(c.sim.now() + 40);
    }
    c.run_until(c.sim.now() + 500);
    for id in 0..3 {
        c.crash(id);
    }
    for id in 0..3 {
        let rec = recover(&c.sim.disk(id).read_records());
        assert!(rec.snap_index > 0, "node {id}: no snapshot on disk");
        assert!(rec.log.len() < 40);
        c.restart(id);
        assert_eq!(c.node(id).snapshot_index(), rec.snap_index);
        let (m, applied) = c.machine(id);
        assert_eq!(applied, rec.snap_index);
        assert_eq!(m, &replay(&c, applied));
    }
    c.run_until(c.sim.now() + 3000);
    assert!(c.violations().is_empty(), "{:?}", c.violations());
    let (m0, i0) = c.machine(0);
    assert_eq!(i0, c.committed().len() as u64);
    for id in 1..3 {
        assert_eq!(c.machine(id), (m0, i0));
    }
}

#[test]
fn full_runs_with_snapshots_stay_linearizable() {
    let cfg = RunConfig {
        snapshot_every: 6,
        ..RunConfig::default()
    };
    for seed in 0..200 {
        let o = run_seed(seed, &cfg);
        assert!(o.ok(), "seed {seed}: {}", o.failure.unwrap());
        assert!(o.completed > 5, "seed {seed} made no progress");
    }
}

#[test]
fn snapshots_off_by_default_keep_traces_unchanged() {
    let a = run_seed(3, &RunConfig::default());
    let b = run_seed(
        3,
        &RunConfig {
            snapshot_every: 0,
            ..RunConfig::default()
        },
    );
    assert_eq!(a.trace_hash, b.trace_hash);
}
