//! Membership changes (learner add/remove/promote, voter-set changes) mixed into the random
//! fault schedule, with the full invariant + linearizability verdict per seed.

use seedraft::runner::{run_seed, schedule, sweep, Fault, RunConfig};

fn cfg() -> RunConfig {
    RunConfig {
        membership: true,
        ..RunConfig::default()
    }
}

#[test]
fn membership_is_off_by_default_and_does_not_perturb_schedules() {
    let plain = RunConfig::default();
    for seed in 0..50 {
        let base = schedule(seed, &plain);
        assert!(base.iter().all(|s| !matches!(
            s.fault,
            Fault::AddLearner(_) | Fault::RemoveLearner(_) | Fault::SetVoters(_)
        )));
        // The crash/partition faults are the same with membership on, just interleaved.
        let with: Vec<_> = schedule(seed, &cfg())
            .into_iter()
            .filter(|s| {
                !matches!(
                    s.fault,
                    Fault::AddLearner(_) | Fault::RemoveLearner(_) | Fault::SetVoters(_)
                )
            })
            .collect();
        assert_eq!(with.len(), base.len(), "seed {seed}");
    }
}

#[test]
fn schedules_are_time_ordered_deterministic_and_end_with_full_voter_set() {
    let c = cfg();
    let mut ops = 0;
    for seed in 0..200 {
        let s = schedule(seed, &c);
        assert_eq!(s, schedule(seed, &c));
        assert!(s.windows(2).all(|w| w[0].at <= w[1].at), "seed {seed} unordered");
        let mut voters = c.nodes;
        for f in &s {
            match &f.fault {
                Fault::SetVoters(v) => {
                    assert!(v.len() >= 3, "seed {seed}: voters shrank below 3");
                    voters = v.len();
                    ops += 1;
                }
                Fault::AddLearner(_) | Fault::RemoveLearner(_) => ops += 1,
                _ => {}
            }
        }
        assert_eq!(voters, c.nodes, "seed {seed}: voters not restored");
    }
    assert!(ops > 200, "schedules barely exercise membership ({ops})");
}

#[test]
fn membership_sweep_is_safe_and_linearizable() {
    let (cov, bad) = sweep(0, 300, &cfg(), 4);
    assert!(
        bad.is_empty(),
        "seed {} failed: {}",
        bad[0].seed,
        bad[0].failure.as_ref().unwrap()
    );
    assert!(cov.membership_ops > 300);
}

#[test]
fn membership_runs_are_reproducible() {
    let c = cfg();
    for seed in [3, 77, 141] {
        let (a, b) = (run_seed(seed, &c), run_seed(seed, &c));
        assert_eq!(a.trace_hash, b.trace_hash);
        assert_eq!(a.committed, b.committed);
    }
}

#[test]
fn scheduled_changes_actually_commit() {
    // Requests the leader refuses are no-ops, so check that a good share really lands in the log.
    use seedraft::raft::Config;
    let c = cfg();
    let (mut scheduled, mut landed) = (0, 0);
    for seed in 0..40 {
        let sched = schedule(seed, &c);
        scheduled += sched
            .iter()
            .filter(|s| {
                matches!(
                    s.fault,
                    Fault::AddLearner(_) | Fault::RemoveLearner(_) | Fault::SetVoters(_)
                )
            })
            .count();
        let mut h = seedraft::kv::KvHarness::with_raft(
            seed,
            c.nodes,
            c.net.clone(),
            c.workload,
            seedraft::raft::RaftConfig::default(),
        );
        for s in &sched {
            h.run_until(s.at);
            match &s.fault {
                Fault::Crash(n) => h.cluster.crash(*n),
                Fault::Restart(n) => h.cluster.restart(*n),
                Fault::Partition(g) => h.cluster.sim.partition(g),
                Fault::Heal => h.cluster.sim.heal(),
                Fault::AddLearner(n) => drop(h.cluster.add_learner(*n)),
                Fault::RemoveLearner(n) => drop(h.cluster.remove_learner(*n)),
                Fault::SetVoters(v) => drop(h.cluster.change_membership(v)),
            }
        }
        h.run_until(c.fault_window + c.settle);
        landed += h
            .cluster
            .committed()
            .iter()
            .filter(|e| e.cmd.and_then(Config::decode).is_some())
            .count();
    }
    assert!(
        landed * 2 >= scheduled,
        "only {landed} of {scheduled} membership requests committed"
    );
}

#[test]
fn membership_with_leases_and_mild_skew_is_safe_and_linearizable() {
    let cfg = RunConfig {
        membership: true,
        lease: 140,
        clock_skew: 10,
        ..RunConfig::default()
    };
    let (cov, bad) = sweep(0, 300, &cfg, 2);
    assert!(
        bad.is_empty(),
        "failures: {:?}",
        bad.iter().map(|o| o.seed).collect::<Vec<_>>()
    );
    assert!(cov.membership_ops > 0);
}

#[test]
fn membership_does_not_mask_lease_bugs_under_heavy_skew() {
    // Membership churn must not hide stale lease reads: heavy skew is still caught, and
    // only ever as a linearizability failure (never a Raft safety violation).
    let cfg = RunConfig {
        membership: true,
        lease: 140,
        clock_skew: 90,
        ..RunConfig::default()
    };
    let (_, bad) = sweep(0, 300, &cfg, 2);
    assert!(!bad.is_empty(), "heavy skew should produce stale reads");
    assert!(bad
        .iter()
        .all(|o| matches!(o.failure, Some(seedraft::runner::Failure::NotLinearizable { .. }))));
}
