use seedraft::raft::Bug;
use seedraft::runner::{run_schedule, run_seed, Failure, Outcome, RunConfig};
use seedraft::shrink::shrink;
use std::process::Command;

fn buggy(bug: Bug) -> RunConfig {
    RunConfig {
        bug: Some(bug),
        ..RunConfig::default()
    }
}

/// First failing seed below `limit`, with its outcome.
fn find_failure(bug: Bug, limit: u64) -> Outcome {
    let cfg = buggy(bug);
    // Broken protocols may panic; that is a reported failure, not a test crash.
    (0..limit)
        .map(|s| run_seed(s, &cfg))
        .find(|o| !o.ok())
        .unwrap_or_else(|| panic!("bug {} not caught in {limit} seeds", bug.name()))
}

#[test]
fn every_injected_bug_is_caught() {
    for (bug, limit) in [
        (Bug::DoubleVote, 60),
        (Bug::SkipLogRecencyCheck, 60),
        (Bug::AcceptStaleAppend, 80),
    ] {
        let o = find_failure(bug, limit);
        assert!(o.failure.is_some(), "{}", bug.name());
    }
}

#[test]
fn the_same_seeds_pass_without_the_bug() {
    // Guards against the detector firing on healthy runs.
    let cfg = RunConfig::default();
    for seed in 0..40 {
        let o = run_seed(seed, &cfg);
        assert!(o.ok(), "seed {seed}: {}", o.failure.unwrap());
    }
}

#[test]
fn safety_bugs_are_reported_as_invariant_violations() {
    let o = find_failure(Bug::SkipLogRecencyCheck, 60);
    assert!(
        matches!(o.failure, Some(Failure::Invariant(_))),
        "{:?}",
        o.failure
    );
    let o = find_failure(Bug::DoubleVote, 60);
    assert!(
        matches!(o.failure, Some(Failure::Invariant(_))),
        "{:?}",
        o.failure
    );
}

#[test]
fn shrinking_yields_a_smaller_one_minimal_reproducer() {
    let bug = Bug::SkipLogRecencyCheck;
    let cfg = buggy(bug);
    let o = find_failure(bug, 60);
    let s = shrink(&o, &cfg).expect("failing outcome shrinks");
    assert!(
        s.schedule.len() < o.schedule.len(),
        "no reduction: {} faults",
        s.schedule.len()
    );
    assert_eq!(s.original_faults, o.schedule.len());
    // The minimal schedule really fails, with the same class of failure.
    let again = run_schedule(o.seed, &cfg, s.schedule.clone());
    assert!(matches!(again.failure, Some(Failure::Invariant(_))));
    // 1-minimality: dropping any single remaining fault makes the failure go away.
    for i in 0..s.schedule.len() {
        let mut less = s.schedule.clone();
        less.remove(i);
        let r = run_schedule(o.seed, &cfg, less);
        assert!(
            !matches!(r.failure, Some(Failure::Invariant(_))),
            "fault {i} was removable"
        );
    }
    // Shrinking is deterministic.
    let s2 = shrink(&o, &cfg).unwrap();
    assert_eq!(s.schedule, s2.schedule);
}

#[test]
fn shrinking_a_passing_run_returns_none() {
    let cfg = RunConfig::default();
    let o = run_seed(1, &cfg);
    assert!(o.ok());
    assert!(shrink(&o, &cfg).is_none());
}

#[test]
fn bug_names_round_trip() {
    for b in Bug::ALL {
        assert_eq!(Bug::parse(b.name()), Some(b));
    }
    assert_eq!(Bug::parse("nope"), None);
}

fn bin(args: &[&str]) -> (i32, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_seedraft"))
        .args(args)
        .output()
        .unwrap();
    (
        out.status.code().unwrap(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
    )
}

#[test]
fn cli_bug_and_shrink() {
    let (code, out) = bin(&[
        "sweep",
        "--seeds",
        "0..20",
        "--bug",
        "skip-log-recency",
        "--shrink",
    ]);
    assert_eq!(code, 1, "{out}");
    assert!(
        out.contains("shrunk fault schedule") && out.contains("reproduce: seedraft run --seed"),
        "{out}"
    );
    let (code, out) = bin(&["run", "--seed", "1", "--bug", "skip-log-recency", "--shrink"]);
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("still fails"), "{out}");
    assert_eq!(bin(&["run", "--seed", "1", "--bug", "bogus"]).0, 2);
}
