//! Parallel sweep, fault-space coverage stats, trace-hash recording, and the seed corpus.

use seedraft::corpus;
use seedraft::raft::Bug;
use seedraft::runner::{run_seed, sweep, Coverage, RunConfig};
use seedraft::sim::{NetConfig, Sim};
use std::process::Command;

#[test]
fn trace_hash_does_not_depend_on_recording() {
    let go = |record: bool| {
        let mut sim: Sim<u64> = Sim::new(9, 4, NetConfig::default());
        sim.set_trace_recording(record);
        for i in 0..50u64 {
            sim.send((i % 4) as usize, ((i + 1) % 4) as usize, i);
        }
        while sim.step().is_some() {}
        (sim.trace_hash(), sim.trace().len())
    };
    let (h_on, n_on) = go(true);
    let (h_off, n_off) = go(false);
    assert_eq!(h_on, h_off);
    assert!(n_on > 0);
    assert_eq!(n_off, 0);
}

#[test]
fn trace_hash_still_tells_runs_apart() {
    let cfg = RunConfig::default();
    assert_eq!(run_seed(3, &cfg).trace_hash, run_seed(3, &cfg).trace_hash);
    assert_ne!(run_seed(3, &cfg).trace_hash, run_seed(4, &cfg).trace_hash);
}

#[test]
fn parallel_sweep_matches_sequential() {
    let cfg = RunConfig::default();
    let (c1, b1) = sweep(0, 24, &cfg, 1);
    let (c3, b3) = sweep(0, 24, &cfg, 3);
    assert_eq!(c1, c3);
    assert!(b1.is_empty() && b3.is_empty());
    assert_eq!(c1.seeds, 24);
}

#[test]
fn parallel_sweep_reports_failures_in_seed_order() {
    let cfg = RunConfig {
        bug: Some(Bug::SkipLogRecencyCheck),
        ..RunConfig::default()
    };
    std::panic::set_hook(Box::new(|_| {}));
    let (_, seq) = sweep(0, 12, &cfg, 1);
    let (_, par) = sweep(0, 12, &cfg, 4);
    assert!(!seq.is_empty(), "bug should be caught in 12 seeds");
    let ids = |v: &[seedraft::runner::Outcome]| v.iter().map(|o| (o.seed, o.trace_hash)).collect::<Vec<_>>();
    assert_eq!(ids(&seq), ids(&par));
    assert!(ids(&par).windows(2).all(|w| w[0].0 < w[1].0));
}

#[test]
fn coverage_counts_what_the_schedules_contain() {
    let cfg = RunConfig::default();
    let mut by_hand = Coverage::default();
    let mut crashes = 0;
    for seed in 0..30 {
        let o = run_seed(seed, &cfg);
        crashes += o
            .schedule
            .iter()
            .filter(|s| matches!(s.fault, seedraft::runner::Fault::Crash(_)))
            .count() as u64;
        by_hand.add(&o);
    }
    assert_eq!(by_hand.crashes, crashes);
    assert_eq!(by_hand.crashes, by_hand.restarts, "every crash is undone");
    assert_eq!(by_hand.partitions, by_hand.heals);
    assert!(by_hand.crashes > 0 && by_hand.partitions > 0);
    assert!(
        by_hand.crash_during_partition > 0,
        "the interesting overlap is exercised"
    );
    assert!(by_hand.reelections > 0);
}

#[test]
fn snapshot_runs_report_installs() {
    let cfg = RunConfig {
        snapshot_every: 8,
        ..RunConfig::default()
    };
    let (cov, bad) = sweep(0, 30, &cfg, 2);
    assert!(bad.is_empty());
    assert!(
        cov.snapshots_installed > 0,
        "some follower should catch up by snapshot"
    );
}

#[test]
fn corpus_parsing() {
    let e = corpus::parse("# c\n\ndouble-vote 3\nnone 9 3 8 # trailing\n").unwrap();
    assert_eq!(e.len(), 2);
    assert_eq!(
        (e[0].bug, e[0].seed, e[0].nodes, e[0].snapshot_every),
        (Some(Bug::DoubleVote), 3, 5, 0)
    );
    assert_eq!(
        (e[1].bug, e[1].seed, e[1].nodes, e[1].snapshot_every),
        (None, 9, 3, 8)
    );
    for bad in [
        "bogus 1",
        "none",
        "none x",
        "none 1 2",
        "none 1 5 z",
        "none 1 5 0 9",
    ] {
        let err = corpus::parse(bad).unwrap_err();
        assert!(err.starts_with("line 1:"), "{bad}: {err}");
    }
}

#[test]
fn checked_in_corpus_replays_as_recorded() {
    std::panic::set_hook(Box::new(|_| {}));
    let text = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/corpus/seeds.txt")).unwrap();
    let entries = corpus::parse(&text).unwrap();
    assert!(entries.iter().any(|e| e.bug.is_some()) && entries.iter().any(|e| e.bug.is_none()));
    let bad = corpus::replay(&entries);
    assert!(bad.is_empty(), "{bad:#?}");
}

#[test]
fn replay_flags_a_bugged_entry_that_is_not_caught() {
    std::panic::set_hook(Box::new(|_| {}));
    // accept-stale-append is rare: most early seeds pass, which a corpus must not claim to fail.
    let cfg = |seed| corpus::Entry {
        bug: Some(Bug::AcceptStaleAppend),
        seed,
        nodes: 5,
        snapshot_every: 0,
    };
    let quiet = (0..30)
        .find(|&s| run_seed(s, &cfg(s).config()).ok())
        .expect("a passing seed");
    let msgs = corpus::replay(&[cfg(quiet)]);
    assert_eq!(msgs.len(), 1, "{msgs:?}");
    assert!(msgs[0].contains("no longer caught"), "{msgs:?}");
}

#[test]
fn cli_sweep_prints_coverage_and_corpus_command_works() {
    let run = |args: &[&str]| {
        let o = Command::new(env!("CARGO_BIN_EXE_seedraft"))
            .args(args)
            .output()
            .unwrap();
        (
            o.status.code().unwrap(),
            String::from_utf8_lossy(&o.stdout).into_owned(),
        )
    };
    let (code, out) = run(&["sweep", "--seeds", "0..6", "--jobs", "2"]);
    assert_eq!(code, 0, "{out}");
    assert!(
        out.contains("6 seeds (0..6)") && out.contains("coverage:"),
        "{out}"
    );
    let (code, out) = run(&["corpus"]);
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("0 unexpected"), "{out}");
    assert_eq!(run(&["sweep", "--seeds", "0..2", "--jobs", "x"]).0, 2);
    assert_eq!(run(&["corpus", "/nonexistent/file"]).0, 2);
}
