use seedraft::runner::{run_seed, schedule, Fault, RunConfig};
use std::process::Command;

#[test]
fn schedule_is_deterministic_and_varies_by_seed() {
    let cfg = RunConfig::default();
    assert_eq!(schedule(3, &cfg), schedule(3, &cfg));
    assert_ne!(schedule(3, &cfg), schedule(4, &cfg));
}

#[test]
fn schedule_keeps_a_majority_up_and_heals_everything() {
    let cfg = RunConfig::default();
    for seed in 0..500 {
        let s = schedule(seed, &cfg);
        let mut down = std::collections::BTreeSet::new();
        let mut part = false;
        let mut last = 0;
        for e in &s {
            assert!(e.at >= last, "seed {seed}: schedule not time-ordered");
            last = e.at;
            match &e.fault {
                Fault::Crash(n) => assert!(down.insert(*n), "seed {seed}: double crash"),
                Fault::Restart(n) => assert!(down.remove(n), "seed {seed}: restart of up node"),
                Fault::Partition(_) => part = true,
                Fault::Heal => part = false,
            }
            assert!(down.len() <= (cfg.nodes - 1) / 2, "seed {seed}: majority lost");
        }
        assert!(down.is_empty() && !part, "seed {seed}: faults left active");
    }
}

#[test]
fn run_is_reproducible() {
    let cfg = RunConfig::default();
    let a = run_seed(17, &cfg);
    let b = run_seed(17, &cfg);
    assert_eq!(a.trace_hash, b.trace_hash);
    assert_eq!(a.schedule, b.schedule);
    assert_eq!(
        (a.ops, a.completed, a.committed),
        (b.ops, b.completed, b.committed)
    );
    assert_ne!(a.trace_hash, run_seed(18, &cfg).trace_hash);
}

#[test]
fn seeds_pass_with_real_progress() {
    let cfg = RunConfig::default();
    let (mut ops, mut done) = (0, 0);
    for seed in 0..60 {
        let o = run_seed(seed, &cfg);
        assert!(o.ok(), "seed {seed}: {}", o.failure.unwrap());
        ops += o.ops;
        done += o.completed;
    }
    assert!(ops > 500 && done * 2 > ops, "weak run: {done}/{ops}");
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
fn cli_sweep_and_run() {
    let (code, out) = bin(&["sweep", "--seeds", "0..8"]);
    assert_eq!(code, 0, "{out}");
    assert!(
        out.contains("8 seeds (0..8)") && out.contains("0 failed"),
        "{out}"
    );
    let (code, a) = bin(&["run", "--seed", "5"]);
    assert_eq!(code, 0, "{a}");
    assert!(a.contains("result: ok") && a.contains("fault schedule:"));
    assert_eq!(
        a,
        bin(&["run", "--seed", "5"]).1,
        "run output must be reproducible"
    );
}

#[test]
fn cli_rejects_bad_usage() {
    assert_eq!(bin(&[]).0, 2);
    assert_eq!(bin(&["sweep", "--seeds", "9..3"]).0, 2);
    assert_eq!(bin(&["run"]).0, 2);
    assert_eq!(bin(&["run", "--seed", "1", "--nodes", "2"]).0, 2);
}

#[test]
fn cli_help_prints_usage_and_succeeds() {
    for flag in ["--help", "-h", "help"] {
        let (code, out) = bin(&[flag]);
        assert_eq!(code, 0, "{flag}");
        assert!(
            out.contains("seedraft sweep") && out.contains("seedraft corpus"),
            "{out}"
        );
    }
    let (code, _) = bin(&["bogus"]);
    assert_eq!(code, 2);
}
