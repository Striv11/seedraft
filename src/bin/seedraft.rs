//! `seedraft run --seed N` replays one seed verbosely; `seedraft sweep --seeds A..B` runs many
//! and reports every failing seed (exit code 1 if any failed).

use seedraft::raft::Bug;
use seedraft::runner::{run_seed, sweep, Outcome, RunConfig};
use seedraft::shrink::shrink;
use std::process::ExitCode;

const USAGE: &str = "usage:
  seedraft run --seed N [--nodes K] [--snapshot-every N] [--bug NAME] [--pre-vote] [--membership] [--lease TICKS] [--clock-skew PCT] [--shrink] [--timeline]
  seedraft sweep --seeds A..B [--nodes K] [--snapshot-every N] [--bug NAME] [--pre-vote] [--membership] [--lease TICKS] [--clock-skew PCT] [--shrink] [--jobs J]
  seedraft corpus [FILE]     replay the seed corpus (default corpus/seeds.txt)
bugs (injected on purpose, to check the harness catches them):
  double-vote, skip-log-recency, accept-stale-append, commit-old-term";

/// Delta-debug a failing outcome and print the minimal fault schedule.
fn print_shrunk(o: &Outcome, cfg: &RunConfig) {
    let Some(s) = shrink(o, cfg) else {
        println!("  (failure did not reproduce while shrinking)");
        return;
    };
    println!(
        "  shrunk fault schedule {} -> {} faults in {} runs:",
        s.original_faults,
        s.schedule.len(),
        s.runs
    );
    for f in &s.schedule {
        println!("    {f}");
    }
    if let Some(f) = &s.outcome.failure {
        println!("  still fails: {f}");
    }
}

fn parse_range(s: &str) -> Option<(u64, u64)> {
    let (a, b) = s.split_once("..")?;
    let (a, b) = (a.parse().ok()?, b.parse().ok()?);
    (a <= b).then_some((a, b))
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match real_main(&args) {
        Ok(code) => code,
        Err(msg) => {
            eprintln!("error: {msg}\n{USAGE}");
            ExitCode::from(2)
        }
    }
}

fn flag<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
}

fn real_main(args: &[String]) -> Result<ExitCode, String> {
    let cmd = args.first().ok_or("missing command")?;
    if matches!(cmd.as_str(), "--help" | "-h" | "help") {
        println!("{USAGE}");
        return Ok(ExitCode::SUCCESS);
    }
    if cmd == "corpus" {
        let path = args.get(1).map_or("corpus/seeds.txt", String::as_str);
        let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
        let entries = seedraft::corpus::parse(&text).map_err(|e| format!("{path}: {e}"))?;
        std::panic::set_hook(Box::new(|_| {}));
        let bad = seedraft::corpus::replay(&entries);
        for b in &bad {
            println!("{b}");
        }
        println!("{} corpus entries, {} unexpected", entries.len(), bad.len());
        return Ok(if bad.is_empty() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        });
    }
    let mut cfg = RunConfig::default();
    if let Some(n) = flag(args, "--nodes") {
        cfg.nodes = n.parse().map_err(|_| "bad --nodes")?;
        if !(3..=9).contains(&cfg.nodes) {
            return Err("--nodes must be 3..=9".into());
        }
    }
    if let Some(n) = flag(args, "--snapshot-every") {
        cfg.snapshot_every = n.parse().map_err(|_| "bad --snapshot-every")?;
    }
    if let Some(b) = flag(args, "--bug") {
        cfg.bug = Some(Bug::parse(b).ok_or("unknown --bug")?);
    }
    if let Some(n) = flag(args, "--lease") {
        cfg.lease = n.parse().map_err(|_| "bad --lease")?;
    }
    if let Some(n) = flag(args, "--clock-skew") {
        cfg.clock_skew = n.parse().map_err(|_| "bad --clock-skew")?;
        if !(0..100).contains(&cfg.clock_skew) {
            return Err("--clock-skew must be 0..=99".into());
        }
    }
    cfg.pre_vote = args.iter().any(|a| a == "--pre-vote");
    cfg.membership = args.iter().any(|a| a == "--membership");
    if cfg.bug.is_some() {
        // Broken protocols may panic on corrupt state; that is reported as a failure.
        std::panic::set_hook(Box::new(|_| {}));
    }
    let do_shrink = args.iter().any(|a| a == "--shrink");
    match cmd.as_str() {
        "run" => {
            let seed: u64 = flag(args, "--seed")
                .ok_or("missing --seed")?
                .parse()
                .map_err(|_| "bad --seed")?;
            let o = run_seed(seed, &cfg);
            println!("seed {seed}: {} nodes", cfg.nodes);
            println!("fault schedule:");
            for s in &o.schedule {
                println!("  {s}");
            }
            println!(
                "ops {} ({} completed), committed entries {}, trace hash {:016x}",
                o.ops, o.completed, o.committed, o.trace_hash
            );
            if args.iter().any(|a| a == "--timeline") {
                print!("{}", seedraft::timeline::render(seed, &cfg, &o.schedule, 72));
            }
            match &o.failure {
                None => println!("result: ok"),
                Some(f) => println!("result: FAIL: {f}"),
            }
            if do_shrink && !o.ok() {
                print_shrunk(&o, &cfg);
            }
            Ok(if o.ok() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            })
        }
        "sweep" => {
            let (lo, hi) = parse_range(flag(args, "--seeds").ok_or("missing --seeds")?)
                .ok_or("--seeds must look like A..B with A <= B (B exclusive)")?;
            let jobs = match flag(args, "--jobs") {
                Some(j) => j.parse().map_err(|_| "bad --jobs")?,
                None => std::thread::available_parallelism().map_or(1, |n| n.get()),
            };
            let started = std::time::Instant::now();
            let (cov, bad) = sweep(lo, hi, &cfg, jobs);
            let mut failed = Vec::new();
            for o in &bad {
                println!("seed {}: FAIL: {}", o.seed, o.failure.as_ref().expect("bad"));
                failed.push(o.seed);
                if do_shrink && failed.len() == 1 {
                    print_shrunk(o, &cfg);
                }
            }
            let ops = cov.ops;
            println!(
                "{} seeds ({lo}..{hi}), {ops} ops checked, {} failed",
                hi - lo,
                failed.len()
            );
            println!(
                "coverage: {} crashes, {} restarts, {} partitions, {} heals; {} seeds crashed during a partition, {} saw a re-election; {} snapshots installed; {} membership ops; {}/{} ops completed",
                cov.crashes,
                cov.restarts,
                cov.partitions,
                cov.heals,
                cov.crash_during_partition,
                cov.reelections,
                cov.snapshots_installed,
                cov.membership_ops,
                cov.completed,
                cov.ops
            );
            eprintln!("{:.1}s on {jobs} thread(s)", started.elapsed().as_secs_f64());
            if let Some(s) = failed.first() {
                println!("reproduce: seedraft run --seed {s}");
                return Ok(ExitCode::FAILURE);
            }
            Ok(ExitCode::SUCCESS)
        }
        other => Err(format!("unknown command {other:?}")),
    }
}
