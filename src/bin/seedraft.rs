//! `seedraft run --seed N` replays one seed verbosely; `seedraft sweep --seeds A..B` runs many
//! and reports every failing seed (exit code 1 if any failed).

use seedraft::raft::Bug;
use seedraft::runner::{run_seed, Outcome, RunConfig};
use seedraft::shrink::shrink;
use std::process::ExitCode;

const USAGE: &str = "usage:
  seedraft run --seed N [--nodes K] [--snapshot-every N] [--bug NAME] [--shrink] [--timeline]
  seedraft sweep --seeds A..B [--nodes K] [--snapshot-every N] [--bug NAME] [--shrink]
bugs (injected on purpose, to check the harness catches them):
  double-vote, skip-log-recency, accept-stale-append";

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
            let mut failed = Vec::new();
            let mut ops = 0;
            for seed in lo..hi {
                let o = run_seed(seed, &cfg);
                ops += o.ops;
                if let Some(f) = &o.failure {
                    println!("seed {seed}: FAIL: {f}");
                    failed.push(seed);
                    if do_shrink && failed.len() == 1 {
                        print_shrunk(&o, &cfg);
                    }
                }
            }
            println!(
                "{} seeds ({lo}..{hi}), {ops} ops checked, {} failed",
                hi - lo,
                failed.len()
            );
            if let Some(s) = failed.first() {
                println!("reproduce: seedraft run --seed {s}");
                return Ok(ExitCode::FAILURE);
            }
            Ok(ExitCode::SUCCESS)
        }
        other => Err(format!("unknown command {other:?}")),
    }
}
