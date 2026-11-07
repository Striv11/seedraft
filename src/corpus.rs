//! Seed corpus: a plain-text list of seeds worth replaying forever (failures found by
//! sweeps under injected bugs, plus the same seeds on the real protocol as regression guards).

use crate::raft::Bug;
use crate::runner::{run_seed, RunConfig};

/// One corpus line: which bug (if any) to inject and which seed/shape to run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub bug: Option<Bug>,
    pub seed: u64,
    pub nodes: usize,
    pub snapshot_every: u64,
}

impl Entry {
    pub fn config(&self) -> RunConfig {
        RunConfig {
            nodes: self.nodes,
            snapshot_every: self.snapshot_every,
            bug: self.bug,
            ..RunConfig::default()
        }
    }
}

/// Parse corpus text. Blank lines and `#` comments are skipped; errors name the line.
pub fn parse(text: &str) -> Result<Vec<Entry>, String> {
    let mut out = Vec::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let at = |msg: &str| format!("line {}: {msg}", n + 1);
        let f: Vec<&str> = line.split_whitespace().collect();
        if !(2..=4).contains(&f.len()) {
            return Err(at("expected `<bug|none> <seed> [nodes] [snapshot_every]`"));
        }
        let bug = if f[0] == "none" {
            None
        } else {
            Some(Bug::parse(f[0]).ok_or_else(|| at("unknown bug"))?)
        };
        let seed = f[1].parse().map_err(|_| at("bad seed"))?;
        let nodes = match f.get(2) {
            Some(v) => v.parse().map_err(|_| at("bad nodes"))?,
            None => 5,
        };
        if !(3..=9).contains(&nodes) {
            return Err(at("nodes must be 3..=9"));
        }
        let snapshot_every = match f.get(3) {
            Some(v) => v.parse().map_err(|_| at("bad snapshot_every"))?,
            None => 0,
        };
        out.push(Entry {
            bug,
            seed,
            nodes,
            snapshot_every,
        });
    }
    Ok(out)
}

/// Replay every entry. Returns one message per entry that did not behave as the corpus says
/// (a bugged entry that passed, or a `none` entry that failed).
pub fn replay(entries: &[Entry]) -> Vec<String> {
    let mut bad = Vec::new();
    for e in entries {
        let o = run_seed(e.seed, &e.config());
        match (e.bug, &o.failure) {
            (Some(b), None) => bad.push(format!(
                "{} seed {}: no longer caught (the run passed)",
                b.name(),
                e.seed
            )),
            (None, Some(f)) => bad.push(format!("none seed {}: FAIL: {f}", e.seed)),
            _ => {}
        }
    }
    bad
}
