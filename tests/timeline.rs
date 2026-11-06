use seedraft::runner::{Fault, RunConfig, Scheduled};
use seedraft::timeline::render;

fn crash_leader_schedule() -> Vec<Scheduled> {
    vec![
        Scheduled {
            at: 1500,
            fault: Fault::Crash(0),
        },
        Scheduled {
            at: 3000,
            fault: Fault::Partition(vec![vec![1]]),
        },
        Scheduled {
            at: 3800,
            fault: Fault::Heal,
        },
        Scheduled {
            at: 4400,
            fault: Fault::Restart(0),
        },
    ]
}

fn cfg() -> RunConfig {
    RunConfig {
        nodes: 3,
        ..RunConfig::default()
    }
}

#[test]
fn render_is_deterministic() {
    let a = render(7, &cfg(), &crash_leader_schedule(), 60);
    let b = render(7, &cfg(), &crash_leader_schedule(), 60);
    assert_eq!(a, b);
    assert_ne!(a, render(8, &cfg(), &crash_leader_schedule(), 60));
}

#[test]
fn shape_and_markers() {
    let s = render(7, &cfg(), &crash_leader_schedule(), 60);
    let lines: Vec<&str> = s.lines().collect();
    // header, fault row, 3 lanes, term row, legend
    assert_eq!(lines.len(), 7, "{s}");
    for l in &lines[1..6] {
        assert_eq!(l.chars().count(), 6 + 60, "{s}");
    }
    let fault = lines[1];
    for m in ['x', 'P', 'H'] {
        assert!(fault.contains(m), "missing {m}: {s}");
    }
    // Crashed node shows as down, and is up again after restart.
    let n0 = lines[2];
    assert!(n0.contains('X'), "{s}");
    assert!(n0.trim_end().ends_with(['.', 'L', 'c']), "{s}");
}

#[test]
fn leader_changes_after_leader_crash() {
    let s = render(7, &cfg(), &crash_leader_schedule(), 60);
    let lines: Vec<&str> = s.lines().collect();
    // Some surviving node leads while n0 is down; at least two distinct nodes lead overall.
    let leaders = lines[2..5].iter().filter(|l| l[6..].contains('L')).count();
    assert!(leaders >= 2, "expected a leader change:\n{s}");
}

#[test]
fn golden_snapshot() {
    let s = render(7, &cfg(), &crash_leader_schedule(), 40);
    let expected = include_str!("golden/timeline_seed7.txt");
    assert_eq!(
        s, expected,
        "update tests/golden/timeline_seed7.txt if the change is intended:\n{s}"
    );
}
