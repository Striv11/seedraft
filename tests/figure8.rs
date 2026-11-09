//! Raft paper figure 8: a leader must not commit an entry from an earlier term just because
//! a majority holds it; a later leader without that entry could still overwrite it.
//! Scripted at the single-node level so the exact message order is deterministic.

use seedraft::raft::{Bug, Entry, Msg, Node, Out, Role};

fn e(term: u64) -> Entry {
    Entry {
        term,
        cmd: Some(term),
    }
}

/// Node 0 of 3 holds [t1, t2] at term 3, wins the election for term 4, and then hears that
/// node 1 has replicated exactly the old entries (index 2) but not its own term-4 no-op.
fn leader_with_old_entry_on_majority(bug: Option<Bug>) -> Node {
    let (mut n, outs) = Node::new(0, 3, 3, None, vec![e(1), e(2)]);
    n.bug = bug;
    let timer = outs
        .iter()
        .find_map(|o| match o {
            Out::SetTimer { id, .. } => Some(*id),
            _ => None,
        })
        .expect("election timer armed");
    n.on_timer(timer);
    assert_eq!(n.term(), 4);
    n.on_message(
        1,
        Msg::VoteReply {
            term: 4,
            granted: true,
        },
    );
    assert_eq!(n.role(), Role::Leader);
    assert_eq!(n.log().len(), 3, "log is [t1, t2, term-4 no-op]");
    n.on_message(
        1,
        Msg::AppendReply {
            term: 4,
            success: true,
            index: 2,
        },
    );
    n
}

#[test]
fn old_term_entry_on_a_majority_is_not_committed() {
    let mut n = leader_with_old_entry_on_majority(None);
    assert_eq!(
        n.commit_index(),
        0,
        "index 2 is from term 2, not the current term"
    );
    // Once the current-term no-op is on a majority, everything before it commits too.
    n.on_message(
        1,
        Msg::AppendReply {
            term: 4,
            success: true,
            index: 3,
        },
    );
    assert_eq!(n.commit_index(), 3);
}

#[test]
fn commit_old_term_bug_commits_it_prematurely() {
    let n = leader_with_old_entry_on_majority(Some(Bug::CommitOldTerm));
    assert_eq!(
        n.commit_index(),
        2,
        "the injected figure-8 bug counts replicas of old entries"
    );
}
