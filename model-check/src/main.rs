//! Arachne model-checking harness (test-plan T3 / decision D-T2) — M0 scaffold.
//!
//! A minimal but real stateright [`Model`]: a bounded replicated log for 3
//! nodes (node 0 is the leader, nodes 1..2 are followers) with safe raft-style
//! replication and quorum-based commit. The checker verifies the raft safety
//! properties the design depends on — log matching, state-machine safety —
//! plus a liveness property (an entry can be committed).
//!
//! This is the M0 onboarding scaffold: the full scoped model check (3-node
//! elections, the designed abstract model, cross-validation against Arachne's
//! real entry encoding) is a later milestone (see `README.md`).

use stateright::report::WriteReporter;
use stateright::{Checker, Model, Property};

/// Bounded log length (the model's log bound).
const MAX_LOG: usize = 3;
/// Node count: node 0 is the leader, nodes 1..2 are followers.
const NODES: usize = 3;
/// A majority of the 3 nodes.
const QUORUM: usize = 2;
/// BFS exploration depth limit for the scaffold.
const DEPTH: usize = 10;
/// The scaffold's fixed term (no elections in this model yet).
const TERM: u8 = 1;

/// One entry in the replicated log: a fixed term and a bounded payload
/// (two values keep the state space small; the full model uses the real
/// command encoding).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Entry {
    term: u8,
    payload: u8,
}

/// One node's log and commit watermark (`commit` = number of applied entries).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Node {
    log: Vec<Entry>,
    commit: usize,
}

impl Node {
    fn new() -> Self {
        Self {
            log: Vec::new(),
            commit: 0,
        }
    }
}

/// The cluster: 3 nodes (node 0 is the leader).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Cluster {
    nodes: [Node; NODES],
}

impl Cluster {
    fn new() -> Self {
        Self {
            nodes: [Node::new(), Node::new(), Node::new()],
        }
    }
}

/// The model's actions.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Action {
    /// The leader appends an entry with the given payload to its own log.
    Append { payload: u8 },
    /// The leader replicates its log to the given follower.
    Replicate { follower: usize },
    /// The given node advances its commit watermark to the given index.
    Commit { node: usize, index: usize },
}

/// Raft conflict resolution: merge the follower's log with the leader's.
///
/// The common prefix is preserved; from the first divergence the follower
/// truncates its conflicting suffix and adopts the leader's entries. If the
/// leader's log is a prefix of the follower's, the follower keeps its longer
/// log (raft retains longer logs).
fn replicate(leader: &[Entry], follower: &[Entry]) -> Vec<Entry> {
    let mut common = 0;
    while common < leader.len() && common < follower.len() && leader[common] == follower[common] {
        common += 1;
    }
    if common >= leader.len() {
        return follower.to_vec();
    }
    let mut merged = follower.to_vec();
    merged.truncate(common);
    merged.extend_from_slice(&leader[common..]);
    merged
}

impl Model for Cluster {
    type State = Cluster;
    type Action = Action;

    fn init_states(&self) -> Vec<Cluster> {
        vec![self.clone()]
    }

    fn actions(&self, state: &Cluster, actions: &mut Vec<Action>) {
        // Deterministic order: appends, then replications, then commits.
        if state.nodes[0].log.len() < MAX_LOG {
            actions.push(Action::Append { payload: 0 });
            actions.push(Action::Append { payload: 1 });
        }
        for follower in 1..NODES {
            actions.push(Action::Replicate { follower });
        }
        for node in 0..NODES {
            for index in 1..=MAX_LOG {
                actions.push(Action::Commit { node, index });
            }
        }
    }

    fn next_state(&self, state: &Cluster, action: Action) -> Option<Cluster> {
        let mut next = state.clone();
        match action {
            Action::Append { payload } => {
                let leader = &mut next.nodes[0];
                if leader.log.len() >= MAX_LOG {
                    return None;
                }
                leader.log.push(Entry { term: TERM, payload });
                Some(next)
            }
            Action::Replicate { follower } => {
                let merged = replicate(&state.nodes[0].log, &state.nodes[follower].log);
                if merged == state.nodes[follower].log {
                    return None;
                }
                next.nodes[follower].log = merged;
                Some(next)
            }
            Action::Commit { node, index } => {
                let target = &next.nodes[node];
                // The node must hold an entry at that index and not have it
                // committed already (the watermark only advances).
                let Some(entry) = target.log.get(index - 1) else {
                    return None;
                };
                if index <= target.commit {
                    return None;
                }
                // Raft commit rule: a quorum of nodes holds the same entry at
                // that index.
                let quorum = next
                    .nodes
                    .iter()
                    .filter(|n| n.log.get(index - 1) == Some(entry))
                    .count();
                if quorum < QUORUM {
                    return None;
                }
                next.nodes[node].commit = index;
                Some(next)
            }
        }
    }

    fn properties(&self) -> Vec<Property<Cluster>> {
        vec![
            // Log matching: no two nodes hold different entries at the same index.
            Property::<Cluster>::always("log_matching", |_, state| {
                for i in 0..NODES {
                    for j in (i + 1)..NODES {
                        let a = &state.nodes[i].log;
                        let b = &state.nodes[j].log;
                        for k in 0..a.len().min(b.len()) {
                            if a[k] != b[k] {
                                return false;
                            }
                        }
                    }
                }
                true
            }),
            // State-machine safety: the committed (applied) prefix is identical
            // on every node.
            Property::<Cluster>::always("state_machine_safety", |_, state| {
                let max_commit = state.nodes.iter().map(|n| n.commit).max().unwrap_or(0);
                for index in 0..max_commit {
                    let applied: Vec<&Entry> = state
                        .nodes
                        .iter()
                        .filter(|n| n.commit > index)
                        .filter_map(|n| n.log.get(index))
                        .collect();
                    if applied.windows(2).any(|w| w[0] != w[1]) {
                        return false;
                    }
                }
                true
            }),
            // The commit watermark never exceeds the log.
            Property::<Cluster>::always("commit_within_log", |_, state| {
                state.nodes.iter().all(|n| n.commit <= n.log.len())
            }),
            // Liveness: some entry can be committed.
            Property::<Cluster>::sometimes("an_entry_committed", |_, state| {
                state.nodes.iter().any(|n| n.commit >= 1)
            }),
        ]
    }
}

fn main() {
    let model = Cluster::new();
    println!(
        "model-check: arachne {} — bounded {NODES}-node replicated log (max_log={MAX_LOG}, depth={DEPTH})",
        arachne::version()
    );
    let checker = model
        .checker()
        .target_max_depth(DEPTH)
        .spawn_bfs()
        .report(&mut WriteReporter::new(&mut std::io::stdout()));
    checker.assert_properties();
    println!(
        "model-check: PASS — no counterexample for any always property, liveness example found \
         ({} unique states, max depth {})",
        checker.unique_state_count(),
        checker.max_depth()
    );
}
