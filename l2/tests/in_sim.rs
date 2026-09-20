//! M1 stage 3b — real tonic over turmoil.
//!
//! Two proofs: a 3-node cluster (real `RaftNode` + `WalStorage` + `Runtime`,
//! real tonic gRPC carried by the `TurmoilIo` seam) elects a leader, commits one
//! write, and replicates it to every node; and the whole trace is byte-identical
//! across two same-seed runs.

use arachne_l2::harness::run_three_node;

/// Fixed seed for the sim + raft election RNG.
const SEED: u64 = 0x5EED_3B;

#[test]
#[ignore = "stage 3b spike (refined diagnosis): an outbound raft RPC \
            (leader -> follower) connects but its response never arrives, so \
            the single-task runtime actor stops between ticks (it is awaiting \
            the send inside `step`). Leadership then churns — check-quorum \
            step-downs with terms 1->2->3... — and a client `put` times out and \
            then returns `quorum unavailable`. This is NOT an actor-logic \
            deadlock: the command is received and answered. See the stage 3b \
            entry in dev-docs/handoff-m1.md for the full evidence and the \
            fixes that were tried and ruled out."]
fn three_node_cluster_elects_commits_and_replicates() {
    let obs = run_three_node(SEED);
    assert_eq!(obs.len(), 3, "one observation per node: {obs:?}");

    // A leader was elected.
    assert!(
        obs.iter().any(|o| o.leader_id != 0),
        "a leader must be elected: {obs:?}"
    );

    // Every node observed the committed value (cross-host replication over real
    // tonic carried by turmoil).
    for o in &obs {
        assert_eq!(
            o.value.as_deref(),
            Some(b"v".as_ref()),
            "node {} did not replicate the committed write: {obs:?}",
            o.raft_id
        );
    }

    // All nodes agree on a single leader.
    let leaders: Vec<u64> = obs.iter().map(|o| o.leader_id).collect();
    assert!(
        leaders.iter().all(|&l| l != 0 && l == leaders[0]),
        "all nodes must agree on one leader: {obs:?}"
    );
}

#[test]
#[ignore = "stage 3b spike: see three_node_cluster_elects_commits_and_replicates"]
fn double_run_same_seed_is_deterministic() {
    let a = run_three_node(SEED);
    let b = run_three_node(SEED);
    assert_eq!(a, b, "the same seed must reproduce identical observations");
}
