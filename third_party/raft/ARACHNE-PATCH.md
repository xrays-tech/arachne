# Arachne D-S1 patch to `raft` 0.7.0

This directory is a byte-identical copy of the crates.io `raft` 0.7.0 source
tree, with ONE minimal, upstreamable modification (decision **D-S1**, test-plan
§5 E3). The single functional change is in `src/raft.rs`: a thread-local,
optional **seedable election RNG**. Upstream `raft` draws the randomized
election timeout with the unseedable, process-wide `thread_rng`
(`reset_randomized_election_timeout`), which makes the L2 same-seed double-run
determinism gate unprovable (election jitter is an uncontrolled entropy source).
The patch adds two public test hooks — `set_election_rng_seed(seed)` and
`clear_election_rng_seed()` — plus a per-node `StdRng` sub-stream seeded by
`seed ^ node_id` ("one sub-stream per host", test-plan E3); when a seed is set,
each node's timeout is drawn from its sub-stream, and when no seed is set the
upstream `thread_rng` behavior is preserved **exactly** (no production behavior
change). The hooks are re-exported at the crate root in `src/lib.rs` (a single,
non-behavioral `pub use` addition — required because `src/raft.rs` is a private
module in non-test builds); that re-export is the only edit outside `raft.rs`
and changes no behavior.

Seeded determinism requires **all nodes to run on a single thread** (test-plan
§5 E8: L2 uses a current-thread runtime). The sub-streams are thread-local, so a
multi-thread runtime that migrates a node between worker threads would restart
that node's stream from `base ^ id`; the unseeded production path is unaffected.

The intent is to upstream this hook to `tikv/raft-rs` so Arachne can drop the
`[patch.crates-io]` override. In-repo tracking is this document plus the root
`Cargo.toml` `[patch.crates-io]` entry; a repo-wide cargo-deny / vendored-source
audit list is **not yet in place** (adding one is tracked for M4). The double-run
gate runs in full once the RNG is injectable. See the root `Cargo.toml`
`[patch.crates-io]` entry and `arachne/tests/determinism_canary.rs` (the
double-run canary that exercises it).
