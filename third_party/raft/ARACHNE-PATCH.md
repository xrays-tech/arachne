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

## 发布状态（v0.2.25）

The vendored copy in `third_party/raft/` has been prepared for release as
the standalone crate **`raft-seedable`** (decision: repo dev-docs). Summary:

- **Manifest identity**: `Cargo.toml` `[package]` renamed to `name =
  "raft-seedable"`, `version = "0.7.0"` (aligned to the raft 0.7.0 API
  baseline), with fork `description` (thread-local seedable election-RNG hook;
  unseeded behavior identical to upstream), `license = "Apache-2.0"` (upstream
  Apache-2.0 `LICENSE` file retained), `repository =
  https://github.com/xrays-tech/arachne`, `readme = "ARACHNE-PATCH.md"`,
  `keywords = ["raft", "consensus", "deterministic"]`,
  `categories = ["algorithms", "network-programming"]`. Edition stays `"2021"`
  (unchanged from the vendored copy; no `rust-version` added). Dependencies
  match the upstream raft 0.7.0 registry manifest (incl. `rand = "0.8"`, whose
  `rand::rngs::StdRng`/`SeedableRng` the hook uses; no dependency changes
  required). An empty `[workspace]` table makes the crate self-contained so it
  builds standalone without joining the surrounding Arachne workspace.
- **Independent build**: green —
  `CARGO_TARGET_DIR=.dsh-target-fork cargo build --manifest-path
  third_party/raft/Cargo.toml` → `Finished `dev` profile ... in 2.01s`. The
  re-export check `grep set_election_rng_seed src/lib.rs` passes (lib.rs:535).
- **Package**: `cargo package --list --allow-dirty` lists 49 files — all
  `src/*` (incl. `raft.rs` with the D-S1 hooks, `lib.rs` re-export), `benches/`
  (5), `examples/` (2), `Cargo.toml`, `LICENSE`, `ARACHNE-PATCH.md`, plus
  cargo's generated `.cargo_vcs_info.json`; no `.git`. `cargo package` produced
  `raft-seedable-0.7.0.crate` (49 files, 487.0KiB, 113.3KiB compressed).
- **Publish preflight**: `CARGO_TARGET_DIR=.dsh-target-fork cargo publish
  --dry-run --allow-dirty --registry crates-io --manifest-path
  third_party/raft/Cargo.toml` — **passed**: "Packaged 49 files ... Verifying
  ... Compiling raft-seedable v0.7.0 ... Finished ... aborting upload due to
  dry run" (nothing uploaded; no registry mutations).
- **Hygiene**: `Cargo.toml.orig` removed (cargo reserves the name for package
  source; the manifest is now the hand-maintained fork, so the auto-gen backup
  no longer applies) — deletion staged via `git rm` (not committed). Build-side
  `Cargo.lock` artifacts are not kept in the vendored dir (matching the
  registry-source layout).
- **Upstreaming** (contributing the D-S1 hook to `tikv/raft-rs` so the
  `[patch.crates-io]` override can be dropped) remains the long-term exit
  route and is independent of this release.
