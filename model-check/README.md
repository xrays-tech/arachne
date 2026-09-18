# model-check

Standalone stateright model-checking harness for Arachne (test-plan T3, decision D-T2).

## Why standalone

`model-check/` is deliberately **not** a member of the root workspace:
stateright is a test-only tool and must not enter the main workspace's
dependency tree. The trailing `[workspace]` table in `Cargo.toml` keeps this
crate a standalone package root. Build and run from this directory:

```sh
cd model-check
cargo run
```

## What it checks (M0 scaffold)

`src/main.rs` defines a minimal but real stateright `Model`: a bounded
replicated log for 3 nodes (node 0 = leader, nodes 1..2 = followers) with
safe raft-style replication (truncate at the first divergence, then adopt the
leader's suffix) and quorum-based commit (a node may commit index `i` only if
a quorum of nodes holds the same entry at `i`).

The BFS checker (bounded depth) verifies:

| Property               | Kind      | Meaning                                                        |
|------------------------|-----------|----------------------------------------------------------------|
| `log_matching`         | always    | No two nodes hold different entries at the same index          |
| `state_machine_safety` | always    | The committed (applied) prefix is identical on every node      |
| `commit_within_log`    | always    | The commit watermark never exceeds the log                     |
| `an_entry_committed`   | sometimes | The model is live: an entry can be committed                   |

The entry payload is a bounded `u8` and the term is fixed (no elections),
which keeps the state space small enough for fast, exhaustive checking. The
harness depends on the real `arachne` crate (lean, `default-features = false`)
so it always builds against the shipped API.

## Scope: later milestone

This is the **M0 onboarding scaffold** — it proves the stateright wiring
(D-T2). The full scoped model check (3-node elections, log matching, and
state-machine safety over the designed abstract model, cross-validated against
Arachne's real entry encoding) is a later milestone (test-plan T3). Per the
design-change checklist, a consensus design change must pass this harness
before it is accepted.
