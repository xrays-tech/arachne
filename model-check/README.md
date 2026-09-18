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

**Honest scope (M0 scaffold).** The model's actions are deliberately
idealized: the leader and the term are **fixed** (no elections), messages are
**never lost, delayed, reordered, or duplicated**, and **no node crashes**. On
top of that, `Commit` is only enabled **when a quorum already holds the entry**
at that index, so the commit rule can never be violated. As a result the safety
properties above are **unfalsifiable by construction** — the model can never
produce a counterexample, so "no counterexample found" currently validates the
**stateright wiring only** (D-T2), not Arachne's real consensus logic.

> Note: the harness does **not** exercise the real `arachne` crate — its only
> use is a `version()` banner in the startup line. It does *not* build against
> the shipped API in any meaningful sense at this stage.

## What M2 must add (to make the properties falsifiable)

- **Elections / term changes** (replace the fixed leader/term);
- **Message loss, delay, reordering, and duplication**;
- **Crashes** (nodes going down and recovering);
- **Real-encoding cross-validation** (drive the model with Arachne's actual
  entry encoding instead of a bounded `u8` payload).

Per the design-change checklist, a consensus design change must pass this
harness before it is accepted — that obligation becomes meaningful only once
the M2 additions above are in place (test-plan T3).
