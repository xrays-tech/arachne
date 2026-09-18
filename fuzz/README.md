# Arachne `cargo-fuzz` harness

This directory is a **standalone** `cargo-fuzz` project (it is *not* a member of
the root Arachne workspace — the trailing `[workspace]` table in `Cargo.toml`
keeps it isolated, and the root workspace deliberately does not list it).

## Fuzz target ① — `wal_recovery`

`fuzz_targets/wal_recovery.rs` treats the fuzzer input as a **WAL segment byte
stream**: it materializes a temp data dir with a valid `META` and that segment,
then calls `WalStorage::open`. The target **must not panic on any input** — every
input must land in one of the two INV6 quadrants (a `Result` of `Ok` =
legal-prefix truncation, or `Err` = fail-start).

## Relationship to the deterministic harness (M0 ② / INV6)

The **primary CI evidence** for M0 ② is the deterministic, in-repo harness
`arachne/tests/wal_mutation.rs`, which applies a systematic, bounded mutation
matrix (every truncation length, single-bit flips across the header/`len`/CRC/
type/payload, and `len`-field corruptions, over single- and multi-segment WALs
including the non-last-segment fail-start path) and asserts each mutation lands
in exactly one of the two quadrants with the committed region never silently
lost. That suite runs in `cargo test --workspace` on stable CI and is the
acceptance gate.

The fuzzer here is the **randomized companion**: it explores unbounded input
space for the same guarantee under nightly CI. It is additive evidence, not a
substitute for the deterministic harness.

## Platform note (macOS aarch64)

The default `cargo fuzz run` (with ASan) **hangs in dyld initializers** on
macOS aarch64 + nightly — even `-help=1` never returns. This is a known
libFuzzer/ASan + macOS dyld interaction, not a bug in the target.

**Workaround for local runs:** use `--sanitizer none`:

```sh
cargo +nightly fuzz run --sanitizer none wal_recovery -- -max_total_time=300
```

This runs the fuzzer without ASan (no memory-safety instrumentation) but still
exercises the coverage-guided input generation and the "never panic" assertion.

The **CI fuzz job** (`.github/workflows/ci.yml`) runs on `ubuntu-latest`
(Linux x86-64) where the default ASan configuration works correctly. The job
is restricted to the `nightly` schedule (not per-PR) to keep PR feedback fast.

## CI job

The fuzz job is defined in `.github/workflows/ci.yml` under the `fuzz` job.
It runs on a nightly schedule (cron: `0 4 * * *`) and on manual dispatch:

- `runs-on: ubuntu-latest`
- Nightly toolchain with `rust-src` component
- `cargo +nightly install cargo-fuzz --locked`
- `cargo +nightly fuzz run wal_recovery -- -max_total_time=300`

## Local invocation

```sh
# one-time: install the fuzzer on nightly
cargo +nightly install cargo-fuzz --locked

# build the target (sanity check)
cargo +nightly fuzz build

# run fuzz target ① on Linux (default ASan):
cargo +nightly fuzz run wal_recovery -- -max_total_time=300

# run on macOS aarch64 (must use --sanitizer none):
cargo +nightly fuzz run --sanitizer none wal_recovery -- -max_total_time=300
```

`libfuzzer-sys` is an external crate confined to this standalone harness; it is
**never** pulled into the root workspace (enforced by `scripts/check-deps.sh`,
which only scans workspace members).
