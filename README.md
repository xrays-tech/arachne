# Arachne

A transport-agnostic, linearizable key-value engine for Rust programs, built on the [raft](https://crates.io/crates/raft) consensus algorithm. Arachne is first a **library**: you embed it in your own process, give it a storage backend and a transport, and get a raft-consistent `Handle` for reads and writes. The workspace also ships a small reference node binary (`arachne-node`) that wires storage + transport + HTTP together so you can run and drive a cluster without writing a line of glue.

The consensus, WAL and KV state machine form the `arachne-kv` core. By design the core is **transport-agnostic**:

- it is built against the *seam* traits (`Transport`, `Storage`, `StateMachine`, `Clock`, `Rng`, `FsyncObserver`, …) — the core itself contains no concrete clock, RNG, network code, or file I/O;
- with `--no-default-features` it compiles as a **lean core with zero external crates**, no network dependency, offline-buildable;
- a concrete transport (`arachne-kv-transport-tonic`, tonic / HTTP-2 / rustls) is plugged in by whoever embeds it, the same way the tests plug in an in-memory transport.

## Embedding

Add the core with the transport you want (published to
[crates.io](https://crates.io/crates/arachne-kv), v0.1):

```shell
$ cargo add arachne-kv
```

or in `Cargo.toml`:

```toml
[dependencies]
arachne-kv = "0.1"                            # default: pulls tonic transport
# or a transport-free lean core (zero external crates):
# arachne-kv = { version = "0.1", default-features = false }
# or a local checkout / workspace member instead of crates.io:
# arachne-kv = { path = "../arachne" }
```

For a single in-process node, `arachne_kv::server` gives you a minimal facade —
no tokio runtime to poll, no `Runtime`/`Handle` plumbing:

```rust,ignore
use arachne_kv::server::{Arachne, ArachneError, ClusterConfig, WalConfig};

// One node per process; the marker is decorative, the node lives for the
// process and is shut down explicitly. single_node is an N=1 cluster.
let _server = Arachne::start(ClusterConfig::single_node(1, &data_dir, WalConfig::default()))?;

Arachne::set(b"key", b"value").await?;
let v = Arachne::get(b"key").await?;          // ReadIndex linearizable read
let v = Arachne::get_stale(b"key").await?;    // local (weak) read
Arachne::delete(b"key").await?;
Arachne::shutdown();
```

`Arachne::start(ClusterConfig)` is the one entry point — Arachne is a
distributed engine, so even a single node is expressed as an N=1 cluster
(`ClusterConfig::single_node`); use `ClusterConfig::member` to join a real
cluster. Each process holds at most one node: a second `Arachne::start`
returns `AlreadyInitialized`; static reads before `start` (or after
`shutdown`) return
`NotInitialized`. For multi-node clusters or membership control, use `Runtime`
/ `Handle` directly.

`Handle` is the full client surface: `put` / `get` / `get_stale` / `delete`, membership operations (`add_learner`, `promote_learner`, `remove_member`, `transfer_leader`, `membership`), raw proposal hooks, and `leader_hint` for redirects. The complete assembly — including how to wire the tonic transport and an HTTP front-end — is the `arachne-node` binary's `node.rs`, which serves as the reference integration.

## Consistency semantics

The operation set is deliberately small, and the consistency story is stated up front (full matrix in `arachne/src/lib.rs`):

| Operation | Semantics |
|---|---|
| `put` / `delete` | linearizable write — appended to the raft log, committed on quorum persistence, replied after apply |
| `get` | linearizable read by default — served via **ReadIndex**; a non-leader node redirects or reports `NotLeader` |
| `get_stale` | stale read — served from the local state machine, documented as **not guaranteed monotone** across calls |

Arachne is a **CP** system: at most one leader at a time, writes and linearizable reads require a quorum, and when the quorum is lost every linearizable operation fails (`QuorumUnavailable`) rather than degrading to read-mostly or splitting the brain. `get_stale` keeps working in that state, with the staleness caveat above.

Errors are explicit and typed: `NotLeader` (with a leader hint when known), `QuorumUnavailable`, `Busy` (proposal/read queue full), `Timeout` (bounded wait elapsed, result unknown), `SessionTableFull` / `SessionExpired` for the write-session machinery, `ShuttingDown`, and `Unrecoverable` for fatal storage errors.

## Persistence

The WAL is a segment-based log, fsync'd before acknowledging writes. On Linux the node uses:

- `fdatasync` instead of full `fsync` for the append barrier, and
- `fallocate` with `FALLOC_FL_KEEP_SIZE` to pre-reserve each new segment's extents — space is committed up front, the logical size is untouched, and the durability barrier stays a data+inode flush rather than data+alloc+journal.

A dedicated flusher thread batches the durability work: concurrent proposals that land in the same window are settled with one device sync instead of one per entry.

## The reference node (`arachne-node`)

The workspace includes a runnable node binary that composes the library with the tonic transport and a Hyper HTTP/1.1 server — useful for running a real cluster and as a working example of embedding Arachne.

```console
$ cargo build --release -p arachne-kv-node
$ arachne-node --config path/to/node.toml
```

The config is a TOML file:

```toml
cluster_id = "my-cluster"
node_id = "n1"
listen = "127.0.0.1:7001"          # raft peer port
data_dir = "/var/lib/arachne"
http_listen = "127.0.0.1:8001"     # HTTP client port
initial_cluster = [
  "n1=127.0.0.1:7001",
  "n2=127.0.0.1:7002",
  "n3=127.0.0.1:7003",
]
profile = "lan"   # "lan" or "wan"; both validate against raft's parameter constraints
```

The node's own entry in `initial_cluster` must match `listen`. A `force-recovery` subcommand implements the manual single-node recovery procedure (`arachne-node force-recovery --help`).

### HTTP interface

| Method and path | Meaning |
|---|---|
| `PUT /kv/<key>/<value>` | write `value` under `key` (linearizable) |
| `DELETE /kv/<key>` | delete `key` (linearizable) |
| `GET /kv/<key>` | linearizable read via ReadIndex |
| `GET /kv/<key>?stale=1` | stale read (local state machine) |
| `GET /members` | current membership, e.g. `voters=1,2,3 learners=4` |
| `POST /members/add-learner/<id>` | add a learner |
| `POST /members/promote/<id>` | promote a caught-up learner |
| `POST /members/remove/<id>` | remove a member (a leader hands over first) |
| `POST /members/transfer-leader/<id>` | hand leadership over |
| `GET /readyz` | readiness probe |
| `GET /metrics` | Prometheus-format counters/gauges (raft, WAL, read-index) |

A successful membership change replies only once it is **applied** (durable), not merely proposed. Errors map onto HTTP status codes with a plain-text body: 409 for `NotLeader` / pending-conflict, 503 for `QuorumUnavailable` / `Busy` / `Timeout`, and the typed session and storage errors.

## A word about measurements

The project keeps an apples-to-apples benchmark against an `etcd` v3.5.21 cluster of the same topology (three nodes, same hardware, same client shape; see `docker/bench` and `docker/bench-etcd`). In those runs (single connection, keep-alive, n = 400):

- linear read: roughly **2900–3500 ops/s / p50 0.28–0.36 ms** vs etcd ~1531 ops/s / p50 0.58 ms
- put: roughly **500–700 ops/s / p50 1.1–1.8 ms** vs etcd ~821 ops/s / p50 1.13 ms — same order of magnitude, close to parity
- stale read at 4 connections: on the order of **2200 ops/s** vs etcd ~1388

A Go benchmark client is also included (`docker/bench/driver/bench_go.go`): Go and the Python process-mode driver agree on server behavior to within a roughly constant ~12% client-side overhead, so the two measurements bracket the server's actual cost. Under a 4-connection linear-read load both Arachne and etcd lose throughput to their single-connection figure (a shared property of the read-index round trip), and run-to-run variance in that regime is large; the single-connection and stale-read numbers above are the stable ones.

These figures are a record of a particular environment (Linux containers, same harness), meant to guide tuning — not a claim about every deployment.

## Testing

Beyond the usual unit and integration tests, the repo is built around a handful of invariants the project refuses to hand-wave:

- a **fsync ledger** records every directory/data fsync, so the WAL durability tests assert *which* barriers ran, not just that writes survived;
- a **crash-injection** feature (`fault-injection`, excluded from release builds) kills the node at precise persist/deliver boundaries, and restart recovery is verified against the ledger;
- **faulty storage** and a slow-disk simulation exercise degraded I/O paths;
- an **L2 same-seed deterministic double-run** gate re-runs scenarios with identical seeds and requires identical outcomes.

Gates are wired as `scripts/check-*.sh`; `scripts/check-perf-baseline.sh` enforces the benchmark thresholds above and tears the docker cluster down afterward.

## Workspace layout

- `arachne-kv` — the embeddable core: `src/consensus`, `src/storage` (WAL, segments, sync), `src/runtime` (the actor + client `Handle`), `src/client`, `src/state_machine`, `src/profile`, and the `seam` traits.
- `arachne-kv-seam` — leaf crate with the seam traits and shared value types; no dependencies of its own.
- `arachne-kv-transport-tonic` — the tonic / rustls transport, enabled by the `transport-tonic` feature.
- `arachne-kv-node` — the reference node binary (HTTP server, config, membership surface).
- `arachne-kv-testsupport`, `arachne-kv-sim` — test and simulation support.
- `l2/`, `l4/`, `fuzz/`, `model-check/` — standalone deterministic test harnesses, not part of the cargo workspace.
- `dev-docs/` — the design record (`propsol-v0.2.md` is the running design authority, with each revision annotated).

The dependency rules between crates are enforced by the `scripts/check-deps.sh` gates: test/sim scaffolding never leaks into production trees, and the lean core never links a transport.

## License

MIT OR Apache-2.0, at your option.
