# Arachne

A Rust distributed, linearizable key-value store, built on the [raft](https://crates.io/crates/raft) consensus algorithm. Arachne is designed as a small embedded cluster store: it keeps the consensus and storage core transport-agnostic, ships one runnable node binary, and exposes a plain HTTP/1.1 interface for reads and writes.

The set of operations is deliberately small, and the consistency story is stated up front (more in `arachne/src/lib.rs`):

| Operation | Semantics |
|---|---|
| `put` / `delete` | linearizable write — appended to the raft log, committed on quorum persistence, replied after apply |
| `get` | linearizable read by default — served via **ReadIndex**; a non-leader node redirects or returns `NotLeader` |
| `get_stale` | a read that tolerates staleness — served straight from the local state machine, documented as **not guaranteed monotone** across calls |

It is a CP system: at most one leader at a time, writes and linearizable reads require a quorum, and when the quorum is lost every linearizable operation fails (`QuorumUnavailable`) rather than degrading to read-mostly or splitting the brain. `get_stale` keeps working in that state, with the staleness caveat above.

## What's in the workspace

The workspace separates the transport-agnostic product core from the concrete network and node layers, so the core stays testable and dependency-light.

| Crate | Role |
|---|---|
| `arachne-seam` | Leaf crate holding the seam traits and shared value types (clock, RNG, transport, storage) that the core depends on. No dependencies of its own. |
| `arachne` | The product core: consensus (`RaftNode`), WAL storage, KV state machine. Never references a concrete transport. With `--no-default-features` it builds as a "lean" core with **zero external crates** and no network dependency at all. |
| `arachne-transport-tonic` | The one concrete transport (tonic / HTTP-2 / rustls), enabled through the `transport-tonic` feature. It is the only crate allowed to reference tonic / rustls types. |
| `arachne-node` | The runnable node binary: WAL + raft + KV state machine plus an HTTP/1.1 server for `/readyz`, `/metrics`, `/kv/*` and `/members*`. |
| `arachne-testsupport` | Test-only plumbing (fsync ledger, faulty storage, in-memory transport) used by the integration suite. |
| `arachne-sim` | Simulation harness. |

The dependency rules between them are enforced by the `scripts/check-deps.sh` gates: test/sim scaffolding never leaks into production trees, and the lean core never links a transport.

## Running a node

```console
$ cargo build --release -p arachne-node
$ arachne-node --config path/to/node.toml
```

The config is a TOML file:

```toml
cluster_id = "my-cluster"
node_id = "n1"
listen = "127.0.0.1:7001"
data_dir = "/var/lib/arachne"
http_listen = "127.0.0.1:8001"
initial_cluster = [
  "n1=127.0.0.1:7001",
  "n2=127.0.0.1:7002",
  "n3=127.0.0.1:7003",
]
profile = "lan"   # "lan" or "wan"; both validate against raft's parameter constraints
```

`listen` is the raft peer port, `http_listen` the HTTP client port, and the node's own entry in `initial_cluster` must match `listen`. A `force-recovery` subcommand exists for the manual single-node recovery procedure; run `arachne-node --help` and `arachne-node force-recovery --help` for the details.

## HTTP interface

| Method and path | Meaning |
|---|---|
| `PUT /kv/<key>/<value>` | write `value` under `key` (linearizable) |
| `DELETE /kv/<key>` | delete `key` (linearizable) |
| `GET /kv/<key>` | linearizable read via ReadIndex |
| `GET /kv/<key>?stale=1` | stale read (local state machine) |
| `GET /members` | current membership, e.g. `voters=1,2,3 learners=4` (local state, any node) |
| `POST /members/add-learner/<id>` | add a learner |
| `POST /members/promote/<id>` | promote a caught-up learner |
| `POST /members/remove/<id>` | remove a member (a leader hands over first; only the new leader performs the removal) |
| `POST /members/transfer-leader/<id>` | hand leadership over |
| `GET /readyz` | readiness probe for the HTTP server |
| `GET /metrics` | Prometheus-format counters/gauges (raft, WAL, read-index) |

A successful membership change replies only once it is **applied** (durable), not merely proposed, so an operator can read `/members` right after a `200` and see the new configuration.

Errors map onto HTTP status codes with a plain-text body that says what happened: 409 for `NotLeader` (with a leader hint when known) and for a node that must hand over leadership first, 503 for `QuorumUnavailable`, `Busy` (a proposal or read queue is full), and `Timeout` (a bounded wait expired; the result is unknown), plus the session errors (`SessionTableFull` / `SessionExpired`) and `ShuttingDown`. Fatal storage errors are `Unrecoverable`. The full per-operation matrix is in `arachne/src/lib.rs`.

## Persistence

The WAL is a segment-based log, fsync'd before acknowledging writes. On Linux the node uses:

- `fdatasync` instead of full `fsync` for the append barrier, and
- `fallocate` with `FALLOC_FL_KEEP_SIZE` to pre-reserve each new segment's extents — space is committed up front, the logical size is untouched, and the durability barrier stays a data+inode flush rather than data+alloc+journal.

A dedicated flusher thread batches the durability work: concurrent proposals that land in the same window are settled with one device sync instead of one per entry.

## A word about measurements

The project keeps an apples-to-apples benchmark against an `etcd` v3.5.21 cluster of the same topology (three nodes, same hardware, same client shape; see `docker/bench` and `docker/bench-etcd`). In those runs (single connection, keep-alive, n = 400):

- linear read: roughly **2900–3500 ops/s / p50 0.28–0.36 ms** vs etcd ~1531 ops/s / p50 0.58 ms
- put: roughly **500–700 ops/s / p50 1.1–1.8 ms** vs etcd ~821 ops/s / p50 1.13 ms — the same order of magnitude, close to parity
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

## Layout notes

- `arachne/` — core: `src/consensus`, `src/storage` (WAL, segments, sync), `src/client`, plus the `seam` traits.
- `arachne-node/` — the binary: HTTP server, config, membership surface.
- `arachne-transport-tonic/` — the tonic transport.
- `l2/`, `l4/`, `fuzz/`, `model-check/` — standalone deterministic test harnesses, not part of the cargo workspace.
- `dev-docs/` — the design record (`propsol-v0.2.md` is the running design authority, with each revision annotated).

## License

MIT OR Apache-2.0, at your option.
