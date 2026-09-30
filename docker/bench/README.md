# docker/bench — Arachne 3-node docker benchmark

A reproducible 3-node `arachne-node` cluster for throughput/latency regression
gating (B1/B2/B3: hyper HTTP + WAL `fdatasync` + WAL group commit).

## What this is

- `docker-compose.yml` — three `arachne-node` containers on a fixed 172.30.0.0/24
  network (static IPs: 11/12/13), each with its own per-node config
  (`configs/n{1,2,3}.toml`) and a named WAL volume. A `driver` service runs the
  in-network Python benchmark so measurements do **not** cross Docker Desktop's
  host-port forwarding (which charges ~20 ms per new connection).
- `Dockerfile` — runtime image `arachne-bench:local`. It copies the
  cross-compiled, statically-linked `bin/arachne-node` binary; alpine is only for a
  shell + busybox `wget` (healthcheck).
- `Dockerfile.driver` — driver image `arachne-bench-driver:local` (alpine + python3).
- `configs/n{1,2,3}.toml` — per-node cluster config (node_id, listen, http_listen,
  initial_cluster, profile).
- `driver/bench.py` — stdlib-only benchmark driver (leader discovery, 3-node
  replication check, put / linear read / stale read throughput + p50/p99).

## Two ways to run it

### A. The gate (recommended) — `scripts/check-perf-baseline.sh`

This is the repeatable, regression-prevention gate. It does the whole loop
automatically and enforces the §2 thresholds:

```bash
bash scripts/check-perf-baseline.sh              # full: T1–T4 + B3 put baseline
bash scripts/check-perf-baseline.sh --fast       # critical items only (put 1-way +
                                                 # linear read 1-way), short duration
bash scripts/check-perf-baseline.sh --etcd       # + etcd v3.5.21 comparison
                                                 # (context only, not gated)
```

Internally it:

1. cross-compiles `arachne-node` for `aarch64-unknown-linux-musl`
   (`cargo zigbuild`, `CARGO_TARGET_DIR` defaults to `<repo>/.dsh-target`);
2. builds the node + driver images (cached build — see note below);
3. brings up the cluster and waits for `readyz` on 8001/8002/8003 (bounded);
4. runs `docker compose run --rm driver --json --hosts node1,node2,node3` and
   parses the JSON report;
5. prints a measured/threshold/verdict table and exits non-zero on any miss;
6. cleans up (`docker compose down -v`, removes the node image).

Exit codes: 0 = all thresholds met · 1 = a threshold missed · 2 = environment
failure · 3 = driver produced no parseable JSON.

`driver/bench.py --json` emits a structured JSON report to **stdout** (human
text is re-routed to stderr), so the gate parses stable fields (`name`, `rps`,
`p50_ms`, `p99_ms`) rather than regex-ing the human table. The default text
output is unchanged.

### B. Manual (ad-hoc) — run the driver directly

For a quick eyeball, from this directory:

```bash
docker compose up -d
python3 driver/bench.py --ports 8001,8002,8003
docker compose down -v
```

Or from inside the cluster network (what the gate does, to avoid Docker Desktop
forwarding noise):

```bash
docker compose run --rm driver --hosts node1,node2,node3
```

## Prerequisites / sandbox notes

- Docker + `docker compose`, `cargo` + `cargo-zigbuild`, and the rust target
  `aarch64-unknown-linux-musl` (`rustup target add aarch64-unknown-linux-musl`).
- **Build the node binary first** (the node image `COPY`s `bin/arachne-node`):
  ```bash
  CARGO_TARGET_DIR=$PWD/.dsh-target cargo zigbuild --release \
      -p arachne-node --target aarch64-unknown-linux-musl
  cp "$CARGO_TARGET_DIR/aarch64-unknown-linux-musl/release/arachne-node" \
      docker/bench/bin/arachne-node
  ```
- **Driver image caching (sandbox).** The driver image's `apk add python3` layer
  must be available in this environment: the alpine APK repo is reachable here
  only through a flaky container TLS path, so a *fresh* `apk add python3` can
  fail. A pre-built `arachne-bench-driver:local` image already carries python3
  and is retained by the gate as a cached dependency (do not delete it on
  this machine). `docker compose build` reuses that cached layer and only re-COPYs
  the new `driver/bench.py`.
- `--etcd` requires network reach to `quay.io` for `coreos/etcd:v3.5.21`; if
  unavailable (e.g. this sandbox) the etcd step is skipped cleanly and the
  arachne result stands.

## Thresholds enforced by the gate

| item | requirement |
|---|---|
| T1 linear read 1-way | rps >= 400 and p50 <= 5.0 ms |
| T2 put 1-way | rps >= 200 and p50 <= 5.0 ms |
| T3 stale read 4-way | rps >= 200 |
| T4 put 1-way p50 | p50 <= 5.0 ms |
| B3 put 1-way baseline | rps >= 400 and p50 <= 3.0 ms |

`--fast` asserts T1 (rps>=400 and p50<=5.0) and T2 (rps>=200 and p50<=5.0) at
a reduced op count.

## Concurrent read caliber (并发读口径)

Three opt-in flags were added to `driver/bench.py` (all default OFF, so the
gate's default path and all prior baselines are byte-identical):

- **`--keep-alive`** — the read benchmarks reuse one persistent HTTP/1.1
  connection per worker (one `HTTPConnection` built outside the request loop,
  `request()`/`getresponse()` reused, re-opened on a broken conn) instead of
  opening a fresh connection per request. This matches etcd's keep-alive /
  goroutine client model and isolates *client-side* per-connection cost from
  *server-side* throughput. The `req()` one-shot path is unchanged, so `--json`
  parsing in the gate is unaffected.
- **`--read-workers 1,2,4,8`** — run the linear/stale reads across a concurrency
  gradient (plus the historical stale 4-way shape). `--n` sets the per-benchmark
  op count. Without the flag the historical read layout is kept; puts are
  unaffected either way.
- **`--process`** — run the read workers as separate Python processes (via
  `concurrent.futures.ProcessPoolExecutor`, one process per worker) instead of
  threads. This fully isolates the GIL so the workers do not serialize on the
  single interpreter lock; it is the way to tell whether the linear-read collapse
  at higher concurrency is driven by client-side GIL cost (A) or server-side
  serialization (B). Orthogonal to `--keep-alive` and `--read-workers`; keep-alive
  semantics are preserved (each worker process builds and reuses its own
  `HTTPConnection`). The job is a module-level, pickle-safe function that owns its
  connection(s), so no in-memory state crosses process boundaries.

```bash
# keep-alive, linear gradient + stale 4-way, 100 ops each (threads)
docker compose run --rm driver --read-workers 1,2,4,8 --n 100 --keep-alive \
    --json --hosts node1,node2,node3

# same gradient, but workers are separate GIL-isolated Python processes
docker compose run --rm driver --read-workers 1,2,4,8 --n 100 --keep-alive \
    --process --json --hosts node1,node2,node3
```

### A/B evidence (fresh 3-node cluster, n=100, linearizable reads on the leader)

| read   | workers | fresh conn (ops/s) | fresh p50 (ms) | keep-alive (ops/s) | keep-alive p50 (ms) |
|--------|---------|--------------------|----------------|--------------------|---------------------|
| linear | 1       | 3175.1             | 0.298          | 4902.0             | 0.195               |
| linear | 2       | 1850.6             | 0.535          | 2410.6             | 0.408               |
| linear | 4       | 1011.1             | 0.991          | 1384.2             | 0.713               |
| linear | 8       | 559.1              | 1.854          | 770.7              | 1.313               |
| stale  | 4       | 2373.1             | 0.417          | 4181.2             | 0.170               |

Findings:

1. **Keep-alive removes the client-side per-request connection setup cost.**
   1-way reads go from 3175 → 4902 ops/s (p50 0.298 → 0.195 ms); stale 4-way
   reads from 2373 → 4181 ops/s (p50 0.417 → 0.170 ms). The old driver's
   per-request `new HTTPConnection + close` (under the GIL) was a real client
   bottleneck (~0.3 ms/op, matching the GIL hypothesis).
2. **Linearizable reads do *not* scale with worker count under either driver.**
   With keep-alive the 4-way read is only 1384 ops/s (p50 0.713 ms) and the 8-way
   read 771 ops/s (p50 1.313 ms) — *below* the 1-way 4902 ops/s. p50 grows
   0.195 → 0.713 → 1.313 ms. Even the fair-caliber keep-alive 4-way read stays
   in the ~700–1500 ops/s band.
3. **Stale reads do *not* collapse with concurrency** (stale 4-way keep-alive
   4181 ops/s), consistent with the *linearizable* read path being the one that
   is serialized.

Conclusion: the driver was a contributing bottleneck (**A**) and keep-alive is
the correct, now-fair client calibration — but the remaining 4-way read gap is
a **server-side serialized critical path (C)** (the actor / WAL durability path),
*not* the driver. The server was deliberately left untouched in this evidence pass.

Continuation (batch-rate observation, server side — see task report):
- Instrument the read path to count the serialized barrier (actor read lock /
  WAL `fdatasync` barrier) per request; confirm the 4-way p50 ≈ 4 × single-request
  barrier, i.e. a single global lock rather than N-way parallelism.
- Correlate read p50 with *concurrent write* load (the B3 group-commit barrier is
  shared): run the linear 4-way read while a put stream is active vs. idle. If read
  p50 degrades with writes, the serialized path is the shared durability barrier.
- Observe how increasing WAL batch size / group-commit frequency (C) moves the
  4-way read p50, to bound how much of the gap is `fdatasync`-driven vs. a read lock.
- Re-run the same gradient once the server-side fix lands; expect linear reads to
  scale toward ~4× the 1-way keep-alive figure (≈ 4902 × 4) if serialization is
  removed.

For same-caliber comparisons against etcd, use the keep-alive driver (etcd's
linearizable 4-way ≈ 2500 ops/s still exceeds arachne's 4-way 1384 under
keep-alive, confirming a remaining server-side deficit rather than a client one).
