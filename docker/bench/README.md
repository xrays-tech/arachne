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
