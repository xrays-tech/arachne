# docker/bench-etcd — etcd 3-node benchmark driver (host of docker/bench-etcd)

A reproducible 3-node **etcd v3.5.21** cluster used as the comparison target
for `docker/bench` (Arachne). The driver is stdlib-only Python talking to
etcd's gRPC-gateway JSON endpoints over HTTP/1.1:

```
POST /v3/kv/put    {"key": b64, "value": b64}
POST /v3/kv/range  {"key": b64[, "linearizable": true]}
```

## What this is

- `docker-compose.yml` — three `quay.io/coreos/etcd:v3.5.21` members on a fixed
  `172.30.0.0/24` (static IPs 11/12/13), client ports 2379/2380/2381, peer
  ports 2380/2381, raft quorum `--initial-advertise` etc. Each node has a named
  WAL volume and an `etcdctl endpoint health` healthcheck.
- `Dockerfile.driver` — stdlib-only Python 3 image `etcd-bench-driver:local`
  (alpine + `python3`) that copies `driver/etcdbench.py` as the entrypoint.
- `driver/etcdbench.py` — the benchmark driver (see §Calibration below for the
  concurrency-read changes added here).

## How to run

```bash
# from this directory (sandbox: quay.io reachable; if not, the pull fails fast)
docker compose up -d                       # 3 nodes + (on demand) driver
docker compose run --rm driver \
    --hosts etcd1,etcd2,etcd3 --read-workers 1,2,4,8
docker compose down -v
```

`--hosts` are the compose service names (resolved in-cluster); `--read-workers
W1,W2,...` is the optional linearizable read concurrency gradient (see
§Calibration).

## Calibration (keep-alive, concurrency) — the change here

This driver now mirrors `docker/bench/driver/bench.py`'s keep-alive read
calibration so the two systems are measured on the **same transport model**:

- **keep-alive** = one HTTP/1.1 connection *per worker*, built outside the request
  loop and reused inside (re-opened on a broken conn). This is `Cli(host, port,
  keepalive=True)` used per-worker. The historical `keepalive=False` "fresh"
  mode (one TCP conn per request — what Arachne's node HTTP server forces) is
  preserved for the apples-to-apples baseline.
- **`--read-workers W1,W2,...`** = run the **linearizable** (keep-alive) read
  benchmark across a worker-count gradient. Each worker owns its own keep-alive
  `Cli`; workers take strided chunks of `n` so every request lands exactly once
  (same as Arachne's `ThreadPoolExecutor` strided split). **Default (no flag)**
  preserves the historical 1-way read layout — all prior output is byte-identical.

The read benchmarks use **n=400** (same n as Arachne's linear/stale reads); the
put benchmarks keep their historical op counts (100 sequential / 120 concurrent).

## Linearizable read concurrency gradient — measured results

Both systems measured on **3-node clusters**, **keep-alive**, **linearizable
reads**, **n=400**, on the same machine, same session (Sep 30, 2026). Arachne
values are medians of 2 fresh-cluster runs; etcd values are medians of 4 runs
(2 fresh + 2 warm) — etcd's 4w/8w lines vary ±30–50% across runs (Python GIL +
`ThreadPoolExecutor` + network), 1w/2w are stable.

| read                        | workers | Arachne (ops/s / p50) | Etcd (ops/s / p50) | etcd / arachne (ops/s) |
|-----------------------------|---------|-----------------------|--------------------|------------------------|
| linearizable, keep-alive    | 1w      | 4105 / 0.24 ms        | 3752 / 0.26 ms     | 0.91×                  |
| linearizable, keep-alive    | 2w      | 2527 / 0.39 ms        | 3333 / 0.29 ms     | 1.32×                  |
| linearizable, keep-alive    | 4w      | 1493 / 0.67 ms        | 2053 / 0.45 ms     | 1.37×                  |
| linearizable, keep-alive    | 8w      | 798  / 1.25 ms        | 1213 / 0.73 ms     | 1.52×                  |
| stale, keep-alive           | 4w      | 4409 / 0.17 ms        | — (not run)        | —                      |

Raw etcd runs (n=400): 1w {2512, 3843, 3664, 3840} · 2w {3282, 3570, 3369,
3297} · 4w {2251, 1855, 1208, 2499} · 8w {1442, 1003, 1015, 1411}.
Arachne fresh runs (n=400): 1w {4174, 4035} · 2w {2536, 2518} · 4w {1482,
1504} · 8w {790, 806} · stale4w {4458, 4359}.

### Interpretation

1. **Both systems' linearizable reads anti-scale** from 2w → 8w, and both drop
   hard at 4w/8w:
   - Arachne: 4105 → 2527 → 1493 → 798 ops/s (1w→8w ≈ −81%, p50 0.24→1.25 ms).
   - Etcd: ~3752 → 3333 → 2053 → 1213 ops/s (1w→8w ≈ −68%, p50 0.26→0.73 ms).
   This matches the Arachne side (see `docker/bench/README.md` §Concurrent read
   caliber): the linearizable read path is serialized server-side in both systems.

2. **Etcd beats Arachne at 2w/4w/8w** (1.3–1.5× ops/s, ~30% lower p50 at 4w)
   but is **slightly behind at 1w** (3752 vs 4105 ops/s). So etcd has a
   weaker single-connection read but **scales better with concurrency** (less
   anti-scaling: −68% vs Arachne's −81%).

3. **Confirms the direction of the earlier claim** ("etcd linearizable 4-way ≈
   2500 ops/s exceeds Arachne 4-way ≈ 1384") — etcd's 4-way keep-alive read
   (~2053 ops/s, range 1208–2499) does exceed Arachne's 4-way (~1493).
   Caveat: the earlier "≈2500" was the **single-connection** (1w keep-alive,
   ~3500–3800 ops/s, run as 1w) figure mislabeled as 4-way; the *real* etcd
   4-way is ~2000 ops/s, not 2500.

4. **Stale reads remain the escape hatch** (Arachne 4-way stale 4409 ops/s vs
   linearizable 4-way 1493 — ~3× faster, p50 0.17 vs 0.67 ms), consistent
   across both systems' read path structure.

## Cleanup

After each run:

```bash
cd docker/bench-etcd
docker compose down -v
docker rmi etcd-bench-driver:local        # if you want the transient image gone
```

No data volumes are committed; the cluster is ephemeral. The driver image is a
build dependency of this dir's workflow and may be retained.
