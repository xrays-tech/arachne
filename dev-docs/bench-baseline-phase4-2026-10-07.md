# Read-concurrency Phase 4 — A/B bench baseline (2026-10-07)

This document records the Phase-4 A/B bench run for the read-concurrency work,
measured **after** task 4.1 (read decoupling: ReadIndex round off the actor thread,
resolve_reads on a background task, `pending_reads` drained in `refresh_metrics()`)
and 4.2 (per-peer bounded outbound queues + background sender task). The goal
is a before/after comparison at the **same read caliber** as the Phase-2 medians
in `dev-docs/bench-baseline-2026-10-07.md` §8: **linearizable reads, `--keep-alive`
+ `--process`, n=400**. The Phase 2 gate was "no-regress: 1w ≥ 4157/s"; Phase 2
showed no regression (1w median 4157). This run confirms whether the read-decoupling
and async-outbound changes hold (or improve) throughput.

## Environment

| | |
|---|---|
| Worktree | `/Users/alex/Projects/workspace/Arachne/.slim/worktrees/read-concurrency-dev` (branch `omos/read-concurrency-dev`) |
| Code under test | commit `160d446` — "feat(consensus): per-peer bounded outbound queues + background sender task (4.2)" (includes 4.1 read decoupling) |
| Machine | macOS (Darwin, Apple Silicon); Docker |
| Build | fresh musl release: `CARGO_TARGET_DIR=.dsh-target cargo zigbuild --release -p arachne-kv-node --target aarch64-unknown-linux-musl` |
| Binary sha256 | `9691b06cad46f39892f81f5a70ac5409fa46c2401651604cf69ba88a1db7aeb0` |
| Cluster | 3-node Arachne, `docker/bench` (ports 8001–8003, raft 7001–7003, subnet 172.30.0.0/24) |
| Leader during run | port **8002** (node2) — all `/metrics` sampled here |
| Read caliber | linearizable, keep-alive + `--process`, n=400, read-workers 1/2/4/8 |
| Rounds | 3, reported as **median** (4w noted as high-variance; median + min/max spread) |
| Latency gate | `read_latency` integration test: **PASS** (2.35 s, in-memory, relative-threshold smoke gate) |

## What changed for reads (Phase 4)

- **4.1 (read decoupling):** the ReadIndex round is no longer executed on the hot actor
  thread. The round is scheduled off-thread via `schedule_read_index` (actor → background
  `ReadIndex` task); `resolve_reads` (drain + reply) now runs on its own background task;
  `refresh_metrics()` drains `pending_reads` so the gauge reflects in-flight reads.
  Net: the actor loop no longer blocks on ReadIndex rounds.
- **4.2 (async outbound):** `SendQueue`/`Sender` with per-peer bounded queues + a
  background sender task; outbound frames no longer block the actor on `send()`;
  the `is_leader` flag is no longer required to fire outbound.
- **Not changed (read caliber):** `ReadIndex` still requires a ReadIndex round; no
  write-storm; no weak reads. The only client-side knob kept is `--process`
  (worker pool), same as Phase 2.

## Per-round raw (driver rps, keep-alive + `--process`, n=400)

| round | linear 1w | linear 2w | linear 4w | linear 8w | stale 4w | put 1w |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| R1 | 4267 | 3522 | 2942 | 2006 | 7679 | 593 |
| R2 | 4271 | 3503 | **1360**† | 2024 | 7644 | 583 |
| R3 | 4201 | 3395 | 2917 | 1924 | 7773 | 715 |
| **median (3)** | **4267** | **3503** | **2917** | **2006** | **7679** | **593** |

† R2 4w is a client-side outlier (see "Anomalies"); it is **included** in the median
per the 3-round protocol but does not move it below the other two rounds (2942 / 2917).

## ReadIndex rounds/sec at 4w (leader `/metrics`)

Measured the same way as Phase-1 §7: `arachne_read_index_rounds_total` Δ over the
4w phase, divided by the 4w-phase duration (window = [linear-2w report,
linear-4w report] from the driver stderr, ~0.04 s, sampled at 15 ms; window is
shorter than a sampling period, so the delta is taken over the full window and
interpolated).

| round | window (s) | Δrounds | rounds/s | reads/round | pending_peak |
|:---:|:---:|:---:|:---:|:---:|:---:|
| R1 | 0.0411 | 131 | 3182 | 0.92 | 2 |
| R2 | 0.0802 | 154 | 1920 | 0.71 | 0 |
| R3 | 0.0411 | 134 | 3257 | 0.90 | 4 |
| **median** | | | **3182** | | |
| min / max (4w spread) | | | 1920 / 3257 (1.7× range) | | |

**4w rounds/sec ≈ 3182/s (median), min 1920 / max 3257.** The wide spread is
entirely client-side (process-pool spawn / GIL), not server-side — the leader's
4w p99 is 0.50–0.75 ms and the Δrounds is ~130–154 in all three rounds.

## A/B comparison vs Phase-2 medians

Phase-2 medians (same read caliber, same machine, docker) from
`dev-docs/bench-baseline-2026-10-07.md` §8.

| benchmark | Phase-2 | Phase-4 (this) | Δ | Δ% | verdict |
|:---:|:---:|:---:|:---:|:---:|:---|
| linear 1w | 4157 | 4267 | +110 | +2.6% | **OK — no regress (≥ 4157)** |
| linear 2w | 3532 | 3503 | −29 | −0.8% | within variance (Phase 2 itself ±2×) |
| linear 4w | 2892 | 2917 | +25 | +0.9% | **OK** |
| linear 8w | 2073 | 2006 | −67 | −3.2% | within variance |
| stale 4w | 7822 | 7679 | −143 | −1.8% | within variance (no-regress not required) |
| put 1w (control) | 583 | 593 | +10 | +1.7% | **OK** (control, unchanged path) |

**Headline: no regression.** 1w no-regress gate **passed** (median 4267 ≥ 4157).
4w held at 2917 (≥ 2892) despite being the high-variance regime. The write path
(put 1w) and the no-ReadIndex path (stale 4w) are unchanged by construction and
remain within the expected run-to-run variance.

## Tail latency

- `read_latency` integration gate: **PASS** (2.35 s). The gate pins the *relative*
  property (linearizable read within a small factor of weak read under a write
  storm), which is the invariant the read decoupling must preserve — it did.
- Driver 4w p99 (this run): R1 0.754 ms, R2 0.503 ms, R3 0.589 ms → **median 0.59 ms**.
  1w p99 median 0.28 ms (vs Phase-1 §8, where 4w p99 was ~1.1 ms). The 4w
  linearizable-read p99 is at or below the Phase-1 baseline, consistent with the
  ReadIndex round moving off the actor thread.

## Anomalies / notes

- **R2 4w outlier (rps 1360, window 0.080 s vs 0.041 s):** a client-side
  process-pool / GIL hickup, not server-side. The leader's 4w p99 stayed low
  (0.50 ms), Δrounds stayed ~154, and the other phases in R2 were normal.
  Per the task, 4w is the high-variance regime and the value is reported as
  median + min/max spread (1920–3257 rounds/s). It does **not** change the
  no-regress conclusion.
- **Low `pending_reads` (0–4) in the 4w phase:** consistent with 4.1 — reads are
  resolved off the actor thread and `refresh_metrics()` drains the queue, so the
  gauge rarely spikes. (Phase-1 §8 saw 1–2k pending at 4w.) This is the *expected*
  effect of decoupling reads from the actor thread.
- Coarse 15 ms sampling over a ~0.04 s window means 3–5 samples per window; treat
  absolute rounds/s as directional (the Phase-1 doc itself is "directional"), with
  the median as the stable signal.

## Repro

```bash
cd /Users/alex/Projects/workspace/Arachne/.slim/worktrees/read-concurrency-dev
# 1. fresh musl release build
CARGO_TARGET_DIR=.dsh-target cargo zigbuild --release \
    -p arachne-kv-node --target aarch64-unknown-linux-musl
# 2. copy into the bench image (git-ignored)
cp -p .dsh-target/aarch64-unknown-linux-musl/release/arachne-node docker/bench/bin/arachne-node

# 3. run the cluster and 3 rounds (A/B driver)
cd docker/bench
docker compose up -d --build   # 3 nodes, ports 8001-8003; leader is whichever node
                               # reports arachne_is_leader 1 (this run: 8002)
python3 -B bench-artifacts/phase4/phase4_orchestrator.py   # 3 rounds → summary.json

# 4. teardown (ports 8001-8003)
docker compose down -v
```

Raw artifacts (git-ignored, under `docker/bench/bench-artifacts/phase4/`):
`round1..3.json` (driver results, per-request latency), `round1..3.txt`
(driver stderr with timestamps), `round1..3.metrics` (leader `/metrics`, 15 ms
samples), `summary.json` (per-round benchmarks + 4w-phase analysis).
