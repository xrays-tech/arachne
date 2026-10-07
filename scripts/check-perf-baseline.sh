#!/usr/bin/env bash
#
# check-perf-baseline.sh — performance-regression gate for B1/B2/B3
# (hyper HTTP + WAL fdatasync + WAL group commit).
#
# Purpose
# -------
# Reproducibly runs the arachne 3-node docker benchmark and asserts the T1–T5
# throughput/latency thresholds (plus the post-B3 put baseline) so the
# hand-rolled "run docker/bench and eyeball the table" step becomes a
# repeatable, diff-friendly gate. It:
#   1. cross-compiles `arachne-node` for aarch64-unknown-linux-musl,
#   2. builds the node runtime image (docker/bench/Dockerfile) and the
#      JSON-mode driver image (Dockerfile.driver),
#   3. brings up the 3-node cluster and waits for `readyz` (bounded),
#   4. runs the in-network driver (JSON report) RUNS times and parses each;
#      T1/T2/T3/T4/B3 are evaluated on the final run, while the high-variance
#      T5 4-way linear read floor is evaluated on the MEDIAN of RUNS
#      (per oracle F5),
#   5. fails if any threshold is missed.
#
# Usage
# -----
#   bash scripts/check-perf-baseline.sh           # full arachne benchmark
#                                                 # (T1–T4 + B3 put baseline +
#                                                 # T5 4-way linear read regression floor;
#                                                 # 4w measured as the median of RUNS)
#   bash scripts/check-perf-baseline.sh --fast    # critical items only (put 1-way
#                                                 # + linear read 1-way), short duration;
#                                                 # T5 (4w) is skipped in --fast mode
#   bash scripts/check-perf-baseline.sh --etcd    # arachne, then the etcd
#                                                 # v3.5.21 counterpart (context only)
#
#   --etcd runs the etcd cluster AFTER arachne finishes (arachne is the pass/fail
#   gate; etcd is comparison data). --fast applies to the arachne run only.
#
# Thresholds (dev-docs/upgrade-http-wal.md §2, post-B3 re-baseline)
# ----------------------------------------------------------------
#   T1  linear read 1-way :  rps >= 400  and  p50 <= 5.0 ms
#   T2  put 1-way         :  rps >= 200  and  p50 <= 5.0 ms
#   T3  stale read 4-way  :  rps >= 200
#   T4  put 1-way p50     :  p50  <= 5.0 ms
#   B3  put 1-way (new)   :  rps >= 400  and  p50 <= 3.0 ms
#   T5  4-way linear read :  rps >= 2739  (regression floor, see below)
#
#   T5 (Phase-5.1) is a REGRESSION-BASED FLOOR, not an absolute: the 4-way linear
#   read must not be worse than the pre-change 4w baseline on this machine — the
#   Phase-1 baseline doc (dev-docs/bench-baseline-2026-10-07.md) main caliber
#   `--process --keep-alive --read-workers 1,2,4,8 n=400` measured 4w linear read
#   at 2739 ops/s (p50 0.365 ms); later phases only improved it (Phase-2 2892,
#   Phase-4 2917). Hence the floor is 2739. The 4w regime is high-variance (~2x,
#   21-run range 1360-3025 per the plan/oracle F5), so T5 evaluates the MEDIAN
#   of RUNS in-script driver runs rather than a single run. T1 (1-way) remains
#   the PRIMARY single-connection signal.
#
#   --fast asserts T1 (rps>=400 and p50<=5.0) and T2 (rps>=200 and p50<=5.0)
#   only, at a reduced op count. --fast does NOT run the 4-way linear read
#   benchmark (the driver's --fast path only emits put 1-way + linear read
#   1-way), so the T5 4w median gate is skipped in --fast mode.
#
# Dependencies
# ------------
#   docker + docker compose, cargo + cargo-zigbuild, rust target
#   aarch64-unknown-linux-musl, python3, curl.
#   --etcd additionally requires network reach to quay.io (coreos/etcd:v3.5.21).
#
# Environment
# -----------
#   CARGO_TARGET_DIR — respected if already set to a repo-local dir; otherwise
#   this script falls back to <repo>/.dsh-target so sandboxed environments that
#   reject out-of-repo target writes still succeed.
#   SKIP_BUILD=1 — skip the zigbuild (assume a fresh binary is already in bin/).
#   ETCD_IMG  — optional; override the etcd image name.
#
# Exit codes
# ----------
#   0  all thresholds met
#   1  one or more thresholds missed
#   2  environment failure (missing dep, build failed, cluster won't come up,
#        port conflict)
#   3  driver produced no parseable JSON
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BENCH_DIR="${ROOT_DIR}/docker/bench"
ETCD_DIR="${ROOT_DIR}/docker/bench-etcd"
BIN_DIR="${BENCH_DIR}/bin"
DRIVER_BIN="${BIN_DIR}/arachne-node"

# --- args ------------------------------------------------------------------
FAST=0
ETCD=0
N=100
# RUNS: how many times to drive the benchmark (for the T5 4w linear-read
# regression floor). The 4w regime is high-variance (~2x), so T5 uses the
# MEDIAN of RUNS runs rather than a single run (oracle F5 / plan 5.1).
# 3 is the default (the plan allows 2-4).
RUNS=3
while [[ $# -gt 0 ]]; do
  case "$1" in
    --fast) FAST=1; N=50; shift;;
    --etcd) ETCD=1; shift;;
    --n)    shift; N="$1"; shift;;
    -h|--help)
      echo "usage: $0 [--fast] [--etcd] [--n OPS]"
      echo "  --fast   critical items only (put 1-way + linear read 1-way), short duration"
      echo "  --etcd   also run the etcd v3.5.21 comparison cluster after arachne"
      echo "  --n N    per-benchmark op scale (default 100; --fast sets 50)"
      exit 0;;
    *) echo "unknown option: $1" >&2; exit 2;;
  esac
done

# --- helpers -----------------------------------------------------------------
die() { local rc="${1:-2}"; shift; printf '\n%s\n' "$*" >&2; exit "${rc}"; }
info() { printf '%s\n' "$*"; }

# --- target dir (sandbox-safe) ------------------------------------------------
if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
  case "${CARGO_TARGET_DIR}" in
    "$ROOT_DIR"/*) : ;;
    *) CARGO_TARGET_DIR="${ROOT_DIR}/.dsh-target"; export CARGO_TARGET_DIR;;
  esac
else
  CARGO_TARGET_DIR="${ROOT_DIR}/.dsh-target"; export CARGO_TARGET_DIR
fi
mkdir -p "${CARGO_TARGET_DIR}"

# --- dependency pre-flight -----------------------------------------------------
for cmd in docker cargo cargo-zigbuild python3 curl rustup; do
  command -v "$cmd" >/dev/null 2>&1 || die 2 "missing dependency: $cmd"
done
rustup target list 2>/dev/null | grep -q "aarch64-unknown-linux-musl (installed)" \
  || die 2 "rust target 'aarch64-unknown-linux-musl' is not installed (run: rustup target add aarch64-unknown-linux-musl)"
docker info >/dev/null 2>&1 || die 2 "docker daemon not reachable"

# --- port-conflict pre-check (tolerate => report and refuse) --------------------
# Real TCP connect check (python stdlib), not a readyz probe: any process
# already binding one of the cluster ports would make compose up fail with a
# cryptic "port already allocated"; catch it up front with a clear message.
if python3 - <<'PY'
import socket, sys
ports = [7001,7002,7003,8001,8002,8003]
busy = []
for p in ports:
    s = socket.socket()
    try:
        s.settimeout(0.2)
        r = s.connect_ex(('127.0.0.1', p))
    finally:
        s.close()
    if r == 0:
        busy.append(p)
print("  busy ports:", ", ".join(map(str,busy)) if busy else "none")
sys.exit(1 if busy else 0)
PY
then
  info "  all cluster ports free"
else
  info "  (port conflict detected — aborting before compose)"
  die 2 "ports in use: 7001-7003/8001-8003; stop the conflicting service"
fi

# --- cleanup trap (guaranteed) --------------------------------------------------
cleanup() {
  local rc=$?
  echo
  echo "== cleanup =="
  # Tidy transient per-run artifacts (temp dirs / logs) if any were created.
  rm -rf "${run_dir:-}" "${err_log:-}" >/dev/null 2>&1 || true
  (cd "${BENCH_DIR}" >/dev/null 2>&1 && docker compose down -v >/dev/null 2>&1) || true
  (cd "${BENCH_DIR}" >/dev/null 2>&1 && docker compose rm -f -v >/dev/null 2>&1) || true
  if [[ ${ETCD} -eq 1 ]]; then
    (cd "${ETCD_DIR}" >/dev/null 2>&1 && docker compose down -v >/dev/null 2>&1) || true
    (cd "${ETCD_DIR}" >/dev/null 2>&1 && docker compose rm -f -v >/dev/null 2>&1) || true
  fi
  # Remove the transient per-run node image (large, specific to this build).
  # The --etcd counterpart, if used.
  docker rmi arachne-bench:local >/dev/null 2>&1 || true
  docker rmi arachne-bench-etcd:local >/dev/null 2>&1 || true
  # NOTE: the driver image arachne-bench-driver:local is INTENTIONALLY kept.
  # It carries the cached `apk add python3` layer, which is the ONLY way python3
  # reaches this sandbox (the alpine APK repo is reachable through a flaky
  # container TLS path; a fresh `apk add python3` fails). Removing it would
  # make subsequent runs fail to rebuild the driver. It is a build *dependency*
  # (like a base image), not per-run residue. Containers, volumes, networks and
  # the node image are all fully torn down.
  echo "  (clusters down, volumes/networks removed, node image removed; " \
       "driver image retained as cached dependency)"
}
trap cleanup EXIT INT TERM

# --- 1. build the aarch64-musl binary --------------------------------------------
echo "== 1. build arachne-node (aarch64-unknown-linux-musl) =="
info "  CARGO_TARGET_DIR=${CARGO_TARGET_DIR}"
if [[ -n "${SKIP_BUILD:-}" ]]; then
  info "  (SKIP_BUILD=1: skipping rebuild)"
else
  if [[ ! -f "${CARGO_TARGET_DIR}/aarch64-unknown-linux-musl/release/arachne-node" ]]; then
    info "  (rebuilding via cargo zigbuild …)"
  else
    info "  (reusing cached binary in ${CARGO_TARGET_DIR}/aarch64-unknown-linux-musl/release/)"
  fi
  if cargo zigbuild --release -p arachne-kv-node --target aarch64-unknown-linux-musl \
      2>&1 | tail -n 25; then
    :
  else
    die 2 "zigbuild failed"
  fi
fi
cp "${CARGO_TARGET_DIR}/aarch64-unknown-linux-musl/release/arachne-node" "${DRIVER_BIN}"
chmod +x "${DRIVER_BIN}"
info "  binary at ${DRIVER_BIN}"

# --- 2. build images --------------------------------------------------------------
# Cached builds (no --no-cache): the driver image's `apk add python3` layer is
# cached in this sandbox (the alpine APK repo is only reachable through a
# flaky container TLS path, but a previously-built driver image already carries
# python3, so the layer is reused). The node image only copies the (freshly
# built) binary, so its COPY layer updates automatically when the binary changes.
cd "${BENCH_DIR}"
echo "== 2. build node + driver images =="
if ! docker build -t arachne-bench:local -f Dockerfile . 2>&1 | tail -n 10; then
  die 2 "node image build failed"
fi
info "  node image arachne-bench:local ready"
if ! docker compose build >/dev/null 2>&1; then
  die 2 "driver image build failed"
fi
info "  driver image arachne-bench-driver:local ready"

# --- 3. start cluster ---------------------------------------------------------------
echo "== 3. start cluster =="
if ! docker compose up -d; then
  die 2 "docker compose up failed"
fi

# --- 4. bounded wait for readyz -----------------------------------------------------
echo "  waiting for readyz on 8001/8002/8003 (bounded, 90s each)…"
for port in 8001 8002 8003; do
  ok=0
  for i in $(seq 1 90); do
    curl -fsS "http://127.0.0.1:${port}/readyz" >/dev/null 2>&1 && { ok=1; break; }
    sleep 1
  done
  [[ ${ok} -eq 1 ]] || die 2 "port ${port} did not become ready after 90s"
  info "  port ${port} ready"
done

# --- 5. run driver (JSON) ---------------------------------------------------------------
err_log="$(mktemp)"
run_dir="$(mktemp -d)"
# driver_run: run the in-network driver once, passing through "$@" (per-run
# flags). stderr is captured to err_log for diagnostics; stdout is the JSON.
driver_run() {
  docker compose run --rm driver --json --hosts node1,node2,node3 "$@"
}
# In --fast mode the driver emits only put 1-way + linear read 1-way, so T5
# (4-way linear read) has no data and is skipped. In full mode we re-run the
# driver RUNS times (Phase-1 caliber) so T5 can take the median (F5).
if [[ ${FAST} -eq 1 ]]; then
  NRUNS=1
  echo "  running driver --json --fast --n ${N} …"
  if ! driver_run --fast --n "${N}" 2>"${err_log}" > "${run_dir}/run_${NRUNS}.json"; then
    cat "${err_log}" >&2
    rm -rf "${run_dir}" "${err_log}"
    die 2 "driver run failed"
  fi
else
  # Full path: Phase-1 caliber -- keep-alive + process (GIL-isolated) + read
  # workers 1,2,4,8 + n=400. This is the same caliber as the Phase-1 baseline
  # (dev-docs/bench-baseline-2026-10-07.md §2.1) whose 4-way linear read is
  # 2739 rps, the regression floor guarded by T5. The historical (fresh-conn,
  # thread) layout would measure far lower and would not be comparable.
  NRUNS=0
  echo "  running driver (keep-alive, process, 4w gradient, n=400) ${RUNS}x …"
  for i in $(seq 1 "${RUNS}"); do
    NRUNS=$((NRUNS + 1))
    echo "    run ${NRUNS}/${RUNS} …"
    if ! driver_run --keep-alive --process --read-workers 1,2,4,8 --n 400 \
          2>"${err_log}" > "${run_dir}/run_${NRUNS}.json"; then
      cat "${err_log}" >&2
      rm -rf "${run_dir}" "${err_log}"
      die 2 "driver run ${NRUNS}/${RUNS} failed"
    fi
  done
fi
rm -f "${err_log}"
info "  driver JSON captured (${NRUNS} run(s) for T5 median)"

# --- 6. verify JSON + evaluate thresholds + print table -------------------------------
info ""
info "== perf gate results =="
# The per-run JSON reports live under RUNDIR (run_1.json … run_NRUNS.json) because
# T5 (4-way linear read) is evaluated on the MEDIAN of RUNS runs (F5). `python3 -`
# reads the *program* from stdin (the heredoc), so the data path comes from env.
RUNDIR="${run_dir}" FAST="${FAST}" NRUNS="${NRUNS}" python3 - <<'PY'
import sys, json, os, glob, statistics

run_dir = os.environ.get("RUNDIR")
fast = (os.environ.get("FAST", "0") == "1")

# Load all runs, in order.
run_files = sorted(glob.glob(os.path.join(run_dir, "run_*.json")))
if not run_files:
    print("ERROR: no driver JSON found under RUNDIR")
    sys.exit(3)
runs = []
for f in run_files:
    try:
        runs.append(json.loads(open(f).read()))
    except Exception:
        print(f"ERROR: could not parse {f}")
        sys.exit(3)

def m(run, name):
    """Return (rps, p50_ms, p99_ms) for the named result in this run, or None."""
    by_name = {x["name"]: x for x in run["results"]}
    r = by_name.get(name)
    if r is None:
        return None
    return float(r["rps"]), float(r["p50_ms"]), float(r["p99_ms"])

# Checks: (label, name, min_rps, p50_max).
checks = [
    # (label, name, min_rps, p50_max)
    ("T1 linear read 1-way", "get-linear-1w", 400.0, 5.0),
    ("T2 put 1-way",         "put-seq-1w",    200.0, 5.0),
    ("T3 stale read 4-way",  "get-stale-4w",  200.0, None),
    ("T4 put 1-way p50",     "put-seq-1w",       None, 5.0),
    ("B3 put 1-way baseline","put-seq-1w",    400.0, 3.0),
]
if fast:
    # --fast emits only put 1-way + linear read 1-way, so only T1/T2 apply.
    checks = [
        ("T1 linear read 1-way", "get-linear-1w", 400.0, 5.0),
        ("T2 put 1-way",         "put-seq-1w",    200.0, 5.0),
    ]

# The T5 4-way linear-read regression floor, evaluated on the MEDIAN of runs.
# 2739 rps = the Phase-1 baseline (dev-docs/bench-baseline-2026-10-07.md, main
# caliber --process --keep-alive, n=400) 4-way linear read; later phases only
# improved it (Phase-2 2892, Phase-4 2917). A p50 cap (1.5 ms) guards against
# a latency regression even where rps happens to hold.
T5 = ("T5 4-way linear read", "get-linear-4w", 2739.0, 1.5)
print(f"{'item':<26} {'rps':>9} {'p50ms':>9} {'p99ms':>9}  verdict")
print("-"*64)
nfail = 0
for label, name, min_rps, p50_max in checks:
    # Existing checks (T1-T4, B3) evaluate on the final run.
    vals = m(runs[-1], name)
    if vals is None:
        verdict = "MISSING"
        nfail += 1
        print(f"{label:<26} {'?':>9} {'?':>9} {'?':>9}  {verdict}")
        continue
    rps, p50, p99 = vals
    ok = True
    if min_rps is not None and rps < min_rps:
        ok = False
    if p50_max is not None and p50 > p50_max:
        ok = False
    verdict = "PASS" if ok else "FAIL"
    if not ok:
        nfail += 1
    print(f"{label:<26} {rps:>9.1f} {p50:>9.3f} {p99:>9.3f}  {verdict}")

if not fast:
    # T5: collect (rps, p50) across every run that has it, then take the median.
    rps_vals = []
    p50_vals = []
    for run in runs:
        v = m(run, T5[1])
        if v is not None:
            rps_vals.append(v[0])
            p50_vals.append(v[1])
    if rps_vals:
        rps_med = statistics.median(rps_vals)
        p50_med = statistics.median(p50_vals)
        min_rps, p50_max = T5[2], T5[3]
        ok = True
        if rps_med < min_rps:
            ok = False
        if p50_max is not None and p50_med > p50_max:
            ok = False
        verdict = "PASS" if ok else "FAIL"
        if not ok:
            nfail += 1
        print(f"{T5[0]:<26} {rps_med:>9.1f} {p50_med:>9.3f}   (median) {verdict}")
        print(f"{'  ' + T5[0]:<28} runs: " +
              ", ".join(f"{round(x,1)}" for x in rps_vals))
    else:
        # No 4-way data (e.g. --fast, or driver changed its names).
        verdict = "NO DATA"
        nfail += 1
        print(f"{T5[0]:<26} {'?':>9} {'?':>9} {'?':>9}  {verdict}")

print("-"*64)
if nfail == 0:
    print("ALL THRESHOLDS MET")
else:
    print(f"{nfail} threshold(s) MISSED")
sys.exit(1 if nfail else 0)
PY
arachne_rc=$?
info ""
info "arachne gate verdict: rc=${arachne_rc} (0=PASS)"

# --- 7. optional etcd counterpart (context only, NON-FATAL, NON-HANGING) ------
if [[ ${ETCD} -eq 1 ]]; then
  echo
  echo "== etcd v3.5.21 comparison cluster (context only, not gated) =="
  info "  [note] --etcd requires network reach to quay.io (coreos/etcd)"
  # Each step is bounded so a restricted network (e.g. this sandbox) can never
  # hang the gate; every failure here is a clean skip, not a failure.
  if timeout 90 docker pull quay.io/coreos/etcd:v3.5.21 >/dev/null 2>&1; then
    if ! cd "${ETCD_DIR}" 2>/dev/null; then
      info "  (skipping etcd: cannot find ${ETCD_DIR})"
    elif ! timeout 90 docker compose build >/dev/null 2>&1; then
      info "  (skipping etcd: driver image build failed)"
    elif ! timeout 60 docker compose up -d >/dev/null 2>&1; then
      info "  (skipping etcd: cluster failed to start)"
    else
      info "  waiting for etcd healthy (bounded, 60s)…"
      healthy=0
      for i in $(seq 1 60); do
        if curl -fsS "http://127.0.0.1:2379/v2/health" >/dev/null 2>&1; then
          healthy=1; break
        fi
        sleep 1
      done
      if [[ ${healthy} -eq 1 ]]; then
        info "  running etcd comparison driver …"
        out_etcd="$(timeout 180 docker compose run --rm driver \
                      --hosts etcd1,etcd2,etcd3 2>&1)" || {
          info "  (etcd driver run failed — context only)"
          out_etcd=""
        }
        if [[ -n "${out_etcd}" ]]; then
          printf '%s\n' "${out_etcd}"
        else
          info "  (no etcd output)"
        fi
      else
        info "  (skipping etcd: cluster did not become healthy in time)"
      fi
    fi
  else
    info "  (skipping etcd: could not pull coreos/etcd — network restricted)"
  fi
fi

# etcd is context-only; the gate result is the arachne result.
exit "${arachne_rc}"