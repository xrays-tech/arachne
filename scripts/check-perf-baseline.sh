#!/usr/bin/env bash
#
# check-perf-baseline.sh — performance-regression gate for B1/B2/B3
# (hyper HTTP + WAL fdatasync + WAL group commit).
#
# Purpose
# -------
# Reproducibly runs the arachne 3-node docker benchmark and asserts the T1–T4
# throughput/latency thresholds (plus the post-B3 put baseline) so the
# hand-rolled "run docker/bench and eyeball the table" step becomes a
# repeatable, diff-friendly gate. It:
#   1. cross-compiles `arachne-node` for aarch64-unknown-linux-musl,
#   2. builds the node runtime image (docker/bench/Dockerfile) and the
#      JSON-mode driver image (Dockerfile.driver),
#   3. brings up the 3-node cluster and waits for `readyz` (bounded),
#   4. runs the in-network driver (JSON report) and parses it,
#   5. fails if any threshold is missed.
#
# Usage
# -----
#   bash scripts/check-perf-baseline.sh           # full arachne benchmark
#                                                 # (T1–T4 + B3 put baseline)
#   bash scripts/check-perf-baseline.sh --fast    # critical items only (put 1-way
#                                                 # + linear read 1-way), short duration
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
#
#   --fast asserts T1 (rps>=400 and p50<=5.0) and T2 (rps>=200 and p50<=5.0)
#   only, at a reduced op count.
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
if [[ ${FAST} -eq 1 ]]; then
  echo "  running driver --json --fast --n ${N} …"
  out="$(docker compose run --rm driver \
          --json --fast --n "${N}" --hosts node1,node2,node3 2>"${err_log}")" \
    || { cat "${err_log}" >&2; die 2 "driver run failed"; }
else
  echo "  running driver --json …"
  out="$(docker compose run --rm driver \
          --json --hosts node1,node2,node3 2>"${err_log}")" \
    || { cat "${err_log}" >&2; die 2 "driver run failed"; }
fi
rm -f "${err_log}"
info "  driver JSON captured"

# --- 6. verify JSON + evaluate thresholds + print table -------------------------------
if ! printf '%s\n' "${out}" | python3 -c 'import sys,json;json.loads(sys.stdin.read())' \
    >/dev/null 2>&1; then
  die 3 "driver did not emit parseable JSON"
fi

info ""
info "== perf gate results =="
# The JSON report is passed via DATA (env var) because `python3 -` reads the
# *program* from stdin (supplied by the heredoc); the data must come from
# elsewhere. The heredoc provides the program; DATA provides the JSON.
DATA="${out}" FAST="${FAST}" python3 - <<'PY'
import sys, json, os
raw = os.environ["DATA"]
data = json.loads(raw)
res = {r["name"]: r for r in data["results"]}
fast = (os.environ.get("FAST", "0") == "1")
def m(name):
    r = res[name]
    return float(r["rps"]), float(r["p50_ms"]), float(r["p99_ms"])
checks = [
    # (label, name, min_rps, p50_max)
    ("T1 linear read 1-way", "get-linear-1w", 400.0, 5.0),
    ("T2 put 1-way",         "put-seq-1w",    200.0, 5.0),
    ("T3 stale read 4-way",  "get-stale-4w",  200.0, None),
    ("T4 put 1-way p50",     "put-seq-1w",       None, 5.0),
    ("B3 put 1-way baseline","put-seq-1w",    400.0, 3.0),
]
if fast:
    checks = [
        ("T1 linear read 1-way", "get-linear-1w", 400.0, 5.0),
        ("T2 put 1-way",         "put-seq-1w",    200.0, 5.0),
    ]
print(f"{'item':<26} {'rps':>9} {'p50ms':>9} {'p99ms':>9}  verdict")
print("-"*64)
nfail = 0
for label, name, min_rps, p50_max in checks:
    rps, p50, p99 = m(name)
    ok = True
    if min_rps is not None and rps < min_rps:
        ok = False
    if p50_max is not None and p50 > p50_max:
        ok = False
    verdict = "PASS" if ok else "FAIL"
    if not ok:
        nfail += 1
    print(f"{label:<26} {rps:>9.1f} {p50:>9.3f} {p99:>9.3f}  {verdict}")
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