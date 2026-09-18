#!/usr/bin/env bash
set -euo pipefail
#
# kill_loop.sh — L4 real-process `kill -9` WAL-durability harness (D-L4).
#
# What it proves (the things simulation cannot — test-plan §2 L4, §11 缺口 1):
#   * the node survives a real `kill -9` (SIGKILL, no graceful shutdown) and
#     restarts on the SAME data dir;
#   * the WAL opens cleanly on every restart — the node's **fail-start on
#     corruption is the signal**: if a torn write / silent corruption reached
#     the committed region, `WalStorage::open` returns Unrecoverable, the node
#     exits non-zero before `/readyz`, and this script FAILs;
#   * the durable progress (`arachne_commit_index` / `arachne_applied_index`)
#     never regresses across restarts (INV12/INV13 on a real disk).
#
# M0 = scaffold that works locally (a few iterations). The Release gate (M4)
# runs this on the dedicated real-disk CI runner for a bounded window.
#
# Usage:
#   bash l4/kill_loop.sh
#   ARACHNE_NODE=/path/to/arachne-node ARACHNE_L4_ITERATIONS=10 bash l4/kill_loop.sh
#
# Environment overrides:
#   ARACHNE_NODE            path to the arachne-node binary (default: build it)
#   ARACHNE_L4_ITERATIONS   number of kill -9 / restart cycles (default: 5)
#   ARACHNE_L4_READY_TIMEOUT per-restart /readyz timeout in seconds (default: 30)
#   ARACHNE_L4_CONFIG       template node.toml (default: l4/node.toml)
#   ARACHNE_L4_KEEP_ON_FAIL keep the run dir on failure for inspection (default: 1)

# --- Locate the repo root (this script lives in <root>/l4) ------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TEMPLATE_CONFIG="${ARACHNE_L4_CONFIG:-${REPO_ROOT}/l4/node.toml}"
ITERATIONS="${ARACHNE_L4_ITERATIONS:-5}"
READY_TIMEOUT_S="${ARACHNE_L4_READY_TIMEOUT:-30}"
KEEP_ON_FAIL="${ARACHNE_L4_KEEP_ON_FAIL:-1}"

if [ ! -f "${TEMPLATE_CONFIG}" ]; then
  echo "L4 FAIL: template config not found: ${TEMPLATE_CONFIG}" >&2
  exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "L4 FAIL: curl is required" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "L4 FAIL: python3 is required (for the config generator)" >&2
  exit 1
fi

# --- Resolve the arachne-node binary (build it if absent) -------------------
find_node_bin() {
  if [ -n "${ARACHNE_NODE:-}" ]; then
    if [ -x "${ARACHNE_NODE}" ]; then
      printf '%s' "${ARACHNE_NODE}"
    else
      echo "L4 FAIL: ARACHNE_NODE is set but not executable: ${ARACHNE_NODE}" >&2
      exit 1
    fi
    return 0
  fi
  local target_dir
  target_dir="$(cd "${REPO_ROOT}" && cargo metadata --no-deps --format-version 1 \
    | python3 -c 'import sys, json; print(json.load(sys.stdin)["target_directory"])')"
  if [ -x "${target_dir}/debug/arachne-node" ]; then
    printf '%s' "${target_dir}/debug/arachne-node"
    return 0
  fi
  echo "L4: arachne-node not built; building it (first run)..."
  ( cd "${REPO_ROOT}" && cargo build -p arachne-node )
  printf '%s' "${target_dir}/debug/arachne-node"
}

# --- Allocate a free loopback port (bind:0, read it, release) ---------------
free_port() {
  python3 - <<'PY'
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

# --- Render a per-restart config from the template (fresh data dir + port) --
generate_config() {
  local port="$1" out="$2"
  python3 - "${TEMPLATE_CONFIG}" "${out}" "${DATA_DIR}" "${port}" <<'PY'
import sys, tomllib, json
tpl_path, out_path, data_dir, port = sys.argv[1:5]
with open(tpl_path, "rb") as f:
    tpl = tomllib.load(f)
body = (
    f"cluster_id = {json.dumps(tpl['cluster_id'])}\n"
    f"node_id = {json.dumps(tpl['node_id'])}\n"
    f"listen = \"127.0.0.1:0\"\n"
    f"data_dir = {json.dumps(data_dir)}\n"
    f"http_listen = \"127.0.0.1:{port}\"\n"
    f"initial_cluster = {json.dumps(tpl['initial_cluster'])}\n"
    f"heartbeat_interval_ms = {int(tpl.get('heartbeat_interval_ms', 50))}\n"
    f"election_timeout_ms = {int(tpl.get('election_timeout_ms', 500))}\n"
)
with open(out_path, "w") as f:
    f.write(body)
PY
}

# --- Wait for /readyz to return 200 (bounded) -------------------------------
wait_ready() {
  local port="$1" timeout_s="$2"
  local deadline code
  deadline=$(( $(date +%s) + timeout_s ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${port}/readyz" 2>/dev/null || true)"
    if [ "${code}" = "200" ]; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

# --- Read one gauge from /metrics (empty if absent) -------------------------
# Tolerant of a no-match: under `set -euo pipefail` a `grep` with no match
# would abort the script before the caller's `-z` diagnostic can fire, so the
# pipeline is guarded with `|| true` (the value is simply empty).
read_metric() {
  local port="$1" name="$2"
  curl -s "http://127.0.0.1:${port}/metrics" 2>/dev/null | grep "^${name} " | awk '{print $2}' || true
}

# --- Temp run dir (holds the durable data dir + per-restart logs) -----------
RUN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/arachne-l4.XXXXXX")"
DATA_DIR="${RUN_DIR}/data"
NODE_PID=""

cleanup() {
  local ec=$?
  # Reap a still-running node (interrupt / failure paths).
  if [ -n "${NODE_PID}" ] && kill -0 "${NODE_PID}" 2>/dev/null; then
    kill -9 "${NODE_PID}" 2>/dev/null || true
  fi
  if [ "${ec}" -ne 0 ] && [ "${KEEP_ON_FAIL}" = "1" ]; then
    echo "L4: keeping run dir for inspection: ${RUN_DIR}" >&2
  else
    rm -rf "${RUN_DIR}"
  fi
}
trap cleanup EXIT

# --- Main -------------------------------------------------------------------
main() {
  local node_bin
  node_bin="$(find_node_bin)"
  echo "L4: node binary = ${node_bin}"
  echo "L4: iterations = ${ITERATIONS}, ready timeout = ${READY_TIMEOUT_S}s"
  echo "L4: durable data dir = ${DATA_DIR}"

  mkdir -p "${DATA_DIR}"

  local last_commit=-1 last_applied=-1
  local i port cfg pid commit applied

  for i in $(seq 1 "${ITERATIONS}"); do
    port="$(free_port)"
    cfg="${RUN_DIR}/node-${i}.toml"
    generate_config "${port}" "${cfg}"
    echo "L4 [$(printf '%02d' "${i}")/${ITERATIONS}]: start (http 127.0.0.1:${port})"

    "${node_bin}" --config "${cfg}" > "${RUN_DIR}/node-${i}.log" 2>&1 &
    pid=$!
    NODE_PID="${pid}"

    if ! wait_ready "${port}" "${READY_TIMEOUT_S}"; then
      echo "L4 FAIL [${i}]: node did not become ready within ${READY_TIMEOUT_S}s (fail-start? see ${RUN_DIR}/node-${i}.log)"
      {
        echo "  --- node-${i}.log (tail) ---"
        tail -n 20 "${RUN_DIR}/node-${i}.log" 2>/dev/null | sed 's/^/  /'
      }
      exit 1
    fi

    # Give the node a brief settle so it has fully (re)applied the committed log.
    sleep 0.5

    commit="$(read_metric "${port}" "arachne_commit_index")"
    applied="$(read_metric "${port}" "arachne_applied_index")"
    if [ -z "${commit}" ] || [ -z "${applied}" ]; then
      echo "L4 FAIL [${i}]: could not read commit/applied from /metrics (commit='${commit}' applied='${applied}')"
      exit 1
    fi
    echo "L4 [${i}]: ready — commit_index=${commit} applied_index=${applied}"

    # INV12/INV13: durable progress must not regress across a restart.
    if [ "${last_commit}" -ge 0 ]; then
      if [ "${commit}" -lt "${last_commit}" ] || [ "${applied}" -lt "${last_applied}" ]; then
        echo "L4 FAIL [${i}]: index regression across restart (commit ${last_commit} -> ${commit}, applied ${last_applied} -> ${applied})"
        exit 1
      fi
    fi
    last_commit="${commit}"
    last_applied="${applied}"

    # The crash under test: SIGKILL (no graceful shutdown, no fsync drain).
    kill -9 "${pid}"
    wait "${pid}" 2>/dev/null || true
    NODE_PID=""
  done

  echo ""
  echo "L4 PASS: ${ITERATIONS} kill -9 cycles survived; WAL reopened cleanly each restart;"
  echo "         commit/applied never regressed (final commit=${last_commit} applied=${last_applied})"
}

main "$@"
