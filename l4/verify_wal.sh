#!/usr/bin/env bash
set -euo pipefail
#
# verify_wal.sh — standalone check that a node can REOPEN a given data dir and
# that the durable log is intact (D-L4 / test-plan §5.5.3, INV6).
#
# The signal is the node's **fail-start on corruption**: `WalStorage::open`
# replays the WAL and returns `Unrecoverable` for any fully-present corrupt
# record or a structural tear within the committed window. When that happens the
# node exits non-zero before `/readyz` — that is exactly the failure this
# checker detects. A healthy data dir reopens cleanly and becomes ready.
#
# Usage:
#   bash l4/verify_wal.sh <data_dir> [node_binary]
#
# Exit codes:
#   0 — the node reopened the data dir and became ready (WAL intact)
#   1 — the node fail-started (corrupt/unrecoverable WAL) or never became ready
#   2 — usage / environment error
#
# This is the offline half of the L4 durability story (test-plan §3.3): the
# run-time `kill -9` loop (kill_loop.sh) is the primary evidence; this checker
# is a reusable probe for a data dir (e.g. after an offline mutation S04/S05/S06).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <data_dir> [node_binary]" >&2
  exit 2
fi

DATA_DIR="$1"
NODE_BIN="${2:-${ARACHNE_NODE:-}}"
READY_TIMEOUT_S="${ARACHNE_L4_READY_TIMEOUT:-30}"

if [ ! -d "${DATA_DIR}" ]; then
  echo "verify_wal FAIL: data dir does not exist: ${DATA_DIR}" >&2
  exit 2
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "verify_wal FAIL: curl is required" >&2
  exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "verify_wal FAIL: python3 is required" >&2
  exit 2
fi

# --- Resolve the node binary -------------------------------------------------
if [ -z "${NODE_BIN}" ]; then
  TARGET_DIR="$(cd "${REPO_ROOT}" && cargo metadata --no-deps --format-version 1 \
    | python3 -c 'import sys, json; print(json.load(sys.stdin)["target_directory"])')"
  if [ -x "${TARGET_DIR}/debug/arachne-node" ]; then
    NODE_BIN="${TARGET_DIR}/debug/arachne-node"
  else
    echo "verify_wal: building arachne-node..."
    ( cd "${REPO_ROOT}" && cargo build -p arachne-node )
    NODE_BIN="${TARGET_DIR}/debug/arachne-node"
  fi
fi
if [ ! -x "${NODE_BIN}" ]; then
  echo "verify_wal FAIL: node binary not executable: ${NODE_BIN}" >&2
  exit 2
fi

# --- A fresh loopback port + a throwaway config pointing at DATA_DIR ---------
PORT="$(python3 - <<'PY'
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"

# Read the stable identity (cluster_id / node_id) from the existing META so the
# reopen is validated against the dir's real cluster/node, then render a config.
# The META is a binary blob (test-plan §5.5.1 / arachne::storage::meta):
#   [u32 magic][u32 fmt][u32 crc][u16 clen][cluster][u16 nlen][node][u64 created]
CONFIG="$(mktemp "${TMPDIR:-/tmp}/arachne-l4-verify.XXXXXX")"
python3 - "${DATA_DIR}" "${CONFIG}" "${PORT}" <<'PY'
import sys, json, os, struct
data_dir, config_path, port = sys.argv[1:4]
meta_path = os.path.join(data_dir, "META")
if not os.path.exists(meta_path):
    sys.stderr.write("verify_wal: no META in %s (not a node data dir?)\n" % data_dir)
    sys.exit(3)
with open(meta_path, "rb") as f:
    buf = f.read()
if len(buf) < 12:
    sys.stderr.write("verify_wal: META too short in %s\n" % data_dir)
    sys.exit(3)
off = 12
(clen,) = struct.unpack_from("<H", buf, off); off += 2
cluster_id = buf[off:off + clen].decode("utf-8"); off += clen
(nlen,) = struct.unpack_from("<H", buf, off); off += 2
node_id = buf[off:off + nlen].decode("utf-8")
body = (
    f"cluster_id = {json.dumps(cluster_id)}\n"
    f"node_id = {json.dumps(node_id)}\n"
    f"listen = \"127.0.0.1:0\"\n"
    f"data_dir = {json.dumps(data_dir)}\n"
    f"http_listen = \"127.0.0.1:{port}\"\n"
    f"initial_cluster = {json.dumps([node_id])}\n"
    f"heartbeat_interval_ms = 50\n"
    f"election_timeout_ms = 500\n"
    f"rpc_timeout_ms = 200\n"
)
with open(config_path, "w") as f:
    f.write(body)
PY

LOG="$(mktemp "${TMPDIR:-/tmp}/arachne-l4-verify-log.XXXXXX")"
cleanup() {
  local ec=$?
  if [ -n "${PID:-}" ] && kill -0 "${PID}" 2>/dev/null; then
    kill -9 "${PID}" 2>/dev/null || true
  fi
  rm -f "${CONFIG}" "${LOG}"
  return "${ec}"
}
trap cleanup EXIT

# --- Start the node on the existing data dir and wait for /readyz -----------
echo "verify_wal: opening ${DATA_DIR} on ${NODE_BIN} (http 127.0.0.1:${PORT})"
"${NODE_BIN}" --config "${CONFIG}" > "${LOG}" 2>&1 &
PID=$!

deadline=$(( $(date +%s) + READY_TIMEOUT_S ))
ready=0
while [ "$(date +%s)" -lt "${deadline}" ]; do
  if [ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${PORT}/readyz" 2>/dev/null || true)" = "200" ]; then
    ready=1
    break
  fi
  # If the node already exited (fail-start), stop polling early.
  if ! kill -0 "${PID}" 2>/dev/null; then
    break
  fi
  sleep 0.1
done

# Reap the node (whether it is still up or already exited).
kill -9 "${PID}" 2>/dev/null || true
wait "${PID}" 2>/dev/null || true
PID=""

if [ "${ready}" -eq 1 ]; then
  echo "verify_wal PASS: data dir reopened cleanly and the node became ready (WAL intact)"
  exit 0
fi

echo "verify_wal FAIL: node did not become ready (fail-start on corrupt WAL?)"
echo "  --- node log (tail) ---"
tail -n 20 "${LOG}" 2>/dev/null | sed 's/^/  /'
exit 1
