#!/usr/bin/env bash
set -euo pipefail
#
# check-release-features.sh — the test-only-feature gate (D-ART-testobs, and
# propsol v0.2.9 L for `fault-injection`).
#
# Two crates carry a test-only feature. Each is the recorded exception to the
# "no test code in production" rule, changes nothing but test instrumentation,
# and must be opt-in only:
#
#   * `arachne-node` / `test-observability` — structured stdout markers
#     (`ready` / `shutdown`) for the L4 fault injector.
#   * `arachne` / `fault-injection` — a crash hook at `RaftNode::step`'s
#     ready-stage boundaries for INV2's precise crash sweep (propsol v0.2.9 L).
#
# This gate proves the **default** (release / publish) artifacts exclude both:
#
#   Gate A — feature resolution: neither feature is a default feature.
#   Gate B — artifact check: `arachne-node`'s default release binary does not
#            contain the observability marker, while a build with the feature
#            does (so the flag is not a no-op and the shipped binary is clean).
#   Gate C — artifact check: `arachne`'s default release rlib does not contain
#            the fault-injection sentinel, while a build with the feature does.
#
# Degrades gracefully: a missing/renamed package fails loudly rather than
# passing vacuously.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

# --- 1. Feature-resolution check -------------------------------------------
echo "== check-release-features: Gate A (test-only features must be opt-in) =="
metadata="$(cargo metadata --no-deps --format-version 1)"
default_features_of() {
  printf '%s' "${metadata}" | python3 -c '
import sys, json
name = sys.argv[1]
meta = json.load(sys.stdin)
for p in meta["packages"]:
    if p["name"] == name:
        print(",".join(p.get("features", {}).get("default", [])))
        break
else:
    print("<<missing>>")
' "$1"
}

node_defaults="$(default_features_of arachne-node)"
if [ "${node_defaults}" = "<<missing>>" ]; then
  echo "FAIL (Gate A): no `arachne-node` package in the workspace metadata"
  exit 1
fi
if [ -n "${node_defaults}" ]; then
  echo "FAIL (Gate A): arachne-node has non-empty default features: ${node_defaults}"
  exit 1
fi

core_defaults="$(default_features_of arachne)"
if [ "${core_defaults}" = "<<missing>>" ]; then
  echo "FAIL (Gate A): no `arachne` package in the workspace metadata"
  exit 1
fi
case ",${core_defaults}," in
  *,fault-injection,*)
    echo "FAIL (Gate A): `fault-injection` is a default feature of arachne (${core_defaults})"
    exit 1
    ;;
esac
echo "PASS (Gate A): neither test-only feature is a default feature"
echo "        arachne-node defaults: [${node_defaults}]; arachne defaults: [${core_defaults}]"

# --- 2. Artifact check ------------------------------------------------------
target_dir="$(printf '%s' "${metadata}" | python3 -c '
import sys, json
print(json.load(sys.stdin)["target_directory"])
')"
binary="${target_dir}/release/arachne-node"
# The literal format-string prefix emitted by `markers::emit`; present only when
# the feature is compiled in.
marker='{"event":"'

hash_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

echo ""
echo "== check-release-features: Gate B (release artifact must exclude the markers) =="
cargo build -p arachne-node --release >/dev/null
if [ ! -f "${binary}" ]; then
  echo "FAIL (Gate B): release binary not found at ${binary}"
  exit 1
fi
if grep -a -q -F "${marker}" "${binary}"; then
  echo "FAIL (Gate B): the default release binary contains the test-observability marker"
  exit 1
fi
default_hash="$(hash_file "${binary}")"

cargo build -p arachne-node --release --features test-observability >/dev/null
if ! grep -a -q -F "${marker}" "${binary}"; then
  echo "FAIL (Gate B): the feature build lacks the marker (the feature is a no-op?)"
  exit 1
fi
feature_hash="$(hash_file "${binary}")"

if [ "${default_hash}" = "${feature_hash}" ]; then
  echo "FAIL (Gate B): the default and feature builds hash identically"
  exit 1
fi

# Leave a clean (default-feature) artifact on disk.
cargo build -p arachne-node --release >/dev/null
echo "PASS (Gate B): default release binary excludes the markers (sha256 ${default_hash:0:12}…)"

# --- 3. Core release artifact: the fault-injection hook ---------------------
#
# Build `arachne` and print the rlib cargo reports for it. The JSON stream is
# emitted even when the unit is fresh, and it names the artifact for the feature
# set just built — mtime is unreliable because the default and feature variants
# coexist in `release/deps/`.
build_core_rlib() {
  # Cargo reports *build errors* on stdout when `--message-format=json` is on
  # (as `compiler-message`/`build-finished` records), so discarding stderr would
  # make a failed build look like an empty result. Keep stderr in a file and
  # echo it on failure: a gate that fails silently is worse than no gate.
  local err
  err="$(mktemp)"
  local out
  if ! out="$(cargo build -p arachne --release "$@" --message-format=json 2>"${err}")"; then
    echo "FAIL (Gate C): cargo build -p arachne --release $* failed" >&2
    cat "${err}" >&2
    printf '%s\n' "${out}" | grep -F '"reason":"compiler-message"' >&2 || true
    rm -f "${err}"
    return 1
  fi
  rm -f "${err}"
  printf '%s\n' "${out}" | python3 -c '
import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line.startswith("{"):
        continue
    try:
        msg = json.loads(line)
    except Exception:
        continue
    if msg.get("reason") != "compiler-artifact":
        continue
    if msg.get("target", {}).get("name") != "arachne":
        continue
    for f in msg.get("filenames", []):
        if f.endswith(".rlib"):
            print(f)
            break
    break
'
}

echo ""
echo "== check-release-features: Gate C (arachne release artifact must exclude the fault-injection hook) =="
hook_sentinel="arachne-fault-injection-hook"

# Default (release/publish) build: the hook must be absent.
rlib="$(build_core_rlib)"
if [ -z "${rlib}" ] || [ ! -f "${rlib}" ]; then
  echo "FAIL (Gate C): could not locate the arachne release rlib"
  exit 1
fi
if grep -a -q -F "${hook_sentinel}" "${rlib}"; then
  echo "FAIL (Gate C): the default release rlib contains the fault-injection hook (${rlib})"
  exit 1
fi
echo "PASS (Gate C): default release rlib excludes the fault-injection hook"

# The same feature carries the test-only slow-disk injection, so check it on the
# same default artifact rather than assuming one sentinel covers the feature.
slow_disk_sentinel="set_flush_delay_ms"
if grep -a -q -F "${slow_disk_sentinel}" "${rlib}"; then
  echo "FAIL (Gate C): the default release rlib contains the slow-disk injection (${rlib})"
  exit 1
fi
echo "PASS (Gate C): default release rlib excludes the slow-disk injection"

# Feature build: the hook must be present (proves the gate is not vacuous).
rlib="$(build_core_rlib --features fault-injection)"
if [ -z "${rlib}" ] || [ ! -f "${rlib}" ]; then
  echo "FAIL (Gate C): could not locate the arachne feature rlib"
  exit 1
fi
if ! grep -a -q -F "${hook_sentinel}" "${rlib}"; then
  echo "FAIL (Gate C): the feature build lacks the hook (the feature is a no-op?)"
  exit 1
fi
# Leave a clean (default-feature) artifact on disk.
build_core_rlib >/dev/null

echo ""
echo "check-release-features: ALL GATES PASSED"
