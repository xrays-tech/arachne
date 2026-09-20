#!/usr/bin/env bash
set -euo pipefail
#
# check-release-features.sh — the D-ART-testobs gate (test-plan §3.3).
#
# `arachne-node` may carry a `test-observability` feature: the single recorded
# exception to the "no test code in production" rule. It appends structured
# stdout markers (`ready` / `shutdown`) that the L4 fault injector uses for
# phase-precise kills, and changes nothing else.
#
# This gate proves that the **default** (release / publish) artifact does not
# contain it:
#
#   Gate A — feature resolution: `arachne-node` has no default features, so
#            `test-observability` is opt-in only.
#   Gate B — artifact check: the default release binary does **not** contain the
#            marker payload, while a build *with* the feature does. This proves
#            the flag is not a no-op AND that the shipped binary is the clean
#            one.
#
# Degrades gracefully: a missing/renamed package fails loudly rather than
# passing vacuously.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

# --- 1. Feature-resolution check -------------------------------------------
echo "== check-release-features: Gate A (test-observability must be opt-in) =="
metadata="$(cargo metadata --no-deps --format-version 1)"
default_features="$(printf '%s' "${metadata}" | python3 -c '
import sys, json
meta = json.load(sys.stdin)
for p in meta["packages"]:
    if p["name"] == "arachne-node":
        print(",".join(p.get("features", {}).get("default", [])))
        break
else:
    print("<<missing>>")
')"
if [ "${default_features}" = "<<missing>>" ]; then
  echo "FAIL (Gate A): no `arachne-node` package in the workspace metadata"
  exit 1
fi
if [ -n "${default_features}" ]; then
  echo "FAIL (Gate A): arachne-node has non-empty default features: ${default_features}"
  exit 1
fi
echo "PASS (Gate A): arachne-node has no default features (test-observability is opt-in)"

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

echo ""
echo "check-release-features: ALL GATES PASSED"
