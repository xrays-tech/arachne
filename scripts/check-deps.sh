#!/usr/bin/env bash
set -euo pipefail
#
# check-deps.sh — enforce the dependency boundaries of the Arachne workspace.
#
# Gates:
#   A. No production crate may pull the test/sim scaffolding
#      (arachne-testsupport / arachne-sim) through its *normal* dependency tree.
#   B. The lean core (`arachne --no-default-features`) must not pull in
#      arachne-transport-tonic.
#   C. The test/sim scaffolding must never pull in the tonic transport:
#      arachne-testsupport's *normal* tree and arachne-sim's *default* tree
#      must both be free of arachne-transport-tonic.
#
# The production-crate list is DERIVED from the workspace members (minus the
# test/sim scaffolding), so a newly added crate is picked up automatically.
#
# NOTE (Gate C / L2): arachne-sim's *deliberate* tonic enablement — exercising
# the real transport in the L2 integration harness — lands with the L2 harness
# in a later phase. It is intentionally NOT wired now; enabling it would re-add
# the tonic edge to the sim's default tree and break Gate C.
#
# Prints clear PASS/FAIL per gate and exits non-zero on any violation. Fails
# loudly (non-zero) if any `cargo` command errors.

# Resolve the repo root from this script's location so the script is CWD-safe.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

# Test/sim scaffolding crates (never "production").
NON_PROD=(arachne-testsupport arachne-sim)
# Crates that must never appear in a production normal-dependency tree.
FORBIDDEN=(arachne-testsupport arachne-sim)

overall_status=0

# --- Derive the production-crate list from the workspace members ------------
# `cargo metadata --no-deps` lists exactly the workspace members. We exclude the
# test/sim scaffolding to get the production set. Fail loudly on any error.
if ! members_json="$(cargo metadata --no-deps --format-version 1)"; then
  echo "FAIL: 'cargo metadata --no-deps' failed"
  exit 1
fi
if ! all_members="$(printf '%s' "${members_json}" | python3 -c '
import sys, json
print("\n".join(sorted(p["name"] for p in json.load(sys.stdin)["packages"])))
')"; then
  echo "FAIL: could not parse workspace members from cargo metadata"
  exit 1
fi

PROD_CRATES=()
while IFS= read -r name; do
  [ -z "${name}" ] && continue
  is_non_prod=0
  for np in "${NON_PROD[@]}"; do
    if [ "${name}" = "${np}" ]; then is_non_prod=1; break; fi
  done
  if [ "${is_non_prod}" -eq 0 ]; then
    PROD_CRATES+=("${name}")
  fi
done <<< "${all_members}"

if [ "${#PROD_CRATES[@]}" -eq 0 ]; then
  echo "FAIL: no production crates derived from workspace members (unexpected)"
  exit 1
fi
echo "check-deps: production crates: ${PROD_CRATES[*]}"

# --- Gate A ------------------------------------------------------------------
echo ""
echo "== check-deps: Gate A (production crates must not depend on test/sim scaffolding) =="
gate_a_status=0
for crate in "${PROD_CRATES[@]}"; do
  # Capture the normal-dependency tree. stderr stays on the terminal; only
  # stdout (the tree) is captured for the checks below.
  if ! tree="$(cargo tree -p "${crate}" -e normal)"; then
    echo "FAIL (Gate A): 'cargo tree -p ${crate} -e normal' failed"
    gate_a_status=1
    overall_status=1
    continue
  fi
  for forbidden in "${FORBIDDEN[@]}"; do
    # A dependency node appears as "<...> <name> <version>"; matching the name
    # surrounded by whitespace is robust to the tree-drawing characters.
    if printf '%s\n' "${tree}" | grep -q -E "[[:space:]]${forbidden}[[:space:]]"; then
      echo "FAIL (Gate A): '${crate}' normal-dep tree contains forbidden crate '${forbidden}'"
      printf '%s\n' "${tree}"
      gate_a_status=1
      overall_status=1
    fi
  done
done
if [ "${gate_a_status}" -eq 0 ]; then
  echo "PASS (Gate A): no production crate depends on test/sim scaffolding"
fi

# --- Gate B ------------------------------------------------------------------
echo ""
echo "== check-deps: Gate B (lean core must not pull in the transport) =="
if ! lean_tree="$(cargo tree -p arachne --no-default-features -e normal)"; then
  echo "FAIL (Gate B): 'cargo tree -p arachne --no-default-features -e normal' failed"
  overall_status=1
else
  if printf '%s\n' "${lean_tree}" | grep -q -E "[[:space:]]arachne-transport-tonic[[:space:]]"; then
    echo "FAIL (Gate B): 'arachne --no-default-features' normal-dep tree contains 'arachne-transport-tonic'"
    printf '%s\n' "${lean_tree}"
    overall_status=1
  else
    echo "PASS (Gate B): lean core has no transport dependency"
  fi
fi

# --- Gate C ------------------------------------------------------------------
# Test/sim scaffolding must never pull in the tonic transport (even
# transitively). We check arachne-testsupport's normal tree (it must not depend
# on `arachne` at all) and arachne-sim's effective (lean) tree (its `arachne`
# dep is default-features=false). NOTE: feature unification means a
# workspace-wide build still links sim against the tonic-enabled arachne rlib;
# the "sim compiles no tonic" property holds for `-p arachne-sim`. See the
# NOTE (Gate C / L2) above.
echo ""
echo "== check-deps: Gate C (test/sim scaffolding must not pull in the tonic transport) =="
gate_c_status=0
for crate in arachne-testsupport arachne-sim; do
  if ! tree="$(cargo tree -p "${crate}" -e normal)"; then
    echo "FAIL (Gate C): 'cargo tree -p ${crate} -e normal' failed"
    gate_c_status=1
    overall_status=1
    continue
  fi
  if printf '%s\n' "${tree}" | grep -q -E "[[:space:]]arachne-transport-tonic[[:space:]]"; then
    echo "FAIL (Gate C): '${crate}' normal-dep tree contains 'arachne-transport-tonic'"
    printf '%s\n' "${tree}"
    gate_c_status=1
    overall_status=1
  fi
done
if [ "${gate_c_status}" -eq 0 ]; then
  echo "PASS (Gate C): test/sim scaffolding does not pull in the tonic transport"
fi

echo ""
if [ "${overall_status}" -eq 0 ]; then
  echo "check-deps: ALL GATES PASSED"
  exit 0
else
  echo "check-deps: FAILED"
  exit 1
fi
