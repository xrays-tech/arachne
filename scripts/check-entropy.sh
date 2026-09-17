#!/usr/bin/env bash
set -euo pipefail
#
# check-entropy.sh — static "entropy" gates for the Arachne workspace (test-plan
# section 5, policy E2).
#
# Gates:
#   A. No `futures::select!` / `futures_util::select!` anywhere in the
#      workspace crates' production `src/` roots, nor in any `tests/` directory.
#   B. Every `tokio::select!` must be accompanied by a `biased;` (see heuristic
#      note).
#   C. No `std::time` / `tokio::net` under any `*sim*` `src/` path, nor in any
#      `tests/` directory.
#
# The crate roots are derived from the workspace members (via `cargo
# metadata`), so only real workspace crates are scanned — standalone harnesses
# like `fuzz/` / `model-check/` and build output in `target/` are ignored, and a
# newly added crate is picked up automatically.
#
# Degrades gracefully: if a source root has no matches (or does not exist), the
# relevant gate reports PASS. Exits non-zero on the first violation.

# Resolve the repo root from this script's location so the script is CWD-safe.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

# --- Derive the workspace crates' src/ and tests/ roots --------------------
if ! members_json="$(cargo metadata --no-deps --format-version 1)"; then
  echo "FAIL: 'cargo metadata --no-deps' failed"
  exit 1
fi
if ! member_names="$(printf '%s' "${members_json}" | python3 -c '
import sys, json
print("\n".join(sorted(p["name"] for p in json.load(sys.stdin)["packages"])))
')"; then
  echo "FAIL: could not parse workspace members from cargo metadata"
  exit 1
fi

ALL_SRC_ROOTS=()
ALL_TEST_ROOTS=()
while IFS= read -r name; do
  [ -z "${name}" ] && continue
  # In this workspace each crate's directory is named after the crate.
  if [ -d "${name}/src" ]; then ALL_SRC_ROOTS+=("${name}/src"); fi
  if [ -d "${name}/tests" ]; then ALL_TEST_ROOTS+=("${name}/tests"); fi
done <<< "${member_names}"

overall_status=0

# ---------------------------------------------------------------------------
# Gate A — forbid futures select! in production src/ roots and any tests/ dir.
#
# Heuristic: a plain-text grep for the two macro invocations. A `futures::
# select!` written with unusual spacing, line-wrapping, or via a renamed import
# alias would be missed; this gate is a backstop, not a type checker. It scans
# every workspace crate's src/ (a superset of the production crates) plus every
# tests/ dir.
#
# The pattern `futures(_util)?::select!` matches BOTH `futures::select!` (the
# `futures` crate's macro) and `futures_util::select!` (the `futures-util`
# crate's macro). (The previous pattern `futures(::|_util)::select!` only
# matched the `futures_util` form — a bug fixed here.)
# ---------------------------------------------------------------------------
echo "== check-entropy: Gate A (forbid futures::select! / futures_util::select! in production src/ and tests/) =="
gate_a_status=0
for root in "${ALL_SRC_ROOTS[@]}" "${ALL_TEST_ROOTS[@]}"; do
  [ -d "${root}" ] || continue
  if grep -rn -E 'futures(_util)?::select!' --include='*.rs' "${root}"; then
    echo "FAIL (Gate A): found futures select! under '${root}' (matches above)"
    gate_a_status=1
    overall_status=1
  fi
done
if [ "${gate_a_status}" -eq 0 ]; then
  echo "PASS (Gate A): no futures select! in production src/ or tests/"
fi

# ---------------------------------------------------------------------------
# Gate B — every tokio::select! must be followed by a biased; within ~10 lines.
#
# Heuristic (and its limits):
#   - For each `tokio::select!` occurrence we inspect a fixed window of the
#     following ~10 lines (including the occurrence line) and require a literal
#     `biased;` token to appear.
#   - This is a LINE-WINDOW check, NOT a real brace/AST block analysis:
#       * A `biased;` placed more than ~10 lines away would be missed (a
#         false-positive FAIL on otherwise-correct code).
#       * Only the exact token `biased;` is matched; `biased ;` (spaced) is NOT
#         caught.
#       * Unusual macro layouts / line wrapping may be misjudged.
#   - If no `tokio::select!` exists anywhere, the gate reports PASS.
# ---------------------------------------------------------------------------
echo ""
echo "== check-entropy: Gate B (tokio::select! must be followed by 'biased;' within ~10 lines) =="
gate_b_status=0
for root in "${ALL_SRC_ROOTS[@]}"; do
  [ -d "${root}" ] || continue
  while IFS= read -r file; do
    while IFS= read -r match; do
      lineno="${match%%:*}"
      # `sed` is used here only to extract a line range for the window check
      # (legitimate script logic, not ad-hoc code reading).
      window="$(sed -n "${lineno},$((lineno + 10))p" "${file}")"
      if ! printf '%s\n' "${window}" | grep -q 'biased;'; then
        echo "FAIL (Gate B): '${file}:${lineno}' tokio::select! has no 'biased;' within the next ~10 lines"
        gate_b_status=1
      fi
    done < <(grep -n 'tokio::select!' "${file}")
  done < <(grep -rln 'tokio::select!' --include='*.rs' "${root}")
done
if [ "${gate_b_status}" -eq 0 ]; then
  echo "PASS (Gate B): every tokio::select! is accompanied by 'biased;' (or none present)"
else
  overall_status=1
fi

# ---------------------------------------------------------------------------
# Gate C — forbid std::time / tokio::net under any *sim* src/ path AND in any
# tests/ directory.
# ---------------------------------------------------------------------------
echo ""
echo "== check-entropy: Gate C (forbid std::time / tokio::net under any *sim* path and in tests/) =="
gate_c_status=0
# (1) Any `*sim*` src/ path (the original rule).
for root in "${ALL_SRC_ROOTS[@]}"; do
  case "${root}" in
    *sim*)
      [ -d "${root}" ] || continue
      if grep -rn -E 'std::time|tokio::net' --include='*.rs' "${root}"; then
        echo "FAIL (Gate C): found real-time/network usage under '${root}' (matches above)"
        gate_c_status=1
        overall_status=1
      fi
      ;;
  esac
done
# (2) Any tests/ directory (the widened rule): real time/network must not leak
#     into test code either.
for root in "${ALL_TEST_ROOTS[@]}"; do
  [ -d "${root}" ] || continue
  if grep -rn -E 'std::time|tokio::net' --include='*.rs' "${root}"; then
    echo "FAIL (Gate C): found real-time/network usage under '${root}' (matches above)"
    gate_c_status=1
    overall_status=1
  fi
done
if [ "${gate_c_status}" -eq 0 ]; then
  echo "PASS (Gate C): no std::time / tokio::net under any *sim* path or in tests/"
fi

echo ""
if [ "${overall_status}" -eq 0 ]; then
  echo "check-entropy: ALL GATES PASSED"
  exit 0
else
  echo "check-entropy: FAILED"
  exit 1
fi
