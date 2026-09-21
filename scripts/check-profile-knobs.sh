#!/usr/bin/env bash
set -euo pipefail
#
# check-profile-knobs.sh — every tuned knob in `ProfileConfig` must actually be
# read by production code, or be listed here as a known gap.
#
# Why this gate exists: `wal_trailing_keep` sat in the profile for two
# milestones without a single reader, and the same was true of `proposal_queue_bytes`
# until M2 wired it. A knob nobody reads is worse than no knob: operators tune
# it, the config validates, and nothing changes. `snapshot_threshold` was the
# same story until it got a trigger.
#
# Coverage is deliberately narrow — the production crates only:
#   * `arachne`       the library,
#   * `arachne-node`  the ops binary,
#   * `arachne-transport-tonic`  the transport.
# Test code does not count: a knob exercised only by a test is still unwired.
# `arachne-seam` has no profile, and the test-support crate must not read it.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

# Knobs that are deliberately not read yet, each with the work that will wire it.
# Keep this list shrinking: the gate fails if an entry becomes used, so a landed
# feature cannot leave a stale exemption behind.
# Empty: every knob is read by production code. The transport's snapshot rate
# was the last gap, closed by rev T T2b (`arachne-node` installs the provider and
# the profile's rate on its factory). Keep this list empty, or shrink it.
known_gaps=(
)

echo "== check-profile-knobs: every ProfileConfig field must be read by production code =="

mapfile -t fields < <(grep -oE '^    pub [a-z0-9_]+:' arachne/src/profile.rs | sed 's/^    pub //; s/:$//' | sort -u)
if [ "${#fields[@]}" -eq 0 ]; then
  echo "FAIL: could not parse any ProfileConfig fields from arachne/src/profile.rs"
  exit 1
fi

is_known_gap() {
  local name="$1" entry
  for entry in "${known_gaps[@]}"; do
    if [ "${entry%%:*}" = "${name}" ]; then
      return 0
    fi
  done
  return 1
}

fail=0
used_gap=""
for field in "${fields[@]}"; do
  # A doc comment that *names* a knob is not code that reads it (the transport
  # documents the profile knob it is given, without seeing the profile). Drop
  # comment-only lines; a real read is never on one.
  readers="$(
    grep -rEn "\b${field}\b" --include=*.rs arachne/src arachne-node/src arachne-transport-tonic/src \
      | grep -v '^arachne/src/profile.rs:' \
      | grep -vE ':[0-9]+:[[:space:]]*(//|\*|/\*)' || true
  )"
  if [ -n "${readers}" ]; then
    if is_known_gap "${field}"; then
      echo "FAIL: '${field}' is listed as a known gap but production code reads it."
      echo "      Remove its entry from known_gaps in this script."
      fail=1
    fi
  else
    if is_known_gap "${field}"; then
      echo "      known gap: ${field} (${known_gaps[*]})" >/dev/null
      used_gap="${used_gap}${used_gap:+, }${field}"
    else
      echo "FAIL: '${field}' is never read outside arachne/src/profile.rs."
      echo "      Wire it, or add it to known_gaps with the work that will."
      fail=1
    fi
  fi
done

if [ "${fail}" -ne 0 ]; then
  echo "check-profile-knobs: FAILED"
  exit 1
fi

echo "PASS: every ProfileConfig field is read (known gaps: ${used_gap:-none})"
echo "check-profile-knobs: ALL GATES PASSED"
