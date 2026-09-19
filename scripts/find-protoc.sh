#!/usr/bin/env bash
#
# find-protoc.sh — resolve and exec a `protoc` 3.x binary.
#
# Why this exists
# ===============
# `protobuf-build` (the codegen used by `raft-proto`, a dependency of the
# `arachne` core) only accepts a `protoc` whose version string is `3.x` — it
# rejects the calendar-versioned `25.x`/`31.x` that modern distributions ship.
# On a machine where the only system `protoc` is too new (as here), the whole
# workspace cannot build.
#
# This wrapper is pointed at by `PROTOC` (see `.cargo/config.toml`). Both
# `protobuf-build` (for `raft-proto`) and `tonic-build` (for
# `arachne-transport-tonic`) invoke it exactly like `protoc` (e.g.
# `protoc --version`, or the usual `-I ... -o ...` codegen arguments), so it
# simply locates a suitable `protoc` and `exec`s it with the same arguments.
#
# Precedence
# ==========
#   1. `$PROTOC_FALLBACK` — an explicit override (if set and usable);
#   2. a short list of well-known `protoc` 3.x install locations;
#   3. `protoc` on `PATH` (if it reports a 3.x version).
set -euo pipefail

candidates=(
  "${PROTOC_FALLBACK:-}"
  "/opt/anaconda3/lib/python3.12/site-packages/torch/bin/protoc"
  "/opt/homebrew/bin/protoc"
  "/usr/local/bin/protoc"
  "protoc"
)

for c in "${candidates[@]}"; do
  [ -n "$c" ] || continue
  if command -v "$c" >/dev/null 2>&1 || [ -x "$c" ]; then
    if "$c" --version 2>/dev/null | grep -qE 'libprotoc 3\.'; then
      exec "$c" "$@"
    fi
  fi
done

echo "find-protoc: no suitable protoc (3.x) found; set PROTOC_FALLBACK to one" >&2
exit 1
