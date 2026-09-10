#!/usr/bin/env bash
# run.sh — build the native host, embed Lua, produce the nested Lua .ct, and
# verify it (GDScript-Recorder milestone N3: Generalization, NO fork).
#
# Steps:
#   1. build host.c (embeds Lua via nixpkgs lua5_4; links this repo's own
#      libcodetracer_trace_writer.a, built by `just build-static-lib`).
#   2. run it: Lua's OWN per-line hook drives a materialized Lua .ct, joined to
#      the host's native coordinate index at the host<->VM boundary.
#   3. decode via ct-print --full and assert with scripts/verify_n3.py:
#      per-line steps, call/return frames, captured values, AND host<->VM
#      ct-nested-join events with resolvable (GEID, tick) keys — plus tamper
#      runs (non-vacuity). EXITS NONZERO on any mismatch.
#
# Tooling comes from ./flake.nix (lua5_4 + clang + zstd + pkg-config + python3),
# NOT a bare `nix shell`. If not already inside that dev shell, re-exec through it.
set -euo pipefail
cd "$(dirname "$0")"

if [[ "${N3_IN_SHELL:-}" != "1" ]]; then
  exec nix develop path:. --command bash run.sh "$@"
fi

OUT="$PWD/out"
rm -rf "$OUT"
mkdir -p "$OUT"

# ct-print: prefer $CT_PRINT, then the repo-root prebuilt binary, then PATH.
CT_PRINT="${CT_PRINT:-}"
if [[ -z "$CT_PRINT" ]]; then
  if [[ -x "../../ct-print" ]]; then
    CT_PRINT="$(cd ../.. && pwd)/ct-print"
  elif command -v ct-print >/dev/null 2>&1; then
    CT_PRINT="$(command -v ct-print)"
  else
    echo "run.sh: ct-print not found (set CT_PRINT, or build codetracer-trace-format-nim#default)" >&2
    exit 2
  fi
fi
echo "run.sh: using ct-print at $CT_PRINT"

# The writer this example links is the one THIS repo builds, not a copy of it.
#
# A prebuilt `vendor/libcodetracer_trace_writer.a` used to be committed here.
# An example that lives inside the repo defining the writer cannot vendor that
# writer without the copy going stale, and this one did: it kept producing
# `meta.dat` v3 containers after the global-line-index correction moved the
# format to v4, and nothing reported it, because no CI lane and no `just`
# target reaches this directory. The repo's own `.gitignore` excludes `*.a` and
# `*.so`; the vendored copy was the single tracked exception to that rule.
#
# So the library is resolved, never carried. `$CT_TRACE_WRITER_LIB` wins if
# set; otherwise it is the repo-root artefact, and a missing one is an error
# naming the command that produces it rather than a fallback to something
# older. The header comes from `../../include` for the same reason.
LIB="${CT_TRACE_WRITER_LIB:-}"
if [[ -z "$LIB" ]]; then
  LIB="$(cd ../.. && pwd)/libcodetracer_trace_writer.a"
fi
if [[ ! -f "$LIB" ]]; then
  echo "run.sh: no CTFS writer static library at $LIB" >&2
  echo "        Build it from the repo root with:" >&2
  echo "            just build-static-lib      # or: nimble buildStaticLib" >&2
  echo "        (or point CT_TRACE_WRITER_LIB at one you already have)." >&2
  exit 2
fi
echo "run.sh: linking CTFS writer at $LIB"

# ---- 1. build the native host -------------------------------------------------
# macOS: the CTFS writer's Nim/arc runtime needs -framework Security
# -framework CoreFoundation; Linux would use -lm -lpthread instead.
FRAMEWORKS=()
if [[ "$(uname -s)" == "Darwin" ]]; then
  FRAMEWORKS=(-framework Security -framework CoreFoundation)
else
  FRAMEWORKS=(-lm -lpthread)
fi

echo "run.sh: building host..."
clang -O1 -g -std=c11 \
  $(pkg-config --cflags lua5.4) -I../../include \
  host.c "$LIB" \
  $(pkg-config --libs lua5.4) \
  $(pkg-config --libs libzstd) \
  "${FRAMEWORKS[@]}" \
  -o host

# ---- 2. run: produce the nested Lua .ct + the host native index --------------
echo "run.sh: recording..."
CT_LUA_TRACE="$OUT" ./host script.lua | tee "$OUT/host_stdout.txt"

test -f "$OUT/lua_trace.ct" || { echo "run.sh: no lua_trace.ct produced" >&2; exit 3; }
test -f "$OUT/host_native_index.json" || { echo "run.sh: no host_native_index.json" >&2; exit 3; }

# ---- 3. decode + verify ------------------------------------------------------
echo "run.sh: decoding via ct-print --full..."
"$CT_PRINT" --full "$OUT/lua_trace.ct" > "$OUT/full.json"

echo "run.sh: verifying..."
python3 scripts/verify_n3.py verify "$OUT/full.json" "$OUT/host_native_index.json"

# Non-vacuity: each tamper must be CAUGHT (verify_n3.py tamper exits 0 iff caught).
for mode in geid step value; do
  python3 scripts/verify_n3.py tamper "$OUT/full.json" "$OUT/host_native_index.json" "$mode"
done

echo "run.sh: N3 PASSED — embedded-Lua nested materialized trace + host<->VM join verified (NO fork)."
