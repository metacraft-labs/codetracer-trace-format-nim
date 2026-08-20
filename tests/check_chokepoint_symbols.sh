#!/usr/bin/env bash
# MT1: assert the three replay-observation "chokepoint" symbols are EXPORTED in
# the C-FFI static library, so a replay-time observer (MCR) can interpose on
# them (e.g. set a breakpoint on the symbol) to derive native<->VM correlation
# for mixed traces. See src/codetracer_trace_writer_ffi.nim "Replay observation
# seam" and codetracer-specs/Planned-Features/Mixed-Trace-Debugging.md §4.
#
# The `gcc` link of tests/test_ffi.c already fails if a symbol is missing, but
# this is an explicit, self-describing guard so a future refactor that inlines
# or hides one of the chokepoints fails loudly here rather than silently
# breaking the replay-time observer.
set -euo pipefail

lib="${1:-libcodetracer_trace_writer.a}"
[ -f "$lib" ] || { echo "chokepoint-symbols: '$lib' not found"; exit 1; }

# nm marks exported text symbols 'T' (weak: 'W'); macOS prefixes an underscore.
for s in trace_writer_register_step trace_writer_register_call trace_writer_register_return; do
  if ! nm "$lib" 2>/dev/null | grep -Eq "[TW] _?${s}$"; then
    echo "chokepoint-symbols: '$s' is not an exported symbol in $lib"
    exit 1
  fi
done

echo "[OK] chokepoint symbols exported: register_step / register_call / register_return"
