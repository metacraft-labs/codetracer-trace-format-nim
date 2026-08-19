#!/usr/bin/env bash
#
# Build the CTFS container writer as a freestanding `wasm32-unknown-unknown`
# module — no WASI, no emscripten runtime, zero wasm imports.  This is the
# shape a browser or a Rust `cdylib` host wants.
#
#   ./wasm/build-standalone.sh
#
# The module exports:
#   ct_init      run Nim module initialisation (required, because of --noMain)
#   ct_build     build a CTFS container in linear memory; returns 0 on success
#   ct_ptr       pointer to the finished container bytes
#   ct_len       length of the finished container bytes
#   ct_selftest  build + re-read + verify entirely inside the module; returns 0
#   memory       the linear memory, so the host can read [ct_ptr, ct_ptr+ct_len)
#
# ---------------------------------------------------------------------------
# --os:any, NOT --os:standalone
# ---------------------------------------------------------------------------
#
# `--os:standalone` is the obvious choice and it does not work in Nim 2.2.4.
# system.nim:2293 reads
#
#     when notJSnotNims and hostOS != "standalone":
#       proc getCurrentException*(): ref Exception ...
#       proc nimBorrowCurrentException(): ref Exception ...
#
# so under `--os:standalone` those two are never defined, while the C backend
# emits `#nimBorrowCurrentException()` for *every* try/except it lowers
# (compiler/ccgstmts.nim:1472). Any exception handling anywhere in the module
# graph therefore fails at semantic-check time with
#
#     Error: system module needs: nimBorrowCurrentException
#
# and neither --panics:on nor --exceptions:goto changes it. `--os:any` is the
# other freestanding OS in Nim's platform table and it is *not* excluded by
# that `when`, so it keeps exceptions while still assuming no OS.
#
# `--os:any` does still #include <string.h> etc. from nimbase.h, so the build
# needs libc *headers* — but only for declarations. It links `-nostdlib`
# against host_stub.c, and the resulting module has no imports at all.
#
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$REPO/wasm/build-standalone}"
mkdir -p "$OUT"

nixpath() { nix build --no-link --print-out-paths "$1"; }

CLANG="${CLANG:-$(nixpath 'nixpkgs#llvmPackages.clang-unwrapped')/bin/clang}"
LLD_BIN="${LLD_BIN:-$(nixpath 'nixpkgs#lld')/bin}"
# Headers only — wasi-libc's are plain musl headers and nothing from its
# libc.a is linked. compiler-rt supplies __multi3, which the 64-bit block
# arithmetic in block_mapping.nim needs and which -nostdlib otherwise leaves
# undefined.
SYSROOT_INC="$(nixpath 'nixpkgs#pkgsCross.wasi32.wasilibc.dev')/include/wasm32-wasip1"
BUILTINS="$(nixpath 'nixpkgs#pkgsCross.wasi32.llvmPackages.compiler-rt')/lib/wasip1/libclang_rt.builtins-wasm32.a"

export PATH="$LLD_BIN:$PATH"   # nim invokes clang, clang needs wasm-ld on PATH

TARGET_C="--target=wasm32-unknown-unknown -nostdlib -isystem $SYSROOT_INC"

echo "==> host_stub.o"
$CLANG $TARGET_C -O2 -c -o "$OUT/host_stub.o" "$REPO/wasm/standalone/host_stub.c"

echo "==> ctfs_standalone.wasm"
nim c \
  --os:any --cpu:wasm32 \
  --cc:clang --clang.exe:"$CLANG" --clang.linkerexe:"$CLANG" \
  --mm:arc -d:useMalloc --threads:off --noMain -d:noSignalHandler \
  -d:release --opt:size --hints:off \
  -p:"$REPO/src" -p:"$REPO/wasm/standalone" \
  --nimcache:"$OUT/nimcache" \
  --passC:"$TARGET_C" \
  --passL:"--target=wasm32-unknown-unknown -nostdlib -Wl,--no-entry -Wl,--export-memory -Wl,--export=ct_build -Wl,--export=ct_ptr -Wl,--export=ct_len -Wl,--export=ct_init -Wl,--export=ct_selftest" \
  --passL:"$OUT/host_stub.o" \
  --passL:"$BUILTINS" \
  -o:"$OUT/ctfs_standalone.wasm" \
  "$REPO/wasm/standalone/ctfs_standalone.nim"

echo
echo "==> ct_selftest (0 means the container was built and read back OK)"
WASMTIME="${WASMTIME:-$(nixpath 'nixpkgs#wasmtime')/bin/wasmtime}"
"$WASMTIME" run --invoke ct_selftest "$OUT/ctfs_standalone.wasm"
