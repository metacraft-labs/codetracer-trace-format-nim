#!/usr/bin/env bash
#
# Build the full TraceWriter as a freestanding `wasm32-unknown-unknown` module
# — no WASI, no emscripten runtime, zero wasm imports — and adjudicate it.
#
#   ./wasm/build-trace-writer-standalone.sh
#
# `build-standalone.sh` does this for the CONTAINER layer. This script does it
# for the layer above: `codetracer_trace_writer`, the one a recorder calls.
# Read `build-standalone.sh`'s header for why the target OS is `--os:any` and
# not `--os:standalone`; everything it says applies here unchanged.
#
# The module exports:
#   ct_init      run Nim module initialisation (required, because of --noMain)
#   ct_build     build a trace container in linear memory; returns 0 on success
#   ct_ptr       pointer to the finished container bytes
#   ct_len       length of the finished container bytes
#   ct_selftest  build + re-read + verify entirely inside the module; returns 0
#   memory       the linear memory, so the host can read [ct_ptr, ct_ptr+ct_len)
#
# ---------------------------------------------------------------------------
# WHAT THIS SCRIPT IS FOR
# ---------------------------------------------------------------------------
#
# The zero-import claim about this writer has been made against a VENDORED copy
# of this repository living in another tree. A vendored copy can be patched, and
# a claim about a patched copy is a claim about the patch, not about what this
# repository ships. This script builds the module out of `src/` and asserts the
# import count here, so the property is checkable in the repository that owns
# the code and moves when the code moves.
#
# It asserts, and exits non-zero on any failure:
#   * the module has ZERO imports (the assertion the shape exists for)
#   * `ct_selftest` returns 0 under wasmtime
#   * the exports the header lists are all present
#
# ---------------------------------------------------------------------------
# libzstd
# ---------------------------------------------------------------------------
#
# `zstd_bindings.nim` carries an unconditional `{.passL: "-lzstd".}`, so the
# writer needs a real libzstd — the container layer alone did not. It is
# cross-built here from the nixpkgs source rather than taken from the host.
#
# NOTE: no `-DZSTD_MULTITHREAD`. zstd tests that macro with `#ifdef` and not for
# a value, so passing `-DZSTD_MULTITHREAD=0` ENABLES the multithreaded path
# rather than disabling it. It does not link at all for wasm32-unknown-unknown.
#
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$REPO/wasm/build-trace-writer-standalone}"
mkdir -p "$OUT"

nixpath() { nix build --no-link --print-out-paths "$1"; }

CLANG="${CLANG:-$(nixpath 'nixpkgs#llvmPackages.clang-unwrapped')/bin/clang}"
LLD_BIN="${LLD_BIN:-$(nixpath 'nixpkgs#lld')/bin}"
SYSROOT_INC="$(nixpath 'nixpkgs#pkgsCross.wasi32.wasilibc.dev')/include/wasm32-wasip1"
BUILTINS="$(nixpath 'nixpkgs#pkgsCross.wasi32.llvmPackages.compiler-rt')/lib/wasip1/libclang_rt.builtins-wasm32.a"
ZSTD_SRC="${ZSTD_SRC:-$(nixpath 'nixpkgs#zstd.src')}"
WASMTIME="${WASMTIME:-$(nixpath 'nixpkgs#wasmtime')/bin/wasmtime}"
WASM_TOOLS="${WASM_TOOLS:-$(nixpath 'nixpkgs#wasm-tools')/bin/wasm-tools}"

export PATH="$LLD_BIN:$PATH"   # nim invokes clang, clang needs wasm-ld on PATH

TARGET_C="--target=wasm32-unknown-unknown -nostdlib -isystem $SYSROOT_INC"

# --- libzstd, cross-built ---------------------------------------------------
ZSTD_DIR="$OUT/zstd"
if [ ! -f "$ZSTD_DIR/libzstd.a" ]; then
  echo "==> libzstd.a (wasm32-unknown-unknown)"
  LLVM_AR="${LLVM_AR:-$(nixpath 'nixpkgs#llvmPackages.bintools-unwrapped')/bin/llvm-ar}"
  mkdir -p "$ZSTD_DIR"
  ( cd "$ZSTD_DIR"
    # shellcheck disable=SC2086
    $CLANG $TARGET_C -c -O2 -DZSTD_DISABLE_ASM=1 -DZSTD_LEGACY_SUPPORT=0 \
      -I"$ZSTD_SRC/lib" -I"$ZSTD_SRC/lib/common" \
      -I"$ZSTD_SRC/lib/compress" -I"$ZSTD_SRC/lib/decompress" \
      "$ZSTD_SRC"/lib/common/*.c "$ZSTD_SRC"/lib/compress/*.c \
      "$ZSTD_SRC"/lib/decompress/*.c
    "$LLVM_AR" rcs libzstd.a ./*.o
    rm -f ./*.o )
fi

# --- host stubs -------------------------------------------------------------
echo "==> host_stub.o"
# shellcheck disable=SC2086
$CLANG $TARGET_C -O2 -c -o "$OUT/host_stub.o" "$REPO/wasm/standalone/host_stub.c"
echo "==> trace_writer_host_stub.o"
# shellcheck disable=SC2086
$CLANG $TARGET_C -O2 -c -o "$OUT/trace_writer_host_stub.o" \
  "$REPO/wasm/standalone/trace_writer_host_stub.c"

# --- the module -------------------------------------------------------------
MODULE="$OUT/trace_writer_standalone.wasm"
echo "==> trace_writer_standalone.wasm"
EXPORTS="-Wl,--export=ct_build -Wl,--export=ct_ptr -Wl,--export=ct_len"
EXPORTS="$EXPORTS -Wl,--export=ct_init -Wl,--export=ct_selftest"
nim c \
  --os:any --cpu:wasm32 \
  --cc:clang --clang.exe:"$CLANG" --clang.linkerexe:"$CLANG" \
  --mm:arc -d:useMalloc --threads:off --noMain -d:noSignalHandler \
  -d:release --opt:size --hints:off \
  -d:ctHostClock -d:ctLeanRecord \
  -p:"$REPO/src" -p:"$REPO/wasm/standalone" \
  --nimcache:"$OUT/nimcache" \
  --passC:"$TARGET_C -I$ZSTD_SRC/lib" \
  --passL:"--target=wasm32-unknown-unknown -nostdlib -Wl,--no-entry -Wl,--export-memory $EXPORTS" \
  --passL:"$OUT/host_stub.o" \
  --passL:"$OUT/trace_writer_host_stub.o" \
  --passL:"-L$ZSTD_DIR" \
  --passL:"$BUILTINS" \
  -o:"$MODULE" \
  "$REPO/wasm/standalone/trace_writer_standalone.nim"

# --- adjudication -----------------------------------------------------------
rc=0
fail() { printf 'FAIL: %s\n' "$*" >&2; rc=1; }

BYTES="$(wc -c <"$MODULE")"
echo
echo "==> module: $MODULE ($BYTES bytes)"

# Disassemble ONCE into a file. Piping `wasm-tools print` straight into
# `grep -q` makes grep exit at the first match, which SIGPIPEs the producer,
# which under `set -o pipefail` fails the pipeline — so every export would be
# reported missing while being present.
WAT="$OUT/trace_writer_standalone.wat"
"$WASM_TOOLS" print "$MODULE" >"$WAT"

echo
echo "==> imports (the assertion this shape exists for)"
IMPORTS="$(grep -oP '\(import "[^"]+" "[^"]+"' "$WAT" || true)"
N_IMPORTS="$(printf '%s' "$IMPORTS" | grep -c . || true)"
if [ "$N_IMPORTS" = 0 ]; then
  echo "    0 imports"
else
  printf '%s\n' "$IMPORTS" | sed 's/^/      /'
  fail "expected 0 imports, found $N_IMPORTS"
fi

echo
echo "==> exports"
for e in ct_init ct_build ct_ptr ct_len ct_selftest memory; do
  if grep -qF "(export \"$e\"" "$WAT"; then
    echo "    $e"
  else
    fail "missing export $e"
  fi
done

echo
echo "==> ct_selftest (0 means the container was built and read back OK)"
SELFTEST="$("$WASMTIME" run --invoke ct_selftest "$MODULE" 2>/dev/null || true)"
echo "    ct_selftest = ${SELFTEST:-<no value>}"
[ "$SELFTEST" = "0" ] || fail "ct_selftest returned ${SELFTEST:-<no value>}, expected 0"

echo
if [ "$rc" = 0 ]; then
  echo "OK: $BYTES bytes, 0 imports, ct_selftest 0"
else
  echo "FAILED"
fi
exit "$rc"
