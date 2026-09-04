#!/usr/bin/env bash
#
# Build the CTFS READER as a freestanding `wasm32-unknown-unknown` module —
# no WASI, no emscripten runtime, zero wasm imports — and adjudicate it.
#
#   ./wasm/build-trace-reader-standalone.sh
#
# `build-trace-writer-standalone.sh` does this for the writer. Read its header
# first: the target choice (`--os:any`, not `--os:standalone`), the cross-built
# libzstd, and the host stubs all carry over unchanged.
#
# ---------------------------------------------------------------------------
# WHAT THIS SCRIPT IS FOR
# ---------------------------------------------------------------------------
#
# A browser embedding of this library is read-heavy: it opens `.ct` containers
# constantly and emits them rarely. The writer's freestanding shape was
# measured; the reader's was assumed. This measures it, and asserts four
# separable things, because each can hold without the next:
#
#   * the reader LINKS freestanding, with ZERO wasm imports;
#   * it RUNS — `ct_selftest` writes a container and reads it back without
#     leaving linear memory, which is the round trip a browser performs;
#   * it decodes a container it did NOT write. `trace_reader_native.nim`
#     emits the same corpus from the host build; `trace_reader_host.mjs`
#     instantiates the module against a literal `{}`, copies those bytes into
#     linear memory, and compares the step count, the decoded (file, line,
#     column) of eleven steps, and every interned path / function / type /
#     varname against expectations the corpus definition computes;
#   * the reader stands ALONE. `trace_reader_only_standalone.wasm` links no
#     writer at all, so its link is not carried by a writer's copy of a
#     symbol, and its size is what a read-only embedding actually pays.
#
# The third is the one the round trip cannot establish: a module that only
# ever reads back its own bytes has proven a loop, not a reader.
#
# ---------------------------------------------------------------------------
# WHAT A HOST HAS TO SUPPLY
# ---------------------------------------------------------------------------
#
# Both stub objects are linked below because the reader+writer module needs
# them, but the READER alone does not: link
# `nimcache-trace_reader_only_standalone/*.o` with neither stub and the
# undefined set is
#
#   malloc free calloc realloc memcmp strlen exit
#
# — an allocator and three memory/string primitives (`memcpy` and `memset` come
# from compiler-rt). No clock, no entropy, no stdio, no filesystem. A browser
# embedding that only READS containers therefore has nothing to decide about a
# CSPRNG or a `Date.now()`; those belong to `trace_writer_host_stub.c` and the
# recording identity it mints, and are reached only by a module that writes.
#
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$REPO/wasm/build-trace-reader-standalone}"
mkdir -p "$OUT"

nixpath() { nix build --no-link --print-out-paths "$1"; }

CLANG="${CLANG:-$(nixpath 'nixpkgs#llvmPackages.clang-unwrapped')/bin/clang}"
LLD_BIN="${LLD_BIN:-$(nixpath 'nixpkgs#lld')/bin}"
SYSROOT_INC="$(nixpath 'nixpkgs#pkgsCross.wasi32.wasilibc.dev')/include/wasm32-wasip1"
BUILTINS="$(nixpath 'nixpkgs#pkgsCross.wasi32.llvmPackages.compiler-rt')/lib/wasip1/libclang_rt.builtins-wasm32.a"
ZSTD_SRC="${ZSTD_SRC:-$(nixpath 'nixpkgs#zstd.src')}"
WASMTIME="${WASMTIME:-$(nixpath 'nixpkgs#wasmtime')/bin/wasmtime}"
WASM_TOOLS="${WASM_TOOLS:-$(nixpath 'nixpkgs#wasm-tools')/bin/wasm-tools}"
WASM_OPT="${WASM_OPT:-$(nixpath 'nixpkgs#binaryen')/bin/wasm-opt}"
NODE="${NODE:-$(command -v node || true)}"

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

# --- the modules ------------------------------------------------------------
READ_EXPORTS="ct_init ct_input_alloc ct_input_len ct_verify_input ct_open_input
              ct_step_count ct_path_count ct_function_count ct_type_count
              ct_varname_count ct_call_count ct_column_aware
              ct_step_position ct_pos_file ct_pos_line ct_pos_column
              ct_str ct_str_ptr ct_str_len"
WRITE_EXPORTS="ct_build ct_ptr ct_len ct_selftest"

build_module() {  # <source-stem> <exports…>
  local stem="$1"; shift
  local flags=""
  for e in "$@"; do flags="$flags -Wl,--export=$e"; done
  echo "==> $stem.wasm"
  nim c \
    --os:any --cpu:wasm32 \
    --cc:clang --clang.exe:"$CLANG" --clang.linkerexe:"$CLANG" \
    --mm:arc -d:useMalloc --threads:off --noMain -d:noSignalHandler \
    -d:release --opt:size --hints:off \
    -d:ctHostClock -d:ctLeanRecord \
    -p:"$REPO/src" -p:"$REPO/wasm/standalone" \
    --nimcache:"$OUT/nimcache-$stem" \
    --passC:"$TARGET_C -I$ZSTD_SRC/lib" \
    --passL:"--target=wasm32-unknown-unknown -nostdlib -Wl,--no-entry -Wl,--export-memory $flags" \
    --passL:"$OUT/host_stub.o" \
    --passL:"$OUT/trace_writer_host_stub.o" \
    --passL:"-L$ZSTD_DIR" \
    --passL:"$BUILTINS" \
    -o:"$OUT/$stem.wasm" \
    "$REPO/wasm/standalone/$stem.nim"
}

# shellcheck disable=SC2086
build_module trace_reader_standalone $READ_EXPORTS $WRITE_EXPORTS
# shellcheck disable=SC2086
build_module trace_reader_only_standalone $READ_EXPORTS

# --- the host-written container --------------------------------------------
# Built by the HOST toolchain, so the bytes the modules read below are bytes
# they did not produce.
echo "==> corpus.ct (host build)"
NATIVE="$OUT/trace_reader_native"
nim c -d:release --hints:off -p:"$REPO/src" \
  --nimcache:"$OUT/nimcache-native" -o:"$NATIVE" \
  "$REPO/wasm/standalone/trace_reader_native.nim"
CORPUS="$OUT/corpus.ct"
EXPECTED="$OUT/corpus.json"
"$NATIVE" "$CORPUS" >"$EXPECTED"
echo "    $(wc -c <"$CORPUS") bytes"

# --- adjudication -----------------------------------------------------------
rc=0
fail() { printf 'FAIL: %s\n' "$*" >&2; rc=1; }

adjudicate() {  # <stem>
  local stem="$1"
  local module="$OUT/$stem.wasm"
  local wat="$OUT/$stem.wat"
  local bytes stripped n_imports n_exports imports

  echo
  echo "=============================================================="
  echo "==> $stem"

  # Disassemble ONCE into a file: piping `wasm-tools print` into `grep -q`
  # makes grep exit at the first match, which SIGPIPEs the producer and, under
  # `set -o pipefail`, fails the pipeline.
  "$WASM_TOOLS" print "$module" >"$wat"

  bytes="$(wc -c <"$module")"
  "$WASM_OPT" -Oz --strip-debug --strip-producers \
    -o "$OUT/$stem.min.wasm" "$module" >/dev/null
  stripped="$(wc -c <"$OUT/$stem.min.wasm")"
  echo "    size: $bytes bytes as linked, $stripped bytes after wasm-opt -Oz --strip-debug"

  imports="$(grep -oP '\(import "[^"]+" "[^"]+"' "$wat" || true)"
  n_imports="$(printf '%s' "$imports" | grep -c . || true)"
  if [ "$n_imports" = 0 ]; then
    echo "    imports: 0"
  else
    printf '%s\n' "$imports" | sed 's/^/      /'
    fail "$stem: expected 0 imports, found $n_imports"
  fi

  n_exports="$(grep -coP '^\s*\(export "' "$wat" || true)"
  echo "    exports: $n_exports"
  local e
  for e in $READ_EXPORTS memory; do
    grep -qF "(export \"$e\"" "$wat" || fail "$stem: missing export $e"
  done

  if grep -qF '(export "ct_selftest"' "$wat"; then
    local selftest
    selftest="$("$WASMTIME" run --invoke ct_selftest "$module" 2>/dev/null || true)"
    echo "    ct_selftest (write + read back inside the module): ${selftest:-<no value>}"
    [ "$selftest" = "0" ] || fail "$stem: ct_selftest returned ${selftest:-<no value>}"
  fi

  echo "    host-fed container, instantiated against a literal {}:"
  if [ -z "$NODE" ]; then
    fail "$stem: node not found; cannot run the host-fed read"
  else
    "$NODE" "$REPO/wasm/standalone/trace_reader_host.mjs" \
      "$module" "$CORPUS" "$EXPECTED" || fail "$stem: host-fed read failed"
  fi
}

adjudicate trace_reader_standalone
adjudicate trace_reader_only_standalone

echo
if [ "$rc" = 0 ]; then
  echo "OK"
else
  echo "FAILED"
fi
exit "$rc"
