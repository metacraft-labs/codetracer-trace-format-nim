#!/usr/bin/env bash
#
# Build the full CTFS TraceWriter through emscripten and run it under node.
#
#   ./wasm/build-emscripten.sh
#
# Builds tests/generate_spec_fixture.nim *unmodified* — the whole TraceWriter:
# paths, functions, steps, calls, values, returns, a seekable-zstd-compressed
# events.log, meta.dat and a UUIDv7 recording id — and writes a real .ct that
# `ct-print --full` reads back identically to the native build.
#
# Emscripten needs no entry-point shim: `-d:emscripten` makes Nim emit the
# two-parameter main that emcc expects, so the __main_argc_argv mismatch that
# the raw wasi-libc path hits (see wasm/wasi_main_shim.c) does not arise.
# It still needs -d:noSignalHandler and -d:ctLeanRecord for the same two
# reasons the WASI build does.
#
# -sNODERAWFS=1 lets the module write to the real filesystem under node
# instead of emscripten's in-memory MEMFS, so the .ct actually lands on disk.
#
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$REPO/wasm/build-emscripten}"
mkdir -p "$OUT"

nixpath() { nix build --no-link --print-out-paths "$1"; }

EMBIN="${EMBIN:-$(nixpath 'nixpkgs#emscripten')/bin}"
ZSTD_SRC="${ZSTD_SRC:-$(nixpath 'nixpkgs#zstd.src')}"
NODE="${NODE:-$(nixpath 'nixpkgs#nodejs')/bin/node}"

export EM_CACHE="${EM_CACHE:-$OUT/emcache}"
export PATH="$EMBIN:$PATH"
mkdir -p "$EM_CACHE"

if [[ ! -f "$OUT/zstd/libzstd.a" ]]; then
  echo "==> building libzstd with emcc"
  mkdir -p "$OUT/zstd"
  ( cd "$OUT/zstd"
    emcc -c -O2 -DZSTD_MULTITHREAD=0 -DZSTD_DISABLE_ASM=1 -DZSTD_LEGACY_SUPPORT=0 \
      -I"$ZSTD_SRC/lib" -I"$ZSTD_SRC/lib/common" \
      -I"$ZSTD_SRC/lib/compress" -I"$ZSTD_SRC/lib/decompress" \
      "$ZSTD_SRC"/lib/common/*.c "$ZSTD_SRC"/lib/compress/*.c \
      "$ZSTD_SRC"/lib/decompress/*.c
    emar rcs libzstd.a ./*.o )
fi

echo "==> generate_spec_fixture.js/.wasm"
nim c \
  -d:emscripten --cpu:wasm32 --os:linux \
  --cc:clang --clang.exe:emcc --clang.linkerexe:emcc \
  --mm:orc -d:useMalloc --threads:off \
  -d:noSignalHandler -d:ctLeanRecord \
  -d:release --hints:off \
  -p:"$REPO/src" \
  --passC:-I"$ZSTD_SRC/lib" \
  --passL:-L"$OUT/zstd" --passL:-lzstd \
  --passL:-sNODERAWFS=1 --passL:-sALLOW_MEMORY_GROWTH=1 \
  --nimcache:"$OUT/nimcache" \
  -o:"$OUT/generate_spec_fixture.js" \
  "$REPO/tests/generate_spec_fixture.nim"

echo
echo "==> running under node"
"$NODE" "$OUT/generate_spec_fixture.js" "$OUT/spec_fixture.ct"
ls -l "$OUT/spec_fixture.ct"
