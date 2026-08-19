#!/usr/bin/env bash
#
# Build the CTFS writer for wasm32-wasip1 and run it under wasmtime.
#
#   ./wasm/build-wasi.sh            # build into wasm/build, then self-check
#
# Produces three modules:
#   ctfs_wasm_demo.wasm      container layer only, links nothing but wasi-libc
#   ctfs_zstd_wasm_demo.wasm container + seekable-zstd, links a wasm libzstd
#   generate_spec_fixture.wasm  the *unmodified* tests/generate_spec_fixture.nim,
#                            i.e. the full TraceWriter, producing a .ct that
#                            `ct-print --full` reads back
#
# ---------------------------------------------------------------------------
# Why these flags
# ---------------------------------------------------------------------------
#
# --cpu:wasm32           Nim 2.2.4 already lists wasm32 in compiler/platform.nim
#                        (intSize 32, littleEndian). No CPU lie is needed. A
#                        bare `nim c --cpu:wasm32` fails only because it then
#                        hands the C to the host gcc:
#                          nimbase.h:561: static assertion failed: "Pointer size
#                          mismatch between Nim and C/C++ backend."
#
# --os:linux             wasi-libc is close enough to POSIX for the CTFS code
#                        (which uses only readFile/writeFile and no `posix`
#                        import) to build against it unchanged.
#
# -d:noSignalHandler     Nim's system module installs SIGSEGV/SIGINT handlers.
#                        wasi-libc refuses:
#                          signal.h:2:2: error: "wasm lacks signal support; to
#                          enable minimal signal emulation, compile with
#                          -D_WASI_EMULATED_SIGNAL and link with
#                          -lwasi-emulated-signal"
#                          system.nim.c:2458:9: error: call to undeclared
#                          function 'signal'
#
# -d:ctLeanRecord        uuid_v7.nim:55 gates a `getentropy()` implementation
#                        behind this define; without it, std/sysrand emits a raw
#                        Linux syscall that does not exist on wasm:
#                          sysrand.nim.c:118:10: error: call to undeclared
#                          function 'syscall'
#                          sysrand.nim.c:118:18: error: use of undeclared
#                          identifier 'SYS_getrandom'
#                        wasi-libc does provide getentropy(). The clean fix
#                        upstream is to widen that `when` to also fire on wasm.
#
# --passC:-Dmain=nimWasiMain + wasi_main_shim.c
#                        See the comment at the top of wasi_main_shim.c. Without
#                        it every module links and then traps immediately.
#
# -d:useMalloc           Nim's default allocator wants mmap; useMalloc routes it
#                        to wasi-libc's malloc.
#
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$REPO/wasm/build}"
mkdir -p "$OUT"

nixpath() { nix build --no-link --print-out-paths "$1"; }

# nixpkgs has no `wasi-sdk` attribute, but pkgsCross.wasi32 is a fully wrapped
# clang with the wasi-libc sysroot and wasm32 compiler-rt already wired up.
WASI_CC="${WASI_CC:-$(nixpath 'nixpkgs#pkgsCross.wasi32.stdenv.cc')/bin/wasm32-unknown-wasip1-clang}"
ZSTD_SRC="${ZSTD_SRC:-$(nixpath 'nixpkgs#zstd.src')}"

# --- libzstd for wasm32 ----------------------------------------------------
# libzstd needs only malloc/free/memcpy/memset, all of which wasi-libc has, so
# it cross-compiles unpatched. zstd_bindings.nim:6 hardcodes `{.passL:
# "-lzstd".}`, which resolves against this archive purely via -L.
if [[ ! -f "$OUT/zstd/libzstd.a" ]]; then
  echo "==> building libzstd for wasm32-wasip1"
  mkdir -p "$OUT/zstd"
  ( cd "$OUT/zstd"
    "$WASI_CC" -c -O2 \
      -DZSTD_MULTITHREAD=0 -DZSTD_DISABLE_ASM=1 -DZSTD_LEGACY_SUPPORT=0 \
      -I"$ZSTD_SRC/lib" -I"$ZSTD_SRC/lib/common" \
      -I"$ZSTD_SRC/lib/compress" -I"$ZSTD_SRC/lib/decompress" \
      "$ZSTD_SRC"/lib/common/*.c "$ZSTD_SRC"/lib/compress/*.c \
      "$ZSTD_SRC"/lib/decompress/*.c
    "${WASI_CC%clang}ar" rcs libzstd.a ./*.o )
fi

# --- entry-point shim ------------------------------------------------------
# Built as a plain object and injected with --passL, so no Nim source in the
# repo has to carry a {.compile.} pragma for it.
"$WASI_CC" -c -O2 -o "$OUT/wasi_main_shim.o" "$REPO/wasm/wasi_main_shim.c"

NIMFLAGS=(
  --cpu:wasm32 --os:linux
  --cc:clang
  --clang.exe:"$WASI_CC" --clang.linkerexe:"$WASI_CC"
  --mm:orc -d:useMalloc --threads:off
  -d:noSignalHandler -d:ctLeanRecord
  --passC:-Dmain=nimWasiMain
  --passC:-I"$ZSTD_SRC/lib"
  --passL:-L"$OUT/zstd"
  --passL:"$OUT/wasi_main_shim.o"
  -d:release --hints:off
  -p:"$REPO/src"
)

build() { # <nim source> <output name>
  echo "==> $2"
  nim c "${NIMFLAGS[@]}" --nimcache:"$OUT/nimcache/$2" -o:"$OUT/$2.wasm" "$1"
}

build "$REPO/wasm/ctfs_wasm_demo.nim"       ctfs_wasm_demo
build "$REPO/wasm/ctfs_zstd_wasm_demo.nim"  ctfs_zstd_wasm_demo
build "$REPO/tests/generate_spec_fixture.nim" generate_spec_fixture

echo
echo "==> running under wasmtime"
WASMTIME="${WASMTIME:-$(nixpath 'nixpkgs#wasmtime')/bin/wasmtime}"
RUN="$OUT/run"; rm -rf "$RUN"; mkdir -p "$RUN"
for m in ctfs_wasm_demo ctfs_zstd_wasm_demo generate_spec_fixture; do
  "$WASMTIME" run --dir "$RUN"::/out "$OUT/$m.wasm" "/out/$m.ct"
done
echo
ls -l "$RUN"
