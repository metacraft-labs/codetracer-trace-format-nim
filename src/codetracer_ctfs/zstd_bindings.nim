{.push raises: [].}

## Minimal bindings for libzstd frame-level compression/decompression.
## Links against the system libzstd library.

when defined(windows) and defined(vcc):
  {.passC: "/IC:\\zstd\\include".}
  {.passL: "C:\\zstd\\lib\\zstd.lib".}
elif defined(windows):
  const
    msys2MingwInc {.strdefine.} = ""
    msys2MingwLib {.strdefine.} = ""
  when msys2MingwInc.len > 0:
    {.passC: "-I" & msys2MingwInc.}
  else:
    {.passC: "-ID:/metacraft-dev-deps/msys2/msys64/mingw64/include".}
  # Use the exact archive instead of adding the MSYS2 library directory to
  # the global search path. The latter can replace a standalone GCC install's
  # implicit pthread/CRT archives with ABI-incompatible MSYS2 copies.
  when msys2MingwLib.len > 0:
    {.passL: msys2MingwLib & "/libzstd.a".}
  else:
    {.passL: "D:/metacraft-dev-deps/msys2/msys64/mingw64/lib/libzstd.a".}
else:
  {.passL: "-lzstd".}

proc ZSTD_compress*(dst: pointer, dstCapacity: csize_t,
                    src: pointer, srcSize: csize_t,
                    compressionLevel: cint): csize_t
  {.importc, header: "<zstd.h>".}

proc ZSTD_decompress*(dst: pointer, dstCapacity: csize_t,
                      src: pointer, compressedSize: csize_t): csize_t
  {.importc, header: "<zstd.h>".}

proc ZSTD_compressBound*(srcSize: csize_t): csize_t
  {.importc, header: "<zstd.h>".}

proc ZSTD_getFrameContentSize*(src: pointer, srcSize: csize_t): culonglong
  {.importc, header: "<zstd.h>".}

proc ZSTD_findFrameCompressedSize*(src: pointer, srcSize: csize_t): csize_t
  {.importc, header: "<zstd.h>".}
  ## Compressed size of the FIRST zstd frame in `src`, ignoring anything that
  ## follows it.  Needed when a chunked stream is read while it is still being
  ## written: the last index-referenced chunk's end offset is not derivable
  ## from the companion index (there is no following entry yet) and the data
  ## file may already carry the leading bytes of the next, not-yet-sealed
  ## chunk.  Feeding that trailing garbage to `ZSTD_decompress` is an error,
  ## so the tailing reader uses this to find the exact frame boundary instead.
  ## Returns a zstd error code (test with `ZSTD_isError`) if `src` does not
  ## begin with a complete frame header.

proc ZSTD_isError*(code: csize_t): cuint
  {.importc, header: "<zstd.h>".}

proc ZSTD_getErrorName*(code: csize_t): cstring
  {.importc, header: "<zstd.h>".}

# Context-based API for reuse across multiple compress/decompress calls
proc ZSTD_createCCtx*(): pointer
  {.importc, header: "<zstd.h>".}

proc ZSTD_freeCCtx*(cctx: pointer): csize_t
  {.importc, header: "<zstd.h>".}

proc ZSTD_compressCCtx*(cctx: pointer, dst: pointer, dstCapacity: csize_t,
                        src: pointer, srcSize: csize_t,
                        compressionLevel: cint): csize_t
  {.importc, header: "<zstd.h>".}

proc ZSTD_createDCtx*(): pointer
  {.importc, header: "<zstd.h>".}

proc ZSTD_freeDCtx*(dctx: pointer): csize_t
  {.importc, header: "<zstd.h>".}

proc ZSTD_decompressDCtx*(dctx: pointer, dst: pointer, dstCapacity: csize_t,
                          src: pointer, srcSize: csize_t): csize_t
  {.importc, header: "<zstd.h>".}

const
  ZSTD_CONTENTSIZE_UNKNOWN* = culonglong(0xFFFFFFFFFFFFFFFF'u64)
  ZSTD_CONTENTSIZE_ERROR* = culonglong(0xFFFFFFFFFFFFFFFE'u64)

# ---------------------------------------------------------------------------
# Shared decompression context
# ---------------------------------------------------------------------------

var sharedDCtx {.threadvar.}: pointer
  ## One reusable ``ZSTD_DCtx`` per thread.  ``ZSTD_decompress`` (the one-shot
  ## API) creates and destroys a context on *every* call, which on a 64 KiB
  ## frame measures as a double-digit percentage of the whole decompression on
  ## this project's reference host.  Readers that inflate a frame per seek pay
  ## that on every cache miss, so they go through ``zstdDecompressShared``
  ## instead.  The context is never freed: it is a fixed per-thread workspace
  ## for the life of the process, the same lifetime libzstd's own examples give
  ## it.  It is thread-local because a ``ZSTD_DCtx`` is not re-entrant.

proc zstdDecompressShared*(dst: pointer, dstCapacity: csize_t,
                           src: pointer, compressedSize: csize_t): csize_t =
  ## Decompress a whole Zstd frame using this thread's reusable context.
  ## Identical in effect to ``ZSTD_decompress``; falls back to it if the
  ## context cannot be created.
  if sharedDCtx.isNil:
    sharedDCtx = ZSTD_createDCtx()
  if sharedDCtx.isNil:
    return ZSTD_decompress(dst, dstCapacity, src, compressedSize)
  ZSTD_decompressDCtx(sharedDCtx, dst, dstCapacity, src, compressedSize)
