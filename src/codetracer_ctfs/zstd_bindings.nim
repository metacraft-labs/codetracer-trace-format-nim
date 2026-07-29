{.push raises: [].}

## Minimal bindings for libzstd frame-level compression/decompression.
## Links against the system libzstd library.

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
