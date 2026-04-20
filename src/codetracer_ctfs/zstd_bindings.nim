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

proc ZSTD_isError*(code: csize_t): cuint
  {.importc, header: "<zstd.h>".}

proc ZSTD_getErrorName*(code: csize_t): cstring
  {.importc, header: "<zstd.h>".}

const
  ZSTD_CONTENTSIZE_UNKNOWN* = culonglong(0xFFFFFFFFFFFFFFFF'u64)
  ZSTD_CONTENTSIZE_ERROR* = culonglong(0xFFFFFFFFFFFFFFFE'u64)
