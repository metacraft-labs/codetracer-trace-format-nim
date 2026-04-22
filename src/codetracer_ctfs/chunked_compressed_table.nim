{.push raises: [].}

## ChunkedCompressedTable: groups fixed-size records into chunks, compresses
## each chunk independently with Zstd, and writes a companion index.
##
## The `.dat` file contains only compressed chunk data (no inline headers).
## The `.idx` file has:
##   [chunk_size: u32 LE]   # records per chunk
##   [offset_0: u64 LE]     # byte offset of chunk 0 in .dat
##   [offset_1: u64 LE]     # byte offset of chunk 1 in .dat
##   ...
##
## To seek to record N:
##   1. chunk = N / chunk_size
##   2. Read idx[4 + chunk * 8] for byte offset
##   3. Compressed chunk size = next offset - this offset (or data end for last)
##   4. Decompress
##   5. Extract record N % chunk_size from decompressed buffer

import results
import ./types
import ./container
import ./zstd_bindings

const DefaultCompressionLevel* = 3

type
  ChunkedCompressedTableWriter* = object
    dataFile: CtfsInternalFile       ## foo.dat
    indexFile: CtfsInternalFile      ## foo.idx
    chunkSize: uint32                ## records per chunk
    recordSize: int                  ## bytes per record
    buffer: seq[byte]                ## accumulates records until chunk is full
    recordCount: int                 ## records in current buffer
    totalRecords: uint64             ## total records written
    dataOffset: uint64               ## bytes written to .dat so far
    compressionLevel: int

  ChunkedCompressedTableReader* = object
    data: seq[byte]          ## raw foo.dat content
    chunkSize: uint32        ## from index header
    recordSize: int
    offsets: seq[uint64]     ## chunk offsets parsed from foo.idx
    totalRecords: uint64     ## computed from chunks
    # Cache for last decompressed chunk
    cachedChunkIdx: int      ## -1 means no cache
    cachedChunk: seq[byte]

proc initChunkedCompressedTableWriter*(
    ctfs: var Ctfs, baseName: string,
    recordSize: int, chunkSize: uint32 = DefaultChunkSize,
    compressionLevel: int = DefaultCompressionLevel
): Result[ChunkedCompressedTableWriter, string] =
  ## Create a new chunked compressed table in the CTFS container.
  ## Creates baseName.dat and baseName.idx files.
  if recordSize <= 0:
    return err("recordSize must be positive")
  if chunkSize == 0:
    return err("chunkSize must be > 0")

  let datRes = ctfs.addFile(baseName & ".dat")
  if datRes.isErr:
    return err("failed to create .dat file: " & datRes.error)

  let idxRes = ctfs.addFile(baseName & ".idx")
  if idxRes.isErr:
    return err("failed to create .idx file: " & idxRes.error)

  var writer = ChunkedCompressedTableWriter(
    dataFile: datRes.get(),
    indexFile: idxRes.get(),
    chunkSize: chunkSize,
    recordSize: recordSize,
    buffer: newSeq[byte](int(chunkSize) * recordSize),
    recordCount: 0,
    totalRecords: 0,
    dataOffset: 0,
    compressionLevel: compressionLevel,
  )

  # Write chunk_size header to index file
  var hdr: array[4, byte]
  let le = toBytesLE(chunkSize)
  for i in 0 ..< 4:
    hdr[i] = le[i]
  let hdrRes = ctfs.writeToFile(writer.indexFile, hdr)
  if hdrRes.isErr:
    return err("failed to write idx header: " & hdrRes.error)

  ok(writer)

proc flushChunk(ctfs: var Ctfs, w: var ChunkedCompressedTableWriter): Result[void, string] =
  ## Compress and write buffered records as one chunk.
  if w.recordCount == 0:
    return ok()

  let srcSize = w.recordCount * w.recordSize
  let bound = ZSTD_compressBound(csize_t(srcSize))
  var compressed = newSeq[byte](int(bound))

  let compressedSize = ZSTD_compress(
    addr compressed[0], csize_t(bound),
    addr w.buffer[0], csize_t(srcSize),
    cint(w.compressionLevel))

  if ZSTD_isError(compressedSize) != 0:
    return err("zstd compress failed: " & $ZSTD_getErrorName(compressedSize))

  # Write offset to index BEFORE writing data
  var offBytes: array[8, byte]
  let offLE = toBytesLE(w.dataOffset)
  for i in 0 ..< 8:
    offBytes[i] = offLE[i]
  let idxRes = ctfs.writeToFile(w.indexFile, offBytes)
  if idxRes.isErr:
    return err("failed to write offset to idx: " & idxRes.error)

  # Write compressed data to .dat
  let datRes = ctfs.writeToFile(w.dataFile, compressed.toOpenArray(0, int(compressedSize) - 1))
  if datRes.isErr:
    return err("failed to write compressed chunk: " & datRes.error)

  w.dataOffset += uint64(compressedSize)
  w.recordCount = 0
  ok()

proc append*(ctfs: var Ctfs, w: var ChunkedCompressedTableWriter,
    record: openArray[byte]): Result[void, string] =
  ## Append a fixed-size record. When the buffer fills a chunk, it is compressed and flushed.
  if record.len != w.recordSize:
    return err("record size mismatch: expected " & $w.recordSize & ", got " & $record.len)

  let offset = w.recordCount * w.recordSize
  for i in 0 ..< w.recordSize:
    w.buffer[offset + i] = record[i]
  w.recordCount += 1
  w.totalRecords += 1

  if w.recordCount == int(w.chunkSize):
    let r = ctfs.flushChunk(w)
    if r.isErr:
      return err(r.error)

  ok()

proc flush*(ctfs: var Ctfs, w: var ChunkedCompressedTableWriter): Result[void, string] =
  ## Flush any remaining buffered records as a partial final chunk.
  ## Must be called before serializing the CTFS.
  ctfs.flushChunk(w)

proc count*(w: ChunkedCompressedTableWriter): uint64 = w.totalRecords

# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

proc initChunkedCompressedTableReader*(
    ctfsBytes: openArray[byte], baseName: string,
    recordSize: int,
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries
): Result[ChunkedCompressedTableReader, string] =
  ## Read a chunked compressed table from CTFS bytes.
  if recordSize <= 0:
    return err("recordSize must be positive")

  let datRes = readInternalFile(ctfsBytes, baseName & ".dat", blockSize, maxEntries)
  if datRes.isErr:
    return err("failed to read .dat: " & datRes.error)
  let datData = datRes.get()

  let idxRes = readInternalFile(ctfsBytes, baseName & ".idx", blockSize, maxEntries)
  if idxRes.isErr:
    return err("failed to read .idx: " & idxRes.error)
  let idxData = idxRes.get()

  if idxData.len < 4:
    return err("index file too small for header")

  # Parse chunk_size from header
  var cs4: array[4, byte]
  for i in 0 ..< 4:
    cs4[i] = idxData[i]
  let chunkSize = fromBytesLE(uint32, cs4)

  if chunkSize == 0:
    return err("chunkSize in index is 0")

  # Parse offsets
  let offsetBytes = idxData.len - 4
  if offsetBytes mod 8 != 0:
    return err("index file has trailing bytes after offsets")
  let numChunks = offsetBytes div 8

  var offsets = newSeq[uint64](numChunks)
  for i in 0 ..< numChunks:
    var o8: array[8, byte]
    for j in 0 ..< 8:
      o8[j] = idxData[4 + i * 8 + j]
    offsets[i] = fromBytesLE(uint64, o8)

  # Compute totalRecords: for full chunks + last chunk partial
  var totalRecords: uint64 = 0
  if numChunks > 0:
    # All chunks except the last are full
    totalRecords = uint64(numChunks - 1) * uint64(chunkSize)

    # For the last chunk, determine decompressed size
    let lastOff = offsets[numChunks - 1]
    let compressedLen = uint64(datData.len) - lastOff
    if compressedLen == 0:
      return err("last chunk has zero compressed size")

    let frameSize = ZSTD_getFrameContentSize(
      unsafeAddr datData[int(lastOff)], csize_t(compressedLen))
    if frameSize == ZSTD_CONTENTSIZE_UNKNOWN or frameSize == ZSTD_CONTENTSIZE_ERROR:
      return err("cannot determine last chunk decompressed size")

    let lastChunkRecords = uint64(frameSize) div uint64(recordSize)
    totalRecords += lastChunkRecords

  ok(ChunkedCompressedTableReader(
    data: datData,
    chunkSize: chunkSize,
    recordSize: recordSize,
    offsets: offsets,
    totalRecords: totalRecords,
    cachedChunkIdx: -1,
    cachedChunk: @[],
  ))

proc count*(r: ChunkedCompressedTableReader): uint64 = r.totalRecords

proc decompressChunk(r: var ChunkedCompressedTableReader,
    chunkIdx: int): Result[void, string] =
  ## Decompress chunk at chunkIdx into the cache.
  if r.cachedChunkIdx == chunkIdx:
    return ok()

  if chunkIdx >= r.offsets.len:
    return err("chunk index out of range: " & $chunkIdx)

  let startOff = r.offsets[chunkIdx]
  let endOff =
    if chunkIdx + 1 < r.offsets.len:
      r.offsets[chunkIdx + 1]
    else:
      uint64(r.data.len)
  let compressedLen = endOff - startOff
  if compressedLen == 0:
    return err("chunk " & $chunkIdx & " has zero compressed size")

  let frameSize = ZSTD_getFrameContentSize(
    unsafeAddr r.data[int(startOff)], csize_t(compressedLen))
  if frameSize == ZSTD_CONTENTSIZE_UNKNOWN or frameSize == ZSTD_CONTENTSIZE_ERROR:
    return err("cannot determine decompressed size for chunk " & $chunkIdx)

  r.cachedChunk.setLen(int(frameSize))
  let decompSize = ZSTD_decompress(
    addr r.cachedChunk[0], csize_t(frameSize),
    unsafeAddr r.data[int(startOff)], csize_t(compressedLen))

  if ZSTD_isError(decompSize) != 0:
    return err("zstd decompress failed for chunk " & $chunkIdx & ": " &
      $ZSTD_getErrorName(decompSize))

  r.cachedChunkIdx = chunkIdx
  ok()

proc read*(r: var ChunkedCompressedTableReader, index: uint64,
    output: var openArray[byte]): Result[void, string] =
  ## Read record at index into output buffer.
  if index >= r.totalRecords:
    return err("index out of range: " & $index & " >= " & $r.totalRecords)
  if output.len < r.recordSize:
    return err("output buffer too small")

  let chunkIdx = int(index div uint64(r.chunkSize))
  let recordInChunk = int(index mod uint64(r.chunkSize))

  let decRes = r.decompressChunk(chunkIdx)
  if decRes.isErr:
    return err(decRes.error)

  let offset = recordInChunk * r.recordSize
  for i in 0 ..< r.recordSize:
    output[i] = r.cachedChunk[offset + i]
  ok()
