{.push raises: [].}

## Execution stream writer/reader for variable-length step events.
##
## Unlike ChunkedCompressedTable (which stores fixed-size records), the
## execution stream packs variable-length StepEvents into fixed-count chunks.
## Each chunk holds up to `chunkSize` events, compressed with Zstd.
##
## Data layout (steps.dat):
##   [compressed chunk 0][compressed chunk 1]...
##
## Each chunk's uncompressed content starts with a u32 LE event count,
## followed by the concatenated encoded events.
##
## Index layout (steps.idx):
##   [chunk_size: u32 LE]           # max events per chunk
##   [total_events: u64 LE]         # total events across all chunks
##   [offset_0: u64 LE]             # byte offset of chunk 0 in .dat
##   [offset_1: u64 LE]             # ...
##
## To read event N:
##   1. chunk = N / chunk_size (but last chunk may be partial)
##   2. Decompress chunk at offsets[chunk]
##   3. Read u32 event_count from decompressed data
##   4. Scan N % chunk_size events to find the target

import results
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ../codetracer_ctfs/zstd_bindings
import ./step_encoding
import ./varint

const
  DefaultExecChunkSize* = 4096  ## events per chunk
  ExecCompressionLevel = 3

type
  ExecStreamWriter* = object
    dataFile: CtfsInternalFile
    indexFile: CtfsInternalFile
    chunkSize: int
    buffer: seq[byte]          ## accumulated encoded events for current chunk
    eventCount: int            ## events in current buffer
    totalEvents: uint64
    dataOffset: uint64         ## running byte offset in data file
    lastGlobalLineIndex: uint64  ## tracks current absolute position for delta context

  ExecStreamReader* = object
    data: seq[byte]            ## raw steps.dat content
    chunkSize*: uint32
    offsets: seq[uint64]       ## chunk byte offsets from steps.idx
    totalEventsVal: uint64
    # Cache for last decompressed chunk
    cachedChunkIdx: int        ## -1 means no cache
    cachedChunk: seq[byte]
    cachedChunkEventCount: uint32

proc initExecStreamWriter*(ctfs: var Ctfs,
    chunkSize: int = DefaultExecChunkSize): Result[ExecStreamWriter, string] =
  ## Create a new execution stream in the CTFS container.
  ## Creates steps.dat and steps.idx files.
  if chunkSize <= 0:
    return err("chunkSize must be positive")

  let datRes = ctfs.addFile("steps.dat")
  if datRes.isErr:
    return err("failed to create steps.dat: " & datRes.error)

  let idxRes = ctfs.addFile("steps.idx")
  if idxRes.isErr:
    return err("failed to create steps.idx: " & idxRes.error)

  var writer = ExecStreamWriter(
    dataFile: datRes.get(),
    indexFile: idxRes.get(),
    chunkSize: chunkSize,
    buffer: @[],
    eventCount: 0,
    totalEvents: 0,
    dataOffset: 0,
    lastGlobalLineIndex: 0,
  )

  # Write index header: u32 chunk_size + u64 total_events (placeholder, updated on flush)
  var hdr: array[12, byte]
  let csLE = toBytesLE(uint32(chunkSize))
  for i in 0 ..< 4:
    hdr[i] = csLE[i]
  # total_events = 0 initially (bytes 4..11)
  let hdrRes = ctfs.writeToFile(writer.indexFile, hdr)
  if hdrRes.isErr:
    return err("failed to write idx header: " & hdrRes.error)

  ok(writer)

proc flushChunk(ctfs: var Ctfs, w: var ExecStreamWriter): Result[void, string] =
  ## Compress and write the current buffer as one chunk.
  if w.eventCount == 0:
    return ok()

  # Prepend u32 LE event count to the uncompressed data
  var uncompressed = newSeq[byte](4 + w.buffer.len)
  let ecLE = toBytesLE(uint32(w.eventCount))
  for i in 0 ..< 4:
    uncompressed[i] = ecLE[i]
  for i in 0 ..< w.buffer.len:
    uncompressed[4 + i] = w.buffer[i]

  let bound = ZSTD_compressBound(csize_t(uncompressed.len))
  var compressed = newSeq[byte](int(bound))

  let compressedSize = ZSTD_compress(
    addr compressed[0], csize_t(bound),
    addr uncompressed[0], csize_t(uncompressed.len),
    cint(ExecCompressionLevel))

  if ZSTD_isError(compressedSize) != 0:
    return err("zstd compress failed: " & $ZSTD_getErrorName(compressedSize))

  # Write byte offset to index
  var offBytes: array[8, byte]
  let offLE = toBytesLE(w.dataOffset)
  for i in 0 ..< 8:
    offBytes[i] = offLE[i]
  let idxRes = ctfs.writeToFile(w.indexFile, offBytes)
  if idxRes.isErr:
    return err("failed to write offset to idx: " & idxRes.error)

  # Write compressed data to .dat
  let datRes = ctfs.writeToFile(w.dataFile,
      compressed.toOpenArray(0, int(compressedSize) - 1))
  if datRes.isErr:
    return err("failed to write compressed chunk: " & datRes.error)

  w.dataOffset += uint64(compressedSize)
  w.eventCount = 0
  w.buffer.setLen(0)
  ok()

proc writeEvent*(ctfs: var Ctfs, w: var ExecStreamWriter,
    event: StepEvent): Result[void, string] =
  ## Write a step event to the execution stream.
  ##
  ## At chunk boundaries (first event in a new chunk), if the event is a
  ## DeltaStep it is automatically converted to an AbsoluteStep so each
  ## chunk is independently decodable.

  var ev = event

  # At the start of a chunk, ensure an AbsoluteStep
  if w.eventCount == 0:
    case ev.kind
    of sekDeltaStep:
      # Convert delta to absolute using tracked position
      let newIndex = uint64(int64(w.lastGlobalLineIndex) + ev.lineDelta)
      ev = StepEvent(kind: sekAbsoluteStep, globalLineIndex: newIndex)
    of sekAbsoluteStep:
      discard  # already absolute, good
    else:
      discard  # Raise/Catch/ThreadSwitch are fine at chunk start

  # Track lastGlobalLineIndex
  case ev.kind
  of sekAbsoluteStep:
    w.lastGlobalLineIndex = ev.globalLineIndex
  of sekDeltaStep:
    w.lastGlobalLineIndex = uint64(int64(w.lastGlobalLineIndex) + ev.lineDelta)
  else:
    discard

  encodeStepEvent(ev, w.buffer)
  w.eventCount += 1
  w.totalEvents += 1

  if w.eventCount >= w.chunkSize:
    ?ctfs.flushChunk(w)

  ok()

proc flush*(ctfs: var Ctfs, w: var ExecStreamWriter): Result[void, string] =
  ## Flush any remaining buffered events as a partial final chunk.
  ## Must be called before serializing the CTFS.
  ## Also updates the total_events field in the index header.
  ?ctfs.flushChunk(w)

  # Rewrite total_events in the index header (bytes 4..11).
  # The CTFS container API is append-only, so we write the total as a
  # trailing u64 that the reader will use instead.
  var teBytes: array[8, byte]
  let teLE = toBytesLE(w.totalEvents)
  for i in 0 ..< 8:
    teBytes[i] = teLE[i]

  # We can't seek back in the CTFS file, so append total_events as a trailer.
  # The reader will read it from the known position after all chunk offsets.
  let res = ctfs.writeToFile(w.indexFile, teBytes)
  if res.isErr:
    return err("failed to write total_events trailer: " & res.error)

  ok()

proc totalEvents*(w: ExecStreamWriter): uint64 = w.totalEvents

# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

proc initExecStreamReader*(ctfsBytes: openArray[byte],
    blockSize: int = 4096,
    maxEntries: int = 170): Result[ExecStreamReader, string] =
  ## Read an execution stream from CTFS bytes.
  let datRes = readInternalFile(ctfsBytes, "steps.dat",
      uint32(blockSize), uint32(maxEntries))
  if datRes.isErr:
    return err("failed to read steps.dat: " & datRes.error)
  let datData = datRes.get()

  let idxRes = readInternalFile(ctfsBytes, "steps.idx",
      uint32(blockSize), uint32(maxEntries))
  if idxRes.isErr:
    return err("failed to read steps.idx: " & idxRes.error)
  let idxData = idxRes.get()

  # Index format:
  #   [0..3]  u32 chunk_size
  #   [4..11] u64 total_events (placeholder, ignored — real value is trailer)
  #   [12..]  u64 offsets...
  #   [last 8 bytes] u64 total_events (trailer)
  if idxData.len < 12:
    return err("index file too small for header")

  var cs4: array[4, byte]
  for i in 0 ..< 4:
    cs4[i] = idxData[i]
  let chunkSize = fromBytesLE(uint32, cs4)
  if chunkSize == 0:
    return err("chunkSize in index is 0")

  # The layout after the 12-byte header is: N offset entries (8 bytes each),
  # followed by an 8-byte total_events trailer.
  let payloadBytes = idxData.len - 12  # after chunk_size + placeholder total
  if payloadBytes < 8:
    return err("index file too small for trailer")

  # The last 8 bytes are the total_events trailer
  let trailerStart = idxData.len - 8
  var te8: array[8, byte]
  for i in 0 ..< 8:
    te8[i] = idxData[trailerStart + i]
  let totalEvents = fromBytesLE(uint64, te8)

  # Offsets are between byte 12 and the trailer
  let offsetRegionBytes = trailerStart - 12
  if offsetRegionBytes mod 8 != 0:
    return err("index file has trailing bytes in offset region")
  let numChunks = offsetRegionBytes div 8

  var offsets = newSeq[uint64](numChunks)
  for i in 0 ..< numChunks:
    var o8: array[8, byte]
    for j in 0 ..< 8:
      o8[j] = idxData[12 + i * 8 + j]
    offsets[i] = fromBytesLE(uint64, o8)

  ok(ExecStreamReader(
    data: datData,
    chunkSize: chunkSize,
    offsets: offsets,
    totalEventsVal: totalEvents,
    cachedChunkIdx: -1,
    cachedChunk: @[],
    cachedChunkEventCount: 0,
  ))

proc totalEvents*(r: ExecStreamReader): uint64 = r.totalEventsVal

proc decompressChunk(r: var ExecStreamReader,
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

  r.cachedChunk.setLen(int(decompSize))

  # Read event count from the first 4 bytes of decompressed data
  if r.cachedChunk.len < 4:
    return err("decompressed chunk too small for event count header")
  var ec4: array[4, byte]
  for i in 0 ..< 4:
    ec4[i] = r.cachedChunk[i]
  r.cachedChunkEventCount = fromBytesLE(uint32, ec4)

  r.cachedChunkIdx = chunkIdx
  ok()

proc readEvent*(r: var ExecStreamReader,
    eventIndex: uint64): Result[StepEvent, string] =
  ## Read a single event by its global index.
  if eventIndex >= r.totalEventsVal:
    return err("event index out of range: " & $eventIndex & " >= " & $r.totalEventsVal)

  let chunkIdx = int(eventIndex div uint64(r.chunkSize))
  let eventInChunk = int(eventIndex mod uint64(r.chunkSize))

  ?r.decompressChunk(chunkIdx)

  # Skip past the u32 event count header
  var pos = 4

  # Scan forward to the desired event
  for i in 0 ..< eventInChunk:
    let ev = decodeStepEvent(r.cachedChunk, pos)
    if ev.isErr:
      return err("failed to decode event " & $i & " while scanning chunk " &
        $chunkIdx & ": " & ev.error)

  # Decode the target event
  decodeStepEvent(r.cachedChunk, pos)

proc readChunkEvents*(r: var ExecStreamReader,
    chunkIdx: int,
    output: var seq[StepEvent]): Result[uint64, string] =
  ## Decode every event of chunk ``chunkIdx`` into ``output`` (cleared
  ## first), returning the chunk's first global event index.
  ##
  ## This is the streaming counterpart to [readEvent]: it pays the
  ## chunk's decode cost exactly once and yields all events in order,
  ## avoiding the O(eventInChunk) re-scan that ``readEvent`` performs
  ## per call.  Bulk readers (FFI bulk accessors, postprocess-style
  ## streamers) should walk chunks via this helper instead of looping
  ## ``readEvent``.
  if chunkIdx < 0 or chunkIdx >= r.offsets.len:
    return err("chunk index out of range: " & $chunkIdx)

  ?r.decompressChunk(chunkIdx)

  let firstEventIdx = uint64(chunkIdx) * uint64(r.chunkSize)
  let eventCount = int(r.cachedChunkEventCount)
  output.setLen(0)
  if eventCount == 0:
    return ok(firstEventIdx)
  output = newSeqOfCap[StepEvent](eventCount)

  var pos = 4
  for i in 0 ..< eventCount:
    let evRes = decodeStepEvent(r.cachedChunk, pos)
    if evRes.isErr:
      return err("failed to decode event " & $i & " while streaming chunk " &
        $chunkIdx & ": " & evRes.error)
    output.add(evRes.get())

  ok(firstEventIdx)

proc chunkIndexFor*(r: ExecStreamReader, eventIndex: uint64): int =
  ## Map a global event index to its containing chunk index.  Useful
  ## for bulk readers that walk full chunks at a time and need to
  ## compute chunk boundaries without divmod-ing in callers.
  int(eventIndex div uint64(r.chunkSize))

proc chunkCount*(r: ExecStreamReader): int =
  ## Number of compressed chunks in this exec stream.
  r.offsets.len
