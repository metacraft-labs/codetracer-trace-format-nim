{.push raises: [].}

## Execution stream writer/reader for variable-length step events.
##
## Unlike ChunkedCompressedTable (which stores fixed-size records), the
## execution stream packs variable-length StepEvents into fixed-count chunks.
## Each chunk holds up to `chunkSize` events, compressed with Zstd.
##
## # Wire format (M24a-1: SPEC-canonical layout)
##
## The on-disk layout matches the canonical spec
## (``codetracer-trace-format-spec/seekable-zstd.md`` §"Chunk Format" +
## §"Companion Index Stream") and is BYTE-COMPATIBLE with the Rust
## ``codetracer_trace_writer::step_stream`` writer /
## ``codetracer_trace_reader::step_stream_reader`` reader.  A bundle written
## by this Nim writer can therefore be read directly by the Rust
## ``StepStreamReader`` (the property the db-backend seekable overlay relies
## on), and vice versa.
##
## Data layout (steps.dat):
##   [zstd(chunk 0)][zstd(chunk 1)]...
##
## Each chunk's uncompressed content is the bare concatenation of encoded
## step events — there is NO per-chunk inline header (no event count).  The
## first step record of every chunk is an AbsoluteStep so each chunk is
## independently decodable (the running absolute global_position_index resets
## at every chunk boundary).
##
## Index layout (steps.idx):
##   [chunk_size: u32 LE]           # max events per chunk
##   [offset_0: u64 LE]             # byte offset of chunk 0 in .dat
##   [offset_1: u64 LE]             # ...
##
## There is NO ``total_events`` header or trailer; the total record count is
## derived by decoding the last chunk (all chunks but the last hold exactly
## ``chunk_size`` records).  To read event N:
##   1. chunk = N / chunk_size (the last chunk may be partial)
##   2. Decompress chunk at offsets[chunk]
##   3. Decode forward, scanning N % chunk_size events to the target
##
## # Backward compatibility (legacy Nim-v4 bundles)
##
## Bundles written by the pre-M24a-1 Nim writer carry a different framing:
## ``steps.idx`` had a ``total_events`` placeholder after the chunk_size header
## plus a ``total_events`` trailer, and each chunk's uncompressed data started
## with a ``u32 LE`` event count.  Those bundles never set the ``meta.dat``
## ``has_step_stream`` flag (bit 9), so the FFI reader distinguishes the two
## layouts by that flag: flag set ⇒ SPEC layout, flag clear ⇒ legacy layout.
## ``initExecStreamReader`` accepts an explicit ``legacy`` parameter for this;
## standalone callers that only ever read freshly-written bundles get the SPEC
## layout by default.

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
    legacy: bool               ## true ⇒ legacy Nim-v4 framing (u32 count
                               ## header per chunk + total_events trailer);
                               ## false ⇒ SPEC layout (header-less chunks,
                               ## no trailer).  See module docs.
    # Cache for last decompressed chunk
    cachedChunkIdx: int        ## -1 means no cache
    chunkDecompressions: uint64
      ## Number of *distinct* Zstd chunk inflations performed since this
      ## reader was opened.  Mirrors the db-backend
      ## ``SeekableCallStream::chunk_decompressions`` bounded-decompression
      ## probe: a targeted ``readEvent`` / ``stepAbsoluteGlobalLineIndex``
      ## inflates at most one new chunk, and clustered reads inside one
      ## chunk inflate it at most once.  Consumers that must PROVE they
      ## only touched a bounded slice of the step stream (rather than
      ## scanning the whole stream) assert this counter stays small.  See
      ## ``chunkDecompressions`` / ``NewTraceReader.execChunkDecompressions``.
    cachedChunk: seq[byte]
    cachedChunkEventCount: uint32
    cachedChunkPayloadStart: int ## byte offset within ``cachedChunk`` where the
                                 ## first encoded event begins: 4 in legacy mode
                                 ## (past the u32 count header), 0 in SPEC mode.

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

  # Write index header: just the u32 chunk_size (SPEC layout — no
  # total_events placeholder, no trailer; matches the Rust step_stream writer).
  var hdr: array[4, byte]
  let csLE = toBytesLE(uint32(chunkSize))
  for i in 0 ..< 4:
    hdr[i] = csLE[i]
  let hdrRes = ctfs.writeToFile(writer.indexFile, hdr)
  if hdrRes.isErr:
    return err("failed to write idx header: " & hdrRes.error)

  ok(writer)

proc flushChunk(ctfs: var Ctfs, w: var ExecStreamWriter): Result[void, string] =
  ## Compress and write the current buffer as one chunk.
  if w.eventCount == 0:
    return ok()

  # SPEC layout: the chunk's uncompressed payload is the bare concatenation
  # of encoded events — no per-chunk event-count header.  (The Rust
  # step_stream writer emits the same header-less chunks, so a chunk written
  # here is byte-for-byte decodable by the Rust StepStreamReader.)
  let bound = ZSTD_compressBound(csize_t(w.buffer.len))
  var compressed = newSeq[byte](int(bound))

  let compressedSize = ZSTD_compress(
    addr compressed[0], csize_t(bound),
    addr w.buffer[0], csize_t(w.buffer.len),
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
    of sekDeltaColumn:
      # In column-aware traces `global_position_index` is a single 1-D
      # address over (line, column) tuples, so a column delta is also a
      # position delta.  At chunk boundaries we promote it to an
      # AbsoluteStep just like sekDeltaStep so the chunk is independently
      # decodable.
      let newIndex = uint64(int64(w.lastGlobalLineIndex) + ev.columnDelta)
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
  of sekDeltaColumn:
    w.lastGlobalLineIndex = uint64(int64(w.lastGlobalLineIndex) + ev.columnDelta)
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
  ##
  ## SPEC layout: ``steps.idx`` carries NO ``total_events`` trailer — the
  ## record count is recoverable from the chunk offsets plus the last chunk's
  ## decoded record count (all chunks but the last hold exactly ``chunk_size``
  ## records).  This matches the Rust ``step_stream`` writer, whose ``steps.idx``
  ## is exactly ``[chunk_size: u32][offset_0: u64]...``.
  ?ctfs.flushChunk(w)
  ok()

proc totalEvents*(w: ExecStreamWriter): uint64 = w.totalEvents

# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

proc decodeSpecChunkRecordCount(compressed: openArray[byte]): Result[int, string] =
  ## Decompress a SPEC-layout chunk (header-less payload) and count its
  ## records by decoding forward to the end of the chunk.  Used to recover the
  ## last chunk's record count (the SPEC ``steps.idx`` carries no
  ## ``total_events``), mirroring the Rust ``StepStreamReader::open`` logic.
  if compressed.len == 0:
    return err("step chunk has zero compressed size")
  let frameSize = ZSTD_getFrameContentSize(
    unsafeAddr compressed[0], csize_t(compressed.len))
  if frameSize == ZSTD_CONTENTSIZE_UNKNOWN or frameSize == ZSTD_CONTENTSIZE_ERROR:
    return err("cannot determine decompressed size for last step chunk")
  var raw = newSeq[byte](int(frameSize))
  let decompSize = ZSTD_decompress(
    addr raw[0], csize_t(frameSize),
    unsafeAddr compressed[0], csize_t(compressed.len))
  if ZSTD_isError(decompSize) != 0:
    return err("zstd decompress failed for last step chunk: " &
      $ZSTD_getErrorName(decompSize))
  raw.setLen(int(decompSize))
  var pos = 0
  var count = 0
  while pos < raw.len:
    let ev = decodeStepEvent(raw, pos)
    if ev.isErr:
      return err("failed to count records in last step chunk: " & ev.error)
    inc count
  ok(count)

proc initExecStreamReader*(ctfsBytes: openArray[byte],
    blockSize: int = 4096,
    maxEntries: int = 170,
    legacy: bool = false): Result[ExecStreamReader, string] =
  ## Read an execution stream from CTFS bytes.
  ##
  ## ``legacy`` selects the on-disk framing (see module docs):
  ##   * ``false`` (default) — SPEC layout: ``steps.idx`` is
  ##     ``[chunk_size: u32][offset_0: u64]...`` (no ``total_events``) and each
  ##     chunk's uncompressed payload is header-less.  Byte-compatible with the
  ##     Rust ``StepStreamReader``.
  ##   * ``true`` — legacy Nim-v4 layout: ``steps.idx`` has a ``total_events``
  ##     placeholder after the header plus a trailing ``total_events`` u64, and
  ##     each chunk's uncompressed data starts with a ``u32`` event count.
  ##
  ## The FFI reader passes ``legacy = not meta.hasStepStream``: pre-M24a-1
  ## bundles never set the ``has_step_stream`` flag, so a clear flag selects the
  ## legacy reader and a set flag the SPEC reader.
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

  if idxData.len < 4:
    return err("index file too small for chunk_size header")

  var cs4: array[4, byte]
  for i in 0 ..< 4:
    cs4[i] = idxData[i]
  let chunkSize = fromBytesLE(uint32, cs4)
  if chunkSize == 0:
    return err("chunkSize in index is 0")

  var offsets: seq[uint64]
  var totalEvents: uint64
  let payloadStart = if legacy: 4 else: 0  ## per-chunk payload offset

  if legacy:
    # Legacy index layout:
    #   [0..3]   u32 chunk_size
    #   [4..11]  u64 total_events placeholder (ignored)
    #   [12..]   u64 offsets...
    #   [last 8] u64 total_events trailer
    if idxData.len < 12:
      return err("index file too small for legacy header")
    let payloadBytes = idxData.len - 12  # after chunk_size + placeholder total
    if payloadBytes < 8:
      return err("index file too small for trailer")
    let trailerStart = idxData.len - 8
    var te8: array[8, byte]
    for i in 0 ..< 8:
      te8[i] = idxData[trailerStart + i]
    totalEvents = fromBytesLE(uint64, te8)
    let offsetRegionBytes = trailerStart - 12
    if offsetRegionBytes mod 8 != 0:
      return err("index file has trailing bytes in offset region")
    let numChunks = offsetRegionBytes div 8
    offsets = newSeq[uint64](numChunks)
    for i in 0 ..< numChunks:
      var o8: array[8, byte]
      for j in 0 ..< 8:
        o8[j] = idxData[12 + i * 8 + j]
      offsets[i] = fromBytesLE(uint64, o8)
  else:
    # SPEC index layout: [chunk_size: u32][offset_0: u64]...  (no trailer).
    let offsetRegionBytes = idxData.len - 4
    if offsetRegionBytes mod 8 != 0:
      return err("index file has trailing bytes in offset region")
    let numChunks = offsetRegionBytes div 8
    offsets = newSeq[uint64](numChunks)
    for i in 0 ..< numChunks:
      var o8: array[8, byte]
      for j in 0 ..< 8:
        o8[j] = idxData[4 + i * 8 + j]
      offsets[i] = fromBytesLE(uint64, o8)

    # Recover total_events: all chunks but the last hold exactly chunk_size
    # records; the last holds whatever decodes out of it (Rust parity).
    if numChunks == 0:
      totalEvents = 0
    else:
      let lastChunk = numChunks - 1
      let startOff = int(offsets[lastChunk])
      let endOff = datData.len
      if startOff > endOff:
        return err("last chunk offset past end of steps.dat")
      let lastCount = ?decodeSpecChunkRecordCount(
        datData.toOpenArray(startOff, endOff - 1))
      totalEvents = uint64(lastChunk) * uint64(chunkSize) + uint64(lastCount)

  ok(ExecStreamReader(
    data: datData,
    chunkSize: chunkSize,
    offsets: offsets,
    totalEventsVal: totalEvents,
    legacy: legacy,
    cachedChunkIdx: -1,
    cachedChunk: @[],
    cachedChunkEventCount: 0,
    cachedChunkPayloadStart: payloadStart,
  ))

proc totalEvents*(r: ExecStreamReader): uint64 = r.totalEventsVal

proc chunkDecompressions*(r: ExecStreamReader): uint64 = r.chunkDecompressions
  ## Distinct Zstd chunk inflations performed so far (bounded-decompression
  ## probe; see the ``chunkDecompressions`` field).

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
  # Account a distinct chunk inflation: we only reach here when the target
  # chunk differs from the cached one (the early-return above caught the
  # cache hit), so each increment is a genuinely new inflation.
  r.chunkDecompressions += 1

  if r.legacy:
    # Legacy chunk: the first 4 bytes are a u32 LE event count, records follow.
    if r.cachedChunk.len < 4:
      return err("decompressed chunk too small for event count header")
    var ec4: array[4, byte]
    for i in 0 ..< 4:
      ec4[i] = r.cachedChunk[i]
    r.cachedChunkEventCount = fromBytesLE(uint32, ec4)
  else:
    # SPEC chunk: header-less payload — count records by decoding forward.
    var pos = 0
    var count = 0
    while pos < r.cachedChunk.len:
      let ev = decodeStepEvent(r.cachedChunk, pos)
      if ev.isErr:
        return err("failed to count records in chunk " & $chunkIdx & ": " &
          ev.error)
      inc count
    r.cachedChunkEventCount = uint32(count)

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

  # Skip past the legacy u32 event count header (0 in SPEC mode).
  var pos = r.cachedChunkPayloadStart

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

  var pos = r.cachedChunkPayloadStart
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
