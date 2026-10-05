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
## first POSITION record of every chunk is an AbsoluteStep, whatever records
## precede it, so each chunk is independently decodable: a reader starts each
## chunk without a cursor and refuses a delta before the chunk's first
## AbsoluteStep (`trace-events.md` §"Encoding Rules").
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

import std/bitops
import results
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ../codetracer_ctfs/streaming
import ../codetracer_ctfs/zstd_bindings
import ../codetracer_ctfs/chunk_cache
import ../codetracer_ctfs/member_view
import ./step_encoding
import ./gdh2_arms
import ./varint

const
  DefaultExecChunkSize* = 4096  ## events per chunk
  ExecCompressionLevel = 3

type
  ExecChunkMeta = object
    ## Per-chunk derived state cached alongside the decompressed payload,
    ## built as far as reads reach.
    ##
    ## Step records are variable-length varints, so the only way to reach
    ## record *k* is to decode records 0..k-1. Each record is decoded once on
    ## the way, which notes where it starts and the cursor position after it,
    ## so a walk over the chunk decodes every record once and a read near the
    ## chunk's start decodes no further than it.
    starts: seq[int32]
      ## Byte offset of record `k` inside the decompressed chunk, `k < known`.
    positions: seq[uint64]
      ## The cursor position after record `k` (`trace-events.md` §"Encoding
      ## Rules", "Reading"), `k < known` and `k < posRefusedAt`.
    known: int
      ## How many records have been decoded.
    nextPos: int
      ## Where record `known` starts.
    complete: bool
      ## Every record of the chunk has been decoded: `known` is its count.
    eventCount: uint32
      ## The chunk's record count, from its header (legacy framing) or once
      ## `complete`.
    cursor: uint64
    anchored: bool
    posRefusedAt: int
      ## The first record whose position is refused (a delta before the
      ## chunk's first `AbsoluteStep`, or a negative position); `high(int)`
      ## when none has been.
    posRefusal: string

  ExecStreamWriter* = object
    dataFile: CtfsInternalFile
    indexFile: CtfsInternalFile
    chunkSize: int
    buffer: seq[byte]          ## accumulated encoded events for current chunk
    eventCount: int            ## events in current buffer
    totalEvents: uint64
    dataOffset: uint64         ## running byte offset in data file
    lastGlobalLineIndex: uint64
      ## The position of the last position record written (the stream's
      ## cursor), used to resolve a caller's delta to a position.
    hasPosition: bool
      ## A position record has been written to this stream.
    chunkHasCursor: bool
      ## A position record has been written to the CURRENT chunk. Each chunk
      ## starts without a cursor (`trace-events.md` §"Encoding Rules").

  ExecStreamReader* = object
    data: MemberView           ## steps.dat, in place
    frameScratch: seq[byte]    ## a chunk's frame when it straddles two runs
    chunkSize*: uint32
    offsets: seq[uint64]       ## chunk byte offsets from steps.idx
    totalEventsVal: uint64
    legacy: bool               ## true ⇒ legacy Nim-v4 framing (u32 count
                               ## header per chunk + total_events trailer);
                               ## false ⇒ SPEC layout (header-less chunks,
                               ## no trailer).  See module docs.
    cache: ChunkCache[ExecChunkMeta]
      ## Decompressed chunks, LRU by byte budget.  This used to be a single
      ## "last chunk" slot: a reader jumping between steps in different chunks
      ## — which is exactly what "navigate to step N" does — re-inflated a Zstd
      ## frame on nearly every call.
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
    allowSourceReload: bool
      ## GDH-M2: does the container declare ``FlagExtHasSourceReload``?
      ## Passed down to every ``decodeStepEvent`` call, so tag 0x08 is
      ## accepted only where the header says it may appear.  Default
      ## FALSE, so a caller that has not been taught about the flag
      ## refuses the tag rather than decoding it.
    stored: bool
      ## The chunks are stored as their content rather than as zstd frames:
      ## the container is compact (`ctfs-container.md` §1f).
    payloadStart: int          ## byte offset within a decompressed chunk where
                               ## the first encoded event begins: 4 in legacy
                               ## mode (past the u32 count header), 0 in SPEC
                               ## mode.
    heldChunk: int
      ## One more than the chunk the last read reached, 0 before any read;
      ## `heldSlot` is its cache slot. Every slot is found through
      ## `chunkSlot`, which sets both, and the cache evicts only inside it,
      ## so the chunk named here is resident: a read in it skips the cache.
    heldSlot: int

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
  ctfs.syncEntry(writer.indexFile)

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

  let chunkStart = w.dataOffset

  # The chunk's bytes, then its offset in the companion index, then ONE
  # publish: every block written since the last seal (the chunk, its mapping,
  # the index, interning records), then the root entries that publish their
  # sizes (`ctfs-container.md` §6, "Durability", rule 2). A follow reader that
  # sees N index entries can assume chunks 0..N-1 are on disk.
  let datRes = ctfs.writeToFile(w.dataFile,
      compressed.toOpenArray(0, int(compressedSize) - 1))
  if datRes.isErr:
    return err("failed to write compressed chunk: " & datRes.error)

  var offBytes: array[8, byte]
  let offLE = toBytesLE(chunkStart)
  for i in 0 ..< 8:
    offBytes[i] = offLE[i]
  let idxRes = ctfs.writeToFile(w.indexFile, offBytes)
  if idxRes.isErr:
    return err("failed to write offset to idx: " & idxRes.error)
  ctfs.syncEntry(w.indexFile)

  w.dataOffset += uint64(compressedSize)
  w.eventCount = 0
  w.buffer.setLen(0)
  ok()

proc varintLen(v: uint64): int {.inline.} =
  ## Bytes an unsigned LEB128 varint of `v` takes.
  if v == 0: 1 else: (64 - countLeadingZeroBits(v) + 6) div 7

proc zigzag(d: int64): uint64 {.inline.} =
  cast[uint64](d shl 1) xor cast[uint64](ashr(d, 63))

proc writeEvent*(ctfs: var Ctfs, w: var ExecStreamWriter,
    event: StepEvent): Result[void, string] =
  ## Write a step event to the execution stream.
  ##
  ## A position event (`AbsoluteStep`, `DeltaStep` or `DeltaColumn`) is
  ## resolved to its position — a delta relative to the last position written
  ## — and then RE-ENCODED by the normative rule of `trace-events.md`
  ## §"Encoding Rules", so two writers given one recording write the same
  ## bytes:
  ##
  ## 1. the first position record of each chunk is an `AbsoluteStep`, whatever
  ##    records precede it in the chunk;
  ## 2. otherwise a delta when the varint of `zigzag(p - cursor)` is strictly
  ##    shorter than the varint of `p` — a `DeltaColumn` when the caller
  ##    registered a column step, a `DeltaStep` otherwise;
  ## 3. otherwise an `AbsoluteStep` (a tie goes to the absolute).
  ##
  ## Every other record is written as given and leaves the cursor alone.
  if w.eventCount == 0:
    w.chunkHasCursor = false

  var isPosition = true
  var pos: uint64
  var column = false
  case event.kind
  of sekAbsoluteStep:
    pos = event.globalLineIndex
  of sekDeltaStep, sekDeltaColumn:
    if not w.hasPosition:
      return err("a " & (if event.kind == sekDeltaStep: "DeltaStep" else:
        "DeltaColumn") & " was given before any position was written to " &
        "steps.dat, so it has nothing to be relative to")
    let d = if event.kind == sekDeltaStep: event.lineDelta else: event.columnDelta
    let p = int64(w.lastGlobalLineIndex) + d
    if p < 0:
      return err("a step delta of " & $d & " from position " &
        $w.lastGlobalLineIndex & " is a negative position")
    pos = uint64(p)
    column = event.kind == sekDeltaColumn
  else:
    isPosition = false

  if isPosition:
    # Encoded straight into the chunk buffer: this is the per-step hot path.
    var useAbsolute = true
    var d: int64 = 0
    if w.chunkHasCursor:
      d = int64(pos) - int64(w.lastGlobalLineIndex)
      useAbsolute = varintLen(pos) <= varintLen(zigzag(d))
    if useAbsolute:
      w.buffer.add(TagAbsoluteStep)
      encodeVarint(pos, w.buffer)
    else:
      w.buffer.add(if column: TagDeltaColumn else: TagDeltaStep)
      encodeVarint(zigzag(d), w.buffer)
    w.lastGlobalLineIndex = pos
    w.hasPosition = true
    w.chunkHasCursor = true
  else:
    encodeStepEvent(event, w.buffer)

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

proc countSpecChunkRecords(raw: openArray[byte],
    allowSourceReload: bool): Result[int, string]

proc decodeSpecChunkRecordCount(compressed: openArray[byte],
    allowSourceReload: bool, stored: bool): Result[int, string] =
  ## Count a SPEC-layout chunk's (header-less payload) records by decoding
  ## forward to the end of the chunk, decompressing it first unless it is
  ## ``stored`` as its content (a compact container, `ctfs-container.md`
  ## §1f).  Used to recover the last chunk's record count (the SPEC
  ## ``steps.idx`` carries no ``total_events``), mirroring the Rust
  ## ``StepStreamReader::open`` logic.
  if stored:
    if compressed.len == 0:
      return err("step chunk is empty")
    return countSpecChunkRecords(compressed, allowSourceReload)
  if compressed.len == 0:
    return err("step chunk has zero compressed size")
  let frameSize = ZSTD_getFrameContentSize(
    unsafeAddr compressed[0], csize_t(compressed.len))
  if frameSize == ZSTD_CONTENTSIZE_UNKNOWN or frameSize == ZSTD_CONTENTSIZE_ERROR:
    return err("cannot determine decompressed size for last step chunk")
  var raw = newSeqUninit[byte](int(frameSize))  # written by the inflate
  let decompSize = zstdDecompressShared(
    if raw.len > 0: addr raw[0] else: nil, csize_t(frameSize),
    unsafeAddr compressed[0], csize_t(compressed.len))
  if ZSTD_isError(decompSize) != 0:
    return err("zstd decompress failed for last step chunk: " &
      $ZSTD_getErrorName(decompSize))
  raw.setLen(int(decompSize))
  countSpecChunkRecords(raw, allowSourceReload)

proc countSpecChunkRecords(raw: openArray[byte],
    allowSourceReload: bool): Result[int, string] =
  var pos = 0
  var count = 0
  while pos < raw.len:
    let ev = decodeStepEvent(raw, pos, allowSourceReload)
    if ev.isErr:
      return err("failed to count records in last step chunk: " & ev.error)
    # FALSIFIER (``gdh2FalsifyUncountedMarker``,
    # gdh2_reload_marker_round_trips): treat the reload marker as "not a
    # record" while still consuming its bytes.  This is the SHORTER,
    # PLAUSIBLE step stream the strict-rejection contract exists to
    # prevent — no error, no diagnostic, just a total that disagrees with
    # the value stream's.  It is here to prove the gate's count
    # comparison has teeth: the naive skip arm cascades into a different
    # tag error and never reaches it.
    when gdh2Arm(gdh2FalsifyUncountedMarker):
      if ev.get().kind != sekSourceReload:
        inc count
    else:
      inc count
  ok(count)

proc openExecStream(datData: sink MemberView, idxData: seq[byte],
    stored, legacy: bool, cacheBytes: uint64,
    allowSourceReload: bool): Result[ExecStreamReader, string] =
  ## The reader of a `steps.dat` (held in place) and its parsed `steps.idx`.
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
      var scratch: seq[byte]
      var lastCount = 0
      datData.withSpan(startOff, endOff - startOff, scratch, frame):
        lastCount = ?decodeSpecChunkRecordCount(frame, allowSourceReload,
          stored)
      totalEvents = uint64(lastChunk) * uint64(chunkSize) + uint64(lastCount)

  # Sized before `offsets` is handed to the reader: a field initialiser that
  # reads `offsets` after the one that takes it may see it moved out, and a
  # cache sized for no chunks misses on every read.
  var cache = initChunkCache[ExecChunkMeta](offsets.len, cacheBytes)
  ok(ExecStreamReader(
    data: move datData,
    chunkSize: chunkSize,
    offsets: move offsets,
    totalEventsVal: totalEvents,
    legacy: legacy,
    cache: move cache,
    allowSourceReload: allowSourceReload,
    payloadStart: payloadStart,
    stored: stored,
  ))

proc initExecStreamReader*(ctfsBytes: openArray[byte],
    blockSize: int = 4096,
    maxEntries: int = 170,
    legacy: bool = false,
    cacheBytes: uint64 = DefaultStreamChunkCacheBytes,
    allowSourceReload: bool = false): Result[ExecStreamReader, string] =
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
  var datRes = readInternalFile(ctfsBytes, "steps.dat",
      uint32(blockSize), uint32(maxEntries))
  if datRes.isErr:
    return err("failed to read steps.dat: " & datRes.error)
  let idxRes = readInternalFile(ctfsBytes, "steps.idx",
      uint32(blockSize), uint32(maxEntries))
  if idxRes.isErr:
    return err("failed to read steps.idx: " & idxRes.error)
  openExecStream(viewBytes(move datRes.get()), idxRes.get(),
    isCompactContainer(ctfsBytes), legacy, cacheBytes, allowSourceReload)

proc initExecStreamReader*(image: ContainerImage,
    blockSize: int = 4096,
    maxEntries: int = 170,
    legacy: bool = false,
    cacheBytes: uint64 = DefaultStreamChunkCacheBytes,
    allowSourceReload: bool = false): Result[ExecStreamReader, string] =
  ## As above, over a container image it shares: `steps.dat` is read in place.
  var datRes = viewMember(image, "steps.dat", uint32(blockSize),
    uint32(maxEntries))
  if datRes.isErr:
    return err("failed to read steps.dat: " & datRes.error)
  let idxRes = viewMember(image, "steps.idx", uint32(blockSize),
    uint32(maxEntries))
  if idxRes.isErr:
    return err("failed to read steps.idx: " & idxRes.error)
  openExecStream(move datRes.get(), idxRes.get().copyOut(0, idxRes.get().len),
    isCompactContainer(image.bytes), legacy, cacheBytes, allowSourceReload)

proc totalEvents*(r: ExecStreamReader): uint64 = r.totalEventsVal

proc chunkDecompressions*(r: ExecStreamReader): uint64 = r.chunkDecompressions
  ## Distinct Zstd chunk inflations performed so far (bounded-decompression
  ## probe; see the ``chunkDecompressions`` field).

proc advanceCursor(ev: StepEvent, i: int, chunkIdx: int, cursor: var uint64,
    anchored: var bool, why: var string): bool =
  ## Move a chunk's cursor past record `i`, `ev` (`trace-events.md`
  ## §"Encoding Rules", "Reading"). False, with `why` set, where the record
  ## has no position to move to. Called once per record decoded, so it builds
  ## no `Result`.
  case ev.kind
  of sekAbsoluteStep:
    cursor = ev.globalLineIndex
    anchored = true
  of sekDeltaStep, sekDeltaColumn:
    if not anchored:
      why = "steps.dat chunk " & $chunkIdx & ": record " & $i & " is a " &
        (if ev.kind == sekDeltaStep: "DeltaStep" else: "DeltaColumn") &
        " before the chunk's first AbsoluteStep, so it has no position to " &
        "be relative to"
      return false
    let d = if ev.kind == sekDeltaStep: ev.lineDelta else: ev.columnDelta
    let p = int64(cursor) + d
    if p < 0:
      why = "steps.dat chunk " & $chunkIdx & ": record " & $i &
        " resolves to a negative position"
      return false
    cursor = uint64(p)
  else:
    discard
  true

proc commitChunk(r: var ExecStreamReader, slot: int,
    chunkIdx: int): Result[int, string]

proc chunkSlot(r: var ExecStreamReader,
    chunkIdx: int): Result[int, string] =
  ## Return the cache slot holding chunk ``chunkIdx``, inflating it first if it
  ## is not resident, and note it as the chunk held.
  let hit = r.cache.find(chunkIdx)
  if hit >= 0:
    r.heldChunk = chunkIdx + 1
    r.heldSlot = hit
    return ok(hit)

  if chunkIdx < 0 or chunkIdx >= r.offsets.len:
    return err("chunk index out of range: " & $chunkIdx)

  let startOff = r.offsets[chunkIdx]
  let endOff =
    if chunkIdx + 1 < r.offsets.len:
      r.offsets[chunkIdx + 1]
    else:
      uint64(r.data.len)
  if startOff > endOff or endOff > uint64(r.data.len):
    return err("chunk " & $chunkIdx & " offsets out of range")
  let compressedLen = endOff - startOff
  if compressedLen == 0:
    return err("chunk " & $chunkIdx & " has zero compressed size")
  let frame = r.data.span(int(startOff), int(compressedLen), r.frameScratch)

  if r.stored:
    let slot = r.cache.acquire()
    r.cache.prepare(slot, int(compressedLen))
    copyMem(addr r.cache.data(slot)[0], frame, int(compressedLen))
    return r.commitChunk(slot, chunkIdx)

  let frameSize = ZSTD_getFrameContentSize(frame, csize_t(compressedLen))
  if frameSize == ZSTD_CONTENTSIZE_UNKNOWN or frameSize == ZSTD_CONTENTSIZE_ERROR:
    return err("cannot determine decompressed size for chunk " & $chunkIdx)
  if frameSize == 0:
    # The writer never emits an empty chunk, so this is a malformed stream.
    # Guard it explicitly: cache slots start empty, so taking `addr data[0]`
    # below would index past the end.
    return err("chunk " & $chunkIdx & " decompresses to zero bytes")

  let slot = r.cache.acquire()
  r.cache.prepare(slot, int(frameSize))
  let decompSize = zstdDecompressShared(
    addr r.cache.data(slot)[0], csize_t(frameSize),
    frame, csize_t(compressedLen))

  if ZSTD_isError(decompSize) != 0:
    # The slot was never committed, so it stays free for the next acquire.
    return err("zstd decompress failed for chunk " & $chunkIdx & ": " &
      $ZSTD_getErrorName(decompSize))

  r.cache.prepare(slot, int(decompSize))
  # Account a distinct chunk inflation: we only reach here on a cache miss, so
  # each increment is a genuinely new inflation.
  r.chunkDecompressions += 1
  r.commitChunk(slot, chunkIdx)

proc commitChunk(r: var ExecStreamReader, slot: int,
    chunkIdx: int): Result[int, string] =
  ## Make the chunk just put in ``slot`` resident, nothing of it decoded yet.
  template m: untyped = r.cache.meta(slot)
  m.nextPos = r.payloadStart
  m.posRefusedAt = high(int)
  if r.legacy:
    # Legacy chunk: the first 4 bytes are a u32 LE event count, records follow.
    if r.cache.data(slot).len < 4:
      return err("decompressed chunk too small for event count header")
    var ec4: array[4, byte]
    for i in 0 ..< 4:
      ec4[i] = r.cache.data(slot)[i]
    m.eventCount = fromBytesLE(uint32, ec4)
    m.complete = m.eventCount == 0
  else:
    m.complete = r.cache.data(slot).len == 0

  r.cache.commit(slot, chunkIdx)
  r.heldChunk = chunkIdx + 1
  r.heldSlot = slot
  ok(slot)

proc decodeNext(r: var ExecStreamReader, slot: int,
    chunkIdx: int): Result[StepEvent, string] {.inline.} =
  ## Decode the next record of the chunk in ``slot`` — record ``known`` — and
  ## note where it starts and the cursor position after it. The chunk must not
  ## be ``complete``.
  let mp = addr r.cache.meta(slot)
  template m: untyped = mp[]
  let i = m.known
  var pos = m.nextPos
  # Decoded into the result, which is returned as it is.
  result = decodeStepEvent(r.cache.data(slot), pos, r.allowSourceReload)
  if result.isErr:
    return err("failed to decode event " & $i & " of chunk " & $chunkIdx &
      ": " & result.error)
  if i >= m.starts.len:
    let cap = max(256, 2 * m.starts.len)
    m.starts.setLenUninit(cap)
    m.positions.setLenUninit(cap)
  m.starts[i] = int32(m.nextPos)
  if m.posRefusedAt == high(int):
    if advanceCursor(result.get(), i, chunkIdx, m.cursor, m.anchored,
        m.posRefusal):
      m.positions[i] = m.cursor
    else:
      m.posRefusedAt = i
  m.nextPos = pos
  m.known = i + 1
  let atEnd =
    if r.legacy: m.known >= int(m.eventCount)
    else: pos >= r.cache.data(slot).len
  if atEnd:
    m.complete = true
    m.eventCount = uint32(m.known)

proc decodeThrough(r: var ExecStreamReader, slot: int, chunkIdx: int,
    i: int): Result[void, string] =
  ## Decode the chunk in ``slot`` until record ``i`` has been, or the chunk
  ## ends first.
  while r.cache.meta(slot).known <= i and not r.cache.meta(slot).complete:
    discard ? r.decodeNext(slot, chunkIdx)
  ok()

proc chunkRecordCount(r: var ExecStreamReader, slot: int,
    chunkIdx: int): Result[int, string] =
  ## How many records the chunk in ``slot`` holds, decoding all of them.
  ? r.decodeThrough(slot, chunkIdx, high(int) - 1)
  ok(r.cache.meta(slot).known)

proc readEvent*(r: var ExecStreamReader,
    eventIndex: uint64): Result[StepEvent, string] =
  ## Read a single event by its global index.
  ##
  ## Decodes its chunk only as far as the event: a walk in order decodes each
  ## record once, and a read of a record already passed decodes that record
  ## alone, from where it was noted to start.
  if eventIndex >= r.totalEventsVal:
    return err("event index out of range: " & $eventIndex & " >= " & $r.totalEventsVal)

  let chunkIdx = int(eventIndex div uint64(r.chunkSize))
  let eventInChunk = int(eventIndex mod uint64(r.chunkSize))

  let slot =
    if r.heldChunk == chunkIdx + 1: r.heldSlot
    else: ? r.chunkSlot(chunkIdx)
  let mp = addr r.cache.meta(slot)
  template m: untyped = mp[]
  if eventInChunk < m.known:
    var pos = int(m.starts[eventInChunk])
    return decodeStepEvent(r.cache.data(slot), pos, r.allowSourceReload)
  if eventInChunk > m.known:
    ? r.decodeThrough(slot, chunkIdx, eventInChunk - 1)
  if m.complete:
    return err("event " & $eventIndex & " past the end of chunk " & $chunkIdx)
  r.decodeNext(slot, chunkIdx)

proc readChunkEvents*(r: var ExecStreamReader,
    chunkIdx: int,
    output: var seq[StepEvent]): Result[uint64, string] =
  ## Decode every event of chunk ``chunkIdx`` into ``output`` (cleared
  ## first), returning the chunk's first global event index.
  ##
  ## This is the streaming counterpart to [readEvent]: it yields a whole
  ## chunk's events in order in a single pass.  Bulk readers (FFI bulk
  ## accessors, postprocess-style streamers) should walk chunks via this
  ## helper rather than looping ``readEvent``, which materialises one
  ## ``StepEvent`` per call.
  if chunkIdx < 0 or chunkIdx >= r.offsets.len:
    return err("chunk index out of range: " & $chunkIdx)

  let slot = ?r.chunkSlot(chunkIdx)

  let firstEventIdx = uint64(chunkIdx) * uint64(r.chunkSize)
  let eventCount = ? r.chunkRecordCount(slot, chunkIdx)
  output.setLen(0)
  if eventCount == 0:
    return ok(firstEventIdx)
  output = newSeqOfCap[StepEvent](eventCount)

  for i in 0 ..< eventCount:
    var pos = int(r.cache.meta(slot).starts[i])
    let evRes = decodeStepEvent(r.cache.data(slot), pos, r.allowSourceReload)
    if evRes.isErr:
      return err("failed to decode event " & $i & " while streaming chunk " &
        $chunkIdx & ": " & evRes.error)
    output.add(evRes.get())

  ok(firstEventIdx)

proc resolveChunkPositions*(events: openArray[StepEvent], chunkIdx: int,
    output: var seq[uint64]): Result[void, string] =
  ## Resolve every record of one decoded chunk to the cursor position after
  ## it, starting without a cursor (`trace-events.md` §"Encoding Rules",
  ## "Reading"). A `DeltaStep` or `DeltaColumn` before the chunk's first
  ## `AbsoluteStep` has nothing to be relative to and is refused, naming the
  ## chunk; it is never resolved against 0 or a cursor carried over from the
  ## previous chunk. A non-position record before the anchor reports 0.
  output.setLen(events.len)
  var cursor = 0'u64
  var anchored = false
  var why: string
  for i in 0 ..< events.len:
    if not advanceCursor(events[i], i, chunkIdx, cursor, anchored, why):
      return err(why)
    output[i] = cursor
  ok()

proc eventPosition*(r: var ExecStreamReader,
    eventIndex: uint64): Result[uint64, string] =
  ## The cursor position after event ``eventIndex``: `resolveChunkPositions`
  ## over its chunk, as far as the event. A position refused at or before the
  ## event (a delta before its chunk's first `AbsoluteStep`) is refused.
  if eventIndex >= r.totalEventsVal:
    return err("step " & $eventIndex & " is past the end of its exec chunk")
  let chunkIdx = int(eventIndex div uint64(r.chunkSize))
  let i = int(eventIndex mod uint64(r.chunkSize))
  let slot = ?r.chunkSlot(chunkIdx)
  template m: untyped = r.cache.meta(slot)
  if i >= m.known:
    ? r.decodeThrough(slot, chunkIdx, i)
    if i >= m.known:
      return err("step " & $eventIndex & " is past the end of its exec chunk")
  if i >= m.posRefusedAt:
    return err(m.posRefusal)
  ok(m.positions[i])

proc chunkPositions*(r: var ExecStreamReader, chunkIdx: int,
    output: var seq[uint64]): Result[void, string] =
  ## `resolveChunkPositions` over chunk `chunkIdx`'s records, decoded in one
  ## pass over the inflated chunk rather than collected into a sequence of
  ## events first.
  if chunkIdx < 0 or chunkIdx >= r.offsets.len:
    return err("chunk index out of range: " & $chunkIdx)
  let slot = ?r.chunkSlot(chunkIdx)
  let count = ? r.chunkRecordCount(slot, chunkIdx)
  template m: untyped = r.cache.meta(slot)
  if m.posRefusedAt < count:
    output.setLen(0)
    return err(m.posRefusal)
  output.setLenUninit(count)  # every entry is written below
  for i in 0 ..< count:
    output[i] = m.positions[i]
  ok()

proc chunkIndexFor*(r: ExecStreamReader, eventIndex: uint64): int =
  ## Map a global event index to its containing chunk index.  Useful
  ## for bulk readers that walk full chunks at a time and need to
  ## compute chunk boundaries without divmod-ing in callers.
  int(eventIndex div uint64(r.chunkSize))

proc chunkCount*(r: ExecStreamReader): int =
  ## Number of compressed chunks in this exec stream.
  r.offsets.len
