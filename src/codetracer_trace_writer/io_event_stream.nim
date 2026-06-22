{.push raises: [].}

## IO event stream: stores I/O / log events (stdout, stderr, file ops, errors)
## for the event-log pane, cross-referenced to the execution stream by
## ``step_id``.
##
## # Wire format (M24a-3: SPEC-canonical chunked layout)
##
## The on-disk layout matches the canonical spec
## (``codetracer-trace-format-spec/seekable-zstd.md`` §"Chunk Format" +
## §"Companion Index Stream", and ``trace-events.md`` §"IO Event Stream
## (`events.dat`)" + §"IO Event Stream Records") and is BYTE-COMPATIBLE with the
## Rust ``codetracer_trace_writer::event_stream::encode_io_event_stream`` writer /
## ``codetracer_trace_reader::io_event_stream_reader::IoEventStreamReader``
## reader.  A bundle written by this Nim writer can therefore have its
## ``events.dat`` read directly by the canonical Rust ``IoEventStreamReader``
## (the property the db-backend event-log overlay relies on), and vice versa.
##
## Data layout (events.dat):
##   [zstd(chunk 0)][zstd(chunk 1)]...
##
## Each chunk groups up to ``chunkSize`` I/O event records.  A chunk's
## uncompressed payload is the concatenation of LENGTH-PREFIXED records:
##   [varint rec_len][rec_bytes] [varint rec_len][rec_bytes] ...
## The length prefix lets the reader index the ``N % chunk_size``-th record
## without re-deriving sizes (records are variable length).  This matches the
## Rust ``encode_io_event_stream`` chunk codec byte-for-byte.
##
## Per-record wire format (``trace-events.md`` §"IO Event Stream Records"):
##   kind     : u8 (EventLogKind ordinal)
##   step_id  : varint   (cross-reference to the execution stream)
##   metadata : varint len + bytes
##   content  : varint len + bytes
## This is byte-identical to the Rust ``IoEventRecord::encode``.  The records
## are NOT tagged (fixed structure).
##
## Index layout (events.idx):
##   [chunk_size: u32 LE]           # records per chunk
##   [offset_0:   u64 LE]           # byte offset of chunk 0 in events.dat
##   [offset_1:   u64 LE]           # ...
## There is NO ``total_events`` header or trailer; the record count is recovered
## by decoding the last chunk (all chunks but the last hold exactly
## ``chunk_size`` records).
##
## ## kind / metadata reconciliation
##
## The on-disk ``kind`` byte is the ``EventLogKind`` ordinal — the SAME u8 the
## legacy ``events.log`` carries (``trace-events.md`` §"EventLogKind").  The Nim
## convenience API surfaces a coarser ``IOEventKind`` (stdout/stderr/file_op/
## error); ``ioEventKindToOrdinal`` maps it to a canonical ``EventLogKind``
## ordinal on write and ``ordinalToIOEventKind`` maps any ``EventLogKind``
## ordinal back to the coarse kind on read.  The mapping round-trips for every
## ``IOEventKind`` (stdout↔Write, stderr↔TraceLogEvent, file_op↔ReadFile,
## error↔Error), so ct-print's ``io_kind`` output is byte-identical across the
## format change.  ``metadata`` is carried verbatim (previously dropped on the
## multi-stream path); ``data`` is the record's ``content``.
##
## # Backward compatibility (legacy Nim-v4 bundles)
##
## Bundles written by the pre-M24a-3 Nim writer used a ``VariableRecordTable``
## (``events.dat`` + ``events.off`` — an uncompressed variable-size record table
## with a u64 offset table), and a different per-record format
## (``u8 kind, varint stepId, varint data_len, data`` — NO metadata, and ``kind``
## was the 4-value ``IOEventKind`` ordinal, NOT the ``EventLogKind`` ordinal).
## Those bundles never set the ``meta.dat`` ``has_io_event_stream`` flag (bit
## 11), so the FFI reader distinguishes the two layouts by that flag: flag set ⇒
## SPEC chunked layout, flag clear ⇒ legacy ``.off`` VRT layout.
## ``initIOEventStreamReader`` accepts an explicit ``legacy`` parameter for this;
## standalone callers that only ever read freshly-written bundles get the SPEC
## layout by default.

import results
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ../codetracer_ctfs/variable_record_table
import ../codetracer_ctfs/zstd_bindings
import ./varint

const
  DefaultEventsChunkSize* = 64
    ## Records per chunk.  I/O event records are moderately sized (spec
    ## §"Stream Summary": 20-1000 bytes each) and accessed by paginated scan,
    ## so a modest chunk gives good page granularity without excessive
    ## per-page decompression.  Matches the Rust ``DEFAULT_EVENTS_CHUNK_SIZE``.
  EventsCompressionLevel = 3
    ## Zstd compression level.  Compatibility does not depend on the level
    ## (zstd decode is level-agnostic), only on the chunk codec.

type
  IOEventKind* = enum
    ioStdout = 0
    ioStderr = 1
    ioFileOp = 2
    ioError = 3

  IOEvent* = object
    kind*: IOEventKind
    stepId*: uint64
    metadata*: seq[byte]  ## event metadata bytes (verbatim; the legacy
                          ## ``RecordEvent.metadata`` string).  Empty by default.
    data*: seq[byte]      ## content bytes (the legacy ``RecordEvent.content``)

  IOEventStreamWriter* = object
    dataFile: CtfsInternalFile
    indexFile: CtfsInternalFile
    chunkSize: int
    buffer: seq[byte]          ## length-prefixed records for the current chunk
    recordCount: int           ## records in the current chunk buffer
    totalRecords: uint64
    dataOffset: uint64         ## running byte offset in events.dat

  IOEventStreamReader* = object
    data: seq[byte]            ## raw events.dat content (SPEC mode)
    chunkSize: uint32
    offsets: seq[uint64]       ## chunk byte offsets from events.idx (SPEC mode)
    totalRecordsVal: uint64
    legacy: bool               ## true ⇒ legacy .off VRT layout; false ⇒ SPEC
    legacyTable: VariableRecordTableReader  ## only valid when legacy == true
    # Cache for last decompressed SPEC chunk: the decoded per-record byte slices.
    cachedChunkIdx: int        ## -1 means no cache
    cachedRecords: seq[seq[byte]]

# ---------------------------------------------------------------------------
# kind ↔ EventLogKind-ordinal reconciliation
# ---------------------------------------------------------------------------

proc ioEventKindToOrdinal*(kind: IOEventKind): uint8 =
  ## Map the coarse ``IOEventKind`` to a canonical ``EventLogKind`` ordinal for
  ## the on-disk ``kind`` byte (``trace-events.md`` §"EventLogKind", matching the
  ## Rust ``EventLogKind as u8``).  The chosen representatives round-trip through
  ## ``ordinalToIOEventKind`` so ct-print's ``io_kind`` output is unchanged:
  ##   ioStdout → Write (0), ioStderr → TraceLogEvent (12),
  ##   ioFileOp → ReadFile (4), ioError → Error (11).
  case kind
  of ioStdout: 0'u8   # elkWrite
  of ioStderr: 12'u8  # elkTraceLogEvent
  of ioFileOp: 4'u8   # elkReadFile
  of ioError: 11'u8   # elkError

proc ordinalToIOEventKind*(ord: uint8): IOEventKind =
  ## Map any ``EventLogKind`` ordinal back to the coarse ``IOEventKind`` the Nim
  ## API surfaces.  Mirrors the FFI's ``toIOEventKind`` collapse so every
  ## ``EventLogKind`` value (0..13, and any future value) resolves to a stable
  ## coarse kind; unknown ordinals default to ``ioStdout``.
  case ord
  of 0, 1, 2:  ioStdout            # Write / WriteFile / WriteOther
  of 3, 4, 5, 6, 7, 8, 9, 10: ioFileOp
                                   # Read* / *Dir / Socket / Open
  of 11: ioError                   # Error
  of 12, 13: ioStderr              # TraceLogEvent / EvmEvent
  else: ioStdout                   # forward-compatible default

# ---------------------------------------------------------------------------
# Per-record encode/decode (SPEC: kind / step_id / metadata / content)
# ---------------------------------------------------------------------------

proc encodeIOEvent*(ev: IOEvent): seq[byte] {.raises: [].} =
  ## Encode an IOEvent into its SPEC wire format (no length prefix):
  ## ``u8 kind, varint step_id, varint metadata_len, metadata,
  ## varint content_len, content`` — byte-identical to the Rust
  ## ``IoEventRecord::encode``.
  var buf: seq[byte]
  buf.add(ioEventKindToOrdinal(ev.kind))
  encodeVarint(ev.stepId, buf)
  encodeVarint(uint64(ev.metadata.len), buf)
  buf.add(ev.metadata)
  encodeVarint(uint64(ev.data.len), buf)
  buf.add(ev.data)
  buf

proc decodeIOEvent*(data: openArray[byte]): Result[IOEvent, string] {.raises: [].} =
  ## Decode an IOEvent from its SPEC wire format (the whole record, no length
  ## prefix).  ``kind`` is reconstructed from the stored ``EventLogKind`` ordinal
  ## via ``ordinalToIOEventKind``.
  if data.len < 1:
    return err("IO event record too short (no kind byte)")

  var pos = 0
  let kindByte = data[pos]
  pos += 1

  let stepId = ?decodeVarint(data, pos)

  let metaLen = int(?decodeVarint(data, pos))
  if pos + metaLen > data.len:
    return err("truncated IO event metadata")
  var meta = newSeq[byte](metaLen)
  for i in 0 ..< metaLen:
    meta[i] = data[pos + i]
  pos += metaLen

  let dataLen = int(?decodeVarint(data, pos))
  if pos + dataLen > data.len:
    return err("truncated IO event content")
  var evData = newSeq[byte](dataLen)
  for i in 0 ..< dataLen:
    evData[i] = data[pos + i]
  pos += dataLen

  if pos != data.len:
    return err("trailing bytes in IO event record")

  ok(IOEvent(
    kind: ordinalToIOEventKind(kindByte),
    stepId: stepId,
    metadata: meta,
    data: evData))

# ---------------------------------------------------------------------------
# Legacy per-record decode (pre-M24a-3 .off VRT framing)
# ---------------------------------------------------------------------------

proc decodeLegacyIOEvent(data: openArray[byte]): Result[IOEvent, string] {.raises: [].} =
  ## Decode a legacy ``.off`` VRT IO event record (pre-M24a-3 framing):
  ## ``u8 kind, varint stepId, varint data_len, data`` — the ``kind`` byte was
  ## the 4-value ``IOEventKind`` ordinal (NOT an ``EventLogKind`` ordinal) and
  ## there was no metadata field.
  if data.len < 1:
    return err("legacy IO event data too short")
  var pos = 0
  let kindByte = data[pos]
  pos += 1
  if kindByte > byte(high(IOEventKind)):
    return err("invalid legacy IO event kind: " & $kindByte)
  let kind = IOEventKind(kindByte)
  let stepId = ?decodeVarint(data, pos)
  let dataLen = int(?decodeVarint(data, pos))
  if pos + dataLen > data.len:
    return err("truncated legacy IO event data")
  var evData = newSeq[byte](dataLen)
  for i in 0 ..< dataLen:
    evData[i] = data[pos + i]
  ok(IOEvent(kind: kind, stepId: stepId, metadata: @[], data: evData))

# ---------------------------------------------------------------------------
# Writer (SPEC chunked layout)
# ---------------------------------------------------------------------------

proc initIOEventStreamWriter*(ctfs: var Ctfs,
    chunkSize: int = DefaultEventsChunkSize): Result[IOEventStreamWriter, string] =
  ## Create the SPEC-canonical ``events.dat`` / ``events.idx`` stream.
  if chunkSize <= 0:
    return err("events chunkSize must be positive")

  let datRes = ctfs.addFile("events.dat")
  if datRes.isErr:
    return err("failed to add events.dat: " & datRes.error)
  let idxRes = ctfs.addFile("events.idx")
  if idxRes.isErr:
    return err("failed to add events.idx: " & idxRes.error)

  var writer = IOEventStreamWriter(
    dataFile: datRes.get(),
    indexFile: idxRes.get(),
    chunkSize: chunkSize,
    buffer: @[],
    recordCount: 0,
    totalRecords: 0,
    dataOffset: 0,
  )

  # Index header: just the u32 chunk_size (SPEC layout — no total_events).
  var hdr: array[4, byte]
  let csLE = toBytesLE(uint32(chunkSize))
  for i in 0 ..< 4:
    hdr[i] = csLE[i]
  let hdrRes = ctfs.writeToFile(writer.indexFile, hdr)
  if hdrRes.isErr:
    return err("failed to write events.idx header: " & hdrRes.error)

  ok(writer)

proc flushChunk(ctfs: var Ctfs, w: var IOEventStreamWriter): Result[void, string] =
  ## Compress the buffered records into one chunk, append to events.dat, and
  ## record the chunk's byte offset in events.idx.
  if w.recordCount == 0:
    return ok()

  let bound = ZSTD_compressBound(csize_t(w.buffer.len))
  var compressed = newSeq[byte](int(bound))
  let compressedSize = ZSTD_compress(
    addr compressed[0], csize_t(bound),
    addr w.buffer[0], csize_t(w.buffer.len),
    cint(EventsCompressionLevel))
  if ZSTD_isError(compressedSize) != 0:
    return err("zstd compress failed for io event chunk: " &
      $ZSTD_getErrorName(compressedSize))

  # Record this chunk's byte offset (the running pre-chunk offset) in
  # events.idx, then append the compressed chunk body to events.dat.
  var offBytes: array[8, byte]
  let offLE = toBytesLE(w.dataOffset)
  for i in 0 ..< 8:
    offBytes[i] = offLE[i]
  let offRes = ctfs.writeToFile(w.indexFile, offBytes)
  if offRes.isErr:
    return err("failed to write events.idx offset: " & offRes.error)

  let datRes = ctfs.writeToFile(w.dataFile,
      compressed.toOpenArray(0, int(compressedSize) - 1))
  if datRes.isErr:
    return err("failed to write io event chunk: " & datRes.error)

  w.dataOffset += uint64(compressedSize)
  w.buffer.setLen(0)
  w.recordCount = 0
  ok()

proc writeEvent*(ctfs: var Ctfs, w: var IOEventStreamWriter,
    ev: IOEvent): Result[void, string] =
  ## Write an IO event.  Events are indexed sequentially in write order.
  let rec = encodeIOEvent(ev)
  # Length-prefix the record within the chunk so the reader can index it.
  encodeVarint(uint64(rec.len), w.buffer)
  w.buffer.add(rec)
  inc w.recordCount
  inc w.totalRecords

  if w.recordCount >= w.chunkSize:
    return flushChunk(ctfs, w)
  ok()

proc flush*(ctfs: var Ctfs, w: var IOEventStreamWriter): Result[void, string] =
  ## Flush any remaining buffered records as a partial final chunk.  Must be
  ## called before serializing the CTFS.  The SPEC ``events.idx`` carries no
  ## ``total_events`` trailer — the count is recoverable from the chunk offsets
  ## plus the last chunk's decoded record count.
  flushChunk(ctfs, w)

proc count*(w: IOEventStreamWriter): uint64 = w.totalRecords

# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

proc decompressChunkRecords(compressed: openArray[byte]):
    Result[seq[seq[byte]], string] =
  ## Decompress one SPEC chunk and split it into its length-prefixed records.
  if compressed.len == 0:
    return ok(newSeq[seq[byte]]())
  let frameSize = ZSTD_getFrameContentSize(
    unsafeAddr compressed[0], csize_t(compressed.len))
  if frameSize == ZSTD_CONTENTSIZE_UNKNOWN or frameSize == ZSTD_CONTENTSIZE_ERROR:
    return err("cannot determine decompressed size for io event chunk")
  var raw = newSeq[byte](int(frameSize))
  if frameSize > 0:
    let decompSize = ZSTD_decompress(
      addr raw[0], csize_t(frameSize),
      unsafeAddr compressed[0], csize_t(compressed.len))
    if ZSTD_isError(decompSize) != 0:
      return err("zstd decompress failed for io event chunk: " &
        $ZSTD_getErrorName(decompSize))
    raw.setLen(int(decompSize))

  var records: seq[seq[byte]] = @[]
  var pos = 0
  while pos < raw.len:
    let recLen = int(?decodeVarint(raw, pos))
    if pos + recLen > raw.len:
      return err("io event record length extends past chunk")
    var rec = newSeq[byte](recLen)
    for j in 0 ..< recLen:
      rec[j] = raw[pos + j]
    pos += recLen
    records.add(rec)
  ok(records)

proc initIOEventStreamReader*(ctfsBytes: openArray[byte],
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries,
    legacy: bool = false): Result[IOEventStreamReader, string] =
  ## Initialize a reader from raw CTFS container bytes.
  ##
  ## ``legacy`` selects the on-disk framing (see module docs):
  ##   * ``false`` (default) — SPEC chunked layout (``events.dat`` chunked Zstd +
  ##     ``events.idx`` = ``[chunk_size: u32][offset: u64]...``).  Byte-compatible
  ##     with the Rust ``IoEventStreamReader``.
  ##   * ``true`` — legacy Nim-v4 ``.off`` VariableRecordTable layout
  ##     (``events.dat`` + ``events.off``, per-record
  ##     ``u8 kind, varint stepId, varint data_len, data``).
  ##
  ## The FFI reader passes ``legacy = not meta.hasIoEventStream``: pre-M24a-3
  ## bundles never set the ``has_io_event_stream`` flag, so a clear flag selects
  ## the legacy reader and a set flag the SPEC reader.
  if legacy:
    let tableRes = initVariableRecordTableReader(ctfsBytes, "events",
        blockSize, maxEntries)
    if tableRes.isErr:
      return err(tableRes.error)
    return ok(IOEventStreamReader(
      legacy: true,
      legacyTable: tableRes.get(),
      cachedChunkIdx: -1))

  let datRes = readInternalFile(ctfsBytes, "events.dat", blockSize, maxEntries)
  if datRes.isErr:
    return err("failed to read events.dat: " & datRes.error)
  let datData = datRes.get()

  let idxRes = readInternalFile(ctfsBytes, "events.idx", blockSize, maxEntries)
  if idxRes.isErr:
    return err("failed to read events.idx: " & idxRes.error)
  let idxData = idxRes.get()

  if idxData.len < 4:
    return err("events.idx too small for chunk_size header")
  var cs4: array[4, byte]
  for i in 0 ..< 4:
    cs4[i] = idxData[i]
  let chunkSize = fromBytesLE(uint32, cs4)
  if chunkSize == 0:
    return err("chunkSize in events.idx is 0")

  let offsetRegionBytes = idxData.len - 4
  if offsetRegionBytes mod 8 != 0:
    return err("events.idx has trailing bytes in offset region")
  let numChunks = offsetRegionBytes div 8
  var offsets = newSeq[uint64](numChunks)
  for i in 0 ..< numChunks:
    var o8: array[8, byte]
    for j in 0 ..< 8:
      o8[j] = idxData[4 + i * 8 + j]
    offsets[i] = fromBytesLE(uint64, o8)

  # Recover total record count: all chunks but the last hold exactly chunk_size
  # records; the last holds whatever decodes out of it (Rust parity).
  var totalRecords: uint64 = 0
  if numChunks > 0:
    let lastChunk = numChunks - 1
    let startOff = int(offsets[lastChunk])
    let endOff = datData.len
    if startOff > endOff:
      return err("last io event chunk offset past end of events.dat")
    let lastRecs = ?decompressChunkRecords(
      datData.toOpenArray(startOff, endOff - 1))
    totalRecords = uint64(lastChunk) * uint64(chunkSize) + uint64(lastRecs.len)

  ok(IOEventStreamReader(
    legacy: false,
    data: datData,
    chunkSize: chunkSize,
    offsets: offsets,
    totalRecordsVal: totalRecords,
    cachedChunkIdx: -1,
    cachedRecords: @[]))

proc count*(r: IOEventStreamReader): uint64 =
  if r.legacy:
    r.legacyTable.count()
  else:
    r.totalRecordsVal

proc readEvent*(r: var IOEventStreamReader,
    index: uint64): Result[IOEvent, string] =
  ## Read the IO event record at the given index, decompressing only its chunk.
  if r.legacy:
    let dataRes = r.legacyTable.read(index)
    if dataRes.isErr:
      return err(dataRes.error)
    return decodeLegacyIOEvent(dataRes.get())

  if index >= r.totalRecordsVal:
    return err("io event index " & $index & " out of range (count " &
      $r.totalRecordsVal & ")")
  let chunkNumber = int(index div uint64(r.chunkSize))
  let within = int(index mod uint64(r.chunkSize))

  if r.cachedChunkIdx != chunkNumber:
    let startOff = int(r.offsets[chunkNumber])
    let endOff =
      if chunkNumber + 1 < r.offsets.len: int(r.offsets[chunkNumber + 1])
      else: r.data.len
    if startOff > endOff or endOff > r.data.len:
      return err("io event chunk offsets out of range")
    let recs = ?decompressChunkRecords(
      r.data.toOpenArray(startOff, endOff - 1))
    r.cachedRecords = recs
    r.cachedChunkIdx = chunkNumber

  if within >= r.cachedRecords.len:
    return err("io event record " & $within & " missing in chunk " &
      $chunkNumber)
  decodeIOEvent(r.cachedRecords[within])
