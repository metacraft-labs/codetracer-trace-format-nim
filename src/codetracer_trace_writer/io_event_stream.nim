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
## ## kind
##
## The on-disk ``kind`` byte is the recorder's exact ``EventLogKind`` ordinal
## (``trace-events.md`` §"EventLogKind (u8 enum)"), and it round-trips exactly:
## the writer stores the ordinal its caller gave and the reader reports it as
## that kind. Values 14-255 are unassigned and refused on both sides, by value.
## ``metadata`` is carried verbatim; ``data`` is the record's ``content``.
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
import ../codetracer_ctfs/streaming
import ../codetracer_ctfs/variable_record_table
import ../codetracer_ctfs/zstd_bindings
import ./varint
import ./record_chunk
import ../codetracer_trace_types

export codetracer_trace_types.EventLogKind

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
  IOEvent* = object
    kind*: EventLogKind
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
    spec: ChunkedRecords       ## events.dat + events.idx (SPEC mode)
    legacy: bool               ## true ⇒ legacy .off VRT layout; false ⇒ SPEC
    legacyTable: VariableRecordTableReader  ## only valid when legacy == true

proc eventLogKindName*(k: EventLogKind): string =
  ## The kind's name as `trace-events.md` §"EventLogKind (u8 enum)" spells it.
  case k
  of elkWrite: "Write"
  of elkWriteFile: "WriteFile"
  of elkWriteOther: "WriteOther"
  of elkRead: "Read"
  of elkReadFile: "ReadFile"
  of elkReadOther: "ReadOther"
  of elkReadDir: "ReadDir"
  of elkOpenDir: "OpenDir"
  of elkCloseDir: "CloseDir"
  of elkSocket: "Socket"
  of elkOpen: "Open"
  of elkError: "Error"
  of elkTraceLogEvent: "TraceLogEvent"
  of elkEvmEvent: "EvmEvent"

proc eventLogKindFromOrdinal*(ord: uint64): Result[EventLogKind, string] =
  ## The ``EventLogKind`` an on-disk or caller-supplied ordinal names; an
  ## unassigned value (14 and up) is refused by value, never mapped onto a
  ## kind (``trace-events.md`` §"EventLogKind (u8 enum)").
  if ord > uint64(high(EventLogKind)):
    return err("event kind " & $ord & " is not an assigned EventLogKind " &
      "(0-" & $ord(high(EventLogKind)) & ")")
  ok(EventLogKind(ord))

# ---------------------------------------------------------------------------
# Per-record encode/decode (SPEC: kind / step_id / metadata / content)
# ---------------------------------------------------------------------------

proc encodeIOEvent*(ev: IOEvent): seq[byte] {.raises: [].} =
  ## Encode an IOEvent into its SPEC wire format (no length prefix):
  ## ``u8 kind, varint step_id, varint metadata_len, metadata,
  ## varint content_len, content`` — byte-identical to the Rust
  ## ``IoEventRecord::encode``.
  var buf: seq[byte]
  buf.add(uint8(ord(ev.kind)))
  encodeVarint(ev.stepId, buf)
  encodeVarint(uint64(ev.metadata.len), buf)
  buf.add(ev.metadata)
  encodeVarint(uint64(ev.data.len), buf)
  buf.add(ev.data)
  buf

proc decodeIOEvent*(data: openArray[byte]): Result[IOEvent, string] {.raises: [].} =
  ## Decode an IOEvent from its SPEC wire format (the whole record, no length
  ## prefix).  ``kind`` is the stored ``EventLogKind`` ordinal; an unassigned
  ## value is refused.
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
    return err("IO event record's fields end at byte " & $pos & " of its " &
      $data.len & "-byte frame")

  let kind = ? eventLogKindFromOrdinal(kindByte)
  ok(IOEvent(
    kind: kind,
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
  # The legacy 4-value API ordinal: stdout, stderr, file op, error.
  let kind = case kindByte
    of 0: elkWrite
    of 1: elkWriteOther
    of 2: elkReadFile
    of 3: elkError
    else: return err("invalid legacy IO event kind: " & $kindByte)
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
    return err("failed to add events.dat: " & datRes.unsafeError)
  let idxRes = ctfs.addFile("events.idx")
  if idxRes.isErr:
    return err("failed to add events.idx: " & idxRes.unsafeError)

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
    return err("failed to write events.idx header: " & hdrRes.unsafeError)
  ctfs.syncEntry(writer.indexFile)

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

  let chunkStart = w.dataOffset

  # The chunk's bytes, then its offset in the companion index, then ONE
  # publish: every block written since the last seal (the chunk, its mapping,
  # the index, interning records), then the root entries that publish their
  # sizes (`ctfs-container.md` §6, "Durability", rule 2). A follow reader that
  # sees N index entries can assume chunks 0..N-1 are on disk.
  let datRes = ctfs.writeToFile(w.dataFile,
      compressed.toOpenArray(0, int(compressedSize) - 1))
  if datRes.isErr:
    return err("failed to write io event chunk: " & datRes.unsafeError)

  var offBytes: array[8, byte]
  let offLE = toBytesLE(chunkStart)
  for i in 0 ..< 8:
    offBytes[i] = offLE[i]
  let offRes = ctfs.writeToFile(w.indexFile, offBytes)
  if offRes.isErr:
    return err("failed to write events.idx offset: " & offRes.unsafeError)
  ctfs.syncEntry(w.indexFile)

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
      return err(tableRes.unsafeError)
    return ok(IOEventStreamReader(legacy: true, legacyTable: tableRes.get()))
  var datRes = readInternalFile(ctfsBytes, "events.dat", blockSize, maxEntries)
  if datRes.isErr:
    return err("failed to read events.dat: " & datRes.unsafeError)
  let idxRes = readInternalFile(ctfsBytes, "events.idx", blockSize, maxEntries)
  if idxRes.isErr:
    return err("failed to read events.idx: " & idxRes.unsafeError)
  ok(IOEventStreamReader(spec: ? openChunkedRecords(
    viewBytes(move datRes.get()), idxRes.get(), "events", "io event",
    isCompactContainer(ctfsBytes))))

proc initIOEventStreamReader*(image: ContainerImage,
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries,
    legacy: bool = false): Result[IOEventStreamReader, string] =
  ## As above, over a container image it shares: `events.dat` is read in
  ## place.
  if legacy:
    let tableRes = initVariableRecordTableReader(image, "events",
        blockSize, maxEntries)
    if tableRes.isErr:
      return err(tableRes.unsafeError)
    return ok(IOEventStreamReader(legacy: true, legacyTable: tableRes.get()))
  var datRes = viewMember(image, "events.dat", blockSize, maxEntries)
  if datRes.isErr:
    return err("failed to read events.dat: " & datRes.unsafeError)
  let idxRes = viewMember(image, "events.idx", blockSize, maxEntries)
  if idxRes.isErr:
    return err("failed to read events.idx: " & idxRes.unsafeError)
  ok(IOEventStreamReader(spec: ? openChunkedRecords(move datRes.get(),
    ? idxRes.get().contents(), "events", "io event",
    isCompactContainer(image.bytes))))

proc refresh*(r: var IOEventStreamReader, image: ContainerImage,
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): Result[void, string] =
  ## Extend the reader by the chunks the container in `image` has published
  ## since it was opened or last refreshed (`ctfs-container.md` §6). A legacy
  ## `.off` layout is never written live and is not followed.
  if r.legacy:
    return err("events.dat: a legacy event table is not followed")
  let dat = viewMember(image, "events.dat", blockSize, maxEntries)
  if dat.isErr:
    return err("failed to read events.dat: " & dat.unsafeError)
  let idx = viewMember(image, "events.idx", blockSize, maxEntries)
  if idx.isErr:
    return err("failed to read events.idx: " & idx.unsafeError)
  r.spec.refresh(dat.unsafeGet(), ? idx.unsafeGet().contents(), "events")

proc count*(r: IOEventStreamReader): uint64 =
  if r.legacy:
    r.legacyTable.count()
  else:
    r.spec.count

proc readEvent*(r: var IOEventStreamReader,
    index: uint64): Result[IOEvent, string] =
  ## Read the IO event record at the given index, decompressing only its chunk.
  if r.legacy:
    let dataRes = r.legacyTable.read(index)
    if dataRes.isErr:
      return err(dataRes.unsafeError)
    return decodeLegacyIOEvent(dataRes.get())

  if index >= r.spec.count:
    return err("io event index " & $index & " out of range (count " &
      $r.spec.count & ")")
  let within = ? r.spec.locate(index)
  let ev = decodeIOEvent(r.spec.record(within))
  if ev.isErr:
    return err("events.dat record " & $index & ": " & ev.unsafeError)
  ev
