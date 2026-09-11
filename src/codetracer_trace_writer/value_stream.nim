{.push raises: [].}

## Value stream: stores variable values per step, parallel-indexed with the
## execution stream.  Record N contains all variables visible at step N.
##
## # Wire format (M24a-2: SPEC-canonical chunked layout)
##
## The on-disk layout matches the canonical spec
## (``codetracer-trace-format-spec/seekable-zstd.md`` §"Chunk Format" +
## §"Companion Index Stream", and ``trace-events.md`` §"Value Stream") and is
## BYTE-COMPATIBLE with the Rust
## ``codetracer_trace_writer::value_stream::encode_value_stream`` writer /
## ``codetracer_trace_reader::value_stream_reader::ValueStreamReader`` reader.
## A bundle written by this Nim writer can therefore have its ``values.dat``
## read directly by the canonical Rust ``ValueStreamReader`` (the property the
## db-backend seekable overlay relies on), and vice versa.
##
## Data layout (values.dat):
##   [zstd(chunk 0)][zstd(chunk 1)]...
##
## Each chunk groups up to ``chunkSize`` value records.  A chunk's uncompressed
## payload is the concatenation of LENGTH-PREFIXED records:
##   [varint rec_len][rec_bytes] [varint rec_len][rec_bytes] ...
## The length prefix lets the reader index the ``N % chunk_size``-th record
## without re-deriving sizes (records are variable length).  This matches the
## Rust ``encode_value_stream`` chunk codec byte-for-byte.
##
## Per-record wire format (one record per step):
##   A record is the concatenation of zero-or-more tagged value-stream events
##   (``trace-events.md`` §"Value Stream Events").  The Nim production writer
##   only emits the tag-0 ``StepValues`` event (or NOTHING for a value-less
##   step — an empty record):
##     Tag 0  StepValues : u8 tag(0x00), varint count,
##                         count × (varint name_id, varint value_len,
##                                  value bytes (CBOR ValueRecord))
##   A value-less step is an EMPTY record (zero bytes) — its length prefix is a
##   single ``0x00``.  This is the spec's "empty record for value-less steps".
##
## Index layout (values.idx):
##   [chunk_size: u32 LE]           # records per chunk
##   [offset_0:   u64 LE]           # byte offset of chunk 0 in values.dat
##   [offset_1:   u64 LE]           # ...
## There is NO ``total_events`` header or trailer; the record count is recovered
## by decoding the last chunk (all chunks but the last hold exactly
## ``chunk_size`` records).
##
## ## type_id reconstruction
##
## The Rust ``StepValues`` pair is ``(name_id, CBOR value)`` — there is NO
## separate ``type_id`` field, because the type id is already embedded inside
## the CBOR ``ValueRecord``.  The Nim ``VariableValue`` keeps a convenience
## ``typeId`` field; on read it is reconstructed from the CBOR value's
## top-level ``type_id`` (``topLevelTypeId``).  Recorders pass a ``typeId`` that
## equals the value's own top-level type id, so the round-trip is lossless for
## the production path; the redundant field is simply dropped from the wire.
##
## # Backward compatibility (legacy Nim-v4 bundles)
##
## Bundles written by the pre-M24a-2 Nim writer used a ``VariableRecordTable``
## (``values.dat`` + ``values.off`` — an uncompressed variable-size record table
## with a u64 offset table), and a different per-record format (``varint count``,
## then ``varint varnameId, varint typeId, varint dataLen, data`` per value).
## Those bundles never set the ``meta.dat`` ``has_value_stream`` flag (bit 10),
## so the FFI reader distinguishes the two layouts by that flag: flag set ⇒ SPEC
## chunked layout, flag clear ⇒ legacy ``.off`` VRT layout.
## ``initValueStreamReader`` accepts an explicit ``legacy`` parameter for this;
## standalone callers that only ever read freshly-written bundles get the SPEC
## layout by default.

import results
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ../codetracer_ctfs/streaming
import ../codetracer_ctfs/variable_record_table
import ../codetracer_ctfs/zstd_bindings
import ../codetracer_trace_types
import ./cbor
import ./varint

const
  DefaultValuesChunkSize* = 256
    ## Records per chunk.  Value records are large (spec §"Stream Summary":
    ## 50-500 bytes each), so a smaller chunk than ``steps.dat`` gives finer
    ## seek granularity.  Matches the Rust ``DEFAULT_VALUES_CHUNK_SIZE``.
  ValuesCompressionLevel = 3
    ## Zstd compression level.  Compatibility does not depend on the level
    ## (zstd decode is level-agnostic), only on the chunk codec.

  TagStepValues = 0'u8
    ## Value-stream event tag 0 (``trace-events.md`` §"Value Stream Events").

  TagAssignment* = 9'u8
    ## Value-stream event tag 9 (``trace-events.md`` §"Value Stream Events"):
    ## ``Assignment {to: varint, pass_by: u8, from: length-prefixed CBOR RValue}``.
    ##
    ## Emitted by ``trace_writer_register_assignment`` and appended AFTER the
    ## tag-0 ``StepValues`` event of the step the assignment belongs to.  The
    ## encoding is byte-identical to the canonical Rust
    ## ``ValueStreamEvent::Assignment`` in
    ## ``codetracer-trace-format/codetracer_trace_writer/src/value_stream.rs``
    ## (``TAG_ASSIGNMENT``), which is what lets the Rust
    ## ``ValueRecordEntry::decode`` read back what this writer produced.

type
  VariableValue* = object
    varnameId*: uint64
    typeId*: uint64
    data*: seq[byte]  ## CBOR-encoded value bytes

  AssignmentEventEntry* = object
    ## One decoded tag-9 ``Assignment`` value-stream event.
    varnameId*: uint64   ## interned varname id of the assignment TARGET
    passBy*: uint8       ## 0 = PassBy::Value, 1 = PassBy::Reference
    rvalueCbor*: seq[byte]
      ## serde-CBOR ``RValue`` describing the right-hand side, stored verbatim

  ValueStreamWriter* = object
    dataFile: CtfsInternalFile
    indexFile: CtfsInternalFile
    chunkSize: int
    buffer: seq[byte]          ## length-prefixed records for the current chunk
    recordCount: int           ## records in the current chunk buffer
    totalRecords: uint64
    dataOffset: uint64         ## running byte offset in values.dat

  ValueStreamReader* = object
    data: seq[byte]            ## raw values.dat content (SPEC mode)
    chunkSize: uint32
    offsets: seq[uint64]       ## chunk byte offsets from values.idx (SPEC mode)
    totalRecordsVal: uint64
    legacy: bool               ## true ⇒ legacy .off VRT layout; false ⇒ SPEC
    legacyTable: VariableRecordTableReader  ## only valid when legacy == true
    # Cache for last decompressed SPEC chunk: the decoded per-record byte slices.
    cachedChunkIdx: int        ## -1 means no cache
    cachedRecords: seq[seq[byte]]

# ---------------------------------------------------------------------------
# type_id reconstruction helper
# ---------------------------------------------------------------------------

proc topLevelTypeId*(v: ValueRecord): uint64 =
  ## Return the top-level ``type_id`` carried by a decoded ``ValueRecord``.
  ## Kinds with no type id (``vrkCell``, ``vrkValueRef``) report 0 — they never
  ## occur as a top-level production step value.  Used to reconstruct the
  ## convenience ``VariableValue.typeId`` field from the CBOR payload.
  case v.kind
  of vrkInt: uint64(v.intTypeId)
  of vrkFloat: uint64(v.floatTypeId)
  of vrkBool: uint64(v.boolTypeId)
  of vrkString: uint64(v.strTypeId)
  of vrkSequence: uint64(v.seqTypeId)
  of vrkTuple: uint64(v.tupleTypeId)
  of vrkStruct: uint64(v.structTypeId)
  of vrkVariant: uint64(v.variantTypeId)
  of vrkReference: uint64(v.refTypeId)
  of vrkRaw: uint64(v.rawTypeId)
  of vrkError: uint64(v.errorTypeId)
  of vrkNone: uint64(v.noneTypeId)
  of vrkBigInt: uint64(v.bigIntTypeId)
  of vrkChar: uint64(v.charTypeId)
  of vrkSet: uint64(v.setTypeId)
  of vrkEnum: uint64(v.enumTypeId)
  of vrkCell, vrkValueRef: 0'u64

proc decodeCborTopLevelTypeId(data: openArray[byte]): uint64 =
  ## Decode the CBOR ``ValueRecord`` in ``data`` and return its top-level
  ## ``type_id``.  Returns 0 on any decode failure (the data is still surfaced
  ## verbatim; only the convenience type id is unavailable).
  if data.len == 0:
    return 0
  var dec = CborDecoder.init(data)
  let recRes = dec.decodeCborValueRecord()
  if recRes.isErr:
    return 0
  topLevelTypeId(recRes.get())

# ---------------------------------------------------------------------------
# Per-record encode/decode (SPEC tag-0 StepValues, parallel-indexed by step)
# ---------------------------------------------------------------------------

proc encodeAssignmentEvent*(varnameId: uint64, passBy: uint8,
    rvalueCbor: openArray[byte], outBuf: var seq[byte]) =
  ## Encode one tag-9 ``Assignment`` value-stream event into ``outBuf``:
  ## ``u8 0x09, varint to, u8 pass_by, varint from_len, from_bytes`` — the
  ## byte-for-byte layout the Rust ``ValueStreamEvent::Assignment`` encoder
  ## produces and its decoder expects.  ``rvalueCbor`` is the serde-CBOR
  ## encoding of the ``RValue`` (adjacently tagged, see ``cbor.nim``'s
  ## ``encodeCborRValue``); this writer stores it verbatim so no re-encoding
  ## can drift the two implementations apart.
  outBuf.add(TagAssignment)
  encodeVarint(varnameId, outBuf)
  outBuf.add(passBy)
  encodeVarint(uint64(rvalueCbor.len), outBuf)
  for b in rvalueCbor:
    outBuf.add(b)

proc encodeRecord(values: openArray[VariableValue],
    extraEvents: openArray[byte], outBuf: var seq[byte]) =
  ## Encode one step's variable values as a SPEC value record.  A step with no
  ## values AND no extra events encodes to ZERO bytes (an empty record),
  ## matching the spec's "empty record for value-less steps".  Otherwise emit a
  ## single tag-0 StepValues event: ``u8 0x00, varint count, count × (varint
  ## name_id, varint len, data)`` — byte-identical to the Rust
  ## ``ValueStreamEvent::StepValues`` encoding — followed by ``extraEvents``
  ## verbatim.
  ##
  ## ``extraEvents`` is a already-encoded concatenation of further tagged
  ## value-stream events (today only tag-9 ``Assignment``).  A record is
  ## defined by the spec as "the concatenation of zero-or-more tagged
  ## value-stream events", so appending them after the StepValues event is the
  ## canonical placement, and the reader walks events until the record's byte
  ## length is exhausted.
  if values.len > 0:
    outBuf.add(TagStepValues)
    encodeVarint(uint64(values.len), outBuf)
    for v in values:
      encodeVarint(v.varnameId, outBuf)
      encodeVarint(uint64(v.data.len), outBuf)
      outBuf.add(v.data)
  if extraEvents.len > 0:
    for b in extraEvents:
      outBuf.add(b)

proc decodeRecord(data: openArray[byte]): Result[seq[VariableValue], string] =
  ## Decode one SPEC value record (a concatenation of tagged events) back into
  ## the Nim ``VariableValue`` list.  The production writer only emits tag-0
  ## ``StepValues``; other tags are walked-and-skipped so the parallel index
  ## stays aligned even if a future record carries additional event kinds.
  if data.len == 0:
    return ok(newSeq[VariableValue]())
  var pos = 0
  var values: seq[VariableValue] = @[]
  while pos < data.len:
    let tag = data[pos]
    inc pos
    case tag
    of TagStepValues:
      let count = int(?decodeVarint(data, pos))
      for _ in 0 ..< count:
        let vnId = ?decodeVarint(data, pos)
        let dLen = int(?decodeVarint(data, pos))
        if pos + dLen > data.len:
          return err("truncated value data in StepValues record")
        var d = newSeq[byte](dLen)
        for j in 0 ..< dLen:
          d[j] = data[pos + j]
        pos += dLen
        values.add(VariableValue(
          varnameId: vnId,
          typeId: decodeCborTopLevelTypeId(d),
          data: d))
    of TagAssignment:
      # Assignment provenance rides in the same record as the step's values
      # (spec §"Value Stream Events" tag 9).  ``readStepValues`` answers
      # "which variables are visible at step N", so the assignment is walked
      # over rather than reported here — but it MUST be walked accurately,
      # because the remaining events of the record follow it.  Use
      # ``decodeRecordAssignments`` to read them.
      discard ?decodeVarint(data, pos)           # to
      if pos >= data.len:
        return err("truncated pass_by in Assignment value-stream event")
      inc pos                                    # pass_by
      let fromLen = int(?decodeVarint(data, pos))
      if pos + fromLen > data.len:
        return err("truncated RValue payload in Assignment value-stream event")
      pos += fromLen
    else:
      # Deliberately an ERROR, not a skip: a value-stream event is not
      # self-delimiting, so an unknown tag cannot be walked over without
      # guessing its length, and a guess would mis-frame every event after it.
      # The consequence to know about: a reader built BEFORE a tag existed
      # refuses every record carrying one. That is how a stale `ct-print`
      # reports "(none)" for a step whose values are present on disk — rebuild
      # it (`just build` in codetracer-trace-format-nim) rather than reaching
      # for the writer.
      return err("unsupported value-stream event tag " & $tag &
        " in Nim value record (this reader predates the tag; rebuild ct-print " &
        "from codetracer-trace-format-nim)")
  ok(values)

proc decodeRecordAssignments*(data: openArray[byte]):
    Result[seq[AssignmentEventEntry], string] =
  ## Decode the tag-9 ``Assignment`` events of one SPEC value record, in
  ## stream order.  Tag-0 ``StepValues`` events are walked over.  This is the
  ## read-back counterpart of ``encodeAssignmentEvent`` and exists so the Nim
  ## side can verify what it wrote without going through the Rust reader.
  var pos = 0
  var found: seq[AssignmentEventEntry] = @[]
  while pos < data.len:
    let tag = data[pos]
    inc pos
    case tag
    of TagStepValues:
      let count = int(?decodeVarint(data, pos))
      for _ in 0 ..< count:
        discard ?decodeVarint(data, pos)         # name_id
        let dLen = int(?decodeVarint(data, pos))
        if pos + dLen > data.len:
          return err("truncated value data in StepValues record")
        pos += dLen
    of TagAssignment:
      let vnId = ?decodeVarint(data, pos)
      if pos >= data.len:
        return err("truncated pass_by in Assignment value-stream event")
      let passBy = data[pos]
      inc pos
      let fromLen = int(?decodeVarint(data, pos))
      if pos + fromLen > data.len:
        return err("truncated RValue payload in Assignment value-stream event")
      var blob = newSeq[byte](fromLen)
      for j in 0 ..< fromLen:
        blob[j] = data[pos + j]
      pos += fromLen
      found.add(AssignmentEventEntry(
        varnameId: vnId, passBy: passBy, rvalueCbor: blob))
    else:
      return err("unsupported value-stream event tag " & $tag &
        " in Nim value record")
  ok(found)

# ---------------------------------------------------------------------------
# Writer (SPEC chunked layout)
# ---------------------------------------------------------------------------

proc initValueStreamWriter*(ctfs: var Ctfs,
    chunkSize: int = DefaultValuesChunkSize): Result[ValueStreamWriter, string] =
  ## Create the SPEC-canonical ``values.dat`` / ``values.idx`` stream.
  if chunkSize <= 0:
    return err("values chunkSize must be positive")

  let datRes = ctfs.addFile("values.dat")
  if datRes.isErr:
    return err("failed to add values.dat: " & datRes.error)
  let idxRes = ctfs.addFile("values.idx")
  if idxRes.isErr:
    return err("failed to add values.idx: " & idxRes.error)

  var writer = ValueStreamWriter(
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
    return err("failed to write values.idx header: " & hdrRes.error)
  ctfs.syncEntry(writer.indexFile)

  ok(writer)

proc flushChunk(ctfs: var Ctfs, w: var ValueStreamWriter): Result[void, string] =
  ## Compress the buffered records into one chunk, append to values.dat, and
  ## record the chunk's byte offset in values.idx.
  if w.recordCount == 0:
    return ok()

  let bound = ZSTD_compressBound(csize_t(w.buffer.len))
  var compressed = newSeq[byte](int(bound))
  let compressedSize = ZSTD_compress(
    addr compressed[0], csize_t(bound),
    addr w.buffer[0], csize_t(w.buffer.len),
    cint(ValuesCompressionLevel))
  if ZSTD_isError(compressedSize) != 0:
    return err("zstd compress failed for value chunk: " &
      $ZSTD_getErrorName(compressedSize))

  let chunkStart = w.dataOffset

  # DATA-FIRST-THEN-INDEX ordering (matches span_stream.flushChunk): append the
  # compressed chunk body to values.dat and sync its size FIRST, then append the
  # chunk's byte offset to values.idx and sync.  A concurrent follow reader that
  # observes N index entries can then always assume chunks 0..N-1 are fully on
  # disk; the reverse order could publish an offset for bytes not yet written,
  # yielding a transient short/zero decode.
  let datRes = ctfs.writeToFile(w.dataFile,
      compressed.toOpenArray(0, int(compressedSize) - 1))
  if datRes.isErr:
    return err("failed to write value chunk: " & datRes.error)
  ctfs.syncEntry(w.dataFile)

  var offBytes: array[8, byte]
  let offLE = toBytesLE(chunkStart)
  for i in 0 ..< 8:
    offBytes[i] = offLE[i]
  let offRes = ctfs.writeToFile(w.indexFile, offBytes)
  if offRes.isErr:
    return err("failed to write values.idx offset: " & offRes.error)
  ctfs.syncEntry(w.indexFile)

  w.dataOffset += uint64(compressedSize)
  w.buffer.setLen(0)
  w.recordCount = 0
  ok()

proc writeStepValues*(ctfs: var Ctfs, w: var ValueStreamWriter,
    values: openArray[VariableValue],
    extraEvents: openArray[byte] = []): Result[void, string] =
  ## Write all variable values for one step.  Call exactly once per step event,
  ## in step order — this preserves the parallel-index invariant (record N ↔
  ## step N).  For steps with no values pass an empty array (an empty record).
  ##
  ## ``extraEvents`` carries already-encoded tagged value-stream events (today
  ## only tag-9 ``Assignment``, built by ``encodeAssignmentEvent``) that belong
  ## to the same step; they are appended after the tag-0 StepValues event.
  var rec: seq[byte] = @[]
  encodeRecord(values, extraEvents, rec)
  # Length-prefix the record within the chunk so the reader can index it.
  encodeVarint(uint64(rec.len), w.buffer)
  w.buffer.add(rec)
  inc w.recordCount
  inc w.totalRecords

  if w.recordCount >= w.chunkSize:
    return flushChunk(ctfs, w)
  ok()

proc flush*(ctfs: var Ctfs, w: var ValueStreamWriter): Result[void, string] =
  ## Flush any remaining buffered records as a partial final chunk.  Must be
  ## called before serializing the CTFS.  The SPEC ``values.idx`` carries no
  ## ``total_events`` trailer — the count is recoverable from the chunk offsets
  ## plus the last chunk's decoded record count.
  flushChunk(ctfs, w)

proc totalRecords*(w: ValueStreamWriter): uint64 = w.totalRecords

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
    return err("cannot determine decompressed size for value chunk")
  var raw = newSeq[byte](int(frameSize))
  if frameSize > 0:
    let decompSize = ZSTD_decompress(
      addr raw[0], csize_t(frameSize),
      unsafeAddr compressed[0], csize_t(compressed.len))
    if ZSTD_isError(decompSize) != 0:
      return err("zstd decompress failed for value chunk: " &
        $ZSTD_getErrorName(decompSize))
    raw.setLen(int(decompSize))

  var records: seq[seq[byte]] = @[]
  var pos = 0
  while pos < raw.len:
    let recLen = int(?decodeVarint(raw, pos))
    if pos + recLen > raw.len:
      return err("value record length extends past chunk")
    var rec = newSeq[byte](recLen)
    for j in 0 ..< recLen:
      rec[j] = raw[pos + j]
    pos += recLen
    records.add(rec)
  ok(records)

proc initValueStreamReader*(ctfsBytes: openArray[byte],
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries,
    legacy: bool = false): Result[ValueStreamReader, string] =
  ## Initialize a reader from raw CTFS container bytes.
  ##
  ## ``legacy`` selects the on-disk framing (see module docs):
  ##   * ``false`` (default) — SPEC chunked layout (``values.dat`` chunked Zstd +
  ##     ``values.idx`` = ``[chunk_size: u32][offset: u64]...``).  Byte-compatible
  ##     with the Rust ``ValueStreamReader``.
  ##   * ``true`` — legacy Nim-v4 ``.off`` VariableRecordTable layout
  ##     (``values.dat`` + ``values.off``, per-record ``varint count`` +
  ##     ``varnameId/typeId/dataLen/data``).
  ##
  ## The FFI reader passes ``legacy = not meta.hasValueStream``: pre-M24a-2
  ## bundles never set the ``has_value_stream`` flag, so a clear flag selects the
  ## legacy reader and a set flag the SPEC reader.
  if legacy:
    let tableRes = initVariableRecordTableReader(ctfsBytes, "values",
        blockSize, maxEntries)
    if tableRes.isErr:
      return err(tableRes.error)
    return ok(ValueStreamReader(
      legacy: true,
      legacyTable: tableRes.get(),
      cachedChunkIdx: -1))

  let datRes = readInternalFile(ctfsBytes, "values.dat", blockSize, maxEntries)
  if datRes.isErr:
    return err("failed to read values.dat: " & datRes.error)
  let datData = datRes.get()

  let idxRes = readInternalFile(ctfsBytes, "values.idx", blockSize, maxEntries)
  if idxRes.isErr:
    return err("failed to read values.idx: " & idxRes.error)
  let idxData = idxRes.get()

  if idxData.len < 4:
    return err("values.idx too small for chunk_size header")
  var cs4: array[4, byte]
  for i in 0 ..< 4:
    cs4[i] = idxData[i]
  let chunkSize = fromBytesLE(uint32, cs4)
  if chunkSize == 0:
    return err("chunkSize in values.idx is 0")

  let offsetRegionBytes = idxData.len - 4
  if offsetRegionBytes mod 8 != 0:
    return err("values.idx has trailing bytes in offset region")
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
      return err("last value chunk offset past end of values.dat")
    let lastRecs = ?decompressChunkRecords(
      datData.toOpenArray(startOff, endOff - 1))
    totalRecords = uint64(lastChunk) * uint64(chunkSize) + uint64(lastRecs.len)

  ok(ValueStreamReader(
    legacy: false,
    data: datData,
    chunkSize: chunkSize,
    offsets: offsets,
    totalRecordsVal: totalRecords,
    cachedChunkIdx: -1,
    cachedRecords: @[]))

proc count*(r: ValueStreamReader): uint64 =
  if r.legacy:
    r.legacyTable.count()
  else:
    r.totalRecordsVal

proc readLegacyRecord(data: openArray[byte]): Result[seq[VariableValue], string] =
  ## Decode a legacy ``.off`` VRT value record (pre-M24a-2 framing):
  ## ``varint count, count × (varint varnameId, varint typeId, varint dataLen,
  ## data)``.
  if data.len == 0:
    return ok(newSeq[VariableValue]())
  var pos = 0
  let count = int(?decodeVarint(data, pos))
  var values = newSeq[VariableValue](count)
  for i in 0 ..< count:
    let vnId = ?decodeVarint(data, pos)
    let tId = ?decodeVarint(data, pos)
    let dLen = int(?decodeVarint(data, pos))
    if pos + dLen > data.len:
      return err("truncated legacy value data")
    var d = newSeq[byte](dLen)
    for j in 0 ..< dLen:
      d[j] = data[pos + j]
    pos += dLen
    values[i] = VariableValue(varnameId: vnId, typeId: tId, data: d)
  ok(values)

proc readStepValues*(r: var ValueStreamReader,
    stepIndex: uint64): Result[seq[VariableValue], string] =
  ## Read all variable values for a given step (record N ↔ step N).
  if r.legacy:
    let dataRes = r.legacyTable.read(stepIndex)
    if dataRes.isErr:
      return err(dataRes.error)
    return readLegacyRecord(dataRes.get())

  if stepIndex >= r.totalRecordsVal:
    return err("value step index " & $stepIndex & " out of range (count " &
      $r.totalRecordsVal & ")")
  let chunkNumber = int(stepIndex div uint64(r.chunkSize))
  let within = int(stepIndex mod uint64(r.chunkSize))

  if r.cachedChunkIdx != chunkNumber:
    let startOff = int(r.offsets[chunkNumber])
    let endOff =
      if chunkNumber + 1 < r.offsets.len: int(r.offsets[chunkNumber + 1])
      else: r.data.len
    if startOff > endOff or endOff > r.data.len:
      return err("value chunk offsets out of range")
    let recs = ?decompressChunkRecords(
      r.data.toOpenArray(startOff, endOff - 1))
    r.cachedRecords = recs
    r.cachedChunkIdx = chunkNumber

  if within >= r.cachedRecords.len:
    return err("value record " & $within & " missing in chunk " & $chunkNumber)
  decodeRecord(r.cachedRecords[within])

proc readStepAssignments*(r: var ValueStreamReader,
    stepIndex: uint64): Result[seq[AssignmentEventEntry], string] =
  ## Read the tag-9 ``Assignment`` events recorded for a given step
  ## (record N ↔ step N).  Legacy ``.off`` VRT bundles never carried them, so
  ## they report an empty sequence rather than an error.
  if r.legacy:
    return ok(newSeq[AssignmentEventEntry]())

  if stepIndex >= r.totalRecordsVal:
    return err("value step index " & $stepIndex & " out of range (count " &
      $r.totalRecordsVal & ")")
  let chunkNumber = int(stepIndex div uint64(r.chunkSize))
  let within = int(stepIndex mod uint64(r.chunkSize))

  if r.cachedChunkIdx != chunkNumber:
    let startOff = int(r.offsets[chunkNumber])
    let endOff =
      if chunkNumber + 1 < r.offsets.len: int(r.offsets[chunkNumber + 1])
      else: r.data.len
    if startOff > endOff or endOff > r.data.len:
      return err("value chunk offsets out of range")
    let recs = ?decompressChunkRecords(
      r.data.toOpenArray(startOff, endOff - 1))
    r.cachedRecords = recs
    r.cachedChunkIdx = chunkNumber

  if within >= r.cachedRecords.len:
    return err("value record " & $within & " missing in chunk " & $chunkNumber)
  decodeRecordAssignments(r.cachedRecords[within])
