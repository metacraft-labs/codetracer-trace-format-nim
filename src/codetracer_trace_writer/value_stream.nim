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
##   emits four of them (or NOTHING for a value-less step — an empty record):
##     Tag 0  StepValues    : u8 tag(0x00), varint count,
##                            count × (varint name_id, varint value_len,
##                                     value bytes (CBOR ValueRecord))
##     Tag 2  DropVariable  : u8 tag(0x02), varint variable_id
##     Tag 3  DropVariables : u8 tag(0x03), varint count, count × varint id
##     Tag 9  Assignment    : u8 tag(0x09), varint to, u8 pass_by,
##                            varint from_len, from bytes (CBOR RValue)
##   Tag 0 comes first when present; the others follow it, in the order the
##   recorder produced them.
##
##   Tags below 10 are NOT self-delimiting — their length is implied by their
##   field layout — so a reader must know every one of them to walk a record
##   at all.  ``decodeRecordEvents`` is the single place that knows; the
##   per-kind accessors filter its output rather than re-walking the bytes.
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

  TagDropVariable* = 2'u8
    ## Value-stream event tag 2 (``trace-events.md`` §"Value Stream Events"):
    ## ``DropVariable {variable_id: varint}`` — "Drop a single variable".
    ##
    ## Distinct from tag 3 in what it claims, not merely in arity: tag 2 is one
    ## variable ending its life, tag 3 is a scope ending and taking its
    ## bindings with it.  A recorder that reports a lone drop as a
    ## one-variable tag 3 asserts a scope boundary the program never had.

  TagDropVariables* = 3'u8
    ## Value-stream event tag 3 (``trace-events.md`` §"Value Stream Events"):
    ## ``DropVariables {count: varint, ids: [varint]}`` — "Drop multiple
    ## variables (end of scope)".
    ##
    ## Emitted by ``trace_writer_register_drop_variables`` and appended AFTER
    ## the tag-0 ``StepValues`` event of the step whose execution left the
    ## scope.  The encoding is byte-identical to the canonical Rust
    ## ``ValueStreamEvent::DropVariables`` in
    ## ``codetracer-trace-format/codetracer_trace_writer/src/value_stream.rs``,
    ## which is what lets each writer's output be read by the other's decoder.

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
    lastRecordStart: int
      ## Offset in ``buffer`` of the most recently written record, or -1 when
      ## no record has been written into the current chunk.
      ##
      ## The most recent record stays amendable, which is what lets a writer
      ## attach values staged after it to the step it belongs to instead of
      ## emitting a second step to carry them (spec §"Where the recording ends
      ## with values still staged": the terminus must not change the step
      ## count, because a recording has exactly N + 1 steps). Keeping it
      ## amendable is why the chunk is flushed BEFORE the next record is
      ## appended rather than after the current one — same chunk contents,
      ## but the record just written is always still here.

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
    lastSkippedTags*: seq[uint8]  ## tags >= 10 skipped in the most recent readStepValues / readStepAssignments
    skippedTags*: seq[uint8]      ## distinct tags >= 10 skipped across all reads
    skippedTagCounts*: seq[(uint8, int)] ## cumulative count per tag

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

proc encodeDropVariableEvent*(variableId: uint64, outBuf: var seq[byte]) =
  ## Encode one tag-2 ``DropVariable`` value-stream event into ``outBuf``:
  ## ``u8 0x02, varint variable_id`` — the byte-for-byte layout
  ## `trace-events.md` §"Value Stream Events" gives for tag 2
  ## (``variable_id: varint``) and the one the Rust
  ## ``ValueStreamEvent::DropVariable`` encoder produces.
  ##
  ## The id is an interned varname id, resolved through the same
  ## ``varnames.dat`` table as the ``StepValues`` pairs in the same record.
  outBuf.add(TagDropVariable)
  encodeVarint(variableId, outBuf)

proc encodeDropVariablesEvent*(variableIds: openArray[uint64],
    outBuf: var seq[byte]) =
  ## Encode one tag-3 ``DropVariables`` value-stream event into ``outBuf``:
  ## ``u8 0x03, varint count, count × varint variable_id`` — the byte-for-byte
  ## layout `trace-events.md` §"Value Stream Events" gives for tag 3
  ## (``count: varint, ids: [varint]``) and the one the Rust
  ## ``ValueStreamEvent::DropVariables`` encoder produces.
  ##
  ## The ids are interned varname ids, so they are resolved through the same
  ## ``varnames.dat`` table as the ``StepValues`` pairs in the same record; a
  ## reader needs no separate mapping to name a dropped variable.
  outBuf.add(TagDropVariables)
  encodeVarint(uint64(variableIds.len), outBuf)
  for id in variableIds:
    encodeVarint(id, outBuf)

proc encodeLengthPrefixedEvent*(tag: uint8, payload: openArray[byte],
    outBuf: var seq[byte]) =
  ## Encode one forward-compatible self-delimiting value-stream event (tag >= 10):
  ## ``u8 tag, varint payload_len, payload_bytes``.
  outBuf.add(tag)
  encodeVarint(uint64(payload.len), outBuf)
  for b in payload:
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
  ## value-stream events (tag-9 ``Assignment``, or forward-compatible tag >= 10).
  ## A record is defined by the spec as "the concatenation of zero-or-more tagged
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

type
  ValueEventKind* = enum
    ## Which tagged value-stream event a decoded record entry is.
    veStepValues
    veDropVariable
    veDropVariables
    veAssignment

  DecodedValueEvent* = object
    ## One decoded tagged value-stream event, in the order it appeared in the
    ## record.  Unknown self-delimiting tags (>= 10) are not represented here:
    ## they are walked over and reported through ``skippedTags`` instead,
    ## because this reader has no way to say what they meant.
    case kind*: ValueEventKind
    of veStepValues:
      values*: seq[VariableValue]
    of veDropVariable:
      droppedId*: uint64
    of veDropVariables:
      droppedIds*: seq[uint64]
    of veAssignment:
      assignment*: AssignmentEventEntry

proc decodeRecordEvents*(data: openArray[byte],
    skippedTags: var seq[uint8]): Result[seq[DecodedValueEvent], string] =
  ## Decode one SPEC value record — "the concatenation of zero-or-more tagged
  ## value-stream events" — into those events, in wire order.
  ##
  ## THIS IS THE ONLY WALKER.  Every accessor below filters its result rather
  ## than walking the bytes itself, because tags below 10 are NOT
  ## self-delimiting: a reader that does not know a tag's field layout cannot
  ## skip it, so each walker has to handle EVERY tag correctly just to reach
  ## the events it does care about.  Independent walkers made that a promise
  ## repeated once per accessor, and one of them getting a tag wrong mis-frames
  ## the whole rest of the record with nothing to show for it.
  ##
  ## Forward-compatibility (HX-S-5 / HX-OQ-8):
  ## Tags >= 10 are self-delimited by a varint length prefix following the tag,
  ## so they can be skipped without knowing their layout; their tags are
  ## recorded in ``skippedTags``.  Unknown tags < 10 are refused by name.
  var pos = 0
  var events: seq[DecodedValueEvent] = @[]
  while pos < data.len:
    let tag = data[pos]
    inc pos
    case tag
    of TagStepValues:
      let count = int(?decodeVarint(data, pos))
      var values = newSeq[VariableValue](count)
      for i in 0 ..< count:
        let vnId = ?decodeVarint(data, pos)
        let dLen = int(?decodeVarint(data, pos))
        if pos + dLen > data.len:
          return err("truncated value data in StepValues record")
        var d = newSeq[byte](dLen)
        for j in 0 ..< dLen:
          d[j] = data[pos + j]
        pos += dLen
        values[i] = VariableValue(
          varnameId: vnId,
          typeId: decodeCborTopLevelTypeId(d),
          data: d)
      events.add(DecodedValueEvent(kind: veStepValues, values: values))
    of TagDropVariable:
      let id = ?decodeVarint(data, pos)
      events.add(DecodedValueEvent(kind: veDropVariable, droppedId: id))
    of TagDropVariables:
      let count = int(?decodeVarint(data, pos))
      var ids = newSeq[uint64](count)
      for i in 0 ..< count:
        ids[i] = ?decodeVarint(data, pos)
      events.add(DecodedValueEvent(kind: veDropVariables, droppedIds: ids))
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
      events.add(DecodedValueEvent(kind: veAssignment,
        assignment: AssignmentEventEntry(
          varnameId: vnId, passBy: passBy, rvalueCbor: blob)))
    else:
      when defined(oldReaderPreForwardCompat):
        return err("unsupported value-stream event tag " & $tag &
          " in Nim value record (this reader predates the tag; rebuild ct-print " &
          "from codetracer-trace-format-nim)")
      else:
        if tag >= 10:
          let payloadLen = int(?decodeVarint(data, pos))
          if pos + payloadLen > data.len:
            return err("truncated payload in value-stream event tag " & $tag &
              " (expected " & $payloadLen & " bytes, only " & $(data.len - pos) & " remain)")
          pos += payloadLen
          skippedTags.add(tag)
        else:
          return err("unsupported value-stream event tag " & $tag &
            " in Nim value record (this reader predates the tag; rebuild ct-print " &
            "from codetracer-trace-format-nim)")
  ok(events)

proc decodeRecordEvents*(data: openArray[byte]):
    Result[seq[DecodedValueEvent], string] =
  var dummy: seq[uint8] = @[]
  decodeRecordEvents(data, dummy)

proc decodeRecord*(data: openArray[byte],
    skippedTags: var seq[uint8]): Result[seq[VariableValue], string] =
  ## The variable values of one record: its tag-0 ``StepValues`` events,
  ## concatenated.  A record that carries none — whether it is empty or holds
  ## only other event kinds — yields an empty sequence.
  var values: seq[VariableValue] = @[]
  for ev in ?decodeRecordEvents(data, skippedTags):
    if ev.kind == veStepValues:
      values.add(ev.values)
  ok(values)

proc decodeRecord*(data: openArray[byte]): Result[seq[VariableValue], string] =
  var dummy: seq[uint8] = @[]
  decodeRecord(data, dummy)

proc decodeRecordAssignments*(data: openArray[byte],
    skippedTags: var seq[uint8]): Result[seq[AssignmentEventEntry], string] =
  ## The tag-9 ``Assignment`` events of one record, in wire order.
  var found: seq[AssignmentEventEntry] = @[]
  for ev in ?decodeRecordEvents(data, skippedTags):
    if ev.kind == veAssignment:
      found.add(ev.assignment)
  ok(found)

proc decodeRecordAssignments*(data: openArray[byte]):
    Result[seq[AssignmentEventEntry], string] =
  var dummy: seq[uint8] = @[]
  decodeRecordAssignments(data, dummy)

proc decodeRecordDropVariables*(data: openArray[byte],
    skippedTags: var seq[uint8]): Result[seq[seq[uint64]], string] =
  ## The tag-3 ``DropVariables`` events of one record, one ``seq[uint64]`` of
  ## interned varname ids per event.
  ##
  ## Each event is reported separately rather than flattened: a record may
  ## carry more than one scope exit, and which ids left together is what makes
  ## a drop a scope boundary rather than a list of unrelated variables.
  var found: seq[seq[uint64]] = @[]
  for ev in ?decodeRecordEvents(data, skippedTags):
    if ev.kind == veDropVariables:
      found.add(ev.droppedIds)
  ok(found)

proc decodeRecordDropVariables*(data: openArray[byte]):
    Result[seq[seq[uint64]], string] =
  var dummy: seq[uint8] = @[]
  decodeRecordDropVariables(data, dummy)

proc decodeRecordDropVariable*(data: openArray[byte],
    skippedTags: var seq[uint8]): Result[seq[uint64], string] =
  ## The tag-2 ``DropVariable`` events of one record — one interned varname id
  ## each, in wire order.
  ##
  ## Reported separately from ``decodeRecordDropVariables`` because the two
  ## tags state different things: tag 2 is one variable ending its life, tag 3
  ## is a scope ending and taking its bindings with it.  Folding a tag-2 event
  ## into the plural accessor would report a lone drop as a one-variable scope
  ## exit, which is a claim about program structure that was never made.
  var found: seq[uint64] = @[]
  for ev in ?decodeRecordEvents(data, skippedTags):
    if ev.kind == veDropVariable:
      found.add(ev.droppedId)
  ok(found)

proc decodeRecordDropVariable*(data: openArray[byte]):
    Result[seq[uint64], string] =
  var dummy: seq[uint8] = @[]
  decodeRecordDropVariable(data, dummy)


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
    lastRecordStart: -1,
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
  # The amendable record went out with the chunk. A later amend refuses by
  # name rather than rewriting whatever byte range happens to be at offset 0.
  w.lastRecordStart = -1
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
  # Flush the full chunk BEFORE appending, so the record written below is
  # still in ``buffer`` when this returns. See ``lastRecordStart``.
  if w.recordCount >= w.chunkSize:
    let flushed = flushChunk(ctfs, w)
    if flushed.isErr:
      return flushed

  var rec: seq[byte] = @[]
  encodeRecord(values, extraEvents, rec)
  # Length-prefix the record within the chunk so the reader can index it.
  w.lastRecordStart = w.buffer.len
  encodeVarint(uint64(rec.len), w.buffer)
  w.buffer.add(rec)
  inc w.recordCount
  inc w.totalRecords
  ok()

proc rewriteLastStepValues*(w: var ValueStreamWriter,
    values: openArray[VariableValue],
    extraEvents: openArray[byte] = []): Result[void, string] =
  ## Replace the most recently written record with one encoding ``values`` and
  ## ``extraEvents``.
  ##
  ## This is how values staged after the last step reach the trace: they are
  ## merged with that step's own values by the caller (which is the party that
  ## still has them) and the record is written again. The step count does not
  ## move, which is the point — the alternative a writer reaches for is a second
  ## step at the last recorded position, and that makes a recording N + 2 steps
  ## long whenever a value happened to be staged at the end.
  ##
  ## Refuses rather than guesses when there is no record to amend.
  if w.lastRecordStart < 0:
    return err("rewriteLastStepValues: no value record has been written into " &
      "the current chunk, so there is nothing to amend")
  if w.lastRecordStart > w.buffer.len:
    return err("rewriteLastStepValues: recorded record offset " &
      $w.lastRecordStart & " is past the buffer end " & $w.buffer.len)
  var rec: seq[byte] = @[]
  encodeRecord(values, extraEvents, rec)
  w.buffer.setLen(w.lastRecordStart)
  encodeVarint(uint64(rec.len), w.buffer)
  w.buffer.add(rec)
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

proc cacheRecordFor(r: var ValueStreamReader,
    stepIndex: uint64): Result[int, string] =
  ## Decompress whichever chunk holds ``stepIndex`` into ``r.cachedRecords``
  ## (reusing the cache when it already holds that chunk) and return the
  ## record's index WITHIN the chunk.
  ##
  ## Shared by the three per-step accessors so they cannot disagree about
  ## which bytes a step's record occupies — each one then differs only in
  ## which tagged events it decodes out of those bytes.
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
  ok(within)

proc noteSkippedTags(r: var ValueStreamReader, skipped: seq[uint8]) =
  ## Fold the tags one decode walked over into the reader's cumulative
  ## forward-compatibility tallies (``lastSkippedTags`` for the decode just
  ## performed, ``skippedTags`` / ``skippedTagCounts`` across the reader's
  ## lifetime).  A skipped tag is a record this reader could not interpret, so
  ## it is counted rather than discarded.
  if skipped.len == 0:
    return
  r.lastSkippedTags = skipped
  for t in skipped:
    if t notin r.skippedTags:
      r.skippedTags.add(t)
    var found = false
    for i in 0 ..< r.skippedTagCounts.len:
      if r.skippedTagCounts[i][0] == t:
        inc r.skippedTagCounts[i][1]
        found = true
        break
    if not found:
      r.skippedTagCounts.add((t, 1))

proc readStepValues*(r: var ValueStreamReader,
    stepIndex: uint64): Result[seq[VariableValue], string] =
  ## Read all variable values for a given step (record N ↔ step N).
  if r.legacy:
    let dataRes = r.legacyTable.read(stepIndex)
    if dataRes.isErr:
      return err(dataRes.error)
    return readLegacyRecord(dataRes.get())

  let within = ?r.cacheRecordFor(stepIndex)
  var skipped: seq[uint8] = @[]
  let res = decodeRecord(r.cachedRecords[within], skipped)
  r.noteSkippedTags(skipped)
  res

proc readStepDropVariable*(r: var ValueStreamReader,
    stepIndex: uint64): Result[seq[uint64], string] =
  ## Read the tag-2 ``DropVariable`` events recorded for a given step
  ## (record N ↔ step N), one interned varname id each.  Legacy ``.off`` VRT
  ## bundles never carried them, so they report an empty sequence rather than
  ## an error.
  r.lastSkippedTags.setLen(0)
  if r.legacy:
    return ok(newSeq[uint64]())

  let within = ?r.cacheRecordFor(stepIndex)
  var skipped: seq[uint8] = @[]
  let res = decodeRecordDropVariable(r.cachedRecords[within], skipped)
  r.noteSkippedTags(skipped)
  res

proc readStepDropVariables*(r: var ValueStreamReader,
    stepIndex: uint64): Result[seq[seq[uint64]], string] =
  ## Read the tag-3 ``DropVariables`` events recorded for a given step
  ## (record N ↔ step N), one ``seq[uint64]`` of interned varname ids per
  ## event.  Legacy ``.off`` VRT bundles never carried them, so they report an
  ## empty sequence rather than an error.
  r.lastSkippedTags.setLen(0)
  if r.legacy:
    return ok(newSeq[seq[uint64]]())

  let within = ?r.cacheRecordFor(stepIndex)
  var skipped: seq[uint8] = @[]
  let res = decodeRecordDropVariables(r.cachedRecords[within], skipped)
  r.noteSkippedTags(skipped)
  res

proc readStepAssignments*(r: var ValueStreamReader,
    stepIndex: uint64): Result[seq[AssignmentEventEntry], string] =
  ## Read the tag-9 ``Assignment`` events recorded for a given step
  ## (record N ↔ step N).  Legacy ``.off`` VRT bundles never carried them, so
  ## they report an empty sequence rather than an error.
  r.lastSkippedTags.setLen(0)
  if r.legacy:
    return ok(newSeq[AssignmentEventEntry]())

  let within = ?r.cacheRecordFor(stepIndex)
  var skipped: seq[uint8] = @[]
  let res = decodeRecordAssignments(r.cachedRecords[within], skipped)
  r.noteSkippedTags(skipped)
  res

proc lastSkippedTags*(r: ValueStreamReader): seq[uint8] =
  r.lastSkippedTags

proc skippedTags*(r: ValueStreamReader): seq[uint8] =
  r.skippedTags

proc skippedTagCounts*(r: ValueStreamReader): seq[(uint8, int)] =
  r.skippedTagCounts

