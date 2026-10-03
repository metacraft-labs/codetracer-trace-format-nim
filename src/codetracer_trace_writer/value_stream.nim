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
import ./record_chunk

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

  TagBindVariable* = 1'u8
    ## ``BindVariable {variable_id: varint, place: signed varint}``.
  TagCellValue* = 4'u8
    ## ``CellValue {place: signed varint, value: varint len + CBOR}``.
  TagCompoundValue* = 5'u8
    ## ``CompoundValue {place: signed varint, value: varint len + CBOR}``.
  TagAssignCell* = 6'u8
    ## ``AssignCell {place: signed varint, new_value: varint len + CBOR}``.
  TagAssignCompoundItem* = 7'u8
    ## ``AssignCompoundItem {place: signed varint, index: varint,
    ## item_place: signed varint}``.
  TagVariableCell* = 8'u8
    ## ``VariableCell {variable_id: varint, place: signed varint}``.
    ##
    ## Tags 1 and 4-8 carry the place model (`trace-events.md` §"Value
    ## Stream"): every tag 0-9 is part of the format, a writer writes every
    ## tag its API exposes and a reader decodes all of them. Places are
    ## zigzag-signed varints and every CBOR value is length-prefixed, byte
    ## for byte as the Rust ``ValueStreamEvent`` encodes them.

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
    held: seq[seq[byte]]
      ## Full chunks, oldest first, that are not written yet because the last
      ## STEP's record is in one of them or in ``buffer`` after them. Only
      ## records that are not steps (thread, raise/catch, reload records)
      ## follow that step's record, so this is bounded by how many of those a
      ## recording emits between two steps.
    chunksWritten: int         ## chunks compressed into values.dat so far
    lastStepChunk: int
      ## Ordinal of the chunk holding the most recent step's record (the
      ## chunk it is in counts ``chunksWritten`` + position in ``held``, and
      ## ``buffer`` comes after ``held``), or -1 when no step has been written.
    lastStepRecordStart: int
      ## Offset of that record within its chunk's bytes.
      ##
      ## The most recent STEP's record stays amendable, which is what lets a
      ## writer attach values staged after it to the step they belong to
      ## instead of emitting a second step to carry them (spec §"Where the
      ## recording ends with values still staged": the terminus must not
      ## change the step count, because a recording has exactly N + 1 steps).
      ## Records that are not steps can follow it — a thread switch, say — and
      ## the values do not belong to those, whose records no step reads; so
      ## the step's chunk, and every chunk after it, is held back from
      ## compression until a later step makes it final.

  ValueStreamReader* = object
    data: seq[byte]            ## raw values.dat content (SPEC mode)
    chunkSize: uint32
    offsets: seq[uint64]       ## chunk byte offsets from values.idx (SPEC mode)
    totalRecordsVal: uint64
    legacy: bool               ## true ⇒ legacy .off VRT layout; false ⇒ SPEC
    legacyTable: VariableRecordTableReader  ## only valid when legacy == true
    chunk: RecordChunk         ## the SPEC chunk the last read inflated
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
  ## The CBOR ``ValueRecord`` in ``data``'s top-level ``type_id``, read
  ## without decoding the value (``cborTopLevelTypeId``).  0 when it has none
  ## or does not parse (the data is still surfaced verbatim; only the
  ## convenience type id is unavailable).
  cborTopLevelTypeId(data)

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

proc encodeBlob(data: openArray[byte], outBuf: var seq[byte]) =
  encodeVarint(uint64(data.len), outBuf)
  for b in data:
    outBuf.add(b)

proc encodeBindVariableEvent*(variableId: uint64, place: int64,
    outBuf: var seq[byte]) =
  ## Tag 1 ``BindVariable``.
  outBuf.add(TagBindVariable)
  encodeVarint(variableId, outBuf)
  encodeSignedVarint(place, outBuf)

proc encodeCellValueEvent*(place: int64, valueCbor: openArray[byte],
    outBuf: var seq[byte]) =
  ## Tag 4 ``CellValue``.
  outBuf.add(TagCellValue)
  encodeSignedVarint(place, outBuf)
  encodeBlob(valueCbor, outBuf)

proc encodeCompoundValueEvent*(place: int64, valueCbor: openArray[byte],
    outBuf: var seq[byte]) =
  ## Tag 5 ``CompoundValue``.
  outBuf.add(TagCompoundValue)
  encodeSignedVarint(place, outBuf)
  encodeBlob(valueCbor, outBuf)

proc encodeAssignCellEvent*(place: int64, newValueCbor: openArray[byte],
    outBuf: var seq[byte]) =
  ## Tag 6 ``AssignCell``.
  outBuf.add(TagAssignCell)
  encodeSignedVarint(place, outBuf)
  encodeBlob(newValueCbor, outBuf)

proc encodeAssignCompoundItemEvent*(place: int64, index: uint64,
    itemPlace: int64, outBuf: var seq[byte]) =
  ## Tag 7 ``AssignCompoundItem``.
  outBuf.add(TagAssignCompoundItem)
  encodeSignedVarint(place, outBuf)
  encodeVarint(index, outBuf)
  encodeSignedVarint(itemPlace, outBuf)

proc encodeVariableCellEvent*(variableId: uint64, place: int64,
    outBuf: var seq[byte]) =
  ## Tag 8 ``VariableCell``.
  outBuf.add(TagVariableCell)
  encodeVarint(variableId, outBuf)
  encodeSignedVarint(place, outBuf)

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
    veBindVariable
    veDropVariable
    veDropVariables
    veCellValue
    veCompoundValue
    veAssignCell
    veAssignCompoundItem
    veVariableCell
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
    of veBindVariable, veVariableCell:
      variableId*: uint64
      variablePlace*: int64
    of veCellValue, veCompoundValue, veAssignCell:
      place*: int64
      valueCbor*: seq[byte]
    of veAssignCompoundItem:
      compoundPlace*: int64
      itemIndex*: uint64
      itemPlace*: int64

proc decodeOneValueEvent(data: openArray[byte], pos: var int, tag: uint8,
    events: var seq[DecodedValueEvent],
    skippedTags: var seq[uint8]): Result[void, string] =
  ## Decode the fields of one tagged value-stream event, its tag already read.
  case tag
  of TagStepValues:
    let count = int(varintOrReturn(data, pos))
    var values = newSeq[VariableValue](count)
    for i in 0 ..< count:
      let vnId = varintOrReturn(data, pos)
      let dLen = int(varintOrReturn(data, pos))
      if dLen < 0 or pos + dLen > data.len:
        return err("truncated value data in StepValues record")
      template d: untyped = data.toOpenArray(pos, pos + dLen - 1)
      values[i] = VariableValue(
        varnameId: vnId,
        typeId: decodeCborTopLevelTypeId(d),
        data: @d)
      pos += dLen
    events.add(DecodedValueEvent(kind: veStepValues, values: values))
  of TagBindVariable, TagVariableCell:
    let vid = varintOrReturn(data, pos)
    let place = signedVarintOrReturn(data, pos)
    if tag == TagBindVariable:
      events.add(DecodedValueEvent(kind: veBindVariable,
        variableId: vid, variablePlace: place))
    else:
      events.add(DecodedValueEvent(kind: veVariableCell,
        variableId: vid, variablePlace: place))
  of TagCellValue, TagCompoundValue, TagAssignCell:
    let place = signedVarintOrReturn(data, pos)
    let vLen = varintOrReturn(data, pos)
    if vLen > uint64(data.len - pos):
      return err("truncated CBOR value in value-stream event tag " & $tag)
    let blob = @(data.toOpenArray(pos, pos + int(vLen) - 1))
    pos += int(vLen)
    case tag
    of TagCellValue:
      events.add(DecodedValueEvent(kind: veCellValue, place: place,
        valueCbor: blob))
    of TagCompoundValue:
      events.add(DecodedValueEvent(kind: veCompoundValue, place: place,
        valueCbor: blob))
    else:
      events.add(DecodedValueEvent(kind: veAssignCell, place: place,
        valueCbor: blob))
  of TagAssignCompoundItem:
    let place = signedVarintOrReturn(data, pos)
    let index = varintOrReturn(data, pos)
    let itemPlace = signedVarintOrReturn(data, pos)
    events.add(DecodedValueEvent(kind: veAssignCompoundItem,
      compoundPlace: place, itemIndex: index, itemPlace: itemPlace))
  of TagDropVariable:
    let id = varintOrReturn(data, pos)
    events.add(DecodedValueEvent(kind: veDropVariable, droppedId: id))
  of TagDropVariables:
    let count = int(varintOrReturn(data, pos))
    var ids = newSeq[uint64](count)
    for i in 0 ..< count:
      ids[i] = varintOrReturn(data, pos)
    events.add(DecodedValueEvent(kind: veDropVariables, droppedIds: ids))
  of TagAssignment:
    let vnId = varintOrReturn(data, pos)
    if pos >= data.len:
      return err("truncated pass_by in Assignment value-stream event")
    let passBy = data[pos]
    inc pos
    let fromLen = int(varintOrReturn(data, pos))
    if fromLen < 0 or pos + fromLen > data.len:
      return err("truncated RValue payload in Assignment value-stream event")
    let blob = @(data.toOpenArray(pos, pos + fromLen - 1))
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
        let payloadLen = int(varintOrReturn(data, pos))
        if pos + payloadLen > data.len:
          return err("truncated payload in value-stream event tag " & $tag &
            " (expected " & $payloadLen & " bytes, only " & $(data.len - pos) & " remain)")
        pos += payloadLen
        skippedTags.add(tag)
      else:
        return err("unsupported value-stream event tag " & $tag &
          " in Nim value record (this reader predates the tag; rebuild ct-print " &
          "from codetracer-trace-format-nim)")
  ok()

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
    let tagStart = pos - 1
    let r = decodeOneValueEvent(data, pos, tag, events, skippedTags)
    if r.isErr:
      return err("value-stream event tag " & $tag & " at byte " & $tagStart &
        ": " & r.error)
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
  var events = ?decodeRecordEvents(data, skippedTags)
  for ev in mitems(events):
    if ev.kind == veStepValues:
      if values.len == 0:
        values = move ev.values
      else:
        for v in mitems(ev.values):
          values.add(move v)
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
    held: @[],
    chunksWritten: 0,
    lastStepChunk: -1,
    lastStepRecordStart: -1,
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

proc writeChunk(ctfs: var Ctfs, w: var ValueStreamWriter,
    chunk: openArray[byte]): Result[void, string] =
  ## Compress one chunk's records, append it to values.dat, and record its
  ## byte offset in values.idx.
  let bound = ZSTD_compressBound(csize_t(chunk.len))
  var compressed = newSeq[byte](int(bound))
  let compressedSize = ZSTD_compress(
    addr compressed[0], csize_t(bound),
    unsafeAddr chunk[0], csize_t(chunk.len),
    cint(ValuesCompressionLevel))
  if ZSTD_isError(compressedSize) != 0:
    return err("zstd compress failed for value chunk: " &
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
    return err("failed to write value chunk: " & datRes.error)

  var offBytes: array[8, byte]
  let offLE = toBytesLE(chunkStart)
  for i in 0 ..< 8:
    offBytes[i] = offLE[i]
  let offRes = ctfs.writeToFile(w.indexFile, offBytes)
  if offRes.isErr:
    return err("failed to write values.idx offset: " & offRes.error)
  ctfs.syncEntry(w.indexFile)

  w.dataOffset += uint64(compressedSize)
  inc w.chunksWritten
  ok()

proc writeHeld(ctfs: var Ctfs, w: var ValueStreamWriter): Result[void, string] =
  ## Write every held chunk, oldest first.
  for chunk in w.held:
    ? writeChunk(ctfs, w, chunk)
  w.held.setLen(0)
  ok()

proc rotateChunk(ctfs: var Ctfs, w: var ValueStreamWriter): Result[void, string] =
  ## The current chunk is full: start a new one. It is written now unless the
  ## last step's record is in it (or in a held chunk before it), in which case
  ## it is held until a later step makes that record final.
  let currentChunk = w.chunksWritten + w.held.len
  if w.lastStepChunk >= w.chunksWritten and w.lastStepChunk <= currentChunk:
    w.held.add(w.buffer)
  else:
    ? writeChunk(ctfs, w, w.buffer)
  w.buffer = @[]
  w.recordCount = 0
  ok()

proc flushChunk(ctfs: var Ctfs, w: var ValueStreamWriter): Result[void, string] =
  ## Write every held chunk and the current one. Called when the stream ends:
  ## nothing after this can amend a record.
  ? writeHeld(ctfs, w)
  if w.recordCount > 0:
    ? writeChunk(ctfs, w, w.buffer)
  w.buffer.setLen(0)
  w.recordCount = 0
  w.lastStepChunk = -1
  w.lastStepRecordStart = -1
  ok()

proc writeStepValues*(ctfs: var Ctfs, w: var ValueStreamWriter,
    values: openArray[VariableValue],
    extraEvents: openArray[byte] = [],
    isStep = true): Result[void, string] =
  ## Write the value record of one exec record.  Call exactly once per exec
  ## record, in order — this preserves the parallel-index invariant (record N
  ## ↔ exec record N).  For a record with no values pass an empty array.
  ##
  ## ``isStep`` is false for an exec record that is not a step (a thread
  ## record, raise/catch, a reload marker): its record is always empty, and it
  ## does not become the record ``rewriteLastStepValues`` amends.
  ##
  ## ``extraEvents`` carries already-encoded tagged value-stream events (today
  ## only tag-9 ``Assignment``, built by ``encodeAssignmentEvent``) that belong
  ## to the same step; they are appended after the tag-0 StepValues event.
  # Start a new chunk BEFORE appending, so a full chunk that holds the last
  # step's record is still amendable when this returns.
  if w.recordCount >= w.chunkSize:
    ? rotateChunk(ctfs, w)

  if isStep:
    # A new step: every earlier record is final, so the held chunks can go.
    ? writeHeld(ctfs, w)
    w.lastStepChunk = w.chunksWritten
    w.lastStepRecordStart = w.buffer.len

  var rec: seq[byte] = @[]
  encodeRecord(values, extraEvents, rec)
  # Length-prefix the record within the chunk so the reader can index it.
  encodeVarint(uint64(rec.len), w.buffer)
  w.buffer.add(rec)
  inc w.recordCount
  inc w.totalRecords
  ok()

proc rewriteLastStepValues*(w: var ValueStreamWriter,
    values: openArray[VariableValue],
    extraEvents: openArray[byte] = []): Result[void, string] =
  ## Replace the most recent STEP's record with one encoding ``values`` and
  ## ``extraEvents``; records written after it (thread and other non-step
  ## records) are kept as they are.
  ##
  ## This is how values staged after the last step reach the trace: they are
  ## merged with that step's own values by the caller (which is the party that
  ## still has them) and the record is written again. The step count does not
  ## move, which is the point — the alternative a writer reaches for is a second
  ## step at the last recorded position, and that makes a recording N + 2 steps
  ## long whenever a value happened to be staged at the end.
  ##
  ## Refuses rather than guesses when there is no record to amend.
  if w.lastStepChunk < w.chunksWritten:
    return err("rewriteLastStepValues: no step's value record is still " &
      "amendable, so there is nothing to amend")
  let pos = w.lastStepChunk - w.chunksWritten
  template splice(chunk: var seq[byte]) =
    let start = w.lastStepRecordStart
    if start < 0 or start >= chunk.len:
      return err("rewriteLastStepValues: recorded record offset " & $start &
        " is outside its chunk of " & $chunk.len & " bytes")
    var p = start
    var oldLen = 0'u64
    var shift = 0
    while true:
      let b = chunk[p]
      oldLen = oldLen or (uint64(b and 0x7F) shl shift)
      inc p
      if (b and 0x80) == 0: break
      shift += 7
    let oldEnd = p + int(oldLen)
    var rec: seq[byte] = @[]
    encodeRecord(values, extraEvents, rec)
    var replacement: seq[byte] = @[]
    encodeVarint(uint64(rec.len), replacement)
    replacement.add(rec)
    chunk = chunk[0 ..< start] & replacement & chunk[oldEnd ..< chunk.len]
  if pos < w.held.len:
    splice(w.held[pos])
  else:
    splice(w.buffer)
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
      chunk: initRecordChunk()))

  var datRes = readInternalFile(ctfsBytes, "values.dat", blockSize, maxEntries)
  if datRes.isErr:
    return err("failed to read values.dat: " & datRes.error)
  var datData = move datRes.get()

  let idxRes = readInternalFile(ctfsBytes, "values.idx", blockSize, maxEntries)
  if idxRes.isErr:
    return err("failed to read values.idx: " & idxRes.error)
  template idxData: untyped = idxRes.get()

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
  # The last chunk, inflated to count its records, stays held for the reads
  # that follow.
  var totalRecords: uint64 = 0
  var chunk = initRecordChunk()
  if numChunks > 0:
    let lastChunk = numChunks - 1
    let startOff = int(offsets[lastChunk])
    let endOff = datData.len
    if startOff > endOff:
      return err("last value chunk offset past end of values.dat")
    ? chunk.load(lastChunk, datData.toOpenArray(startOff, endOff - 1), "value")
    totalRecords = uint64(lastChunk) * uint64(chunkSize) + uint64(chunk.len)

  ok(ValueStreamReader(
    legacy: false,
    data: move datData,
    chunkSize: chunkSize,
    offsets: move offsets,
    totalRecordsVal: totalRecords,
    chunk: move chunk))

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
  ## Inflate whichever chunk holds ``stepIndex`` into ``r.chunk``
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

  if r.chunk.held != chunkNumber:
    let startOff = int(r.offsets[chunkNumber])
    let endOff =
      if chunkNumber + 1 < r.offsets.len: int(r.offsets[chunkNumber + 1])
      else: r.data.len
    if startOff > endOff or endOff > r.data.len:
      return err("value chunk offsets out of range")
    ? r.chunk.load(chunkNumber, r.data.toOpenArray(startOff, endOff - 1),
      "value")

  if within >= r.chunk.len:
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
  let res = decodeRecord(r.chunk.record(within), skipped)
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
  let res = decodeRecordDropVariable(r.chunk.record(within), skipped)
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
  let res = decodeRecordDropVariables(r.chunk.record(within), skipped)
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
  let res = decodeRecordAssignments(r.chunk.record(within), skipped)
  r.noteSkippedTags(skipped)
  res

proc readStepEvents*(r: var ValueStreamReader,
    stepIndex: uint64): Result[seq[DecodedValueEvent], string] =
  ## Every value-stream event of a step's record, tags 0-9, in wire order.
  r.lastSkippedTags.setLen(0)
  if r.legacy:
    let vals = ? r.readStepValues(stepIndex)
    if vals.len == 0:
      return ok(newSeq[DecodedValueEvent]())
    return ok(@[DecodedValueEvent(kind: veStepValues, values: vals)])
  let within = ?r.cacheRecordFor(stepIndex)
  var skipped: seq[uint8] = @[]
  let res = decodeRecordEvents(r.chunk.record(within), skipped)
  r.noteSkippedTags(skipped)
  if res.isErr:
    return err("values.dat record " & $stepIndex & ": " & res.error)
  res

proc lastSkippedTags*(r: ValueStreamReader): seq[uint8] =
  r.lastSkippedTags

proc skippedTags*(r: ValueStreamReader): seq[uint8] =
  r.skippedTags

proc skippedTagCounts*(r: ValueStreamReader): seq[(uint8, int)] =
  r.skippedTagCounts

