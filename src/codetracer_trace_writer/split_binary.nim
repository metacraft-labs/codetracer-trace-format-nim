{.push raises: [].}

## Split-binary event encoding/decoding for CodeTracer traces.
##
## Wire format per event:
##   1-byte tag (variant index 0..23)
##   Fixed fields: little-endian integers at natural width
##   Strings: 4-byte LE length + UTF-8 bytes
##   Dynamic payloads (ValueRecord, TypeRecord, AssignmentRecord):
##     4-byte LE payload length + CBOR-encoded bytes
##
## Dynamic payloads are CBOR-encoded to match the Rust split_binary
## module's format (which uses cbor4ii + serde). This enables
## cross-language trace compatibility.

import results
import stew/endians2
import ./cbor
import ../codetracer_trace_types
export codetracer_trace_types, results, cbor

# ===========================================================================
# Low-level binary helpers (optimized: bulk memcpy instead of byte-at-a-time)
# ===========================================================================

{.push checks: off, boundChecks: off.}

proc writeU8(out_buf: var seq[byte], v: byte) {.inline.} =
  out_buf.add(v)

proc writeU32(out_buf: var seq[byte], v: uint32) {.inline.} =
  let pos = out_buf.len
  out_buf.setLen(pos + 4)
  let le = toBytesLE(v)
  copyMem(addr out_buf[pos], unsafeAddr le[0], 4)

proc writeU64(out_buf: var seq[byte], v: uint64) {.inline.} =
  let pos = out_buf.len
  out_buf.setLen(pos + 8)
  let le = toBytesLE(v)
  copyMem(addr out_buf[pos], unsafeAddr le[0], 8)

proc writeI64(out_buf: var seq[byte], v: int64) {.inline.} =
  writeU64(out_buf, cast[uint64](v))

proc writeStr(out_buf: var seq[byte], s: string) {.inline.} =
  let lenLE = toBytesLE(uint32(s.len))
  let pos = out_buf.len
  out_buf.setLen(pos + 4 + s.len)
  copyMem(addr out_buf[pos], unsafeAddr lenLE[0], 4)
  if s.len > 0:
    copyMem(addr out_buf[pos + 4], unsafeAddr s[0], s.len)

proc writeF64(out_buf: var seq[byte], v: float64) {.inline.} =
  writeU64(out_buf, cast[uint64](v))

proc writeBool(out_buf: var seq[byte], v: bool) {.inline.} =
  out_buf.add(if v: 1'u8 else: 0'u8)

{.pop.} # checks: off, boundChecks: off

proc readU8(data: openArray[byte], pos: var int): Result[byte, string] =
  if pos >= data.len: return err("unexpected end of data reading u8")
  let v = data[pos]
  pos += 1
  ok(v)

proc readU32(data: openArray[byte], pos: var int): Result[uint32, string] =
  if pos + 4 > data.len: return err("unexpected end of data reading u32")
  let v = uint32(data[pos]) or
          (uint32(data[pos+1]) shl 8) or
          (uint32(data[pos+2]) shl 16) or
          (uint32(data[pos+3]) shl 24)
  pos += 4
  ok(v)

proc readU64(data: openArray[byte], pos: var int): Result[uint64, string] =
  if pos + 8 > data.len: return err("unexpected end of data reading u64")
  let v = uint64(data[pos]) or
          (uint64(data[pos+1]) shl 8) or
          (uint64(data[pos+2]) shl 16) or
          (uint64(data[pos+3]) shl 24) or
          (uint64(data[pos+4]) shl 32) or
          (uint64(data[pos+5]) shl 40) or
          (uint64(data[pos+6]) shl 48) or
          (uint64(data[pos+7]) shl 56)
  pos += 8
  ok(v)

proc readI64(data: openArray[byte], pos: var int): Result[int64, string] =
  let v = ?readU64(data, pos)
  ok(cast[int64](v))

proc readF64(data: openArray[byte], pos: var int): Result[float64, string] =
  let v = ?readU64(data, pos)
  ok(cast[float64](v))

proc readBool(data: openArray[byte], pos: var int): Result[bool, string] =
  let v = ?readU8(data, pos)
  ok(v != 0)

proc readStr(data: openArray[byte], pos: var int): Result[string, string] =
  let length = ?readU32(data, pos)
  let len = int(length)
  if pos + len > data.len: return err("unexpected end of data reading string")
  var s = newString(len)
  for i in 0 ..< len:
    s[i] = char(data[pos + i])
  pos += len
  ok(s)

proc readBytes(data: openArray[byte], pos: var int, count: int): Result[seq[byte], string] =
  if pos + count > data.len: return err("unexpected end of data reading bytes")
  var result_bytes = newSeq[byte](count)
  for i in 0 ..< count:
    result_bytes[i] = data[pos + i]
  pos += count
  ok(result_bytes)

# ===========================================================================
# CBOR-based encoding for dynamic payloads
# ===========================================================================
#
# Dynamic payloads (ValueRecord, TypeRecord, AssignmentRecord, etc.) are
# now encoded using CBOR to match the Rust split_binary module's format
# (cbor4ii + serde). The CBOR encoder/decoder is in cbor.nim.
#
# The functions below are thin wrappers that produce a seq[byte] payload
# from the CBOR encoder, and decode payloads using the CBOR decoder.

proc decodeCborPayloadValueRecord(data: openArray[byte]): Result[ValueRecord, string] =
  var dec = CborDecoder.init(data)
  dec.decodeCborValueRecord()

proc decodeCborPayloadTypeRecord(data: openArray[byte]): Result[TypeRecord, string] =
  var dec = CborDecoder.init(data)
  dec.decodeCborTypeRecord()

proc decodeCborPayloadAssignmentRecord(data: openArray[byte]): Result[AssignmentRecord, string] =
  var dec = CborDecoder.init(data)
  dec.decodeCborAssignmentRecord()

proc decodeCborPayloadFullValueRecord(data: openArray[byte]): Result[FullValueRecord, string] =
  var dec = CborDecoder.init(data)
  dec.decodeCborFullValueRecord()

proc decodeCborPayloadCallArgs(data: openArray[byte]): Result[seq[FullValueRecord], string] =
  var dec = CborDecoder.init(data)
  dec.decodeCborCallArgs()

# Direct-to-buffer CBOR payload writing (eliminates temp buffer allocation)
# Writes a 4-byte length placeholder, encodes CBOR directly into the main
# buffer, then patches the length.
template writePayloadCborDirect(out_buf: var seq[byte], body: untyped) =
  let lenPos = out_buf.len
  out_buf.setLen(lenPos + 4)
  let startPos = out_buf.len
  body
  let payloadLen = uint32(out_buf.len - startPos)
  let patchLE = toBytesLE(payloadLen)
  copyMem(addr out_buf[lenPos], unsafeAddr patchLE[0], 4)

# Encode procs that write CBOR directly into an external buffer (zero-copy via move)
proc encodeCborValueRecordInto*(buf: var seq[byte], v: ValueRecord) {.inline.} =
  var enc = CborEncoder(buf: move buf)
  enc.encodeCborValueRecord(v)
  buf = move enc.buf

proc encodeCborTypeRecordInto*(buf: var seq[byte], t: TypeRecord) {.inline.} =
  var enc = CborEncoder(buf: move buf)
  enc.encodeCborTypeRecord(t)
  buf = move enc.buf

proc encodeCborAssignmentRecordInto*(buf: var seq[byte], a: AssignmentRecord) {.inline.} =
  var enc = CborEncoder(buf: move buf)
  enc.encodeCborAssignmentRecord(a)
  buf = move enc.buf

proc encodeCborCallArgsInto*(buf: var seq[byte], args: seq[FullValueRecord]) {.inline.} =
  var enc = CborEncoder(buf: move buf)
  enc.encodeCborCallArgs(args)
  buf = move enc.buf

# Helper: write a payload with 4-byte LE length prefix
proc writePayload(out_buf: var seq[byte], payload: seq[byte]) {.inline.} =
  let pos = out_buf.len
  out_buf.setLen(pos + 4 + payload.len)
  let lenLE = toBytesLE(uint32(payload.len))
  copyMem(addr out_buf[pos], unsafeAddr lenLE[0], 4)
  if payload.len > 0:
    copyMem(addr out_buf[pos + 4], unsafeAddr payload[0], payload.len)

proc readPayload(data: openArray[byte], pos: var int): Result[seq[byte], string] =
  let length = ?readU32(data, pos)
  readBytes(data, pos, int(length))

# ===========================================================================
# SplitBinaryEncoder
# ===========================================================================

type
  SplitBinaryEncoder* = object
    buf*: seq[byte]

proc init*(T: type SplitBinaryEncoder, capacity: int = 65536): SplitBinaryEncoder =
  result.buf = newSeqOfCap[byte](capacity)

proc clear*(enc: var SplitBinaryEncoder) =
  enc.buf.setLen(0)

proc getBytes*(enc: SplitBinaryEncoder): seq[byte] =
  enc.buf

{.push checks: off, boundChecks: off.}

proc encodeEvent*(enc: var SplitBinaryEncoder, event: TraceLowLevelEvent) =
  ## Encode a single TraceLowLevelEvent into the encoder's buffer.
  case event.kind
  of tleStep:
    writeU8(enc.buf, 0)
    writeU64(enc.buf, uint64(event.step.pathId))
    writeI64(enc.buf, int64(event.step.line))

  of tlePath:
    writeU8(enc.buf, 1)
    writeStr(enc.buf, event.path)

  of tleVariableName:
    writeU8(enc.buf, 2)
    writeStr(enc.buf, event.varName)

  of tleVariable:
    writeU8(enc.buf, 3)
    writeStr(enc.buf, event.variable)

  of tleType:
    writeU8(enc.buf, 4)
    writePayloadCborDirect(enc.buf):
      encodeCborTypeRecordInto(enc.buf, event.typeRecord)

  of tleValue:
    writeU8(enc.buf, 5)
    writeU64(enc.buf, uint64(event.fullValue.variableId))
    writePayloadCborDirect(enc.buf):
      encodeCborValueRecordInto(enc.buf, event.fullValue.value)

  of tleFunction:
    writeU8(enc.buf, 6)
    writeU64(enc.buf, uint64(event.functionRecord.pathId))
    writeI64(enc.buf, int64(event.functionRecord.line))
    writeStr(enc.buf, event.functionRecord.name)

  of tleCall:
    writeU8(enc.buf, 7)
    writeU64(enc.buf, uint64(event.callRecord.functionId))
    writePayloadCborDirect(enc.buf):
      encodeCborCallArgsInto(enc.buf, event.callRecord.args)

  of tleReturn:
    writeU8(enc.buf, 8)
    writePayloadCborDirect(enc.buf):
      encodeCborValueRecordInto(enc.buf, event.returnRecord.returnValue)

  of tleEvent:
    writeU8(enc.buf, 9)
    writeU8(enc.buf, byte(ord(event.recordEvent.kind)))
    writeStr(enc.buf, event.recordEvent.metadata)
    writeStr(enc.buf, event.recordEvent.content)

  of tleAsm:
    writeU8(enc.buf, 10)
    writeU32(enc.buf, uint32(event.asmLines.len))
    for line in event.asmLines:
      writeStr(enc.buf, line)

  of tleBindVariable:
    writeU8(enc.buf, 11)
    writeU64(enc.buf, uint64(event.bindVar.variableId))
    writeI64(enc.buf, int64(event.bindVar.place))

  of tleAssignment:
    writeU8(enc.buf, 12)
    writePayloadCborDirect(enc.buf):
      encodeCborAssignmentRecordInto(enc.buf, event.assignment)

  of tleDropVariables:
    writeU8(enc.buf, 13)
    writeU32(enc.buf, uint32(event.dropVarIds.len))
    for id in event.dropVarIds:
      writeU64(enc.buf, uint64(id))

  of tleCompoundValue:
    writeU8(enc.buf, 14)
    writeI64(enc.buf, int64(event.compoundValue.place))
    writePayloadCborDirect(enc.buf):
      encodeCborValueRecordInto(enc.buf, event.compoundValue.value)

  of tleCellValue:
    writeU8(enc.buf, 15)
    writeI64(enc.buf, int64(event.cellValue.place))
    writePayloadCborDirect(enc.buf):
      encodeCborValueRecordInto(enc.buf, event.cellValue.value)

  of tleAssignCompoundItem:
    writeU8(enc.buf, 16)
    writeI64(enc.buf, int64(event.assignCompoundItem.place))
    writeU64(enc.buf, event.assignCompoundItem.index)
    writeI64(enc.buf, int64(event.assignCompoundItem.itemPlace))

  of tleAssignCell:
    writeU8(enc.buf, 17)
    writeI64(enc.buf, int64(event.assignCell.place))
    writePayloadCborDirect(enc.buf):
      encodeCborValueRecordInto(enc.buf, event.assignCell.newValue)

  of tleVariableCell:
    writeU8(enc.buf, 18)
    writeU64(enc.buf, uint64(event.variableCell.variableId))
    writeI64(enc.buf, int64(event.variableCell.place))

  of tleDropVariable:
    writeU8(enc.buf, 19)
    writeU64(enc.buf, uint64(event.dropVarId))

  of tleThreadStart:
    writeU8(enc.buf, 20)
    writeU64(enc.buf, uint64(event.threadStartId))

  of tleThreadExit:
    writeU8(enc.buf, 21)
    writeU64(enc.buf, uint64(event.threadExitId))

  of tleThreadSwitch:
    writeU8(enc.buf, 22)
    writeU64(enc.buf, uint64(event.threadSwitchId))

  of tleDropLastStep:
    writeU8(enc.buf, 23)

{.pop.} # checks: off, boundChecks: off

# ===========================================================================
# SplitBinaryDecoder
# ===========================================================================

proc decodeEvent*(data: openArray[byte], pos: var int): Result[TraceLowLevelEvent, string] =
  ## Decode a single TraceLowLevelEvent from data starting at pos.
  let tag = ?readU8(data, pos)
  case tag
  of 0: # Step
    let pathId = ?readU64(data, pos)
    let line = ?readI64(data, pos)
    ok(TraceLowLevelEvent(kind: tleStep,
      step: StepRecord(pathId: PathId(pathId), line: Line(line))))

  of 1: # Path
    let s = ?readStr(data, pos)
    ok(TraceLowLevelEvent(kind: tlePath, path: s))

  of 2: # VariableName
    let s = ?readStr(data, pos)
    ok(TraceLowLevelEvent(kind: tleVariableName, varName: s))

  of 3: # Variable
    let s = ?readStr(data, pos)
    ok(TraceLowLevelEvent(kind: tleVariable, variable: s))

  of 4: # Type
    let payload = ?readPayload(data, pos)
    let tr = ?decodeCborPayloadTypeRecord(payload)
    ok(TraceLowLevelEvent(kind: tleType, typeRecord: tr))

  of 5: # Value
    let varId = ?readU64(data, pos)
    let payload = ?readPayload(data, pos)
    let value = ?decodeCborPayloadValueRecord(payload)
    ok(TraceLowLevelEvent(kind: tleValue,
      fullValue: FullValueRecord(variableId: VariableId(varId), value: value)))

  of 6: # Function
    let pathId = ?readU64(data, pos)
    let line = ?readI64(data, pos)
    let name = ?readStr(data, pos)
    ok(TraceLowLevelEvent(kind: tleFunction,
      functionRecord: FunctionRecord(pathId: PathId(pathId), line: Line(line), name: name)))

  of 7: # Call
    let funcId = ?readU64(data, pos)
    let payload = ?readPayload(data, pos)
    let args = ?decodeCborPayloadCallArgs(payload)
    ok(TraceLowLevelEvent(kind: tleCall,
      callRecord: CallRecord(functionId: FunctionId(funcId), args: args)))

  of 8: # Return
    let payload = ?readPayload(data, pos)
    let rv = ?decodeCborPayloadValueRecord(payload)
    ok(TraceLowLevelEvent(kind: tleReturn,
      returnRecord: ReturnRecord(returnValue: rv)))

  of 9: # Event
    let kindByte = ?readU8(data, pos)
    if int(kindByte) > ord(high(EventLogKind)):
      return err("invalid EventLogKind: " & $kindByte)
    let kind = EventLogKind(kindByte)
    let metadata = ?readStr(data, pos)
    let content = ?readStr(data, pos)
    ok(TraceLowLevelEvent(kind: tleEvent,
      recordEvent: RecordEvent(kind: kind, metadata: metadata, content: content)))

  of 10: # Asm
    let count = ?readU32(data, pos)
    var lines = newSeq[string](int(count))
    for i in 0 ..< int(count):
      lines[i] = ?readStr(data, pos)
    ok(TraceLowLevelEvent(kind: tleAsm, asmLines: lines))

  of 11: # BindVariable
    let varId = ?readU64(data, pos)
    let place = ?readI64(data, pos)
    ok(TraceLowLevelEvent(kind: tleBindVariable,
      bindVar: BindVariableRecord(variableId: VariableId(varId), place: Place(place))))

  of 12: # Assignment
    let payload = ?readPayload(data, pos)
    let ar = ?decodeCborPayloadAssignmentRecord(payload)
    ok(TraceLowLevelEvent(kind: tleAssignment, assignment: ar))

  of 13: # DropVariables
    let count = ?readU32(data, pos)
    var ids = newSeq[VariableId](int(count))
    for i in 0 ..< int(count):
      let id = ?readU64(data, pos)
      ids[i] = VariableId(id)
    ok(TraceLowLevelEvent(kind: tleDropVariables, dropVarIds: ids))

  of 14: # CompoundValue
    let place = ?readI64(data, pos)
    let payload = ?readPayload(data, pos)
    let value = ?decodeCborPayloadValueRecord(payload)
    ok(TraceLowLevelEvent(kind: tleCompoundValue,
      compoundValue: CompoundValueRecord(place: Place(place), value: value)))

  of 15: # CellValue
    let place = ?readI64(data, pos)
    let payload = ?readPayload(data, pos)
    let value = ?decodeCborPayloadValueRecord(payload)
    ok(TraceLowLevelEvent(kind: tleCellValue,
      cellValue: CellValueRecord(place: Place(place), value: value)))

  of 16: # AssignCompoundItem
    let place = ?readI64(data, pos)
    let index = ?readU64(data, pos)
    let itemPlace = ?readI64(data, pos)
    ok(TraceLowLevelEvent(kind: tleAssignCompoundItem,
      assignCompoundItem: AssignCompoundItemRecord(
        place: Place(place), index: index, itemPlace: Place(itemPlace))))

  of 17: # AssignCell
    let place = ?readI64(data, pos)
    let payload = ?readPayload(data, pos)
    let nv = ?decodeCborPayloadValueRecord(payload)
    ok(TraceLowLevelEvent(kind: tleAssignCell,
      assignCell: AssignCellRecord(place: Place(place), newValue: nv)))

  of 18: # VariableCell
    let varId = ?readU64(data, pos)
    let place = ?readI64(data, pos)
    ok(TraceLowLevelEvent(kind: tleVariableCell,
      variableCell: VariableCellRecord(variableId: VariableId(varId), place: Place(place))))

  of 19: # DropVariable
    let varId = ?readU64(data, pos)
    ok(TraceLowLevelEvent(kind: tleDropVariable, dropVarId: VariableId(varId)))

  of 20: # ThreadStart
    let tid = ?readU64(data, pos)
    ok(TraceLowLevelEvent(kind: tleThreadStart, threadStartId: ThreadId(tid)))

  of 21: # ThreadExit
    let tid = ?readU64(data, pos)
    ok(TraceLowLevelEvent(kind: tleThreadExit, threadExitId: ThreadId(tid)))

  of 22: # ThreadSwitch
    let tid = ?readU64(data, pos)
    ok(TraceLowLevelEvent(kind: tleThreadSwitch, threadSwitchId: ThreadId(tid)))

  of 23: # DropLastStep
    ok(TraceLowLevelEvent(kind: tleDropLastStep))

  else:
    err("unknown event tag: " & $tag)

proc decodeAllEvents*(data: openArray[byte]): Result[seq[TraceLowLevelEvent], string] =
  ## Decode all events from a byte buffer.
  var events: seq[TraceLowLevelEvent]
  var pos = 0
  while pos < data.len:
    let event = ?decodeEvent(data, pos)
    events.add(event)
  ok(events)
