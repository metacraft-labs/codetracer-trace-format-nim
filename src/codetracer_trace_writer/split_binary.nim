{.push raises: [].}

## Split-binary event encoding/decoding for CodeTracer traces.
##
## Wire format per event:
##   1-byte tag (variant index 0..23)
##   Fixed fields: little-endian integers at natural width
##   Strings: 4-byte LE length + UTF-8 bytes
##   Dynamic payloads (ValueRecord, TypeRecord, AssignmentRecord):
##     4-byte LE payload length + Nim-native binary bytes
##
## This matches the Rust split_binary module's layout for all
## fixed-size and string fields. The dynamic payloads use a
## Nim-native encoding (not CBOR) so traces written by Nim are
## currently only readable by Nim.

import results
import ../codetracer_trace_types
export codetracer_trace_types, results

# ===========================================================================
# Low-level binary helpers
# ===========================================================================

proc writeU8(out_buf: var seq[byte], v: byte) {.inline.} =
  out_buf.add(v)

proc writeU32(out_buf: var seq[byte], v: uint32) {.inline.} =
  out_buf.add(byte(v and 0xFF))
  out_buf.add(byte((v shr 8) and 0xFF))
  out_buf.add(byte((v shr 16) and 0xFF))
  out_buf.add(byte((v shr 24) and 0xFF))

proc writeU64(out_buf: var seq[byte], v: uint64) {.inline.} =
  out_buf.add(byte(v and 0xFF))
  out_buf.add(byte((v shr 8) and 0xFF))
  out_buf.add(byte((v shr 16) and 0xFF))
  out_buf.add(byte((v shr 24) and 0xFF))
  out_buf.add(byte((v shr 32) and 0xFF))
  out_buf.add(byte((v shr 40) and 0xFF))
  out_buf.add(byte((v shr 48) and 0xFF))
  out_buf.add(byte((v shr 56) and 0xFF))

proc writeI64(out_buf: var seq[byte], v: int64) {.inline.} =
  writeU64(out_buf, cast[uint64](v))

proc writeStr(out_buf: var seq[byte], s: string) =
  writeU32(out_buf, uint32(s.len))
  for c in s:
    out_buf.add(byte(c))

proc writeF64(out_buf: var seq[byte], v: float64) {.inline.} =
  writeU64(out_buf, cast[uint64](v))

proc writeBool(out_buf: var seq[byte], v: bool) {.inline.} =
  out_buf.add(if v: 1'u8 else: 0'u8)

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
# Native binary encoding for dynamic payloads
# ===========================================================================

# Forward declarations
proc writeValueRecord(out_buf: var seq[byte], v: ValueRecord) {.raises: [].}
proc readValueRecord(data: openArray[byte], pos: var int): Result[ValueRecord, string] {.raises: [].}

proc writeTypeRecord(out_buf: var seq[byte], t: TypeRecord) =
  writeU8(out_buf, byte(ord(t.kind)))
  writeStr(out_buf, t.langType)
  writeU8(out_buf, byte(ord(t.specificInfo.kind)))
  case t.specificInfo.kind
  of tsikNone: discard
  of tsikStruct:
    writeU32(out_buf, uint32(t.specificInfo.fields.len))
    for f in t.specificInfo.fields:
      writeStr(out_buf, f.name)
      writeU64(out_buf, uint64(f.typeId))
  of tsikPointer:
    writeU64(out_buf, uint64(t.specificInfo.dereferenceTypeId))

proc readTypeRecord(data: openArray[byte], pos: var int): Result[TypeRecord, string] =
  let kindByte = ?readU8(data, pos)
  if int(kindByte) > ord(high(TypeKind)):
    return err("invalid TypeKind: " & $kindByte)
  let kind = TypeKind(kindByte)
  let langType = ?readStr(data, pos)
  let siKindByte = ?readU8(data, pos)
  if int(siKindByte) > ord(high(TypeSpecificInfoKind)):
    return err("invalid TypeSpecificInfoKind: " & $siKindByte)
  let siKind = TypeSpecificInfoKind(siKindByte)
  var si: TypeSpecificInfo
  case siKind
  of tsikNone:
    si = TypeSpecificInfo(kind: tsikNone)
  of tsikStruct:
    let count = ?readU32(data, pos)
    var fields = newSeq[FieldTypeRecord](int(count))
    for i in 0 ..< int(count):
      let name = ?readStr(data, pos)
      let tid = ?readU64(data, pos)
      fields[i] = FieldTypeRecord(name: name, typeId: TypeId(tid))
    si = TypeSpecificInfo(kind: tsikStruct, fields: fields)
  of tsikPointer:
    let tid = ?readU64(data, pos)
    si = TypeSpecificInfo(kind: tsikPointer, dereferenceTypeId: TypeId(tid))
  ok(TypeRecord(kind: kind, langType: langType, specificInfo: si))

proc writeValueRecord(out_buf: var seq[byte], v: ValueRecord) =
  writeU8(out_buf, byte(ord(v.kind)))
  case v.kind
  of vrkInt:
    writeI64(out_buf, v.intVal)
    writeU64(out_buf, uint64(v.intTypeId))
  of vrkFloat:
    writeF64(out_buf, v.floatVal)
    writeU64(out_buf, uint64(v.floatTypeId))
  of vrkBool:
    writeBool(out_buf, v.boolVal)
    writeU64(out_buf, uint64(v.boolTypeId))
  of vrkString:
    writeStr(out_buf, v.text)
    writeU64(out_buf, uint64(v.strTypeId))
  of vrkSequence:
    writeU32(out_buf, uint32(v.seqElements.len))
    for e in v.seqElements:
      writeValueRecord(out_buf, e)
    writeBool(out_buf, v.isSlice)
    writeU64(out_buf, uint64(v.seqTypeId))
  of vrkTuple:
    writeU32(out_buf, uint32(v.tupleElements.len))
    for e in v.tupleElements:
      writeValueRecord(out_buf, e)
    writeU64(out_buf, uint64(v.tupleTypeId))
  of vrkStruct:
    writeU32(out_buf, uint32(v.fieldValues.len))
    for e in v.fieldValues:
      writeValueRecord(out_buf, e)
    writeU64(out_buf, uint64(v.structTypeId))
  of vrkVariant:
    writeStr(out_buf, v.discriminator)
    if v.contents.len > 0:
      writeBool(out_buf, true)
      writeValueRecord(out_buf, v.contents[0])
    else:
      writeBool(out_buf, false)
    writeU64(out_buf, uint64(v.variantTypeId))
  of vrkReference:
    if v.dereferenced.len > 0:
      writeBool(out_buf, true)
      writeValueRecord(out_buf, v.dereferenced[0])
    else:
      writeBool(out_buf, false)
    writeU64(out_buf, v.address)
    writeBool(out_buf, v.mutable)
    writeU64(out_buf, uint64(v.refTypeId))
  of vrkRaw:
    writeStr(out_buf, v.rawStr)
    writeU64(out_buf, uint64(v.rawTypeId))
  of vrkError:
    writeStr(out_buf, v.errorMsg)
    writeU64(out_buf, uint64(v.errorTypeId))
  of vrkNone:
    writeU64(out_buf, uint64(v.noneTypeId))
  of vrkCell:
    writeI64(out_buf, int64(v.cellPlace))
  of vrkBigInt:
    writeU32(out_buf, uint32(v.bigIntBytes.len))
    for b in v.bigIntBytes:
      out_buf.add(b)
    writeBool(out_buf, v.negative)
    writeU64(out_buf, uint64(v.bigIntTypeId))
  of vrkChar:
    writeU8(out_buf, byte(v.charVal))
    writeU64(out_buf, uint64(v.charTypeId))

proc readValueRecord(data: openArray[byte], pos: var int): Result[ValueRecord, string] =
  let kindByte = ?readU8(data, pos)
  if int(kindByte) > ord(high(ValueRecordKind)):
    return err("invalid ValueRecordKind: " & $kindByte)
  let kind = ValueRecordKind(kindByte)
  case kind
  of vrkInt:
    let i = ?readI64(data, pos)
    let tid = ?readU64(data, pos)
    ok(ValueRecord(kind: vrkInt, intVal: i, intTypeId: TypeId(tid)))
  of vrkFloat:
    let f = ?readF64(data, pos)
    let tid = ?readU64(data, pos)
    ok(ValueRecord(kind: vrkFloat, floatVal: f, floatTypeId: TypeId(tid)))
  of vrkBool:
    let b = ?readBool(data, pos)
    let tid = ?readU64(data, pos)
    ok(ValueRecord(kind: vrkBool, boolVal: b, boolTypeId: TypeId(tid)))
  of vrkString:
    let text = ?readStr(data, pos)
    let tid = ?readU64(data, pos)
    ok(ValueRecord(kind: vrkString, text: text, strTypeId: TypeId(tid)))
  of vrkSequence:
    let count = ?readU32(data, pos)
    var elems = newSeq[ValueRecord](int(count))
    for i in 0 ..< int(count):
      elems[i] = ?readValueRecord(data, pos)
    let isSlice = ?readBool(data, pos)
    let tid = ?readU64(data, pos)
    ok(ValueRecord(kind: vrkSequence, seqElements: elems, isSlice: isSlice, seqTypeId: TypeId(tid)))
  of vrkTuple:
    let count = ?readU32(data, pos)
    var elems = newSeq[ValueRecord](int(count))
    for i in 0 ..< int(count):
      elems[i] = ?readValueRecord(data, pos)
    let tid = ?readU64(data, pos)
    ok(ValueRecord(kind: vrkTuple, tupleElements: elems, tupleTypeId: TypeId(tid)))
  of vrkStruct:
    let count = ?readU32(data, pos)
    var elems = newSeq[ValueRecord](int(count))
    for i in 0 ..< int(count):
      elems[i] = ?readValueRecord(data, pos)
    let tid = ?readU64(data, pos)
    ok(ValueRecord(kind: vrkStruct, fieldValues: elems, structTypeId: TypeId(tid)))
  of vrkVariant:
    let disc = ?readStr(data, pos)
    let hasContents = ?readBool(data, pos)
    var contents: seq[ValueRecord]
    if hasContents:
      contents = @[?readValueRecord(data, pos)]
    let tid = ?readU64(data, pos)
    ok(ValueRecord(kind: vrkVariant, discriminator: disc, contents: contents, variantTypeId: TypeId(tid)))
  of vrkReference:
    let hasDeref = ?readBool(data, pos)
    var deref: seq[ValueRecord]
    if hasDeref:
      deref = @[?readValueRecord(data, pos)]
    let address = ?readU64(data, pos)
    let mutable = ?readBool(data, pos)
    let tid = ?readU64(data, pos)
    ok(ValueRecord(kind: vrkReference, dereferenced: deref, address: address, mutable: mutable, refTypeId: TypeId(tid)))
  of vrkRaw:
    let r = ?readStr(data, pos)
    let tid = ?readU64(data, pos)
    ok(ValueRecord(kind: vrkRaw, rawStr: r, rawTypeId: TypeId(tid)))
  of vrkError:
    let msg = ?readStr(data, pos)
    let tid = ?readU64(data, pos)
    ok(ValueRecord(kind: vrkError, errorMsg: msg, errorTypeId: TypeId(tid)))
  of vrkNone:
    let tid = ?readU64(data, pos)
    ok(ValueRecord(kind: vrkNone, noneTypeId: TypeId(tid)))
  of vrkCell:
    let p = ?readI64(data, pos)
    ok(ValueRecord(kind: vrkCell, cellPlace: Place(p)))
  of vrkBigInt:
    let count = ?readU32(data, pos)
    let bytes = ?readBytes(data, pos, int(count))
    let neg = ?readBool(data, pos)
    let tid = ?readU64(data, pos)
    ok(ValueRecord(kind: vrkBigInt, bigIntBytes: bytes, negative: neg, bigIntTypeId: TypeId(tid)))
  of vrkChar:
    let c = ?readU8(data, pos)
    let tid = ?readU64(data, pos)
    ok(ValueRecord(kind: vrkChar, charVal: char(c), charTypeId: TypeId(tid)))

proc writeFullValueRecord(out_buf: var seq[byte], fvr: FullValueRecord) =
  writeU64(out_buf, uint64(fvr.variableId))
  writeValueRecord(out_buf, fvr.value)

proc readFullValueRecord(data: openArray[byte], pos: var int): Result[FullValueRecord, string] =
  let vid = ?readU64(data, pos)
  let value = ?readValueRecord(data, pos)
  ok(FullValueRecord(variableId: VariableId(vid), value: value))

proc writeAssignmentRecord(out_buf: var seq[byte], a: AssignmentRecord) =
  writeU64(out_buf, uint64(a.to))
  writeU8(out_buf, byte(ord(a.passBy)))
  writeU8(out_buf, byte(ord(a.frm.kind)))
  case a.frm.kind
  of rvkSimple:
    writeU64(out_buf, uint64(a.frm.simpleId))
  of rvkCompound:
    writeU32(out_buf, uint32(a.frm.compoundIds.len))
    for id in a.frm.compoundIds:
      writeU64(out_buf, uint64(id))

proc readAssignmentRecord(data: openArray[byte], pos: var int): Result[AssignmentRecord, string] =
  let toId = ?readU64(data, pos)
  let passByByte = ?readU8(data, pos)
  if int(passByByte) > ord(high(PassBy)):
    return err("invalid PassBy: " & $passByByte)
  let passBy = PassBy(passByByte)
  let rvKindByte = ?readU8(data, pos)
  if int(rvKindByte) > ord(high(RValueKind)):
    return err("invalid RValueKind: " & $rvKindByte)
  let rvKind = RValueKind(rvKindByte)
  var frm: RValue
  case rvKind
  of rvkSimple:
    let sid = ?readU64(data, pos)
    frm = RValue(kind: rvkSimple, simpleId: VariableId(sid))
  of rvkCompound:
    let count = ?readU32(data, pos)
    var ids = newSeq[VariableId](int(count))
    for i in 0 ..< int(count):
      let id = ?readU64(data, pos)
      ids[i] = VariableId(id)
    frm = RValue(kind: rvkCompound, compoundIds: ids)
  ok(AssignmentRecord(to: VariableId(toId), passBy: passBy, frm: frm))

# Helper: write a payload with 4-byte LE length prefix
proc writePayload(out_buf: var seq[byte], payload: seq[byte]) =
  writeU32(out_buf, uint32(payload.len))
  for b in payload:
    out_buf.add(b)

proc readPayload(data: openArray[byte], pos: var int): Result[seq[byte], string] =
  let length = ?readU32(data, pos)
  readBytes(data, pos, int(length))

# ===========================================================================
# SplitBinaryEncoder
# ===========================================================================

type
  SplitBinaryEncoder* = object
    buf*: seq[byte]

proc init*(T: type SplitBinaryEncoder): SplitBinaryEncoder =
  SplitBinaryEncoder(buf: @[])

proc clear*(enc: var SplitBinaryEncoder) =
  enc.buf.setLen(0)

proc getBytes*(enc: SplitBinaryEncoder): seq[byte] =
  enc.buf

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
    var payload: seq[byte]
    writeTypeRecord(payload, event.typeRecord)
    writePayload(enc.buf, payload)

  of tleValue:
    writeU8(enc.buf, 5)
    writeU64(enc.buf, uint64(event.fullValue.variableId))
    var payload: seq[byte]
    writeValueRecord(payload, event.fullValue.value)
    writePayload(enc.buf, payload)

  of tleFunction:
    writeU8(enc.buf, 6)
    writeU64(enc.buf, uint64(event.functionRecord.pathId))
    writeI64(enc.buf, int64(event.functionRecord.line))
    writeStr(enc.buf, event.functionRecord.name)

  of tleCall:
    writeU8(enc.buf, 7)
    writeU64(enc.buf, uint64(event.callRecord.functionId))
    var payload: seq[byte]
    writeU32(payload, uint32(event.callRecord.args.len))
    for arg in event.callRecord.args:
      writeFullValueRecord(payload, arg)
    writePayload(enc.buf, payload)

  of tleReturn:
    writeU8(enc.buf, 8)
    var payload: seq[byte]
    writeValueRecord(payload, event.returnRecord.returnValue)
    writePayload(enc.buf, payload)

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
    var payload: seq[byte]
    writeAssignmentRecord(payload, event.assignment)
    writePayload(enc.buf, payload)

  of tleDropVariables:
    writeU8(enc.buf, 13)
    writeU32(enc.buf, uint32(event.dropVarIds.len))
    for id in event.dropVarIds:
      writeU64(enc.buf, uint64(id))

  of tleCompoundValue:
    writeU8(enc.buf, 14)
    writeI64(enc.buf, int64(event.compoundValue.place))
    var payload: seq[byte]
    writeValueRecord(payload, event.compoundValue.value)
    writePayload(enc.buf, payload)

  of tleCellValue:
    writeU8(enc.buf, 15)
    writeI64(enc.buf, int64(event.cellValue.place))
    var payload: seq[byte]
    writeValueRecord(payload, event.cellValue.value)
    writePayload(enc.buf, payload)

  of tleAssignCompoundItem:
    writeU8(enc.buf, 16)
    writeI64(enc.buf, int64(event.assignCompoundItem.place))
    writeU64(enc.buf, event.assignCompoundItem.index)
    writeI64(enc.buf, int64(event.assignCompoundItem.itemPlace))

  of tleAssignCell:
    writeU8(enc.buf, 17)
    writeI64(enc.buf, int64(event.assignCell.place))
    var payload: seq[byte]
    writeValueRecord(payload, event.assignCell.newValue)
    writePayload(enc.buf, payload)

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
    var ppos = 0
    let tr = ?readTypeRecord(payload, ppos)
    ok(TraceLowLevelEvent(kind: tleType, typeRecord: tr))

  of 5: # Value
    let varId = ?readU64(data, pos)
    let payload = ?readPayload(data, pos)
    var ppos = 0
    let value = ?readValueRecord(payload, ppos)
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
    var ppos = 0
    let count = ?readU32(payload, ppos)
    var args = newSeq[FullValueRecord](int(count))
    for i in 0 ..< int(count):
      args[i] = ?readFullValueRecord(payload, ppos)
    ok(TraceLowLevelEvent(kind: tleCall,
      callRecord: CallRecord(functionId: FunctionId(funcId), args: args)))

  of 8: # Return
    let payload = ?readPayload(data, pos)
    var ppos = 0
    let rv = ?readValueRecord(payload, ppos)
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
    var ppos = 0
    let ar = ?readAssignmentRecord(payload, ppos)
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
    var ppos = 0
    let value = ?readValueRecord(payload, ppos)
    ok(TraceLowLevelEvent(kind: tleCompoundValue,
      compoundValue: CompoundValueRecord(place: Place(place), value: value)))

  of 15: # CellValue
    let place = ?readI64(data, pos)
    let payload = ?readPayload(data, pos)
    var ppos = 0
    let value = ?readValueRecord(payload, ppos)
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
    var ppos = 0
    let nv = ?readValueRecord(payload, ppos)
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
