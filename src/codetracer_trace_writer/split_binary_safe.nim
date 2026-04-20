{.push raises: [].}

## Split-binary event encoding using SafeBuffer (safe, GC-managed memory).
## This is a drop-in replacement for the FastBuffer-based encoder.
## Same wire format, same API, but uses seq[byte] with cursor instead of raw alloc.

import results
import stew/endians2
import ./cbor
import ./safe_buffer
import ../codetracer_trace_types
export codetracer_trace_types, results, cbor, safe_buffer

# Direct-to-buffer CBOR payload writing using SafeBuffer.
# Writes a 4-byte length placeholder, encodes CBOR directly into the buffer,
# then patches the length.
template writePayloadCborDirect(buf: var SafeBuffer, body: untyped) =
  let lenPos = buf.pos
  # Reserve 4 bytes for length
  ensureCapacity(buf, 4)
  buf.pos += 4
  let startPos = buf.pos
  body
  let payloadLen = uint32(buf.pos - startPos)
  copyMem(addr buf.data[lenPos], unsafeAddr payloadLen, 4)

# CBOR encoding directly into SafeBuffer
proc encodeCborValueRecordInto*(buf: var SafeBuffer, v: ValueRecord) {.inline.}
proc encodeCborTypeRecordInto*(buf: var SafeBuffer, t: TypeRecord) {.inline.}
proc encodeCborAssignmentRecordInto*(buf: var SafeBuffer, a: AssignmentRecord) {.inline.}
proc encodeCborCallArgsInto*(buf: var SafeBuffer, args: seq[FullValueRecord]) {.inline.}
proc encodeCborFullValueRecordInto*(buf: var SafeBuffer, fvr: FullValueRecord) {.inline.}

# Pre-computed CBOR key bytes
const
  CborKeyKind2 = [0x64'u8, 0x6B, 0x69, 0x6E, 0x64]
  CborKeyI2 = [0x61'u8, 0x69]
  CborKeyTypeId2 = [0x67'u8, 0x74, 0x79, 0x70, 0x65, 0x5F, 0x69, 0x64]
  CborKeyF2 = [0x61'u8, 0x66]
  CborKeyB2 = [0x61'u8, 0x62]
  CborKeyText2 = [0x64'u8, 0x74, 0x65, 0x78, 0x74]
  CborKeyElements2 = [0x68'u8, 0x65, 0x6C, 0x65, 0x6D, 0x65, 0x6E, 0x74, 0x73]
  CborKeyIsSlice2 = [0x68'u8, 0x69, 0x73, 0x5F, 0x73, 0x6C, 0x69, 0x63, 0x65]
  CborKeyFieldValues2 = [0x6C'u8, 0x66, 0x69, 0x65, 0x6C, 0x64, 0x5F, 0x76, 0x61, 0x6C, 0x75, 0x65, 0x73]
  CborKeyDiscriminator2 = [0x6D'u8, 0x64, 0x69, 0x73, 0x63, 0x72, 0x69, 0x6D, 0x69, 0x6E, 0x61, 0x74, 0x6F, 0x72]
  CborKeyContents2 = [0x68'u8, 0x63, 0x6F, 0x6E, 0x74, 0x65, 0x6E, 0x74, 0x73]
  CborKeyDereferenced2 = [0x6C'u8, 0x64, 0x65, 0x72, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6E, 0x63, 0x65, 0x64]
  CborKeyAddress2 = [0x67'u8, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73]
  CborKeyMutable2 = [0x67'u8, 0x6D, 0x75, 0x74, 0x61, 0x62, 0x6C, 0x65]
  CborKeyR2 = [0x61'u8, 0x72]
  CborKeyMsg2 = [0x63'u8, 0x6D, 0x73, 0x67]
  CborKeyPlace2 = [0x65'u8, 0x70, 0x6C, 0x61, 0x63, 0x65]
  CborKeyNegative2 = [0x68'u8, 0x6E, 0x65, 0x67, 0x61, 0x74, 0x69, 0x76, 0x65]
  CborKeyC2 = [0x61'u8, 0x63]
  CborKeyTo2 = [0x62'u8, 0x74, 0x6F]
  CborKeyPassBy2 = [0x67'u8, 0x70, 0x61, 0x73, 0x73, 0x5F, 0x62, 0x79]
  CborKeyFrom2 = [0x64'u8, 0x66, 0x72, 0x6F, 0x6D]
  CborKey02 = [0x61'u8, 0x30]
  CborKeyVariableId2 = [0x6B'u8, 0x76, 0x61, 0x72, 0x69, 0x61, 0x62, 0x6C, 0x65, 0x5F, 0x69, 0x64]
  CborKeyValue2 = [0x65'u8, 0x76, 0x61, 0x6C, 0x75, 0x65]
  CborKeyLangType2 = [0x69'u8, 0x6C, 0x61, 0x6E, 0x67, 0x5F, 0x74, 0x79, 0x70, 0x65]
  CborKeySpecificInfo2 = [0x6D'u8, 0x73, 0x70, 0x65, 0x63, 0x69, 0x66, 0x69, 0x63, 0x5F, 0x69, 0x6E, 0x66, 0x6F]
  CborKeyFields2 = [0x66'u8, 0x66, 0x69, 0x65, 0x6C, 0x64, 0x73]
  CborKeyName2 = [0x64'u8, 0x6E, 0x61, 0x6D, 0x65]
  CborKeyDereferenceTypeId2 = [0x73'u8, 0x64, 0x65, 0x72, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6E, 0x63, 0x65, 0x5F, 0x74, 0x79, 0x70, 0x65, 0x5F, 0x69, 0x64]

{.push checks: off, boundChecks: off.}

proc encodeCborValueRecordInto*(buf: var SafeBuffer, v: ValueRecord) =
  case v.kind
  of vrkInt:
    buf.writeCborMapHeader(3)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("Int")
    buf.writeOpenArray(CborKeyI2)
    buf.writeCborInt(v.intVal)
    buf.writeOpenArray(CborKeyTypeId2)
    buf.writeCborUint(uint64(v.intTypeId))

  of vrkFloat:
    buf.writeCborMapHeader(3)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("Float")
    buf.writeOpenArray(CborKeyF2)
    buf.writeCborFloat64(v.floatVal)
    buf.writeOpenArray(CborKeyTypeId2)
    buf.writeCborUint(uint64(v.floatTypeId))

  of vrkBool:
    buf.writeCborMapHeader(3)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("Bool")
    buf.writeOpenArray(CborKeyB2)
    buf.writeCborBool(v.boolVal)
    buf.writeOpenArray(CborKeyTypeId2)
    buf.writeCborUint(uint64(v.boolTypeId))

  of vrkString:
    buf.writeCborMapHeader(3)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("String")
    buf.writeOpenArray(CborKeyText2)
    buf.writeCborTextString(v.text)
    buf.writeOpenArray(CborKeyTypeId2)
    buf.writeCborUint(uint64(v.strTypeId))

  of vrkSequence:
    buf.writeCborMapHeader(4)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("Sequence")
    buf.writeOpenArray(CborKeyElements2)
    buf.writeCborArrayHeader(uint64(v.seqElements.len))
    for e in v.seqElements:
      encodeCborValueRecordInto(buf, e)
    buf.writeOpenArray(CborKeyIsSlice2)
    buf.writeCborBool(v.isSlice)
    buf.writeOpenArray(CborKeyTypeId2)
    buf.writeCborUint(uint64(v.seqTypeId))

  of vrkTuple:
    buf.writeCborMapHeader(3)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("Tuple")
    buf.writeOpenArray(CborKeyElements2)
    buf.writeCborArrayHeader(uint64(v.tupleElements.len))
    for e in v.tupleElements:
      encodeCborValueRecordInto(buf, e)
    buf.writeOpenArray(CborKeyTypeId2)
    buf.writeCborUint(uint64(v.tupleTypeId))

  of vrkStruct:
    buf.writeCborMapHeader(3)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("Struct")
    buf.writeOpenArray(CborKeyFieldValues2)
    buf.writeCborArrayHeader(uint64(v.fieldValues.len))
    for e in v.fieldValues:
      encodeCborValueRecordInto(buf, e)
    buf.writeOpenArray(CborKeyTypeId2)
    buf.writeCborUint(uint64(v.structTypeId))

  of vrkVariant:
    buf.writeCborMapHeader(4)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("Variant")
    buf.writeOpenArray(CborKeyDiscriminator2)
    buf.writeCborTextString(v.discriminator)
    buf.writeOpenArray(CborKeyContents2)
    if v.contents.len > 0:
      encodeCborValueRecordInto(buf, v.contents[0])
    else:
      buf.writeCborMapHeader(2)
      buf.writeOpenArray(CborKeyKind2)
      buf.writeCborTextString("None")
      buf.writeOpenArray(CborKeyTypeId2)
      buf.writeCborUint(0)
    buf.writeOpenArray(CborKeyTypeId2)
    buf.writeCborUint(uint64(v.variantTypeId))

  of vrkReference:
    buf.writeCborMapHeader(5)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("Reference")
    buf.writeOpenArray(CborKeyDereferenced2)
    if v.dereferenced.len > 0:
      encodeCborValueRecordInto(buf, v.dereferenced[0])
    else:
      buf.writeCborMapHeader(2)
      buf.writeOpenArray(CborKeyKind2)
      buf.writeCborTextString("None")
      buf.writeOpenArray(CborKeyTypeId2)
      buf.writeCborUint(0)
    buf.writeOpenArray(CborKeyAddress2)
    buf.writeCborUint(v.address)
    buf.writeOpenArray(CborKeyMutable2)
    buf.writeCborBool(v.mutable)
    buf.writeOpenArray(CborKeyTypeId2)
    buf.writeCborUint(uint64(v.refTypeId))

  of vrkRaw:
    buf.writeCborMapHeader(3)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("Raw")
    buf.writeOpenArray(CborKeyR2)
    buf.writeCborTextString(v.rawStr)
    buf.writeOpenArray(CborKeyTypeId2)
    buf.writeCborUint(uint64(v.rawTypeId))

  of vrkError:
    buf.writeCborMapHeader(3)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("Error")
    buf.writeOpenArray(CborKeyMsg2)
    buf.writeCborTextString(v.errorMsg)
    buf.writeOpenArray(CborKeyTypeId2)
    buf.writeCborUint(uint64(v.errorTypeId))

  of vrkNone:
    buf.writeCborMapHeader(2)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("None")
    buf.writeOpenArray(CborKeyTypeId2)
    buf.writeCborUint(uint64(v.noneTypeId))

  of vrkCell:
    buf.writeCborMapHeader(2)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("Cell")
    buf.writeOpenArray(CborKeyPlace2)
    buf.writeCborInt(int64(v.cellPlace))

  of vrkBigInt:
    buf.writeCborMapHeader(4)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("BigInt")
    buf.writeOpenArray(CborKeyB2)
    buf.writeCborByteString(v.bigIntBytes)
    buf.writeOpenArray(CborKeyNegative2)
    buf.writeCborBool(v.negative)
    buf.writeOpenArray(CborKeyTypeId2)
    buf.writeCborUint(uint64(v.bigIntTypeId))

  of vrkChar:
    buf.writeCborMapHeader(3)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("Char")
    buf.writeOpenArray(CborKeyC2)
    buf.writeCborTextString($v.charVal)
    buf.writeOpenArray(CborKeyTypeId2)
    buf.writeCborUint(uint64(v.charTypeId))

proc encodeCborTypeSpecificInfoInto(buf: var SafeBuffer, si: TypeSpecificInfo) {.inline.} =
  case si.kind
  of tsikNone:
    buf.writeCborMapHeader(1)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("None")
  of tsikStruct:
    buf.writeCborMapHeader(2)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("Struct")
    buf.writeOpenArray(CborKeyFields2)
    buf.writeCborArrayHeader(uint64(si.fields.len))
    for f in si.fields:
      buf.writeCborMapHeader(2)
      buf.writeOpenArray(CborKeyName2)
      buf.writeCborTextString(f.name)
      buf.writeOpenArray(CborKeyTypeId2)
      buf.writeCborUint(uint64(f.typeId))
  of tsikPointer:
    buf.writeCborMapHeader(2)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("Pointer")
    buf.writeOpenArray(CborKeyDereferenceTypeId2)
    buf.writeCborUint(uint64(si.dereferenceTypeId))

proc encodeCborTypeRecordInto*(buf: var SafeBuffer, t: TypeRecord) =
  buf.writeCborMapHeader(3)
  buf.writeOpenArray(CborKeyKind2)
  buf.writeCborUint(uint64(ord(t.kind)))
  buf.writeOpenArray(CborKeyLangType2)
  buf.writeCborTextString(t.langType)
  buf.writeOpenArray(CborKeySpecificInfo2)
  encodeCborTypeSpecificInfoInto(buf, t.specificInfo)

proc encodeCborRValueInto(buf: var SafeBuffer, rv: RValue) {.inline.} =
  case rv.kind
  of rvkSimple:
    buf.writeCborMapHeader(2)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("Simple")
    buf.writeOpenArray(CborKey02)
    buf.writeCborUint(uint64(rv.simpleId))
  of rvkCompound:
    buf.writeCborMapHeader(2)
    buf.writeOpenArray(CborKeyKind2)
    buf.writeCborTextString("Compound")
    buf.writeOpenArray(CborKey02)
    buf.writeCborArrayHeader(uint64(rv.compoundIds.len))
    for id in rv.compoundIds:
      buf.writeCborUint(uint64(id))

proc encodeCborAssignmentRecordInto*(buf: var SafeBuffer, a: AssignmentRecord) =
  buf.writeCborMapHeader(3)
  buf.writeOpenArray(CborKeyTo2)
  buf.writeCborUint(uint64(a.to))
  buf.writeOpenArray(CborKeyPassBy2)
  case a.passBy
  of pbValue: buf.writeCborTextString("Value")
  of pbReference: buf.writeCborTextString("Reference")
  buf.writeOpenArray(CborKeyFrom2)
  encodeCborRValueInto(buf, a.frm)

proc encodeCborFullValueRecordInto*(buf: var SafeBuffer, fvr: FullValueRecord) =
  buf.writeCborMapHeader(2)
  buf.writeOpenArray(CborKeyVariableId2)
  buf.writeCborUint(uint64(fvr.variableId))
  buf.writeOpenArray(CborKeyValue2)
  encodeCborValueRecordInto(buf, fvr.value)

proc encodeCborCallArgsInto*(buf: var SafeBuffer, args: seq[FullValueRecord]) =
  buf.writeCborArrayHeader(uint64(args.len))
  for arg in args:
    encodeCborFullValueRecordInto(buf, arg)

{.pop.} # checks: off, boundChecks: off

# ===========================================================================
# SafeSplitBinaryEncoder
# ===========================================================================

type
  SafeSplitBinaryEncoder* = object
    buf*: SafeBuffer

proc init*(T: type SafeSplitBinaryEncoder, capacity: int = 65536): SafeSplitBinaryEncoder =
  result.buf = initSafeBuffer(capacity)

proc clear*(enc: var SafeSplitBinaryEncoder) =
  enc.buf.clear()

proc getBytes*(enc: SafeSplitBinaryEncoder): seq[byte] =
  enc.buf.toSeq()

{.push checks: off, boundChecks: off.}

proc encodeEvent*(enc: var SafeSplitBinaryEncoder, event: TraceLowLevelEvent) =
  case event.kind
  of tleStep:
    enc.buf.writeU8(0)
    enc.buf.writeU64(uint64(event.step.pathId))
    enc.buf.writeI64(int64(event.step.line))

  of tlePath:
    enc.buf.writeU8(1)
    enc.buf.writeStr(event.path)

  of tleVariableName:
    enc.buf.writeU8(2)
    enc.buf.writeStr(event.varName)

  of tleVariable:
    enc.buf.writeU8(3)
    enc.buf.writeStr(event.variable)

  of tleType:
    enc.buf.writeU8(4)
    writePayloadCborDirect(enc.buf):
      encodeCborTypeRecordInto(enc.buf, event.typeRecord)

  of tleValue:
    enc.buf.writeU8(5)
    enc.buf.writeU64(uint64(event.fullValue.variableId))
    writePayloadCborDirect(enc.buf):
      encodeCborValueRecordInto(enc.buf, event.fullValue.value)

  of tleFunction:
    enc.buf.writeU8(6)
    enc.buf.writeU64(uint64(event.functionRecord.pathId))
    enc.buf.writeI64(int64(event.functionRecord.line))
    enc.buf.writeStr(event.functionRecord.name)

  of tleCall:
    enc.buf.writeU8(7)
    enc.buf.writeU64(uint64(event.callRecord.functionId))
    writePayloadCborDirect(enc.buf):
      encodeCborCallArgsInto(enc.buf, event.callRecord.args)

  of tleReturn:
    enc.buf.writeU8(8)
    writePayloadCborDirect(enc.buf):
      encodeCborValueRecordInto(enc.buf, event.returnRecord.returnValue)

  of tleEvent:
    enc.buf.writeU8(9)
    enc.buf.writeU8(byte(ord(event.recordEvent.kind)))
    enc.buf.writeStr(event.recordEvent.metadata)
    enc.buf.writeStr(event.recordEvent.content)

  of tleAsm:
    enc.buf.writeU8(10)
    enc.buf.writeU32(uint32(event.asmLines.len))
    for line in event.asmLines:
      enc.buf.writeStr(line)

  of tleBindVariable:
    enc.buf.writeU8(11)
    enc.buf.writeU64(uint64(event.bindVar.variableId))
    enc.buf.writeI64(int64(event.bindVar.place))

  of tleAssignment:
    enc.buf.writeU8(12)
    writePayloadCborDirect(enc.buf):
      encodeCborAssignmentRecordInto(enc.buf, event.assignment)

  of tleDropVariables:
    enc.buf.writeU8(13)
    enc.buf.writeU32(uint32(event.dropVarIds.len))
    for id in event.dropVarIds:
      enc.buf.writeU64(uint64(id))

  of tleCompoundValue:
    enc.buf.writeU8(14)
    enc.buf.writeI64(int64(event.compoundValue.place))
    writePayloadCborDirect(enc.buf):
      encodeCborValueRecordInto(enc.buf, event.compoundValue.value)

  of tleCellValue:
    enc.buf.writeU8(15)
    enc.buf.writeI64(int64(event.cellValue.place))
    writePayloadCborDirect(enc.buf):
      encodeCborValueRecordInto(enc.buf, event.cellValue.value)

  of tleAssignCompoundItem:
    enc.buf.writeU8(16)
    enc.buf.writeI64(int64(event.assignCompoundItem.place))
    enc.buf.writeU64(event.assignCompoundItem.index)
    enc.buf.writeI64(int64(event.assignCompoundItem.itemPlace))

  of tleAssignCell:
    enc.buf.writeU8(17)
    enc.buf.writeI64(int64(event.assignCell.place))
    writePayloadCborDirect(enc.buf):
      encodeCborValueRecordInto(enc.buf, event.assignCell.newValue)

  of tleVariableCell:
    enc.buf.writeU8(18)
    enc.buf.writeU64(uint64(event.variableCell.variableId))
    enc.buf.writeI64(int64(event.variableCell.place))

  of tleDropVariable:
    enc.buf.writeU8(19)
    enc.buf.writeU64(uint64(event.dropVarId))

  of tleThreadStart:
    enc.buf.writeU8(20)
    enc.buf.writeU64(uint64(event.threadStartId))

  of tleThreadExit:
    enc.buf.writeU8(21)
    enc.buf.writeU64(uint64(event.threadExitId))

  of tleThreadSwitch:
    enc.buf.writeU8(22)
    enc.buf.writeU64(uint64(event.threadSwitchId))

  of tleDropLastStep:
    enc.buf.writeU8(23)

{.pop.} # checks: off, boundChecks: off
