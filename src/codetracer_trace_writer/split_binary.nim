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
import ./safe_buffer
import ../codetracer_trace_types
export codetracer_trace_types, results, cbor, safe_buffer

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

# CBOR encoding directly into SafeBuffer (no intermediate seq[byte])
proc encodeCborValueRecordInto*(buf: var SafeBuffer, v: ValueRecord) {.inline.}
proc encodeCborTypeRecordInto*(buf: var SafeBuffer, t: TypeRecord) {.inline.}
proc encodeCborAssignmentRecordInto*(buf: var SafeBuffer, a: AssignmentRecord) {.inline.}
proc encodeCborCallArgsInto*(buf: var SafeBuffer, args: seq[FullValueRecord]) {.inline.}
proc encodeCborFullValueRecordInto*(buf: var SafeBuffer, fvr: FullValueRecord) {.inline.}

# Pre-computed CBOR key bytes (same as in cbor.nim)
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

proc readPayload(data: openArray[byte], pos: var int): Result[seq[byte], string] =
  let length = ?readU32(data, pos)
  readBytes(data, pos, int(length))

# ===========================================================================
# StackStage — fixed-size stack-array staging for zero-allocation encoding
# ===========================================================================
#
# For fixed-size events, we stage all bytes into a 256-byte stack array
# and flush once to the output buffer. This eliminates per-field
# ensureCapacity calls and produces a single bulk copy.

type
  StackStage* {.byref.} = object
    buf*: array[256, byte]  # Fixed stack allocation, no heap
    pos*: int

template stkWriteU8*(stk: var StackStage, v: uint8) =
  stk.buf[stk.pos] = v
  stk.pos += 1

template stkWriteU32*(stk: var StackStage, v: uint32) =
  let stkTmpU32 = v
  copyMem(addr stk.buf[stk.pos], unsafeAddr stkTmpU32, 4)
  stk.pos += 4

template stkWriteU64*(stk: var StackStage, v: uint64) =
  let stkTmpU64 = v
  copyMem(addr stk.buf[stk.pos], unsafeAddr stkTmpU64, 8)
  stk.pos += 8

template stkWriteI64*(stk: var StackStage, v: int64) =
  let stkTmpI64 = v
  copyMem(addr stk.buf[stk.pos], unsafeAddr stkTmpI64, 8)
  stk.pos += 8

template flushTo*(stk: StackStage, output: var SafeBuffer) =
  ## Flush staged bytes from stack array into the SafeBuffer in one bulk copy.
  ensureCapacity(output, stk.pos)
  copyMem(addr output.data[output.pos], unsafeAddr stk.buf[0], stk.pos)
  output.pos += stk.pos

# ===========================================================================
# SplitBinaryEncoder
# ===========================================================================

type
  SplitBinaryEncoder* = object
    buf*: SafeBuffer

proc init*(T: type SplitBinaryEncoder, capacity: int = 65536): SplitBinaryEncoder =
  result.buf = initSafeBuffer(capacity)

proc clear*(enc: var SplitBinaryEncoder) =
  enc.buf.clear()

proc getBytes*(enc: SplitBinaryEncoder): seq[byte] =
  enc.buf.toSeq()

proc destroy*(enc: var SplitBinaryEncoder) =
  ## No-op: SafeBuffer uses GC-managed memory, no manual dealloc needed.
  enc.buf.clear()

{.push checks: off, boundChecks: off.}

proc encodeEvent*(enc: var SplitBinaryEncoder, event: TraceLowLevelEvent) =
  ## Encode a single TraceLowLevelEvent into the encoder's buffer.
  ## Fixed-size events use StackStage to batch all writes into a single
  ## bulk copy. Events with variable-length data (strings, CBOR) stage
  ## the fixed header on the stack, then append variable data directly.
  case event.kind
  of tleStep:
    # 1 + 8 + 8 = 17 bytes, fits stack
    var stk: StackStage
    stk.stkWriteU8(0)
    stk.stkWriteU64(uint64(event.step.pathId))
    stk.stkWriteI64(int64(event.step.line))
    stk.flushTo(enc.buf)

  of tlePath:
    # Header (1 + 4 = 5 bytes) on stack, then string direct
    var stk: StackStage
    stk.stkWriteU8(1)
    stk.stkWriteU32(uint32(event.path.len))
    stk.flushTo(enc.buf)
    if event.path.len > 0:
      ensureCapacity(enc.buf, event.path.len)
      copyMem(addr enc.buf.data[enc.buf.pos], unsafeAddr event.path[0], event.path.len)
      enc.buf.pos += event.path.len

  of tleVariableName:
    var stk: StackStage
    stk.stkWriteU8(2)
    stk.stkWriteU32(uint32(event.varName.len))
    stk.flushTo(enc.buf)
    if event.varName.len > 0:
      ensureCapacity(enc.buf, event.varName.len)
      copyMem(addr enc.buf.data[enc.buf.pos], unsafeAddr event.varName[0], event.varName.len)
      enc.buf.pos += event.varName.len

  of tleVariable:
    var stk: StackStage
    stk.stkWriteU8(3)
    stk.stkWriteU32(uint32(event.variable.len))
    stk.flushTo(enc.buf)
    if event.variable.len > 0:
      ensureCapacity(enc.buf, event.variable.len)
      copyMem(addr enc.buf.data[enc.buf.pos], unsafeAddr event.variable[0], event.variable.len)
      enc.buf.pos += event.variable.len

  of tleType:
    enc.buf.writeU8(4)
    writePayloadCborDirect(enc.buf):
      encodeCborTypeRecordInto(enc.buf, event.typeRecord)

  of tleValue:
    # Header: 1 + 8 = 9 bytes on stack, then CBOR payload direct
    var stk: StackStage
    stk.stkWriteU8(5)
    stk.stkWriteU64(uint64(event.fullValue.variableId))
    stk.flushTo(enc.buf)
    writePayloadCborDirect(enc.buf):
      encodeCborValueRecordInto(enc.buf, event.fullValue.value)

  of tleFunction:
    # Header: 1 + 8 + 8 = 17 bytes on stack, then string
    var stk: StackStage
    stk.stkWriteU8(6)
    stk.stkWriteU64(uint64(event.functionRecord.pathId))
    stk.stkWriteI64(int64(event.functionRecord.line))
    stk.stkWriteU32(uint32(event.functionRecord.name.len))
    stk.flushTo(enc.buf)
    if event.functionRecord.name.len > 0:
      ensureCapacity(enc.buf, event.functionRecord.name.len)
      copyMem(addr enc.buf.data[enc.buf.pos], unsafeAddr event.functionRecord.name[0], event.functionRecord.name.len)
      enc.buf.pos += event.functionRecord.name.len

  of tleCall:
    # Header: 1 + 8 = 9 bytes on stack, then CBOR payload
    var stk: StackStage
    stk.stkWriteU8(7)
    stk.stkWriteU64(uint64(event.callRecord.functionId))
    stk.flushTo(enc.buf)
    writePayloadCborDirect(enc.buf):
      encodeCborCallArgsInto(enc.buf, event.callRecord.args)

  of tleReturn:
    enc.buf.writeU8(8)
    writePayloadCborDirect(enc.buf):
      encodeCborValueRecordInto(enc.buf, event.returnRecord.returnValue)

  of tleEvent:
    # Header: 1 + 1 = 2 bytes on stack, then two strings
    var stk: StackStage
    stk.stkWriteU8(9)
    stk.stkWriteU8(byte(ord(event.recordEvent.kind)))
    stk.flushTo(enc.buf)
    enc.buf.writeStr(event.recordEvent.metadata)
    enc.buf.writeStr(event.recordEvent.content)

  of tleAsm:
    enc.buf.writeU8(10)
    enc.buf.writeU32(uint32(event.asmLines.len))
    for line in event.asmLines:
      enc.buf.writeStr(line)

  of tleBindVariable:
    # 1 + 8 + 8 = 17 bytes, fits stack
    var stk: StackStage
    stk.stkWriteU8(11)
    stk.stkWriteU64(uint64(event.bindVar.variableId))
    stk.stkWriteI64(int64(event.bindVar.place))
    stk.flushTo(enc.buf)

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
    # Header: 1 + 8 = 9 bytes on stack, then CBOR
    var stk: StackStage
    stk.stkWriteU8(14)
    stk.stkWriteI64(int64(event.compoundValue.place))
    stk.flushTo(enc.buf)
    writePayloadCborDirect(enc.buf):
      encodeCborValueRecordInto(enc.buf, event.compoundValue.value)

  of tleCellValue:
    var stk: StackStage
    stk.stkWriteU8(15)
    stk.stkWriteI64(int64(event.cellValue.place))
    stk.flushTo(enc.buf)
    writePayloadCborDirect(enc.buf):
      encodeCborValueRecordInto(enc.buf, event.cellValue.value)

  of tleAssignCompoundItem:
    # 1 + 8 + 8 + 8 = 25 bytes, fits stack
    var stk: StackStage
    stk.stkWriteU8(16)
    stk.stkWriteI64(int64(event.assignCompoundItem.place))
    stk.stkWriteU64(event.assignCompoundItem.index)
    stk.stkWriteI64(int64(event.assignCompoundItem.itemPlace))
    stk.flushTo(enc.buf)

  of tleAssignCell:
    var stk: StackStage
    stk.stkWriteU8(17)
    stk.stkWriteI64(int64(event.assignCell.place))
    stk.flushTo(enc.buf)
    writePayloadCborDirect(enc.buf):
      encodeCborValueRecordInto(enc.buf, event.assignCell.newValue)

  of tleVariableCell:
    # 1 + 8 + 8 = 17 bytes, fits stack
    var stk: StackStage
    stk.stkWriteU8(18)
    stk.stkWriteU64(uint64(event.variableCell.variableId))
    stk.stkWriteI64(int64(event.variableCell.place))
    stk.flushTo(enc.buf)

  of tleDropVariable:
    # 1 + 8 = 9 bytes, fits stack
    var stk: StackStage
    stk.stkWriteU8(19)
    stk.stkWriteU64(uint64(event.dropVarId))
    stk.flushTo(enc.buf)

  of tleThreadStart:
    # 1 + 8 = 9 bytes, fits stack
    var stk: StackStage
    stk.stkWriteU8(20)
    stk.stkWriteU64(uint64(event.threadStartId))
    stk.flushTo(enc.buf)

  of tleThreadExit:
    var stk: StackStage
    stk.stkWriteU8(21)
    stk.stkWriteU64(uint64(event.threadExitId))
    stk.flushTo(enc.buf)

  of tleThreadSwitch:
    var stk: StackStage
    stk.stkWriteU8(22)
    stk.stkWriteU64(uint64(event.threadSwitchId))
    stk.flushTo(enc.buf)

  of tleDropLastStep:
    var stk: StackStage
    stk.stkWriteU8(23)
    stk.flushTo(enc.buf)

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
