{.push raises: [].}

## Minimal CBOR encoder/decoder producing byte-identical output to Rust's
## `cbor4ii` crate with serde for the CodeTracer trace types.
##
## Serde's `#[serde(tag = "kind")]` for enums produces a CBOR map with a
## "kind" key containing the variant name as a text string. Struct fields
## are serialized in declaration order as CBOR text-string map keys.
##
## CBOR encoding reference (RFC 8949):
##   Major type 0: unsigned int
##   Major type 1: negative int (value = -1 - N)
##   Major type 2: byte string (length-prefixed)
##   Major type 3: text string (length-prefixed)
##   Major type 4: array (length-prefixed)
##   Major type 5: map (length-prefixed, count = number of key-value pairs)
##   Major type 7: false=0xf4, true=0xf5, null=0xf6, float64=0xfb + 8 bytes BE

import results
import stew/endians2
import ../codetracer_trace_types

export results, codetracer_trace_types

# ===========================================================================
# CborEncoder
# ===========================================================================

type
  CborEncoder* = object
    buf*: seq[byte]

proc init*(T: type CborEncoder, capacity: int = 256): CborEncoder =
  result.buf = newSeqOfCap[byte](capacity)

proc clear*(enc: var CborEncoder) =
  enc.buf.setLen(0)

proc getBytes*(enc: CborEncoder): seq[byte] =
  enc.buf

# ===========================================================================
# Pre-computed CBOR key bytes for frequently used keys
# ===========================================================================

const
  # CBOR text(4) "kind" = 0x64 + "kind"
  CborKeyKind* = [0x64'u8, 0x6B, 0x69, 0x6E, 0x64]
  # CBOR text(1) "i" = 0x61 + "i"
  CborKeyI* = [0x61'u8, 0x69]
  # CBOR text(7) "type_id" = 0x67 + "type_id"
  CborKeyTypeId* = [0x67'u8, 0x74, 0x79, 0x70, 0x65, 0x5F, 0x69, 0x64]
  # CBOR text(1) "f" = 0x61 + "f"
  CborKeyF* = [0x61'u8, 0x66]
  # CBOR text(1) "b" = 0x61 + "b"
  CborKeyB* = [0x61'u8, 0x62]
  # CBOR text(4) "text" = 0x64 + "text"
  CborKeyText* = [0x64'u8, 0x74, 0x65, 0x78, 0x74]
  # CBOR text(8) "elements" = 0x68 + "elements"
  CborKeyElements* = [0x68'u8, 0x65, 0x6C, 0x65, 0x6D, 0x65, 0x6E, 0x74, 0x73]
  # CBOR text(8) "is_slice" = 0x68 + "is_slice"
  CborKeyIsSlice* = [0x68'u8, 0x69, 0x73, 0x5F, 0x73, 0x6C, 0x69, 0x63, 0x65]
  # CBOR text(12) "field_values" = 0x6C + "field_values"
  CborKeyFieldValues* = [0x6C'u8, 0x66, 0x69, 0x65, 0x6C, 0x64, 0x5F, 0x76, 0x61, 0x6C, 0x75, 0x65, 0x73]
  # CBOR text(13) "discriminator" = 0x6D + "discriminator"
  CborKeyDiscriminator* = [0x6D'u8, 0x64, 0x69, 0x73, 0x63, 0x72, 0x69, 0x6D, 0x69, 0x6E, 0x61, 0x74, 0x6F, 0x72]
  # CBOR text(8) "contents" = 0x68 + "contents"
  CborKeyContents* = [0x68'u8, 0x63, 0x6F, 0x6E, 0x74, 0x65, 0x6E, 0x74, 0x73]
  # CBOR text(13) "dereferenced" (length 12) = 0x6C + "dereferenced"
  CborKeyDereferenced* = [0x6C'u8, 0x64, 0x65, 0x72, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6E, 0x63, 0x65, 0x64]
  # CBOR text(7) "address" = 0x67 + "address"
  CborKeyAddress* = [0x67'u8, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73]
  # CBOR text(7) "mutable" = 0x67 + "mutable"
  CborKeyMutable* = [0x67'u8, 0x6D, 0x75, 0x74, 0x61, 0x62, 0x6C, 0x65]
  # CBOR text(1) "r" = 0x61 + "r"
  CborKeyR* = [0x61'u8, 0x72]
  # CBOR text(3) "msg" = 0x63 + "msg"
  CborKeyMsg* = [0x63'u8, 0x6D, 0x73, 0x67]
  # CBOR text(5) "place" = 0x65 + "place"
  CborKeyPlace* = [0x65'u8, 0x70, 0x6C, 0x61, 0x63, 0x65]
  # CBOR text(8) "negative" = 0x68 + "negative"
  CborKeyNegative* = [0x68'u8, 0x6E, 0x65, 0x67, 0x61, 0x74, 0x69, 0x76, 0x65]
  # CBOR text(1) "c" = 0x61 + "c"
  CborKeyC* = [0x61'u8, 0x63]
  # CBOR text(2) "to" = 0x62 + "to"
  CborKeyTo = [0x62'u8, 0x74, 0x6F]
  # CBOR text(7) "pass_by" = 0x67 + "pass_by"
  CborKeyPassBy = [0x67'u8, 0x70, 0x61, 0x73, 0x73, 0x5F, 0x62, 0x79]
  # CBOR text(4) "from" = 0x64 + "from"
  CborKeyFrom = [0x64'u8, 0x66, 0x72, 0x6F, 0x6D]
  # CBOR text(1) "0" = 0x61 + "0"
  CborKey0 = [0x61'u8, 0x30]
  # CBOR text(11) "variable_id" = 0x6B + "variable_id"
  CborKeyVariableId = [0x6B'u8, 0x76, 0x61, 0x72, 0x69, 0x61, 0x62, 0x6C, 0x65, 0x5F, 0x69, 0x64]
  # CBOR text(5) "value" = 0x65 + "value"
  CborKeyValue = [0x65'u8, 0x76, 0x61, 0x6C, 0x75, 0x65]
  # CBOR text(9) "lang_type" = 0x69 + "lang_type"
  CborKeyLangType = [0x69'u8, 0x6C, 0x61, 0x6E, 0x67, 0x5F, 0x74, 0x79, 0x70, 0x65]
  # CBOR text(13) "specific_info" = 0x6D + "specific_info"
  CborKeySpecificInfo = [0x6D'u8, 0x73, 0x70, 0x65, 0x63, 0x69, 0x66, 0x69, 0x63, 0x5F, 0x69, 0x6E, 0x66, 0x6F]
  # CBOR text(6) "fields" = 0x66 + "fields"
  CborKeyFields = [0x66'u8, 0x66, 0x69, 0x65, 0x6C, 0x64, 0x73]
  # CBOR text(4) "name" = 0x64 + "name"
  CborKeyName = [0x64'u8, 0x6E, 0x61, 0x6D, 0x65]
  # CBOR text(20) "dereference_type_id" = 0x73 + ... (length 19)
  CborKeyDereferenceTypeId = [0x73'u8, 0x64, 0x65, 0x72, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6E, 0x63, 0x65, 0x5F, 0x74, 0x79, 0x70, 0x65, 0x5F, 0x69, 0x64]

proc writePrecomputed*(enc: var CborEncoder, data: openArray[byte]) {.inline.} =
  let pos = enc.buf.len
  enc.buf.setLen(pos + data.len)
  copyMem(addr enc.buf[pos], unsafeAddr data[0], data.len)

# ---------------------------------------------------------------------------
# Low-level CBOR writing (optimized with bulk memcpy and inline)
# ---------------------------------------------------------------------------

{.push checks: off, boundChecks: off.}

proc writeTypeAndValue(enc: var CborEncoder, majorType: byte, value: uint64) {.inline.} =
  ## Write a CBOR type/value header. majorType is 0..7 (shifted left by 5).
  let mt = majorType shl 5
  if value <= 23:
    enc.buf.add(mt or byte(value))
  elif value <= 0xFF:
    enc.buf.add(mt or 24)
    enc.buf.add(byte(value))
  elif value <= 0xFFFF:
    let pos = enc.buf.len
    enc.buf.setLen(pos + 3)
    enc.buf[pos] = mt or 25
    enc.buf[pos + 1] = byte(value shr 8)
    enc.buf[pos + 2] = byte(value)
  elif value <= 0xFFFF_FFFF'u64:
    let pos = enc.buf.len
    enc.buf.setLen(pos + 5)
    enc.buf[pos] = mt or 26
    enc.buf[pos + 1] = byte(value shr 24)
    enc.buf[pos + 2] = byte(value shr 16)
    enc.buf[pos + 3] = byte(value shr 8)
    enc.buf[pos + 4] = byte(value)
  else:
    let pos = enc.buf.len
    enc.buf.setLen(pos + 9)
    enc.buf[pos] = mt or 27
    enc.buf[pos + 1] = byte(value shr 56)
    enc.buf[pos + 2] = byte(value shr 48)
    enc.buf[pos + 3] = byte(value shr 40)
    enc.buf[pos + 4] = byte(value shr 32)
    enc.buf[pos + 5] = byte(value shr 24)
    enc.buf[pos + 6] = byte(value shr 16)
    enc.buf[pos + 7] = byte(value shr 8)
    enc.buf[pos + 8] = byte(value)

proc writeUint*(enc: var CborEncoder, value: uint64) {.inline.} =
  ## CBOR major type 0: unsigned integer.
  enc.writeTypeAndValue(0, value)

proc writeNegInt*(enc: var CborEncoder, value: uint64) {.inline.} =
  ## CBOR major type 1: negative integer. Encodes -(value+1).
  enc.writeTypeAndValue(1, value)

proc writeInt*(enc: var CborEncoder, value: int64) {.inline.} =
  ## Write a signed integer. Uses major type 0 for non-negative, 1 for negative.
  if value >= 0:
    enc.writeUint(uint64(value))
  else:
    # CBOR negative: -1 - N, so N = -(value+1) = -value - 1
    enc.writeNegInt(uint64(-1 - value))

proc writeByteString*(enc: var CborEncoder, data: openArray[byte]) {.inline.} =
  ## CBOR major type 2: byte string.
  enc.writeTypeAndValue(2, uint64(data.len))
  if data.len > 0:
    let pos = enc.buf.len
    enc.buf.setLen(pos + data.len)
    copyMem(addr enc.buf[pos], unsafeAddr data[0], data.len)

proc writeTextString*(enc: var CborEncoder, s: string) {.inline.} =
  ## CBOR major type 3: text string.
  enc.writeTypeAndValue(3, uint64(s.len))
  if s.len > 0:
    let pos = enc.buf.len
    enc.buf.setLen(pos + s.len)
    copyMem(addr enc.buf[pos], unsafeAddr s[0], s.len)

proc writeArrayHeader*(enc: var CborEncoder, count: uint64) {.inline.} =
  ## CBOR major type 4: array header.
  enc.writeTypeAndValue(4, count)

proc writeMapHeader*(enc: var CborEncoder, count: uint64) {.inline.} =
  ## CBOR major type 5: map header (count = number of key-value pairs).
  enc.writeTypeAndValue(5, count)

proc writeBool*(enc: var CborEncoder, value: bool) {.inline.} =
  ## CBOR major type 7: boolean.
  if value:
    enc.buf.add(0xF5'u8)
  else:
    enc.buf.add(0xF4'u8)

proc writeNull*(enc: var CborEncoder) {.inline.} =
  ## CBOR major type 7: null.
  enc.buf.add(0xF6'u8)

proc writeFloat64*(enc: var CborEncoder, value: float64) {.inline.} =
  ## CBOR major type 7, additional info 27: IEEE 754 double (8 bytes BE).
  let pos = enc.buf.len
  enc.buf.setLen(pos + 9)
  enc.buf[pos] = 0xFB'u8
  let bits = cast[uint64](value)
  enc.buf[pos + 1] = byte(bits shr 56)
  enc.buf[pos + 2] = byte(bits shr 48)
  enc.buf[pos + 3] = byte(bits shr 40)
  enc.buf[pos + 4] = byte(bits shr 32)
  enc.buf[pos + 5] = byte(bits shr 24)
  enc.buf[pos + 6] = byte(bits shr 16)
  enc.buf[pos + 7] = byte(bits shr 8)
  enc.buf[pos + 8] = byte(bits)

# Convenience: write a map key (text string)
proc writeKey*(enc: var CborEncoder, key: string) {.inline.} =
  enc.writeTextString(key)

{.pop.} # checks: off, boundChecks: off

# ===========================================================================
# CborDecoder
# ===========================================================================

type
  CborDecoder* = object
    data*: seq[byte]
    pos*: int

proc init*(T: type CborDecoder, data: openArray[byte]): CborDecoder =
  var d: seq[byte]
  d.setLen(data.len)
  for i in 0 ..< data.len:
    d[i] = data[i]
  CborDecoder(data: d, pos: 0)

proc init*(T: type CborDecoder, data: seq[byte]): CborDecoder =
  CborDecoder(data: data, pos: 0)

proc remaining*(dec: CborDecoder): int =
  dec.data.len - dec.pos

# ---------------------------------------------------------------------------
# Low-level CBOR reading
# ---------------------------------------------------------------------------

proc readByte(dec: var CborDecoder): Result[byte, string] =
  if dec.pos >= dec.data.len:
    return err("cbor: unexpected end of data")
  let b = dec.data[dec.pos]
  dec.pos += 1
  ok(b)

proc peekByte(dec: CborDecoder): Result[byte, string] =
  if dec.pos >= dec.data.len:
    return err("cbor: unexpected end of data on peek")
  ok(dec.data[dec.pos])

proc readBeU16(dec: var CborDecoder): Result[uint16, string] =
  if dec.pos + 2 > dec.data.len:
    return err("cbor: unexpected end reading u16")
  let v = (uint16(dec.data[dec.pos]) shl 8) or uint16(dec.data[dec.pos + 1])
  dec.pos += 2
  ok(v)

proc readBeU32(dec: var CborDecoder): Result[uint32, string] =
  if dec.pos + 4 > dec.data.len:
    return err("cbor: unexpected end reading u32")
  let v = (uint32(dec.data[dec.pos]) shl 24) or
          (uint32(dec.data[dec.pos + 1]) shl 16) or
          (uint32(dec.data[dec.pos + 2]) shl 8) or
          uint32(dec.data[dec.pos + 3])
  dec.pos += 4
  ok(v)

proc readBeU64(dec: var CborDecoder): Result[uint64, string] =
  if dec.pos + 8 > dec.data.len:
    return err("cbor: unexpected end reading u64")
  let v = (uint64(dec.data[dec.pos]) shl 56) or
          (uint64(dec.data[dec.pos + 1]) shl 48) or
          (uint64(dec.data[dec.pos + 2]) shl 40) or
          (uint64(dec.data[dec.pos + 3]) shl 32) or
          (uint64(dec.data[dec.pos + 4]) shl 24) or
          (uint64(dec.data[dec.pos + 5]) shl 16) or
          (uint64(dec.data[dec.pos + 6]) shl 8) or
          uint64(dec.data[dec.pos + 7])
  dec.pos += 8
  ok(v)

proc readTypeAndValue(dec: var CborDecoder): Result[(byte, uint64), string] =
  ## Read a CBOR type header. Returns (majorType, value).
  let initial = ?dec.readByte()
  let majorType = initial shr 5
  let additional = initial and 0x1F
  if additional <= 23:
    ok((majorType, uint64(additional)))
  elif additional == 24:
    let v = ?dec.readByte()
    ok((majorType, uint64(v)))
  elif additional == 25:
    let v = ?dec.readBeU16()
    ok((majorType, uint64(v)))
  elif additional == 26:
    let v = ?dec.readBeU32()
    ok((majorType, uint64(v)))
  elif additional == 27:
    let v = ?dec.readBeU64()
    ok((majorType, uint64(v)))
  else:
    err("cbor: unsupported additional info: " & $additional)

proc readUint*(dec: var CborDecoder): Result[uint64, string] =
  let (mt, v) = ?dec.readTypeAndValue()
  if mt != 0:
    return err("cbor: expected unsigned int (major 0), got major " & $mt)
  ok(v)

proc readInt*(dec: var CborDecoder): Result[int64, string] =
  ## Read a signed integer (CBOR major 0 or 1).
  let b = ?dec.peekByte()
  let mt = b shr 5
  if mt == 0:
    let v = ?dec.readUint()
    if v > uint64(high(int64)):
      return err("cbor: unsigned int too large for int64")
    ok(int64(v))
  elif mt == 1:
    let (_, v) = ?dec.readTypeAndValue()
    # CBOR negative: -1 - v
    if v > uint64(high(int64)):
      return err("cbor: negative int too large for int64")
    ok(-1'i64 - int64(v))
  else:
    err("cbor: expected integer (major 0 or 1), got major " & $mt)

proc readTextString*(dec: var CborDecoder): Result[string, string] =
  let (mt, length) = ?dec.readTypeAndValue()
  if mt != 3:
    return err("cbor: expected text string (major 3), got major " & $mt)
  let len = int(length)
  if dec.pos + len > dec.data.len:
    return err("cbor: string length exceeds data")
  var s = newString(len)
  for i in 0 ..< len:
    s[i] = char(dec.data[dec.pos + i])
  dec.pos += len
  ok(s)

proc readByteString*(dec: var CborDecoder): Result[seq[byte], string] =
  let (mt, length) = ?dec.readTypeAndValue()
  if mt != 2:
    return err("cbor: expected byte string (major 2), got major " & $mt)
  let len = int(length)
  if dec.pos + len > dec.data.len:
    return err("cbor: byte string length exceeds data")
  var bs = newSeq[byte](len)
  for i in 0 ..< len:
    bs[i] = dec.data[dec.pos + i]
  dec.pos += len
  ok(bs)

proc readArrayHeader*(dec: var CborDecoder): Result[uint64, string] =
  let (mt, count) = ?dec.readTypeAndValue()
  if mt != 4:
    return err("cbor: expected array (major 4), got major " & $mt)
  ok(count)

proc readMapHeader*(dec: var CborDecoder): Result[uint64, string] =
  let (mt, count) = ?dec.readTypeAndValue()
  if mt != 5:
    return err("cbor: expected map (major 5), got major " & $mt)
  ok(count)

proc readBool*(dec: var CborDecoder): Result[bool, string] =
  let b = ?dec.readByte()
  if b == 0xF5:
    ok(true)
  elif b == 0xF4:
    ok(false)
  else:
    err("cbor: expected bool (0xF4/0xF5), got 0x" & $b)

proc readNull*(dec: var CborDecoder): Result[void, string] =
  let b = ?dec.readByte()
  if b == 0xF6:
    ok()
  else:
    err("cbor: expected null (0xF6), got 0x" & $b)

proc isNull*(dec: CborDecoder): bool =
  if dec.pos < dec.data.len:
    dec.data[dec.pos] == 0xF6
  else:
    false

proc readFloat64*(dec: var CborDecoder): Result[float64, string] =
  let b = ?dec.readByte()
  if b != 0xFB:
    return err("cbor: expected float64 (0xFB), got 0x" & $b)
  let raw = ?dec.readBeU64()
  ok(cast[float64](raw))

# Convenience: read a map key and verify it matches expected
proc readExpectedKey*(dec: var CborDecoder, expected: string): Result[void, string] =
  let key = ?dec.readTextString()
  if key != expected:
    return err("cbor: expected key '" & expected & "', got '" & key & "'")
  ok()

# ===========================================================================
# Encode specific types matching Rust serde + cbor4ii output
# ===========================================================================

# ---- ValueRecord ----
# Rust: #[serde(tag = "kind")] enum ValueRecord { Int { i, type_id }, ... }
# Serde produces a CBOR map with "kind" key + variant fields in declaration order.

proc encodeCborValueRecord*(enc: var CborEncoder, v: ValueRecord) {.raises: [].}

proc encodeCborValueRecordImpl(enc: var CborEncoder, v: ValueRecord) =
  case v.kind
  of vrkInt:
    enc.writeMapHeader(3)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("Int")
    enc.writePrecomputed(CborKeyI)
    enc.writeInt(v.intVal)
    enc.writePrecomputed(CborKeyTypeId)
    enc.writeUint(uint64(v.intTypeId))

  of vrkFloat:
    enc.writeMapHeader(3)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("Float")
    enc.writePrecomputed(CborKeyF)
    enc.writeFloat64(v.floatVal)
    enc.writePrecomputed(CborKeyTypeId)
    enc.writeUint(uint64(v.floatTypeId))

  of vrkBool:
    enc.writeMapHeader(3)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("Bool")
    enc.writePrecomputed(CborKeyB)
    enc.writeBool(v.boolVal)
    enc.writePrecomputed(CborKeyTypeId)
    enc.writeUint(uint64(v.boolTypeId))

  of vrkString:
    enc.writeMapHeader(3)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("String")
    enc.writePrecomputed(CborKeyText)
    enc.writeTextString(v.text)
    enc.writePrecomputed(CborKeyTypeId)
    enc.writeUint(uint64(v.strTypeId))

  of vrkSequence:
    enc.writeMapHeader(4)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("Sequence")
    enc.writePrecomputed(CborKeyElements)
    enc.writeArrayHeader(uint64(v.seqElements.len))
    for e in v.seqElements:
      enc.encodeCborValueRecord(e)
    enc.writePrecomputed(CborKeyIsSlice)
    enc.writeBool(v.isSlice)
    enc.writePrecomputed(CborKeyTypeId)
    enc.writeUint(uint64(v.seqTypeId))

  of vrkTuple:
    enc.writeMapHeader(3)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("Tuple")
    enc.writePrecomputed(CborKeyElements)
    enc.writeArrayHeader(uint64(v.tupleElements.len))
    for e in v.tupleElements:
      enc.encodeCborValueRecord(e)
    enc.writePrecomputed(CborKeyTypeId)
    enc.writeUint(uint64(v.tupleTypeId))

  of vrkStruct:
    enc.writeMapHeader(3)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("Struct")
    enc.writePrecomputed(CborKeyFieldValues)
    enc.writeArrayHeader(uint64(v.fieldValues.len))
    for e in v.fieldValues:
      enc.encodeCborValueRecord(e)
    enc.writePrecomputed(CborKeyTypeId)
    enc.writeUint(uint64(v.structTypeId))

  of vrkVariant:
    enc.writeMapHeader(4)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("Variant")
    enc.writePrecomputed(CborKeyDiscriminator)
    enc.writeTextString(v.discriminator)
    enc.writePrecomputed(CborKeyContents)
    # Rust has Box<ValueRecord>, serde serializes the inner value directly
    if v.contents.len > 0:
      enc.encodeCborValueRecord(v.contents[0])
    else:
      # Fallback: encode None value
      enc.writeMapHeader(2)
      enc.writePrecomputed(CborKeyKind)
      enc.writeTextString("None")
      enc.writePrecomputed(CborKeyTypeId)
      enc.writeUint(0)
    enc.writePrecomputed(CborKeyTypeId)
    enc.writeUint(uint64(v.variantTypeId))

  of vrkReference:
    enc.writeMapHeader(5)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("Reference")
    enc.writePrecomputed(CborKeyDereferenced)
    # Rust has Box<ValueRecord>
    if v.dereferenced.len > 0:
      enc.encodeCborValueRecord(v.dereferenced[0])
    else:
      enc.writeMapHeader(2)
      enc.writePrecomputed(CborKeyKind)
      enc.writeTextString("None")
      enc.writePrecomputed(CborKeyTypeId)
      enc.writeUint(0)
    enc.writePrecomputed(CborKeyAddress)
    enc.writeUint(v.address)
    enc.writePrecomputed(CborKeyMutable)
    enc.writeBool(v.mutable)
    enc.writePrecomputed(CborKeyTypeId)
    enc.writeUint(uint64(v.refTypeId))

  of vrkRaw:
    enc.writeMapHeader(3)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("Raw")
    enc.writePrecomputed(CborKeyR)
    enc.writeTextString(v.rawStr)
    enc.writePrecomputed(CborKeyTypeId)
    enc.writeUint(uint64(v.rawTypeId))

  of vrkError:
    enc.writeMapHeader(3)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("Error")
    enc.writePrecomputed(CborKeyMsg)
    enc.writeTextString(v.errorMsg)
    enc.writePrecomputed(CborKeyTypeId)
    enc.writeUint(uint64(v.errorTypeId))

  of vrkNone:
    enc.writeMapHeader(2)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("None")
    enc.writePrecomputed(CborKeyTypeId)
    enc.writeUint(uint64(v.noneTypeId))

  of vrkCell:
    enc.writeMapHeader(2)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("Cell")
    enc.writePrecomputed(CborKeyPlace)
    enc.writeInt(int64(v.cellPlace))

  of vrkBigInt:
    enc.writeMapHeader(4)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("BigInt")
    enc.writePrecomputed(CborKeyB)
    enc.writeByteString(v.bigIntBytes)
    enc.writePrecomputed(CborKeyNegative)
    enc.writeBool(v.negative)
    enc.writePrecomputed(CborKeyTypeId)
    enc.writeUint(uint64(v.bigIntTypeId))

  of vrkChar:
    enc.writeMapHeader(3)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("Char")
    enc.writePrecomputed(CborKeyC)
    # Rust char is serialized as a string by serde
    enc.writeTextString($v.charVal)
    enc.writePrecomputed(CborKeyTypeId)
    enc.writeUint(uint64(v.charTypeId))

proc encodeCborValueRecord*(enc: var CborEncoder, v: ValueRecord) =
  encodeCborValueRecordImpl(enc, v)

# ---- Decode ValueRecord ----

proc decodeCborValueRecord*(dec: var CborDecoder): Result[ValueRecord, string] {.raises: [].}

proc decodeCborValueRecordImpl(dec: var CborDecoder): Result[ValueRecord, string] =
  discard ?dec.readMapHeader()

  # First key must be "kind"
  let kindKey = ?dec.readTextString()
  if kindKey != "kind":
    return err("cbor: expected 'kind' key, got '" & kindKey & "'")
  let kindStr = ?dec.readTextString()

  case kindStr
  of "Int":
    discard ?dec.readTextString()  # "i"
    let i = ?dec.readInt()
    discard ?dec.readTextString()  # "type_id"
    let tid = ?dec.readUint()
    ok(ValueRecord(kind: vrkInt, intVal: i, intTypeId: TypeId(tid)))

  of "Float":
    discard ?dec.readTextString()  # "f"
    let f = ?dec.readFloat64()
    discard ?dec.readTextString()  # "type_id"
    let tid = ?dec.readUint()
    ok(ValueRecord(kind: vrkFloat, floatVal: f, floatTypeId: TypeId(tid)))

  of "Bool":
    discard ?dec.readTextString()  # "b"
    let b = ?dec.readBool()
    discard ?dec.readTextString()  # "type_id"
    let tid = ?dec.readUint()
    ok(ValueRecord(kind: vrkBool, boolVal: b, boolTypeId: TypeId(tid)))

  of "String":
    discard ?dec.readTextString()  # "text"
    let text = ?dec.readTextString()
    discard ?dec.readTextString()  # "type_id"
    let tid = ?dec.readUint()
    ok(ValueRecord(kind: vrkString, text: text, strTypeId: TypeId(tid)))

  of "Sequence":
    discard ?dec.readTextString()  # "elements"
    let count = ?dec.readArrayHeader()
    var elems = newSeq[ValueRecord](int(count))
    for i in 0 ..< int(count):
      elems[i] = ?dec.decodeCborValueRecord()
    discard ?dec.readTextString()  # "is_slice"
    let isSlice = ?dec.readBool()
    discard ?dec.readTextString()  # "type_id"
    let tid = ?dec.readUint()
    ok(ValueRecord(kind: vrkSequence, seqElements: elems, isSlice: isSlice, seqTypeId: TypeId(tid)))

  of "Tuple":
    discard ?dec.readTextString()  # "elements"
    let count = ?dec.readArrayHeader()
    var elems = newSeq[ValueRecord](int(count))
    for i in 0 ..< int(count):
      elems[i] = ?dec.decodeCborValueRecord()
    discard ?dec.readTextString()  # "type_id"
    let tid = ?dec.readUint()
    ok(ValueRecord(kind: vrkTuple, tupleElements: elems, tupleTypeId: TypeId(tid)))

  of "Struct":
    discard ?dec.readTextString()  # "field_values"
    let count = ?dec.readArrayHeader()
    var elems = newSeq[ValueRecord](int(count))
    for i in 0 ..< int(count):
      elems[i] = ?dec.decodeCborValueRecord()
    discard ?dec.readTextString()  # "type_id"
    let tid = ?dec.readUint()
    ok(ValueRecord(kind: vrkStruct, fieldValues: elems, structTypeId: TypeId(tid)))

  of "Variant":
    discard ?dec.readTextString()  # "discriminator"
    let disc = ?dec.readTextString()
    discard ?dec.readTextString()  # "contents"
    let contents = @[?dec.decodeCborValueRecord()]
    discard ?dec.readTextString()  # "type_id"
    let tid = ?dec.readUint()
    ok(ValueRecord(kind: vrkVariant, discriminator: disc, contents: contents, variantTypeId: TypeId(tid)))

  of "Reference":
    discard ?dec.readTextString()  # "dereferenced"
    let deref = @[?dec.decodeCborValueRecord()]
    discard ?dec.readTextString()  # "address"
    let address = ?dec.readUint()
    discard ?dec.readTextString()  # "mutable"
    let mutable = ?dec.readBool()
    discard ?dec.readTextString()  # "type_id"
    let tid = ?dec.readUint()
    ok(ValueRecord(kind: vrkReference, dereferenced: deref, address: address, mutable: mutable, refTypeId: TypeId(tid)))

  of "Raw":
    discard ?dec.readTextString()  # "r"
    let r = ?dec.readTextString()
    discard ?dec.readTextString()  # "type_id"
    let tid = ?dec.readUint()
    ok(ValueRecord(kind: vrkRaw, rawStr: r, rawTypeId: TypeId(tid)))

  of "Error":
    discard ?dec.readTextString()  # "msg"
    let msg = ?dec.readTextString()
    discard ?dec.readTextString()  # "type_id"
    let tid = ?dec.readUint()
    ok(ValueRecord(kind: vrkError, errorMsg: msg, errorTypeId: TypeId(tid)))

  of "None":
    discard ?dec.readTextString()  # "type_id"
    let tid = ?dec.readUint()
    ok(ValueRecord(kind: vrkNone, noneTypeId: TypeId(tid)))

  of "Cell":
    discard ?dec.readTextString()  # "place"
    let place = ?dec.readInt()
    ok(ValueRecord(kind: vrkCell, cellPlace: Place(place)))

  of "BigInt":
    discard ?dec.readTextString()  # "b"
    let b = ?dec.readByteString()
    discard ?dec.readTextString()  # "negative"
    let neg = ?dec.readBool()
    discard ?dec.readTextString()  # "type_id"
    let tid = ?dec.readUint()
    ok(ValueRecord(kind: vrkBigInt, bigIntBytes: b, negative: neg, bigIntTypeId: TypeId(tid)))

  of "Char":
    discard ?dec.readTextString()  # "c"
    let cs = ?dec.readTextString()
    let c = if cs.len > 0: cs[0] else: '\0'
    discard ?dec.readTextString()  # "type_id"
    let tid = ?dec.readUint()
    ok(ValueRecord(kind: vrkChar, charVal: c, charTypeId: TypeId(tid)))

  else:
    err("cbor: unknown ValueRecord kind: '" & kindStr & "'")

proc decodeCborValueRecord*(dec: var CborDecoder): Result[ValueRecord, string] =
  decodeCborValueRecordImpl(dec)

# ---- TypeRecord ----
# Rust: TypeRecord { kind: TypeKind, lang_type: String, specific_info: TypeSpecificInfo }
# TypeSpecificInfo is #[serde(tag = "kind")] enum

proc encodeCborTypeSpecificInfo(enc: var CborEncoder, si: TypeSpecificInfo) {.inline.} =
  case si.kind
  of tsikNone:
    enc.writeMapHeader(1)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("None")
  of tsikStruct:
    enc.writeMapHeader(2)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("Struct")
    enc.writePrecomputed(CborKeyFields)
    enc.writeArrayHeader(uint64(si.fields.len))
    for f in si.fields:
      enc.writeMapHeader(2)
      enc.writePrecomputed(CborKeyName)
      enc.writeTextString(f.name)
      enc.writePrecomputed(CborKeyTypeId)
      enc.writeUint(uint64(f.typeId))
  of tsikPointer:
    enc.writeMapHeader(2)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("Pointer")
    enc.writePrecomputed(CborKeyDereferenceTypeId)
    enc.writeUint(uint64(si.dereferenceTypeId))

proc encodeCborTypeRecord*(enc: var CborEncoder, t: TypeRecord) {.inline.} =
  enc.writeMapHeader(3)
  enc.writePrecomputed(CborKeyKind)
  # TypeKind uses serde_repr — serialized as u8 integer
  enc.writeUint(uint64(ord(t.kind)))
  enc.writePrecomputed(CborKeyLangType)
  enc.writeTextString(t.langType)
  enc.writePrecomputed(CborKeySpecificInfo)
  enc.encodeCborTypeSpecificInfo(t.specificInfo)

proc decodeCborTypeSpecificInfo(dec: var CborDecoder): Result[TypeSpecificInfo, string] =
  discard ?dec.readMapHeader()
  let kindKey = ?dec.readTextString()
  if kindKey != "kind":
    return err("cbor: expected 'kind' in TypeSpecificInfo, got '" & kindKey & "'")
  let kindStr = ?dec.readTextString()

  case kindStr
  of "None":
    ok(TypeSpecificInfo(kind: tsikNone))
  of "Struct":
    discard ?dec.readTextString()  # "fields"
    let count = ?dec.readArrayHeader()
    var fields = newSeq[FieldTypeRecord](int(count))
    for i in 0 ..< int(count):
      discard ?dec.readMapHeader()  # 2
      discard ?dec.readTextString()  # "name"
      let name = ?dec.readTextString()
      discard ?dec.readTextString()  # "type_id"
      let tid = ?dec.readUint()
      fields[i] = FieldTypeRecord(name: name, typeId: TypeId(tid))
    ok(TypeSpecificInfo(kind: tsikStruct, fields: fields))
  of "Pointer":
    discard ?dec.readTextString()  # "dereference_type_id"
    let tid = ?dec.readUint()
    ok(TypeSpecificInfo(kind: tsikPointer, dereferenceTypeId: TypeId(tid)))
  else:
    err("cbor: unknown TypeSpecificInfo kind: '" & kindStr & "'")

proc decodeCborTypeRecord*(dec: var CborDecoder): Result[TypeRecord, string] =
  discard ?dec.readMapHeader()
  discard ?dec.readTextString()  # "kind"
  let kindVal = ?dec.readUint()
  if int(kindVal) > ord(high(TypeKind)):
    return err("cbor: invalid TypeKind: " & $kindVal)
  let kind = TypeKind(kindVal)
  discard ?dec.readTextString()  # "lang_type"
  let langType = ?dec.readTextString()
  discard ?dec.readTextString()  # "specific_info"
  let si = ?dec.decodeCborTypeSpecificInfo()
  ok(TypeRecord(kind: kind, langType: langType, specificInfo: si))

# ---- AssignmentRecord ----
# Rust: AssignmentRecord { to: VariableId, pass_by: PassBy, from: RValue }
# PassBy is a normal enum (serde default: externally tagged)
# RValue is #[serde(tag = "kind")] enum

proc encodeCborRValue(enc: var CborEncoder, rv: RValue) {.inline.} =
  case rv.kind
  of rvkSimple:
    enc.writeMapHeader(2)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("Simple")
    enc.writePrecomputed(CborKey0)
    enc.writeUint(uint64(rv.simpleId))

  of rvkCompound:
    enc.writeMapHeader(2)
    enc.writePrecomputed(CborKeyKind)
    enc.writeTextString("Compound")
    enc.writePrecomputed(CborKey0)
    enc.writeArrayHeader(uint64(rv.compoundIds.len))
    for id in rv.compoundIds:
      enc.writeUint(uint64(id))

proc encodeCborAssignmentRecord*(enc: var CborEncoder, a: AssignmentRecord) {.inline.} =
  enc.writeMapHeader(3)
  enc.writePrecomputed(CborKeyTo)
  enc.writeUint(uint64(a.to))
  enc.writePrecomputed(CborKeyPassBy)
  # PassBy is a regular enum, serde default = externally tagged strings
  case a.passBy
  of pbValue: enc.writeTextString("Value")
  of pbReference: enc.writeTextString("Reference")
  enc.writePrecomputed(CborKeyFrom)
  enc.encodeCborRValue(a.frm)

proc decodeCborRValue(dec: var CborDecoder): Result[RValue, string] =
  discard ?dec.readMapHeader()
  let kindKey = ?dec.readTextString()
  if kindKey != "kind":
    return err("cbor: expected 'kind' in RValue, got '" & kindKey & "'")
  let kindStr = ?dec.readTextString()

  case kindStr
  of "Simple":
    discard ?dec.readTextString()  # "0"
    let id = ?dec.readUint()
    ok(RValue(kind: rvkSimple, simpleId: VariableId(id)))
  of "Compound":
    discard ?dec.readTextString()  # "0"
    let count = ?dec.readArrayHeader()
    var ids = newSeq[VariableId](int(count))
    for i in 0 ..< int(count):
      ids[i] = VariableId(?dec.readUint())
    ok(RValue(kind: rvkCompound, compoundIds: ids))
  else:
    err("cbor: unknown RValue kind: '" & kindStr & "'")

proc decodeCborAssignmentRecord*(dec: var CborDecoder): Result[AssignmentRecord, string] =
  discard ?dec.readMapHeader()
  discard ?dec.readTextString()  # "to"
  let toId = ?dec.readUint()
  discard ?dec.readTextString()  # "pass_by"
  let passStr = ?dec.readTextString()
  let passBy = case passStr
    of "Value": pbValue
    of "Reference": pbReference
    else: return err("cbor: unknown PassBy: '" & passStr & "'")
  discard ?dec.readTextString()  # "from"
  let frm = ?dec.decodeCborRValue()
  ok(AssignmentRecord(to: VariableId(toId), passBy: passBy, frm: frm))

# ---- FullValueRecord ----

proc encodeCborFullValueRecord*(enc: var CborEncoder, fvr: FullValueRecord) {.inline.} =
  enc.writeMapHeader(2)
  enc.writePrecomputed(CborKeyVariableId)
  enc.writeUint(uint64(fvr.variableId))
  enc.writePrecomputed(CborKeyValue)
  enc.encodeCborValueRecord(fvr.value)

proc decodeCborFullValueRecord*(dec: var CborDecoder): Result[FullValueRecord, string] =
  discard ?dec.readMapHeader()
  discard ?dec.readTextString()  # "variable_id"
  let vid = ?dec.readUint()
  discard ?dec.readTextString()  # "value"
  let value = ?dec.decodeCborValueRecord()
  ok(FullValueRecord(variableId: VariableId(vid), value: value))

# ---- CallArgs (Vec[FullValueRecord]) ----

proc encodeCborCallArgs*(enc: var CborEncoder, args: seq[FullValueRecord]) {.inline.} =
  enc.writeArrayHeader(uint64(args.len))
  for arg in args:
    enc.encodeCborFullValueRecord(arg)

proc decodeCborCallArgs*(dec: var CborDecoder): Result[seq[FullValueRecord], string] =
  let count = ?dec.readArrayHeader()
  var args = newSeq[FullValueRecord](int(count))
  for i in 0 ..< int(count):
    args[i] = ?dec.decodeCborFullValueRecord()
  ok(args)
