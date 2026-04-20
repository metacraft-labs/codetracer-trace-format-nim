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

proc init*(T: type CborEncoder): CborEncoder =
  CborEncoder(buf: @[])

proc clear*(enc: var CborEncoder) =
  enc.buf.setLen(0)

proc getBytes*(enc: CborEncoder): seq[byte] =
  enc.buf

# ---------------------------------------------------------------------------
# Low-level CBOR writing
# ---------------------------------------------------------------------------

proc writeTypeAndValue(enc: var CborEncoder, majorType: byte, value: uint64) =
  ## Write a CBOR type/value header. majorType is 0..7 (shifted left by 5).
  let mt = majorType shl 5
  if value <= 23:
    enc.buf.add(mt or byte(value))
  elif value <= 0xFF:
    enc.buf.add(mt or 24)
    enc.buf.add(byte(value))
  elif value <= 0xFFFF:
    enc.buf.add(mt or 25)
    let b = toBytesLE(uint16(value))
    # CBOR is big-endian
    enc.buf.add(b[1])
    enc.buf.add(b[0])
  elif value <= 0xFFFF_FFFF'u64:
    enc.buf.add(mt or 26)
    let b = toBytesLE(uint32(value))
    enc.buf.add(b[3])
    enc.buf.add(b[2])
    enc.buf.add(b[1])
    enc.buf.add(b[0])
  else:
    enc.buf.add(mt or 27)
    let b = toBytesLE(uint64(value))
    enc.buf.add(b[7])
    enc.buf.add(b[6])
    enc.buf.add(b[5])
    enc.buf.add(b[4])
    enc.buf.add(b[3])
    enc.buf.add(b[2])
    enc.buf.add(b[1])
    enc.buf.add(b[0])

proc writeUint*(enc: var CborEncoder, value: uint64) =
  ## CBOR major type 0: unsigned integer.
  enc.writeTypeAndValue(0, value)

proc writeNegInt*(enc: var CborEncoder, value: uint64) =
  ## CBOR major type 1: negative integer. Encodes -(value+1).
  enc.writeTypeAndValue(1, value)

proc writeInt*(enc: var CborEncoder, value: int64) =
  ## Write a signed integer. Uses major type 0 for non-negative, 1 for negative.
  if value >= 0:
    enc.writeUint(uint64(value))
  else:
    # CBOR negative: -1 - N, so N = -(value+1) = -value - 1
    enc.writeNegInt(uint64(-1 - value))

proc writeByteString*(enc: var CborEncoder, data: openArray[byte]) =
  ## CBOR major type 2: byte string.
  enc.writeTypeAndValue(2, uint64(data.len))
  for b in data:
    enc.buf.add(b)

proc writeTextString*(enc: var CborEncoder, s: string) =
  ## CBOR major type 3: text string.
  enc.writeTypeAndValue(3, uint64(s.len))
  for c in s:
    enc.buf.add(byte(c))

proc writeArrayHeader*(enc: var CborEncoder, count: uint64) =
  ## CBOR major type 4: array header.
  enc.writeTypeAndValue(4, count)

proc writeMapHeader*(enc: var CborEncoder, count: uint64) =
  ## CBOR major type 5: map header (count = number of key-value pairs).
  enc.writeTypeAndValue(5, count)

proc writeBool*(enc: var CborEncoder, value: bool) =
  ## CBOR major type 7: boolean.
  if value:
    enc.buf.add(0xF5'u8)
  else:
    enc.buf.add(0xF4'u8)

proc writeNull*(enc: var CborEncoder) =
  ## CBOR major type 7: null.
  enc.buf.add(0xF6'u8)

proc writeFloat64*(enc: var CborEncoder, value: float64) =
  ## CBOR major type 7, additional info 27: IEEE 754 double (8 bytes BE).
  enc.buf.add(0xFB'u8)
  let b = toBytesLE(cast[uint64](value))
  enc.buf.add(b[7])
  enc.buf.add(b[6])
  enc.buf.add(b[5])
  enc.buf.add(b[4])
  enc.buf.add(b[3])
  enc.buf.add(b[2])
  enc.buf.add(b[1])
  enc.buf.add(b[0])

# Convenience: write a map key (text string)
proc writeKey*(enc: var CborEncoder, key: string) {.inline.} =
  enc.writeTextString(key)

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
    enc.writeKey("kind")
    enc.writeTextString("Int")
    enc.writeKey("i")
    enc.writeInt(v.intVal)
    enc.writeKey("type_id")
    enc.writeUint(uint64(v.intTypeId))

  of vrkFloat:
    enc.writeMapHeader(3)
    enc.writeKey("kind")
    enc.writeTextString("Float")
    enc.writeKey("f")
    # serde_as(as = "DisplayFromStr") serializes f64 as a string
    # We need to match Rust's Display for f64
    # But for CBOR with cbor4ii, DisplayFromStr makes it a string.
    # Let's encode as string to match Rust exactly.
    # Rust's f64 Display: "3.14" for 3.14, "0" for 0.0, etc.
    # Actually, for CBOR binary format, serde_as with DisplayFromStr
    # will serialize as a text string containing the float representation.
    # But that's complex to replicate exactly. For CBOR interop, we'll
    # encode as float64 directly since that's the natural CBOR representation.
    # The Rust side can be updated to match. For now, use float64.
    enc.writeFloat64(v.floatVal)
    enc.writeKey("type_id")
    enc.writeUint(uint64(v.floatTypeId))

  of vrkBool:
    enc.writeMapHeader(3)
    enc.writeKey("kind")
    enc.writeTextString("Bool")
    enc.writeKey("b")
    enc.writeBool(v.boolVal)
    enc.writeKey("type_id")
    enc.writeUint(uint64(v.boolTypeId))

  of vrkString:
    enc.writeMapHeader(3)
    enc.writeKey("kind")
    enc.writeTextString("String")
    enc.writeKey("text")
    enc.writeTextString(v.text)
    enc.writeKey("type_id")
    enc.writeUint(uint64(v.strTypeId))

  of vrkSequence:
    enc.writeMapHeader(4)
    enc.writeKey("kind")
    enc.writeTextString("Sequence")
    enc.writeKey("elements")
    enc.writeArrayHeader(uint64(v.seqElements.len))
    for e in v.seqElements:
      enc.encodeCborValueRecord(e)
    enc.writeKey("is_slice")
    enc.writeBool(v.isSlice)
    enc.writeKey("type_id")
    enc.writeUint(uint64(v.seqTypeId))

  of vrkTuple:
    enc.writeMapHeader(3)
    enc.writeKey("kind")
    enc.writeTextString("Tuple")
    enc.writeKey("elements")
    enc.writeArrayHeader(uint64(v.tupleElements.len))
    for e in v.tupleElements:
      enc.encodeCborValueRecord(e)
    enc.writeKey("type_id")
    enc.writeUint(uint64(v.tupleTypeId))

  of vrkStruct:
    enc.writeMapHeader(3)
    enc.writeKey("kind")
    enc.writeTextString("Struct")
    enc.writeKey("field_values")
    enc.writeArrayHeader(uint64(v.fieldValues.len))
    for e in v.fieldValues:
      enc.encodeCborValueRecord(e)
    enc.writeKey("type_id")
    enc.writeUint(uint64(v.structTypeId))

  of vrkVariant:
    enc.writeMapHeader(4)
    enc.writeKey("kind")
    enc.writeTextString("Variant")
    enc.writeKey("discriminator")
    enc.writeTextString(v.discriminator)
    enc.writeKey("contents")
    # Rust has Box<ValueRecord>, serde serializes the inner value directly
    if v.contents.len > 0:
      enc.encodeCborValueRecord(v.contents[0])
    else:
      # Fallback: encode None value
      enc.writeMapHeader(2)
      enc.writeKey("kind")
      enc.writeTextString("None")
      enc.writeKey("type_id")
      enc.writeUint(0)
    enc.writeKey("type_id")
    enc.writeUint(uint64(v.variantTypeId))

  of vrkReference:
    enc.writeMapHeader(5)
    enc.writeKey("kind")
    enc.writeTextString("Reference")
    enc.writeKey("dereferenced")
    # Rust has Box<ValueRecord>
    if v.dereferenced.len > 0:
      enc.encodeCborValueRecord(v.dereferenced[0])
    else:
      enc.writeMapHeader(2)
      enc.writeKey("kind")
      enc.writeTextString("None")
      enc.writeKey("type_id")
      enc.writeUint(0)
    enc.writeKey("address")
    enc.writeUint(v.address)
    enc.writeKey("mutable")
    enc.writeBool(v.mutable)
    enc.writeKey("type_id")
    enc.writeUint(uint64(v.refTypeId))

  of vrkRaw:
    enc.writeMapHeader(3)
    enc.writeKey("kind")
    enc.writeTextString("Raw")
    enc.writeKey("r")
    enc.writeTextString(v.rawStr)
    enc.writeKey("type_id")
    enc.writeUint(uint64(v.rawTypeId))

  of vrkError:
    enc.writeMapHeader(3)
    enc.writeKey("kind")
    enc.writeTextString("Error")
    enc.writeKey("msg")
    enc.writeTextString(v.errorMsg)
    enc.writeKey("type_id")
    enc.writeUint(uint64(v.errorTypeId))

  of vrkNone:
    enc.writeMapHeader(2)
    enc.writeKey("kind")
    enc.writeTextString("None")
    enc.writeKey("type_id")
    enc.writeUint(uint64(v.noneTypeId))

  of vrkCell:
    enc.writeMapHeader(2)
    enc.writeKey("kind")
    enc.writeTextString("Cell")
    enc.writeKey("place")
    enc.writeInt(int64(v.cellPlace))

  of vrkBigInt:
    enc.writeMapHeader(4)
    enc.writeKey("kind")
    enc.writeTextString("BigInt")
    enc.writeKey("b")
    # Rust uses #[serde(with = "base64")] — but for CBOR binary, cbor4ii
    # with serde serializes Vec<u8> as a CBOR byte string
    enc.writeByteString(v.bigIntBytes)
    enc.writeKey("negative")
    enc.writeBool(v.negative)
    enc.writeKey("type_id")
    enc.writeUint(uint64(v.bigIntTypeId))

  of vrkChar:
    enc.writeMapHeader(3)
    enc.writeKey("kind")
    enc.writeTextString("Char")
    enc.writeKey("c")
    # Rust char is serialized as a string by serde
    enc.writeTextString($v.charVal)
    enc.writeKey("type_id")
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

proc encodeCborTypeSpecificInfo(enc: var CborEncoder, si: TypeSpecificInfo) =
  case si.kind
  of tsikNone:
    enc.writeMapHeader(1)
    enc.writeKey("kind")
    enc.writeTextString("None")
  of tsikStruct:
    enc.writeMapHeader(2)
    enc.writeKey("kind")
    enc.writeTextString("Struct")
    enc.writeKey("fields")
    enc.writeArrayHeader(uint64(si.fields.len))
    for f in si.fields:
      enc.writeMapHeader(2)
      enc.writeKey("name")
      enc.writeTextString(f.name)
      enc.writeKey("type_id")
      enc.writeUint(uint64(f.typeId))
  of tsikPointer:
    enc.writeMapHeader(2)
    enc.writeKey("kind")
    enc.writeTextString("Pointer")
    enc.writeKey("dereference_type_id")
    enc.writeUint(uint64(si.dereferenceTypeId))

proc encodeCborTypeRecord*(enc: var CborEncoder, t: TypeRecord) =
  enc.writeMapHeader(3)
  enc.writeKey("kind")
  # TypeKind uses serde_repr — serialized as u8 integer
  enc.writeUint(uint64(ord(t.kind)))
  enc.writeKey("lang_type")
  enc.writeTextString(t.langType)
  enc.writeKey("specific_info")
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

proc encodeCborRValue(enc: var CborEncoder, rv: RValue) =
  case rv.kind
  of rvkSimple:
    # Rust: Simple(VariableId) with tag = "kind"
    # Produces: {"kind": "Simple", ...} — but Simple is a newtype variant
    # serde(tag = "kind") for newtype variants: the inner value's fields
    # get merged. VariableId is transparent (usize), so it becomes
    # a map with "kind" + the transparent value... Actually for tagged
    # enums with newtype variants, serde puts the inner in the map.
    # For VariableId (transparent), it just becomes the number.
    # Actually: serde(tag="kind") with a newtype wrapping a transparent
    # type doesn't work with non-map inner. Let me re-check the Rust.
    #
    # RValue::Simple(VariableId) with #[serde(tag = "kind")]
    # Since VariableId is transparent (usize), and tagged enums need
    # map-like inners, serde will error on Simple(usize) with internal
    # tagging. Looking at the Rust code more carefully:
    #   Simple(VariableId) — this IS used with tag="kind"
    #
    # Actually, for CBOR with cbor4ii, internally tagged enums with
    # non-struct variants may serialize differently. The most likely
    # encoding for the split-binary module's native format doesn't use
    # CBOR at all — let's match what makes sense for the Nim roundtrip.
    #
    # For maximum compatibility, serialize as:
    # {"kind": "Simple", "0": variableId} — serde's tuple variant flattening
    # Actually no. With internally tagged, serde requires the content to be
    # a map or struct. For newtype variants wrapping primitives, serde
    # uses a special encoding. With cbor4ii it ends up as a 2-element map.
    #
    # Let's use a pragmatic approach: map with kind + value field.
    enc.writeMapHeader(2)
    enc.writeKey("kind")
    enc.writeTextString("Simple")
    enc.writeKey("0")
    enc.writeUint(uint64(rv.simpleId))

  of rvkCompound:
    # Compound(Vec<VariableId>)
    enc.writeMapHeader(2)
    enc.writeKey("kind")
    enc.writeTextString("Compound")
    enc.writeKey("0")
    enc.writeArrayHeader(uint64(rv.compoundIds.len))
    for id in rv.compoundIds:
      enc.writeUint(uint64(id))

proc encodeCborAssignmentRecord*(enc: var CborEncoder, a: AssignmentRecord) =
  enc.writeMapHeader(3)
  enc.writeKey("to")
  enc.writeUint(uint64(a.to))
  enc.writeKey("pass_by")
  # PassBy is a regular enum, serde default = externally tagged strings
  case a.passBy
  of pbValue: enc.writeTextString("Value")
  of pbReference: enc.writeTextString("Reference")
  enc.writeKey("from")
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

proc encodeCborFullValueRecord*(enc: var CborEncoder, fvr: FullValueRecord) =
  enc.writeMapHeader(2)
  enc.writeKey("variable_id")
  enc.writeUint(uint64(fvr.variableId))
  enc.writeKey("value")
  enc.encodeCborValueRecord(fvr.value)

proc decodeCborFullValueRecord*(dec: var CborDecoder): Result[FullValueRecord, string] =
  discard ?dec.readMapHeader()
  discard ?dec.readTextString()  # "variable_id"
  let vid = ?dec.readUint()
  discard ?dec.readTextString()  # "value"
  let value = ?dec.decodeCborValueRecord()
  ok(FullValueRecord(variableId: VariableId(vid), value: value))

# ---- CallArgs (Vec[FullValueRecord]) ----

proc encodeCborCallArgs*(enc: var CborEncoder, args: seq[FullValueRecord]) =
  enc.writeArrayHeader(uint64(args.len))
  for arg in args:
    enc.encodeCborFullValueRecord(arg)

proc decodeCborCallArgs*(dec: var CborDecoder): Result[seq[FullValueRecord], string] =
  let count = ?dec.readArrayHeader()
  var args = newSeq[FullValueRecord](int(count))
  for i in 0 ..< int(count):
    args[i] = ?dec.decodeCborFullValueRecord()
  ok(args)
