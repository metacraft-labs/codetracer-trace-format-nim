{.push raises: [].}

## Streaming value encoder — writes CBOR bytes directly without building
## an intermediate ValueRecord tree. Produces byte-identical output to
## `encodeCborValueRecord` in cbor.nim.
##
## Usage:
##   var sve = StreamingValueEncoder.init()
##   discard sve.writeInt(42, typeId = 7)
##   let bytes = sve.getBytes()
##   sve.reset()  # reuse for next value

import results
import ./cbor

export results

const MaxNestingDepth* = 32

type
  CompoundKind* = enum
    ckStruct      ## CBOR map with "kind":"Struct", "field_values" array
    ckSequence    ## CBOR map with "kind":"Sequence", "elements" array + "is_slice" + "type_id"
    ckTuple       ## CBOR map with "kind":"Tuple", "elements" array + "type_id"
    ckVariant     ## CBOR map with "kind":"Variant", "discriminator", "contents" value, "type_id"
    ckReference   ## CBOR map with "kind":"Reference", "dereferenced" value, "address", "mutable", "type_id"

  CompoundFrame = object
    kind: CompoundKind
    expectedCount: int
    writtenCount: int
    typeId: uint64
    isSlice: bool   # only used for ckSequence
    address: uint64 # only used for ckReference
    mutable: bool   # only used for ckReference

  StreamingValueEncoder* = object
    enc*: CborEncoder
    stack: array[MaxNestingDepth, CompoundFrame]
    depth: int
    finished: bool

proc init*(T: type StreamingValueEncoder, capacity: int = 256): StreamingValueEncoder =
  StreamingValueEncoder(
    enc: CborEncoder.init(capacity),
    depth: 0,
    finished: false
  )

proc reset*(sve: var StreamingValueEncoder) =
  sve.enc.clear()
  sve.depth = 0
  sve.finished = false

proc getBytes*(sve: StreamingValueEncoder): seq[byte] =
  sve.enc.getBytes()

proc getBytesView*(sve: StreamingValueEncoder): lent seq[byte] =
  ## Return view of internal buffer without copying.
  sve.enc.buf

# --- Leaf value writers ---
# Each produces a complete CBOR map matching encodeCborValueRecordImpl exactly.

proc writeInt*(sve: var StreamingValueEncoder, value: int64, typeId: uint64): Result[void, string] =
  ## map(3) { "kind":"Int", "i": value, "type_id": typeId }
  sve.enc.writeMapHeader(3)
  sve.enc.writePrecomputed(CborKeyKind)
  sve.enc.writeTextString("Int")
  sve.enc.writePrecomputed(CborKeyI)
  sve.enc.writeInt(value)
  sve.enc.writePrecomputed(CborKeyTypeId)
  sve.enc.writeUint(typeId)
  ok()

proc writeFloat*(sve: var StreamingValueEncoder, value: float64, typeId: uint64): Result[void, string] =
  ## map(3) { "kind":"Float", "f": value, "type_id": typeId }
  sve.enc.writeMapHeader(3)
  sve.enc.writePrecomputed(CborKeyKind)
  sve.enc.writeTextString("Float")
  sve.enc.writePrecomputed(CborKeyF)
  sve.enc.writeFloat64(value)
  sve.enc.writePrecomputed(CborKeyTypeId)
  sve.enc.writeUint(typeId)
  ok()

proc writeBool*(sve: var StreamingValueEncoder, value: bool, typeId: uint64): Result[void, string] =
  ## map(4) { "kind":"Bool", "b": value, "text": "true"|"false", "type_id": typeId }
  ## The `text` field carries the printed boolean so ct-print / downstream
  ## decoders can surface it without re-deriving from `b` — matching how
  ## generic `wrap_value<bool>(true)` call args need `value.text == "true"`
  ## (see codetracer-move-recorder test_generics_bool_arg_decodes_text).
  sve.enc.writeMapHeader(4)
  sve.enc.writePrecomputed(CborKeyKind)
  sve.enc.writeTextString("Bool")
  sve.enc.writePrecomputed(CborKeyB)
  sve.enc.writeBool(value)
  sve.enc.writePrecomputed(CborKeyText)
  sve.enc.writeTextString(if value: "true" else: "false")
  sve.enc.writePrecomputed(CborKeyTypeId)
  sve.enc.writeUint(typeId)
  ok()

proc writeString*(sve: var StreamingValueEncoder, text: string, typeId: uint64): Result[void, string] =
  ## map(3) { "kind":"String", "text": textString, "type_id": typeId }
  sve.enc.writeMapHeader(3)
  sve.enc.writePrecomputed(CborKeyKind)
  sve.enc.writeTextString("String")
  sve.enc.writePrecomputed(CborKeyText)
  sve.enc.writeTextString(text)
  sve.enc.writePrecomputed(CborKeyTypeId)
  sve.enc.writeUint(typeId)
  ok()

proc writeNone*(sve: var StreamingValueEncoder, typeId: uint64): Result[void, string] =
  ## map(2) { "kind":"None", "type_id": typeId }
  sve.enc.writeMapHeader(2)
  sve.enc.writePrecomputed(CborKeyKind)
  sve.enc.writeTextString("None")
  sve.enc.writePrecomputed(CborKeyTypeId)
  sve.enc.writeUint(typeId)
  ok()

proc writeRaw*(sve: var StreamingValueEncoder, rawStr: string, typeId: uint64): Result[void, string] =
  ## map(3) { "kind":"Raw", "r": textString, "type_id": typeId }
  sve.enc.writeMapHeader(3)
  sve.enc.writePrecomputed(CborKeyKind)
  sve.enc.writeTextString("Raw")
  sve.enc.writePrecomputed(CborKeyR)
  sve.enc.writeTextString(rawStr)
  sve.enc.writePrecomputed(CborKeyTypeId)
  sve.enc.writeUint(typeId)
  ok()

proc writeRef*(sve: var StreamingValueEncoder, refId: uint32): Result[void, string] =
  ## Write a ValueRef — CBOR tag 256 + unsigned int ref_id.
  ## References a previously-encoded compound value.
  sve.enc.writeTag(CborTagValueRef)
  sve.enc.writeUint(uint64(refId))
  ok()

proc writeError*(sve: var StreamingValueEncoder, msg: string, typeId: uint64): Result[void, string] =
  ## map(3) { "kind":"Error", "msg": textString, "type_id": typeId }
  sve.enc.writeMapHeader(3)
  sve.enc.writePrecomputed(CborKeyKind)
  sve.enc.writeTextString("Error")
  sve.enc.writePrecomputed(CborKeyMsg)
  sve.enc.writeTextString(msg)
  sve.enc.writePrecomputed(CborKeyTypeId)
  sve.enc.writeUint(typeId)
  ok()

proc writeChar*(sve: var StreamingValueEncoder, c: char, typeId: uint64): Result[void, string] =
  ## map(3) { "kind":"Char", "c": textString(1), "type_id": typeId }
  ## Mirrors `encodeCborValueRecordImpl(vrkChar)` in cbor.nim — Rust serde
  ## serializes `char` as a single-codepoint string.
  sve.enc.writeMapHeader(3)
  sve.enc.writePrecomputed(CborKeyKind)
  sve.enc.writeTextString("Char")
  sve.enc.writePrecomputed(CborKeyC)
  sve.enc.writeTextString($c)
  sve.enc.writePrecomputed(CborKeyTypeId)
  sve.enc.writeUint(typeId)
  ok()

proc writeEnum*(sve: var StreamingValueEncoder, name: string, ordinal: int64,
                typeId: uint64): Result[void, string] =
  ## CTFS-M-TypeSchema: emit an Enum leaf — `{kind:"Enum", name:..., ordinal:..., type_id:...}`.
  sve.enc.writeMapHeader(4)
  sve.enc.writePrecomputed(CborKeyKind)
  sve.enc.writeTextString("Enum")
  sve.enc.writePrecomputed(CborKeyName)
  sve.enc.writeTextString(name)
  sve.enc.writePrecomputed(CborKeyOrdinal)
  sve.enc.writeInt(ordinal)
  sve.enc.writePrecomputed(CborKeyTypeId)
  sve.enc.writeUint(typeId)
  ok()

proc writeBigInt*(sve: var StreamingValueEncoder, bytes: openArray[byte], negative: bool,
                  typeId: uint64): Result[void, string] =
  ## map(4) { "kind":"BigInt", "b": byteString, "negative": bool, "type_id": typeId }
  ## `b` carries the big-endian unsigned magnitude bytes; sign lives in `negative`.
  sve.enc.writeMapHeader(4)
  sve.enc.writePrecomputed(CborKeyKind)
  sve.enc.writeTextString("BigInt")
  sve.enc.writePrecomputed(CborKeyB)
  sve.enc.writeByteString(bytes)
  sve.enc.writePrecomputed(CborKeyNegative)
  sve.enc.writeBool(negative)
  sve.enc.writePrecomputed(CborKeyTypeId)
  sve.enc.writeUint(typeId)
  ok()

# --- Compound value writers ---

proc beginStruct*(sve: var StreamingValueEncoder, typeId: uint64, fieldCount: int): Result[void, string] =
  ## Begin a struct value. Write fieldCount child values, then call endCompound().
  ## Layout: map(3) { "kind":"Struct", "field_values": array(fieldCount), "type_id": typeId }
  if sve.depth >= MaxNestingDepth:
    return err("nesting too deep (max " & $MaxNestingDepth & ")")
  sve.enc.writeMapHeader(3)
  sve.enc.writePrecomputed(CborKeyKind)
  sve.enc.writeTextString("Struct")
  sve.enc.writePrecomputed(CborKeyFieldValues)
  sve.enc.writeArrayHeader(uint64(fieldCount))
  # type_id is written by endCompound after the array elements
  sve.stack[sve.depth] = CompoundFrame(
    kind: ckStruct, expectedCount: fieldCount, writtenCount: 0, typeId: typeId)
  sve.depth += 1
  ok()

proc beginSequence*(sve: var StreamingValueEncoder, typeId: uint64, elementCount: int,
                    isSlice: bool = false): Result[void, string] =
  ## Begin a sequence value. Write elementCount child values, then call endCompound().
  ## Layout: map(4) { "kind":"Sequence", "elements": array(N), "is_slice": bool, "type_id": typeId }
  if sve.depth >= MaxNestingDepth:
    return err("nesting too deep (max " & $MaxNestingDepth & ")")
  sve.enc.writeMapHeader(4)
  sve.enc.writePrecomputed(CborKeyKind)
  sve.enc.writeTextString("Sequence")
  sve.enc.writePrecomputed(CborKeyElements)
  sve.enc.writeArrayHeader(uint64(elementCount))
  # is_slice and type_id are written by endCompound after the array elements
  sve.stack[sve.depth] = CompoundFrame(
    kind: ckSequence, expectedCount: elementCount, writtenCount: 0,
    typeId: typeId, isSlice: isSlice)
  sve.depth += 1
  ok()

proc beginVariant*(sve: var StreamingValueEncoder, discriminator: string,
                   typeId: uint64): Result[void, string] =
  ## Begin a variant value. Write exactly one inner value, then call endCompound().
  ## Layout: map(4) { "kind":"Variant", "discriminator": textString, "contents": value, "type_id": typeId }
  if sve.depth >= MaxNestingDepth:
    return err("nesting too deep (max " & $MaxNestingDepth & ")")
  sve.enc.writeMapHeader(4)
  sve.enc.writePrecomputed(CborKeyKind)
  sve.enc.writeTextString("Variant")
  sve.enc.writePrecomputed(CborKeyDiscriminator)
  sve.enc.writeTextString(discriminator)
  sve.enc.writePrecomputed(CborKeyContents)
  # type_id is written by endCompound after the inner value
  sve.stack[sve.depth] = CompoundFrame(
    kind: ckVariant, expectedCount: 1, writtenCount: 0, typeId: typeId)
  sve.depth += 1
  ok()

proc beginReference*(sve: var StreamingValueEncoder, address: uint64, mutable: bool,
                     typeId: uint64): Result[void, string] =
  ## Begin a reference value. Write exactly one inner (dereferenced) value, then call endCompound().
  ## Layout: map(5) { "kind":"Reference", "dereferenced": value, "address": uint, "mutable": bool, "type_id": typeId }
  if sve.depth >= MaxNestingDepth:
    return err("nesting too deep (max " & $MaxNestingDepth & ")")
  sve.enc.writeMapHeader(5)
  sve.enc.writePrecomputed(CborKeyKind)
  sve.enc.writeTextString("Reference")
  sve.enc.writePrecomputed(CborKeyDereferenced)
  # address, mutable, type_id are written by endCompound after the inner value
  sve.stack[sve.depth] = CompoundFrame(
    kind: ckReference, expectedCount: 1, writtenCount: 0, typeId: typeId,
    address: address, mutable: mutable)
  sve.depth += 1
  ok()

proc beginTuple*(sve: var StreamingValueEncoder, typeId: uint64, elementCount: int): Result[void, string] =
  ## Begin a tuple value. Write elementCount child values, then call endCompound().
  ## Layout: map(3) { "kind":"Tuple", "elements": array(N), "type_id": typeId }
  if sve.depth >= MaxNestingDepth:
    return err("nesting too deep (max " & $MaxNestingDepth & ")")
  sve.enc.writeMapHeader(3)
  sve.enc.writePrecomputed(CborKeyKind)
  sve.enc.writeTextString("Tuple")
  sve.enc.writePrecomputed(CborKeyElements)
  sve.enc.writeArrayHeader(uint64(elementCount))
  # type_id is written by endCompound
  sve.stack[sve.depth] = CompoundFrame(
    kind: ckTuple, expectedCount: elementCount, writtenCount: 0, typeId: typeId)
  sve.depth += 1
  ok()

proc endCompound*(sve: var StreamingValueEncoder): Result[void, string] =
  ## End the current compound value (struct, sequence, tuple).
  ## Writes trailing map fields (is_slice, type_id) that follow the array.
  if sve.depth <= 0:
    return err("endCompound without matching begin")
  sve.depth -= 1
  let frame = sve.stack[sve.depth]
  case frame.kind
  of ckStruct:
    # After field_values array: write type_id
    sve.enc.writePrecomputed(CborKeyTypeId)
    sve.enc.writeUint(frame.typeId)
  of ckSequence:
    # After elements array: write is_slice, then type_id
    sve.enc.writePrecomputed(CborKeyIsSlice)
    sve.enc.writeBool(frame.isSlice)
    sve.enc.writePrecomputed(CborKeyTypeId)
    sve.enc.writeUint(frame.typeId)
  of ckTuple:
    # After elements array: write type_id
    sve.enc.writePrecomputed(CborKeyTypeId)
    sve.enc.writeUint(frame.typeId)
  of ckVariant:
    # After contents value: write type_id
    sve.enc.writePrecomputed(CborKeyTypeId)
    sve.enc.writeUint(frame.typeId)
  of ckReference:
    # After dereferenced value: write address, mutable, type_id
    sve.enc.writePrecomputed(CborKeyAddress)
    sve.enc.writeUint(frame.address)
    sve.enc.writePrecomputed(CborKeyMutable)
    sve.enc.writeBool(frame.mutable)
    sve.enc.writePrecomputed(CborKeyTypeId)
    sve.enc.writeUint(frame.typeId)
  ok()
