## `cborTopLevelTypeId` reads a CBOR `ValueRecord`'s top-level `type_id`
## without decoding the value. `VariableValue.typeId` answers with it, so it
## must answer what a full decode does:
## `topLevelTypeId(decodeCborValueRecord(bytes))`.
##
## The corpus is every `ValueRecord` kind, alone and nested inside each
## container kind, written by the production encoder; on every one the two
## must agree. They may differ only on a record the full decoder refuses: the
## full path answers 0 there, and the skimmer answers whatever top-level
## `type_id` it can still reach.
##
## No mocks: both sides are the shipped encoder and decoders.

import std/unittest
import results
import codetracer_trace_types
import codetracer_trace_writer/cbor
import codetracer_trace_writer/value_stream

proc tid(n: uint64): TypeId = TypeId(n)

proc leaves(): seq[ValueRecord] =
  @[
    ValueRecord(kind: vrkInt, intVal: -7, intTypeId: tid(3)),
    ValueRecord(kind: vrkInt, intVal: 1 shl 40, intTypeId: tid(70_000)),
    ValueRecord(kind: vrkFloat, floatVal: 2.5, floatTypeId: tid(4)),
    ValueRecord(kind: vrkBool, boolVal: true, boolTypeId: tid(5)),
    ValueRecord(kind: vrkString, text: "type_id", strTypeId: tid(6)),
    ValueRecord(kind: vrkRaw, rawStr: "raw", rawTypeId: tid(7)),
    ValueRecord(kind: vrkError, errorMsg: "boom", errorTypeId: tid(8)),
    ValueRecord(kind: vrkNone, noneTypeId: tid(9)),
    ValueRecord(kind: vrkCell, cellPlace: Place(12)),
    ValueRecord(kind: vrkBigInt, bigIntBytes: @[1'u8, 2, 3], negative: true,
      bigIntTypeId: tid(10)),
    ValueRecord(kind: vrkChar, charVal: 'x', charTypeId: tid(11)),
    ValueRecord(kind: vrkValueRef, refId: 42),
    ValueRecord(kind: vrkEnum, enumName: "Red", enumOrdinal: 2,
      enumTypeId: tid(12)),
  ]

proc corpus(): seq[ValueRecord] =
  let ls = leaves()
  result = ls
  for i, leaf in ls:
    let inner = @[leaf, ls[(i + 1) mod ls.len]]
    result.add ValueRecord(kind: vrkSequence, seqElements: inner,
      isSlice: i mod 2 == 0, seqTypeId: tid(100 + uint64(i)))
    result.add ValueRecord(kind: vrkTuple, tupleElements: inner,
      tupleTypeId: tid(200 + uint64(i)))
    result.add ValueRecord(kind: vrkStruct, fieldValues: inner,
      fieldNames: @["a", "type_id"], structTypeId: tid(300 + uint64(i)))
    result.add ValueRecord(kind: vrkStruct, fieldValues: inner,
      structTypeId: tid(350 + uint64(i)))
    result.add ValueRecord(kind: vrkVariant, discriminator: "Some",
      contents: @[leaf], variantTypeId: tid(400 + uint64(i)))
    result.add ValueRecord(kind: vrkReference, dereferenced: @[leaf],
      address: 0xdead, mutable: true, refTypeId: tid(500 + uint64(i)))
    result.add ValueRecord(kind: vrkSet, setMembers: inner,
      setTypeId: tid(600 + uint64(i)))

proc encode(v: ValueRecord): seq[byte] =
  var enc = CborEncoder.init(64)
  enc.encodeCborValueRecord(v)
  enc.getBytes()

proc fullDecode(b: openArray[byte]): (bool, uint64) =
  var dec = CborDecoder.init(b)
  let r = dec.decodeCborValueRecord()
  if r.isErr: (false, 0'u64) else: (true, topLevelTypeId(r.get()))

suite "a value's top-level type id, read without decoding it":

  test "the corpus covers every kind":
    var kinds: set[ValueRecordKind]
    for v in corpus(): kinds.incl v.kind
    check kinds == {low(ValueRecordKind) .. high(ValueRecordKind)}

  test "every kind, alone and nested, agrees with the full decode":
    for v in corpus():
      let b = encode(v)
      let (ok, want) = fullDecode(b)
      check ok
      check cborTopLevelTypeId(b) == want
      check want == topLevelTypeId(v)

  test "a kind with no type id, and bytes that are not a map, answer 0":
    check cborTopLevelTypeId(encode(ValueRecord(kind: vrkCell,
      cellPlace: Place(1)))) == 0
    check cborTopLevelTypeId(encode(ValueRecord(kind: vrkValueRef,
      refId: 9))) == 0
    check cborTopLevelTypeId([]) == 0
    check cborTopLevelTypeId([0x18'u8]) == 0      # a truncated integer head
    check cborTopLevelTypeId([0xBF'u8, 0xFF]) == 0 # an indefinite-length map

  test "a value's type id is its CBOR's, whoever built the value":
    # A `VariableValue` is the `(name id, CBOR)` pair; nothing else carries
    # a type id that could disagree with the one in its bytes.
    check not compiles(VariableValue(varnameId: 1, typeId: 2))
    for v in corpus():
      check VariableValue(varnameId: 1, data: encode(v)).typeId ==
        topLevelTypeId(v)
    check VariableValue(varnameId: 1).typeId == 0

