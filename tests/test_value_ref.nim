## Tests for ValueRef (vrkValueRef) — cyclic value encoding via CBOR tag 256.

import codetracer_trace_writer/cbor
import codetracer_trace_writer/streaming_value_encoder
import codetracer_trace_types

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc check(cond: bool, msg: string) {.raises: [Exception].} =
  if not cond:
    raise newException(CatchableError, "FAIL: " & msg)

proc roundtrip(v: ValueRecord): ValueRecord {.raises: [Exception].} =
  var enc = CborEncoder.init(128)
  enc.encodeCborValueRecord(v)
  let bytes = enc.getBytes()
  var dec = CborDecoder.init(bytes)
  let res = dec.decodeCborValueRecord()
  if res.isOk:
    return res.get
  raise newException(CatchableError, "decode failed")

# ---------------------------------------------------------------------------
# test_value_ref_encode_decode
# ---------------------------------------------------------------------------

proc testValueRefEncodeDecode() {.raises: [Exception].} =
  echo "--- test_value_ref_encode_decode ---"

  let vref = ValueRecord(kind: vrkValueRef, refId: 42'u32)
  let decoded = roundtrip(vref)

  check decoded.kind == vrkValueRef, "expected vrkValueRef, got " & $decoded.kind
  check decoded.refId == 42'u32, "expected refId=42, got " & $decoded.refId

  echo "  OK"

# ---------------------------------------------------------------------------
# test_value_ref_mutual_reference
# ---------------------------------------------------------------------------

proc testValueRefMutualReference() {.raises: [Exception].} =
  echo "--- test_value_ref_mutual_reference ---"

  # Struct A: fields = [Int(1), ValueRef(1)]  — A references B (id=1)
  let structA = ValueRecord(kind: vrkStruct, fieldValues: @[
    ValueRecord(kind: vrkInt, intVal: 1, intTypeId: TypeId(7)),
    ValueRecord(kind: vrkValueRef, refId: 1'u32),
  ], structTypeId: TypeId(100))

  # Struct B: fields = [Int(2), ValueRef(0)]  — B references A (id=0)
  let structB = ValueRecord(kind: vrkStruct, fieldValues: @[
    ValueRecord(kind: vrkInt, intVal: 2, intTypeId: TypeId(7)),
    ValueRecord(kind: vrkValueRef, refId: 0'u32),
  ], structTypeId: TypeId(101))

  let decodedA = roundtrip(structA)
  let decodedB = roundtrip(structB)

  check decodedA.kind == vrkStruct, "A: expected vrkStruct"
  check decodedA.fieldValues.len == 2, "A: expected 2 fields"
  check decodedA.fieldValues[0].kind == vrkInt, "A[0]: expected vrkInt"
  check decodedA.fieldValues[0].intVal == 1, "A[0]: expected intVal=1"
  check decodedA.fieldValues[1].kind == vrkValueRef, "A[1]: expected vrkValueRef"
  check decodedA.fieldValues[1].refId == 1'u32, "A[1]: expected refId=1"

  check decodedB.kind == vrkStruct, "B: expected vrkStruct"
  check decodedB.fieldValues.len == 2, "B: expected 2 fields"
  check decodedB.fieldValues[0].kind == vrkInt, "B[0]: expected vrkInt"
  check decodedB.fieldValues[0].intVal == 2, "B[0]: expected intVal=2"
  check decodedB.fieldValues[1].kind == vrkValueRef, "B[1]: expected vrkValueRef"
  check decodedB.fieldValues[1].refId == 0'u32, "B[1]: expected refId=0"

  echo "  OK"

# ---------------------------------------------------------------------------
# test_value_ref_backward_compat
# ---------------------------------------------------------------------------

proc testValueRefBackwardCompat() {.raises: [Exception].} =
  echo "--- test_value_ref_backward_compat ---"

  # Encode values WITHOUT any ValueRef (old format) and verify they still decode
  let values = @[
    ValueRecord(kind: vrkInt, intVal: 99, intTypeId: TypeId(7)),
    ValueRecord(kind: vrkString, text: "hello", strTypeId: TypeId(9)),
    ValueRecord(kind: vrkStruct, fieldValues: @[
      ValueRecord(kind: vrkBool, boolVal: true, boolTypeId: TypeId(1)),
    ], structTypeId: TypeId(20)),
    ValueRecord(kind: vrkNone, noneTypeId: TypeId(0)),
  ]

  for i in 0 ..< values.len:
    let decoded = roundtrip(values[i])
    check decoded == values[i], "backward compat failed for value " & $i

  echo "  OK"

# ---------------------------------------------------------------------------
# test_value_ref_streaming_encoder
# ---------------------------------------------------------------------------

proc testValueRefStreamingEncoder() {.raises: [Exception].} =
  echo "--- test_value_ref_streaming_encoder ---"

  # Tree-based encode
  let vref = ValueRecord(kind: vrkValueRef, refId: 7'u32)
  var enc = CborEncoder.init(64)
  enc.encodeCborValueRecord(vref)
  let treeBytes = enc.getBytes()

  # Streaming encode
  var sve = StreamingValueEncoder.init(64)
  discard sve.writeRef(7'u32)
  let streamBytes = sve.getBytes()

  check treeBytes == streamBytes,
    "streaming ValueRef bytes differ from tree-based"

  # Decode the streaming output
  var dec = CborDecoder.init(streamBytes)
  let res = dec.decodeCborValueRecord()
  check res.isOk, "decode streaming ValueRef failed"
  let decoded = res.get
  check decoded.kind == vrkValueRef, "expected vrkValueRef"
  check decoded.refId == 7'u32, "expected refId=7"

  echo "  OK"

# ---------------------------------------------------------------------------
# test_value_ref_equality
# ---------------------------------------------------------------------------

proc testValueRefEquality() {.raises: [Exception].} =
  echo "--- test_value_ref_equality ---"

  let a = ValueRecord(kind: vrkValueRef, refId: 5'u32)
  let b = ValueRecord(kind: vrkValueRef, refId: 5'u32)
  let c = ValueRecord(kind: vrkValueRef, refId: 6'u32)

  check a == b, "equal ValueRefs should be =="
  check not (a == c), "different refIds should not be =="

  echo "  OK"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

proc main() {.raises: [Exception].} =
  testValueRefEncodeDecode()
  testValueRefMutualReference()
  testValueRefBackwardCompat()
  testValueRefStreamingEncoder()
  testValueRefEquality()
  echo "All ValueRef tests passed."

try:
  main()
except CatchableError as e:
  echo "TEST FAILED: " & e.msg
  quit(1)
