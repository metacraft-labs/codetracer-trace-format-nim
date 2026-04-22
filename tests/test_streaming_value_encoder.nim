{.push raises: [].}

## Tests for the streaming value encoder.
## Verifies byte-identical output with tree-based encodeCborValueRecord.

import std/[times, strformat]
import codetracer_trace_writer/cbor
import codetracer_trace_writer/streaming_value_encoder
import codetracer_trace_types

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc treeEncode(v: ValueRecord): seq[byte] =
  var enc = CborEncoder.init(256)
  enc.encodeCborValueRecord(v)
  enc.getBytes()

proc check(cond: bool, msg: string) {.raises: [CatchableError].} =
  if not cond:
    raise newException(CatchableError, "FAIL: " & msg)

proc hexDump(data: seq[byte], maxLen: int = 64): string {.raises: [].} =
  var s = ""
  for i in 0 ..< min(data.len, maxLen):
    if i > 0: s.add ' '
    let b = data[i]
    const hex = "0123456789abcdef"
    s.add hex[int(b shr 4)]
    s.add hex[int(b and 0x0F)]
  if data.len > maxLen:
    s.add " ..."
  s

# ---------------------------------------------------------------------------
# test_streaming_encoder_byte_identical
# ---------------------------------------------------------------------------

proc testByteIdentical() {.raises: [CatchableError].} =
  echo "--- test_streaming_encoder_byte_identical ---"

  # Int
  block:
    let treeBytes = treeEncode(ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(7)))
    var sve = StreamingValueEncoder.init()
    discard sve.writeInt(42, typeId = 7)
    let streamBytes = sve.getBytes()
    check treeBytes == streamBytes, "Int mismatch: tree=" & hexDump(treeBytes) & " stream=" & hexDump(streamBytes)
    echo "  Int: OK"

  # Int negative
  block:
    let treeBytes = treeEncode(ValueRecord(kind: vrkInt, intVal: -100, intTypeId: TypeId(3)))
    var sve = StreamingValueEncoder.init()
    discard sve.writeInt(-100, typeId = 3)
    let streamBytes = sve.getBytes()
    check treeBytes == streamBytes, "Int(neg) mismatch"
    echo "  Int(neg): OK"

  # Float
  block:
    let treeBytes = treeEncode(ValueRecord(kind: vrkFloat, floatVal: 3.14, floatTypeId: TypeId(5)))
    var sve = StreamingValueEncoder.init()
    discard sve.writeFloat(3.14, typeId = 5)
    let streamBytes = sve.getBytes()
    check treeBytes == streamBytes, "Float mismatch: tree=" & hexDump(treeBytes) & " stream=" & hexDump(streamBytes)
    echo "  Float: OK"

  # Bool true
  block:
    let treeBytes = treeEncode(ValueRecord(kind: vrkBool, boolVal: true, boolTypeId: TypeId(1)))
    var sve = StreamingValueEncoder.init()
    discard sve.writeBool(true, typeId = 1)
    let streamBytes = sve.getBytes()
    check treeBytes == streamBytes, "Bool(true) mismatch"
    echo "  Bool(true): OK"

  # Bool false
  block:
    let treeBytes = treeEncode(ValueRecord(kind: vrkBool, boolVal: false, boolTypeId: TypeId(2)))
    var sve = StreamingValueEncoder.init()
    discard sve.writeBool(false, typeId = 2)
    let streamBytes = sve.getBytes()
    check treeBytes == streamBytes, "Bool(false) mismatch"
    echo "  Bool(false): OK"

  # String
  block:
    let treeBytes = treeEncode(ValueRecord(kind: vrkString, text: "hello", strTypeId: TypeId(10)))
    var sve = StreamingValueEncoder.init()
    discard sve.writeString("hello", typeId = 10)
    let streamBytes = sve.getBytes()
    check treeBytes == streamBytes, "String mismatch: tree=" & hexDump(treeBytes) & " stream=" & hexDump(streamBytes)
    echo "  String: OK"

  # None
  block:
    let treeBytes = treeEncode(ValueRecord(kind: vrkNone, noneTypeId: TypeId(0)))
    var sve = StreamingValueEncoder.init()
    discard sve.writeNone(typeId = 0)
    let streamBytes = sve.getBytes()
    check treeBytes == streamBytes, "None mismatch"
    echo "  None: OK"

  # Raw
  block:
    let treeBytes = treeEncode(ValueRecord(kind: vrkRaw, rawStr: "<opaque>", rawTypeId: TypeId(99)))
    var sve = StreamingValueEncoder.init()
    discard sve.writeRaw("<opaque>", typeId = 99)
    let streamBytes = sve.getBytes()
    check treeBytes == streamBytes, "Raw mismatch: tree=" & hexDump(treeBytes) & " stream=" & hexDump(streamBytes)
    echo "  Raw: OK"

  # Error
  block:
    let treeBytes = treeEncode(ValueRecord(kind: vrkError, errorMsg: "oops", errorTypeId: TypeId(50)))
    var sve = StreamingValueEncoder.init()
    discard sve.writeError("oops", typeId = 50)
    let streamBytes = sve.getBytes()
    check treeBytes == streamBytes, "Error mismatch"
    echo "  Error: OK"

  # Struct with 2 Int fields
  block:
    let treeVal = ValueRecord(kind: vrkStruct, fieldValues: @[
      ValueRecord(kind: vrkInt, intVal: 1, intTypeId: TypeId(7)),
      ValueRecord(kind: vrkInt, intVal: 2, intTypeId: TypeId(7)),
    ], structTypeId: TypeId(20))
    let treeBytes = treeEncode(treeVal)
    var sve = StreamingValueEncoder.init()
    discard sve.beginStruct(typeId = 20, fieldCount = 2)
    discard sve.writeInt(1, typeId = 7)
    discard sve.writeInt(2, typeId = 7)
    discard sve.endCompound()
    let streamBytes = sve.getBytes()
    check treeBytes == streamBytes, "Struct mismatch: tree=" & hexDump(treeBytes) & " stream=" & hexDump(streamBytes)
    echo "  Struct(2 fields): OK"

  # Sequence with 3 Int elements
  block:
    let treeVal = ValueRecord(kind: vrkSequence, seqElements: @[
      ValueRecord(kind: vrkInt, intVal: 10, intTypeId: TypeId(7)),
      ValueRecord(kind: vrkInt, intVal: 20, intTypeId: TypeId(7)),
      ValueRecord(kind: vrkInt, intVal: 30, intTypeId: TypeId(7)),
    ], isSlice: false, seqTypeId: TypeId(15))
    let treeBytes = treeEncode(treeVal)
    var sve = StreamingValueEncoder.init()
    discard sve.beginSequence(typeId = 15, elementCount = 3, isSlice = false)
    discard sve.writeInt(10, typeId = 7)
    discard sve.writeInt(20, typeId = 7)
    discard sve.writeInt(30, typeId = 7)
    discard sve.endCompound()
    let streamBytes = sve.getBytes()
    check treeBytes == streamBytes, "Sequence mismatch: tree=" & hexDump(treeBytes) & " stream=" & hexDump(streamBytes)
    echo "  Sequence(3 elements): OK"

  # Tuple with 2 elements
  block:
    let treeVal = ValueRecord(kind: vrkTuple, tupleElements: @[
      ValueRecord(kind: vrkInt, intVal: 100, intTypeId: TypeId(7)),
      ValueRecord(kind: vrkString, text: "abc", strTypeId: TypeId(10)),
    ], tupleTypeId: TypeId(25))
    let treeBytes = treeEncode(treeVal)
    var sve = StreamingValueEncoder.init()
    discard sve.beginTuple(typeId = 25, elementCount = 2)
    discard sve.writeInt(100, typeId = 7)
    discard sve.writeString("abc", typeId = 10)
    discard sve.endCompound()
    let streamBytes = sve.getBytes()
    check treeBytes == streamBytes, "Tuple mismatch: tree=" & hexDump(treeBytes) & " stream=" & hexDump(streamBytes)
    echo "  Tuple(2 elements): OK"

  echo "  ALL byte-identical tests passed."

# ---------------------------------------------------------------------------
# test_streaming_encoder_nesting
# ---------------------------------------------------------------------------

proc testNesting() {.raises: [CatchableError].} =
  echo "--- test_streaming_encoder_nesting ---"

  # Struct containing a Sequence containing Structs (3 levels)
  let treeVal = ValueRecord(kind: vrkStruct, fieldValues: @[
    ValueRecord(kind: vrkSequence, seqElements: @[
      ValueRecord(kind: vrkStruct, fieldValues: @[
        ValueRecord(kind: vrkInt, intVal: 1, intTypeId: TypeId(7)),
      ], structTypeId: TypeId(30)),
      ValueRecord(kind: vrkStruct, fieldValues: @[
        ValueRecord(kind: vrkInt, intVal: 2, intTypeId: TypeId(7)),
      ], structTypeId: TypeId(30)),
    ], isSlice: false, seqTypeId: TypeId(40)),
    ValueRecord(kind: vrkBool, boolVal: true, boolTypeId: TypeId(1)),
  ], structTypeId: TypeId(50))

  let treeBytes = treeEncode(treeVal)

  var sve = StreamingValueEncoder.init()
  discard sve.beginStruct(typeId = 50, fieldCount = 2)
  # field 0: sequence of structs
  discard sve.beginSequence(typeId = 40, elementCount = 2, isSlice = false)
  # seq element 0: struct with 1 int field
  discard sve.beginStruct(typeId = 30, fieldCount = 1)
  discard sve.writeInt(1, typeId = 7)
  discard sve.endCompound()  # end inner struct
  # seq element 1: struct with 1 int field
  discard sve.beginStruct(typeId = 30, fieldCount = 1)
  discard sve.writeInt(2, typeId = 7)
  discard sve.endCompound()  # end inner struct
  discard sve.endCompound()  # end sequence
  # field 1: bool
  discard sve.writeBool(true, typeId = 1)
  discard sve.endCompound()  # end outer struct

  let streamBytes = sve.getBytes()
  check treeBytes == streamBytes, "Nested struct/seq/struct mismatch:\n  tree=" & hexDump(treeBytes, 128) & "\n  stream=" & hexDump(streamBytes, 128)

  # Verify roundtrip via decoder
  var dec = CborDecoder.init(streamBytes)
  let decoded = dec.decodeCborValueRecord()
  if decoded.isErr:
    check false, "Decode failed"

  echo "  Nested 3-level: OK (byte-identical + roundtrip decode)"

  # Test nesting depth limit
  block:
    var sve2 = StreamingValueEncoder.init()
    var ok = true
    for i in 0 ..< MaxNestingDepth:
      let r = sve2.beginStruct(typeId = 0, fieldCount = 1)
      if r.isErr:
        ok = false
        break
    check ok, "Should allow " & $MaxNestingDepth & " levels"
    let r = sve2.beginStruct(typeId = 0, fieldCount = 0)
    check r.isErr, "Should reject depth > " & $MaxNestingDepth
    echo "  Nesting depth limit: OK"

  echo "  ALL nesting tests passed."

# ---------------------------------------------------------------------------
# bench_streaming_vs_tree_encoding
# ---------------------------------------------------------------------------

proc benchStreamingVsTree() {.raises: [CatchableError].} =
  echo "--- bench_streaming_vs_tree_encoding ---"
  const N = 100_000

  # Tree-based: encode N Int values
  block:
    let t0 = cpuTime()
    var enc = CborEncoder.init(256)
    for i in 0 ..< N:
      enc.clear()
      enc.encodeCborValueRecord(ValueRecord(kind: vrkInt, intVal: int64(i), intTypeId: TypeId(7)))
    let elapsed = cpuTime() - t0
    let throughput = float64(N) / elapsed
    echo fmt"  Tree  Int x{N}: {elapsed:.4f}s ({throughput:.0f} values/s)"

  # Streaming: encode N Int values
  block:
    let t0 = cpuTime()
    var sve = StreamingValueEncoder.init(256)
    for i in 0 ..< N:
      sve.reset()
      discard sve.writeInt(int64(i), typeId = 7)
    let elapsed = cpuTime() - t0
    let throughput = float64(N) / elapsed
    echo fmt"  Stream Int x{N}: {elapsed:.4f}s ({throughput:.0f} values/s)"

  # Tree-based: encode N Struct values (2 fields each)
  block:
    let t0 = cpuTime()
    var enc = CborEncoder.init(256)
    for i in 0 ..< N:
      enc.clear()
      enc.encodeCborValueRecord(ValueRecord(kind: vrkStruct, fieldValues: @[
        ValueRecord(kind: vrkInt, intVal: int64(i), intTypeId: TypeId(7)),
        ValueRecord(kind: vrkInt, intVal: int64(i + 1), intTypeId: TypeId(7)),
      ], structTypeId: TypeId(20)))
    let elapsed = cpuTime() - t0
    let throughput = float64(N) / elapsed
    echo fmt"  Tree  Struct x{N}: {elapsed:.4f}s ({throughput:.0f} values/s)"

  # Streaming: encode N Struct values (2 fields each)
  block:
    let t0 = cpuTime()
    var sve = StreamingValueEncoder.init(256)
    for i in 0 ..< N:
      sve.reset()
      discard sve.beginStruct(typeId = 20, fieldCount = 2)
      discard sve.writeInt(int64(i), typeId = 7)
      discard sve.writeInt(int64(i + 1), typeId = 7)
      discard sve.endCompound()
    let elapsed = cpuTime() - t0
    let throughput = float64(N) / elapsed
    echo fmt"  Stream Struct x{N}: {elapsed:.4f}s ({throughput:.0f} values/s)"

  echo "  Benchmark complete."

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

proc main() {.raises: [CatchableError].} =
  testByteIdentical()
  testNesting()
  benchStreamingVsTree()
  echo "All streaming value encoder tests passed."

try:
  main()
except CatchableError as e:
  echo "TEST FAILED: " & e.msg
  quit(1)
