## Tests for the v4 (multi-stream) read path through TraceReader.readEvents.
##
## CTFS-M-Fix-Reader: codetracer-nim's vm_trace.nim was ported to the v4
## MultiStreamTraceWriter, but the legacy codetracer_trace_reader.readEvents
## API still only supported the v3 single-stream events.log layout.  This
## test exercises the new v4 dispatch path in readEvents.

import std/[os, assertions]
import results
import codetracer_trace_types
import codetracer_trace_reader
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/io_event_stream
import codetracer_trace_writer/cbor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc encodeValue(v: ValueRecord): seq[byte] =
  var enc = CborEncoder.init()
  enc.encodeCborValueRecord(v)
  enc.getBytes()

proc tmpPath(name: string): string = getTempDir() / name

proc cleanupFile(path: string) =
  try:
    removeFile(path)
  except OSError:
    discard

# ---------------------------------------------------------------------------
# Test 1: a tiny v4 trace round-trips through readEvents
# ---------------------------------------------------------------------------

proc test_v4_basic_round_trip() =
  let path = tmpPath("test_reader_v4_basic.ct")
  cleanupFile(path)

  # ---- Build a v4 trace ----
  var wRes = initMultiStreamWriter(path, "test_prog")
  doAssert wRes.isOk, "initMultiStreamWriter failed: " & wRes.error
  var w = wRes.get()
  w.metadata.args = @["--flag"]
  w.metadata.workdir = "/tmp"

  let p0 = w.registerPath("/src/main.nim")
  doAssert p0.isOk
  let p1 = w.registerPath("/src/helper.nim")
  doAssert p1.isOk

  let f0 = w.registerFunction("foo")
  doAssert f0.isOk

  let vn0 = w.registerVarname("x")
  doAssert vn0.isOk
  let vn1 = w.registerVarname("y")
  doAssert vn1.isOk

  let t0 = w.registerType("int")
  doAssert t0.isOk

  # Step 0 (main.nim line 10) — one int value x=42
  let intVal = ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(t0.get()))
  doAssert w.registerStep(0, 10, @[
    VariableValue(varnameId: vn0.get(), typeId: t0.get(), data: encodeValue(intVal))
  ]).isOk

  # Call foo()
  doAssert w.registerCall(f0.get(), @[]).isOk

  # Step 1 (main.nim line 11) — one int value y=99
  let intVal2 = ValueRecord(kind: vrkInt, intVal: 99, intTypeId: TypeId(t0.get()))
  doAssert w.registerStep(0, 11, @[
    VariableValue(varnameId: vn1.get(), typeId: t0.get(), data: encodeValue(intVal2))
  ]).isOk

  # Return from foo()
  doAssert w.registerReturn(@[]).isOk

  # Step 2 (helper.nim line 5) — no values
  doAssert w.registerStep(1, 5, @[]).isOk

  # An IO event (stdout)
  doAssert w.registerIOEvent(ioStdout, "hi\n".toOpenArrayByte(0, 2)).isOk

  doAssert w.close().isOk

  # Write the bytes to disk so we can openTrace(path) like real consumers.
  let bytes = w.toBytes()
  w.closeCtfs()
  try:
    let f = open(path, fmWrite)
    discard f.writeBytes(bytes, 0, bytes.len)
    f.close()
  except IOError, OSError:
    doAssert false, "failed to write trace file"

  # ---- Open and read back ----
  var readerRes = openTrace(path)
  doAssert readerRes.isOk, "openTrace failed: " & readerRes.unsafeError
  var reader = readerRes.get()

  doAssert reader.isV4, "expected v4 trace, got v3"
  doAssert reader.metadata.program == "test_prog"
  doAssert reader.metadata.args.len == 1 and reader.metadata.args[0] == "--flag"
  doAssert reader.paths.len == 2
  doAssert reader.paths[0] == "/src/main.nim"
  doAssert reader.paths[1] == "/src/helper.nim"

  let evRes = reader.readEvents()
  doAssert evRes.isOk, "readEvents failed: " & evRes.unsafeError

  # Count events by kind
  var pathCount, funcCount, stepCount, callCount, returnCount, valueCount, ioCount = 0
  for ev in reader.events:
    case ev.kind
    of tlePath: pathCount += 1
    of tleFunction: funcCount += 1
    of tleStep: stepCount += 1
    of tleCall: callCount += 1
    of tleReturn: returnCount += 1
    of tleValue: valueCount += 1
    of tleEvent: ioCount += 1
    else: discard

  doAssert pathCount == 2, "expected 2 Path events, got " & $pathCount
  doAssert funcCount == 1, "expected 1 Function event, got " & $funcCount
  doAssert stepCount == 3, "expected 3 Step events, got " & $stepCount
  doAssert callCount == 1, "expected 1 Call event, got " & $callCount
  doAssert returnCount == 1, "expected 1 Return event, got " & $returnCount
  doAssert valueCount == 2, "expected 2 Value events, got " & $valueCount
  doAssert ioCount == 1, "expected 1 IO Event, got " & $ioCount

  # Verify function name 'foo' appears
  var foundFoo = false
  for ev in reader.events:
    if ev.kind == tleFunction and ev.functionRecord.name == "foo":
      foundFoo = true
  doAssert foundFoo, "expected a Function event named 'foo'"

  # Verify Path events carry both file paths
  var foundMain = false
  var foundHelper = false
  for ev in reader.events:
    if ev.kind == tlePath:
      if ev.path == "/src/main.nim": foundMain = true
      if ev.path == "/src/helper.nim": foundHelper = true
  doAssert foundMain and foundHelper, "missing Path events"

  # Verify Step ordering: first step before first call (matches v3 expectation)
  var firstStepIdx = -1
  var firstCallIdx = -1
  for i, ev in reader.events:
    if ev.kind == tleStep and firstStepIdx < 0:
      firstStepIdx = i
    if ev.kind == tleCall and firstCallIdx < 0:
      firstCallIdx = i
  doAssert firstStepIdx >= 0 and firstCallIdx >= 0
  doAssert firstStepIdx < firstCallIdx,
    "expected Step before Call (Step idx=" & $firstStepIdx &
    " vs Call idx=" & $firstCallIdx & ")"

  # Verify Call followed by Return
  var sawCall = false
  var returnAfterCall = false
  for ev in reader.events:
    if ev.kind == tleCall: sawCall = true
    elif ev.kind == tleReturn and sawCall: returnAfterCall = true
  doAssert returnAfterCall, "Return should appear after Call"

  # Verify decoded int Value events recover 42 and 99
  var foundInt42 = false
  var foundInt99 = false
  for ev in reader.events:
    if ev.kind == tleValue and ev.fullValue.value.kind == vrkInt:
      if ev.fullValue.value.intVal == 42: foundInt42 = true
      if ev.fullValue.value.intVal == 99: foundInt99 = true
  doAssert foundInt42, "expected Value event with int 42"
  doAssert foundInt99, "expected Value event with int 99"

  cleanupFile(path)
  echo "PASS: test_v4_basic_round_trip"


# ---------------------------------------------------------------------------
# Test 2: empty v4 trace
# ---------------------------------------------------------------------------

proc test_v4_empty() =
  let path = tmpPath("test_reader_v4_empty.ct")
  cleanupFile(path)

  var wRes = initMultiStreamWriter(path, "empty_prog")
  doAssert wRes.isOk
  var w = wRes.get()
  doAssert w.close().isOk

  let bytes = w.toBytes()
  w.closeCtfs()
  try:
    let f = open(path, fmWrite)
    discard f.writeBytes(bytes, 0, bytes.len)
    f.close()
  except IOError, OSError:
    doAssert false

  var readerRes = openTrace(path)
  doAssert readerRes.isOk
  var reader = readerRes.get()
  doAssert reader.isV4
  doAssert reader.readEvents().isOk
  doAssert reader.events.len == 0, "expected 0 events, got " & $reader.events.len

  cleanupFile(path)
  echo "PASS: test_v4_empty"


test_v4_basic_round_trip()
test_v4_empty()
echo "ALL PASS: test_reader_v4"
