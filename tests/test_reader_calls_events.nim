{.push raises: [].}

## Tests for NewTraceReader call and IO event access (M20).
##
## test_reader_call_access: Write 100 calls, read back via reader, verify.
## test_reader_event_page: Write 200 IO events, read a page of 50, verify.
## bench_call_tree_viewport: Write 1000 calls, load 30-call viewport.
## bench_event_page_load: Write 500 events, load 50-event page.

import std/times
import results
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_trace_types
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/interning_table
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/exec_stream
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/call_stream
import codetracer_trace_writer/io_event_stream
import codetracer_trace_writer/new_trace_reader

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc toBytes(s: string): seq[byte] {.raises: [].} =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

proc intToStr(n: int): string {.raises: [].} =
  if n == 0: return "0"
  var v = n
  var digits: seq[char]
  var neg = false
  if v < 0:
    neg = true
    v = -v
  while v > 0:
    digits.add(char(ord('0') + v mod 10))
    v = v div 10
  if neg: digits.add('-')
  result = newString(digits.len)
  for i in 0 ..< digits.len:
    result[i] = digits[digits.len - 1 - i]

## Write a trace with `numCalls` call records and `numEvents` IO events.
proc writeCallsAndEventsTrace(numCalls: int, numEvents: int): seq[byte] {.raises: [].} =
  var ctfs = createCtfs()

  let metaFileRes = ctfs.addFile("meta.dat")
  doAssert metaFileRes.isOk
  var metaFile = metaFileRes.get()
  let meta = TraceMetadata(program: "call_event_test", args: @[], workdir: "/tmp")
  let metaWr = ctfs.writeMetaDat(metaFile, meta, @["/src/main.py"])
  doAssert metaWr.isOk

  let tabRes = initTraceInterningTables(ctfs)
  doAssert tabRes.isOk
  var tab = tabRes.get()
  discard ctfs.ensurePathId(tab, "/src/main.py")
  for i in 0 ..< 10:
    discard ctfs.ensureFunctionId(tab, "func_" & intToStr(i))
  discard ctfs.ensureTypeId(tab, "int")
  discard ctfs.ensureVarnameId(tab, "x")

  # We need at least 1 step for a valid trace
  let execRes = initExecStreamWriter(ctfs, chunkSize = 64)
  doAssert execRes.isOk
  var execW = execRes.get()

  let valRes = initValueStreamWriter(ctfs)
  doAssert valRes.isOk
  var valW = valRes.get()

  # Write a single step
  let wr0 = ctfs.writeEvent(execW, StepEvent(kind: sekAbsoluteStep, globalLineIndex: 0))
  doAssert wr0.isOk
  let v0 = ctfs.writeStepValues(valW, @[])
  doAssert v0.isOk

  # Write call records
  let callRes = initCallStreamWriter(ctfs)
  doAssert callRes.isOk
  var callW = callRes.get()

  for i in 0 ..< numCalls:
    let funcId = uint64(i mod 10)
    let parentKey = if i == 0: -1'i64 else: int64(i div 2)
    let depth = uint32(if i == 0: 0 else: 1)
    var children: seq[uint64]
    let childA = i * 2 + 1
    let childB = i * 2 + 2
    if childA < numCalls:
      children.add(uint64(childA))
    if childB < numCalls:
      children.add(uint64(childB))

    let cr = ctfs.writeCall(callW, call_stream.CallRecord(
      functionId: funcId,
      parentCallKey: parentKey,
      entryStep: uint64(i),
      exitStep: uint64(i + 1),
      depth: depth,
      args: @[intToStr(i).toBytes],
      returnValue: intToStr(i * 10).toBytes,
      exception: @[],
      children: children))
    doAssert cr.isOk, "writeCall " & intToStr(i) & " failed: " & cr.error

  # Write IO events
  let ioRes = initIOEventStreamWriter(ctfs)
  doAssert ioRes.isOk
  var ioW = ioRes.get()

  for i in 0 ..< numEvents:
    let kind = if i mod 2 == 0: ioStdout else: ioStderr
    let ioWr = ctfs.writeEvent(ioW, IOEvent(
      kind: kind,
      stepId: uint64(i * 3),
      data: ("output_" & intToStr(i)).toBytes))
    doAssert ioWr.isOk, "writeEvent IO " & intToStr(i) & " failed: " & ioWr.error

  let flushRes = ctfs.flush(execW)
  doAssert flushRes.isOk

  result = ctfs.toBytes()
  ctfs.closeCtfs()

# ---------------------------------------------------------------------------
# test_reader_call_access
# ---------------------------------------------------------------------------

proc test_reader_call_access() {.raises: [].} =
  let numCalls = 100
  let data = writeCallsAndEventsTrace(numCalls, 0)

  let readerRes = openNewTraceFromBytes(data)
  doAssert readerRes.isOk, "open failed: " & readerRes.error
  var reader = readerRes.get()

  # Verify callCount
  let cc = reader.callCount()
  doAssert cc.isOk, "callCount failed: " & cc.error
  doAssert cc.get() == uint64(numCalls), "callCount mismatch: " & $cc.get()

  # Read each call and verify
  for i in 0 ..< numCalls:
    let res = reader.call(uint64(i))
    doAssert res.isOk, "call " & intToStr(i) & " failed: " & res.error
    let c = res.get()

    let expectedFuncId = uint64(i mod 10)
    doAssert c.functionId == expectedFuncId,
      "call " & intToStr(i) & " functionId: " & $c.functionId & " != " & $expectedFuncId

    let expectedParent = if i == 0: -1'i64 else: int64(i div 2)
    doAssert c.parentCallKey == expectedParent,
      "call " & intToStr(i) & " parentCallKey: " & $c.parentCallKey & " != " & $expectedParent

    doAssert c.entryStep == uint64(i),
      "call " & intToStr(i) & " entryStep mismatch"
    doAssert c.exitStep == uint64(i + 1),
      "call " & intToStr(i) & " exitStep mismatch"

    doAssert c.args.len == 1, "call " & intToStr(i) & " args count"
    doAssert c.args[0] == intToStr(i).toBytes, "call " & intToStr(i) & " arg data"
    doAssert c.returnValue == intToStr(i * 10).toBytes,
      "call " & intToStr(i) & " returnValue"

    # Verify children
    var expectedChildren: seq[uint64]
    let childA = i * 2 + 1
    let childB = i * 2 + 2
    if childA < numCalls:
      expectedChildren.add(uint64(childA))
    if childB < numCalls:
      expectedChildren.add(uint64(childB))
    doAssert c.children == expectedChildren,
      "call " & intToStr(i) & " children mismatch"

  # Test callRange iterator
  var iterCount = 0
  for c in reader.callRange(10, 20):
    let idx = 10 + iterCount
    doAssert c.entryStep == uint64(idx),
      "callRange iter entryStep mismatch at " & intToStr(idx)
    iterCount += 1
  doAssert iterCount == 20, "callRange iterator count: " & intToStr(iterCount) & " != 20"

  # Test callRange openArray
  var buf: array[15, call_stream.CallRecord]
  let n = reader.callRange(50, 15, buf)
  doAssert n == 15, "callRange openArray count: " & intToStr(n) & " != 15"
  for j in 0 ..< 15:
    doAssert buf[j].entryStep == uint64(50 + j),
      "callRange openArray entryStep mismatch at " & intToStr(j)

  echo "PASS: test_reader_call_access"

# ---------------------------------------------------------------------------
# test_reader_event_page
# ---------------------------------------------------------------------------

proc test_reader_event_page() {.raises: [].} =
  let numEvents = 200
  let data = writeCallsAndEventsTrace(0, numEvents)

  let readerRes = openNewTraceFromBytes(data)
  doAssert readerRes.isOk, "open failed: " & readerRes.error
  var reader = readerRes.get()

  # Verify eventCount
  let ec = reader.ioEventCount()
  doAssert ec.isOk, "ioEventCount failed: " & ec.error
  doAssert ec.get() == uint64(numEvents), "ioEventCount mismatch: " & $ec.get()

  # Read events 50-99 (a page of 50)
  var page: array[50, IOEvent]
  let n = reader.events(50, 50, page)
  doAssert n == 50, "events openArray count: " & intToStr(n) & " != 50"

  for j in 0 ..< 50:
    let idx = 50 + j
    let expectedStepId = uint64(idx * 3)
    doAssert page[j].stepId == expectedStepId,
      "event " & intToStr(idx) & " stepId: " & $page[j].stepId & " != " & $expectedStepId

    let expectedKind = if idx mod 2 == 0: ioStdout else: ioStderr
    doAssert page[j].kind == expectedKind,
      "event " & intToStr(idx) & " kind mismatch"

    let expectedData = ("output_" & intToStr(idx)).toBytes
    doAssert page[j].data == expectedData,
      "event " & intToStr(idx) & " data mismatch"

  # Test events iterator
  var iterCount = 0
  for ev in reader.events(100, 30):
    let idx = 100 + iterCount
    doAssert ev.stepId == uint64(idx * 3),
      "events iter stepId mismatch at " & intToStr(idx)
    iterCount += 1
  doAssert iterCount == 30, "events iterator count: " & intToStr(iterCount) & " != 30"

  # Single event access
  let ev0 = reader.ioEvent(0)
  doAssert ev0.isOk, "ioEvent 0 failed: " & ev0.error
  doAssert ev0.get().kind == ioStdout, "event 0 kind"
  doAssert ev0.get().stepId == 0, "event 0 stepId"
  doAssert ev0.get().data == "output_0".toBytes, "event 0 data"

  echo "PASS: test_reader_event_page"

# ---------------------------------------------------------------------------
# bench_call_tree_viewport
# ---------------------------------------------------------------------------

proc bench_call_tree_viewport() {.raises: [].} =
  let numCalls = 1000
  let data = writeCallsAndEventsTrace(numCalls, 0)

  let readerRes = openNewTraceFromBytes(data)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  let iterations = 100
  let viewportSize = 30'u64

  let start = cpuTime()
  for iter in 0 ..< iterations:
    let offset = uint64(iter * 7 mod (numCalls - int(viewportSize)))
    var buf: array[30, call_stream.CallRecord]
    let n = reader.callRange(offset, viewportSize, buf)
    doAssert n == int(viewportSize), "viewport load count mismatch"
  let elapsed = cpuTime() - start

  let avgUs = (elapsed / float(iterations)) * 1_000_000.0

  echo "{\"benchmark\": \"call_tree_viewport\"" &
    ", \"num_calls\": " & $numCalls &
    ", \"viewport_size\": " & $viewportSize &
    ", \"iterations\": " & $iterations &
    ", \"avg_us\": " & $avgUs & "}"

  doAssert avgUs < 500.0, "call viewport too slow: " & $avgUs & "us > 500us"
  echo "PASS: bench_call_tree_viewport"

# ---------------------------------------------------------------------------
# bench_event_page_load
# ---------------------------------------------------------------------------

proc bench_event_page_load() {.raises: [].} =
  let numEvents = 500
  let data = writeCallsAndEventsTrace(0, numEvents)

  let readerRes = openNewTraceFromBytes(data)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  let iterations = 100
  let pageSize = 50'u64

  let start = cpuTime()
  for iter in 0 ..< iterations:
    let offset = uint64(iter * 3 mod (numEvents - int(pageSize)))
    var buf: array[50, IOEvent]
    let n = reader.events(offset, pageSize, buf)
    doAssert n == int(pageSize), "page load count mismatch"
  let elapsed = cpuTime() - start

  let avgUs = (elapsed / float(iterations)) * 1_000_000.0

  echo "{\"benchmark\": \"event_page_load\"" &
    ", \"num_events\": " & $numEvents &
    ", \"page_size\": " & $pageSize &
    ", \"iterations\": " & $iterations &
    ", \"avg_us\": " & $avgUs & "}"

  doAssert avgUs < 1000.0, "event page too slow: " & $avgUs & "us > 1000us"
  echo "PASS: bench_event_page_load"

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

test_reader_call_access()
test_reader_event_page()
bench_call_tree_viewport()
bench_event_page_load()
echo "ALL PASS: test_reader_calls_events"
