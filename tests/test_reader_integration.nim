{.push raises: [].}

## Integration tests for NewTraceReader (M21).
##
## test_reader_full_integration: Write a complete trace (100 steps, 10 calls,
##   5 events), open with NewTraceReader, exercise every method.
## test_reader_cache_eviction: Write trace with many chunks, verify sequential
##   and random access patterns both return correct data.

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

## Write a complete trace with 100 steps, 10 calls, 5 IO events.
proc writeFullTrace(): seq[byte] {.raises: [].} =
  var ctfs = createCtfs()

  # Meta
  let metaFileRes = ctfs.addFile("meta.dat")
  doAssert metaFileRes.isOk
  var metaFile = metaFileRes.get()
  let meta = TraceMetadata(
    program: "integration_test",
    args: @["--verbose", "--count=100"],
    workdir: "/home/test")
  let metaWr = ctfs.writeMetaDat(metaFile, meta,
    @["/src/main.py", "/src/utils.py", "/src/math.py"],
    recorderId = "m21-integration")
  doAssert metaWr.isOk

  # Interning tables
  let tabRes = initTraceInterningTables(ctfs)
  doAssert tabRes.isOk
  var tab = tabRes.get()

  discard ctfs.ensurePathId(tab, "/src/main.py")    # 0
  discard ctfs.ensurePathId(tab, "/src/utils.py")    # 1
  discard ctfs.ensurePathId(tab, "/src/math.py")     # 2

  discard ctfs.ensureFunctionId(tab, "main")         # 0
  discard ctfs.ensureFunctionId(tab, "process")      # 1
  discard ctfs.ensureFunctionId(tab, "compute")      # 2
  discard ctfs.ensureFunctionId(tab, "log_output")   # 3

  discard ctfs.ensureTypeId(tab, "int")              # 0
  discard ctfs.ensureTypeId(tab, "str")              # 1
  discard ctfs.ensureTypeId(tab, "float")            # 2

  discard ctfs.ensureVarnameId(tab, "i")             # 0
  discard ctfs.ensureVarnameId(tab, "x")             # 1
  discard ctfs.ensureVarnameId(tab, "result")        # 2
  discard ctfs.ensureVarnameId(tab, "msg")           # 3

  # Streams
  let execRes = initExecStreamWriter(ctfs, chunkSize = 32)
  doAssert execRes.isOk
  var execW = execRes.get()

  let valRes = initValueStreamWriter(ctfs)
  doAssert valRes.isOk
  var valW = valRes.get()

  let callRes = initCallStreamWriter(ctfs)
  doAssert callRes.isOk
  var callW = callRes.get()

  let ioRes = initIOEventStreamWriter(ctfs)
  doAssert ioRes.isOk
  var ioW = ioRes.get()

  # Write 100 steps with values
  for i in 0 ..< 100:
    var ev: StepEvent
    if i == 0:
      ev = StepEvent(kind: sekAbsoluteStep, globalLineIndex: 0)
    else:
      ev = StepEvent(kind: sekDeltaStep, lineDelta: 1)
    let wr = ctfs.writeEvent(execW, ev)
    doAssert wr.isOk

    # Each step has 1-2 values
    var vals: seq[VariableValue]
    vals.add(VariableValue(varnameId: 0, typeId: 0, data: intToStr(i).toBytes))
    if i mod 5 == 0:
      vals.add(VariableValue(varnameId: 1, typeId: 1,
        data: ("step_" & intToStr(i)).toBytes))
    let vr = ctfs.writeStepValues(valW, vals)
    doAssert vr.isOk

  # Write 10 calls (main -> process*5, each process -> compute)
  # Call 0: main (root)
  let cw0 = ctfs.writeCall(callW, call_stream.CallRecord(
    functionId: 0, parentCallKey: -1, entryStep: 0, exitStep: 99,
    depth: 0, args: @[],
    returnValue: @[VoidReturnMarker],
    exception: @[],
    children: @[1'u64, 2'u64, 3'u64, 4'u64, 5'u64]))
  doAssert cw0.isOk

  # Calls 1-5: process
  for i in 1 .. 5:
    var children: seq[uint64]
    children.add(uint64(5 + i))  # each process has one compute child
    let cw = ctfs.writeCall(callW, call_stream.CallRecord(
      functionId: 1, parentCallKey: 0,
      entryStep: uint64(i * 10), exitStep: uint64(i * 10 + 9),
      depth: 1,
      args: @[intToStr(i).toBytes],
      returnValue: intToStr(i * 100).toBytes,
      exception: @[],
      children: children))
    doAssert cw.isOk

  # Calls 6-10: compute (leaves)
  for i in 6 .. 10:
    let parentIdx = i - 5
    let cw = ctfs.writeCall(callW, call_stream.CallRecord(
      functionId: 2, parentCallKey: int64(parentIdx),
      entryStep: uint64(parentIdx * 10 + 2),
      exitStep: uint64(parentIdx * 10 + 5),
      depth: 2,
      args: @[intToStr(parentIdx).toBytes, "10".toBytes],
      returnValue: intToStr(parentIdx * 10).toBytes,
      exception: @[],
      children: @[]))
    doAssert cw.isOk

  # Write 5 IO events
  for i in 0 ..< 5:
    let kind = if i < 3: ioStdout else: ioStderr
    let ioWr = ctfs.writeEvent(ioW, IOEvent(
      kind: kind,
      stepId: uint64(i * 20),
      data: ("line_" & intToStr(i) & "\n").toBytes))
    doAssert ioWr.isOk

  let flushRes = ctfs.flush(execW)
  doAssert flushRes.isOk

  result = ctfs.toBytes()
  ctfs.closeCtfs()

# ---------------------------------------------------------------------------
# test_reader_full_integration
# ---------------------------------------------------------------------------

proc test_reader_full_integration() {.raises: [].} =
  let data = writeFullTrace()
  let readerRes = openNewTraceFromBytes(data)
  doAssert readerRes.isOk, "open failed: " & readerRes.error
  var reader = readerRes.get()

  # --- Metadata ---
  doAssert reader.meta.program == "integration_test", "program mismatch"
  doAssert reader.meta.args.len == 2, "args len"
  doAssert reader.meta.args[0] == "--verbose", "arg 0"
  doAssert reader.meta.args[1] == "--count=100", "arg 1"
  doAssert reader.meta.workdir == "/home/test", "workdir"
  doAssert reader.meta.recorderId == "m21-integration", "recorderId"

  # --- Interning: paths ---
  doAssert reader.pathCount() == 3, "pathCount"
  let p0 = reader.path(0)
  doAssert p0.isOk and p0.get() == "/src/main.py", "path 0"
  let p1 = reader.path(1)
  doAssert p1.isOk and p1.get() == "/src/utils.py", "path 1"
  let p2 = reader.path(2)
  doAssert p2.isOk and p2.get() == "/src/math.py", "path 2"

  # --- Interning: functions ---
  doAssert reader.functionCount() == 4, "functionCount"
  let f0 = reader.function(0)
  doAssert f0.isOk and f0.get() == "main", "func 0"
  let f1 = reader.function(1)
  doAssert f1.isOk and f1.get() == "process", "func 1"
  let f2 = reader.function(2)
  doAssert f2.isOk and f2.get() == "compute", "func 2"
  let f3 = reader.function(3)
  doAssert f3.isOk and f3.get() == "log_output", "func 3"

  # --- Interning: types ---
  doAssert reader.typeCount() == 3, "typeCount"
  let t0 = reader.typeName(0)
  doAssert t0.isOk and t0.get() == "int", "type 0"
  let t1 = reader.typeName(1)
  doAssert t1.isOk and t1.get() == "str", "type 1"
  let t2 = reader.typeName(2)
  doAssert t2.isOk and t2.get() == "float", "type 2"

  # --- Interning: varnames ---
  doAssert reader.varnameCount() == 4, "varnameCount"
  let vn0 = reader.varname(0)
  doAssert vn0.isOk and vn0.get() == "i", "varname 0"
  let vn1 = reader.varname(1)
  doAssert vn1.isOk and vn1.get() == "x", "varname 1"
  let vn2 = reader.varname(2)
  doAssert vn2.isOk and vn2.get() == "result", "varname 2"
  let vn3 = reader.varname(3)
  doAssert vn3.isOk and vn3.get() == "msg", "varname 3"

  # --- Steps ---
  let sc = reader.stepCount()
  doAssert sc.isOk, "stepCount failed: " & sc.error
  doAssert sc.get() == 100, "stepCount: " & $sc.get()

  let s0 = reader.step(0)
  doAssert s0.isOk, "step 0 failed"
  doAssert s0.get().kind == sekAbsoluteStep, "step 0 kind"
  doAssert s0.get().globalLineIndex == 0, "step 0 gli"

  let s50 = reader.step(50)
  doAssert s50.isOk, "step 50 failed"
  let e50 = s50.get()
  case e50.kind
  of sekAbsoluteStep:
    doAssert e50.globalLineIndex == 50, "step 50 gli"
  of sekDeltaStep:
    doAssert e50.lineDelta == 1, "step 50 delta"
  else:
    doAssert false, "step 50 unexpected kind"

  # --- Values ---
  let vals0 = reader.values(0'u64)
  doAssert vals0.isOk, "values 0 failed: " & vals0.error
  doAssert vals0.get().len == 2, "values 0 count"  # step 0 is divisible by 5
  doAssert vals0.get()[0].data == "0".toBytes, "values 0 first data"

  let vals1 = reader.values(1'u64)
  doAssert vals1.isOk, "values 1 failed"
  doAssert vals1.get().len == 1, "values 1 count"  # step 1, not divisible by 5

  let vals10 = reader.values(10'u64)
  doAssert vals10.isOk, "values 10 failed"
  doAssert vals10.get().len == 2, "values 10 count"  # step 10, divisible by 5

  # openArray overload
  var valBuf: array[4, VariableValue]
  let vn = reader.values(0'u64, valBuf)
  doAssert vn == 2, "values openArray count"

  # iterator overload
  var iterCount = 0
  for v in reader.valuesIter(0'u64):
    iterCount += 1
  doAssert iterCount == 2, "valuesIter count"

  # valueCount
  let vc = reader.valueCount()
  doAssert vc.isOk, "valueCount failed"
  doAssert vc.get() == 100, "valueCount: " & $vc.get()

  # --- Calls ---
  let cc = reader.callCount()
  doAssert cc.isOk, "callCount failed: " & cc.error
  doAssert cc.get() == 11, "callCount: " & $cc.get()  # 1 main + 5 process + 5 compute

  # Call 0: main (root)
  let c0 = reader.call(0)
  doAssert c0.isOk, "call 0 failed: " & c0.error
  doAssert c0.get().functionId == 0, "call 0 functionId"
  doAssert c0.get().parentCallKey == -1, "call 0 parentCallKey"
  doAssert c0.get().entryStep == 0, "call 0 entryStep"
  doAssert c0.get().exitStep == 99, "call 0 exitStep"
  doAssert c0.get().depth == 0, "call 0 depth"
  doAssert c0.get().children.len == 5, "call 0 children count"

  # Call 1: process (child of main)
  let c1 = reader.call(1)
  doAssert c1.isOk, "call 1 failed"
  doAssert c1.get().functionId == 1, "call 1 functionId"
  doAssert c1.get().parentCallKey == 0, "call 1 parentCallKey"
  doAssert c1.get().entryStep == 10, "call 1 entryStep"
  doAssert c1.get().returnValue == "100".toBytes, "call 1 returnValue"
  doAssert c1.get().children.len == 1, "call 1 children"
  doAssert c1.get().children[0] == 6, "call 1 child is compute(6)"

  # Call 6: compute (leaf, child of process 1)
  let c6 = reader.call(6)
  doAssert c6.isOk, "call 6 failed"
  doAssert c6.get().functionId == 2, "call 6 functionId"
  doAssert c6.get().parentCallKey == 1, "call 6 parentCallKey"
  doAssert c6.get().depth == 2, "call 6 depth"
  doAssert c6.get().children.len == 0, "call 6 children"

  # callRange iterator
  var callIterCount = 0
  for c in reader.callRange(0, 5):
    callIterCount += 1
  doAssert callIterCount == 5, "callRange iter count"

  # callRange openArray
  var callBuf: array[3, call_stream.CallRecord]
  let cn = reader.callRange(1, 3, callBuf)
  doAssert cn == 3, "callRange openArray count"
  doAssert callBuf[0].functionId == 1, "callRange buf[0] functionId"
  doAssert callBuf[1].functionId == 1, "callRange buf[1] functionId"
  doAssert callBuf[2].functionId == 1, "callRange buf[2] functionId"

  # --- IO Events ---
  let ec = reader.ioEventCount()
  doAssert ec.isOk, "ioEventCount failed: " & ec.error
  doAssert ec.get() == 5, "ioEventCount: " & $ec.get()

  let io0 = reader.ioEvent(0)
  doAssert io0.isOk, "ioEvent 0 failed"
  doAssert io0.get().kind == ioStdout, "io 0 kind"
  doAssert io0.get().stepId == 0, "io 0 stepId"
  doAssert io0.get().data == "line_0\n".toBytes, "io 0 data"

  let io3 = reader.ioEvent(3)
  doAssert io3.isOk, "ioEvent 3 failed"
  doAssert io3.get().kind == ioStderr, "io 3 kind"
  doAssert io3.get().stepId == 60, "io 3 stepId"

  let io4 = reader.ioEvent(4)
  doAssert io4.isOk, "ioEvent 4 failed"
  doAssert io4.get().kind == ioStderr, "io 4 kind"
  doAssert io4.get().stepId == 80, "io 4 stepId"
  doAssert io4.get().data == "line_4\n".toBytes, "io 4 data"

  # events iterator
  var evIterCount = 0
  for ev in reader.events(0, 5):
    evIterCount += 1
  doAssert evIterCount == 5, "events iter count"

  # events openArray
  var evBuf: array[3, IOEvent]
  let en = reader.events(1, 3, evBuf)
  doAssert en == 3, "events openArray count"
  doAssert evBuf[0].stepId == 20, "events buf[0] stepId"
  doAssert evBuf[1].stepId == 40, "events buf[1] stepId"
  doAssert evBuf[2].stepId == 60, "events buf[2] stepId"

  echo "PASS: test_reader_full_integration"

# ---------------------------------------------------------------------------
# test_reader_cache_eviction
# ---------------------------------------------------------------------------

proc test_reader_cache_eviction() {.raises: [].} =
  ## Write a trace with many chunks (small chunk size), verify sequential
  ## and random access patterns both return correct data.
  let numSteps = 500
  var ctfs = createCtfs()

  let metaFileRes = ctfs.addFile("meta.dat")
  doAssert metaFileRes.isOk
  var metaFile = metaFileRes.get()
  let meta = TraceMetadata(program: "cache_test", args: @[], workdir: "/tmp")
  let metaWr = ctfs.writeMetaDat(metaFile, meta, @["/src/cache.py"])
  doAssert metaWr.isOk

  let tabRes = initTraceInterningTables(ctfs)
  doAssert tabRes.isOk
  var tab = tabRes.get()
  discard ctfs.ensurePathId(tab, "/src/cache.py")
  discard ctfs.ensureFunctionId(tab, "test_fn")
  discard ctfs.ensureTypeId(tab, "int")
  discard ctfs.ensureVarnameId(tab, "i")

  # Use very small chunk size to create many chunks
  let execRes = initExecStreamWriter(ctfs, chunkSize = 16)
  doAssert execRes.isOk
  var execW = execRes.get()

  let valRes = initValueStreamWriter(ctfs)
  doAssert valRes.isOk
  var valW = valRes.get()

  for i in 0 ..< numSteps:
    var ev: StepEvent
    if i == 0:
      ev = StepEvent(kind: sekAbsoluteStep, globalLineIndex: 0)
    else:
      ev = StepEvent(kind: sekDeltaStep, lineDelta: 1)
    let r = ctfs.writeEvent(execW, ev)
    doAssert r.isOk

    let vr = ctfs.writeStepValues(valW, @[
      VariableValue(varnameId: 0, typeId: 0, data: intToStr(i).toBytes)])
    doAssert vr.isOk

  let flushRes = ctfs.flush(execW)
  doAssert flushRes.isOk

  let data = ctfs.toBytes()
  ctfs.closeCtfs()

  let readerRes = openNewTraceFromBytes(data)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  # Sequential access: forward
  for i in 0'u64 ..< uint64(numSteps):
    let ev = reader.step(i)
    doAssert ev.isOk, "fwd step " & $i & " failed: " & ev.error

    let vals = reader.values(i)
    doAssert vals.isOk, "fwd values " & $i & " failed: " & vals.error
    doAssert vals.get().len == 1, "fwd values count at " & $i
    doAssert vals.get()[0].data == intToStr(int(i)).toBytes,
      "fwd value data at " & $i

  # Sequential access: backward
  for j in countdown(numSteps - 1, 0):
    let i = uint64(j)
    let ev = reader.step(i)
    doAssert ev.isOk, "bwd step " & $i & " failed: " & ev.error

    let vals = reader.values(i)
    doAssert vals.isOk, "bwd values " & $i & " failed: " & vals.error
    doAssert vals.get()[0].data == intToStr(int(i)).toBytes,
      "bwd value data at " & $i

  # Random access with LCG
  var seed: uint64 = 98765
  for trial in 0 ..< 200:
    seed = (seed * 6364136223846793005'u64 + 1442695040888963407'u64)
    let idx = seed mod uint64(numSteps)

    let ev = reader.step(idx)
    doAssert ev.isOk, "rnd step " & $idx & " failed: " & ev.error

    let vals = reader.values(idx)
    doAssert vals.isOk, "rnd values " & $idx & " failed: " & vals.error
    doAssert vals.get().len == 1, "rnd values count at " & $idx
    doAssert vals.get()[0].data == intToStr(int(idx)).toBytes,
      "rnd value data at " & $idx

  echo "PASS: test_reader_cache_eviction"

# ---------------------------------------------------------------------------
# test_proportional_call_search
# ---------------------------------------------------------------------------

proc writeTraceWithSortedCalls(): seq[byte] {.raises: [].} =
  ## Write a trace with 1000 steps and 50 calls, sorted by entryStep.
  ## Each call covers ~20 steps. Calls are non-overlapping leaf calls
  ## wrapped by one root call spanning all steps.
  var ctfs = createCtfs()

  let metaFileRes = ctfs.addFile("meta.dat")
  doAssert metaFileRes.isOk
  var metaFile = metaFileRes.get()
  let meta = TraceMetadata(program: "search_test", args: @[], workdir: "/tmp")
  let metaWr = ctfs.writeMetaDat(metaFile, meta, @["/src/main.py"])
  doAssert metaWr.isOk

  let tabRes = initTraceInterningTables(ctfs)
  doAssert tabRes.isOk
  var tab = tabRes.get()
  discard ctfs.ensurePathId(tab, "/src/main.py")
  discard ctfs.ensureFunctionId(tab, "main")
  discard ctfs.ensureFunctionId(tab, "work")
  discard ctfs.ensureTypeId(tab, "int")
  discard ctfs.ensureVarnameId(tab, "i")

  let execRes = initExecStreamWriter(ctfs, chunkSize = 64)
  doAssert execRes.isOk
  var execW = execRes.get()

  let valRes = initValueStreamWriter(ctfs)
  doAssert valRes.isOk
  var valW = valRes.get()

  let callRes = initCallStreamWriter(ctfs)
  doAssert callRes.isOk
  var callW = callRes.get()

  # Write 1000 steps
  for i in 0 ..< 1000:
    var ev: StepEvent
    if i == 0:
      ev = StepEvent(kind: sekAbsoluteStep, globalLineIndex: 0)
    else:
      ev = StepEvent(kind: sekDeltaStep, lineDelta: 1)
    let wr = ctfs.writeEvent(execW, ev)
    doAssert wr.isOk
    let vr = ctfs.writeStepValues(valW, @[
      VariableValue(varnameId: 0, typeId: 0, data: intToStr(i).toBytes)])
    doAssert vr.isOk

  # Call 0: root call spanning all steps (depth 0)
  var childKeys: seq[uint64]
  for i in 1 .. 50:
    childKeys.add(uint64(i))
  let cw0 = ctfs.writeCall(callW, call_stream.CallRecord(
    functionId: 0, parentCallKey: -1,
    entryStep: 0, exitStep: 999, depth: 0,
    args: @[], returnValue: @[VoidReturnMarker], exception: @[],
    children: childKeys))
  doAssert cw0.isOk

  # Calls 1-50: leaf calls, each covering 20 steps, sorted by entryStep.
  # Call i covers steps [(i-1)*20, (i-1)*20+19].
  for i in 1 .. 50:
    let entry = uint64((i - 1) * 20)
    let exit = entry + 19
    let cw = ctfs.writeCall(callW, call_stream.CallRecord(
      functionId: 1, parentCallKey: 0,
      entryStep: entry, exitStep: exit, depth: 1,
      args: @[], returnValue: intToStr(i).toBytes, exception: @[],
      children: @[]))
    doAssert cw.isOk

  let flushRes = ctfs.flush(execW)
  doAssert flushRes.isOk

  result = ctfs.toBytes()
  ctfs.closeCtfs()

proc test_proportional_call_search() {.raises: [].} =
  let data = writeTraceWithSortedCalls()
  let readerRes = openNewTraceFromBytes(data)
  doAssert readerRes.isOk, "open failed: " & readerRes.error
  var reader = readerRes.get()

  let cc = reader.callCount()
  doAssert cc.isOk and cc.get() == 51, "expected 51 calls, got " & $cc.get()

  # Test every step finds its enclosing call, and the call range contains that step.
  for stepIdx in 0'u64 ..< 1000'u64:
    let res = reader.callForStep(stepIdx)
    doAssert res.isOk, "callForStep(" & $stepIdx & ") failed: " & res.error
    let c = res.get()
    doAssert stepIdx >= c.entryStep and stepIdx <= c.exitStep,
      "step " & $stepIdx & " not in call range [" &
      $c.entryStep & ", " & $c.exitStep & "]"

  # Verify specific steps land in expected leaf calls (depth 1).
  # Step 0 should be in call with entryStep=0 (call 1), depth 1.
  let r0 = reader.callForStep(0)
  doAssert r0.isOk
  doAssert r0.get().entryStep == 0 and r0.get().exitStep == 19, "step 0 call"
  doAssert r0.get().depth == 1, "step 0 should find leaf call (depth 1)"

  # Step 25 should be in call with entryStep=20, exitStep=39
  let r25 = reader.callForStep(25)
  doAssert r25.isOk
  doAssert r25.get().entryStep == 20 and r25.get().exitStep == 39, "step 25 call"

  # Step 999 should be in call with entryStep=980, exitStep=999
  let r999 = reader.callForStep(999)
  doAssert r999.isOk
  doAssert r999.get().entryStep == 980 and r999.get().exitStep == 999, "step 999 call"

  # Step 500 should be in call with entryStep=500, exitStep=519
  let r500 = reader.callForStep(500)
  doAssert r500.isOk
  doAssert r500.get().entryStep == 500 and r500.get().exitStep == 519, "step 500 call"

  # Out-of-range step should fail
  let rOob = reader.callForStep(1000)
  doAssert rOob.isErr, "step 1000 should fail"

  echo "PASS: test_proportional_call_search"

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

test_reader_full_integration()
test_reader_cache_eviction()
test_proportional_call_search()
echo "ALL PASS: test_reader_integration"
