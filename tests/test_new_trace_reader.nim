{.push raises: [].}

## Tests for the seek-based NewTraceReader (M18 + M19).
##
## M18: interning table loading at startup, startup time benchmark.
## M19: random step access, value access, sequential cache test, benchmarks.

import std/[options, times]
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
  ## Simple int-to-string without exceptions (for {.push raises: [].} compat).
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

## Write a trace with the given parameters and return serialized bytes.
## This is the shared setup used by most tests.
proc writeSmallTrace(): seq[byte] {.raises: [].} =
  var ctfs = createCtfs()

  # meta.dat
  let metaFileRes = ctfs.addFile("meta.dat")
  doAssert metaFileRes.isOk
  var metaFile = metaFileRes.get()
  let meta = TraceMetadata(program: "test_prog", args: @["--run"], workdir: "/tmp")
  let metaWr = ctfs.writeMetaDat(metaFile, meta, @["/src/main.py", "/src/helper.py"],
    recorderId = "reader-test")
  doAssert metaWr.isOk

  # interning tables
  let tabRes = initTraceInterningTables(ctfs)
  doAssert tabRes.isOk
  var tab = tabRes.get()

  discard ctfs.ensurePathId(tab, "/src/main.py")
  discard ctfs.ensurePathId(tab, "/src/helper.py")
  discard ctfs.ensureFunctionId(tab, "main")
  discard ctfs.ensureFunctionId(tab, "add")
  discard ctfs.ensureFunctionId(tab, "divide")
  discard ctfs.ensureTypeId(tab, "int")
  discard ctfs.ensureTypeId(tab, "str")
  discard ctfs.ensureVarnameId(tab, "x")
  discard ctfs.ensureVarnameId(tab, "y")
  discard ctfs.ensureVarnameId(tab, "result")

  # streams
  let execRes = initExecStreamWriter(ctfs, chunkSize = 64)
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

  # 8 step events (same as integration test)
  let wr0 = ctfs.writeEvent(execW, StepEvent(kind: sekAbsoluteStep, globalLineIndex: 1))
  doAssert wr0.isOk
  let v0 = ctfs.writeStepValues(valW, @[
    VariableValue(varnameId: 0, typeId: 0, data: "42".toBytes),
    VariableValue(varnameId: 1, typeId: 1, data: "hello".toBytes)])
  doAssert v0.isOk

  for i in 1 .. 4:
    let wr = ctfs.writeEvent(execW, StepEvent(kind: sekDeltaStep, lineDelta: 1))
    doAssert wr.isOk
    if i < 3:
      let vr = ctfs.writeStepValues(valW, @[
        VariableValue(varnameId: 0, typeId: 0, data: "42".toBytes),
        VariableValue(varnameId: 1, typeId: 1, data: "hello".toBytes)])
      doAssert vr.isOk
    elif i == 3:
      let vr = ctfs.writeStepValues(valW, @[
        VariableValue(varnameId: 0, typeId: 0, data: "42".toBytes),
        VariableValue(varnameId: 1, typeId: 1, data: "hello".toBytes),
        VariableValue(varnameId: 2, typeId: 0, data: "52".toBytes)])
      doAssert vr.isOk
    else:
      let vr = ctfs.writeStepValues(valW, @[])
      doAssert vr.isOk

  # Raise
  let wr5 = ctfs.writeEvent(execW, StepEvent(
    kind: sekRaise, exceptionTypeId: 0, message: "division by zero".toBytes))
  doAssert wr5.isOk
  let v5 = ctfs.writeStepValues(valW, @[])
  doAssert v5.isOk

  # Catch
  let wr6 = ctfs.writeEvent(execW, StepEvent(
    kind: sekCatch, catchExceptionTypeId: 0))
  doAssert wr6.isOk
  let v6 = ctfs.writeStepValues(valW, @[])
  doAssert v6.isOk

  # Step 7
  let wr7 = ctfs.writeEvent(execW, StepEvent(kind: sekDeltaStep, lineDelta: 1))
  doAssert wr7.isOk
  let v7 = ctfs.writeStepValues(valW, @[])
  doAssert v7.isOk

  # calls
  let cw0 = ctfs.writeCall(callW, call_stream.CallRecord(
    functionId: 0, parentCallKey: -1, entryStep: 0, exitStep: 7,
    depth: 0, args: @[], returnValue: @[VoidReturnMarker],
    exception: @[], children: @[1'u64, 2'u64]))
  doAssert cw0.isOk

  let cw1 = ctfs.writeCall(callW, call_stream.CallRecord(
    functionId: 1, parentCallKey: 0, entryStep: 2, exitStep: 2,
    depth: 1, args: @["42".toBytes, "10".toBytes],
    returnValue: "52".toBytes, exception: @[], children: @[]))
  doAssert cw1.isOk

  # IO event
  let ioWr = ctfs.writeEvent(ioW, IOEvent(
    kind: ioStdout, stepId: 3, data: "52\n".toBytes))
  doAssert ioWr.isOk

  let flushRes = ctfs.flush(execW)
  doAssert flushRes.isOk

  result = ctfs.toBytes()
  ctfs.closeCtfs()

## Write a trace with `numSteps` steps (each with one value) and return bytes.
proc writeLargeTrace(numSteps: int, chunkSize: int = DefaultExecChunkSize): seq[byte] {.raises: [].} =
  var ctfs = createCtfs()

  let metaFileRes = ctfs.addFile("meta.dat")
  doAssert metaFileRes.isOk
  var metaFile = metaFileRes.get()
  let meta = TraceMetadata(program: "bench", args: @[], workdir: "/tmp")
  let metaWr = ctfs.writeMetaDat(metaFile, meta, @["/src/bench.py"])
  doAssert metaWr.isOk

  let tabRes = initTraceInterningTables(ctfs)
  doAssert tabRes.isOk
  var tab = tabRes.get()
  discard ctfs.ensurePathId(tab, "/src/bench.py")
  discard ctfs.ensureFunctionId(tab, "bench_fn")
  discard ctfs.ensureTypeId(tab, "int")
  discard ctfs.ensureVarnameId(tab, "i")

  let execRes = initExecStreamWriter(ctfs, chunkSize = chunkSize)
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

    let iStr = intToStr(i)
    var iBytes = newSeq[byte](iStr.len)
    for j in 0 ..< iStr.len:
      iBytes[j] = byte(iStr[j])
    let vr = ctfs.writeStepValues(valW, @[
      VariableValue(varnameId: 0, typeId: 0, data: iBytes)])
    doAssert vr.isOk

  let flushRes = ctfs.flush(execW)
  doAssert flushRes.isOk

  result = ctfs.toBytes()
  ctfs.closeCtfs()

# ---------------------------------------------------------------------------
# M18: test_reader_interning_load
# ---------------------------------------------------------------------------

proc test_reader_interning_load() {.raises: [].} =
  let data = writeSmallTrace()
  let readerRes = openNewTraceFromBytes(data)
  doAssert readerRes.isOk, "openNewTraceFromBytes failed: " & readerRes.error
  var reader = readerRes.get()

  # Metadata
  doAssert reader.meta.program == "test_prog", "program mismatch: " & reader.meta.program
  doAssert reader.meta.workdir == "/tmp", "workdir mismatch"
  doAssert reader.meta.args.len == 1 and reader.meta.args[0] == "--run", "args mismatch"
  doAssert reader.meta.recorderId == "reader-test", "recorderId mismatch"

  # Paths
  doAssert reader.pathCount() == 2, "pathCount mismatch"
  let p0 = reader.path(0)
  doAssert p0.isOk and p0.get() == "/src/main.py", "path 0 mismatch"
  let p1 = reader.path(1)
  doAssert p1.isOk and p1.get() == "/src/helper.py", "path 1 mismatch"

  # Functions
  doAssert reader.functionCount() == 3, "functionCount mismatch"
  let f0 = reader.function(0)
  doAssert f0.isOk and f0.get() == "main", "func 0 mismatch"
  let f1 = reader.function(1)
  doAssert f1.isOk and f1.get() == "add", "func 1 mismatch"
  let f2 = reader.function(2)
  doAssert f2.isOk and f2.get() == "divide", "func 2 mismatch"

  # Types
  doAssert reader.typeCount() == 2, "typeCount mismatch"
  let t0 = reader.typeName(0)
  doAssert t0.isOk and t0.get() == "int", "type 0 mismatch"
  let t1 = reader.typeName(1)
  doAssert t1.isOk and t1.get() == "str", "type 1 mismatch"

  # Varnames
  doAssert reader.varnameCount() == 3, "varnameCount mismatch"
  let vn0 = reader.varname(0)
  doAssert vn0.isOk and vn0.get() == "x", "varname 0 mismatch"
  let vn1 = reader.varname(1)
  doAssert vn1.isOk and vn1.get() == "y", "varname 1 mismatch"
  let vn2 = reader.varname(2)
  doAssert vn2.isOk and vn2.get() == "result", "varname 2 mismatch"

  echo "PASS: test_reader_interning_load"

# ---------------------------------------------------------------------------
# M18: bench_reader_startup_time
# ---------------------------------------------------------------------------

proc bench_reader_startup_time() {.raises: [].} =
  ## Write a trace with 100K interned names, measure open() time.
  var ctfs = createCtfs()

  let metaFileRes = ctfs.addFile("meta.dat")
  doAssert metaFileRes.isOk
  var metaFile = metaFileRes.get()
  let meta = TraceMetadata(program: "bench_startup", args: @[], workdir: "/tmp")
  let metaWr = ctfs.writeMetaDat(metaFile, meta, @["/src/bench.py"])
  doAssert metaWr.isOk

  let tabRes = initTraceInterningTables(ctfs)
  doAssert tabRes.isOk
  var tab = tabRes.get()

  # Intern 100K varnames
  for i in 0 ..< 100_000:
    let name = "var_" & intToStr(i)
    let r = ctfs.ensureVarnameId(tab, name)
    doAssert r.isOk

  # Also intern some paths/funcs/types for completeness
  for i in 0 ..< 100:
    discard ctfs.ensurePathId(tab, "/src/file_" & intToStr(i) & ".py")
    discard ctfs.ensureFunctionId(tab, "func_" & intToStr(i))
    discard ctfs.ensureTypeId(tab, "Type_" & intToStr(i))

  let ctfsBytes = ctfs.toBytes()
  ctfs.closeCtfs()

  # Measure startup time
  let iterations = 10
  let start = cpuTime()
  for iter in 0 ..< iterations:
    let readerRes = openNewTraceFromBytes(ctfsBytes)
    doAssert readerRes.isOk, "open failed: " & readerRes.error
    let reader = readerRes.get()
    # Verify a sample to ensure tables are actually loaded
    let vn = reader.varname(50_000)
    doAssert vn.isOk and vn.get() == "var_50000", "varname 50000 mismatch"

  let elapsed = cpuTime() - start
  let avgMs = (elapsed / float(iterations)) * 1000.0

  echo "{\"benchmark\": \"reader_startup_time\"" &
    ", \"iterations\": " & $iterations &
    ", \"avg_ms\": " & $avgMs &
    ", \"container_bytes\": " & $ctfsBytes.len & "}"

  doAssert avgMs < 200.0, "startup too slow: " & $avgMs & "ms > 200ms"
  echo "PASS: bench_reader_startup_time"

# ---------------------------------------------------------------------------
# M19: test_reader_random_step_access
# ---------------------------------------------------------------------------

proc test_reader_random_step_access() {.raises: [].} =
  let numSteps = 10_000
  let data = writeLargeTrace(numSteps)

  let readerRes = openNewTraceFromBytes(data)
  doAssert readerRes.isOk, "open failed: " & readerRes.error
  var reader = readerRes.get()

  let sc = reader.stepCount()
  doAssert sc.isOk, "stepCount failed: " & sc.error
  doAssert sc.get() == uint64(numSteps), "stepCount mismatch: " & $sc.get()

  # Navigate to 100 pseudo-random steps and verify
  # Simple LCG for deterministic "random" indices
  var seed: uint64 = 12345
  for trial in 0 ..< 100:
    seed = (seed * 6364136223846793005'u64 + 1442695040888963407'u64)
    let idx = seed mod uint64(numSteps)

    let ev = reader.step(idx)
    doAssert ev.isOk, "step " & $idx & " failed: " & ev.error

    if idx == 0:
      doAssert ev.get().kind == sekAbsoluteStep, "step 0 should be AbsoluteStep"
      doAssert ev.get().globalLineIndex == 0, "step 0 gli mismatch"
    else:
      # All non-zero steps are DeltaStep(+1) or AbsoluteStep at chunk boundaries
      let e = ev.get()
      case e.kind
      of sekAbsoluteStep:
        doAssert e.globalLineIndex == idx, "absolute step gli mismatch at " & $idx &
          ": got " & $e.globalLineIndex
      of sekDeltaStep:
        doAssert e.lineDelta == 1, "delta mismatch at step " & $idx
      else:
        doAssert false, "unexpected event kind at step " & $idx

  echo "PASS: test_reader_random_step_access"

# ---------------------------------------------------------------------------
# M19: test_reader_values_access
# ---------------------------------------------------------------------------

proc test_reader_values_access() {.raises: [].} =
  let numSteps = 10_000
  let data = writeLargeTrace(numSteps)

  let readerRes = openNewTraceFromBytes(data)
  doAssert readerRes.isOk, "open failed: " & readerRes.error
  var reader = readerRes.get()

  # Check 100 pseudo-random steps
  var seed: uint64 = 67890
  for trial in 0 ..< 100:
    seed = (seed * 6364136223846793005'u64 + 1442695040888963407'u64)
    let idx = seed mod uint64(numSteps)

    let vals = reader.values(idx)
    doAssert vals.isOk, "values at step " & $idx & " failed: " & vals.error
    let vs = vals.get()
    doAssert vs.len == 1, "expected 1 value at step " & $idx & ", got " & $vs.len
    doAssert vs[0].varnameId == 0, "varnameId mismatch at " & $idx
    doAssert vs[0].typeId == 0, "typeId mismatch at " & $idx

    # Verify the value data encodes the step index
    let expected = intToStr(int(idx))
    var expectedBytes = newSeq[byte](expected.len)
    for j in 0 ..< expected.len:
      expectedBytes[j] = byte(expected[j])
    doAssert vs[0].data == expectedBytes, "value data mismatch at step " & $idx

  # Also test the openArray overload
  var buf: array[8, VariableValue]
  let n = reader.values(0'u64, buf)
  doAssert n == 1, "openArray values count mismatch"
  doAssert buf[0].varnameId == 0, "openArray varnameId mismatch"

  # Test the iterator
  var iterCount = 0
  for v in reader.valuesIter(0'u64):
    iterCount += 1
    doAssert v.varnameId == 0
  doAssert iterCount == 1, "iterator count mismatch"

  echo "PASS: test_reader_values_access"

# ---------------------------------------------------------------------------
# M19: test_reader_lru_cache_sequential
# ---------------------------------------------------------------------------

proc test_reader_lru_cache_sequential() {.raises: [].} =
  ## Step through 1000 steps sequentially; verify all correct.
  ## Cache hits are implicit in that this works without error and is fast.
  let numSteps = 1_000
  let data = writeLargeTrace(numSteps)

  let readerRes = openNewTraceFromBytes(data)
  doAssert readerRes.isOk, "open failed: " & readerRes.error
  var reader = readerRes.get()

  for i in 0'u64 ..< uint64(numSteps):
    let ev = reader.step(i)
    doAssert ev.isOk, "step " & $i & " failed: " & ev.error

    if i == 0:
      doAssert ev.get().kind == sekAbsoluteStep
    else:
      let e = ev.get()
      case e.kind
      of sekAbsoluteStep:
        doAssert e.globalLineIndex == i
      of sekDeltaStep:
        doAssert e.lineDelta == 1
      else:
        doAssert false, "unexpected event kind at step " & $i

    let vals = reader.values(i)
    doAssert vals.isOk, "values " & $i & " failed: " & vals.error
    doAssert vals.get().len == 1

  echo "PASS: test_reader_lru_cache_sequential"

# ---------------------------------------------------------------------------
# M19: bench_navigate_to_step
# ---------------------------------------------------------------------------

proc bench_navigate_to_step() {.raises: [].} =
  let numSteps = 10_000
  let data = writeLargeTrace(numSteps)

  let readerRes = openNewTraceFromBytes(data)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  let numNavigations = 1_000
  var seed: uint64 = 11111

  let start = cpuTime()
  for trial in 0 ..< numNavigations:
    seed = (seed * 6364136223846793005'u64 + 1442695040888963407'u64)
    let idx = seed mod uint64(numSteps)
    let ev = reader.step(idx)
    doAssert ev.isOk
  let elapsed = cpuTime() - start

  let totalUs = elapsed * 1_000_000.0
  let medianUs = totalUs / float(numNavigations)

  echo "{\"benchmark\": \"navigate_to_step\"" &
    ", \"num_steps\": " & $numSteps &
    ", \"navigations\": " & $numNavigations &
    ", \"total_us\": " & $totalUs &
    ", \"avg_us\": " & $medianUs & "}"

  doAssert medianUs < 100.0, "navigate too slow: " & $medianUs & "us > 100us"
  echo "PASS: bench_navigate_to_step"

# ---------------------------------------------------------------------------
# M19: bench_load_locals
# ---------------------------------------------------------------------------

proc bench_load_locals() {.raises: [].} =
  let numSteps = 10_000
  let data = writeLargeTrace(numSteps)

  let readerRes = openNewTraceFromBytes(data)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  let numReads = 1_000
  var seed: uint64 = 22222

  let start = cpuTime()
  for trial in 0 ..< numReads:
    seed = (seed * 6364136223846793005'u64 + 1442695040888963407'u64)
    let idx = seed mod uint64(numSteps)
    let vals = reader.values(idx)
    doAssert vals.isOk
    doAssert vals.get().len == 1
  let elapsed = cpuTime() - start

  let totalUs = elapsed * 1_000_000.0
  let avgUs = totalUs / float(numReads)

  echo "{\"benchmark\": \"load_locals\"" &
    ", \"num_steps\": " & $numSteps &
    ", \"reads\": " & $numReads &
    ", \"total_us\": " & $totalUs &
    ", \"avg_us\": " & $avgUs & "}"

  doAssert avgUs < 500.0, "load_locals too slow: " & $avgUs & "us > 500us"
  echo "PASS: bench_load_locals"

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

test_reader_interning_load()
bench_reader_startup_time()
test_reader_random_step_access()
test_reader_values_access()
test_reader_lru_cache_sequential()
bench_navigate_to_step()
bench_load_locals()
echo "ALL PASS: test_new_trace_reader"
