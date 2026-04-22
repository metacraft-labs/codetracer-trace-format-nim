{.push raises: [].}

## Multi-stream writer integration test (M17).
##
## Wires together all Phase 3 components (meta.dat, interning tables,
## exec stream, value stream, call stream, IO event stream) to write
## a complete trace and verify the output round-trips correctly.

import std/[options, times]
import results
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_trace_types
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/interning_table
import codetracer_trace_writer/global_line_index
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/exec_stream
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/call_stream
import codetracer_trace_writer/io_event_stream

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc toBytes(s: string): seq[byte] {.raises: [].} =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

proc hasInternalFile(ctfsBytes: openArray[byte], name: string,
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): bool {.raises: [].} =
  let res = readInternalFile(ctfsBytes, name, blockSize, maxEntries)
  res.isOk

# ---------------------------------------------------------------------------
# Main integration test
# ---------------------------------------------------------------------------

proc test_multi_stream_writer_integration() {.raises: [].} =
  ## Simulates writing a trace for a small Python program:
  ##
  ##   File: /src/main.py (10 lines)
  ##   File: /src/helper.py (5 lines)
  ##
  ##   def main():
  ##     x = 42
  ##     y = "hello"
  ##     result = add(x, 10)
  ##     print(result)         # stdout
  ##     try:
  ##       divide(10, 0)       # raises
  ##     except ZeroDivisionError:
  ##       pass
  ##
  ##   def add(a, b):
  ##     return a + b
  ##
  ##   def divide(a, b):
  ##     return a / b          # raises ZeroDivisionError

  # ------ 1. Create CTFS container ------
  var ctfs = createCtfs()

  # ------ 2. Write meta.dat ------
  let metaFileRes = ctfs.addFile("meta.dat")
  doAssert metaFileRes.isOk, "addFile meta.dat failed: " & metaFileRes.error
  var metaFile = metaFileRes.get()

  let meta = TraceMetadata(
    program: "test_prog",
    args: @["--run"],
    workdir: "/tmp"
  )
  let paths = @["/src/main.py", "/src/helper.py"]

  let metaRes = ctfs.writeMetaDat(metaFile, meta, paths, recorderId = "integration-test")
  doAssert metaRes.isOk, "writeMetaDat failed: " & metaRes.error

  # ------ 3. Init interning tables ------
  let tablesRes = initTraceInterningTables(ctfs)
  doAssert tablesRes.isOk, "initTraceInterningTables failed: " & tablesRes.error
  var tables = tablesRes.get()

  # ------ 4. Intern paths ------
  let pathId0 = ctfs.ensurePathId(tables, "/src/main.py")
  doAssert pathId0.isOk and pathId0.get() == 0, "pathId0 failed"
  let pathId1 = ctfs.ensurePathId(tables, "/src/helper.py")
  doAssert pathId1.isOk and pathId1.get() == 1, "pathId1 failed"
  # Re-intern should return same ID
  let pathId0Dup = ctfs.ensurePathId(tables, "/src/main.py")
  doAssert pathId0Dup.isOk and pathId0Dup.get() == 0, "pathId0 dedup failed"

  # ------ 5. Intern functions ------
  let funcMain = ctfs.ensureFunctionId(tables, "main")
  doAssert funcMain.isOk and funcMain.get() == 0, "funcMain failed"
  let funcAdd = ctfs.ensureFunctionId(tables, "add")
  doAssert funcAdd.isOk and funcAdd.get() == 1, "funcAdd failed"
  let funcDivide = ctfs.ensureFunctionId(tables, "divide")
  doAssert funcDivide.isOk and funcDivide.get() == 2, "funcDivide failed"

  # ------ 6. Intern types ------
  let typeInt = ctfs.ensureTypeId(tables, "int")
  doAssert typeInt.isOk and typeInt.get() == 0, "typeInt failed"
  let typeStr = ctfs.ensureTypeId(tables, "str")
  doAssert typeStr.isOk and typeStr.get() == 1, "typeStr failed"

  # ------ 7. Intern varnames ------
  let vnX = ctfs.ensureVarnameId(tables, "x")
  doAssert vnX.isOk and vnX.get() == 0, "vnX failed"
  let vnY = ctfs.ensureVarnameId(tables, "y")
  doAssert vnY.isOk and vnY.get() == 1, "vnY failed"
  let vnResult = ctfs.ensureVarnameId(tables, "result")
  doAssert vnResult.isOk and vnResult.get() == 2, "vnResult failed"
  let vnA = ctfs.ensureVarnameId(tables, "a")
  doAssert vnA.isOk and vnA.get() == 3, "vnA failed"
  let vnB = ctfs.ensureVarnameId(tables, "b")
  doAssert vnB.isOk and vnB.get() == 4, "vnB failed"

  # ------ 8. Build global line index ------
  let gli = buildGlobalLineIndex(@[10'u64, 5'u64])
  doAssert gli.totalLines == 15, "totalLines should be 15"
  # main.py line 1 -> global 1, helper.py line 0 -> global 10
  doAssert gli.globalIndex(0, 1) == 1, "globalIndex(0,1)"
  doAssert gli.globalIndex(1, 0) == 10, "globalIndex(1,0)"

  # Resolve back
  let (fileId, line) = gli.resolve(1)
  doAssert fileId == 0 and line == 1, "resolve(1)"
  let (fileId2, line2) = gli.resolve(10)
  doAssert fileId2 == 1 and line2 == 0, "resolve(10)"

  # ------ 9. Init stream writers ------
  let execRes = initExecStreamWriter(ctfs, chunkSize = 64)
  doAssert execRes.isOk, "initExecStreamWriter failed: " & execRes.error
  var execWriter = execRes.get()

  let valRes = initValueStreamWriter(ctfs)
  doAssert valRes.isOk, "initValueStreamWriter failed: " & valRes.error
  var valWriter = valRes.get()

  let callRes = initCallStreamWriter(ctfs)
  doAssert callRes.isOk, "initCallStreamWriter failed: " & callRes.error
  var callWriter = callRes.get()

  let ioRes = initIOEventStreamWriter(ctfs)
  doAssert ioRes.isOk, "initIOEventStreamWriter failed: " & ioRes.error
  var ioWriter = ioRes.get()

  # ------ 10. Write trace events ------
  # Step 0: AbsoluteStep at main.py:1 (global line = 1)
  let step0Idx = gli.globalIndex(0, 1)  # = 1
  let wr0 = ctfs.writeEvent(execWriter, StepEvent(
    kind: sekAbsoluteStep, globalLineIndex: step0Idx))
  doAssert wr0.isOk, "step0 failed: " & wr0.error
  let vr0 = ctfs.writeStepValues(valWriter, @[
    VariableValue(varnameId: 0, typeId: 0, data: "42".toBytes),
    VariableValue(varnameId: 1, typeId: 1, data: "hello".toBytes),
  ])
  doAssert vr0.isOk, "values0 failed: " & vr0.error

  # Step 1: DeltaStep(+1) at main.py:2
  let wr1 = ctfs.writeEvent(execWriter, StepEvent(
    kind: sekDeltaStep, lineDelta: 1))
  doAssert wr1.isOk, "step1 failed: " & wr1.error
  let vr1 = ctfs.writeStepValues(valWriter, @[
    VariableValue(varnameId: 0, typeId: 0, data: "42".toBytes),
    VariableValue(varnameId: 1, typeId: 1, data: "hello".toBytes),
  ])
  doAssert vr1.isOk, "values1 failed: " & vr1.error

  # Step 2: DeltaStep(+1) at main.py:3 — after add() call returns, result=52
  let wr2 = ctfs.writeEvent(execWriter, StepEvent(
    kind: sekDeltaStep, lineDelta: 1))
  doAssert wr2.isOk, "step2 failed: " & wr2.error
  let vr2 = ctfs.writeStepValues(valWriter, @[
    VariableValue(varnameId: 0, typeId: 0, data: "42".toBytes),
    VariableValue(varnameId: 1, typeId: 1, data: "hello".toBytes),
    VariableValue(varnameId: 2, typeId: 0, data: "52".toBytes),
  ])
  doAssert vr2.isOk, "values2 failed: " & vr2.error

  # Step 3: DeltaStep(+1) at main.py:4 — print(result)
  let wr3 = ctfs.writeEvent(execWriter, StepEvent(
    kind: sekDeltaStep, lineDelta: 1))
  doAssert wr3.isOk, "step3 failed: " & wr3.error
  let vr3 = ctfs.writeStepValues(valWriter, @[
    VariableValue(varnameId: 0, typeId: 0, data: "42".toBytes),
    VariableValue(varnameId: 1, typeId: 1, data: "hello".toBytes),
    VariableValue(varnameId: 2, typeId: 0, data: "52".toBytes),
  ])
  doAssert vr3.isOk, "values3 failed: " & vr3.error

  # IO event: stdout "52"
  let ioWr0 = ctfs.writeEvent(ioWriter, IOEvent(
    kind: ioStdout, stepId: 3, data: "52\n".toBytes))
  doAssert ioWr0.isOk, "ioEvent0 failed: " & ioWr0.error

  # Step 4: DeltaStep(+1) at main.py:5 — entering try block
  let wr4 = ctfs.writeEvent(execWriter, StepEvent(
    kind: sekDeltaStep, lineDelta: 1))
  doAssert wr4.isOk, "step4 failed: " & wr4.error
  let vr4 = ctfs.writeStepValues(valWriter, @[])
  doAssert vr4.isOk, "values4 failed: " & vr4.error

  # Step 5: Raise — ZeroDivisionError
  let raiseMsg = "division by zero".toBytes
  let wr5 = ctfs.writeEvent(execWriter, StepEvent(
    kind: sekRaise, exceptionTypeId: 0, message: raiseMsg))
  doAssert wr5.isOk, "step5 Raise failed: " & wr5.error
  let vr5 = ctfs.writeStepValues(valWriter, @[])
  doAssert vr5.isOk, "values5 failed: " & vr5.error

  # Step 6: Catch
  let wr6 = ctfs.writeEvent(execWriter, StepEvent(
    kind: sekCatch, catchExceptionTypeId: 0))
  doAssert wr6.isOk, "step6 Catch failed: " & wr6.error
  let vr6 = ctfs.writeStepValues(valWriter, @[])
  doAssert vr6.isOk, "values6 failed: " & vr6.error

  # Step 7: DeltaStep(+1) at main.py:8 — except handler (pass)
  let wr7 = ctfs.writeEvent(execWriter, StepEvent(
    kind: sekDeltaStep, lineDelta: 1))
  doAssert wr7.isOk, "step7 failed: " & wr7.error
  let vr7 = ctfs.writeStepValues(valWriter, @[])
  doAssert vr7.isOk, "values7 failed: " & vr7.error

  # Write call records:
  # Call 0 (main): entry=0, exit=7, depth=0, children=[1,2]
  let cw0 = ctfs.writeCall(callWriter, call_stream.CallRecord(
    functionId: 0, parentCallKey: -1, entryStep: 0, exitStep: 7,
    depth: 0, args: @[], returnValue: @[VoidReturnMarker],
    exception: @[], children: @[1'u64, 2'u64]))
  doAssert cw0.isOk, "call0 failed: " & cw0.error

  # Call 1 (add): entry=2, exit=2, depth=1, args=[42,10], return=52
  let cw1 = ctfs.writeCall(callWriter, call_stream.CallRecord(
    functionId: 1, parentCallKey: 0, entryStep: 2, exitStep: 2,
    depth: 1, args: @["42".toBytes, "10".toBytes],
    returnValue: "52".toBytes, exception: @[], children: @[]))
  doAssert cw1.isOk, "call1 failed: " & cw1.error

  # Call 2 (divide): entry=5, exit=6, depth=1, exception="ZeroDivisionError"
  let cw2 = ctfs.writeCall(callWriter, call_stream.CallRecord(
    functionId: 2, parentCallKey: 0, entryStep: 5, exitStep: 6,
    depth: 1, args: @["10".toBytes, "0".toBytes],
    returnValue: @[VoidReturnMarker],
    exception: "ZeroDivisionError".toBytes, children: @[]))
  doAssert cw2.isOk, "call2 failed: " & cw2.error

  # ------ 11. Flush all streams ------
  let flushExec = ctfs.flush(execWriter)
  doAssert flushExec.isOk, "flush exec failed: " & flushExec.error

  # ------ 12. Serialize to bytes ------
  let ctfsBytes = ctfs.toBytes()
  doAssert ctfsBytes.len > 0, "empty CTFS output"

  # ------ 13. Verify file layout ------
  doAssert hasInternalFile(ctfsBytes, "meta.dat"), "missing meta.dat"
  doAssert hasInternalFile(ctfsBytes, "steps.dat"), "missing steps.dat"
  doAssert hasInternalFile(ctfsBytes, "steps.idx"), "missing steps.idx"
  doAssert hasInternalFile(ctfsBytes, "values.dat"), "missing values.dat"
  doAssert hasInternalFile(ctfsBytes, "values.off"), "missing values.off"
  doAssert hasInternalFile(ctfsBytes, "calls.dat"), "missing calls.dat"
  doAssert hasInternalFile(ctfsBytes, "calls.off"), "missing calls.off"
  doAssert hasInternalFile(ctfsBytes, "events.dat"), "missing events.dat"
  doAssert hasInternalFile(ctfsBytes, "events.off"), "missing events.off"
  doAssert hasInternalFile(ctfsBytes, "paths.dat"), "missing paths.dat"
  doAssert hasInternalFile(ctfsBytes, "paths.off"), "missing paths.off"
  doAssert hasInternalFile(ctfsBytes, "funcs.dat"), "missing funcs.dat"
  doAssert hasInternalFile(ctfsBytes, "funcs.off"), "missing funcs.off"
  doAssert hasInternalFile(ctfsBytes, "types.dat"), "missing types.dat"
  doAssert hasInternalFile(ctfsBytes, "types.off"), "missing types.off"
  doAssert hasInternalFile(ctfsBytes, "varnames.dat"), "missing varnames.dat"
  doAssert hasInternalFile(ctfsBytes, "varnames.off"), "missing varnames.off"

  # ------ 14. Read back and verify ------

  # 14a. meta.dat
  let metaBytes = readInternalFile(ctfsBytes, "meta.dat")
  doAssert metaBytes.isOk, "read meta.dat failed: " & metaBytes.error
  let metaContents = readMetaDat(metaBytes.get())
  doAssert metaContents.isOk, "parse meta.dat failed: " & metaContents.error
  let mc = metaContents.get()
  doAssert mc.program == "test_prog", "program mismatch: " & mc.program
  doAssert mc.workdir == "/tmp", "workdir mismatch: " & mc.workdir
  doAssert mc.args.len == 1, "args len mismatch"
  doAssert mc.args[0] == "--run", "arg0 mismatch: " & mc.args[0]
  doAssert mc.recorderId == "integration-test",
    "recorderId mismatch: " & mc.recorderId
  doAssert mc.paths.len == 2, "paths len mismatch"
  doAssert mc.paths[0] == "/src/main.py", "path0 mismatch: " & mc.paths[0]
  doAssert mc.paths[1] == "/src/helper.py", "path1 mismatch: " & mc.paths[1]
  doAssert mc.mcrFields.isNone, "mcrFields should be none"

  # 14b. Interning tables
  let pathsReader = initInterningTableReader(ctfsBytes, "paths")
  doAssert pathsReader.isOk, "paths reader failed: " & pathsReader.error
  let pr = pathsReader.get()
  doAssert pr.count() == 2, "paths count mismatch: " & $pr.count()
  let p0 = pr.readById(0)
  doAssert p0.isOk and p0.get() == "/src/main.py",
    "path0 readback: " & (if p0.isOk: p0.get() else: p0.error)
  let p1 = pr.readById(1)
  doAssert p1.isOk and p1.get() == "/src/helper.py",
    "path1 readback: " & (if p1.isOk: p1.get() else: p1.error)

  let funcsReader = initInterningTableReader(ctfsBytes, "funcs")
  doAssert funcsReader.isOk, "funcs reader failed: " & funcsReader.error
  let fr = funcsReader.get()
  doAssert fr.count() == 3, "funcs count mismatch: " & $fr.count()
  let f0 = fr.readById(0)
  doAssert f0.isOk and f0.get() == "main", "func0 mismatch"
  let f1 = fr.readById(1)
  doAssert f1.isOk and f1.get() == "add", "func1 mismatch"
  let f2 = fr.readById(2)
  doAssert f2.isOk and f2.get() == "divide", "func2 mismatch"

  let typesReader = initInterningTableReader(ctfsBytes, "types")
  doAssert typesReader.isOk, "types reader failed: " & typesReader.error
  let tr = typesReader.get()
  doAssert tr.count() == 2, "types count mismatch: " & $tr.count()
  let t0 = tr.readById(0)
  doAssert t0.isOk and t0.get() == "int", "type0 mismatch"
  let t1 = tr.readById(1)
  doAssert t1.isOk and t1.get() == "str", "type1 mismatch"

  let varnamesReader = initInterningTableReader(ctfsBytes, "varnames")
  doAssert varnamesReader.isOk, "varnames reader failed: " & varnamesReader.error
  let vr = varnamesReader.get()
  doAssert vr.count() == 5, "varnames count mismatch: " & $vr.count()
  let vn0 = vr.readById(0)
  doAssert vn0.isOk and vn0.get() == "x", "varname0 mismatch"
  let vn1 = vr.readById(1)
  doAssert vn1.isOk and vn1.get() == "y", "varname1 mismatch"
  let vn2 = vr.readById(2)
  doAssert vn2.isOk and vn2.get() == "result", "varname2 mismatch"
  let vn3 = vr.readById(3)
  doAssert vn3.isOk and vn3.get() == "a", "varname3 mismatch"
  let vn4 = vr.readById(4)
  doAssert vn4.isOk and vn4.get() == "b", "varname4 mismatch"

  # 14c. Exec stream
  var execReader = initExecStreamReader(ctfsBytes)
  doAssert execReader.isOk, "exec reader failed: " & execReader.error
  var er = execReader.get()
  doAssert er.totalEvents() == 8, "exec totalEvents mismatch: " & $er.totalEvents()

  # Event 0: AbsoluteStep(globalLineIndex=1)
  let ev0 = er.readEvent(0)
  doAssert ev0.isOk, "readEvent 0 failed: " & ev0.error
  doAssert ev0.get().kind == sekAbsoluteStep, "event0 kind mismatch"
  doAssert ev0.get().globalLineIndex == 1, "event0 gli mismatch: " &
    $ev0.get().globalLineIndex

  # Event 1: DeltaStep(+1)
  let ev1 = er.readEvent(1)
  doAssert ev1.isOk, "readEvent 1 failed: " & ev1.error
  doAssert ev1.get().kind == sekDeltaStep, "event1 kind mismatch"
  doAssert ev1.get().lineDelta == 1, "event1 delta mismatch"

  # Event 2: DeltaStep(+1)
  let ev2 = er.readEvent(2)
  doAssert ev2.isOk, "readEvent 2 failed: " & ev2.error
  doAssert ev2.get().kind == sekDeltaStep, "event2 kind mismatch"
  doAssert ev2.get().lineDelta == 1, "event2 delta mismatch"

  # Event 3: DeltaStep(+1)
  let ev3 = er.readEvent(3)
  doAssert ev3.isOk, "readEvent 3 failed: " & ev3.error
  doAssert ev3.get().kind == sekDeltaStep, "event3 kind mismatch"
  doAssert ev3.get().lineDelta == 1, "event3 delta mismatch"

  # Event 4: DeltaStep(+1)
  let ev4 = er.readEvent(4)
  doAssert ev4.isOk, "readEvent 4 failed: " & ev4.error
  doAssert ev4.get().kind == sekDeltaStep, "event4 kind mismatch"
  doAssert ev4.get().lineDelta == 1, "event4 delta mismatch"

  # Event 5: Raise
  let ev5 = er.readEvent(5)
  doAssert ev5.isOk, "readEvent 5 failed: " & ev5.error
  doAssert ev5.get().kind == sekRaise, "event5 kind mismatch"
  doAssert ev5.get().exceptionTypeId == 0, "event5 typeId mismatch"
  doAssert ev5.get().message == "division by zero".toBytes,
    "event5 message mismatch"

  # Event 6: Catch
  let ev6 = er.readEvent(6)
  doAssert ev6.isOk, "readEvent 6 failed: " & ev6.error
  doAssert ev6.get().kind == sekCatch, "event6 kind mismatch"
  doAssert ev6.get().catchExceptionTypeId == 0, "event6 typeId mismatch"

  # Event 7: DeltaStep(+1)
  let ev7 = er.readEvent(7)
  doAssert ev7.isOk, "readEvent 7 failed: " & ev7.error
  doAssert ev7.get().kind == sekDeltaStep, "event7 kind mismatch"
  doAssert ev7.get().lineDelta == 1, "event7 delta mismatch"

  # 14d. Value stream
  let valReaderRes = initValueStreamReader(ctfsBytes)
  doAssert valReaderRes.isOk, "value reader failed: " & valReaderRes.error
  let valReader = valReaderRes.get()
  doAssert valReader.count() == 8, "value count mismatch: " & $valReader.count()

  # Step 0: x=42, y="hello"
  let vals0 = valReader.readStepValues(0)
  doAssert vals0.isOk, "readStepValues 0 failed: " & vals0.error
  doAssert vals0.get().len == 2, "step0 values count: " & $vals0.get().len
  doAssert vals0.get()[0].varnameId == 0, "step0 val0 varnameId"
  doAssert vals0.get()[0].typeId == 0, "step0 val0 typeId"
  doAssert vals0.get()[0].data == "42".toBytes, "step0 val0 data"
  doAssert vals0.get()[1].varnameId == 1, "step0 val1 varnameId"
  doAssert vals0.get()[1].typeId == 1, "step0 val1 typeId"
  doAssert vals0.get()[1].data == "hello".toBytes, "step0 val1 data"

  # Step 2: x=42, y="hello", result=52
  let vals2 = valReader.readStepValues(2)
  doAssert vals2.isOk, "readStepValues 2 failed: " & vals2.error
  doAssert vals2.get().len == 3, "step2 values count: " & $vals2.get().len
  doAssert vals2.get()[2].varnameId == 2, "step2 val2 varnameId"
  doAssert vals2.get()[2].data == "52".toBytes, "step2 val2 data"

  # Step 4: empty values (entering try block)
  let vals4 = valReader.readStepValues(4)
  doAssert vals4.isOk, "readStepValues 4 failed: " & vals4.error
  doAssert vals4.get().len == 0, "step4 should have no values"

  # 14e. Call stream
  let callReaderRes = initCallStreamReader(ctfsBytes)
  doAssert callReaderRes.isOk, "call reader failed: " & callReaderRes.error
  let callReader = callReaderRes.get()
  doAssert callReader.count() == 3, "call count mismatch: " & $callReader.count()

  # Call 0 (main)
  let call0 = callReader.readCall(0)
  doAssert call0.isOk, "readCall 0 failed: " & call0.error
  let c0 = call0.get()
  doAssert c0.functionId == 0, "call0 functionId"
  doAssert c0.parentCallKey == -1, "call0 parentCallKey"
  doAssert c0.entryStep == 0, "call0 entryStep"
  doAssert c0.exitStep == 7, "call0 exitStep"
  doAssert c0.depth == 0, "call0 depth"
  doAssert c0.args.len == 0, "call0 args"
  doAssert c0.returnValue == @[VoidReturnMarker], "call0 returnValue"
  doAssert c0.exception.len == 0, "call0 exception"
  doAssert c0.children.len == 2, "call0 children count"
  doAssert c0.children[0] == 1 and c0.children[1] == 2, "call0 children values"

  # Call 1 (add)
  let call1 = callReader.readCall(1)
  doAssert call1.isOk, "readCall 1 failed: " & call1.error
  let c1 = call1.get()
  doAssert c1.functionId == 1, "call1 functionId"
  doAssert c1.parentCallKey == 0, "call1 parentCallKey"
  doAssert c1.entryStep == 2, "call1 entryStep"
  doAssert c1.exitStep == 2, "call1 exitStep"
  doAssert c1.depth == 1, "call1 depth"
  doAssert c1.args.len == 2, "call1 args count"
  doAssert c1.args[0] == "42".toBytes, "call1 arg0"
  doAssert c1.args[1] == "10".toBytes, "call1 arg1"
  doAssert c1.returnValue == "52".toBytes, "call1 returnValue"
  doAssert c1.exception.len == 0, "call1 exception"
  doAssert c1.children.len == 0, "call1 children"

  # Call 2 (divide)
  let call2 = callReader.readCall(2)
  doAssert call2.isOk, "readCall 2 failed: " & call2.error
  let c2 = call2.get()
  doAssert c2.functionId == 2, "call2 functionId"
  doAssert c2.parentCallKey == 0, "call2 parentCallKey"
  doAssert c2.entryStep == 5, "call2 entryStep"
  doAssert c2.exitStep == 6, "call2 exitStep"
  doAssert c2.depth == 1, "call2 depth"
  doAssert c2.args.len == 2, "call2 args count"
  doAssert c2.args[0] == "10".toBytes, "call2 arg0"
  doAssert c2.args[1] == "0".toBytes, "call2 arg1"
  doAssert c2.returnValue == @[VoidReturnMarker], "call2 returnValue"
  doAssert c2.exception == "ZeroDivisionError".toBytes, "call2 exception"
  doAssert c2.children.len == 0, "call2 children"

  # 14f. IO events
  let ioReaderRes = initIOEventStreamReader(ctfsBytes)
  doAssert ioReaderRes.isOk, "io reader failed: " & ioReaderRes.error
  let ioReader = ioReaderRes.get()
  doAssert ioReader.count() == 1, "io count mismatch: " & $ioReader.count()

  let io0 = ioReader.readEvent(0)
  doAssert io0.isOk, "readEvent io0 failed: " & io0.error
  let ioEv = io0.get()
  doAssert ioEv.kind == ioStdout, "io0 kind mismatch"
  doAssert ioEv.stepId == 3, "io0 stepId mismatch: " & $ioEv.stepId
  doAssert ioEv.data == "52\n".toBytes, "io0 data mismatch"

  ctfs.closeCtfs()
  echo "PASS: test_multi_stream_writer_integration"


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

proc bench_multi_stream_write_throughput() {.raises: [].} =
  ## Write 100K steps with values, 10K calls, 1K IO events.
  ## Measure total time and throughput.
  let totalSteps = 100_000
  let totalCalls = 10_000
  let totalIOEvents = 1_000

  var ctfs = createCtfs()

  # Meta
  let metaFileRes = ctfs.addFile("meta.dat")
  doAssert metaFileRes.isOk
  var metaFile = metaFileRes.get()
  let meta = TraceMetadata(program: "bench", args: @[], workdir: "/tmp")
  let metaWr = ctfs.writeMetaDat(metaFile, meta, @["/src/bench.py"])
  doAssert metaWr.isOk

  # Interning
  let tabRes = initTraceInterningTables(ctfs)
  doAssert tabRes.isOk
  var tab = tabRes.get()
  discard ctfs.ensurePathId(tab, "/src/bench.py")
  discard ctfs.ensureFunctionId(tab, "bench_fn")
  discard ctfs.ensureTypeId(tab, "int")
  discard ctfs.ensureVarnameId(tab, "i")

  # Streams
  let execRes = initExecStreamWriter(ctfs)
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

  let startTime = cpuTime()

  # Write steps + values
  for i in 0 ..< totalSteps:
    var ev: StepEvent
    if i == 0:
      ev = StepEvent(kind: sekAbsoluteStep, globalLineIndex: 0)
    else:
      ev = StepEvent(kind: sekDeltaStep, lineDelta: 1)
    let r = ctfs.writeEvent(execW, ev)
    doAssert r.isOk

    # Simple value per step
    var iBytes: seq[byte]
    let iStr = $i
    iBytes = newSeq[byte](iStr.len)
    for j in 0 ..< iStr.len:
      iBytes[j] = byte(iStr[j])
    let vr = ctfs.writeStepValues(valW, @[
      VariableValue(varnameId: 0, typeId: 0, data: iBytes)])
    doAssert vr.isOk

  # Write calls
  for i in 0 ..< totalCalls:
    let cr = ctfs.writeCall(callW, call_stream.CallRecord(
      functionId: 0, parentCallKey: -1,
      entryStep: uint64(i * 10), exitStep: uint64(i * 10 + 9),
      depth: 0, args: @[], returnValue: @[VoidReturnMarker],
      exception: @[], children: @[]))
    doAssert cr.isOk

  # Write IO events
  for i in 0 ..< totalIOEvents:
    let ioEvt = ctfs.writeEvent(ioW, IOEvent(
      kind: ioStdout, stepId: uint64(i * 100),
      data: "output\n".toBytes))
    doAssert ioEvt.isOk

  # Flush
  let flushRes = ctfs.flush(execW)
  doAssert flushRes.isOk

  let elapsed = cpuTime() - startTime
  let totalEvents = totalSteps + totalCalls + totalIOEvents
  let eventsPerSec = float(totalEvents) / elapsed
  let ctfsBytes = ctfs.toBytes()
  let bytesPerSec = float(ctfsBytes.len) / elapsed

  echo "{\"benchmark\": \"multi_stream_write_throughput\"" &
    ", \"total_steps\": " & $totalSteps &
    ", \"total_calls\": " & $totalCalls &
    ", \"total_io_events\": " & $totalIOEvents &
    ", \"elapsed_sec\": " & $elapsed &
    ", \"events_per_sec\": " & $eventsPerSec &
    ", \"container_bytes\": " & $ctfsBytes.len &
    ", \"bytes_per_sec\": " & $bytesPerSec & "}"

  ctfs.closeCtfs()
  echo "PASS: bench_multi_stream_write_throughput"


proc bench_multi_stream_trace_size() {.raises: [].} =
  ## Compare multi-stream trace size vs estimated old format size.
  ## Old format: ~17 bytes per step * 100K = ~1.7 MB uncompressed.
  let totalSteps = 100_000

  var ctfs = createCtfs()

  let metaFileRes = ctfs.addFile("meta.dat")
  doAssert metaFileRes.isOk
  var metaFile = metaFileRes.get()
  let meta = TraceMetadata(program: "bench_size", args: @[], workdir: "/tmp")
  let metaWr = ctfs.writeMetaDat(metaFile, meta, @["/src/bench.py"])
  doAssert metaWr.isOk

  let tabRes = initTraceInterningTables(ctfs)
  doAssert tabRes.isOk
  var tab = tabRes.get()
  discard ctfs.ensurePathId(tab, "/src/bench.py")
  discard ctfs.ensureFunctionId(tab, "fn")
  discard ctfs.ensureTypeId(tab, "int")
  discard ctfs.ensureVarnameId(tab, "x")

  let execRes = initExecStreamWriter(ctfs)
  doAssert execRes.isOk
  var execW = execRes.get()

  let valRes = initValueStreamWriter(ctfs)
  doAssert valRes.isOk
  var valW = valRes.get()

  for i in 0 ..< totalSteps:
    var ev: StepEvent
    if i == 0:
      ev = StepEvent(kind: sekAbsoluteStep, globalLineIndex: 0)
    else:
      ev = StepEvent(kind: sekDeltaStep, lineDelta: 1)
    let r = ctfs.writeEvent(execW, ev)
    doAssert r.isOk

    let vr = ctfs.writeStepValues(valW, @[
      VariableValue(varnameId: 0, typeId: 0, data: "42".toBytes)])
    doAssert vr.isOk

  let flushRes = ctfs.flush(execW)
  doAssert flushRes.isOk

  let ctfsBytes = ctfs.toBytes()
  let newFormatSize = ctfsBytes.len
  let oldFormatEstimate = totalSteps * 17  # ~17 bytes per step in old format

  let ratio = float(newFormatSize) / float(oldFormatEstimate)

  echo "{\"benchmark\": \"multi_stream_trace_size\"" &
    ", \"total_steps\": " & $totalSteps &
    ", \"new_format_bytes\": " & $newFormatSize &
    ", \"old_format_estimate_bytes\": " & $oldFormatEstimate &
    ", \"ratio\": " & $ratio & "}"

  ctfs.closeCtfs()
  echo "PASS: bench_multi_stream_trace_size"


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

test_multi_stream_writer_integration()
bench_multi_stream_write_throughput()
bench_multi_stream_trace_size()
echo "ALL PASS: test_multi_stream_integration"
