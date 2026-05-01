## Cross-format compatibility test (M42).
##
## Verifies producer-consumer compatibility: traces written by the
## MultiStreamTraceWriter (new format) can be read by the NewTraceReader,
## and traces written by the old TraceWriter can be read by the old
## TraceReader. Both produce valid output with matching content.
##
## This bridges M17 (multi-stream integration) and M21 (reader integration)
## by explicitly testing the cross-format contract.

import std/[options, os, times]
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
import codetracer_trace_reader
import codetracer_trace_writer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc readU32LE(data: openArray[byte], offset: int): uint32 =
  uint32(data[offset]) or
    (uint32(data[offset + 1]) shl 8) or
    (uint32(data[offset + 2]) shl 16) or
    (uint32(data[offset + 3]) shl 24)

proc toBytes(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

proc getTmpPath(name: string): string =
  try:
    getTempDir() / name
  except OSError:
    "/tmp" / name

proc cleanupFile(path: string) =
  try:
    removeFile(path)
  except OSError:
    discard

# ---------------------------------------------------------------------------
# Test 1: MultiStreamTraceWriter -> NewTraceReader roundtrip
# ---------------------------------------------------------------------------

proc test_new_format_write_read_roundtrip() =
  ## Write a trace with the multi-stream writer (new format) and verify
  ## it can be fully read back by the NewTraceReader with correct content.
  var ctfs = createCtfs()

  # meta.dat
  let metaFileRes = ctfs.addFile("meta.dat")
  doAssert metaFileRes.isOk, "addFile meta.dat failed"
  var metaFile = metaFileRes.get()

  let meta = TraceMetadata(
    program: "cross_test",
    args: @["--flag", "value"],
    workdir: "/tmp/cross"
  )
  let paths = @["/src/main.py", "/src/utils.py"]
  let metaWr = ctfs.writeMetaDat(metaFile, meta, paths, recorderId = "cross-format-test")
  doAssert metaWr.isOk, "writeMetaDat failed"

  # interning tables
  let tabRes = initTraceInterningTables(ctfs)
  doAssert tabRes.isOk, "initTraceInterningTables failed"
  var tab = tabRes.get()

  # Intern all entries
  let pathId0 = ctfs.ensurePathId(tab, "/src/main.py")
  let pathId1 = ctfs.ensurePathId(tab, "/src/utils.py")
  let funcId0 = ctfs.ensureFunctionId(tab, "main")
  let funcId1 = ctfs.ensureFunctionId(tab, "helper")
  let typeId0 = ctfs.ensureTypeId(tab, "int")
  let typeId1 = ctfs.ensureTypeId(tab, "str")
  let varId0 = ctfs.ensureVarnameId(tab, "x")
  let varId1 = ctfs.ensureVarnameId(tab, "msg")

  # streams
  let execRes = initExecStreamWriter(ctfs, chunkSize = 64)
  doAssert execRes.isOk, "initExecStreamWriter failed"
  var execW = execRes.get()

  let valRes = initValueStreamWriter(ctfs)
  doAssert valRes.isOk, "initValueStreamWriter failed"
  var valW = valRes.get()

  let callRes = initCallStreamWriter(ctfs)
  doAssert callRes.isOk, "initCallStreamWriter failed"
  var callW = callRes.get()

  let ioRes = initIOEventStreamWriter(ctfs)
  doAssert ioRes.isOk, "initIOEventStreamWriter failed"
  var ioW = ioRes.get()

  # Write 4 step events with values
  let wr0 = ctfs.writeEvent(execW, StepEvent(kind: sekAbsoluteStep, globalLineIndex: 1))
  doAssert wr0.isOk
  let v0 = ctfs.writeStepValues(valW, @[
    VariableValue(varnameId: 0, typeId: 0, data: "42".toBytes)])
  doAssert v0.isOk

  let wr1 = ctfs.writeEvent(execW, StepEvent(kind: sekDeltaStep, lineDelta: 1))
  doAssert wr1.isOk
  let v1 = ctfs.writeStepValues(valW, @[
    VariableValue(varnameId: 0, typeId: 0, data: "42".toBytes),
    VariableValue(varnameId: 1, typeId: 1, data: "hello".toBytes)])
  doAssert v1.isOk

  let wr2 = ctfs.writeEvent(execW, StepEvent(kind: sekDeltaStep, lineDelta: 2))
  doAssert wr2.isOk
  let v2 = ctfs.writeStepValues(valW, @[])
  doAssert v2.isOk

  let wr3 = ctfs.writeEvent(execW, StepEvent(kind: sekDeltaStep, lineDelta: -1))
  doAssert wr3.isOk
  let v3 = ctfs.writeStepValues(valW, @[
    VariableValue(varnameId: 0, typeId: 0, data: "99".toBytes)])
  doAssert v3.isOk

  # Write calls
  let cw0 = ctfs.writeCall(callW, call_stream.CallRecord(
    functionId: 0, parentCallKey: -1, entryStep: 0, exitStep: 3,
    depth: 0, args: @[], returnValue: @[VoidReturnMarker],
    exception: @[], children: @[1'u64]))
  doAssert cw0.isOk

  let cw1 = ctfs.writeCall(callW, call_stream.CallRecord(
    functionId: 1, parentCallKey: 0, entryStep: 1, exitStep: 2,
    depth: 1, args: @[CallArg(varnameId: 0, value: "42".toBytes)],
    returnValue: "hello".toBytes, exception: @[], children: @[]))
  doAssert cw1.isOk

  # Write IO event
  let ioWr = ctfs.writeEvent(ioW, IOEvent(
    kind: ioStdout, stepId: 2, data: "hello\n".toBytes))
  doAssert ioWr.isOk

  let flushRes = ctfs.flush(execW)
  doAssert flushRes.isOk

  let traceBytes = ctfs.toBytes()
  ctfs.closeCtfs()

  # Now read with NewTraceReader
  let readerRes = openNewTraceFromBytes(traceBytes)
  doAssert readerRes.isOk, "openNewTraceFromBytes failed: " & readerRes.error
  var reader = readerRes.get()

  # Verify metadata
  doAssert reader.meta.program == "cross_test",
    "program mismatch: " & reader.meta.program
  doAssert reader.meta.args == @["--flag", "value"],
    "args mismatch"
  doAssert reader.meta.workdir == "/tmp/cross",
    "workdir mismatch: " & reader.meta.workdir

  # Verify interning tables
  let p0 = reader.path(0)
  doAssert p0.isOk and p0.get() == "/src/main.py",
    "path 0 mismatch"
  let p1 = reader.path(1)
  doAssert p1.isOk and p1.get() == "/src/utils.py",
    "path 1 mismatch"

  let f0 = reader.function(0)
  doAssert f0.isOk and f0.get() == "main",
    "func 0 mismatch"
  let f1 = reader.function(1)
  doAssert f1.isOk and f1.get() == "helper",
    "func 1 mismatch"

  let t0 = reader.typeName(0)
  doAssert t0.isOk and t0.get() == "int",
    "type 0 mismatch"
  let t1 = reader.typeName(1)
  doAssert t1.isOk and t1.get() == "str",
    "type 1 mismatch"

  let vn0 = reader.varname(0)
  doAssert vn0.isOk and vn0.get() == "x",
    "varname 0 mismatch"
  let vn1 = reader.varname(1)
  doAssert vn1.isOk and vn1.get() == "msg",
    "varname 1 mismatch"

  # Verify step count
  let sc = reader.stepCount()
  doAssert sc.isOk and sc.get() == 4,
    "step count should be 4, got: " & (if sc.isOk: $sc.get() else: sc.error)

  # Verify step access
  let s0 = reader.step(0)
  doAssert s0.isOk, "step(0) failed: " & s0.error

  let s3 = reader.step(3)
  doAssert s3.isOk, "step(3) failed: " & s3.error

  # Verify values at step 0: 1 variable
  let vals0 = reader.values(0)
  doAssert vals0.isOk, "values(0) failed: " & vals0.error
  doAssert vals0.get().len == 1, "step 0 should have 1 value"

  # Verify values at step 1: 2 variables
  let vals1 = reader.values(1)
  doAssert vals1.isOk, "values(1) failed: " & vals1.error
  doAssert vals1.get().len == 2, "step 1 should have 2 values"

  # Verify call count
  let cc = reader.callCount()
  doAssert cc.isOk and cc.get() == 2,
    "call count should be 2"

  # Verify IO event count
  let ioCount = reader.ioEventCount()
  doAssert ioCount.isOk and ioCount.get() == 1,
    "IO event count should be 1"

  echo "PASS: test_new_format_write_read_roundtrip"


# ---------------------------------------------------------------------------
# Test 2: Old TraceWriter -> Old TraceReader roundtrip
# ---------------------------------------------------------------------------

proc test_old_format_write_read_roundtrip() =
  ## Write a trace with the old TraceWriter (split-binary .ct) and verify
  ## it can be fully read back by the old TraceReader with correct content.
  let path = getTmpPath("test_cross_format_old.ct")
  cleanupFile(path)

  # Write trace using old TraceWriter
  var writerRes = newTraceWriter(path, "old_test_prog", @["--arg1", "arg2"],
                                  workdir = "/tmp/old_test")
  doAssert writerRes.isOk, "newTraceWriter failed"
  var w = writerRes.get()

  doAssert w.writePath("/src/main.nim").isOk
  doAssert w.writePath("/src/helper.nim").isOk
  doAssert w.writeFunction(0, 1, "main").isOk
  doAssert w.writeFunction(1, 10, "helper").isOk
  doAssert w.writeStep(0, 5).isOk
  doAssert w.writeCall(0).isOk
  doAssert w.writeValue(0, ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(7))).isOk
  doAssert w.writeStep(1, 11).isOk
  doAssert w.writeCall(1).isOk
  doAssert w.writeValue(1, ValueRecord(kind: vrkString, text: "hello", strTypeId: TypeId(8))).isOk
  doAssert w.writeReturn().isOk
  doAssert w.writeStep(0, 6).isOk
  doAssert w.writeReturn().isOk
  doAssert w.close().isOk

  # Read trace using old TraceReader
  let readerRes = openTrace(path)
  doAssert readerRes.isOk, "openTrace failed"
  var reader = readerRes.get()

  # Verify metadata
  doAssert reader.metadata.program == "old_test_prog",
    "program mismatch: " & reader.metadata.program
  doAssert reader.metadata.args.len == 2
  doAssert reader.metadata.args[0] == "--arg1"
  doAssert reader.metadata.args[1] == "arg2"
  doAssert reader.metadata.workdir == "/tmp/old_test"

  # Verify paths
  doAssert reader.paths.len == 2
  doAssert reader.paths[0] == "/src/main.nim"
  doAssert reader.paths[1] == "/src/helper.nim"

  # Read events
  let readRes = reader.readEvents()
  doAssert readRes.isOk, "readEvents failed"

  # Verify event count: 2 paths + 2 functions + 3 steps + 2 calls + 2 values + 2 returns = 13
  doAssert reader.eventCount == 13,
    "event count mismatch: " & $reader.eventCount & " vs 13"

  # Verify specific event types
  doAssert reader.events[0].kind == tlePath
  doAssert reader.events[0].path == "/src/main.nim"

  doAssert reader.events[1].kind == tlePath
  doAssert reader.events[1].path == "/src/helper.nim"

  doAssert reader.events[2].kind == tleFunction
  doAssert reader.events[2].functionRecord.name == "main"

  doAssert reader.events[3].kind == tleFunction
  doAssert reader.events[3].functionRecord.name == "helper"

  doAssert reader.events[4].kind == tleStep
  doAssert reader.events[4].step.pathId == PathId(0)
  doAssert reader.events[4].step.line == Line(5)

  doAssert reader.events[5].kind == tleCall
  doAssert reader.events[5].callRecord.functionId == FunctionId(0)

  doAssert reader.events[6].kind == tleValue
  doAssert reader.events[6].fullValue.value.kind == vrkInt
  doAssert reader.events[6].fullValue.value.intVal == 42

  doAssert reader.events[7].kind == tleStep
  doAssert reader.events[7].step.pathId == PathId(1)
  doAssert reader.events[7].step.line == Line(11)

  doAssert reader.events[12].kind == tleReturn
  doAssert reader.events[12].returnRecord.returnValue.kind == vrkNone

  cleanupFile(path)
  echo "PASS: test_old_format_write_read_roundtrip"


# ---------------------------------------------------------------------------
# Test 3: Both formats produce structurally compatible output
# ---------------------------------------------------------------------------

proc test_both_formats_produce_valid_ctfs() =
  ## Verify that both formats produce valid CTFS containers with correct
  ## magic bytes, ensuring they are compatible with the Rust reader.
  let path = getTmpPath("test_cross_format_ctfs.ct")
  cleanupFile(path)

  # Old format: write and verify CTFS magic
  var writerRes = newTraceWriter(path, "compat_test", @[])
  doAssert writerRes.isOk
  var w = writerRes.get()
  doAssert w.writeStep(0, 1).isOk
  doAssert w.close().isOk

  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk
  let oldData = readRes.get()

  # Verify old format CTFS magic
  doAssert hasCtfsMagic(oldData), "old format missing CTFS magic"
  doAssert oldData[5] == 4'u8, "old format version mismatch"

  # New format: write and verify CTFS magic
  var ctfs = createCtfs()
  let metaFileRes = ctfs.addFile("meta.dat")
  doAssert metaFileRes.isOk
  var metaFile = metaFileRes.get()
  let meta = TraceMetadata(program: "compat_test", args: @[], workdir: "/tmp")
  let metaWr = ctfs.writeMetaDat(metaFile, meta, @["/src/test.py"])
  doAssert metaWr.isOk

  let tabRes = initTraceInterningTables(ctfs)
  doAssert tabRes.isOk
  var tab = tabRes.get()
  discard ctfs.ensurePathId(tab, "/src/test.py")

  let execRes = initExecStreamWriter(ctfs, chunkSize = 64)
  doAssert execRes.isOk
  var execW = execRes.get()

  let valRes = initValueStreamWriter(ctfs)
  doAssert valRes.isOk
  var valW = valRes.get()

  let wr0 = ctfs.writeEvent(execW, StepEvent(kind: sekAbsoluteStep, globalLineIndex: 1))
  doAssert wr0.isOk
  let v0 = ctfs.writeStepValues(valW, @[])
  doAssert v0.isOk
  let flushRes = ctfs.flush(execW)
  doAssert flushRes.isOk

  let newData = ctfs.toBytes()
  ctfs.closeCtfs()

  # Verify new format CTFS magic
  doAssert newData.len >= 12, "new format data too small"
  doAssert newData[0] == 0xC0'u8, "new format magic[0]"
  doAssert newData[1] == 0xDE'u8, "new format magic[1]"
  doAssert newData[2] == 0x72'u8, "new format magic[2]"
  doAssert newData[3] == 0xAC'u8, "new format magic[3]"
  doAssert newData[4] == 0xE2'u8, "new format magic[4]"
  doAssert newData[5] == 4'u8, "new format version mismatch"

  # Both use the same block size
  let oldBlockSize = readU32LE(oldData, 8)
  let newBlockSize = readU32LE(newData, 8)
  doAssert oldBlockSize == newBlockSize,
    "block size mismatch: old=" & $oldBlockSize & " new=" & $newBlockSize

  cleanupFile(path)
  echo "PASS: test_both_formats_produce_valid_ctfs"


# ---------------------------------------------------------------------------
# Test 4: Old format multi-chunk roundtrip
# ---------------------------------------------------------------------------

proc test_old_format_multi_chunk_roundtrip() =
  ## Verify that multi-chunk old format traces survive the write-read cycle.
  ## This tests that the split-binary encoder, Zstd compression, and chunk
  ## index all work together across producer and consumer.
  let path = getTmpPath("test_cross_format_chunks.ct")
  cleanupFile(path)

  # Small chunk threshold to force multiple chunks
  var writerRes = newTraceWriter(path, "chunk_test", @[],
                                  chunkThreshold = 10)
  doAssert writerRes.isOk
  var w = writerRes.get()

  # Write 100 step events
  for i in 0 ..< 100:
    doAssert w.writeStep(uint64(i mod 3), int64(i + 1)).isOk

  doAssert w.close().isOk

  # Read back
  let readerRes = openTrace(path)
  doAssert readerRes.isOk
  var reader = readerRes.get()
  let readRes = reader.readEvents()
  doAssert readRes.isOk

  doAssert reader.eventCount == 100,
    "multi-chunk event count mismatch: " & $reader.eventCount & " vs 100"

  # Verify all events survived the roundtrip
  for i in 0 ..< 100:
    doAssert reader.events[i].kind == tleStep,
      "event " & $i & " should be tleStep"
    doAssert reader.events[i].step.pathId == PathId(uint64(i mod 3)),
      "event " & $i & " pathId mismatch"
    doAssert reader.events[i].step.line == Line(int64(i + 1)),
      "event " & $i & " line mismatch"

  cleanupFile(path)
  echo "PASS: test_old_format_multi_chunk_roundtrip"


# ---------------------------------------------------------------------------
# Run all tests
# ---------------------------------------------------------------------------

test_new_format_write_read_roundtrip()
test_old_format_write_read_roundtrip()
test_both_formats_produce_valid_ctfs()
test_old_format_multi_chunk_roundtrip()
echo "ALL PASS: test_cross_format"
