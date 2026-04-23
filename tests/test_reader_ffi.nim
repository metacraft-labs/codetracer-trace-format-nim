## Tests for the reader C FFI exports.
## Writes a trace, saves to disk, then reads it back through the
## ct_reader_* functions defined in codetracer_trace_writer_ffi.nim.
##
## We include the FFI module and pop the raises pragma to allow
## test code to use string contains() etc.

# Include the FFI module first (it has {.push raises: [].})
include codetracer_trace_writer_ffi

# Pop the raises restriction so test code can use strutils.contains etc.
{.pop.}

import std/strutils

# We also need these imports that may not be pulled in by the FFI include
import codetracer_ctfs/container
import codetracer_trace_writer/interning_table
import codetracer_trace_writer/exec_stream

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc toBytes(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

proc readFfiString(buf: ptr uint8, length: csize_t): string =
  if buf.isNil or length == 0.csize_t:
    return ""
  result = newString(int(length))
  copyMem(addr result[0], buf, int(length))
  ct_free_buffer(buf)

proc ffiGetStr(h: pointer, id: uint64,
    getter: proc(h: pointer, id: uint64, outLen: ptr csize_t): ptr uint8 {.cdecl.}): string =
  var outLen: csize_t
  let buf = getter(h, id, addr outLen)
  readFfiString(buf, outLen)

proc ffiGetMeta(h: pointer,
    getter: proc(h: pointer, outLen: ptr csize_t): ptr uint8 {.cdecl.}): string =
  var outLen: csize_t
  let buf = getter(h, addr outLen)
  readFfiString(buf, outLen)

proc ffiGetJson(h: pointer, key: uint64,
    getter: proc(h: pointer, key: uint64, outLen: ptr csize_t): ptr uint8 {.cdecl.}): string =
  var outLen: csize_t
  let buf = getter(h, key, addr outLen)
  readFfiString(buf, outLen)

# ---------------------------------------------------------------------------
# Write a test trace to a temp file
# ---------------------------------------------------------------------------

proc writeTestTrace(path: string) =
  var ctfs = createCtfs()

  let metaFileRes = ctfs.addFile("meta.dat")
  doAssert metaFileRes.isOk
  var metaFile = metaFileRes.get()
  let meta = TraceMetadata(program: "test_ffi_prog", args: @["--test"], workdir: "/tmp/ffi")
  let metaWr = ctfs.writeMetaDat(metaFile, meta, @["/src/main.py", "/src/util.py"],
    recorderId = "ffi-test")
  doAssert metaWr.isOk

  let tabRes = initTraceInterningTables(ctfs)
  doAssert tabRes.isOk
  var tab = tabRes.get()

  discard ctfs.ensurePathId(tab, "/src/main.py")
  discard ctfs.ensurePathId(tab, "/src/util.py")
  discard ctfs.ensureFunctionId(tab, "main")
  discard ctfs.ensureFunctionId(tab, "helper")
  discard ctfs.ensureTypeId(tab, "int")
  discard ctfs.ensureTypeId(tab, "str")
  discard ctfs.ensureVarnameId(tab, "x")
  discard ctfs.ensureVarnameId(tab, "y")

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

  # Step 0: absolute step at line 1
  doAssert ctfs.writeEvent(execW, StepEvent(kind: sekAbsoluteStep, globalLineIndex: 1)).isOk
  doAssert ctfs.writeStepValues(valW, @[
    VariableValue(varnameId: 0, typeId: 0, data: "42".toBytes),
    VariableValue(varnameId: 1, typeId: 1, data: "hello".toBytes)]).isOk

  # Step 1: delta step +1
  doAssert ctfs.writeEvent(execW, StepEvent(kind: sekDeltaStep, lineDelta: 1)).isOk
  doAssert ctfs.writeStepValues(valW, @[
    VariableValue(varnameId: 0, typeId: 0, data: "43".toBytes)]).isOk

  # Step 2: delta step +2
  doAssert ctfs.writeEvent(execW, StepEvent(kind: sekDeltaStep, lineDelta: 2)).isOk
  doAssert ctfs.writeStepValues(valW, @[]).isOk

  # Call 0: main() covering steps 0-2
  doAssert ctfs.writeCall(callW, call_stream.CallRecord(
    functionId: 0, parentCallKey: -1, entryStep: 0, exitStep: 2,
    depth: 0, args: @[], returnValue: @[VoidReturnMarker],
    exception: @[], children: @[1'u64])).isOk

  # Call 1: helper() covering step 1
  doAssert ctfs.writeCall(callW, call_stream.CallRecord(
    functionId: 1, parentCallKey: 0, entryStep: 1, exitStep: 1,
    depth: 1, args: @["42".toBytes], returnValue: "43".toBytes,
    exception: @[], children: @[])).isOk

  # IO event
  doAssert ctfs.writeEvent(ioW, IOEvent(
    kind: ioStdout, stepId: 1, data: "output\n".toBytes)).isOk

  doAssert ctfs.flush(execW).isOk

  let ctfsBytes = ctfs.toBytes()
  ctfs.closeCtfs()

  var f = open(path, fmWrite)
  if ctfsBytes.len > 0:
    discard f.writeBuffer(unsafeAddr ctfsBytes[0], ctfsBytes.len)
  f.close()

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

proc test_reader_ffi_lifecycle() =
  let tmpPath = "/tmp/test_reader_ffi.ct"
  writeTestTrace(tmpPath)

  # NimMain already called by Nim's own startup for this executable

  let h = ct_reader_open(cstring(tmpPath))
  doAssert h != nil, "ct_reader_open failed: " & $trace_writer_last_error()

  # Metadata
  doAssert ffiGetMeta(h, ct_reader_program) == "test_ffi_prog"
  doAssert ffiGetMeta(h, ct_reader_workdir) == "/tmp/ffi"
  echo "PASS: metadata"

  # Interning counts
  doAssert ct_reader_path_count(h) == 2
  doAssert ct_reader_function_count(h) == 2
  doAssert ct_reader_type_count(h) == 2
  doAssert ct_reader_varname_count(h) == 2

  # Interning lookups
  doAssert ffiGetStr(h, 0, ct_reader_path) == "/src/main.py"
  doAssert ffiGetStr(h, 1, ct_reader_path) == "/src/util.py"
  doAssert ffiGetStr(h, 0, ct_reader_function) == "main"
  doAssert ffiGetStr(h, 1, ct_reader_function) == "helper"
  doAssert ffiGetStr(h, 0, ct_reader_type_name) == "int"
  doAssert ffiGetStr(h, 1, ct_reader_type_name) == "str"
  doAssert ffiGetStr(h, 0, ct_reader_varname) == "x"
  doAssert ffiGetStr(h, 1, ct_reader_varname) == "y"
  echo "PASS: interning"

  # Step count and access
  doAssert ct_reader_step_count(h) == 3, "step count: " & $ct_reader_step_count(h)

  let step0 = ffiGetJson(h, 0, ct_reader_step)
  doAssert step0.contains("absolute_step"), "step 0: " & step0
  doAssert step0.contains("\"global_line_index\":1"), "step 0 gli: " & step0

  let step1 = ffiGetJson(h, 1, ct_reader_step)
  doAssert step1.contains("delta_step"), "step 1: " & step1
  doAssert step1.contains("\"line_delta\":1"), "step 1: " & step1

  let step2 = ffiGetJson(h, 2, ct_reader_step)
  doAssert step2.contains("delta_step"), "step 2: " & step2
  doAssert step2.contains("\"line_delta\":2"), "step 2: " & step2
  echo "PASS: steps"

  # Values
  let vals0 = ffiGetJson(h, 0, ct_reader_values)
  doAssert vals0.count("varname_id") == 2, "vals0: " & vals0

  let vals1 = ffiGetJson(h, 1, ct_reader_values)
  doAssert vals1.count("varname_id") == 1, "vals1: " & vals1

  let vals2 = ffiGetJson(h, 2, ct_reader_values)
  doAssert vals2 == "[]", "vals2: " & vals2
  echo "PASS: values"

  # Calls
  doAssert ct_reader_call_count(h) == 2

  let call0 = ffiGetJson(h, 0, ct_reader_call)
  doAssert call0.contains("\"function_id\":0"), "call0: " & call0
  doAssert call0.contains("\"entry_step\":0"), "call0: " & call0
  doAssert call0.contains("\"exit_step\":2"), "call0: " & call0

  let call1 = ffiGetJson(h, 1, ct_reader_call)
  doAssert call1.contains("\"function_id\":1"), "call1: " & call1
  doAssert call1.contains("\"depth\":1"), "call1: " & call1
  echo "PASS: calls"

  # Call for step
  let cfs = ffiGetJson(h, 1, ct_reader_call_for_step)
  doAssert cfs.contains("\"function_id\":1"), "cfs: " & cfs
  echo "PASS: call_for_step"

  # IO events
  doAssert ct_reader_event_count(h) == 1

  let ev0 = ffiGetJson(h, 0, ct_reader_event)
  doAssert ev0.contains("\"kind\":\"stdout\""), "ev0: " & ev0
  doAssert ev0.contains("\"step_id\":1"), "ev0: " & ev0
  echo "PASS: io_events"

  # Print sample JSON for manual inspection
  echo "  step0 JSON: " & ffiGetJson(h, 0, ct_reader_step)
  echo "  vals0 JSON: " & ffiGetJson(h, 0, ct_reader_values)
  echo "  call0 JSON: " & ffiGetJson(h, 0, ct_reader_call)
  echo "  ev0   JSON: " & ffiGetJson(h, 0, ct_reader_event)

  ct_reader_close(h)
  removeFile(tmpPath)
  echo "PASS: test_reader_ffi_lifecycle (complete)"

proc test_reader_ffi_structured_accessors() =
  ## Test the new structured FFI functions (no JSON parsing).
  let tmpPath = "/tmp/test_reader_ffi_structured.ct"
  writeTestTrace(tmpPath)

  let h = ct_reader_open(cstring(tmpPath))
  doAssert h != nil, "ct_reader_open failed: " & $trace_writer_last_error()

  # -- Step locations --
  # The test trace writes:
  #   step 0: AbsoluteStep(globalLineIndex=1) -> path 0, line 1
  #   step 1: DeltaStep(+1) -> GLI=2 -> path 0, line 2
  #   step 2: DeltaStep(+2) -> GLI=4 -> path 0, line 4
  # With 2 paths and DefaultLinesPerFile=100_000, path 0 covers GLI [0, 99999].
  var pathId, line: uint64

  doAssert ct_reader_step_location(h, 0, addr pathId, addr line) == 0
  doAssert pathId == 0, "step 0 pathId: " & $pathId
  doAssert line == 1, "step 0 line: " & $line

  doAssert ct_reader_step_location(h, 1, addr pathId, addr line) == 0
  doAssert pathId == 0, "step 1 pathId: " & $pathId
  doAssert line == 2, "step 1 line: " & $line

  doAssert ct_reader_step_location(h, 2, addr pathId, addr line) == 0
  doAssert pathId == 0, "step 2 pathId: " & $pathId
  doAssert line == 4, "step 2 line: " & $line
  echo "PASS: step_location"

  # -- Step values (structured) --
  doAssert ct_reader_step_value_count(h, 0) == 2
  doAssert ct_reader_step_value_count(h, 1) == 1
  doAssert ct_reader_step_value_count(h, 2) == 0

  var varnameId, typeId: uint64
  var dataPtr: ptr uint8
  var dataLen: csize_t

  doAssert ct_reader_step_value(h, 0, 0, addr varnameId, addr typeId, addr dataPtr, addr dataLen) == 0
  doAssert varnameId == 0, "val 0,0 varnameId: " & $varnameId
  doAssert typeId == 0, "val 0,0 typeId: " & $typeId
  doAssert dataLen == 2.csize_t, "val 0,0 dataLen: " & $dataLen  # "42"
  if not dataPtr.isNil:
    ct_free_buffer(dataPtr)

  doAssert ct_reader_step_value(h, 0, 1, addr varnameId, addr typeId, addr dataPtr, addr dataLen) == 0
  doAssert varnameId == 1, "val 0,1 varnameId: " & $varnameId
  doAssert typeId == 1, "val 0,1 typeId: " & $typeId
  doAssert dataLen == 5.csize_t, "val 0,1 dataLen: " & $dataLen  # "hello"
  if not dataPtr.isNil:
    ct_free_buffer(dataPtr)

  # Out of range should fail
  doAssert ct_reader_step_value(h, 0, 99, addr varnameId, addr typeId, addr dataPtr, addr dataLen) != 0
  echo "PASS: step_value"

  # -- Call fields (structured) --
  var functionId: uint64
  var parentKey: int64
  var entryStep, exitStep: uint64
  var depth: uint32
  var childrenCount: uint64

  doAssert ct_reader_call_fields(h, 0, addr functionId, addr parentKey,
    addr entryStep, addr exitStep, addr depth, addr childrenCount) == 0
  doAssert functionId == 0, "call 0 functionId: " & $functionId
  doAssert parentKey == -1, "call 0 parentKey: " & $parentKey
  doAssert entryStep == 0, "call 0 entryStep: " & $entryStep
  doAssert exitStep == 2, "call 0 exitStep: " & $exitStep
  doAssert depth == 0, "call 0 depth: " & $depth
  doAssert childrenCount == 1, "call 0 childrenCount: " & $childrenCount

  # Call child
  doAssert ct_reader_call_child(h, 0, 0) == 1

  doAssert ct_reader_call_fields(h, 1, addr functionId, addr parentKey,
    addr entryStep, addr exitStep, addr depth, addr childrenCount) == 0
  doAssert functionId == 1, "call 1 functionId: " & $functionId
  doAssert parentKey == 0, "call 1 parentKey: " & $parentKey
  doAssert depth == 1, "call 1 depth: " & $depth
  doAssert childrenCount == 0, "call 1 childrenCount: " & $childrenCount
  echo "PASS: call_fields"

  # -- Event fields (structured) --
  var kind: uint8
  var stepId: uint64

  doAssert ct_reader_event_fields(h, 0, addr kind, addr stepId,
    addr dataPtr, addr dataLen) == 0
  doAssert kind == 0, "event 0 kind: " & $kind  # ioStdout = 0
  doAssert stepId == 1, "event 0 stepId: " & $stepId
  doAssert dataLen == 7.csize_t, "event 0 dataLen: " & $dataLen  # "output\n"
  if not dataPtr.isNil:
    ct_free_buffer(dataPtr)
  echo "PASS: event_fields"

  ct_reader_close(h)
  removeFile(tmpPath)
  echo "PASS: test_reader_ffi_structured_accessors (complete)"

proc test_reader_ffi_null_safety() =
  # NimMain already called by Nim's own startup for this executable

  doAssert ct_reader_open(nil) == nil
  doAssert ct_reader_open(cstring("/tmp/nonexistent.ct")) == nil
  doAssert ct_reader_step_count(nil) == 0
  doAssert ct_reader_call_count(nil) == 0
  doAssert ct_reader_event_count(nil) == 0
  doAssert ct_reader_path_count(nil) == 0

  var outLen: csize_t
  doAssert ct_reader_step(nil, 0, addr outLen) == nil
  doAssert ct_reader_values(nil, 0, addr outLen) == nil
  doAssert ct_reader_call(nil, 0, addr outLen) == nil
  doAssert ct_reader_event(nil, 0, addr outLen) == nil
  doAssert ct_reader_program(nil, addr outLen) == nil

  # Structured accessors null safety
  var pathId, lineVal: uint64
  doAssert ct_reader_step_location(nil, 0, addr pathId, addr lineVal) != 0
  doAssert ct_reader_step_value_count(nil, 0) == 0

  var vn, ti: uint64
  var dp: ptr uint8
  var dl: csize_t
  doAssert ct_reader_step_value(nil, 0, 0, addr vn, addr ti, addr dp, addr dl) != 0

  var fi: uint64
  var pk: int64
  var es, xs: uint64
  var d: uint32
  var cc: uint64
  doAssert ct_reader_call_fields(nil, 0, addr fi, addr pk, addr es, addr xs, addr d, addr cc) != 0
  doAssert ct_reader_call_child(nil, 0, 0) == high(uint64)

  var k: uint8
  var si: uint64
  doAssert ct_reader_event_fields(nil, 0, addr k, addr si, addr dp, addr dl) != 0

  ct_reader_close(nil)
  echo "PASS: test_reader_ffi_null_safety"

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

test_reader_ffi_lifecycle()
test_reader_ffi_structured_accessors()
test_reader_ffi_null_safety()
echo "ALL PASS: test_reader_ffi"
