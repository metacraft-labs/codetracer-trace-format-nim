## Tests for the high-level TraceReader API.

import std/[os, json, strutils]
import results
import codetracer_trace_reader
import codetracer_trace_writer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

template checkOk(res: untyped, msg: string = "") =
  ## Assert a Result is Ok, printing the error if not.
  if res.isErr:
    doAssert false, msg & " error: " & res.unsafeError

proc getTmpPath(name: string): string =
  getTempDir() / name

proc cleanupFile(path: string) =
  try:
    removeFile(path)
  except OSError:
    discard

# ---------------------------------------------------------------------------
# test_reader_basic
# ---------------------------------------------------------------------------

proc test_reader_basic() =
  let path = getTmpPath("test_reader_basic.ct")
  cleanupFile(path)

  # Write a trace
  var writerRes = newTraceWriter(path, "test_prog", @["arg1", "arg2"],
                                  workdir = "/tmp/work")
  checkOk(writerRes, "newTraceWriter failed")
  var w = writerRes.get()

  doAssert w.writePath("/src/main.nim").isOk
  doAssert w.writeStep(0, 10).isOk
  doAssert w.writeFunction(0, 5, "main").isOk
  doAssert w.writeCall(0).isOk
  doAssert w.writeValue(1, ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(7))).isOk
  doAssert w.writeReturn().isOk
  doAssert w.close().isOk

  # Read it back
  let readerRes = openTrace(path)
  checkOk(readerRes, "openTrace failed")
  var reader = readerRes.get()

  # Verify metadata
  doAssert reader.metadata.program == "test_prog",
    "program mismatch: " & reader.metadata.program
  doAssert reader.metadata.args.len == 2
  doAssert reader.metadata.args[0] == "arg1"
  doAssert reader.metadata.args[1] == "arg2"
  doAssert reader.metadata.workdir == "/tmp/work"

  # Verify paths
  doAssert reader.paths.len == 1
  doAssert reader.paths[0] == "/src/main.nim"

  # Read events
  let readRes = reader.readEvents()
  checkOk(readRes, "readEvents failed")

  # Verify events
  doAssert reader.eventCount == 6, "event count mismatch: " & $reader.eventCount

  doAssert reader.events[0].kind == tlePath
  doAssert reader.events[0].path == "/src/main.nim"

  doAssert reader.events[1].kind == tleStep
  doAssert reader.events[1].step.pathId == PathId(0)
  doAssert reader.events[1].step.line == Line(10)

  doAssert reader.events[2].kind == tleFunction
  doAssert reader.events[2].functionRecord.name == "main"
  doAssert reader.events[2].functionRecord.pathId == PathId(0)
  doAssert reader.events[2].functionRecord.line == Line(5)

  doAssert reader.events[3].kind == tleCall
  doAssert reader.events[3].callRecord.functionId == FunctionId(0)

  doAssert reader.events[4].kind == tleValue
  doAssert reader.events[4].fullValue.variableId == VariableId(1)
  doAssert reader.events[4].fullValue.value.kind == vrkInt
  doAssert reader.events[4].fullValue.value.intVal == 42

  doAssert reader.events[5].kind == tleReturn
  doAssert reader.events[5].returnRecord.returnValue.kind == vrkNone

  cleanupFile(path)
  echo "PASS: test_reader_basic"

# ---------------------------------------------------------------------------
# test_reader_multi_chunk
# ---------------------------------------------------------------------------

proc test_reader_multi_chunk() =
  let path = getTmpPath("test_reader_multi_chunk.ct")
  cleanupFile(path)

  # Use small chunk threshold to force multiple chunks
  let chunkThreshold = 10
  var writerRes = newTraceWriter(path, "chunked", @[],
                                  chunkThreshold = chunkThreshold)
  doAssert writerRes.isOk
  var w = writerRes.get()

  # Write 55 events across multiple chunks
  var expectedEvents: seq[TraceLowLevelEvent]
  for i in 0 ..< 55:
    let event = TraceLowLevelEvent(kind: tleStep,
      step: StepRecord(pathId: PathId(uint64(i mod 3)), line: Line(int64(i + 1))))
    expectedEvents.add(event)
    doAssert w.writeEvent(event).isOk

  doAssert w.close().isOk

  # Read back
  let readerRes = openTrace(path)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  let readRes = reader.readEvents()
  checkOk(readRes, "readEvents failed")

  doAssert reader.eventCount == 55,
    "event count mismatch: " & $reader.eventCount & " vs 55"

  for i in 0 ..< 55:
    doAssert reader.events[i] == expectedEvents[i],
      "event mismatch at index " & $i

  cleanupFile(path)
  echo "PASS: test_reader_multi_chunk"

# ---------------------------------------------------------------------------
# test_reader_json_output
# ---------------------------------------------------------------------------

proc test_reader_json_output() =
  let path = getTmpPath("test_reader_json.ct")
  cleanupFile(path)

  var writerRes = newTraceWriter(path, "json_test", @["--flag"],
                                  workdir = "/home")
  doAssert writerRes.isOk
  var w = writerRes.get()

  doAssert w.writePath("/test.py").isOk
  doAssert w.writeStep(0, 1).isOk
  doAssert w.writeValue(0, ValueRecord(kind: vrkInt, intVal: 99, intTypeId: TypeId(1))).isOk
  doAssert w.close().isOk

  let readerRes = openTrace(path)
  doAssert readerRes.isOk
  var reader = readerRes.get()
  doAssert reader.readEvents().isOk

  # Test full JSON
  let jsonStr = reader.toJson()
  try:
    let node = parseJson(jsonStr)
    doAssert node.hasKey("metadata")
    doAssert node.hasKey("paths")
    doAssert node.hasKey("events")
    doAssert node["metadata"]["program"].getStr() == "json_test"
    doAssert node["metadata"]["args"][0].getStr() == "--flag"
    doAssert node["metadata"]["workdir"].getStr() == "/home"
    doAssert node["paths"][0].getStr() == "/test.py"
    doAssert node["events"].len == 3
    doAssert node["events"][0]["type"].getStr() == "Path"
    doAssert node["events"][1]["type"].getStr() == "Step"
    doAssert node["events"][2]["type"].getStr() == "Value"
    doAssert node["events"][2]["value"]["kind"].getStr() == "Int"
    doAssert node["events"][2]["value"]["i"].getInt() == 99
  except JsonParsingError:
    doAssert false, "toJson output is not valid JSON"
  except KeyError:
    doAssert false, "JSON missing expected key"

  # Test JSON events only
  let eventsJson = reader.toJsonEvents()
  try:
    let arr = parseJson(eventsJson)
    doAssert arr.kind == JArray
    doAssert arr.len == 3
  except JsonParsingError:
    doAssert false, "toJsonEvents output is not valid JSON"

  cleanupFile(path)
  echo "PASS: test_reader_json_output"

# ---------------------------------------------------------------------------
# test_reader_text_output
# ---------------------------------------------------------------------------

proc test_reader_text_output() =
  let path = getTmpPath("test_reader_text.ct")
  cleanupFile(path)

  var writerRes = newTraceWriter(path, "text_test", @[],
                                  workdir = "/workspace")
  doAssert writerRes.isOk
  var w = writerRes.get()

  doAssert w.writePath("/src/app.nim").isOk
  doAssert w.writeStep(0, 42).isOk
  doAssert w.writeFunction(0, 1, "hello").isOk
  doAssert w.close().isOk

  let readerRes = openTrace(path)
  doAssert readerRes.isOk
  var reader = readerRes.get()
  doAssert reader.readEvents().isOk

  let text = reader.toPrettyText()
  doAssert "=== Trace ===" in text
  doAssert "program: text_test" in text
  doAssert "workdir: /workspace" in text
  doAssert "Path" in text
  doAssert "Step" in text
  doAssert "path_id=0" in text
  doAssert "line=42" in text
  doAssert "Function" in text
  doAssert "hello" in text

  cleanupFile(path)
  echo "PASS: test_reader_text_output"

# ---------------------------------------------------------------------------
# test_reader_summary
# ---------------------------------------------------------------------------

proc test_reader_summary() =
  let path = getTmpPath("test_reader_summary.ct")
  cleanupFile(path)

  var writerRes = newTraceWriter(path, "summary_test", @["a", "b"])
  doAssert writerRes.isOk
  var w = writerRes.get()

  doAssert w.writePath("/p.nim").isOk
  doAssert w.writeStep(0, 1).isOk
  doAssert w.writeStep(0, 2).isOk
  doAssert w.writeStep(0, 3).isOk
  doAssert w.writeFunction(0, 1, "f").isOk
  doAssert w.writeCall(0).isOk
  doAssert w.writeReturn().isOk
  doAssert w.close().isOk

  let readerRes = openTrace(path)
  doAssert readerRes.isOk
  var reader = readerRes.get()
  doAssert reader.readEvents().isOk

  let summary = reader.toSummary()
  doAssert "program: summary_test" in summary
  doAssert "events: 7" in summary
  doAssert "steps: 3" in summary
  doAssert "paths: 1" in summary
  doAssert "functions: 1" in summary
  doAssert "calls: 1" in summary
  doAssert "returns: 1" in summary

  cleanupFile(path)
  echo "PASS: test_reader_summary"

# ---------------------------------------------------------------------------
# test_reader_roundtrip_all_event_types
# ---------------------------------------------------------------------------

proc test_reader_roundtrip_all_event_types() =
  let path = getTmpPath("test_reader_roundtrip_all.ct")
  cleanupFile(path)

  var writerRes = newTraceWriter(path, "roundtrip", @[],
                                  chunkThreshold = 100)
  doAssert writerRes.isOk
  var w = writerRes.get()

  var originalEvents: seq[TraceLowLevelEvent]

  # Step
  originalEvents.add(TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(0), line: Line(1))))
  # Path
  originalEvents.add(TraceLowLevelEvent(kind: tlePath, path: "/test.nim"))
  # VariableName
  originalEvents.add(TraceLowLevelEvent(kind: tleVariableName, varName: "x"))
  # Variable
  originalEvents.add(TraceLowLevelEvent(kind: tleVariable, variable: "myVar"))
  # Function
  originalEvents.add(TraceLowLevelEvent(kind: tleFunction,
    functionRecord: FunctionRecord(pathId: PathId(0), line: Line(10), name: "doWork")))
  # Call
  originalEvents.add(TraceLowLevelEvent(kind: tleCall,
    callRecord: codetracer_trace_types.CallRecord(functionId: FunctionId(1), args: @[])))
  # Value (Int)
  originalEvents.add(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(0),
      value: ValueRecord(kind: vrkInt, intVal: -7, intTypeId: TypeId(2)))))
  # Value (String)
  originalEvents.add(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(1),
      value: ValueRecord(kind: vrkString, text: "hello", strTypeId: TypeId(9)))))
  # Return
  originalEvents.add(TraceLowLevelEvent(kind: tleReturn,
    returnRecord: ReturnRecord(returnValue: ValueRecord(
      kind: vrkInt, intVal: 0, intTypeId: TypeId(7)))))
  # BindVariable
  originalEvents.add(TraceLowLevelEvent(kind: tleBindVariable,
    bindVar: BindVariableRecord(variableId: VariableId(5), place: Place(3))))
  # DropVariable
  originalEvents.add(TraceLowLevelEvent(kind: tleDropVariable,
    dropVarId: VariableId(5)))
  # ThreadStart
  originalEvents.add(TraceLowLevelEvent(kind: tleThreadStart,
    threadStartId: ThreadId(1)))
  # ThreadSwitch
  originalEvents.add(TraceLowLevelEvent(kind: tleThreadSwitch,
    threadSwitchId: ThreadId(1)))
  # ThreadExit
  originalEvents.add(TraceLowLevelEvent(kind: tleThreadExit,
    threadExitId: ThreadId(1)))
  # DropLastStep
  originalEvents.add(TraceLowLevelEvent(kind: tleDropLastStep))

  for event in originalEvents:
    doAssert w.writeEvent(event).isOk

  doAssert w.close().isOk

  # Read back
  let readerRes = openTrace(path)
  doAssert readerRes.isOk
  var reader = readerRes.get()
  doAssert reader.readEvents().isOk

  doAssert reader.eventCount == originalEvents.len,
    "event count mismatch: " & $reader.eventCount & " vs " & $originalEvents.len

  for i in 0 ..< originalEvents.len:
    doAssert reader.events[i] == originalEvents[i],
      "event mismatch at index " & $i & ": got kind " & $reader.events[i].kind &
      " expected " & $originalEvents[i].kind

  cleanupFile(path)
  echo "PASS: test_reader_roundtrip_all_event_types"

# ---------------------------------------------------------------------------
# test_reader_error_handling
# ---------------------------------------------------------------------------

proc test_reader_error_handling() =
  # Non-existent file
  let res1 = openTrace("/nonexistent/path/file.ct")
  doAssert res1.isErr, "should fail on non-existent file"

  # Create a file with garbage content
  let path = getTmpPath("test_reader_bad.ct")
  try:
    writeFile(path, "not a ctfs file")
  except IOError, OSError:
    discard

  let res2 = openTrace(path)
  doAssert res2.isErr, "should fail on non-CTFS file"

  cleanupFile(path)
  echo "PASS: test_reader_error_handling"

# ---------------------------------------------------------------------------
# Run all tests
# ---------------------------------------------------------------------------

test_reader_basic()
test_reader_multi_chunk()
test_reader_json_output()
test_reader_text_output()
test_reader_summary()
test_reader_roundtrip_all_event_types()
test_reader_error_handling()
echo "ALL PASS: test_trace_reader"
