## Golden fixture tests — text-based comparison.
##
## Each test:
## 1. Generates a .ct or binary file from known inputs
## 2. Pretty-prints it to deterministic text
## 3. Compares against a committed .expected text file
##
## If the format changes intentionally, run `generate_golden_fixtures`
## to regenerate the .expected files.

import std/os
import std/strutils
import std/json
import results
import stew/endians2
import codetracer_ctfs
import codetracer_trace_writer
import ct_pretty_print

const FixtureDir = currentSourcePath().parentDir / "fixtures"

# ---------------------------------------------------------------------------
# Diff helper
# ---------------------------------------------------------------------------

proc diffStrings(actual, expected: string): string =
  ## Simple line-by-line diff for error reporting.
  let actualLines = actual.splitLines()
  let expectedLines = expected.splitLines()
  var diffs: seq[string]
  let maxLines = max(actualLines.len, expectedLines.len)
  for i in 0 ..< maxLines:
    let a = if i < actualLines.len: actualLines[i] else: "<missing>"
    let e = if i < expectedLines.len: expectedLines[i] else: "<missing>"
    if a != e:
      diffs.add("  line " & $(i + 1) & ":")
      diffs.add("    expected: " & e)
      diffs.add("    actual:   " & a)
      if diffs.len > 30:
        diffs.add("  ... (truncated)")
        break
  diffs.join("\n")

# ---------------------------------------------------------------------------
# Generators (same inputs as the old generate_golden_fixtures.nim)
# ---------------------------------------------------------------------------

proc generateSplitBinaryEvents(): seq[byte] =
  ## 10 known events encoded in split-binary format (uncompressed).
  var enc = SplitBinaryEncoder.init()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(0), line: Line(1))))
  enc.encodeEvent(TraceLowLevelEvent(kind: tlePath,
    path: "/src/main.nim"))
  enc.encodeEvent(TraceLowLevelEvent(kind: tleFunction,
    functionRecord: FunctionRecord(pathId: PathId(0), line: Line(1), name: "main")))
  enc.encodeEvent(TraceLowLevelEvent(kind: tleCall,
    callRecord: CallRecord(functionId: FunctionId(0), args: @[])))
  enc.encodeEvent(TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(0), line: Line(5))))
  enc.encodeEvent(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(1),
      value: ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(7)))))
  enc.encodeEvent(TraceLowLevelEvent(kind: tleReturn,
    returnRecord: ReturnRecord(
      returnValue: ValueRecord(kind: vrkInt, intVal: 0, intTypeId: TypeId(7)))))
  enc.encodeEvent(TraceLowLevelEvent(kind: tleDropLastStep))
  enc.encodeEvent(TraceLowLevelEvent(kind: tleThreadStart,
    threadStartId: ThreadId(1)))
  enc.encodeEvent(TraceLowLevelEvent(kind: tleEvent,
    recordEvent: RecordEvent(kind: elkWrite, metadata: "stdout", content: "hello\n")))
  enc.getBytes()

proc generateCtfsBasic(): seq[byte] =
  ## CTFS container with 2 internal files: "hello.txt" and "data.bin"
  var c = createCtfs()
  let f1Res = c.addFile("hello.txt")
  doAssert f1Res.isOk
  var f1 = f1Res.get()
  let helloData = cast[seq[byte]]("Hello, CTFS!")
  doAssert c.writeToFile(f1, helloData).isOk

  let f2Res = c.addFile("data.bin")
  doAssert f2Res.isOk
  var f2 = f2Res.get()
  var binData = newSeq[byte](256)
  for i in 0 ..< 256:
    binData[i] = byte(i)
  doAssert c.writeToFile(f2, binData).isOk

  c.closeCtfs()
  c.toBytes()

proc generateSeekableZstd3Frames(): seq[byte] =
  ## 3 frames of known data with small threshold.
  let frameSize = 100
  var enc = newSeekableZstdEncoder(frameThreshold = frameSize)
  var data = newSeq[byte](300)
  for i in 0 ..< 300:
    data[i] = byte(i mod 256)
  enc.write(data)
  enc.finish()

proc generateTraceComplete(): seq[byte] =
  ## Full trace writer output with known events.
  let path = getTempDir() / "golden_regen_trace_complete.ct"
  try:
    removeFile(path)
  except OSError:
    discard

  var writerRes = newTraceWriter(path, "test_program", @["--flag", "input.txt"],
                                  workdir = "/home/user/project",
                                  chunkThreshold = 5)
  doAssert writerRes.isOk, "newTraceWriter failed: " & writerRes.error
  var w = writerRes.get()

  doAssert w.writePath("/src/main.nim").isOk
  doAssert w.writePath("/src/lib.nim").isOk
  doAssert w.writeFunction(0, 1, "main").isOk
  doAssert w.writeStep(0, 1).isOk
  doAssert w.writeCall(0).isOk
  doAssert w.writeStep(0, 5).isOk
  doAssert w.writeStep(0, 6).isOk
  doAssert w.writeStep(0, 7).isOk
  doAssert w.writeStep(1, 10).isOk
  doAssert w.writeReturn().isOk
  doAssert w.writeStep(0, 8).isOk
  doAssert w.writeStep(0, 9).isOk

  doAssert w.close().isOk

  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk, "readCtfsFromFile failed: " & readRes.error
  result = readRes.get()

  try:
    removeFile(path)
  except OSError:
    discard

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

proc test_split_binary_events() =
  let data = generateSplitBinaryEvents()
  let actual = prettyPrintEvents(data)
  let expectedPath = FixtureDir / "split_binary_events.expected"
  let expected = readFile(expectedPath)
  doAssert actual == expected,
    "Split-binary events output differs from expected:\n" & diffStrings(actual, expected)
  echo "PASS: test_split_binary_events"

proc test_ctfs_basic() =
  let data = generateCtfsBasic()
  let actual = prettyPrintCtFile(data)
  let expectedPath = FixtureDir / "ctfs_basic.expected"
  let expected = readFile(expectedPath)
  doAssert actual == expected,
    "CTFS basic output differs from expected:\n" & diffStrings(actual, expected)
  echo "PASS: test_ctfs_basic"

proc test_seekable_zstd() =
  let data = generateSeekableZstd3Frames()
  let actual = prettyPrintSeekableZstd(data)
  let expectedPath = FixtureDir / "seekable_zstd_3frames.expected"
  let expected = readFile(expectedPath)
  doAssert actual == expected,
    "Seekable Zstd output differs from expected:\n" & diffStrings(actual, expected)
  echo "PASS: test_seekable_zstd"

proc test_trace_complete() =
  let data = generateTraceComplete()
  let actual = prettyPrintCtFile(data)
  let expectedPath = FixtureDir / "trace_complete.expected"
  let expected = readFile(expectedPath)
  doAssert actual == expected,
    "Trace complete output differs from expected:\n" & diffStrings(actual, expected)
  echo "PASS: test_trace_complete"

# ---------------------------------------------------------------------------
# Run all tests
# ---------------------------------------------------------------------------

test_split_binary_events()
test_ctfs_basic()
test_seekable_zstd()
test_trace_complete()
echo "ALL PASS: test_golden_fixtures"
