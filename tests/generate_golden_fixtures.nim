## Regenerate .expected text files for golden fixture tests.
##
## Run this when the format changes intentionally:
##   nim c -r tests/generate_golden_fixtures.nim

import std/os
import std/json
import results
import codetracer_ctfs
import codetracer_trace_writer
import ct_pretty_print

const FixtureDir = currentSourcePath().parentDir / "fixtures"

# ---------------------------------------------------------------------------
# Generators (identical to test_golden_fixtures.nim)
# ---------------------------------------------------------------------------

proc generateSplitBinaryEvents(): seq[byte] =
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
  let frameSize = 100
  var enc = newSeekableZstdEncoder(frameThreshold = frameSize)
  var data = newSeq[byte](300)
  for i in 0 ..< 300:
    data[i] = byte(i mod 256)
  enc.write(data)
  enc.finish()

proc generateTraceComplete(): seq[byte] =
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
# Main: generate all .expected files
# ---------------------------------------------------------------------------

proc main() =
  createDir(FixtureDir)

  block:
    let data = generateSplitBinaryEvents()
    let text = prettyPrintEvents(data)
    let path = FixtureDir / "split_binary_events.expected"
    writeFile(path, text)
    echo "Generated: ", path

  block:
    let data = generateCtfsBasic()
    let text = prettyPrintCtFile(data)
    let path = FixtureDir / "ctfs_basic.expected"
    writeFile(path, text)
    echo "Generated: ", path

  block:
    let data = generateSeekableZstd3Frames()
    let text = prettyPrintSeekableZstd(data)
    let path = FixtureDir / "seekable_zstd_3frames.expected"
    writeFile(path, text)
    echo "Generated: ", path

  block:
    let data = generateTraceComplete()
    let text = prettyPrintCtFile(data)
    let path = FixtureDir / "trace_complete.expected"
    writeFile(path, text)
    echo "Generated: ", path

  echo "All .expected files regenerated."

main()
