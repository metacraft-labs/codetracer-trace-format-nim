## Generate a .ct fixture file for the codetracer-trace-format-spec repo.
##
## A minimal but representative split-stream recording, written by the
## split-stream writer:
##   - 2 source paths
##   - 2 types (`int`, `string`)
##   - 1 function (`main`) and one call of it
##   - 4 steps, two of them carrying a value (`x = 42`, `msg = "hello"`)
##   - the call's return
##
## The recording id is fixed so regenerating the fixture reproduces it byte
## for byte.
##
## Run:  nim c -r -p:src tests/generate_spec_fixture.nim <output-path>

import std/os
import results
import codetracer_trace_types
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/cbor

const FixtureRecordingId = "0192f8a0-0000-7000-8000-000000000001"

proc cborOf(v: ValueRecord): seq[byte] =
  var enc = CborEncoder.init()
  enc.encodeCborValueRecord(v)
  enc.getBytes()

proc main() =
  let outputPath =
    if paramCount() >= 1: paramStr(1)
    else: getTempDir() / "spec_fixture.ct"
  removeFile(outputPath)

  var w = initMultiStreamWriter(outputPath, "factorial",
    recordingId = FixtureRecordingId).get()
  w.metadata.args = @["5"]
  w.metadata.workdir = "/home/user/demo"

  let mainNim = w.registerPath("/src/main.nim").get()
  let mathUtils = w.registerPath("/src/math_utils.nim").get()
  let intT = w.registerType("int", uint8(ord(tkInt))).get()
  let strT = w.registerType("string", uint8(ord(tkString))).get()
  let mainFn = w.registerFunctionAt("/src/main.nim", 1, "main").get()
  let x = w.registerVarname("x").get()
  let msg = w.registerVarname("msg").get()

  doAssert w.registerCall(mainFn, []).isOk
  doAssert w.registerStep(mainNim, 1, []).isOk
  doAssert w.registerStep(mainNim, 3, []).isOk
  doAssert w.registerStep(mainNim, 4, [VariableValue(varnameId: x,
    data: cborOf(ValueRecord(kind: vrkInt, intVal: 42,
      intTypeId: TypeId(intT))))]).isOk
  doAssert w.registerStep(mathUtils, 10, [VariableValue(varnameId: msg,
    data: cborOf(ValueRecord(kind: vrkString, text: "hello",
      strTypeId: TypeId(strT))))]).isOk
  doAssert w.registerReturn().isOk

  doAssert w.close().isOk
  doAssert w.closeCtfs().isOk
  echo "Fixture written to: ", outputPath

main()
