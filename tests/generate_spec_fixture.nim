## Generate a .ct fixture file for the codetracer-trace-format-spec repo.
##
## Produces a minimal but representative trace with known events:
##   - 2 Path events (register source files)
##   - 1 Function event (define "main")
##   - 4 Step events (walk through lines)
##   - 1 Call event (enter "main")
##   - 2 Value events (an int and a string)
##   - 1 Return event
##
## Run:  nim c -r tests/generate_spec_fixture.nim <output-path>

import std/os
import results
import codetracer_trace_writer
import codetracer_trace_types

proc main() =
  let outputPath =
    if paramCount() >= 1: paramStr(1)
    else: getTempDir() / "spec_fixture.ct"

  var writerRes = newTraceWriter(outputPath, "factorial", @["5"],
                                  workdir = "/home/user/demo",
                                  chunkThreshold = 64)
  doAssert writerRes.isOk, "newTraceWriter failed: " & writerRes.error
  var w = writerRes.get()

  # Register source files (Path events)
  doAssert w.writePath("/src/main.nim").isOk       # pathId 0
  doAssert w.writePath("/src/math_utils.nim").isOk # pathId 1

  # Define a function
  doAssert w.writeFunction(0, 1, "main").isOk      # functionId 0

  # Step into main at line 1
  doAssert w.writeStep(0, 1).isOk

  # Call main
  doAssert w.writeCall(0).isOk

  # Step through some lines
  doAssert w.writeStep(0, 3).isOk
  doAssert w.writeStep(0, 4).isOk

  # Record an integer value: x = 42
  doAssert w.writeValue(1, ValueRecord(
    kind: vrkInt, intVal: 42, intTypeId: TypeId(7))).isOk

  # Step to another line
  doAssert w.writeStep(1, 10).isOk

  # Record a string value: msg = "hello"
  doAssert w.writeValue(2, ValueRecord(
    kind: vrkString, text: "hello", strTypeId: TypeId(9))).isOk

  # Return from main with int value 0
  doAssert w.writeReturn().isOk

  doAssert w.close().isOk
  echo "Fixture written to: ", outputPath

main()
