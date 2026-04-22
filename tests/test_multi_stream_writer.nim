{.push raises: [].}

## Tests for MultiStreamTraceWriter (M25).
##
## Creates a trace using the high-level MultiStreamTraceWriter API,
## then reads it back with NewTraceReader and verifies all data.

import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/call_stream
import codetracer_trace_writer/io_event_stream
import codetracer_trace_writer/value_stream

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc toBytes(s: string): seq[byte] {.raises: [].} =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

# ---------------------------------------------------------------------------
# Main test: write 100 steps, 10 calls, 5 IO events
# ---------------------------------------------------------------------------

proc test_basic_round_trip() {.raises: [].} =
  let writerRes = initMultiStreamWriter("test.ct", "test_program")
  doAssert writerRes.isOk, "initMultiStreamWriter failed: " & writerRes.error
  var w = writerRes.get()

  # Set metadata
  w.metadata.args = @["--verbose"]
  w.metadata.workdir = "/tmp/test"

  # Register paths
  let p0 = w.registerPath("/src/main.py")
  doAssert p0.isOk and p0.get() == 0, "registerPath main.py"
  let p1 = w.registerPath("/src/helper.py")
  doAssert p1.isOk and p1.get() == 1, "registerPath helper.py"

  # Re-register should return same ID
  let p0dup = w.registerPath("/src/main.py")
  doAssert p0dup.isOk and p0dup.get() == 0, "re-register main.py"

  # Write 100 steps: alternate between files, sequential lines
  # Steps 0-49 in main.py (lines 1-50), steps 50-99 in helper.py (lines 1-50)
  for i in 0 ..< 100:
    let pathId = if i < 50: 0'u64 else: 1'u64
    let line = uint64((i mod 50) + 1)

    var vals: seq[VariableValue]
    if i < 50:
      vals = @[VariableValue(varnameId: 0, typeId: 0, data: ($i).toBytes)]
    else:
      vals = @[VariableValue(varnameId: 1, typeId: 1,
        data: ("val_" & $i).toBytes)]

    let res = w.registerStep(pathId, line, vals)
    doAssert res.isOk, "registerStep " & $i & " failed: " & res.error

  # Write 5 IO events (at various points)
  for i in 0 ..< 5:
    let msg = "output_" & $i & "\n"
    let res = w.registerIOEvent(ioStdout, msg.toBytes)
    doAssert res.isOk, "registerIOEvent " & $i & " failed: " & res.error

  doAssert w.stepCount == 100, "stepCount should be 100, got " & $w.stepCount

  # Close and get bytes
  let closeRes = w.close()
  doAssert closeRes.isOk, "close failed: " & closeRes.error

  let ctfsBytes = w.toBytes()
  doAssert ctfsBytes.len > 0, "empty CTFS output"

  w.closeCtfs()

  # ------ Read back with NewTraceReader ------
  let readerRes = openNewTraceFromBytes(ctfsBytes)
  doAssert readerRes.isOk, "openNewTraceFromBytes failed: " & readerRes.error
  var reader = readerRes.get()

  # Verify metadata
  doAssert reader.meta.program == "test_program",
    "program mismatch: " & reader.meta.program
  doAssert reader.meta.workdir == "/tmp/test",
    "workdir mismatch: " & reader.meta.workdir
  doAssert reader.meta.args.len == 1 and reader.meta.args[0] == "--verbose",
    "args mismatch"
  doAssert reader.meta.paths.len == 2,
    "paths len: " & $reader.meta.paths.len
  doAssert reader.meta.paths[0] == "/src/main.py", "path 0 mismatch"
  doAssert reader.meta.paths[1] == "/src/helper.py", "path 1 mismatch"

  # Verify interning tables
  doAssert reader.pathCount() == 2, "pathCount: " & $reader.pathCount()
  let rp0 = reader.path(0)
  doAssert rp0.isOk and rp0.get() == "/src/main.py", "path 0 readback"
  let rp1 = reader.path(1)
  doAssert rp1.isOk and rp1.get() == "/src/helper.py", "path 1 readback"

  # Verify step count
  let sc = reader.stepCount()
  doAssert sc.isOk and sc.get() == 100, "stepCount readback: " &
    (if sc.isOk: $sc.get() else: sc.error)

  # Verify first step is AbsoluteStep
  let ev0 = reader.step(0)
  doAssert ev0.isOk, "step 0 failed: " & ev0.error
  doAssert ev0.get().kind == sekAbsoluteStep, "step 0 should be absolute"

  # Verify step 1 is delta (sequential line in same file)
  let ev1 = reader.step(1)
  doAssert ev1.isOk, "step 1 failed: " & ev1.error
  doAssert ev1.get().kind == sekDeltaStep, "step 1 should be delta"
  doAssert ev1.get().lineDelta == 1, "step 1 delta should be +1"

  # Verify step at file boundary (step 50 switches to helper.py)
  let ev50 = reader.step(50)
  doAssert ev50.isOk, "step 50 failed: " & ev50.error
  # Step 50 is at helper.py line 1, previous was main.py line 50
  # The GLI jump is large, so it should be AbsoluteStep
  doAssert ev50.get().kind == sekAbsoluteStep,
    "step 50 should be absolute (file switch)"

  # Verify values for step 0
  let vals0 = reader.values(0)
  doAssert vals0.isOk, "values 0 failed: " & vals0.error
  doAssert vals0.get().len == 1, "step 0 should have 1 value"
  doAssert vals0.get()[0].data == "0".toBytes, "step 0 value data"

  # Verify values for step 50
  let vals50 = reader.values(50)
  doAssert vals50.isOk, "values 50 failed: " & vals50.error
  doAssert vals50.get().len == 1, "step 50 should have 1 value"
  doAssert vals50.get()[0].varnameId == 1, "step 50 varnameId"
  doAssert vals50.get()[0].data == "val_50".toBytes, "step 50 value data"

  # Verify IO events
  let ioCount = reader.ioEventCount()
  doAssert ioCount.isOk and ioCount.get() == 5,
    "ioEventCount: " & (if ioCount.isOk: $ioCount.get() else: ioCount.error)

  let io0 = reader.ioEvent(0)
  doAssert io0.isOk, "ioEvent 0 failed: " & io0.error
  doAssert io0.get().kind == ioStdout, "io 0 kind"
  doAssert io0.get().data == "output_0\n".toBytes, "io 0 data"

  echo "PASS: test_basic_round_trip"


# ---------------------------------------------------------------------------
# Test with calls and returns
# ---------------------------------------------------------------------------

proc test_calls_and_returns() {.raises: [].} =
  let writerRes = initMultiStreamWriter("test_calls.ct", "call_test")
  doAssert writerRes.isOk, "init failed: " & writerRes.error
  var w = writerRes.get()

  let p0 = w.registerPath("/src/main.py")
  doAssert p0.isOk

  # Simulate: main() calls foo() which calls bar()
  # Step 0: main entry
  let s0 = w.registerStep(0, 1, @[])
  doAssert s0.isOk
  let c0 = w.registerCall(0, @[])  # main()
  doAssert c0.isOk

  # Step 1: inside main
  let s1 = w.registerStep(0, 2, @[])
  doAssert s1.isOk

  # Step 2: foo entry
  let s2 = w.registerStep(0, 10, @[])
  doAssert s2.isOk
  let c1 = w.registerCall(1, @["42".toBytes])  # foo(42)
  doAssert c1.isOk

  # Step 3: inside foo
  let s3 = w.registerStep(0, 11, @[])
  doAssert s3.isOk

  # Step 4: bar entry
  let s4 = w.registerStep(0, 20, @[])
  doAssert s4.isOk
  let c2 = w.registerCall(2, @["10".toBytes, "20".toBytes])  # bar(10,20)
  doAssert c2.isOk

  # Step 5: inside bar
  let s5 = w.registerStep(0, 21, @[])
  doAssert s5.isOk

  # Return from bar
  let r2 = w.registerReturn("30".toBytes)
  doAssert r2.isOk

  # Step 6: back in foo
  let s6 = w.registerStep(0, 12, @[])
  doAssert s6.isOk

  # Return from foo
  let r1 = w.registerReturn("99".toBytes)
  doAssert r1.isOk

  # Step 7: back in main
  let s7 = w.registerStep(0, 3, @[])
  doAssert s7.isOk

  # Return from main
  let r0 = w.registerReturn()
  doAssert r0.isOk

  let closeRes = w.close()
  doAssert closeRes.isOk, "close failed: " & closeRes.error

  let ctfsBytes = w.toBytes()
  w.closeCtfs()

  # Read back
  let readerRes = openNewTraceFromBytes(ctfsBytes)
  doAssert readerRes.isOk, "reader failed: " & readerRes.error
  var reader = readerRes.get()

  # Verify 8 steps
  let sc = reader.stepCount()
  doAssert sc.isOk and sc.get() == 8, "stepCount: " &
    (if sc.isOk: $sc.get() else: sc.error)

  # Verify 3 calls
  let cc = reader.callCount()
  doAssert cc.isOk and cc.get() == 3, "callCount: " &
    (if cc.isOk: $cc.get() else: cc.error)

  # Call 0 is bar (written first on return)
  let call0 = reader.call(0)
  doAssert call0.isOk, "call 0 failed: " & call0.error
  let c0r = call0.get()
  doAssert c0r.functionId == 2, "call0 functionId: " & $c0r.functionId
  doAssert c0r.depth == 2, "call0 depth: " & $c0r.depth
  doAssert c0r.args.len == 2, "call0 args count"
  doAssert c0r.args[0] == "10".toBytes, "call0 arg0"
  doAssert c0r.args[1] == "20".toBytes, "call0 arg1"
  doAssert c0r.returnValue == "30".toBytes, "call0 returnValue"

  # Call 1 is foo (returned after bar)
  let call1 = reader.call(1)
  doAssert call1.isOk, "call 1 failed: " & call1.error
  let c1r = call1.get()
  doAssert c1r.functionId == 1, "call1 functionId: " & $c1r.functionId
  doAssert c1r.depth == 1, "call1 depth: " & $c1r.depth
  doAssert c1r.args.len == 1, "call1 args count"
  doAssert c1r.returnValue == "99".toBytes, "call1 returnValue"
  doAssert c1r.children.len == 1, "call1 should have 1 child"
  doAssert c1r.children[0] == 0, "call1 child should be call 0 (bar)"

  # Call 2 is main (returned last)
  let call2 = reader.call(2)
  doAssert call2.isOk, "call 2 failed: " & call2.error
  let c2r = call2.get()
  doAssert c2r.functionId == 0, "call2 functionId: " & $c2r.functionId
  doAssert c2r.depth == 0, "call2 depth: " & $c2r.depth
  doAssert c2r.parentCallKey == -1, "call2 parentCallKey"
  doAssert c2r.returnValue == @[call_stream.VoidReturnMarker],
    "call2 returnValue should be void"
  doAssert c2r.children.len == 1, "call2 should have 1 child"
  doAssert c2r.children[0] == 1, "call2 child should be call 1 (foo)"

  echo "PASS: test_calls_and_returns"


# ---------------------------------------------------------------------------
# Test raise/catch
# ---------------------------------------------------------------------------

proc test_raise_catch() {.raises: [].} =
  let writerRes = initMultiStreamWriter("test_exc.ct", "exc_test")
  doAssert writerRes.isOk
  var w = writerRes.get()

  let p0 = w.registerPath("/src/main.py")
  doAssert p0.isOk

  # Step 0: normal step
  let s0 = w.registerStep(0, 1, @[VariableValue(
    varnameId: 0, typeId: 0, data: "42".toBytes)])
  doAssert s0.isOk

  # Step 1: raise
  let raiseRes = w.registerRaise(0, "division by zero".toBytes)
  doAssert raiseRes.isOk

  # Step 2: catch
  let catchRes = w.registerCatch(0)
  doAssert catchRes.isOk

  # Step 3: normal step after catch
  let s3 = w.registerStep(0, 5, @[])
  doAssert s3.isOk

  let closeRes = w.close()
  doAssert closeRes.isOk

  let ctfsBytes = w.toBytes()
  w.closeCtfs()

  # Read back
  let readerRes = openNewTraceFromBytes(ctfsBytes)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  let sc = reader.stepCount()
  doAssert sc.isOk and sc.get() == 4, "stepCount: " &
    (if sc.isOk: $sc.get() else: sc.error)

  # Step 0: AbsoluteStep
  let ev0 = reader.step(0)
  doAssert ev0.isOk and ev0.get().kind == sekAbsoluteStep

  # Step 1: Raise
  let ev1 = reader.step(1)
  doAssert ev1.isOk, "step 1 failed: " & ev1.error
  doAssert ev1.get().kind == sekRaise, "step 1 should be raise"
  doAssert ev1.get().message == "division by zero".toBytes

  # Step 2: Catch
  let ev2 = reader.step(2)
  doAssert ev2.isOk, "step 2 failed: " & ev2.error
  doAssert ev2.get().kind == sekCatch, "step 2 should be catch"

  # Step 3: should be step (absolute or delta)
  let ev3 = reader.step(3)
  doAssert ev3.isOk, "step 3 failed: " & ev3.error
  doAssert ev3.get().kind == sekAbsoluteStep or ev3.get().kind == sekDeltaStep,
    "step 3 should be a step event"

  # Values for step 0 should have 1 value
  let vals0 = reader.values(0)
  doAssert vals0.isOk and vals0.get().len == 1, "step 0 values"

  # Values for step 1 (raise) should be empty
  let vals1 = reader.values(1)
  doAssert vals1.isOk and vals1.get().len == 0, "raise values should be empty"

  echo "PASS: test_raise_catch"


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

test_basic_round_trip()
test_calls_and_returns()
test_raise_catch()
echo "ALL PASS: test_multi_stream_writer"
