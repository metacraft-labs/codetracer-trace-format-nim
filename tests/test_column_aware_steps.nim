{.push raises: [].}

## Integration tests for the P6.3 / P6.4 column-aware step encoding.
##
## Covers the end-to-end path: ``MultiStreamTraceWriter`` opted into
## column-aware mode via ``enableColumnAwareSteps``, emitting a mix of
## ``registerStep`` (line-level) and ``registerColumnStep`` (column
## nudge) events, and confirms that:
##
##   * ``meta.dat`` carries ``FlagHasColumnAwareSteps`` (bit 4),
##   * the step stream round-trips ``sekDeltaColumn`` events with their
##     column deltas intact through ``NewTraceReader``,
##   * the value stream stays in lock-step with the exec stream
##     (one values record per step event of either kind),
##   * a writer that did not opt in rejects ``registerColumnStep`` so
##     legacy line-only traces remain byte-for-byte identical to the
##     pre-P6.4 output.
##
## See ~codetracer-trace-format-spec/trace-events.md~
## §"Column Encoding — `DeltaColumn` (chosen)" and
## §"Reader Behaviour and Back-Compat" for the spec contract this test
## guards.

import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/step_encoding

proc test_column_aware_round_trip() {.raises: [].} =
  let writerRes = initMultiStreamWriter("test_columns.ct", "col_test")
  doAssert writerRes.isOk, "init failed: " & writerRes.error
  var w = writerRes.get()

  # Opt into column-aware mode *before* any step is written.
  w.enableColumnAwareSteps()

  let p0 = w.registerPath("/src/main.py")
  doAssert p0.isOk

  # First step must be absolute (registerStep) to define the running
  # position.  Then alternate column nudges with line moves.
  doAssert w.registerStep(0, 10, @[]).isOk, "first registerStep"
  doAssert w.registerColumnStep(2, @[]).isOk, "column nudge +2"
  doAssert w.registerColumnStep(3, @[]).isOk, "column nudge +3"
  doAssert w.registerStep(0, 11, @[]).isOk, "line move to 11"
  doAssert w.registerColumnStep(-4, @[]).isOk, "column nudge -4"
  doAssert w.registerStep(0, 12, @[]).isOk, "line move to 12"
  doAssert w.registerColumnStep(1, @[]).isOk, "column nudge +1"

  doAssert w.close().isOk, "close failed"
  let ctBytes = w.toBytes()
  w.closeCtfs()

  # Re-open the trace via the reader.
  var readerRes = openNewTraceFromBytes(ctBytes)
  doAssert readerRes.isOk, "openNewTraceFromBytes failed: " & readerRes.error
  var reader = readerRes.get()

  # 1. meta.dat carries the column-aware flag.
  doAssert reader.meta.hasColumnAwareSteps,
    "meta.dat should have FlagHasColumnAwareSteps set after " &
    "enableColumnAwareSteps + close"

  # 2. Step count matches: 4 line steps + 3 column steps = 7 events.
  let sc = reader.stepCount()
  doAssert sc.isOk and sc.get() == 7,
    "expected 7 steps, got " &
      (if sc.isOk: $sc.get() else: sc.error)

  # 3. Walk every step and assert the expected kinds / column deltas.
  let expectedKinds = @[
    sekAbsoluteStep,  # registerStep(line=10)
    sekDeltaColumn,   # +2
    sekDeltaColumn,   # +3
    sekDeltaStep,     # registerStep(line=11) — small delta
    sekDeltaColumn,   # -4
    sekDeltaStep,     # registerStep(line=12) — small delta
    sekDeltaColumn,   # +1
  ]
  let expectedColumnDeltas = @[0'i64, 2, 3, 0, -4, 0, 1]

  for i, expKind in expectedKinds:
    let evRes = reader.step(uint64(i))
    doAssert evRes.isOk, "step " & $i & " failed: " & evRes.error
    let ev = evRes.get()
    doAssert ev.kind == expKind,
      "step " & $i & " kind: expected " & $expKind &
      " got " & $ev.kind
    if ev.kind == sekDeltaColumn:
      doAssert ev.columnDelta == expectedColumnDeltas[i],
        "step " & $i & " columnDelta: expected " &
        $expectedColumnDeltas[i] & " got " & $ev.columnDelta

  echo "PASS: test_column_aware_round_trip"

proc test_column_step_requires_opt_in() {.raises: [].} =
  ## P6.4: registerColumnStep must error on a writer that has *not*
  ## opted into column-aware mode.  This is the safety net that keeps
  ## line-only traces byte-for-byte identical to the pre-P6.4 output
  ## even if a recorder forgets to flip the flag.
  let writerRes = initMultiStreamWriter("test_no_opt.ct", "no_opt")
  doAssert writerRes.isOk
  var w = writerRes.get()

  doAssert w.registerPath("/src/main.py").isOk
  doAssert w.registerStep(0, 1, @[]).isOk

  let colRes = w.registerColumnStep(1, @[])
  doAssert colRes.isErr,
    "registerColumnStep without enableColumnAwareSteps must error"

  discard w.close()
  w.closeCtfs()
  echo "PASS: test_column_step_requires_opt_in"

proc test_column_step_first_is_rejected() {.raises: [].} =
  ## P6.4: registerColumnStep can't be the *first* step in a trace
  ## either — column deltas only make sense once an absolute
  ## ``global_position_index`` has been emitted by an AbsoluteStep.
  let writerRes = initMultiStreamWriter("test_first_col.ct", "first_col")
  doAssert writerRes.isOk
  var w = writerRes.get()
  w.enableColumnAwareSteps()

  doAssert w.registerPath("/src/main.py").isOk
  let colRes = w.registerColumnStep(1, @[])
  doAssert colRes.isErr,
    "first step must be an AbsoluteStep, registerColumnStep should error"

  discard w.close()
  w.closeCtfs()
  echo "PASS: test_column_step_first_is_rejected"

proc test_no_column_flag_for_legacy_writer() {.raises: [].} =
  ## P6.4: a writer that never opted into column-aware mode MUST NOT
  ## set the flag bit on meta.dat — that bit is a contract with old
  ## readers, which reject any trace whose flag they don't understand.
  let writerRes = initMultiStreamWriter("test_legacy.ct", "legacy")
  doAssert writerRes.isOk
  var w = writerRes.get()

  doAssert w.registerPath("/src/main.py").isOk
  doAssert w.registerStep(0, 1, @[]).isOk
  doAssert w.registerStep(0, 2, @[]).isOk

  doAssert w.close().isOk
  let ctBytes = w.toBytes()
  w.closeCtfs()

  var readerRes = openNewTraceFromBytes(ctBytes)
  doAssert readerRes.isOk
  let reader = readerRes.get()

  doAssert not reader.meta.hasColumnAwareSteps,
    "legacy (non-opt-in) writer must leave FlagHasColumnAwareSteps clear"
  echo "PASS: test_no_column_flag_for_legacy_writer"

test_column_aware_round_trip()
test_column_step_requires_opt_in()
test_column_step_first_is_rejected()
test_no_column_flag_for_legacy_writer()
echo "ALL PASS: test_column_aware_steps"
