## Regression (#601): an I/O event registered through the C FFI must be
## attributed to the step the recorder is CURRENTLY on — the step whose
## line performed the write — not to the previously emitted step.
##
## Shape (exactly what every recorder that goes through this FFI emits;
## the JavaScript instrumenter is the reported case, but Python, Ruby and
## the native recorder drive the same entry points):
##
##   * ``trace_writer_register_step`` does not emit its step.  It flushes
##     the PREVIOUS pending step and merely BUFFERS the new one, so that
##     variable values registered afterwards still attach to it
##     (``flushPendingStep``).
##   * A ``console.log`` / ``print`` on that same line then reaches
##     ``trace_writer_register_special_event`` while the step for the
##     writing line is still pending — i.e. not yet counted in
##     ``msWriter.stepCount``.
##   * ``registerIOEvent`` stamped ``stepCount - 1``, which under those
##     circumstances names the step BEFORE the writing line.
##
## The debugger reads that ``step_id`` verbatim (the CTFS reader and the
## db-backend's flow preloader both do), so the flow view rendered every
## line of program output against the preceding source line.  This is a
## multi-stream/CTFS-only defect: the legacy single-stream fallback in the
## same proc emits the event in stream order and the Rust trace processor
## stamps its own ``current_step_id``, which is correct.
##
## The fix gives ``registerIOEvent`` an explicit ``stepId`` and has the FFI
## pass the id a pending step WILL take — the same accounting
## ``trace_writer_next_step_index`` already documents.
##
## Falsifiability:
##
##   * Reverting ``registerIOEvent`` to the unconditional
##     ``stepCount - 1`` (or dropping the ``stepId`` argument at the FFI
##     call site) makes ``test_io_events_land_on_their_own_step`` fail,
##     naming the line the output was attributed to.
##   * "Just call ``flushPendingStep`` first" fixes the attribution but
##     breaks ``test_values_registered_after_a_write_stay_on_the_write_step``:
##     with the step already flushed, values registered after the write
##     become orphans and ``flushPendingStep`` re-homes them onto an
##     invented synthetic step instead of the step they belong to.  That
##     assertion exists to reject the naive fix, so do not delete it.

# Include the FFI module so we can drive the C entry points directly.
# Mirrors tests/test_orphan_call_args_step_location.nim.
include codetracer_trace_writer_ffi

# Drop the `raises: []` push from the FFI module so the test body can
# use higher-level helpers.
{.pop.}

import std/[strutils, sequtils]

const
  AppPath = "/srv/app.js"
  ## The reporter's program: three consecutive `console.log` statements,
  ## each preceded by the instrumenter's `__ct.step` for its own line.
  SumLine = 9'i64
  DoubledLine = 10'i64
  FinalLine = 11'i64

proc readFfiStr(buf: ptr uint8, length: csize_t): string =
  if buf.isNil or length == 0.csize_t:
    return ""
  result = newString(int(length))
  copyMem(addr result[0], buf, int(length))
  ct_free_buffer(buf)

proc lineOf(r: pointer, step: uint64): uint64 =
  var pathIds = [0'u64]
  var lines = [0'u64]
  var cols = [0'u64]
  let written = ct_reader_step_locations_with_columns(
    r, step, 1'u64, addr pathIds[0], addr lines[0], addr cols[0])
  doAssert written == 1'u64,
    "ct_reader_step_locations_with_columns(" & $step & ") failed: " &
      $trace_writer_last_error()
  lines[0]

proc stepAtLine(r: pointer, line: int64): uint64 =
  ## Index of the first (here: only) step recorded at ``line``.
  for s in 0'u64 ..< ct_reader_step_count(r):
    if lineOf(r, s) == uint64(line):
      return s
  doAssert false, "no step was recorded at line " & $line
  0'u64

proc eventStepOf(r: pointer, content: string): uint64 =
  ## The ``step_id`` of the IO event whose payload is ``content``.
  for i in 0'u64 ..< ct_reader_event_count(r):
    var kind: uint8
    var stepId: uint64
    var data: ptr uint8
    var dataLen: csize_t
    if ct_reader_event_fields(r, i, addr kind, addr stepId,
        addr data, addr dataLen) == 0:
      let payload = readFfiStr(data, dataLen)
      if payload == content:
        return stepId
  doAssert false, "no IO event carrying " & content.escape() & " was recorded"
  0'u64

proc varnameId(r: pointer, name: string): uint64 =
  ## The interned varname_id of ``name``, or high(uint64) if absent.
  result = high(uint64)
  for i in 0'u64 ..< ct_reader_varname_count(r):
    var outLen: csize_t
    if readFfiStr(ct_reader_varname(r, i, addr outLen), outLen) == name:
      return i

proc stepsCarrying(r: pointer, name: string): seq[uint64] =
  ## Indices of every step whose value record carries ``name``.
  let want = varnameId(r, name)
  if want == high(uint64):
    return @[]
  for s in 0'u64 ..< ct_reader_step_count(r):
    for k in 0'u64 ..< ct_reader_step_value_count(r, s):
      var vnId, tId: uint64
      var data: ptr uint8
      var dataLen: csize_t
      if ct_reader_step_value(r, s, k, addr vnId, addr tId,
          addr data, addr dataLen) == 0:
        if not data.isNil: ct_free_buffer(data)
        if vnId == want:
          result.add s
          break

proc newColumnAwareHandle(dir, name: string): TraceWriterHandle =
  ## A writer configured the way the JavaScript / Python recorders
  ## configure it: multi-stream (CTFS) and column-aware.
  createDir(dir)
  let ctPath = dir / (name & ".ct")
  if fileExists(ctPath): removeFile(ctPath)
  result = trace_writer_new(cstring(name), ffiBinary)
  doAssert result != nil,
    "trace_writer_new failed: " & $trace_writer_last_error()
  doAssert trace_writer_begin_events(result, cstring(dir / "events.bin")) == 0,
    "begin_events failed: " & $trace_writer_last_error()
  trace_writer_enable_column_aware_steps(result)
  var lineLengths = newSeq[uint32](120)
  for i in 0 ..< lineLengths.len:
    lineLengths[i] = 80'u32
  doAssert trace_writer_register_path_with_line_lengths(
    result, cstring(AppPath), cint(lineLengths.len),
    cast[ptr UncheckedArray[uint32]](addr lineLengths[0])) == 0,
    "register_path_with_line_lengths failed: " & $trace_writer_last_error()

proc emitWrite(handle: TraceWriterHandle, content: string) =
  ## What a recorder's stdout hook does for a `console.log` / `print`.
  trace_writer_register_special_event(
    handle, ffiElkWrite, cstring("stdout"), cstring(content))

proc recordThreeWrites(dir, name: string): string =
  ## Record the reporter's program and return the container path.
  ##
  ## Ordering is deliberate and load-bearing:
  ##   * every write is registered while its OWN line's step is still
  ##     pending (that is the bug's precondition), and
  ##   * `total` is registered AFTER the line-11 write but belongs to
  ##     line 11's step (the write-site values the JS instrumenter emits
  ##     after each statement) — the guard against the naive fix.
  let handle = newColumnAwareHandle(dir, name)
  let path = cstring(AppPath)

  trace_writer_register_step(handle, path, SumLine)
  handle.emitWrite("Sum: 42")

  trace_writer_register_step(handle, path, DoubledLine)
  handle.emitWrite("Doubled: 84")

  trace_writer_register_step(handle, path, FinalLine)
  handle.emitWrite("Final: 94")
  trace_writer_register_variable_int(
    handle, cstring("total"), 94'i64, ffiTkInt, cstring("int"))

  doAssert trace_writer_close(handle) == 0,
    "close failed: " & $trace_writer_last_error()
  trace_writer_free(handle)
  dir / (name & ".ct")

proc describe(r: pointer, stepId: uint64): string =
  "step " & $stepId & " (line " & $lineOf(r, stepId) & ")"

proc test_io_events_land_on_their_own_step() =
  let outDir = getTempDir() / "ct_io_event_pending_step_attribution"
  let ctPath = recordThreeWrites(outDir, "three_writes")

  let r = ct_reader_open(cstring(ctPath))
  doAssert r != nil, "ct_reader_open failed: " & $trace_writer_last_error()

  doAssert ct_reader_event_count(r) == 3'u64,
    "expected the three recorded writes, got " & $ct_reader_event_count(r)

  for (content, line) in [("Sum: 42", SumLine), ("Doubled: 84", DoubledLine),
                          ("Final: 94", FinalLine)]:
    let want = stepAtLine(r, line)
    let got = eventStepOf(r, content)
    # Assertion 1/2/3: the event names the step of the line that wrote it.
    doAssert got == want,
      "the write " & content.escape() & " (source line " & $line &
      ") was attributed to " & describe(r, got) & ", not to " &
      describe(r, want) & ". The FFI buffers one step, so at the moment " &
      "the write is registered the step for its own line is still " &
      "pending and an unconditional `stepCount - 1` names the PREVIOUS " &
      "step — the flow view then renders the output one source line too " &
      "high (issue #601)."
    doAssert lineOf(r, got) == uint64(line),
      "the write " & content.escape() & " resolves to line " &
      $lineOf(r, got) & ", not line " & $line

  echo "PASS: IO events land on the step of the line that wrote them"
  ct_reader_close(r)

proc test_values_registered_after_a_write_stay_on_the_write_step() =
  ## Guards against "just flush the pending step in
  ## register_special_event".  Flushing there would emit line 11's step
  ## early, leaving `total` — registered after the write but belonging to
  ## the same statement — with no step to attach to, so
  ## `flushPendingStep`'s orphan branch would invent a separate synthetic
  ## step for it.
  let outDir = getTempDir() / "ct_io_event_pending_step_attribution"
  let ctPath = recordThreeWrites(outDir, "three_writes_values")

  let r = ct_reader_open(cstring(ctPath))
  doAssert r != nil, "ct_reader_open failed: " & $trace_writer_last_error()

  let carriers = stepsCarrying(r, "total")
  # Assertion 4: the value survived at all (the M-leo guarantee).
  doAssert carriers.len > 0,
    "the variable registered after the write never reached the value " &
    "stream — orphan values must never be dropped"

  let finalStep = stepAtLine(r, FinalLine)
  # Assertion 5: and it is on the SAME step as the write it follows,
  # not on a synthetic step invented after an early flush.
  doAssert carriers == @[finalStep],
    "the variable registered after the line-" & $FinalLine & " write " &
    "landed on " & carriers.mapIt(describe(r, it)).join(", ") &
    " instead of on " & describe(r, finalStep) & " alone. An early " &
    "`flushPendingStep` in register_special_event strands same-statement " &
    "values in the orphan branch, which re-homes them onto an invented " &
    "step; pass the pending step's id to registerIOEvent instead."

  doAssert eventStepOf(r, "Final: 94") == finalStep,
    "the line-" & $FinalLine & " write and the value registered after " &
    "it disagree about which step they belong to"

  echo "PASS: values registered after a write stay on the write's step"
  ct_reader_close(r)

proc test_a_write_before_begin_events_does_not_kill_the_recorded_process() =
  ## Latent defect found while fixing the attribution: the multi-stream
  ## branch of `trace_writer_register_special_event` was the only one that
  ## did not check `msWriterReady`, so an event arriving before
  ## `trace_writer_begin_events` indexed an empty stream table.  An
  ## IndexDefect cannot be caught across the C boundary, so it terminated
  ## the RECORDED PROCESS rather than dropping one event.
  let handle = trace_writer_new(cstring("premature"), ffiBinary)
  doAssert handle != nil,
    "trace_writer_new failed: " & $trace_writer_last_error()
  handle.emitWrite("output before the trace was opened")
  trace_writer_free(handle)
  echo "PASS: a write before begin_events is dropped, not fatal"

test_io_events_land_on_their_own_step()
test_values_registered_after_a_write_stay_on_the_write_step()
test_a_write_before_begin_events_does_not_kill_the_recorded_process()
echo "ALL PASS: test_io_event_pending_step_attribution"
