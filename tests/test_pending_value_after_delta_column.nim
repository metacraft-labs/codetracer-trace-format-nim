## A variable registered after a column nudge reaches the step the nudge
## belongs to, and a column offered for a file with no column axis does not
## move that step to another line.
##
## This file began as the M-leo regression: `register_variable_*` landing
## after `trace_writer_register_delta_column` stranded the value, because
## `flushPendingStep` short-circuited on a `hasPendingStep` the eager column
## step had already cleared. That mechanism no longer exists — the column is
## folded into the pending line step, so the step and its column are ONE wire
## event and a trailing `register_variable` lands in the same flush window as
## the step it annotates (spec §"Canonical Recorder Integration Pattern").
##
## The file's own history is the reason it now asserts more than that. It was
## never added to the nimble `test` task, so from the day it was written it
## described a pipeline whose behaviour had moved on and nothing contradicted
## it: it still demanded the two separate steps the fold removed, and it drove
## the whole sequence against a path registered with NO per-line table, which
## is the one shape where a column may not be applied at all. Both would have
## been caught on the first run. See `conformance-testing.md` §"A gate that
## cannot pass".
##
## Falsifiability:
##
## * Remove the `effectiveDelta` guard in `registerStepWithColumn` and the
##   untabled case reports line 15 — the requested line plus its own column
##   minus one — instead of line 10.
## * Restore the pre-fold behaviour (emit the line step eagerly on
##   `register_delta_column`) and the tabled case sees two steps, with `x` on
##   neither or on the wrong one.
## * Make `flushPendingStep` park gap-staged values on a synthesized
##   `DeltaColumn` again and `x` stops being reachable from any step.

# Include the FFI module so we can call the FFI procs directly from Nim.
# Mirrors tests/test_reader_ffi.nim.
include codetracer_trace_writer_ffi

# Drop the `raises: []` push from the FFI module so the test body can
# use higher-level helpers (strutils.contains, etc.).
{.pop.}

import std/strutils

const
  StepLine = 10'i64
  ColumnDelta = 5'i64
    ## Requested column is `ColumnDelta + 1` = 6.
  FileLines = 20
  LineWidth = 80'u32

proc readFfiStr(buf: ptr uint8, length: csize_t): string =
  if buf.isNil or length == 0.csize_t:
    return ""
  result = newString(int(length))
  copyMem(addr result[0], buf, int(length))
  ct_free_buffer(buf)

proc valuesJson(h: pointer, step: uint64): string =
  var outLen: csize_t
  let buf = ct_reader_values(h, step, addr outLen)
  readFfiStr(buf, outLen)

proc varnameId(h: pointer, wanted: string): uint64 =
  ## The interned id for `wanted`, or `high(uint64)` when it was never
  ## interned. Value records name a `varname_id`, so a test that wants to
  ## say WHICH variable it found has to resolve the id.
  result = high(uint64)
  for i in 0'u64 ..< ct_reader_varname_count(h):
    var outLen: csize_t
    let buf = ct_reader_varname(h, i, addr outLen)
    if readFfiStr(buf, outLen) == wanted:
      return i

proc stepLocation(h: pointer, step: uint64): (uint64, uint64, uint64) =
  ## `(path_id, line, column)` for one step.
  var pathIds = newSeq[uint64](1)
  var lines = newSeq[uint64](1)
  var columns = newSeq[uint64](1)
  let written = ct_reader_step_locations_with_columns(
    h, step, 1'u64, addr pathIds[0], addr lines[0], addr columns[0])
  doAssert written == 1'u64,
    "step " & $step & " did not resolve: " & $trace_writer_last_error()
  (pathIds[0], lines[0], columns[0])

proc recordOne(outDir, name, path: string, tabled: bool): (string, seq[uint64]) =
  ## Drive the M-leo sequence once — step, column nudge, variable, return —
  ## against `path`, registering a per-line table for it only when `tabled`.
  ##
  ## Returns the container path and the path ids the writer reports having
  ## dropped a column for.
  createDir(outDir)
  let eventsPath = outDir / (name & "_events.bin")
  let ctPath = outDir / (name & ".ct")
  if fileExists(ctPath): removeFile(ctPath)

  let handle = trace_writer_new(cstring(name), ffiBinary)
  doAssert handle != nil, "trace_writer_new failed: " &
    $trace_writer_last_error()
  doAssert trace_writer_begin_events(handle, cstring(eventsPath)) == 0,
    "begin_events failed: " & $trace_writer_last_error()

  # Opt into column-aware mode BEFORE the first step (the flag is
  # trace-global).
  trace_writer_enable_column_aware_steps(handle)

  if tabled:
    # The per-line table is what gives the file a column axis. Without it the
    # file is sized by the line-only fallback and one address is one line.
    var lengths = newSeq[uint32](FileLines)
    for i in 0 ..< FileLines:
      lengths[i] = LineWidth
    doAssert trace_writer_register_path_with_line_lengths(
      handle, cstring(path), cint(FileLines),
      cast[ptr UncheckedArray[uint32]](addr lengths[0])) == 0,
      "register_path_with_line_lengths failed: " &
        $trace_writer_last_error()

  # 1. Line step. The running position must be defined before a column
  # delta can be applied to it.
  trace_writer_register_step(handle, cstring(path), StepLine)

  # 2. Column nudge. Folded into the pending step rather than emitted as a
  # second event.
  trace_writer_register_delta_column(handle, ColumnDelta)

  # 3. A variable AFTER the nudge — in the M-leo shape, the parameters of
  # the about-to-return function.
  let typeId = trace_writer_ensure_type_id(handle, ffiTkInt, cstring("int"))
  doAssert typeId != high(csize_t), "ensure_type_id failed"
  trace_writer_register_variable_int(
    handle, cstring("x"), 42'i64, ffiTkInt, cstring("int"))

  # 4. Return, which flushes the pending step and everything staged on it.
  trace_writer_register_return(handle)

  doAssert trace_writer_close(handle) == 0,
    "close failed: " & $trace_writer_last_error()
  let dropped = handle.msWriter.columnsDroppedForPaths
  trace_writer_free(handle)
  (ctPath, dropped)

proc test_a_trailing_variable_lands_on_the_column_aware_step() =
  ## One step, at the position the recorder asked for, carrying `x`.
  let (ctPath, dropped) = recordOne(
    getTempDir() / "ct_pending_value_after_delta_column",
    "tabled", "/src/aleo.leo", tabled = true)

  doAssert dropped.len == 0,
    "the file has a per-line table, so no column should have been dropped; " &
    "writer reported " & $dropped

  let r = ct_reader_open(cstring(ctPath))
  doAssert r != nil, "ct_reader_open failed: " & $trace_writer_last_error()

  # The step and its column are ONE event. Two would mean the fold had been
  # undone, and the value would be attached to whichever of them flushed.
  doAssert ct_reader_step_count(r) == 1'u64,
    "register_step + register_delta_column is one combined event; got " &
    $ct_reader_step_count(r) & " steps"

  let (_, line, column) = stepLocation(r, 0'u64)
  doAssert (line, column) == (uint64(StepLine), uint64(ColumnDelta + 1)),
    "the step must decode to the (line, column) the recorder asked for, " &
    "(10, 6); got (" & $line & ", " & $column & ")"

  let xId = varnameId(r, "x")
  doAssert xId != high(uint64), "varname 'x' must be interned in the trace"

  # On THAT step, not on some step. A scan over every record answers "the
  # value is in the container somewhere", which is true of a value parked at
  # an index no step occupies.
  let vals = valuesJson(r, 0'u64)
  doAssert vals.contains("\"varname_id\":" & $xId),
    "variable 'x' (varname_id=" & $xId & ") must be on step 0's value " &
    "record; that record holds " & vals

  ct_reader_close(r)
  echo "PASS: a trailing variable lands on the column-aware step it annotates"

proc test_a_column_without_an_axis_leaves_the_step_on_its_own_line() =
  ## THE OTHER HALF. The same sequence against a file with no per-line table.
  ## The step is kept at its line, the column is dropped, and the writer says
  ## which file lost it.
  let (ctPath, dropped) = recordOne(
    getTempDir() / "ct_pending_value_after_delta_column",
    "untabled", "/src/untabled.leo", tabled = false)

  let r = ct_reader_open(cstring(ctPath))
  doAssert r != nil, "ct_reader_open failed: " & $trace_writer_last_error()

  doAssert ct_reader_step_count(r) == 1'u64,
    "the step is kept — only its column is refused; got " &
    $ct_reader_step_count(r) & " steps"

  # Line 10, not line 15. One address is one line in a file sized by the
  # line-only fallback, so a folded column delta of 5 would land five lines
  # further down and read back as a position the program never executed.
  # This is the assertion the defect moves; the report below is how a caller
  # finds out, and it is checked second so a failure names the position first.
  let (_, line, column) = stepLocation(r, 0'u64)
  doAssert (line, column) == (uint64(StepLine), 1'u64),
    "a file with no column axis must resolve to (10, 1); got (" &
    $line & ", " & $column & ")"

  doAssert dropped.len == 1,
    "the writer must report the one file whose column it dropped; got " &
    $dropped

  # The step surviving is the point — the value still has somewhere to go.
  let xId = varnameId(r, "x")
  doAssert xId != high(uint64), "varname 'x' must be interned in the trace"
  let vals = valuesJson(r, 0'u64)
  doAssert vals.contains("\"varname_id\":" & $xId),
    "dropping the column must not cost the step its values; step 0 holds " &
    vals

  ct_reader_close(r)
  echo "PASS: a column without an axis leaves the step on its own line"

# Run the tests
test_a_trailing_variable_lands_on_the_column_aware_step()
test_a_column_without_an_axis_leaves_the_step_on_its_own_line()

echo "ALL PASS: test_pending_value_after_delta_column"
