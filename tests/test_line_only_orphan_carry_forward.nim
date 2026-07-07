## Line-only regression (92fce3a): in a LINE-ONLY trace (a recorder that
## calls ``register_step`` with a bare line and never opts into
## column-aware steps — e.g. ton, cardano, circom), a
## ``register_variable_*`` call whose value becomes known only AFTER the
## callee's own ``register_step`` already flushed the pending step was
## silently DROPPED.
##
## This is the ``var X = call()`` shape:
##
##   1. register_step(caller line)         # buffers pending step S1
##   2. register_step(callee entry)        # flushes S1, buffers S2
##   ...  callee runs and returns  ...
##   3. register_variable_int("x", ...)    # value of X, known post-return
##   4. register_step(next caller line)    # buffers S3
##
## At (3) there is no pending step for the caller's post-return position,
## so ``x`` lands in the orphan ``pendingValues`` queue.  Commit 92fce3a
## made ``flushPendingStep``'s orphan branch column-aware-ONLY: for
## line-only traces it did ``pendingValues.setLen(0)`` and DROPPED the
## value.  The fix restores the pre-92fce3a carry-forward: line-only
## orphan values stay in ``pendingValues`` and attach to the NEXT step's
## flush, so ``x`` reaches the value stream.
##
## Falsifiability: revert the line-only carry-forward (restore the
## unconditional ``pendingValues.setLen(0)`` in the orphan branch) and
## this test fails — ``x`` disappears from every value record.

# Include the FFI module so we can call the FFI procs directly from Nim.
# Mirrors tests/test_pending_value_after_delta_column.nim.
include codetracer_trace_writer_ffi

# Drop the `raises: []` push from the FFI module so the test body can
# use higher-level helpers (strutils.contains, etc.).
{.pop.}

import std/strutils

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

proc varnameId(r: pointer, name: string): uint64 =
  ## Return the interned varname_id of ``name`` or high(uint64) if absent.
  result = high(uint64)
  let vnCount = ct_reader_varname_count(r)
  for i in 0'u64 ..< vnCount:
    var outLen: csize_t
    let buf = ct_reader_varname(r, i, addr outLen)
    if readFfiStr(buf, outLen) == name:
      return i

proc valueOnAnyStep(r: pointer, name: string): bool =
  ## True if ``name`` surfaces in SOME step's value record.
  let id = varnameId(r, name)
  if id == high(uint64):
    return false
  for s in 0'u64 ..< ct_reader_step_count(r):
    if valuesJson(r, s).contains("\"varname_id\":" & $id):
      return true
  false

proc test_line_only_orphan_carries_forward() =
  ## Direct FFI reproduction of the line-only ``var X = call()`` shape.
  ## NOTE: column-aware mode is NOT enabled, so the trace is line-only
  ## (columnAwareSteps == false), exactly like the ton / cardano
  ## recorders.
  let outDir = getTempDir() / "ct_line_only_orphan_carry_forward"
  createDir(outDir)
  let eventsPath = outDir / "events.bin"
  let ctPath = outDir / "line_only.ct"
  if fileExists(ctPath): removeFile(ctPath)

  let handle = trace_writer_new(cstring("line_only"), ffiBinary)
  doAssert handle != nil, "trace_writer_new failed: " &
    $trace_writer_last_error()

  doAssert trace_writer_begin_events(handle, cstring(eventsPath)) == 0,
    "begin_events failed: " & $trace_writer_last_error()

  # DELIBERATELY line-only: no trace_writer_enable_column_aware_steps.
  let path = cstring("/src/main.tolk")

  # 1. Caller line `var x = f()`.
  trace_writer_register_step(handle, path, 10'i64)
  # 2. Enter callee `f` — flushes step S1 (no values yet), buffers S2.
  trace_writer_register_step(handle, path, 20'i64)
  # ... callee returns ...
  trace_writer_register_return(handle)
  # 3. The value of `x` becomes known only now, AFTER f() returned.
  #    There is no pending step for the caller's post-return position,
  #    so this lands in the orphan pendingValues queue.
  trace_writer_register_variable_int(
    handle, cstring("x"), 42'i64, ffiTkInt, cstring("int"))
  # 4. Next caller line — buffers S3.  The fix carries `x` forward so it
  #    attaches to this step's flush.  Pre-fix `x` was dropped here.
  trace_writer_register_step(handle, path, 11'i64)
  # A second binding on the next line to confirm normal flow still works.
  trace_writer_register_variable_int(
    handle, cstring("y"), 7'i64, ffiTkInt, cstring("int"))

  doAssert trace_writer_close(handle) == 0,
    "close failed: " & $trace_writer_last_error()
  trace_writer_free(handle)

  let r = ct_reader_open(cstring(ctPath))
  doAssert r != nil, "ct_reader_open failed: " &
    $trace_writer_last_error()

  doAssert valueOnAnyStep(r, "x"),
    "line-only orphan regression: variable 'x' (var x = f()) was " &
    "DROPPED from every value record — 92fce3a made the orphan drain " &
    "column-aware-only; the line-only carry-forward must preserve it"
  doAssert valueOnAnyStep(r, "y"),
    "variable 'y' missing — normal (non-orphan) flow regressed"

  echo "PASS: line-only orphan 'x' carried forward to a later step"
  ct_reader_close(r)

test_line_only_orphan_carries_forward()
echo "ALL PASS: test_line_only_orphan_carry_forward"
