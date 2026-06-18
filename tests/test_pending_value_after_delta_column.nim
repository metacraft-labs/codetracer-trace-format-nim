## M-leo regression: a ``register_variable_*`` call that lands after a
## ``trace_writer_register_delta_column`` would silently strand the value
## in the FFI's ``pendingValues`` queue.  The ``register_return`` /
## ``close`` paths called ``flushPendingStep`` which short-circuited
## when ``hasPendingStep`` was false (the column step having already
## been emitted), so the trailing variable never reached the value
## stream.
##
## This regression test mirrors the ``test_aleo_instructions_test``
## failure shape M-leo surfaced:
##
##   1. trace_writer_register_step(path, line)
##   2. trace_writer_register_delta_column(+col)
##   3. trace_writer_register_variable_int("x", 42, ...)
##   4. trace_writer_register_return()
##
## Without the fix step 4 silently drops the ``x`` value.  With the fix
## the FFI buffers the column step until its accompanying values arrive
## (or flushes any orphaned ``pendingValues`` via a synthetic zero-delta
## column step at close/return time) so the value surfaces in the
## value stream.

# Include the FFI module so we can call the FFI procs directly from Nim.
# Mirrors tests/test_reader_ffi.nim.
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

proc test_pending_value_survives_delta_column() =
  ## Direct FFI reproduction of the M-leo aleo recorder sequence.
  ##
  ##   register_step → register_delta_column →
  ##   register_variable_int → register_return → close
  ##
  ## After close the trace must contain a value record carrying the
  ## variable.  Pre-fix the value was silently dropped because
  ## ``flushPendingStep`` short-circuited (``hasPendingStep == false``
  ## after the column step's eager emit).
  let outDir = getTempDir() / "ct_pending_value_after_delta_column"
  createDir(outDir)
  let eventsPath = outDir / "events.bin"
  let ctPath = outDir / "aleo_test.ct"
  # Clean prior runs so this test is idempotent.
  if fileExists(ctPath): removeFile(ctPath)

  let handle = trace_writer_new(cstring("aleo_test"), ffiBinary)
  doAssert handle != nil, "trace_writer_new failed: " &
    $trace_writer_last_error()

  doAssert trace_writer_begin_events(handle, cstring(eventsPath)) == 0,
    "begin_events failed: " & $trace_writer_last_error()

  # Opt into column-aware mode BEFORE the first step (spec requires the
  # flag to be trace-global).
  trace_writer_enable_column_aware_steps(handle)

  # 1. Initial line step — the running ``global_position_index`` must be
  # defined before a DeltaColumn can be applied.
  trace_writer_register_step(handle, cstring("/src/aleo.leo"), 10'i64)

  # 2. Column nudge.  Pre-fix this would have flushed the line step
  # eagerly and emitted the column step with empty values, clearing
  # ``hasPendingStep`` so step 3's value got stranded.
  trace_writer_register_delta_column(handle, 5'i64)

  # 3. Register a variable AFTER the column step.  In the M-leo
  # failure shape these were the parameters of the about-to-return
  # function.
  let typeId = trace_writer_ensure_type_id(
    handle, ffiTkInt, cstring("int"))
  doAssert typeId != high(csize_t), "ensure_type_id failed"
  trace_writer_register_variable_int(
    handle, cstring("x"), 42'i64, ffiTkInt, cstring("int"))

  # 4. Return — pre-fix the ``flushPendingStep`` here was a no-op
  # because ``hasPendingStep`` had already been cleared by step 2,
  # so ``x`` never reached the value stream.
  trace_writer_register_return(handle)

  doAssert trace_writer_close(handle) == 0,
    "close failed: " & $trace_writer_last_error()
  trace_writer_free(handle)

  # Read back the trace via the reader FFI and check that ``x`` shows
  # up somewhere in the value stream.
  let r = ct_reader_open(cstring(ctPath))
  doAssert r != nil, "ct_reader_open failed: " &
    $trace_writer_last_error()

  let stepCount = ct_reader_step_count(r)
  doAssert stepCount >= 2'u64,
    "expected at least 2 steps (line + column), got " & $stepCount

  # Locate the variable name's id so we can match against value records
  # by id (the JSON payload reports the interned varname_id).
  var xId: uint64 = high(uint64)
  let vnCount = ct_reader_varname_count(r)
  for i in 0'u64 ..< vnCount:
    var outLen: csize_t
    let buf = ct_reader_varname(r, i, addr outLen)
    let name = readFfiStr(buf, outLen)
    if name == "x":
      xId = i
      break
  doAssert xId != high(uint64),
    "varname 'x' must be interned in the trace"

  # The value must appear on SOME step record.  Pre-fix every record
  # was empty.  Post-fix it surfaces either on the column step (the
  # buffered-step path) or on a synthetic zero-delta column step
  # appended at flush time (the orphan-drain path).
  var foundOnStep: int64 = -1
  for s in 0'u64 ..< ct_reader_step_count(r):
    let vals = valuesJson(r, s)
    if vals.contains("\"varname_id\":" & $xId):
      foundOnStep = int64(s)
      break

  doAssert foundOnStep >= 0,
    "variable 'x' (varname_id=" & $xId &
    ") missing from every value record — " &
    "M-leo regression: pendingValues stranded after register_delta_column"

  echo "PASS: variable 'x' surfaced on step ", foundOnStep,
    " of ", ct_reader_step_count(r)

  ct_reader_close(r)

# Run the test
test_pending_value_survives_delta_column()
echo "ALL PASS: test_pending_value_after_delta_column"
