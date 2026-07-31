## Regression: the synthetic step the FFI invents for ORPHAN call
## arguments must carry a truthful source location instead of inheriting
## the position of the last unrelated step.
##
## Shape (exactly what the Python recorder emits for a Flask app with a
## parameterised route such as ``/api/users/<int:user_id>``):
##
##   * ``NimTraceWriter::arg`` registers each call argument as a step
##     VARIABLE (``trace_writer_register_variable_*``) and stages it
##     (``trace_writer_register_call_arg``) immediately before
##     ``trace_writer_register_call``.
##   * At that moment the caller's own pending step has already been
##     flushed — the previous FFI event was the preceding request's
##     ``after_request`` hook ``register_return``.  So the argument
##     values land in the orphan ``pendingValues`` queue with no step to
##     attach to.
##   * ``flushPendingStep``'s orphan branch then invents a step for
##     them.  It used to invent a ZERO-DELTA ``registerColumnStep``,
##     which inherits the position of the last emitted step — i.e. the
##     PREVIOUS REQUEST's hook return line, in a different logical unit
##     entirely.
##
## That synthetic step is the first recorded event of the new request,
## so it is what a ``web-request`` span's ``start_step`` binds to.
## Double-clicking such a request in the Request Panel therefore seeked
## to a stale line belonging to the previous request.
##
## The fix threads the callee's DEFINITION site (resolved from the FFI's
## function registry) from ``trace_writer_register_call`` into
## ``flushPendingStep``, which emits the synthetic step there — the
## location where those argument values are actually in scope.
##
## Falsifiability: restore the unconditional
## ``registerColumnStep(0'i64, ...)`` in the orphan branch and
## ``test_orphan_call_args_land_on_the_callee_def_line`` fails, naming
## the stale line it landed on.
##
## The sibling guarantees this branch must keep are asserted elsewhere
## and deliberately not re-litigated here:
## ``tests/test_line_only_orphan_carry_forward.nim`` (line-only traces
## carry orphan values forward rather than dropping them) — plus
## ``test_orphan_without_a_known_location_still_reaches_the_stream``
## below, which pins the no-location fallback so the M-leo "never drop
## orphan values" guarantee survives the new early-return path.

# Include the FFI module so we can drive the C entry points directly.
# Mirrors tests/test_line_only_orphan_carry_forward.nim.
include codetracer_trace_writer_ffi

# Drop the `raises: []` push from the FFI module so the test body can
# use higher-level helpers.
{.pop.}

const
  AppPath = "/srv/app.py"
  ## Definition lines of the three recorded functions, mirroring the
  ## flask fixture's `test-programs/web/flask/app.py`.
  ListUsersDefLine = 28'i64
  PublishRouteDefLine = 64'i64
  GetUserDefLine = 39'i64
  ## The last line of the `after_request` hook — the line the buggy
  ## zero-delta column step inherited.
  HookReturnLine = 71'i64

proc readFfiStr(buf: ptr uint8, length: csize_t): string =
  if buf.isNil or length == 0.csize_t:
    return ""
  result = newString(int(length))
  copyMem(addr result[0], buf, int(length))
  ct_free_buffer(buf)

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

proc encodeInt(value: int64, typeId: csize_t): seq[byte] =
  var sve = StreamingValueEncoder.init()
  discard sve.writeInt(value, uint64(typeId))
  sve.getBytes()

proc encodeRaw(repr: string, typeId: csize_t): seq[byte] =
  var sve = StreamingValueEncoder.init()
  discard sve.writeRaw(repr, uint64(typeId))
  sve.getBytes()

proc stageArg(handle: TraceWriterHandle, name: string, cbor: seq[byte]) =
  ## What ``NimTraceWriter::arg`` does: register the argument as a step
  ## variable AND stage it on the call record.  The first half is what
  ## seeds ``pendingValues``.
  trace_writer_register_variable_cbor(
    handle, cstring(name), unsafeAddr cbor[0], csize_t(cbor.len))
  trace_writer_register_call_arg(
    handle, cstring(name), unsafeAddr cbor[0], csize_t(cbor.len))

proc newColumnAwareHandle(dir, name: string): TraceWriterHandle =
  createDir(dir)
  let ctPath = dir / (name & ".ct")
  if fileExists(ctPath): removeFile(ctPath)
  result = trace_writer_new(cstring(name), ffiBinary)
  doAssert result != nil,
    "trace_writer_new failed: " & $trace_writer_last_error()
  doAssert trace_writer_begin_events(result, cstring(dir / "events.bin")) == 0,
    "begin_events failed: " & $trace_writer_last_error()
  # Column-aware, like every recorder that goes through `arg()`.
  trace_writer_enable_column_aware_steps(result)
  # Register the source file WITH its per-line lengths, as the recorder
  # does on first sighting of a path, so column resolution is live.
  var lineLengths = newSeq[uint32](120)
  for i in 0 ..< lineLengths.len:
    lineLengths[i] = 80'u32
  doAssert trace_writer_register_path_with_line_lengths(
    result, cstring(AppPath), cint(lineLengths.len),
    cast[ptr UncheckedArray[uint32]](addr lineLengths[0])) == 0,
    "register_path_with_line_lengths failed: " & $trace_writer_last_error()

proc test_orphan_call_args_land_on_the_callee_def_line() =
  let outDir = getTempDir() / "ct_orphan_call_args_step_location"
  let ctPath = outDir / "flask_like.ct"
  let handle = newColumnAwareHandle(outDir, "flask_like")
  let path = cstring(AppPath)

  let intType = trace_writer_ensure_type_id(handle, ffiTkInt, cstring("int"))
  let rawType = trace_writer_ensure_type_id(handle, ffiTkRaw, cstring("Object"))
  doAssert intType != high(csize_t) and rawType != high(csize_t)

  let fidListUsers = trace_writer_ensure_function_id(
    handle, cstring("list_users"), path, ListUsersDefLine)
  let fidPublishRoute = trace_writer_ensure_function_id(
    handle, cstring("publish_route"), path, PublishRouteDefLine)
  let fidGetUser = trace_writer_ensure_function_id(
    handle, cstring("get_user"), path, GetUserDefLine)

  # ---- request 1: GET /api/users -> list_users(), no arguments -------
  trace_writer_register_call(handle, fidListUsers)
  trace_writer_register_step(handle, path, ListUsersDefLine)  # entry step
  trace_writer_register_step(handle, path, 30'i64)
  trace_writer_register_return(handle)

  # ---- request 1: the after_request hook publish_route(response) -----
  # `arg()` seeds pendingValues with `response`; the caller's step was
  # already flushed by the return above, so it is orphaned.
  let responseCbor = encodeRaw("<Response 24 bytes [200 OK]>", rawType)
  stageArg(handle, "response", responseCbor)
  trace_writer_register_call(handle, fidPublishRoute)
  trace_writer_register_step(handle, path, PublishRouteDefLine)  # entry step
  trace_writer_register_step(handle, path, HookReturnLine)
  trace_writer_register_return(handle)   # flushes the hook's last step

  # ---- request 2: GET /api/users/2 -> get_user(user_id) -------------
  # THE BUG: `user_id` is orphaned exactly like `response` was, but the
  # last emitted step now belongs to the PREVIOUS request's hook.
  let userIdCbor = encodeInt(2'i64, intType)
  stageArg(handle, "user_id", userIdCbor)
  trace_writer_register_call(handle, fidGetUser)
  trace_writer_register_step(handle, path, GetUserDefLine)  # entry step
  trace_writer_register_step(handle, path, 41'i64)
  trace_writer_register_return(handle)

  doAssert trace_writer_close(handle) == 0,
    "close failed: " & $trace_writer_last_error()
  trace_writer_free(handle)

  let r = ct_reader_open(cstring(ctPath))
  doAssert r != nil, "ct_reader_open failed: " & $trace_writer_last_error()

  let userIdSteps = stepsCarrying(r, "user_id")
  doAssert userIdSteps.len > 0,
    "the orphaned call argument 'user_id' never reached the value " &
    "stream — the M-leo guarantee (orphan values must not be dropped) " &
    "regressed"

  let synthetic = userIdSteps[0]
  let line = lineOf(r, synthetic)
  doAssert line == uint64(GetUserDefLine),
    "the synthetic step invented for the orphaned call arguments of " &
    "get_user (step " & $synthetic & ", the first recorded event of " &
    "the parameterised route's request and therefore the step a " &
    "web-request span's start_step binds to) reports line " & $line &
    ", not the callee's definition line " & $GetUserDefLine & ". " &
    (if line == uint64(HookReturnLine):
       "Line " & $HookReturnLine & " is the PREVIOUS request's " &
       "after_request hook return: the zero-delta column step " &
       "inherited a position from a different logical unit, so a " &
       "Request Panel double-click seeks into the wrong request."
     else:
       "It inherited an unrelated previous position.")

  # The same must hold for the hook's own orphaned `response` argument:
  # it belongs at publish_route's definition line, not at whatever line
  # the previous handler happened to stop on.
  let responseSteps = stepsCarrying(r, "response")
  doAssert responseSteps.len > 0, "orphaned argument 'response' was dropped"
  let responseLine = lineOf(r, responseSteps[0])
  doAssert responseLine == uint64(PublishRouteDefLine),
    "the synthetic step for publish_route's orphaned 'response' " &
    "argument reports line " & $responseLine & ", not the callee's " &
    "definition line " & $PublishRouteDefLine

  echo "PASS: orphaned call arguments land on the callee's definition line"
  ct_reader_close(r)

proc test_orphan_without_a_known_location_still_reaches_the_stream() =
  ## The no-location fallback (M-leo, 92fce3a): when the orphan values
  ## are NOT call arguments there is no callee to name, so the synthetic
  ## step is still a zero-delta column step — but the values must reach
  ## the value stream, exactly as before.  This pins the fallback so the
  ## new early-return in the orphan branch cannot start dropping values.
  let outDir = getTempDir() / "ct_orphan_without_location"
  let ctPath = outDir / "no_location.ct"
  let handle = newColumnAwareHandle(outDir, "no_location")
  let path = cstring(AppPath)

  trace_writer_register_step(handle, path, 10'i64)
  trace_writer_register_return(handle)          # flushes the step
  # Registered with no pending step and no following call: nothing can
  # name a truthful location, so the drain happens at close.
  trace_writer_register_variable_int(
    handle, cstring("late"), 7'i64, ffiTkInt, cstring("int"))

  doAssert trace_writer_close(handle) == 0,
    "close failed: " & $trace_writer_last_error()
  trace_writer_free(handle)

  let r = ct_reader_open(cstring(ctPath))
  doAssert r != nil, "ct_reader_open failed: " & $trace_writer_last_error()
  doAssert stepsCarrying(r, "late").len > 0,
    "the no-location orphan drain dropped 'late' — the M-leo guarantee " &
    "(orphan values must never be lost) regressed"
  echo "PASS: no-location orphan values still reach the value stream"
  ct_reader_close(r)

test_orphan_call_args_land_on_the_callee_def_line()
test_orphan_without_a_known_location_still_reaches_the_stream()
echo "ALL PASS: test_orphan_call_args_step_location"
