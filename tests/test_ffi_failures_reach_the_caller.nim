## Every class of C ABI entry point reports a failure its caller can see.
##
## A failure inside an exported writer function used to look like success. An
## exception unwinding out of an `exportc` proc returns to the C caller with
## the result's default value — 0, which `trace_writer_close` defines as
## success — so a BEAM recording whose close hit an index error was written
## EMPTY and reported nothing. A `void` entry point that failed had no return
## value to fail through at all, and its caller never learned that an event
## was missing.
##
## Built with `-d:ffiFaultInjection`, which makes the entry point named in
## `ffiFaultTarget` raise a Defect on entry — the one mechanism that can force
## a failure inside ANY entry point without depending on a writer bug that
## happens to exist today. For each class, one entry point is failed and the
## caller-visible result is asserted:
##
##   * `void` writer calls (`register_step`, `register_call`,
##     `register_thread_switch`): `last_error` names the call, and
##     `trace_writer_close` returns failure naming it — the latch;
##   * `cint` writer calls (`register_path_with_line_count`): 1 and
##     `last_error`;
##   * id-returning writer calls, each with its own sentinel
##     (`ensure_function_id` → SIZE_MAX and latched, `register_path_version`
##     → UINT64_MAX, `register_source_reload` → 0, `register_source_view` →
##     -1);
##   * `trace_writer_close` itself — the BEAM case — returns 1;
##   * `trace_writer_free` (no handle survives it) does not crash;
##   * reader, meta.dat and value-encoder calls: a NULL handle, a failure
##     count or a -1 boolean, each with `last_error`.
##
## And the structural half: every `exportc` proc in the FFI module carries a
## guard, so an entry point added later without one fails this test rather
## than reintroducing the defect.
##
## THE MUTATION CHECK. Two further builds each remove one half of the guard,
## and the test MUST fail under both; the nimble `test` task runs them and
## refuses a zero exit:
##
##   * `-d:ffiGuardNoCatch` — the injected Defect escapes the entry point, as
##     it did before the guard existed;
##   * `-d:ffiGuardNoLatch` — a failed `void` call is no longer remembered, so
##     `trace_writer_close` reports success over the lost event.
##
## No mocks: real entry points, a real container, the real reader. The only
## artificial thing is the injected Defect, which stands in for the index and
## range errors that reach these entry points in practice.

include codetracer_trace_writer_ffi

{.pop.}


when not defined(ffiFaultInjection):
  {.error: "build this test with -d:ffiFaultInjection".}

const Program = "ffi_failures"

proc lastErr(): string =
  let e = trace_writer_last_error()
  if e.isNil: "" else: $e

proc arm(name: string) =
  ffiFaultTarget = name
  trace_writer_clear_last_error()

proc disarm() =
  ffiFaultTarget = ""

proc newWriter(table = false): TraceWriterHandle =
  result = trace_writer_new(cstring(Program), ffiBinary)
  doAssert result != nil, lastErr()
  doAssert trace_writer_begin_in_memory(result) == 0, lastErr()
  if table:
    doAssert trace_writer_enable_line_count_table(result) == 0, lastErr()
    doAssert trace_writer_register_path_with_line_count(
      result, "/src/a.ex", 10) == 0, lastErr()
  trace_writer_start(result, "/src/a.ex", 1)

proc expectCloseFails(h: TraceWriterHandle, naming: string) =
  let rc = trace_writer_close(h)
  let err = lastErr()
  doAssert rc != 0,
    "close must FAIL after " & naming & " failed; it returned success and " &
    "the recording would be incomplete with nothing reported"
  doAssert naming in err,
    "close's error must name the failed call " & naming & "; got: " & err
  trace_writer_free(h)

proc test_an_unmatched_return_is_a_notice_not_a_failure() =
  ## The one refusal that is NOT latched: a return with no matching call
  ## (a recorder that attached mid-execution sees returns from frames it
  ## never saw enter). It is reported in last_error, and close succeeds.
  let h = newWriter()
  trace_writer_register_step(h, "/src/a.ex", 2)
  trace_writer_register_return(h)
  trace_writer_register_return(h)  # the second one pops past the root
  doAssert "call stack underflow" in lastErr(), lastErr()
  doAssert trace_writer_close(h) == 0,
    "an unmatched return must not fail the close: " & lastErr()
  trace_writer_free(h)
  echo "PASS: test_an_unmatched_return_is_a_notice_not_a_failure"

proc test_void_writer_calls_latch_into_close() =
  for (name, call) in [
      ("trace_writer_register_step",
        proc (h: TraceWriterHandle) = trace_writer_register_step(h, "/src/a.ex", 2)),
      ("trace_writer_register_call",
        proc (h: TraceWriterHandle) = trace_writer_register_call(h, 0)),
      ("trace_writer_register_thread_switch",
        proc (h: TraceWriterHandle) = trace_writer_register_thread_switch(h, 3))]:
    let h = newWriter()
    arm(name)
    call(h)
    disarm()
    doAssert name in lastErr(), name & ": last_error must name it; got: " & lastErr()
    trace_writer_register_step(h, "/src/a.ex", 3)  # a later call succeeds…
    expectCloseFails(h, name)                      # …and close still fails
  echo "PASS: test_void_writer_calls_latch_into_close"

proc test_cint_writer_call_returns_failure() =
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert trace_writer_begin_in_memory(h) == 0
  doAssert trace_writer_enable_line_count_table(h) == 0
  arm("trace_writer_register_path_with_line_count")
  let rc = trace_writer_register_path_with_line_count(h, "/src/a.ex", 10)
  disarm()
  doAssert rc == 1, "a failing cint call returns 1; got " & $rc
  doAssert "trace_writer_register_path_with_line_count" in lastErr(), lastErr()
  trace_writer_free(h)
  echo "PASS: test_cint_writer_call_returns_failure"

proc test_id_returning_calls_return_their_sentinel() =
  block:
    let h = newWriter()
    arm("trace_writer_ensure_function_id")
    let id = trace_writer_ensure_function_id(h, "f", "/src/a.ex", 1)
    disarm()
    doAssert id == high(csize_t), "ensure_function_id fails with SIZE_MAX; got " & $id
    doAssert "trace_writer_ensure_function_id" in lastErr(), lastErr()
    # Callers do not check this sentinel, so it is latched as well.
    expectCloseFails(h, "trace_writer_ensure_function_id")
  block:
    let h = newWriter(table = true)
    arm("trace_writer_register_path_version")
    let id = trace_writer_register_path_version(h, "/src/a.ex", 12)
    disarm()
    doAssert id == CtTwInvalidPathId, "got " & $id
    doAssert "trace_writer_register_path_version" in lastErr(), lastErr()
    trace_writer_free(h)
  block:
    let h = newWriter(table = true)
    let v2 = trace_writer_register_path_version(h, "/src/a.ex", 12)
    var change = CtTwSourceReloadChange(old_path_id: 0, new_path_id: v2, generation: 2)
    arm("trace_writer_register_source_reload")
    let ordinal = trace_writer_register_source_reload(h,
      cast[ptr UncheckedArray[CtTwSourceReloadChange]](addr change), 1, 0)
    disarm()
    doAssert ordinal == CtTwInvalidReloadOrdinal, "got " & $ordinal
    doAssert "trace_writer_register_source_reload" in lastErr(), lastErr()
    trace_writer_free(h)
  block:
    let h = newWriter()
    arm("trace_writer_register_source_view")
    let view = trace_writer_register_source_view(h, 0, 1, "v", 1, nil, 0, nil, 0)
    disarm()
    doAssert view == -1, "got " & $view
    doAssert "trace_writer_register_source_view" in lastErr(), lastErr()
    trace_writer_free(h)
  echo "PASS: test_id_returning_calls_return_their_sentinel"

proc test_close_itself_failing_is_reported() =
  let h = newWriter()
  trace_writer_register_step(h, "/src/a.ex", 2)
  arm("trace_writer_close")
  let rc = trace_writer_close(h)
  disarm()
  doAssert rc == 1,
    "an error inside trace_writer_close must return 1; it returned " & $rc &
    " — the silent empty trace"
  doAssert "trace_writer_close" in lastErr(), lastErr()
  trace_writer_free(h)
  echo "PASS: test_close_itself_failing_is_reported"

proc test_free_does_not_crash() =
  let h = newWriter()
  arm("trace_writer_free")
  trace_writer_free(h)
  disarm()
  doAssert "trace_writer_free" in lastErr(), lastErr()
  echo "PASS: test_free_does_not_crash"

proc test_reader_meta_and_encoder_calls_fail_visibly() =
  # A real container to read.
  let h = newWriter()
  trace_writer_register_step(h, "/src/a.ex", 2)
  doAssert trace_writer_close(h) == 0, lastErr()
  let path = getTempDir() / "ffi_failures.ct"
  block:
    let n = int(trace_writer_container_len(h))
    var f = open(path, fmWrite)
    discard f.writeBuffer(trace_writer_container_ptr(h), n)
    f.close()
  trace_writer_free(h)

  arm("ct_reader_open")
  doAssert ct_reader_open(cstring(path)) == nil, "a failing open returns NULL"
  disarm()
  doAssert "ct_reader_open" in lastErr(), lastErr()

  let r = ct_reader_open(cstring(path))
  doAssert r != nil, lastErr()
  arm("ct_reader_has_column_aware_steps")
  doAssert ct_reader_has_column_aware_steps(r) == -1,
    "a failing boolean query answers -1, never a plausible 0 or 1"
  disarm()
  arm("ct_reader_step_locations")
  var ids, lines: array[4, uint64]
  doAssert ct_reader_step_locations(r, 0, 1, addr ids[0], addr lines[0]) == high(uint64)
  disarm()
  doAssert "ct_reader_step_locations" in lastErr(), lastErr()
  ct_reader_close(r)

  var bytes = @[byte 1, 2, 3]
  arm("ct_read_meta_dat")
  doAssert ct_read_meta_dat(addr bytes[0], csize_t(bytes.len)) == nil
  disarm()
  doAssert "ct_read_meta_dat" in lastErr(), lastErr()

  let enc = ct_value_encoder_new()
  arm("ct_value_write_int")
  doAssert ct_value_write_int(enc, 5, 0) == 1
  disarm()
  doAssert "ct_value_write_int" in lastErr(), lastErr()
  ct_value_encoder_free(enc)
  echo "PASS: test_reader_meta_and_encoder_calls_fail_visibly"

proc test_every_exported_proc_is_guarded() =
  const src = staticRead("../src/codetracer_trace_writer_ffi.nim")
  const unguarded = ["trace_writer_last_error", "trace_writer_clear_last_error",
    "trace_writer_build_config", "codetracer_trace_writer_init"]
  var missing: seq[string] = @[]
  var i = 0
  var seen = 0
  while true:
    let at = src.find("{.exportc", i)
    if at < 0: break
    let close = src.find(".}", at)
    let pragma = src[at ..< close]
    let procAt = src.rfind("proc ", last = at)
    var nameEnd = procAt + 5
    while nameEnd < src.len and src[nameEnd] in IdentChars: inc nameEnd
    let name = src[procAt + 5 ..< nameEnd]
    if "ffiGuard" notin pragma and name notin unguarded:
      missing.add(name)
    inc seen
    i = close
  doAssert seen > 100, "the scan found only " & $seen & " exported procs"
  doAssert missing.len == 0,
    "exported entry points without an ffiGuard pragma: " & missing.join(", ")
  echo "PASS: test_every_exported_proc_is_guarded"

test_void_writer_calls_latch_into_close()
test_an_unmatched_return_is_a_notice_not_a_failure()
test_cint_writer_call_returns_failure()
test_id_returning_calls_return_their_sentinel()
test_close_itself_failing_is_reported()
test_free_does_not_crash()
test_reader_meta_and_encoder_calls_fail_visibly()
test_every_exported_proc_is_guarded()
echo "ALL PASS: test_ffi_failures_reach_the_caller"
