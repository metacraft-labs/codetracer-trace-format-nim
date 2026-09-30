## A call registered through the C ABI begins at the NEXT step, whatever
## steps are still pending when it arrives.
##
## `calls.dat`'s `first_step_id` is the "First step in this call"
## (`trace-events.md` §"Call Stream Records (`calls.dat`)"), and the writer's
## convention is the next-step one: the call's range starts at the first step
## the callee's body emits. The C ABI holds the most recent step pending, and
## `trace_writer_register_call` flushed it only once some step had already
## been flushed. So while nothing had been flushed yet — right after
## `trace_writer_start`, whose entry step is itself pending, and in a
## recording that never called `start` — the call was opened before the
## pending step was written, and that step (the `<toplevel>` entry step, or
## the caller's own line) became the call's first step. The pure-Rust writer,
## for the same calls, puts the entry on the next step.
##
## Asserted: in both situations the call's `entryStep` is the step the callee
## emitted, not the one pending before the call.
##
## No mocks: the real C entry points, read back with this repository's
## reader.

include codetracer_trace_writer_ffi

{.pop.}

import codetracer_trace_writer/new_trace_reader as ntr

proc lastErr(): string =
  let e = trace_writer_last_error()
  if e.isNil: "" else: $e

proc record(withStart: bool): seq[byte] =
  let h = trace_writer_new(cstring("call_entry"), ffiBinary)
  doAssert h != nil, lastErr()
  doAssert trace_writer_begin_in_memory(h) == 0, lastErr()
  if withStart:
    trace_writer_start(h, "/src/main.ex", 1)
  else:
    trace_writer_register_step(h, "/src/main.ex", 1)
  let f = trace_writer_ensure_function_id(h, "f", "/src/f.ex", 3)
  # The call arrives while the previous step is still pending.
  trace_writer_register_call(h, f)
  trace_writer_register_step(h, "/src/f.ex", 3)
  trace_writer_register_step(h, "/src/f.ex", 4)
  trace_writer_register_return(h)
  doAssert trace_writer_close(h) == 0, lastErr()
  let n = int(trace_writer_container_len(h))
  result = newSeq[byte](n)
  copyMem(addr result[0], trace_writer_container_ptr(h), n)
  trace_writer_free(h)

proc check(withStart: bool) =
  var r = ntr.openNewTraceFromBytes(record(withStart)).get()
  # With `start`, call 0 is `<toplevel>` and `f` is call 1.
  let key = if withStart: 1'u64 else: 0'u64
  let c = r.call(key).get()
  # Steps: 0 = main.ex:1 (the entry step, or the caller's line), 1 = f.ex:3.
  doAssert c.entryStep == 1,
    "f's call must begin at its own first step (1, f.ex:3), not at the step " &
    "that was pending when it was registered (withStart=" & $withStart &
    "); its entry step is " & $c.entryStep
  doAssert c.exitStep == 2, "f ends at f.ex:4 (step 2); got " & $c.exitStep
  echo "PASS: withStart=", withStart

check(withStart = true)
check(withStart = false)
echo "ALL PASS: test_ffi_call_entry_is_the_next_step"
