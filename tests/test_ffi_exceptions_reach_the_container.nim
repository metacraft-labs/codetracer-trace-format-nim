## Exceptions written through the C ABI reach the container: a Raise and a
## Catch in the step stream, with the exception type and message given, and a
## call that exits by an exception carrying it, as CBOR, in its call record.
##
## What is asserted, and why each can fail:
##
##   1. `trace_writer_register_raise` writes a Raise event with the type id and
##      the message bytes; a writer that drops either, or writes the event as a
##      step, reads back differently.
##   2. `trace_writer_register_catch` writes a Catch event with the type id.
##   3. `trace_writer_register_return_exception` closes the innermost call with
##      the exception's CBOR in its record and no return value; the enclosing
##      call, closed by `trace_writer_register_return`, carries none.
##   4. A raise with a NULL message of non-zero length, and a return by an empty
##      exception, are refused by name through `trace_writer_last_error`, and
##      latched for `trace_writer_close`.
##
## No mocks: the container is produced by the real C entry points and read
## back through this repository's own reader.

include codetracer_trace_writer_ffi

{.pop.}

import std/strutils
import codetracer_trace_writer/new_trace_reader as ntr

const Program = "ffi_exceptions"

proc ffiLastError(): string =
  let e = trace_writer_last_error()
  if e.isNil: "" else: $e

proc open(): TraceWriterHandle =
  result = trace_writer_new(cstring(Program), ffiBinary)
  doAssert result != nil, ffiLastError()
  doAssert trace_writer_begin_in_memory(result) == 0, ffiLastError()

proc containerOf(h: TraceWriterHandle): seq[byte] =
  doAssert trace_writer_close(h) == 0, "trace_writer_close: " & ffiLastError()
  let n = int(trace_writer_container_len(h))
  result = newSeq[byte](n)
  copyMem(addr result[0], trace_writer_container_ptr(h), n)
  trace_writer_free(h)

let exceptionCbor = @[0xA1'u8, 0x64, byte('k'), byte('i'), byte('n'), byte('d'),
  0x65, byte('E'), byte('r'), byte('r'), byte('o'), byte('r')]

block raise_catch_and_a_call_that_exits_by_an_exception:
  let h = open()
  let errType = trace_writer_ensure_type_id(h, FfiTypeKind(7), "ValueError")
  let outer = trace_writer_ensure_function_id(h, "outer", "/src/a.py", 1)
  let inner = trace_writer_ensure_function_id(h, "inner", "/src/a.py", 5)
  trace_writer_register_step(h, "/src/a.py", 1)
  trace_writer_register_call(h, outer)
  trace_writer_register_step(h, "/src/a.py", 2)
  trace_writer_register_call(h, inner)
  trace_writer_register_step(h, "/src/a.py", 6)
  let message = "boom"
  trace_writer_register_raise(h, uint64(errType),
    cast[ptr uint8](unsafeAddr message[0]), csize_t(message.len))
  trace_writer_register_return_exception(h, unsafeAddr exceptionCbor[0],
    csize_t(exceptionCbor.len))
  trace_writer_register_catch(h, uint64(errType))
  trace_writer_register_step(h, "/src/a.py", 3)
  trace_writer_register_return(h)
  var r = ntr.openNewTraceFromBytes(containerOf(h)).get()

  var raises, catches: seq[StepEvent]
  for i in 0'u64 ..< r.stepCount().get():
    let ev = r.step(i).get()
    case ev.kind
    of sekRaise: raises.add(ev)
    of sekCatch: catches.add(ev)
    else: discard
  doAssert raises.len == 1, "one Raise, got " & $raises.len
  doAssert raises[0].exceptionTypeId == uint64(errType)
  doAssert raises[0].message == cast[seq[byte]](message), $raises[0].message
  doAssert catches.len == 1 and catches[0].catchExceptionTypeId == uint64(errType)

  doAssert r.callCount().get() == 2
  let outerRec = r.call(0).get()
  let innerRec = r.call(1).get()
  doAssert outerRec.functionId == uint64(outer) and innerRec.functionId == uint64(inner)
  doAssert innerRec.exception == exceptionCbor, $innerRec.exception
  doAssert outerRec.exception.len == 0, $outerRec.exception
  echo "PASS: raise_catch_and_a_call_that_exits_by_an_exception"

proc expectRefused(h: TraceWriterHandle, what: string) =
  doAssert what in ffiLastError(), what & ": " & ffiLastError()
  doAssert trace_writer_close(h) != 0, what & ": close did not fail"
  trace_writer_free(h)

block refusals_are_named_and_latched:
  block:
    let h = open()
    trace_writer_register_raise(h, 1, nil, 3)
    expectRefused(h, "trace_writer_register_raise")
  block:
    let h = open()
    let f = trace_writer_ensure_function_id(h, "f", "/src/a.py", 1)
    trace_writer_register_step(h, "/src/a.py", 1)
    trace_writer_register_call(h, f)
    trace_writer_register_return_exception(h, nil, 0)
    expectRefused(h, "trace_writer_register_return_exception")
  echo "PASS: refusals_are_named_and_latched"
