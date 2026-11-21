## `ct_assignment` takes its right-hand side's kind and its pass-by mode as
## integers. One outside the enumeration is refused, naming it, and the
## writer goes on: it was converted into the enumeration unchecked, and the
## case over it took no branch — an illegal instruction that killed the
## recorded process.
##
## No mocks: the real C entry points.
## reader.

include codetracer_trace_writer_ffi

{.pop.}

import std/strutils

proc lastErr(): string =
  let e = trace_writer_last_error()
  if e.isNil: "" else: $e

let h = trace_writer_new(cstring("assign_kinds"), ffiBinary)
doAssert h != nil, lastErr()
doAssert trace_writer_begin_in_memory(h) == 0, lastErr()
trace_writer_register_step(h, "/src/a.py", 1)
trace_writer_clear_last_error()
ct_assignment(h, "t", FfiPassBy.Value, cast[FfiRValueKind](9), 0, nil, 0, nil, 0, 0)
doAssert "9" in lastErr(), "an unknown RValue kind is refused, naming it: " & lastErr()
trace_writer_clear_last_error()
ct_assignment(h, "t", cast[FfiPassBy](5), FfiRValueKind.Literal, 0, nil, 0, nil, 0, 0)
doAssert "5" in lastErr(), "an unknown pass-by mode is refused, naming it: " & lastErr()
trace_writer_clear_last_error()
ct_assignment(h, "t", FfiPassBy.Value, FfiRValueKind.Literal, 0, nil, 0, nil, 0, 0)
doAssert lastErr().len == 0, lastErr()
trace_writer_register_step(h, "/src/a.py", 2)
doAssert trace_writer_close(h) != 0, "the refusals are reported at the close"
trace_writer_free(h)
echo "test_ffi_assignment_refuses_unknown_kinds: OK"
