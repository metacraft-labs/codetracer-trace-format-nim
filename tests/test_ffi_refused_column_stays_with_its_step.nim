## A column delta belongs to the step it was registered on, and a refused
## step does not hand it to the next one.
##
## `trace_writer_register_delta_column` folds its delta into the buffered
## step. On a line-only writer that step cannot carry a column, so its flush
## is refused when the host moves on, and the host's next step replaces it.
## The delta was left behind on the handle: every later step was flushed with
## it, refused in turn, and the rest of the recording was lost one step at a
## time.
##
## Asserted: after one refused step, the next steps are recorded at their own
## lines, and the close reports the one refusal.
##
## No mocks: the real C entry points, read back with this repository's
## reader.

include codetracer_trace_writer_ffi

{.pop.}

import codetracer_trace_writer/new_trace_reader as ntr

proc lastErr(): string =
  let e = trace_writer_last_error()
  if e.isNil: "" else: $e

let h = trace_writer_new(cstring("stale_column"), ffiBinary)
doAssert h != nil, lastErr()
doAssert trace_writer_begin_in_memory(h) == 0, lastErr()
trace_writer_register_step(h, "/src/a.py", 1)
trace_writer_register_step(h, "/src/a.py", 2)
trace_writer_register_delta_column(h, 4)
trace_writer_clear_last_error()
trace_writer_register_step(h, "/src/a.py", 3)
doAssert lastErr().len > 0, "the step that carried a column is refused"
trace_writer_clear_last_error()
trace_writer_register_step(h, "/src/a.py", 4)
doAssert lastErr().len == 0,
  "the step after the refused one is not refused for its column: " & lastErr()
trace_writer_register_step(h, "/src/a.py", 5)
doAssert lastErr().len == 0, lastErr()
doAssert trace_writer_close(h) != 0, "the refusal is reported at the close"
let n = int(trace_writer_container_len(h))
var bytes = newSeq[byte](n)
copyMem(addr bytes[0], trace_writer_container_ptr(h), n)
trace_writer_free(h)

var r = ntr.openNewTraceFromBytes(bytes).get()
doAssert r.stepCount().get() == 4,
  "steps 1, 3, 4 and 5 are recorded (2 was refused); got " & $r.stepCount().get()
var lines: seq[uint64]
for i in 0'u64 ..< 4:
  lines.add(r.stepAbsoluteGlobalLineIndex(i).get() + 1)
doAssert lines == @[1'u64, 3, 4, 5], $lines
echo "test_ffi_refused_column_stays_with_its_step: OK"
