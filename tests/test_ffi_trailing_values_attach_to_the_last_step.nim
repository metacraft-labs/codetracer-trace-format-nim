## Values still staged when a recording ends attach to the last STEP, even when
## exec records that are not steps (thread switches) came after it.
##
## `trace-events.md` §"Where the recording ends with values still staged": the
## values go to the last step the writer emitted, and no step is added for
## them. A thread switch occupies an exec-record index and owns a value record,
## but it is not a step: a value written into its record sits at an index
## `variables_at` never answers for, so the value is written and then never
## readable — the failure the spec warns about for `DeltaColumn`.
##
## What is asserted here, and why each one can fail:
##
##   1. With one thread switch between the last step and the close, the staged
##      value is in the step's record, beside the value the step already had,
##      and the switch's record is empty. A writer that amends "the last record"
##      puts it in the switch's record instead.
##   2. The same with more thread switches after the step than a value chunk
##      holds, so the step's record is in an earlier chunk than the close. A
##      writer that can only amend the chunk it is still filling loses the
##      value, or puts it in the wrong record.
##   3. The step count is unchanged: no step is added to carry the value.
##
## No mocks: the container is produced by the real C entry points and read
## back through this repository's own reader.

include codetracer_trace_writer_ffi

{.pop.}

import codetracer_trace_writer/new_trace_reader as ntr

const Program = "ffi_trailing_values"

proc ffiLastError(): string =
  let e = trace_writer_last_error()
  if e.isNil: "" else: $e

proc record(switchesAfterLastStep: int): seq[byte] =
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert h != nil, ffiLastError()
  doAssert trace_writer_begin_in_memory(h) == 0, ffiLastError()
  let intType = trace_writer_ensure_type_id(h, FfiTypeKind(7), "i64")
  doAssert trace_writer_register_variable_name(h, "own") == 0
  doAssert trace_writer_register_variable_name(h, "trailing") == 1
  trace_writer_register_step(h, "/src/a.nr", 1)
  trace_writer_register_step(h, "/src/a.nr", 2)
  trace_writer_register_variable_int_by_type_id(h, "own", 1, intType)
  for k in 0 ..< switchesAfterLastStep:
    trace_writer_register_thread_switch(h, uint64(k mod 3))
  # No step is open: the switch closed it. This value is staged, and no step
  # follows it.
  trace_writer_register_variable_int_by_type_id(h, "trailing", 2, intType)
  doAssert trace_writer_close(h) == 0, "trace_writer_close: " & ffiLastError()
  let n = int(trace_writer_container_len(h))
  result = newSeq[byte](n)
  copyMem(addr result[0], trace_writer_container_ptr(h), n)
  trace_writer_free(h)

proc names(r: var NewTraceReader, n: uint64): seq[uint64] =
  for v in r.values(n).get():
    result.add(v.varnameId)

proc check(switches: int) =
  var r = ntr.openNewTraceFromBytes(record(switches)).get()
  let total = r.stepCount().get()
  doAssert total == uint64(2 + switches),
    $switches & " switches: two steps and one record per switch, and no step " &
    "added for the trailing value; got " & $total & " exec records"
  doAssert r.names(0) == newSeq[uint64](),
    $switches & " switches: step 0 has no values; got " & $r.names(0)
  doAssert r.names(1) == @[0'u64, 1'u64],
    $switches & " switches: the trailing value belongs to the last step (1), " &
    "beside the value it already had; got " & $r.names(1)
  for k in 0 ..< switches:
    let idx = uint64(2 + k)
    doAssert r.names(idx).len == 0,
      $switches & " switches: a thread switch's record carries no values; " &
      "record " & $idx & " has " & $r.names(idx)
  echo "PASS: trailing value after ", switches, " thread switch(es)"

check(1)
check(DefaultValuesChunkSize + 3)
