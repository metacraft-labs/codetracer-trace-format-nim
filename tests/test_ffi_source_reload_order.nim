## A source reload marker written through the C ABI lands AFTER every step
## registered before it.
##
## The C ABI holds the most recent step pending until the next event, so that
## values registered after it can join its record. Every other exec record the
## C ABI writes — a thread switch, a thread start or exit — flushes that
## pending step first. A reload marker that did not would be written BEFORE
## the step the host registered ahead of it, and the container would state a
## boundary the execution did not have: the step that ran the old version
## would appear to run after the reload (`trace-events.md` §"Source Reload
## Marker (Tag 0x08)").
##
## Asserted here: the marker's exec index is exactly one past the last step
## registered before it, and the step registered after it follows it.
##
## No mocks: the container is produced by the real C entry points and read
## back with this repository's own reader.

include codetracer_trace_writer_ffi

{.pop.}

import codetracer_trace_writer/new_trace_reader as ntr

const
  Program = "ffi_source_reload_order"
  PathA = "/srv/game.gd"
  CountA = 10'u64

proc ffiLastError(): string =
  let e = trace_writer_last_error()
  if e.isNil: "" else: $e

proc test_the_marker_follows_the_step_registered_before_it() =
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert h != nil, "trace_writer_new failed: " & ffiLastError()
  doAssert trace_writer_begin_in_memory(h) == 0, ffiLastError()
  doAssert trace_writer_enable_line_count_table(h) == 0, ffiLastError()
  doAssert trace_writer_declare_source_reload(h) == 0, ffiLastError()
  doAssert trace_writer_register_path_with_line_count(
    h, cstring(PathA), CountA) == 0, ffiLastError()
  trace_writer_start(h, cstring(PathA), 1)
  # This step is still PENDING in the C ABI when the reload arrives.
  trace_writer_register_step(h, cstring(PathA), 3)

  let v2 = trace_writer_register_path_version(h, cstring(PathA), 12)
  doAssert v2 != CtTwInvalidPathId, ffiLastError()
  var change = CtTwSourceReloadChange(old_path_id: 0, new_path_id: v2,
    generation: 2)
  let ordinal = trace_writer_register_source_reload(h,
    cast[ptr UncheckedArray[CtTwSourceReloadChange]](addr change), 1, 0)
  doAssert ordinal == 1, "register_source_reload: " & ffiLastError()
  trace_writer_register_step(h, cstring(PathA), 11)
  doAssert trace_writer_close(h) == 0, "trace_writer_close: " & ffiLastError()

  let n = int(trace_writer_container_len(h))
  var bytes = newSeq[byte](n)
  copyMem(addr bytes[0], trace_writer_container_ptr(h), n)
  trace_writer_free(h)

  var r = ntr.openNewTraceFromBytes(bytes).get()
  let markers = r.sourceReloads().get()
  doAssert markers.len == 1, "expected one marker, got " & $markers.len
  # Exec records: 0 = the entry step from `start`, 1 = line 3, 2 = the
  # marker, 3 = line 11 of the new version.
  doAssert markers[0].stepIndex == 2,
    "the marker must follow the step registered before it (exec index 2); " &
    "it is at " & $markers[0].stepIndex & ", ahead of a step that ran the " &
    "old version"
  let logical = r.logicalStepCount().get()
  doAssert logical == 3,
    "three steps and one marker; logical step count was " & $logical

  echo "PASS: test_the_marker_follows_the_step_registered_before_it"

test_the_marker_follows_the_step_registered_before_it()
echo "ALL PASS: test_ffi_source_reload_order"
