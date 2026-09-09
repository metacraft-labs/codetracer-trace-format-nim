## The C ABI's recording-id setter: a caller that already has an identity can
## use it instead of one the writer mints.
##
## Every constructor on this ABI resolved the recording id itself — provided
## when non-empty, freshly minted otherwise — and the C ABI had no way to
## provide one. For a host with a real entropy source that is a preference.
## For one without, it is a correctness problem: on `wasm32-unknown-unknown`
## there is no CSPRNG and no wall clock, so `newUuidV7` mints from whatever the
## host stubs answer, and a constant answer means every recording a page
## produces carries the SAME id and collides with every other one in a trace
## store. Such a host has an identity already; this is how it says so.
##
## What is asserted here, and why each one can fail:
##
##   1. The id a caller pins is the id `meta.dat` carries, read back through
##      this repository's own byte-taking reader.
##   2. **Its own CONTROL, and the assertion above is worthless without it:**
##      a writer that does NOT call the setter gets a DIFFERENT id, and a valid
##      one. Without this arm, assertion 1 would still pass if the setter were
##      a no-op and the pinned id happened to be what the stubbed clock and
##      entropy mint — which, on a host with constant stubs, is exactly the
##      situation. It is the arm that says the setter DID something.
##   3. An id that is not a canonical UUIDv7 is refused BY NAME, so a caller
##      cannot put a string in `meta.dat` through this door that
##      `ct_write_meta_dat` would refuse through its own.
##   4. An EMPTY id is refused rather than silently treated as "mint one". A
##      caller passing an empty id is a caller whose identity is missing, and
##      answering that with a fresh UUID hides it.
##   5. Setting it AFTER the writer is begun is refused. The constructors have
##      already resolved an identity by then, so accepting one here would mean
##      rewriting a container's id rather than choosing it — and the caller
##      would have no way to tell which of the two it got.
##   6. The refusals are refusals, not silent successes: the writer that
##      refused in 3, 4 and 5 still produces a container, and that container
##      carries an id that is NOT the string it refused.

# Include the FFI module so the C entry points can be driven directly.
# Mirrors tests/test_ffi_in_memory.nim.
include codetracer_trace_writer_ffi

# Drop the `raises: []` push from the FFI module so the test body can use
# higher-level helpers.
{.pop.}

import std/strutils

const
  Program = "recording_id_probe"
  AppPath = "/srv/probe.js"
  PinnedId = "01890a5d-ac96-774b-bcce-b302099a8057"

proc drive(h: TraceWriterHandle) =
  trace_writer_start(h, cstring(AppPath), 1'i64)
  for ln in 1'i64 .. 3'i64:
    trace_writer_register_step(h, cstring(AppPath), ln)

proc containerOf(h: TraceWriterHandle): string =
  let n = int(trace_writer_container_len(h))
  if n == 0:
    return ""
  result = newString(n)
  copyMem(addr result[0], trace_writer_container_ptr(h), n)

proc recordingIdOf(container: string, note: string): string =
  ## The id `meta.dat` carries, read back through this repository's own reader.
  ## Read from the container's BYTES rather than from a file, because that is
  ## the constructor the embedder this setter exists for has.
  var data = newSeq[byte](container.len)
  if container.len > 0:
    copyMem(addr data[0], unsafeAddr container[0], container.len)
  let r = openNewTraceFromBytes(data)
  doAssert r.isOk, "openNewTraceFromBytes(" & note & ") failed: " & r.error
  result = r.get().meta.recordingId
  doAssert result.len > 0,
    "the " & note & " container carries no recording id at all"

proc buildInMemory(pin: string, note: string): string =
  ## One container. `pin` is set when non-empty; nothing else differs.
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert h != nil, "trace_writer_new failed: " & $trace_writer_last_error()
  if pin.len > 0:
    doAssert trace_writer_set_recording_id(h, cstring(pin)) == 0,
      "set_recording_id(" & pin & ") was refused: " & $trace_writer_last_error()
  doAssert trace_writer_begin_in_memory(h) == 0,
    "begin_in_memory failed: " & $trace_writer_last_error()
  drive(h)
  doAssert trace_writer_close(h) == 0,
    "close failed: " & $trace_writer_last_error()
  result = containerOf(h)
  doAssert result.len > 0, "the " & note & " arm produced no container"
  trace_writer_free(h)

removeDir(getTempDir() / "ct_ffi_recording_id")

# 1. The pinned id is the id the container carries.
let pinned = recordingIdOf(buildInMemory(PinnedId, "pinned"), "pinned")
doAssert pinned == PinnedId,
  "the container's recording id is " & pinned & ", not the pinned " & PinnedId

# 2. THE CONTROL. Without the setter the id is a different one, and a valid
#    one — so assertion 1 measured the setter rather than agreeing with a
#    default that happened to match.
let minted = recordingIdOf(buildInMemory("", "minted"), "minted")
doAssert minted != PinnedId,
  "a writer that never called set_recording_id produced the pinned id " &
  minted & " anyway, so assertion 1 cannot tell a working setter from a no-op"
doAssert validateRecordingIdStr(minted).isOk,
  "the minted id " & minted & " is not a canonical UUIDv7"

# 3, 4, 5. The three refusals, each by name, each on a writer that still works.
proc refusalOf(body: proc(h: TraceWriterHandle): cint,
               note: string): tuple[msg, id: string] =
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert h != nil, "trace_writer_new failed: " & $trace_writer_last_error()
  doAssert body(h) == 1.cint,
    "set_recording_id ACCEPTED the " & note & " case, which it must refuse"
  let msg = $trace_writer_last_error()
  doAssert msg.len > 0, "the " & note & " refusal named no reason"
  # 6. The refusal did not break the writer, and the container it goes on to
  #    produce does not carry the string that was refused. `begin_in_memory` is
  #    idempotent, so the after-begin arm — which has already begun — passes
  #    through it unchanged rather than needing its own branch.
  doAssert trace_writer_begin_in_memory(h) == 0,
    "the writer that refused the " & note & " case would not begin: " &
    $trace_writer_last_error()
  drive(h)
  doAssert trace_writer_close(h) == 0,
    "the writer that refused the " & note & " case then failed to close: " &
    $trace_writer_last_error()
  let id = recordingIdOf(containerOf(h), "after-" & note)
  doAssert validateRecordingIdStr(id).isOk,
    "after the " & note & " refusal the container's id " & id &
    " is not a canonical UUIDv7"
  doAssert id != PinnedId,
    "after the " & note & " refusal the container carries the id the call " &
    "handed over, so the refusal was a refusal in its return value only"
  trace_writer_free(h)
  (msg, id)

let notUuid = refusalOf(proc(h: TraceWriterHandle): cint =
  trace_writer_set_recording_id(h, cstring("not-a-uuid")), "non-UUID")
doAssert "UUIDv7" in notUuid.msg,
  "the non-UUID refusal said '" & notUuid.msg & "', which does not name the reason"

let empty = refusalOf(proc(h: TraceWriterHandle): cint =
  trace_writer_set_recording_id(h, cstring("")), "empty")
doAssert "empty" in empty.msg,
  "the empty-id refusal said '" & empty.msg & "', which does not name the reason"

let late = refusalOf(proc(h: TraceWriterHandle): cint =
  doAssert trace_writer_begin_in_memory(h) == 0,
    "begin_in_memory failed: " & $trace_writer_last_error()
  trace_writer_set_recording_id(h, cstring(PinnedId)), "after-begin")
doAssert "already open" in late.msg,
  "the after-begin refusal said '" & late.msg & "', which does not name the reason"

echo "test_ffi_recording_id: OK"
echo "  pinned  = ", pinned
echo "  minted  = ", minted
