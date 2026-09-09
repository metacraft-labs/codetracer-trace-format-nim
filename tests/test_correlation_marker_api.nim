## The shared correlation-marker API, end to end.
##
## `registerCorrelationMarker` is the entry point every CTFS recorder binds to
## (`Correlation-Markers.md` §2.4). One declaration must produce BOTH halves,
## and this asserts both, because either alone is a silent half-failure:
##
## 1. A `MarkerPayload` in an IO event's metadata — where the debugger's
##    cross-process origin walk and `ct print` already look.
## 2. An entry in `corrmark.ns` — so a consumer can find it by lookup rather
##    than by decoding the event stream.
##
## The intended check for the payload half is `ct print --json-events`, the
## CANONICAL decoder: `addEventMetadata` already hoists `correlation_marker` /
## `boundary_id` / `direction` / `key_value` to the top level, which would make
## it the conformance target for every future binding too.
##
## THAT ROUTE IS BLOCKED BY A PRE-EXISTING DEFECT, not by the marker work, so
## this asserts the payload reached `events.dat` structurally instead.
## Measured: a `MultiStreamTraceWriter` container closes with `meta.dat` at
## **size 0** — with or without any marker — so `ct print` reads an empty
## program, every capability flag false, and gates all four stream counts to
## "(unavailable)" even though `events.dat`, `steps.dat` and `values.dat` all
## carry bytes. Reproduce with `ct-print --full` on any container this writer
## produces. Until that is fixed, no `ct print`-based conformance test can see
## a marker, which is worth knowing before someone writes one and concludes
## the recorder is at fault.

import std/os
import results
import ../src/codetracer_ctfs/container
import ../src/codetracer_trace_writer/multi_stream_writer
import ../src/codetracer_trace_writer/corrmark_builder

proc u32le(data: openArray[byte], off: int): uint32 =
  for i in 0 ..< 4:
    result = result or (uint32(data[off + i]) shl (i * 8))

proc readNamespace(data: seq[byte]): Result[seq[byte], string] =
  ## Read `corrmark.ns` straight out of the container, the way a consumer
  ## would: block size at offset 8, max root entries at offset 12.
  readInternalFile(data, CorrmarkNamespaceName, u32le(data, 8), u32le(data, 12))

proc recordTrace(path: string) =
  var w = initMultiStreamWriter(path, "marker_demo", chunkSize = 4,
    recordingId = "01949fcc-7d92-7e9c-aaaa-cccccccccccc").get()
  doAssert w.registerPath("/src/app.rb").isOk
  doAssert w.registerStep(0, 1, []).isOk
  # The NUMERIC path is primary: intern once, then pass the id. This is the
  # shape a binding uses on a hot path, so it is the shape the test exercises.
  let mid = w.ensureMarkerId("order-processing").get()
  doAssert w.registerCorrelationMarkerById(
    "send", mid, "order-processing", "order-42", "the order body",
    "Outbound order").isOk
  doAssert w.registerStep(0, 2, []).isOk
  # The string form is a WRAPPER over the numeric one and must intern to the
  # same id — if it did not, the two APIs would index into different buckets.
  doAssert w.ensureMarkerId("order-processing").get() == mid,
    "interning must be idempotent"
  # "receive" must normalise to "recv": an unrecognised spelling would be an
  # unpairable marker, so the API picks a side rather than erroring.
  doAssert w.registerCorrelationMarker(
    "receive", "order-processing", "order-42").isOk
  doAssert w.close().isOk

proc test_marker_payload_reaches_the_event_stream() =
  ## The payload half: a declared marker must land in `events.dat`, which is
  ## where `ct print` and the db-backend's origin walk read it from.
  ##
  ## Asserted structurally (the stream exists and grew) rather than by
  ## decoding, because the canonical decoder cannot currently be reached — see
  ## the module header. A stronger assertion belongs here the moment it can be.
  let path = getTempDir() / "test_corrmark_api.ct"
  removeFile(path)
  recordTrace(path)

  let data = readCtfsFromFile(path).get()
  let events = readInternalFile(data, "events.dat", u32le(data, 8), u32le(data, 12))
  doAssert events.isOk, "events.dat missing: " & events.error
  doAssert events.get().len > 0,
    "a declared marker must reach events.dat, which is empty"

  removeFile(path)
  echo "PASS: test_marker_payload_reaches_the_event_stream"

proc test_marker_is_indexed_in_corrmark_ns() =
  let path = getTempDir() / "test_corrmark_api_idx.ct"
  removeFile(path)
  recordTrace(path)

  let data = readCtfsFromFile(path).get()
  let ns = readNamespace(data)
  doAssert ns.isOk, "corrmark.ns missing from the container: " & ns.error

  var idx = openCorrmarkIndex(ns.get()).get()
  # Label id 0 is the first interned label, "order-processing".
  let hits = idx.lookupBoundary(0'u64, "order-42")
  doAssert hits.isOk, "lookup failed: " & hits.error
  doAssert hits.get().len == 2,
    "expected both sides of the crossing, got " & $hits.get().len
  doAssert hits.get()[0].markerIdOf() == 0'u64, "marker id must round-trip"

  # A key that was never declared is a clean miss, not an error — and NOT the
  # same answer as the namespace being absent (contract §9).
  let miss = idx.lookupBoundary(0'u64, "order-999")
  doAssert miss.isOk and miss.get().len == 0, "expected a clean miss"

  # A different boundary must not resolve to this one's markers.  Under
  # interning the index key is `marker_id (8 bytes) || key_value`, so the
  # ambiguity the old `boundary || NUL || key` separator guarded against is
  # gone by construction: the split point is always byte 8.
  let other = idx.lookupBoundary(7'u64, "order-42")
  doAssert other.isOk and other.get().len == 0,
    "a different boundary id must not resolve to this one's markers"

  removeFile(path)
  echo "PASS: test_marker_is_indexed_in_corrmark_ns"

proc test_absence_is_distinguishable() =
  ## A recording that declares no marker writes NO namespace. That absence is
  ## the "never indexed" state, which a consumer must not report as "does not
  ## cover that span". If this writes an empty index instead, the two states
  ## become indistinguishable and the contract's §9 property is lost.
  let path = getTempDir() / "test_corrmark_api_none.ct"
  removeFile(path)
  var w = initMultiStreamWriter(path, "no_markers", chunkSize = 4,
    recordingId = "01949fcc-7d92-7e9c-aaaa-dddddddddddd").get()
  doAssert w.registerPath("/src/app.rb").isOk
  doAssert w.registerStep(0, 1, []).isOk
  doAssert w.close().isOk

  let data = readCtfsFromFile(path).get()
  let ns = readNamespace(data)
  doAssert ns.isErr,
    "a recording with no markers must not write corrmark.ns at all"

  removeFile(path)
  echo "PASS: test_absence_is_distinguishable"

when isMainModule:
  test_marker_payload_reaches_the_event_stream()
  test_marker_is_indexed_in_corrmark_ns()
  test_absence_is_distinguishable()
  echo "=== correlation marker API tests passed ==="
