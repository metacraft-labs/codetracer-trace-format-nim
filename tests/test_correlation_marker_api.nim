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
## The payload half is checked through `ct print`'s OWN decoder — the same
## `buildFullDocument` / `addEventMetadata` pair the `--full` and `--events`
## modes run — so this is the conformance target every future binding can
## reuse: a marker that decodes here decodes in the tool users run. It is
## reached in-process via `codetracer_ct_print_lib` rather than by shelling
## out, because the `ct-print` binary at the repo root is a build artifact
## that can be older than the checkout (that stale artifact has already
## produced one false finding in this area).
##
## An earlier revision of this header recorded that the `ct print` route was
## "blocked by a pre-existing defect": a container whose `meta.dat` read back
## at size 0, making the whole recording decode as an empty program. That was
## real, and it is now fixed and pinned by
## `tests/test_close_publishes_entry_sizes.nim`. The diagnosis in that note
## was wrong, though, and the correction is worth keeping: the meta bytes WERE
## written. What was missing was the publication of block 0's entry-size
## array, which `writeToFile` only updates in memory — so the file written
## LAST always read back empty, and `meta.dat` is always written last. The
## trigger was reading the container after `close()` without `closeCtfs()`,
## which this test used to do.

import std/[os, strutils]
import results
import ../src/codetracer_ctfs/container
import ../src/codetracer_trace_writer/multi_stream_writer
import ../src/codetracer_trace_writer/corrmark_builder
import ../src/codetracer_trace_writer/new_trace_reader

# ct-print's own JSON rendering, so the assertion below is made against the
# canonical decoder rather than a second one written for the test.  Same
# mechanism `tests/test_ct_print_full.nim` uses.
include ../src/codetracer_ct_print_lib

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
  doAssert w.closeCtfs().isOk

proc test_marker_payload_decodes_through_ct_print() =
  ## The payload half, through the CANONICAL decoder.
  ##
  ## A declared marker must come back out of `ct print` with
  ## `correlation_marker` / `boundary_id` / `direction` / `key_value` hoisted
  ## to the top level of the event — that hoisting is what the debugger's
  ## cross-process origin walk and every consumer downstream key on. Asserting
  ## the bytes merely reached `events.dat` would pass on a payload whose field
  ## names had drifted, which is precisely the failure this shared API exists
  ## to prevent: such a marker is invisible, not broken, and nothing reports an
  ## error.
  let path = getTempDir() / "test_corrmark_api.ct"
  removeFile(path)
  recordTrace(path)

  var readerRes = openNewTrace(path)
  doAssert readerRes.isOk, "ct print could not open the recording: " &
    readerRes.error
  var reader = readerRes.get()
  let doc = buildFullDocument(reader, FullOpts(stripPaths: false))

  doAssert doc["metadata"]["program"].getStr() == "marker_demo",
    "the recording decoded with an empty program: " & $doc["metadata"]

  var markers: seq[JsonNode] = @[]
  for ev in doc["events"]:
    if isCorrelationMarker(ev):
      markers.add(ev)

  doAssert markers.len == 2,
    "expected both declared markers in ct print's output, got " &
    $markers.len

  doAssert markers[0]["boundary_id"].getStr() == "order-processing"
  doAssert markers[0]["direction"].getStr() == "send"
  doAssert markers[0]["key_value"].getStr() == "order-42"
  doAssert markers[0]["correlation_marker"]["show_value"].getStr() ==
    "the order body"
  doAssert markers[0]["correlation_marker"]["description"].getStr() ==
    "Outbound order"
  doAssert markers[0]["correlation_marker"]["marker_id"].getInt() == 0

  # "receive" normalises to "recv" — an unrecognised spelling would produce an
  # unpairable marker, so the API picks a side rather than erroring.
  doAssert markers[1]["direction"].getStr() == "recv",
    "'receive' must normalise to 'recv', got " &
    markers[1]["direction"].getStr()

  removeFile(path)
  echo "PASS: test_marker_payload_decodes_through_ct_print"

proc test_marker_mints_no_step() =
  ## Contract §11a.6: a marker attaches to the enclosing step and mints none
  ## of its own. Minting one would insert an exec-stream event no user code
  ## executed and shift every later step index — the indices spans'
  ## `start_step` / `end_step` are measured in.
  let path = getTempDir() / "test_corrmark_api_steps.ct"
  removeFile(path)
  recordTrace(path)

  var reader = openNewTrace(path).get()
  let doc = buildFullDocument(reader, FullOpts(stripPaths: false))
  doAssert doc["counts"]["steps"].getInt() == 2,
    "recordTrace registers exactly 2 steps around 2 markers; ct print sees " &
    $doc["counts"]["steps"].getInt() & " — a marker minted a step"

  removeFile(path)
  echo "PASS: test_marker_mints_no_step"

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
  doAssert w.closeCtfs().isOk

  let data = readCtfsFromFile(path).get()
  let ns = readNamespace(data)
  doAssert ns.isErr,
    "a recording with no markers must not write corrmark.ns at all"

  removeFile(path)
  echo "PASS: test_absence_is_distinguishable"

const
  # The M25 observability corpus's own ids, so the shape this asserts is the
  # shape the cross-repo consumer queries with.
  DemoTraceIdHex = "6f92f3577b34da6a3ce929d0e0e4ab14"
  DemoSpanIdHex = "51000000000025aa"
  DemoWallNs = 1788897919650415375'u64
  DemoMonotonicNs = 2052008038067771'u64

proc recordSpanCoverage(path: string) =
  var w = initMultiStreamWriter(path, "span_demo", chunkSize = 4,
    recordingId = "01949fcc-7d92-7e9c-aaaa-111111111111").get()
  doAssert w.registerPath("/src/app.py").isOk
  doAssert w.registerStep(0, 1, []).isOk
  doAssert w.registerStep(0, 2, []).isOk
  doAssert w.registerSpanCoverageHex(
    DemoTraceIdHex, DemoSpanIdHex, DemoWallNs, DemoMonotonicNs).isOk
  doAssert w.close().isOk
  doAssert w.closeCtfs().isOk

proc test_span_coverage_is_indexed_and_confirmable() =
  ## The kind-0 half: an observability recorder declares "this recording
  ## covers this span", and a consumer resolves it by `(trace_id, span_id)`
  ## alone, getting back the coordinates it needs to open a replay.
  let path = getTempDir() / "test_corrmark_span.ct"
  removeFile(path)
  recordSpanCoverage(path)

  let data = readCtfsFromFile(path).get()
  var idx = openCorrmarkIndex(readNamespace(data).get()).get()

  let traceId = decodeHexId(DemoTraceIdHex, 16).get()
  let spanId = decodeHexId(DemoSpanIdHex, 8).get()
  let hits = idx.lookup(traceId, spanId)
  doAssert hits.isOk, "lookup failed: " & hits.error
  doAssert hits.get().len == 1,
    "expected exactly one covering entry, got " & $hits.get().len
  let hit = hits.get()[0]
  doAssert hit.kind == MarkerKindSpan
  doAssert hit.wallTimeUnixNs == DemoWallNs,
    "wall_time_unix_ns must round-trip exactly — the consumer asserts it"
  doAssert hit.monotonicTimeNs == DemoMonotonicNs
  doAssert hit.geid == 2'u64,
    "geid must be the step count at declaration time, got " & $hit.geid

  # A span this recording does NOT cover is a clean miss, not an error, and
  # not the same answer as the namespace being absent (contract §9).
  var otherSpan = spanId
  otherSpan[7] = otherSpan[7] xor 0xFF'u8
  let miss = idx.lookup(traceId, otherSpan)
  doAssert miss.isOk and miss.get().len == 0,
    "an uncovered span must be a clean miss"

  removeFile(path)
  echo "PASS: test_span_coverage_is_indexed_and_confirmable"

proc test_span_ids_are_wire_bytes_not_hex() =
  ## §7 keys the index on the WIRE bytes. Hashing the hex rendering instead
  ## produces a different key — an index that is present, correct-looking and
  ## permanently unqueryable, with no error anywhere. This pins the two forms
  ## to the same key, and pins that a malformed id is REJECTED rather than
  ## truncated or padded into an entry nobody can find.
  let path = getTempDir() / "test_corrmark_span_bytes.ct"
  removeFile(path)

  var w = initMultiStreamWriter(path, "span_bytes", chunkSize = 4,
    recordingId = "01949fcc-7d92-7e9c-aaaa-222222222222").get()
  doAssert w.registerStep(0, 1, []).isOk

  let traceId = decodeHexId(DemoTraceIdHex, 16).get()
  let spanId = decodeHexId(DemoSpanIdHex, 8).get()
  doAssert w.registerSpanCoverage(traceId, spanId, 1, 2).isOk

  # Wrong widths must fail loudly.
  doAssert w.registerSpanCoverage(traceId[0 ..< 8], spanId, 1, 2).isErr,
    "a 8-byte trace_id must be rejected, not zero-padded"
  doAssert w.registerSpanCoverage(traceId, spanId[0 ..< 4], 1, 2).isErr,
    "a 4-byte span_id must be rejected"
  doAssert w.registerSpanCoverageHex("nothex", DemoSpanIdHex, 1, 2).isErr,
    "a non-hex trace_id must be rejected"
  doAssert w.registerSpanCoverageHex(
      DemoTraceIdHex & "00", DemoSpanIdHex, 1, 2).isErr,
    "an over-long hex trace_id must be rejected"
  # Upper case is the same identifier.
  doAssert decodeHexId(DemoSpanIdHex.toUpperAscii(), 8).get() == spanId

  doAssert w.close().isOk
  doAssert w.closeCtfs().isOk

  let data = readCtfsFromFile(path).get()
  var idx = openCorrmarkIndex(readNamespace(data).get()).get()
  doAssert idx.lookup(traceId, spanId).get().len == 1,
    "the byte form and the hex form must resolve to the same key"

  removeFile(path)
  echo "PASS: test_span_ids_are_wire_bytes_not_hex"

proc test_index_is_enumerable_for_inspection() =
  ## Span coverage has no other observable surface. A kind-0 entry mints no
  ## `MarkerPayload` and no IO event (contract §10.2), so a recorder that
  ## declared coverage and one that dropped the call produce byte-identical
  ## event streams. `allEntries` — and `ct print --correlation-index` over it —
  ## is what tells them apart, which is why it exists despite being O(index)
  ## and therefore useless as a lookup.
  let path = getTempDir() / "test_corrmark_enumerate.ct"
  removeFile(path)
  recordSpanCoverage(path)

  let data = readCtfsFromFile(path).get()
  var idx = openCorrmarkIndex(readNamespace(data).get()).get()
  let entries = idx.allEntries()
  doAssert entries.isOk, entries.error
  doAssert entries.get().len == 1,
    "expected the one declared span, got " & $entries.get().len

  let e = entries.get()[0]
  doAssert e.kind == MarkerKindSpan
  doAssert e.traceIdHexOf() == DemoTraceIdHex,
    "trace id must render back to the hex it was declared with, got " &
    e.traceIdHexOf()
  doAssert e.spanIdHexOf() == DemoSpanIdHex, e.spanIdHexOf()
  doAssert e.wallTimeUnixNs == DemoWallNs
  doAssert e.monotonicTimeNs == DemoMonotonicNs

  removeFile(path)
  echo "PASS: test_index_is_enumerable_for_inspection"

when isMainModule:
  test_marker_payload_decodes_through_ct_print()
  test_marker_mints_no_step()
  test_marker_is_indexed_in_corrmark_ns()
  test_span_coverage_is_indexed_and_confirmable()
  test_span_ids_are_wire_bytes_not_hex()
  test_index_is_enumerable_for_inspection()
  test_absence_is_distinguishable()
  echo "=== correlation marker API tests passed ==="
