when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Integration tests for the RS-M1 request/interval span streams — `spans.dat`,
## `spans.idx` and `spantype.ns` — plus the `meta.dat` bit 13
## `FlagHasSpanStream` that gates them.
##
## Spec: ``codetracer-specs/Trace-Files/CTFS-Request-Span-Streams.md``.
## Milestone: RS-M1 in
## ``codetracer-specs/Planned-Features/Request-Panel-Live-Sessions.milestones.org``.
##
## # Design
##
## **No mocks.** Every test drives the production writer
## (`initSpanStreamWriter` / `writeSpan` / `flush` / `writeSpanTypeNamespace`)
## into a REAL CTFS container and reads it back through the production reader
## (`initSpanStreamReader` / `settledSpans` / `readSpansSince`), so the tests
## exercise the actual chunk codec, the actual Zstd frames, the actual
## companion index and the actual `meta.dat` parser. The tail test additionally
## uses a real on-disk streaming container (`createCtfsStreaming`) and re-reads
## it from the filesystem between appends, so the "reader observes a growing
## container" claim is tested against real file I/O rather than an in-memory
## buffer that a mock could trivially satisfy. Per workspace policy a mock
## would have to be justified here; none is used, so there is nothing to
## justify.
##
## # Scenarios and assertions
##
## 1. `span_stream_roundtrip_both_bindings` — writes an INLINE-bound
##    web-request span (execution lives in this container:
##    `(process_ord, thread_id, step range)`) and an EXTERNAL/child-bound span
##    (`flags.external`, execution in a different container named by
##    `external_recording` + `external_path`), both with the full well-known
##    HTTP metadata set, plus a `span_type: "process"` span. Asserts
##    field-level equality of every wire field after a real container
##    round-trip, INCLUDING metadata key/value ordering (the wire format
##    preserves emission order — a `Table` would not) and including `status`.
##    Also asserts `spantype.ns` maps each interned span type to exactly its
##    own span ids, so a reader can fetch just the processes without scanning
##    the request records.
##
## 2. `span_stream_tail_during_active_write` — opens a reader on a container
##    that is STILL BEING WRITTEN (streaming mode, never closed), reads the
##    committed prefix, appends more spans, and asserts the reader observes
##    exactly the new spans via companion-index growth. The delta is taken
##    with `readSpansSince(knownChunkCount)`, i.e. only the chunks added since
##    the cursor are decoded. No finalization step happens at any point — the
##    container is still open when the last assertion runs.
##
## 3. `span_stream_last_record_wins` — appends an OPEN record and then a
##    COMPLETION record for the same `span_id`, and asserts the reader yields
##    ONE settled span carrying the completion's status/end fields, while the
##    raw record count is still 2 (the stream stayed append-only).
##
## 4. `span_stream_ignored_without_feature_bit` — see the long comment on that
##    test: `KnownFlags` makes bit 13 a REJECTING change, not an ignorable one,
##    so the milestone's literal wording is not implementable. The test asserts
##    what is actually correct, and pins the rejection behaviour explicitly.
##
## Supporting tests cover the milestone's "fail-closed decode errors"
## deliverable, the paging reader API, the production `MultiStreamTraceWriter`
## path, and — `span_stream_short_chunk_mid_stream` — the case a repeated
## `flush` creates: a chunk holding fewer than `chunk_size` records in the
## MIDDLE of the stream, after which record index and chunk number can no
## longer be related by arithmetic.

import std/[os, tables]
import results
import codetracer_ctfs
import codetracer_trace_types
import codetracer_trace_writer/span_stream
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/multi_stream_writer

const TestRecordingId = "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc assertSpanEqual(got, want: SpanRecord, ctx: string) =
  ## Field-level equality over EVERY wire field, including metadata ordering.
  doAssert got.spanId == want.spanId, ctx & ": spanId " & $got.spanId
  doAssert got.parentSpanId == want.parentSpanId, ctx & ": parentSpanId"
  doAssert got.isOpen == want.isOpen, ctx & ": isOpen"
  doAssert got.isExternal == want.isExternal, ctx & ": isExternal"
  doAssert got.status == want.status,
    ctx & ": status got " & $got.status & " want " & $want.status
  doAssert got.startWallNs == want.startWallNs, ctx & ": startWallNs"
  doAssert got.endWallNs == want.endWallNs, ctx & ": endWallNs"
  doAssert got.processOrd == want.processOrd, ctx & ": processOrd"
  doAssert got.threadId == want.threadId, ctx & ": threadId"
  doAssert got.startStep == want.startStep, ctx & ": startStep"
  doAssert got.endStep == want.endStep, ctx & ": endStep"
  doAssert got.externalRecording == want.externalRecording,
    ctx & ": externalRecording got '" & got.externalRecording & "'"
  doAssert got.externalPath == want.externalPath,
    ctx & ": externalPath got '" & got.externalPath & "'"
  doAssert got.spanType == want.spanType,
    ctx & ": spanType got '" & got.spanType & "'"
  doAssert got.label == want.label, ctx & ": label got '" & got.label & "'"
  doAssert got.contiguousOnOneThread == want.contiguousOnOneThread,
    ctx & ": contiguousOnOneThread"
  doAssert got.sharesTimeline == want.sharesTimeline, ctx & ": sharesTimeline"
  doAssert got.concurrentWithSiblings == want.concurrentWithSiblings,
    ctx & ": concurrentWithSiblings"
  # Metadata ordering is part of the contract: compare as an ORDERED sequence,
  # element by element, not as a set or a map.
  doAssert got.metadata.len == want.metadata.len,
    ctx & ": metadata count got " & $got.metadata.len &
    " want " & $want.metadata.len
  for i in 0 ..< want.metadata.len:
    doAssert got.metadata[i][0] == want.metadata[i][0],
      ctx & ": metadata key at index " & $i & " got '" &
      got.metadata[i][0] & "' want '" & want.metadata[i][0] & "'"
    doAssert got.metadata[i][1] == want.metadata[i][1],
      ctx & ": metadata value at index " & $i & " got '" &
      got.metadata[i][1] & "' want '" & want.metadata[i][1] & "'"

proc webRequestSpan(spanId: uint64, methodName, url, status: string,
    startStep, endStep: uint64): SpanRecord =
  ## A completed, inline-bound web-request span with the full well-known
  ## metadata set from the spec's "Well-known metadata keys" table, in a
  ## deliberately NON-alphabetical order so the ordering assertion is
  ## meaningful.
  SpanRecord(
    spanId: spanId,
    parentSpanId: 0,
    isOpen: false,
    isExternal: false,
    status: spanStatusOk,
    startWallNs: 1_700_000_000_000_000_000'u64 + spanId * 1_000_000,
    endWallNs: 1_700_000_000_000_000_000'u64 + spanId * 1_000_000 + 12_000_000,
    processOrd: 0,
    threadId: 7,
    startStep: startStep,
    endStep: endStep,
    spanType: "web-request",
    label: methodName & " " & url,
    contiguousOnOneThread: true,
    sharesTimeline: true,
    concurrentWithSiblings: false,
    metadata: @[
      ("http.method", methodName),
      ("http.url", url),
      ("http.status_code", status),
      ("http.duration_ms", "12"),
      ("http.route", "/api/users/:id"),
      ("http.response_size", "2148"),
      ("http.remote_addr", "127.0.0.1"),
      ("framework", "flask"),
    ])

# ---------------------------------------------------------------------------
# 1. span_stream_roundtrip_both_bindings
# ---------------------------------------------------------------------------

proc span_stream_roundtrip_both_bindings() {.raises: [].} =
  ## Write inline-bound and external/child-bound spans with full HTTP metadata
  ## into a real CTFS container; read them back through the production reader;
  ## assert field-level equality including metadata ordering and status.
  var ctfs = createCtfs()

  # Chunk size 2 so three spans span multiple chunks — the round trip must
  # survive the chunk boundary, not just a single-chunk happy path.
  let wRes = initSpanStreamWriter(ctfs, chunkSize = 2)
  doAssert wRes.isOk, "initSpanStreamWriter failed: " & wRes.error
  var writer = wRes.get()

  # (a) inline-bound web request — execution lives in THIS container.
  let inlineSpan = webRequestSpan(1, "GET", "/api/users/42", "200", 100, 350)

  # (b) external/child-bound web request — execution lives in a DIFFERENT
  #     container; this is what session_manifest.jsonl's `trace_dir` did.
  var externalSpan = webRequestSpan(2, "POST", "/api/orders", "500", 0, 0)
  externalSpan.isExternal = true
  externalSpan.status = spanStatusError
  externalSpan.externalRecording = "01949fcc-7d92-7e9c-cccc-dddddddddddd"
  externalSpan.externalPath = "requests/req-0002.ct"
  externalSpan.metadata.add(("error.message", "upstream timeout"))

  # (c) a process span — the same interval model, different span_type.
  let processSpan = SpanRecord(
    spanId: 3,
    parentSpanId: 0,
    isOpen: false,
    isExternal: false,
    status: spanStatusOk,
    startWallNs: 1_700_000_000_000_000_000'u64,
    endWallNs: 1_700_000_000_900_000_000'u64,
    processOrd: 1,
    threadId: 0,
    startStep: 0,
    endStep: 999,
    spanType: "process",
    label: "/usr/bin/php-fpm",
    contiguousOnOneThread: false,
    # sharesTimeline FALSE on purpose, and it is the only span here with the
    # bit clear: a separate OS process has its own timeline, so its ordering is
    # NOT comparable with the request spans'. It also exercises `structural`
    # bit 1 in its zero state — with every record setting it, a decoder that
    # hardcoded `true` would pass.
    sharesTimeline: false,
    concurrentWithSiblings: true,
    metadata: @[
      ("process.pid", "4242"),
      ("process.parent_pid", "1"),
      ("process.exe", "/usr/bin/php-fpm"),
      ("process.args", "--nodaemonize"),
      ("process.has_execed", "true"),
    ])

  let want = @[inlineSpan, externalSpan, processSpan]
  for s in want:
    let r = writeSpan(ctfs, writer, s)
    doAssert r.isOk, "writeSpan failed for span " & $s.spanId & ": " & r.error
  doAssert span_stream.flush(ctfs, writer).isOk
  doAssert writeSpanTypeNamespace(ctfs, writer).isOk

  # Declare the feature in meta.dat, as a real recorder would.
  let metaFileRes = ctfs.addFile("meta.dat")
  doAssert metaFileRes.isOk
  var metaFile = metaFileRes.get()
  let meta = TraceMetadata(
    recordingId: TestRecordingId, program: "server", workdir: "/srv")
  doAssert ctfs.writeMetaDat(metaFile, meta, [], hasSpanStream = true).isOk

  let raw = ctfs.toBytes()

  # --- read back through the production reader ---
  let rRes = initSpanStreamReader(raw)
  doAssert rRes.isOk, "initSpanStreamReader failed: " & rRes.error
  let reader = rRes.get()
  doAssert reader.count == 3,
    "expected 3 records, got " & $reader.count
  doAssert reader.chunkCount == 2,
    "expected 2 chunks at chunkSize 2, got " & $reader.chunkCount

  let gotRes = reader.settledSpans()
  doAssert gotRes.isOk, "settledSpans failed: " & gotRes.error
  let got = gotRes.get()
  doAssert got.len == 3, "expected 3 settled spans, got " & $got.len

  for i in 0 ..< want.len:
    assertSpanEqual(got[i], want[i], "span " & $want[i].spanId)

  # The external span must round-trip its binding, and the inline one must
  # carry NO binding strings at all.
  doAssert got[1].isExternal and got[1].externalPath == "requests/req-0002.ct"
  doAssert not got[0].isExternal
  doAssert got[0].externalRecording.len == 0 and got[0].externalPath.len == 0
  # ... and status must survive distinctly per span.
  doAssert got[0].status == spanStatusOk
  doAssert got[1].status == spanStatusError
  # The `structural` bits must survive distinctly too, in BOTH states — the
  # process span deliberately clears sharesTimeline while the requests set it.
  doAssert got[0].sharesTimeline, "the request span shares the timeline"
  doAssert got[1].sharesTimeline, "the external request span shares it too"
  doAssert not got[2].sharesTimeline,
    "the process span must round-trip sharesTimeline FALSE"
  doAssert got[2].concurrentWithSiblings and not got[2].contiguousOnOneThread,
    "the process span's other two structural bits must survive alongside it"

  # meta.dat must advertise the feature.
  let metaBytes = readInternalFile(raw, "meta.dat")
  doAssert metaBytes.isOk, "reading meta.dat failed: " & metaBytes.error
  let parsed = readMetaDat(metaBytes.get())
  doAssert parsed.isOk, "readMetaDat failed: " & parsed.error
  doAssert parsed.get().hasSpanStream, "meta.dat bit 13 must be set"

  # spantype.ns must let a reader fetch just one type's ids.
  let nsRes = readSpanTypeNamespace(raw)
  doAssert nsRes.isOk, "readSpanTypeNamespace failed: " & nsRes.error
  let ns = nsRes.get()
  doAssert ns.len == 2, "expected 2 interned span types, got " & $ns.len
  doAssert spanIdsOfType(ns, "web-request") == @[1'u64, 2'u64],
    "web-request span ids mismatch"
  doAssert spanIdsOfType(ns, "process") == @[3'u64],
    "process span ids mismatch"
  doAssert spanIdsOfType(ns, "test").len == 0,
    "absent span type must yield no ids"

  echo "PASS: span_stream_roundtrip_both_bindings"

# ---------------------------------------------------------------------------
# 2. span_stream_tail_during_active_write
# ---------------------------------------------------------------------------

proc span_stream_tail_during_active_write() {.raises: [].} =
  ## Open a reader on a container that is still being written, read the
  ## committed prefix, append more spans, and assert the reader observes
  ## exactly the new spans via companion-index growth with no finalization.
  let path = getTempDir() / "test_span_stream_tail.ct"
  try:
    removeFile(path)
  except OSError:
    discard

  let cRes = createCtfsStreaming(path)
  doAssert cRes.isOk, "createCtfsStreaming failed: " & cRes.error
  var ctfs = cRes.get()
  doAssert ctfs.isStreaming

  # chunkSize 2: every two spans seal a chunk and grow spans.idx by one u64.
  let wRes = initSpanStreamWriter(ctfs, chunkSize = 2)
  doAssert wRes.isOk, "initSpanStreamWriter failed: " & wRes.error
  var writer = wRes.get()

  # --- phase 1: write four spans (two sealed chunks) ---
  for i in 1'u64 .. 4'u64:
    let s = webRequestSpan(i, "GET", "/p/" & $i, "200", i * 10, i * 10 + 5)
    doAssert writeSpan(ctfs, writer, s).isOk

  # A concurrent reader re-reads the container FROM DISK. The writer has not
  # been flushed or closed; nothing finalizes anything.
  let disk1 = readCtfsFromFile(path)
  doAssert disk1.isOk, "readCtfsFromFile failed: " & disk1.error
  let r1Res = initSpanStreamReader(disk1.get())
  doAssert r1Res.isOk, "reader on live container failed: " & r1Res.error
  let r1 = r1Res.get()

  let prefixRes = r1.settledSpans()
  doAssert prefixRes.isOk, "settledSpans on live container: " & prefixRes.error
  let prefix = prefixRes.get()
  doAssert prefix.len == 4,
    "committed prefix should hold 4 spans, got " & $prefix.len
  for i in 0 ..< 4:
    doAssert prefix[i].spanId == uint64(i + 1)
    doAssert prefix[i].label == "GET /p/" & $(i + 1)

  # This is the tailing cursor: the companion-index length the reader has seen.
  let cursor = r1.chunkCount
  doAssert cursor == 2, "expected 2 sealed chunks, got " & $cursor

  # --- phase 2: append three more spans while the container stays open ---
  for i in 5'u64 .. 7'u64:
    let s = webRequestSpan(i, "GET", "/p/" & $i, "200", i * 10, i * 10 + 5)
    doAssert writeSpan(ctfs, writer, s).isOk
  # Spans 5+6 sealed chunk 2 automatically; publish span 7's partial chunk the
  # way a live recorder does to make an in-flight span visible.
  doAssert span_stream.flush(ctfs, writer).isOk

  # The reader re-opens the STILL-GROWING container and asks only for what is
  # new, identified by the index length it remembered.
  let disk2 = readCtfsFromFile(path)
  doAssert disk2.isOk, "second readCtfsFromFile failed: " & disk2.error
  let r2Res = initSpanStreamReader(disk2.get())
  doAssert r2Res.isOk, "second reader failed: " & r2Res.error
  let r2 = r2Res.get()

  doAssert r2.chunkCount == 4,
    "companion index must have grown to 4 chunks, got " & $r2.chunkCount
  doAssert r2.chunkCount > cursor, "companion index must have grown"

  let deltaRes = r2.readSpansSince(cursor)
  doAssert deltaRes.isOk, "readSpansSince failed: " & deltaRes.error
  let delta = deltaRes.get()

  # EXACTLY the new spans — not the prefix, not a superset.
  doAssert delta.len == 3,
    "expected exactly 3 new spans, got " & $delta.len
  for i in 0 ..< 3:
    doAssert delta[i].spanId == uint64(i + 5),
      "delta span " & $i & " should be span id " & $(i + 5) &
      ", got " & $delta[i].spanId
    doAssert delta[i].label == "GET /p/" & $(i + 5)
    doAssert delta[i].metadata.len == 8, "delta spans keep their metadata"

  # And the full view now holds all seven.
  let allRes = r2.settledSpans()
  doAssert allRes.isOk
  doAssert allRes.get().len == 7, "expected 7 spans total"

  # A cursor already at the head yields an empty delta rather than an error.
  let emptyRes = r2.readSpansSince(r2.chunkCount)
  doAssert emptyRes.isOk and emptyRes.get().len == 0,
    "an up-to-date cursor must yield an empty delta"

  # NO finalization step has run: the container is still open right now.
  doAssert ctfs.isStreaming, "container must still be open (no finalization)"
  ctfs.closeCtfs()
  try:
    removeFile(path)
  except OSError:
    discard

  echo "PASS: span_stream_tail_during_active_write"

# ---------------------------------------------------------------------------
# 3. span_stream_last_record_wins
# ---------------------------------------------------------------------------

proc span_stream_last_record_wins() {.raises: [].} =
  ## Append an open record and then its completion record for the same
  ## span_id; assert the reader yields one settled span.
  var ctfs = createCtfs()
  let wRes = initSpanStreamWriter(ctfs, chunkSize = 8)
  doAssert wRes.isOk
  var writer = wRes.get()

  # The OPEN record: request started, outcome unknown, no end fields yet.
  let openRec = SpanRecord(
    spanId: 1,
    isOpen: true,
    status: spanStatusUnknown,
    startWallNs: 1_700_000_000_000_000_000'u64,
    endWallNs: 0,
    processOrd: 0,
    threadId: 3,
    startStep: 500,
    endStep: 0,
    spanType: "web-request",
    label: "GET /slow",
    contiguousOnOneThread: true,
    sharesTimeline: true,
    metadata: @[("http.method", "GET"), ("http.url", "/slow")])
  doAssert writeSpan(ctfs, writer, openRec).isOk

  # A different span in between, to prove resolution is per span_id and not
  # merely "take the last record in the stream".
  let other = webRequestSpan(2, "GET", "/fast", "200", 600, 610)
  doAssert writeSpan(ctfs, writer, other).isOk

  # The COMPLETION record: same span_id, now settled.
  let doneRec = SpanRecord(
    spanId: 1,
    isOpen: false,
    status: spanStatusOk,
    startWallNs: 1_700_000_000_000_000_000'u64,
    endWallNs: 1_700_000_002_500_000_000'u64,
    processOrd: 0,
    threadId: 3,
    startStep: 500,
    endStep: 1_400,
    spanType: "web-request",
    label: "GET /slow",
    contiguousOnOneThread: true,
    sharesTimeline: true,
    metadata: @[
      ("http.method", "GET"),
      ("http.url", "/slow"),
      ("http.status_code", "200"),
      ("http.duration_ms", "2500")])
  doAssert writeSpan(ctfs, writer, doneRec).isOk
  doAssert span_stream.flush(ctfs, writer).isOk

  let raw = ctfs.toBytes()
  let rRes = initSpanStreamReader(raw)
  doAssert rRes.isOk
  let reader = rRes.get()

  # The stream stayed strictly append-only: three RAW records on the wire.
  doAssert reader.count == 3,
    "stream must keep all 3 raw records (append-only), got " & $reader.count

  # But the settled view has exactly two spans, and span 1 is the COMPLETION.
  let settledRes = reader.settledSpans()
  doAssert settledRes.isOk, "settledSpans failed: " & settledRes.error
  let settled = settledRes.get()
  doAssert settled.len == 2,
    "expected 2 settled spans (last-record-wins), got " & $settled.len

  assertSpanEqual(settled[0], doneRec, "settled span 1")
  doAssert not settled[0].isOpen, "settled span must not still be open"
  doAssert settled[0].status == spanStatusOk
  doAssert settled[0].endStep == 1_400
  doAssert settled[0].endWallNs == 1_700_000_002_500_000_000'u64
  doAssert settled[0].metadata.len == 4,
    "completion metadata must replace the open record's, got " &
    $settled[0].metadata.len
  assertSpanEqual(settled[1], other, "settled span 2")

  # spantype.ns counts the span once, not once per record.
  doAssert writeSpanTypeNamespace(ctfs, writer).isOk
  let ns = readSpanTypeNamespace(ctfs.toBytes())
  doAssert ns.isOk, "readSpanTypeNamespace failed: " & ns.error
  doAssert spanIdsOfType(ns.get(), "web-request") == @[1'u64, 2'u64],
    "an open record and its completion must contribute one span id"

  echo "PASS: span_stream_last_record_wins"

# ---------------------------------------------------------------------------
# 4. span_stream_ignored_without_feature_bit
# ---------------------------------------------------------------------------

proc span_stream_ignored_without_feature_bit() {.raises: [].} =
  ## The milestone asks: "a reader build that does not know bit 13 opens the
  ## same container unchanged and reports no spans."
  ##
  ## **That is not implementable, and the discrepancy is a real finding.**
  ## `meta_dat.nim` defines `KnownFlags` as the OR of every bit the reader
  ## understands and `readMetaDat` REJECTS any container whose flag word has a
  ## bit outside that mask. A reader that does not know bit 13 therefore
  ## refuses a span-bearing container outright — it never gets as far as
  ## "reports no spans". `CTFS-Request-Span-Streams.md` says this itself
  ## ("This is not a backward-compatible addition… An older reader will
  ## *refuse* a span-bearing container, not ignore the streams"), while its own
  ## Compatibility section still claims "Old readers ignore bit 13 and see a
  ## normal container". The two statements contradict each other; the strict
  ## reader is what actually governs `.ct` files, so this test pins the strict
  ## behaviour rather than forcing the milestone's wording.
  ##
  ## What is asserted instead, which is what the requirement was protecting:
  ##   (a) a pre-bit-13 reader's flag mask REJECTS a span-bearing container,
  ##       cleanly and with a diagnosable error — it does not misdecode;
  ##   (b) a container written WITHOUT spans is unchanged: bit 13 clear, and
  ##       none of the three span files present;
  ##   (c) a span-AWARE reader opening that span-free container reports NO
  ##       spans — the genuine "ignored" path, and the one a consumer built on
  ##       this milestone actually takes.

  # --- (a) the pre-bit-13 reader mask rejects a span-bearing container ---
  # Reconstruct the KnownFlags mask as it stood before RS-M1 and show bit 13
  # falls outside it: that is precisely why readMetaDat rejects.
  const PreRsM1KnownFlags: uint16 =
    FlagHasMcrFields or FlagHasReplayLaunchFields or FlagHasLayoutSnapshot or
    FlagHasTraceFilterProvenance or FlagHasColumnAwareSteps or
    FlagHasAlternateSourceViews or FlagSupportsColumnBreakpoints or
    FlagSupportsColumnMotions or FlagHasCallStream or FlagHasStepStream or
    FlagHasValueStream or FlagHasIoEventStream or FlagHasInterningTables
  doAssert (FlagHasSpanStream and PreRsM1KnownFlags) == 0,
    "bit 13 must be outside the pre-RS-M1 known mask"
  doAssert FlagHasSpanStream == 0x2000'u16, "FlagHasSpanStream must be bit 13"
  doAssert (KnownFlags and FlagHasSpanStream) != 0,
    "the current reader must know bit 13"

  # Build a real span-bearing container and confirm a mask that lacks bit 13
  # would classify it as carrying unknown bits — the reject path.
  var withSpans = createCtfs()
  let wRes = initSpanStreamWriter(withSpans)
  doAssert wRes.isOk
  var writer = wRes.get()
  doAssert writeSpan(withSpans, writer,
    webRequestSpan(1, "GET", "/x", "200", 1, 2)).isOk
  doAssert span_stream.flush(withSpans, writer).isOk
  doAssert writeSpanTypeNamespace(withSpans, writer).isOk
  let mf1 = withSpans.addFile("meta.dat")
  doAssert mf1.isOk
  var metaFile1 = mf1.get()
  let meta1 = TraceMetadata(
    recordingId: TestRecordingId, program: "server", workdir: "/srv")
  doAssert withSpans.writeMetaDat(
    metaFile1, meta1, [], hasSpanStream = true).isOk
  let withSpansBytes = withSpans.toBytes()

  let metaRaw1 = readInternalFile(withSpansBytes, "meta.dat")
  doAssert metaRaw1.isOk
  let flagWord = uint16(metaRaw1.get()[6]) or (uint16(metaRaw1.get()[7]) shl 8)
  doAssert (flagWord and FlagHasSpanStream) != 0,
    "the span-bearing container must carry bit 13"
  doAssert (flagWord and not PreRsM1KnownFlags) != 0,
    "a pre-RS-M1 reader must see an unknown bit and reject, not ignore"

  # --- (b) a container WITHOUT spans is unchanged ---
  var noSpans = createCtfs()
  let mf2 = noSpans.addFile("meta.dat")
  doAssert mf2.isOk
  var metaFile2 = mf2.get()
  let meta2 = TraceMetadata(
    recordingId: TestRecordingId, program: "server", workdir: "/srv")
  doAssert noSpans.writeMetaDat(metaFile2, meta2, []).isOk
  let noSpansBytes = noSpans.toBytes()

  let parsed2 = readMetaDat(readInternalFile(noSpansBytes, "meta.dat").get())
  doAssert parsed2.isOk, "span-free meta.dat must parse: " & parsed2.error
  doAssert not parsed2.get().hasSpanStream,
    "a container without spans must have bit 13 CLEAR"
  let flagWord2 =
    uint16(readInternalFile(noSpansBytes, "meta.dat").get()[6]) or
    (uint16(readInternalFile(noSpansBytes, "meta.dat").get()[7]) shl 8)
  doAssert flagWord2 == 0,
    "a span-free meta.dat flag word must be 0, got " & $flagWord2
  doAssert not hasSpanStreamFiles(noSpansBytes),
    "a span-free container must not carry spans.dat"
  doAssert not hasInternalFile(noSpansBytes, "spans.idx"),
    "a span-free container must not carry spans.idx"
  doAssert not hasInternalFile(noSpansBytes, "spantype.ns"),
    "a span-free container must not carry spantype.ns"

  # --- (c) a span-aware reader reports NO spans for that container ---
  # Gating on the meta.dat bit is what a consumer does; the bit is clear, so
  # there are no spans. And going to the stream directly fails cleanly rather
  # than inventing spans.
  doAssert not parsed2.get().hasSpanStream
  let strayReader = initSpanStreamReader(noSpansBytes)
  doAssert strayReader.isErr,
    "opening a span stream that does not exist must fail, not fabricate spans"

  echo "PASS: span_stream_ignored_without_feature_bit"

# ---------------------------------------------------------------------------
# Supporting: a SHORT chunk in the MIDDLE of the stream
# ---------------------------------------------------------------------------

proc span_stream_short_chunk_mid_stream() {.raises: [].} =
  ## `flush` is documented as safe to call repeatedly during a live recording,
  ## and both `MultiStreamTraceWriter.flushSpans` and `trace_writer_flush_spans`
  ## expose it per request. Every such call seals whatever is buffered, so a
  ## chunk holding FEWER than `chunk_size` records lands in the MIDDLE of the
  ## stream and every later record's append-order index stops agreeing with
  ## `chunk_number * chunk_size + within`.
  ##
  ## Regression test for exactly that. Before the fix, `count()` reported
  ## `last_chunk * chunk_size + last_chunk_records` (14 here instead of 9, and
  ## 66 instead of 3 through the writer below) and `readSpan` addressed the
  ## wrong chunk, failing with "span record N missing in chunk M". The
  ## chunk-iterating readers (`settledSpans` / `readSpansSince`) were always
  ## right, which is how the bug stayed invisible: no existing test built a
  ## non-final partial chunk, so `count()` and random access silently lied.
  ##
  ## The test therefore keeps WRITING after each mid-stream flush, and checks
  ## the count-based and the chunk-based views against each other.

  # --- (a) the raw span-stream writer, with flushes interleaved ---
  block raw_writer:
    var ctfs = createCtfs()
    # chunkSize 4, so a chunk sealed by `flush` before its 4th record is
    # unambiguously short.
    let wRes = initSpanStreamWriter(ctfs, chunkSize = 4)
    doAssert wRes.isOk, "initSpanStreamWriter failed: " & wRes.error
    var writer = wRes.get()

    proc span(i: uint64): SpanRecord =
      var s = webRequestSpan(i, "GET", "/p/" & $i, "200", i * 10, i * 10 + 5)
      # One span with the bit clear, so `structural` bit 1 is exercised in
      # both states across a chunk boundary too.
      if i == 5: s.sharesTimeline = false
      s

    # chunk 0: SHORT — 2 of 4 records, sealed early by a live flush.
    for i in 1'u64 .. 2'u64:
      doAssert writeSpan(ctfs, writer, span(i)).isOk
    doAssert span_stream.flush(ctfs, writer).isOk

    # chunk 1: FULL — 4 records, sealed automatically. Writing continues right
    # after the short chunk; this is the case the old arithmetic broke on.
    for i in 3'u64 .. 6'u64:
      doAssert writeSpan(ctfs, writer, span(i)).isOk

    # chunk 2: SHORT — a single record, published immediately.
    doAssert writeSpan(ctfs, writer, span(7)).isOk
    doAssert span_stream.flush(ctfs, writer).isOk

    # chunk 3: SHORT — the final partial chunk, 2 records.
    for i in 8'u64 .. 9'u64:
      doAssert writeSpan(ctfs, writer, span(i)).isOk
    doAssert span_stream.flush(ctfs, writer).isOk

    doAssert writer.count == 9'u64,
      "the writer counted " & $writer.count & " records, expected 9"

    let rRes = initSpanStreamReader(ctfs.toBytes())
    doAssert rRes.isOk, "initSpanStreamReader failed: " & rRes.error
    var reader = rRes.get()

    doAssert reader.chunkCount == 4,
      "expected 4 sealed chunks, got " & $reader.chunkCount
    doAssert reader.chunkSizeRecords == 4'u32,
      "the index header still advertises chunk_size 4"

    # count() must be the TRUE record count, not lastChunk*chunkSize + tail
    # (which would be 3*4 + 2 = 14).
    doAssert reader.count == 9'u64,
      "count() must be 9 across the short chunks, got " & $reader.count

    # Each chunk's real occupancy, including the short ones.
    let wantPerChunk = [2, 4, 1, 2]
    for c in 0 ..< 4:
      doAssert reader.recordsInChunk(c) == wantPerChunk[c],
        "chunk " & $c & " should hold " & $wantPerChunk[c] &
        " records, got " & $reader.recordsInChunk(c)

    # Random access for EVERY index must land on the right record.
    for i in 0'u64 ..< 9'u64:
      let got = reader.readSpan(i)
      doAssert got.isOk, "readSpan(" & $i & ") failed: " & got.error
      assertSpanEqual(got.get(), span(i + 1), "readSpan(" & $i & ")")

    doAssert reader.readSpan(9).isErr,
      "index 9 is past the end and must error"

    # The chunk-iterating view must agree with the count-based one.
    let settledRes = reader.settledSpans()
    doAssert settledRes.isOk, "settledSpans failed: " & settledRes.error
    let settled = settledRes.get()
    doAssert settled.len == 9,
      "settledSpans must yield 9 spans, got " & $settled.len
    for i in 0 ..< 9:
      assertSpanEqual(settled[i], span(uint64(i + 1)), "settled " & $i)
    doAssert not settled[4].sharesTimeline,
      "span 5 must round-trip sharesTimeline FALSE across the short chunk"

    # readSpansSince across the short chunks: a cursor taken before chunk 2
    # must yield exactly the records of chunks 2 and 3.
    let sinceRes = reader.readSpansSince(2)
    doAssert sinceRes.isOk, "readSpansSince failed: " & sinceRes.error
    let since = sinceRes.get()
    doAssert since.len == 3,
      "chunks 2..3 hold 3 records, got " & $since.len
    for i in 0 ..< 3:
      doAssert since[i].spanId == uint64(i + 7),
        "delta record " & $i & " should be span " & $(i + 7) &
        ", got " & $since[i].spanId

    # Paging is unaffected but must still see all nine.
    let page = reader.pageSpans(1, 0)
    doAssert page.isOk and page.get().len == 9,
      "an unlimited page must hold all 9 spans"

  # --- (b) the production writer, on the exact sequence tests/test_ffi.c runs:
  #         open record -> flush_spans -> completion -> external -> close ---
  block production_writer:
    let wRes = initMultiStreamWriter("shortchunk.ct", "server",
      recordingId = TestRecordingId)
    doAssert wRes.isOk, "initMultiStreamWriter failed: " & wRes.error
    var w = wRes.get()
    let p0 = w.registerPath("/srv/app.py")
    doAssert p0.isOk
    doAssert w.registerStep(p0.get(), 1, []).isOk

    # The in-flight record for span 1.
    let openRec = SpanRecord(
      spanId: 1,
      isOpen: true,
      status: spanStatusUnknown,
      startWallNs: 1_700_000_000_000_000_000'u64,
      processOrd: 0,
      threadId: 3,
      startStep: 100,
      spanType: "web-request",
      label: "GET /slow",
      contiguousOnOneThread: true,
      sharesTimeline: true,
      metadata: @[("http.method", "GET"), ("http.url", "/slow")])
    doAssert w.registerSpan(openRec).isOk

    # A per-request flush, which is what the API is FOR — and which seals a
    # chunk holding 1 of the default chunkSize (64) records.
    doAssert w.flushSpans().isOk

    # ...and the recording keeps going.
    let doneRec = webRequestSpan(1, "GET", "/slow", "200", 100, 900)
    doAssert w.registerSpan(doneRec).isOk

    var externalSpan = webRequestSpan(2, "POST", "/api/orders", "500", 0, 0)
    externalSpan.isExternal = true
    externalSpan.status = spanStatusError
    externalSpan.externalRecording = "01949fcc-7d92-7e9c-cccc-dddddddddddd"
    externalSpan.externalPath = "requests/req-0002.ct"
    doAssert w.registerSpan(externalSpan).isOk

    doAssert w.spanCount == 3'u64,
      "3 raw span records registered, got " & $w.spanCount
    doAssert w.close().isOk
    let bytes = w.toBytes()

    let rRes = initSpanStreamReader(bytes)
    doAssert rRes.isOk, "initSpanStreamReader failed: " & rRes.error
    var reader = rRes.get()

    # Before the fix this reported 66: 1 * 64 (the "full" chunk 0 that is
    # actually 1 record long) + 2.
    doAssert reader.count == 3'u64,
      "count() must be 3 after a mid-recording flush_spans, got " &
      $reader.count
    doAssert reader.chunkCount == 2,
      "the flush must have sealed a second chunk, got " & $reader.chunkCount
    doAssert reader.recordsInChunk(0) == 1 and reader.recordsInChunk(1) == 2,
      "chunk occupancy must be 1 then 2"

    # Before the fix readSpan(1) failed with "span record 1 missing in chunk 0".
    let rec0 = reader.readSpan(0)
    doAssert rec0.isOk, "readSpan(0) failed: " & rec0.error
    assertSpanEqual(rec0.get(), openRec, "raw record 0 (the open record)")
    let rec1 = reader.readSpan(1)
    doAssert rec1.isOk, "readSpan(1) failed: " & rec1.error
    assertSpanEqual(rec1.get(), doneRec, "raw record 1 (the completion)")
    let rec2 = reader.readSpan(2)
    doAssert rec2.isOk, "readSpan(2) failed: " & rec2.error
    assertSpanEqual(rec2.get(), externalSpan, "raw record 2 (the external)")
    doAssert reader.readSpan(3).isErr, "index 3 is past the end"

    # And the settled view still resolves span 1 to its completion.
    let settled = reader.settledSpans()
    doAssert settled.isOk, "settledSpans failed: " & settled.error
    doAssert settled.get().len == 2,
      "2 settled spans (last-record-wins), got " & $settled.get().len
    assertSpanEqual(settled.get()[0], doneRec, "settled span 1")
    doAssert not settled.get()[0].isOpen,
      "span 1 must settle to its completion, not the open record"

  echo "PASS: span_stream_short_chunk_mid_stream"

# ---------------------------------------------------------------------------
# Supporting: fail-closed decoding (RS-M1 deliverable)
# ---------------------------------------------------------------------------

proc span_stream_fail_closed_decode() {.raises: [].} =
  ## "Fail-closed decode errors for truncated or malformed records — never
  ## silently drop a span." Each case must be an ERROR, not a dropped or
  ## partially populated record.
  let good = webRequestSpan(1, "GET", "/api/users/42", "200", 10, 20)
  let encRes = encodeSpanRecord(good)
  doAssert encRes.isOk, "encode failed: " & encRes.error
  let enc = encRes.get()
  doAssert decodeSpanRecord(enc).isOk, "the reference record must decode"

  # Truncation at every prefix length must be rejected, never partially read.
  for cut in 0 ..< enc.len:
    let res = decodeSpanRecord(enc.toOpenArray(0, cut - 1))
    doAssert res.isErr,
      "truncated record of length " & $cut & " must be rejected"

  # Trailing bytes inside a record are a framing error.
  var extra = enc
  extra.add(0'u8)
  doAssert decodeSpanRecord(extra).isErr,
    "trailing bytes must be rejected"

  # Unknown flags bits (bit 2 is not defined in v1).
  var badFlags = enc
  badFlags[2] = badFlags[2] or 0x04'u8
  doAssert decodeSpanRecord(badFlags).isErr,
    "unknown flags bit must be rejected"

  # Invalid status (only 0/1/2 are defined).
  var badStatus = enc
  badStatus[3] = 9'u8
  doAssert decodeSpanRecord(badStatus).isErr,
    "invalid status value must be rejected"

  # Unknown STRUCTURAL bits (only bits 0..2 are defined in v1). The structural
  # byte has no fixed offset in general, but for a record with no metadata the
  # tail is exactly `[structural u8][metadata_count varint = 0]`, so encode one
  # such record and mutate its second-to-last byte. The two assertions below
  # pin that layout, so this stops testing the wrong byte if the wire format
  # ever moves.
  let minimal = SpanRecord(
    spanId: 9,
    status: spanStatusOk,
    startWallNs: 1_700_000_000_000_000_000'u64,
    endWallNs: 1_700_000_000_100_000_000'u64,
    processOrd: 0,
    threadId: 1,
    startStep: 1,
    endStep: 2,
    spanType: "web-request",
    label: "GET /x",
    contiguousOnOneThread: true,
    sharesTimeline: true,
    concurrentWithSiblings: false,
    metadata: @[])
  let minEncRes = encodeSpanRecord(minimal)
  doAssert minEncRes.isOk, "minimal encode failed: " & minEncRes.error
  let minEnc = minEncRes.get()
  doAssert minEnc[^1] == 0'u8,
    "a metadata-free record must end with metadata_count 0"
  doAssert minEnc[^2] ==
    (SpanStructuralContiguous or SpanStructuralSharesTimeline),
    "the structural byte must be the second-to-last byte here, got 0x" &
    $minEnc[^2]
  doAssert decodeSpanRecord(minEnc).isOk, "the minimal record must decode"

  for undefinedBit in [0x08'u8, 0x10'u8, 0x20'u8, 0x40'u8, 0x80'u8]:
    var badStructural = minEnc
    badStructural[^2] = badStructural[^2] or undefinedBit
    doAssert decodeSpanRecord(badStructural).isErr,
      "unknown structural bit 0x" & $undefinedBit & " must be rejected"

  # ...while every DEFINED combination of the structural bits, including all
  # three clear and all three set, must decode to exactly those booleans.
  for bits in 0'u8 .. 7'u8:
    var variant = minimal
    variant.contiguousOnOneThread = (bits and SpanStructuralContiguous) != 0
    variant.sharesTimeline = (bits and SpanStructuralSharesTimeline) != 0
    variant.concurrentWithSiblings = (bits and SpanStructuralConcurrent) != 0
    let vEnc = encodeSpanRecord(variant)
    doAssert vEnc.isOk, "encode failed for structural bits " & $bits
    doAssert vEnc.get()[^2] == bits,
      "structural byte must be " & $bits & ", got " & $vEnc.get()[^2]
    let vDec = decodeSpanRecord(vEnc.get())
    doAssert vDec.isOk, "decode failed for structural bits " & $bits
    assertSpanEqual(vDec.get(), variant, "structural bits " & $bits)

  # span_id 0 is not a legal identity (ids are 1-based).
  var zeroId = SpanRecord(spanId: 0, spanType: "web-request", label: "x")
  doAssert encodeSpanRecord(zeroId).isErr, "span_id 0 must be rejected"

  # An open record carrying end fields contradicts the wire format.
  var badOpen = good
  badOpen.isOpen = true
  doAssert encodeSpanRecord(badOpen).isErr,
    "an open record with non-zero end fields must be rejected"

  # Binding fields without flags.external would be silently lost on the wire.
  var badExternal = good
  badExternal.externalPath = "somewhere.ct"
  doAssert encodeSpanRecord(badExternal).isErr,
    "external binding fields without flags.external must be rejected"

  # A corrupted chunk must fail the READ, not yield a short span list.
  var ctfs = createCtfs()
  let wRes = initSpanStreamWriter(ctfs, chunkSize = 4)
  doAssert wRes.isOk
  var writer = wRes.get()
  for i in 1'u64 .. 4'u64:
    doAssert writeSpan(ctfs, writer,
      webRequestSpan(i, "GET", "/p/" & $i, "200", i, i + 1)).isOk
  doAssert span_stream.flush(ctfs, writer).isOk
  var raw = ctfs.toBytes()
  # Corrupt the compressed chunk body: find spans.dat and flip bytes in it.
  let datOff = readInternalFile(raw, "spans.dat")
  doAssert datOff.isOk and datOff.get().len > 0
  # Locate the chunk bytes inside the container image and damage them.
  let needle = datOff.get()
  var found = -1
  for i in 0 .. raw.len - needle.len:
    var matches = true
    for j in 0 ..< needle.len:
      if raw[i + j] != needle[j]:
        matches = false
        break
    if matches:
      found = i
      break
  doAssert found >= 0, "could not locate spans.dat payload in the container"
  for j in 0 ..< needle.len:
    raw[found + j] = raw[found + j] xor 0xFF'u8

  let corruptRes = initSpanStreamReader(raw)
  if corruptRes.isOk:
    let spans = corruptRes.get().settledSpans()
    doAssert spans.isErr,
      "a corrupted span chunk must fail the read, not drop spans silently"
  # (an init-time failure is equally fail-closed and also acceptable)

  echo "PASS: span_stream_fail_closed_decode"

# ---------------------------------------------------------------------------
# Supporting: paging by span id (RS-M1 reader API deliverable)
# ---------------------------------------------------------------------------

proc span_stream_page_by_span_id() {.raises: [].} =
  ## "Reader API: page by span id" — `ct/load-request-spans`'s
  ## `{ fromSpanId, limit }`.
  var ctfs = createCtfs()
  let wRes = initSpanStreamWriter(ctfs, chunkSize = 5)
  doAssert wRes.isOk
  var writer = wRes.get()
  for i in 1'u64 .. 25'u64:
    doAssert writeSpan(ctfs, writer,
      webRequestSpan(i, "GET", "/p/" & $i, "200", i * 2, i * 2 + 1)).isOk
  doAssert span_stream.flush(ctfs, writer).isOk

  let rRes = initSpanStreamReader(ctfs.toBytes())
  doAssert rRes.isOk
  let reader = rRes.get()
  doAssert reader.count == 25

  let page1 = reader.pageSpans(1, 10)
  doAssert page1.isOk and page1.get().len == 10
  doAssert page1.get()[0].spanId == 1 and page1.get()[9].spanId == 10

  let page2 = reader.pageSpans(11, 10)
  doAssert page2.isOk and page2.get().len == 10
  doAssert page2.get()[0].spanId == 11 and page2.get()[9].spanId == 20

  let page3 = reader.pageSpans(21, 10)
  doAssert page3.isOk and page3.get().len == 5,
    "final page should be short, got " & $page3.get().len
  doAssert page3.get()[0].spanId == 21 and page3.get()[4].spanId == 25

  let beyond = reader.pageSpans(26, 10)
  doAssert beyond.isOk and beyond.get().len == 0

  # Random access by record index must agree with the settled view.
  var mutReader = reader
  let rec0 = mutReader.readSpan(0)
  doAssert rec0.isOk and rec0.get().spanId == 1
  let rec24 = mutReader.readSpan(24)
  doAssert rec24.isOk and rec24.get().spanId == 25
  doAssert mutReader.readSpan(25).isErr, "out-of-range index must error"

  echo "PASS: span_stream_page_by_span_id"

# ---------------------------------------------------------------------------
# Supporting: the production MultiStreamTraceWriter path
# ---------------------------------------------------------------------------

proc span_stream_multi_stream_writer_gating() {.raises: [].} =
  ## The path a real recorder takes: `registerSpan` on the production
  ## `MultiStreamTraceWriter`, then `close()`.
  ##
  ## Also pins the byte-compatibility contract that makes bit 13 safe to
  ## allocate at all: the span files and the flag bit appear ONLY when a span
  ## was actually registered. Because bit 13 is REJECTING for older readers,
  ## a writer that stamped it unconditionally would break every container it
  ## produced.
  block with_spans:
    let wRes = initMultiStreamWriter("spans.ct", "server",
      recordingId = TestRecordingId)
    doAssert wRes.isOk, "initMultiStreamWriter failed: " & wRes.error
    var w = wRes.get()
    let p0 = w.registerPath("/srv/app.py")
    doAssert p0.isOk
    doAssert w.registerStep(p0.get(), 1, []).isOk

    doAssert w.registerSpan(
      webRequestSpan(1, "GET", "/api/users/42", "200", 0, 0)).isOk
    doAssert w.registerSpan(
      webRequestSpan(2, "POST", "/api/orders", "201", 0, 0)).isOk
    doAssert w.spanCount == 2, "spanCount should be 2, got " & $w.spanCount
    doAssert w.close().isOk
    let bytes = w.toBytes()

    let meta = readMetaDat(readInternalFile(bytes, "meta.dat").get())
    doAssert meta.isOk, "readMetaDat failed: " & meta.error
    doAssert meta.get().hasSpanStream,
      "registering a span must set meta.dat bit 13"
    doAssert hasSpanStreamFiles(bytes), "spans.dat must be present"
    doAssert hasInternalFile(bytes, "spans.idx"), "spans.idx must be present"
    doAssert hasInternalFile(bytes, "spantype.ns"),
      "spantype.ns must be present"

    let rRes = initSpanStreamReader(bytes)
    doAssert rRes.isOk, "initSpanStreamReader failed: " & rRes.error
    let settled = rRes.get().settledSpans()
    doAssert settled.isOk and settled.get().len == 2
    doAssert settled.get()[0].label == "GET /api/users/42"
    doAssert settled.get()[1].label == "POST /api/orders"

  block without_spans:
    let wRes = initMultiStreamWriter("nospans.ct", "server",
      recordingId = TestRecordingId)
    doAssert wRes.isOk
    var w = wRes.get()
    let p0 = w.registerPath("/srv/app.py")
    doAssert p0.isOk
    doAssert w.registerStep(p0.get(), 1, []).isOk
    doAssert w.spanCount == 0
    doAssert w.close().isOk
    let bytes = w.toBytes()

    let meta = readMetaDat(readInternalFile(bytes, "meta.dat").get())
    doAssert meta.isOk, "span-free meta.dat must still parse: " & meta.error
    doAssert not meta.get().hasSpanStream,
      "a recording with no spans must leave bit 13 CLEAR"
    doAssert not hasSpanStreamFiles(bytes),
      "a recording with no spans must not gain spans.dat"
    doAssert not hasInternalFile(bytes, "spans.idx"),
      "a recording with no spans must not gain spans.idx"
    doAssert not hasInternalFile(bytes, "spantype.ns"),
      "a recording with no spans must not gain spantype.ns"

  echo "PASS: span_stream_multi_stream_writer_gating"

# ---------------------------------------------------------------------------

span_stream_roundtrip_both_bindings()
span_stream_tail_during_active_write()
span_stream_last_record_wins()
span_stream_ignored_without_feature_bit()
span_stream_short_chunk_mid_stream()
span_stream_fail_closed_decode()
span_stream_page_by_span_id()
span_stream_multi_stream_writer_gating()
