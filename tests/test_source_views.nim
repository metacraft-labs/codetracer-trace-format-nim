{.push raises: [].}

## Integration tests for the alternate-source-views wire-format extension
## (spec §"Alternate Source Views (Deminification Support)" in
## ``codetracer-trace-format-spec/internal-files.md``).
##
## Covers the end-to-end path: ``MultiStreamTraceWriter.registerSourceView``
## buffers formatted views together with a sourcemap, ``close()`` emits
## ``source_views.dat`` / ``source_views.off`` into the CTFS container
## AND sets ``FlagHasAlternateSourceViews`` (bit 5) on meta.dat, and
## ``NewTraceReader`` round-trips every field byte-for-byte.
##
## Each assertion uses ``doAssert`` on exact values so regressions show
## up as immediate failures rather than as silent quality drift.

import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/meta_dat

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

proc toBytesSeq(s: string): seq[byte] {.raises: [].} =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

proc test_register_source_view_sets_flag_bit_5() {.raises: [].} =
  ## Registering a single source view must (a) increment the writer's
  ## source-view count, (b) cause ``close()`` to set bit 5 on meta.dat,
  ## and (c) cause the reader to surface
  ## ``meta.hasAlternateSourceViews == true`` plus
  ## ``sourceViewCount() == 1``.
  let writerRes = initMultiStreamWriter("test_sv_flag.ct", "sv_flag")
  doAssert writerRes.isOk, "init failed: " & writerRes.error
  var w = writerRes.get()

  let p0 = w.registerPath("/src/min.js")
  doAssert p0.isOk and p0.get() == 0'u64

  # Emit one step so the exec stream isn't empty (the trace would still
  # close cleanly without it, but a step keeps this test honest about
  # the rest-of-trace machinery still firing alongside source_views).
  doAssert w.registerStep(0, 1, @[]).isOk

  let svRes = w.registerSourceView(
    0'u64, 1'u8, "min.fmt.js",
    toBytesSeq("var x = 1;\nvar y = 2;\n"),
    toBytesSeq("{\"version\":3,\"mappings\":\"\"}"))
  doAssert svRes.isOk, "registerSourceView failed: " & svRes.error
  doAssert svRes.get() == 0'u64,
    "first registerSourceView should return index 0, got " & $svRes.get()

  doAssert w.close().isOk, "close failed"
  let bytes = w.toBytes()
  w.closeCtfs()

  let readerRes = openNewTraceFromBytes(bytes)
  doAssert readerRes.isOk, "open failed: " & readerRes.error
  let reader = readerRes.get()

  doAssert reader.meta.hasAlternateSourceViews,
    "meta.dat must carry FlagHasAlternateSourceViews after registerSourceView"
  doAssert reader.sourceViewCount() == 1'u64,
    "expected 1 source view, got " & $reader.sourceViewCount()
  echo "PASS: test_register_source_view_sets_flag_bit_5"


proc test_source_view_round_trip() {.raises: [].} =
  ## Every field on a SourceViewRecord must round-trip byte-for-byte
  ## through the on-disk encoding.  The known inputs below are picked
  ## so each varint length covers >1-byte cases (content_len = 200
  ## is a 2-byte varint, map_len = 64 is a 2-byte varint).
  let writerRes = initMultiStreamWriter("test_sv_round.ct", "sv_round")
  doAssert writerRes.isOk
  var w = writerRes.get()

  let p0 = w.registerPath("/src/bundle.min.js")
  doAssert p0.isOk and p0.get() == 0'u64

  doAssert w.registerStep(0, 1, @[]).isOk

  # Build content with a known length 200 to exercise a multi-byte
  # content_len varint.  Sourcemap is 64 bytes for the same reason.
  var content = newSeq[byte](200)
  for i in 0 ..< 200:
    content[i] = byte((i * 7 + 3) and 0xFF)
  var smap = newSeq[byte](64)
  for i in 0 ..< 64:
    smap[i] = byte((i * 11 + 5) and 0xFF)

  let svIdxRes = w.registerSourceView(
    0'u64, 2'u8, "bundle.fmt.js", content, smap)
  doAssert svIdxRes.isOk
  doAssert svIdxRes.get() == 0'u64

  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()

  let readerRes = openNewTraceFromBytes(bytes)
  doAssert readerRes.isOk
  let reader = readerRes.get()

  doAssert reader.meta.hasAlternateSourceViews
  doAssert reader.sourceViewCount() == 1'u64

  let svRes = reader.sourceView(0'u64)
  doAssert svRes.isOk, "sourceView(0) failed: " & svRes.error
  let sv = svRes.get()

  doAssert sv.pathId == 0'u64,
    "path_id round-trip: expected 0, got " & $sv.pathId
  doAssert sv.viewKind == 2'u8,
    "view_kind round-trip: expected 2, got " & $sv.viewKind
  doAssert sv.viewName == "bundle.fmt.js",
    "view_name round-trip: expected 'bundle.fmt.js', got '" & sv.viewName & "'"
  doAssert sv.content.len == 200,
    "content length round-trip: expected 200, got " & $sv.content.len
  for i in 0 ..< 200:
    doAssert sv.content[i] == byte((i * 7 + 3) and 0xFF),
      "content[" & $i & "] mismatch"
  doAssert sv.sourcemapV3.len == 64,
    "map length round-trip: expected 64, got " & $sv.sourcemapV3.len
  for i in 0 ..< 64:
    doAssert sv.sourcemapV3[i] == byte((i * 11 + 5) and 0xFF),
      "sourcemap[" & $i & "] mismatch"
  echo "PASS: test_source_view_round_trip"


proc test_legacy_trace_has_no_views() {.raises: [].} =
  ## A writer that never calls ``registerSourceView`` MUST NOT set bit 5
  ## on meta.dat and MUST NOT emit ``source_views.dat`` — the
  ## byte-for-byte back-compat contract for pre-extension traces.
  let writerRes = initMultiStreamWriter("test_sv_legacy.ct", "sv_legacy")
  doAssert writerRes.isOk
  var w = writerRes.get()
  doAssert w.registerPath("/src/a.py").isOk
  doAssert w.registerStep(0, 1, @[]).isOk
  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()

  let readerRes = openNewTraceFromBytes(bytes)
  doAssert readerRes.isOk
  let reader = readerRes.get()

  doAssert not reader.meta.hasAlternateSourceViews,
    "legacy writer must leave FlagHasAlternateSourceViews clear"
  doAssert reader.sourceViewCount() == 0'u64,
    "legacy writer must have 0 source views, got " &
      $reader.sourceViewCount()
  echo "PASS: test_legacy_trace_has_no_views"


proc test_multiple_views_per_path() {.raises: [].} =
  ## Two views for the same ``path_id`` (a hybrid JS file with both
  ## prettier + black hypothetical formatting): both must come back
  ## via ``sourceViewsForPath(pathId)`` in registration order.
  let writerRes = initMultiStreamWriter("test_sv_multi.ct", "sv_multi")
  doAssert writerRes.isOk
  var w = writerRes.get()
  let pA = w.registerPath("/src/x.poly")
  doAssert pA.isOk and pA.get() == 0'u64
  let pB = w.registerPath("/src/y.poly")
  doAssert pB.isOk and pB.get() == 1'u64

  doAssert w.registerStep(0, 1, @[]).isOk

  let v0 = w.registerSourceView(
    0'u64, 1'u8, "x.prettier.poly",
    toBytesSeq("/* prettier */"), toBytesSeq(""))
  doAssert v0.isOk and v0.get() == 0'u64

  # Interleave a view targeting the other path so we exercise the
  # reverse-index reset between pathIds.
  let v1 = w.registerSourceView(
    1'u64, 1'u8, "y.prettier.poly",
    toBytesSeq("/* prettier y */"), toBytesSeq(""))
  doAssert v1.isOk and v1.get() == 1'u64

  let v2 = w.registerSourceView(
    0'u64, 2'u8, "x.black.poly",
    toBytesSeq("# black"), toBytesSeq("{}"))
  doAssert v2.isOk and v2.get() == 2'u64

  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()

  let readerRes = openNewTraceFromBytes(bytes)
  doAssert readerRes.isOk
  let reader = readerRes.get()

  doAssert reader.sourceViewCount() == 3'u64

  let viewsForA = reader.sourceViewsForPath(0'u64)
  doAssert viewsForA.len == 2,
    "expected 2 views for path 0, got " & $viewsForA.len
  doAssert viewsForA[0] == 0'u64,
    "first view for path 0 should be index 0, got " & $viewsForA[0]
  doAssert viewsForA[1] == 2'u64,
    "second view for path 0 should be index 2, got " & $viewsForA[1]

  let viewsForB = reader.sourceViewsForPath(1'u64)
  doAssert viewsForB.len == 1,
    "expected 1 view for path 1, got " & $viewsForB.len
  doAssert viewsForB[0] == 1'u64,
    "view for path 1 should be index 1, got " & $viewsForB[0]

  # And per-record content stays distinct.
  let sv0 = reader.sourceView(0'u64)
  doAssert sv0.isOk and sv0.get().viewKind == 1'u8 and
    sv0.get().viewName == "x.prettier.poly"
  let sv2 = reader.sourceView(2'u64)
  doAssert sv2.isOk and sv2.get().viewKind == 2'u8 and
    sv2.get().viewName == "x.black.poly"
  echo "PASS: test_multiple_views_per_path"


proc test_register_invalid_path_id_errors() {.raises: [].} =
  ## ``registerSourceView`` MUST reject a ``path_id`` beyond the
  ## currently-registered paths so a malformed index never reaches the
  ## on-disk record.  Tests both the "no paths registered yet" boundary
  ## (path_id = 0 with empty paths.dat) and "one past the last
  ## registered id" (path_id = 1 after registering one path).
  let writerRes = initMultiStreamWriter("test_sv_bad.ct", "sv_bad")
  doAssert writerRes.isOk
  var w = writerRes.get()

  # Case 1: no paths registered yet → path_id = 0 must error.
  let r0 = w.registerSourceView(
    0'u64, 1'u8, "phantom.js", toBytesSeq("x"), toBytesSeq(""))
  doAssert r0.isErr,
    "registerSourceView with no paths registered must error"

  # Case 2: register a path, then use a path_id one past the last.
  doAssert w.registerPath("/src/a.py").isOk
  let r1 = w.registerSourceView(
    1'u64, 1'u8, "stray.py", toBytesSeq("x"), toBytesSeq(""))
  doAssert r1.isErr,
    "registerSourceView with path_id past last registered must error"

  # Sanity: a valid pathId still works after the failed attempts.
  let r2 = w.registerSourceView(
    0'u64, 1'u8, "a.fmt.py", toBytesSeq("x"), toBytesSeq(""))
  doAssert r2.isOk and r2.get() == 0'u64,
    "valid registerSourceView after rejected ones should succeed at index 0"

  doAssert w.close().isOk
  w.closeCtfs()
  echo "PASS: test_register_invalid_path_id_errors"


when isMainModule:
  test_register_source_view_sets_flag_bit_5()
  test_source_view_round_trip()
  test_legacy_trace_has_no_views()
  test_multiple_views_per_path()
  test_register_invalid_path_id_errors()
  echo "ALL test_source_views PASS"
