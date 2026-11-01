## Tests for ct-print's native MCR shard decoder.
##
## The native recorder (codetracer-native-recorder/ct_recorder) writes a
## CTFS container with a binary `meta.dat` (carrying the MCR fields), a
## `paths.dat` interning table, per-thread `tNNNN` event streams and an
## optional `eventlog.dat` / `eventlog.idx` OS event log
## (`MCR-Event-Log-Stream.md` §2). ct-print routes such a container through
## `native_decoder` instead of the multi-stream reader, which would produce
## an empty document with `-1` sentinel counts.
##
## Strategy:
##   1. Build a small native bundle in-process. The container, `meta.dat`
##      and `paths.dat` come from this library's own writers
##      (`createCtfs`, `writeMetaDatToBuffer`, the interning-table writer);
##      the thread-stream and event-log bytes are the recorder's layout,
##      written by hand because the recorder package is not a dependency.
##   2. Decode it with `buildNativeFullDocument` and assert on the JSON:
##      top-level keys, metadata from `meta.dat`'s MCR fields, per-thread
##      event counts, OS event content, ordering by GEID.
##   3. Snapshot test against tests/goldens/native_replay_hello.full.json.
##      Set CT_PRINT_WRITE_GOLDEN=1 to refresh it.
##   4. Mutation tests: a corrupted magic or event size is a hard error,
##      not an empty document.
##   5. The event log is read only under its current names: a bundle that
##      stores it under the retired `event_log.dat` / `event_log.idx` words
##      (names base40 cannot pack, which recorders before the rename stored
##      lossily) is refused naming the member; a bundle with only one of
##      the two members is refused; an entry whose kind byte is outside the
##      stream's table (§2.3, 0-14) is refused by value.
##   6. Real-bundle test, when a native recorder fixture is reachable
##      (CT_NATIVE_FIXTURE=<path>, or the sibling repo's well-known paths);
##      otherwise skipped with a `[skip]` line.
##
## No mocks: every bundle is a real CTFS container decoded by the real
## decoder.

import std/[os, json, options, strutils]
import results
import stew/endians2
import codetracer_trace_types
import codetracer_ctfs
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/interning_table
import native_decoder
import native_bundle_encoding

# ---------------------------------------------------------------------------
# Bundle builder
# ---------------------------------------------------------------------------

const
  Tid0 = 0'u32
  Tid1 = 1'u32
  SourcePath = "/synthetic/src/main.c"

let
  writePayload = @[byte 'h', byte 'i', byte '\n']
  t0Stream = encodeEventHeader(0'u16, 26'u32, Tid0, 1'u64, 100'u64) &
             encodeEventHeader(1'u16, 26'u32, Tid0, 2'u64, 110'u64)

proc buildSyntheticBundle(osKind = 0'u8, withDat = true,
                          withIdx = true): seq[byte] =
  ## A small native MCR bundle:
  ##   meta.dat     — program/args/workdir + MCR fields (platform=linux,
  ##                  tick source "none", granularity "edge",
  ##                  hookProfile=test, hookStrategies=[ldpreload])
  ##   paths.dat    — one registered source path
  ##   t00000000000 — 2 thread events (lock acquire begin/success)
  ##   t00000000001 — 1 thread event (write syscall, payload "hi\n")
  ##   eventlog.dat + eventlog.idx — 1 OS event of kind `osKind`
  ##                  (0 = elkWrite: the write of "hi\n" to stdout)
  var ctfs = createCtfs()

  let meta = writeMetaDatToBuffer(
    TraceMetadata(
      recordingId: "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
      workdir: "/synthetic",
      program: "/synthetic/program",
      args: @["--demo", "fixture"]),
    mcrFields = some(McrMetaFields(
      tickSource: tsMonotonic,
      totalThreads: 2,
      atomicMode: amRelaxed,
      totalEvents: 3,
      platform: "linux",
      tickGranularity: "edge",
      tickSourceStr: "none",
      atomicModeStr: "relaxed",
      hookProfile: "test",
      hookStrategies: @["ldpreload"])))
  var metaFile = ctfs.addFile("meta.dat").get()
  doAssert ctfs.writeToFile(metaFile, meta).isOk

  var tables = initTraceInterningTables(ctfs).get()
  doAssert ctfs.ensurePathId(tables, SourcePath).get() == 0

  var t0File = ctfs.addFile(threadFileName(Tid0)).get()
  doAssert ctfs.writeToFile(t0File, t0Stream).isOk

  var t1Stream = encodeEventHeader(20'u16, 26'u32 + uint32(writePayload.len),
                                   Tid1, 3'u64, 200'u64)
  t1Stream.add writePayload
  var t1File = ctfs.addFile(threadFileName(Tid1)).get()
  doAssert ctfs.writeToFile(t1File, t1Stream).isOk

  if withDat:
    let entry = encodeOsEntry(geid = 3'u64, tick = 200'u64, tid = Tid1,
                              kind = osKind, fd = 1'i32, returnValue = 3'i64,
                              metadata = "stdout", content = writePayload)
    var datFile = ctfs.addFile(EventLogDat).get()
    doAssert ctfs.writeToFile(datFile, encodeOsEventLogChunk(@[entry])).isOk
  if withIdx:
    var idxFile = ctfs.addFile(EventLogIdx).get()
    doAssert ctfs.writeToFile(idxFile, encodeOsEventLogIndex(
      entryCount = 1'u32, chunkOffsets = [0'u64], chunkGeids = [3'u64])).isOk

  result = ctfs.toBytes()
  ctfs.closeCtfs()

proc plantWord(image: var seq[byte], name: string, word: uint64) =
  ## Store member `name`'s root-directory entry under `word` instead.
  let current = base40Encode(name)
  var planted = false
  for i in 0 ..< int(DefaultMaxRootEntries):
    let off = HeaderSize + ExtHeaderSize + i * FileEntrySize + 16
    if off + 8 > image.len: break
    if fromBytesLE(uint64, image.toOpenArray(off, off + 7)) == current:
      let w = toBytesLE(word)
      for k in 0 ..< 8: image[off + k] = w[k]
      planted = true
  doAssert planted, "no directory entry for " & name

proc fullDoc(bytes: seq[byte], strip = false): JsonNode =
  let docR = buildNativeFullDocument(bytes, NativeOpts(stripPaths: strip))
  doAssert docR.isOk, docR.error
  docR.get()

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

proc test_detection_picks_native_bundle() =
  let bytes = buildSyntheticBundle()
  doAssert isNativeBundle(bytes), "expected isNativeBundle to return true"
  let infoR = detectNativeBundle(bytes)
  doAssert infoR.isOk, "detectNativeBundle: " & infoR.error
  let info = infoR.get()
  doAssert info.threadStreams.len == 2,
    "expected 2 thread streams, got " & $info.threadStreams.len
  doAssert info.threadStreams[0][0] == Tid0
  doAssert info.threadStreams[1][0] == Tid1
  doAssert info.hasEventLog
  echo "[ok] test_detection_picks_native_bundle"

proc test_full_document_shape() =
  let doc = fullDoc(buildSyntheticBundle())
  for k in ["metadata", "paths", "functions", "varnames", "types",
            "counts", "threads", "events"]:
    doAssert doc.hasKey(k), "missing top-level key: " & k
  let md = doc["metadata"]
  doAssert md["recorder"].getStr == "native-mcr"
  # `meta.dat` carrying MCR fields is what makes a recording MCR.
  doAssert md["recording_mode"].getStr == "mcr", $md
  doAssert md["program"].getStr == "/synthetic/program", $md
  doAssert md["workdir"].getStr == "/synthetic", $md
  doAssert md["args"] == %*["--demo", "fixture"], $md
  doAssert md["platform"].getStr == "linux", $md
  doAssert md["tick_source"].getStr == "none", $md
  doAssert md["tick_definition"].getStr == "edge", $md
  doAssert md["hook_profile"].getStr == "test", $md
  doAssert md["hook_strategies"] == %*["ldpreload"], $md
  doAssert md["ctfs_version"].getStr == $MetaDatVersion, $md
  doAssert doc["paths"] == %*[SourcePath], $doc["paths"]
  doAssert doc["counts"]["paths"].getInt == 1
  doAssert doc["counts"]["thread_streams"].getInt == 2
  doAssert doc["counts"]["thread_events"].getInt == 3
  doAssert doc["counts"]["io_events"].getInt == 1
  echo "[ok] test_full_document_shape"

proc test_thread_id_field_per_event() =
  let doc = fullDoc(buildSyntheticBundle())
  var thread0Count = 0
  var thread1Count = 0
  for ev in doc["events"].elems:
    doAssert ev.hasKey("thread_id"), "every event must carry thread_id"
    if ev["kind"].getStr == "thread_event":
      let tid = ev["thread_id"].getInt
      if tid == int(Tid0): thread0Count += 1
      elif tid == int(Tid1): thread1Count += 1
  doAssert thread0Count == 2, "thread 0 should have 2 events, got " & $thread0Count
  doAssert thread1Count == 1, "thread 1 should have 1 event, got " & $thread1Count
  echo "[ok] test_thread_id_field_per_event"

proc test_event_ordering_by_geid() =
  let doc = fullDoc(buildSyntheticBundle())
  var lastGeid = -1
  for ev in doc["events"].elems:
    if ev["kind"].getStr == "thread_event":
      let g = ev["geid"].getInt
      doAssert g >= lastGeid,
        "thread events must be sorted by geid, saw " & $g & " after " &
        $lastGeid
      lastGeid = g
  echo "[ok] test_event_ordering_by_geid"

proc test_thread_event_payload_decoded() =
  let doc = fullDoc(buildSyntheticBundle())
  var found = false
  for ev in doc["events"].elems:
    if ev["kind"].getStr == "thread_event" and
        ev["event_type"].getStr == "evOsWrite":
      doAssert ev["payload"]["len"].getInt == 3
      doAssert ev["payload"]["text"].getStr == "hi\n"
      found = true
  doAssert found, "expected an evOsWrite thread event"
  echo "[ok] test_thread_event_payload_decoded"

proc test_os_event_decoded() =
  let doc = fullDoc(buildSyntheticBundle())
  var found = false
  for ev in doc["events"].elems:
    if ev["kind"].getStr == "os_event":
      doAssert ev["os_kind"].getStr == "elkWrite"
      doAssert ev["fd"].getInt == 1
      doAssert ev["return_value"].getInt == 3
      doAssert ev["metadata"].getStr == "stdout"
      doAssert ev["content_text"].getStr == "hi\n"
      found = true
  doAssert found, "expected an elkWrite OS event"
  echo "[ok] test_os_event_decoded"

proc test_thread_start_event_present() =
  ## The bundle uses lock-acquire events as a stand-in for the real thread
  ## spawn/exit events; the decoder names them via nativeEventTypeName (the
  ## table ct_events/event_types.nim publishes).
  let doc = fullDoc(buildSyntheticBundle())
  var sawAcquireBegin = false
  var sawAcquireSuccess = false
  for ev in doc["events"].elems:
    if ev["kind"].getStr == "thread_event":
      let n = ev["event_type"].getStr
      if n == "evSyncLockAcquireBegin": sawAcquireBegin = true
      if n == "evSyncLockAcquireSuccess": sawAcquireSuccess = true
  doAssert sawAcquireBegin and sawAcquireSuccess
  echo "[ok] test_thread_start_event_present"

proc test_determinism() =
  ## Twice through the decoder must produce byte-identical JSON.
  let bytes = buildSyntheticBundle()
  doAssert pretty(fullDoc(bytes), indent = 2) ==
           pretty(fullDoc(bytes), indent = 2),
    "native decode must be deterministic"
  echo "[ok] test_determinism"

proc test_strip_paths() =
  let doc = fullDoc(buildSyntheticBundle(), strip = true)
  doAssert doc["metadata"]["program"].getStr == "<program>",
    "program path must be redacted with --strip-paths"
  doAssert doc["metadata"]["workdir"].getStr == "<workdir>",
    "workdir must be redacted with --strip-paths"
  doAssert doc["metadata"]["args"][0].getStr == "--demo"  # not absolute
  echo "[ok] test_strip_paths"

proc test_mutation_corrupts_magic() =
  ## Flip a byte of the CTFS magic and confirm decoding fails loudly.
  var bytes = buildSyntheticBundle()
  bytes[0] = 0x00
  let docR = buildNativeFullDocument(bytes, NativeOpts(stripPaths: false))
  doAssert docR.isErr,
    "decoder should reject corrupted magic, but it returned ok"
  doAssert "magic" in docR.error.toLowerAscii() or
      "ctfs" in docR.error.toLowerAscii(),
    "error should mention magic/CTFS, got: " & docR.error
  echo "[ok] test_mutation_corrupts_magic (error: " & docR.error & ")"

proc test_mutation_corrupts_event_size() =
  ## Raise thread 0's first EventHeader.size past the end of its stream: the
  ## decoder must report it rather than produce a wrong document.
  var bytes = buildSyntheticBundle()
  var at = -1
  for i in 0 .. bytes.len - t0Stream.len:
    if bytes.toOpenArray(i, i + t0Stream.len - 1) == t0Stream:
      doAssert at < 0, "thread 0's stream bytes occur twice in the image"
      at = i
  doAssert at >= 0, "thread 0's stream bytes not found in the image"
  # size (offset 2, u32 LE) = 255, far beyond the 52-byte stream.
  bytes[at + 2] = 0xFF
  let docR = buildNativeFullDocument(bytes, NativeOpts(stripPaths: false))
  doAssert docR.isErr,
    "decoder should reject corrupted event size, but it returned ok"
  doAssert "size" in docR.error.toLowerAscii() or
      "overrun" in docR.error.toLowerAscii() or
      "truncated" in docR.error.toLowerAscii(),
    "error should mention the size/overrun, got: " & docR.error
  echo "[ok] test_mutation_corrupts_event_size (error: " & docR.error & ")"

proc test_event_log_retired_names_are_refused_by_name() =
  for (current, retired) in [(EventLogDat, RetiredEventLogNames[0]),
                             (EventLogIdx, RetiredEventLogNames[1])]:
    var image = buildSyntheticBundle()
    plantWord(image, current, base40Encode(retired))
    let r = buildNativeFullDocument(image, NativeOpts(stripPaths: false))
    doAssert r.isErr, "a bundle carrying " & retired & " decoded: " & $r.get()
    doAssert ("`" & retired & "`") in r.error,
      "the refusal must name " & retired & ": " & r.error
    doAssert not isNativeBundle(image)
  echo "[ok] test_event_log_retired_names_are_refused_by_name"

proc test_event_log_one_member_without_the_other_is_refused() =
  for (withDat, withIdx, missing) in [(true, false, EventLogIdx),
                                      (false, true, EventLogDat)]:
    let r = buildNativeFullDocument(
      buildSyntheticBundle(withDat = withDat, withIdx = withIdx),
      NativeOpts(stripPaths: false))
    doAssert r.isErr, "a bundle without " & missing & " decoded: " & $r.get()
    doAssert missing in r.error,
      "the refusal must name " & missing & ": " & r.error
  echo "[ok] test_event_log_one_member_without_the_other_is_refused"

proc test_event_log_without_both_members_has_no_os_events() =
  let doc = fullDoc(buildSyntheticBundle(withDat = false, withIdx = false))
  doAssert doc["counts"]["io_events"].getInt == 0
  doAssert doc["counts"]["thread_events"].getInt == 3
  echo "[ok] test_event_log_without_both_members_has_no_os_events"

proc test_event_log_every_assigned_kind_is_named() =
  for kind in 0'u8 .. EventLogMaxKind:
    let doc = fullDoc(buildSyntheticBundle(osKind = kind))
    var names: seq[string] = @[]
    for ev in doc["events"].elems:
      if ev["kind"].getStr == "os_event":
        names.add ev["os_kind"].getStr
        doAssert ev["os_kind_id"].getInt == int(kind)
    doAssert names == @[osEventKindName(kind)], $kind & ": " & $names
    doAssert not names[0].startsWith("elk_unknown"), names[0]
  echo "[ok] test_event_log_every_assigned_kind_is_named"

proc test_event_log_unassigned_kind_is_refused_by_value() =
  for kind in [EventLogMaxKind + 1, 24'u8, 255'u8]:
    let r = buildNativeFullDocument(buildSyntheticBundle(osKind = kind),
                                    NativeOpts(stripPaths: false))
    doAssert r.isErr, "event log kind " & $kind & " decoded: " & $r.get()
    doAssert ("unassigned event log kind " & $kind) in r.error, r.error
  echo "[ok] test_event_log_unassigned_kind_is_refused_by_value"

proc test_golden_snapshot() =
  ## Snapshot comparison against tests/goldens/native_replay_hello.full.json.
  ## The *parsed* JSON is compared, so a JSON formatter re-laying the file
  ## out does not matter. Set CT_PRINT_WRITE_GOLDEN=1 to refresh the golden
  ## after intentional changes.
  let goldenPath = currentSourcePath().parentDir() /
    "goldens" / "native_replay_hello.full.json"
  let actual = pretty(fullDoc(buildSyntheticBundle(), strip = true),
                      indent = 2)

  if getEnv("CT_PRINT_WRITE_GOLDEN", "") == "1":
    createDir(goldenPath.parentDir())
    writeFile(goldenPath, actual & "\n")
    echo "[wrote golden] " & goldenPath
    return

  doAssert fileExists(goldenPath), "missing golden " & goldenPath &
    " (CT_PRINT_WRITE_GOLDEN=1 writes it)"
  var expectedNode: JsonNode
  try:
    expectedNode = parseJson(readFile(goldenPath))
  except CatchableError as e:
    raise newException(AssertionDefect,
      "golden " & goldenPath & " is not valid JSON: " & e.msg)
  if actual != pretty(expectedNode, indent = 2):
    let diffPath = goldenPath & ".actual"
    writeFile(diffPath, actual)
    raise newException(AssertionDefect,
      "ct-print --full (native) output diverges from golden.\n" &
      "  expected: " & goldenPath & "\n" &
      "  actual:   " & diffPath & "\n" &
      "  To accept new output: CT_PRINT_WRITE_GOLDEN=1 nim c -r ...\n")
  echo "[ok] test_golden_snapshot"

proc test_real_fixture() =
  ## Runs only when a real native bundle is reachable: CT_NATIVE_FIXTURE, or
  ## the sibling recorder repo's well-known paths. Skipped loudly otherwise.
  let envFixture = getEnv("CT_NATIVE_FIXTURE", "")
  let candidates = @[
    envFixture,
    expandTilde("~/metacraft/codetracer-native-recorder/tests/dotnet-poc/" &
                "linux_replay_hello.ct"),
    expandTilde("~/metacraft/codetracer-native-recorder/ct_cooperative/" &
                "build/android_e2e_test/android_e2e_test.ct"),
  ]
  var fixturePath = ""
  for c in candidates:
    if c.len > 0 and fileExists(c):
      fixturePath = c
      break
  if fixturePath.len == 0:
    echo "[skip] test_real_fixture (set CT_NATIVE_FIXTURE=<path> to enable)"
    return
  let docR = decodeNativeFromFile(fixturePath, NativeOpts(stripPaths: true))
  doAssert docR.isOk, "real-fixture decode failed: " & docR.error
  let doc = docR.get()
  doAssert doc.hasKey("counts"), "real fixture missing counts"
  doAssert doc["metadata"]["recorder"].getStr == "native-mcr"
  echo "[ok] test_real_fixture (" & fixturePath & "): " &
      $doc["counts"]["thread_streams"].getInt & " threads, " &
      $doc["counts"]["thread_events"].getInt & " thread events, " &
      $doc["counts"]["io_events"].getInt & " io events"

# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

test_detection_picks_native_bundle()
test_full_document_shape()
test_thread_id_field_per_event()
test_event_ordering_by_geid()
test_thread_event_payload_decoded()
test_os_event_decoded()
test_thread_start_event_present()
test_determinism()
test_strip_paths()
test_mutation_corrupts_magic()
test_mutation_corrupts_event_size()
test_event_log_retired_names_are_refused_by_name()
test_event_log_one_member_without_the_other_is_refused()
test_event_log_without_both_members_has_no_os_events()
test_event_log_every_assigned_kind_is_named()
test_event_log_unassigned_kind_is_refused_by_value()
test_golden_snapshot()
test_real_fixture()

echo ""
echo "All ct-print native decoder tests passed."
