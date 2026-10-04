## The writer chooses the container profile from a measured RAW-BYTE
## threshold, over the split-stream writer.
##
## Spec: `codetracer-trace-format-spec/ctfs-container.md` §1e (the threshold,
## the converting writer), §1f (a framed member in a compact container), §1d
## (the compact body).
##
## **What each arm is for.**
##
## 1. `test_the_threshold_is_measured_on_the_compact_members` — the quantity
##    is the compact container's member lengths, `Size - 28 - 24*N`, checked
##    against the container written rather than against the writer's report.
## 2. `test_the_boundary_is_asserted_from_both_sides` — a threshold equal to
##    the recording's raw bytes writes the full profile, one byte more the
##    compact one, and both containers answer every query the full container
##    written with no threshold answers. The comparison is shown able to fail:
##    the same recording one step shorter must answer differently.
## 3. `test_the_threshold_is_raw_bytes_not_stored_bytes` — a recording whose
##    full container is far below the threshold but whose raw members are not
##    is written full. CONTROL: a threshold on the stored size would choose
##    compact for it, which is asserted from the sizes the writer produced.
## 4. `test_a_compact_container_carries_no_zstd_frames` — every chunk of every
##    framed member of the compact container is the full container's frame
##    INFLATED (§1f), and none begins with the zstd frame magic. CONTROL: the
##    same scan finds the magic at every chunk of the full container.
## 5. `test_a_file_backed_writer_leaves_the_chosen_container` — at `path`, the
##    compact container when chosen and the full one otherwise, with no
##    temporary left behind.
##
## No mocks: every container comes from this repository's split-stream writer
## and is read back through `NewTraceReader`.

import std/[os, strutils]
import results
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_ctfs/zstd_bindings
import codetracer_trace_types
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/step_map_builder
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/call_stream
import codetracer_profile_writer

const
  TmpDir = "tmp_profile_threshold_choice"
  RecordingId = "01949fcc-7d92-7e9c-8ccc-eeeeeeeeeeee"

proc record(w: var MultiStreamTraceWriter, steps: int, compressible = false) =
  doAssert w.registerPath("/src/a.nim").isOk
  doAssert w.registerPath("/src/b.nim").isOk
  let f = w.registerFunction("work").get()
  var x = 12345'u64
  for i in 0 ..< steps:
    x = x * 6364136223846793005'u64 + 1442695040888963407'u64
    let payload =
      if compressible: newSeq[byte](24)
      else: @[byte(x shr 56), byte(x shr 48), byte(x shr 40), byte(x shr 32)]
    doAssert w.registerStep(uint64(i mod 2), uint64(1 + i mod 90),
      [VariableValue(varnameId: 0, data: payload)]).isOk
    if i mod 5 == 0:
      doAssert w.registerCall(f, []).isOk
    if i mod 5 == 2:
      doAssert w.registerReturn().isOk

proc write(steps: int, threshold: uint64, path = "",
    compressible = false): ProfileWriter =
  result = initProfileWriter(path, "profile_choice", threshold,
    recordingId = RecordingId).get()
  result.writer.record(steps, compressible)
  doAssert result.close().isOk

proc enc(ev: StepEvent): seq[byte] =
  encodeStepEvent(ev, result)

proc answers(data: seq[byte]): seq[string] =
  ## Every answer `NewTraceReader` gives over `data`.
  var r = openNewTraceFromBytes(data).get()
  for i in 0'u64 ..< r.stepCount().get():
    result.add($enc(r.step(i).get()) & " " &
      $r.stepAbsoluteGlobalLineIndex(i).get() & " " & $r.values(i).get())
  for k in 0'u64 ..< r.callCount().get():
    result.add($r.call(k).get())
  var m = openStepMapIn(data).get()
  for ln in m.loadAll().get():
    result.add($ln)

proc test_the_threshold_is_measured_on_the_compact_members() =
  doAssert DefaultRawByteThreshold == 1'u64 shl 20
  let pw = write(3000, DefaultRawByteThreshold)
  doAssert pw.chosenProfile == cpCompact
  let image = pw.toBytes()
  let n = int(readU32LE(image, CompactMemberCountOffset))
  doAssert uint64(image.len - CompactDirectoryOffset - n * CompactDirEntrySize) ==
    pw.rawMemberBytes,
    "the raw bytes reported (" & $pw.rawMemberBytes & ") are not the compact " &
    "container's member bytes"
  echo "PASS: test_the_threshold_is_measured_on_the_compact_members"

proc test_the_boundary_is_asserted_from_both_sides() =
  let full = write(5000, 0)
  doAssert full.chosenProfile == cpFull
  let raw = full.rawMemberBytes
  let want = answers(full.toBytes())
  let atThreshold = write(5000, raw)
  doAssert atThreshold.chosenProfile == cpFull,
    "raw bytes equal to the threshold must write the full profile"
  doAssert atThreshold.toBytes() == full.toBytes()
  let under = write(5000, raw + 1)
  doAssert under.chosenProfile == cpCompact,
    "raw bytes one under the threshold must write the compact profile"
  doAssert isCompactContainer(under.toBytes())
  doAssert answers(under.toBytes()) == want,
    "the compact container does not answer as the full one"
  doAssert answers(write(4999, raw + 1).toBytes()) != want,
    "CONTROL FAILED: a recording one step shorter answers the same"
  echo "PASS: test_the_boundary_is_asserted_from_both_sides"

proc test_the_threshold_is_raw_bytes_not_stored_bytes() =
  let probe = write(20_000, 0, compressible = true)
  let storedSize = uint64(probe.toBytes().len)
  let raw = probe.rawMemberBytes
  doAssert storedSize * 2 < raw, "the recording does not compress: " &
    $storedSize & " stored, " & $raw & " raw"
  let threshold = (storedSize + raw) div 2
  doAssert storedSize < threshold,
    "CONTROL: a stored-size rule would choose compact at this threshold"
  let pw = write(20_000, threshold, compressible = true)
  doAssert pw.chosenProfile == cpFull,
    "a recording of " & $raw & " raw bytes was written compact under a " &
    $threshold & "-byte threshold: the rule measured stored bytes"
  echo "PASS: test_the_threshold_is_raw_bytes_not_stored_bytes"

proc chunkStarts(member, idx: seq[byte]): seq[(int, int)] =
  let chunks = (idx.len - 4) div 8
  for c in 0 ..< chunks:
    let s = int(readU64LE(idx, 4 + 8 * c))
    let e = if c + 1 < chunks: int(readU64LE(idx, 12 + 8 * c)) else: member.len
    result.add((s, e))

proc test_a_compact_container_carries_no_zstd_frames() =
  let full = write(9000, 0).toBytes()
  let compact = write(9000, high(uint64)).toBytes()
  doAssert isCompactContainer(compact)
  const magic = [0x28'u8, 0xB5, 0x2F, 0xFD]
  var chunksSeen = 0
  for stem in ["steps", "values", "calls"]:
    let fd = readInternalFile(full, stem & ".dat").get()
    let fi = readInternalFile(full, stem & ".idx").get()
    let cd = readInternalFile(compact, stem & ".dat").get()
    let ci = readInternalFile(compact, stem & ".idx").get()
    doAssert fi.len == ci.len and fi[0 ..< 4] == ci[0 ..< 4],
      stem & ".idx changed shape"
    let fc = chunkStarts(fd, fi)
    let cc = chunkStarts(cd, ci)
    doAssert fc.len == cc.len and fc.len > 1, stem & ": " & $fc.len & " chunks"
    for c in 0 ..< fc.len:
      doAssert fd[fc[c][0] ..< fc[c][0] + 4] == @magic,
        "CONTROL FAILED: " & stem & " chunk " & $c & " of the full container " &
        "is not a zstd frame, so the scan below proves nothing"
      doAssert cd[cc[c][0] ..< cc[c][0] + 4] != @magic,
        stem & " chunk " & $c & " of the compact container is a zstd frame"
      var inflated: seq[byte]
      doAssert appendFrameContent(fd.toOpenArray(fc[c][0], fc[c][1] - 1),
        stem, inflated).isOk
      doAssert cd[cc[c][0] ..< cc[c][1]] == inflated,
        stem & " chunk " & $c & " is not the full container's frame inflated"
      inc chunksSeen
  doAssert chunksSeen > 10
  echo "PASS: test_a_compact_container_carries_no_zstd_frames"

proc test_a_file_backed_writer_leaves_the_chosen_container() =
  createDir(TmpDir)
  let small = TmpDir / "small.ct"
  let pw = write(2000, DefaultRawByteThreshold, small)
  doAssert pw.chosenProfile == cpCompact
  doAssert readFile(small) == cast[string](pw.toBytes())
  doAssert answers(cast[seq[byte]](readFile(small))) ==
    answers(write(2000, 0).toBytes())
  let large = TmpDir / "large.ct"
  let pf = write(2000, 1, large)
  doAssert pf.chosenProfile == cpFull
  doAssert readFile(large) == cast[string](pf.toBytes())
  doAssert openNewTrace(large).isOk
  for f in walkDir(TmpDir):
    doAssert not f.path.endsWith(".tmp"), "left behind: " & f.path
  removeDir(TmpDir)
  echo "PASS: test_a_file_backed_writer_leaves_the_chosen_container"

test_the_threshold_is_measured_on_the_compact_members()
test_the_boundary_is_asserted_from_both_sides()
test_the_threshold_is_raw_bytes_not_stored_bytes()
test_a_compact_container_carries_no_zstd_frames()
test_a_file_backed_writer_leaves_the_chosen_container()
echo "ALL PASS: test_profile_threshold_choice"
