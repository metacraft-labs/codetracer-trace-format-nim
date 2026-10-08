

## `NewTraceReader` reads version-6 containers in both profiles, and a framed
## member of a compact container as its content.
##
## Spec: `codetracer-trace-format-spec/ctfs-container.md` §1a (version 6 and
## whole-file compression), §1c (the refusals), §1d (the compact body), §1f
## (a framed member in a compact container).
##
## One recording, written by this repository's split-stream writer across
## several chunks of every stream, is read in five forms: the version-5
## container the writer produced, its version-6 full twin, its compact
## container (`compactMembersOf`, §1f), and each of the last two stored under
## whole-file zstd. Every form must answer every query the version-5 one
## answers — each step's record and position, each step's values, each call,
## each I/O event, every path, and the breakpoint index whole and line by
## line.
##
## CONTROL: the compact container laid out from the full container's members
## COPIED VERBATIM (frames and all, which §1e forbids a writer to emit) must
## NOT read as the recording: a reader that inflated whatever it was handed,
## or a comparison that could not tell, would pass it.
##
## No mocks: the containers come from the real writer and the real
## conversion, and are read through the real reader.

import std/[os, options, strutils]
import results
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_trace_types
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/step_map_builder
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/call_stream
import codetracer_trace_writer/io_event_stream
import codetracer_trace_writer/compact_profile

const TmpDir = "tmp_version_6_containers_read"

proc recording(): seq[byte] =
  ## 9,000 steps over three files (three `steps.dat` chunks, thirty-six
  ## `values.dat` chunks), a call every 7 steps, an I/O event every 40, and
  ## a thread switch every 1,000.
  var w = initMultiStreamWriter("", "v6_reads",
    recordingId = "01949fcc-7d92-7e9c-8ccc-dddddddddddd").get()
  for p in ["/src/a.nim", "/src/b.nim", "/src/c.nim"]:
    doAssert w.registerPath(p).isOk
  let f = w.registerFunction("work").get()
  for i in 0 ..< 9000:
    if i mod 1000 == 999:
      doAssert w.registerThreadSwitch(uint64(i div 1000)).isOk
    let vals = @[VariableValue(varnameId: uint64(i mod 5),
      data: @[byte(i and 0xff), byte(i shr 8)])]
    doAssert w.registerStep(uint64(i mod 3), uint64(1 + (i * 7) mod 300),
      vals).isOk
    if i mod 7 == 0:
      doAssert w.registerCall(f, @[CallArg(varnameId: 1,
        value: @[byte(i and 0x7f)])]).isOk
    if i mod 7 == 3:
      doAssert w.registerReturn(@[byte(9)]).isOk
    if i mod 40 == 0:
      doAssert w.registerIOEvent(elkWrite, @[byte(65), byte(i and 0xff)]).isOk
  doAssert w.close().isOk
  result = w.toBytes()
  doAssert w.closeCtfs().isOk

proc toV6Full(v5: seq[byte]): seq[byte] =
  ## The version-6 full twin of a version-5 container: the same body behind
  ## the 24-byte header, so block 0's entry array starts 8 bytes later
  ## (`ctfs-container.md` §1a). Every other block is unchanged.
  let blockSize = int(readU32LE(v5, 8))
  for i in blockSize - 8 ..< blockSize:
    doAssert v5[i] == 0, "block 0 has no 8 bytes to spare"
  result = v5
  for i in countdown(blockSize - 1, V6HeaderSize):
    result[i] = v5[i - 8]
  result[5] = CtfsVersionV6
  for i in 16 ..< V6HeaderSize:
    result[i] = 0

proc enc(ev: StepEvent): seq[byte] =
  encodeStepEvent(ev, result)

proc answers(data: seq[byte]): Result[seq[string], string] =
  ## Every answer the reader gives over `data`, one line each.
  var r = ? openNewTraceFromBytes(data)
  var lines: seq[string]
  for i in 0'u64 ..< r.pathCount():
    lines.add("path " & ? r.path(i))
  let n = ? r.stepCount()
  for i in 0'u64 ..< n:
    let ev = ? r.step(i)
    lines.add("step " & $enc(ev) & " at " & $(? r.stepAbsoluteGlobalLineIndex(i)))
    for v in ? r.values(i):
      lines.add("  value " & $v.varnameId & " " & $v.data)
  for k in 0'u64 ..< (? r.callCount()):
    let c = ? r.call(k)
    lines.add("call " & $c)
  for k in 0'u64 ..< (? r.ioEventCount()):
    let e = ? r.ioEvent(k)
    lines.add("event " & $e)
  var m = ? openStepMapIn(data)
  for ln in ? m.loadAll():
    lines.add("line " & $ln.pathId & ":" & $ln.line & " " & $ln.steps)
    if ? m.lookup(ln.pathId, uint64(ln.line)) != ln.steps:
      return err("lookup disagrees with loadAll at " & $ln.pathId & ":" &
        $ln.line)
  ok(lines)

proc test_every_form_answers_as_version_5() =
  let full = recording()
  let want = answers(full).get()
  doAssert want.len > 19_000, "the recording is smaller than meant: " & $want.len
  let compact = encodeCompactContainer(compactMembersOf(full).get()).get()
  doAssert isCompactContainer(compact)
  let v6 = toV6Full(full)
  let forms = @[
    ("version-6 full", v6),
    ("compact", compact),
    ("version-6 full under whole-file zstd", compressImage(v6).get()),
    ("compact under whole-file zstd", compressImage(compact).get()),
  ]
  for (name, bytes) in forms:
    let got = answers(bytes)
    doAssert got.isOk, name & ": " & got.error
    doAssert got.get().len == want.len,
      name & ": " & $got.get().len & " answers, not " & $want.len
    for i in 0 ..< want.len:
      doAssert got.get()[i] == want[i], name & ", answer " & $i & ": " &
        got.get()[i] & " / " & want[i]
  # From a path, too.
  createDir(TmpDir)
  let p = TmpDir / "compact.ct"
  try: writeFile(p, cast[string](compact)) except IOError: doAssert false
  doAssert openNewTrace(p).isOk
  echo "PASS: test_every_form_answers_as_version_5"

proc test_a_compact_container_with_frames_is_not_the_recording() =
  ## CONTROL. Members copied verbatim keep their zstd frames; a compact
  ## container's chunk is content, so read as one this is not the recording.
  let full = recording()
  let want = answers(full).get()
  let verbatim = encodeCompactContainer(collectFullProfileMembers(full).get()).get()
  let got = answers(verbatim)
  doAssert got.isErr or got.get() != want,
    "a compact container carrying zstd frames read back as the recording: " &
    "the reader inflated what §1f says is content, or the comparison is blind"
  echo "PASS: test_a_compact_container_with_frames_is_not_the_recording"

proc test_version_6_refusals_name_the_value() =
  let compact = encodeCompactContainer(compactMembersOf(recording()).get()).get()
  var badProfile = compact
  badProfile[V6ProfileOffset] = 2
  let a = openNewTraceFromBytes(badProfile)
  doAssert a.isErr and "2" in a.error, $a
  var badScheme = compact
  badScheme[V6CompressionOffset] = 5
  let b = openNewTraceFromBytes(badScheme)
  doAssert b.isErr and "5" in b.error, $b
  var badReserved = compact
  badReserved[V6ReservedOffset + 3] = 1
  let c = openNewTraceFromBytes(badReserved)
  doAssert c.isErr, "a non-zero reserved byte was accepted"
  var shifted = compact
  # The second directory entry's offset, one byte on: §1d check 3.
  let e = compactDirEntryOffset(1) + CompactDirOffsetOffset
  writeU64LE(shifted, e, readU64LE(shifted, e) + 1)
  let d = openNewTraceFromBytes(shifted)
  doAssert d.isErr and "check 3" in d.error, $d
  var v7 = compact
  v7[5] = 7
  let f = openNewTraceFromBytes(v7)
  doAssert f.isErr and "version 7" in f.error and "version 5" in f.error and
    "version 6" in f.error, $f
  echo "PASS: test_version_6_refusals_name_the_value"

test_every_form_answers_as_version_5()
test_a_compact_container_with_frames_is_not_the_recording()
test_version_6_refusals_name_the_value()
removeDir(TmpDir)
echo "ALL PASS: test_version_6_containers_read"
