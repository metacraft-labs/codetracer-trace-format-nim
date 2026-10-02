{.push raises: [].}

## CCP-4: the writer chooses the profile from a measured RAW-BYTE threshold.
##
## Spec: `codetracer-trace-format-spec/ctfs-container.md` §1e (the threshold,
## the switchover rule, the raw-member obligation), §1d (the compact body).
##
## **What each arm is for, and why it is shaped the way it is.**
##
## 1. `test_the_threshold_is_configuration_in_raw_stream_bytes` — the default
##    is 1 MiB, the unit is RAW stream bytes, and the unit is checked by
##    arithmetic rather than by reading the constant back: a plain `Step` is 17
##    split-binary bytes, so N steps must move the measured quantity by exactly
##    17*N. An arm that only asserted `DefaultRawByteThreshold == 1 shl 20`
##    would pin the number and say nothing about what it counts.
##
## 2. `test_the_switchover_loses_no_events` — the arm the milestone calls the
##    one place in this campaign where a bug is SILENT. A recording crossing
##    the threshold is written twice, once switching mid-recording and once
##    with the threshold at zero (full throughout), and the two containers must
##    answer every query identically. THE CONTROL IS A PLANTED DROP: the same
##    recording written with `dropBufferedPrefix` must make the comparison
##    FAIL, and the dropped container must still OPEN CLEANLY — which is the
##    whole point, since a full container missing its prefix is otherwise
##    perfectly well-formed and nothing downstream can tell.
##
## 3. `test_the_boundary_is_asserted_from_both_sides` — boundary recordings one
##    event under and one event over the DEFAULT threshold, each asserted to
##    produce the expected profile AND to contain every event. The under-side
##    container is compact and is compared against the same recording written
##    full, so "contains every event" is checked against an oracle rather than
##    against its own event count.
##
## 4. `test_the_threshold_is_measured_in_raw_bytes` — two recordings of EQUAL
##    raw size and wildly different compressibility must take the SAME path.
##    CONTROL: the same two recordings are classified by a threshold on
##    COMPRESSED size, using the compressed sizes the writer itself produced,
##    and that rule must SPLIT them. Without the control the arm would pass on
##    either implementation.
##
## 5. `test_a_compact_container_carries_no_zstd_frames` — CCP-2's finding 2:
##    its reference encoder copies member payloads VERBATIM, so a compact
##    container converted from a full one inherits that container's per-member
##    zstd frames, and emitting genuinely raw members is CCP-4's writer's job.
##    Asserted two ways: zero occurrences of the zstd frame magic in the
##    writer-produced compact image, and the raw `events.log` equal to the full
##    writer's own `events.log` INFLATED — so "raw" is checked against what the
##    full profile would have compressed rather than against the writer's
##    intent. CONTROL: the scan must find frames in the full container, or it
##    is a scanner that returns zero for everything.
##
## NO MOCKS. Both containers of every pair are produced by this repository's
## real writers — the compact one by `ProfileWriter` through CCP-2's reference
## encoder, the full one by `TraceWriter` through the streaming CTFS container —
## and both are read back through the real readers (`openTrace` + `readEvents`
## for the full profile, `readCompactTrace` + §1d's six checks for the compact
## one). The only injected behaviour is `ProfileWriterFaults.dropBufferedPrefix`,
## which is a deliberate DEFECT rather than a stand-in: it exists so arm 2's
## control can show the arm is able to fail.

import std/[os, strutils]
import results
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_ctfs/compact
import codetracer_ctfs/base40
import codetracer_ctfs/chunk_index
import codetracer_ctfs/zstd_bindings
import codetracer_trace_types
import codetracer_trace_writer
import codetracer_trace_writer/split_binary
import codetracer_trace_writer/meta_dat
import codetracer_trace_reader
import codetracer_profile_writer

const
  TmpDir = "tmp_profile_threshold_choice"
  PlainStepBytes = 17
    ## `split_binary.nim`: a `Step` with no column is tag(0) + pathId(8) +
    ## line(8). The arithmetic the raw-byte arm checks rests on this, so it is
    ## named once and asserted against the encoder rather than assumed.
  ZstdFrameMagic = [0x28'u8, 0xb5, 0x2f, 0xfd]
    ## The zstd frame magic. §1d forbids per-member compression in a compact
    ## container, and this is what that forbids, in bytes.
  RecordingId = "01949fcc-7d92-7e9c-8ccc-c4c4c4c4c4c4"
    ## Pinned so two containers of "the same recording" really are, and so the
    ## `meta.dat` preamble is byte-identical across the pair — which is what
    ## makes two recordings' RAW sizes comparable to the byte in arm 4.

var failures = 0

proc check(cond: bool, msg: string) =
  if not cond:
    echo "  FAIL: ", msg
    failures += 1

proc fail(msg: string) =
  echo "  FAIL: ", msg
  failures += 1

proc ensureTmp() =
  try:
    createDir(TmpDir)
  except OSError, IOError:
    doAssert false, "could not create " & TmpDir

proc readFileBytes(path: string): seq[byte] =
  let r = readCtfsFromFile(path)
  doAssert r.isOk, "reading " & path & ": " & r.unsafeError
  r.get()

# ---------------------------------------------------------------------------
# The recordings. One generator, two shapes, so a pair differs in exactly the
# property under test.
# ---------------------------------------------------------------------------

type RecordingShape = enum
  rsCompressible   ## every step identical: 17 bytes that repeat
  rsIncompressible ## pathId and line from a PRNG: ~random bytes
  rsMixed          ## paths, steps and calls, so a switchover's prefix is not
                   ## one event kind repeated

proc xorshift(state: var uint64): uint64 =
  ## A deterministic PRNG, written out rather than taken from `std/random`, so
  ## the incompressible recording is byte-identical on every run and on every
  ## platform. Reproducibility matters here: the arm's control compares a
  ## compressed size against a threshold.
  var x = state
  x = x xor (x shl 13)
  x = x xor (x shr 7)
  x = x xor (x shl 17)
  state = x
  x

proc recordingEvents(shape: RecordingShape, count: int):
    seq[TraceLowLevelEvent] =
  ## `count` events of the given shape. Every event is a plain `Step` for the
  ## two single-shape recordings, so their raw sizes are equal by construction
  ## (17 bytes each) and the only difference between them is compressibility.
  var state = 0x243f6a8885a308d3'u64
  case shape
  of rsCompressible:
    for _ in 0 ..< count:
      result.add(TraceLowLevelEvent(kind: tleStep,
        step: StepRecord(pathId: PathId(0), line: Line(1))))
  of rsIncompressible:
    for _ in 0 ..< count:
      let a = xorshift(state)
      let b = xorshift(state)
      # `cast` and not `int64(b)`: a `uint64` above `high(int64)` is a
      # RangeDefect under a checked conversion, and the point of this shape is
      # that all eight bytes of the line field are random. The reinterpretation
      # keeps the bytes and is what a recorder emitting a negative line would
      # produce anyway.
      result.add(TraceLowLevelEvent(kind: tleStep,
        step: StepRecord(pathId: PathId(a), line: Line(cast[int64](b)))))
  of rsMixed:
    for i in 0 ..< count:
      if i mod 500 == 0:
        result.add(TraceLowLevelEvent(kind: tlePath,
          path: "/src/module_" & $(i div 500) & ".py"))
      elif i mod 97 == 0:
        result.add(TraceLowLevelEvent(kind: tleCall,
          callRecord: CallRecord(functionId: FunctionId(i mod 11), args: @[])))
      else:
        let a = xorshift(state)
        result.add(TraceLowLevelEvent(kind: tleStep,
          step: StepRecord(pathId: PathId(uint64(i mod 8)),
                           line: Line(int64(a mod 4096)))))

proc writeRecording(path: string, events: openArray[TraceLowLevelEvent],
    threshold: uint64,
    faults: ProfileWriterFaults = ProfileWriterFaults()):
    tuple[profile: CtfsProfile, rawBytes: uint64, switched: bool,
          bufferedAtSwitch: uint64] =
  ## Write `events` through the profile-choosing writer and report what it
  ## decided. The same entry point for every arm, including the threshold-zero
  ## oracle, so the oracle is not a second implementation.
  var wRes = newProfileWriter(path, "ccp4_threshold", @["--arm"],
    workdir = "/home/test", rawByteThreshold = threshold,
    recordingId = RecordingId, faults = faults)
  doAssert wRes.isOk, "newProfileWriter: " & wRes.unsafeError
  var w = wRes.get()
  for ev in events:
    let r = w.writeEvent(ev)
    doAssert r.isOk, "writeEvent: " & r.unsafeError
  let raw = w.rawStreamBytes()
  let switched = w.switchedToFull()
  let atSwitch = w.bufferedEventsAtSwitch()
  let c = w.close()
  doAssert c.isOk, "close: " & c.unsafeError
  (w.chosenProfile(), raw, switched, atSwitch)

# ---------------------------------------------------------------------------
# The query surface, answered from either profile.
# ---------------------------------------------------------------------------

type QueryAnswers = object
  profile: CtfsProfile
  eventCount: int
  eventBytes: seq[seq[byte]]
    ## Each event re-encoded through the split-binary encoder. Comparing
    ## re-encoded bytes rather than fields is a FIELD-BY-FIELD comparison that
    ## cannot be hand-maintained into incompleteness: a field this file forgot
    ## to list would still change the bytes.
  paths: seq[string]
  program: string
  workdir: string
  args: seq[string]
  recordingId: string
  memberNames: seq[string]

proc canonicalBytes(ev: TraceLowLevelEvent): seq[byte] =
  var enc = SplitBinaryEncoder.init(1024)
  enc.encodeEvent(ev)
  result = enc.getBytes()
  enc.destroy()

proc fullProfileMemberNames(data: openArray[byte]): seq[string]
    {.raises: [].} =
  ## The member names of a FULL container, in `FileEntry`-array order, read out
  ## of the array itself.
  ##
  ## The ORDER is the point. The profile-choosing writer claims to emit a
  ## compact container's members in the order the full profile's array carries
  ## them -- `events.log`, `events.fmt`, `meta.dat`, then the paths table --
  ## which is what makes §1d's "a compact and a full container of one recording
  ## name the same members identically" true of THIS writer and not only of the
  ## reference encoder. A comparison against a hand-listed set of names would
  ## have been true whatever order the writer picked, so the names come from the
  ## container.
  let blockSize = readU32LE(data, 8)
  doAssert blockSize != 0'u32, "the full container declares a zero block size"
  var maxEntries = readU32LE(data, 12)
  if maxEntries == 0'u32:
    maxEntries = uint32(
      (int(blockSize) - HeaderSize - ExtHeaderSize) div FileEntrySize)
  for i in 0 ..< int(maxEntries):
    let off = HeaderSize + ExtHeaderSize + i * FileEntrySize
    if off + FileEntrySize > data.len:
      break
    let entrySize = readU64LE(data, off)
    let entryMap = readU64LE(data, off + 8)
    let encoded = readU64LE(data, off + 16)
    if entrySize == 0'u64 and entryMap == 0'u64 and encoded == 0'u64:
      continue
    result.add(base40Decode(encoded))

proc answersFromFull(path: string): QueryAnswers =
  ## Every query, answered through the production full-profile reader.
  var rRes = openTrace(path)
  doAssert rRes.isOk, "openTrace " & path & ": " & rRes.unsafeError
  var r = rRes.get()
  let ev = r.readEvents()
  doAssert ev.isOk, "readEvents " & path & ": " & ev.unsafeError
  result.profile = cpFull
  result.eventCount = r.events.len
  for e in r.events:
    result.eventBytes.add(canonicalBytes(e))
  result.paths = r.paths
  result.program = r.metadata.program
  result.workdir = r.metadata.workdir
  result.args = r.metadata.args
  result.recordingId = r.metadata.recordingId
  result.memberNames = fullProfileMemberNames(readFileBytes(path))

proc answersFromCompact(path: string): QueryAnswers =
  ## Every query, answered through the compact load path: §1d's six checks,
  ## then a directory slice and one decode. No chunk walk and no decompressor.
  let image = readFileBytes(path)
  let tRes = readCompactTrace(image)
  doAssert tRes.isOk, "readCompactTrace " & path & ": " & tRes.unsafeError
  let t = tRes.get()
  let mRes = readMetaDat(t.metaDat)
  doAssert mRes.isOk, "readMetaDat of the compact container: " & mRes.unsafeError
  let m = mRes.get()
  result.profile = cpCompact
  result.eventCount = t.events.len
  for e in t.events:
    result.eventBytes.add(canonicalBytes(e))
  result.paths = t.paths
  result.program = m.program
  result.workdir = m.workdir
  result.args = m.args
  result.recordingId = m.recordingId
  result.memberNames = t.memberNames

proc answersFor(path: string, profile: CtfsProfile): QueryAnswers =
  if profile == cpCompact: answersFromCompact(path) else: answersFromFull(path)

proc compareAnswers(a, b: QueryAnswers, labelA, labelB: string): seq[string] =
  ## Every difference, named. Returns the empty sequence when the two agree on
  ## the whole surface. Returned rather than asserted so arm 2's control can
  ## require it to be NON-empty.
  if a.eventCount != b.eventCount:
    result.add("event count: " & labelA & " has " & $a.eventCount & ", " &
      labelB & " has " & $b.eventCount)
  let n = min(a.eventCount, b.eventCount)
  var firstDiff = -1
  var diffCount = 0
  for i in 0 ..< n:
    if a.eventBytes[i] != b.eventBytes[i]:
      if firstDiff < 0:
        firstDiff = i
      diffCount += 1
  if firstDiff >= 0:
    result.add("events differ at " & $diffCount & " of " & $n &
      " shared positions, first at index " & $firstDiff)
  if a.paths != b.paths:
    result.add("paths: " & labelA & " has " & $a.paths.len & " (" &
      $a.paths & "), " & labelB & " has " & $b.paths.len & " (" &
      $b.paths & ")")
  if a.program != b.program:
    result.add("program: '" & a.program & "' vs '" & b.program & "'")
  if a.workdir != b.workdir:
    result.add("workdir: '" & a.workdir & "' vs '" & b.workdir & "'")
  if a.args != b.args:
    result.add("args: " & $a.args & " vs " & $b.args)
  if a.recordingId != b.recordingId:
    result.add("recording id: '" & a.recordingId & "' vs '" &
      b.recordingId & "'")
  if a.memberNames != b.memberNames:
    result.add("member names: " & $a.memberNames & " vs " & $b.memberNames)

# ---------------------------------------------------------------------------
# Zstd frame census and the full profile's own events.log, inflated.
# ---------------------------------------------------------------------------

proc countZstdFrameMagic(data: openArray[byte]): int =
  for i in 0 .. data.len - 4:
    if data[i] == ZstdFrameMagic[0] and data[i + 1] == ZstdFrameMagic[1] and
       data[i + 2] == ZstdFrameMagic[2] and data[i + 3] == ZstdFrameMagic[3]:
      result += 1

proc fullEventsLogInflated(path: string): seq[byte] =
  ## The full container's `events.log` with every chunk inflated and the
  ## results concatenated — i.e. the bytes the full writer compressed. This is
  ## the ORACLE for the compact profile's raw `events.log`: it is produced by
  ## walking the full profile's own chunk framing with the production reader's
  ## own inflater, so the comparison is against what the other profile holds
  ## rather than against this writer's claim about it.
  let data = readFileBytes(path)
  let blockSize = readU32LE(data, 8)
  let maxEntries = readU32LE(data, 12)
  let logRes = readInternalFile(data, "events.log", blockSize, maxEntries)
  doAssert logRes.isOk, "reading events.log of " & path & ": " & logRes.unsafeError
  let log = logRes.get()
  var pos = 0
  if log.len >= HeaderSize and hasCtfsMagic(log):
    pos = HeaderSize
  while pos + ChunkIndexEntrySize <= log.len:
    let chunk = decodeChunkHeader(log, pos)
    if chunk.compressedSize == 0:
      break
    pos += ChunkIndexEntrySize
    doAssert pos + int(chunk.compressedSize) <= log.len,
      "chunk extends beyond events.log of " & path
    let inflated = inflateEventsLogChunk(
      log[pos ..< pos + int(chunk.compressedSize)])
    doAssert inflated.isOk, "inflating a chunk of " & path & ": " &
      inflated.unsafeError
    for b in inflated.get():
      result.add(b)
    pos += int(chunk.compressedSize)

proc fullEventsLogCompressedBytes(path: string): int =
  ## The sum of the full container's chunk `compressedSize` fields: the
  ## COMPRESSED size of the event stream, as the writer itself produced it.
  ## Arm 4's control is built on this rather than on a fresh call to
  ## `ZSTD_compress`, so the alternative rule it falsifies is the one a writer
  ## would actually have implemented.
  let data = readFileBytes(path)
  let logRes = readInternalFile(data, "events.log", readU32LE(data, 8),
    readU32LE(data, 12))
  doAssert logRes.isOk, "reading events.log of " & path & ": " & logRes.unsafeError
  let log = logRes.get()
  var pos = 0
  if log.len >= HeaderSize and hasCtfsMagic(log):
    pos = HeaderSize
  while pos + ChunkIndexEntrySize <= log.len:
    let chunk = decodeChunkHeader(log, pos)
    if chunk.compressedSize == 0:
      break
    result += int(chunk.compressedSize)
    pos += ChunkIndexEntrySize + int(chunk.compressedSize)

# ---------------------------------------------------------------------------
# Arm 1: the threshold is configuration, in raw stream bytes, 1 MiB default.
# ---------------------------------------------------------------------------

proc test_the_threshold_is_configuration_in_raw_stream_bytes() =
  echo "test_the_threshold_is_configuration_in_raw_stream_bytes"

  check(DefaultRawByteThreshold == 1'u64 shl 20,
    "the default threshold is " & $DefaultRawByteThreshold &
    ", not 1 MiB (1048576)")

  # The UNIT, checked by arithmetic. A plain Step is 17 split-binary bytes, so
  # N of them must move the measured quantity by exactly 17*N. This is what
  # distinguishes "raw stream bytes" from any other unit the field could have
  # been counting.
  let probe = canonicalBytes(TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(0), line: Line(1))))
  check(probe.len == PlainStepBytes,
    "a plain Step encodes to " & $probe.len & " bytes, not " &
    $PlainStepBytes & ": the arithmetic below rests on this")

  var wRes = newProfileWriter(TmpDir / "unit.ct", "ccp4_threshold",
    @["--arm"], workdir = "/home/test",
    rawByteThreshold = DefaultRawByteThreshold, recordingId = RecordingId)
  doAssert wRes.isOk, "newProfileWriter: " & wRes.unsafeError
  var w = wRes.get()
  let preamble = w.rawStreamBytes()
  check(preamble > 0'u64,
    "the measured quantity is 0 before any event: every member the compact " &
    "container would carry is counted, including meta.dat and events.fmt")
  check(w.isBuffering(), "the writer is not buffering at open")
  check(w.chosenProfile() == cpCompact,
    "a writer under its threshold does not report the compact profile")

  const Steps = 1000
  for i in 0 ..< Steps:
    doAssert w.writeStep(0, 1).isOk, "writeStep " & $i
  let afterSteps = w.rawStreamBytes()
  check(afterSteps == preamble + uint64(Steps * PlainStepBytes),
    "1000 plain steps moved the measured quantity by " &
    $(afterSteps - preamble) & " bytes, not " & $(Steps * PlainStepBytes) &
    ": the unit is not raw stream bytes")

  # A Path event adds its string to paths.dat AND an 8-byte offset, plus the
  # table's leading zero on first use. Checked because the measured quantity
  # claims to cover every member and not only the event stream.
  let beforePath = w.rawStreamBytes()
  const P = "/src/first.py"
  doAssert w.writePath(P).isOk, "writePath"
  let afterPath = w.rawStreamBytes()
  let pathEventBytes = canonicalBytes(
    TraceLowLevelEvent(kind: tlePath, path: P)).len
  check(afterPath - beforePath ==
      uint64(pathEventBytes + P.len + 16),
    "a first Path event moved the measured quantity by " &
    $(afterPath - beforePath) & ", not " &
    $(pathEventBytes + P.len + 16) &
    " (the event's own bytes, the paths.dat record, and two paths.off " &
    "entries — the leading zero and the record's end)")
  doAssert w.close().isOk, "close"

  # The threshold is CONFIGURATION: a writer given a different one honours it.
  var w2Res = newProfileWriter(TmpDir / "unit_cfg.ct", "ccp4_threshold",
    @["--arm"], workdir = "/home/test", rawByteThreshold = 4096'u64,
    recordingId = RecordingId)
  doAssert w2Res.isOk, "newProfileWriter: " & w2Res.unsafeError
  var w2 = w2Res.get()
  check(w2.rawByteThreshold() == 4096'u64,
    "the configured threshold reads back as " & $w2.rawByteThreshold())
  var crossedAt = 0'u64
  for i in 0 ..< 1000:
    doAssert w2.writeStep(0, 1).isOk, "writeStep " & $i
    if w2.switchedToFull() and crossedAt == 0'u64:
      crossedAt = w2.rawStreamBytes()
      break
  check(w2.switchedToFull(),
    "a 4096-byte threshold was not crossed by 1000 steps (17,000 bytes)")
  check(crossedAt >= 4096'u64 and crossedAt < 4096'u64 + PlainStepBytes.uint64,
    "the switchover happened at " & $crossedAt &
    " raw bytes: it must happen on the FIRST event that reaches the " &
    "threshold, so the measured size at the switch is in [4096, 4113)")
  doAssert w2.close().isOk, "close"
  echo "  threshold default = ", DefaultRawByteThreshold,
    " raw stream bytes; preamble = ", preamble,
    " B (meta.dat + events.fmt); plain Step = ", PlainStepBytes, " B"
  echo (if failures == 0: "PASS" else: "FAILED"),
    " test_the_threshold_is_configuration_in_raw_stream_bytes"

# ---------------------------------------------------------------------------
# Arm 2: the switchover loses no events, with the planted-drop control.
# ---------------------------------------------------------------------------

proc test_the_switchover_loses_no_events() =
  echo "test_the_switchover_loses_no_events"
  let before = failures

  # A threshold small enough that the switchover lands in the MIDDLE of the
  # recording, with events of three kinds on both sides of it. 24,576 bytes is
  # about 1,500 events in, of roughly 6,000.
  const Threshold = 24_576'u64
  let events = recordingEvents(rsMixed, 6000)

  let switchedPath = TmpDir / "switch_default.ct"
  let oraclePath = TmpDir / "switch_oracle.ct"
  let droppedPath = TmpDir / "switch_dropped.ct"

  let sw = writeRecording(switchedPath, events, Threshold)
  let orc = writeRecording(oraclePath, events, 0'u64)

  check(sw.profile == cpFull,
    "a recording crossing the threshold produced profile " & $sw.profile)
  check(sw.switched, "the writer did not record a switchover")
  check(sw.bufferedAtSwitch > 0'u64,
    "the switchover happened with nothing buffered: the arm would then be " &
    "testing a pass-through and not a switchover")
  check(sw.bufferedAtSwitch < uint64(events.len),
    "the whole recording was buffered before the switch (" &
    $sw.bufferedAtSwitch & " of " & $events.len &
    "): the switchover is not mid-recording")
  check(orc.profile == cpFull,
    "the threshold-zero oracle produced profile " & $orc.profile)
  # The threshold-zero oracle DOES report a switchover, and the expectation
  # here was corrected rather than the code: at zero the switch happens in
  # `newProfileWriter`, before any event, through the SAME code path as any
  # other threshold. That is deliberate — an oracle reached by a second code
  # path would not be an oracle — so what distinguishes "full throughout" from
  # "switched mid-recording" is not the flag but the number of events that were
  # buffered, which must be zero.
  check(orc.bufferedAtSwitch == 0'u64,
    "the threshold-zero oracle buffered " & $orc.bufferedAtSwitch &
    " events before going full: at zero it must be full from its first byte")

  let a = answersFor(switchedPath, sw.profile)
  let b = answersFor(oraclePath, orc.profile)
  let diffs = compareAnswers(a, b, "switched", "always-full")
  for d in diffs:
    fail("switched vs always-full: " & d)
  check(a.eventCount == events.len,
    "the switched container holds " & $a.eventCount & " of " &
    $events.len & " events")

  # THE PLANTED DROP. The milestone's own words: an arm that cannot fail on a
  # dropped prefix is not testing the thing that matters.
  let dr = writeRecording(droppedPath, events, Threshold,
    ProfileWriterFaults(dropBufferedPrefix: true))
  check(dr.profile == cpFull,
    "the planted-drop container is not a full container")
  let c = answersFor(droppedPath, dr.profile)
  let dropDiffs = compareAnswers(c, b, "planted-drop", "always-full")
  check(dropDiffs.len > 0,
    "THE CONTROL DID NOT FIRE: a container written with the buffered " &
    "prefix deliberately discarded compared EQUAL to the oracle, so this " &
    "arm cannot detect a dropped prefix and proves nothing")
  check(c.eventCount == events.len - int(dr.bufferedAtSwitch),
    "the planted-drop container holds " & $c.eventCount &
    " events; dropping a prefix of " & $dr.bufferedAtSwitch & " of " &
    $events.len & " should leave " &
    $(events.len - int(dr.bufferedAtSwitch)))

  # And the half that makes the defect SILENT: the dropped container is
  # perfectly well-formed. It opens, its metadata reads, its events decode.
  # Nothing but a comparison against the oracle can tell it is incomplete.
  var droppedReader = openTrace(droppedPath)
  check(droppedReader.isOk,
    "the planted-drop container does not even open: then the defect is not " &
    "silent and this arm is pinning the wrong hazard — " &
    (if droppedReader.isErr: droppedReader.unsafeError else: ""))
  if droppedReader.isOk:
    var rr = droppedReader.get()
    check(rr.readEvents().isOk,
      "the planted-drop container's events do not decode")
    check(rr.metadata.program == "ccp4_threshold",
      "the planted-drop container's metadata does not read back")

  echo "  switchover at ", sw.bufferedAtSwitch, " of ", events.len,
    " events (threshold ", Threshold, " raw bytes); ",
    a.eventCount, " events in both containers; control reported ",
    dropDiffs.len, " difference(s), dropped container holds ",
    c.eventCount
  echo (if failures == before: "PASS" else: "FAILED"),
    " test_the_switchover_loses_no_events"

# ---------------------------------------------------------------------------
# Arm 3: the boundary, from both sides, at the DEFAULT threshold.
# ---------------------------------------------------------------------------

proc test_the_boundary_is_asserted_from_both_sides() =
  echo "test_the_boundary_is_asserted_from_both_sides"
  let before = failures

  # The boundary is computed from the writer's own measured preamble, not
  # guessed: the largest number of plain steps whose raw size stays strictly
  # under the default threshold, and that number plus one.
  var probeRes = newProfileWriter(TmpDir / "probe.ct", "ccp4_threshold",
    @["--arm"], workdir = "/home/test",
    rawByteThreshold = DefaultRawByteThreshold, recordingId = RecordingId)
  doAssert probeRes.isOk, "newProfileWriter: " & probeRes.unsafeError
  var probe = probeRes.get()
  let preamble = probe.rawStreamBytes()
  doAssert probe.close().isOk, "close probe"

  let underCount = int((DefaultRawByteThreshold - 1 - preamble) div
    uint64(PlainStepBytes))
  let underEvents = recordingEvents(rsCompressible, underCount)
  let overEvents = recordingEvents(rsCompressible, underCount + 1)

  let underPath = TmpDir / "boundary_under.ct"
  let overPath = TmpDir / "boundary_over.ct"
  let underOracle = TmpDir / "boundary_under_oracle.ct"
  let overOracle = TmpDir / "boundary_over_oracle.ct"

  let u = writeRecording(underPath, underEvents, DefaultRawByteThreshold)
  let o = writeRecording(overPath, overEvents, DefaultRawByteThreshold)

  check(u.rawBytes < DefaultRawByteThreshold,
    "the under-side recording measures " & $u.rawBytes &
    " raw bytes, not under " & $DefaultRawByteThreshold)
  check(DefaultRawByteThreshold - u.rawBytes <= uint64(PlainStepBytes),
    "the under-side recording is " &
    $(DefaultRawByteThreshold - u.rawBytes) &
    " bytes short of the threshold: that is more than one event, so it is " &
    "not a BOUNDARY recording")
  check(u.profile == cpCompact,
    "a recording " & $(DefaultRawByteThreshold - u.rawBytes) &
    " bytes under the threshold produced profile " & $u.profile)
  check(not u.switched, "the under-side writer switched")
  check(o.profile == cpFull,
    "a recording one event over the threshold produced profile " & $o.profile)
  check(o.switched, "the over-side writer did not switch")
  check(o.bufferedAtSwitch == uint64(overEvents.len),
    "the over-side switchover happened with " & $o.bufferedAtSwitch &
    " events buffered of " & $overEvents.len &
    ": the threshold is crossed by the LAST event, so every event before " &
    "it must have been buffered")

  # Each side asserted to contain EVERY event, against the same recording
  # written always-full. This is deliberate: an arm that checked only the
  # profile would pass on a writer that chose correctly and wrote nothing.
  let uo = writeRecording(underOracle, underEvents, 0'u64)
  let oo = writeRecording(overOracle, overEvents, 0'u64)
  let ua = answersFor(underPath, u.profile)
  let ub = answersFor(underOracle, uo.profile)
  for d in compareAnswers(ua, ub, "compact (under)", "always-full"):
    fail("under-side: " & d)
  check(ua.eventCount == underEvents.len,
    "the compact container holds " & $ua.eventCount & " of " &
    $underEvents.len & " events")

  let oa = answersFor(overPath, o.profile)
  let ob = answersFor(overOracle, oo.profile)
  for d in compareAnswers(oa, ob, "full (over)", "always-full"):
    fail("over-side: " & d)
  check(oa.eventCount == overEvents.len,
    "the over-side container holds " & $oa.eventCount & " of " &
    $overEvents.len & " events")

  var underSize = 0'i64
  var overSize = 0'i64
  try:
    underSize = getFileSize(underPath)
    overSize = getFileSize(overPath)
  except OSError, IOError:
    fail("could not stat the boundary containers")
  echo "  under: ", underCount, " steps, ", u.rawBytes, " raw bytes (",
    DefaultRawByteThreshold - u.rawBytes, " short), profile ", u.profile,
    ", container ", underSize, " B"
  echo "  over:  ", underCount + 1, " steps, ", o.rawBytes,
    " raw bytes at the switch, profile ", o.profile, ", container ",
    overSize, " B"
  echo (if failures == before: "PASS" else: "FAILED"),
    " test_the_boundary_is_asserted_from_both_sides"

# ---------------------------------------------------------------------------
# Arm 4: the threshold is measured in raw bytes, with the compressed-size
# control that falsifies the alternative implementation.
# ---------------------------------------------------------------------------

proc test_the_threshold_is_measured_in_raw_bytes() =
  echo "test_the_threshold_is_measured_in_raw_bytes"
  let before = failures

  const Threshold = 65_536'u64
  const Steps = 4_700   ## 17 * 4700 = 79,900 raw bytes, comfortably over
  let compressible = recordingEvents(rsCompressible, Steps)
  let incompressible = recordingEvents(rsIncompressible, Steps)

  let cPath = TmpDir / "raw_compressible.ct"
  let iPath = TmpDir / "raw_incompressible.ct"
  let c = writeRecording(cPath, compressible, Threshold)
  let i = writeRecording(iPath, incompressible, Threshold)

  # `rawBytes` here is the size AT THE SWITCHOVER — frozen when the decision
  # was taken — so this says the two recordings reached the threshold at the
  # same byte. The TOTAL raw sizes are compared below, where neither writer
  # switches and the figure is therefore the whole recording's.
  check(c.rawBytes == i.rawBytes,
    "the two recordings did not reach the threshold at the same raw size (" &
    $c.rawBytes & " vs " & $i.rawBytes & "): the arm's premise is that they " &
    "do, and the only difference between them is compressibility")
  check(c.profile == i.profile,
    "two recordings of equal raw size took DIFFERENT paths: " &
    $c.profile & " and " & $i.profile &
    " — which is what a threshold on compressed size would do")
  check(c.profile == cpFull,
    "both recordings measure " & $c.rawBytes & " raw bytes against a " &
    $Threshold & "-byte threshold, so both must be full, not " & $c.profile)

  # THE CONTROL. The same two recordings, classified by a threshold on
  # COMPRESSED size — using the compressed sizes the writer itself produced,
  # so the rule being falsified is the one a writer would have implemented.
  let cComp = fullEventsLogCompressedBytes(cPath)
  let iComp = fullEventsLogCompressedBytes(iPath)
  let cCompactUnderCompressedRule = uint64(cComp) < Threshold
  let iCompactUnderCompressedRule = uint64(iComp) < Threshold
  check(cCompactUnderCompressedRule != iCompactUnderCompressedRule,
    "THE CONTROL DID NOT FIRE: a threshold on compressed size classifies " &
    "both recordings the same way (" & $cComp & " and " & $iComp &
    " bytes against " & $Threshold & "), so this arm does not distinguish " &
    "a raw-byte threshold from a compressed-size one")
  check(cCompactUnderCompressedRule,
    "the compressible recording compresses to " & $cComp &
    " bytes, which is not under the " & $Threshold & "-byte threshold")
  check(not iCompactUnderCompressedRule,
    "the incompressible recording compresses to " & $iComp &
    " bytes, which is under the " & $Threshold & "-byte threshold")

  # Both must also take the same path on the OTHER side of the boundary, so
  # the agreement is not an artefact of one threshold.
  let cUnder = writeRecording(TmpDir / "raw_compressible_under.ct",
    compressible, DefaultRawByteThreshold)
  let iUnder = writeRecording(TmpDir / "raw_incompressible_under.ct",
    incompressible, DefaultRawByteThreshold)
  check(cUnder.profile == iUnder.profile and cUnder.profile == cpCompact,
    "under a 1 MiB threshold the two equal-raw-size recordings took " &
    $cUnder.profile & " and " & $iUnder.profile & ", not both compact")
  # These two never switch, so their measured size is the WHOLE recording's
  # raw size — and this is the equality the arm's premise actually needs.
  check(cUnder.rawBytes == iUnder.rawBytes,
    "the two recordings' TOTAL raw sizes differ (" & $cUnder.rawBytes &
    " vs " & $iUnder.rawBytes & ")")
  check(cUnder.rawBytes == uint64(Steps * PlainStepBytes) + 95'u64,
    "the total raw size is " & $cUnder.rawBytes & ", not " &
    $(Steps * PlainStepBytes + 95) &
    " (" & $Steps & " plain steps plus the 95-byte preamble)")

  echo "  ", Steps, " plain steps each, total raw size ", cUnder.rawBytes,
    " B both; threshold ", Threshold, " B reached at ", c.rawBytes,
    " B in both. raw rule -> ", c.profile, " / ", i.profile,
    " (same). compressed sizes ", cComp, " / ", iComp, " B (",
    formatFloat(iComp.float / max(cComp, 1).float, ffDecimal, 1),
    "x apart) -> compressed rule would give ",
    (if cCompactUnderCompressedRule: "compact" else: "full"), " / ",
    (if iCompactUnderCompressedRule: "compact" else: "full"), " (SPLIT)"
  echo (if failures == before: "PASS" else: "FAILED"),
    " test_the_threshold_is_measured_in_raw_bytes"

# ---------------------------------------------------------------------------
# Arm 5: a writer-produced compact container's members are genuinely RAW.
# ---------------------------------------------------------------------------

proc test_a_compact_container_carries_no_zstd_frames() =
  echo "test_a_compact_container_carries_no_zstd_frames"
  let before = failures

  # Large enough that the full writer seals SEVERAL chunks, so the full
  # container really does carry many frames and the comparison spans chunk
  # boundaries. 12,000 events at the default 4,096-per-chunk is three seals.
  let events = recordingEvents(rsMixed, 12_000)
  let compactPath = TmpDir / "raw_members_compact.ct"
  let fullPath = TmpDir / "raw_members_full.ct"
  let comp = writeRecording(compactPath, events, DefaultRawByteThreshold)
  let full = writeRecording(fullPath, events, 0'u64)
  check(comp.profile == cpCompact,
    "the compact arm produced profile " & $comp.profile)
  check(full.profile == cpFull, "the full arm produced profile " & $full.profile)

  let compactImage = readFileBytes(compactPath)
  let fullImage = readFileBytes(fullPath)
  let compactFrames = countZstdFrameMagic(compactImage)
  let fullFrames = countZstdFrameMagic(fullImage)

  # THE CONTROL FIRST: the census must find frames in the full container, or
  # it is a scanner that returns zero for everything and the assertion below
  # would be vacuous.
  check(fullFrames > 0,
    "the zstd-frame census found " & $fullFrames &
    " frames in a FULL container of the same recording: the scanner is " &
    "broken and the compact assertion below proves nothing")
  check(compactFrames == 0,
    "a writer-produced COMPACT container carries " & $compactFrames &
    " zstd frame magics. §1d: a compact container's members are stored as " &
    "written, and no member of this member set (events.log raw, events.fmt " &
    "text, meta.dat binary, paths.dat text, paths.off u64 table) is a " &
    "format that carries a frame")

  # And the positive half: the raw bytes are what the full writer compressed.
  let tRes = readCompactTrace(compactImage)
  doAssert tRes.isOk, "readCompactTrace: " & tRes.unsafeError
  let t = tRes.get()
  let oracle = fullEventsLogInflated(fullPath)
  check(t.eventsLog.len == oracle.len,
    "the compact events.log is " & $t.eventsLog.len &
    " bytes; the full container's events.log inflates to " & $oracle.len)
  if t.eventsLog.len == oracle.len:
    var firstDiff = -1
    for i in 0 ..< oracle.len:
      if t.eventsLog[i] != oracle[i]:
        firstDiff = i
        break
    check(firstDiff < 0,
      "the compact events.log differs from the full container's inflated " &
      "events.log at byte " & $firstDiff &
      ": the compact member is not the bytes the full profile compressed")
  check(t.eventsFmt == "split-binary",
    "the compact container's events.fmt reads '" & t.eventsFmt & "'")

  echo "  compact container ", compactImage.len, " B, ", compactFrames,
    " zstd frames; full container ", fullImage.len, " B, ", fullFrames,
    " zstd frames; raw events.log ", t.eventsLog.len,
    " B == the full profile's inflated events.log"

  # MEASURED, not asserted: the per-member ratio. The Introduction's RESOLVED
  # section says raw-plus-one-shot pays if and only if the ONE-SHOT ratio
  # exceeds the PER-MEMBER one, and that CCP-2's counter-example reversed
  # because its per-member ratio reached 9.30x. Printing this container's
  # per-member ratio is what lets the rule be checked against it rather than
  # recited, and it is a measurement rather than an assertion because the rule
  # is about compressors this test does not run.
  let storedRes = collectFullProfileMembers(fullImage)
  if storedRes.isOk:
    var storedBytes = 0
    for m in storedRes.get():
      storedBytes += m.payload.len
    var rawBytes = 0
    for e in readCompactDirectory(compactImage).get().entries:
      rawBytes += int(e.length)
    echo "  member payloads: raw ", rawBytes, " B, stored ", storedBytes,
      " B — per-member ratio ",
      formatFloat(rawBytes.float / max(storedBytes, 1).float, ffDecimal, 2),
      "x (the RESOLVED section's discriminator; CCP-2's reversing container ",
      "reached 9.30x)"
  else:
    echo "  member payloads: NOT MEASURED (", storedRes.unsafeError, ")"
  echo (if failures == before: "PASS" else: "FAILED"),
    " test_a_compact_container_carries_no_zstd_frames"

# ---------------------------------------------------------------------------

when isMainModule:
  ensureTmp()
  test_the_threshold_is_configuration_in_raw_stream_bytes()
  test_the_switchover_loses_no_events()
  test_the_boundary_is_asserted_from_both_sides()
  test_the_threshold_is_measured_in_raw_bytes()
  test_a_compact_container_carries_no_zstd_frames()
  if failures == 0:
    # `CCP4_KEEP_FIXTURES=1` leaves both containers of every pair in place, the
    # way CCP-1's arm does, so the boundary and raw-member measurements in the
    # milestone can be re-taken from the same bytes rather than from a rerun.
    var keep = false
    try:
      keep = getEnv("CCP4_KEEP_FIXTURES").len > 0
    except OSError, IOError:
      discard
    if not keep:
      try:
        removeDir(TmpDir)
      except OSError, IOError:
        discard
    else:
      echo "  fixtures kept in ", TmpDir, " (CCP4_KEEP_FIXTURES)"
    echo "ALL PASS: CCP-4 the writer chooses the profile from a measured ",
      "raw-byte threshold"
  else:
    echo "FAILURES: ", failures, " (fixtures left in ", TmpDir, ")"
    quit(1)
