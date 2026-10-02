when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## CCP-4: the writer chooses the container profile from a measured RAW-BYTE
## threshold.
##
## Spec: `codetracer-trace-format-spec/ctfs-container.md` §1e (the threshold,
## the switchover rule and the raw-member obligation), with §1d normative for
## the compact body this emits and §1a/§1b/§1c for the version-6 header.
##
## **What this writer is.** It buffers a recording in memory and, at `close`,
## emits the COMPACT profile if the recording ended before a configured
## raw-byte threshold was reached. If the threshold is reached mid-recording it
## switches to the ordinary streaming FULL-profile writer, replaying everything
## it has buffered in order, so nothing already buffered is lost.
##
## **The unit is RAW stream bytes, before any compression.** §1e says why, and
## it is not a convenience: the decision the threshold makes is whether the
## whole trace can be RESIDENT at load, which is a property of logical size.
## A threshold on compressed size would make the profile depend on
## compressibility, so two recordings of the same size could take different
## paths for no reason a reader could act on. `rawStreamBytes` below is
## therefore the sum of the member payloads the compact container would carry,
## measured as they are produced and with no compressor anywhere near it.
##
## **The members this emits are genuinely RAW, and that is the half CCP-2
## could not do.** CCP-2's reference encoder copies member payloads VERBATIM —
## which is exactly what makes its round-trip byte-exact — so a compact
## container built by converting a full one inherits that container's
## per-member zstd frames. §1d says a compact container carries no per-member
## compression, so the obligation lands on a writer, and it lands here:
## `events.log` is the split-binary stream as the encoder produced it, with no
## chunk headers and no zstd frame, and the oracle for it is the full-profile
## writer's own output inflated. A container this writer produces contains no
## zstd frame magic at all, because no member of its member set is a format
## that carries one.
##
## **The switchover is the one place in this campaign where a bug is silent.**
## A wrong layout fails to decode and a wrong index fails byte-identity, but a
## switchover that drops or reorders the buffered prefix produces a VALID
## full-profile container that is missing part of the recording, and nothing
## downstream can tell. So `ProfileWriterFaults.dropBufferedPrefix` is a
## committed fault-injection seam: it exists so the test that asserts the
## switchover loses nothing can be shown to FAIL when the prefix is dropped.
## An arm that cannot fail on a dropped prefix is not testing the thing that
## matters, and a control that is run once and recorded in prose stops being
## run. No production path sets it.
##
## **The cost of buffering, stated rather than discovered.** While this writer
## is under the threshold nothing has been written to `path` at all, so a
## recording killed before `close` leaves no container — where the streaming
## full writer leaves everything up to its last seal (`ctfs-container.md` §6,
## "Durability"). That is inherent to "buffers in memory" and is the price of
## the profile, not a defect in this module. Past the switchover the durability
## is the full writer's, unchanged.

import results
import codetracer_ctfs/types
import codetracer_ctfs/compact
import codetracer_trace_types
import codetracer_trace_writer
import codetracer_trace_writer/split_binary
import codetracer_trace_writer/meta_dat

export results, codetracer_trace_types, compact

const
  DefaultRawByteThreshold*: uint64 = 1'u64 shl 20
    ## 1 MiB of RAW stream bytes, and it is a JUDGEMENT recorded as one rather
    ## than a measurement. The container that opened this campaign has 85,118
    ## bytes of logical content, an order of magnitude under it, and blockchain
    ## traces are bounded by gas rather than by taste — so the default sits
    ## well above the workload that motivated the profile while staying small
    ## enough that a whole-file load is unremarkable on any device that can run
    ## the client at all. CCP-5's peak-memory measurement is where the figure
    ## is defended with a number; until then it is a default, not a bound.

  EventsFmtContent* = "split-binary"
    ## What `events.fmt` names, identically in both profiles: the encoding of
    ## `events.log`. The compact profile does not change the encoding of a
    ## member, only whether a member is stored compressed.

type
  ProfileWriterFaults* = object
    ## Fault injection for CCP-4's controls. See the module header: these
    ## exist so the arms that assert the switchover loses nothing can be shown
    ## to fail. Nothing in the library sets them.
    dropBufferedPrefix*: bool
      ## Discard the buffered prefix at the switchover instead of replaying
      ## it. The result is a well-formed full-profile container missing its
      ## first N events — precisely the silent defect the arm exists to catch.

  ProfileWriter* = object
    ## A trace writer that decides its container profile from the raw bytes it
    ## has seen. Writes to `path` at `close` (compact) or from the switchover
    ## onward (full).
    threshold: uint64
    faults: ProfileWriterFaults
    path: string
    chunkThreshold: int
    metadata: TraceMetadata
    metaDatBytes: seq[byte]
    eventsFmtBytes: seq[byte]

    buffering: bool
    buffered: seq[TraceLowLevelEvent]
    raw: SplitBinaryEncoder
      ## The raw `events.log`, accumulated as events arrive. This is both the
      ## compact profile's payload and the quantity the threshold measures, so
      ## the number the decision is made on and the bytes that get written are
      ## the same bytes.
    pathRecords: seq[string]

    full: TraceWriter
    fullOpen: bool
    profile: CtfsProfile
    switched: bool
    bufferedAtSwitch: uint64
    frozenRawBytes: uint64
      ## The measured raw size at the instant of the switchover, taken BEFORE
      ## the buffer is released. Without it `rawStreamBytes` would answer with
      ## the preamble alone after a switch — the buffer having been cleared —
      ## and the number that reports the decision would be the one number the
      ## decision was not made on.
    eventCount: uint64
    closed: bool

proc toBytes(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

# ---------------------------------------------------------------------------
# The measured quantity
# ---------------------------------------------------------------------------

proc pathsTableBytes(pathRecords: openArray[string]): uint64 =
  ## The raw bytes `paths.dat` + `paths.off` occupy. `paths.off` is a
  ## `VariableRecordTable`'s offset member: one `u64` LE per record plus the
  ## leading zero, N + 1 entries, the last one the data length. Zero when there
  ## are no `Path` events at all, because the full writer creates the table
  ## lazily and a compact container that carried two empty members where the
  ## full one carries none would not name the same members.
  if pathRecords.len == 0:
    return 0'u64
  var total = 8'u64 * uint64(pathRecords.len + 1)
  for p in pathRecords:
    total += uint64(p.len)
  total

proc rawStreamBytes*(w: ProfileWriter): uint64 =
  ## The RAW bytes of the members the compact container would carry, before any
  ## compression: `events.log` as the split-binary encoder produced it,
  ## `events.fmt`, `meta.dat`, and the paths table when there is one.
  ##
  ## Every member is counted, including the two that do not grow with the
  ## recording. That is deliberate: the question the threshold answers is
  ## whether the WHOLE container can be resident, so the quantity measured is
  ## the whole container's payload and not the part of it that happens to be
  ## interesting. Excluding a constant would make the threshold a statement
  ## about the preamble rather than about the recording.
  ##
  ## Past the switchover this is FROZEN at the value it had when the decision
  ## was taken, because the buffer it was measuring has been released. The
  ## alternative — recomputing from an emptied buffer — would answer with the
  ## preamble alone and report a figure the decision was demonstrably not made
  ## on.
  if not w.buffering:
    return w.frozenRawBytes
  uint64(w.metaDatBytes.len) + uint64(w.eventsFmtBytes.len) +
    uint64(w.raw.getDataLen()) + pathsTableBytes(w.pathRecords)

proc rawByteThreshold*(w: ProfileWriter): uint64 = w.threshold
proc isBuffering*(w: ProfileWriter): bool = w.buffering
proc switchedToFull*(w: ProfileWriter): bool = w.switched
proc bufferedEventsAtSwitch*(w: ProfileWriter): uint64 = w.bufferedAtSwitch
proc eventCount*(w: ProfileWriter): uint64 = w.eventCount

proc chosenProfile*(w: ProfileWriter): CtfsProfile =
  ## The profile the writer has chosen. Before `close` and before any
  ## switchover this is the profile it WOULD choose if the recording ended
  ## here, which is the compact one; after a switchover it is `cpFull` and
  ## cannot go back, because the full container is already being written.
  w.profile

# ---------------------------------------------------------------------------
# The switchover
# ---------------------------------------------------------------------------

proc switchToFullProfile(w: var ProfileWriter): Result[void, string] =
  ## Open the streaming full-profile writer and replay the buffered prefix
  ## into it, in order. After this the writer is a pass-through.
  ##
  ## The replay order is the arrival order, because the only thing that makes
  ## a full container of a switched recording indistinguishable from one
  ## written full throughout is that every event reaches the stream once, in
  ## the order it arrived. `Path` events are replayed through the full writer's
  ## own `writeEvent`, so its interning assigns the same ids in the same order
  ## rather than this module having a second opinion about path ids.
  w.frozenRawBytes = w.rawStreamBytes()
  let fullRes = newTraceWriter(w.path, w.metadata.program, w.metadata.args,
    w.metadata.workdir, w.chunkThreshold, w.metadata.recordingId)
  if fullRes.isErr:
    return err("switching to the full profile: " & fullRes.unsafeError)
  w.full = fullRes.get()
  w.fullOpen = true
  w.buffering = false
  w.profile = cpFull
  w.switched = true
  w.bufferedAtSwitch = uint64(w.buffered.len)

  var prefix = w.buffered
  w.buffered = @[]
  if w.faults.dropBufferedPrefix:
    # THE PLANTED DROP. See `ProfileWriterFaults`.
    prefix = @[]
  for ev in prefix:
    let r = w.full.writeEvent(ev)
    if r.isErr:
      return err("replaying the buffered prefix into the full profile: " &
        r.error)

  # The raw buffer is the compact profile's payload and the compact profile is
  # no longer reachable, so it is released here rather than carried to close.
  w.raw.clear()
  w.pathRecords = @[]
  ok()

# ---------------------------------------------------------------------------
# Open / write / close
# ---------------------------------------------------------------------------

proc newProfileWriter*(path: string, program: string, args: seq[string],
    workdir: string = "",
    rawByteThreshold: uint64 = DefaultRawByteThreshold,
    chunkThreshold: int = DefaultChunkThreshold,
    recordingId: string = "",
    faults: ProfileWriterFaults = ProfileWriterFaults()
): Result[ProfileWriter, string] =
  ## Open a profile-choosing writer at `path`. Nothing is written to `path`
  ## until either the threshold is crossed or `close` is called.
  ##
  ## `rawByteThreshold = 0` means ALWAYS FULL, and it means it literally: the
  ## switchover happens here, before any event, so the container is a streaming
  ## full-profile one from its first byte. That is the oracle arm of CCP-4's
  ## verification, and it is the same code path as any other threshold rather
  ## than a special case, so the oracle is not a second implementation.
  var resolvedId = recordingId
  if resolvedId.len == 0:
    let uuidRes = newUuidV7()
    if uuidRes.isErr:
      return err("failed to mint recording_id: " & uuidRes.error)
    resolvedId = $uuidRes.get()
  else:
    let valRes = validateRecordingIdStr(resolvedId)
    if valRes.isErr:
      return err("recordingId is not a canonical UUIDv7: " & valRes.error)

  let meta = TraceMetadata(recordingId: resolvedId, program: program,
    args: args, workdir: workdir)
  # The SAME encoder the full writer reaches through `writeMetaDat`, with the
  # same (default) flag input, so the two profiles' `meta.dat` is one byte
  # sequence produced in one place. A second opinion about the metadata
  # document would make the equality the tests assert a coincidence.
  let metaRes = encodeMetaDat(meta, MetaDatFlagsInput())
  if metaRes.isErr:
    return err("encoding meta.dat: " & metaRes.error)

  var w = ProfileWriter(
    threshold: rawByteThreshold,
    faults: faults,
    path: path,
    chunkThreshold: chunkThreshold,
    metadata: meta,
    metaDatBytes: metaRes.get(),
    eventsFmtBytes: toBytes(EventsFmtContent),
    buffering: true,
    raw: SplitBinaryEncoder.init(),
    profile: cpCompact,
    closed: false)

  if w.rawStreamBytes() >= w.threshold:
    ? w.switchToFullProfile()
  ok(w)

proc writeEvent*(w: var ProfileWriter, event: TraceLowLevelEvent):
    Result[void, string] =
  ## Accept one event. While buffering, the event is encoded into the raw
  ## `events.log` buffer AND retained for replay; the two are kept together
  ## because the encoded bytes are what the threshold measures and the events
  ## are what a switchover has to hand to the full writer.
  if w.closed:
    return err("ProfileWriter is already closed")
  if not w.buffering:
    ? w.full.writeEvent(event)
    w.eventCount += 1
    return ok()

  w.raw.encodeEvent(event)
  if event.kind == tlePath:
    w.pathRecords.add(event.path)
  w.buffered.add(event)
  w.eventCount += 1

  if w.rawStreamBytes() >= w.threshold:
    return w.switchToFullProfile()
  ok()

proc writeStep*(w: var ProfileWriter, pathId: uint64, line: int64):
    Result[void, string] =
  w.writeEvent(TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(pathId), line: Line(line))))

proc writePath*(w: var ProfileWriter, path: string): Result[void, string] =
  w.writeEvent(TraceLowLevelEvent(kind: tlePath, path: path))

proc compactMembers*(w: ProfileWriter): Result[seq[CompactMember], string] =
  ## The compact profile's member set, in the order the full profile's
  ## `FileEntry` array would carry it — `events.log`, `events.fmt`,
  ## `meta.dat`, then the paths table if there is one. The order matters: §1d
  ## says a compact and a full container of one recording name the same members
  ## identically, and the full writer adds them in exactly this order
  ## (`events.log` and `events.fmt` at open, `meta.dat` before the first event,
  ## the paths table at the first `Path` event).
  ##
  ## Every payload here is RAW. `events.log` is the split-binary stream with no
  ## chunk header and no zstd frame, `events.fmt` is twelve ASCII bytes,
  ## `meta.dat` is `encodeMetaDat`'s output, `paths.dat` is the path strings
  ## concatenated and `paths.off` is N + 1 `u64` LE offsets. None of these is a
  ## format that carries a compressed frame, which is why a container this
  ## builds contains no zstd frame magic anywhere.
  if not w.buffering:
    return err("this writer switched to the full profile at " &
      $w.rawStreamBytes() & " raw bytes: it has no compact member set, and " &
      "its container is the full one at " & w.path)
  var members: seq[CompactMember] = @[
    CompactMember(name: "events.log", payload: w.raw.getBytes()),
    CompactMember(name: "events.fmt", payload: w.eventsFmtBytes),
    CompactMember(name: "meta.dat", payload: w.metaDatBytes)]
  if w.pathRecords.len > 0:
    var dat: seq[byte]
    var off = newSeq[byte](8 * (w.pathRecords.len + 1))
    var cursor = 0'u64
    writeU64LE(off, 0, 0'u64)
    for i, p in w.pathRecords.pairs:
      for ch in p:
        dat.add(byte(ch))
      cursor += uint64(p.len)
      writeU64LE(off, 8 * (i + 1), cursor)
    members.add(CompactMember(name: "paths.dat", payload: dat))
    members.add(CompactMember(name: "paths.off", payload: off))
  ok(members)

proc compactImage*(w: ProfileWriter): Result[seq[byte], string] =
  ## The compact container image this writer would write, through CCP-2's
  ## reference encoder. Exposed so a caller (and the tests) can have the bytes
  ## without going through the filesystem.
  let members = ? w.compactMembers()
  encodeCompactContainer(members)

proc writeImage(path: string, image: openArray[byte]): Result[void, string] =
  try:
    let f = open(path, fmWrite)
    if image.len > 0:
      discard f.writeBuffer(unsafeAddr image[0], image.len)
    f.close()
    ok()
  except IOError:
    err("failed to write container to " & path)
  except OSError:
    err("failed to write container to " & path)

proc close*(w: var ProfileWriter): Result[void, string] =
  ## Finish the recording. Under the threshold this is where the compact
  ## container comes into existence; past the switchover it closes the full
  ## writer that has been streaming since.
  if w.closed:
    return ok()
  if w.fullOpen:
    ? w.full.close()
    w.profile = cpFull
  else:
    let image = ? w.compactImage()
    ? writeImage(w.path, image)
    w.profile = cpCompact
  w.raw.destroy()
  w.closed = true
  ok()

# ---------------------------------------------------------------------------
# Reading a compact container back
# ---------------------------------------------------------------------------

type
  CompactTrace* = object
    ## What a compact container carries, decoded. This is the Nim side of the
    ## load path; CCP-5 is where the db-backend does the same thing through its
    ## memory-backed facades.
    memberNames*: seq[string]
    eventsLog*: seq[byte]   ## the RAW split-binary stream, as stored
    eventsFmt*: string
    metaDat*: seq[byte]
    events*: seq[TraceLowLevelEvent]
    paths*: seq[string]

proc readCompactTrace*(image: openArray[byte],
    bodyReconstructed = false): Result[CompactTrace, string] =
  ## Decode a compact container: its directory through §1d's six checks, then
  ## its members. `events.log` is decoded as split-binary with NO chunk walk,
  ## because a compact container's members are stored as written and there is
  ## no chunk framing to walk — which is the load path collapsing from "walk a
  ## block map, inflate N frames" to "slice a directory, decode once".
  let dir = ? readCompactDirectory(image, bodyReconstructed)
  var t = CompactTrace()
  for e in dir.entries:
    t.memberNames.add(e.name)

  t.eventsLog = ? compactMemberBytes(image, dir, "events.log")
  t.events = ? decodeAllEvents(t.eventsLog)

  let fmt = ? compactMemberBytes(image, dir, "events.fmt")
  for b in fmt:
    t.eventsFmt.add(char(b))

  t.metaDat = ? compactMemberBytes(image, dir, "meta.dat")

  if findCompactMember(dir, "paths.dat") >= 0:
    let dat = ? compactMemberBytes(image, dir, "paths.dat")
    let off = ? compactMemberBytes(image, dir, "paths.off")
    if off.len < 8 or off.len mod 8 != 0:
      return err("paths.off is " & $off.len & " bytes: an offset table holds " &
        "N + 1 u64 LE entries, the last one the data length")
    let entries = off.len div 8
    for i in 0 ..< entries - 1:
      let start = readU64LE(off, i * 8)
      let stop = readU64LE(off, (i + 1) * 8)
      if stop < start or stop > uint64(dat.len):
        return err("paths.off entry " & $i & " spans " & $start & ".." &
          $stop & " of a " & $dat.len & "-byte paths.dat")
      var p = ""
      for j in int(start) ..< int(stop):
        p.add(char(dat[j]))
      t.paths.add(p)
  ok(t)
