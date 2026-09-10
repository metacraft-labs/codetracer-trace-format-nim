## GDH-M2 — the reload boundary is discoverable in the container.
##
## The two container-level gates of
## ``codetracer-specs/Planned-Features/GDScript-Hot-Reload-Multi-Version-Sources.milestones.org``
## § GDH-M2:
##
##   1. ``gdh2_reload_marker_round_trips``       (GDH-G7)
##   2. ``gdh2_unknown_tag_is_refused_by_name``  (the strict-rejection contract)
##
## The third, ``gdh2_no_reload_container_is_byte_identical`` (GDH-G9),
## needs two BUILDS of the writer and therefore lives in
## ``tests/run_gdh2_gates.sh`` with ``tests/gdh2_identity_probe.nim``.
##
## ``allowed_mocks: none``, and none are used.  Every container here is
## produced by the production ``MultiStreamTraceWriter`` and read back
## through ``openNewTraceFromBytes`` — the same two objects a recorder and
## a debugger use.  The one hand-built artefact is the negative fixture of
## gate 2, and it is not a mock: it is the real container, with its real
## ``meta.dat`` header rewritten to schema version 4 in place, because
## constructing a container the writer REFUSES to produce is the only way
## to reach the reader's refusal path at all.
##
## ---------------------------------------------------------------------
## What GDH-G7 is actually about, since it decides the shape of gate 1
##
## A consumer *can* infer a version change from the path indices alone: a
## step whose path id differs from the previous step's, at the same path
## string, is a transition.  Design §6.3.1 refuses that inference for
## three reasons, and the campaign brief adds a fourth constraint in as
## many words: **GDH-G7 must not be satisfiable by inference from the path
## indices, nor by emitting a zeroed marker.**
##
## So "a marker is present" is not what gate 1 asserts.  It asserts that
## every field of every marker is CROSS-TIED to something else in the
## container: the ordinal to the marker's position in the sequence, the
## ``old_path_id`` to the id the step BEFORE the marker resolves to, the
## ``new_path_id`` to the id the step AFTER it resolves to, and both to
## the same path STRING.  A marker of zeros passes "a marker is present"
## and fails every one of those.  ``gdh2FalsifyZeroedMarker`` is in the
## harness to prove that rather than to claim it.
##
## Every failure is raised with a ``GDH2-FAIL[<gate>]`` prefix so the arm
## driver can require that an arm went red in the gate it was aimed at,
## rather than merely that the process exited non-zero — a compile error,
## a missing fixture or an unrelated crash also exit non-zero, and
## counting those as "the falsifier worked" is the silent self-pass this
## campaign is under a standing mandate to prevent.

import std/[os, strutils, options]
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/gdh2_arms
import codetracer_trace_writer/global_line_index
import codetracer_ctfs/container

# ---------------------------------------------------------------------------
# Failure reporting
# ---------------------------------------------------------------------------

type Gdh2GateDefect = object of CatchableError

var currentGate = "<none>"

proc fail(msg: string) {.noreturn.} =
  raise newException(Gdh2GateDefect,
    "GDH2-FAIL[" & currentGate & "]: " & msg)

template check(cond: bool, msg: string) =
  ## A TEMPLATE, not a proc: Nim evaluates a proc's arguments eagerly, so
  ## `check(r.isOk, "..." & r.error)` would read `.error` off a
  ## successful Result and abort with a ResultDefect on the PASSING path.
  ## A harness that dies when the subject is healthy is a harness whose
  ## green result nobody ever sees.  (GDH-M1 shipped that defect twice
  ## before its driver caught it.)
  if not cond:
    fail(msg)

# ---------------------------------------------------------------------------
# The fixture
# ---------------------------------------------------------------------------

const
  ProbePath = "res://gdh2/probe.gd"
  OtherPath = "res://gdh2/autoload.gd"
  V1Lines = 40'u64
  V2Lines = 63'u64
  V3Lines = 71'u64
  OtherLines = 12'u64
  # Lines executed in each version.  V2's and V3's lie PAST the end of
  # V1, which is the property that makes mis-attribution detectable at
  # all: under a single path entry they would be addressed inside the
  # NEXT file's range and read back as a location that was never
  # recorded (design §2.1).
  V1Steps = [10'i64, 11, 12]
  V2Steps = [45'i64, 50]
  V3Steps = [70'i64]
  Marker1InFlight = 3'u64
  Marker2InFlight = 0'u64
  Marker1Generation = 2'u64
  Marker2Generation = 3'u64

let workDir = getTempDir() / "ctfnim-gdh2"

proc newWriter(name, recordingId: string): MultiStreamTraceWriter =
  discard existsOrCreateDir(workDir)
  let r = initMultiStreamWriter(workDir / (name & ".build"), name,
    recordingId = recordingId)
  check(r.isOk, "initMultiStreamWriter(" & name & "): " & r.error)
  result = r.get()

proc finish(w: var MultiStreamTraceWriter): seq[byte] =
  let c = w.close()
  check(c.isOk, "close(): " & c.error)
  result = w.toBytes()
  discard w.closeCtfs()
  check(result.len > 0, "the writer produced an empty container")

proc openContainer(bytes: seq[byte]): NewTraceReader =
  let r = openNewTraceFromBytes(bytes)
  check(r.isOk, "openNewTraceFromBytes: " & r.error)
  result = r.get()

type BuiltContainer = object
  bytes: seq[byte]
  v1Id, v2Id, v3Id, otherId: uint64
  marker1Ordinal, marker2Ordinal: uint64
  execEvents: uint64          ## every record the exec stream carries
  logicalSteps: uint64        ## exec records that are real steps

proc buildTwoMarkerContainer(): BuiltContainer =
  ## Two reloads of one file, with an unrelated second file registered
  ## alongside so that "the marker names the right ids" is a claim that
  ## can be wrong rather than one there is only one way to satisfy.
  var w = newWriter("gdh2-markers", "01890000-0000-7000-8000-00000000d002")
  let en = w.enableLineCountTable()
  check(en.isOk, "enableLineCountTable: " & en.error)

  let v1 = w.registerPath(ProbePath, lineCount = V1Lines)
  check(v1.isOk, "registerPath v1: " & v1.error)
  let other = w.registerPath(OtherPath, lineCount = OtherLines)
  check(other.isOk, "registerPath other: " & other.error)

  var logical = 0'u64
  for i, line in V1Steps.pairs:
    let r = w.registerStep(v1.get(), uint64(line), @[])
    check(r.isOk, "registerStep v1 line " & $line & ": " & r.error)
    logical += 1
    # The unrelated file is stepped through in the MIDDLE of v1's run,
    # not immediately before the reload: the step adjacent to a marker
    # has to belong to the reloaded file for the cross-tie below to say
    # anything, and a fixture that put a foreign file there would make
    # the tie fail for a reason that is about the fixture.
    if i == 0:
      let o1 = w.registerStep(other.get(), 4'u64, @[])
      check(o1.isOk, "registerStep other: " & o1.error)
      logical += 1

  let v2 = w.registerPathVersion(ProbePath, V2Lines)
  check(v2.isOk, "registerPathVersion v2: " & v2.error)
  let m1 = w.registerSourceReload(
    [SourceReloadChange(oldPathId: v1.get(), newPathId: v2.get(),
      generation: Marker1Generation)],
    inFlightFrames = Marker1InFlight)
  check(m1.isOk, "registerSourceReload 1: " & m1.error)

  for line in V2Steps:
    let r = w.registerStep(v2.get(), uint64(line), @[])
    check(r.isOk, "registerStep v2 line " & $line & ": " & r.error)
    logical += 1

  let v3 = w.registerPathVersion(ProbePath, V3Lines)
  check(v3.isOk, "registerPathVersion v3: " & v3.error)
  let m2 = w.registerSourceReload(
    [SourceReloadChange(oldPathId: v2.get(), newPathId: v3.get(),
      generation: Marker2Generation)],
    inFlightFrames = Marker2InFlight)
  check(m2.isOk, "registerSourceReload 2: " & m2.error)

  for line in V3Steps:
    let r = w.registerStep(v3.get(), uint64(line), @[])
    check(r.isOk, "registerStep v3 line " & $line & ": " & r.error)
    logical += 1

  let bytes = w.finish()
  BuiltContainer(
    bytes: bytes,
    v1Id: v1.get(), v2Id: v2.get(), v3Id: v3.get(), otherId: other.get(),
    marker1Ordinal: m1.get(), marker2Ordinal: m2.get(),
    execEvents: logical + 2,
    logicalSteps: logical)

proc buildNoMarkerContainer(): seq[byte] =
  ## The CONTROL: the same shape of recording with no reload at all.
  ## Decoded by the same instrument, it must report ZERO markers — and
  ## its event stream must be non-empty, so "zero markers" is a
  ## statement about a real dump rather than about an empty one.
  var w = newWriter("gdh2-plain", "01890000-0000-7000-8000-00000000d003")
  let en = w.enableLineCountTable()
  check(en.isOk, "enableLineCountTable: " & en.error)
  let p = w.registerPath(ProbePath, lineCount = V1Lines)
  check(p.isOk, "registerPath: " & p.error)
  let o = w.registerPath(OtherPath, lineCount = OtherLines)
  check(o.isOk, "registerPath other: " & o.error)
  for line in V1Steps:
    let r = w.registerStep(p.get(), uint64(line), @[])
    check(r.isOk, "registerStep: " & r.error)
  let o1 = w.registerStep(o.get(), 4'u64, @[])
  check(o1.isOk, "registerStep other: " & o1.error)
  w.finish()

# ---------------------------------------------------------------------------
# Gate 1 — the marker round-trips, and every field is cross-tied
# ---------------------------------------------------------------------------

proc stepPathId(r: var NewTraceReader, gli: GlobalLineIndex,
    n: uint64): uint64 =
  ## The path id step ``n`` resolves to.  This is the independent side of
  ## every cross-tie below: it comes from the STEP STREAM and the global
  ## position space, not from the marker.
  let absRes = r.stepAbsoluteGlobalLineIndex(n)
  check(absRes.isOk, "step " & $n & " has no absolute position: " & absRes.error)
  # `tryResolve`, not `resolve`: the unchecked form CLAMPS an address
  # above the top of the space to the last file, which yields a file id
  # that exists and a line that was never recorded — precisely the silent
  # wrong answer this campaign exists to remove.  A gate that read a
  # clamped id would compare two numbers and learn nothing.
  let res = gli.tryResolve(absRes.get())
  check(res.isOk, "step " & $n & " does not resolve: " & res.error)
  uint64(res.get()[0])

proc gate_reload_marker_round_trips() =
  currentGate = "gdh2_reload_marker_round_trips"
  let built = buildTwoMarkerContainer()

  var r = openContainer(built.bytes)

  # --- anti-vacuity, before any per-marker assertion ----------------
  #
  # A decoder that silently skipped an unknown tag would yield ZERO
  # markers, and every per-marker assertion below would then be a
  # universal quantification over the empty set — which passes for free
  # (trap 4).  So the COUNT is asserted first, and it is asserted
  # against a number derived from the fixture rather than written here
  # by hand.
  check(r.meta.hasSourceReload,
    "the container does not declare FlagExtHasSourceReload, so the " &
    "reader will refuse every marker and this gate would measure a " &
    "refusal rather than a round trip")
  check(r.meta.version == MetaDatVersionExtendedFlags,
    "a container carrying a marker must be at meta.dat schema version " &
    $MetaDatVersionExtendedFlags & "; it is at " & $r.meta.version)

  let markersRes = r.sourceReloads()
  check(markersRes.isOk, "sourceReloads: " & markersRes.error)
  let markers = markersRes.get()
  check(markers.len == 2,
    "expected exactly 2 markers, decoded " & $markers.len &
    ". Every assertion below is over this set, so an empty or short " &
    "set would satisfy them vacuously")

  let countRes = r.sourceReloadCount()
  check(countRes.isOk, "sourceReloadCount: " & countRes.error)
  check(countRes.get() == 2'u64,
    "sourceReloadCount says " & $countRes.get() & ", the decoded list " &
    "has 2. The two disagreeing means one of them is not reading the " &
    "stream")

  # --- completeness: the exec stream is fully accounted for ---------
  #
  # `values.dat` is parallel-indexed to the exec stream (record N <-> step
  # N), so its record count is an INDEPENDENT statement of how many exec
  # records the container has — it does not go through the step decoder
  # at all.  Comparing the two is what catches a decoder that silently
  # dropped records: an assertion over the step decoder's own count
  # would agree with itself no matter what it skipped.
  let totalRes = r.stepCount()
  check(totalRes.isOk, "stepCount: " & totalRes.error)
  let valRes = r.valueCount()
  check(valRes.isOk, "valueCount: " & valRes.error)
  check(totalRes.get() == valRes.get(),
    "the exec stream decodes to " & $totalRes.get() & " records but the " &
    "value stream — which is parallel-indexed to it and decoded by a " &
    "different reader — holds " & $valRes.get() & ". A step stream that " &
    "decodes SHORTER than the container says is the skip-the-unknown-tag " &
    "failure, and it is wrong bytes rather than an error")
  check(totalRes.get() == built.execEvents,
    "the container holds " & $totalRes.get() & " exec records; the " &
    "fixture emitted " & $built.execEvents)

  let logicalRes = r.logicalStepCount()
  check(logicalRes.isOk, "logicalStepCount: " & logicalRes.error)
  check(logicalRes.get() == built.logicalSteps,
    "logicalStepCount says " & $logicalRes.get() & " but the fixture " &
    "emitted " & $built.logicalSteps & " real steps. The marker is a " &
    "timeline annotation and must not be counted as a step (design §7.3)")
  check(logicalRes.get() + countRes.get() == totalRes.get(),
    "steps (" & $logicalRes.get() & ") + markers (" & $countRes.get() &
    ") != exec records (" & $totalRes.get() & "). Something in the " &
    "stream is being reported as neither")

  # --- ordinals -----------------------------------------------------
  check(markers[0].reloadOrdinal == 1'u64,
    "first marker's ordinal is " & $markers[0].reloadOrdinal & ", not 1")
  check(markers[1].reloadOrdinal == 2'u64,
    "SECOND marker's ordinal is " & $markers[1].reloadOrdinal & ", not 2. " &
    "A constant ordinal makes a second reload indistinguishable from " &
    "the first — `repro_hcr_agent.c:1338`'s `symbolGeneration: 1` " &
    "defect, which survived there because nothing consumed the field")
  check(markers[0].reloadOrdinal == built.marker1Ordinal and
        markers[1].reloadOrdinal == built.marker2Ordinal,
    "the ordinals the writer returned (" & $built.marker1Ordinal & ", " &
    $built.marker2Ordinal & ") are not the ones the container carries")

  # --- the cross-ties: this is GDH-G7 -------------------------------
  let gli = r.globalPositionSpace()
  var checkedTies = 0
  for i, m in markers:
    check(m.changed.len == 1,
      "marker " & $(i + 1) & " names " & $m.changed.len & " changed " &
      "files, expected 1")
    let ch = m.changed[0]
    check(ch.oldPathId != ch.newPathId,
      "marker " & $(i + 1) & " reports old_path_id == new_path_id == " &
      $ch.oldPathId & ". A reload that minted no new index cannot " &
      "attribute its post-reload steps to the version that ran them")

    # The step immediately BEFORE the marker must resolve to old_path_id,
    # and the one immediately AFTER to new_path_id.  Neither number comes
    # from the marker: both come from the step stream and the position
    # space.  A marker of zeros fails here even though it is present,
    # well-formed and counted.
    check(m.stepIndex > 0'u64,
      "marker " & $(i + 1) & " is the first record in the stream, so " &
      "there is no preceding step to tie it to")
    check(m.stepIndex + 1 < totalRes.get(),
      "marker " & $(i + 1) & " is the last record in the stream, so " &
      "there is no following step to tie it to")
    let before = stepPathId(r, gli, m.stepIndex - 1)
    let after = stepPathId(r, gli, m.stepIndex + 1)
    check(before == ch.oldPathId,
      "marker " & $(i + 1) & " claims old_path_id " & $ch.oldPathId &
      " but the step before it resolves to path " & $before &
      ". The marker must name the ids the steps on either side of it " &
      "actually carry, or it is an annotation that cannot be checked " &
      "against the trace")
    check(after == ch.newPathId,
      "marker " & $(i + 1) & " claims new_path_id " & $ch.newPathId &
      " but the step after it resolves to path " & $after)
    checkedTies += 2

    # Same virtual path on both sides — GDH-G2's property, restated at
    # the marker: only the INDEX discriminates a version.
    let oldStr = r.path(ch.oldPathId)
    let newStr = r.path(ch.newPathId)
    check(oldStr.isOk and newStr.isOk,
      "marker " & $(i + 1) & " names a path id that does not resolve to " &
      "a string")
    check(oldStr.get().len > 0,
      "marker " & $(i + 1) & "'s old path string is EMPTY; a comparison " &
      "of two empty strings is true for free")
    check(oldStr.get() == newStr.get(),
      "marker " & $(i + 1) & " crosses from `" & oldStr.get() & "` to `" &
      newStr.get() & "`. The virtual path is the same file across a " &
      "reload; only the index differs")
    check(oldStr.get() == ProbePath,
      "marker " & $(i + 1) & " names `" & oldStr.get() & "`, not the " &
      "fixture's `" & ProbePath & "`")

    # And the version ordinals must advance, which is the container-side
    # counterpart the marker's WIRE generation is deliberately off by one
    # from (design §7.0).
    let oldOrd = r.pathVersionOrdinal(ch.oldPathId)
    let newOrd = r.pathVersionOrdinal(ch.newPathId)
    check(oldOrd.isOk and newOrd.isOk, "version ordinals do not resolve")
    check(newOrd.get() == oldOrd.get() + 1,
      "marker " & $(i + 1) & " crosses from version ordinal " &
      $oldOrd.get() & " to " & $newOrd.get() & ", which is not the next one")
    check(ch.generation == uint64(newOrd.get()) + 1,
      "marker " & $(i + 1) & " carries wire generation " & $ch.generation &
      " while the new version's container ordinal is " & $newOrd.get() &
      ". They are off by one BY CONSTRUCTION (design §7.0) and this is " &
      "the gate that records the mapping rather than assuming it")
    check(ch.generation >= 2'u64,
      "marker " & $(i + 1) & " carries generation " & $ch.generation &
      "; 1 is the content the process started with, so a reload's " &
      "generation is 2 or more")
  check(checkedTies == 4,
    "expected 4 cross-ties (two per marker); made " & $checkedTies &
    ". A loop that ran fewer times than the fixture has markers " &
    "satisfies every assertion inside it")

  # --- in-flight frames, recorded rather than implied ----------------
  check(markers[0].inFlightFrames == Marker1InFlight,
    "marker 1's in_flight_frames is " & $markers[0].inFlightFrames &
    ", expected " & $Marker1InFlight)
  check(markers[1].inFlightFrames == Marker2InFlight,
    "marker 2's in_flight_frames is " & $markers[1].inFlightFrames &
    ", expected " & $Marker2InFlight)
  check(markers[0].inFlightFrames != markers[1].inFlightFrames,
    "both markers report the same in_flight_frames, so the field is " &
    "not shown to carry anything")

  # --- CONTROL ARM: no reload, same instrument ----------------------
  let plain = buildNoMarkerContainer()
  var pr = openContainer(plain)
  check(pr.meta.version == MetaDatVersion,
    "CONTROL ARM: a container with no reload must stay at meta.dat " &
    "schema version " & $MetaDatVersion & "; it is at " & $pr.meta.version)
  check(not pr.meta.hasSourceReload,
    "CONTROL ARM: a container with no reload declares the extended flag")
  let plainTotal = pr.stepCount()
  check(plainTotal.isOk, "CONTROL ARM: stepCount: " & plainTotal.error)
  check(plainTotal.get() > 0'u64,
    "CONTROL ARM: the control container's event stream is EMPTY, so " &
    "`zero markers` would be a statement about nothing (trap 4)")
  let plainMarkers = pr.sourceReloadCount()
  check(plainMarkers.isOk, "CONTROL ARM: sourceReloadCount: " & plainMarkers.error)
  check(plainMarkers.get() == 0'u64,
    "CONTROL ARM: a container with no reload reports " &
    $plainMarkers.get() & " markers")

  echo "PASS: gdh2_reload_marker_round_trips (2 markers, 4 cross-ties, " &
    $totalRes.get() & " exec records == " & $valRes.get() & " value records)"

# ---------------------------------------------------------------------------
# Gate 2 — tag 0x08 without the declaration is refused BY NAME
# ---------------------------------------------------------------------------

proc downgradeMetaDatToV4(bytes: seq[byte]): seq[byte] =
  ## Rewrite the container's ``meta.dat`` header from schema version 5 to
  ## version 4, IN PLACE, by dropping the ``flags_ext`` word.
  ##
  ## The result is a real container — real CTFS framing, real streams,
  ## real interning tables — whose step stream carries tag 0x08 and whose
  ## header does not declare it.  The writer refuses to produce that
  ## combination, which is precisely why it has to be built here: a
  ## reader's refusal path is not reachable from any input the writer can
  ## make.
  ##
  ## The rewrite keeps meta.dat's LENGTH unchanged (the four bytes are
  ## shifted out at the front and the tail is left as trailing bytes,
  ## which ``readMetaDat`` ignores), so no CTFS size or block mapping
  ## moves.  The alternative — rebuilding the container — would be a
  ## reimplementation of the writer inside its own test.
  result = bytes
  # Locate the meta.dat payload by its magic.  There is exactly one
  # "CTMD" followed by a version word of 5 in a container this small; the
  # search asserts it found exactly one so a miss cannot pass silently.
  var hits: seq[int] = @[]
  for i in 0 .. result.len - 12:
    if result[i] == 0x43 and result[i+1] == 0x54 and
       result[i+2] == 0x4D and result[i+3] == 0x44 and
       result[i+4] == 5'u8 and result[i+5] == 0'u8:
      hits.add(i)
  check(hits.len == 1,
    "expected exactly one v5 CTMD header in the container, found " &
    $hits.len & ". A fixture builder that cannot find its subject must " &
    "die rather than return the bytes unchanged — an unmodified " &
    "container would make the refusal below impossible and the gate " &
    "would report a pass it never earned")
  let at = hits[0]
  # version 5 -> 4
  result[at + 4] = 4'u8
  # drop the 4-byte flags_ext word: shift the remainder of the payload
  # left over it.  The last 4 bytes of the region become trailing bytes.
  for i in at + 8 ..< result.len - 4:
    result[i] = result[i + 4]

proc gate_unknown_tag_is_refused_by_name() =
  currentGate = "gdh2_unknown_tag_is_refused_by_name"
  let built = buildTwoMarkerContainer()

  # --- CONTROL ARM FIRST: the unmodified bytes must decode -----------
  #
  # Run before the negative case on purpose.  It is the positive twin of
  # the negative assertion, through the same code path: if the fixture
  # were malformed for some unrelated reason, this goes red and the
  # refusal below stops being evidence about the flag (trap 4a — the
  # pairing IS the control).
  block:
    var ok = openContainer(built.bytes)
    let sc = ok.stepCount()
    check(sc.isOk,
      "CONTROL ARM: the unmodified container does not decode: " & sc.error)
    check(sc.get() == built.execEvents,
      "CONTROL ARM: the unmodified container decodes to " & $sc.get() &
      " records, expected " & $built.execEvents)

  let downgraded = downgradeMetaDatToV4(built.bytes)
  check(downgraded.len == built.bytes.len,
    "the downgraded fixture changed length (" & $downgraded.len & " vs " &
    $built.bytes.len & "), so the CTFS framing no longer matches the " &
    "sizes in block 0 and any refusal below would be about that")

  # --- anti-vacuity: the container OPENS, and its header parses ------
  #
  # HLX-M1's resolver defect verbatim: it answered "not found" when it
  # could not read the table, so a refusal caused by an unreadable file
  # was indistinguishable from a refusal caused by the thing under test.
  # The header must therefore be proven readable BEFORE the refusal is
  # asserted, and proven to say the specific thing the refusal is
  # supposed to be about.
  var r = openContainer(downgraded)
  check(r.meta.version == MetaDatVersion,
    "the downgraded fixture is at schema version " & $r.meta.version &
    ", expected " & $MetaDatVersion)
  check(not r.meta.hasSourceReload,
    "the downgraded fixture still declares the source-reload flag, so " &
    "the reader would accept the tag and there is nothing to refuse")
  check(r.meta.flagsExt == 0'u32,
    "the downgraded fixture still carries a flags_ext word")
  check(r.pathCount() >= 2,
    "the downgraded fixture's paths.dat did not survive the rewrite (" &
    $r.pathCount() & " entries); the header did not really parse")
  check(r.meta.recordingId.len == 36,
    "the downgraded fixture's recording id is `" & r.meta.recordingId &
    "`, so the header shifted rather than being rewritten")

  # --- the refusal, and it must name the TAG ------------------------
  let sc = r.stepCount()
  check(sc.isErr,
    "the reader ACCEPTED a step stream carrying tag 0x08 over a " &
    "container that does not declare it. It reported " &
    (if sc.isOk: $sc.get() else: "?") & " records. Accepting is not the " &
    "worst outcome available here — SKIPPING is, because a skipped " &
    "record's payload is re-read as further events and the stream " &
    "decodes shorter and plausibly")
  check(sc.error.contains("tag: 8"),
    "the refusal does not name the tag; got: " & sc.error)
  check(sc.error.contains("FlagExtHasSourceReload"),
    "the refusal does not name the flag the container is missing, so a " &
    "caller learns that something is wrong but not what; got: " & sc.error)

  # --- and it must be a refusal, not a SHORTER stream ---------------
  #
  # The falsifier for this gate is "skip instead of refuse", and a skip
  # does not raise an error at all.  Comparing the decoded record count
  # against the value stream's — an independent index — is what catches
  # it, per the entry's own falsifier clause.
  let vc = r.valueCount()
  check(vc.isOk, "the value stream did not open: " & vc.error)
  check(vc.get() == built.execEvents,
    "the value stream holds " & $vc.get() & " records, the fixture " &
    "emitted " & $built.execEvents & "; the fixture is not what this " &
    "gate thinks it is")
  if sc.isOk:
    check(sc.get() == vc.get(),
      "the step stream decoded to " & $sc.get() & " records while the " &
      "value stream — parallel-indexed to it — holds " & $vc.get() &
      ". This is the skip-the-unknown-tag outcome: wrong bytes, no error")

  echo "PASS: gdh2_unknown_tag_is_refused_by_name (refused: " &
    sc.error.substr(0, 60) & "...)"

# ---------------------------------------------------------------------------
# Inertness, then the gates
# ---------------------------------------------------------------------------

proc assertInert() =
  currentGate = "gdh2_harness_inertness"
  let arm = activeGdh2FalsifierArm()
  check(arm.len == 0,
    "this binary was compiled with the falsifier arm `" & arm &
    "` ACTIVE. A green result here would be a measurement of a mutant, " &
    "not of the writer. Falsifier arms are run by " &
    "tests/run_gdh2_gates.sh, which REQUIRES them to fail")
  echo "PASS: gdh2_harness_inertness (no falsifier arm compiled in)"

when isMainModule:
  let selected = if paramCount() >= 1: paramStr(1) else: "all"

  # `dump <path>` writes the two-marker container so the SHIPPED
  # `ct-print` binary can be pointed at it.  The deliverable is that
  # `ct-print --events` emits the marker as its own event kind, and the
  # only way to show that is to RUN the instrument rather than to assert
  # through the library it does not import.
  if selected == "dump":
    currentGate = "gdh2_dump_container"
    check(paramCount() >= 2, "dump needs an output path")
    let built = buildTwoMarkerContainer()
    writeFile(paramStr(2), cast[string](built.bytes))
    echo "wrote " & paramStr(2) & " (" & $built.bytes.len & " bytes, " &
      "path ids " & $built.v1Id & "/" & $built.v2Id & "/" & $built.v3Id &
      ", " & $built.execEvents & " exec records)"
    quit(0)
  if selected == "dump-plain":
    currentGate = "gdh2_dump_plain_container"
    check(paramCount() >= 2, "dump-plain needs an output path")
    let plain = buildNoMarkerContainer()
    writeFile(paramStr(2), cast[string](plain))
    echo "wrote " & paramStr(2) & " (" & $plain.len & " bytes, no markers)"
    quit(0)

  let arm = activeGdh2FalsifierArm()
  if arm.len > 0:
    echo "ARMED: " & arm
  else:
    assertInert()

  var ran = 0
  if selected in ["all", "round_trip"]:
    gate_reload_marker_round_trips(); ran += 1
  if selected in ["all", "unknown_tag"]:
    gate_unknown_tag_is_refused_by_name(); ran += 1

  if ran == 0:
    currentGate = "gdh2_gate_selection"
    fail("`" & selected & "` names no gate, so nothing ran. A selector " &
      "that silently matches nothing turns every arm below it green")
  echo "GDH-M2: " & $ran & " gate(s) passed (" & selected & ")"
