## GDH-M1 — a path may be registered more than once, and the position
## space stays sound.
##
## The four gates of
## ``codetracer-specs/Planned-Features/GDScript-Hot-Reload-Multi-Version-Sources.milestones.org``
## § GDH-M1, at the writer/reader level:
##
##   1. ``gdh1_two_versions_one_path_round_trip``   (GDH-G1)
##   2. ``gdh1_same_string_different_index``        (GDH-G2)
##   3. ``gdh1_insertion_above_does_not_shift_v1``  (GDH-G4)
##   4. ``gdh1_step_past_a_version_line_count_is_refused``
##
## ``allowed_mocks: none``, and none are used. Every container here is
## produced by the production ``MultiStreamTraceWriter`` and read back
## through ``openNewTraceFromBytes`` — the same two objects a recorder and
## a debugger use. The fixture pair is read from the real filesystem
## (``tests/fixtures/gdh1/probe_v{1,2}.gd``) and the insertion height is
## DERIVED by diffing them here, never written into the verifier: a v2
## that inserted nothing could not shift anything, and a gate that took
## the height on trust would not notice.
##
## ---------------------------------------------------------------------
## Why this file is shaped around falsifier ARMS rather than assertions
## alone
##
## Every gate below has at least one named mutation that MUST turn it red.
## Those mutations are compiled into the writer under
## ``-d:gdh1FalsifierArms -d:gdh1Falsify<Name>`` (two defines, so no stray
## one can arm anything), and ``tests/run_gdh1_gates.sh`` runs each arm as
## a separate process and requires a NON-ZERO exit that is not 124.
##
## This file therefore asserts its own inertness first: a green run that
## was compiled with an arm active is not a green run, it is a
## measurement of a mutant. ``activeGdh1FalsifierArm()`` is compiled to a
## constant, so the check costs nothing and cannot be bypassed by
## configuration.
##
## Every failure is raised with a ``GDH1-FAIL[<gate>]`` prefix so the arm
## driver can require that the arm went red in the gate it was aimed at,
## rather than merely that the process exited non-zero — a compile error,
## a missing fixture or an unrelated crash also exit non-zero, and
## counting those as "the falsifier worked" is the silent self-pass this
## campaign is under a standing mandate to prevent.

import std/[os, strutils, options]
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/interning_table
import codetracer_trace_writer/global_line_index
import codetracer_ctfs/container

# ---------------------------------------------------------------------------
# Failure reporting
# ---------------------------------------------------------------------------

type Gdh1GateDefect = object of CatchableError

var currentGate = "<none>"

proc fail(msg: string) {.noreturn.} =
  raise newException(Gdh1GateDefect,
    "GDH1-FAIL[" & currentGate & "]: " & msg)

template check(cond: bool, msg: string) =
  ## A TEMPLATE, not a proc, and that is load-bearing: Nim evaluates a
  ## proc's arguments eagerly, so `check(r.isOk, "..." & r.error)` would
  ## read `.error` off a successful Result and abort with a ResultDefect
  ## on the PASSING path. A harness that dies when the subject is
  ## healthy is a harness whose green result nobody ever sees.
  if not cond:
    fail(msg)

# ---------------------------------------------------------------------------
# Fixtures — read from the real filesystem, diffed here
# ---------------------------------------------------------------------------

const FixtureDir = currentSourcePath().parentDir() / "fixtures" / "gdh1"
const ProbePath = "res://gdh1/probe.gd"
const OtherPath = "res://gdh1/autoload.gd"
const TrailingPath = "res://gdh1/util.gd"
const ProbeDefLine = "func probe(n: int) -> String:"

proc readFixtureLines(name: string): seq[string] =
  let p = FixtureDir / name
  if not fileExists(p):
    fail("fixture " & p & " does not exist. A gate whose inputs are " &
      "missing must DIE, not report a pass on a comparison it never made")
  result = readFile(p).splitLines()
  # `splitLines` on a trailing-newline file yields a final empty element;
  # the file's line count is the number of lines it has.
  if result.len > 0 and result[^1].len == 0:
    result.setLen(result.len - 1)
  if result.len == 0:
    fail("fixture " & p & " is empty")

proc lcsLength(a, b: seq[string]): seq[seq[int]] =
  ## Classic O(n*m) LCS table. The fixtures are 40 and 63 lines, so this
  ## is 2 520 cells — a real diff rather than a subtraction dressed up as
  ## one, which matters because the property under test is "lines were
  ## INSERTED ABOVE", not "the file got longer".
  result = newSeq[seq[int]](a.len + 1)
  for i in 0 .. a.len:
    result[i] = newSeq[int](b.len + 1)
  for i in countdown(a.len - 1, 0):
    for j in countdown(b.len - 1, 0):
      result[i][j] =
        if a[i] == b[j]: result[i + 1][j + 1] + 1
        else: max(result[i + 1][j], result[i][j + 1])

type DiffOp = enum doEqual, doInsert, doDelete

proc diffOps(a, b: seq[string]): seq[(DiffOp, int, int)] =
  ## `(op, indexInA, indexInB)` per line, walking the LCS table.
  let L = lcsLength(a, b)
  result = @[]
  var i = 0
  var j = 0
  while i < a.len and j < b.len:
    if a[i] == b[j]:
      result.add((doEqual, i, j)); i += 1; j += 1
    elif L[i + 1][j] >= L[i][j + 1]:
      result.add((doDelete, i, -1)); i += 1
    else:
      result.add((doInsert, -1, j)); j += 1
  while i < a.len:
    result.add((doDelete, i, -1)); i += 1
  while j < b.len:
    result.add((doInsert, -1, j)); j += 1

type Insertion = object
  runsAboveProbe: int         ## contiguous insert runs lying above the probe
  countAboveProbe: int        ## lines inserted above the probe
  lastInsertedLineInV2: int   ## 1-based, of the run above the probe
  probeDefLineV1: int         ## 1-based
  probeDefLineV2: int         ## 1-based

proc measureInsertion(v1, v2: seq[string]): Insertion =
  ## Derive the insertion height from the fixtures themselves.
  ##
  ## Measured as CONTIGUOUS INSERT RUNS rather than as a total, and only
  ## the runs above the probe definition are counted, following GDH-M0's
  ## fixture rule ("exactly ONE insert opcode, required to lie entirely
  ## above v2's `^func probe(` line"). A total would be satisfied by 23
  ## lines scattered through the file, which is not "inserted above" and
  ## would not shift the probe by 23.
  var probe1 = -1
  var probe2 = -1
  for idx, ln in v1:
    if ln.strip() == ProbeDefLine and probe1 < 0: probe1 = idx + 1
  for idx, ln in v2:
    if ln.strip() == ProbeDefLine and probe2 < 0: probe2 = idx + 1
  check(probe1 > 0, "v1 has no `" & ProbeDefLine & "` line")
  check(probe2 > 0, "v2 has no `" & ProbeDefLine & "` line")

  var runs = 0
  var inserted = 0
  var last = -1
  var inRun = false
  for (op, _, bIdx) in diffOps(v1, v2):
    if op == doInsert and bIdx + 1 < probe2:
      if not inRun:
        runs += 1
        inRun = true
      inserted += 1
      last = bIdx + 1
    else:
      inRun = false
  Insertion(runsAboveProbe: runs, countAboveProbe: inserted,
    lastInsertedLineInV2: last,
    probeDefLineV1: probe1, probeDefLineV2: probe2)

# ---------------------------------------------------------------------------
# Container construction helpers (production writer, no mocks)
# ---------------------------------------------------------------------------

let workDir = getTempDir() / "ctfnim-gdh1"

proc newWriter(name: string): MultiStreamTraceWriter =
  discard existsOrCreateDir(workDir)
  let r = initMultiStreamWriter(workDir / (name & ".build"), name)
  check(r.isOk, "initMultiStreamWriter(" & name & "): " & r.error)
  result = r.get()

proc finish(w: var MultiStreamTraceWriter): seq[byte] =
  let c = w.close()
  check(c.isOk, "close(): " & c.error)
  result = w.toBytes()
  discard w.closeCtfs()
  check(result.len > 0, "the writer produced an empty container")

proc mustRegisterStep(w: var MultiStreamTraceWriter,
    pathId, line: uint64) =
  let r = w.registerStep(pathId, line, @[])
  check(r.isOk, "registerStep(path " & $pathId & ", line " & $line &
    "): " & r.error)

proc openContainer(bytes: seq[byte]): NewTraceReader =
  let r = openNewTraceFromBytes(bytes)
  check(r.isOk, "openNewTraceFromBytes: " & r.error)
  result = r.get()

proc fixtureBytes(lines: seq[string]): seq[byte] =
  let text = lines.join("\n") & "\n"
  result = newSeq[byte](text.len)
  for i, c in text:
    result[i] = byte(c)

# ---------------------------------------------------------------------------
# Shared anti-vacuity: the streams are read under their REAL names
# ---------------------------------------------------------------------------

proc assertSourceViewStreamsAreFindable(bytes: seq[byte]) =
  ## Trap 4, stated in the gate: base40 caps a CTFS internal name at 12
  ## characters, so the spec's `source_views.dat` truncates and collides
  ## with `source_views.off`. The writer works around it by naming the
  ## streams `srcviews.dat` / `srcviews.off`. A gate that greps the
  ## container's member list for the SPEC name finds nothing and passes
  ## every "must not contain" check it writes.
  ##
  ## So: the real names must be PRESENT, and the spec name must be
  ## ABSENT — the second half is what proves the first half was a real
  ## lookup and not a function that answers true for everything.
  check(hasInternalFile(bytes, "srcviews.dat"),
    "the container has no `srcviews.dat`; the source views were not " &
    "written, and every assertion about their contents below would be " &
    "vacuous")
  check(hasInternalFile(bytes, "srcviews.off"),
    "the container has no `srcviews.off`")
  check(not hasInternalFile(bytes, "source_views.dat"),
    "`source_views.dat` resolved — the base40 truncation this check " &
    "exists to pin has changed, and every harness that greps for the " &
    "real name must be revisited")

# ---------------------------------------------------------------------------
# GATE 1 — gdh1_two_versions_one_path_round_trip (GDH-G1)
# ---------------------------------------------------------------------------

proc buildTwoVersionContainer(v1, v2: seq[string]):
    tuple[bytes: seq[byte], v1Id, v2Id: uint64] =
  var w = newWriter("gdh1_two_versions")
  check(w.enableLineCountTable().isOk, "enableLineCountTable refused")

  let r1 = w.registerPath(ProbePath, lineCount = uint64(v1.len))
  check(r1.isOk, "registerPath(v1): " & r1.error)
  let v1Id = r1.get()
  let sv1 = w.registerSourceView(v1Id, 0'u8, "raw", fixtureBytes(v1), @[])
  check(sv1.isOk, "registerSourceView(v1): " & sv1.error)
  w.mustRegisterStep(v1Id, uint64(v1.len))

  # Before any version exists, the string-taking step path must behave
  # exactly as it always did — the interning lookup's answer.
  let beforeCurrent = w.currentPathId(ProbePath)
  check(beforeCurrent.isNone,
    "currentPathId answered before any version was registered; a path " &
    "with one entry must report `none` so the caller can tell " &
    "\"unversioned\" from \"version 0\"")
  let beforeStepId = w.pathIdForStep(ProbePath)
  check(beforeStepId.isOk and beforeStepId.get() == v1Id,
    "pathIdForStep must resolve to the interned id (" & $v1Id &
    ") when no version has been registered")

  let r2 = w.registerPathVersion(ProbePath, uint64(v2.len))
  check(r2.isOk, "registerPathVersion(v2): " & r2.error)
  let v2Id = r2.get()
  let sv2 = w.registerSourceView(v2Id, 0'u8, "raw", fixtureBytes(v2), @[])
  check(sv2.isOk, "registerSourceView(v2): " & sv2.error)
  w.mustRegisterStep(v2Id, uint64(v2.len))

  # Design §6.1's tractability claim: the Godot fork's hot path stays
  # `register_step(handle, path_string, line)` and does NOT become
  # version-aware. That only holds if the writer resolves a bare path
  # string to the NEWEST version, which is what this asserts.
  let afterCurrent = w.currentPathId(ProbePath)
  check(afterCurrent.isSome and afterCurrent.get() == v2Id,
    "after registering v2, currentPathId(" & ProbePath & ") must be " &
    $v2Id & "; got " &
    (if afterCurrent.isSome: $afterCurrent.get() else: "none"))
  let afterStepId = w.pathIdForStep(ProbePath)
  check(afterStepId.isOk and afterStepId.get() == v2Id,
    "a bare registerStep(path_string, line) after a reload must resolve " &
    "to the newest version (" & $v2Id & "); it resolved to " &
    (if afterStepId.isOk: $afterStepId.get() else: afterStepId.error) &
    ". Resolving to the superseded id renders every post-reload step " &
    "against the old text")
  # And the map is keyed PER PATH STRING, not a global "most recent id".
  # An unseen path must not inherit the versioned path's answer. Under the
  # line-count table an unseen path cannot be implicitly registered at all
  # (it has no count to record), so the correct outcome is a refusal that
  # NAMES it — never `v2Id`, which is what a global map would return and
  # which would attach the other file's steps to the reloaded one.
  let unrelated = w.pathIdForStep(OtherPath)
  check(unrelated.isErr,
    "pathIdForStep on an unseen path returned " &
    (if unrelated.isOk: $unrelated.get() else: "?") &
    "; under the line-count table an unseen path has no line count and " &
    "must be refused rather than interned with an assumed size")
  check(unrelated.error.contains(OtherPath),
    "the refusal must name the path it refused; got: " & unrelated.error)

  (bytes: w.finish(), v1Id: v1Id, v2Id: v2Id)

proc gate_two_versions_one_path_round_trip() =
  currentGate = "gdh1_two_versions_one_path_round_trip"
  let v1 = readFixtureLines("probe_v1.gd")
  let v2 = readFixtureLines("probe_v2.gd")
  check(v1.len == 40, "fixture v1 must be 40 lines; got " & $v1.len)
  check(v2.len == 63, "fixture v2 must be 63 lines; got " & $v2.len)

  let built = buildTwoVersionContainer(v1, v2)
  var r = openContainer(built.bytes)

  # --- anti-vacuity, before any claim about the subject -----------------
  check(r.meta.hasLineCountTable,
    "the container does not declare meta.dat bit 14, so it states no " &
    "file sizes and the line counts below would be assumptions")
  check(r.pathCount() == 2'u64,
    "expected exactly 2 paths.dat entries; got " & $r.pathCount() &
    ". One entry means the versioned registration deduped")

  let p0 = r.path(0'u64)
  let p1 = r.path(1'u64)
  check(p0.isOk, "path(0): " & p0.error)
  check(p1.isOk, "path(1): " & p1.error)
  check(p0.get().len > 0, "path(0) decoded to an EMPTY string; a " &
    "comparison of two empty strings is true for free")
  check(p1.get().len > 0, "path(1) decoded to an EMPTY string")
  check(p0.get() == ProbePath,
    "path(0) must be the fixture's literal path `" & ProbePath &
    "`; got `" & p0.get() & "`")
  check(p1.get() == ProbePath,
    "path(1) must be the fixture's literal path `" & ProbePath &
    "`; got `" & p1.get() & "` — the versioned registration mangled " &
    "the string")

  assertSourceViewStreamsAreFindable(built.bytes)
  check(r.sourceViewCount() == 2'u64,
    "expected 2 source views, one per version; got " & $r.sourceViewCount())

  # --- the subject ------------------------------------------------------
  check(built.v1Id != built.v2Id,
    "the two versions were given the SAME path id (" & $built.v1Id &
    "); a version IS the index, so identical ids are no versioning at all")

  check(r.recordedLineCount(built.v1Id) == 40'u64,
    "v1's slot must be sized to its own 40 lines; the container states " &
    $r.recordedLineCount(built.v1Id))
  check(r.recordedLineCount(built.v2Id) == 63'u64,
    "v2's slot must be sized to its own 63 lines; the container states " &
    $r.recordedLineCount(built.v2Id) &
    ". A slot of " & $DefaultLinesPerFile & " means the version was " &
    "appended without its count and fell back to the ceiling — the " &
    "silent mode this milestone exists to remove")

  let o0 = r.pathVersionOrdinal(built.v1Id)
  let o1 = r.pathVersionOrdinal(built.v2Id)
  check(o0.isOk and o0.get() == 0'u64,
    "v1's version ordinal must be 0; got " &
    (if o0.isOk: $o0.get() else: o0.error))
  check(o1.isOk and o1.get() == 1'u64,
    "v2's version ordinal must be 1; got " &
    (if o1.isOk: $o1.get() else: o1.error))
  let ids = r.pathIdsForString(ProbePath)
  check(ids == @[built.v1Id, built.v2Id],
    "pathIdsForString must list both ids in path-id order; got " & $ids)

  # Two DISTINCT retrievable source views, each attached to its own id.
  let vs1 = r.sourceViewsForPath(built.v1Id)
  let vs2 = r.sourceViewsForPath(built.v2Id)
  check(vs1.len == 1, "v1 must carry exactly one source view; got " & $vs1.len)
  check(vs2.len == 1, "v2 must carry exactly one source view; got " & $vs2.len)
  let sv1 = r.sourceView(vs1[0])
  let sv2 = r.sourceView(vs2[0])
  check(sv1.isOk and sv2.isOk, "a registered source view did not read back")
  check(sv1.get().content.len > 0, "v1's source view is EMPTY")
  check(sv2.get().content.len > 0, "v2's source view is EMPTY")
  check(sv1.get().content != sv2.get().content,
    "the two source views hold IDENTICAL bytes; the container carries " &
    "one version's text twice, which is indistinguishable from carrying " &
    "one version")
  check(sv1.get().content == fixtureBytes(v1),
    "v1's source view is not probe_v1.gd's bytes")
  check(sv2.get().content == fixtureBytes(v2),
    "v2's source view is not probe_v2.gd's bytes")

  # --- control arm: registerPath twice yields ONE entry -----------------
  block controlArm:
    var cw = newWriter("gdh1_control_registerpath_twice")
    check(cw.enableLineCountTable().isOk, "control: enableLineCountTable")
    let a = cw.registerPath(ProbePath, lineCount = uint64(v1.len))
    check(a.isOk, "control registerPath #1: " & a.error)
    let b = cw.registerPath(ProbePath, lineCount = uint64(v2.len))
    check(b.isOk, "control registerPath #2: " & b.error)
    cw.mustRegisterStep(a.get(), 1'u64)
    let cbytes = cw.finish()
    var cr = openContainer(cbytes)
    check(cr.pathCount() == 1'u64,
      "CONTROL ARM: registerPath called twice must still yield ONE " &
      "paths.dat entry; got " & $cr.pathCount() & ". If it yields two, " &
      "the fixture — not registerPathVersion — is what produced the two " &
      "entries above, and this gate measures nothing")
    check(a.get() == b.get(),
      "CONTROL ARM: registerPath must dedup to the same id")
    check(cr.recordedLineCount(0'u64) == 40'u64,
      "CONTROL ARM: the deduped entry keeps the FIRST count")

  echo "PASS: gdh1_two_versions_one_path_round_trip"

# ---------------------------------------------------------------------------
# GATE 2 — gdh1_same_string_different_index (GDH-G2)
# ---------------------------------------------------------------------------

proc gate_same_string_different_index() =
  currentGate = "gdh1_same_string_different_index"
  let v1 = readFixtureLines("probe_v1.gd")
  let v2 = readFixtureLines("probe_v2.gd")

  var w = newWriter("gdh1_same_string")
  check(w.enableLineCountTable().isOk, "enableLineCountTable refused")
  let r1 = w.registerPath(ProbePath, lineCount = uint64(v1.len))
  check(r1.isOk, "registerPath: " & r1.error)
  # The control arm's genuinely different path, registered ALONGSIDE, so
  # the string comparison is shown to discriminate rather than to be a
  # function that answers "equal" for everything.
  let rOther = w.registerPath(OtherPath, lineCount = 12'u64)
  check(rOther.isOk, "registerPath(other): " & rOther.error)
  let r2 = w.registerPathVersion(ProbePath, uint64(v2.len))
  check(r2.isOk, "registerPathVersion: " & r2.error)
  w.mustRegisterStep(r1.get(), 1'u64)
  let bytes = w.finish()
  var r = openContainer(bytes)

  check(r.pathCount() == 3'u64,
    "expected 3 entries (v1, other, v2); got " & $r.pathCount())

  let a = r.path(r1.get())
  let b = r.path(r2.get())
  let o = r.path(rOther.get())
  check(a.isOk and b.isOk and o.isOk, "a path did not decode")

  # --- anti-vacuity -----------------------------------------------------
  check(a.get().len > 0, "v1's payload is EMPTY — two empty strings " &
    "compare equal for free (trap 5)")
  check(b.get().len > 0, "v2's payload is EMPTY")
  check(a.get() == ProbePath,
    "v1's payload must equal the fixture's literal path `" & ProbePath &
    "`; got `" & a.get() & "`")
  check(b.get() == ProbePath,
    "v2's payload must equal the fixture's literal path `" & ProbePath &
    "`; got `" & b.get() & "`. Appending `#2`, or interposing a " &
    "generation, makes two entries while breaking every consumer that " &
    "resolves a user-supplied path")

  # --- the subject: equal AFTER qualifier splitting, ids differ ---------
  let splitA = splitInterningPayload(a.get())
  let splitB = splitInterningPayload(b.get())
  check(splitA.name == splitB.name,
    "the two versions' path strings differ after qualifier splitting: `" &
    splitA.name & "` vs `" & splitB.name & "`")
  check(splitA.qualifier == splitB.qualifier,
    "the two versions carry DIFFERENT qualifiers (`" & splitA.qualifier &
    "` vs `" & splitB.qualifier & "`); the qualifier is a producer " &
    "namespace, not a version (design §6.2)")
  check(a.get() == b.get(),
    "the two payloads are not byte-identical")
  check(r1.get() != r2.get(),
    "the two versions share a path id (" & $r1.get() & ")")

  # --- control arm ------------------------------------------------------
  check(o.get() != a.get(),
    "CONTROL ARM: a genuinely different path (`" & OtherPath &
    "`) compared EQUAL to `" & ProbePath & "`; the comparison does not " &
    "discriminate and every equality above is worthless")
  check(splitInterningPayload(o.get()).name != splitA.name,
    "CONTROL ARM: the different path's split name compared equal too")

  # --- the same property at the interning table, under a real qualifier -
  # GDH-OQ-8's subject: a qualified payload carries a literal 0x1f. Two
  # versions of one file must still compare equal as strings once the
  # qualifier is split off, or design §7.1's path-string comparisons do
  # not hold for a qualified producer.
  block qualifiedArm:
    var ctfs = createCtfs()
    let itRes = initInterningTableWriter(ctfs, "paths")
    check(itRes.isOk, "initInterningTableWriter: " & itRes.error)
    var it = itRes.get()
    let q1 = ctfs.appendQualifiedPathWithLineCount(it, "gdscript",
      ProbePath, 40'u64)
    let q2 = ctfs.appendQualifiedPathWithLineCount(it, "gdscript",
      ProbePath, 63'u64)
    check(q1.isOk and q2.isOk, "qualified append refused")
    check(q1.get() != q2.get(),
      "qualified: two versions share an id")
    let pay1 = qualifiedPayload("gdscript", ProbePath)
    check(pay1.contains(InterningUnitSeparator),
      "the qualified payload carries no 0x1f separator; this arm is " &
      "not testing what it claims to")
    let s1 = splitInterningPayload(pay1)
    check(s1.qualifier == "gdscript" and s1.name == ProbePath,
      "splitInterningPayload did not recover (gdscript, " & ProbePath &
      "); got (" & s1.qualifier & ", " & s1.name & ")")

  echo "PASS: gdh1_same_string_different_index"

# ---------------------------------------------------------------------------
# GATE 3 — gdh1_insertion_above_does_not_shift_v1 (GDH-G4)
# ---------------------------------------------------------------------------
#
# The fixture deliberately puts the versioned file BETWEEN two others.
#
# A file's base in the global position space is the sum of the sizes of
# the files BEFORE it, so a file's base is invariant to its OWN size. The
# `pathLineCounts[v1_id]`-overwrite arm therefore cannot move v1's base;
# what it moves is the base of every file AFTER v1. A fixture in which v1
# is the only file, or the last one, leaves that arm green while the
# defect is fully present — so the gate asserts the property over EVERY
# path registered before v2, with v1's own line coverage counted
# separately and exactly.
#
# The sweep is emitted TWICE — once before v2 exists and once after — and
# the two runs must decode identically. The "after" run is what the
# insert-at-index-0 arm moves; the "before" run is the milestone's stated
# property. Both are properties the design requires (§7.1 renders a call
# recorded in v1 against v1 no matter when it was recorded), and only
# both together make the two arms lethal.

type SweepEntry = object
  pathId: uint64
  line: uint64

proc buildInsertionContainer(v1, v2: seq[string], registerV2: bool):
    tuple[bytes: seq[byte], sweep: seq[SweepEntry], v1Id: uint64,
          v1Lines: int, otherLines: int] =
  var w = newWriter(
    if registerV2: "gdh1_insertion" else: "gdh1_insertion_control")
  check(w.enableLineCountTable().isOk, "enableLineCountTable refused")

  let rOther = w.registerPath(OtherPath, lineCount = 12'u64)
  check(rOther.isOk, "registerPath(other): " & rOther.error)
  let r1 = w.registerPath(ProbePath, lineCount = uint64(v1.len))
  check(r1.isOk, "registerPath(v1): " & r1.error)
  let rTrail = w.registerPath(TrailingPath, lineCount = 9'u64)
  check(rTrail.isOk, "registerPath(trailing): " & rTrail.error)

  var sweep: seq[SweepEntry] = @[]
  var v1Lines = 0
  var otherLines = 0
  proc emitSweep(w: var MultiStreamTraceWriter) =
    for line in 1'u64 .. 12'u64:
      sweep.add(SweepEntry(pathId: rOther.get(), line: line))
      w.mustRegisterStep(rOther.get(), line)
      otherLines += 1
    for line in 1'u64 .. uint64(v1.len):
      sweep.add(SweepEntry(pathId: r1.get(), line: line))
      w.mustRegisterStep(r1.get(), line)
      v1Lines += 1
    for line in 1'u64 .. 9'u64:
      sweep.add(SweepEntry(pathId: rTrail.get(), line: line))
      w.mustRegisterStep(rTrail.get(), line)
      otherLines += 1

  emitSweep(w)
  if registerV2:
    let r2 = w.registerPathVersion(ProbePath, uint64(v2.len))
    check(r2.isOk, "registerPathVersion: " & r2.error)
  emitSweep(w)

  (bytes: w.finish(), sweep: sweep, v1Id: r1.get(),
   v1Lines: v1Lines, otherLines: otherLines)

proc verifySweep(bytes: seq[byte], sweep: seq[SweepEntry],
    v1Id: uint64, v1Lines, otherLines: int, label: string) =
  var r = openContainer(bytes)
  check(r.meta.hasLineCountTable, label & ": container states no sizes")

  let scRes = r.stepCount()
  check(scRes.isOk, label & ": stepCount: " & scRes.error)
  check(scRes.get() == uint64(sweep.len),
    label & ": the container carries " & $scRes.get() & " steps but " &
    $sweep.len & " were registered; a truncated stream would satisfy " &
    "every per-step comparison below")

  var glis = newSeq[uint64](sweep.len)
  let n = r.stepAbsoluteGlobalLineIndices(0'u64, uint64(sweep.len), glis)
  check(n.isOk, label & ": stepAbsoluteGlobalLineIndices: " & n.error)
  check(n.get() == uint64(sweep.len),
    label & ": resolved " & $n.get() & " of " & $sweep.len & " steps")

  let space = r.globalPositionSpace()
  var checkedV1 = 0
  var checkedOther = 0
  for i, want in sweep:
    let back = space.tryResolve(glis[i])
    check(back.isOk,
      label & ": step " & $i & " (registered at path " & $want.pathId &
      " line " & $want.line & ") encodes to " & $glis[i] &
      " which does not resolve: " & back.error)
    let got = back.get()
    check(uint64(got[0]) == want.pathId and got[1] == want.line,
      label & ": step " & $i & " was registered at (path " &
      $want.pathId & ", line " & $want.line & ") and decodes to (path " &
      $got[0] & ", line " & $got[1] & "). Registering a new version " &
      "moved an address that was already emitted")
    if want.pathId == v1Id: checkedV1 += 1
    else: checkedOther += 1

  # --- anti-vacuity: the loop actually ran, over exactly v1's lines -----
  check(checkedV1 == v1Lines,
    label & ": checked " & $checkedV1 & " v1 addresses but v1 has " &
    $v1Lines & " registered lines; a loop that ran zero times satisfies " &
    "\"every address is unchanged\"")
  check(checkedV1 > 0, label & ": zero v1 addresses were checked")
  check(checkedOther == otherLines,
    label & ": checked " & $checkedOther & " non-v1 addresses, expected " &
    $otherLines)
  check(uint64(checkedV1 div 2) == r.recordedLineCount(v1Id),
    label & ": the number of v1 lines checked per sweep (" &
    $(checkedV1 div 2) & ") must equal v1's recorded line count (" &
    $r.recordedLineCount(v1Id) & ")")

proc gate_insertion_above_does_not_shift_v1() =
  currentGate = "gdh1_insertion_above_does_not_shift_v1"
  let v1 = readFixtureLines("probe_v1.gd")
  let v2 = readFixtureLines("probe_v2.gd")

  # --- anti-vacuity: the insertion is real, derived, and above the probe
  let ins = measureInsertion(v1, v2)
  check(ins.countAboveProbe >= 5,
    "the fixtures differ by only " & $ins.countAboveProbe &
    " line(s) inserted above the probe; a v2 that inserted nothing " &
    "could not shift anything and this gate would be vacuous")
  check(ins.runsAboveProbe == 1,
    "the insertion above the probe is split across " &
    $ins.runsAboveProbe & " run(s); GDH-M0's fixture rule requires " &
    "exactly ONE contiguous insert lying entirely above `" &
    ProbeDefLine & "`, because scattered insertions do not shift the " &
    "probe by their total")
  check(ins.lastInsertedLineInV2 < ins.probeDefLineV2,
    "the last inserted line is at v2 line " & $ins.lastInsertedLineInV2 &
    ", at or below the probe definition at line " & $ins.probeDefLineV2 &
    "; the insertion must lie ABOVE the probe")
  check(ins.probeDefLineV2 - ins.probeDefLineV1 == ins.countAboveProbe,
    "the probe moved by " & $(ins.probeDefLineV2 - ins.probeDefLineV1) &
    " lines but the diff found " & $ins.countAboveProbe &
    " insertions above it")
  check(v1 != v2,
    "the two fixture versions are byte-identical; a reload that changed " &
    "nothing cannot distinguish a version from its predecessor")
  check(v1.len == 40 and v2.len == 63,
    "the fixtures must be 40 and 63 lines; got " & $v1.len & " and " &
    $v2.len)

  # --- the subject ------------------------------------------------------
  let withV2 = buildInsertionContainer(v1, v2, registerV2 = true)
  verifySweep(withV2.bytes, withV2.sweep, withV2.v1Id, withV2.v1Lines,
    withV2.otherLines, "with v2")

  var rv = openContainer(withV2.bytes)
  check(rv.pathCount() == 4'u64,
    "expected 4 entries (other, v1, trailing, v2); got " & $rv.pathCount() &
    ". A count of 3 means the version was written over an existing slot " &
    "instead of appended")
  check(rv.recordedLineCount(withV2.v1Id) == 40'u64,
    "v1's recorded line count changed to " &
    $rv.recordedLineCount(withV2.v1Id) & "; registering v2 resized v1")
  let v2Ids = rv.pathIdsForString(ProbePath)
  check(v2Ids.len == 2, "expected 2 entries for " & ProbePath & "; got " &
    $v2Ids.len)
  check(rv.recordedLineCount(v2Ids[1]) == 63'u64,
    "v2's slot is sized " & $rv.recordedLineCount(v2Ids[1]) & ", not 63")
  check(v2Ids[1] == 3'u64,
    "the new version must be APPENDED (id 3); it landed at id " &
    $v2Ids[1] & ". An index-0 insertion moves every earlier file's base")

  # --- control arm: the same sweep with v2 never registered -------------
  let noV2 = buildInsertionContainer(v1, v2, registerV2 = false)
  verifySweep(noV2.bytes, noV2.sweep, noV2.v1Id, noV2.v1Lines,
    noV2.otherLines, "CONTROL ARM (v2 never registered)")

  echo "PASS: gdh1_insertion_above_does_not_shift_v1 (" &
    $withV2.v1Lines & " v1 addresses, " & $withV2.otherLines &
    " others, insertion height " & $ins.countAboveProbe & ")"

# ---------------------------------------------------------------------------
# GATE 4 — gdh1_step_past_a_version_line_count_is_refused
# ---------------------------------------------------------------------------

proc gate_step_past_a_version_line_count_is_refused() =
  currentGate = "gdh1_step_past_a_version_line_count_is_refused"
  let v1 = readFixtureLines("probe_v1.gd")
  let v2 = readFixtureLines("probe_v2.gd")

  # A line that exists ONLY in v2: past v1's end, within v2's.
  let onlyInV2 = uint64(v1.len) + 5'u64
  check(onlyInV2 > uint64(v1.len) and onlyInV2 <= uint64(v2.len),
    "line " & $onlyInV2 & " must be past v1's " & $v1.len &
    " lines and within v2's " & $v2.len)

  var w = newWriter("gdh1_step_past_count")
  check(w.enableLineCountTable().isOk, "enableLineCountTable refused")

  # --- anti-vacuity: the writer is in line-count-table mode -------------
  # `checkLineWithinFile` is a DOCUMENTED no-op when the table is off, so
  # a gate run against a writer that never opted in passes while testing
  # nothing.
  check(w.lineCountTable,
    "the writer is NOT in line-count-table mode; checkLineWithinFile is " &
    "a documented no-op there and the refusal below would be untested")

  let r1 = w.registerPath(ProbePath, lineCount = uint64(v1.len))
  check(r1.isOk, "registerPath(v1): " & r1.error)
  let v1Id = r1.get()
  let r2 = w.registerPathVersion(ProbePath, uint64(v2.len))
  check(r2.isOk, "registerPathVersion(v2): " & r2.error)
  let v2Id = r2.get()

  # --- control arm: a line WITHIN v1's count is accepted ---------------
  let within = w.registerStep(v1Id, uint64(v1.len), @[])
  check(within.isOk,
    "CONTROL ARM: a step at v1's LAST line (" & $v1.len & ") must be " &
    "accepted; it was refused: " & within.error &
    ". A refusal that is universal is not a bound")

  # v2's own slot accommodates the same line number.
  let inV2 = w.registerStep(v2Id, onlyInV2, @[])
  check(inV2.isOk,
    "a step at line " & $onlyInV2 & " of v2 must be accepted — v2 has " &
    $v2.len & " lines and its slot is sized to them: " & inV2.error)

  # --- the subject ------------------------------------------------------
  let past = w.registerStep(v1Id, onlyInV2, @[])
  if past.isOk:
    # The falsifier arm's world. Prove the HARM rather than settling for
    # the absence of an error: the accepted step must be decoded and
    # shown to resolve somewhere it was never recorded.
    let bytes = w.finish()
    var r = openContainer(bytes)
    let scRes = r.stepCount()
    check(scRes.isOk, "stepCount: " & scRes.error)
    var glis = newSeq[uint64](int(scRes.get()))
    let n = r.stepAbsoluteGlobalLineIndices(0'u64, scRes.get(), glis)
    check(n.isOk, "stepAbsoluteGlobalLineIndices: " & n.error)
    let space = r.globalPositionSpace()
    let back = space.tryResolve(glis[^1])
    # `back.error` must not be read unless `back` is an err — reading the
    # unset arm of a Result aborts with a ResultDefect, and an abort here
    # would exit non-zero WITHOUT the GDH1-FAIL prefix, which the arm
    # driver refuses to count as a red gate. It caught exactly that.
    let where =
      if back.isOk:
        "(path " & $back.get()[0] & ", line " & $back.get()[1] & ")"
      else:
        "an address outside the trace's space (" & back.error & ")"
    fail("a step at line " & $onlyInV2 & " of a file this trace records " &
      "as having " & $v1.len & " lines was ACCEPTED, and reads back as " &
      where & " — a location that was never recorded. " &
      "`checkLineWithinFile` did not refuse it")

  check(past.error.contains($onlyInV2),
    "the refusal must name the offending line " & $onlyInV2 & "; got: " &
    past.error)
  check(past.error.contains(ProbePath),
    "the refusal must name the file " & ProbePath & "; got: " & past.error)
  check(past.error.contains($v1.len),
    "the refusal must name the count the trace records (" & $v1.len &
    "); got: " & past.error)

  # --- and a zero line count is refused by the SAME named diagnostic ----
  let zero = w.registerPathVersion(ProbePath, 0'u64)
  check(zero.isErr, "registerPathVersion accepted a zero lineCount")
  check(zero.error == zeroLineCountDiagnostic(ProbePath),
    "registerPathVersion must refuse a zero lineCount with the SAME " &
    "named diagnostic ensureQualifiedPathIdWithLineCount uses.\n" &
    "  got:      " & zero.error & "\n" &
    "  expected: " & zeroLineCountDiagnostic(ProbePath))

  # --- and a writer with NO line-count table refuses outright -----------
  # A versioned record on such a writer has nowhere to put a size: both
  # versions would be laid out at the `DefaultLinesPerFile` stride and
  # nothing — not `checkLineWithinFile`, not any reader — could bound a
  # version against its own count. That is precisely GDH-M0's silent
  # mis-attribution mode, so the refusal is asserted here rather than left
  # to the implementation's good intentions.
  block noTableArm:
    var nw = newWriter("gdh1_no_line_count_table")
    check(not nw.lineCountTable,
      "the control writer already has a line-count table; this arm would " &
      "not be testing the refusal it names")
    let base = nw.registerPath(ProbePath)
    check(base.isOk, "registerPath on a bare writer: " & base.error)
    let refused = nw.registerPathVersion(ProbePath, uint64(v2.len))
    check(refused.isErr,
      "registerPathVersion was ACCEPTED on a writer with no line-count " &
      "table; the second record carries no size, both versions land on " &
      "the DefaultLinesPerFile stride, and the mis-attribution is silent")
    check(refused.error.contains(ProbePath),
      "the refusal must name the path; got: " & refused.error)
    check(refused.error.contains("line-count table"),
      "the refusal must name the missing line-count table, so a caller " &
      "learns what to turn on; got: " & refused.error)
    # CONTROL: the same writer still accepts an ordinary registerPath, so
    # the refusal is shown to be specific to the versioned append and not
    # a writer that refuses everything.
    let stillOk = nw.registerPath(OtherPath)
    check(stillOk.isOk,
      "CONTROL ARM: a bare registerPath must still succeed on this " &
      "writer; it failed with " & stillOk.error)
    discard nw.finish()

  discard w.finish()
  echo "PASS: gdh1_step_past_a_version_line_count_is_refused"

# ---------------------------------------------------------------------------
# Inertness, then the gates
# ---------------------------------------------------------------------------

proc assertInert() =
  currentGate = "gdh1_harness_inertness"
  let arm = activeGdh1FalsifierArm()
  check(arm.len == 0,
    "this binary was compiled with the falsifier arm `" & arm &
    "` ACTIVE. A green result here would be a measurement of a mutant, " &
    "not of the writer. Falsifier arms are run by " &
    "tests/run_gdh1_gates.sh, which REQUIRES them to fail")
  echo "PASS: gdh1_harness_inertness (no falsifier arm compiled in)"

when isMainModule:
  # An optional argument selects ONE gate, which is how the falsifier-arm
  # driver aims each mutation at the gate it is supposed to kill: an arm
  # that turns some OTHER gate red has not been shown to discriminate,
  # and "the process exited non-zero" is not the property being measured.
  let selected = if paramCount() >= 1: paramStr(1) else: "all"

  # `dump <path>` writes the two-version container so the SHIPPED
  # `ct-print` binary can be pointed at it. Deliverable 4 of GDH-M1 is an
  # instrument "that does not require linking the reader", and the only
  # way to show that is to run the instrument.
  if selected == "dump":
    currentGate = "gdh1_dump_container"
    check(paramCount() >= 2, "dump needs an output path")
    let v1 = readFixtureLines("probe_v1.gd")
    let v2 = readFixtureLines("probe_v2.gd")
    let built = buildTwoVersionContainer(v1, v2)
    writeFile(paramStr(2), cast[string](built.bytes))
    echo "wrote " & paramStr(2) & " (" & $built.bytes.len & " bytes, ids " &
      $built.v1Id & " and " & $built.v2Id & ")"
    quit(0)

  let arm = activeGdh1FalsifierArm()
  if arm.len > 0:
    echo "ARMED: " & arm
  else:
    assertInert()

  var ran = 0
  if selected in ["all", "two_versions"]:
    gate_two_versions_one_path_round_trip(); ran += 1
  if selected in ["all", "same_string"]:
    gate_same_string_different_index(); ran += 1
  if selected in ["all", "insertion"]:
    gate_insertion_above_does_not_shift_v1(); ran += 1
  if selected in ["all", "step_past"]:
    gate_step_past_a_version_line_count_is_refused(); ran += 1

  if ran == 0:
    currentGate = "gdh1_gate_selection"
    fail("`" & selected & "` names no gate, so nothing ran. A selector " &
      "that silently matches nothing turns every arm below it green")
  echo "GDH-M1: " & $ran & " gate(s) passed (" & selected & ")"
