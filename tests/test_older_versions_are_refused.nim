## Every reader door refuses a container from before the 2026-10 revision,
## naming the version it found and the one it reads:
##
## * `meta.dat` at any schema version but 6 (`internal-files.md` §"Version
##   History", v6: "Readers refuse every other version, naming it") — the
##   bytes after `recorder_id` were a path list up to version 5, and versions
##   3 and below also packed line-only positions one line higher;
## * the CTFS container at any version but 5 (`ctfs-container.md` §2,
##   "Older versions are refused").
##
## Checked through `readMetaDat`, `openNewTraceFromBytes`, `openNewTrace`,
## `openTrace` and the C ABI's `ct_reader_open` (the door the db-backend takes),
## and DISCRIMINATING: the same container at the current versions opens and
## its steps read back at the recorded lines.
##
## This replaces the version-3 refusal test and its caller opt-in
## (`acceptShiftedGlobalIndex`): pre-1.0 there is no compatibility path, and a
## version 3 header cannot be parsed under version 6's layout at all.
##
## No mocks: containers written by this repository's writer, version bytes
## stamped in place, read back through the real readers.

include codetracer_trace_writer_ffi

{.pop.}

import std/strutils
import codetracer_trace_reader as legacy_reader

const
  TmpDir = "tmp_older_versions_refused"
  Steps: array[3, (uint64, uint64)] = [(0'u64, 3'u64), (1'u64, 7'u64),
    (0'u64, 12'u64)]

proc writeCurrent(file: string) =
  var w = initMultiStreamWriter(file & ".build", "older_versions").get()
  doAssert w.registerPath("/src/a.rb").isOk
  doAssert w.registerPath("/src/b.rb").isOk
  for (p, l) in Steps:
    doAssert w.registerStep(p, l, @[]).isOk
  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()
  writeFile(file, cast[string](bytes))

proc stamp(file: string, metaVersion = -1, ctfsVersion = -1) =
  ## Rewrite the meta.dat schema version and/or the container version byte in
  ## place, touching nothing else.
  var raw = readFile(file)
  if metaVersion >= 0:
    let i = raw.find("CTMD")
    doAssert i >= 0 and raw.count("CTMD") == 1, "cannot find meta.dat in " & file
    raw[i + 4] = char(metaVersion and 0xFF)
    raw[i + 5] = char((metaVersion shr 8) and 0xFF)
  if ctfsVersion >= 0:
    raw[5] = char(ctfsVersion)
  writeFile(file, raw)

proc refusalsOf(file: string): seq[string] =
  ## The refusal from every reader door; an opened door contributes "".
  let bytes = cast[seq[byte]](readFile(file))
  let a = openNewTraceFromBytes(bytes)
  result.add(if a.isErr: a.unsafeError else: "")
  let b = openNewTrace(file)
  result.add(if b.isErr: b.unsafeError else: "")
  let c = legacy_reader.openTrace(file)
  result.add(if c.isErr: c.unsafeError else: "")
  let h = ct_reader_open(file.cstring)
  if h.isNil:
    result.add($trace_writer_last_error())
  else:
    ct_reader_close(h)
    result.add("")

proc test_meta_dat_versions_other_than_6_are_refused_by_name() =
  for v in [3, 4, 5, 7]:
    let f = TmpDir / ("meta_v" & $v & ".ct")
    writeCurrent(f)
    stamp(f, metaVersion = v)
    for i, e in refusalsOf(f):
      doAssert e.len > 0, "reader door " & $i & " opened a meta.dat v" & $v
      doAssert ("version " & $v) in e and "version 6" in e,
        "door " & $i & " must name version " & $v & " and version 6: " & e
  echo "PASS: test_meta_dat_versions_other_than_6_are_refused_by_name"

proc test_container_versions_other_than_5_are_refused_by_name() =
  for v in [2, 3, 4, 6]:
    let f = TmpDir / ("ctfs_v" & $v & ".ct")
    writeCurrent(f)
    stamp(f, ctfsVersion = v)
    for i, e in refusalsOf(f):
      doAssert e.len > 0, "reader door " & $i & " opened a CTFS v" & $v
      doAssert ("version " & $v) in e and "version 5" in e,
        "door " & $i & " must name version " & $v & " and version 5: " & e
  echo "PASS: test_container_versions_other_than_5_are_refused_by_name"

proc test_the_current_versions_open_at_the_recorded_lines() =
  let f = TmpDir / "current.ct"
  writeCurrent(f)
  for i, e in refusalsOf(f):
    doAssert e.len == 0, "reader door " & $i & " refused a current container: " & e
  var r = openNewTrace(f).get()
  var gli = r.globalPositionSpace()
  doAssert r.stepCount().get() == uint64(Steps.len)
  for i, (p, l) in Steps:
    let pos = r.stepAbsoluteGlobalLineIndex(uint64(i)).get()
    doAssert gli.resolve(pos) == (int(p), l),
      "step " & $i & " read back at " & $gli.resolve(pos)
  echo "PASS: test_the_current_versions_open_at_the_recorded_lines"

proc test_the_container_version_and_the_meta_dat_schema_are_two_gates() =
  ## THE FINDING, pinned: these are two fields and two gates, and the one in
  ## front is not the one that decides.
  ##
  ## It was proposed (CTFS-Compact-Profile CCP-4, finding 7) that this reader
  ## should widen its CONTAINER version set to {3, 4, 5}, on the correct
  ## ground that §1c's rule is "refuse a version you do not IMPLEMENT" and
  ## this library implements the version-3 and version-4 bodies — their
  ## `MapBlock` simply never sets the direct-block tag, so the same code reads
  ## them. The argument about the body is right. The conclusion does not
  ## follow, and this arm is why: a container below version 5 also carries a
  ## `meta.dat` below schema 6, whose body is genuinely different (through
  ## version 5 there is a path list after `recorder_id`; from 6 there is not),
  ## so the second gate refuses it whatever the first one does.
  ##
  ## MEASURED when this arm was written, over 173 real containers in the
  ## workspace: 136 at container version 3 or 4 — every one of them carrying
  ## `meta.dat` schema 3, 4 or 5 or no `meta.dat` at all — and 24 at container
  ## version 5, every one carrying schema 6. Not one counterexample in either
  ## direction. Widening the container gate alone therefore changes the
  ## MESSAGE on all 136 and the OUTCOME on none, and it changes the message
  ## for the worse: a "schema version 3 is not supported" or, for the
  ## `meta.dat`-less ones, a "meta.dat is missing — the bundle is truncated",
  ## in place of a refusal that names the field actually responsible.
  ##
  ## So what this asserts is not the floor but the SHAPE: the two gates refuse
  ## with two different messages naming two different fields, and neither
  ## subsumes the other. If that ever stops holding — if a container below 5
  ## appears carrying schema 6 — the reasoning above has to be re-taken rather
  ## than assumed, and this arm is what makes that visible.
  let older = TmpDir / "ctfs4_schema6.ct"
  writeCurrent(older)
  stamp(older, ctfsVersion = 4)            # schema left at 6
  for i, e in refusalsOf(older):
    doAssert e.len > 0, "door " & $i & " opened a container at version 4"
    doAssert "container version 4" in e,
      "door " & $i & " must refuse on the CONTAINER version, which is the " &
      "field that is wrong, and name it: " & e
    doAssert "schema version" notin e,
      "door " & $i & " blamed the schema for a container-version problem: " & e

  let newer = TmpDir / "ctfs5_schema4.ct"
  writeCurrent(newer)
  stamp(newer, metaVersion = 4)            # container left at 5
  for i, e in refusalsOf(newer):
    doAssert e.len > 0, "door " & $i & " opened a meta.dat at schema 4"
    doAssert "schema version 4" in e,
      "door " & $i & " must refuse on the SCHEMA version and name it: " & e
    doAssert "container version" notin e,
      "door " & $i & " blamed the container version for a schema problem: " & e

  echo "PASS: test_the_container_version_and_the_meta_dat_schema_are_two_gates"

when isMainModule:
  createDir(TmpDir)
  test_meta_dat_versions_other_than_6_are_refused_by_name()
  test_container_versions_other_than_5_are_refused_by_name()
  test_the_current_versions_open_at_the_recorded_lines()
  test_the_container_version_and_the_meta_dat_schema_are_two_gates()
  removeDir(TmpDir)
