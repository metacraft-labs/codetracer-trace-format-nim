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

when isMainModule:
  createDir(TmpDir)
  test_meta_dat_versions_other_than_6_are_refused_by_name()
  test_container_versions_other_than_5_are_refused_by_name()
  test_the_current_versions_open_at_the_recorded_lines()
  removeDir(TmpDir)
