## The interning tables are carried all four or none
## (`internal-files.md` §"Interning Tables").
##
## A container that carries `paths.dat` without `funcs`, `types` and
## `varnames` was committed as a fixture once: a one-off converter created the
## paths table alone, and this library's reader answered the missing tables
## as empty while the Rust readers refused the container. Asserted here:
##
##   * the trace reader (`openNewTraceFromBytes`) and the native-bundle reader
##     (`detectNativeBundle`, which `ct-print` uses for an MCR recording)
##     refuse every strict subset of the eight members, with the message the
##     Rust readers give (pinned literally below; the Rust repository's
##     `interning_table_set_cross_reader` test compares the two readers on the
##     same files);
##   * the writer refuses to finish such a container — `closeCtfs`,
##     `writeCtfsToFile` and `finishCtfs` all return the refusal — while a
##     container with all four tables, with none, or with `markers` alone
##     (an interning table outside the set) finishes.
##
## No mocks: real containers from the real writer, read by the real readers.

when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

import std/[options, os, strutils]
import results
import codetracer_trace_types
import codetracer_ctfs/[types, container, streaming, interning_set]
import codetracer_trace_writer/[interning_table, meta_dat, new_trace_reader]
import native_decoder

const PathsOnly =
  "the container carries paths.dat, paths.off but not funcs.dat, " &
  "funcs.off, types.dat, types.off, varnames.dat, varnames.off: the " &
  "interning tables paths, funcs, types and varnames are carried all four " &
  "or none (internal-files.md \"Interning Tables\")"

proc withMembers(members: openArray[string]): Ctfs =
  ## A container holding exactly `members`, each an empty table half: a
  ## `.off` member holds the single offset 0, as an empty table's does.
  result = createCtfs()
  for m in members:
    var f = result.addFile(m).get()
    if m.endsWith(".off"):
      doAssert result.writeToFile(f, @[0'u8, 0, 0, 0, 0, 0, 0, 0]).isOk

proc pathsOnlyFromTheWriter(): Ctfs =
  ## What the converter did: `initInterningTableWriter(dst, "paths")` and one
  ## path, with nothing else.
  result = createCtfs()
  var it = initInterningTableWriter(result, "paths").get()
  doAssert result.ensureId(it, "/src/main.c").get() == 0

proc readerRefusal(c: Ctfs): string =
  let r = openNewTraceFromBytes(c.toBytes())
  if r.isOk: "" else: r.error

block the_refusal_names_what_is_carried_and_what_is_missing:
  var present: array[8, bool]
  present[0] = true
  present[1] = true
  doAssert interningSetRefusal(present) == PathsOnly
  doAssert interningSetRefusal(default(array[8, bool])) == ""
  var all: array[8, bool]
  for i in 0 ..< 8: all[i] = true
  doAssert interningSetRefusal(all) == ""
  echo "PASS the_refusal_names_what_is_carried_and_what_is_missing"

block the_trace_reader_refuses_paths_without_the_other_three:
  let got = readerRefusal(pathsOnlyFromTheWriter())
  doAssert got == PathsOnly, "got: " & got
  echo "PASS the_trace_reader_refuses_paths_without_the_other_three"

block the_trace_reader_refuses_every_strict_subset:
  # Every non-empty strict subset of the eight members, by bit mask.
  var refused = 0
  for mask in 1 ..< 255:
    var members: seq[string]
    var present: array[8, bool]
    for i, name in InterningTableMembers:
      if (mask and (1 shl i)) != 0:
        members.add name
        present[i] = true
    let got = readerRefusal(withMembers(members))
    doAssert got == interningSetRefusal(present),
      "subset " & members.join(",") & ": got " & got
    inc refused
  doAssert refused == 254
  echo "PASS the_trace_reader_refuses_every_strict_subset (", refused, ")"

block the_trace_reader_opens_all_four_and_none:
  var c = createCtfs()
  discard initTraceInterningTables(c).get()
  let all = openNewTraceFromBytes(c.toBytes())
  doAssert all.isOk, all.error
  let none = openNewTraceFromBytes(createCtfs().toBytes())
  doAssert none.isOk, none.error
  echo "PASS the_trace_reader_opens_all_four_and_none"

proc nativeBundle(tables: openArray[string]): seq[byte] =
  ## A native MCR bundle — MCR `meta.dat` and one thread stream — carrying
  ## the interning tables `tables`, each with one record.
  var c = createCtfs()
  let meta = writeMetaDatToBuffer(
    TraceMetadata(recordingId: "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
      program: "/synthetic/program"),
    mcrFields = some(McrMetaFields(platform: "linux", tickSourceStr: "none",
      tickGranularity: "edge", atomicModeStr: "relaxed")))
  var mf = c.addFile("meta.dat").get()
  doAssert c.writeToFile(mf, meta).isOk
  for t in tables:
    var it = initInterningTableWriter(c, t).get()
    discard c.ensureId(it, "x").get()
  var tf = c.addFile("t00000000000").get()
  doAssert c.writeToFile(tf, @[0'u8]).isOk
  c.toBytes()

block the_native_bundle_reader_refuses_paths_without_the_other_three:
  let got = detectNativeBundle(nativeBundle(["paths"]))
  doAssert got.isErr and got.error == PathsOnly, $got
  let all = detectNativeBundle(nativeBundle(["paths", "funcs", "types",
    "varnames"]))
  doAssert all.isOk, all.error
  let none = detectNativeBundle(nativeBundle([]))
  doAssert none.isOk, none.error
  echo "PASS the_native_bundle_reader_refuses_paths_without_the_other_three"

let dir = getTempDir() / ("interning_table_set_" & $getCurrentProcessId())
createDir(dir)

block the_writer_refuses_to_finish_a_partial_set:
  var a = pathsOnlyFromTheWriter()
  let fin = finishCtfs(a)
  doAssert fin.isErr and fin.error == PathsOnly,
    (if fin.isOk: "finished" else: fin.error)
  let wrote = writeCtfsToFile(a, dir / "partial.ct")
  doAssert wrote.isErr and wrote.error == PathsOnly, $wrote
  doAssert not fileExists(dir / "partial.ct"),
    "a refused container must not be written"
  var s = createCtfsStreaming(dir / "streamed.ct").get()
  var it = initInterningTableWriter(s, "paths").get()
  discard s.ensureId(it, "/src/main.c").get()
  let closed = closeCtfs(s)
  doAssert closed.isErr and closed.error == PathsOnly, $closed
  echo "PASS the_writer_refuses_to_finish_a_partial_set"

block the_writer_refuses_any_strict_subset:
  for members in [@["funcs.dat", "funcs.off"], @["paths.off"],
      @["paths.dat", "paths.off", "funcs.dat", "funcs.off", "types.dat",
        "types.off", "varnames.dat"]]:
    var c = withMembers(members)
    let fin = finishCtfs(c)
    doAssert fin.isErr and fin.error.startsWith("the container carries " &
      members.join(", ") & " but not "), members.join(",") & ": " &
      (if fin.isOk: "finished" else: fin.error)
    doAssert closeCtfs(c).isErr
  echo "PASS the_writer_refuses_any_strict_subset"

block the_writer_finishes_all_four_none_and_markers_alone:
  var all = createCtfs()
  discard initTraceInterningTables(all).get()
  doAssert finishCtfs(all).isOk
  doAssert writeCtfsToFile(all, dir / "all.ct").isOk
  doAssert closeCtfs(all).isOk
  var none = createCtfs()
  doAssert finishCtfs(none).isOk
  doAssert closeCtfs(none).isOk
  var markers = createCtfs()
  var it = initInterningTableWriter(markers, "markers").get()
  discard markers.ensureId(it, "checkpoint").get()
  doAssert finishCtfs(markers).isOk
  doAssert writeCtfsToFile(markers, dir / "markers.ct").isOk
  var s = createCtfsStreaming(dir / "streamed-all.ct").get()
  discard initTraceInterningTables(s).get()
  doAssert closeCtfs(s).isOk
  echo "PASS the_writer_finishes_all_four_none_and_markers_alone"

removeDir(dir)
