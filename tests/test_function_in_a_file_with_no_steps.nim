## A function declared in a file no step ever visits is written at close.
##
## `close()` flushes `funcs.dat` last, because a record's
## `global_line_index` needs the finished position space. A declaration path
## that nothing else interned is interned there, and the space the addresses
## are computed in must include it. Sizing the space first and interning the
## path afterwards indexes past the end of the space — an `IndexDefect` inside
## `close()`, which is what a BEAM recording of a request handler whose module
## never stepped (`nested.ex`) hit.
##
## Asserted, in the line-only and the column-aware layout, with ONE and with
## TWO such files (the second is the one that falls off the end of a space
## sized before either was interned):
##
##   * `close()` succeeds;
##   * each function reads back with the address of its declaration line in
##     its own file's slot, laid out after the files the steps registered;
##   * `meta.dat`'s path list and `paths.dat` agree on every file.
##
## Under the line-count table a declaration path with no recorded size cannot
## be laid out at all, and `close()` refuses it by name instead.
##
## No mocks: a real writer writes a real container, read back by this
## repository's reader.

import std/[os, strutils]
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/global_line_index

proc write(columnAware: bool, stepless: seq[string]): seq[byte] =
  var w = initMultiStreamWriter(getTempDir() / "fn_no_steps.ct", "fn_no_steps").get()
  if columnAware:
    doAssert w.enableColumnAwareSteps().isOk
  doAssert w.registerPath("/src/main.ex", [8'u32, 8]).isOk
  doAssert w.registerFunctionAt("/src/main.ex", 2, "main").isOk
  for i, p in stepless:
    doAssert w.registerFunctionAt(p, uint64(3 + i), "f" & $i).isOk
  doAssert w.registerStep(0, 1, @[]).isOk
  let closed = w.close()
  doAssert closed.isOk, "close() must succeed with " & $stepless.len &
    " function file(s) no step visited (columnAware=" & $columnAware &
    "); got: " & closed.error
  result = w.toBytes()
  w.closeCtfs()

proc check(columnAware: bool, stepless: seq[string]) =
  var r = openNewTraceFromBytes(write(columnAware, stepless)).get()
  doAssert r.pathCount() == uint64(1 + stepless.len),
    "every declaration file is a paths.dat record; got " & $r.pathCount()
  for i, p in stepless:
    doAssert r.path(uint64(1 + i)).get() == p
    let rec = r.functionRecord(uint64(1 + i)).get()
    # funcs.dat addresses are LINE addresses in both layouts, one
    # DefaultLinesPerFile slot per file (none of these files records a size).
    let want = uint64(1 + i) * DefaultLinesPerFile + uint64(3 + i) - 1
    doAssert rec.globalLineIndex == want,
      "f" & $i & " must be at line " & $(3 + i) & " of file " & $(1 + i) &
      " (address " & $want & "); got " & $rec.globalLineIndex
  echo "PASS: columnAware=", columnAware, " stepless files=", stepless.len

proc test_under_the_line_count_table_it_is_refused_by_name() =
  var w = initMultiStreamWriter(getTempDir() / "fn_no_steps_lct.ct", "lct").get()
  doAssert w.enableLineCountTable().isOk
  doAssert w.registerPath("/src/main.ex", lineCount = 10).isOk
  doAssert w.registerFunctionAt("/src/nested.ex", 3, "f").isOk
  doAssert w.registerStep(0, 1, @[]).isOk
  let closed = w.close()
  doAssert closed.isErr,
    "a declaration path with no recorded line count cannot be laid out " &
    "under the line-count table; close() must refuse it"
  doAssert "/src/nested.ex" in closed.error,
    "the refusal must name the path; got: " & closed.error
  w.closeCtfs()
  echo "PASS: test_under_the_line_count_table_it_is_refused_by_name"

for columnAware in [false, true]:
  check(columnAware, @["/src/nested.ex"])
  check(columnAware, @["/src/nested.ex", "/src/router.ex"])
test_under_the_line_count_table_it_is_refused_by_name()
echo "ALL PASS: test_function_in_a_file_with_no_steps"
