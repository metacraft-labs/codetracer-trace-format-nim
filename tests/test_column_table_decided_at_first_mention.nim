## A column-aware file's table is decided by the writer when the file is first
## mentioned.
##
## Spec: ~codetracer-trace-format-spec/internal-files.md~ §"`paths.dat`
## Layout A", "Every Layout A record carries a table of non-zero size, and a
## file's table is fixed when the file is first interned":
##
## * a table whose lines hold nothing gives its first line one position:
##   `[0]` is recorded as `[1]`, `[0, 0]` as `[1, 0]`; any other table is
##   recorded as given;
## * an empty table, or none at all — the path first mentioned by a step, a
##   function or an id request — records the conventional table: 100000 lines
##   of 1024 positions;
## * on a file with the conventional table a column above 1024 is recorded at
##   column 1024 of its line, and a line above 100000 is refused, naming the
##   path. A file with any other table keeps its columns as given;
## * the conventional table is written as `line_count = 0` with no line
##   lengths, its only encoding, whether the writer chose it or the recorder
##   passed it; a reader decodes `0` as the conventional table;
## * a non-empty table offered for a path already interned is refused,
##   naming the path, unless it is the recorded table: the file's size fixed
##   the base of every later file. That includes a table offered after a
##   step, a function or an id request first mentioned the path.
##
## Before this rule an empty table was written as `line_count` 0: the file had
## no positions, shared its base with the next file, and every position in it
## resolved into that file.
##
## No mocks: the containers come from this repository's writer and are read
## back through the real reader.

import std/[os, strutils, assertions, options]
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_ctfs/container

const
  P = "/src/app.py"
  Q = "/src/other.py"

let dir = getTempDir() / "ctfnim-column-table-decided-at-first-mention"
createDir(dir)

proc columnAwareWriter(name: string): MultiStreamTraceWriter =
  var w = initMultiStreamWriter(dir / name & ".build", name).get()
  doAssert w.enableColumnAwareSteps().isOk
  w

proc finish(w: var MultiStreamTraceWriter, name: string): NewTraceReader =
  let closed = w.close()
  doAssert closed.isOk, "close: " & closed.error
  let file = dir / name & ".ct"
  writeFile(file, cast[string](w.toBytes()))
  discard w.closeCtfs()
  let r = openNewTrace(file)
  doAssert r.isOk, "openNewTrace: " & r.error
  r.get()

proc table(r: NewTraceReader, file: uint64): seq[uint32] =
  for i in 0'u32 ..< uint32(r.lineCountRaw(file)):
    result.add(r.lineLengthRaw(file, i).get())

proc isConventional(t: seq[uint32]): bool =
  t.len == 100000 and t[0] == 1024 and t[^1] == 1024 and t[50000] == 1024

proc at(r: var NewTraceReader, n: uint64): (uint64, uint32, uint32) =
  let g = r.stepAbsoluteGlobalLineIndex(n)
  doAssert g.isOk, g.error
  let d = r.decodeGlobalPositionIndex(g.get())
  doAssert d.isOk, d.error
  (d.get().file, d.get().line, d.get().column)

proc test_an_empty_table_records_the_conventional_table() =
  var w = columnAwareWriter("empty_table")
  let id = w.registerPath(P, newSeq[uint32]())
  doAssert id.isOk, id.error
  doAssert w.registerStep(id.get(), 1, @[]).isOk
  var r = w.finish("empty_table")
  let t = r.table(0)
  doAssert t.isConventional,
    "an empty table must be recorded as 100000 lines of 1024 positions; " &
    "the record has " & $t.len & " lines"
  echo "PASS: test_an_empty_table_records_the_conventional_table"

proc test_a_path_first_mentioned_without_a_table_gets_the_conventional_one() =
  ## By an id request, by a step naming it, and by a function declared in
  ## it (written at close).
  var w = columnAwareWriter("implicit")
  doAssert w.registerPath(Q, [5'u32, 5]).isOk
  let byId = w.registerPath(P)
  doAssert byId.isOk, byId.error
  let byStep = w.pathIdForStep("/src/stepped.py")
  doAssert byStep.isOk, byStep.error
  doAssert w.registerStep(byStep.get(), 3, @[]).isOk
  doAssert w.registerFunctionAt("/src/declared.py", 7, "f").isOk
  var r = w.finish("implicit")
  doAssert r.pathCount() == 4
  doAssert r.table(0) == @[5'u32, 5], "a given table is recorded as given"
  for f in 1'u64 .. 3'u64:
    doAssert r.table(f).isConventional,
      "file " & $f & " (" & r.path(f).get() & ") was first mentioned " &
      "without a table and must have the conventional one; it has " &
      $r.lineCountRaw(f) & " lines"
  # File 2 starts after file 0's 10 positions and file 1's 10^8.
  doAssert r.at(0) == (2'u64, 3'u32, 1'u32), $r.at(0)
  echo "PASS: test_a_path_first_mentioned_without_a_table_gets_the_conventional_one"

proc test_a_table_whose_lines_hold_nothing_gives_its_first_line_a_position() =
  var w = columnAwareWriter("all_zero")
  doAssert w.registerPath("/src/empty.py", [0'u32]).isOk
  doAssert w.registerPath("/src/blank.py", [0'u32, 0]).isOk
  doAssert w.registerPath("/src/mixed.py", [0'u32, 3, 0]).isOk
  doAssert w.registerStep(1, 1, @[]).isOk
  doAssert w.registerStep(2, 2, @[]).isOk
  var r = w.finish("all_zero")
  doAssert r.table(0) == @[1'u32], "[0] is recorded as [1]; got " & $r.table(0)
  doAssert r.table(1) == @[1'u32, 0],
    "[0, 0] is recorded as [1, 0], keeping its line count; got " & $r.table(1)
  doAssert r.table(2) == @[0'u32, 3, 0],
    "a table with a non-zero line is recorded as given; got " & $r.table(2)
  doAssert r.at(0) == (1'u64, 1'u32, 1'u32), "step on /src/blank.py: " & $r.at(0)
  doAssert r.at(1) == (2'u64, 2'u32, 1'u32), "step on /src/mixed.py: " & $r.at(1)
  echo "PASS: test_a_table_whose_lines_hold_nothing_gives_its_first_line_a_position"

proc test_the_same_table_again_and_a_bare_lookup_return_the_id() =
  var w = columnAwareWriter("identical")
  let first = w.registerPath(P, [3'u32, 4])
  doAssert first.isOk, first.error
  let again = w.registerPath(P, [3'u32, 4])
  doAssert again.isOk and again.get() == first.get()
  let bare = w.registerPath(P)
  doAssert bare.isOk and bare.get() == first.get()
  let empty = w.registerPath(P, newSeq[uint32]())
  doAssert empty.isOk and empty.get() == first.get(),
    "an empty table for an interned path is a lookup, not a new table"
  var r = w.finish("identical")
  doAssert r.pathCount() == 1 and r.table(0) == @[3'u32, 4]
  echo "PASS: test_the_same_table_again_and_a_bare_lookup_return_the_id"

proc test_columns_and_lines_on_a_conventional_file() =
  var w = columnAwareWriter("conventional")
  let small = w.registerPath(Q, [2000'u32]).get()
  let conv = w.registerPath(P).get()
  # Step 0: column 2000 is recorded at column 1024.
  doAssert w.registerStepWithColumn(conv, 5, 1999, @[]).isOk
  # Steps 1-2: a column move past 1024 stops at 1024.
  doAssert w.registerStepWithColumn(conv, 6, 1000, @[]).isOk
  doAssert w.registerColumnStep(100, @[]).isOk
  # Step 3: the last line is in range; one past it is refused naming the path.
  doAssert w.registerStep(conv, 100000, @[]).isOk
  let past = w.registerStep(conv, 100001, @[])
  doAssert past.isErr and P in past.error and "100001" in past.error,
    "a step past line 100000 of a conventional file must be refused naming " &
    "it: " & (if past.isErr: past.error else: "accepted")
  let pastCol = w.registerStepWithColumn(conv, 100001, 3, @[])
  doAssert pastCol.isErr and P in pastCol.error
  # Step 4: a file with its own table keeps a column past 1024 as given.
  doAssert w.registerStepWithColumn(small, 1, 1499, @[]).isOk
  # Step 5: line 0 is line 1.
  doAssert w.registerStep(conv, 0, @[]).isOk
  var r = w.finish("conventional")
  doAssert r.at(0) == (conv, 5'u32, 1024'u32),
    "column 2000 must be recorded at column 1024 of line 5: " & $r.at(0)
  doAssert r.at(1) == (conv, 6'u32, 1001'u32), $r.at(1)
  doAssert r.at(2) == (conv, 6'u32, 1024'u32),
    "a move of 100 from column 1001 must stop at column 1024: " & $r.at(2)
  doAssert r.at(3) == (conv, 100000'u32, 1'u32), $r.at(3)
  doAssert r.at(4) == (small, 1'u32, 1500'u32),
    "a file with its own table is not clamped: " & $r.at(4)
  doAssert r.at(5) == (conv, 1'u32, 1'u32), $r.at(5)
  echo "PASS: test_columns_and_lines_on_a_conventional_file"

proc test_a_recorder_built_conventional_table_is_the_conventional_table() =
  ## The rule is about the table, not the route: a recorder that builds the
  ## 100000 x 1024 table itself gets the same clamp.
  var w = columnAwareWriter("recorder_built")
  var t = newSeq[uint32](100000)
  for x in t.mitems: x = 1024
  let id = w.registerPath(P, t).get()
  doAssert w.registerStepWithColumn(id, 2, 5000, @[]).isOk
  doAssert w.registerStep(id, 100001, @[]).isErr
  var r = w.finish("recorder_built")
  doAssert r.at(0) == (id, 2'u32, 1024'u32), $r.at(0)
  echo "PASS: test_a_recorder_built_conventional_table_is_the_conventional_table"

proc test_a_later_different_table_is_refused_naming_the_path() =
  for (first, later) in [(@[3'u32, 4], @[3'u32, 5]), (@[3'u32, 4], @[3'u32, 4, 5]),
                         (newSeq[uint32](), @[3'u32, 4])]:
    var w = columnAwareWriter("late")
    doAssert w.registerPath(P, first).isOk
    let res = w.registerPath(P, later)
    doAssert res.isErr, "a table " & $later & " offered after " & P &
      " was interned with " & $first & " must be refused; it returned id " &
      $res.get()
    doAssert P in res.error and "first interned" in res.error, res.error
  echo "PASS: test_a_later_different_table_is_refused_naming_the_path"

proc test_a_table_after_an_implicit_mention_is_refused() =
  ## The Python recorder's ordering until `e573455`: a step interned the
  ## file, then its real table arrived.
  var w = columnAwareWriter("after_step")
  let id = w.pathIdForStep(P)
  doAssert id.isOk
  doAssert w.registerStep(id.get(), 1, @[]).isOk
  let late = w.registerPath(P, [3'u32, 4])
  doAssert late.isErr and P in late.error, "a table after the step that " &
    "interned the file must be refused naming it"
  echo "PASS: test_a_table_after_an_implicit_mention_is_refused"

proc test_a_later_table_equal_to_the_recorded_one_is_accepted() =
  ## Compared after the same normalisation: `[0]` again is `[1]` again, and
  ## the conventional table built by the recorder is the one the writer
  ## chose for an implicit mention.
  var w = columnAwareWriter("late_equal")
  doAssert w.registerPath(P, [0'u32]).isOk
  doAssert w.registerPath(P, [0'u32]).isOk
  doAssert w.registerPath(P, [1'u32]).isOk
  doAssert w.registerPath(Q).isOk
  doAssert w.registerPath(Q, conventionalLineLengths()).isOk
  echo "PASS: test_a_later_table_equal_to_the_recorded_one_is_accepted"

proc pathsDatOf(name: string): seq[byte] =
  let bytes = cast[seq[byte]](readFile(dir / name & ".ct"))
  readInternalFile(bytes, "paths.dat").get()

proc test_the_conventional_table_is_written_as_line_count_zero() =
  ## One byte of table instead of 100000 varints, by either route.
  for (name, table) in [("conv_by_writer", newSeq[uint32]()),
                        ("conv_by_recorder", conventionalLineLengths())]:
    var w = columnAwareWriter(name)
    doAssert w.registerPath(P, table).isOk
    doAssert w.registerStep(0, 3, @[]).isOk
    var r = w.finish(name)
    let dat = pathsDatOf(name)
    var want = @[byte(P.len)]
    for c in P: want.add(byte(c))
    want.add(0'u8)
    doAssert dat == want, name & ": the conventional table must be the " &
      "record `path_len, path, 0`; paths.dat holds " & $dat.len & " bytes"
    doAssert r.table(0).isConventional,
      name & ": line_count 0 must read back as 100000 lines of 1024"
    doAssert r.at(0) == (0'u64, 3'u32, 1'u32), $r.at(0)
    doAssert r.pathTableKind(0) == some(ptkConventional),
      name & ": the reader says the file has the conventional table"
  echo "PASS: test_the_conventional_table_is_written_as_line_count_zero"

proc test_the_reader_names_each_kind_of_path_table() =
  ## `line_count` 0 is the conventional table, never "no table": the kind
  ## tells a caller which a file has without reading 100000 line lengths.
  var w = columnAwareWriter("kinds")
  doAssert w.registerPath(P, [3'u32]).isOk
  doAssert w.registerPath(Q).isOk
  doAssert w.registerStep(0, 1, @[]).isOk
  var r = w.finish("kinds")
  doAssert r.pathTableKind(0) == some(ptkLines)
  doAssert r.pathTableKind(1) == some(ptkConventional)
  doAssert r.pathTableKind(2).isNone, "no such path"
  var lw = initMultiStreamWriter(dir / "kinds_bare.build", "kinds_bare").get()
  doAssert lw.registerPath(P).isOk
  doAssert lw.registerStep(0, 1, @[]).isOk
  var lr = lw.finish("kinds_bare")
  doAssert lr.pathTableKind(0) == some(ptkBare)
  var cw = initMultiStreamWriter(dir / "kinds_count.build", "kinds_count").get()
  doAssert cw.enableLineCountTable().isOk
  doAssert cw.registerPath(P, lineCount = 7).isOk
  doAssert cw.registerStep(0, 1, @[]).isOk
  var cr = cw.finish("kinds_count")
  doAssert cr.pathTableKind(0) == some(ptkLineCount)
  echo "PASS: test_the_reader_names_each_kind_of_path_table"

proc test_a_line_only_writer_ignores_tables() =
  var w = initMultiStreamWriter(dir / "line_only.build", "line_only").get()
  doAssert w.registerPath(P).isOk
  doAssert w.registerPath(P, [3'u32, 4]).isOk
  doAssert w.registerPath(Q, newSeq[uint32]()).isOk
  doAssert w.registerStep(0, 200000, @[]).isOk,
    "a line-only writer has no conventional table to bound a line by"
  echo "PASS: test_a_line_only_writer_ignores_tables"

test_an_empty_table_records_the_conventional_table()
test_a_path_first_mentioned_without_a_table_gets_the_conventional_one()
test_a_table_whose_lines_hold_nothing_gives_its_first_line_a_position()
test_the_same_table_again_and_a_bare_lookup_return_the_id()
test_columns_and_lines_on_a_conventional_file()
test_a_recorder_built_conventional_table_is_the_conventional_table()
test_a_later_different_table_is_refused_naming_the_path()
test_a_table_after_an_implicit_mention_is_refused()
test_a_later_table_equal_to_the_recorded_one_is_accepted()
test_the_conventional_table_is_written_as_line_count_zero()
test_the_reader_names_each_kind_of_path_table()
test_a_line_only_writer_ignores_tables()
