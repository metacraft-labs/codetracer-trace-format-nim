## A column-aware trace may give some of its files a real per-line table and
## leave others to the conventional one, and the two kinds of file are sized
## differently in the global position space. Writer and reader have to size
## them the same way or they disagree about which file a position belongs to.
##
## `registerPath` takes the line lengths as an optional argument, so the mix
## is the ordinary case, not a corner one: a recorder that has line lengths
## for the sources it compiled and none for a dependency it only stepped
## through produces exactly it.
##
## The two sizings:
##
##   * a file WITH a line-length table occupies `sum(line_lengths)`
##     addresses, because a `global_position_index` inside it addresses a
##     column, and
##   * a file given NO table has the conventional table, written as
##     `line_count = 0` (`internal-files.md` §"`paths.dat` Layout A"): 100000
##     lines of 1024 positions, `ConventionalFileSize` addresses, resolved by
##     that rule rather than by a stored table.
##
## Until 2026-10 a `line_count = 0` record meant "no table", was sized
## `DefaultLinesPerFile` by the writer and `0` by the reader, and its
## positions could not carry a column. What is asserted here, and why each
## one can fail:
##
##   1. The writer's encoding of (file 1, line 1) is 20 and of (file 1,
##      line 3) is 20 + 2 * 1024 — read off the container's step stream.
##   2. `readEvents` reports the steps at the files and lines they were
##      registered at. A reader whose file sizing disagrees with the
##      writer's reports them in the wrong file, and returns `ok`.
##   3. The column decode resolves a position in the conventional file by
##      the rule, column included.
##   4. A fully tabled column-aware trace still resolves line AND column
##      exactly.
##
## No mocks: the containers come from this repository's writer and are read
## back through the real reader.

import std/[os, assertions]
import results
import codetracer_trace_types
import codetracer_trace_reader
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/global_line_index

const
  TabledPath = "/src/main.py"
  UntabledPath = "/vendor/dependency.py"
  TabledLineLengths = [10'u32, 10'u32]

let dir = getTempDir() / "ctfnim-mixed-column-aware-position-space"

proc writeMixedTrace(file: string) =
  ## File 0 carries a two-line length table (20 addressable columns in
  ## total); file 1 is given none. Both are stepped through.
  var w = initMultiStreamWriter(file & ".build", "mixed_column_aware").get()
  doAssert w.enableColumnAwareSteps().isOk
  doAssert w.registerPath(TabledPath, TabledLineLengths).isOk
  doAssert w.registerPath(UntabledPath).isOk
  doAssert w.registerStep(0, 1, @[]).isOk
  doAssert w.registerStep(0, 2, @[]).isOk
  doAssert w.registerStep(1, 1, @[]).isOk
  doAssert w.registerStepWithColumn(1, 3, 6, @[]).isOk
  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()
  writeFile(file, cast[string](bytes))

proc writeFullyTabledTrace(file: string) =
  ## Both files tabled.
  var w = initMultiStreamWriter(file & ".build", "fully_tabled_column_aware").get()
  doAssert w.enableColumnAwareSteps().isOk
  doAssert w.registerPath(TabledPath, TabledLineLengths).isOk
  doAssert w.registerPath(UntabledPath, [8'u32, 8'u32, 8'u32]).isOk
  doAssert w.registerStep(0, 2, @[]).isOk
  doAssert w.registerStep(1, 3, @[]).isOk
  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()
  writeFile(file, cast[string](bytes))

# ---------------------------------------------------------------------------

proc test_the_writer_puts_file_one_just_past_file_zeros_capacity() =
  let file = dir / "mixed_arithmetic.ct"
  writeMixedTrace(file)
  var reader = openNewTrace(file).get()
  let g2 = reader.stepAbsoluteGlobalLineIndex(2)
  doAssert g2.isOk and g2.get() == 20'u64,
    "(file 1, line 1) must encode as file 0's capacity, 20; got " & $g2
  let g3 = reader.stepAbsoluteGlobalLineIndex(3)
  doAssert g3.isOk and g3.get() == 20'u64 + 2 * 1024 + 6,
    "(file 1, line 3, column 7) must encode as 20 + 2 * 1024 + 6; got " & $g3
  echo "PASS: test_the_writer_puts_file_one_just_past_file_zeros_capacity"

proc test_reader_reports_the_file_the_step_was_registered_in() =
  let file = dir / "mixed_readevents.ct"
  writeMixedTrace(file)
  var reader = openTrace(file).get()
  doAssert reader.readEvents().isOk
  var located: seq[(uint64, int64)]
  for ev in reader.events:
    if ev.kind == tleStep:
      located.add((uint64(ev.step.pathId), int64(ev.step.line)))
  doAssert located == @[(0'u64, 1'i64), (0'u64, 2'i64), (1'u64, 1'i64),
    (1'u64, 3'i64)], "steps resolved to " & $located
  echo "PASS: test_reader_reports_the_file_the_step_was_registered_in"

proc test_the_conventional_file_resolves_by_the_rule() =
  let file = dir / "mixed_decode.ct"
  writeMixedTrace(file)
  var reader = openNewTrace(file).get()
  doAssert reader.decodeGlobalPositionIndex(20'u64).get() ==
    (file: 1'u64, line: 1'u32, column: 1'u32)
  doAssert reader.decodeGlobalPositionIndex(20'u64 + 2 * 1024 + 6).get() ==
    (file: 1'u64, line: 3'u32, column: 7'u32)
  doAssert reader.decodeGlobalPositionIndex(20'u64 + ConventionalFileSize - 1).get() ==
    (file: 1'u64, line: 100000'u32, column: 1024'u32)
  doAssert reader.decodeGlobalPositionIndex(20'u64 + ConventionalFileSize).isErr,
    "one past the conventional file is past the space"
  doAssert reader.decodeGlobalPositionIndex(10'u64).get() ==
    (file: 0'u64, line: 2'u32, column: 1'u32)
  echo "PASS: test_the_conventional_file_resolves_by_the_rule"

proc test_fully_tabled_trace_still_resolves_line_and_column() =
  let file = dir / "fully_tabled.ct"
  writeFullyTabledTrace(file)
  var reader = openTrace(file).get()
  doAssert reader.readEvents().isOk
  var located: seq[(uint64, int64, bool, int64)]
  for ev in reader.events:
    if ev.kind == tleStep:
      located.add((uint64(ev.step.pathId), int64(ev.step.line),
                   ev.step.hasColumn, int64(ev.step.column)))
  doAssert located == @[(0'u64, 2'i64, true, 1'i64), (1'u64, 3'i64, true, 1'i64)],
    $located
  echo "PASS: test_fully_tabled_trace_still_resolves_line_and_column"

proc test_one_sizing_rule_serves_writer_and_reader() =
  ## A file's slot is the number of positions it has: `sum(line_lengths)`
  ## for a tabled column-aware file, `ConventionalFileSize` for one whose
  ## table is the conventional rule, its recorded line count in a line-only
  ## trace, and `DefaultLinesPerFile` where a line-only trace records none.
  doAssert fileAddressCount(TabledLineLengths) == 20'u64
  doAssert fileAddressCount([], lineCount = 12'u64) == 12'u64
  doAssert fileAddressCount([]) == DefaultLinesPerFile
  doAssert positionSpaceCounts([@TabledLineLengths, newSeq[uint32]()],
    [], 2, columnAware = true) == @[20'u64, ConventionalFileSize]
  doAssert positionSpaceCounts([@TabledLineLengths, newSeq[uint32]()],
    [], 2, columnAware = false) == @[DefaultLinesPerFile, DefaultLinesPerFile]
  doAssert positionSpaceCounts([], [12'u64, 30'u64], 2,
    columnAware = false) == @[12'u64, 30'u64]
  doAssert positionSpaceCounts([], [12'u64], 2, columnAware = false) ==
    @[12'u64, DefaultLinesPerFile]
  echo "PASS: test_one_sizing_rule_serves_writer_and_reader"

when isMainModule:
  removeDir(dir)
  createDir(dir)
  test_one_sizing_rule_serves_writer_and_reader()
  test_the_writer_puts_file_one_just_past_file_zeros_capacity()
  test_reader_reports_the_file_the_step_was_registered_in()
  test_the_conventional_file_resolves_by_the_rule()
  test_fully_tabled_trace_still_resolves_line_and_column()
  removeDir(dir)
  echo "All mixed column-aware position-space tests passed."
