## A column-aware trace may carry per-line length tables for some of its
## files and not for others, and the two kinds of file are sized differently
## in the global position space. Writer and reader have to size them the
## same way or they disagree about which file a position belongs to.
##
## `registerPath` takes the line lengths as an optional argument, so the mix
## is the ordinary case, not a corner one: a recorder that has line lengths
## for the sources it compiled and none for a dependency it only stepped
## through produces exactly it.
##
## The two sizings:
##
##   * a file WITH a line-length table occupies its byte capacity —
##     `sum(line_lengths)` — because a `global_position_index` inside it
##     addresses a column, and
##   * a file WITHOUT one occupies `DefaultLinesPerFile` addresses, because
##     its positions can only address a line.
##
## `multi_stream_writer.rebuildGli` applies both rules. The reader's
## `ensurePositionTables` sized an untabled file `0`, so every file after it
## sat `DefaultLinesPerFile` too low, and the line-only fallback that catches
## a failed column decode rebuilt the space with `DefaultLinesPerFile`
## everywhere — a third layout, agreeing with neither.
##
## What that produced, on the fixture below (file 0 lengths `[10, 10]`, file
## 1 untabled): the writer encodes (file 1, line 1) as 20, file 1's own
## base. The column decode refuses it — file 1 has no line table — and the
## fallback, which sizes file 0 as `DefaultLinesPerFile`, reads 20 as an
## offset into file 0 and answers (file 0, line 21). 20 is INSIDE the
## fallback's space, so `tryResolve` cannot catch it: the guard added for a
## foreign packing only refuses addresses above the top, and this one is
## not.
##
## What is asserted here, and why each one can fail:
##
##   1. The writer's own encoding of (file 1, line 1) is 20 — the arithmetic
##      the rest of the test is about, read off the container's step stream.
##   2. `readEvents` reports the steps at the files and lines they were
##      registered at. A reader whose file sizing disagrees with the
##      writer's reports file 0 line 21 instead, and returns `ok`.
##   3. The column decode, asked for a position in an untabled file, refuses
##      by naming the missing table rather than answering out of the
##      neighbouring file.
##   4. A fully tabled column-aware trace still resolves line AND column
##      exactly. A "fix" that made every position fall back to the line-only
##      space would pass 2 and fail this.
##
## No mocks: the containers come from this repository's writer and are read
## back through the real reader.

import std/[os, strutils, assertions]
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
  ## total); file 1 carries none. Both are stepped through.
  var w = initMultiStreamWriter(file & ".build", "mixed_column_aware").get()
  w.enableColumnAwareSteps()
  doAssert w.registerPath(TabledPath, TabledLineLengths).isOk
  doAssert w.registerPath(UntabledPath).isOk
  doAssert w.registerStep(0, 1, @[]).isOk
  doAssert w.registerStep(0, 2, @[]).isOk
  doAssert w.registerStep(1, 1, @[]).isOk
  doAssert w.registerStep(1, 3, @[]).isOk
  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()
  writeFile(file, cast[string](bytes))

proc writeFullyTabledTrace(file: string) =
  ## Both files tabled — the case the column decode is for.
  var w = initMultiStreamWriter(file & ".build", "fully_tabled_column_aware").get()
  w.enableColumnAwareSteps()
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
  ## THE ARITHMETIC. File 0 has 20 addressable columns, so file 1's base is
  ## 20, and its first line sits at that base: (file 1, line 1) is 20. Read
  ## off the container rather than recomputed, so a change to the writer's
  ## sizing shows up here first.
  let file = dir / "mixed_arithmetic.ct"
  writeMixedTrace(file)

  var readerRes = openNewTrace(file)
  doAssert readerRes.isOk, "openNewTrace failed: " & readerRes.error
  var reader = readerRes.get()

  var expectedFileZeroCapacity: uint64 = 0
  for L in TabledLineLengths:
    expectedFileZeroCapacity += uint64(L)
  doAssert expectedFileZeroCapacity == 20'u64

  let g2 = reader.stepAbsoluteGlobalLineIndex(2)
  doAssert g2.isOk, "step 2 gli: " & g2.error
  doAssert g2.get() == 20'u64,
    "(file 1, line 1) must encode as file 0's capacity (" &
    $expectedFileZeroCapacity & "), which is file 1's own base; the " &
    "container holds " & $g2.get()

  let g3 = reader.stepAbsoluteGlobalLineIndex(3)
  doAssert g3.isOk, "step 3 gli: " & g3.error
  doAssert g3.get() == 22'u64,
    "(file 1, line 3) must encode as 22; the container holds " & $g3.get()

  echo "PASS: test_the_writer_puts_file_one_just_past_file_zeros_capacity"

proc test_reader_reports_the_file_the_step_was_registered_in() =
  ## THE REPRODUCER, end to end. Before the reader sized an untabled file
  ## the way the writer does, step 2 came back as (file 0, line 21) — a file
  ## that has two lines — and step 3 as (file 0, line 23), with no error.
  let file = dir / "mixed_readevents.ct"
  writeMixedTrace(file)

  var readerRes = openTrace(file)
  doAssert readerRes.isOk, "openTrace failed"
  var reader = readerRes.get()

  let res = reader.readEvents()
  doAssert res.isOk, "readEvents failed: " & res.error

  var located: seq[(uint64, int64)]
  for ev in reader.events:
    if ev.kind == tleStep:
      located.add((uint64(ev.step.pathId), int64(ev.step.line)))

  doAssert located.len == 4,
    "expected 4 steps, got " & $located.len & ": " & $located
  doAssert located[0] == (0'u64, 1'i64), "step 0: " & $located[0]
  doAssert located[1] == (0'u64, 2'i64), "step 1: " & $located[1]
  doAssert located[2] == (1'u64, 1'i64),
    "step 2 was registered at (file 1, line 1); the reader says " &
    $located[2] & ". (file 0, line 21) is the untabled file's base read as " &
    "a line of the file before it."
  doAssert located[3] == (1'u64, 3'i64),
    "step 3 was registered at (file 1, line 3); the reader says " & $located[3]

  echo "PASS: test_reader_reports_the_file_the_step_was_registered_in"

proc test_untabled_file_refuses_a_column_by_name() =
  ## The column decode has nothing to say about a file with no line table,
  ## and must say THAT rather than resolve into the neighbouring file. The
  ## refusal is what routes the position to the line-only fallback.
  let file = dir / "mixed_decode.ct"
  writeMixedTrace(file)

  var readerRes = openNewTrace(file)
  doAssert readerRes.isOk, "openNewTrace failed: " & readerRes.error
  var reader = readerRes.get()

  let decoded = reader.decodeGlobalPositionIndex(20'u64)
  doAssert decoded.isErr,
    "20 is in file 1, which has no line-length table; the decoder answered " &
    $decoded.get()
  doAssert "file 1" in decoded.error,
    "the refusal must name the file it landed in: " & decoded.error

  # And the tabled file still decodes, so the refusal is about the missing
  # table and not about the decoder being switched off.
  let ok0 = reader.decodeGlobalPositionIndex(10'u64)
  doAssert ok0.isOk, "10 is column 1 of line 2 of file 0: " & ok0.error
  doAssert ok0.get() == (file: 0'u64, line: 2'u32, column: 1'u32),
    "10 must decode to (file 0, line 2, column 1); got " & $ok0.get()

  echo "PASS: test_untabled_file_refuses_a_column_by_name"

proc test_fully_tabled_trace_still_resolves_line_and_column() =
  ## THE OTHER HALF. Sizing untabled files like the writer must not cost the
  ## tabled ones their columns — a reader that answered everything from the
  ## line-only space would pass the reproducer and fail here.
  let file = dir / "fully_tabled.ct"
  writeFullyTabledTrace(file)

  var readerRes = openTrace(file)
  doAssert readerRes.isOk, "openTrace failed"
  var reader = readerRes.get()
  doAssert reader.readEvents().isOk

  var located: seq[(uint64, int64, bool, int64)]
  for ev in reader.events:
    if ev.kind == tleStep:
      located.add((uint64(ev.step.pathId), int64(ev.step.line),
                   ev.step.hasColumn, int64(ev.step.column)))

  doAssert located.len == 2, "expected 2 steps, got " & $located
  doAssert located[0] == (0'u64, 2'i64, true, 1'i64), "step 0: " & $located[0]
  doAssert located[1] == (1'u64, 3'i64, true, 1'i64), "step 1: " & $located[1]

  echo "PASS: test_fully_tabled_trace_still_resolves_line_and_column"

proc test_one_sizing_rule_serves_writer_and_reader() =
  ## The rule itself, stated once: a file's slot is the number of
  ## positions it has. What a position is differs by mode — an
  ## addressable column where the file has a per-line table, a line where
  ## it has a recorded line count — and a file the trace sizes neither
  ## way occupies `DefaultLinesPerFile`. Both parties call this, so a
  ## change to either has to change this line.
  doAssert fileAddressCount(TabledLineLengths) == 20'u64,
    "a tabled file occupies sum(line_lengths); got " &
    $fileAddressCount(TabledLineLengths)
  doAssert fileAddressCount([], lineCount = 12'u64) == 12'u64,
    "a file with a recorded line count occupies that many addresses; got " &
    $fileAddressCount([], lineCount = 12'u64)
  doAssert fileAddressCount([]) == DefaultLinesPerFile,
    "a file the trace sizes neither way occupies DefaultLinesPerFile; got " &
    $fileAddressCount([])

  # The per-line table wins where both are present: a column-aware file
  # is addressed in columns, and its line count is the table's length
  # rather than its size.
  doAssert fileAddressCount(TabledLineLengths, lineCount = 12'u64) == 20'u64,
    "a tabled file is sized in columns even when a line count is also " &
    "supplied; got " & $fileAddressCount(TabledLineLengths, lineCount = 12'u64)

  let mixed = positionSpaceCounts([@TabledLineLengths, newSeq[uint32]()],
    [], 2, columnAware = true)
  doAssert mixed == @[20'u64, DefaultLinesPerFile],
    "mixed column-aware space: " & $mixed

  let lineOnly = positionSpaceCounts([@TabledLineLengths, newSeq[uint32]()],
    [], 2, columnAware = false)
  doAssert lineOnly == @[DefaultLinesPerFile, DefaultLinesPerFile],
    "a line-only trace with no recorded counts ignores any line tables " &
    "lying around: " & $lineOnly

  let counted = positionSpaceCounts([], [12'u64, 30'u64], 2,
    columnAware = false)
  doAssert counted == @[12'u64, 30'u64],
    "a line-only trace with recorded counts is sized by them: " & $counted

  let shortCounts = positionSpaceCounts([], [12'u64], 2, columnAware = false)
  doAssert shortCounts == @[12'u64, DefaultLinesPerFile],
    "a file the counts do not cover falls back to the default: " &
    $shortCounts

  echo "PASS: test_one_sizing_rule_serves_writer_and_reader"

when isMainModule:
  removeDir(dir)
  createDir(dir)
  test_one_sizing_rule_serves_writer_and_reader()
  test_the_writer_puts_file_one_just_past_file_zeros_capacity()
  test_reader_reports_the_file_the_step_was_registered_in()
  test_untabled_file_refuses_a_column_by_name()
  test_fully_tabled_trace_still_resolves_line_and_column()
  removeDir(dir)
  echo "All mixed column-aware position-space tests passed."
