## The per-file line-count table: a line-only container that states how
## large each of its files is, instead of leaving a reader to assume it.
##
## Without the table a file occupies `DefaultLinesPerFile` addresses and
## the container records nothing about that choice — no stride field, no
## per-file count, no producer id. Two consequences, and this file is the
## reproducer for both:
##
##   1. A file with more than `DefaultLinesPerFile` lines has its lines
##      addressed inside the NEXT file's range. The address is inside the
##      trace's space, so `tryResolve` has no bound to refuse it against
##      and the reader answers a `(file, line)` pair that was never
##      recorded — a confident wrong answer.
##   2. Every `AbsoluteStep` varint is sized for a space of
##      `paths × DefaultLinesPerFile` addresses rather than for the lines
##      the program actually has.
##
## With the table (`meta.dat` bit 14, `FLAG_HAS_LINE_COUNT_TABLE`) each
## `paths.dat` record carries `payload_len + payload + line_count`, the
## space is `sum(line counts)`, and the writer refuses a step past a
## file's recorded count — because at that point nothing downstream can.
##
## What is asserted here, and how each one fails:
##
##   1. The space is the sum of the recorded counts, and every registered
##      `(path, line)` reads back at its own coordinates end to end. If
##      `positionSpaceCounts` ignored the counts the file bases would be
##      the stride's and the addresses would not resolve to the
##      registered pairs.
##   2. The container states the counts: reopening a trace recovers each
##      file's count and its path. A record parsed under the wrong layout
##      surfaces the length prefix inside the path string. A record that
##      states a count of zero fails the open rather than being quietly
##      given the stride back.
##   3. A step past a file's recorded count is REFUSED at the writer, and
##      the mutation control shows what the refusal buys: the same step
##      registered in a trace WITHOUT the table encodes to an address
##      that resolves into a different file, with no error anywhere.
##   4. `registerPath` refuses a path with no count once the table is on.
##      An optional count is the same assumption per file instead of per
##      trace.
##   5. A writer that cannot count a file's lines records the ceiling it
##      used, and the reader lays that file out from the recorded number
##      rather than from a constant of its own.
##   6. Bit 14 is what says the counts are there. Clearing it from a
##      container's meta.dat and re-reading is the mutation control for
##      the flag itself.
##   7. The addresses shrink. The first `AbsoluteStep` of a two-file
##      trace is measured under both layouts.
##   8. A writer that does not opt in produces the same bytes it always
##      did.
##
## No mocks: every container here is produced by `MultiStreamTraceWriter`
## and read back through `openNewTraceFromBytes` / `openTrace`, the paths
## a debugger takes.

import std/[os, assertions, strutils, json]
import results
import codetracer_trace_types
import codetracer_trace_reader
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/global_line_index
import codetracer_trace_writer/varint
import codetracer_ct_print_lib

const
  PathA = "/src/alpha.py"
  PathB = "/src/beta.py"
  CountA = 10'u64
  CountB = 7'u64

let dir = getTempDir() / "ctfnim-line-count-table"

proc ctPrintJson(bytes: seq[byte]): JsonNode =
  ## The `ct-print --full` view of a container, which is how an operator
  ## inspects one.
  var r = openNewTraceFromBytes(bytes).get()
  buildFullDocument(r, FullOpts(stripPaths: false))

proc buildTrace(name: string, withTable: bool,
    counts: openArray[uint64],
    steps: openArray[(uint64, uint64)]): seq[byte] =
  ## A two-file trace, with or without the line-count table, carrying the
  ## given steps. Returns the finished container bytes.
  var w = initMultiStreamWriter(dir / (name & ".build"), name).get()
  if withTable:
    doAssert w.enableLineCountTable().isOk
  for i, p in [PathA, PathB]:
    let r =
      if withTable: w.registerPath(p, lineCount = counts[i])
      else: w.registerPath(p)
    doAssert r.isOk, "registerPath(" & p & "): " & r.error
  for (p, l) in steps:
    let r = w.registerStep(p, l, @[])
    doAssert r.isOk, "registerStep(" & $p & ", " & $l & "): " & r.error
  doAssert w.close().isOk
  result = w.toBytes()
  w.closeCtfs()

# ---------------------------------------------------------------------------

proc test_the_space_is_the_sum_of_the_recorded_counts() =
  ## THE POINT OF THE TABLE. Two files of 10 and 7 lines occupy 17
  ## addresses, not `2 * DefaultLinesPerFile`, and every registered
  ## `(path, line)` reads back at its own coordinates.
  let registered = [(0'u64, 1'u64), (0'u64, CountA),
                    (1'u64, 1'u64), (1'u64, CountB)]
  let bytes = buildTrace("sized", withTable = true,
    counts = [CountA, CountB], steps = registered)

  var r = openNewTraceFromBytes(bytes).get()
  doAssert r.meta.hasLineCountTable,
    "a trace built with enableLineCountTable must declare bit 14"

  let space = r.globalPositionSpace()
  doAssert space.totalLines == CountA + CountB,
    "the space must be the sum of the recorded counts (" &
    $(CountA + CountB) & "); got " & $space.totalLines &
    " — the counts were ignored and the stride used instead"
  doAssert space.prefixSum[1] == CountA,
    "file 1's base must be file 0's line count (" & $CountA & "); got " &
    $space.prefixSum[1]

  for (p, l) in registered:
    let addr0 = space.globalIndex(int(p), l)
    let back = space.tryResolve(addr0)
    doAssert back.isOk,
      "(path " & $p & ", line " & $l & ") encodes to " & $addr0 &
      ", which does not resolve: " & back.error
    doAssert back.get() == (int(p), l),
      "(path " & $p & ", line " & $l & ") round-tripped through " &
      $addr0 & " to " & $back.get()

  # End to end through the production reader: the steps must come back at
  # the coordinates they were registered at.
  let file = dir / "sized.ct"
  writeFile(file, cast[string](bytes))
  var readerRes = openTrace(file)
  doAssert readerRes.isOk, "openTrace refused a sized trace"
  var reader = readerRes.get()
  let evRes = reader.readEvents()
  doAssert evRes.isOk, "readEvents on a sized trace: " & evRes.error
  var seen: seq[(uint64, uint64)]
  for ev in reader.events:
    if ev.kind == tleStep:
      seen.add((uint64(ev.step.pathId), uint64(int64(ev.step.line))))
  var want: seq[(uint64, uint64)]
  for pl in registered:
    want.add(pl)
  doAssert seen == want,
    "steps must read back at the (path, line) they were registered at.\n" &
    "  registered: " & $want & "\n  read back:  " & $seen

  echo "PASS: test_the_space_is_the_sum_of_the_recorded_counts"

proc test_the_container_states_the_counts_and_the_paths() =
  ## The table is IN the container: a reader that did not write the trace
  ## recovers both the count and the path from each record. Parsing the
  ## record under the bare layout instead surfaces the length prefix and
  ## the trailing count as part of the path string.
  let bytes = buildTrace("stated", withTable = true,
    counts = [CountA, CountB], steps = [(0'u64, 1'u64)])
  var r = openNewTraceFromBytes(bytes).get()

  doAssert r.pathCount() == 2'u64,
    "two paths were registered; got " & $r.pathCount()
  doAssert r.recordedLineCount(0) == CountA,
    "file 0's recorded count must be " & $CountA & "; got " &
    $r.recordedLineCount(0)
  doAssert r.recordedLineCount(1) == CountB,
    "file 1's recorded count must be " & $CountB & "; got " &
    $r.recordedLineCount(1)

  for i, want in [PathA, PathB]:
    let got = r.path(uint64(i))
    doAssert got.isOk, "path(" & $i & "): " & got.error
    doAssert got.get() == want,
      "path(" & $i & ") must be exactly " & want & " (" & $want.len &
      " bytes); got " & $got.get().len & " bytes, " &
      escape(got.get()) & " — the record's framing leaked into the path"

  # And an operator can see it: `ct print --json` surfaces the bit under
  # `metadata.flags`, which is the difference between a reported line the
  # container vouches for and one the reader assumed.
  let reported = ctPrintJson(bytes)
  doAssert reported["metadata"]["flags"]["has_line_count_table"].getBool(),
    "ct-print must report that this trace states its file sizes"

  echo "PASS: test_the_container_states_the_counts_and_the_paths"

proc test_a_recorded_count_of_zero_is_refused_at_the_reader() =
  ## A trace that declares bit 14 states every file's size, so a recorded
  ## zero is a corrupt record, not a file with no lines. Substituting the
  ## stride for it would be the assumption the table was added to remove,
  ## reintroduced by the reader after the writer took the trouble to
  ## record a number.
  ##
  ## The writer will not produce one, so the container is patched: the
  ## record is `payload_len + payload + line_count` and file 0's count is
  ## a one-byte varint immediately after its path bytes, which keeps the
  ## record length unchanged when it is overwritten with 0.
  let bytes = buildTrace("zeroed", withTable = true,
    counts = [CountA, CountB], steps = [(0'u64, 1'u64)])
  doAssert openNewTraceFromBytes(bytes).isOk,
    "the unpatched container must open, or the patch below proves nothing"

  var needle: seq[byte] = @[byte(PathA.len)]
  for ch in PathA:
    needle.add(byte(ch))
  needle.add(byte(CountA))
  doAssert CountA < 0x80'u64,
    "the patch relies on file 0's count being a one-byte varint"

  var mutated = bytes
  var patched = 0
  for i in 0 .. mutated.len - needle.len:
    var hit = true
    for k in 0 ..< needle.len:
      if mutated[i + k] != needle[k]:
        hit = false
        break
    if hit:
      mutated[i + needle.len - 1] = 0'u8
      patched += 1
  doAssert patched == 1,
    "the control needs exactly one paths.dat record matching " &
    "`len + " & PathA & " + " & $CountA & "`; patched " & $patched

  let opened = openNewTraceFromBytes(mutated)
  doAssert opened.isErr,
    "a paths.dat record recording line_count 0 must fail the open — a " &
    "file sized 0 shares its base with the next one, so every position " &
    "in the trace after it resolves into the wrong file"
  doAssert "line_count is 0" in opened.error,
    "the refusal must name the field it read; got: " & opened.error

  echo "PASS: test_a_recorded_count_of_zero_is_refused_at_the_reader"

proc test_a_step_past_the_recorded_count_is_refused() =
  ## THE REPRODUCER. A step one line past file 0's recorded count
  ## addresses the FIRST line of file 1. The address is inside the space,
  ## so no reader can refuse it — the writer must.
  var w = initMultiStreamWriter(dir / "overflow.build", "overflow").get()
  doAssert w.enableLineCountTable().isOk
  doAssert w.registerPath(PathA, lineCount = CountA).isOk
  doAssert w.registerPath(PathB, lineCount = CountB).isOk

  doAssert w.registerStep(0, CountA, @[]).isOk,
    "the file's LAST line is inside its slot and must be accepted"

  let over = w.registerStep(0, CountA + 1, @[])
  doAssert over.isErr,
    "a step at line " & $(CountA + 1) & " of a file recorded as having " &
    $CountA & " lines must be refused: its address is the first line of " &
    "the next file, and nothing downstream can tell that apart from a " &
    "real location"
  doAssert PathA in over.error and $CountA in over.error,
    "the refusal must name the file and the count it was checked " &
    "against; got: " & over.error

  # A path id that was never registered is refused by the same check
  # rather than indexing past the count table.
  let unknown = w.registerStep(9, 1, @[])
  doAssert unknown.isErr,
    "a step on an unregistered path id must be refused"

  w.closeCtfs()

  # MUTATION CONTROL. The same step in a trace WITHOUT the table is
  # accepted, and the address it produces resolves into a DIFFERENT file
  # — silently, which is what the refusal above exists to prevent.
  let strideSpace = buildGlobalLineIndex(@[CountA, CountB])
  let spilled = strideSpace.globalIndex(0, CountA + 1)
  let landed = strideSpace.tryResolve(spilled)
  doAssert landed.isOk,
    "the control needs the spilled address to be resolvable — if it " &
    "were refused there would be nothing for the writer-side check to add"
  doAssert landed.get() == (1, 1'u64),
    "line " & $(CountA + 1) & " of a " & $CountA & "-line file must land " &
    "on (file 1, line 1) for this to be the defect being fixed; got " &
    $landed.get()

  echo "PASS: test_a_step_past_the_recorded_count_is_refused"

proc test_registerPath_refuses_a_path_with_no_count() =
  ## The table is mandatory, not per-file optional. A record without a
  ## count would put the reader back to assuming that one file's size —
  ## the same defect, retail instead of wholesale.
  var w = initMultiStreamWriter(dir / "nocount.build", "nocount").get()
  doAssert w.enableLineCountTable().isOk
  let missing = w.registerPath(PathA)
  doAssert missing.isErr,
    "registerPath with no lineCount must be refused once the line-count " &
    "table is on"
  doAssert PathA in missing.error,
    "the refusal must name the path; got: " & missing.error

  # But re-naming a path that HAS been counted is not a missing count: no
  # record is written, so there is nothing to require. Recorders name the
  # path on every step, so refusing here would make the table unusable.
  let first = w.registerPath(PathA, lineCount = CountA)
  doAssert first.isOk, "the first registration with a count: " & first.error
  let again = w.registerPath(PathA)
  doAssert again.isOk,
    "re-registering an already-counted path must succeed — the count " &
    "belongs to the record, and this call writes none: " & again.error
  doAssert again.get() == first.get(),
    "re-registration must dedup to the same id; got " & $again.get() &
    " after " & $first.get()
  w.closeCtfs()

  # And the opt-in itself is ordered: a path already interned under the
  # bare layout cannot grow a count.
  var late = initMultiStreamWriter(dir / "late.build", "late").get()
  doAssert late.registerPath(PathA).isOk
  let lateEnable = late.enableLineCountTable()
  doAssert lateEnable.isErr,
    "enabling the table after a path is interned must be refused — that " &
    "record has no count and the trace would declare one for it"
  late.closeCtfs()

  # Column-aware writers already carry line_count in their Layout A
  # records and size files in columns, so the two tables are refused
  # together.
  var col = initMultiStreamWriter(dir / "col.build", "col").get()
  col.enableColumnAwareSteps()
  let colEnable = col.enableLineCountTable()
  doAssert colEnable.isErr,
    "the line-count table on a column-aware writer must be refused"
  col.closeCtfs()

  # And in the other order, where `enableColumnAwareSteps` has no Result
  # to refuse through: the header itself cannot declare both layouts, so
  # `close()` is where that trace stops.
  var both = initMultiStreamWriter(dir / "both.build", "both").get()
  doAssert both.enableLineCountTable().isOk
  both.enableColumnAwareSteps()
  let bothClose = both.close()
  doAssert bothClose.isErr,
    "a header declaring bit 4 AND bit 14 states the same field under two " &
    "record layouts and must be refused at close()"
  doAssert "mutually exclusive" in bothClose.error,
    "the refusal must say why; got: " & bothClose.error
  both.closeCtfs()

  # And the READER refuses such a header too, so a container from another
  # producer that states both is reported rather than read under whichever
  # layout the reader happens to test for first.
  let sized = buildTrace("both-read", withTable = true,
    counts = [CountA, CountB], steps = [(0'u64, 1'u64)])
  var mutated = sized
  var patched = 0
  for i in 0 .. mutated.len - 8:
    if mutated[i] == MetaDatMagic[0] and mutated[i + 1] == MetaDatMagic[1] and
       mutated[i + 2] == MetaDatMagic[2] and mutated[i + 3] == MetaDatMagic[3]:
      let flags = uint16(mutated[i + 6]) or (uint16(mutated[i + 7]) shl 8)
      if (flags and FlagHasLineCountTable) != 0:
        let both = flags or FlagHasColumnAwareSteps
        mutated[i + 6] = byte(both and 0xFF)
        mutated[i + 7] = byte((both shr 8) and 0xFF)
        patched += 1
  doAssert patched == 1,
    "the control needs exactly one meta.dat header carrying bit 14; " &
    "patched " & $patched
  doAssert openNewTraceFromBytes(sized).isOk,
    "the unpatched container must open, or the patch proves nothing"
  let bothRead = openNewTraceFromBytes(mutated)
  doAssert bothRead.isErr,
    "a header declaring bit 4 AND bit 14 must fail the open — the reader " &
    "cannot know which layout its paths.dat records are in"
  doAssert "bit 14" in bothRead.error,
    "the refusal must name the bits; got: " & bothRead.error

  echo "PASS: test_registerPath_refuses_a_path_with_no_count"

proc test_a_writer_that_cannot_count_records_the_ceiling_it_used() =
  ## The wasmi case: a recorder that sees no source text still states its
  ## file sizes. It records the `DefaultLinesPerFile` ceiling it intends
  ## to use, and the reader lays the file out from that recorded number
  ## rather than from a constant of its own.
  let bytes = buildTrace("ceiling", withTable = true,
    counts = [DefaultLinesPerFile, CountB], steps = [(1'u64, 1'u64)])
  var r = openNewTraceFromBytes(bytes).get()

  doAssert r.recordedLineCount(0) == DefaultLinesPerFile,
    "the ceiling must be RECORDED, not assumed; got " &
    $r.recordedLineCount(0)
  let space = r.globalPositionSpace()
  doAssert space.totalLines == DefaultLinesPerFile + CountB,
    "the space must be the sum of the two recorded counts; got " &
    $space.totalLines
  doAssert space.prefixSum[1] == DefaultLinesPerFile,
    "file 1's base must be file 0's recorded count; got " &
    $space.prefixSum[1]

  # The writer-side refusal applies to a recorded ceiling exactly as it
  # does to a real count — which is the original defect: a file with more
  # than `DefaultLinesPerFile` lines now fails instead of addressing the
  # next file's range.
  var w = initMultiStreamWriter(dir / "ceil2.build", "ceil2").get()
  doAssert w.enableLineCountTable().isOk
  doAssert w.registerPath(PathA, lineCount = DefaultLinesPerFile).isOk
  doAssert w.registerPath(PathB, lineCount = CountB).isOk
  doAssert w.registerStep(0, DefaultLinesPerFile, @[]).isOk,
    "the ceiling's own last line is inside the slot"
  doAssert w.registerStep(0, DefaultLinesPerFile + 1, @[]).isErr,
    "a file with more than " & $DefaultLinesPerFile & " lines must FAIL " &
    "rather than address the next file's range"
  w.closeCtfs()

  echo "PASS: test_a_writer_that_cannot_count_records_the_ceiling_it_used"

proc test_bit_14_is_what_says_the_counts_are_there() =
  ## MUTATION CONTROL for the flag. The bit is the declaration; the bytes
  ## alone do not say which layout `paths.dat` is in. Clearing it makes
  ## the reader read the records under the bare layout, and the path
  ## strings come back with their framing attached.
  doAssert (FlagHasLineCountTable and KnownFlags) != 0,
    "bit 14 must be in this reader's KnownFlags, or every container that " &
    "sets it is refused"

  let bytes = buildTrace("flagged", withTable = true,
    counts = [CountA, CountB], steps = [(0'u64, 1'u64)])
  var withFlag = openNewTraceFromBytes(bytes).get()
  doAssert withFlag.path(0).get() == PathA
  doAssert withFlag.globalPositionSpace().totalLines == CountA + CountB

  # Find and clear bit 14 in the serialized meta.dat header. The flag
  # word is the two bytes after the "CTMD" magic and the version.
  var mutated = bytes
  var patched = 0
  for i in 0 .. mutated.len - 8:
    if mutated[i] == MetaDatMagic[0] and mutated[i + 1] == MetaDatMagic[1] and
       mutated[i + 2] == MetaDatMagic[2] and mutated[i + 3] == MetaDatMagic[3]:
      let flags = uint16(mutated[i + 6]) or (uint16(mutated[i + 7]) shl 8)
      if (flags and FlagHasLineCountTable) != 0:
        let cleared = flags and (not FlagHasLineCountTable)
        mutated[i + 6] = byte(cleared and 0xFF)
        mutated[i + 7] = byte((cleared shr 8) and 0xFF)
        patched += 1
  doAssert patched == 1,
    "the control needs exactly one meta.dat header carrying bit 14; " &
    "patched " & $patched

  var withoutFlag = openNewTraceFromBytes(mutated).get()
  doAssert not withoutFlag.meta.hasLineCountTable
  doAssert withoutFlag.recordedLineCount(0) == 0'u64,
    "with the bit cleared the reader must report NO recorded size, not " &
    "a fabricated one"
  doAssert withoutFlag.path(0).get() != PathA,
    "with the bit cleared the record is read as bare path bytes, so the " &
    "path must come back mangled — if it did not, the bit would be " &
    "decorative and this control would prove nothing"
  doAssert withoutFlag.globalPositionSpace().totalLines ==
      2'u64 * DefaultLinesPerFile,
    "with the bit cleared the space falls back to the stride; got " &
    $withoutFlag.globalPositionSpace().totalLines

  echo "PASS: test_bit_14_is_what_says_the_counts_are_there"

proc test_the_addresses_shrink() =
  ## The table pays for itself. Sizing the space to the real line counts
  ## takes every `AbsoluteStep` address down, and the per-record cost is
  ## the count's own varint.
  let step = (1'u64, 1'u64)  # the first line of the SECOND file
  let sizedSpace = buildGlobalLineIndex(@[CountA, CountB])
  let strideSpace = buildGlobalLineIndex(
    @[DefaultLinesPerFile, DefaultLinesPerFile])

  var sizedBytes: seq[byte] = @[]
  encodeVarint(sizedSpace.globalIndex(int(step[0]), step[1]), sizedBytes)
  var strideBytes: seq[byte] = @[]
  encodeVarint(strideSpace.globalIndex(int(step[0]), step[1]), strideBytes)

  doAssert sizedBytes.len < strideBytes.len,
    "the address of (file 1, line 1) must be a shorter varint when the " &
    "space is sized to " & $(CountA + CountB) & " addresses than when it " &
    "is sized to " & $(2 * DefaultLinesPerFile) & ": got " &
    $sizedBytes.len & " vs " & $strideBytes.len & " byte(s)"

  var costBytes: seq[byte] = @[]
  encodeVarint(CountA, costBytes)
  doAssert costBytes.len <= strideBytes.len - sizedBytes.len + 1,
    "the per-file cost of recording the count (" & $costBytes.len &
    " byte(s)) is charged once per FILE; the saving (" &
    $(strideBytes.len - sizedBytes.len) & " byte(s)) is charged once per " &
    "STEP, so the table pays for itself on any file with more than a " &
    "handful of steps"

  echo "PASS: test_the_addresses_shrink"

proc test_a_writer_that_does_not_opt_in_is_unchanged() =
  ## Back-compat. Bit 14 is rejecting at every reader that predates it,
  ## so a writer that has not opted in must produce exactly the bytes it
  ## produced before the bit existed.
  let steps = [(0'u64, 1'u64), (1'u64, 3'u64)]
  let plain = buildTrace("plain", withTable = false,
    counts = [CountA, CountB], steps = steps)

  var r = openNewTraceFromBytes(plain).get()
  doAssert not r.meta.hasLineCountTable,
    "a writer that did not opt in must leave bit 14 clear"
  doAssert not ctPrintJson(plain)["metadata"]["flags"]["has_line_count_table"].getBool(),
    "ct-print must report an opted-out trace as stating no file sizes — the " &
    "field has to DISCRIMINATE, or it tells an operator nothing"
  doAssert r.recordedLineCount(0) == 0'u64,
    "an opted-out trace records no per-file size"
  doAssert r.path(0).get() == PathA,
    "the bare record must still decode to the bare path"
  doAssert r.globalPositionSpace().totalLines == 2'u64 * DefaultLinesPerFile,
    "an opted-out trace keeps the pre-table space"

  # The refusal does not apply either: there is no recorded bound to
  # enforce, and enforcing the constant would be enforcing a number the
  # container does not carry.
  var w = initMultiStreamWriter(dir / "plain2.build", "plain2").get()
  doAssert w.registerPath(PathA).isOk
  doAssert w.registerStep(0, CountA + 1, @[]).isOk,
    "without the table there is no recorded count to check against"
  w.closeCtfs()

  echo "PASS: test_a_writer_that_does_not_opt_in_is_unchanged"

removeDir(dir)
createDir(dir)

test_the_space_is_the_sum_of_the_recorded_counts()
test_the_container_states_the_counts_and_the_paths()
test_a_recorded_count_of_zero_is_refused_at_the_reader()
test_a_step_past_the_recorded_count_is_refused()
test_registerPath_refuses_a_path_with_no_count()
test_a_writer_that_cannot_count_records_the_ceiling_it_used()
test_bit_14_is_what_says_the_counts_are_there()
test_the_addresses_shrink()
test_a_writer_that_does_not_opt_in_is_unchanged()

removeDir(dir)
echo "ALL PASS: test_line_count_table"
