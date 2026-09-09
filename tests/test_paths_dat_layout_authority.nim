{.push raises: [].}

## `meta.dat` bit 4 is the sole authority on the `paths.dat` record layout.
##
## A `paths.dat` record is either the raw path bytes (line-only) or
## ``path_len + path_bytes + line_count + line_lengths`` (column-aware
## "Layout A"), and which one it is is stated by ``FlagHasColumnAwareSteps``
## in ``meta.dat``. The two record spaces OVERLAP, so the layout cannot be
## recovered from the bytes: an ordinary 97-byte ASCII path decodes as a
## complete, record-exact Layout A record purely by coincidence.
##
##   '/'                 → 0x2F = 47  → path_len 47
##   46 filler + '/'     → the 47 path bytes
##   '0'                 → 0x30 = 48  → line_count 48
##   48 × filler         → 48 single-byte signed-varint line lengths
##   ────────────────────────────────────────────────────────────────
##   97 bytes consumed exactly
##
## A reader that treats "it decoded" as evidence therefore answers a
## line-only trace with a truncated path, a fabricated per-file line table,
## and a plausible-looking `(file, line, column)` that is not where the step
## was — wrong answers with no error. These tests pin the refusal: the meta
## flag decides, a coincidence never promotes, and the caller who genuinely
## needs the pre-``708ee44`` recorder-bug recovery asks for it by name.
##
## See ~codetracer-trace-format-spec/internal-files.md~ §"paths.dat per-line
## offset table" and ~trace-events.md~ §"Reader Behaviour and Back-Compat".

import std/[options, strutils]
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/global_line_index

const
  # '/' + 46 filler + '/' + '0' + 48 filler = 97 bytes. Two distinct paths of
  # this shape so the whole-table probe (which requires EVERY record to
  # decode) is satisfied, matching the reported reproducer.
  CoincidentPathA = "/" & repeat('a', 46) & "/" & "0" & repeat('b', 48)
  CoincidentPathB = "/" & repeat('c', 46) & "/" & "0" & repeat('d', 48)
  OrdinaryPath = "/src/main.py"

proc resolveStepLocation(reader: var NewTraceReader, gli: GlobalLineIndex,
    stepGli: uint64): (int, uint64) =
  ## The routing every step-location consumer performs, mirrored from
  ## ``codetracer_ct_print.nim``'s ``resolveStepLocation`` and the FFI's
  ## ``ct_reader_step_locations_with_columns``: a column-aware trace resolves
  ## its byte-offset ``global_position_index`` through
  ## ``decodeGlobalPositionIndex``, a line-only trace through the
  ## line-count-based ``GlobalLineIndex``. Because the routing keys off
  ## ``meta.hasColumnAwareSteps``, a promoted flag sends a line-only trace
  ## down the wrong branch and the caller sees the wrong line.
  if reader.meta.hasColumnAwareSteps:
    let posRes = reader.decodeGlobalPositionIndex(stepGli)
    if posRes.isOk:
      return (int(posRes.get().file), uint64(posRes.get().line))
  gli.resolve(stepGli)

proc lineOnlyGli(pathCount: int): GlobalLineIndex =
  ## The index the writer used to encode line-only steps.
  var counts = newSeq[uint64](pathCount)
  for i in 0 ..< pathCount:
    counts[i] = DefaultLinesPerFile
  buildGlobalLineIndex(counts)

proc buildLineOnlyTrace(name: string, paths: openArray[string]):
    seq[byte] {.raises: [].} =
  ## A plain line-only trace: no ``enableColumnAwareSteps``, so ``meta.dat``
  ## bit 4 stays clear and every ``paths.dat`` record is the raw path bytes.
  ## Steps sit on lines 1, 2 and 3 of file 0.
  let writerRes = initMultiStreamWriter(name & ".ct", name)
  doAssert writerRes.isOk, "init failed: " & writerRes.error
  var w = writerRes.get()
  for p in paths:
    doAssert w.registerPath(p).isOk, "registerPath failed for " & p
  doAssert w.registerStep(0, 1, @[]).isOk
  doAssert w.registerStep(0, 2, @[]).isOk
  doAssert w.registerStep(0, 3, @[]).isOk
  doAssert w.close().isOk, "close failed"
  result = w.toBytes()
  w.closeCtfs()

# ---------------------------------------------------------------------------

proc test_coincidental_layout_a_is_not_promoted() =
  ## THE REPRODUCER. A line-only trace whose two paths happen to satisfy the
  ## Layout A grammar must read back as the line-only trace it is: the full
  ## path, no per-file line table, and step 2 on line 3 where it was written.
  let bytes = buildLineOnlyTrace("test_pd_coincident",
    [CoincidentPathA, CoincidentPathB])

  var readerRes = openNewTraceFromBytes(bytes)
  doAssert readerRes.isOk, "open failed: " & readerRes.error
  var reader = readerRes.get()

  doAssert not reader.meta.hasColumnAwareSteps,
    "meta.dat declares line-only steps; a paths.dat coincidence must not " &
    "promote the trace to column-aware"

  # The path comes back whole. Under the promoted reading only the 47-byte
  # prefix that the coincidental `path_len` describes was returned.
  let p0 = reader.path(0)
  doAssert p0.isOk, "path(0) failed: " & p0.error
  doAssert p0.get() == CoincidentPathA,
    "path 0 must round-trip all " & $CoincidentPathA.len & " bytes, got " &
    $p0.get().len & ": " & p0.get()
  let p1 = reader.path(1)
  doAssert p1.isOk and p1.get() == CoincidentPathB,
    "path 1 must round-trip whole"

  # No per-file line table was recorded, so none may be reported. Under the
  # promoted reading each file claimed 48 lines that no recorder ever wrote.
  doAssert reader.lineCountRaw(0) == 0'u64,
    "line-only trace must report no Layout A lines for file 0, got " &
    $reader.lineCountRaw(0)
  doAssert reader.lineCountRaw(1) == 0'u64,
    "line-only trace must report no Layout A lines for file 1, got " &
    $reader.lineCountRaw(1)
  doAssert reader.lineLengthRaw(0, 0).isNone,
    "no per-line data was recorded, so none may be surfaced"
  doAssert reader.lineLength(0, 0).isNone,
    "gated line-length query must be none on a line-only trace"

  # Step 2 was written at (file 0, line 3) and must read back there. Under
  # the promoted reading it resolved to (file 0, line 1) — the byte-offset
  # decoder read the GLI as a within-line column on line 1.
  let gli = lineOnlyGli(int(reader.pathCount()))
  let abs2 = reader.stepAbsoluteGlobalLineIndex(2)
  doAssert abs2.isOk, "step 2 GLI: " & abs2.error
  let loc2 = resolveStepLocation(reader, gli, abs2.get())
  doAssert loc2 == (0, 3'u64),
    "step 2 expected (file 0, line 3), got (file " & $loc2[0] & ", line " &
    $loc2[1] & ")"

  # The byte-offset decoder must refuse outright rather than offer a
  # plausible triple; it produced (file 0, line 1, column 4) when promoted.
  let dec2 = reader.decodeGlobalPositionIndex(abs2.get())
  doAssert dec2.isErr,
    "decodeGlobalPositionIndex must refuse a line-only trace, got " & $dec2.get()

  # Silence is not an option: the reader names the coincidence so an
  # operator holding a genuinely affected trace can act on it.
  doAssert reader.columnAwarePathsSuspected,
    "records that decode as Layout A under a clear meta bit must be reported"

  echo "PASS: test_coincidental_layout_a_is_not_promoted"

proc test_ordinary_line_only_trace_is_not_suspected() =
  ## Control for the report above: it must be capable of being false. An
  ## ordinary short path ends right after its bytes, leaving no room for a
  ## ``line_count``, so nothing about it looks like Layout A.
  let bytes = buildLineOnlyTrace("test_pd_ordinary", [OrdinaryPath])

  var readerRes = openNewTraceFromBytes(bytes)
  doAssert readerRes.isOk, "open failed: " & readerRes.error
  var reader = readerRes.get()

  doAssert not reader.meta.hasColumnAwareSteps
  doAssert not reader.columnAwarePathsSuspected,
    "an ordinary line-only path must not be reported as suspected Layout A"
  let p0 = reader.path(0)
  doAssert p0.isOk and p0.get() == OrdinaryPath, "path 0 must round-trip"
  doAssert reader.lineCountRaw(0) == 0'u64

  let gli = lineOnlyGli(int(reader.pathCount()))
  let abs2 = reader.stepAbsoluteGlobalLineIndex(2)
  doAssert abs2.isOk, "step 2 GLI: " & abs2.error
  doAssert resolveStepLocation(reader, gli, abs2.get()) == (0, 3'u64)

  echo "PASS: test_ordinary_line_only_trace_is_not_suspected"

proc test_assume_override_refuses_a_real_line_only_trace() =
  ## ``assumeColumnAwarePaths`` is the caller's escape hatch for the
  ## pre-``708ee44`` recorder bug, and it is authoritative rather than
  ## speculative: applied to a trace whose records are NOT Layout A the open
  ## fails with a named ``paths.dat[N]: …`` error. It cannot quietly fall
  ## back to the line-only reading, which is what made the old promotion
  ## unable to tell a real recovery from a coincidence.
  let bytes = buildLineOnlyTrace("test_pd_override_bad", [OrdinaryPath])

  let readerRes = openNewTraceFromBytes(bytes, assumeColumnAwarePaths = true)
  doAssert readerRes.isErr,
    "the override must fail loudly on records that are not Layout A"
  doAssert readerRes.error.startsWith("paths.dat[0]"),
    "the failure must name the offending record, got: " & readerRes.error

  echo "PASS: test_assume_override_refuses_a_real_line_only_trace"

proc test_assume_override_reads_layout_a_records() =
  ## The other half of the override's contract: where the records DO parse as
  ## Layout A it delivers the per-file tables and the column-aware reading,
  ## which is the whole point of keeping the recovery available. The
  ## coincidental container stands in for an affected recorder's trace — the
  ## two are byte-indistinguishable, which is exactly why the decision has to
  ## be the caller's and cannot be the reader's.
  let bytes = buildLineOnlyTrace("test_pd_override_ok",
    [CoincidentPathA, CoincidentPathB])

  var readerRes = openNewTraceFromBytes(bytes, assumeColumnAwarePaths = true)
  doAssert readerRes.isOk, "override open failed: " & readerRes.error
  var reader = readerRes.get()

  doAssert reader.meta.hasColumnAwareSteps,
    "the override makes the handle read the trace as column-aware"
  doAssert reader.lineCountRaw(0) == 48'u64,
    "the Layout A record declares 48 lines, got " & $reader.lineCountRaw(0)
  doAssert not reader.columnAwarePathsSuspected,
    "nothing is suspected once the layout has been decided"

  echo "PASS: test_assume_override_reads_layout_a_records"

proc test_declared_column_aware_trace_is_unaffected() =
  ## The majority path. A trace that declares bit 4 keeps its per-file line
  ## tables, its path strings and its byte-offset position decoding exactly
  ## as before — the refusal above applies only to traces that declare
  ## line-only steps.
  let writerRes = initMultiStreamWriter("test_pd_column_aware.ct", "pd_col")
  doAssert writerRes.isOk, "init failed: " & writerRes.error
  var w = writerRes.get()
  w.enableColumnAwareSteps()

  # File 0: 4 lines × 10 columns (size 40). File 1: 6 lines × 20 columns.
  let llA: seq[uint32] = @[10'u32, 10, 10, 10]
  let llB: seq[uint32] = @[20'u32, 20, 20, 20, 20, 20]
  doAssert w.registerPath("/A.py", llA).isOk
  doAssert w.registerPath("/B.py", llB).isOk
  doAssert w.registerStep(0, 2, @[]).isOk
  doAssert w.registerStep(1, 3, @[]).isOk
  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()

  var readerRes = openNewTraceFromBytes(bytes)
  doAssert readerRes.isOk, "open failed: " & readerRes.error
  var reader = readerRes.get()

  doAssert reader.meta.hasColumnAwareSteps
  doAssert not reader.columnAwarePathsSuspected,
    "a declared column-aware trace has nothing to suspect"

  let p0 = reader.path(0)
  doAssert p0.isOk and p0.get() == "/A.py", "path 0 must decode from Layout A"
  let p1 = reader.path(1)
  doAssert p1.isOk and p1.get() == "/B.py", "path 1 must decode from Layout A"

  doAssert reader.lineCountRaw(0) == uint64(llA.len)
  doAssert reader.lineCountRaw(1) == uint64(llB.len)
  for i, expected in llA:
    let q = reader.lineLength(0, uint32(i))
    doAssert q.isSome and q.get() == expected,
      "file 0 line " & $i & " expected " & $expected
  for i, expected in llB:
    let q = reader.lineLength(1, uint32(i))
    doAssert q.isSome and q.get() == expected,
      "file 1 line " & $i & " expected " & $expected

  # Positions still decode through the byte-offset algorithm.
  let abs0 = reader.stepAbsoluteGlobalLineIndex(0)
  doAssert abs0.isOk, "step 0 GLI: " & abs0.error
  let dec0 = reader.decodeGlobalPositionIndex(abs0.get())
  doAssert dec0.isOk, "decode step 0: " & dec0.error
  doAssert dec0.get() == (file: 0'u64, line: 2'u32, column: 1'u32),
    "step 0 expected (file 0, line 2, col 1), got " & $dec0.get()

  let abs1 = reader.stepAbsoluteGlobalLineIndex(1)
  doAssert abs1.isOk, "step 1 GLI: " & abs1.error
  let dec1 = reader.decodeGlobalPositionIndex(abs1.get())
  doAssert dec1.isOk, "decode step 1: " & dec1.error
  doAssert dec1.get() == (file: 1'u64, line: 3'u32, column: 1'u32),
    "step 1 expected (file 1, line 3, col 1), got " & $dec1.get()

  echo "PASS: test_declared_column_aware_trace_is_unaffected"

test_coincidental_layout_a_is_not_promoted()
test_ordinary_line_only_trace_is_not_suspected()
test_assume_override_refuses_a_real_line_only_trace()
test_assume_override_reads_layout_a_records()
test_declared_column_aware_trace_is_unaffected()

echo "ALL PASS: test_paths_dat_layout_authority"
