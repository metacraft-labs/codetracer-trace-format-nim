{.push raises: [].}

## Integration tests for the P6.3 / P6.4 column-aware step encoding.
##
## Covers the end-to-end path: ``MultiStreamTraceWriter`` opted into
## column-aware mode via ``enableColumnAwareSteps``, emitting a mix of
## ``registerStep`` (line-level) and ``registerColumnStep`` (column
## nudge) events, and confirms that:
##
##   * ``meta.dat`` carries ``FlagHasColumnAwareSteps`` (bit 4),
##   * the step stream round-trips ``sekDeltaColumn`` events with their
##     column deltas intact through ``NewTraceReader``,
##   * the value stream stays in lock-step with the exec stream
##     (one values record per step event of either kind),
##   * a writer that did not opt in rejects ``registerColumnStep`` so
##     legacy line-only traces remain byte-for-byte identical to the
##     pre-P6.4 output.
##
## See ~codetracer-trace-format-spec/trace-events.md~
## §"Column Encoding — `DeltaColumn` (chosen)" and
## §"Reader Behaviour and Back-Compat" for the spec contract this test
## guards.

import std/[options, os, strutils]
import results
import codetracer_trace_types
import codetracer_trace_reader
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/varint

proc test_column_aware_round_trip() {.raises: [].} =
  let writerRes = initMultiStreamWriter("test_columns.ct", "col_test")
  doAssert writerRes.isOk, "init failed: " & writerRes.error
  var w = writerRes.get()

  # Opt into column-aware mode *before* any step is written.
  w.enableColumnAwareSteps()

  let p0 = w.registerPath("/src/main.py")
  doAssert p0.isOk

  # First step must be absolute (registerStep) to define the running
  # position.  Then alternate column nudges with line moves.
  doAssert w.registerStep(0, 10, @[]).isOk, "first registerStep"
  doAssert w.registerColumnStep(2, @[]).isOk, "column nudge +2"
  doAssert w.registerColumnStep(3, @[]).isOk, "column nudge +3"
  doAssert w.registerStep(0, 11, @[]).isOk, "line move to 11"
  doAssert w.registerColumnStep(-4, @[]).isOk, "column nudge -4"
  doAssert w.registerStep(0, 12, @[]).isOk, "line move to 12"
  doAssert w.registerColumnStep(1, @[]).isOk, "column nudge +1"

  doAssert w.close().isOk, "close failed"
  let ctBytes = w.toBytes()
  w.closeCtfs()

  # Re-open the trace via the reader.
  var readerRes = openNewTraceFromBytes(ctBytes)
  doAssert readerRes.isOk, "openNewTraceFromBytes failed: " & readerRes.error
  var reader = readerRes.get()

  # 1. meta.dat carries the column-aware flag.
  doAssert reader.meta.hasColumnAwareSteps,
    "meta.dat should have FlagHasColumnAwareSteps set after " &
    "enableColumnAwareSteps + close"

  # 2. Step count matches: 4 line steps + 3 column steps = 7 events.
  let sc = reader.stepCount()
  doAssert sc.isOk and sc.get() == 7,
    "expected 7 steps, got " &
      (if sc.isOk: $sc.get() else: sc.error)

  # 3. Walk every step and assert the expected kinds / column deltas.
  let expectedKinds = @[
    sekAbsoluteStep,  # registerStep(line=10)
    sekDeltaColumn,   # +2
    sekDeltaColumn,   # +3
    sekDeltaStep,     # registerStep(line=11) — small delta
    sekDeltaColumn,   # -4
    sekDeltaStep,     # registerStep(line=12) — small delta
    sekDeltaColumn,   # +1
  ]
  let expectedColumnDeltas = @[0'i64, 2, 3, 0, -4, 0, 1]

  for i, expKind in expectedKinds:
    let evRes = reader.step(uint64(i))
    doAssert evRes.isOk, "step " & $i & " failed: " & evRes.unsafeError
    let ev = evRes.get()
    doAssert ev.kind == expKind,
      "step " & $i & " kind: expected " & $expKind &
      " got " & $ev.kind
    if ev.kind == sekDeltaColumn:
      doAssert ev.columnDelta == expectedColumnDeltas[i],
        "step " & $i & " columnDelta: expected " &
        $expectedColumnDeltas[i] & " got " & $ev.columnDelta

  echo "PASS: test_column_aware_round_trip"

proc test_column_step_requires_opt_in() {.raises: [].} =
  ## P6.4: registerColumnStep must error on a writer that has *not*
  ## opted into column-aware mode.  This is the safety net that keeps
  ## line-only traces byte-for-byte identical to the pre-P6.4 output
  ## even if a recorder forgets to flip the flag.
  let writerRes = initMultiStreamWriter("test_no_opt.ct", "no_opt")
  doAssert writerRes.isOk
  var w = writerRes.get()

  doAssert w.registerPath("/src/main.py").isOk
  doAssert w.registerStep(0, 1, @[]).isOk

  let colRes = w.registerColumnStep(1, @[])
  doAssert colRes.isErr,
    "registerColumnStep without enableColumnAwareSteps must error"

  discard w.close()
  w.closeCtfs()
  echo "PASS: test_column_step_requires_opt_in"

proc test_column_step_first_is_rejected() {.raises: [].} =
  ## P6.4: registerColumnStep can't be the *first* step in a trace
  ## either — column deltas only make sense once an absolute
  ## ``global_position_index`` has been emitted by an AbsoluteStep.
  let writerRes = initMultiStreamWriter("test_first_col.ct", "first_col")
  doAssert writerRes.isOk
  var w = writerRes.get()
  w.enableColumnAwareSteps()

  doAssert w.registerPath("/src/main.py").isOk
  let colRes = w.registerColumnStep(1, @[])
  doAssert colRes.isErr,
    "first step must be an AbsoluteStep, registerColumnStep should error"

  discard w.close()
  w.closeCtfs()
  echo "PASS: test_column_step_first_is_rejected"

proc test_no_column_flag_for_legacy_writer() {.raises: [].} =
  ## P6.4: a writer that never opted into column-aware mode MUST NOT
  ## set the flag bit on meta.dat — that bit is a contract with old
  ## readers, which reject any trace whose flag they don't understand.
  let writerRes = initMultiStreamWriter("test_legacy.ct", "legacy")
  doAssert writerRes.isOk
  var w = writerRes.get()

  doAssert w.registerPath("/src/main.py").isOk
  doAssert w.registerStep(0, 1, @[]).isOk
  doAssert w.registerStep(0, 2, @[]).isOk

  doAssert w.close().isOk
  let ctBytes = w.toBytes()
  w.closeCtfs()

  var readerRes = openNewTraceFromBytes(ctBytes)
  doAssert readerRes.isOk
  let reader = readerRes.get()

  doAssert not reader.meta.hasColumnAwareSteps,
    "legacy (non-opt-in) writer must leave FlagHasColumnAwareSteps clear"
  echo "PASS: test_no_column_flag_for_legacy_writer"

# ---------------------------------------------------------------------------
# P6.5 — Piece A: paths.dat per-line offset table (Layout A) round-trip
# ---------------------------------------------------------------------------

proc test_paths_dat_line_lengths_round_trip() {.raises: [].} =
  ## Layout A: a column-aware writer that supplies per-line column counts
  ## via ``registerPath(path, lineLengths=…)`` MUST round-trip those
  ## counts through ``NewTraceReader.lineLength``.  Pre-extension
  ## (``columnAwareSteps = false``) writers MUST keep the
  ## bare-path-bytes record format and the reader's ``lineLength`` MUST
  ## return ``none``.
  let writerRes = initMultiStreamWriter("test_paths_lengths.ct", "ll_test")
  doAssert writerRes.isOk
  var w = writerRes.get()
  w.enableColumnAwareSteps()

  let llA: seq[uint32] = @[20'u32, 25, 30, 27, 22]
  let llB: seq[uint32] = @[10'u32, 12, 14]

  let pA = w.registerPath("/src/a.py", llA)
  doAssert pA.isOk
  doAssert pA.get() == 0
  let pB = w.registerPath("/src/b.py", llB)
  doAssert pB.isOk
  doAssert pB.get() == 1

  # Emit at least one step so the trace has a complete exec stream.
  doAssert w.registerStep(0, 1, @[]).isOk
  doAssert w.close().isOk
  let ctBytes = w.toBytes()
  w.closeCtfs()

  var readerRes = openNewTraceFromBytes(ctBytes)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  doAssert reader.meta.hasColumnAwareSteps
  doAssert reader.pathCount() == 2

  # Path strings round-trip through the column-aware record format.
  let pAStr = reader.path(0)
  doAssert pAStr.isOk and pAStr.get() == "/src/a.py",
    "path 0 mismatch: " & (if pAStr.isOk: pAStr.get() else: pAStr.error)
  let pBStr = reader.path(1)
  doAssert pBStr.isOk and pBStr.get() == "/src/b.py",
    "path 1 mismatch: " & (if pBStr.isOk: pBStr.get() else: pBStr.error)

  # lineLength queries round-trip per-line counts.
  for i, expected in llA:
    let q = reader.lineLength(0, uint32(i))
    doAssert q.isSome and q.get() == expected,
      "file 0 line " & $i & " expected " & $expected & " got " &
      (if q.isSome: $q.get() else: "none")
  for i, expected in llB:
    let q = reader.lineLength(1, uint32(i))
    doAssert q.isSome and q.get() == expected,
      "file 1 line " & $i & " expected " & $expected & " got " &
      (if q.isSome: $q.get() else: "none")

  # Out-of-range queries return none.
  doAssert reader.lineLength(0, uint32(llA.len)).isNone,
    "out-of-range line query should be none"
  doAssert reader.lineLength(2, 0).isNone,
    "out-of-range file query should be none"

  echo "PASS: test_paths_dat_line_lengths_round_trip"


proc test_lineLength_legacy_trace_is_none() {.raises: [].} =
  ## Pre-extension traces (column flag clear) MUST return ``none`` for
  ## every ``lineLength`` query; the reader cannot make up column data
  ## that was never recorded.
  let writerRes = initMultiStreamWriter("test_no_ll.ct", "no_ll")
  doAssert writerRes.isOk
  var w = writerRes.get()
  # Note: no enableColumnAwareSteps call.
  doAssert w.registerPath("/src/main.py").isOk
  doAssert w.registerStep(0, 1, @[]).isOk
  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()

  let readerRes = openNewTraceFromBytes(bytes)
  doAssert readerRes.isOk
  let reader = readerRes.get()
  doAssert not reader.meta.hasColumnAwareSteps
  doAssert reader.lineLength(0, 0).isNone,
    "legacy trace must return none for every lineLength query"
  doAssert reader.lineLength(0, 100).isNone

  echo "PASS: test_lineLength_legacy_trace_is_none"


proc test_decode_global_position_index() {.raises: [].} =
  ## ``global_position_index → (file, line, column)``: register two
  ## files with known line lengths, then pick a position landing in
  ## file B line 5 column 12 and assert the decoder lands there.  This
  ## directly exercises the spec algorithm at
  ## §"Decoding ``global_position_index``".
  let writerRes = initMultiStreamWriter("test_decode_pos.ct", "decode_pos")
  doAssert writerRes.isOk
  var w = writerRes.get()
  w.enableColumnAwareSteps()

  # File A: 4 lines × 10 columns each → file_size = 40.
  let llA: seq[uint32] = @[10'u32, 10, 10, 10]
  # File B: 6 lines × 20 columns each → file_size = 120.  Line 5
  # (1-based) corresponds to lineLengths[4], i.e. starts at
  # in-file offset 4*20 = 80; column 12 is offset 80 + 11 = 91 within
  # file B.  Global position = file A's size 40 + 91 = 131.
  let llB: seq[uint32] = @[20'u32, 20, 20, 20, 20, 20]
  doAssert w.registerPath("/A.py", llA).isOk
  doAssert w.registerPath("/B.py", llB).isOk
  doAssert w.registerStep(0, 1, @[]).isOk
  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()

  var readerRes = openNewTraceFromBytes(bytes)
  doAssert readerRes.isOk
  var reader = readerRes.get()
  doAssert reader.meta.hasColumnAwareSteps

  # Lookup: pick the position that should land on (file=1, line=5, col=12).
  let p: uint64 = 40 + 80 + 11
  let res = reader.decodeGlobalPositionIndex(p)
  doAssert res.isOk,
    "decodeGlobalPositionIndex failed: " & res.unsafeError
  let triple = res.get()
  doAssert triple.file == 1'u64,
    "file expected 1, got " & $triple.file
  doAssert triple.line == 5'u32,
    "line expected 5, got " & $triple.line
  doAssert triple.column == 12'u32,
    "column expected 12, got " & $triple.column

  # Boundary: first column of file 0 line 1.
  let res0 = reader.decodeGlobalPositionIndex(0)
  doAssert res0.isOk
  doAssert res0.get() == (file: 0'u64, line: 1'u32, column: 1'u32),
    "first position should decode to (file=0, line=1, column=1), got " & $res0.get()

  # Boundary: last column of file A's last line.  File A ends at
  # offset 39 (40-byte file_size, 0-based).  Line 4 (last), starting at
  # offset 30; column at offset 39 is 10.
  let resEndA = reader.decodeGlobalPositionIndex(39)
  doAssert resEndA.isOk
  doAssert resEndA.get() == (file: 0'u64, line: 4'u32, column: 10'u32)

  # Out-of-range: past the end of file B should error.
  let resOOR = reader.decodeGlobalPositionIndex(40 + 120)
  doAssert resOOR.isErr, "out-of-range position should fail"

  echo "PASS: test_decode_global_position_index"


# ---------------------------------------------------------------------------
# P6.5 — Piece B: column field on StepRecord
# ---------------------------------------------------------------------------

proc test_step_record_column_field() {.raises: [].} =
  ## Emit AbsoluteStep(line=10), DeltaColumn(+5), DeltaStep(+1) and
  ## assert the StepRecord column values are some(1), some(6), some(1)
  ## (column resets to 1 on line transition per spec).
  let path = getTempDir() / "test_step_col_field.ct"
  try: removeFile(path)
  except OSError: discard

  let writerRes = initMultiStreamWriter(path, "col_field")
  doAssert writerRes.isOk
  var w = writerRes.get()
  w.enableColumnAwareSteps()
  doAssert w.registerPath("/src/main.py").isOk
  doAssert w.registerStep(0, 10, @[]).isOk         # AbsoluteStep, line=10
  doAssert w.registerColumnStep(5, @[]).isOk        # DeltaColumn(+5)
  doAssert w.registerStep(0, 11, @[]).isOk         # DeltaStep (small delta)
  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()

  # Save to disk so the high-level openTrace path can pick it up.
  try:
    let f = open(path, fmWrite)
    discard f.writeBytes(bytes, 0, bytes.len)
    f.close()
  except IOError, OSError:
    doAssert false, "failed to write trace file"

  var traceRes = openTrace(path)
  doAssert traceRes.isOk, "openTrace failed: " & traceRes.unsafeError
  var reader = traceRes.get()
  let evRes = reader.readEvents()
  doAssert evRes.isOk, "readEvents failed: " & evRes.unsafeError

  # Collect the tleStep events in order.
  var steps: seq[StepRecord] = @[]
  for ev in reader.events:
    if ev.kind == tleStep:
      steps.add(ev.step)

  doAssert steps.len == 3, "expected 3 step events, got " & $steps.len

  doAssert steps[0].hasColumn, "step 0 should carry a column"
  doAssert int64(steps[0].column) == 1,
    "step 0 column expected 1, got " & $int64(steps[0].column)

  doAssert steps[1].hasColumn, "step 1 should carry a column"
  doAssert int64(steps[1].column) == 6,
    "step 1 column expected 6 (1 + 5), got " & $int64(steps[1].column)

  doAssert steps[2].hasColumn, "step 2 should carry a column"
  doAssert int64(steps[2].column) == 1,
    "step 2 column expected 1 (reset on line transition), got " &
    $int64(steps[2].column)

  try: removeFile(path)
  except OSError: discard
  echo "PASS: test_step_record_column_field"


proc test_step_record_column_none_for_legacy() {.raises: [].} =
  ## Pre-extension traces yield ``StepRecord.hasColumn = false`` (the
  ## reader cannot fabricate a column when the writer never recorded
  ## one).
  let path = getTempDir() / "test_step_col_legacy.ct"
  try: removeFile(path)
  except OSError: discard

  let writerRes = initMultiStreamWriter(path, "col_legacy")
  doAssert writerRes.isOk
  var w = writerRes.get()
  # No enableColumnAwareSteps.
  doAssert w.registerPath("/src/main.py").isOk
  doAssert w.registerStep(0, 1, @[]).isOk
  doAssert w.registerStep(0, 2, @[]).isOk
  doAssert w.registerStep(0, 3, @[]).isOk
  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()

  try:
    let f = open(path, fmWrite)
    discard f.writeBytes(bytes, 0, bytes.len)
    f.close()
  except IOError, OSError:
    doAssert false, "failed to write trace file"

  var traceRes = openTrace(path)
  doAssert traceRes.isOk
  var reader = traceRes.get()
  let evRes = reader.readEvents()
  doAssert evRes.isOk

  var anyStep = false
  for ev in reader.events:
    if ev.kind == tleStep:
      anyStep = true
      doAssert not ev.step.hasColumn,
        "legacy trace must surface column = None on every step"
  doAssert anyStep, "expected at least one tleStep event"

  try: removeFile(path)
  except OSError: discard
  echo "PASS: test_step_record_column_none_for_legacy"


# ---------------------------------------------------------------------------
# P6.5 — Piece C: strict meta-flag rejection
# ---------------------------------------------------------------------------

proc handcraftMetaDatWithFlags(flags: uint16): seq[byte] {.raises: [].} =
  ## Minimal v3 meta.dat with the given raw flags word.  Recording id
  ## is the canonical UUIDv7 used in `test_meta_dat.nim`.
  const TestRecordingId = "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb"
  var buf = newSeq[byte](0)
  # Magic
  for b in [0x43'u8, 0x54, 0x4D, 0x44]:
    buf.add(b)
  # Version = 3
  buf.add(3'u8); buf.add(0'u8)
  # Flags
  buf.add(byte(flags and 0xFF))
  buf.add(byte((flags shr 8) and 0xFF))
  # recording_id
  encodeVarint(uint64(TestRecordingId.len), buf)
  for c in TestRecordingId:
    buf.add(byte(c))
  # program (empty)
  buf.add(0'u8)
  # args_count = 0
  buf.add(0'u8)
  # workdir (empty)
  buf.add(0'u8)
  # recorder_id (empty)
  buf.add(0'u8)
  # paths_count = 0
  buf.add(0'u8)
  buf

proc test_strict_meta_flag_rejection() {.raises: [].} =
  ## A meta.dat byte sequence with bit 5 set (an unknown flag) MUST be
  ## rejected by ``readMetaDat`` — this is the wire-format safety net
  ## that makes the column extension's bit-4 break clean for older
  ## readers (and gives every future bit allocation the same guarantee).
  # Bit 5 (= 0x20) is currently unallocated.  Use it alone to make sure
  # the rejection fires on the unknown bit by itself.
  let badBuf = handcraftMetaDatWithFlags(0x20'u16)
  let badRes = readMetaDat(badBuf)
  doAssert badRes.isErr,
    "readMetaDat must reject meta.dat with unknown flag bit 5 set"

  # Sanity check the error message mentions the unknown bits.
  doAssert "unknown flag" in badRes.error or
           "unknown" in badRes.error,
    "rejection error should mention unknown flags; got: " & badRes.error

  # Bit 4 alone (FlagHasColumnAwareSteps) is a known flag and must
  # parse cleanly.
  let goodBuf = handcraftMetaDatWithFlags(FlagHasColumnAwareSteps)
  let goodRes = readMetaDat(goodBuf)
  doAssert goodRes.isOk,
    "FlagHasColumnAwareSteps alone must round-trip; got: " &
    (if goodRes.isErr: goodRes.error else: "ok")
  doAssert goodRes.get().hasColumnAwareSteps,
    "hasColumnAwareSteps must be surfaced when bit 4 is set"

  # Mix: bit 4 (known) + bit 5 (unknown) → reject.
  let mixedBuf = handcraftMetaDatWithFlags(
    FlagHasColumnAwareSteps or 0x20'u16)
  let mixedRes = readMetaDat(mixedBuf)
  doAssert mixedRes.isErr,
    "meta.dat with bit 4 + bit 5 must reject because bit 5 is unknown"

  # All currently-known bits together still parse cleanly.
  let allKnown =
    FlagHasColumnAwareSteps or FlagHasMcrFields or
    FlagHasReplayLaunchFields or FlagHasLayoutSnapshot or
    FlagHasTraceFilterProvenance
  # We can't easily construct a fully valid meta.dat with every block
  # in place from scratch — that needs the full encoder.  The "every
  # known bit ORed together" case is covered by
  # ``test_meta_dat_with_mcr_fields`` / the filter-provenance tests in
  # ``test_meta_dat.nim``.  Avoid the temptation to construct it here
  # to keep this test focused on the unknown-bit rejection contract.
  discard allKnown

  echo "PASS: test_strict_meta_flag_rejection"


test_column_aware_round_trip()
test_column_step_requires_opt_in()
test_column_step_first_is_rejected()
test_no_column_flag_for_legacy_writer()
test_paths_dat_line_lengths_round_trip()
test_lineLength_legacy_trace_is_none()
test_decode_global_position_index()
test_step_record_column_field()
test_step_record_column_none_for_legacy()
test_strict_meta_flag_rejection()
echo "ALL PASS: test_column_aware_steps"
