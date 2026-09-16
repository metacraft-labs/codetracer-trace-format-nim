when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Tests for the value stream writer/reader (M24a-2 SPEC chunked layout).
##
## The on-disk format is now the SPEC-canonical chunked Zstd ``values.dat`` +
## ``values.idx`` (byte-compatible with the Rust ``ValueStreamReader``).  The
## per-record format is a tag-0 ``StepValues`` event carrying
## ``(name_id, CBOR value)`` pairs — there is no separate ``type_id`` on the
## wire; the reader reconstructs ``VariableValue.typeId`` from the CBOR value's
## top-level ``type_id``.  These tests therefore feed REAL CBOR-encoded values
## (so the reconstructed ``typeId`` round-trips), and exercise empty records,
## multi-chunk streams, and the parallel-index invariant (record N ↔ step N).

import std/strutils
import results
import codetracer_ctfs/container
import codetracer_ctfs/variable_record_table
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/varint
import codetracer_trace_writer/cbor
import codetracer_trace_types

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

type Rng = object
  state: uint64

proc initRng(seed: uint64): Rng = Rng(state: seed)

proc next(r: var Rng): uint64 =
  r.state = r.state xor (r.state shl 13)
  r.state = r.state xor (r.state shr 7)
  r.state = r.state xor (r.state shl 17)
  r.state

proc encInt(value: int64, typeId: uint64): seq[byte] =
  ## A real CBOR-encoded Int ValueRecord whose top-level type_id == typeId,
  ## so the reader's reconstructed VariableValue.typeId round-trips exactly.
  var enc = CborEncoder.init()
  enc.encodeCborValueRecord(ValueRecord(
    kind: vrkInt, intVal: value, intTypeId: TypeId(typeId)))
  enc.getBytes()

proc makeValues(rng: var Rng, count: int): seq[VariableValue] =
  ## Build `count` variables with reproducible varname/type ids and a real
  ## CBOR Int payload encoding that same type id.
  var vals = newSeq[VariableValue](count)
  for i in 0 ..< count:
    let vnId = rng.next() mod 10000
    let tId = rng.next() mod 500
    let iv = int64(rng.next() mod 1_000_000)
    vals[i] = VariableValue(varnameId: vnId, typeId: tId, data: encInt(iv, tId))
  vals

proc assertEqualVals(got, expected: seq[VariableValue], ctx: string) =
  doAssert got.len == expected.len,
    ctx & ": var count mismatch: got " & $got.len & " expected " & $expected.len
  for v in 0 ..< got.len:
    doAssert got[v].varnameId == expected[v].varnameId,
      ctx & " var " & $v & ": varnameId mismatch"
    doAssert got[v].typeId == expected[v].typeId,
      ctx & " var " & $v & ": typeId mismatch (got " & $got[v].typeId &
      " expected " & $expected[v].typeId & ")"
    doAssert got[v].data == expected[v].data,
      ctx & " var " & $v & ": data mismatch"

# ---------------------------------------------------------------------------
# test_value_stream_write_read — multi-chunk round trip
# ---------------------------------------------------------------------------

proc test_value_stream_write_read() {.raises: [].} =
  const numSteps = 10_000
  const numChecks = 100

  var ctfs = createCtfs()
  # Small chunk size so the 10K-step stream spans many chunks (exercises the
  # per-chunk seek + the multi-chunk record-count recovery).
  let writerRes = initValueStreamWriter(ctfs, chunkSize = 64)
  doAssert writerRes.isOk, "initValueStreamWriter failed: " & writerRes.error
  var writer = writerRes.get()

  var writeRng = initRng(42)
  for i in 0 ..< numSteps:
    let numVars = int(writeRng.next() mod 4) + 2  # 2 to 5
    let vals = makeValues(writeRng, numVars)
    let r = writeStepValues(ctfs, writer, vals)
    doAssert r.isOk, "writeStepValues failed at step " & $i & ": " & r.error
  let fr = value_stream.flush(ctfs, writer)
  doAssert fr.isOk, "flush failed: " & fr.error

  let rawBytes = ctfs.toBytes()
  let readerRes = initValueStreamReader(rawBytes)
  doAssert readerRes.isOk, "initValueStreamReader failed: " & readerRes.error
  var reader = readerRes.get()
  doAssert reader.count == uint64(numSteps),
    "count mismatch: got " & $reader.count & " expected " & $numSteps

  var checkRng = initRng(99)
  for check in 0 ..< numChecks:
    let stepIdx = int(checkRng.next() mod uint64(numSteps))

    var replayRng = initRng(42)
    for s in 0 ..< stepIdx:
      let numVars = int(replayRng.next() mod 4) + 2
      discard makeValues(replayRng, numVars)
    let expectedCount = int(replayRng.next() mod 4) + 2
    let expected = makeValues(replayRng, expectedCount)

    let readRes = readStepValues(reader, uint64(stepIdx))
    doAssert readRes.isOk,
      "readStepValues failed at step " & $stepIdx & ": " & readRes.error
    assertEqualVals(readRes.get(), expected, "step " & $stepIdx)

  echo "PASS: test_value_stream_write_read"

# ---------------------------------------------------------------------------
# test_value_stream_empty_record — parallel-index invariant with empties
# ---------------------------------------------------------------------------

proc test_value_stream_empty_record() {.raises: [].} =
  var ctfs = createCtfs()
  let writerRes = initValueStreamWriter(ctfs, chunkSize = 4)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  var rng = initRng(7)
  let vals0 = makeValues(rng, 3)
  doAssert writeStepValues(ctfs, writer, vals0).isOk
  doAssert writeStepValues(ctfs, writer, newSeq[VariableValue]()).isOk  # step 1 empty
  let vals2 = makeValues(rng, 1)
  doAssert writeStepValues(ctfs, writer, vals2).isOk
  doAssert writeStepValues(ctfs, writer, newSeq[VariableValue]()).isOk  # step 3 empty
  let vals4 = makeValues(rng, 2)
  doAssert writeStepValues(ctfs, writer, vals4).isOk
  for i in 5 .. 9:
    if i mod 2 == 0:
      doAssert writeStepValues(ctfs, writer, makeValues(rng, 1)).isOk
    else:
      doAssert writeStepValues(ctfs, writer, newSeq[VariableValue]()).isOk
  doAssert value_stream.flush(ctfs, writer).isOk

  let rawBytes = ctfs.toBytes()
  let readerRes = initValueStreamReader(rawBytes)
  doAssert readerRes.isOk
  var reader = readerRes.get()
  doAssert reader.count == 10

  let got1 = readStepValues(reader, 1)
  doAssert got1.isOk and got1.get().len == 0, "step 1 should be empty"
  let got3 = readStepValues(reader, 3)
  doAssert got3.isOk and got3.get().len == 0, "step 3 should be empty"

  let got0 = readStepValues(reader, 0)
  doAssert got0.isOk
  assertEqualVals(got0.get(), vals0, "step 0")

  let got2 = readStepValues(reader, 2)
  doAssert got2.isOk
  assertEqualVals(got2.get(), vals2, "step 2")

  let got4 = readStepValues(reader, 4)
  doAssert got4.isOk and got4.get().len == 2

  echo "PASS: test_value_stream_empty_record"

# ---------------------------------------------------------------------------
# test_value_stream_many_variables — a single large record
# ---------------------------------------------------------------------------

proc test_value_stream_many_variables() {.raises: [].} =
  const numVars = 60

  var ctfs = createCtfs()
  let writerRes = initValueStreamWriter(ctfs)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  var rng = initRng(123)
  let vals = makeValues(rng, numVars)
  doAssert writeStepValues(ctfs, writer, vals).isOk
  doAssert value_stream.flush(ctfs, writer).isOk

  let rawBytes = ctfs.toBytes()
  let readerRes = initValueStreamReader(rawBytes)
  doAssert readerRes.isOk
  var reader = readerRes.get()
  doAssert reader.count == 1

  let got = readStepValues(reader, 0)
  doAssert got.isOk, "readStepValues failed: " & got.error
  assertEqualVals(got.get(), vals, "single step")

  echo "PASS: test_value_stream_many_variables"

# ---------------------------------------------------------------------------
# test_value_stream_legacy_back_compat — old .off VRT bundles still read
# ---------------------------------------------------------------------------

proc encodeLegacyRecord(values: openArray[VariableValue]): seq[byte] =
  ## Encode one step's values in the PRE-M24a-2 legacy framing:
  ## ``varint count, count × (varint varnameId, varint typeId, varint dataLen,
  ## data)``.  Used to synthesize an old-format ``values.dat``/``values.off``
  ## VariableRecordTable so we can prove the reader's legacy path still works.
  var rec: seq[byte] = @[]
  encodeVarint(uint64(values.len), rec)
  for v in values:
    encodeVarint(v.varnameId, rec)
    encodeVarint(v.typeId, rec)
    encodeVarint(uint64(v.data.len), rec)
    rec.add(v.data)
  rec

proc test_value_stream_legacy_back_compat() {.raises: [].} =
  # Hand-build a legacy .off VariableRecordTable named "values" with the old
  # per-record framing (a separate verbatim typeId field), then read it via
  # the legacy reader path — this is exactly what the FFI reader selects when
  # the has_value_stream flag is clear (pre-M24a-2 Nim-v4 bundle).
  var ctfs = createCtfs()
  let tableRes = initVariableRecordTableWriter(ctfs, "values")
  doAssert tableRes.isOk
  var table = tableRes.get()

  var rng = initRng(555)
  var steps: seq[seq[VariableValue]] = @[]
  for i in 0 ..< 50:
    let vals =
      if i mod 7 == 0: newSeq[VariableValue]()  # value-less step
      else: makeValues(rng, int(rng.next() mod 3) + 1)
    steps.add(vals)
    let appendRes = ctfs.append(table, encodeLegacyRecord(vals))
    doAssert appendRes.isOk, "append legacy record failed: " & appendRes.error

  let rawBytes = ctfs.toBytes()
  let readerRes = initValueStreamReader(rawBytes, legacy = true)
  doAssert readerRes.isOk, "legacy reader init failed: " & readerRes.error
  var reader = readerRes.get()
  doAssert reader.count == 50,
    "legacy count mismatch: got " & $reader.count

  for i in 0 ..< 50:
    let got = readStepValues(reader, uint64(i))
    doAssert got.isOk, "legacy readStepValues failed at " & $i & ": " & got.error
    # In legacy mode the verbatim typeId is read straight off the wire (NOT
    # reconstructed from CBOR), so it round-trips exactly.
    assertEqualVals(got.get(), steps[i], "legacy step " & $i)

  echo "PASS: test_value_stream_legacy_back_compat"

# ---------------------------------------------------------------------------
# test_value_stream_assignment_events — tag-9 Assignment rides in the same
# record as the step's values, and both sides read back exactly
# ---------------------------------------------------------------------------

proc test_value_stream_assignment_events() {.raises: [].} =
  ## The tag-9 ``Assignment`` event (``trace-events.md`` §"Value Stream
  ## Events") shares a record with the step's tag-0 ``StepValues`` event, so
  ## every combination has to read back correctly through BOTH accessors:
  ## ``readStepValues`` must return the values and walk the assignments over,
  ## ``readStepAssignments`` must return the assignments and walk the values
  ## over, and neither may lose framing when the record carries both.
  ##
  ## The byte layout is asserted literally as well, because "byte-identical to
  ## the canonical Rust ``ValueStreamEvent::Assignment`` encoder" is the whole
  ## reason the Rust reader can read what this writer produces — and a
  ## round-trip through one implementation cannot notice the two drifting
  ## together.
  doAssert TagAssignment == 9'u8,
    "the Assignment tag is fixed by the spec at 9, got " & $TagAssignment

  # --- the wire layout, spelled out ---------------------------------------
  # u8 0x09, varint to, u8 pass_by, varint from_len, from_bytes
  block:
    var buf: seq[byte] = @[]
    encodeAssignmentEvent(300'u64, 1'u8, [0xAA'u8, 0xBB'u8], buf)
    var expected: seq[byte] = @[9'u8]
    encodeVarint(300'u64, expected)
    expected.add(1'u8)
    encodeVarint(2'u64, expected)
    expected.add(0xAA'u8)
    expected.add(0xBB'u8)
    doAssert buf == expected,
      "encodeAssignmentEvent layout drifted: got " & $buf & " expected " &
        $expected

  # --- four record shapes, written in order --------------------------------
  # 0: values only        1: assignments only
  # 2: both               3: neither (an empty record)
  var rng = initRng(4242)
  let step0Values = makeValues(rng, 3)
  let step2Values = makeValues(rng, 2)

  # (varnameId, passBy, rvalue bytes) for every assignment we write.
  let step1Assignments = @[
    (11'u64, 0'u8, @[1'u8, 2'u8, 3'u8]),
    (12'u64, 1'u8, @[4'u8])]
  let step2Assignments = @[
    (99'u64, 1'u8, @[7'u8, 8'u8])]

  proc encodeAll(entries: seq[(uint64, uint8, seq[byte])]): seq[byte] =
    var buf: seq[byte] = @[]
    for e in entries:
      encodeAssignmentEvent(e[0], e[1], e[2], buf)
    buf

  var ctfs = createCtfs()
  let writerRes = initValueStreamWriter(ctfs, chunkSize = 2)
  doAssert writerRes.isOk, "initValueStreamWriter failed: " & writerRes.error
  var writer = writerRes.get()

  let w0 = writeStepValues(ctfs, writer, step0Values)
  doAssert w0.isOk, "step 0 write failed: " & w0.error
  let w1 = writeStepValues(ctfs, writer, [], encodeAll(step1Assignments))
  doAssert w1.isOk, "step 1 write failed: " & w1.error
  let w2 = writeStepValues(ctfs, writer, step2Values, encodeAll(step2Assignments))
  doAssert w2.isOk, "step 2 write failed: " & w2.error
  let w3 = writeStepValues(ctfs, writer, [])
  doAssert w3.isOk, "step 3 write failed: " & w3.error
  let fr = value_stream.flush(ctfs, writer)
  doAssert fr.isOk, "flush failed: " & fr.error

  let readerRes = initValueStreamReader(ctfs.toBytes())
  doAssert readerRes.isOk, "initValueStreamReader failed: " & readerRes.error
  var reader = readerRes.get()
  doAssert reader.count == 4'u64,
    "record count mismatch: got " & $reader.count & " expected 4"

  proc assignmentsAt(r: var ValueStreamReader, idx: uint64):
      seq[AssignmentEventEntry] =
    let got = readStepAssignments(r, idx)
    doAssert got.isOk,
      "readStepAssignments failed at " & $idx & ": " & got.error
    got.get()

  proc valuesAt(r: var ValueStreamReader, idx: uint64): seq[VariableValue] =
    let got = readStepValues(r, idx)
    doAssert got.isOk, "readStepValues failed at " & $idx & ": " & got.error
    got.get()

  proc assertAssignments(got: seq[AssignmentEventEntry],
      expected: seq[(uint64, uint8, seq[byte])], ctx: string) =
    doAssert got.len == expected.len,
      ctx & ": assignment count mismatch, got " & $got.len & " expected " &
        $expected.len
    for i in 0 ..< got.len:
      doAssert got[i].varnameId == expected[i][0],
        ctx & " #" & $i & ": target mismatch"
      doAssert got[i].passBy == expected[i][1],
        ctx & " #" & $i & ": pass_by mismatch"
      doAssert got[i].rvalueCbor == expected[i][2],
        ctx & " #" & $i & ": rvalue payload mismatch"

  # Step 0 — values only.  The assignment accessor must report NONE, which is
  # the control that keeps "step 1 has two" from passing for free.
  assertEqualVals(valuesAt(reader, 0'u64), step0Values, "step 0")
  assertAssignments(assignmentsAt(reader, 0'u64), @[], "step 0")

  # Step 1 — assignments only.  `readStepValues` must not error on the tag it
  # is walking over, and must report no values.
  doAssert valuesAt(reader, 1'u64).len == 0,
    "step 1 carries no StepValues event and must report no values"
  assertAssignments(assignmentsAt(reader, 1'u64), step1Assignments, "step 1")

  # Step 2 — BOTH in one record.  This is the framing case: each accessor has
  # to walk the other's event accurately to reach the end of the record.
  assertEqualVals(valuesAt(reader, 2'u64), step2Values, "step 2")
  assertAssignments(assignmentsAt(reader, 2'u64), step2Assignments, "step 2")

  # Step 3 — neither.
  doAssert valuesAt(reader, 3'u64).len == 0, "step 3 must report no values"
  assertAssignments(assignmentsAt(reader, 3'u64), @[], "step 3")

  echo "PASS: test_value_stream_assignment_events"

# ---------------------------------------------------------------------------
# test_value_stream_drop_variables_events — tag-3 DropVariables, and the
# three-way framing that proves every accessor walks the others' events
# ---------------------------------------------------------------------------

proc test_value_stream_drop_variables_events() {.raises: [].} =
  ## `trace-events.md` §"Value Stream Events" gives tag 3 as
  ## ``DropVariables {count: varint, ids: [varint]}`` — "Drop multiple
  ## variables (end of scope)".  This asserts that shape FIELD BY FIELD against
  ## the spec rather than against whatever the encoder happens to emit: a
  ## round-trip through one implementation cannot notice the encoder and the
  ## decoder drifting together, which is exactly how a wire format silently
  ## forks.
  ##
  ## The framing half matters more than the layout half.  A record is "the
  ## concatenation of zero-or-more tagged value-stream events", and tags below
  ## 10 are NOT self-delimiting — so an accessor that does not walk tag 3
  ## accurately mis-frames every event after it.  The three-event record below
  ## is the case that catches that, and it is the reason this test writes
  ## values, a drop, and an assignment into ONE record.
  doAssert TagDropVariables == 3'u8,
    "the DropVariables tag is fixed by the spec at 3, got " & $TagDropVariables

  # --- the wire layout, spelled out ---------------------------------------
  # u8 0x03, varint count, count × varint variable_id
  block:
    var buf: seq[byte] = @[]
    encodeDropVariablesEvent([7'u64, 300'u64], buf)
    var expected: seq[byte] = @[3'u8]
    encodeVarint(2'u64, expected)      # count
    encodeVarint(7'u64, expected)      # ids[0]
    encodeVarint(300'u64, expected)    # ids[1] — multi-byte varint
    doAssert buf == expected,
      "encodeDropVariablesEvent layout drifted: got " & $buf & " expected " &
        $expected

  # An empty drop still encodes as a well-formed event (tag + zero count), so
  # a decoder walking it advances by exactly two bytes.
  block:
    var buf: seq[byte] = @[]
    encodeDropVariablesEvent([], buf)
    doAssert buf == @[3'u8, 0'u8],
      "an empty DropVariables must encode as tag + count 0, got " & $buf

  # --- record shapes -------------------------------------------------------
  # 0: values only   1: drops only   2: values + drop + assignment   3: neither
  var rng = initRng(31337)
  let step0Values = makeValues(rng, 2)
  let step2Values = makeValues(rng, 3)

  # Two separate scope exits in one record: which ids left TOGETHER is what
  # makes a drop a scope boundary, so they must not be flattened into one.
  let step1Drops = @[@[4'u64, 5'u64], @[9'u64]]
  let step2Drops = @[@[21'u64]]

  proc encodeDrops(groups: seq[seq[uint64]]): seq[byte] =
    var buf: seq[byte] = @[]
    for g in groups:
      encodeDropVariablesEvent(g, buf)
    buf

  var ctfs = createCtfs()
  let writerRes = initValueStreamWriter(ctfs, chunkSize = 2)
  doAssert writerRes.isOk, "initValueStreamWriter failed: " & writerRes.error
  var writer = writerRes.get()

  let w0 = writeStepValues(ctfs, writer, step0Values)
  doAssert w0.isOk, "step 0 write failed: " & w0.error
  let w1 = writeStepValues(ctfs, writer, [], encodeDrops(step1Drops))
  doAssert w1.isOk, "step 1 write failed: " & w1.error
  # Step 2 carries a drop AND an assignment after its values — tag 0, then
  # tag 3, then tag 9, in one record.
  var step2Extra = encodeDrops(step2Drops)
  encodeAssignmentEvent(77'u64, 1'u8, [0xDE'u8, 0xAD'u8], step2Extra)
  let w2 = writeStepValues(ctfs, writer, step2Values, step2Extra)
  doAssert w2.isOk, "step 2 write failed: " & w2.error
  let w3 = writeStepValues(ctfs, writer, [])
  doAssert w3.isOk, "step 3 write failed: " & w3.error
  let fr = value_stream.flush(ctfs, writer)
  doAssert fr.isOk, "flush failed: " & fr.error

  let readerRes = initValueStreamReader(ctfs.toBytes())
  doAssert readerRes.isOk, "initValueStreamReader failed: " & readerRes.error
  var reader = readerRes.get()
  doAssert reader.count == 4'u64,
    "record count mismatch: got " & $reader.count & " expected 4"

  proc dropsAt(r: var ValueStreamReader, idx: uint64): seq[seq[uint64]] =
    let got = readStepDropVariables(r, idx)
    doAssert got.isOk,
      "readStepDropVariables failed at " & $idx & ": " & got.error
    got.get()

  proc valuesAt(r: var ValueStreamReader, idx: uint64): seq[VariableValue] =
    let got = readStepValues(r, idx)
    doAssert got.isOk, "readStepValues failed at " & $idx & ": " & got.error
    got.get()

  proc assignmentsAt(r: var ValueStreamReader, idx: uint64):
      seq[AssignmentEventEntry] =
    let got = readStepAssignments(r, idx)
    doAssert got.isOk,
      "readStepAssignments failed at " & $idx & ": " & got.error
    got.get()

  proc assertDrops(got, expected: seq[seq[uint64]], ctx: string) =
    doAssert got.len == expected.len,
      ctx & ": drop-event count mismatch, got " & $got.len & " expected " &
        $expected.len
    for i in 0 ..< got.len:
      doAssert got[i] == expected[i],
        ctx & " #" & $i & ": dropped ids mismatch, got " & $got[i] &
          " expected " & $expected[i]

  # Step 0 — values only.  The drop accessor reporting NONE is the control
  # that keeps "step 1 has two" from passing for free.
  assertEqualVals(valuesAt(reader, 0'u64), step0Values, "step 0")
  assertDrops(dropsAt(reader, 0'u64), @[], "step 0")

  # Step 1 — drops only.  `readStepValues` must walk the tag it does not
  # report and reach the end of the record without erroring.
  doAssert valuesAt(reader, 1'u64).len == 0,
    "step 1 carries no StepValues event and must report no values"
  assertDrops(dropsAt(reader, 1'u64), step1Drops, "step 1")

  # Step 2 — the three-way framing case.  Each accessor has to walk the other
  # TWO events accurately to reach the end of the record, so a mis-framed
  # tag 3 surfaces here as a wrong assignment or a lost value, not just as a
  # wrong drop.
  assertEqualVals(valuesAt(reader, 2'u64), step2Values, "step 2")
  assertDrops(dropsAt(reader, 2'u64), step2Drops, "step 2")
  let step2Assignments = assignmentsAt(reader, 2'u64)
  doAssert step2Assignments.len == 1,
    "step 2 carries one assignment after its drop, got " &
      $step2Assignments.len
  doAssert step2Assignments[0].varnameId == 77'u64,
    "step 2 assignment target mismatch — the tag-3 walk mis-framed the tag-9 " &
      "event that follows it"
  doAssert step2Assignments[0].rvalueCbor == @[0xDE'u8, 0xAD'u8],
    "step 2 assignment payload mismatch after the tag-3 walk"

  # Step 3 — neither.
  doAssert valuesAt(reader, 3'u64).len == 0, "step 3 must report no values"
  assertDrops(dropsAt(reader, 3'u64), @[], "step 3")

  echo "PASS: test_value_stream_drop_variables_events"

# ---------------------------------------------------------------------------
# test_value_stream_drop_variable_events — tag-2 DropVariable, and that it is
# never confused with its plural neighbour
# ---------------------------------------------------------------------------

proc test_value_stream_drop_variable_events() {.raises: [].} =
  ## `trace-events.md` §"Value Stream Events" gives tag 2 as
  ## ``DropVariable {variable_id: varint}`` — "Drop a single variable".
  ##
  ## The interesting case is not tag 2 alone but tag 2 NEXT TO tag 3.  The two
  ## are adjacent in the tag space and adjacent in meaning, and the failure
  ## that matters is not a crash — it is one being reported as the other, so a
  ## lone variable ending its life reads as a scope boundary that the program
  ## never had.  Each accessor must therefore see ONLY its own tag, in a record
  ## that carries both.
  doAssert TagDropVariable == 2'u8,
    "the DropVariable tag is fixed by the spec at 2, got " & $TagDropVariable

  # --- the wire layout, spelled out ---------------------------------------
  # u8 0x02, varint variable_id
  block:
    var buf: seq[byte] = @[]
    encodeDropVariableEvent(300'u64, buf)
    var expected: seq[byte] = @[2'u8]
    encodeVarint(300'u64, expected)    # multi-byte varint
    doAssert buf == expected,
      "encodeDropVariableEvent layout drifted: got " & $buf & " expected " &
        $expected

  # A single drop and a two-variable scope exit, in ONE record, with values
  # and an assignment around them — every tag the writer emits, at once.
  var rng = initRng(909090)
  let stepValues = makeValues(rng, 2)

  var extra: seq[byte] = @[]
  encodeDropVariableEvent(41'u64, extra)
  encodeDropVariablesEvent([50'u64, 51'u64], extra)
  encodeDropVariableEvent(42'u64, extra)
  encodeAssignmentEvent(60'u64, 0'u8, [0x01'u8], extra)

  var ctfs = createCtfs()
  let writerRes = initValueStreamWriter(ctfs, chunkSize = 2)
  doAssert writerRes.isOk, "initValueStreamWriter failed: " & writerRes.error
  var writer = writerRes.get()

  let w0 = writeStepValues(ctfs, writer, stepValues, extra)
  doAssert w0.isOk, "step 0 write failed: " & w0.error
  let w1 = writeStepValues(ctfs, writer, [])
  doAssert w1.isOk, "step 1 write failed: " & w1.error
  let fr = value_stream.flush(ctfs, writer)
  doAssert fr.isOk, "flush failed: " & fr.error

  let readerRes = initValueStreamReader(ctfs.toBytes())
  doAssert readerRes.isOk, "initValueStreamReader failed: " & readerRes.error
  var reader = readerRes.get()

  # Tag 2 must report BOTH singular drops and NEITHER of the plural event's
  # ids — that is the confusion this test exists to catch.
  let singular = readStepDropVariable(reader, 0'u64)
  doAssert singular.isOk, "readStepDropVariable failed: " & singular.error
  doAssert singular.get() == @[41'u64, 42'u64],
    "tag-2 accessor must report exactly the singular drops, in wire order; got " &
      $singular.get()

  # Tag 3 must report the scope exit and NOT the two singular drops.
  let plural = readStepDropVariables(reader, 0'u64)
  doAssert plural.isOk, "readStepDropVariables failed: " & plural.error
  doAssert plural.get() == @[@[50'u64, 51'u64]],
    "tag-3 accessor must report exactly the scope exit, not the singular " &
      "drops beside it; got " & $plural.get()

  # The neighbours still frame correctly with two new tags between them.
  let vals = readStepValues(reader, 0'u64)
  doAssert vals.isOk, "readStepValues failed: " & vals.error
  assertEqualVals(vals.get(), stepValues, "step 0")

  let assigns = readStepAssignments(reader, 0'u64)
  doAssert assigns.isOk, "readStepAssignments failed: " & assigns.error
  doAssert assigns.get().len == 1 and assigns.get()[0].varnameId == 60'u64,
    "the assignment after two drop kinds must still frame correctly; got " &
      $assigns.get().len & " assignment(s)"

  # A record with neither drop kind reports neither — the control that stops
  # the assertions above passing for free.
  let noneSingular = readStepDropVariable(reader, 1'u64)
  doAssert noneSingular.isOk and noneSingular.get().len == 0,
    "step 1 carries no tag-2 event and must report none"
  let nonePlural = readStepDropVariables(reader, 1'u64)
  doAssert nonePlural.isOk and nonePlural.get().len == 0,
    "step 1 carries no tag-3 event and must report none"

  echo "PASS: test_value_stream_drop_variable_events"

# ---------------------------------------------------------------------------
# test_value_stream_unknown_tag_is_refused_by_name — the forward-compat rule
# ---------------------------------------------------------------------------

proc test_value_stream_unknown_tag_is_refused_by_name() {.raises: [].} =
  ## Value-stream event tags < 10 are NOT self-delimiting, so a reader that
  ## meets an unhandled tag < 10 cannot walk past it and must refuse the whole
  ## record rather than guess a length and mis-frame everything after it.
  ##
  ## That refusal is a forward-compatibility hazard with a specific symptom —
  ## a binary older than a tag reports a step as having no variables — so the
  ## message has to NAME the remedy.
  var ctfs = createCtfs()
  let writerRes = initValueStreamWriter(ctfs, chunkSize = 4)
  doAssert writerRes.isOk, "initValueStreamWriter failed: " & writerRes.error
  var writer = writerRes.get()

  # A record carrying an unhandled tag < 10 (e.g. tag 5).
  let unknown: seq[byte] = @[0x05'u8, 0x01'u8]
  let w = writeStepValues(ctfs, writer, [], unknown)
  doAssert w.isOk, "write failed: " & w.error
  let fr = value_stream.flush(ctfs, writer)
  doAssert fr.isOk, "flush failed: " & fr.error

  let readerRes = initValueStreamReader(ctfs.toBytes())
  doAssert readerRes.isOk, "initValueStreamReader failed: " & readerRes.error
  var reader = readerRes.get()

  let got = readStepValues(reader, 0'u64)
  doAssert got.isErr, "an unknown value-stream tag < 10 must be REFUSED, not skipped"
  doAssert got.error.contains("5"),
    "the refusal must name the tag it could not walk: " & got.error
  doAssert got.error.contains("rebuild ct-print"),
    "the refusal must name the remedy (rebuilding a stale reader): " & got.error

  echo "PASS: test_value_stream_unknown_tag_is_refused_by_name"

# ---------------------------------------------------------------------------
# test_value_stream_forward_compat_tag_skipped — HX-S-5 / HX-OQ-8
# ---------------------------------------------------------------------------

proc test_value_stream_forward_compat_tag_skipped() {.raises: [].} =
  ## Under HX-S-5 / HX-OQ-8, value-stream event tags >= 10 are self-delimited
  ## by a varint length prefix.  An unknown tag >= 10 is skipped cleanly,
  ## preserving the step's visible variable values (StepValues).
  var rng = initRng(7777)
  let testValues = makeValues(rng, 2)

  var ctfs = createCtfs()
  let writerRes = initValueStreamWriter(ctfs, chunkSize = 4)
  doAssert writerRes.isOk, "initValueStreamWriter failed: " & writerRes.error
  var writer = writerRes.get()

  # Encode a forward-compatible unknown event tag 10 with a 4-byte payload.
  var extra: seq[byte] = @[]
  encodeLengthPrefixedEvent(10'u8, [0xDE'u8, 0xAD'u8, 0xBE'u8, 0xEF'u8], extra)

  # Write step 0 with visible values AND the unknown tag 10 event.
  let w = writeStepValues(ctfs, writer, testValues, extra)
  doAssert w.isOk, "write failed: " & w.error
  let fr = value_stream.flush(ctfs, writer)
  doAssert fr.isOk, "flush failed: " & fr.error

  let readerRes = initValueStreamReader(ctfs.toBytes())
  doAssert readerRes.isOk, "initValueStreamReader failed: " & readerRes.error
  var reader = readerRes.get()

  let got = readStepValues(reader, 0'u64)
  doAssert got.isOk, "tag >= 10 must be SKIPPED, not refused: " & (if got.isErr: got.error else: "")
  assertEqualVals(got.get(), testValues, "forward-compat step 0 values preserved")

  # Verify the reader recorded the skipped tag.
  doAssert reader.lastSkippedTags == @[10'u8],
    "lastSkippedTags must record skipped tag 10, got: " & $reader.lastSkippedTags
  doAssert reader.skippedTags == @[10'u8],
    "skippedTags must record skipped tag 10, got: " & $reader.skippedTags
  doAssert reader.skippedTagCounts == @[(10'u8, 1)],
    "skippedTagCounts mismatch: " & $reader.skippedTagCounts

  echo "PASS: test_value_stream_forward_compat_tag_skipped"

proc test_value_stream_forward_compat_truncated_payload_refused() {.raises: [].} =
  ## If a tag >= 10 specifies a length prefix that extends beyond the record,
  ## the reader must report a truncation error rather than panic or overrun.
  var ctfs = createCtfs()
  let writerRes = initValueStreamWriter(ctfs, chunkSize = 4)
  doAssert writerRes.isOk, "initValueStreamWriter failed: " & writerRes.error
  var writer = writerRes.get()

  # Tag 127 with declared payload length of 10 bytes, but only 2 bytes provided.
  let truncated: seq[byte] = @[127'u8, 10'u8, 0xAA'u8, 0xBB'u8]
  let w = writeStepValues(ctfs, writer, [], truncated)
  doAssert w.isOk, "write failed: " & w.error
  let fr = value_stream.flush(ctfs, writer)
  doAssert fr.isOk, "flush failed: " & fr.error

  let readerRes = initValueStreamReader(ctfs.toBytes())
  doAssert readerRes.isOk, "initValueStreamReader failed: " & readerRes.error
  var reader = readerRes.get()

  let got = readStepValues(reader, 0'u64)
  doAssert got.isErr, "truncated tag >= 10 must be refused with truncation error"
  doAssert got.error.contains("truncated payload in value-stream event tag 127"),
    "error must describe truncation: " & got.error

  echo "PASS: test_value_stream_forward_compat_truncated_payload_refused"

# Run all tests
test_value_stream_write_read()
test_value_stream_empty_record()
test_value_stream_many_variables()
test_value_stream_legacy_back_compat()
test_value_stream_assignment_events()
test_value_stream_drop_variables_events()
test_value_stream_drop_variable_events()
test_value_stream_unknown_tag_is_refused_by_name()
test_value_stream_forward_compat_tag_skipped()
test_value_stream_forward_compat_truncated_payload_refused()

