when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Tests for the value stream writer/reader.

import results
import codetracer_ctfs/container
import codetracer_trace_writer/value_stream

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Simple xorshift PRNG for reproducible random values (no exceptions).
type Rng = object
  state: uint64

proc initRng(seed: uint64): Rng = Rng(state: seed)

proc next(r: var Rng): uint64 =
  r.state = r.state xor (r.state shl 13)
  r.state = r.state xor (r.state shr 7)
  r.state = r.state xor (r.state shl 17)
  r.state

proc makeData(rng: var Rng, length: int): seq[byte] =
  var d = newSeq[byte](length)
  for i in 0 ..< length:
    d[i] = byte(rng.next() mod 256)
  d

proc makeValues(rng: var Rng, count: int): seq[VariableValue] =
  var vals = newSeq[VariableValue](count)
  for i in 0 ..< count:
    let dataLen = int(rng.next() mod 20) + 1
    vals[i] = VariableValue(
      varnameId: rng.next() mod 10000,
      typeId: rng.next() mod 500,
      data: makeData(rng, dataLen),
    )
  vals

# ---------------------------------------------------------------------------
# test_value_stream_write_read
# ---------------------------------------------------------------------------

proc test_value_stream_write_read() {.raises: [].} =
  const numSteps = 10_000
  const numChecks = 100

  var ctfs = createCtfs()
  let writerRes = initValueStreamWriter(ctfs)
  doAssert writerRes.isOk, "initValueStreamWriter failed: " & writerRes.error
  var writer = writerRes.get()

  # We need a second RNG to regenerate expected values for verification.
  # Write 10K steps, each with 2-5 variables.
  var writeRng = initRng(42)
  for i in 0 ..< numSteps:
    let numVars = int(writeRng.next() mod 4) + 2  # 2 to 5
    let vals = makeValues(writeRng, numVars)
    let r = writeStepValues(ctfs, writer, vals)
    doAssert r.isOk, "writeStepValues failed at step " & $i & ": " & r.error

  # Serialize and read back
  let rawBytes = ctfs.toBytes()
  let readerRes = initValueStreamReader(rawBytes)
  doAssert readerRes.isOk, "initValueStreamReader failed: " & readerRes.error
  let reader = readerRes.get()
  doAssert reader.count == uint64(numSteps),
    "count mismatch: got " & $reader.count & " expected " & $numSteps

  # Pick 100 random steps to verify
  var checkRng = initRng(99)
  for check in 0 ..< numChecks:
    let stepIdx = int(checkRng.next() mod uint64(numSteps))

    # Regenerate expected values for stepIdx by replaying the write RNG
    var replayRng = initRng(42)
    for s in 0 ..< stepIdx:
      let numVars = int(replayRng.next() mod 4) + 2
      discard makeValues(replayRng, numVars)
    let expectedCount = int(replayRng.next() mod 4) + 2
    let expected = makeValues(replayRng, expectedCount)

    let readRes = readStepValues(reader, uint64(stepIdx))
    doAssert readRes.isOk, "readStepValues failed at step " & $stepIdx & ": " & readRes.error
    let got = readRes.get()
    doAssert got.len == expected.len,
      "step " & $stepIdx & ": var count mismatch: got " & $got.len & " expected " & $expected.len

    for v in 0 ..< got.len:
      doAssert got[v].varnameId == expected[v].varnameId,
        "step " & $stepIdx & " var " & $v & ": varnameId mismatch"
      doAssert got[v].typeId == expected[v].typeId,
        "step " & $stepIdx & " var " & $v & ": typeId mismatch"
      doAssert got[v].data == expected[v].data,
        "step " & $stepIdx & " var " & $v & ": data mismatch"

  echo "PASS: test_value_stream_write_read"

# ---------------------------------------------------------------------------
# test_value_stream_empty_record
# ---------------------------------------------------------------------------

proc test_value_stream_empty_record() {.raises: [].} =
  var ctfs = createCtfs()
  let writerRes = initValueStreamWriter(ctfs)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  # Step 0: 3 variables
  var rng = initRng(7)
  let vals0 = makeValues(rng, 3)
  let r0 = writeStepValues(ctfs, writer, vals0)
  doAssert r0.isOk

  # Step 1: 0 variables (empty)
  let r1 = writeStepValues(ctfs, writer, newSeq[VariableValue]())
  doAssert r1.isOk

  # Step 2: 1 variable
  let vals2 = makeValues(rng, 1)
  let r2 = writeStepValues(ctfs, writer, vals2)
  doAssert r2.isOk

  # Step 3: 0 variables (empty)
  let r3 = writeStepValues(ctfs, writer, newSeq[VariableValue]())
  doAssert r3.isOk

  # Step 4: 2 variables
  let vals4 = makeValues(rng, 2)
  let r4 = writeStepValues(ctfs, writer, vals4)
  doAssert r4.isOk

  # Steps 5-9: alternating empty/non-empty
  for i in 5 .. 9:
    if i mod 2 == 0:
      let vals = makeValues(rng, 1)
      let r = writeStepValues(ctfs, writer, vals)
      doAssert r.isOk
    else:
      let r = writeStepValues(ctfs, writer, newSeq[VariableValue]())
      doAssert r.isOk

  # Serialize and read back
  let rawBytes = ctfs.toBytes()
  let readerRes = initValueStreamReader(rawBytes)
  doAssert readerRes.isOk
  let reader = readerRes.get()
  doAssert reader.count == 10

  # Step 1 should be empty
  let got1 = readStepValues(reader, 1)
  doAssert got1.isOk
  doAssert got1.get().len == 0, "step 1 should have 0 variables, got " & $got1.get().len

  # Step 3 should be empty
  let got3 = readStepValues(reader, 3)
  doAssert got3.isOk
  doAssert got3.get().len == 0, "step 3 should have 0 variables"

  # Step 0 should have 3 variables
  let got0 = readStepValues(reader, 0)
  doAssert got0.isOk
  doAssert got0.get().len == 3, "step 0 should have 3 variables, got " & $got0.get().len

  # Verify step 0 values match
  for v in 0 ..< 3:
    doAssert got0.get()[v].varnameId == vals0[v].varnameId
    doAssert got0.get()[v].typeId == vals0[v].typeId
    doAssert got0.get()[v].data == vals0[v].data

  # Step 2 should have 1 variable
  let got2 = readStepValues(reader, 2)
  doAssert got2.isOk
  doAssert got2.get().len == 1, "step 2 should have 1 variable, got " & $got2.get().len
  doAssert got2.get()[0].varnameId == vals2[0].varnameId
  doAssert got2.get()[0].typeId == vals2[0].typeId
  doAssert got2.get()[0].data == vals2[0].data

  # Step 4 should have 2 variables
  let got4 = readStepValues(reader, 4)
  doAssert got4.isOk
  doAssert got4.get().len == 2

  echo "PASS: test_value_stream_empty_record"

# ---------------------------------------------------------------------------
# test_value_stream_many_variables
# ---------------------------------------------------------------------------

proc test_value_stream_many_variables() {.raises: [].} =
  const numVars = 60

  var ctfs = createCtfs()
  let writerRes = initValueStreamWriter(ctfs)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  var rng = initRng(123)
  let vals = makeValues(rng, numVars)
  let r = writeStepValues(ctfs, writer, vals)
  doAssert r.isOk, "writeStepValues with " & $numVars & " vars failed: " & r.error

  let rawBytes = ctfs.toBytes()
  let readerRes = initValueStreamReader(rawBytes)
  doAssert readerRes.isOk
  let reader = readerRes.get()
  doAssert reader.count == 1

  let got = readStepValues(reader, 0)
  doAssert got.isOk, "readStepValues failed: " & got.error
  let gotVals = got.get()
  doAssert gotVals.len == numVars,
    "expected " & $numVars & " variables, got " & $gotVals.len

  for i in 0 ..< numVars:
    doAssert gotVals[i].varnameId == vals[i].varnameId,
      "var " & $i & ": varnameId mismatch: got " & $gotVals[i].varnameId &
      " expected " & $vals[i].varnameId
    doAssert gotVals[i].typeId == vals[i].typeId,
      "var " & $i & ": typeId mismatch"
    doAssert gotVals[i].data == vals[i].data,
      "var " & $i & ": data mismatch"

  echo "PASS: test_value_stream_many_variables"

# Run all tests
test_value_stream_write_read()
test_value_stream_empty_record()
test_value_stream_many_variables()
