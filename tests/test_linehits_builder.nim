{.push raises: [].}

## Tests and benchmarks for LinehitsBuilder (M44).

import std/times
import results
import codetracer_trace_writer/linehits_builder
import codetracer_trace_writer/multi_stream_writer

# ---------------------------------------------------------------------------
# Test: materialized builder (standalone)
# ---------------------------------------------------------------------------

proc test_linehits_materialized_builder() =
  ## Create linehits builder, record 1000 hits across 50 lines,
  ## finalize, then verify each line has the correct step_ids.
  var b = initLinehitsBuilder()

  # Record 1000 hits: step i hits line (i mod 50)
  const NumSteps = 1000
  const NumLines = 50

  # Build expected mapping: expected[line] = seq of step_ids
  var expected: array[NumLines, seq[uint64]]
  for i in 0 ..< NumSteps:
    let line = i mod NumLines
    let stepId = uint64(i)
    b.recordHit(uint64(line), stepId)
    expected[line].add(stepId)

  doAssert b.lineCount == NumLines,
    "lineCount: got " & $b.lineCount & " want " & $NumLines

  let finRes = b.finalize()
  doAssert finRes.isOk, "finalize failed: " & finRes.error

  # Verify each line
  for line in 0 ..< NumLines:
    let hitsRes = b.lookupHits(uint64(line))
    doAssert hitsRes.isOk, "lookupHits failed for line " & $line &
      ": " & hitsRes.error
    let hits = hitsRes.get()
    let exp = expected[line]
    doAssert hits.len == exp.len,
      "line " & $line & ": got " & $hits.len & " hits, want " & $exp.len
    for i in 0 ..< hits.len:
      doAssert hits[i] == exp[i],
        "line " & $line & " hit " & $i & ": got " & $hits[i] &
        " want " & $exp[i]

    # Verify hitCount matches
    doAssert b.hitCount(uint64(line)) == hits.len,
      "hitCount mismatch for line " & $line

  # Verify missing line returns error
  let missRes = b.lookupHits(uint64(NumLines + 100))
  doAssert missRes.isErr, "expected error for missing line"

  echo "PASS: test_linehits_materialized_builder"

# ---------------------------------------------------------------------------
# Test: double finalize rejected
# ---------------------------------------------------------------------------

proc test_linehits_double_finalize() =
  var b = initLinehitsBuilder()
  b.recordHit(0, 0)
  let r1 = b.finalize()
  doAssert r1.isOk
  let r2 = b.finalize()
  doAssert r2.isErr, "double finalize should fail"

  echo "PASS: test_linehits_double_finalize"

# ---------------------------------------------------------------------------
# Test: lookup before finalize rejected
# ---------------------------------------------------------------------------

proc test_linehits_lookup_before_finalize() =
  var b = initLinehitsBuilder()
  b.recordHit(0, 0)
  let res = b.lookupHits(0)
  doAssert res.isErr, "lookup before finalize should fail"

  echo "PASS: test_linehits_lookup_before_finalize"

# ---------------------------------------------------------------------------
# Test: via MultiStreamTraceWriter
# ---------------------------------------------------------------------------

proc test_linehits_via_multi_stream_writer() =
  ## Create MultiStreamTraceWriter with linehits enabled,
  ## write 100 steps across 5 source lines, close, then
  ## verify each line has the correct step_ids.
  let writerRes = initMultiStreamWriter("test_lh.ct", "linehits_test")
  doAssert writerRes.isOk, "initMultiStreamWriter failed: " & writerRes.error
  var w = writerRes.get()

  w.enableLinehits()

  let p0 = w.registerPath("/src/main.py")
  doAssert p0.isOk

  # Write 100 steps: step i hits line (i mod 5) + 1
  const NumSteps = 100
  const NumLines = 5

  # Build expected mapping (using global line index)
  # With DefaultLinesPerFile=100000, file 0, line L -> GLI = L
  # Lines are 1..5, so GLI values are 1..5
  var expected: array[NumLines, seq[uint64]]  # index 0..4 maps to GLI 1..5
  for i in 0 ..< NumSteps:
    let line = uint64((i mod NumLines) + 1)
    let stepId = uint64(i)
    expected[i mod NumLines].add(stepId)

    let res = w.registerStep(0, line, @[])
    doAssert res.isOk, "registerStep " & $i & " failed: " & res.error

  doAssert w.stepCount == uint64(NumSteps)

  let closeRes = w.close()
  doAssert closeRes.isOk, "close failed: " & closeRes.error

  # Verify linehits via the builder (still accessible after close)
  for idx in 0 ..< NumLines:
    let gli = uint64(idx + 1)
    let exp = expected[idx]
    let hitsRes = w.linehits.lookupHits(gli)
    doAssert hitsRes.isOk, "lookupHits failed for GLI " & $gli &
      ": " & hitsRes.error
    let hits = hitsRes.get()
    doAssert hits.len == exp.len,
      "GLI " & $gli & ": got " & $hits.len & " hits, want " & $exp.len
    for i in 0 ..< hits.len:
      doAssert hits[i] == exp[i],
        "GLI " & $gli & " hit " & $i & ": got " & $hits[i] &
        " want " & $exp[i]

  w.closeCtfs()

  echo "PASS: test_linehits_via_multi_stream_writer"

# ---------------------------------------------------------------------------
# Test: writer without linehits still works
# ---------------------------------------------------------------------------

proc test_writer_without_linehits() =
  ## Verify that not enabling linehits doesn't break anything.
  let writerRes = initMultiStreamWriter("test_no_lh.ct", "no_linehits")
  doAssert writerRes.isOk
  var w = writerRes.get()

  let p0 = w.registerPath("/src/main.py")
  doAssert p0.isOk

  for i in 0 ..< 10:
    let res = w.registerStep(0, uint64(i + 1), @[])
    doAssert res.isOk

  let closeRes = w.close()
  doAssert closeRes.isOk

  w.closeCtfs()

  echo "PASS: test_writer_without_linehits"

# ---------------------------------------------------------------------------
# Bench: linehits builder overhead
# ---------------------------------------------------------------------------

proc bench_linehits_builder_overhead() =
  ## Record 100K steps without vs with linehits builder.
  ## Compare times.
  const N = 100_000
  const NumLines = 200

  # Without builder
  let t0 = cpuTime()
  block:
    let writerRes = initMultiStreamWriter("bench_no_lh.ct", "bench_no_lh")
    doAssert writerRes.isOk
    var w = writerRes.get()
    let p0 = w.registerPath("/src/main.py")
    doAssert p0.isOk
    for i in 0 ..< N:
      let line = uint64((i mod NumLines) + 1)
      let res = w.registerStep(0, line, @[])
      doAssert res.isOk
    let closeRes = w.close()
    doAssert closeRes.isOk
    w.closeCtfs()
  let elapsedWithout = cpuTime() - t0

  # With builder
  let t1 = cpuTime()
  block:
    let writerRes = initMultiStreamWriter("bench_with_lh.ct", "bench_with_lh")
    doAssert writerRes.isOk
    var w = writerRes.get()
    w.enableLinehits()
    let p0 = w.registerPath("/src/main.py")
    doAssert p0.isOk
    for i in 0 ..< N:
      let line = uint64((i mod NumLines) + 1)
      let res = w.registerStep(0, line, @[])
      doAssert res.isOk
    let closeRes = w.close()
    doAssert closeRes.isOk
    w.closeCtfs()
  let elapsedWith = cpuTime() - t1

  echo "{\"benchmark\": \"linehits_builder_overhead\", " &
    "\"steps\": " & $N & ", " &
    "\"without_sec\": " & $elapsedWithout & ", " &
    "\"with_sec\": " & $elapsedWith & ", " &
    "\"overhead_sec\": " & $(elapsedWith - elapsedWithout) & "}"

  echo "PASS: bench_linehits_builder_overhead"

# ---------------------------------------------------------------------------
# Bench: standalone builder 100K steps
# ---------------------------------------------------------------------------

proc bench_linehits_100k_steps() =
  ## Record 100K step hits into the builder, finalize, measure time.
  const N = 100_000
  const NumLines = 500

  var b = initLinehitsBuilder()

  let t0 = cpuTime()
  for i in 0 ..< N:
    b.recordHit(uint64(i mod NumLines), uint64(i))
  let recordTime = cpuTime() - t0

  let t1 = cpuTime()
  let finRes = b.finalize()
  doAssert finRes.isOk
  let finalizeTime = cpuTime() - t1

  # Verify a sample lookup
  let hitsRes = b.lookupHits(0)
  doAssert hitsRes.isOk
  doAssert hitsRes.get().len == N div NumLines

  echo "{\"benchmark\": \"linehits_100k_steps\", " &
    "\"steps\": " & $N & ", " &
    "\"lines\": " & $NumLines & ", " &
    "\"record_sec\": " & $recordTime & ", " &
    "\"finalize_sec\": " & $finalizeTime & ", " &
    "\"total_sec\": " & $(recordTime + finalizeTime) & "}"

  echo "PASS: bench_linehits_100k_steps"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

when isMainModule:
  test_linehits_materialized_builder()
  test_linehits_double_finalize()
  test_linehits_lookup_before_finalize()
  test_linehits_via_multi_stream_writer()
  test_writer_without_linehits()
  bench_linehits_builder_overhead()
  bench_linehits_100k_steps()
  echo "All linehits builder tests passed."
