{.push raises: [].}

## Integration test: file-based trace access (M23b).
##
## Verifies that CTFS traces written to disk can be read back via the
## standard file API (simulating NFS/SSHFS or any POSIX file access).
##
## Tests:
##   1. Write multi-stream trace to a temp file, read back with openNewTrace()
##   2. Random block access works via file-based seeking
##   3. Companion index enables O(1) chunk lookup
##   4. Read latency measurement

import std/[times, os]
import results
import codetracer_ctfs/container
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/call_stream
import codetracer_trace_writer/io_event_stream

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc toBytes(s: string): seq[byte] {.raises: [].} =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

proc intToStr(n: int): string {.raises: [].} =
  if n == 0: return "0"
  var v = n
  var digits: seq[char]
  var neg = false
  if v < 0:
    neg = true
    v = -v
  while v > 0:
    digits.add(char(ord('0') + v mod 10))
    v = v div 10
  if neg: digits.add('-')
  result = newString(digits.len)
  for i in 0 ..< digits.len:
    result[i] = digits[digits.len - 1 - i]

proc getTempPath(): string {.raises: [].} =
  try:
    getTempDir() / "ctfs_file_access_test.ct"
  except OSError:
    "/tmp/ctfs_file_access_test.ct"

proc removeTempFile(path: string) {.raises: [].} =
  try:
    removeFile(path)
  except OSError:
    discard

# ---------------------------------------------------------------------------
# Test 1: Write multi-stream trace to file, read back
# ---------------------------------------------------------------------------

proc test_file_write_read_roundtrip() {.raises: [].} =
  let path = getTempPath()
  defer: removeTempFile(path)

  const numSteps = 500
  const numCalls = 5
  const numIOEvents = 3

  # Write trace using MultiStreamTraceWriter
  let writerRes = initMultiStreamWriter("test_file.ct", "file_test_prog")
  doAssert writerRes.isOk, "initMultiStreamWriter failed: " & writerRes.error
  var w = writerRes.get()

  w.metadata.args = @["--test"]
  w.metadata.workdir = "/tmp/test"

  let p0 = w.registerPath("/src/main.py")
  doAssert p0.isOk
  let p1 = w.registerPath("/src/helper.py")
  doAssert p1.isOk

  # Write steps with values
  for i in 0 ..< numSteps:
    let pathId = if i < numSteps div 2: 0'u64 else: 1'u64
    let line = uint64((i mod 50) + 1)
    let iStr = intToStr(i)
    let vals = @[VariableValue(
      varnameId: 0, typeId: 0, data: iStr.toBytes)]
    let res = w.registerStep(pathId, line, vals)
    doAssert res.isOk, "registerStep " & $i & " failed: " & res.error

  # Write IO events
  for i in 0 ..< numIOEvents:
    let msg = "output_" & $i & "\n"
    let res = w.registerIOEvent(ioStdout, msg.toBytes)
    doAssert res.isOk

  doAssert w.stepCount == numSteps

  let closeRes = w.close()
  doAssert closeRes.isOk, "close failed: " & closeRes.error

  # Write to disk
  let ctfsBytes = w.toBytes()
  w.closeCtfs()

  try:
    writeFile(path, ctfsBytes)
  except IOError:
    doAssert false, "failed to write temp file"
  except OSError:
    doAssert false, "OS error writing temp file"

  # Read back from disk using file-based API
  let readerRes = openNewTrace(path)
  doAssert readerRes.isOk, "openNewTrace failed: " & readerRes.error
  var reader = readerRes.get()

  # Verify metadata
  doAssert reader.meta.program == "file_test_prog",
    "program mismatch: " & reader.meta.program
  doAssert reader.meta.workdir == "/tmp/test",
    "workdir mismatch: " & reader.meta.workdir

  # Verify step count
  let sc = reader.stepCount()
  doAssert sc.isOk, "stepCount failed: " & sc.error
  doAssert sc.get() == uint64(numSteps),
    "stepCount mismatch: " & $sc.get() & " expected " & $numSteps

  # Verify paths
  doAssert reader.pathCount() == 2, "pathCount mismatch"
  let rp0 = reader.path(0)
  doAssert rp0.isOk and rp0.get() == "/src/main.py", "path 0 mismatch"
  let rp1 = reader.path(1)
  doAssert rp1.isOk and rp1.get() == "/src/helper.py", "path 1 mismatch"

  # Verify a few step values
  for idx in [0'u64, 100, 250, 499]:
    let vals = reader.values(idx)
    doAssert vals.isOk, "values at step " & $idx & " failed: " & vals.error
    let vs = vals.get()
    doAssert vs.len == 1, "expected 1 value at step " & $idx & ", got " & $vs.len
    let expected = intToStr(int(idx))
    var expectedBytes = newSeq[byte](expected.len)
    for j in 0 ..< expected.len:
      expectedBytes[j] = byte(expected[j])
    doAssert vs[0].data == expectedBytes,
      "value data mismatch at step " & $idx

  echo "PASS: test_file_write_read_roundtrip"

# ---------------------------------------------------------------------------
# Test 2: Random block access via file seeking
# ---------------------------------------------------------------------------

proc test_file_random_access() {.raises: [].} =
  let path = getTempPath()
  defer: removeTempFile(path)

  const numSteps = 2000

  let writerRes = initMultiStreamWriter("random.ct", "random_prog")
  doAssert writerRes.isOk
  var w = writerRes.get()

  let p0 = w.registerPath("/src/main.py")
  doAssert p0.isOk

  for i in 0 ..< numSteps:
    let iStr = intToStr(i)
    let vals = @[VariableValue(varnameId: 0, typeId: 0, data: iStr.toBytes)]
    let res = w.registerStep(0'u64, uint64((i mod 100) + 1), vals)
    doAssert res.isOk

  let closeRes = w.close()
  doAssert closeRes.isOk

  let ctfsBytes = w.toBytes()
  w.closeCtfs()

  try:
    writeFile(path, ctfsBytes)
  except IOError:
    doAssert false, "write failed"
  except OSError:
    doAssert false, "OS error"

  let readerRes = openNewTrace(path)
  doAssert readerRes.isOk, "openNewTrace failed: " & readerRes.error
  var reader = readerRes.get()

  # Access steps in random order (LCG-based pseudo-random)
  var seed: uint64 = 42
  for trial in 0 ..< 200:
    seed = (seed * 6364136223846793005'u64 + 1442695040888963407'u64)
    let idx = seed mod uint64(numSteps)

    let ev = reader.step(idx)
    doAssert ev.isOk, "step " & $idx & " failed: " & ev.error

    let vals = reader.values(idx)
    doAssert vals.isOk, "values " & $idx & " failed: " & vals.error
    let vs = vals.get()
    doAssert vs.len == 1

    let expected = intToStr(int(idx))
    var expectedBytes = newSeq[byte](expected.len)
    for j in 0 ..< expected.len:
      expectedBytes[j] = byte(expected[j])
    doAssert vs[0].data == expectedBytes,
      "value mismatch at step " & $idx

  echo "PASS: test_file_random_access"

# ---------------------------------------------------------------------------
# Test 3: Chunk index O(1) lookup verification
# ---------------------------------------------------------------------------

proc test_chunk_index_lookup() {.raises: [].} =
  ## Verify that the chunk index enables O(1) lookup by checking that
  ## accessing the first and last steps in a multi-chunk trace both work
  ## and return correct data.
  let path = getTempPath()
  defer: removeTempFile(path)

  const numSteps = 5000  # enough to span multiple exec stream chunks

  let writerRes = initMultiStreamWriter("chunks.ct", "chunk_prog")
  doAssert writerRes.isOk
  var w = writerRes.get()

  let p0 = w.registerPath("/src/main.py")
  doAssert p0.isOk

  for i in 0 ..< numSteps:
    let iStr = intToStr(i)
    let vals = @[VariableValue(varnameId: 0, typeId: 0, data: iStr.toBytes)]
    let res = w.registerStep(0'u64, uint64((i mod 200) + 1), vals)
    doAssert res.isOk

  let closeRes = w.close()
  doAssert closeRes.isOk

  let ctfsBytes = w.toBytes()
  w.closeCtfs()

  try:
    writeFile(path, ctfsBytes)
  except IOError:
    doAssert false, "write failed"
  except OSError:
    doAssert false, "OS error"

  let readerRes = openNewTrace(path)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  # Access first step
  let first = reader.step(0)
  doAssert first.isOk, "step 0 failed: " & first.error
  doAssert first.get().kind == sekAbsoluteStep

  let firstVals = reader.values(0)
  doAssert firstVals.isOk
  doAssert firstVals.get()[0].data == "0".toBytes

  # Access last step (crosses chunk boundary from first)
  let lastIdx = uint64(numSteps - 1)
  let last = reader.step(lastIdx)
  doAssert last.isOk, "step " & $lastIdx & " failed: " & last.error

  let lastVals = reader.values(lastIdx)
  doAssert lastVals.isOk
  let expectedLast = intToStr(numSteps - 1)
  var expectedBytes = newSeq[byte](expectedLast.len)
  for j in 0 ..< expectedLast.len:
    expectedBytes[j] = byte(expectedLast[j])
  doAssert lastVals.get()[0].data == expectedBytes,
    "last step value mismatch"

  # Access a step in the middle
  let midIdx = uint64(numSteps div 2)
  let mid = reader.step(midIdx)
  doAssert mid.isOk, "step " & $midIdx & " failed: " & mid.error
  let midVals = reader.values(midIdx)
  doAssert midVals.isOk
  let expectedMid = intToStr(int(midIdx))
  var expMidBytes = newSeq[byte](expectedMid.len)
  for j in 0 ..< expectedMid.len:
    expMidBytes[j] = byte(expectedMid[j])
  doAssert midVals.get()[0].data == expMidBytes

  echo "PASS: test_chunk_index_lookup"

# ---------------------------------------------------------------------------
# Test 4: Read latency measurement
# ---------------------------------------------------------------------------

proc bench_file_read_latency() {.raises: [].} =
  let path = getTempPath()
  defer: removeTempFile(path)

  const numSteps = 5000
  const numQueries = 500

  let writerRes = initMultiStreamWriter("latency.ct", "latency_prog")
  doAssert writerRes.isOk
  var w = writerRes.get()

  let p0 = w.registerPath("/src/main.py")
  doAssert p0.isOk

  for i in 0 ..< numSteps:
    let iStr = intToStr(i)
    let vals = @[VariableValue(varnameId: 0, typeId: 0, data: iStr.toBytes)]
    let res = w.registerStep(0'u64, uint64((i mod 100) + 1), vals)
    doAssert res.isOk

  let closeRes = w.close()
  doAssert closeRes.isOk

  let ctfsBytes = w.toBytes()
  w.closeCtfs()

  try:
    writeFile(path, ctfsBytes)
  except IOError:
    doAssert false, "write failed"
  except OSError:
    doAssert false, "OS error"

  let readerRes = openNewTrace(path)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  # Measure random access latency
  var seed: uint64 = 54321
  let start = cpuTime()
  for trial in 0 ..< numQueries:
    seed = (seed * 6364136223846793005'u64 + 1442695040888963407'u64)
    let idx = seed mod uint64(numSteps)
    let ev = reader.step(idx)
    doAssert ev.isOk
    let vals = reader.values(idx)
    doAssert vals.isOk
  let elapsed = cpuTime() - start

  let avgUs = (elapsed * 1_000_000.0) / float(numQueries)

  echo "{\"benchmark\": \"file_read_latency\", " &
    "\"queries\": " & $numQueries & ", " &
    "\"avg_us\": " & $avgUs & "}"

  # File-based access should complete within 1ms per query on any reasonable FS
  doAssert avgUs < 1000.0,
    "file read latency too high: " & $avgUs & "us > 1000us"

  echo "PASS: bench_file_read_latency"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

test_file_write_read_roundtrip()
test_file_random_access()
test_chunk_index_lookup()
bench_file_read_latency()
echo "ALL PASS: test_file_access"
