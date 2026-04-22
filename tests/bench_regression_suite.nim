{.push raises: [].}

## Unified benchmark regression suite (M53).
##
## Runs all per-milestone benchmarks and outputs a single JSON report
## compatible with benchmark-action/github-action-benchmark.
##
## Output format: [{"name": "...", "unit": "...", "value": ...}, ...]
##
## Must be compiled with -d:release.

import std/[monotimes, times, algorithm]
import results
import codetracer_ctfs
import codetracer_ctfs/btree
import codetracer_ctfs/namespace
import codetracer_ctfs/namespace_descriptor
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/exec_stream
import codetracer_trace_writer/varint
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/interning_table
import codetracer_trace_writer/call_stream
import codetracer_trace_writer/io_event_stream

# ---------------------------------------------------------------------------
# Result accumulator
# ---------------------------------------------------------------------------

type BenchResult = object
  name: string
  unit: string
  value: float64

var benchResults: seq[BenchResult]

proc addResult(name, unit: string, value: float64) =
  benchResults.add(BenchResult(name: name, unit: unit, value: value))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc fillRecord(buf: var openArray[byte], index: int) =
  for i in 0 ..< buf.len:
    buf[i] = byte((index * 31 + i * 7) mod 256)

type Rng = object
  state: uint64

proc initRng(seed: uint64): Rng = Rng(state: seed)

proc next(r: var Rng): uint64 =
  r.state = r.state xor (r.state shl 13)
  r.state = r.state xor (r.state shr 7)
  r.state = r.state xor (r.state shl 17)
  r.state

proc randomData(r: var Rng, minLen, maxLen: int): seq[byte] =
  let length = minLen + int(r.next() mod uint64(maxLen - minLen + 1))
  result = newSeq[byte](length)
  for i in 0 ..< length:
    result[i] = byte(r.next() and 0xFF)

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

proc toBytes(s: string): seq[byte] {.raises: [].} =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

# ---------------------------------------------------------------------------
# M4: FixedRecordTable random access
# ---------------------------------------------------------------------------

proc bench_fixed_record_table() =
  const recordSize = 16
  const numRecords = 100_000
  const numReads = 100_000

  var ctfs = createCtfs()
  let writerRes = initFixedRecordTableWriter(ctfs, "bench.dat", recordSize)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  var rec: array[recordSize, byte]
  for i in 0 ..< numRecords:
    fillRecord(rec, i)
    let r = ctfs.append(writer, rec)
    doAssert r.isOk

  let rawBytes = ctfs.toBytes()
  let fileDataRes = readInternalFile(rawBytes, "bench.dat")
  doAssert fileDataRes.isOk
  let fileData = fileDataRes.get()
  let readerRes = initFixedRecordTableReader(fileData, recordSize)
  doAssert readerRes.isOk
  let reader = readerRes.get()

  var rng = initRng(12345)
  var indices = newSeq[uint64](numReads)
  for i in 0 ..< numReads:
    indices[i] = rng.next() mod uint64(numRecords)

  var buf: array[recordSize, byte]
  let startTime = getMonoTime()
  for i in 0 ..< numReads:
    let rr = reader.read(indices[i], buf)
    doAssert rr.isOk
  let endTime = getMonoTime()

  let totalNs = (endTime - startTime).inNanoseconds
  let perLookupNs = totalNs div int64(numReads)
  addResult("fixed_record_table_random_access", "ns", float64(perLookupNs))

# ---------------------------------------------------------------------------
# M5: VariableRecordTable seek
# ---------------------------------------------------------------------------

proc bench_variable_record_table() =
  const numRecords = 100_000
  const numReads = 100_000

  proc makeRecord(index: int, length: int): seq[byte] =
    var rec = newSeq[byte](length)
    for i in 0 ..< length:
      rec[i] = byte((index * 31 + i * 7) mod 256)
    rec

  var ctfs = createCtfs()
  let writerRes = initVariableRecordTableWriter(ctfs, "bench")
  doAssert writerRes.isOk
  var writer = writerRes.get()

  for i in 0 ..< numRecords:
    let length = (i mod 91) + 10
    let rec = makeRecord(i, length)
    let r = ctfs.append(writer, rec)
    doAssert r.isOk

  let rawBytes = ctfs.toBytes()
  let readerRes = initVariableRecordTableReader(rawBytes, "bench")
  doAssert readerRes.isOk
  let reader = readerRes.get()

  var rng = initRng(12345)
  var indices = newSeq[uint64](numReads)
  for i in 0 ..< numReads:
    indices[i] = rng.next() mod uint64(numRecords)

  let startTime = getMonoTime()
  for i in 0 ..< numReads:
    let rr = reader.read(indices[i])
    doAssert rr.isOk
  let endTime = getMonoTime()

  let totalNs = (endTime - startTime).inNanoseconds
  let perLookupNs = totalNs div int64(numReads)
  addResult("variable_record_table_seek", "ns", float64(perLookupNs))

# ---------------------------------------------------------------------------
# M6: ChunkedCompressedTable decompress + write
# ---------------------------------------------------------------------------

proc bench_chunked_compressed_table() =
  const recordSize = 16
  const numRecords = 1_000_000
  const chunkSize = 4096'u32
  const numReads = 1000

  # Write phase
  var ctfs = createCtfs()
  let writerRes = initChunkedCompressedTableWriter(ctfs, "benchd", recordSize, chunkSize)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  var rec: array[recordSize, byte]
  for i in 0 ..< numRecords:
    fillRecord(rec, i)
    let r = ctfs.append(writer, rec)
    doAssert r.isOk
  let flushRes = ctfs.flush(writer)
  doAssert flushRes.isOk

  let rawBytes = ctfs.toBytes()
  let readerRes = initChunkedCompressedTableReader(rawBytes, "benchd", recordSize)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  # Decompress benchmark
  var rng = initRng(54321)
  var indices = newSeq[uint64](numReads)
  for i in 0 ..< numReads:
    indices[i] = rng.next() mod uint64(numRecords)

  var buf: array[recordSize, byte]
  let startTime = getMonoTime()
  for i in 0 ..< numReads:
    let rr = reader.read(indices[i], buf)
    doAssert rr.isOk
  let endTime = getMonoTime()

  let totalNs = (endTime - startTime).inNanoseconds
  let perLookupNs = totalNs div int64(numReads)
  addResult("chunked_table_decompress", "ns", float64(perLookupNs))

  # Write throughput benchmark (smaller scale for suite)
  block:
    const writeRecords = 1_000_000
    var ctfs2 = createCtfs()
    let wr2 = initChunkedCompressedTableWriter(ctfs2, "benchw", recordSize, chunkSize)
    doAssert wr2.isOk
    var w2 = wr2.get()

    var rec2: array[recordSize, byte]
    for i in 0 ..< recordSize:
      rec2[i] = byte(i * 7)

    let start2 = getMonoTime()
    for i in 0 ..< writeRecords:
      rec2[0] = byte(i mod 256)
      rec2[1] = byte((i shr 8) mod 256)
      let r = ctfs2.append(w2, rec2)
      doAssert r.isOk
    let flush2 = ctfs2.flush(w2)
    doAssert flush2.isOk
    let end2 = getMonoTime()

    let wNs = (end2 - start2).inNanoseconds
    let recsPerSec = int64(writeRecords) * 1_000_000_000'i64 div wNs
    addResult("chunked_table_write_throughput", "records/sec", float64(recsPerSec))

# ---------------------------------------------------------------------------
# M7: B-tree lookup
# ---------------------------------------------------------------------------

proc keyToDescriptor(key: uint64, size: int): seq[byte] =
  result = newSeq[byte](size)
  var k = key
  for i in 0 ..< 8:
    result[i] = byte(k and 0xFF)
    k = k shr 8

proc bench_btree_lookup() =
  const N = 1_000_000
  const Q = 100_000

  var tree = initBTree(8)
  for i in 0'u64 ..< N:
    let key = i * 10
    tree.insert(key, keyToDescriptor(key, 8))

  var rng = Rng(state: 12345'u64)
  var queryKeys = newSeq[uint64](Q)
  for i in 0 ..< Q:
    queryKeys[i] = (rng.next() mod N) * 10

  var durations = newSeq[int64](Q)
  for i in 0 ..< Q:
    let t0 = getMonoTime()
    let res = tree.lookup(queryKeys[i])
    let t1 = getMonoTime()
    doAssert res.isOk
    durations[i] = (t1 - t0).inNanoseconds

  sort(durations)
  let medianNs = durations[Q div 2]
  addResult("btree_lookup_median", "ns", float64(medianNs))

# ---------------------------------------------------------------------------
# M9: Namespace append + lookup
# ---------------------------------------------------------------------------

proc bench_namespace() =
  const N = 100_000
  const Q = 10_000
  var ns = initNamespace("bench_ns", ltTypeA)
  var rng = Rng(state: 123'u64)

  # Append benchmark
  let appendStart = cpuTime()
  for i in 0 ..< N:
    let data = rng.randomData(8, 32)
    let res = ns.append(uint64(i), data)
    doAssert res.isOk
  let appendElapsed = cpuTime() - appendStart
  let appendThroughput = float(N) / appendElapsed
  addResult("namespace_append_throughput", "entries/sec", appendThroughput)

  # Lookup benchmark
  var lookupRng = Rng(state: 789'u64)
  var queryKeys = newSeq[uint64](Q)
  for i in 0 ..< Q:
    queryKeys[i] = lookupRng.next() mod uint64(N)

  var durations = newSeq[int64](Q)
  for i in 0 ..< Q:
    let t0 = getMonoTime()
    let res = ns.lookup(queryKeys[i])
    let t1 = getMonoTime()
    doAssert res.isOk
    durations[i] = (t1 - t0).inNanoseconds

  sort(durations)
  let medianNs = durations[Q div 2]
  addResult("namespace_lookup_median", "ns", float64(medianNs))

# ---------------------------------------------------------------------------
# M10: DeltaStep bytes/step
# ---------------------------------------------------------------------------

proc bench_delta_step() =
  let totalSteps = 100_000
  var buf: seq[byte]

  for i in 0 ..< totalSteps:
    if i mod 10 == 0:
      encodeStepEvent(StepEvent(kind: sekAbsoluteStep, globalLineIndex: uint64(i * 2)), buf)
    else:
      encodeStepEvent(StepEvent(kind: sekDeltaStep, lineDelta: 1), buf)

  let bytesPerStep = float(buf.len) / float(totalSteps)
  addResult("delta_step_bytes_per_step", "bytes/step", bytesPerStep)

# ---------------------------------------------------------------------------
# M11: Varint throughput
# ---------------------------------------------------------------------------

proc bench_varint() =
  const N = 10_000_000

  var values: seq[uint64]
  values.setLen(N)
  var state: uint64 = 0xDEAD_BEEF_CAFE_1234'u64
  for i in 0 ..< N:
    state = state * 6364136223846793005'u64 + 1442695040888963407'u64
    let bucket = state shr 62
    case bucket
    of 0, 1:
      values[i] = (state shr 2) and 0x3FFF
    of 2:
      values[i] = (state shr 2) and 0xFFF_FFFF
    else:
      values[i] = state

  # Encode
  var buf = newSeq[byte](N * 10)
  var writePos = 0
  let encStart = getMonoTime()
  for i in 0 ..< N:
    encodeVarintTo(values[i], buf, writePos)
  let encEnd = getMonoTime()

  let totalBytes = writePos
  let encNs = float64((encEnd - encStart).inNanoseconds)
  let encOpsPerSec = float64(N) / (encNs / 1e9)
  addResult("varint_encode_throughput", "ops/sec", encOpsPerSec)

  # Decode
  buf.setLen(totalBytes)
  var readPos = 0
  let decStart = getMonoTime()
  for i in 0 ..< N:
    let r = decodeVarint(buf, readPos)
    doAssert r.isOk
  let decEnd = getMonoTime()

  let decNs = float64((decEnd - decStart).inNanoseconds)
  let decOpsPerSec = float64(N) / (decNs / 1e9)
  addResult("varint_decode_throughput", "ops/sec", decOpsPerSec)

# ---------------------------------------------------------------------------
# M13: ExecStream write throughput
# ---------------------------------------------------------------------------

proc bench_exec_stream_write() =
  let totalSteps = 1_000_000
  var ctfs = createCtfs()
  var writerRes = initExecStreamWriter(ctfs)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  let startTime = cpuTime()
  for i in 0 ..< totalSteps:
    var ev: StepEvent
    if i mod 10 == 0:
      ev = StepEvent(kind: sekAbsoluteStep, globalLineIndex: uint64(i * 2))
    else:
      ev = StepEvent(kind: sekDeltaStep, lineDelta: 1)
    let r = ctfs.writeEvent(writer, ev)
    doAssert r.isOk

  let flushRes = ctfs.flush(writer)
  doAssert flushRes.isOk
  let elapsed = cpuTime() - startTime

  let eventsPerSec = float(totalSteps) / elapsed
  addResult("exec_stream_write_throughput", "events/sec", eventsPerSec)

# ---------------------------------------------------------------------------
# M19: NewTraceReader step navigation + value load
# ---------------------------------------------------------------------------

proc writeLargeTrace(numSteps: int): seq[byte] =
  var ctfs = createCtfs()

  let metaFileRes = ctfs.addFile("meta.dat")
  doAssert metaFileRes.isOk
  var metaFile = metaFileRes.get()
  let meta = TraceMetadata(program: "bench", args: @[], workdir: "/tmp")
  let metaWr = ctfs.writeMetaDat(metaFile, meta, @["/src/bench.py"])
  doAssert metaWr.isOk

  let tabRes = initTraceInterningTables(ctfs)
  doAssert tabRes.isOk
  var tab = tabRes.get()
  discard ctfs.ensurePathId(tab, "/src/bench.py")
  discard ctfs.ensureFunctionId(tab, "bench_fn")
  discard ctfs.ensureTypeId(tab, "int")
  discard ctfs.ensureVarnameId(tab, "i")

  let execRes = initExecStreamWriter(ctfs)
  doAssert execRes.isOk
  var execW = execRes.get()

  let valRes = initValueStreamWriter(ctfs)
  doAssert valRes.isOk
  var valW = valRes.get()

  for i in 0 ..< numSteps:
    var ev: StepEvent
    if i == 0:
      ev = StepEvent(kind: sekAbsoluteStep, globalLineIndex: 0)
    else:
      ev = StepEvent(kind: sekDeltaStep, lineDelta: 1)
    let r = ctfs.writeEvent(execW, ev)
    doAssert r.isOk

    let iStr = intToStr(i)
    var iBytes = newSeq[byte](iStr.len)
    for j in 0 ..< iStr.len:
      iBytes[j] = byte(iStr[j])
    let vr = ctfs.writeStepValues(valW, @[
      VariableValue(varnameId: 0, typeId: 0, data: iBytes)])
    doAssert vr.isOk

  let flushRes = ctfs.flush(execW)
  doAssert flushRes.isOk

  result = ctfs.toBytes()
  ctfs.closeCtfs()

proc bench_new_trace_reader() =
  let numSteps = 10_000
  let data = writeLargeTrace(numSteps)

  let readerRes = openNewTraceFromBytes(data)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  let numNavigations = 1_000
  var seed: uint64 = 11111

  # Step navigation
  let stepStart = cpuTime()
  for trial in 0 ..< numNavigations:
    seed = (seed * 6364136223846793005'u64 + 1442695040888963407'u64)
    let idx = seed mod uint64(numSteps)
    let ev = reader.step(idx)
    doAssert ev.isOk
  let stepElapsed = cpuTime() - stepStart
  let avgStepUs = (stepElapsed * 1_000_000.0) / float(numNavigations)
  addResult("reader_navigate_to_step", "us", avgStepUs)

  # Value load
  seed = 22222
  let valStart = cpuTime()
  for trial in 0 ..< numNavigations:
    seed = (seed * 6364136223846793005'u64 + 1442695040888963407'u64)
    let idx = seed mod uint64(numSteps)
    let vals = reader.values(idx)
    doAssert vals.isOk
    doAssert vals.get().len == 1
  let valElapsed = cpuTime() - valStart
  let avgValUs = (valElapsed * 1_000_000.0) / float(numNavigations)
  addResult("reader_load_locals", "us", avgValUs)

# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------

proc emitJson() =
  var json = "["
  for i, r in benchResults:
    if i > 0: json.add(",")
    json.add("\n  {\"name\": \"")
    json.add(r.name)
    json.add("\", \"unit\": \"")
    json.add(r.unit)
    json.add("\", \"value\": ")
    # For integer-like values, emit without decimal point
    if r.value == float64(int64(r.value)) and r.value < 1e15:
      json.add($int64(r.value))
    else:
      json.add($r.value)
    json.add("}")
  json.add("\n]\n")
  echo json

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

echo "Running benchmark regression suite..."

bench_fixed_record_table()
echo "  [done] FixedRecordTable random access"

bench_variable_record_table()
echo "  [done] VariableRecordTable seek"

bench_chunked_compressed_table()
echo "  [done] ChunkedCompressedTable decompress + write"

bench_btree_lookup()
echo "  [done] B-tree lookup"

bench_namespace()
echo "  [done] Namespace append + lookup"

bench_delta_step()
echo "  [done] DeltaStep bytes/step"

bench_varint()
echo "  [done] Varint throughput"

bench_exec_stream_write()
echo "  [done] ExecStream write throughput"

bench_new_trace_reader()
echo "  [done] NewTraceReader navigation + value load"

echo ""
echo "=== Benchmark Report ==="
emitJson()
echo "PASS: bench_regression_suite"
