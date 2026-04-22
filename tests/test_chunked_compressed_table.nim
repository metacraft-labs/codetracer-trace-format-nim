when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Tests and benchmarks for ChunkedCompressedTable.

import std/monotimes
import std/times
import results
import codetracer_ctfs

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc fillRecord(buf: var openArray[byte], index: int) =
  ## Fill a record buffer with a deterministic pattern based on index.
  for i in 0 ..< buf.len:
    buf[i] = byte((index * 31 + i * 7) mod 256)

# Simple xorshift PRNG for reproducible random indices (no exceptions).
type Rng = object
  state: uint64

proc initRng(seed: uint64): Rng = Rng(state: seed)

proc next(r: var Rng): uint64 =
  r.state = r.state xor (r.state shl 13)
  r.state = r.state xor (r.state shr 7)
  r.state = r.state xor (r.state shl 17)
  r.state

# ---------------------------------------------------------------------------
# test_chunked_compressed_table_write_read
# ---------------------------------------------------------------------------

proc test_chunked_compressed_table_write_read() {.raises: [].} =
  const recordSize = 16
  const numRecords = 100_000
  const chunkSize = 4096'u32

  var ctfs = createCtfs()
  let writerRes = initChunkedCompressedTableWriter(ctfs, "steps", recordSize, chunkSize)
  doAssert writerRes.isOk, "initWriter failed: " & writerRes.error
  var writer = writerRes.get()

  var rec: array[recordSize, byte]
  for i in 0 ..< numRecords:
    fillRecord(rec, i)
    let r = ctfs.append(writer, rec)
    doAssert r.isOk, "append failed at record " & $i & ": " & r.error

  let flushRes = ctfs.flush(writer)
  doAssert flushRes.isOk, "flush failed: " & flushRes.error
  doAssert writer.count == uint64(numRecords),
    "count mismatch: " & $writer.count

  # Read back
  let rawBytes = ctfs.toBytes()
  let readerRes = initChunkedCompressedTableReader(rawBytes, "steps", recordSize)
  doAssert readerRes.isOk, "initReader failed: " & readerRes.error
  var reader = readerRes.get()
  doAssert reader.count == uint64(numRecords),
    "reader count mismatch: " & $reader.count & " expected " & $numRecords

  # Read 100 random records and verify
  var rng = initRng(42)
  var buf: array[recordSize, byte]
  var expected: array[recordSize, byte]
  for check in 0 ..< 100:
    let idx = int(rng.next() mod uint64(numRecords))
    let rr = reader.read(uint64(idx), buf)
    doAssert rr.isOk, "read failed at index " & $idx & ": " & rr.error
    fillRecord(expected, idx)
    for b in 0 ..< recordSize:
      doAssert buf[b] == expected[b],
        "byte mismatch at record " & $idx & " byte " & $b &
        ": got " & $buf[b] & " expected " & $expected[b]

  echo "PASS: test_chunked_compressed_table_write_read"

# ---------------------------------------------------------------------------
# test_chunked_compressed_table_random_access
# ---------------------------------------------------------------------------

proc test_chunked_compressed_table_random_access() {.raises: [].} =
  const recordSize = 16
  const numRecords = 50_000
  const chunkSize = 1024'u32  # smaller chunks to force more chunk switches

  var ctfs = createCtfs()
  let writerRes = initChunkedCompressedTableWriter(ctfs, "events", recordSize, chunkSize)
  doAssert writerRes.isOk, "initWriter failed: " & writerRes.error
  var writer = writerRes.get()

  var rec: array[recordSize, byte]
  for i in 0 ..< numRecords:
    fillRecord(rec, i)
    let r = ctfs.append(writer, rec)
    doAssert r.isOk, "append failed at record " & $i & ": " & r.error

  let flushRes = ctfs.flush(writer)
  doAssert flushRes.isOk, "flush failed: " & flushRes.error

  let rawBytes = ctfs.toBytes()
  let readerRes = initChunkedCompressedTableReader(rawBytes, "events", recordSize)
  doAssert readerRes.isOk, "initReader failed: " & readerRes.error
  var reader = readerRes.get()

  # Access records from many different chunks to exercise decompression
  var rng = initRng(999)
  var buf: array[recordSize, byte]
  var expected: array[recordSize, byte]
  let numChunks = int((numRecords + int(chunkSize) - 1) div int(chunkSize))

  # Pick one record from each chunk
  for chunkIdx in 0 ..< numChunks:
    let baseRecord = chunkIdx * int(chunkSize)
    let maxInChunk = min(int(chunkSize), numRecords - baseRecord)
    let recordInChunk = int(rng.next() mod uint64(maxInChunk))
    let globalIdx = baseRecord + recordInChunk

    let rr = reader.read(uint64(globalIdx), buf)
    doAssert rr.isOk, "read failed at index " & $globalIdx & ": " & rr.error
    fillRecord(expected, globalIdx)
    for b in 0 ..< recordSize:
      doAssert buf[b] == expected[b],
        "byte mismatch at record " & $globalIdx & " byte " & $b

  echo "PASS: test_chunked_compressed_table_random_access"

# ---------------------------------------------------------------------------
# test_chunked_compressed_partial_write
# ---------------------------------------------------------------------------

proc test_chunked_compressed_partial_write() {.raises: [].} =
  const recordSize = 16
  const chunkSize = 4096'u32

  # --- Sub-test: 100 records (less than one chunk) ---
  block:
    const numRecords = 100
    var ctfs = createCtfs()
    let writerRes = initChunkedCompressedTableWriter(ctfs, "partial", recordSize, chunkSize)
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
    let readerRes = initChunkedCompressedTableReader(rawBytes, "partial", recordSize)
    doAssert readerRes.isOk
    var reader = readerRes.get()
    doAssert reader.count == uint64(numRecords),
      "partial count mismatch: " & $reader.count & " expected " & $numRecords

    var buf: array[recordSize, byte]
    var expected: array[recordSize, byte]
    for i in 0 ..< numRecords:
      let rr = reader.read(uint64(i), buf)
      doAssert rr.isOk
      fillRecord(expected, i)
      for b in 0 ..< recordSize:
        doAssert buf[b] == expected[b]

  # --- Sub-test: exactly chunkSize records ---
  block:
    let numRecords = int(chunkSize)
    var ctfs = createCtfs()
    let writerRes = initChunkedCompressedTableWriter(ctfs, "exact", recordSize, chunkSize)
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
    let readerRes = initChunkedCompressedTableReader(rawBytes, "exact", recordSize)
    doAssert readerRes.isOk
    var reader = readerRes.get()
    doAssert reader.count == uint64(numRecords),
      "exact count mismatch: " & $reader.count & " expected " & $numRecords

    var buf: array[recordSize, byte]
    var expected: array[recordSize, byte]
    for i in 0 ..< numRecords:
      let rr = reader.read(uint64(i), buf)
      doAssert rr.isOk
      fillRecord(expected, i)
      for b in 0 ..< recordSize:
        doAssert buf[b] == expected[b]

  # --- Sub-test: chunkSize + 1 records ---
  block:
    let numRecords = int(chunkSize) + 1
    var ctfs = createCtfs()
    let writerRes = initChunkedCompressedTableWriter(ctfs, "plus1", recordSize, chunkSize)
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
    let readerRes = initChunkedCompressedTableReader(rawBytes, "plus1", recordSize)
    doAssert readerRes.isOk
    var reader = readerRes.get()
    doAssert reader.count == uint64(numRecords),
      "plus1 count mismatch: " & $reader.count & " expected " & $numRecords

    var buf: array[recordSize, byte]
    var expected: array[recordSize, byte]
    for i in 0 ..< numRecords:
      let rr = reader.read(uint64(i), buf)
      doAssert rr.isOk
      fillRecord(expected, i)
      for b in 0 ..< recordSize:
        doAssert buf[b] == expected[b]

  echo "PASS: test_chunked_compressed_partial_write"

# ---------------------------------------------------------------------------
# bench_chunked_table_decompress
# ---------------------------------------------------------------------------

proc bench_chunked_table_decompress() {.raises: [].} =
  const recordSize = 16
  const numRecords = 1_000_000
  const chunkSize = 4096'u32
  const numReads = 1000

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

  # Generate random indices from different chunks
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

  echo "{\"name\": \"chunked_table_decompress\", \"unit\": \"ns\", \"value\": " & $perLookupNs & "}"
  doAssert perLookupNs < 50000, "per-lookup latency too high: " & $perLookupNs & "ns (limit 50000ns)"

  echo "PASS: bench_chunked_table_decompress"

# ---------------------------------------------------------------------------
# bench_chunked_table_write_throughput
# ---------------------------------------------------------------------------

proc bench_chunked_table_write_throughput() {.raises: [].} =
  const recordSize = 16
  const numRecords = 10_000_000
  const chunkSize = 4096'u32

  var ctfs = createCtfs()
  let writerRes = initChunkedCompressedTableWriter(ctfs, "benchw", recordSize, chunkSize)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  var rec: array[recordSize, byte]
  # Pre-fill a pattern (same for all records in this throughput test)
  for i in 0 ..< recordSize:
    rec[i] = byte(i * 7)

  let startTime = getMonoTime()
  for i in 0 ..< numRecords:
    rec[0] = byte(i mod 256)
    rec[1] = byte((i shr 8) mod 256)
    let r = ctfs.append(writer, rec)
    doAssert r.isOk
  let flushRes = ctfs.flush(writer)
  doAssert flushRes.isOk
  let endTime = getMonoTime()

  let totalNs = (endTime - startTime).inNanoseconds
  let recordsPerSec = int64(numRecords) * 1_000_000_000'i64 div totalNs

  echo "{\"name\": \"chunked_table_write_throughput\", \"unit\": \"records/sec\", \"value\": " & $recordsPerSec & "}"
  # Note: this target requires -d:release to hit reliably
  when defined(release):
    doAssert recordsPerSec > 20_000_000,
      "write throughput too low: " & $recordsPerSec & " records/sec (limit 20M)"

  echo "PASS: bench_chunked_table_write_throughput"

# Run all tests
test_chunked_compressed_table_write_read()
test_chunked_compressed_table_random_access()
test_chunked_compressed_partial_write()
bench_chunked_table_decompress()
bench_chunked_table_write_throughput()
