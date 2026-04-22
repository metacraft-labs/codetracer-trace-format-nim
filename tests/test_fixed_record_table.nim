when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Tests and benchmark for FixedRecordTable.

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
# test_fixed_record_table_write_read
# ---------------------------------------------------------------------------

proc test_fixed_record_table_write_read() {.raises: [].} =
  const recordSize = 16
  const numRecords = 10000

  var ctfs = createCtfs()
  let writerRes = initFixedRecordTableWriter(ctfs, "records.dat", recordSize)
  doAssert writerRes.isOk, "initFixedRecordTableWriter failed: " & writerRes.error
  var writer = writerRes.get()

  var rec: array[recordSize, byte]
  for i in 0 ..< numRecords:
    fillRecord(rec, i)
    let r = ctfs.append(writer, rec)
    doAssert r.isOk, "append failed at record " & $i & ": " & r.error

  doAssert writer.count == uint64(numRecords),
    "count mismatch: " & $writer.count

  # Read back
  let rawBytes = ctfs.toBytes()
  let fileDataRes = readInternalFile(rawBytes, "records.dat")
  doAssert fileDataRes.isOk, "readInternalFile failed: " & fileDataRes.error
  let fileData = fileDataRes.get()

  let readerRes = initFixedRecordTableReader(fileData, recordSize)
  doAssert readerRes.isOk, "initFixedRecordTableReader failed: " & readerRes.error
  let reader = readerRes.get()
  doAssert reader.count == uint64(numRecords),
    "reader count mismatch: " & $reader.count

  # Read 100 random indices and verify
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

  echo "PASS: test_fixed_record_table_write_read"

# ---------------------------------------------------------------------------
# test_fixed_record_table_edge_cases
# ---------------------------------------------------------------------------

proc test_fixed_record_table_edge_cases() {.raises: [].} =
  # -- Empty table --
  block:
    var ctfs = createCtfs()
    let writerRes = initFixedRecordTableWriter(ctfs, "empty.dat", 8)
    doAssert writerRes.isOk
    let writer = writerRes.get()
    doAssert writer.count == 0

    let rawBytes = ctfs.toBytes()
    let fileDataRes = readInternalFile(rawBytes, "empty.dat")
    doAssert fileDataRes.isOk, "readInternalFile failed for empty: " & fileDataRes.error
    let fileData = fileDataRes.get()
    let readerRes = initFixedRecordTableReader(fileData, 8)
    doAssert readerRes.isOk
    let reader = readerRes.get()
    doAssert reader.count == 0, "empty table count should be 0, got " & $reader.count
    var buf: array[8, byte]
    let rr = reader.read(0, buf)
    doAssert rr.isErr, "read at index 0 on empty table should return error"

  # -- Single record --
  block:
    var ctfs = createCtfs()
    let writerRes = initFixedRecordTableWriter(ctfs, "single.dat", 8)
    doAssert writerRes.isOk
    var writer = writerRes.get()
    var rec: array[8, byte]
    for i in 0 ..< 8:
      rec[i] = byte(0xAB + i)
    let r = ctfs.append(writer, rec)
    doAssert r.isOk
    doAssert writer.count == 1

    let rawBytes = ctfs.toBytes()
    let fileDataRes = readInternalFile(rawBytes, "single.dat")
    doAssert fileDataRes.isOk
    let fileData = fileDataRes.get()
    let readerRes = initFixedRecordTableReader(fileData, 8)
    doAssert readerRes.isOk
    let reader = readerRes.get()
    doAssert reader.count == 1
    var buf: array[8, byte]
    let rr = reader.read(0, buf)
    doAssert rr.isOk
    for i in 0 ..< 8:
      doAssert buf[i] == rec[i], "single record byte " & $i & " mismatch"

  # -- Block boundary: recordSize=4093 (just under 4096), 3 records --
  # The second record straddles a block boundary.
  block:
    const recSz = 4093
    var ctfs = createCtfs()
    let writerRes = initFixedRecordTableWriter(ctfs, "boundary.dat", recSz)
    doAssert writerRes.isOk
    var writer = writerRes.get()
    var rec = newSeq[byte](recSz)
    for r in 0 ..< 3:
      for i in 0 ..< recSz:
        rec[i] = byte((r * 17 + i * 3) mod 256)
      let res = ctfs.append(writer, rec)
      doAssert res.isOk, "append boundary record " & $r & " failed: " & res.error

    let rawBytes = ctfs.toBytes()
    let fileDataRes = readInternalFile(rawBytes, "boundary.dat")
    doAssert fileDataRes.isOk, "readInternalFile boundary failed: " & fileDataRes.error
    let fileData = fileDataRes.get()
    let readerRes = initFixedRecordTableReader(fileData, recSz)
    doAssert readerRes.isOk
    let reader = readerRes.get()
    doAssert reader.count == 3, "boundary count should be 3, got " & $reader.count

    var buf = newSeq[byte](recSz)
    for r in 0 ..< 3:
      let rr = reader.read(uint64(r), buf)
      doAssert rr.isOk, "read boundary record " & $r & " failed: " & rr.error
      for i in 0 ..< recSz:
        let expected = byte((r * 17 + i * 3) mod 256)
        doAssert buf[i] == expected,
          "boundary record " & $r & " byte " & $i &
          ": got " & $buf[i] & " expected " & $expected

  # -- Record exactly at block size: recordSize=4096, 2 records --
  block:
    const recSz = 4096
    var ctfs = createCtfs()
    let writerRes = initFixedRecordTableWriter(ctfs, "exact.dat", recSz)
    doAssert writerRes.isOk
    var writer = writerRes.get()
    var rec = newSeq[byte](recSz)
    for r in 0 ..< 2:
      for i in 0 ..< recSz:
        rec[i] = byte((r * 53 + i) mod 256)
      let res = ctfs.append(writer, rec)
      doAssert res.isOk, "append exact record " & $r & " failed: " & res.error

    let rawBytes = ctfs.toBytes()
    let fileDataRes = readInternalFile(rawBytes, "exact.dat")
    doAssert fileDataRes.isOk
    let fileData = fileDataRes.get()
    let readerRes = initFixedRecordTableReader(fileData, recSz)
    doAssert readerRes.isOk
    let reader = readerRes.get()
    doAssert reader.count == 2

    var buf = newSeq[byte](recSz)
    for r in 0 ..< 2:
      let rr = reader.read(uint64(r), buf)
      doAssert rr.isOk
      for i in 0 ..< recSz:
        let expected = byte((r * 53 + i) mod 256)
        doAssert buf[i] == expected,
          "exact record " & $r & " byte " & $i & " mismatch"

  # -- Small records: recordSize=1, 10000 records --
  block:
    const recSz = 1
    const numRecs = 10000
    var ctfs = createCtfs()
    let writerRes = initFixedRecordTableWriter(ctfs, "tiny.dat", recSz)
    doAssert writerRes.isOk
    var writer = writerRes.get()
    var rec: array[1, byte]
    for r in 0 ..< numRecs:
      rec[0] = byte(r mod 256)
      let res = ctfs.append(writer, rec)
      doAssert res.isOk

    let rawBytes = ctfs.toBytes()
    let fileDataRes = readInternalFile(rawBytes, "tiny.dat")
    doAssert fileDataRes.isOk
    let fileData = fileDataRes.get()
    let readerRes = initFixedRecordTableReader(fileData, recSz)
    doAssert readerRes.isOk
    let reader = readerRes.get()
    doAssert reader.count == uint64(numRecs)

    var buf: array[1, byte]
    for r in 0 ..< numRecs:
      let rr = reader.read(uint64(r), buf)
      doAssert rr.isOk
      doAssert buf[0] == byte(r mod 256),
        "tiny record " & $r & ": got " & $buf[0] & " expected " & $byte(r mod 256)

  echo "PASS: test_fixed_record_table_edge_cases"

# ---------------------------------------------------------------------------
# bench_fixed_record_table_random_access
# ---------------------------------------------------------------------------

proc bench_fixed_record_table_random_access() {.raises: [].} =
  const recordSize = 16
  const numRecords = 100_000
  const numReads = 100_000

  # Write records
  var ctfs = createCtfs()
  let writerRes = initFixedRecordTableWriter(ctfs, "bench.dat", recordSize)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  var rec: array[recordSize, byte]
  for i in 0 ..< numRecords:
    fillRecord(rec, i)
    let r = ctfs.append(writer, rec)
    doAssert r.isOk

  # Read back
  let rawBytes = ctfs.toBytes()
  let fileDataRes = readInternalFile(rawBytes, "bench.dat")
  doAssert fileDataRes.isOk
  let fileData = fileDataRes.get()
  let readerRes = initFixedRecordTableReader(fileData, recordSize)
  doAssert readerRes.isOk
  let reader = readerRes.get()

  # Generate random indices
  var rng = initRng(12345)
  var indices = newSeq[uint64](numReads)
  for i in 0 ..< numReads:
    indices[i] = rng.next() mod uint64(numRecords)

  # Benchmark reads
  var buf: array[recordSize, byte]
  let startTime = getMonoTime()
  for i in 0 ..< numReads:
    let rr = reader.read(indices[i], buf)
    doAssert rr.isOk
  let endTime = getMonoTime()

  let totalNs = (endTime - startTime).inNanoseconds
  let perLookupNs = totalNs div int64(numReads)

  echo "{\"name\": \"fixed_record_table_random_access\", \"unit\": \"ns\", \"value\": " & $perLookupNs & "}"
  doAssert perLookupNs < 1000, "per-lookup latency too high: " & $perLookupNs & "ns"

  echo "PASS: bench_fixed_record_table_random_access"

# Run all tests
test_fixed_record_table_write_read()
test_fixed_record_table_edge_cases()
bench_fixed_record_table_random_access()
