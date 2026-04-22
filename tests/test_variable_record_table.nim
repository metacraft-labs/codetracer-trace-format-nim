when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Tests and benchmark for VariableRecordTable.

import std/monotimes
import std/times
import results
import codetracer_ctfs

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc makeRecord(index: int, length: int): seq[byte] =
  ## Create a deterministic record of the given length based on index.
  var rec = newSeq[byte](length)
  for i in 0 ..< length:
    rec[i] = byte((index * 31 + i * 7) mod 256)
  rec

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
# test_variable_record_table_write_read
# ---------------------------------------------------------------------------

proc test_variable_record_table_write_read() {.raises: [].} =
  const numRecords = 10000

  var ctfs = createCtfs()
  let writerRes = initVariableRecordTableWriter(ctfs, "records")
  doAssert writerRes.isOk, "initVariableRecordTableWriter failed: " & writerRes.error
  var writer = writerRes.get()

  # Write 10000 records of varying lengths
  for i in 0 ..< numRecords:
    let length = (i mod 100) + 1
    let rec = makeRecord(i, length)
    let r = ctfs.append(writer, rec)
    doAssert r.isOk, "append failed at record " & $i & ": " & r.error

  doAssert writer.count == uint64(numRecords),
    "count mismatch: " & $writer.count

  # Serialize and read back
  let rawBytes = ctfs.toBytes()
  let readerRes = initVariableRecordTableReader(rawBytes, "records")
  doAssert readerRes.isOk, "initVariableRecordTableReader failed: " & readerRes.error
  let reader = readerRes.get()
  doAssert reader.count == uint64(numRecords),
    "reader count mismatch: " & $reader.count

  # Read 100 random indices and verify
  var rng = initRng(42)
  for check in 0 ..< 100:
    let idx = int(rng.next() mod uint64(numRecords))
    let length = (idx mod 100) + 1
    let expected = makeRecord(idx, length)
    let readRes = reader.read(uint64(idx))
    doAssert readRes.isOk, "read failed at index " & $idx & ": " & readRes.error
    let got = readRes.get()
    doAssert got.len == expected.len,
      "length mismatch at record " & $idx & ": got " & $got.len & " expected " & $expected.len
    for b in 0 ..< got.len:
      doAssert got[b] == expected[b],
        "byte mismatch at record " & $idx & " byte " & $b &
        ": got " & $got[b] & " expected " & $expected[b]

  echo "PASS: test_variable_record_table_write_read"

# ---------------------------------------------------------------------------
# test_variable_record_table_edge_cases
# ---------------------------------------------------------------------------

proc test_variable_record_table_edge_cases() {.raises: [].} =
  # -- Zero-length record --
  block:
    var ctfs = createCtfs()
    let writerRes = initVariableRecordTableWriter(ctfs, "empty_rec")
    doAssert writerRes.isOk
    var writer = writerRes.get()

    let emptyRec = newSeq[byte](0)
    let r = ctfs.append(writer, emptyRec)
    doAssert r.isOk, "append empty record failed: " & r.error
    doAssert writer.count == 1

    let rawBytes = ctfs.toBytes()
    let readerRes = initVariableRecordTableReader(rawBytes, "empty_rec")
    doAssert readerRes.isOk
    let reader = readerRes.get()
    doAssert reader.count == 1

    let readRes = reader.read(0)
    doAssert readRes.isOk
    let got = readRes.get()
    doAssert got.len == 0, "empty record length should be 0, got " & $got.len

  # -- Large record (larger than one block) --
  block:
    var ctfs = createCtfs()
    let writerRes = initVariableRecordTableWriter(ctfs, "large_rec")
    doAssert writerRes.isOk
    var writer = writerRes.get()

    const largeSize = 5000
    var largeRec = newSeq[byte](largeSize)
    for i in 0 ..< largeSize:
      largeRec[i] = byte((i * 13 + 7) mod 256)
    let r = ctfs.append(writer, largeRec)
    doAssert r.isOk, "append large record failed: " & r.error

    let rawBytes = ctfs.toBytes()
    let readerRes = initVariableRecordTableReader(rawBytes, "large_rec")
    doAssert readerRes.isOk
    let reader = readerRes.get()
    doAssert reader.count == 1

    let readRes = reader.read(0)
    doAssert readRes.isOk
    let got = readRes.get()
    doAssert got.len == largeSize, "large record length mismatch"
    for i in 0 ..< largeSize:
      doAssert got[i] == largeRec[i],
        "large record byte " & $i & " mismatch: got " & $got[i] & " expected " & $largeRec[i]

  # -- 100K tiny records (1 byte each) --
  block:
    const numTiny = 100_000
    var ctfs = createCtfs()
    let writerRes = initVariableRecordTableWriter(ctfs, "tiny_recs")
    doAssert writerRes.isOk
    var writer = writerRes.get()

    var rec: array[1, byte]
    for i in 0 ..< numTiny:
      rec[0] = byte(i mod 256)
      let r = ctfs.append(writer, rec)
      doAssert r.isOk, "append tiny record " & $i & " failed: " & r.error

    doAssert writer.count == uint64(numTiny),
      "tiny count mismatch: " & $writer.count

    let rawBytes = ctfs.toBytes()
    let readerRes = initVariableRecordTableReader(rawBytes, "tiny_recs")
    doAssert readerRes.isOk
    let reader = readerRes.get()
    doAssert reader.count == uint64(numTiny),
      "tiny reader count mismatch: " & $reader.count

    # Spot check a few
    var rng = initRng(99)
    for check in 0 ..< 20:
      let idx = int(rng.next() mod uint64(numTiny))
      let readRes = reader.read(uint64(idx))
      doAssert readRes.isOk
      let got = readRes.get()
      doAssert got.len == 1, "tiny record length should be 1"
      doAssert got[0] == byte(idx mod 256),
        "tiny record " & $idx & ": got " & $got[0] & " expected " & $byte(idx mod 256)

  # -- Mixed: [empty, large, tiny, medium] --
  block:
    var ctfs = createCtfs()
    let writerRes = initVariableRecordTableWriter(ctfs, "mixed")
    doAssert writerRes.isOk
    var writer = writerRes.get()

    # Record 0: empty
    let r0 = ctfs.append(writer, newSeq[byte](0))
    doAssert r0.isOk

    # Record 1: large (5000 bytes)
    var large = newSeq[byte](5000)
    for i in 0 ..< 5000:
      large[i] = byte((i * 3 + 11) mod 256)
    let r1 = ctfs.append(writer, large)
    doAssert r1.isOk

    # Record 2: tiny (1 byte)
    let r2 = ctfs.append(writer, [0xAB'u8])
    doAssert r2.isOk

    # Record 3: medium (200 bytes)
    var medium = newSeq[byte](200)
    for i in 0 ..< 200:
      medium[i] = byte((i * 17 + 5) mod 256)
    let r3 = ctfs.append(writer, medium)
    doAssert r3.isOk

    doAssert writer.count == 4

    let rawBytes = ctfs.toBytes()
    let readerRes = initVariableRecordTableReader(rawBytes, "mixed")
    doAssert readerRes.isOk
    let reader = readerRes.get()
    doAssert reader.count == 4

    # Verify record 0 (empty)
    let got0 = reader.read(0)
    doAssert got0.isOk
    doAssert got0.get().len == 0

    # Verify record 1 (large)
    let got1 = reader.read(1)
    doAssert got1.isOk
    let g1 = got1.get()
    doAssert g1.len == 5000
    for i in 0 ..< 5000:
      doAssert g1[i] == large[i], "mixed large byte " & $i & " mismatch"

    # Verify record 2 (tiny)
    let got2 = reader.read(2)
    doAssert got2.isOk
    let g2 = got2.get()
    doAssert g2.len == 1
    doAssert g2[0] == 0xAB'u8

    # Verify record 3 (medium)
    let got3 = reader.read(3)
    doAssert got3.isOk
    let g3 = got3.get()
    doAssert g3.len == 200
    for i in 0 ..< 200:
      doAssert g3[i] == medium[i], "mixed medium byte " & $i & " mismatch"

  echo "PASS: test_variable_record_table_edge_cases"

# ---------------------------------------------------------------------------
# bench_variable_record_table_seek
# ---------------------------------------------------------------------------

proc bench_variable_record_table_seek() {.raises: [].} =
  const numRecords = 100_000
  const numReads = 100_000

  # Write records of varying sizes (10-100 bytes)
  var ctfs = createCtfs()
  let writerRes = initVariableRecordTableWriter(ctfs, "bench")
  doAssert writerRes.isOk
  var writer = writerRes.get()

  for i in 0 ..< numRecords:
    let length = (i mod 91) + 10  # 10 to 100 bytes
    let rec = makeRecord(i, length)
    let r = ctfs.append(writer, rec)
    doAssert r.isOk

  # Read back
  let rawBytes = ctfs.toBytes()
  let readerRes = initVariableRecordTableReader(rawBytes, "bench")
  doAssert readerRes.isOk
  let reader = readerRes.get()

  # Generate random indices
  var rng = initRng(12345)
  var indices = newSeq[uint64](numReads)
  for i in 0 ..< numReads:
    indices[i] = rng.next() mod uint64(numRecords)

  # Benchmark reads
  let startTime = getMonoTime()
  for i in 0 ..< numReads:
    let rr = reader.read(indices[i])
    doAssert rr.isOk
  let endTime = getMonoTime()

  let totalNs = (endTime - startTime).inNanoseconds
  let perLookupNs = totalNs div int64(numReads)

  echo "{\"name\": \"variable_record_table_seek\", \"unit\": \"ns\", \"value\": " & $perLookupNs & "}"
  doAssert perLookupNs < 5000, "per-lookup latency too high: " & $perLookupNs & "ns"

  echo "PASS: bench_variable_record_table_seek"

# Run all tests
test_variable_record_table_write_read()
test_variable_record_table_edge_cases()
bench_variable_record_table_seek()
