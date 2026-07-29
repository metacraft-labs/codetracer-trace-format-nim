when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Correctness tests for ChunkedCompressedTable.
##
## The two throughput/latency benchmarks that used to live at the bottom of
## this file are in `tests/bench_chunked_table.nim` (the `bench` task) — see
## the header there for why.  What they were the only witness for on the
## reader side, the decompressed-chunk cache, is asserted here instead with
## counters rather than a clock, in
## `test_chunked_table_chunk_cache_is_correct`.

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
# test_chunked_table_chunk_cache_is_correct
# ---------------------------------------------------------------------------

proc test_chunked_table_chunk_cache_is_correct() {.raises: [].} =
  ## `ChunkedCompressedTableReader` keeps decompressed chunks in an LRU cache
  ## (`src/codetracer_ctfs/chunk_cache.nim`).  A cache that returns stale bytes
  ## is far worse than a slow reader, so prove three things *without a clock*:
  ##
  ##   1. a re-read of an index whose chunk is already resident returns bytes
  ##      identical to the cold read, and costs no extra Zstd inflation;
  ##   2. reading a *different* chunk after a cached one is still correct, and
  ##      going back to the first chunk is a hit rather than a re-inflation;
  ##   3. the same holds when the budget forces eviction — a chunk that was
  ##      evicted and re-inflated still yields the same bytes.
  const recordSize = 16
  const chunkSize = 64'u32
  const numChunks = 16
  const numRecords = int(chunkSize) * numChunks

  var ctfs = createCtfs()
  let writerRes = initChunkedCompressedTableWriter(ctfs, "cache", recordSize, chunkSize)
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

  var expected: array[recordSize, byte]
  var cold: array[recordSize, byte]
  var warm: array[recordSize, byte]

  # --- (1) + (2): a budget wide enough for the whole table. ---
  block:
    let readerRes = initChunkedCompressedTableReader(rawBytes, "cache", recordSize)
    doAssert readerRes.isOk, "initReader failed: " & readerRes.error
    var reader = readerRes.get()

    # Cold read of a record in chunk 0.
    let a = 5'u64
    doAssert reader.read(a, cold).isOk
    fillRecord(expected, int(a))
    for b in 0 ..< recordSize:
      doAssert cold[b] == expected[b], "cold read wrong at byte " & $b
    doAssert reader.cacheMisses == 1, "expected one inflation, got " & $reader.cacheMisses

    # Repeated read of the SAME index: identical bytes, no new inflation.
    doAssert reader.read(a, warm).isOk
    for b in 0 ..< recordSize:
      doAssert warm[b] == cold[b],
        "cached re-read differs from cold read at byte " & $b
    doAssert reader.cacheMisses == 1,
      "a resident chunk was inflated again: misses=" & $reader.cacheMisses
    doAssert reader.cacheHits == 1, "expected one cache hit, got " & $reader.cacheHits

    # A different index in the SAME chunk: still a hit, still correct.
    let sameChunk = 40'u64
    doAssert reader.read(sameChunk, warm).isOk
    fillRecord(expected, int(sameChunk))
    for b in 0 ..< recordSize:
      doAssert warm[b] == expected[b], "same-chunk read wrong at byte " & $b
    doAssert reader.cacheMisses == 1,
      "a resident chunk was inflated again: misses=" & $reader.cacheMisses

    # A record in a DIFFERENT chunk must not be served from the cached one.
    let otherChunk = uint64(chunkSize) * 9 + 17
    doAssert reader.read(otherChunk, warm).isOk
    fillRecord(expected, int(otherChunk))
    for b in 0 ..< recordSize:
      doAssert warm[b] == expected[b],
        "read of a different chunk returned stale bytes at byte " & $b
    doAssert reader.cacheMisses == 2,
      "expected a second inflation for a second chunk, got " & $reader.cacheMisses

    # Back to chunk 0: both chunks are resident, so this is a hit.
    doAssert reader.read(a, warm).isOk
    for b in 0 ..< recordSize:
      doAssert warm[b] == cold[b], "return to chunk 0 differs at byte " & $b
    doAssert reader.cacheMisses == 2,
      "chunk 0 was evicted by a 2-chunk working set: misses=" & $reader.cacheMisses
    doAssert reader.residentChunks == 2,
      "expected 2 resident chunks, got " & $reader.residentChunks

    # Every record still reads back correctly with a fully populated cache.
    for i in 0 ..< numRecords:
      doAssert reader.read(uint64(i), warm).isOk
      fillRecord(expected, i)
      for b in 0 ..< recordSize:
        doAssert warm[b] == expected[b],
          "full sweep mismatch at record " & $i & " byte " & $b

  # --- (3): a one-chunk budget forces eviction on every chunk change. ---
  block:
    let readerRes = initChunkedCompressedTableReader(rawBytes, "cache", recordSize,
      cacheBytes = uint64(chunkSize) * uint64(recordSize))
    doAssert readerRes.isOk, "initReader (small budget) failed: " & readerRes.error
    var reader = readerRes.get()

    # Alternate between two chunks so each read evicts the other.
    for round in 0 ..< 4:
      for idx in [3'u64, uint64(chunkSize) * 11 + 3]:
        doAssert reader.read(idx, warm).isOk
        fillRecord(expected, int(idx))
        for b in 0 ..< recordSize:
          doAssert warm[b] == expected[b],
            "evicting cache returned stale bytes for record " & $idx &
            " byte " & $b & " (round " & $round & ")"
    doAssert reader.residentChunks == 1,
      "one-chunk budget kept " & $reader.residentChunks & " chunks resident"

    # And a full sweep is still byte-exact under constant eviction.
    for i in 0 ..< numRecords:
      doAssert reader.read(uint64(i), warm).isOk
      fillRecord(expected, i)
      for b in 0 ..< recordSize:
        doAssert warm[b] == expected[b],
          "evicting-cache sweep mismatch at record " & $i & " byte " & $b

  # --- (4): a budget that is NOT a whole number of chunks. This is the path
  #     where a slot is admitted first and evicted afterwards, i.e. the one
  #     that recycles evicted slots; a cache that dropped them instead would
  #     hold every chunk it ever saw and blow through its own budget.
  block:
    const chunkBytes = uint64(chunkSize) * uint64(recordSize)
    let readerRes = initChunkedCompressedTableReader(rawBytes, "cache", recordSize,
      cacheBytes = chunkBytes * 5 div 2)
    doAssert readerRes.isOk, "initReader (fractional budget) failed: " & readerRes.error
    var reader = readerRes.get()

    for pass in 0 ..< 3:
      for i in 0 ..< numRecords:
        doAssert reader.read(uint64(i), warm).isOk
        fillRecord(expected, i)
        for b in 0 ..< recordSize:
          doAssert warm[b] == expected[b],
            "fractional-budget sweep mismatch at record " & $i &
            " byte " & $b & " (pass " & $pass & ")"
      doAssert reader.residentChunks <= 3,
        "cache exceeded its byte budget: " & $reader.residentChunks &
        " chunks resident for a 2.5-chunk budget (pass " & $pass & ")"
      # The real memory bound: evicted buffers must be recycled, not stranded.
      doAssert reader.cacheSlotCount <= 4,
        "cache is holding " & $reader.cacheSlotCount &
        " chunk buffers for a 2.5-chunk budget after " & $(pass + 1) &
        " sweeps of a " & $numChunks & "-chunk table — evicted slots are " &
        "not being reused"

  echo "PASS: test_chunked_table_chunk_cache_is_correct"

# Run all tests
test_chunked_compressed_table_write_read()
test_chunked_compressed_table_random_access()
test_chunked_compressed_partial_write()
test_chunked_table_chunk_cache_is_correct()
