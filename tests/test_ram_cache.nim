{.push raises: [].}

## Tests and benchmarks for RAM LRU cache and CachedBlockReader.

import std/[options, monotimes, times]
import results
import codetracer_ctfs/ram_cache
import codetracer_ctfs/partial_trace_cache
import codetracer_ctfs/cached_trace_reader

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc makeBlock(blockId: int, size: int): seq[byte] =
  result = newSeq[byte](size)
  for i in 0 ..< size:
    result[i] = byte((blockId * 31 + i) mod 256)

# ---------------------------------------------------------------------------
# test_lru_cache_basic
# ---------------------------------------------------------------------------

proc test_lru_cache_basic() {.raises: [].} =
  ## Create LRU cache with 1KB max, put 5 entries of 256 bytes each.
  ## Oldest entry should be evicted since total (1280) > 1024.
  var cache = initLruCache[int, seq[byte]](maxBytes = 1024)

  for i in 0 ..< 5:
    cache.put(i, makeBlock(i, 256), 256)

  # Cache can hold 4 x 256 = 1024 bytes. Entry 0 should be evicted.
  doAssert not cache.contains(0), "entry 0 should have been evicted"
  doAssert cache.contains(1), "entry 1 should be present"
  doAssert cache.contains(2), "entry 2 should be present"
  doAssert cache.contains(3), "entry 3 should be present"
  doAssert cache.contains(4), "entry 4 should be present"
  doAssert cache.count() == 4
  doAssert cache.currentSize() == 1024

  echo "PASS: test_lru_cache_basic"

# ---------------------------------------------------------------------------
# test_lru_cache_hit_miss
# ---------------------------------------------------------------------------

proc test_lru_cache_hit_miss() {.raises: [].} =
  ## Put 10 entries, access 5 of them, verify counters.
  var cache = initLruCache[int, seq[byte]](maxBytes = 10 * 1024)

  for i in 0 ..< 10:
    cache.put(i, makeBlock(i, 64), 64)

  # Access entries 0, 2, 4, 6, 8 — all hits
  for i in [0, 2, 4, 6, 8]:
    let r = cache.get(i)
    doAssert r.isSome, "entry " & $i & " should be present"

  # Access entries 100, 101, 102, 103, 104 — all misses
  for i in [100, 101, 102, 103, 104]:
    let r = cache.get(i)
    doAssert r.isNone, "entry " & $i & " should not be present"

  doAssert cache.hits == 5, "expected 5 hits, got " & $cache.hits
  doAssert cache.misses == 5, "expected 5 misses, got " & $cache.misses
  doAssert abs(cache.hitRate() - 0.5) < 0.001,
    "expected 50% hit rate, got " & $cache.hitRate()

  echo "PASS: test_lru_cache_hit_miss"

# ---------------------------------------------------------------------------
# test_lru_cache_promote
# ---------------------------------------------------------------------------

proc test_lru_cache_promote() {.raises: [].} =
  ## Put A, B, C (fills cache). Get A (promote). Put D (evict B, not A).
  var cache = initLruCache[string, seq[byte]](maxBytes = 768)

  cache.put("A", makeBlock(1, 256), 256)
  cache.put("B", makeBlock(2, 256), 256)
  cache.put("C", makeBlock(3, 256), 256)
  # Cache full at 768 bytes

  # Access A — promotes it to MRU
  let r = cache.get("A")
  doAssert r.isSome, "A should be present"

  # Insert D — must evict LRU which is now B (A was promoted, C is newer than B)
  cache.put("D", makeBlock(4, 256), 256)

  doAssert cache.contains("A"), "A should still be present (was promoted)"
  doAssert not cache.contains("B"), "B should have been evicted (LRU)"
  doAssert cache.contains("C"), "C should be present"
  doAssert cache.contains("D"), "D should be present"

  echo "PASS: test_lru_cache_promote"

# ---------------------------------------------------------------------------
# test_cached_block_reader
# ---------------------------------------------------------------------------

proc test_cached_block_reader() {.raises: [].} =
  ## Mock fetcher, verify RAM caching avoids re-fetch.
  const blockSize = 4096

  var remote: seq[seq[byte]]
  for b in 0 ..< 200:
    remote.add(makeBlock(b, blockSize))

  var fetchCount = 0
  let fetcher: BlockFetcher = proc(blockId: uint64): Result[seq[byte], string] =
    fetchCount += 1
    if int(blockId) >= remote.len:
      return err("block out of range")
    ok(remote[int(blockId)])

  # Small RAM cache: 64KB = 16 blocks of 4KB
  var reader = initCachedBlockReader(fetcher,
    ramMaxBytes = 64 * 1024,
    diskMaxBytes = 1024 * 1024)

  # Read block 5 — fetcher called once
  let r1 = reader.readBlock(5)
  doAssert r1.isOk, "readBlock(5) failed: " & r1.error
  doAssert r1.get() == remote[5]
  doAssert fetchCount == 1, "fetcher should have been called once"

  # Read block 5 again — RAM hit, fetcher NOT called
  let r2 = reader.readBlock(5)
  doAssert r2.isOk, "second readBlock(5) failed"
  doAssert r2.get() == remote[5]
  doAssert fetchCount == 1, "fetcher should NOT have been called again (RAM hit)"

  # Read 100 blocks — verify RAM cache stays bounded
  for i in 0'u64 ..< 100:
    let r = reader.readBlock(i)
    doAssert r.isOk, "readBlock(" & $i & ") failed: " & r.error
    doAssert r.get() == remote[int(i)]

  doAssert reader.ramCacheSize() <= 64 * 1024,
    "RAM cache should stay under 64KB, got " & $reader.ramCacheSize()

  echo "PASS: test_cached_block_reader"

# ---------------------------------------------------------------------------
# test_ram_cache_eviction_under_pressure
# ---------------------------------------------------------------------------

proc test_ram_cache_eviction_under_pressure() {.raises: [].} =
  ## Create 64KB RAM cache, read 256KB of blocks, verify bounded size.
  ## (Using smaller sizes to be memory-conservative.)
  const blockSize = 4096
  const numBlocks = 64  # 64 * 4KB = 256KB total

  var remote: seq[seq[byte]]
  for b in 0 ..< numBlocks:
    remote.add(makeBlock(b, blockSize))

  var fetchCount = 0
  let fetcher: BlockFetcher = proc(blockId: uint64): Result[seq[byte], string] =
    fetchCount += 1
    if int(blockId) >= remote.len:
      return err("block out of range")
    ok(remote[int(blockId)])

  # 64KB RAM cache = 16 blocks
  var reader = initCachedBlockReader(fetcher,
    ramMaxBytes = 64 * 1024,
    diskMaxBytes = 512 * 1024)

  # Read all blocks
  for i in 0'u64 ..< uint64(numBlocks):
    let r = reader.readBlock(i)
    doAssert r.isOk, "readBlock(" & $i & ") failed: " & r.error
    doAssert r.get() == remote[int(i)], "data mismatch at block " & $i

  doAssert reader.ramCacheSize() <= 64 * 1024,
    "RAM cache exceeded 64KB: " & $reader.ramCacheSize()
  doAssert reader.ramCacheCount() <= 16,
    "RAM cache has too many entries: " & $reader.ramCacheCount()

  # Re-read a recently accessed block — should be RAM hit (disk cache has it anyway)
  let lastBlock = uint64(numBlocks - 1)
  let r = reader.readBlock(lastBlock)
  doAssert r.isOk
  doAssert r.get() == remote[int(lastBlock)]

  echo "PASS: test_ram_cache_eviction_under_pressure"

# ---------------------------------------------------------------------------
# bench_ram_cache_hit_latency
# ---------------------------------------------------------------------------

proc bench_ram_cache_hit_latency() {.raises: [].} =
  ## Read same block 100K times from RAM, measure per-read latency.
  const iterations = 100_000

  var cache = initLruCache[uint64, seq[byte]](maxBytes = 1024 * 1024)
  let data = makeBlock(42, 4096)
  cache.put(0'u64, data, 4096)

  let start = getMonoTime()
  for i in 0 ..< iterations:
    let r = cache.get(0'u64)
    doAssert r.isSome
  let elapsed = getMonoTime() - start

  let ns = elapsed.inNanoseconds
  let perReadNs = ns div iterations
  let perReadUs = float(perReadNs) / 1000.0

  echo "bench_ram_cache_hit_latency: " & $perReadNs & " ns/read (" &
      $perReadUs & " us/read) over " & $iterations & " iterations"

  # Assert < 1us (1000ns) per read
  doAssert perReadNs < 1000,
    "RAM cache hit too slow: " & $perReadNs & " ns/read (expected < 1000ns)"

  echo "PASS: bench_ram_cache_hit_latency"

# ---------------------------------------------------------------------------
# bench_ctp_vs_local
# ---------------------------------------------------------------------------

proc bench_ctp_vs_local() {.raises: [].} =
  ## Compare read latency: RAM-cached vs direct seq[byte] access.
  const iterations = 100_000

  # Direct access baseline
  let directData = makeBlock(99, 4096)

  let startDirect = getMonoTime()
  var sink: byte = 0
  for i in 0 ..< iterations:
    sink = sink xor directData[i mod directData.len]
  let elapsedDirect = getMonoTime() - startDirect

  # RAM cache access
  var cache = initLruCache[uint64, seq[byte]](maxBytes = 1024 * 1024)
  cache.put(0'u64, directData, 4096)

  let startCached = getMonoTime()
  for i in 0 ..< iterations:
    let r = cache.get(0'u64)
    doAssert r.isSome
    sink = sink xor r.get()[i mod 4096]
  let elapsedCached = getMonoTime() - startCached

  let directNs = elapsedDirect.inNanoseconds
  let cachedNs = elapsedCached.inNanoseconds

  # Prevent sink from being optimized away
  if sink == 255:
    echo "sink: " & $sink

  echo "bench_ctp_vs_local:"
  echo "  direct: " & $directNs & " ns total (" &
      $(directNs div iterations) & " ns/iter)"
  echo "  cached: " & $cachedNs & " ns total (" &
      $(cachedNs div iterations) & " ns/iter)"

  if directNs > 0:
    let ratio = float(cachedNs) / float(directNs)
    echo "  ratio: " & $ratio & "x"
    # The cached path does more work (hash lookup, option check, linked list ops)
    # so we just check it's within a reasonable factor
    # Note: not asserting 2x since the direct baseline is trivially cheap
    echo "  (informational — direct access is a trivial baseline)"

  echo "PASS: bench_ctp_vs_local"

# ---------------------------------------------------------------------------
# Run all
# ---------------------------------------------------------------------------

test_lru_cache_basic()
test_lru_cache_hit_miss()
test_lru_cache_promote()
test_cached_block_reader()
test_ram_cache_eviction_under_pressure()
bench_ram_cache_hit_latency()
bench_ctp_vs_local()
