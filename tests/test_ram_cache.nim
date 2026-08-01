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

# A cache value that counts every time it is copied. This is how
# `test_lru_cache_hit_does_not_copy_the_value` proves the claim structurally
# instead of inferring it from a clock or from a heap-size delta: the copy of a
# `seq` payload goes through `=copy`, so a counter in that hook is an exact
# census of the 4 KiB memcpys a given access pattern performs.
type CountedBlock = object
  data: seq[byte]

var countedBlockCopies = 0

proc `=copy`(dst: var CountedBlock, src: CountedBlock) =
  inc countedBlockCopies
  dst.data = src.data

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
# test_lru_cache_try_get_matches_get
# ---------------------------------------------------------------------------

proc test_lru_cache_try_get_matches_get() {.raises: [].} =
  ## The borrowing accessor must be observationally identical to the copying
  ## one: same hit/miss verdict, same bytes, same MRU promotion, same counters.
  var borrowing = initLruCache[string, seq[byte]](maxBytes = 768)
  var copying = initLruCache[string, seq[byte]](maxBytes = 768)

  for pair in [("A", 1), ("B", 2), ("C", 3)]:
    borrowing.put(pair[0], makeBlock(pair[1], 256), 256)
    copying.put(pair[0], makeBlock(pair[1], 256), 256)

  # Same verdict and same bytes on a hit …
  let borrowed = borrowing.tryGet("A")
  let copied = copying.get("A")
  doAssert not borrowed.isNil, "tryGet must report the hit"
  doAssert copied.isSome, "get must report the same hit"
  doAssert borrowed[] == copied.get(), "tryGet and get must yield the same bytes"
  doAssert borrowed[] == makeBlock(1, 256), "tryGet must yield the stored bytes"

  # … and the same verdict on a miss.
  doAssert borrowing.tryGet("Z").isNil, "tryGet must report the miss"
  doAssert copying.get("Z").isNone, "get must report the same miss"

  # Same counters after the identical access sequence.
  doAssert borrowing.hits == copying.hits,
    "hit counters diverged: " & $borrowing.hits & " vs " & $copying.hits
  doAssert borrowing.misses == copying.misses,
    "miss counters diverged: " & $borrowing.misses & " vs " & $copying.misses

  # Same MRU promotion: the hit on A must have saved A from the next eviction
  # in both caches, and evicted B in both.
  borrowing.put("D", makeBlock(4, 256), 256)
  copying.put("D", makeBlock(4, 256), 256)
  doAssert borrowing.contains("A") and copying.contains("A"),
    "tryGet must promote to MRU exactly as get does"
  doAssert (not borrowing.contains("B")) and (not copying.contains("B")),
    "both caches must have evicted the same LRU entry"

  echo "PASS: test_lru_cache_try_get_matches_get"

# ---------------------------------------------------------------------------
# test_lru_cache_hit_does_not_copy_the_value
# ---------------------------------------------------------------------------

proc test_lru_cache_hit_does_not_copy_the_value() {.raises: [].} =
  ## Structural proof — no clock involved — that a hit on a cache holding a
  ## 4096-byte block neither allocates nor copies the block.
  ##
  ## Two independent witnesses, because either one alone is weak:
  ##
  ## 1. **Aliasing.** The pointer `tryGet` returns must address the cache's own
  ##    storage, not a copy of it: repeated hits return the *same* address, and
  ##    a write through that address is visible to the next hit. A copying
  ##    accessor cannot satisfy either.
  ## 2. **A copy census.** With a value type whose `=copy` hook counts calls,
  ##    10_000 `tryGet` hits must perform *exactly zero* copies. The same loop
  ##    through `get` is measured next to it and must perform at least one copy
  ##    per hit — otherwise the zero above would be vacuous (it would prove the
  ##    hook is not wired up, not that the accessor borrows).
  const blockSize = 4096
  var cache = initLruCache[uint64, seq[byte]](maxBytes = 1024 * 1024)
  cache.put(0'u64, makeBlock(42, blockSize), uint64(blockSize))

  # (1) Aliasing.
  let first = cache.tryGet(0'u64)
  let second = cache.tryGet(0'u64)
  doAssert not first.isNil and not second.isNil, "both accesses must hit"
  doAssert first == second,
    "tryGet returned two different addresses for the same entry — it copied"
  doAssert first[][0].addr == second[][0].addr,
    "the seq payloads differ, so the value itself was copied out"
  # A write through the borrow is a write into the cache.
  first[][7] = 0xAB'u8
  let third = cache.tryGet(0'u64)
  doAssert third[][7] == 0xAB'u8,
    "the borrow did not alias the cached entry"

  # (2) Copy census.
  const iterations = 10_000
  var counted = initLruCache[uint64, CountedBlock](maxBytes = 1024 * 1024)
  counted.put(0'u64, CountedBlock(data: makeBlock(42, blockSize)),
    uint64(blockSize))

  var sink: byte = 0

  countedBlockCopies = 0
  for i in 0 ..< iterations:
    let hit = counted.tryGet(0'u64)
    doAssert not hit.isNil
    sink = sink xor hit[].data[i mod blockSize]
  let borrowingCopies = countedBlockCopies

  countedBlockCopies = 0
  for i in 0 ..< iterations:
    let owned = counted.get(0'u64)
    doAssert owned.isSome
    sink = sink xor owned.get().data[i mod blockSize]
  let owningCopies = countedBlockCopies

  if sink == 255:  # keep the loops from being optimised away
    echo "sink: " & $sink

  doAssert borrowingCopies == 0,
    "tryGet copied the value " & $borrowingCopies & " times over " &
      $iterations & " hits — the hit path is not borrowing"
  doAssert owningCopies >= iterations,
    "the copy census is not wired up: get copied only " & $owningCopies &
      " times over " & $iterations & " hits, so `borrowingCopies == 0` proves " &
      "nothing"

  echo "PASS: test_lru_cache_hit_does_not_copy_the_value (" &
    $borrowingCopies & " copies borrowing vs " & $owningCopies & " owning)"

# ---------------------------------------------------------------------------
# test_borrow_never_escapes_the_cache
# ---------------------------------------------------------------------------

proc test_borrow_never_escapes_the_cache() {.raises: [].} =
  ## `tryGet` hands out a pointer into storage the cache owns, and that
  ## pointer stops being valid at the cache's next `put` or `clear` — an
  ## eviction frees the entry it addresses. So the property that has to hold
  ## is not "the borrow is safe" (it is only safe by discipline) but **no
  ## borrow leaves the accessor's own expression**. Both in-package consumers
  ## dereference immediately and hand the caller an owned value; this test
  ## pins that, from the outside, for both of them.
  ##
  ## The two failure modes it would catch are the two that matter:
  ##
  ## 1. The value is handed out by reference — a caller mutating what it got
  ##    would then be writing into the cache, and every later reader would see
  ##    the corruption.
  ## 2. The value is *moved* out of the cache rather than copied. `some(hit[])`
  ##    and `ok(ramHit[])` both pass a `ptr` dereference to a `sink` parameter,
  ##    which must copy because the callee does not own that location; if it
  ##    ever moved instead, the cached entry would be left empty and the next
  ##    hit would silently return nothing.
  const blockSize = 4096

  # --- the owning accessor, straight on the cache ---------------------------
  var cache = initLruCache[uint64, seq[byte]](maxBytes = 2 * blockSize)
  cache.put(1'u64, makeBlock(1, blockSize), uint64(blockSize))

  var owned = cache.get(1'u64).get()
  doAssert owned == makeBlock(1, blockSize)

  # The cache still holds its own copy — `get` copied, it did not move.
  let stillThere = cache.tryGet(1'u64)
  doAssert not stillThere.isNil, "the entry disappeared after `get`"
  doAssert stillThere[] == makeBlock(1, blockSize),
    "`get` moved the value out of the cache instead of copying it"

  # Writing to what the caller owns must not reach the cache.
  owned[0] = owned[0] xor 0xFF'u8
  doAssert cache.tryGet(1'u64)[] == makeBlock(1, blockSize),
    "`get` returned an alias into the cache, not a copy"

  # And the owned copy outlives the entry it came from: two more 4 KiB
  # entries evict key 1 from a 8 KiB budget, which frees the very node a
  # borrow would have pointed at.
  cache.put(2'u64, makeBlock(2, blockSize), uint64(blockSize))
  cache.put(3'u64, makeBlock(3, blockSize), uint64(blockSize))
  doAssert not cache.contains(1'u64), "key 1 should have been evicted"
  var expected = makeBlock(1, blockSize)
  expected[0] = expected[0] xor 0xFF'u8
  doAssert owned == expected,
    "the value `get` returned did not survive the eviction of its entry"

  # `clear` is the other mutation point named in the borrow rule.
  let survivor = cache.get(3'u64).get()
  cache.clear()
  doAssert cache.count() == 0
  doAssert survivor == makeBlock(3, blockSize),
    "the value `get` returned did not survive `clear`"

  # --- and through `CachedBlockReader.readBlock`, the migrated consumer ------
  var remote: seq[seq[byte]]
  for b in 0 ..< 64:
    remote.add(makeBlock(b, blockSize))
  let fetcher: BlockFetcher = proc(blockId: uint64): Result[seq[byte], string] =
    if int(blockId) >= remote.len:
      return err("block out of range")
    ok(remote[int(blockId)])

  # 64 KiB of RAM cache = 16 blocks.
  var reader = initCachedBlockReader(fetcher,
    ramMaxBytes = 64 * 1024,
    diskMaxBytes = 1024 * 1024)

  doAssert reader.readBlock(7'u64).isOk           # populate: the miss path
  let hit = reader.readBlock(7'u64)               # the `tryGet` path
  doAssert hit.isOk, "second readBlock(7) failed: " & hit.error
  var got = hit.get()
  doAssert got == remote[7]

  got[0] = got[0] xor 0xFF'u8
  let reread = reader.readBlock(7'u64)
  doAssert reread.isOk
  doAssert reread.get() == remote[7],
    "readBlock returned an alias into the RAM cache — a caller mutating the " &
    "block it was given corrupted the cache"

  # The block a caller already holds must survive its entry being evicted:
  # 32 further 4 KiB reads through a 16-entry cache retire key 7.
  let held = reread.get()
  for i in 0'u64 ..< 32:
    doAssert reader.readBlock(i).isOk
  doAssert reader.ramCacheCount() <= 16
  doAssert held == remote[7],
    "the block readBlock returned did not survive the eviction of its entry"

  echo "PASS: test_borrow_never_escapes_the_cache"

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
  ##
  ## The gate is on `tryGet`, the accessor a reader actually takes through the
  ## cache. It used to be on `get`, whose ``Option[V]`` return copies the 4 KiB
  ## block out on every hit — an allocation plus a 4 KiB memcpy plus a free per
  ## read, which was ~95% of the number this benchmark reported and left the
  ## 1000 ns gate clearing by ~6%. The threshold below is unchanged; what
  ## changed is that the hit path no longer does that work. Both paths are
  ## measured, and the *ratio* is asserted below, so this cannot quietly
  ## regress into a copying accessor again.
  const iterations = 100_000

  var cache = initLruCache[uint64, seq[byte]](maxBytes = 1024 * 1024)
  let data = makeBlock(42, 4096)
  cache.put(0'u64, data, 4096)

  let start = getMonoTime()
  for i in 0 ..< iterations:
    let r = cache.tryGet(0'u64)
    doAssert not r.isNil
  let elapsed = getMonoTime() - start

  let ns = elapsed.inNanoseconds
  let perReadNs = ns div iterations
  let perReadUs = float(perReadNs) / 1000.0

  # The owning accessor, for the record and for the ratio assertion.
  let startCopying = getMonoTime()
  for i in 0 ..< iterations:
    let r = cache.get(0'u64)
    doAssert r.isSome
  let elapsedCopying = getMonoTime() - startCopying
  let perCopyingReadNs = elapsedCopying.inNanoseconds div iterations

  echo "bench_ram_cache_hit_latency: " & $perReadNs & " ns/read (" &
      $perReadUs & " us/read) over " & $iterations & " iterations"
  echo "  copying accessor (get, Option[V]): " & $perCopyingReadNs & " ns/read"

  # Assert < 1us (1000ns) per read
  doAssert perReadNs < 1000,
    "RAM cache hit too slow: " & $perReadNs & " ns/read (expected < 1000ns)"

  # The borrowing accessor must actually be borrowing. A 4 KiB copy is not
  # free on any machine, so if `tryGet` ever starts copying again this ratio
  # collapses towards 1 long before the absolute gate above turns red.
  doAssert perReadNs * 2 < perCopyingReadNs,
    "tryGet (" & $perReadNs & " ns) is not materially cheaper than get (" &
      $perCopyingReadNs & " ns) — the hit path is copying again"

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
    let r = cache.tryGet(0'u64)
    doAssert not r.isNil
    sink = sink xor r[][i mod 4096]
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
test_lru_cache_try_get_matches_get()
test_lru_cache_hit_does_not_copy_the_value()
test_borrow_never_escapes_the_cache()
test_cached_block_reader()
test_ram_cache_eviction_under_pressure()
bench_ram_cache_hit_latency()
bench_ctp_vs_local()
