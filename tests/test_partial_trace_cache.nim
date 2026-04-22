{.push raises: [].}

## Tests for partial trace cache (.ctp) implementation.

import results
import std/sets
import codetracer_ctfs/partial_trace_cache

proc test_ctp_fetch_on_miss() {.raises: [].} =
  ## Create a "remote" trace, fetch a block, verify caching.
  const blockSize = 4096
  const numBlocks = 20

  # Build a fake remote: numBlocks blocks of predictable data.
  var remote: seq[seq[byte]]
  for b in 0 ..< numBlocks:
    var blk = newSeq[byte](blockSize)
    for i in 0 ..< blockSize:
      blk[i] = byte((b * 31 + i) mod 256)
    remote.add(blk)

  var fetchCount = 0
  let fetcher: BlockFetcher = proc(blockId: uint64): Result[seq[byte], string] =
    fetchCount += 1
    if int(blockId) >= remote.len:
      return err("block out of range")
    ok(remote[int(blockId)])

  var cache = initPartialTraceCache(fetcher, blockSize = blockSize)

  # Block 5 is not cached yet.
  doAssert not cache.hasBlock(5)

  # Fetch block 5 — should call the fetcher.
  let r1 = cache.fetchBlock(5)
  doAssert r1.isOk, "fetchBlock(5) failed: " & r1.error
  doAssert r1.get() == remote[5], "fetched data mismatch"
  doAssert fetchCount == 1, "fetcher should have been called once"

  # Block 5 is now cached.
  doAssert cache.hasBlock(5)

  # Fetch block 5 again — should NOT call the fetcher.
  let r2 = cache.fetchBlock(5)
  doAssert r2.isOk, "second fetchBlock(5) failed"
  doAssert r2.get() == remote[5], "cached data mismatch"
  doAssert fetchCount == 1, "fetcher should NOT have been called again"

  echo "PASS: test_ctp_fetch_on_miss"

proc test_ctp_presence_idx() {.raises: [].} =
  ## Fetch blocks in non-sorted order, serialize presence, verify sorted.
  const blockSize = 64

  var remote: seq[seq[byte]]
  for b in 0 ..< 20:
    var blk = newSeq[byte](blockSize)
    for i in 0 ..< blockSize:
      blk[i] = byte(b)
    remote.add(blk)

  let fetcher: BlockFetcher = proc(blockId: uint64): Result[seq[byte], string] =
    if int(blockId) >= remote.len:
      return err("out of range")
    ok(remote[int(blockId)])

  var cache = initPartialTraceCache(fetcher, blockSize = blockSize)

  # Fetch in non-sorted order: 3, 7, 1, 10
  doAssert cache.fetchBlock(3).isOk
  doAssert cache.fetchBlock(7).isOk
  doAssert cache.fetchBlock(1).isOk
  doAssert cache.fetchBlock(10).isOk

  doAssert cache.presenceCount() == 4

  # Serialize and verify sorted order.
  let idx = cache.serializePresenceIdx()
  doAssert idx.len == 4 * 8, "expected 32 bytes, got " & $idx.len

  # Read back the u64s and verify sorted: [1, 3, 7, 10]
  let loaded = loadPresenceIdx(idx)
  doAssert loaded.len == 4
  doAssert 1'u64 in loaded
  doAssert 3'u64 in loaded
  doAssert 7'u64 in loaded
  doAssert 10'u64 in loaded

  # Verify the raw bytes are sorted by reading sequentially.
  proc readU64LE(data: openArray[byte], offset: int): uint64 =
    for j in 0 ..< 8:
      result = result or (uint64(data[offset + j]) shl (j * 8))

  let v0 = readU64LE(idx, 0)
  let v1 = readU64LE(idx, 8)
  let v2 = readU64LE(idx, 16)
  let v3 = readU64LE(idx, 24)
  doAssert v0 == 1 and v1 == 3 and v2 == 7 and v3 == 10,
    "presence index not sorted: " & $v0 & "," & $v1 & "," & $v2 & "," & $v3

  echo "PASS: test_ctp_presence_idx"

proc test_ctp_permanent_evictable() {.raises: [].} =
  ## Fetch 10 blocks, mark 3 as permanent, evict down to 5 blocks.
  const blockSize = 128

  var remote: seq[seq[byte]]
  for b in 0 ..< 20:
    var blk = newSeq[byte](blockSize)
    for i in 0 ..< blockSize:
      blk[i] = byte(b)
    remote.add(blk)

  let fetcher: BlockFetcher = proc(blockId: uint64): Result[seq[byte], string] =
    if int(blockId) >= remote.len:
      return err("out of range")
    ok(remote[int(blockId)])

  var cache = initPartialTraceCache(fetcher, blockSize = blockSize)

  # Fetch 10 blocks (0..9).
  for i in 0'u64 ..< 10:
    doAssert cache.fetchBlock(i).isOk

  doAssert cache.presenceCount() == 10

  # Mark blocks 2, 5, 8 as permanent.
  cache.markPermanent(2)
  cache.markPermanent(5)
  cache.markPermanent(8)
  doAssert cache.permanentCount() == 3

  # Evict to fit 5 blocks worth of space.
  let targetBytes = uint64(5 * blockSize)
  let evicted = cache.evict(targetBytes)
  doAssert evicted == 5, "expected 5 evicted, got " & $evicted

  doAssert cache.presenceCount() == 5

  # The 3 permanent blocks must survive.
  doAssert cache.hasBlock(2), "permanent block 2 was evicted"
  doAssert cache.hasBlock(5), "permanent block 5 was evicted"
  doAssert cache.hasBlock(8), "permanent block 8 was evicted"

  echo "PASS: test_ctp_permanent_evictable"

proc test_ctp_transparent_api() {.raises: [].} =
  ## Create a fake multi-block trace, serve via fetcher, verify reads.
  const blockSize = 256
  const numBlocks = 8

  # Build a fake trace: sequence of blocks with known content.
  var traceData = newSeq[byte](blockSize * numBlocks)
  for i in 0 ..< traceData.len:
    traceData[i] = byte(i mod 256)

  let fetcher: BlockFetcher = proc(blockId: uint64): Result[seq[byte], string] =
    let start = int(blockId) * blockSize
    if start + blockSize > traceData.len:
      return err("block out of range")
    var blk = newSeq[byte](blockSize)
    for i in 0 ..< blockSize:
      blk[i] = traceData[start + i]
    ok(blk)

  var cache = initPartialTraceCache(fetcher, blockSize = blockSize)

  # Read several blocks and verify content matches the original trace.
  for b in 0'u64 ..< uint64(numBlocks):
    let r = cache.fetchBlock(b)
    doAssert r.isOk, "fetchBlock(" & $b & ") failed: " & r.error
    let data = r.get()
    let start = int(b) * blockSize
    for i in 0 ..< blockSize:
      doAssert data[i] == traceData[start + i],
        "mismatch at block " & $b & " offset " & $i

  doAssert cache.presenceCount() == numBlocks

  echo "PASS: test_ctp_transparent_api"

proc test_ctp_name_classification() {.raises: [].} =
  ## Verify file name classification for permanent vs evictable.
  # Permanent files
  doAssert isPermanentFileName("meta.dat")
  doAssert isPermanentFileName("paths.off")
  doAssert isPermanentFileName("presence.idx")
  doAssert isPermanentFileName("linehits.tc")
  doAssert isPermanentFileName("steps.ns")

  # Evictable files
  doAssert not isPermanentFileName("steps.dat")
  doAssert not isPermanentFileName("values.bin")
  doAssert not isPermanentFileName("events.log")

  doAssert isEvictableFileName("steps.dat")
  doAssert not isEvictableFileName("meta.dat")

  echo "PASS: test_ctp_name_classification"

# Run all tests
test_ctp_fetch_on_miss()
test_ctp_presence_idx()
test_ctp_permanent_evictable()
test_ctp_transparent_api()
test_ctp_name_classification()
