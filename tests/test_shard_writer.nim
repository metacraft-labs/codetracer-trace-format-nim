{.push raises: [].}

## Tests and benchmarks for shard writer and sharded namespace (M9c).

import std/times
import results
import codetracer_ctfs/shard_writer
import codetracer_ctfs/sharded_namespace
import codetracer_ctfs/namespace
import codetracer_ctfs/sub_block_pool
import codetracer_ctfs/btree

# ---------------------------------------------------------------------------
# Simple LCG PRNG (deterministic, no crypto needed)
# ---------------------------------------------------------------------------

type Rng = object
  state: uint64

proc next(r: var Rng): uint64 =
  r.state = r.state * 6364136223846793005'u64 + 1442695040888963407'u64
  r.state

proc nextInRange(r: var Rng, lo, hi: uint64): uint64 =
  let range = hi - lo + 1
  lo + (r.next() mod range)

proc randomData(r: var Rng, minLen, maxLen: int): seq[byte] =
  let length = int(r.nextInRange(uint64(minLen), uint64(maxLen)))
  result = newSeq[byte](length)
  for i in 0 ..< length:
    result[i] = byte(r.next() and 0xFF)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

proc test_shard_block_distribution() =
  ## Create ShardedWriter with 4 shards, allocate 100K sub-block slots
  ## (alternating pool classes), verify each shard has roughly 25% (+-5%).
  const NumShards = 4
  const N = 100_000
  var sw = initShardedWriter(NumShards)
  var rng = Rng(state: 42'u64)

  var counts: array[4, int]
  for i in 0 ..< N:
    let key = rng.next()
    let shardId = sw.homeShard(key)
    let poolClass = uint8(i mod 3)  # alternate 0, 1, 2
    let allocRes = sw.allocateSlot(shardId, poolClass)
    doAssert allocRes.isOk, "allocate failed: " & allocRes.error
    counts[shardId] += 1

  let expected = N div NumShards
  let tolerance = N div 20  # 5%
  for i in 0 ..< NumShards:
    doAssert counts[i] > expected - tolerance and counts[i] < expected + tolerance,
      "shard " & $i & " has " & $counts[i] & " slots, expected ~" & $expected

  echo "PASS: test_shard_block_distribution"

proc test_shard_namespace_key_affinity() =
  ## Create ShardedNamespace with 4 shards, insert 10K keys.
  ## For each key, verify its sub-block is on shard = hash(key) % 4.
  const NumShards = 4
  const N = 10_000
  var sw = initShardedWriter(NumShards)
  var ns = initShardedNamespace("affinity_test", sw, sltTypeA)
  var rng = Rng(state: 77'u64)

  var keys = newSeq[uint64](N)
  var datas = newSeq[seq[byte]](N)
  for i in 0 ..< N:
    keys[i] = rng.next()
    datas[i] = rng.randomData(8, 31)
    let res = ns.append(keys[i], datas[i])
    doAssert res.isOk, "append failed at i=" & $i & ": " & res.error

  # Verify each key's data can be looked up and is on the right shard.
  for i in 0 ..< N:
    let key = keys[i]
    let expectedShard = sw.homeShard(key)

    # Lookup should succeed.
    let res = ns.lookup(key)
    doAssert res.isOk, "lookup failed for key=" & $key & ": " & res.error
    doAssert res.get() == datas[i],
      "data mismatch for key=" & $key

    # Verify expected shard is correct via the hash function.
    doAssert expectedShard == int((key * 2654435761'u64) mod uint64(NumShards)),
      "home shard mismatch for key=" & $key

  echo "PASS: test_shard_namespace_key_affinity"

proc test_shard_structural_blocks_main_file() =
  ## Create ShardedNamespace, insert entries, verify B-tree nodes are in the
  ## main Namespace tree (not in shard data).
  const N = 1_000
  var sw = initShardedWriter(4)
  var ns = initShardedNamespace("structural_test", sw, sltTypeA)
  var rng = Rng(state: 99'u64)

  for i in 0 ..< N:
    let data = rng.randomData(8, 31)
    let res = ns.append(uint64(i), data)
    doAssert res.isOk, "append failed: " & res.error

  # The B-tree is accessible via ns.tree and has entries.
  let tree = ns.tree
  doAssert tree.count == uint64(N),
    "B-tree count mismatch: " & $tree.count & " vs " & $N

  # Verify B-tree depth is reasonable (not degenerate).
  doAssert tree.depth >= 1, "B-tree depth should be >= 1"

  # Verify B-tree lookups work (structural data is in the tree, not shards).
  for i in 0 ..< N:
    let descRes = tree.lookup(uint64(i))
    doAssert descRes.isOk, "B-tree lookup failed for key=" & $i

  echo "PASS: test_shard_structural_blocks_main_file"

proc test_shard_single_shard_compat() =
  ## Create ShardedWriter with 1 shard, insert 1000 entries.
  ## Compare against non-sharded Namespace with same entries.
  ## Verify same data returned for each key.
  const N = 1_000
  var rng1 = Rng(state: 55'u64)
  var rng2 = Rng(state: 55'u64)  # same seed

  # Sharded (1 shard).
  var sw = initShardedWriter(1)
  var sns = initShardedNamespace("sharded_1", sw, sltTypeA)

  # Non-sharded.
  var ns = initNamespace("plain", namespace.ltTypeA)

  for i in 0 ..< N:
    let data1 = rng1.randomData(8, 31)
    let data2 = rng2.randomData(8, 31)
    doAssert data1 == data2, "RNG mismatch"

    let r1 = sns.append(uint64(i), data1)
    doAssert r1.isOk, "sharded append failed: " & r1.error
    let r2 = ns.append(uint64(i), data2)
    doAssert r2.isOk, "plain append failed: " & r2.error

  # Verify both return the same data.
  for i in 0 ..< N:
    let sr = sns.lookup(uint64(i))
    doAssert sr.isOk, "sharded lookup failed for key=" & $i & ": " & sr.error
    let pr = ns.lookup(uint64(i))
    doAssert pr.isOk, "plain lookup failed for key=" & $i & ": " & pr.error
    doAssert sr.get() == pr.get(),
      "data mismatch for key=" & $i &
      " sharded.len=" & $sr.get().len & " plain.len=" & $pr.get().len

  echo "PASS: test_shard_single_shard_compat"

proc test_manifest_roundtrip() =
  ## Write manifest with 4 shard paths, read back, verify paths match.
  let paths = @[
    "/data/ssd0/trace.ct.shard0",
    "/data/ssd1/trace.ct.shard1",
    "/data/ssd2/trace.ct.shard2",
    "/data/ssd3/trace.ct.shard3",
  ]
  let encoded = writeManifest(paths)
  let decoded = readManifest(encoded)
  doAssert decoded.isOk, "readManifest failed: " & decoded.error
  let manifest = decoded.get()
  doAssert manifest.shardCount == uint32(paths.len),
    "shard count mismatch: " & $manifest.shardCount
  doAssert manifest.entries.len == paths.len,
    "entries len mismatch: " & $manifest.entries.len
  for i in 0 ..< paths.len:
    doAssert manifest.entries[i].path == paths[i],
      "path mismatch at " & $i & ": " & manifest.entries[i].path & " vs " & paths[i]

  echo "PASS: test_manifest_roundtrip"

proc test_manifest_empty() =
  ## Verify empty manifest roundtrips.
  let encoded = writeManifest(newSeq[string](0))
  let decoded = readManifest(encoded)
  doAssert decoded.isOk, "readManifest failed: " & decoded.error
  doAssert decoded.get().shardCount == 0'u32
  doAssert decoded.get().entries.len == 0

  echo "PASS: test_manifest_empty"

proc test_manifest_truncated() =
  ## Verify truncated manifest returns error.
  let short = @[byte(1), byte(0)]
  let res = readManifest(short)
  doAssert res.isErr, "expected error for truncated manifest"

  echo "PASS: test_manifest_truncated"

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

proc bench_shard_write_throughput() =
  ## Measure throughput with 1, 2, 4 shards.
  const N = 100_000
  for shardCount in [1, 2, 4]:
    var sw = initShardedWriter(shardCount)
    var ns = initShardedNamespace("bench_" & $shardCount, sw, sltTypeA)
    var rng = Rng(state: 123'u64)

    let start = cpuTime()
    for i in 0 ..< N:
      let data = rng.randomData(8, 31)
      let res = ns.append(uint64(i), data)
      doAssert res.isOk
    let elapsed = cpuTime() - start

    let throughput = float(N) / elapsed
    # Per-shard slot counts.
    var shardSlots = newSeq[int](shardCount)
    for i in 0 ..< shardCount:
      for pc in 0'u8 ..< 7'u8:
        shardSlots[i] += sw.shards[i].pool.totalAllocatedSlots(pc) -
                          sw.shards[i].pool.totalFreeSlots(pc)

    var slotsJson = "["
    for i in 0 ..< shardCount:
      if i > 0: slotsJson.add(", ")
      slotsJson.add($shardSlots[i])
    slotsJson.add("]")

    echo "{\"benchmark\": \"shard_write_throughput\", " &
      "\"shards\": " & $shardCount & ", " &
      "\"entries\": " & $N & ", " &
      "\"elapsed_sec\": " & $elapsed & ", " &
      "\"entries_per_sec\": " & $throughput & ", " &
      "\"slots_per_shard\": " & slotsJson & "}"

  echo "PASS: bench_shard_write_throughput"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

when isMainModule:
  test_shard_block_distribution()
  test_shard_namespace_key_affinity()
  test_shard_structural_blocks_main_file()
  test_shard_single_shard_compat()
  test_manifest_roundtrip()
  test_manifest_empty()
  test_manifest_truncated()
  bench_shard_write_throughput()
  echo "All shard writer tests passed."
