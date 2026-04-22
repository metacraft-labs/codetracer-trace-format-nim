{.push raises: [].}

## Shard writer for CTFS block sharding (M9c).
##
## Distributes data blocks across multiple shards while keeping structural
## blocks (B-tree, mapping, indices) in the main .ct file. Each shard has
## its own sub-block pool manager with independent free lists.
##
## Key affinity: hash(key) % shardCount determines which shard owns a key's
## sub-blocks.

import results
import ./types
import ./sub_block_pool
import ../codetracer_trace_writer/varint

export results

type
  ShardWriter* = object
    id*: int
    pool*: SubBlockPoolManager
    data*: seq[byte]  ## shard's data blocks
    blockCount*: int

  ShardedWriter* = object
    shards*: seq[ShardWriter]
    shardCount*: int
    nextBlockRoundRobin*: int  ## for graduated multi-block round-robin

  ManifestEntry* = object
    path*: string

  Manifest* = object
    shardCount*: uint32
    entries*: seq[ManifestEntry]

# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

proc initShardWriter(id: int): ShardWriter =
  ShardWriter(
    id: id,
    pool: initSubBlockPoolManager(),
    data: newSeq[byte](0),
    blockCount: 0,
  )

proc initShardedWriter*(shardCount: int): ShardedWriter =
  ## Create a sharded writer with the given number of shards.
  ## Each shard gets its own independent sub-block pool manager.
  var sw = ShardedWriter(
    shardCount: shardCount,
    nextBlockRoundRobin: 0,
  )
  sw.shards = newSeq[ShardWriter](shardCount)
  for i in 0 ..< shardCount:
    sw.shards[i] = initShardWriter(i)
  sw

# ---------------------------------------------------------------------------
# Key hashing / shard selection
# ---------------------------------------------------------------------------

proc homeShard*(sw: ShardedWriter, key: uint64): int =
  ## Determine which shard owns a key's sub-blocks.
  ## Uses Knuth multiplicative hash for more uniform distribution.
  int((key * 2654435761'u64) mod uint64(sw.shardCount))

# ---------------------------------------------------------------------------
# Slot operations (delegate to per-shard pool)
# ---------------------------------------------------------------------------

proc allocateSlot*(sw: var ShardedWriter, shardId: int,
    poolClass: uint8): Result[SubBlockAllocation, string] =
  ## Allocate a sub-block slot on the specified shard.
  if shardId < 0 or shardId >= sw.shardCount:
    return err("shard id out of range: " & $shardId)
  sw.shards[shardId].pool.allocate(poolClass)

proc freeSlot*(sw: var ShardedWriter, shardId: int,
    alloc: SubBlockAllocation): Result[void, string] =
  ## Free a sub-block slot on the specified shard.
  if shardId < 0 or shardId >= sw.shardCount:
    return err("shard id out of range: " & $shardId)
  sw.shards[shardId].pool.free(alloc)

proc readSlot*(sw: ShardedWriter, shardId: int,
    alloc: SubBlockAllocation,
    output: var openArray[byte]): Result[int, string] =
  ## Read data from a sub-block slot on the specified shard.
  if shardId < 0 or shardId >= sw.shardCount:
    return err("shard id out of range: " & $shardId)
  sw.shards[shardId].pool.readSlot(alloc, output)

proc writeSlot*(sw: var ShardedWriter, shardId: int,
                alloc: SubBlockAllocation,
                data: openArray[byte]): Result[void, string] =
  ## Write data to a sub-block slot on the specified shard.
  if shardId < 0 or shardId >= sw.shardCount:
    return err("shard id out of range: " & $shardId)
  sw.shards[shardId].pool.writeSlot(alloc, data)

proc promoteSlot*(sw: var ShardedWriter, shardId: int,
                  alloc: SubBlockAllocation,
                  newPoolClass: uint8): Result[SubBlockAllocation, string] =
  ## Promote a sub-block slot to a larger pool class on the same shard.
  if shardId < 0 or shardId >= sw.shardCount:
    return err("shard id out of range: " & $shardId)
  sw.shards[shardId].pool.promote(alloc, newPoolClass)

# ---------------------------------------------------------------------------
# Manifest serialization
# ---------------------------------------------------------------------------

proc writeManifest*(paths: openArray[string]): seq[byte] =
  ## Serialize a manifest: u32 shardCount + (varint len + path bytes) per shard.
  var buf = newSeq[byte](4)
  let count = uint32(paths.len)
  let le = toBytesLE(count)
  buf[0] = le[0]
  buf[1] = le[1]
  buf[2] = le[2]
  buf[3] = le[3]
  for path in paths:
    encodeVarint(uint64(path.len), buf)
    for ch in path:
      buf.add(byte(ch))
  buf

proc readManifest*(data: openArray[byte]): Result[Manifest, string] =
  ## Deserialize a manifest from bytes.
  if data.len < 4:
    return err("manifest too short")
  var arr: array[4, byte]
  arr[0] = data[0]
  arr[1] = data[1]
  arr[2] = data[2]
  arr[3] = data[3]
  let shardCount = fromBytesLE(uint32, arr)

  var pos = 4
  var entries = newSeq[ManifestEntry](int(shardCount))
  for i in 0 ..< int(shardCount):
    let lenRes = decodeVarint(data, pos)
    if lenRes.isErr:
      return err("manifest: failed to decode path length: " & lenRes.error)
    let pathLen = int(lenRes.get())
    if pos + pathLen > data.len:
      return err("manifest: path data truncated")
    var path = newString(pathLen)
    for j in 0 ..< pathLen:
      path[j] = char(data[pos + j])
    pos += pathLen
    entries[i] = ManifestEntry(path: path)

  ok(Manifest(shardCount: shardCount, entries: entries))
