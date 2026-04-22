{.push raises: [].}

## Sharded namespace: combines B-tree key index with sharded sub-block pools.
##
## The B-tree always stays in the main (non-sharded) file. Data sub-blocks
## are distributed across shards based on key hash affinity:
##   shard = hash(key) % shardCount
##
## Descriptors encode the shard ID implicitly via the key hash, so lookups
## recompute the home shard from the key.

import results
import ./btree
import ./sub_block_pool
import ./namespace_descriptor
import ./shard_writer

export results

type
  ShardedLeafType* = enum
    sltTypeA = 0  ## 8-byte descriptors
    sltTypeB = 1  ## 16-byte descriptors

  ShardedNamespace* = object
    name*: string
    leafType*: ShardedLeafType
    tree: BTree  # always in main file (not sharded)
    writer: ptr ShardedWriter  # non-owning ref
    entryCount: uint64

proc initShardedNamespace*(name: string, writer: var ShardedWriter,
    leafType: ShardedLeafType = sltTypeA): ShardedNamespace =
  let descSize = case leafType
    of sltTypeA: 8
    of sltTypeB: 16
  ShardedNamespace(
    name: name,
    leafType: leafType,
    tree: initBTree(descSize),
    writer: addr writer,
    entryCount: 0,
  )

proc count*(ns: ShardedNamespace): uint64 {.inline.} =
  ns.entryCount

proc tree*(ns: ShardedNamespace): BTree {.inline.} =
  ns.tree

# ---------------------------------------------------------------------------
# Append
# ---------------------------------------------------------------------------

proc append*(ns: var ShardedNamespace, key: uint64,
    data: openArray[byte]): Result[void, string] =
  ## Append a key-value entry. The data is stored on the shard determined by
  ## hash(key) % shardCount. The descriptor is inserted into the B-tree
  ## (which lives in the main file).
  let dataLen = data.len
  if dataLen == 0:
    return err("cannot append empty data")

  # Find smallest pool class that fits.
  var poolClass: uint8 = 0
  while poolClass < 6 and (poolSize(poolClass) < dataLen or
        dataLen > int((1'u16 shl usedBytesBits(poolClass)) - 1)):
    poolClass += 1

  if dataLen > poolSize(6) or
      dataLen > int((1'u16 shl usedBytesBits(6)) - 1):
    return err("data too large for sub-block pools")

  let shardId = ns.writer[].homeShard(key)

  # Allocate slot on the home shard.
  let allocRes = ns.writer[].allocateSlot(shardId, poolClass)
  if allocRes.isErr:
    return err("allocation failed: " & allocRes.error)
  let alloc = allocRes.get()

  # Write data to slot.
  let writeRes = ns.writer[].writeSlot(shardId, alloc, data)
  if writeRes.isErr:
    return err("write failed: " & writeRes.error)

  # Create descriptor.
  var descriptor: seq[byte]
  case ns.leafType
  of sltTypeA:
    let desc = encodeTypeASubBlock(
      uint64(alloc.blockNum), poolClass, alloc.slotIndex, uint16(dataLen))
    descriptor = newSeq[byte](8)
    let raw = desc.raw
    for i in 0 ..< 8:
      descriptor[i] = byte((raw shr (i * 8)) and 0xFF)
  of sltTypeB:
    let desc = encodeTypeBSubBlock(
      uint64(alloc.blockNum), poolClass, alloc.slotIndex, uint16(dataLen))
    descriptor = newSeq[byte](16)
    for i in 0 ..< 8:
      descriptor[i] = byte((desc.word0 shr (i * 8)) and 0xFF)
    for i in 0 ..< 8:
      descriptor[8 + i] = byte((desc.word1 shr (i * 8)) and 0xFF)

  # Insert into B-tree (main file).
  ns.tree.insert(key, descriptor)
  ns.entryCount += 1
  ok()

# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------

proc lookupTypeA(ns: ShardedNamespace, key: uint64,
    descriptor: seq[byte]): Result[seq[byte], string] =
  var raw: uint64 = 0
  for i in 0 ..< 8:
    raw = raw or (uint64(descriptor[i]) shl (i * 8))
  let desc = TypeADescriptor(raw: raw)
  if desc.isGraduated:
    return err("graduated entries not yet supported in sharded lookup")
  let sub = decodeTypeASubBlock(desc)
  let alloc = SubBlockAllocation(
    blockNum: sub.blockNum,
    slotIndex: sub.slotIndex,
    poolClass: sub.poolClass,
  )
  let shardId = ns.writer[].homeShard(key)
  var buf = newSeq[byte](poolSize(sub.poolClass))
  let readRes = ns.writer[].readSlot(shardId, alloc, buf)
  if readRes.isErr:
    return err(readRes.error)
  buf.setLen(int(sub.usedBytes))
  ok(buf)

proc lookupTypeB(ns: ShardedNamespace, key: uint64,
    descriptor: seq[byte]): Result[seq[byte], string] =
  var w0: uint64 = 0
  for i in 0 ..< 8:
    w0 = w0 or (uint64(descriptor[i]) shl (i * 8))
  var w1: uint64 = 0
  for i in 0 ..< 8:
    w1 = w1 or (uint64(descriptor[8 + i]) shl (i * 8))
  let desc = TypeBDescriptor(word0: w0, word1: w1)
  if desc.isGraduated:
    return err("graduated entries not yet supported in sharded lookup")
  let sub = decodeTypeBSubBlock(desc)
  let alloc = SubBlockAllocation(
    blockNum: sub.blockNum,
    slotIndex: sub.slotIndex,
    poolClass: sub.poolClass,
  )
  let shardId = ns.writer[].homeShard(key)
  var buf = newSeq[byte](poolSize(sub.poolClass))
  let readRes = ns.writer[].readSlot(shardId, alloc, buf)
  if readRes.isErr:
    return err(readRes.error)
  buf.setLen(int(sub.usedBytes))
  ok(buf)

# ---------------------------------------------------------------------------
# Lookup
# ---------------------------------------------------------------------------

proc lookup*(ns: ShardedNamespace, key: uint64): Result[seq[byte], string] =
  ## Look up a key and return its data. The shard is determined by
  ## recomputing hash(key) % shardCount.
  let descRes = ns.tree.lookup(key)
  if descRes.isErr:
    return err(descRes.error)
  let descriptor = descRes.get()

  case ns.leafType
  of sltTypeA:
    ns.lookupTypeA(key, descriptor)
  of sltTypeB:
    ns.lookupTypeB(key, descriptor)
