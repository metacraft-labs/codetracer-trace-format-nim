{.push raises: [].}

## Namespace: unified abstraction combining B-tree key index with sub-block
## allocation pools. Maps uint64 keys to variable-length data stored in
## sub-block pools.

import results
import ./btree
import ./sub_block_pool
import ./namespace_descriptor

type
  LeafType* = enum
    ltTypeA = 0  ## 8-byte descriptors, for many small entries
    ltTypeB = 1  ## 16-byte descriptors, for fewer large entries

  Namespace* = object
    name*: string
    leafType*: LeafType
    tree: BTree
    pool: SubBlockPoolManager
    entryCount: uint64

  NamespaceEntry* = object
    key*: uint64
    data*: seq[byte]

proc initNamespace*(name: string, leafType: LeafType = ltTypeA): Namespace =
  let descSize = case leafType
    of ltTypeA: 8
    of ltTypeB: 16
  Namespace(
    name: name,
    leafType: leafType,
    tree: initBTree(descSize),
    pool: initSubBlockPoolManager(),
    entryCount: 0,
  )

proc count*(ns: Namespace): uint64 {.inline.} =
  ns.entryCount

proc append*(ns: var Namespace, key: uint64,
    data: openArray[byte]): Result[void, string] =
  ## Append a key-value entry to the namespace.
  ## Allocates a sub-block of the appropriate size class, writes data,
  ## then inserts the key with its descriptor into the B-tree.
  let dataLen = data.len
  if dataLen == 0:
    return err("cannot append empty data")

  # Find smallest pool class that fits both the data and the usedBytes
  # descriptor field. The usedBytes field has `usedBytesBits(pc)` bits,
  # so max representable value is (1 shl usedBytesBits) - 1.
  var poolClass: uint8 = 0
  while poolClass < 6 and (poolSize(poolClass) < dataLen or
        dataLen > int((1'u16 shl usedBytesBits(poolClass)) - 1)):
    poolClass += 1

  if dataLen > poolSize(6) or
      dataLen > int((1'u16 shl usedBytesBits(6)) - 1):
    return err("data too large for sub-block pools")

  # Allocate slot.
  let allocRes = ns.pool.allocate(poolClass)
  if allocRes.isErr:
    return err("allocation failed: " & allocRes.error)
  let alloc = allocRes.get()

  # Write data to slot.
  let writeRes = ns.pool.writeSlot(alloc, data)
  if writeRes.isErr:
    return err("write failed: " & writeRes.error)

  # Create descriptor based on leaf type.
  var descriptor: seq[byte]
  case ns.leafType
  of ltTypeA:
    let desc = encodeTypeASubBlock(
      uint64(alloc.blockNum), poolClass, alloc.slotIndex, uint16(dataLen))
    descriptor = newSeq[byte](8)
    let raw = desc.raw
    for i in 0 ..< 8:
      descriptor[i] = byte((raw shr (i * 8)) and 0xFF)
  of ltTypeB:
    let desc = encodeTypeBSubBlock(
      uint64(alloc.blockNum), poolClass, alloc.slotIndex, uint16(dataLen))
    descriptor = newSeq[byte](16)
    for i in 0 ..< 8:
      descriptor[i] = byte((desc.word0 shr (i * 8)) and 0xFF)
    for i in 0 ..< 8:
      descriptor[8 + i] = byte((desc.word1 shr (i * 8)) and 0xFF)

  # Insert into B-tree.
  ns.tree.insert(key, descriptor)
  ns.entryCount += 1
  ok()

proc lookupTypeA(ns: Namespace,
    descriptor: seq[byte]): Result[seq[byte], string] =
  var raw: uint64 = 0
  for i in 0 ..< 8:
    raw = raw or (uint64(descriptor[i]) shl (i * 8))
  let desc = TypeADescriptor(raw: raw)
  if desc.isGraduated:
    return err("graduated entries not yet supported in lookup")
  let sub = decodeTypeASubBlock(desc)
  let alloc = SubBlockAllocation(
    blockNum: sub.blockNum,
    slotIndex: sub.slotIndex,
    poolClass: sub.poolClass,
  )
  var buf = newSeq[byte](poolSize(sub.poolClass))
  let readRes = ns.pool.readSlot(alloc, buf)
  if readRes.isErr:
    return err(readRes.error)
  buf.setLen(int(sub.usedBytes))
  ok(buf)

proc lookupTypeB(ns: Namespace,
    descriptor: seq[byte]): Result[seq[byte], string] =
  var w0: uint64 = 0
  for i in 0 ..< 8:
    w0 = w0 or (uint64(descriptor[i]) shl (i * 8))
  var w1: uint64 = 0
  for i in 0 ..< 8:
    w1 = w1 or (uint64(descriptor[8 + i]) shl (i * 8))
  let desc = TypeBDescriptor(word0: w0, word1: w1)
  if desc.isGraduated:
    return err("graduated entries not yet supported in lookup")
  let sub = decodeTypeBSubBlock(desc)
  let alloc = SubBlockAllocation(
    blockNum: sub.blockNum,
    slotIndex: sub.slotIndex,
    poolClass: sub.poolClass,
  )
  var buf = newSeq[byte](poolSize(sub.poolClass))
  let readRes = ns.pool.readSlot(alloc, buf)
  if readRes.isErr:
    return err(readRes.error)
  buf.setLen(int(sub.usedBytes))
  ok(buf)

proc lookup*(ns: Namespace, key: uint64): Result[seq[byte], string] =
  ## Look up a key and return its data.
  let descRes = ns.tree.lookup(key)
  if descRes.isErr:
    return err(descRes.error)
  let descriptor = descRes.get()

  case ns.leafType
  of ltTypeA:
    ns.lookupTypeA(descriptor)
  of ltTypeB:
    ns.lookupTypeB(descriptor)

iterator items*(ns: Namespace, lo, hi: uint64): tuple[key: uint64, data: seq[byte]] =
  ## Yield all entries with keys in [lo, hi] in ascending order.
  for entry in ns.tree.rangeIter(lo, hi):
    let dataRes = case ns.leafType
      of ltTypeA: ns.lookupTypeA(entry.descriptor)
      of ltTypeB: ns.lookupTypeB(entry.descriptor)
    if dataRes.isOk:
      yield (entry.key, dataRes.get())

proc rangeScan*(ns: Namespace, lo, hi: uint64,
                output: var openArray[NamespaceEntry]): int =
  ## Collect entries with keys in [lo, hi] into output.
  ## Returns the number of entries written (capped by output.len).
  var i = 0
  for entry in ns.items(lo, hi):
    if i >= output.len:
      break
    output[i] = NamespaceEntry(key: entry.key, data: entry.data)
    i += 1
  i
