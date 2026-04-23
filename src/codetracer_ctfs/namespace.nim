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

proc tree*(ns: Namespace): BTree {.inline.} =
  ## Access the namespace's B-tree (for analysis).
  ns.tree

proc pool*(ns: Namespace): SubBlockPoolManager {.inline.} =
  ## Access the namespace's pool manager (for analysis).
  ns.pool

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

# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------
#
# Namespace on-disk format:
#   [Header: 32 bytes]
#     Bytes  0-3:   magic "NS\x01\0"
#     Byte   4:     leafType (0 = TypeA, 1 = TypeB)
#     Bytes  5-7:   reserved (0)
#     Bytes  8-15:  entryCount (uint64 LE)
#     Bytes 16-19:  nameLen (uint32 LE)
#     Bytes 20-23:  btreeLen (uint32 LE) — length of serialized B-tree
#     Bytes 24-27:  poolDataLen (uint32 LE) — length of serialized pool section
#     Bytes 28-31:  reserved (0)
#   [name: nameLen bytes]
#   [B-tree bytes: btreeLen bytes]
#   [Pool data section: poolDataLen bytes]
#
# Pool data section format:
#   For each of the 7 pool classes (in order 0..6):
#     [blockCount: uint32 LE][freeListHead: 7 bytes][buffer: blockCount * 4096 bytes]
#   freeListHead: [blockIdx: uint32 LE][slotIndex: uint16 LE][isEmpty: uint8]

const
  NsHeaderSize = 32
  FreeListHeadSize = 7  # 4 + 2 + 1

proc writeU16LE(buf: var seq[byte], offset: int, val: uint16) =
  buf[offset] = byte(val and 0xFF)
  buf[offset + 1] = byte((val shr 8) and 0xFF)

proc writeU32LE(buf: var seq[byte], offset: int, val: uint32) =
  buf[offset] = byte(val and 0xFF)
  buf[offset + 1] = byte((val shr 8) and 0xFF)
  buf[offset + 2] = byte((val shr 16) and 0xFF)
  buf[offset + 3] = byte((val shr 24) and 0xFF)

proc readU32LE(buf: openArray[byte], offset: int): uint32 =
  uint32(buf[offset]) or
    (uint32(buf[offset + 1]) shl 8) or
    (uint32(buf[offset + 2]) shl 16) or
    (uint32(buf[offset + 3]) shl 24)

proc writeU64LE(buf: var seq[byte], offset: int, val: uint64) =
  for i in 0 ..< 8:
    buf[offset + i] = byte((val shr (i * 8)) and 0xFF)

proc readU64LE(buf: openArray[byte], offset: int): uint64 =
  for i in 0 ..< 8:
    result = result or (uint64(buf[offset + i]) shl (i * 8))

proc serialize*(ns: Namespace): seq[byte] =
  ## Serialize the namespace (B-tree + pool data) for persistence in a CTFS
  ## container. Returns a self-contained byte sequence that can be written
  ## to disk and later restored with deserializeNamespace().
  let btreeBytes = ns.tree.serialize()
  let nameBytes = ns.name.len

  # Compute pool data size.
  var poolDataLen = 0
  for pc in 0 ..< 7:
    poolDataLen += 4 + FreeListHeadSize  # blockCount + freeListHead
    poolDataLen += ns.pool.buffers[pc].len

  let totalSize = NsHeaderSize + nameBytes + btreeBytes.len + poolDataLen
  result = newSeq[byte](totalSize)

  # Header.
  result[0] = byte('N')
  result[1] = byte('S')
  result[2] = 1  # version
  result[3] = 0
  result[4] = uint8(ns.leafType)
  writeU64LE(result, 8, ns.entryCount)
  writeU32LE(result, 16, uint32(nameBytes))
  writeU32LE(result, 20, uint32(btreeBytes.len))
  writeU32LE(result, 24, uint32(poolDataLen))

  var offset = NsHeaderSize

  # Name.
  for i in 0 ..< nameBytes:
    result[offset + i] = byte(ns.name[i])
  offset += nameBytes

  # B-tree.
  for i in 0 ..< btreeBytes.len:
    result[offset + i] = btreeBytes[i]
  offset += btreeBytes.len

  # Pool data.
  for pc in 0 ..< 7:
    writeU32LE(result, offset, ns.pool.blockCounts[pc])
    offset += 4
    # Free list head.
    let flh = ns.pool.freeListHeads[pc]
    writeU32LE(result, offset, flh.blockIdx)
    offset += 4
    writeU16LE(result, offset, flh.slotIndex)
    offset += 2
    result[offset] = if flh.isEmpty: 1'u8 else: 0'u8
    offset += 1
    # Buffer data.
    for i in 0 ..< ns.pool.buffers[pc].len:
      result[offset + i] = ns.pool.buffers[pc][i]
    offset += ns.pool.buffers[pc].len

proc deserializeNamespace*(data: openArray[byte],
    leafType: LeafType): Result[Namespace, string] =
  ## Deserialize a namespace from bytes produced by serialize().
  if data.len < NsHeaderSize:
    return err("data too short for namespace header")

  if data[0] != byte('N') or data[1] != byte('S') or
      data[2] != 1 or data[3] != 0:
    return err("invalid namespace magic")

  let storedLeafType = int(data[4])
  if storedLeafType != int(leafType):
    return err("leaf type mismatch: stored=" & $storedLeafType &
                " expected=" & $int(leafType))

  let entryCount = readU64LE(data, 8)
  let nameLen = int(readU32LE(data, 16))
  let btreeLen = int(readU32LE(data, 20))
  let poolDataLen = int(readU32LE(data, 24))

  let expectedSize = NsHeaderSize + nameLen + btreeLen + poolDataLen
  if data.len < expectedSize:
    return err("data too short: need " & $expectedSize & " got " & $data.len)

  var offset = NsHeaderSize

  # Read name.
  var name = newString(nameLen)
  for i in 0 ..< nameLen:
    name[i] = char(data[offset + i])
  offset += nameLen

  # Read B-tree.
  let descSize = case leafType
    of ltTypeA: 8
    of ltTypeB: 16
  let btreeSlice = data[offset ..< offset + btreeLen]
  let btreeRes = btree.deserialize(btreeSlice, descSize)
  if btreeRes.isErr:
    return err("B-tree deserialize failed: " & btreeRes.error)
  offset += btreeLen

  # Read pool data.
  var pool = initSubBlockPoolManager()
  for pc in 0 ..< 7:
    if offset + 4 + FreeListHeadSize > data.len:
      return err("pool data truncated at class " & $pc)
    let blockCount = readU32LE(data, offset)
    offset += 4

    let blockIdx = readU32LE(data, offset)
    offset += 4
    let slotIndex = uint16(data[offset]) or (uint16(data[offset + 1]) shl 8)
    offset += 2
    let isEmpty = data[offset] == 1
    offset += 1

    pool.blockCounts[pc] = blockCount
    pool.freeListHeads[pc] = FreeListHead(
      blockIdx: blockIdx,
      slotIndex: slotIndex,
      isEmpty: isEmpty,
    )

    let bufLen = int(blockCount) * 4096
    pool.buffers[pc] = newSeq[byte](bufLen)
    for i in 0 ..< bufLen:
      if offset + i >= data.len:
        return err("pool buffer data truncated at class " & $pc)
      pool.buffers[pc][i] = data[offset + i]
    offset += bufLen

  var ns = Namespace(
    name: name,
    leafType: leafType,
    tree: btreeRes.get(),
    pool: pool,
    entryCount: entryCount,
  )
  ok(ns)
