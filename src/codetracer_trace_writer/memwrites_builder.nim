{.push raises: [].}

## Production CoW namespace encoder for `memwrites.tc`.
##
## The image matches the Rust db-backend `memwrites_namespace.rs` format:
## an `NSB1` Type-B `CowBTree` keyed by memory address. Each descriptor is
## `[payload_offset:u64][payload_len:u64]` and points at appended 40-byte
## records:
## `[interval_id:u32][tick:u64][pc:u64][size:u32][old_value:u64][new_value:u64]`.

import std/[algorithm, tables]
import results
import ../codetracer_ctfs/cow_btree

export results

type
  MemwriteCowRecord* = object
    intervalId*: uint32
    tick*: uint64
    pc*: uint64
    address*: uint64
    size*: uint32
    oldValue*: uint64
    newValue*: uint64

const MemwriteRecordSize* = 4 + 8 + 8 + 4 + 8 + 8

proc putU32LE(dst: var seq[byte], v: uint32) =
  for i in 0 ..< 4:
    dst.add(byte((v shr (i * 8)) and 0xFF))

proc putU64LE(dst: var seq[byte], v: uint64) =
  for i in 0 ..< 8:
    dst.add(byte((v shr (i * 8)) and 0xFF))

proc readU32LE(data: openArray[byte], off: int): uint32 =
  for i in 0 ..< 4:
    result = result or (uint32(data[off + i]) shl (i * 8))

proc readU64LE(data: openArray[byte], off: int): uint64 =
  for i in 0 ..< 8:
    result = result or (uint64(data[off + i]) shl (i * 8))

proc descriptor(offset, size: uint64): seq[byte] =
  result = @[]
  result.putU64LE(offset)
  result.putU64LE(size)

proc encodeRecord(rec: MemwriteCowRecord, dst: var seq[byte]) =
  dst.putU32LE(rec.intervalId)
  dst.putU64LE(rec.tick)
  dst.putU64LE(rec.pc)
  dst.putU32LE(rec.size)
  dst.putU64LE(rec.oldValue)
  dst.putU64LE(rec.newValue)

proc sortRecords(records: var seq[MemwriteCowRecord]) =
  records.sort(proc(a, b: MemwriteCowRecord): int =
    if a.address != b.address:
      return cmp(a.address, b.address)
    if a.intervalId != b.intervalId:
      return cmp(a.intervalId, b.intervalId)
    cmp(a.tick, b.tick)
  )

proc serializeMemwritesCowNamespace*(records: openArray[MemwriteCowRecord]):
    Result[seq[byte], string] =
  ## Build a production `memwrites.tc` CoW namespace image.
  ##
  ## Empty input returns an empty `NSB1` namespace, so callers that always create
  ## `memwrites.tc` can still emit a valid CoW image.
  var sorted: seq[MemwriteCowRecord] = @records
  sorted.sortRecords()

  var keys: seq[uint64]
  var byAddress = initTable[uint64, seq[MemwriteCowRecord]]()
  for rec in sorted:
    if not byAddress.hasKey(rec.address):
      keys.add(rec.address)
    byAddress.mgetOrPut(rec.address, @[]).add(rec)

  var sizingTree = initCowBTree(cltTypeB, skipSubBlocks = true)
  let zeroDesc = descriptor(0, 0)
  for key in keys:
    discard ?sizingTree.insertAndCommit(key, zeroDesc)
  let payloadBase = uint64(sizingTree.serialize().len)

  var payload: seq[byte] = @[]
  var finalTree = initCowBTree(cltTypeB, skipSubBlocks = true)
  for key in keys:
    let off = payloadBase + uint64(payload.len)
    let before = payload.len
    for rec in byAddress.getOrDefault(key):
      rec.encodeRecord(payload)
    discard ?finalTree.insertAndCommit(key, descriptor(off, uint64(payload.len - before)))

  var image = finalTree.serialize()
  image.add(payload)
  while image.len mod PageSize != 0:
    image.add(0)
  ok(image)

proc decodeCowMemwritesPayloadForTest*(image: openArray[byte],
    address: uint64): Result[seq[MemwriteCowRecord], string] =
  ## Test helper that proves callers emitted a real `NSB1` CowBTree image and
  ## resolves one address through the same Nim `CowBTree` reader as production.
  let loaded = ?loadCowBTree(image, cltTypeB)
  let desc = ?loaded.lookup(address)
  if desc.len != 16:
    return err("bad memwrites descriptor size")
  let off = int(readU64LE(desc, 0))
  let size = int(readU64LE(desc, 8))
  if off < 0 or size < 0 or off + size > image.len:
    return err("memwrites payload out of bounds")
  if size mod MemwriteRecordSize != 0:
    return err("memwrites payload has partial record")

  var records: seq[MemwriteCowRecord]
  var pos = off
  let endPos = off + size
  while pos < endPos:
    var rec: MemwriteCowRecord
    rec.address = address
    rec.intervalId = readU32LE(image, pos)
    pos += 4
    rec.tick = readU64LE(image, pos)
    pos += 8
    rec.pc = readU64LE(image, pos)
    pos += 8
    rec.size = readU32LE(image, pos)
    pos += 4
    rec.oldValue = readU64LE(image, pos)
    pos += 8
    rec.newValue = readU64LE(image, pos)
    pos += 8
    records.add(rec)
  ok(records)

proc decodeCowMemwritesNamespace*(image: openArray[byte]):
    Result[seq[MemwriteCowRecord], string] =
  ## Decode every write from a production `NSB1` `memwrites.tc` image.
  let loaded = ?loadCowBTree(image, cltTypeB)
  let addresses = ?loaded.keys()
  var decoded: seq[MemwriteCowRecord]
  for address in addresses:
    let records = ?decodeCowMemwritesPayloadForTest(image, address)
    for rec in records:
      decoded.add(rec)
  decoded.sortRecords()
  ok(decoded)
