when defined(nimPreviewSlimSystem):
  import std/[assertions]

{.push raises: [].}

## Sub-block pool manager for CTFS.
##
## Divides CTFS blocks (4096 bytes) into smaller allocation units (32B-2048B)
## for namespaces with millions of small entries. Each pool class has its own
## free list of available slots.
##
## Pool classes:
##   0 = 32B, 1 = 64B, 2 = 128B, 3 = 256B, 4 = 512B, 5 = 1024B, 6 = 2048B

import results
import ./types
import ./namespace_descriptor

const
  NumPoolClasses* = 7
  SlotsPerBlock*: array[7, int] = [128, 64, 32, 16, 8, 4, 2]
  NextPtrSize = 6  ## bytes for free-list next pointer (4 blockIdx + 2 slotIndex)

type
  FreeListHead* = object
    blockIdx*: uint32    ## block index within the pool's buffer (not CTFS block num)
    slotIndex*: uint16
    isEmpty*: bool       ## true when free list has no entries

  SubBlockAllocation* = object
    blockNum*: uint64    ## block index within the pool's buffer
    slotIndex*: uint16
    poolClass*: uint8

  SubBlockPoolManager* = object
    buffers*: array[7, seq[byte]]     ## flat buffer per pool class
    freeListHeads*: array[7, FreeListHead]
    blockCounts*: array[7, uint32]    ## number of 4096-byte blocks per pool class

proc initSubBlockPoolManager*(): SubBlockPoolManager =
  ## Initialize pool manager. All free lists start empty.
  var pm: SubBlockPoolManager
  for i in 0 ..< NumPoolClasses:
    pm.buffers[i] = newSeq[byte](0)
    pm.freeListHeads[i] = FreeListHead(blockIdx: 0, slotIndex: 0, isEmpty: true)
    pm.blockCounts[i] = 0
  pm

proc slotOffset(poolClass: uint8, blockIdx: uint32, slotIndex: uint16): int {.inline.} =
  ## Compute byte offset within a pool buffer for a given slot.
  int(blockIdx) * int(DefaultBlockSize) + int(slotIndex) * poolSize(poolClass)

proc writeNextPtr*(buf: var seq[byte], offset: int, blockIdx: uint32, slotIndex: uint16) =
  ## Write a 6-byte free-list next pointer at the given offset.
  let le32 = toBytesLE(blockIdx)
  buf[offset + 0] = le32[0]
  buf[offset + 1] = le32[1]
  buf[offset + 2] = le32[2]
  buf[offset + 3] = le32[3]
  let le16 = toBytesLE(slotIndex)
  buf[offset + 4] = le16[0]
  buf[offset + 5] = le16[1]

proc readNextPtr*(buf: openArray[byte], offset: int): (uint32, uint16) =
  ## Read a 6-byte free-list next pointer from the given offset.
  var arr32: array[4, byte]
  arr32[0] = buf[offset + 0]
  arr32[1] = buf[offset + 1]
  arr32[2] = buf[offset + 2]
  arr32[3] = buf[offset + 3]
  let blockIdx = fromBytesLE(uint32, arr32)
  var arr16: array[2, byte]
  arr16[0] = buf[offset + 4]
  arr16[1] = buf[offset + 5]
  let slotIndex = fromBytesLE(uint16, arr16)
  (blockIdx, slotIndex)

proc expandPool(pm: var SubBlockPoolManager, poolClass: uint8) =
  ## Allocate a new 4096-byte block for the given pool class, divide it into
  ## slots, chain them into the free list, and set the free list head.
  let pc = int(poolClass)
  let oldLen = pm.buffers[pc].len
  pm.buffers[pc].setLen(oldLen + int(DefaultBlockSize))

  # Zero the new block.
  for i in oldLen ..< pm.buffers[pc].len:
    pm.buffers[pc][i] = 0

  let blockIdx = pm.blockCounts[pc]
  pm.blockCounts[pc] += 1

  let slots = SlotsPerBlock[pc]

  # Chain all slots in the new block into a free list.
  # Slot 0 becomes the head, slot N-1 points to the old head (or end-of-list).
  # Each slot's next pointer: slot[i] -> slot[i+1], slot[N-1] -> old head.
  for i in 0 ..< slots:
    let off = slotOffset(poolClass, blockIdx, uint16(i))
    if i < slots - 1:
      # Point to next slot in same block.
      writeNextPtr(pm.buffers[pc], off, blockIdx, uint16(i + 1))
    else:
      # Last slot: point to old free list head (or end marker).
      if pm.freeListHeads[pc].isEmpty:
        writeNextPtr(pm.buffers[pc], off, 0'u32, 0'u16)  # end of list
      else:
        writeNextPtr(pm.buffers[pc], off,
          pm.freeListHeads[pc].blockIdx,
          pm.freeListHeads[pc].slotIndex)

  # New head is slot 0 of the new block.
  pm.freeListHeads[pc] = FreeListHead(blockIdx: blockIdx, slotIndex: 0, isEmpty: false)

proc allocate*(pm: var SubBlockPoolManager, poolClass: uint8): Result[SubBlockAllocation, string] =
  ## Allocate a sub-block slot of the given pool class.
  ## If the free list is empty, expands the pool with a new block.
  if poolClass >= uint8(NumPoolClasses):
    return err("invalid pool class: " & $poolClass)

  let pc = int(poolClass)

  if pm.freeListHeads[pc].isEmpty:
    pm.expandPool(poolClass)

  # Pop head of free list.
  let head = pm.freeListHeads[pc]
  let off = slotOffset(poolClass, head.blockIdx, head.slotIndex)
  let (nextBlock, nextSlot) = readNextPtr(pm.buffers[pc], off)

  # Clear the slot data (zero it out).
  let pSize = poolSize(poolClass)
  for i in 0 ..< pSize:
    pm.buffers[pc][off + i] = 0

  # Update free list head.
  if nextBlock == 0 and nextSlot == 0:
    pm.freeListHeads[pc] = FreeListHead(blockIdx: 0, slotIndex: 0, isEmpty: true)
  else:
    pm.freeListHeads[pc] = FreeListHead(blockIdx: nextBlock, slotIndex: nextSlot, isEmpty: false)

  ok(SubBlockAllocation(
    blockNum: uint64(head.blockIdx),
    slotIndex: head.slotIndex,
    poolClass: poolClass))

proc free*(pm: var SubBlockPoolManager, alloc: SubBlockAllocation): Result[void, string] =
  ## Return a sub-block slot to its pool's free list.
  if alloc.poolClass >= uint8(NumPoolClasses):
    return err("invalid pool class: " & $alloc.poolClass)

  let pc = int(alloc.poolClass)
  let off = slotOffset(alloc.poolClass, uint32(alloc.blockNum), alloc.slotIndex)

  if off + NextPtrSize > pm.buffers[pc].len:
    return err("slot offset out of bounds")

  # Write next pointer to current head.
  if pm.freeListHeads[pc].isEmpty:
    writeNextPtr(pm.buffers[pc], off, 0'u32, 0'u16)
  else:
    writeNextPtr(pm.buffers[pc], off,
      pm.freeListHeads[pc].blockIdx,
      pm.freeListHeads[pc].slotIndex)

  # This slot becomes the new head.
  pm.freeListHeads[pc] = FreeListHead(
    blockIdx: uint32(alloc.blockNum),
    slotIndex: alloc.slotIndex,
    isEmpty: false)

  ok()

proc readSlot*(pm: SubBlockPoolManager, alloc: SubBlockAllocation,
    output: var openArray[byte]): Result[int, string] =
  ## Read data from a sub-block slot. Returns number of bytes read.
  if alloc.poolClass >= uint8(NumPoolClasses):
    return err("invalid pool class: " & $alloc.poolClass)

  let pc = int(alloc.poolClass)
  let off = slotOffset(alloc.poolClass, uint32(alloc.blockNum), alloc.slotIndex)
  let pSize = poolSize(alloc.poolClass)

  if off + pSize > pm.buffers[pc].len:
    return err("slot offset out of bounds")

  let toCopy = min(output.len, pSize)
  for i in 0 ..< toCopy:
    output[i] = pm.buffers[pc][off + i]

  ok(toCopy)

proc writeSlot*(pm: var SubBlockPoolManager, alloc: SubBlockAllocation,
                data: openArray[byte]): Result[void, string] =
  ## Write data to a sub-block slot. data.len must be <= poolSize(poolClass).
  if alloc.poolClass >= uint8(NumPoolClasses):
    return err("invalid pool class: " & $alloc.poolClass)

  let pSize = poolSize(alloc.poolClass)
  if data.len > pSize:
    return err("data too large for pool class " & $alloc.poolClass &
      ": " & $data.len & " > " & $pSize)

  let pc = int(alloc.poolClass)
  let off = slotOffset(alloc.poolClass, uint32(alloc.blockNum), alloc.slotIndex)

  if off + pSize > pm.buffers[pc].len:
    return err("slot offset out of bounds")

  for i in 0 ..< data.len:
    pm.buffers[pc][off + i] = data[i]

  ok()

proc promote*(pm: var SubBlockPoolManager, oldAlloc: SubBlockAllocation,
              newPoolClass: uint8): Result[SubBlockAllocation, string] =
  ## Promote an entry from one pool class to the next larger one.
  ## Allocates in newPoolClass, copies data, frees old slot.
  if newPoolClass <= oldAlloc.poolClass:
    return err("new pool class must be larger than old")
  if newPoolClass >= uint8(NumPoolClasses):
    return err("invalid new pool class: " & $newPoolClass)

  let oldSize = poolSize(oldAlloc.poolClass)

  # Read old data.
  var oldData = newSeq[byte](oldSize)
  let readRes = pm.readSlot(oldAlloc, oldData)
  if readRes.isErr:
    return err(readRes.error)

  # Allocate new slot.
  let newAllocRes = pm.allocate(newPoolClass)
  if newAllocRes.isErr:
    return err(newAllocRes.error)
  let newAlloc = newAllocRes.get()

  # Write old data to new slot.
  let writeRes = pm.writeSlot(newAlloc, oldData)
  if writeRes.isErr:
    return err(writeRes.error)

  # Free old slot.
  let freeRes = pm.free(oldAlloc)
  if freeRes.isErr:
    return err(freeRes.error)

  ok(newAlloc)

# -----------------------------------------------------------------------------
# Stats accessors (for space analyzer)
# -----------------------------------------------------------------------------

proc totalAllocatedSlots*(pm: SubBlockPoolManager, poolClass: uint8): int =
  ## Total number of slots (allocated + free) for the given pool class.
  if poolClass >= uint8(NumPoolClasses):
    return 0
  let pc = int(poolClass)
  int(pm.blockCounts[pc]) * SlotsPerBlock[pc]

proc totalFreeSlots*(pm: SubBlockPoolManager, poolClass: uint8): int =
  ## Count free slots by walking the free list for the given pool class.
  if poolClass >= uint8(NumPoolClasses):
    return 0
  let pc = int(poolClass)
  if pm.freeListHeads[pc].isEmpty:
    return 0
  var count = 0
  var blockIdx = pm.freeListHeads[pc].blockIdx
  var slotIdx = pm.freeListHeads[pc].slotIndex
  # Walk the free list until we hit the sentinel (0, 0).
  # Guard against infinite loops with a max iteration count.
  let maxIter = totalAllocatedSlots(pm, poolClass) + 1
  while count < maxIter:
    count += 1
    let off = slotOffset(poolClass, blockIdx, slotIdx)
    if off + NextPtrSize > pm.buffers[pc].len:
      break
    let (nextBlock, nextSlot) = readNextPtr(pm.buffers[pc], off)
    if nextBlock == 0 and nextSlot == 0:
      break
    blockIdx = nextBlock
    slotIdx = nextSlot
  count

proc poolClassBlockCounts*(pm: SubBlockPoolManager): array[7, uint32] =
  ## Return the block counts per pool class.
  pm.blockCounts

proc graduate*(pm: var SubBlockPoolManager, oldAlloc: SubBlockAllocation): Result[seq[byte], string] =
  ## Graduate an entry from sub-block to a standalone byte sequence.
  ## Returns the data copied from the sub-block slot. The old slot is freed.
  ## (Full CTFS internal file creation is deferred to M9c when block-level
  ## backing is integrated.)
  if oldAlloc.poolClass >= uint8(NumPoolClasses):
    return err("invalid pool class: " & $oldAlloc.poolClass)

  let oldSize = poolSize(oldAlloc.poolClass)
  var oldData = newSeq[byte](oldSize)
  let readRes = pm.readSlot(oldAlloc, oldData)
  if readRes.isErr:
    return err(readRes.error)

  let freeRes = pm.free(oldAlloc)
  if freeRes.isErr:
    return err(freeRes.error)

  ok(oldData)
