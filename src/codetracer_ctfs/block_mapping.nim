{.push raises: [].}

## Multi-level chain mapping for CTFS block allocation.
##
## Implements the bottom-up chain mapping model (up to 5 levels,
## matching the Rust implementation):
##   Level 1 (root): entries[0..N-2] are direct data block pointers.
##     entries[N-1] points to a level-2 mapping block (or 0 if not needed).
##   Level 2: entries[0..N-2] each point to level-1 sub-blocks.
##     entries[N-1] points to a level-3 mapping block (or 0 if not needed).
##   Level k: same pattern, up to level 5.

import results
import ./types

proc flushBlockRange*(c: var Ctfs, offset: int, size: int) =
  ## Write a range of in-memory data to the streaming file.
  if not c.streaming:
    return
  if offset + size > c.data.len:
    return
  try:
    c.streamFile.setFilePos(int64(offset))
    discard c.streamFile.writeBuffer(addr c.data[offset], size)
  except IOError, OSError:
    discard

proc flushBlock*(c: var Ctfs, blockNum: uint64) =
  ## Write a single block to the streaming file.
  if not c.streaming:
    return
  let offset = c.blockOffset(blockNum)
  let size = int(c.blockSize)
  c.flushBlockRange(offset, size)

proc allocBlock*(c: var Ctfs): uint64 =
  ## Allocate the next free block, extending the data buffer as needed.
  let blk = c.nextFreeBlock
  c.nextFreeBlock += 1
  let needed = int(c.nextFreeBlock) * int(c.blockSize)
  if needed > c.data.len:
    c.data.setLen(needed)
  # When streaming, write the new (zeroed) block to disk immediately
  # so the file size on disk grows to match the in-memory buffer.
  if c.streaming:
    c.flushBlock(blk)
  blk

proc zeroBlock*(c: var Ctfs, blockNum: uint64) =
  ## Zero out a block.
  let start = c.blockOffset(blockNum)
  for i in 0 ..< int(c.blockSize):
    c.data[start + i] = 0

proc readPtr*(c: Ctfs, blockNum: uint64, index: uint64): uint64 =
  ## Read a u64 pointer at a given entry index within a block.
  let off = c.blockOffset(blockNum) + int(index) * 8
  readU64LE(c.data, off)

proc writePtr*(c: var Ctfs, blockNum: uint64, index: uint64, val: uint64) =
  ## Write a u64 pointer at a given entry index within a block.
  let off = c.blockOffset(blockNum) + int(index) * 8
  writeU64LE(c.data, off, val)

proc levelCapacity*(usable: uint64, level: uint32): uint64 =
  ## Compute the number of data blocks addressable by a single level.
  ## Level 1: usable, Level 2: usable^2, Level k: usable^k
  var cap: uint64 = 1
  for i in 0 ..< level:
    # Saturating multiply to avoid overflow.
    let prev = cap
    cap = cap * usable
    if cap div usable != prev:
      return high(uint64)  # Overflow -> effectively unlimited.
  cap

# --- multi-level chain mapping ------------------------------------------------

proc navigateAndInsert*(c: var Ctfs, mappingBlock: uint64, level: uint32,
                        idxWithinLevel: uint64, dataBlock: uint64,
                        usable: uint64): Result[void, string] =
  ## Navigate within a level-k block to insert a data block pointer.
  ## For level 1: just write entries[idx] = dataBlock.
  ## For level k>1: find the sub-entry, follow/allocate, recurse.
  if level == 1:
    assert idxWithinLevel < usable
    c.writePtr(mappingBlock, idxWithinLevel, dataBlock)
    return ok()

  # Level k > 1: each entry covers levelCapacity(usable, level-1) data blocks.
  let subCap = levelCapacity(usable, level - 1)
  let entryIdx = idxWithinLevel div subCap
  let subIdx = idxWithinLevel mod subCap

  assert entryIdx < usable

  # Read or allocate the sub-block.
  var childBlock = c.readPtr(mappingBlock, entryIdx)
  if childBlock == 0:
    childBlock = c.allocBlock()
    c.zeroBlock(childBlock)
    c.writePtr(mappingBlock, entryIdx, childBlock)

  c.navigateAndInsert(childBlock, level - 1, subIdx, dataBlock, usable)

proc insertDataBlock*(c: var Ctfs, rootBlock: uint64, blockIndex: uint64,
                      dataBlock: uint64): Result[void, string] =
  ## Insert a data block pointer at the given blockIndex using the
  ## bottom-up chain model matching the Rust CtfsWriter.
  let usable = c.usableEntries()

  var idx = blockIndex
  var currentLevelBlock = rootBlock
  var level: uint32 = 1

  # Walk up through levels until we find which level contains this index.
  while true:
    let cap = levelCapacity(usable, level)
    if idx < cap:
      break
    idx -= cap
    level += 1

    if level > MaxChainLevels:
      return err("file too large: exceeds 5-level mapping")

    # Follow or create the chain pointer from currentLevelBlock[N-1].
    let chainPtr = c.readPtr(currentLevelBlock, usable)
    if chainPtr == 0:
      let newBlock = c.allocBlock()
      c.zeroBlock(newBlock)
      c.writePtr(currentLevelBlock, usable, newBlock)
      currentLevelBlock = newBlock
    else:
      currentLevelBlock = chainPtr

  # Navigate down from level to place the data block pointer.
  let res = c.navigateAndInsert(currentLevelBlock, level, idx, dataBlock, usable)

  # Flush the mapping block(s) when streaming so the data block pointer
  # is visible to concurrent readers and persists in the final file.
  if res.isOk and c.streaming:
    c.flushBlock(rootBlock)
    if currentLevelBlock != rootBlock:
      c.flushBlock(currentLevelBlock)

  res

proc navigateAndLookup*(c: Ctfs, mappingBlock: uint64, level: uint32,
                        idxWithinLevel: uint64, usable: uint64): uint64 =
  ## Navigate down through mapping blocks to find a data block pointer.
  if level == 1:
    return c.readPtr(mappingBlock, idxWithinLevel)

  let subCap = levelCapacity(usable, level - 1)
  let entryIdx = idxWithinLevel div subCap
  let subIdx = idxWithinLevel mod subCap

  let childBlock = c.readPtr(mappingBlock, entryIdx)
  if childBlock == 0:
    return 0

  c.navigateAndLookup(childBlock, level - 1, subIdx, usable)

proc lookupDataBlock*(c: Ctfs, rootBlock: uint64,
                      blockIndex: uint64): uint64 =
  ## Look up the data block number at the given block index using the chain.
  let usable = c.usableEntries()

  var idx = blockIndex
  var currentLevelBlock = rootBlock
  var level: uint32 = 1

  # Walk up through levels.
  while true:
    let cap = levelCapacity(usable, level)
    if idx < cap:
      break
    idx -= cap
    level += 1
    if level > MaxChainLevels:
      return 0

    let chainPtr = c.readPtr(currentLevelBlock, usable)
    if chainPtr == 0:
      return 0
    currentLevelBlock = chainPtr

  # Navigate down to find the data block.
  c.navigateAndLookup(currentLevelBlock, level, idx, usable)
