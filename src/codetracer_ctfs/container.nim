when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## CTFS container create/read/write/close operations.

import std/[algorithm, tables]
import results
import ./types
import ./base40
import ./block_mapping

proc createCtfs*(
    blockSize: uint32 = DefaultBlockSize,
    maxRootEntries: uint32 = DefaultMaxRootEntries,
    encryption: CtfsEncryptionMethod = emNone,
    maxShards: uint8 = DefaultMaxShards): Ctfs =
  ## Create a new in-memory CTFS container (version 5).
  ## Header layout (per spec):
  ##   [0-4] magic  [5] version  [6] encryption  [7] max_shards
  ## Compression is NOT in the header — it is a property of each member's format.
  var c: Ctfs
  c.blockSize = blockSize
  c.maxRootEntries = maxRootEntries
  c.encryption = encryption
  c.maxShards = maxShards
  # `ctfs-container.md` §1: the root region is `root_blocks` contiguous blocks
  # from block 0 — one whenever the declared entries fit block 0, more when
  # they overflow it — and data allocation begins after it.
  let rootBlocks = rootBlockCount(blockSize, maxRootEntries, maxShards)
  c.data = newSeq[byte](int(rootBlocks) * int(blockSize))
  c.nextFreeBlock = rootBlocks

  # Write header (8 bytes)
  c.data[0] = CtfsMagic[0]
  c.data[1] = CtfsMagic[1]
  c.data[2] = CtfsMagic[2]
  c.data[3] = CtfsMagic[3]
  c.data[4] = CtfsMagic[4]
  c.data[5] = CtfsVersion
  c.data[6] = uint8(encryption)   # encryption method
  c.data[7] = maxShards            # max shards

  # Write extended header (8 bytes)
  writeU32LE(c.data, 8, blockSize)
  writeU32LE(c.data, 12, maxRootEntries)

  c

proc remapMappingTree(c: var Ctfs, rootBlock: uint64,
                      moved: Table[uint64, uint64], touched: var seq[uint64]) =
  ## Rewrite, through `moved`, every block pointer of the member whose level-1
  ## mapping block is `rootBlock` (already its new number): the level-1 data
  ## pointers and chain pointer, and every level-k block the chain reaches
  ## with the subtrees below it (`block_mapping.nim`).  Data blocks are never
  ## read as pointers.
  let usable = c.usableEntries()
  proc fix(c: var Ctfs, blk: uint64, slot: uint64,
           moved: Table[uint64, uint64]): uint64 =
    let p = c.readPtr(blk, slot)
    if p != 0'u64 and moved.hasKey(p):
      let q = moved.getOrDefault(p, p)
      c.writePtr(blk, slot, q)
      return q
    p
  proc subtree(c: var Ctfs, blk: uint64, level: uint32,
               moved: Table[uint64, uint64], touched: var seq[uint64],
               usable: uint64) =
    touched.add blk
    for i in 0'u64 ..< usable:
      let child = fix(c, blk, i, moved)
      if level > 1'u32 and child != 0'u64:
        subtree(c, child, level - 1, moved, touched, usable)
    discard fix(c, blk, usable, moved)
  subtree(c, rootBlock, 1, moved, touched, usable)
  var cur = c.readPtr(rootBlock, usable)
  var level = 2'u32
  while cur != 0'u64 and level <= uint32(MaxChainLevels):
    subtree(c, cur, level, moved, touched, usable)
    cur = c.readPtr(cur, usable)
    inc level

proc growRootDirectory*(c: var Ctfs): Result[void, string] =
  ## Double the root region (`ctfs-container.md` §1: the file-entry array
  ## continues into the blocks after block 0, `root_blocks` of them, and data
  ## allocation begins after it).  The blocks the larger region needs are
  ## allocated already, so each is MOVED to the end of the container and every
  ## pointer to it rewritten: entries' `MapBlock` (in any of its three forms)
  ## and the pointers inside every member's mapping blocks.  Nothing else in a
  ## container holds a block number.  The format does not change: the header's
  ## `MaxRootEntries` grows, which every reader already sizes the root by.
  ##
  ## Order, for a reader of a streaming container: the moved copies and the
  ## rewritten mapping blocks are written first, the root region (entries and
  ## header) last, so the old root keeps resolving to intact blocks until the
  ## new one replaces it.
  let bs = uint64(c.blockSize)
  let oldRoot = c.rootBlockCount()
  let newRoot = max(oldRoot * 2'u64, oldRoot + 1'u64)
  let rBytes = 7'u64 * uint64(c.maxShards) * 6'u64
  let newMax = (newRoot * bs - uint64(HeaderSize + ExtHeaderSize) - rBytes) div
    uint64(FileEntrySize)
  if newMax > uint64(high(uint32)):
    return err("the root directory cannot grow past " & $high(uint32) & " entries")
  if c.rootGrowthLimit != 0'u32 and newMax > uint64(c.rootGrowthLimit):
    return err("the root directory may not grow past " & $c.rootGrowthLimit &
               " entries (rootGrowthLimit)")
  # 1. Move the blocks the larger root region takes over.
  var moved = initTable[uint64, uint64]()
  var copies: seq[uint64] = @[]
  let allocatedEnd = c.nextFreeBlock
  for b in oldRoot ..< min(newRoot, allocatedEnd):
    let nb = c.allocBlock()
    copyMem(addr c.data[c.blockOffset(nb)], addr c.data[c.blockOffset(b)], int(bs))
    moved[b] = nb
    copies.add nb
  if allocatedEnd < newRoot:
    # The new root region reaches past the allocated blocks: claim the rest
    # so data allocation begins after it.
    while c.nextFreeBlock < newRoot:
      discard c.allocBlock()
  # 2. Rewrite every pointer to a moved block.
  var touched: seq[uint64] = @[]
  for i in 0 ..< int(c.maxRootEntries):
    let off = c.fileEntryOffset(i)
    let m = readU64LE(c.data, off + 8)
    if m == 0'u64: continue
    if isDirectMapBlock(m):
      let d = directDataBlock(m)
      if moved.hasKey(d):
        writeU64LE(c.data, off + 8, CtfsDirect or moved.getOrDefault(d, d))
      continue
    let nm = moved.getOrDefault(m, m)
    if nm != m: writeU64LE(c.data, off + 8, nm)
    c.remapMappingTree(nm, moved, touched)
  if c.streaming:
    for b in copies: c.flushBlock(b)
    for b in touched: c.flushBlock(b)
  # 3. The old blocks become root region: zero what follows the entry array,
  #    then publish the larger directory.
  let entriesEnd = c.fileEntryOffset(int(c.maxRootEntries))
  let regionEnd = int(newRoot * bs)
  for k in entriesEnd ..< regionEnd:
    c.data[k] = 0
  c.maxRootEntries = uint32(newMax)
  writeU32LE(c.data, 12, c.maxRootEntries)
  if c.streaming:
    c.flushRootBlocks()
  ok()

proc addFile*(c: var Ctfs, name: string): Result[CtfsInternalFile, string] =
  ## Add a new named file to the container. Returns a handle for writing.
  ##
  ## Creating a member writes its name and nothing else: no block is claimed,
  ## so a member that is never written stays `(Size, MapBlock) = (0, 0)` in
  ## the finished container (`ctfs-container.md` §2 and §5, "Creating a
  ## File"). Its first write gives it a data block.
  ##
  ## A DUPLICATE root name is rejected.  Every reader resolves a name to the
  ## FIRST matching root entry, so appending a second entry with the same name
  ## does not update that file — it SHADOWS it, and the shadowed member becomes
  ## unreachable while still occupying the container.  That failure mode is
  ## invisible (the write "succeeds", the read returns stale bytes), so the
  ## writer refuses it rather than letting a caller discover it downstream.
  ## An empty member is `(0, 0)` with its name, so the name alone decides.
  ##
  ## A name that base40 cannot pack — longer than 12 characters, or with a
  ## character outside `0-9 a-z . / -` — is refused by name
  ## (`ctfs-container.md` §3): packing it would store a different name.
  let refusal = base40Refusal(name)
  if refusal.len > 0:
    return err(refusal)
  let encodedName = base40Encode(name)

  # Reject a duplicate before claiming anything.
  for i in 0 ..< int(c.maxRootEntries):
    let off = c.fileEntryOffset(i)
    if off + 24 > c.data.len: break
    if readU64LE(c.data, off + 16) == encodedName:
      return err("duplicate CTFS file name '" & name & "': a container may " &
                 "hold only one member per name (readers take the first, so " &
                 "a second entry would silently shadow it)")

  # Find first empty file entry: all 24 bytes zero.
  for i in 0 ..< int(c.maxRootEntries):
    let off = c.fileEntryOffset(i)
    let entrySize = readU64LE(c.data, off)
    let entryMap = readU64LE(c.data, off + 8)
    let entryName = readU64LE(c.data, off + 16)
    if entrySize == 0 and entryMap == 0 and entryName == 0:
      writeU64LE(c.data, off + 16, encodedName)
      # When streaming, flush the root region so the new file entry is
      # visible to concurrent readers.
      if c.streaming:
        c.flushRootBlocks()
      return ok(CtfsInternalFile(entryIndex: i, writePos: 0, dataBlockCount: 0))

  # Every entry is taken: the root directory grows (`growRootDirectory`), so
  # the number of members is not a limit a recording can reach.  The first new
  # slot is the one just past the old array.
  let firstNew = int(c.maxRootEntries)
  let g = c.growRootDirectory()
  if g.isErr:
    return err("no free file entry slots, and the root directory could not " &
               "grow: " & g.unsafeError)
  let off = c.fileEntryOffset(firstNew)
  writeU64LE(c.data, off + 16, encodedName)
  if c.streaming:
    c.flushRootBlocks()
  ok(CtfsInternalFile(entryIndex: firstNew, writePos: 0, dataBlockCount: 0))

proc resolveFileBlock*(c: Ctfs, mapBlock: uint64, blockIndex: uint64): uint64 =
  ## The data block holding block `blockIndex` of a member whose entry carries
  ## `mapBlock`, in any of its three forms (`ctfs-container.md` §2), or 0 when
  ## it does not resolve.
  if mapBlock == 0:
    return 0
  if isDirectMapBlock(mapBlock):
    return (if blockIndex == 0: directDataBlock(mapBlock) else: 0)
  c.lookupDataBlock(mapBlock, blockIndex)

proc writeToFile*(c: var Ctfs, f: var CtfsInternalFile,
                  data: openArray[byte]): Result[void, string] =
  ## Append data to an internal file (`ctfs-container.md` §5, "Appending
  ## Data"). A member that fits one block is stored DIRECT — its entry's
  ## `MapBlock` is its only data block, tagged with `CtfsDirect` — and is
  ## mapped from the append that takes it past one block on: that append
  ## claims a level-1 mapping block first, puts the direct block in its slot
  ## 0, claims the new data blocks in file order, and stores the untagged
  ## mapping block in `MapBlock` before it stores the new `Size`.
  ##
  ## **Every block number this proc turns into a byte offset is checked first,
  ## and that is a data-integrity rule rather than defensiveness.** Block 0 is
  ## the container header and the root directory, so a block number of `0` —
  ## which is what `lookupDataBlock` returns for *any* mapping it cannot
  ## resolve, at any level — addresses byte offset 0. Writing a caller's
  ## payload there does not damage one stream, it overwrites the header and
  ## the entire entry array, so the container stops being a container and
  ## every stream in it becomes unreachable. Measured before this check
  ## existed, on a sealed container with one level-1 mapping slot zeroed:
  ## `writeToFile` returned `ok()`, the CTFS magic was gone, and both members
  ## came back as `internal file not found`.
  ##
  ## This is the destructive twin of the read-side defect M61a closed in nine
  ## readers (`CTFS-Binary-Format.md` §4 and the write-side note under §5d).
  ## The read side hands back the wrong bytes; this side deletes the user's
  ## recording. Pinned by `tests/test_write_null_data_block.nim`.
  if data.len == 0:
    return ok()

  let entryOff = c.fileEntryOffset(f.entryIndex)
  let entryMap = readU64LE(c.data, entryOff + 8)
  let bs = uint64(c.blockSize)
  let newSize = f.writePos + uint64(data.len)
  # The root region is every block before `rootBlockCount` (block 0 alone
  # unless the entry array overflows it, `ctfs-container.md` §1), and no
  # member's block may lie inside it.
  let rootBlocks = c.rootBlockCount()

  # The entry's current form, checked before anything is claimed or walked.
  if entryMap == 0:
    # Empty. A non-zero size here is the null pointer a crash between
    # publishing an entry's size and its `MapBlock` leaves.
    if f.writePos != 0:
      return err("internal file entry " & $f.entryIndex & " has size " &
        $f.writePos & " and a null MapBlock (its mapping root); refusing to write " &
        "through it")
  elif isDirectMapBlock(entryMap):
    let b = directDataBlock(entryMap)
    if b < rootBlocks or b >= c.nextFreeBlock:
      return err("internal file entry " & $f.entryIndex & " has direct data " &
        "block " & $b & ", which is outside the container's " &
        $c.nextFreeBlock & " allocated blocks or inside its " & $rootBlocks &
        "-block root region; refusing to write through it")
    if f.writePos > bs:
      return err("internal file entry " & $f.entryIndex & " is direct but " &
        "holds " & $f.writePos & " bytes, more than one " & $bs &
        "-byte block; refusing to write through it")
  elif entryMap < rootBlocks or entryMap >= c.nextFreeBlock:
    return err("internal file entry " & $f.entryIndex & " has mapping root block " &
      $entryMap & ", which is outside the container's " & $c.nextFreeBlock &
      " allocated blocks or inside its " & $rootBlocks &
      "-block root region; refusing to write through it")

  # Cases 1 and 2: the member fits one block after this append.
  if newSize <= bs and (entryMap == 0 or isDirectMapBlock(entryMap)):
    let b =
      if entryMap == 0: c.allocBlock()
      else: directDataBlock(entryMap)
    let start = c.blockOffset(b) + int(f.writePos)
    copyMem(addr c.data[start], unsafeAddr data[0], data.len)
    if c.streaming:
      c.flushBlockRange(start, data.len)
    if entryMap == 0:
      writeU64LE(c.data, entryOff + 8, CtfsDirect or b)
    f.writePos = newSize
    writeU64LE(c.data, entryOff, f.writePos)
    return ok()

  # Case 3: the member is (or becomes) mapped. An empty or direct member
  # claims its level-1 mapping block first; the direct block goes to slot 0.
  var mapBlock = entryMap
  let transition = entryMap == 0 or isDirectMapBlock(entryMap)
  let blocksAtStart = c.nextFreeBlock
  let bytesAtStart = c.data.len
  if transition:
    mapBlock = c.allocBlock()
    c.zeroBlock(mapBlock)
    if entryMap != 0:
      c.writePtr(mapBlock, 0, directDataBlock(entryMap))

  var written = 0
  while written < data.len:
    let fileBlockIdx = int(f.writePos) div int(c.blockSize)
    let offsetInBlock = int(f.writePos) mod int(c.blockSize)

    # Determine the data block for this file position.
    # If we're at the start of a new block, allocate and insert it.
    var dataBlock: uint64

    if offsetInBlock == 0:
      # Need a new data block. It is allocated only *provisionally*: if the
      # mapping cannot accept the pointer — the container was damaged and
      # `insertDataBlock` refuses to allocate over a null pointer that an
      # earlier index already went through — the allocation is rolled back, so a
      # refused append leaves the in-memory container byte-identical to what it
      # found. `insertDataBlock` is all-or-nothing itself: with the null-pointer
      # rule in place, both of its failure branches return before allocating or
      # writing anything (an allocation there implies a zero remainder, which
      # cannot fail at any deeper level), so this one rollback is the whole of
      # it. In *streaming* mode `allocBlock` has already extended the file on
      # disk by one zero block; that block is unreferenced and a later
      # `openClosedCtfs` simply counts it as allocated, so it is waste rather
      # than damage.
      let blocksBefore = c.nextFreeBlock
      let bytesBefore = c.data.len
      dataBlock = c.allocBlock()
      let insertRes = c.insertDataBlock(mapBlock, uint64(fileBlockIdx), dataBlock)
      if insertRes.isErr:
        if transition:
          c.nextFreeBlock = blocksAtStart
          c.data.setLen(bytesAtStart)
        else:
          c.nextFreeBlock = blocksBefore
          c.data.setLen(bytesBefore)
        return err(insertRes.error)
    else:
      # Mid-block write: look up the existing data block by navigating the chain.
      dataBlock = c.lookupDataBlock(mapBlock, uint64(fileBlockIdx))

    # `lookupDataBlock` answers "unresolved" with 0 — from a null level-1 slot,
    # a null chain pointer, a null child, or a level overflow — and 0 is block
    # 0. Refuse it here, where the block number becomes a byte offset, so no
    # unresolved mapping can put payload into the header and root directory.
    # The upper bound catches the same failure pointing the other way: a
    # garbage pointer past the allocator's high-water mark, which would
    # otherwise index past `c.data` and die with an IndexDefect.
    if dataBlock == 0'u64:
      return err("null data block at index " & $fileBlockIdx &
        " of internal file entry " & $f.entryIndex &
        ": its mapping does not resolve, and block 0 is the container header " &
        "and root directory — refusing to write there")
    if dataBlock < rootBlocks:
      return err("data block " & $fileBlockIdx & " of internal file entry " &
        $f.entryIndex & " resolves to block " & $dataBlock & ", inside the " &
        "container's " & $rootBlocks & "-block root directory; refusing to " &
        "write there")
    if dataBlock >= c.nextFreeBlock:
      return err("data block " & $fileBlockIdx & " of internal file entry " &
        $f.entryIndex & " resolves to block " & $dataBlock &
        ", which is outside the container's " & $c.nextFreeBlock &
        " allocated blocks")

    # Write data into the block.
    let blockStart = c.blockOffset(dataBlock)
    let space = int(c.blockSize) - offsetInBlock
    let toWrite = min(space, data.len - written)
    copyMem(addr c.data[blockStart + offsetInBlock], unsafeAddr data[written],
      toWrite)

    # Flush the bytes just written when streaming. Only those: the rest of
    # the block is either earlier payload, flushed by the call that wrote it,
    # or the zeroes `allocBlock` flushed when the block was allocated. A whole
    # block per call is 4 KiB of I/O for a 20-byte interning record.
    if c.streaming:
      c.flushBlockRange(blockStart + offsetInBlock, toWrite)

    written += toWrite
    f.writePos += uint64(toWrite)

  # A transition's mapping block is complete now; it reaches the file before
  # the entry that publishes it, and `MapBlock` is stored before `Size`.
  if transition:
    if c.streaming:
      c.flushBlock(mapBlock)
    writeU64LE(c.data, entryOff + 8, mapBlock)
  writeU64LE(c.data, entryOff, f.writePos)
  ok()

proc rewriteFileContent*(c: var Ctfs, f: CtfsInternalFile,
                         data: openArray[byte]): Result[void, string] =
  ## Overwrite an internal file's bytes IN PLACE, keeping its size, its
  ## file-entry slot and its already-allocated data blocks exactly as they
  ## are.  The new content must be the same length as the old one; anything
  ## else would need block (de)allocation, which would move every later
  ## block and change the container layout.
  ##
  ## Re-serialising a same-length file over its own blocks keeps the layout
  ## fixed and touches only the bytes that changed — used for late-decided
  ## fields such as `meta.dat`'s feature-flag word, whose value cannot be
  ## known at `openTraceWriter` time and whose blocks must not move.
  if f.writePos == 0:
    return err("rewriteFileContent: file has no content to rewrite")
  if uint64(data.len) != f.writePos:
    return err("rewriteFileContent: length mismatch (have " & $f.writePos &
               " bytes, got " & $data.len & ")")

  let entryOff = c.fileEntryOffset(f.entryIndex)
  let mapBlock = readU64LE(c.data, entryOff + 8)

  var written = 0
  while written < data.len:
    let fileBlockIdx = uint64(written) div uint64(c.blockSize)
    let offsetInBlock = written mod int(c.blockSize)
    let dataBlock = c.resolveFileBlock(mapBlock, fileBlockIdx)
    if dataBlock == 0:
      return err("rewriteFileContent: missing data block " & $fileBlockIdx)
    let blockStart = c.blockOffset(dataBlock)
    let toWrite = min(int(c.blockSize) - offsetInBlock, data.len - written)
    for i in 0 ..< toWrite:
      c.data[blockStart + offsetInBlock + i] = data[written + i]
    if c.streaming:
      c.flushBlock(dataBlock)
    written += toWrite
  ok()

proc truncateFileContent*(c: var Ctfs, f: CtfsInternalFile):
    Result[CtfsInternalFile, string] =
  ## Make an existing file entry EMPTY again — `(Size, MapBlock) = (0, 0)` —
  ## and return a handle at position 0, so the caller can rewrite its content
  ## at a different length than before.
  ##
  ## `rewriteFileContent` cannot do this: it is deliberately length-preserving
  ## because its callers must not move any later block. This is the opposite
  ## case — re-finalising a stream whose new image is a different length.
  ##
  ## The old mapping and data blocks are abandoned in place.  Nothing
  ## references them once the entry's pointer moves, and the container's block
  ## allocator only ever moves forward, so the orphans are inert padding. They
  ## are NOT reclaimed, which is why this is an append-time finalisation step
  ## and not something to call in a loop.
  let entryOff = c.fileEntryOffset(f.entryIndex)
  writeU64LE(c.data, entryOff, 0)
  writeU64LE(c.data, entryOff + 8, 0)
  if c.streaming:
    c.flushRootBlocks()
  ok(CtfsInternalFile(entryIndex: f.entryIndex, writePos: 0, dataBlockCount: 0))

proc publish*(c: var Ctfs) =
  ## Hand everything written so far to the operating system, data before the
  ## root directory that publishes it (`ctfs-container.md` §6, "Writer
  ## Protocol" and "Durability"): in deferred mode the dirty blocks outside
  ## the root region, coalesced into runs, then the root region (header and
  ## every file entry's `Size` and `MapBlock`); then the stdio buffer is
  ## flushed, so the bytes have been `write`n and survive the process. No
  ## `fsync`: rule 5 does not require one.
  if not c.streaming:
    return
  let rootBlocks = c.rootBlockCount()
  if c.deferWrites and c.dirtyBlocks.len > 0:
    c.dirtyBlocks.sort()
    let bs = int(c.blockSize)
    var i = 0
    while i < c.dirtyBlocks.len:
      var j = i
      while j + 1 < c.dirtyBlocks.len and
          c.dirtyBlocks[j + 1] == c.dirtyBlocks[j] + 1:
        inc j
      var first = c.dirtyBlocks[i]
      let last = c.dirtyBlocks[j]
      if last >= rootBlocks:
        if first < rootBlocks:
          first = rootBlocks
        let off = int(first) * bs
        let size = min(int(last + 1) * bs, c.data.len) - off
        if size > 0:
          try:
            c.streamFile.setFilePos(int64(off))
            discard c.streamFile.writeBuffer(addr c.data[off], size)
          except IOError, OSError:
            discard
      i = j + 1
    for b in c.dirtyBlocks:
      c.dirtyMark[int(b)] = false
    c.dirtyBlocks.setLen(0)
  let rootBytes = min(int(rootBlocks) * int(c.blockSize), c.data.len)
  try:
    c.streamFile.setFilePos(0)
    discard c.streamFile.writeBuffer(addr c.data[0], rootBytes)
    c.streamFile.flushFile()
  except IOError, OSError:
    discard

proc syncRootBlock*(c: var Ctfs) =
  ## Publish block 0 — the header and the root file-entry array — to the
  ## streaming file.
  ##
  ## **The entry array holds every internal file's SIZE, and `writeToFile`
  ## updates that size in memory only.** It flushes the data block it just
  ## filled, so the payload is durable, but block 0 is rewritten only by
  ## `addFile` / `truncateFileContent` / `closeCtfs`. A container read off
  ## disk in between therefore reports the size each entry had at the last
  ## such call — and for the file written LAST, that size is 0, so the member
  ## reads back empty while its bytes sit in the container untouched.
  ##
  ## `meta.dat` is always the last file a trace writer writes, which made this
  ## the observable form of the defect: `close()` returned `ok`, and a reader
  ## that opened the path without a `closeCtfs()` saw an empty program with
  ## every capability flag clear and gated every stream count to
  ## "(unavailable)". Pinned by `tests/test_close_publishes_entry_sizes.nim`.
  ##
  ## The stdio buffer is flushed as well.  `flushBlockRange` only issues the
  ## write; without the flush the published block 0 can still be sitting in
  ## the runtime's buffer, so a reader in this process — a test decoding the
  ## trace it has just written — reads the pre-close image back.
  c.publish()

proc closeCtfs*(c: var Ctfs): Result[void, string] {.discardable.} =
  ## Close the container. When streaming, flushes all data and closes the file.
  ##
  ## Returns the I/O failure rather than swallowing it. The final write is the
  ## one that publishes block 0's entry-size array, so losing it silently
  ## yields a container whose members read back empty — a corrupt recording
  ## that reports success. `{.discardable.}` keeps the ~50 existing
  ## statement-position callers compiling, but a caller on a recording path
  ## should check it.
  if not c.streaming:
    return ok()
  var failure = ""
  try:
    # Final flush of all in-memory data to disk.
    c.streamFile.setFilePos(0)
    discard c.streamFile.writeBuffer(addr c.data[0], c.data.len)
    c.streamFile.flushFile()
    c.streamFile.close()
  except IOError as e:
    failure = "failed to finalize CTFS container: " & e.msg
  except OSError as e:
    failure = "OS error finalizing CTFS container: " & e.msg
  c.streaming = false
  if failure.len > 0:
    return err(failure)
  ok()

proc entryIndex*(f: CtfsInternalFile): int =
  ## Return the file entry index (for use with syncEntry).
  f.entryIndex

proc isStreaming*(c: Ctfs): bool =
  ## Return true if this container is in streaming mode.
  c.streaming

proc toBytes*(c: Ctfs): seq[byte] =
  ## Return the raw container bytes for writing to disk.
  c.data

proc writeCtfsToFile*(c: Ctfs, path: string): Result[void, string] =
  ## Write the CTFS container to a file on disk.
  try:
    writeFile(path, c.data)
    ok()
  except IOError as e:
    err("failed to write CTFS file: " & path & " (" & e.msg & ")")
  except OSError as e:
    err("OS error writing CTFS file: " & path & " (" & e.msg & ")")

proc readCtfsFromFile*(path: string): Result[seq[byte], string] =
  ## Read raw CTFS container bytes from a file.
  try:
    let data = readFile(path)
    var bytes = newSeq[byte](data.len)
    for i in 0 ..< data.len:
      bytes[i] = byte(data[i])
    ok(bytes)
  except IOError:
    err("failed to read CTFS file: " & path)
  except OSError:
    err("OS error reading CTFS file: " & path)

type
  CtfsEntryLookup* = object
    ## A root-directory lookup. `found` is reported separately from the two
    ## values because `(0, 0)` is a legitimately empty member as well as the
    ## shape of "no such name" (`ctfs-container.md` §4, "A null is not an
    ## absence").
    found*: bool
    index*: int
    size*: uint64
    mapBlock*: uint64

proc findFileEntryKey*(data: openArray[byte], encoded: uint64,
    maxEntries: uint32 = DefaultMaxRootEntries): CtfsEntryLookup =
  ## Find the entry whose name word is `encoded` in the root directory of the
  ## container image `data`. For a key no name of today packs to — the one a
  ## writer stored for a name base40 could not pack, which a reader of old
  ## recordings still has to find — and for `findFileEntry`.
  ##
  ## The entry array starts after the header: 16 bytes at version 5, 24 at
  ## version 6 (`ctfs-container.md` §1a). A compact container has no entry
  ## array; `readInternalFile` and `hasInternalFile` look its members up in
  ## its directory.
  let base =
    if data.len > 5 and data[5] == CtfsVersionV6: V6HeaderSize
    else: HeaderSize + ExtHeaderSize
  for i in 0 ..< int(maxEntries):
    let off = base + i * FileEntrySize
    if off + FileEntrySize > data.len:
      break
    if readU64LE(data, off + 16) == encoded:
      return CtfsEntryLookup(found: true, index: i,
        size: readU64LE(data, off), mapBlock: readU64LE(data, off + 8))
  CtfsEntryLookup(found: false)

proc findFileEntry*(data: openArray[byte], name: string,
    maxEntries: uint32 = DefaultMaxRootEntries): CtfsEntryLookup =
  ## Find `name` in the root directory of the container image `data`. A name
  ## base40 cannot pack is not found: its packing is a different name's.
  if not base40Encodable(name):
    return CtfsEntryLookup(found: false)
  findFileEntryKey(data, base40Encode(name), maxEntries)

proc truncatedContainerNote(wholeBlocks: uint64, blockSize: uint32,
    len: int): string =
  " — the container carries " & $wholeBlocks & " whole " & $blockSize &
    "-byte blocks in " & $len &
    " bytes, so it is truncated or its tail write was interrupted"

proc blockRefusal(what: string, n: uint64, name, rest: string,
    wholeBlocks: uint64, blockSize: uint32, len: int): string =
  what & " " & $n & " of internal file " & name & rest &
    truncatedContainerNote(wholeBlocks, blockSize, len)

type
  MemberRun* = tuple[at: int, len: int]
    ## `len` bytes of a member, stored at `at` in the container image.

type
  BlockLoader* = proc (b: uint64): bool {.closure, raises: [], gcsafe.}
    ## Makes block `b` of an image that is read as it is used hold its bytes
    ## (`member_view.openFileImage`); false when it cannot.

proc memberRuns*(data: openArray[byte], name: string,
    fileSize: uint64, mapBlock: uint64,
    blockSize: uint32,
    loader: BlockLoader = nil): Result[seq[MemberRun], string] =
  ## Where a member's `fileSize` bytes lie in the container image `data`, in
  ## member order, given its entry's `MapBlock`: runs of physically
  ## consecutive blocks, the last cut to the member's size. `readMemberBytes`
  ## copies them out; a `MemberView` reads them in place. Any of
  ## the three forms `ctfs-container.md` §2 defines: `0` (empty), tagged with
  ## `CtfsDirect` (its only data block) or a level-1 mapping block (§4).
  ##
  ## `CTFS-Binary-Format.md` §5d: a reader MUST accept a container whose length
  ## is not a whole number of blocks — the state a crash *inside* an append's
  ## tail write leaves — and ignore the bytes past the last whole block. What
  ## makes accepting that safe is **flooring**: `wholeBlocks` below is
  ## `floor(len / blockSize)`, never rounded up, so the incomplete final block
  ## is unaddressable.
  ##
  ## Flooring only helps if the bound is applied on *every* path from a block
  ## number to bytes, and there are three: the entry's mapping root (or its
  ## direct block), each mapping block walked to resolve the data block, and
  ## the **data block itself**. The last is the easy one to miss — it is read
  ## directly rather than through the same helper as the others, and the final
  ## data block's copy is clamped to the entry's `Size`, so a short read out of
  ## the partial region *succeeds*. Pinned by `tests/test_partial_tail_bounds.nim`.
  ##
  ## A `0` is refused separately from an out-of-range number, because it means
  ## something different — "unallocated", §4 — and because block 0 is the root
  ## directory, so an unrefused null resolves *into* the container's own
  ## header and entry array and reads it back as the stream.
  if blockSize == 0'u32:
    return err("zero block size")
  # floor, never `+ blockSize - 1`: rounding up would make the incomplete
  # final block addressable, which is the one arithmetic §5d forbids.
  let wholeBlocks = uint64(data.len div int(blockSize))
  # `<what> <n> of internal file <name><rest>` and the truncation note: one
  # formatter for every block number found past the container's end, called
  # only for a refusal.
  template outOfBounds(what: string, n: uint64, rest: string): string =
    blockRefusal(what, n, name, rest, wholeBlocks, blockSize, data.len)
  # A mapping block of an image read as it is used is read before it is
  # walked (`loader`); an image held whole has every block already.
  template loaded(b: uint64): bool = loader == nil or loader(b)
  template unloaded(b: uint64): string = "mapping block " & $b & " not readable"

  if mapBlock == 0'u64:
    if fileSize == 0:
      return ok(newSeq[MemberRun](0))
    return err("internal file " & name & " has size " & $fileSize &
      " and a null MapBlock (its mapping root): the member's block was never " &
      "published or has been overwritten; block 0 is the container's root directory and no " &
      "member may name it")

  if isDirectMapBlock(mapBlock):
    let b = directDataBlock(mapBlock)
    if b == 0'u64:
      return err("internal file " & name & " names data block 0 directly; " &
        "block 0 is the container's root directory and no member may name it")
    if b >= wholeBlocks:
      return err(outOfBounds("direct data block", b, " is out of bounds"))
    if fileSize > uint64(blockSize):
      return err("internal file " & name & " is stored in one direct block " &
        "but declares " & $fileSize & " bytes, more than one " & $blockSize &
        "-byte block holds")
    if fileSize == 0:
      return ok(newSeq[MemberRun](0))
    return ok(@[(at: int(b) * int(blockSize), len: int(fileSize))])

  # Path 1 of 3: the entry's mapping root.
  if mapBlock >= wholeBlocks:
    return err(outOfBounds("mapping root block", mapBlock, " is out of bounds"))
  if fileSize == 0:
    return ok(newSeq[MemberRun](0))

  var runs: seq[MemberRun]
  let usable = uint64(blockSize) div 8 - 1

  var remaining = int(fileSize)
  var destPos = 0
  var blockIdx: uint64 = 0

  while remaining > 0:
    var idx = blockIdx
    var currentLevelBlock = mapBlock
    var level: uint32 = 1

    block findLevel:
      while true:
        var cap: uint64 = 1
        for l in 0'u32 ..< level:
          cap = cap * usable
        if idx < cap:
          break findLevel
        idx -= cap
        level += 1
        if level > MaxChainLevels:
          return err("block index too large for mapping")
        let chainOff = int(currentLevelBlock) * int(blockSize) + int(usable) * 8
        if chainOff + 8 > data.len:
          return err("chain pointer out of bounds")
        if not loaded(currentLevelBlock):
          return err(unloaded(currentLevelBlock))
        let chainPtr = readU64LE(data, chainOff)
        if chainPtr == 0:
          return err("missing chain pointer at level " & $level &
            " of internal file " & name)
        # Path 2a of 3: a mapping block reached through the chain.
        if chainPtr >= wholeBlocks:
          return err(outOfBounds("chain pointer at level", level,
            " names block " & $chainPtr & ", which is out of bounds"))
        currentLevelBlock = chainPtr

    var navBlock = currentLevelBlock
    var navLevel = level
    var navIdx = idx
    while navLevel > 1:
      var subCap: uint64 = 1
      for l in 0'u32 ..< (navLevel - 1):
        subCap = subCap * usable
      let entryIdx = navIdx div subCap
      let subIdx = navIdx mod subCap
      let childOff = int(navBlock) * int(blockSize) + int(entryIdx) * 8
      if childOff + 8 > data.len:
        return err("child pointer out of bounds")
      if not loaded(navBlock):
        return err(unloaded(navBlock))
      let childBlock = readU64LE(data, childOff)
      if childBlock == 0:
        return err("missing child block at level " & $navLevel &
          " of internal file " & name)
      # Path 2b of 3: a mapping block reached by descending the hierarchy.
      if childBlock >= wholeBlocks:
        return err(outOfBounds("child block pointer at level", navLevel,
          " names block " & $childBlock & ", which is out of bounds"))
      navBlock = childBlock
      navIdx = subIdx
      navLevel -= 1

    let ptrOff = int(navBlock) * int(blockSize) + int(navIdx) * 8
    if ptrOff + 8 > data.len:
      return err("data block pointer out of bounds")
    if not loaded(navBlock):
      return err(unloaded(navBlock))
    let dataBlock = readU64LE(data, ptrOff)
    if dataBlock == 0:
      return err("null data block at index " & $blockIdx & " of internal file " &
        name)

    # Path 3 of 3, and the one that was missing once. The `blockOff + toCopy >
    # data.len` check below is NOT this bound: `toCopy` is clamped to what is
    # left of the entry, so a stream whose last data block landed in the
    # partial region was served successfully out of bytes the container does
    # not own. Check the block NUMBER, before its bytes are touched — and
    # before it is multiplied by the block size, which on a damaged container
    # can overflow.
    if dataBlock >= wholeBlocks:
      return err(outOfBounds("data block", blockIdx,
        " is block " & $dataBlock & ", which is out of bounds"))

    let blockOff = int(dataBlock) * int(blockSize)
    let toCopy = min(remaining, int(blockSize))
    if blockOff + toCopy > data.len:
      return err("data block content out of bounds")
    if runs.len > 0 and runs[^1].at + runs[^1].len == blockOff:
      runs[^1].len += toCopy
    else:
      runs.add((at: blockOff, len: toCopy))

    destPos += toCopy
    remaining -= toCopy
    blockIdx += 1

  ok(runs)

proc readMemberBytes*(data: openArray[byte], name: string,
    fileSize: uint64, mapBlock: uint64,
    blockSize: uint32): Result[seq[byte], string] =
  ## A member's `fileSize` bytes, copied out of the image (`memberRuns` says
  ## where they are and applies every bound).
  let runs = ? memberRuns(data, name, fileSize, mapBlock, blockSize)
  var bytes = newSeqUninit[byte](int(fileSize))  # every byte copied below
  var dest = 0
  for r in runs:
    copyMem(addr bytes[dest], unsafeAddr data[r.at], r.len)
    dest += r.len
  ok(bytes)

proc hasCtfsMagic*(data: openArray[byte]): bool =
  ## Check whether the first bytes match the CTFS magic.
  if data.len < 5:
    return false
  data[0] == CtfsMagic[0] and
  data[1] == CtfsMagic[1] and
  data[2] == CtfsMagic[2] and
  data[3] == CtfsMagic[3] and
  data[4] == CtfsMagic[4]

proc hasValidVersion*(data: openArray[byte]): bool =
  ## True when the version byte is 5, the version every door of this library
  ## reads (`ctfs-container.md` §2, "Older versions are refused"). Callers
  ## that report a refusal use `ctfsVersionError`, which names the version
  ## found. Version 6 is read by the doors listed at `CtfsVersionV6`, which
  ## gate on `ctfsReadableVersionError` instead.
  ctfsVersionError(data).len == 0

# ---------------------------------------------------------------------------
# The version-6 header fields: profile, and the whole-file compression scheme.
#
# `ctfs-container.md` §1a, §1b, §1c. Both fields are CLOSED sets, and §1c makes
# refusing an unknown value normative rather than advisory. The reason it is
# normative is this format's own history: a layout change once shipped under an
# unchanged version stamp, and a reader that trusted the stamp placed every step
# one line high AND returned success. `meta_dat.nim`'s
# `LastShiftedGlobalIndexVersion` and its opt-in are what that cost.
#
# So none of the five procs below has a permissive arm. An unknown scheme is not
# `none`, an unknown profile is not `full`, a non-zero reserved byte is not
# "ignored", and a header too short to carry a field the version declares is a
# refusal rather than an absent field — "the byte says 0" and "there is no byte"
# are different facts, and a parser that answers both the same way cannot report
# the second.
# ---------------------------------------------------------------------------

proc parseCtfsProfile*(value: uint8): Result[CtfsProfile, string] =
  ## Parse byte 16 of a version-6 header against the CLOSED set of
  ## `ctfs-container.md` §1a. An unrecognised value is an error and never
  ## `cpFull`: a parser that maps everything it does not recognise onto the
  ## most-capable value reports success on a container it is about to misread.
  case value
  of 0'u8: ok(cpFull)
  of 1'u8: ok(cpCompact)
  else: err("unknown CTFS container profile " & $value &
    ": the set is closed at 0 (full) and 1 (compact) — ctfs-container.md §1a")

proc parseWholeFileCompression*(
    value: uint8): Result[CtfsWholeFileCompression, string] =
  ## Parse byte 17 of a version-6 header against the CLOSED set of
  ## `ctfs-container.md` §1b. An unrecognised value is an error and never
  ## `wfcNone`: reading an unknown scheme as "no compression" hands the caller
  ## a body it will parse as a container and that is not one.
  case value
  of 0'u8: ok(wfcNone)
  of 1'u8: ok(wfcZstd)
  else: err("unknown CTFS whole-file compression scheme " & $value &
    ": the set is closed at 0 (none) and 1 (zstd) — ctfs-container.md §1b")

proc readCtfsProfile*(data: openArray[byte]): Result[CtfsProfile, string] =
  ## The profile a container declares.
  ##
  ## For version 5 the answer is `cpFull`, and that is an inference from a
  ## KNOWN version with a fully specified body — not a default applied to an
  ## unrecognised value. Version 6 reads byte 16 and parses it against the
  ## closed set. Every other version is the refusal `ctfsVersionError` gives,
  ## which is what keeps the two cases apart.
  if data.len < 6:
    return err(ctfsVersionError(data))
  if data[5] == CtfsVersionV6:
    if data.len <= V6ProfileOffset:
      return err("CTFS header declares version " & $CtfsVersionV6 &
        " but is only " & $data.len & " bytes, too short to carry the " &
        "profile byte at offset " & $V6ProfileOffset &
        "; a missing profile is not profile 0 (full)")
    return parseCtfsProfile(data[V6ProfileOffset])
  let versionErr = ctfsVersionError(data)
  if versionErr.len > 0:
    return err(versionErr)
  ok(cpFull)

proc readWholeFileCompression*(
    data: openArray[byte]): Result[CtfsWholeFileCompression, string] =
  ## The whole-file compression scheme a container declares. `wfcNone` for
  ## version 5, by the same known-version argument as `readCtfsProfile`; a
  ## truncated version-6 header is a refusal rather than `wfcNone`.
  if data.len < 6:
    return err(ctfsVersionError(data))
  if data[5] == CtfsVersionV6:
    if data.len <= V6CompressionOffset:
      return err("CTFS header declares version " & $CtfsVersionV6 &
        " but is only " & $data.len & " bytes, too short to carry the " &
        "compression byte at offset " & $V6CompressionOffset &
        "; a missing scheme is not scheme 0 (none)")
    return parseWholeFileCompression(data[V6CompressionOffset])
  let versionErr = ctfsVersionError(data)
  if versionErr.len > 0:
    return err(versionErr)
  ok(wfcNone)

proc checkV6Reserved*(data: openArray[byte]): Result[void, string] =
  ## Bytes 18--23 of a version-6 header MUST be zero. A non-zero value is a
  ## refusal rather than something to ignore, because "ignored" and "unknown"
  ## are the same byte: the only way to spend those bytes is another version
  ## bump.
  ##
  ## A buffer too short to carry the version byte is deferred rather than
  ## judged — this check cannot know whether it is looking at a version-6
  ## header at all, and `ctfsVersionError` owns that question. That is the one
  ## permissive answer here and it is stated so it is a decision.
  if data.len < 6 or data[5] != CtfsVersionV6:
    return ok()
  if data.len < V6HeaderSize:
    return err("CTFS header declares version " & $CtfsVersionV6 &
      " but is only " & $data.len & " bytes, short of the " & $V6HeaderSize &
      "-byte version-" & $CtfsVersionV6 & " header")
  for i in 0 ..< V6ReservedLen:
    let off = V6ReservedOffset + i
    if data[off] != 0'u8:
      return err("CTFS version-" & $CtfsVersionV6 &
        " reserved byte at offset " & $off & " is " & $data[off] &
        ", not 0: the reserved area is not a growth area and a non-zero " &
        "value there is a container this reader cannot account for — " &
        "ctfs-container.md §1")
  ok()

proc readEncryptionMethod*(data: openArray[byte]): CtfsEncryptionMethod =
  ## Read the encryption method from a CTFS header (byte 6).
  if data.len < 7:
    return emNone
  case data[6]
  of 0: emNone
  of 1: emAes256Gcm
  else: emNone

proc readMaxShards*(data: openArray[byte]): uint8 =
  ## Read the max_shards field from a CTFS header (byte 7).
  if data.len < 8:
    return DefaultMaxShards
  data[7]

# ---------------------------------------------------------------------------
# The compact profile's directory (`ctfs-container.md` §1d)
# ---------------------------------------------------------------------------
#
# Here rather than in `compact.nim` because a member is looked up by name
# through `readInternalFile` whatever the profile, and that lookup lives in
# this module; `compact.nim` re-exports all of it.

const
  CompactMemberCountOffset* = V6HeaderSize
    ## Offset of the u32 LE member count: 24, immediately after the header.
  CompactDirectoryOffset* = V6HeaderSize + 4
    ## Offset of the first directory entry: 28.
  CompactDirEntrySize* = 24
    ## `(name: u64, offset: u64, length: u64)` — the same 24 bytes a
    ## `FileEntry` occupies, carrying a different three fields.
  CompactDirNameOffset* = 0
  CompactDirOffsetOffset* = 8
  CompactDirLengthOffset* = 16
  CompactEmptySize* = CompactDirectoryOffset
    ## The size of a compact container with no members: 28 bytes.

type
  CompactDirEntry* = object
    name*: string
    encodedName*: uint64
    offset*: uint64
    length*: uint64

  CompactDirectory* = object
    ## A validated directory. Constructing one of these is the only way to get
    ## at a member, so every read goes through §1d's six checks.
    entries*: seq[CompactDirEntry]
    size*: uint64  ## the container image's length, as checked

proc compactDirEntryOffset*(index: int): int =
  ## Byte offset of directory entry `index` from the start of the image.
  CompactDirectoryOffset + index * CompactDirEntrySize

proc nameIsWellFormed*(encoded: uint64): bool =
  ## §1d check 5: a name is non-zero and round-trips through §3's packing.
  ##
  ## Two things this refuses that a bare `base40Decode` does not. A `u64` at or
  ## above `40^12` names nothing — the 12 base-40 digits cannot represent it —
  ## and re-encoding the 12 digits it does carry does not reproduce it. And a
  ## packing with a padding digit before a non-padding one decodes to a string
  ## with an embedded NUL, which §3's encoder cannot produce and which DOES
  ## re-encode to itself (the encoder maps anything outside the alphabet to
  ## index 0), so the round-trip alone misses it and `base40Encodable` is what
  ## catches it.
  if encoded == 0'u64:
    return false
  let decoded = base40Decode(encoded)
  base40Encodable(decoded) and base40Encode(decoded) == encoded

proc compactRefusal(what: string, value: uint64, rule: string): string =
  ## A compact container's refusal, naming the offending value and the rule
  ## (`ctfs-container.md` §1c).
  "compact container " & what & " " & $value & " (ctfs-container.md " & rule & ")"

proc readCompactDirectory*(data: openArray[byte],
    bodyReconstructed = false): Result[CompactDirectory, string] =
  ## Parse and VALIDATE the directory of a compact container, applying all six
  ## of `ctfs-container.md` §1d's checks and naming the offending value.
  ##
  ## The header gate comes first and comes through the §1c parsers, so a
  ## container that is not a version-6 compact one is refused here rather than
  ## read as a directory that is not one.
  ##
  ## `bodyReconstructed` exists because of a subtlety in §1a that is easy to get
  ## backwards, and this function did get it backwards once. A reader of a
  ## container under a whole-file scheme reconstructs the image as
  ## `header || decompress(rest)`, and the reconstructed image KEEPS the
  ## original 24-byte header — so it still declares its scheme. A decoder that
  ## refused any container declaring `wfcZstd` would therefore refuse the
  ## legitimately reconstructed image as well as the stored one, which is the
  ## opposite of the intended safety. So the field is not the gate: the caller
  ## states whether it has done the reconstruction, and the DEFAULT is that it
  ## has not, so handing this function stored compressed bytes is still a
  ## refusal that names the reason rather than a directory read out of a
  ## compressed body.
  if not hasCtfsMagic(data):
    return err("not a CTFS container: the first five bytes are not the magic")
  let profile = ?readCtfsProfile(data)
  if profile != cpCompact:
    return err("container declares profile " & $profile &
      ", not compact (ctfs-container.md §1d)")
  ?checkV6Reserved(data)
  # Parsed unconditionally, because an UNKNOWN scheme is §1c's refusal whether
  # or not the caller claims to have reconstructed anything.
  let scheme = ?readWholeFileCompression(data)
  if scheme != wfcNone and not bodyReconstructed:
    return err(compactRefusal("declares whole-file compression", uint64(ord(scheme)),
      "§1a: its body is read after it is reconstructed"))
  # §1d: there are no blocks, no FileEntry array and no block-number space,
  # so a block size, a root-entry maximum or a shard count other than 0 would
  # be a second spelling of one state.
  let blockSize = readU32LE(data, 8)
  if blockSize != 0'u32:
    return err(compactRefusal("declares BlockSize", blockSize, "§1d: 0"))
  let maxRootEntries = readU32LE(data, 12)
  if maxRootEntries != 0'u32:
    return err(compactRefusal("declares MaxRootEntries", maxRootEntries,
      "§1d: 0"))
  let maxShards = readMaxShards(data)
  if maxShards != 0'u8:
    return err(compactRefusal("declares MaxShards", maxShards, "§1a: 0"))

  if data.len < CompactDirectoryOffset:
    return err(compactRefusal("is too short for a member count, in bytes",
      uint64(data.len), "§1d"))

  let count = readU32LE(data, CompactMemberCountOffset)
  # §1d check 1: the directory itself fits.
  let dirEnd = uint64(CompactDirectoryOffset) +
    uint64(count) * uint64(CompactDirEntrySize)
  if dirEnd > uint64(data.len):
    return err(compactRefusal("declares more members than its size holds",
      count, "§1d check 1"))

  var dir = CompactDirectory(size: uint64(data.len))
  var expected = dirEnd  # §1d check 2: the first member starts right here.
  for i in 0 ..< int(count):
    let e = compactDirEntryOffset(i)
    let encoded = readU64LE(data, e + CompactDirNameOffset)
    let offset = readU64LE(data, e + CompactDirOffsetOffset)
    let length = readU64LE(data, e + CompactDirLengthOffset)

    # §1d check 5.
    if not nameIsWellFormed(encoded):
      return err(compactRefusal("entry " & $i & " carries name word", encoded,
        "§1d check 5"))
    let name = base40Decode(encoded)

    # §1d checks 2 and 3, as one: the member begins where its predecessor
    # ended, and the first begins where the directory ended. A gap would be
    # padding, and an overlap or a jump would serve a shifted member.
    if offset != expected:
      return err(compactRefusal("entry " & $i & " ('" & name &
        "') declares offset", offset,
        if i == 0: "§1d check 2" else: "§1d check 3"))
    if length > uint64(data.len) or offset + length > uint64(data.len):
      return err(compactRefusal("entry " & $i & " ('" & name &
        "') runs past the container with length", length, "§1d check 4"))

    # §1d check 6.
    for prev in dir.entries:
      if prev.encodedName == encoded:
        return err(compactRefusal("names '" & name & "' twice, at entry",
          uint64(i), "§1d check 6"))

    dir.entries.add(CompactDirEntry(name: name, encodedName: encoded,
                                    offset: offset, length: length))
    expected = offset + length

  # §1d check 4: nothing follows the last member. This is the check that makes
  # "no padding" an assertion against the bytes rather than a restatement of
  # the encoder's intent, and it is also what refuses a truncated container
  # whose directory happens to be intact: Size = 28 + 24*N + sum(length).
  if expected != uint64(data.len):
    return err(compactRefusal("has its members end at " & $expected &
      " and its size is", uint64(data.len), "§1d check 4"))

  ok(dir)

proc findCompactMember*(dir: CompactDirectory, name: string): int =
  ## Index of `name` in a validated directory, or -1. A linear search over one
  ## `u64` per member: §1d states the directory is NOT sorted, so this is the
  ## only correct lookup.
  if not base40Encodable(name):
    return -1
  let encoded = base40Encode(name)
  for i, e in dir.entries.pairs:
    if e.encodedName == encoded:
      return i
  -1

# ---------------------------------------------------------------------------
# Members by name, in either profile
# ---------------------------------------------------------------------------

type
  ContainerBody = enum
    cbFull      ## block 0, the entry array and the block map (version 5, or 6
                ## at profile 0)
    cbCompact   ## the compact directory (version 6, profile 1)

proc containerBody(data: openArray[byte]): Result[ContainerBody, string] =
  ## Which body `data` carries, refusing every version this library does not
  ## read and, at version 6, every header value §1c has a reader refuse.
  ##
  ## A container declaring a whole-file scheme is refused too: §1a's body
  ## under the scheme is not the image any offset in it addresses, so the
  ## caller reconstructs `header || decompress(rest)` first
  ## (`compact.reconstructImage`) and reads members out of that.
  let versionErr = ctfsReadableVersionError(data)
  if versionErr.len > 0:
    return err(versionErr)
  if data[5] != CtfsVersionV6:
    return ok(cbFull)
  let profile = ?readCtfsProfile(data)
  ?checkV6Reserved(data)
  let scheme = ?readWholeFileCompression(data)
  if scheme != wfcNone:
    return err("CTFS container declares whole-file compression scheme " &
      $scheme & ": its body is read after it is reconstructed as header || " &
      "decompress(rest) (ctfs-container.md §1a), and these bytes are the " &
      "stored body")
  ok(if profile == cpCompact: cbCompact else: cbFull)

proc checkReadableContainer*(data: openArray[byte]): Result[void, string] =
  ## Refuse, naming the value, a container whose members this library cannot
  ## read: a version other than 5 and 6, a version-6 header §1c refuses, a
  ## body still under a whole-file scheme, or a compact directory that fails
  ## one of §1d's checks. A reader opening a container calls it first, so a
  ## container it cannot read is refused rather than read as one with no
  ## members.
  if not hasCtfsMagic(data):
    return err("not a CTFS container: the first five bytes are not the magic")
  if ? containerBody(data) == cbCompact:
    discard ? readCompactDirectory(data)
  ok()

proc isCompactContainer*(data: openArray[byte]): bool =
  ## True for a container whose header declares the compact profile, whose
  ## framed members store their chunks as content (`ctfs-container.md` §1f).
  data.len > V6ProfileOffset and data[5] == CtfsVersionV6 and
    data[V6ProfileOffset] == uint8(ord(cpCompact))

proc locateMember*(data: openArray[byte], name: string,
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries,
    loader: BlockLoader = nil):
    Result[seq[MemberRun], string] =
  ## Where an internal file's bytes lie in the container image `data`.
  ##
  ## Reads versions 5 and 6 in both profiles, and refuses every other version
  ## before it resolves anything (`ctfs-container.md` §2, "Older versions are
  ## refused", and §1c). A full body's member is resolved through its entry's
  ## `MapBlock` (`memberRuns`); a compact body's through its directory, which
  ## is checked against all six of §1d's rules first, and is one run. A name
  ## base40 cannot pack is refused by name (`ctfs-container.md` §3).
  let refusal = base40Refusal(name)
  if refusal.len > 0:
    return err(refusal)
  case ?containerBody(data)
  of cbFull:
    let entry = findFileEntry(data, name, maxEntries)
    if not entry.found:
      return err("internal file not found: " & name)
    memberRuns(data, name, entry.size, entry.mapBlock, blockSize, loader)
  of cbCompact:
    let dir = ?readCompactDirectory(data)
    let idx = findCompactMember(dir, name)
    if idx < 0:
      return err("internal file not found: " & name)
    let e = dir.entries[idx]
    if e.length == 0:
      return ok(newSeq[MemberRun](0))
    ok(@[(at: int(e.offset), len: int(e.length))])

proc readInternalFile*(data: openArray[byte], name: string,
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): Result[seq[byte], string] =
  ## Read the complete content of an internal CTFS file, copied out of the
  ## image: `locateMember` says where it is and applies every check.
  let runs = ? locateMember(data, name, blockSize, maxEntries)
  var total = 0
  for r in runs: total += r.len
  var bytes = newSeqUninit[byte](total)  # every byte copied below
  var dest = 0
  for r in runs:
    copyMem(addr bytes[dest], unsafeAddr data[r.at], r.len)
    dest += r.len
  ok(bytes)

proc hasInternalFile*(data: openArray[byte], name: string,
    maxEntries: uint32 = DefaultMaxRootEntries): bool =
  ## Return true iff the container carries an internal file with the given
  ## name — including an EMPTY one, whose entry is `(0, 0)` with its name
  ## (`ctfs-container.md` §2). Presence is the name's, not the size's. A
  ## container this library cannot read carries nothing it can name.
  let body = containerBody(data)
  if body.isErr:
    # The version-5 answer did not depend on the version byte; keep it so for
    # every container that is not version 6.
    return data.len > 5 and data[5] != CtfsVersionV6 and
      findFileEntry(data, name, maxEntries).found
  case body.get()
  of cbFull: findFileEntry(data, name, maxEntries).found
  of cbCompact:
    let dir = readCompactDirectory(data)
    dir.isOk and findCompactMember(dir.get(), name) >= 0
