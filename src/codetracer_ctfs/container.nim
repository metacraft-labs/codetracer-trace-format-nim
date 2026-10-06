when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## CTFS container create/read/write/close operations.

import std/algorithm
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
  doAssert maxRootEntries != 0 or uint64(blockSize) >= uint64(rootEntryStart(maxShards)),
    "CTFS auto-fill root prefix does not fit block 0"
  c.maxRootEntries = effectiveRootEntryCount(blockSize, maxRootEntries, maxShards)
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

  err("no free file entry slots")

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

proc findFileEntry*(data: openArray[byte], name: string,
    maxEntries: uint32 = DefaultMaxRootEntries): CtfsEntryLookup =
  ## Find `name` in the root directory of the container image `data`.
  let layout = rootDirectoryLayout(data)
  if layout.error.len > 0:
    return CtfsEntryLookup(found: false)
  let entryStart = layout.entryStart
  let encoded = base40Encode(name)
  for i in 0 ..< int(min(maxEntries, layout.entryCount)):
    let off = entryStart + i * FileEntrySize
    if off + FileEntrySize > data.len:
      break
    if readU64LE(data, off + 16) == encoded:
      return CtfsEntryLookup(found: true, index: i,
        size: readU64LE(data, off), mapBlock: readU64LE(data, off + 8))
  CtfsEntryLookup(found: false)

proc readMemberBytes*(data: openArray[byte], name: string,
    fileSize: uint64, mapBlock: uint64,
    blockSize: uint32): Result[seq[byte], string] =
  ## Read a member's `fileSize` bytes given its entry's `MapBlock`, in any of
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
  let truncatedNote = " — the container carries " & $wholeBlocks &
    " whole " & $blockSize & "-byte blocks in " & $data.len &
    " bytes, so it is truncated or its tail write was interrupted"

  if mapBlock == 0'u64:
    if fileSize == 0:
      return ok(newSeq[byte](0))
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
      return err("direct data block " & $b & " of internal file " & name &
        " is out of bounds" & truncatedNote)
    if fileSize > uint64(blockSize):
      return err("internal file " & name & " is stored in one direct block " &
        "but declares " & $fileSize & " bytes, more than one " & $blockSize &
        "-byte block holds")
    var direct = newSeq[byte](int(fileSize))
    if fileSize > 0:
      copyMem(addr direct[0], unsafeAddr data[int(b) * int(blockSize)],
        int(fileSize))
    return ok(direct)

  # Path 1 of 3: the entry's mapping root.
  if mapBlock >= wholeBlocks:
    return err("mapping root block " & $mapBlock & " of internal file " & name &
      " is out of bounds" & truncatedNote)
  if fileSize == 0:
    return ok(newSeq[byte](0))

  var fileBytes = newSeq[byte](int(fileSize))
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
        let chainPtr = readU64LE(data, chainOff)
        if chainPtr == 0:
          return err("missing chain pointer at level " & $level &
            " of internal file " & name)
        # Path 2a of 3: a mapping block reached through the chain.
        if chainPtr >= wholeBlocks:
          return err("chain pointer at level " & $level & " of internal file " &
            name & " names block " & $chainPtr & ", which is out of bounds" &
            truncatedNote)
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
      let childBlock = readU64LE(data, childOff)
      if childBlock == 0:
        return err("missing child block at level " & $navLevel &
          " of internal file " & name)
      # Path 2b of 3: a mapping block reached by descending the hierarchy.
      if childBlock >= wholeBlocks:
        return err("child block pointer at level " & $navLevel &
          " of internal file " & name & " names block " & $childBlock &
          ", which is out of bounds" & truncatedNote)
      navBlock = childBlock
      navIdx = subIdx
      navLevel -= 1

    let ptrOff = int(navBlock) * int(blockSize) + int(navIdx) * 8
    if ptrOff + 8 > data.len:
      return err("data block pointer out of bounds")
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
      return err("data block " & $blockIdx & " of internal file " & name &
        " is block " & $dataBlock & ", which is out of bounds" & truncatedNote)

    let blockOff = int(dataBlock) * int(blockSize)
    let toCopy = min(remaining, int(blockSize))
    if blockOff + toCopy > data.len:
      return err("data block content out of bounds")
    copyMem(addr fileBytes[destPos], unsafeAddr data[blockOff], toCopy)

    destPos += toCopy
    remaining -= toCopy
    blockIdx += 1

  ok(fileBytes)

proc readInternalFile*(data: openArray[byte], name: string,
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): Result[seq[byte], string] =
  ## Read the complete content of an internal CTFS file.
  ##
  ## Refuses a container whose version is not 5 before it resolves anything
  ## (`ctfs-container.md` §2, "Older versions are refused"). See
  ## `readMemberBytes` for the three `MapBlock` forms and the bounds applied.
  let versionErr = ctfsVersionError(data)
  if versionErr.len > 0:
    return err(versionErr)
  let entry = findFileEntry(data, name, maxEntries)
  if not entry.found:
    return err("internal file not found: " & name)
  readMemberBytes(data, name, entry.size, entry.mapBlock, blockSize)

proc hasInternalFile*(data: openArray[byte], name: string,
    maxEntries: uint32 = DefaultMaxRootEntries): bool =
  ## Return true iff the CTFS root directory carries an internal file with
  ## the given name — including an EMPTY one, whose entry is `(0, 0)` with its
  ## name (`ctfs-container.md` §2). Presence is the name's, not the size's.
  findFileEntry(data, name, maxEntries).found

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
  ## True when the version byte is 5, the only version this library reads
  ## (`ctfs-container.md` §2, "Older versions are refused"). Callers that
  ## report a refusal use `ctfsVersionError`, which names the version found.
  ##
  ## Version 6 is NOT read here, and the omission is the gate the compact
  ## profile rests on: see `CtfsVersionV6`.
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
