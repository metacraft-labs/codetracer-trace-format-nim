when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## CTFS container create/read/write/close operations.

import results
import ./types
import ./base40
import ./block_mapping

proc createCtfs*(
    blockSize: uint32 = DefaultBlockSize,
    maxRootEntries: uint32 = DefaultMaxRootEntries,
    encryption: CtfsEncryptionMethod = emNone,
    maxShards: uint8 = DefaultMaxShards): Ctfs =
  ## Create a new in-memory CTFS v4 container.
  ## Header layout (per spec):
  ##   [0-4] magic  [5] version  [6] encryption  [7] max_shards
  ## Compression is NOT in the header — it is per-stream in meta.dat.
  var c: Ctfs
  c.blockSize = blockSize
  c.maxRootEntries = maxRootEntries
  c.encryption = encryption
  c.maxShards = maxShards
  c.data = newSeq[byte](int(blockSize))
  c.nextFreeBlock = 1  # Block 0 is the root block

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
  ## A DUPLICATE root name is rejected.  Every reader resolves a name to the
  ## FIRST matching root entry, so appending a second entry with the same name
  ## does not update that file — it SHADOWS it, and the shadowed member becomes
  ## unreachable while still occupying the container.  That failure mode is
  ## invisible (the write "succeeds", the read returns stale bytes), so the
  ## writer refuses it rather than letting a caller discover it downstream.
  let encodedName = base40Encode(name)

  # Reject a duplicate before allocating anything.
  for i in 0 ..< int(c.maxRootEntries):
    let off = c.fileEntryOffset(i)
    if off + 24 > c.data.len: break
    if readU64LE(c.data, off + 16) == encodedName and
       (readU64LE(c.data, off) != 0 or readU64LE(c.data, off + 8) != 0):
      return err("duplicate CTFS file name '" & name & "': a container may " &
                 "hold only one member per name (readers take the first, so " &
                 "a second entry would silently shadow it)")

  # Find first empty file entry.
  for i in 0 ..< int(c.maxRootEntries):
    let off = c.fileEntryOffset(i)
    let entrySize = readU64LE(c.data, off)
    let entryMap = readU64LE(c.data, off + 8)
    let entryName = readU64LE(c.data, off + 16)
    if entrySize == 0 and entryMap == 0 and entryName == 0:
      # Found empty slot -- write name and allocate a level-1 mapping block.
      writeU64LE(c.data, off + 16, encodedName)
      let mapBlock = c.allocBlock()
      c.zeroBlock(mapBlock)
      writeU64LE(c.data, off + 8, mapBlock)
      # When streaming, flush the root block (block 0) so the new file entry
      # and mapping block pointer are visible to concurrent readers.
      if c.streaming:
        c.flushBlock(0)
        c.flushBlock(mapBlock)
      return ok(CtfsInternalFile(entryIndex: i, writePos: 0, dataBlockCount: 0))

  err("no free file entry slots")

proc writeToFile*(c: var Ctfs, f: var CtfsInternalFile,
                  data: openArray[byte]): Result[void, string] =
  ## Append data to an internal file. Uses multi-level block mapping.
  ##
  ## **Every block number this proc turns into a byte offset is checked first,
  ## and that is a data-integrity rule rather than defensiveness.** Block 0 is
  ## the container header and the root directory, so a block number of `0` —
  ## which is what `lookupDataBlock` returns for *any* mapping it cannot
  ## resolve, at any level — addresses byte offset 0. Writing a caller's
  ## payload there does not damage one stream, it overwrites the header and
  ## the entire entry array, so the `.ct` stops being a container and every
  ## member in it becomes unreachable. Pinned by
  ## `tests/test_ctfs_append_null_data_block.nim`.
  if data.len == 0:
    return ok()

  let entryOff = c.fileEntryOffset(f.entryIndex)
  let mapBlock = readU64LE(c.data, entryOff + 8)
  # The entry's mapping root, checked before any walk: a zero here is the state
  # a crash between publishing an entry's size and publishing its mapping root
  # leaves, and the walk below would read its pointers out of block 0.
  if mapBlock == 0'u64 or mapBlock >= c.nextFreeBlock:
    return err("internal file entry " & $f.entryIndex & " has mapping root block " &
      $mapBlock & ", which is outside the container's " & $c.nextFreeBlock &
      " allocated blocks; refusing to write through it")

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
      # refused append does not leave the writer's block count ahead of the
      # mapping. `insertDataBlock` is all-or-nothing itself: with the
      # null-pointer rule in place, both of its failure branches return before
      # allocating or writing anything, so this one rollback is the whole of it.
      let blocksBefore = c.nextFreeBlock
      let bytesBefore = c.data.len
      dataBlock = c.allocBlock()
      let insertRes = c.insertDataBlock(mapBlock, uint64(fileBlockIdx), dataBlock)
      if insertRes.isErr:
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
    if dataBlock >= c.nextFreeBlock:
      return err("data block " & $fileBlockIdx & " of internal file entry " &
        $f.entryIndex & " resolves to block " & $dataBlock &
        ", which is outside the container's " & $c.nextFreeBlock &
        " allocated blocks")

    # Write data into the block.
    let blockStart = c.blockOffset(dataBlock)
    let space = int(c.blockSize) - offsetInBlock
    let toWrite = min(space, data.len - written)
    for i in 0 ..< toWrite:
      c.data[blockStart + offsetInBlock + i] = data[written + i]

    # Flush this data block to disk when streaming.
    if c.streaming:
      c.flushBlock(dataBlock)

    written += toWrite
    f.writePos += uint64(toWrite)

  # Update file size.
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
    let dataBlock = c.lookupDataBlock(mapBlock, fileBlockIdx)
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
  ## Point an existing file entry at a FRESH, empty mapping block and return a
  ## handle at position 0, so the caller can rewrite its content at a
  ## different length than before.
  ##
  ## `rewriteFileContent` cannot do this: it is deliberately length-preserving
  ## because its callers (e.g. `meta.dat`) must not move any later block. This
  ## is the opposite case — re-finalising a stream whose new image is a
  ## different length.
  ##
  ## The old mapping and data blocks are abandoned in place.  Nothing
  ## references them once the entry's pointer moves, and the container's block
  ## allocator only ever moves forward, so the orphans are inert padding — the
  ## same trade `addFile` already makes for a file that is created and never
  ## written.  They are NOT reclaimed, which is why this is an append-time
  ## finalisation step and not something to call in a loop.
  let entryOff = c.fileEntryOffset(f.entryIndex)
  let mapBlock = c.allocBlock()
  c.zeroBlock(mapBlock)
  writeU64LE(c.data, entryOff + 8, mapBlock)
  writeU64LE(c.data, entryOff, 0)
  if c.streaming:
    c.flushBlock(0)
    c.flushBlock(mapBlock)
  ok(CtfsInternalFile(entryIndex: f.entryIndex, writePos: 0, dataBlockCount: 0))

proc closeCtfs*(c: var Ctfs) =
  ## Close the container. When streaming, flushes all data and closes the file.
  if c.streaming:
    try:
      # Final flush of all in-memory data to disk.
      c.streamFile.setFilePos(0)
      discard c.streamFile.writeBuffer(addr c.data[0], c.data.len)
      c.streamFile.flushFile()
      c.streamFile.close()
    except IOError, OSError:
      discard
    c.streaming = false

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

proc readInternalFile*(data: openArray[byte], name: string,
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): Result[seq[byte], string] =
  ## Read the complete content of an internal CTFS file by following the block mapping.
  let encoded = base40Encode(name)
  var fileSize: uint64 = 0
  var mapBlock: uint64 = 0
  block findEntry:
    for i in 0 ..< int(maxEntries):
      let off = HeaderSize + ExtHeaderSize + i * FileEntrySize
      if off + FileEntrySize > data.len:
        break
      let entrySize = readU64LE(data, off)
      let entryMap = readU64LE(data, off + 8)
      let entryName = readU64LE(data, off + 16)
      if entryName == encoded:
        fileSize = entrySize
        mapBlock = entryMap
        break findEntry
    return err("internal file not found: " & name)

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
          return err("missing chain pointer at level " & $level)
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
        return err("missing child block at level " & $navLevel)
      navBlock = childBlock
      navIdx = subIdx
      navLevel -= 1

    let ptrOff = int(navBlock) * int(blockSize) + int(navIdx) * 8
    if ptrOff + 8 > data.len:
      return err("data block pointer out of bounds")
    let dataBlock = readU64LE(data, ptrOff)
    if dataBlock == 0:
      return err("null data block at index " & $blockIdx)

    let blockOff = int(dataBlock) * int(blockSize)
    let toCopy = min(remaining, int(blockSize))
    if blockOff + toCopy > data.len:
      return err("data block content out of bounds")
    for i in 0 ..< toCopy:
      fileBytes[destPos + i] = data[blockOff + i]

    destPos += toCopy
    remaining -= toCopy
    blockIdx += 1

  ok(fileBytes)

proc hasInternalFile*(data: openArray[byte], name: string,
    maxEntries: uint32 = DefaultMaxRootEntries): bool =
  ## Return true iff the CTFS root directory carries an internal file with
  ## the given name.  Used by readers to decide which stream layout a bundle
  ## advertises (e.g. ``steps.dat`` for the split execution stream vs the
  ## legacy combined ``events.log``).  A name whose root entry has both a
  ## zero size AND a zero map block is treated as absent, matching the
  ## sentinel the writer leaves for unallocated root slots.
  let encoded = base40Encode(name)
  for i in 0 ..< int(maxEntries):
    let off = HeaderSize + ExtHeaderSize + i * FileEntrySize
    if off + FileEntrySize > data.len:
      break
    let entryName = readU64LE(data, off + 16)
    if entryName == encoded:
      let entrySize = readU64LE(data, off)
      let entryMap = readU64LE(data, off + 8)
      return not (entrySize == 0'u64 and entryMap == 0'u64)
  false

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
  ## Check whether the version byte is v2, v3, or v4 (all accepted by v4 readers).
  if data.len < 6:
    return false
  data[5] == CtfsVersion or data[5] == CtfsVersionV3 or data[5] == CtfsVersionV2

proc readCompressionMethod*(data: openArray[byte]): CtfsCompressionMethod =
  ## Read the compression method from a CTFS header.
  ## V3 layout: byte 6 = compression, byte 7 = encryption.
  ## V4 layout: compression is NOT in the header (per spec, it is per-stream in meta.dat).
  ## Returns cmNone for v2 and v4+ files; reads byte 6 only for v3.
  if data.len < 7:
    return cmNone
  let version = data[5]
  if version == CtfsVersionV3:
    # V3 (old layout): byte 6 was compression
    case data[6]
    of 0: cmNone
    of 1: cmZstd
    of 2: cmLz4
    else: cmNone
  else:
    # V4+ and V2: no compression in header
    cmNone

proc readEncryptionMethod*(data: openArray[byte]): CtfsEncryptionMethod =
  ## Read the encryption method from a CTFS header.
  ## V3 layout: byte 7 = encryption.
  ## V4 layout: byte 6 = encryption.
  ## Returns emNone for v2 files.
  if data.len < 7:
    return emNone
  let version = data[5]
  if version == CtfsVersionV3:
    # V3 (old layout): byte 7 was encryption
    if data.len < 8:
      return emNone
    case data[7]
    of 0: emNone
    of 1: emAes256Gcm
    else: emNone
  else:
    # V4+ layout: byte 6 = encryption
    case data[6]
    of 0: emNone
    of 1: emAes256Gcm
    else: emNone

proc readMaxShards*(data: openArray[byte]): uint8 =
  ## Read the max_shards field from a CTFS v4+ header (byte 7).
  ## Returns 1 for v2/v3 files (no max_shards in those versions).
  if data.len < 8:
    return DefaultMaxShards
  let version = data[5]
  if version >= CtfsVersion:
    # V4+ layout: byte 7 = max_shards
    data[7]
  else:
    DefaultMaxShards
