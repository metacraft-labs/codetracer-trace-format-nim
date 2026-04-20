{.push raises: [].}

## CTFS container create/read/write/close operations.

import results
import ./types
import ./base40
import ./block_mapping

proc createCtfs*(blockSize: uint32 = DefaultBlockSize,
                 maxRootEntries: uint32 = DefaultMaxRootEntries,
                 compression: CtfsCompressionMethod = cmNone,
                 encryption: CtfsEncryptionMethod = emNone): Ctfs =
  ## Create a new in-memory CTFS v3 container.
  var c: Ctfs
  c.blockSize = blockSize
  c.maxRootEntries = maxRootEntries
  c.compression = compression
  c.encryption = encryption
  c.data = newSeq[byte](int(blockSize))
  c.nextFreeBlock = 1  # Block 0 is the root block

  # Write header (8 bytes)
  c.data[0] = CtfsMagic[0]
  c.data[1] = CtfsMagic[1]
  c.data[2] = CtfsMagic[2]
  c.data[3] = CtfsMagic[3]
  c.data[4] = CtfsMagic[4]
  c.data[5] = CtfsVersion
  c.data[6] = uint8(compression)  # compression method
  c.data[7] = uint8(encryption)   # encryption method

  # Write extended header (8 bytes)
  writeU32LE(c.data, 8, blockSize)
  writeU32LE(c.data, 12, maxRootEntries)

  c

proc addFile*(c: var Ctfs, name: string): Result[CtfsInternalFile, string] =
  ## Add a new named file to the container. Returns a handle for writing.
  let encodedName = base40Encode(name)

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
  if data.len == 0:
    return ok()

  let entryOff = c.fileEntryOffset(f.entryIndex)
  let mapBlock = readU64LE(c.data, entryOff + 8)

  var written = 0
  while written < data.len:
    let fileBlockIdx = int(f.writePos) div int(c.blockSize)
    let offsetInBlock = int(f.writePos) mod int(c.blockSize)

    # Determine the data block for this file position.
    # If we're at the start of a new block, allocate and insert it.
    var dataBlock: uint64

    if offsetInBlock == 0:
      # Need a new data block.
      dataBlock = c.allocBlock()
      let insertRes = c.insertDataBlock(mapBlock, uint64(fileBlockIdx), dataBlock)
      if insertRes.isErr:
        return err(insertRes.error)
    else:
      # Mid-block write: look up the existing data block by navigating the chain.
      dataBlock = c.lookupDataBlock(mapBlock, uint64(fileBlockIdx))

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
  ## Check whether the version byte is v2 or v3 (both accepted by v3 readers).
  if data.len < 6:
    return false
  data[5] == CtfsVersion or data[5] == CtfsVersionV2

proc readCompressionMethod*(data: openArray[byte]): CtfsCompressionMethod =
  ## Read the compression method from a CTFS header.
  ## Returns cmNone for v2 files (bytes 6-7 were reserved as 0x00).
  if data.len < 7:
    return cmNone
  case data[6]
  of 0: cmNone
  of 1: cmZstd
  of 2: cmLz4
  else: cmNone  # Unknown method, treat as none

proc readEncryptionMethod*(data: openArray[byte]): CtfsEncryptionMethod =
  ## Read the encryption method from a CTFS header.
  ## Returns emNone for v2 files.
  if data.len < 8:
    return emNone
  case data[7]
  of 0: emNone
  of 1: emAes256Gcm
  else: emNone
