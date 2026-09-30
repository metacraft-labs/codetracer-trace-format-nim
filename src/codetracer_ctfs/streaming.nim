when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## CTFS streaming mode — creates containers that stream writes to disk
## so concurrent readers can see data as it is written.

import results
import ./types
import ./block_mapping
import ./container

proc createCtfsStreaming*(path: string, blockSize: uint32 = DefaultBlockSize,
                          maxRootEntries: uint32 = DefaultMaxRootEntries,
                          encryption: CtfsEncryptionMethod = emNone,
                          maxShards: uint8 = DefaultMaxShards): Result[Ctfs, string] =
  ## Create a new CTFS v4 container that streams writes to disk.
  ## The file is opened immediately and the initial root region (header +
  ## file entries: block 0, plus the blocks the entry array overflows into)
  ## is written so concurrent readers can see the container structure as
  ## soon as it is created.
  var c = createCtfs(blockSize, maxRootEntries, encryption, maxShards)
  try:
    c.streamFile = open(path, fmReadWrite)
    c.streamPath = path
    c.streaming = true
    # Write the initial root region to disk.
    discard c.streamFile.writeBuffer(addr c.data[0], c.data.len)
    c.streamFile.flushFile()
    ok(c)
  except IOError:
    err("failed to open streaming file: " & path)
  except OSError:
    err("failed to open streaming file: " & path)

proc syncEntry*(c: var Ctfs, f: CtfsInternalFile) =
  ## Update the file entry's size field on disk so concurrent readers can
  ## see the current logical file size. Also flushes the root block so
  ## newly added file entries are visible.
  if not c.streaming:
    return
  # The file entry lives in the root region (block 0, or a later root block
  # when the entry array overflows block 0). Write the entire entry (24
  # bytes: size + mapBlock + name) so the reader sees a consistent snapshot.
  let entryOff = c.fileEntryOffset(f.entryIndex)
  try:
    c.streamFile.setFilePos(int64(entryOff))
    if entryOff + FileEntrySize <= c.data.len:
      discard c.streamFile.writeBuffer(addr c.data[entryOff], FileEntrySize)
    c.streamFile.flushFile()
  except IOError, OSError:
    discard

proc syncAllEntries*(c: var Ctfs) =
  ## Flush the entire root region (block 0 and any blocks the entry array
  ## overflows into) to disk, updating all file entry sizes at once for
  ## concurrent readers.
  if not c.streaming:
    return
  c.flushRootBlocks()
  try:
    c.streamFile.flushFile()
  except IOError, OSError:
    discard
