when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## CTFS streaming mode — creates containers that stream writes to disk
## so concurrent readers can see data as it is written.

import results
import ./types
import ./container

proc createCtfsStreaming*(path: string, blockSize: uint32 = DefaultBlockSize,
                          maxRootEntries: uint32 = DefaultMaxRootEntries,
                          encryption: CtfsEncryptionMethod = emNone,
                          maxShards: uint8 = DefaultMaxShards): Result[Ctfs, string] =
  ## Create a new CTFS container (version 5) that streams writes to disk.
  ## The file is opened immediately and the initial root region (header +
  ## file entries: block 0, plus the blocks the entry array overflows into)
  ## is written so concurrent readers can see the container structure as
  ## soon as it is created.
  let refusal = blockSizeRefusal(blockSize)
  if refusal.len > 0:
    return err(refusal)
  var c = createCtfs(blockSize, maxRootEntries, encryption, maxShards)
  try:
    c.streamFile = open(path, fmReadWrite)
    c.streamPath = path
    c.streaming = true
    c.deferWrites = true
    # Write the initial root region to disk.
    discard c.streamFile.writeBuffer(addr c.data[0], c.data.len)
    c.streamFile.flushFile()
    ok(c)
  except IOError:
    err("failed to open streaming file: " & path)
  except OSError:
    err("failed to open streaming file: " & path)

proc syncEntry*(c: var Ctfs, f: CtfsInternalFile) =
  ## Publish the container (see `publish`) so concurrent readers see every
  ## member's current size, `f`'s included.
  c.publish()

proc syncAllEntries*(c: var Ctfs) =
  ## Publish the container (see `publish`): every member's data, then the
  ## whole root region with every entry's size.
  c.publish()
