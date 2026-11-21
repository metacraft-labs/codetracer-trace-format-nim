when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Keyed streams (`ctfs-container.md` §7a): container streams exactly like a
## root member's, with a `Size` and a `MapBlock` of §2's three forms resolved
## by §4's walk, but referenced from a keyed member's record instead of a root
## entry. The record holds `(size, map_block)`; the stream's writer appends
## here and then publishes the two words in the record, `MapBlock` before
## `Size` (§6, "Writer Protocol"), through the page first and the file after.
##
## The append is `writeToFile`'s, on a `(size, mapBlock)` pair the caller
## holds: a stream that fits one block is direct (its only data block, tagged
## with `CtfsDirect`); the append that takes it past one block claims its
## level-1 mapping block first, puts the direct block in slot 0, and claims the
## new data blocks in order. Every block number is checked before it becomes a
## byte offset.

import results
import ./types
import ./block_mapping

type
  KStream* = object
    ## A keyed stream's two in-place words, as its writer holds them.
    size*: uint64
    mapBlock*: uint64

proc appendStream*(c: var Ctfs, s: var KStream, data: openArray[byte],
                   label = "keyed stream"): Result[void, string] =
  ## Append `data` to the stream; the caller publishes the new `(size,
  ## mapBlock)` in its record afterwards.
  if data.len == 0:
    return ok()
  let bs = uint64(c.blockSize)
  let rootBlocks = c.rootBlockCount()
  let newSize = s.size + uint64(data.len)
  if s.mapBlock == 0:
    if s.size != 0:
      return err(label & " has size " & $s.size & " and a null MapBlock")
  elif isDirectMapBlock(s.mapBlock):
    let b = directDataBlock(s.mapBlock)
    if b < rootBlocks or b >= c.nextFreeBlock or s.size > bs:
      return err(label & " has a direct block " & $b & " it cannot hold")
  elif s.mapBlock < rootBlocks or s.mapBlock >= c.nextFreeBlock:
    return err(label & " has mapping root block " & $s.mapBlock &
      " outside the allocated blocks")
  if newSize <= bs and (s.mapBlock == 0 or isDirectMapBlock(s.mapBlock)):
    let b = if s.mapBlock == 0: c.allocBlock() else: directDataBlock(s.mapBlock)
    let start = c.blockOffset(b) + int(s.size)
    copyMem(addr c.data[start], unsafeAddr data[0], data.len)
    if c.streaming: c.flushBlockRange(start, data.len)
    s.mapBlock = CtfsDirect or b
    s.size = newSize
    return ok()
  var mapBlock = s.mapBlock
  let transition = s.mapBlock == 0 or isDirectMapBlock(s.mapBlock)
  if transition:
    mapBlock = c.allocBlock()
    c.zeroBlock(mapBlock)
    if s.mapBlock != 0:
      c.writePtr(mapBlock, 0, directDataBlock(s.mapBlock))
  var pos = s.size
  var written = 0
  while written < data.len:
    let idx = pos div bs
    let within = int(pos mod bs)
    var b: uint64
    if within == 0:
      b = c.allocBlock()
      ? c.insertDataBlock(mapBlock, idx, b)
    else:
      b = c.lookupDataBlock(mapBlock, idx)
    if b == 0'u64 or b < rootBlocks or b >= c.nextFreeBlock:
      return err(label & ": block " & $idx & " resolves to " & $b &
        ", which no stream may name")
    let start = c.blockOffset(b) + within
    let k = min(int(bs) - within, data.len - written)
    copyMem(addr c.data[start], unsafeAddr data[written], k)
    if c.streaming: c.flushBlockRange(start, k)
    written += k
    pos += uint64(k)
  if transition and c.streaming:
    c.flushBlock(mapBlock)
  s.mapBlock = mapBlock
  s.size = pos
  ok()
