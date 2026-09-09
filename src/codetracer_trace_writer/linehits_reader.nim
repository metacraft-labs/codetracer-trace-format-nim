{.push raises: [].}

## Read side of ``linehits.tc``.
##
## The writer serialises its line-hit index into the container as a `CowBTree`
## whose Type-B descriptors (`[payload_offset:u64][payload_len:u64]`) address
## varint step-id lists appended after the page image — see
## ``linehits_builder.nim`` for the encoder.
##
## This module links no writer. That matters for a read-only embedding: the
## decoder for this file used to exist only as a helper inside the builder, so
## reading a line-hit index meant linking the accumulator, its hash tables and
## the B-tree insert path that produced it.

import results
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ../codetracer_ctfs/cow_btree
import ./varint

export results

const
  LinehitsFileName* = "linehits.tc"
    ## The container-internal file the builder emits. Twelve characters, so it
    ## survives the base40 filename encoding without truncation.

  LinehitsDescriptorSize = 16
    ## `[payload_offset:u64][payload_len:u64]` — the Type-B leaf descriptor.

type
  LinehitsReader* = object
    image: seq[byte]
      ## The whole `linehits.tc` image. The B-tree pages are its prefix and the
      ## step-id payloads its suffix, and a descriptor's offset is relative to
      ## the image, not to the payload region — so the bytes are kept whole.
    tree: CowBTree

proc readU64LE(data: openArray[byte], off: int): uint64 =
  for i in 0 ..< 8:
    result = result or (uint64(data[off + i]) shl (i * 8))

proc hasLinehits*(ctfsBytes: openArray[byte],
    maxEntries: uint32 = DefaultMaxRootEntries): bool =
  ## Whether the container carries a line-hit index at all. A trace whose
  ## recorder never called ``enableLinehits`` has none, which is not an error.
  hasInternalFile(ctfsBytes, LinehitsFileName, maxEntries)

proc initLinehitsReader*(ctfsBytes: openArray[byte],
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries):
    Result[LinehitsReader, string] =
  ## Open the line-hit index. Fails when the container has no ``linehits.tc``
  ## or when the image is not a well-formed namespace B-tree.
  let raw = readInternalFile(ctfsBytes, LinehitsFileName, blockSize, maxEntries)
  if raw.isErr:
    return err("failed to read " & LinehitsFileName & ": " & raw.error)
  let image = raw.get()
  let tree = ?loadCowBTree(image, cltTypeB)
  ok(LinehitsReader(image: image, tree: tree))

proc positionCount*(r: LinehitsReader): uint64 =
  ## Number of distinct positions the index carries a hit list for.
  r.tree.count()

proc positions*(r: LinehitsReader): Result[seq[uint64], string] =
  ## Every indexed position, in B-tree key order.
  r.tree.keys()

proc hits*(r: LinehitsReader,
    positionIndex: uint64): Result[seq[uint64], string] =
  ## The step ids that executed ``positionIndex``, in the order the recorder
  ## observed them. ``err`` when the position carries no hits — the index is
  ## sparse, so absence is the normal answer for an unexecuted line and the
  ## caller decides what it means.
  let desc = ?r.tree.lookup(positionIndex)
  if desc.len != LinehitsDescriptorSize:
    return err("linehits descriptor for position " & $positionIndex &
      " is " & $desc.len & " bytes, expected " & $LinehitsDescriptorSize)
  let off = int(readU64LE(desc, 0))
  let size = int(readU64LE(desc, 8))
  if off < 0 or size < 0 or off > r.image.len or off + size > r.image.len:
    return err("linehits payload for position " & $positionIndex &
      " is out of bounds")
  var pos = off
  let endPos = off + size
  var steps: seq[uint64] = @[]
  while pos < endPos:
    steps.add(?decodeVarint(r.image, pos))
  ok(steps)

{.pop.}
