{.push raises: [].}

## Linehits builder: accumulates (globalLineIndex -> stepId) mappings
## during recording, then writes them to a namespace at finalize time.
##
## Each namespace entry is a concatenation of varint-encoded step_ids
## for a given global line index.

import std/[algorithm, tables]
import results
import ../codetracer_ctfs/cow_btree
import ./varint

export results

type
  LinehitsBuilder* = object
    ## Accumulated step_ids per global line index.
    ## Each value is a growing buffer of varint-encoded step_ids.
    pending: Table[uint64, seq[byte]]
    finalizedHits: Table[uint64, seq[byte]]
    cowImage: seq[byte]
    finalized: bool

proc initLinehitsBuilder*(): LinehitsBuilder =
  LinehitsBuilder(
    pending: initTable[uint64, seq[byte]](),
    finalizedHits: initTable[uint64, seq[byte]](),
    cowImage: @[],
    finalized: false,
  )

proc putU64LE(dst: var seq[byte], v: uint64) =
  for i in 0 ..< 8:
    dst.add(byte((v shr (i * 8)) and 0xFF))

proc readU64LE(data: openArray[byte], off: int): uint64 =
  for i in 0 ..< 8:
    result = result or (uint64(data[off + i]) shl (i * 8))

proc descriptor(offset, size: uint64): seq[byte] =
  result = @[]
  result.putU64LE(offset)
  result.putU64LE(size)

proc buildCowImage(entries: seq[(uint64, seq[byte])]): Result[seq[byte], string] =
  ## Build a production CoW namespace image for linehits.
  ##
  ## The `CowBTree` stores a Type-B descriptor per global-line key:
  ## `[payload_offset:u64][payload_len:u64]`. The varint-encoded step-id lists
  ## live in page-aligned payload bytes appended after the B-tree page image.
  ## Rust opens the index through `CowNamespaceReader`, then resolves the
  ## descriptor into the payload region.
  var sizingTree = initCowBTree(cltTypeB, skipSubBlocks = true)
  let zeroDesc = descriptor(0, 0)
  for (key, _) in entries:
    discard ?sizingTree.insertAndCommit(key, zeroDesc)
  let payloadBase = uint64(sizingTree.serialize().len)

  var payload: seq[byte] = @[]
  var finalTree = initCowBTree(cltTypeB, skipSubBlocks = true)
  for (key, data) in entries:
    let off = payloadBase + uint64(payload.len)
    payload.add(data)
    discard ?finalTree.insertAndCommit(key, descriptor(off, uint64(data.len)))

  var image = finalTree.serialize()
  image.add(payload)
  while image.len mod PageSize != 0:
    image.add(0)
  ok(image)

proc recordHit*(b: var LinehitsBuilder, globalLineIndex: uint64,
    stepId: uint64) =
  ## Record that step `stepId` executed source line `globalLineIndex`.
  var buf = addr b.pending.mgetOrPut(globalLineIndex, newSeq[byte]())
  encodeVarint(stepId, buf[])

proc finalize*(b: var LinehitsBuilder): Result[void, string] =
  ## Flush all pending entries into the namespace.
  ## Must be called exactly once before any lookups.
  if b.finalized:
    return err("linehits builder already finalized")
  var keys: seq[uint64]
  for key in b.pending.keys:
    keys.add(key)
  keys.sort()

  var entries: seq[(uint64, seq[byte])]
  for key in keys:
    let data = b.pending.getOrDefault(key)
    b.finalizedHits[key] = data
    entries.add((key, data))

  b.cowImage = ?buildCowImage(entries)
  b.finalized = true
  ok()

proc serializeCowNamespace*(b: LinehitsBuilder): Result[seq[byte], string] =
  ## Return the finalized CoW namespace image written as `linehits.tc`.
  if not b.finalized:
    return err("linehits builder not finalized")
  ok(b.cowImage)

proc lookupHits*(b: LinehitsBuilder,
    globalLineIndex: uint64): Result[seq[uint64], string] =
  ## Query: return all step_ids that hit the given global line index.
  ## Only valid after finalize().
  if not b.finalized:
    return err("linehits builder not finalized")
  if not b.finalizedHits.hasKey(globalLineIndex):
    return err("key not found")
  let data = b.finalizedHits.getOrDefault(globalLineIndex)
  var stepIds: seq[uint64]
  var pos = 0
  while pos < data.len:
    let v = ?decodeVarint(data, pos)
    stepIds.add(v)
  ok(stepIds)

proc hitCount*(b: LinehitsBuilder, globalLineIndex: uint64): int =
  ## Count hits for a line without fully decoding all step_ids.
  ## Returns 0 if not finalized or key not found.
  if not b.finalized:
    return 0
  if not b.finalizedHits.hasKey(globalLineIndex):
    return 0
  let data = b.finalizedHits.getOrDefault(globalLineIndex)
  var count = 0
  var pos = 0
  while pos < data.len:
    let v = decodeVarint(data, pos)
    if v.isErr:
      break
    count += 1
  count

proc lineCount*(b: LinehitsBuilder): int =
  ## Number of distinct lines that have been hit.
  b.pending.len

proc decodeCowLinehitsPayloadForTest*(image: openArray[byte],
    globalLineIndex: uint64): Result[seq[uint64], string] =
  ## Test helper used by the Nim suite to prove the production writer emitted a
  ## real `CowBTree` image, not the legacy whole-tree namespace blob.
  let loaded = ?loadCowBTree(image, cltTypeB)
  let desc = ?loaded.lookup(globalLineIndex)
  if desc.len != 16:
    return err("bad linehits descriptor size")
  let off = int(readU64LE(desc, 0))
  let size = int(readU64LE(desc, 8))
  if off < 0 or size < 0 or off + size > image.len:
    return err("linehits payload out of bounds")
  var pos = off
  let endPos = off + size
  var hits: seq[uint64]
  while pos < endPos:
    hits.add(?decodeVarint(image, pos))
  ok(hits)
