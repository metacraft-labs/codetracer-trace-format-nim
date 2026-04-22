when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Partial trace cache (.ctp) — local block-level caching for remote traces.
##
## Blocks are fetched on demand via a callback-based fetcher interface.
## The cache tracks which blocks are present locally and which are
## permanent (exempt from eviction). Presence and permanent sets are
## serialized as sorted u64 arrays for on-disk persistence.

import results
import std/[algorithm, sets, strutils, tables]

type
  BlockFetcher* = proc(blockId: uint64): Result[seq[byte], string] {.closure, raises: [].}
    ## Callback to fetch a block from the remote source.
    ## Returns blockSize bytes (one block) or an error.

  PartialTraceCache* = object
    blocks: Table[uint64, seq[byte]]  ## In-memory block storage
    presence: HashSet[uint64]          ## Block numbers we have locally
    permanent: HashSet[uint64]         ## Block numbers exempt from eviction
    fetcher: BlockFetcher              ## How to get missing blocks
    maxCacheBytes*: uint64             ## Max cache size before eviction
    blockSize*: int

proc initPartialTraceCache*(fetcher: BlockFetcher,
    maxCacheBytes: uint64 = 256 * 1024 * 1024,
    blockSize: int = 4096): PartialTraceCache =
  ## Create a new partial trace cache with the given fetcher callback.
  PartialTraceCache(
    blocks: initTable[uint64, seq[byte]](),
    presence: initHashSet[uint64](),
    permanent: initHashSet[uint64](),
    fetcher: fetcher,
    maxCacheBytes: maxCacheBytes,
    blockSize: blockSize
  )

proc hasBlock*(cache: PartialTraceCache, blockId: uint64): bool =
  ## Check whether a block is present in the local cache.
  blockId in cache.presence

proc readLocalBlock(cache: PartialTraceCache, blockId: uint64): Result[seq[byte], string] =
  ## Read a block from the in-memory store.
  try:
    if blockId in cache.blocks:
      ok(cache.blocks[blockId])
    else:
      err("block " & $blockId & " not in local store")
  except KeyError:
    err("block " & $blockId & " not in local store")

proc storeBlock(cache: var PartialTraceCache, blockId: uint64,
                data: seq[byte]): Result[void, string] =
  ## Store a block in the in-memory store.
  cache.blocks[blockId] = data
  ok()

proc removeBlock(cache: var PartialTraceCache, blockId: uint64) =
  ## Remove a block from the in-memory store.
  cache.blocks.del(blockId)

proc fetchBlock*(cache: var PartialTraceCache, blockId: uint64): Result[seq[byte], string] =
  ## Get a block. Checks local cache first, fetches remotely if missing.
  if blockId in cache.presence:
    return cache.readLocalBlock(blockId)

  # Fetch from remote
  let data = ?cache.fetcher(blockId)
  if data.len != cache.blockSize:
    return err("fetched block has wrong size: " & $data.len &
      ", expected: " & $cache.blockSize)

  # Store locally
  ?cache.storeBlock(blockId, data)
  cache.presence.incl(blockId)
  ok(data)

proc markPermanent*(cache: var PartialTraceCache, blockId: uint64) =
  ## Mark a block as permanent (won't be evicted).
  cache.permanent.incl(blockId)

proc evict*(cache: var PartialTraceCache, targetBytes: uint64): int =
  ## Evict evictable blocks until cache is under targetBytes.
  ## Returns number of blocks evicted.
  var evicted = 0
  var toEvict: seq[uint64]
  for blockId in cache.presence:
    if blockId notin cache.permanent:
      toEvict.add(blockId)

  for blockId in toEvict:
    if uint64(cache.presence.len) * uint64(cache.blockSize) <= targetBytes:
      break
    cache.presence.excl(blockId)
    cache.removeBlock(blockId)
    evicted += 1
  evicted

proc presenceCount*(cache: PartialTraceCache): int =
  ## Number of blocks currently in the cache.
  cache.presence.len

proc permanentCount*(cache: PartialTraceCache): int =
  ## Number of blocks marked as permanent.
  cache.permanent.len

# --- Presence / permanent index serialization ---------------------------------

proc serializePresenceIdx*(cache: PartialTraceCache): seq[byte] =
  ## Serialize presence.idx: sorted array of u64 block numbers (little-endian).
  var sorted = newSeq[uint64]()
  for blockId in cache.presence:
    sorted.add(blockId)
  sorted.sort()
  var res = newSeq[byte](sorted.len * 8)
  for i, blockId in sorted:
    for j in 0 ..< 8:
      res[i * 8 + j] = byte((blockId shr (j * 8)) and 0xFF)
  res

proc serializePermanentIdx*(cache: PartialTraceCache): seq[byte] =
  ## Serialize permanent.idx: sorted array of u64 block numbers (little-endian).
  var sorted = newSeq[uint64]()
  for blockId in cache.permanent:
    sorted.add(blockId)
  sorted.sort()
  var res = newSeq[byte](sorted.len * 8)
  for i, blockId in sorted:
    for j in 0 ..< 8:
      res[i * 8 + j] = byte((blockId shr (j * 8)) and 0xFF)
  res

proc loadPresenceIdx*(data: openArray[byte]): HashSet[uint64] =
  ## Deserialize a presence/permanent index from a sorted u64 LE array.
  var res = initHashSet[uint64]()
  var pos = 0
  while pos + 8 <= data.len:
    var blockId: uint64 = 0
    for j in 0 ..< 8:
      blockId = blockId or (uint64(data[pos + j]) shl (j * 8))
    res.incl(blockId)
    pos += 8
  res

# --- Name-based classification -----------------------------------------------

proc isPermanentFileName*(name: string): bool =
  ## Files that should be permanently cached (not evicted).
  ## Includes offset indices, companion indices, metadata, and namespace files.
  name.endsWith(".off") or     # offset indices
  name.endsWith(".idx") or     # companion indices
  name == "meta.dat" or        # metadata
  name.endsWith(".tc") or      # namespace files
  name.endsWith(".ns")         # namespace files

proc isEvictableFileName*(name: string): bool =
  ## Files that can be evicted from the cache when space is needed.
  not isPermanentFileName(name)
