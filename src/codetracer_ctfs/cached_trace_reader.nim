when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Cached block reader with two cache layers: RAM LRU + .ctp disk.
##
## Reads check RAM first (fast), then disk cache (may trigger a remote
## fetch via the BlockFetcher callback), and promote fetched data to RAM.

import std/options
import results
import ./partial_trace_cache
import ./ram_cache

type
  CachedBlockReader* = object
    diskCache: PartialTraceCache        ## .ctp disk layer
    ramCache: LruCache[uint64, seq[byte]]  ## RAM LRU layer
    blockSize: int

proc initCachedBlockReader*(fetcher: BlockFetcher,
    ramMaxBytes: uint64 = 256 * 1024 * 1024,
    diskMaxBytes: uint64 = 1024 * 1024 * 1024): CachedBlockReader =
  ## Create a two-layer cached block reader.
  ## - `fetcher`: callback for remote block fetching
  ## - `ramMaxBytes`: max RAM cache size (default 256MB)
  ## - `diskMaxBytes`: max disk cache size (default 1GB)
  CachedBlockReader(
    diskCache: initPartialTraceCache(fetcher, maxCacheBytes = diskMaxBytes),
    ramCache: initLruCache[uint64, seq[byte]](maxBytes = ramMaxBytes),
    blockSize: 4096
  )

proc readBlock*(reader: var CachedBlockReader, blockId: uint64): Result[seq[byte], string] =
  ## Read a block through the two-layer cache.
  ## Check RAM cache first, then disk cache, then fetch remotely.

  # 1. RAM hit?
  let ramResult = reader.ramCache.get(blockId)
  if ramResult.isSome:
    return ok(ramResult.get())

  # 2. Disk cache (may fetch remotely)
  let data = ?reader.diskCache.fetchBlock(blockId)

  # 3. Promote to RAM cache
  reader.ramCache.put(blockId, data, uint64(data.len))
  ok(data)

proc ramHitRate*(reader: CachedBlockReader): float =
  ## Current RAM cache hit rate.
  reader.ramCache.hitRate()

proc diskPresenceCount*(reader: CachedBlockReader): int =
  ## Number of blocks present in the disk cache.
  reader.diskCache.presenceCount()

proc ramCacheSize*(reader: CachedBlockReader): uint64 =
  ## Current RAM cache size in bytes.
  reader.ramCache.currentSize()

proc ramCacheCount*(reader: CachedBlockReader): int =
  ## Number of entries in the RAM cache.
  reader.ramCache.count()
