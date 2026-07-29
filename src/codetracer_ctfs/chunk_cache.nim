when defined(nimPreviewSlimSystem):
  import std/[assertions]

{.push raises: [].}

## Bounded LRU cache of **decompressed** chunk payloads, shared by the chunked
## readers in this package.
##
## Every chunked reader here (``ChunkedCompressedTableReader``,
## ``ExecStreamReader``, …) seeks by decompressing the Zstd frame that holds the
## requested record.  Historically each of them kept exactly *one* decompressed
## chunk, so any access pattern that alternates between chunks — which is what
## a debugger's "jump to step N" does — re-inflated a whole frame per lookup.
## On a 245-chunk table a uniform random read pattern hit that one slot ~0.4% of
## the time; on a 3-chunk step stream it hit it ~33% of the time.  This module
## replaces that single slot with a real LRU so a working set that fits the
## budget is inflated once and then read for free.
##
## Design notes:
##
## * Chunk indices are dense (``0 ..< numChunks``), so the key→slot map is a
##   flat ``seq[int32]`` rather than a ``Table``: O(1), no hashing, no
##   allocation on the hot path.  (``ram_cache.LruCache`` is the general
##   ``Table`` + ``DoublyLinkedList`` cache for sparse keys; it also copies its
##   value out on every hit, which for a 64 KiB chunk payload would reintroduce
##   a large part of the cost this cache exists to remove.)
## * Eviction is by **bytes**, not by entry count, because chunk payloads vary
##   in size between streams.  The budget is a cap, not a reservation: a reader
##   whose whole stream is smaller than the budget never evicts, and a reader
##   with one enormous chunk still keeps that one chunk resident.
## * Slots carry a caller-supplied metadata value ``M`` so a reader can cache
##   per-chunk derived state (record counts, record start offsets) next to the
##   bytes and not recompute it on every hit.
##
## Usage is find / acquire / commit:
##
## ```nim
## var slot = cache.find(chunkIdx)
## if slot < 0:
##   slot = cache.acquire()
##   # … fill cache.data(slot) (and cache.meta(slot)); on failure just return,
##   # the uncommitted slot stays free …
##   cache.commit(slot, chunkIdx)
## ```

const
  DefaultChunkCacheBytes* = 64 * 1024 * 1024
    ## Default byte budget for decompressed chunk payloads of a *random-access
    ## table* reader.  Conservative next to ``ram_cache.initLruCache``'s
    ## 256 MiB default for raw CTFS blocks, and a cap rather than a
    ## reservation — a reader only ever holds the chunks it actually touched.

  DefaultStreamChunkCacheBytes* = 8 * 1024 * 1024
    ## Default budget for the per-kind *stream* readers (steps / values /
    ## calls / events).  Deliberately smaller than the table budget: the
    ## streams are also walked sequentially, by `ct-print` and the bulk FFI
    ## accessors, and an LRU keeps everything it sees until it hits its cap,
    ## so the budget is a direct floor on that walk's peak RSS.  8 MiB still
    ## holds several hundred step chunks, which is far more than any
    ## navigation working set needs.

type
  ChunkCacheSlot[M] = object
    key: int          ## chunk index held by this slot; -1 when free
    data: seq[byte]
    meta: M
    prev, next: int32 ## intrusive LRU links (slot indices); -1 = none

  ChunkCache*[M] = object
    slots: seq[ChunkCacheSlot[M]]
    index: seq[int32]  ## chunk index -> slot index, -1 when not resident
    head, tail: int32  ## MRU / LRU ends of the intrusive list; -1 = empty
    free: seq[int32]
      ## Slots that have been evicted and unlinked.  They still own their
      ## payload buffer, so reusing one avoids a fresh allocation — and, more
      ## importantly, without this list an evicted slot would be reachable from
      ## neither ``index`` nor the LRU list and its buffer would be held for
      ## the cache's whole lifetime, silently breaking the byte budget.
    maxBytes: uint64
    currentBytes: uint64
    hits*: uint64
    misses*: uint64

proc initChunkCache*[M](numChunks: int,
    maxBytes: uint64 = DefaultChunkCacheBytes): ChunkCache[M] =
  ## Create a cache able to hold up to ``maxBytes`` of decompressed payload for
  ## a stream of ``numChunks`` chunks.
  var c = ChunkCache[M](
    slots: @[],
    index: newSeq[int32](max(numChunks, 0)),
    head: -1,
    tail: -1,
    free: @[],
    maxBytes: max(maxBytes, 1'u64),
    currentBytes: 0,
    hits: 0,
    misses: 0,
  )
  for i in 0 ..< c.index.len:
    c.index[i] = -1
  c

# ---------------------------------------------------------------------------
# Intrusive LRU list helpers
# ---------------------------------------------------------------------------

proc unlink[M](c: var ChunkCache[M], s: int32) =
  let prev = c.slots[s].prev
  let next = c.slots[s].next
  if prev >= 0: c.slots[prev].next = next else: c.head = next
  if next >= 0: c.slots[next].prev = prev else: c.tail = prev
  c.slots[s].prev = -1
  c.slots[s].next = -1

proc linkFront[M](c: var ChunkCache[M], s: int32) =
  c.slots[s].prev = -1
  c.slots[s].next = c.head
  if c.head >= 0: c.slots[c.head].prev = s
  c.head = s
  if c.tail < 0: c.tail = s

proc releaseSlot[M](c: var ChunkCache[M], s: int32) =
  ## Drop whatever ``s`` currently holds (bytes, key registration) without
  ## unlinking it from the LRU list.
  let key = c.slots[s].key
  if key >= 0:
    if key < c.index.len and c.index[key] == s:
      c.index[key] = -1
    c.currentBytes -= min(c.currentBytes, uint64(c.slots[s].data.len))
    c.slots[s].key = -1

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

proc find*[M](c: var ChunkCache[M], key: int): int =
  ## Slot index holding ``key``, or -1 on a miss.  A hit is promoted to MRU.
  if key < 0 or key >= c.index.len:
    c.misses += 1
    return -1
  let s = c.index[key]
  if s < 0:
    c.misses += 1
    return -1
  c.hits += 1
  if c.head != s:
    c.unlink(s)
    c.linkFront(s)
  int(s)

proc acquire*[M](c: var ChunkCache[M]): int =
  ## Reserve a slot for a caller that just missed.  The slot is empty and is
  ## **not** yet reachable through ``find`` — call [commit] once it is filled.
  ## Until then it is the LRU victim, so an aborted fill costs nothing.
  var s: int32
  if c.free.len > 0:
    # An already-evicted slot: it owns a buffer of roughly the right size.
    s = c.free.pop()
  elif (c.currentBytes >= c.maxBytes or c.slots.len >= c.index.len) and c.tail >= 0:
    # At budget (or already holding one slot per chunk): recycle the least
    # recently used slot instead of growing.
    s = c.tail
    c.releaseSlot(s)
    c.unlink(s)
  else:
    s = int32(c.slots.len)
    c.slots.add(ChunkCacheSlot[M](key: -1, data: @[], prev: -1, next: -1))
  c.slots[s].data.setLen(0)
  c.slots[s].meta = default(M)
  c.slots[s].key = -1
  # Uncommitted slots sit at the LRU end so a failed fill is reclaimed first.
  c.slots[s].prev = c.tail
  c.slots[s].next = -1
  if c.tail >= 0: c.slots[c.tail].next = s
  c.tail = s
  if c.head < 0: c.head = s
  int(s)

proc commit*[M](c: var ChunkCache[M], slot: int, key: int) =
  ## Publish a filled slot under ``key`` and evict, LRU first, until the cache
  ## is back inside its byte budget.  The just-committed slot is never evicted.
  let s = int32(slot)
  if key < 0 or key >= c.index.len:
    return
  # A stale resident copy of the same key must go (readers never write two
  # different payloads for one chunk index, but be defensive: a duplicate would
  # leak bytes out of the accounting).
  let existing = c.index[key]
  if existing >= 0 and existing != s:
    c.releaseSlot(existing)
    c.unlink(existing)
    c.free.add(existing)
  c.slots[s].key = key
  c.index[key] = s
  c.currentBytes += uint64(c.slots[s].data.len)
  c.unlink(s)
  c.linkFront(s)
  while c.currentBytes > c.maxBytes and c.tail >= 0 and c.tail != s:
    let victim = c.tail
    c.releaseSlot(victim)
    c.unlink(victim)
    # Evicted slots keep their buffer and go on the free list so the next
    # `acquire` reuses them; dropping them here would hold their memory
    # forever with nothing able to reach it.
    c.free.add(victim)

proc prepare*[M](c: var ChunkCache[M], slot: int, size: int) =
  ## Size ``slot``'s payload buffer to ``size`` **without** zero-filling it.
  ##
  ## Callers immediately overwrite the whole buffer with a Zstd frame, so the
  ## zero-fill ``setLen`` performs is pure waste — and it is not cheap: it is a
  ## per-element loop, so in a debug build inflating a 64 KiB chunk into a
  ## fresh slot spent more time zeroing the destination than decompressing into
  ## it.  (The old single-slot readers hid this: the one buffer reached its
  ## final length on the first chunk and every later ``setLen`` was a no-op.)
  when compiles(c.slots[slot].data.setLenUninit(size)):
    c.slots[slot].data.setLenUninit(size)
  else:
    c.slots[slot].data.setLen(size)

proc data*[M](c: var ChunkCache[M], slot: int): var seq[byte] =
  ## Mutable payload buffer of ``slot``.
  c.slots[slot].data

proc meta*[M](c: var ChunkCache[M], slot: int): var M =
  ## Mutable metadata of ``slot``.
  c.slots[slot].meta

proc clear*[M](c: var ChunkCache[M]) =
  ## Drop every resident chunk (keeps the configured budget).
  for i in 0 ..< c.index.len:
    c.index[i] = -1
  c.slots.setLen(0)
  c.free.setLen(0)
  c.head = -1
  c.tail = -1
  c.currentBytes = 0

proc residentChunks*[M](c: ChunkCache[M]): int =
  ## Number of chunks currently resident (committed slots).
  for s in c.slots:
    if s.key >= 0: result += 1

proc slotCount*[M](c: ChunkCache[M]): int = c.slots.len
  ## Total slots the cache has ever allocated — resident, plus evicted ones
  ## held on the free list for reuse.  Every slot owns a payload buffer, so
  ## this, not [residentChunks], is what bounds the cache's real memory. It
  ## must stay proportional to the byte budget however long the cache runs.

proc residentBytes*[M](c: ChunkCache[M]): uint64 = c.currentBytes

proc budgetBytes*[M](c: ChunkCache[M]): uint64 = c.maxBytes

proc hitRate*[M](c: ChunkCache[M]): float =
  let total = c.hits + c.misses
  if total == 0: 0.0 else: float(c.hits) / float(total)
