when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Generic LRU cache backed by a fixed-capacity table with size-based eviction.
##
## Uses a DoublyLinkedList for O(1) promote/evict and a Table for O(1) lookup.
## Eviction is based on total byte size, not entry count.
##
## Two accessors, and the difference is not stylistic:
##
## * `tryGet` is the **hit path**.  It returns a borrowed ``ptr V`` into the
##   entry the cache already owns, so a hit costs a hash lookup and two
##   linked-list pointer swaps and nothing else — no allocation, no copy,
##   regardless of how large ``V`` is.
## * `get` is the **owning** accessor.  It returns ``Option[V]``, which means it
##   allocates and copies the value out on every hit.  For the 4 KiB CTFS
##   blocks this cache actually holds, that copy was ~95% of the measured cost
##   of a hit (see `tests/test_ram_cache.nim`), so `get` is for callers that
##   genuinely need to own the value past the next cache mutation, not for
##   anyone reading through the cache in a loop.
##
## The borrow rule for `tryGet` is the ordinary one: the pointer is valid until
## the next mutation of the cache (`put`, `clear`, or the eviction `put`
## triggers).  Nothing in this package holds one across a `put`.

import std/[tables, lists, options]

type
  LruEntry[K, V] = tuple[key: K, value: V, size: uint64]

  LruCache*[K, V] = object
    maxBytes: uint64
    currentBytes: uint64
    table: Table[K, DoublyLinkedNode[LruEntry[K, V]]]
    order: DoublyLinkedList[LruEntry[K, V]]
    hits*: uint64
    misses*: uint64

proc initLruCache*[K, V](maxBytes: uint64 = 256 * 1024 * 1024): LruCache[K, V] =
  ## Create a new LRU cache with the given maximum byte capacity.
  LruCache[K, V](
    maxBytes: maxBytes,
    currentBytes: 0,
    table: initTable[K, DoublyLinkedNode[LruEntry[K, V]]](),
    order: initDoublyLinkedList[LruEntry[K, V]](),
    hits: 0,
    misses: 0
  )

proc tryGet*[K, V](cache: var LruCache[K, V], key: K): ptr V =
  ## Non-copying accessor — **the hit path**.
  ##
  ## Returns a borrowed pointer to the value the cache already owns on a hit,
  ## and ``nil`` on a miss.  Promotes to MRU, exactly as `get` does, and
  ## maintains the same hit/miss counters.
  ##
  ## The pointer aliases the cache's own storage: it stays valid until the next
  ## `put` or `clear` on this cache (a `put` may evict the very entry that was
  ## returned).  Read through it, or copy out of it, before mutating the cache.
  ##
  ## `getOrDefault` rather than ``key in table`` + ``table[key]`` is deliberate:
  ## it is one lookup instead of two, and the entry type is a ref so the "not
  ## found" default is ``nil``, which needs no ``KeyError`` handler.
  let node = cache.table.getOrDefault(key)
  if node.isNil:
    cache.misses += 1
    return nil
  # Promote to MRU (head of list)
  cache.order.remove(node)
  cache.order.prepend(node)
  cache.hits += 1
  addr node.value.value

proc get*[K, V](cache: var LruCache[K, V], key: K): Option[V] =
  ## Owning accessor. Returns none() on miss, some() on hit. Promotes to MRU.
  ##
  ## This **copies** the value out — for a 4 KiB block that is an allocation
  ## plus a 4 KiB memcpy plus a free, per call.  Prefer `tryGet` unless the
  ## caller has to keep the value past the cache's next mutation.
  let hit = cache.tryGet(key)
  if hit.isNil:
    none(V)
  else:
    some(hit[])

proc evictLru[K, V](cache: var LruCache[K, V]) =
  ## Evict the least recently used entry (tail of list).
  let tail = cache.order.tail
  if tail != nil:
    cache.order.remove(tail)
    cache.currentBytes -= tail.value.size
    try:
      cache.table.del(tail.value.key)
    except KeyError:
      discard

proc put*[K, V](cache: var LruCache[K, V], key: K, value: V, size: uint64) =
  ## Insert a value. Evicts LRU entries if needed to stay under maxBytes.
  try:
    # If key already exists, remove the old entry first
    if key in cache.table:
      let oldNode = cache.table[key]
      cache.currentBytes -= oldNode.value.size
      cache.order.remove(oldNode)
      cache.table.del(key)
  except KeyError:
    discard

  # Evict until we have room (or cache is empty)
  while cache.currentBytes + size > cache.maxBytes and cache.order.head != nil:
    cache.evictLru()

  # Insert new entry at head (MRU position)
  let entry: LruEntry[K, V] = (key: key, value: value, size: size)
  let node = newDoublyLinkedNode(entry)
  cache.order.prepend(node)
  cache.table[key] = node
  cache.currentBytes += size

proc contains*[K, V](cache: LruCache[K, V], key: K): bool =
  ## Check if a key is in the cache without affecting LRU order.
  key in cache.table

proc clear*[K, V](cache: var LruCache[K, V]) =
  ## Remove all entries from the cache.
  cache.table.clear()
  cache.order = initDoublyLinkedList[LruEntry[K, V]]()
  cache.currentBytes = 0

proc currentSize*[K, V](cache: LruCache[K, V]): uint64 =
  ## Current total size of cached entries in bytes.
  cache.currentBytes

proc count*[K, V](cache: LruCache[K, V]): int =
  ## Number of entries in the cache.
  cache.table.len

proc hitRate*[K, V](cache: LruCache[K, V]): float =
  ## Hit rate as a fraction [0.0, 1.0]. Returns 0.0 if no accesses yet.
  let total = cache.hits + cache.misses
  if total == 0:
    0.0
  else:
    float(cache.hits) / float(total)
