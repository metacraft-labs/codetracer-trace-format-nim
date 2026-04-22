when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Generic LRU cache backed by a fixed-capacity table with size-based eviction.
##
## Uses a DoublyLinkedList for O(1) promote/evict and a Table for O(1) lookup.
## Eviction is based on total byte size, not entry count.

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

proc get*[K, V](cache: var LruCache[K, V], key: K): Option[V] =
  ## Get a value. Returns none() on miss, some() on hit. Promotes to MRU.
  try:
    if key in cache.table:
      let node = cache.table[key]
      # Promote to MRU (head of list)
      cache.order.remove(node)
      cache.order.prepend(node)
      cache.hits += 1
      return some(node.value.value)
    else:
      cache.misses += 1
      return none(V)
  except KeyError:
    cache.misses += 1
    return none(V)

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
