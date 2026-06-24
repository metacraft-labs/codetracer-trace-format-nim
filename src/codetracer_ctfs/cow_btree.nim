{.push raises: [].}

## Copy-on-write, crash-safe, block-backed namespace B-tree (LMDB-style).
##
## This is the M3 writer for namespace B-tree key indexes. Unlike the legacy
## in-memory `btree.nim` (which re-serialises the WHOLE tree on every flush, so
## superseded index pages simply vanish with the old serialisation), this store
## keeps the B-tree as fixed-size **pages** in a single block-addressed buffer
## and updates it **incrementally and copy-on-write**:
##
## - **Path-copying CoW** (CTFS-Binary-Format.md §10 "Copy-on-Write B-Tree
##   Reclamation"): to modify a node we pop a fresh page from the unified
##   free-list (else bump-allocate), copy the node's bytes into it, apply the
##   change there, and copy-up the parent chain to a brand-new root. Reachable
##   index pages are NEVER mutated in place.
##
## - **Double-buffered atomic root commit**: the `NamespaceHeader` carries two
##   root slots `root_block[2]` paired with monotonically increasing
##   `commit_id[2]` (§10 / the NamespaceHeader). A commit writes the new root
##   into the slot NOT currently in use, behind a write barrier that mirrors the
##   §6 `FileEntry.Size` writer protocol (data pages durable → barrier → publish
##   the root). The committed root is the valid slot with the highest commit id;
##   a torn root write tears at most the slot being written, so a crash before
##   the flip leaves the previous tree intact.
##
## - **Unified free-list with a whole-block size class** (§1 / §10): superseded
##   full-block pages are reclaimed through the SAME free-list abstraction the
##   sub-block pools use — a whole-block class whose unit is one entire page, an
##   in-page next-pointer chain rooted in the header. NOT a separate B-tree
##   free-list.
##
## - **MVCC reader-gated reclamation**: a superseded page is "pending free"
##   tagged with the commit id that freed it, and returns to the whole-block
##   free-list only once no active reader holds a snapshot (root) older than that
##   id. The writer tracks the oldest active reader's commit id via a reader
##   table (LMDB's oldest-txnid lower bound).
##
## The page buffer this store maintains is the on-disk byte image of the
## namespace file: a Rust reader (see the db-backend `cow_namespace_reader`)
## reads the NamespaceHeader, selects the highest-valid-commit-id root, and
## traverses the immutable page graph. The byte layout is therefore part of the
## wire format and is documented inline below.

import results
export results

const
  PageSize* = 4096
    ## One B-tree page == one CTFS block.

  # ── NamespaceHeader (page 0) ────────────────────────────────────────────
  # The header lives at the start of page 0 (the namespace file's first
  # block). It is the §10 NamespaceHeader extended with the free-list root
  # and the next-free-page bump pointer so the page store is fully
  # self-describing on disk:
  #
  #   [0..3]    magic "NSB1"            (namespace B-tree, format 1)
  #   [4..11]   root_block[0]  (u64 LE) double-buffered B-tree root slot 0
  #   [12..19]  root_block[1]  (u64 LE) double-buffered B-tree root slot 1
  #   [20..27]  commit_id[0]   (u64 LE) commit tag for slot 0 (0 = empty slot)
  #   [28..35]  commit_id[1]   (u64 LE) commit tag for slot 1
  #   [36]      flags          (u8)     bit0 leaf_type; bit1 skip_sub_blocks
  #   [37..44]  free_list_head (u64 LE) head page of the whole-block free chain
  #                                     (0 = empty)
  #   [45..52]  next_free_page (u64 LE) bump-allocation cursor (first never-used
  #                                     page number)
  #   [53..60]  page_count     (u64 LE) total pages currently in the buffer
  #
  # Page 0 is reserved for the header and is never a B-tree node or a free
  # page. Page numbers in root_block / child pointers are 1-based indices into
  # the page buffer; 0 means "none".
  HdrMagic0 = byte('N')
  HdrMagic1 = byte('S')
  HdrMagic2 = byte('B')
  HdrMagic3 = byte('1')

  OffRoot0 = 4
  OffRoot1 = 12
  OffCommit0 = 20
  OffCommit1 = 28
  OffFlags = 36
  OffFreeHead = 37
  OffNextFree = 45
  OffPageCount = 53
  HeaderTotal = 61

  # ── B-tree node page layout ─────────────────────────────────────────────
  # Each node occupies one PageSize page:
  #   [0]       node_kind (0 = internal, 1 = leaf)
  #   [1]       reserved (0)
  #   [2..3]    count (u16 LE) — number of keys
  #   [4..7]    reserved (0)
  #   [8..]     payload
  #
  # Leaf payload:   [keys: count*8] [descriptors: count*descriptorSize]
  # Internal payload: [keys: count*8] [children: (count+1)*8 (u64 LE page nums)]
  NodeHeaderBytes = 8
  KindInternal = 0'u8
  KindLeaf = 1'u8
    ## A free whole-block page stores the next free page number (u64 LE) in its
    ## first 8 bytes — the in-page next-pointer chain (§1/§10), identical in
    ## spirit to the sub-block free chains but the unit is a whole page.

type
  CowLeafType* = enum
    cltTypeA = 0  ## 8-byte descriptors (many small entries)
    cltTypeB = 1  ## 16-byte descriptors (fewer large entries)

  PendingFree = object
    ## A superseded page held back from reuse until the oldest reader has
    ## advanced past `freedAt`.
    page: uint64
    freedAt: uint64  ## commit id at which the page became unreachable

  ReaderHandle* = object
    ## A pinned read snapshot. Holds the root page + commit id observed when the
    ## reader started; the writer never mutates a page reachable from it.
    active: bool
    slot: int
    rootPage*: uint64
    commitId*: uint64

  CowBTree* = object
    ## Block-backed copy-on-write namespace B-tree page store.
    descriptorSize*: int          ## 8 (Type A) or 16 (Type B)
    leafType*: CowLeafType
    skipSubBlocks*: bool
    order*: int                   ## max keys per node before split
    pages: seq[byte]              ## flat page buffer; page N at N*PageSize
    pageCount: uint64             ## number of pages currently allocated in the buffer
    nextFreePage: uint64          ## bump cursor (first never-used page)
    freeListHead: uint64          ## head of the whole-block free chain (0 = none)
    root0, root1: uint64          ## double-buffered root slots
    commit0, commit1: uint64      ## commit ids per slot
    lastCommit: uint64            ## highest commit id issued so far
    count: uint64                 ## number of live keys
    pendingFree: seq[PendingFree] ## superseded pages awaiting reader-gated reuse
    readers: seq[ReaderHandle]    ## active read snapshots (reader table)

# ---------------------------------------------------------------------------
# Little-endian byte helpers
# ---------------------------------------------------------------------------

proc wU16(buf: var seq[byte], off: int, v: uint16) {.inline.} =
  buf[off] = byte(v and 0xFF)
  buf[off + 1] = byte((v shr 8) and 0xFF)

proc rU16(buf: openArray[byte], off: int): uint16 {.inline.} =
  uint16(buf[off]) or (uint16(buf[off + 1]) shl 8)

proc wU64(buf: var seq[byte], off: int, v: uint64) {.inline.} =
  for i in 0 ..< 8:
    buf[off + i] = byte((v shr (i * 8)) and 0xFF)

proc rU64(buf: openArray[byte], off: int): uint64 {.inline.} =
  for i in 0 ..< 8:
    result = result or (uint64(buf[off + i]) shl (i * 8))

# ---------------------------------------------------------------------------
# Header (page 0) accessors — kept in sync between the struct and the on-disk
# image so the buffer is always a faithful wire image.
# ---------------------------------------------------------------------------

proc writeHeader(t: var CowBTree) =
  ## Re-serialise the NamespaceHeader into page 0. Called after any root /
  ## free-list / allocation-cursor change so the buffer image is canonical.
  t.pages[0] = HdrMagic0
  t.pages[1] = HdrMagic1
  t.pages[2] = HdrMagic2
  t.pages[3] = HdrMagic3
  wU64(t.pages, OffRoot0, t.root0)
  wU64(t.pages, OffRoot1, t.root1)
  wU64(t.pages, OffCommit0, t.commit0)
  wU64(t.pages, OffCommit1, t.commit1)
  var flags: uint8 = uint8(t.leafType)
  if t.skipSubBlocks:
    flags = flags or 0b10
  t.pages[OffFlags] = flags
  wU64(t.pages, OffFreeHead, t.freeListHead)
  wU64(t.pages, OffNextFree, t.nextFreePage)
  wU64(t.pages, OffPageCount, t.pageCount)

proc pageBase(page: uint64): int {.inline.} =
  int(page) * PageSize

# ---------------------------------------------------------------------------
# Page allocation — the unified free-list (whole-block size class) with
# bump-allocation fallback (§10 step 1 of path-copying).
# ---------------------------------------------------------------------------

proc ensureCapacity(t: var CowBTree, page: uint64) =
  ## Grow the backing buffer so `page` is addressable, zero-filling new pages.
  let needed = (int(page) + 1) * PageSize
  if t.pages.len < needed:
    let oldLen = t.pages.len
    t.pages.setLen(needed)
    for i in oldLen ..< needed:
      t.pages[i] = 0

proc popFreePage(t: var CowBTree): uint64 =
  ## Pop a page off the whole-block free list, or 0 if the list is empty.
  ## The in-page next pointer (first 8 bytes) names the successor (§10).
  if t.freeListHead == 0:
    return 0
  let page = t.freeListHead
  let base = pageBase(page)
  let nextPage = rU64(t.pages, base)
  t.freeListHead = nextPage
  page

proc pushFreePage(t: var CowBTree, page: uint64) =
  ## Push a page onto the whole-block free list. The freed page's first 8 bytes
  ## become the next-pointer to the old head (in-page free chain).
  let base = pageBase(page)
  # Zero the page so a reused page never leaks stale node bytes, then write the
  # next pointer.
  for i in 0 ..< PageSize:
    t.pages[base + i] = 0
  wU64(t.pages, base, t.freeListHead)
  t.freeListHead = page

proc allocPage(t: var CowBTree): uint64 =
  ## Allocate a fresh page: pop the unified free-list first, else bump-allocate
  ## via the next-free cursor (§10: "popped from the unified free list … or
  ## bump-allocated via NextFreeBlock when the free list is empty").
  let reused = t.popFreePage()
  if reused != 0:
    let base = pageBase(reused)
    for i in 0 ..< PageSize:
      t.pages[base + i] = 0
    return reused
  let page = t.nextFreePage
  t.nextFreePage += 1
  if page >= t.pageCount:
    t.pageCount = page + 1
  t.ensureCapacity(page)
  let base = pageBase(page)
  for i in 0 ..< PageSize:
    t.pages[base + i] = 0
  page

# ---------------------------------------------------------------------------
# Node page read/write helpers
# ---------------------------------------------------------------------------

proc nodeKind(t: CowBTree, page: uint64): uint8 {.inline.} =
  t.pages[pageBase(page)]

proc nodeIsLeaf(t: CowBTree, page: uint64): bool {.inline.} =
  t.nodeKind(page) == KindLeaf

proc nodeCount(t: CowBTree, page: uint64): int {.inline.} =
  int(rU16(t.pages, pageBase(page) + 2))

proc setNodeHeader(t: var CowBTree, page: uint64, isLeaf: bool, count: int) =
  let base = pageBase(page)
  t.pages[base] = if isLeaf: KindLeaf else: KindInternal
  t.pages[base + 1] = 0
  wU16(t.pages, base + 2, uint16(count))
  for i in 4 ..< 8:
    t.pages[base + i] = 0

proc nodeKey(t: CowBTree, page: uint64, i: int): uint64 {.inline.} =
  rU64(t.pages, pageBase(page) + NodeHeaderBytes + i * 8)

proc leafDescOffset(t: CowBTree, page: uint64, count, i: int): int {.inline.} =
  pageBase(page) + NodeHeaderBytes + count * 8 + i * t.descriptorSize

proc childPtrOffset(page: uint64, count, i: int): int {.inline.} =
  pageBase(page) + NodeHeaderBytes + count * 8 + i * 8

proc nodeChild(t: CowBTree, page: uint64, count, i: int): uint64 {.inline.} =
  rU64(t.pages, childPtrOffset(page, count, i))

proc copyPage(t: var CowBTree, src: uint64): uint64 =
  ## Path-copy step: allocate a fresh page and copy `src`'s bytes into it.
  ## This is the operation that guarantees a reachable page is never mutated
  ## in place — every modification works on a private copy.
  let dst = t.allocPage()
  let sb = pageBase(src)
  let db = pageBase(dst)
  for i in 0 ..< PageSize:
    t.pages[db + i] = t.pages[sb + i]
  dst

proc lowerBound(t: CowBTree, page: uint64, count: int, key: uint64): int =
  var lo = 0
  var hi = count
  while lo < hi:
    let mid = (lo + hi) shr 1
    if t.nodeKey(page, mid) < key:
      lo = mid + 1
    else:
      hi = mid
  lo

# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

proc orderFor(descriptorSize: int): int =
  let entrySize = 8 + descriptorSize
  (PageSize - NodeHeaderBytes) div entrySize

proc initCowBTree*(leafType: CowLeafType,
                   skipSubBlocks: bool = false): CowBTree =
  ## Create an empty CoW namespace B-tree page store. The buffer starts with
  ## just page 0 (the NamespaceHeader); both root slots are empty (commit id 0)
  ## until the first commit.
  let descSize = case leafType
    of cltTypeA: 8
    of cltTypeB: 16
  var t = CowBTree(
    descriptorSize: descSize,
    leafType: leafType,
    skipSubBlocks: skipSubBlocks,
    order: orderFor(descSize),
    pages: newSeq[byte](PageSize),  # page 0 only
    pageCount: 1,
    nextFreePage: 1,
    freeListHead: 0,
    root0: 0, root1: 0,
    commit0: 0, commit1: 0,
    lastCommit: 0,
    count: 0,
    pendingFree: @[],
    readers: @[],
  )
  t.writeHeader()
  t

proc count*(t: CowBTree): uint64 {.inline.} = t.count
proc pageBufferLen*(t: CowBTree): int {.inline.} = t.pages.len

# ---------------------------------------------------------------------------
# Committed-root selection (the reader's view; also used by the writer to find
# the base tree to copy from).
# ---------------------------------------------------------------------------

proc committedSlot(t: CowBTree): int =
  ## Return the slot holding the highest valid commit id, or -1 if the tree has
  ## never been committed (both slots empty). A slot with commit id 0 is empty.
  if t.commit0 == 0 and t.commit1 == 0:
    return -1
  if t.commit1 > t.commit0:
    1
  else:
    0

proc committedRoot*(t: CowBTree): uint64 =
  ## The currently-published B-tree root page (0 = empty namespace).
  let s = t.committedSlot()
  if s < 0:
    0
  elif s == 0:
    t.root0
  else:
    t.root1

proc committedCommitId*(t: CowBTree): uint64 =
  let s = t.committedSlot()
  if s < 0: 0
  elif s == 0: t.commit0
  else: t.commit1

# ---------------------------------------------------------------------------
# Lookup (traverses an immutable committed tree)
# ---------------------------------------------------------------------------

proc lookupFrom(t: CowBTree, root: uint64,
                key: uint64): Result[seq[byte], string] =
  if root == 0:
    return err("key not found")
  var page = root
  while true:
    let count = t.nodeCount(page)
    let idx = t.lowerBound(page, count, key)
    if t.nodeIsLeaf(page):
      if idx < count and t.nodeKey(page, idx) == key:
        let off = t.leafDescOffset(page, count, idx)
        return ok(t.pages[off ..< off + t.descriptorSize])
      else:
        return err("key not found")
    else:
      var childIdx = idx
      if idx < count and t.nodeKey(page, idx) == key:
        childIdx = idx + 1
      page = t.nodeChild(page, count, childIdx)

proc lookup*(t: CowBTree, key: uint64): Result[seq[byte], string] =
  ## Look up a key in the currently-committed tree.
  t.lookupFrom(t.committedRoot(), key)

proc lookupAt*(t: CowBTree, reader: ReaderHandle,
               key: uint64): Result[seq[byte], string] =
  ## Look up a key in the tree pinned by `reader` (its snapshot root), giving
  ## snapshot isolation regardless of writer commits since the pin.
  t.lookupFrom(reader.rootPage, key)

proc collectKeysFrom(t: CowBTree, page: uint64, into: var seq[uint64]) =
  if page == 0:
    return
  let count = t.nodeCount(page)
  if t.nodeIsLeaf(page):
    for i in 0 ..< count:
      into.add(t.nodeKey(page, i))
  else:
    for i in 0 .. count:
      t.collectKeysFrom(t.nodeChild(page, count, i), into)

proc keys*(t: CowBTree): Result[seq[uint64], string] =
  ## Return all committed keys in ascending B-tree order.
  var collected: seq[uint64]
  t.collectKeysFrom(t.committedRoot(), collected)
  ok(collected)

# ---------------------------------------------------------------------------
# Copy-on-write insertion
# ---------------------------------------------------------------------------
#
# Insertion returns, for the touched subtree, a freshly-copied page (the new
# spine node) plus an optional split (a promoted key + a new right sibling
# page). The recursion copies every node on the path from root to the touched
# leaf onto fresh pages, redirecting child pointers to the new pages, so the
# old spine stays byte-for-byte intact until the commit publishes the new root.

type
  CowInsert = object
    newPage: uint64       ## fresh copy of the visited node (the new spine page)
    didSplit: bool
    wasUpdate: bool
    promotedKey: uint64
    rightPage: uint64     ## new right sibling when didSplit

proc writeLeaf(t: var CowBTree, page: uint64, keys: seq[uint64],
               descs: seq[seq[byte]]) =
  ## (Re)write a leaf page from explicit key/descriptor sequences.
  t.setNodeHeader(page, true, keys.len)
  let base = pageBase(page)
  for i in 0 ..< keys.len:
    wU64(t.pages, base + NodeHeaderBytes + i * 8, keys[i])
  let descBase = base + NodeHeaderBytes + keys.len * 8
  for i in 0 ..< descs.len:
    let off = descBase + i * t.descriptorSize
    for b in 0 ..< t.descriptorSize:
      t.pages[off + b] = if b < descs[i].len: descs[i][b] else: 0

proc writeInternal(t: var CowBTree, page: uint64, keys: seq[uint64],
                   children: seq[uint64]) =
  t.setNodeHeader(page, false, keys.len)
  let base = pageBase(page)
  for i in 0 ..< keys.len:
    wU64(t.pages, base + NodeHeaderBytes + i * 8, keys[i])
  let childBase = base + NodeHeaderBytes + keys.len * 8
  for i in 0 ..< children.len:
    wU64(t.pages, childBase + i * 8, children[i])

proc readLeaf(t: CowBTree, page: uint64): (seq[uint64], seq[seq[byte]]) =
  let count = t.nodeCount(page)
  var keys = newSeq[uint64](count)
  var descs = newSeq[seq[byte]](count)
  for i in 0 ..< count:
    keys[i] = t.nodeKey(page, i)
    let off = t.leafDescOffset(page, count, i)
    descs[i] = t.pages[off ..< off + t.descriptorSize]
  (keys, descs)

proc readInternal(t: CowBTree, page: uint64): (seq[uint64], seq[uint64]) =
  let count = t.nodeCount(page)
  var keys = newSeq[uint64](count)
  var children = newSeq[uint64](count + 1)
  for i in 0 ..< count:
    keys[i] = t.nodeKey(page, i)
  for i in 0 .. count:
    children[i] = t.nodeChild(page, count, i)
  (keys, children)

proc cowInsert(t: var CowBTree, page: uint64, key: uint64,
               desc: seq[byte]): CowInsert =
  ## Copy-on-write insert into the subtree rooted at `page`. Returns the new
  ## (copied) spine page for this subtree.
  if t.nodeIsLeaf(page):
    var (keys, descs) = t.readLeaf(page)
    let idx = t.lowerBound(page, keys.len, key)
    if idx < keys.len and keys[idx] == key:
      # Update existing key — still CoW: write to a fresh page.
      descs[idx] = desc
      let np = t.copyPage(page)  # copy gives us a private page; rewrite below
      t.writeLeaf(np, keys, descs)
      return CowInsert(newPage: np, didSplit: false, wasUpdate: true)
    keys.insert(key, idx)
    descs.insert(desc, idx)
    let np = t.allocPage()
    if keys.len > t.order:
      # Split: left keeps [0,mid), right takes [mid,len).
      let mid = keys.len div 2
      let promoted = keys[mid]
      let rightPage = t.allocPage()
      t.writeLeaf(np, keys[0 ..< mid], descs[0 ..< mid])
      t.writeLeaf(rightPage, keys[mid ..< keys.len], descs[mid ..< descs.len])
      return CowInsert(newPage: np, didSplit: true, wasUpdate: false,
                       promotedKey: promoted, rightPage: rightPage)
    t.writeLeaf(np, keys, descs)
    return CowInsert(newPage: np, didSplit: false, wasUpdate: false)
  else:
    var (keys, children) = t.readInternal(page)
    var idx = t.lowerBound(page, keys.len, key)
    if idx < keys.len and keys[idx] == key:
      idx = idx + 1
    let sub = t.cowInsert(children[idx], key, desc)
    children[idx] = sub.newPage  # redirect to the new child page (copy-up)
    if sub.didSplit:
      keys.insert(sub.promotedKey, idx)
      children.insert(sub.rightPage, idx + 1)
      if keys.len > t.order:
        let mid = keys.len div 2
        let promoted = keys[mid]
        let np = t.allocPage()
        let rightPage = t.allocPage()
        t.writeInternal(np, keys[0 ..< mid], children[0 ..< mid + 1])
        t.writeInternal(rightPage, keys[mid + 1 ..< keys.len],
                        children[mid + 1 ..< children.len])
        return CowInsert(newPage: np, didSplit: true, wasUpdate: sub.wasUpdate,
                         promotedKey: promoted, rightPage: rightPage)
    let np = t.allocPage()
    t.writeInternal(np, keys, children)
    CowInsert(newPage: np, didSplit: false, wasUpdate: sub.wasUpdate)

proc collectReachable(t: CowBTree, root: uint64, into: var seq[uint64]) =
  ## Collect every page reachable from `root` (for superseded-page detection).
  if root == 0:
    return
  var stack = @[root]
  while stack.len > 0:
    let page = stack.pop()
    into.add(page)
    if not t.nodeIsLeaf(page):
      let count = t.nodeCount(page)
      for i in 0 .. count:
        stack.add(t.nodeChild(page, count, i))

proc insertAndCommit*(t: var CowBTree, key: uint64,
                      descriptor: openArray[byte]): Result[uint64, string] =
  ## Insert (or update) a key copy-on-write and atomically commit a new root.
  ## Returns the new commit id.
  ##
  ## Steps (mirroring §10 + the §6 write barrier):
  ##   1. Copy-on-write the spine from the OLD committed root to a NEW root,
  ##      popping fresh pages from the unified free-list / bump-allocating.
  ##   2. (Write barrier point — in a real flush, the new pages are made durable
  ##      here BEFORE the root is published.)
  ##   3. Publish the new root into the slot NOT currently in use, with a higher
  ##      commit id. The committed root is the highest-commit-id valid slot.
  ##   4. Mark pages reachable from the OLD root but not the NEW root as
  ##      "pending free", tagged with the new commit id (reader-gated reclaim).
  if descriptor.len != t.descriptorSize:
    return err("descriptor size mismatch: got " & $descriptor.len &
               " expected " & $t.descriptorSize)
  let desc = @descriptor
  let oldRoot = t.committedRoot()
  let oldSlot = t.committedSlot()

  var newRoot: uint64
  var isUpdate = false
  if oldRoot == 0:
    # Empty namespace: create the first leaf as the new root.
    let leaf = t.allocPage()
    t.writeLeaf(leaf, @[key], @[desc])
    newRoot = leaf
  else:
    let res = t.cowInsert(oldRoot, key, desc)
    isUpdate = res.wasUpdate
    if res.didSplit:
      let np = t.allocPage()
      t.writeInternal(np, @[res.promotedKey], @[res.newPage, res.rightPage])
      newRoot = np
    else:
      newRoot = res.newPage

  # Publish into the unused slot with a higher commit id (double buffering).
  let newCommit = t.lastCommit + 1
  t.lastCommit = newCommit
  let writeSlot = if oldSlot < 0: 0 else: 1 - oldSlot
  if writeSlot == 0:
    t.root0 = newRoot
    t.commit0 = newCommit
  else:
    t.root1 = newRoot
    t.commit1 = newCommit

  if not isUpdate:
    t.count += 1

  # Reader-gated reclamation bookkeeping: pages reachable from the old root but
  # NOT from the new root are now superseded. Hold them "pending free" tagged
  # with the freeing commit id.
  if oldRoot != 0:
    var oldPages: seq[uint64]
    t.collectReachable(oldRoot, oldPages)
    var newPages: seq[uint64]
    t.collectReachable(newRoot, newPages)
    for p in oldPages:
      if p notin newPages:
        t.pendingFree.add(PendingFree(page: p, freedAt: newCommit))

  t.writeHeader()
  ok(newCommit)

# ---------------------------------------------------------------------------
# Bulk load — bottom-up single-pass constructor (O(N) page writes, ONE commit)
# ---------------------------------------------------------------------------
#
# `insertAndCommit` is the incremental path: each call copy-on-write copies the
# spine from root to the touched leaf and atomically publishes a NEW root, so
# building a tree of N keys does N spine-copies + N atomic commits — O(N log N)
# page writes and N commit-id increments, with every superseded intermediate
# spine page accumulating in the buffer (the build never calls `reclaimPending`).
# At 100k keys the M3 benchmark measured this at ~7.5 s.
#
# `bulkLoad` builds the SAME logical tree from a PRE-SORTED batch in one
# bottom-up pass: pack the leaves left-to-right, then build each internal level
# over the level below, then publish the single final root in ONE commit. It
# allocates only the LIVE pages (no abandoned spine copies), so the image is
# both produced in O(N) and is markedly smaller and cleaner.
#
# WIRE-FORMAT NOTE: the produced image is the SAME NSB1 wire format every other
# path emits — a valid `NamespaceHeader` (page 0) selecting a committed root in
# slot 0 with `commit_id == 1`, plus a well-formed immutable page graph using
# the documented leaf/internal node layout. It is therefore read identically by
# `loadCowBTree` (Nim) and `CowNamespaceReader` (Rust); only the page PACKING
# differs from the per-key build (denser, no superseded pages), NOT the format.
# A bulk-built tree and a per-key-built tree of the same keys are value- and
# reader-equivalent but NOT byte-identical (the per-key image carries abandoned
# CoW pages, a higher commit id, and an alternating root slot).
#
# The B-tree separator invariant the lookup relies on: for an internal node with
# keys `[s0, s1, …]` and children `[c0, c1, …, cn]`, `lookup(key)` takes the
# `lowerBound(key)` index `i` and descends into `c_{i+1}` when `key == s_i`, else
# `c_i`. Splits promote the FIRST key of a right leaf (B+-tree-style copy-up), so
# the separator before child `c` is the SMALLEST key in `c`'s subtree — which is
# exactly what we use here.

proc bulkLoad*(t: var CowBTree,
               entries: openArray[(uint64, seq[byte])]): Result[uint64, string] =
  ## Build a committed tree from a PRE-SORTED, duplicate-free batch of
  ## `(key, descriptor)` entries in a single bottom-up pass, publishing ONE
  ## commit (id 1, slot 0). `t` MUST be a fresh, never-committed tree (as from
  ## `initCowBTree`) — bulk load is a constructor, not a merge.
  ##
  ## Requirements (all validated; a violation is an `Err`, never a silent
  ## mis-build): `t` has no prior commit; `entries` is strictly ascending by key
  ## (sorted, no duplicates); every descriptor is exactly `descriptorSize` bytes.
  ## An empty batch leaves the tree empty (`committedRoot == 0`), matching a
  ## per-key build of zero keys.
  ##
  ## Returns the new commit id (always 1 for a fresh tree).
  if t.committedSlot() >= 0:
    return err("bulkLoad requires a fresh, never-committed tree")
  if t.count != 0 or t.nextFreePage != 1 or t.pageCount != 1:
    return err("bulkLoad requires a pristine tree (no prior allocations)")

  # Validate the batch up front (ascending, unique, correct descriptor width).
  for i in 0 ..< entries.len:
    if entries[i][1].len != t.descriptorSize:
      return err("descriptor size mismatch at entry " & $i & ": got " &
                 $entries[i][1].len & " expected " & $t.descriptorSize)
    if i > 0 and entries[i][0] <= entries[i - 1][0]:
      return err("bulkLoad batch not strictly ascending at entry " & $i)

  if entries.len == 0:
    # Nothing to commit: leave the empty namespace as-is (no root published).
    t.writeHeader()
    return ok(0'u64)

  # ---- pack the leaf level ------------------------------------------------
  # Split the sorted entries into consecutive runs of at most `order` keys, each
  # written into a freshly allocated leaf page. We also remember each leaf's
  # FIRST key (its subtree minimum) — the separator material for the level above.
  type LevelNode = tuple[page: uint64, minKey: uint64]
  var level: seq[LevelNode]
  var i = 0
  while i < entries.len:
    let runLen = min(t.order, entries.len - i)
    var keys = newSeq[uint64](runLen)
    var descs = newSeq[seq[byte]](runLen)
    for j in 0 ..< runLen:
      keys[j] = entries[i + j][0]
      descs[j] = entries[i + j][1]
    let page = t.allocPage()
    t.writeLeaf(page, keys, descs)
    level.add((page: page, minKey: keys[0]))
    i += runLen

  # ---- build internal levels until a single root remains ------------------
  # Each internal node groups up to `order + 1` children from the level below
  # (so up to `order` separator keys). The separator before child `c` is `c`'s
  # subtree minimum — the smallest key reachable through it.
  while level.len > 1:
    var parent: seq[LevelNode]
    var c = 0
    while c < level.len:
      let groupLen = min(t.order + 1, level.len - c)
      var keys: seq[uint64]
      var children = newSeq[uint64](groupLen)
      for g in 0 ..< groupLen:
        children[g] = level[c + g].page
        if g > 0:
          # Separator before this child == the child's subtree minimum.
          keys.add(level[c + g].minKey)
      let page = t.allocPage()
      t.writeInternal(page, keys, children)
      # The group's subtree minimum is the leftmost child's minimum.
      parent.add((page: page, minKey: level[c].minKey))
      c += groupLen
    level = parent

  # ---- publish the single root in one commit (slot 0, id 1) ----------------
  let root = level[0].page
  t.root0 = root
  t.commit0 = 1
  t.lastCommit = 1
  t.count = uint64(entries.len)
  t.writeHeader()
  ok(1'u64)

# ---------------------------------------------------------------------------
# MVCC reader table + reader-gated reclamation
# ---------------------------------------------------------------------------

proc oldestReaderCommit(t: CowBTree): uint64 =
  ## The lowest commit id pinned by any active reader, or the current committed
  ## commit id when there are no readers (everything older is reclaimable).
  var oldest = t.committedCommitId()
  var any = false
  for r in t.readers:
    if r.active:
      any = true
      if r.commitId < oldest:
        oldest = r.commitId
  if any: oldest else: t.committedCommitId()

proc reclaimPending*(t: var CowBTree): int =
  ## Return every pending-free page whose freeing commit id is `<=` the oldest
  ## active reader's pinned commit id to the unified whole-block free list.
  ## Pages freed by a commit the oldest reader has NOT yet advanced past stay
  ## pending. Returns the number of pages reclaimed.
  ##
  ## Gating rule: a page freed at commit C is safe to reuse once no active
  ## reader is pinned to a root with commit id < C — i.e. once the oldest
  ## reader's pinned commit id is >= C.
  let oldest = oldestReaderCommit(t)
  var reclaimed = 0
  var stillPending: seq[PendingFree] = @[]
  for pf in t.pendingFree:
    if pf.freedAt <= oldest:
      t.pushFreePage(pf.page)
      reclaimed += 1
    else:
      stillPending.add(pf)
  t.pendingFree = stillPending
  if reclaimed > 0:
    t.writeHeader()
  reclaimed

proc pendingFreeCount*(t: CowBTree): int {.inline.} = t.pendingFree.len
proc freeListLen*(t: CowBTree): int =
  ## Walk the whole-block free chain and count its pages (test/diagnostic).
  var n = 0
  var p = t.freeListHead
  while p != 0:
    n += 1
    p = rU64(t.pages, pageBase(p))
  n

proc beginRead*(t: var CowBTree): ReaderHandle =
  ## Pin the current committed root as a read snapshot and register it in the
  ## reader table. The returned handle traverses an immutable tree.
  let slot = t.committedSlot()
  var h = ReaderHandle(
    active: true,
    slot: slot,
    rootPage: t.committedRoot(),
    commitId: t.committedCommitId(),
  )
  t.readers.add(h)
  h

proc endRead*(t: var CowBTree, reader: ReaderHandle) =
  ## Release a read snapshot from the reader table so its pinned commit id no
  ## longer gates reclamation. Matched by `rootPage` + `commitId`.
  for i in 0 ..< t.readers.len:
    if t.readers[i].active and
       t.readers[i].rootPage == reader.rootPage and
       t.readers[i].commitId == reader.commitId:
      t.readers[i].active = false
      break
  # Compact released entries.
  var live: seq[ReaderHandle] = @[]
  for r in t.readers:
    if r.active:
      live.add(r)
  t.readers = live

# ---------------------------------------------------------------------------
# Serialisation (the on-disk page image)
# ---------------------------------------------------------------------------

proc serialize*(t: CowBTree): seq[byte] =
  ## The page buffer IS the on-disk image: page 0 is the canonical
  ## NamespaceHeader, pages 1.. are B-tree node / free pages. Returns a copy.
  t.pages

proc loadCowBTree*(image: openArray[byte],
                   leafType: CowLeafType): Result[CowBTree, string] =
  ## Reconstruct a CoW B-tree from an on-disk page image (page 0 =
  ## NamespaceHeader). Validates the magic and that the image is page-aligned.
  ## The reconstructed tree is read-ready: `committedRoot` selects the highest
  ## valid commit id slot and `lookup` traverses the published tree.
  ##
  ## This is the Nim-side analogue of the Rust reader: it proves a CoW-written
  ## image round-trips, and it is what the crash-safety test uses to read a torn
  ## image. Writers can resume from a loaded tree as well.
  if image.len < HeaderTotal:
    return err("image too short for namespace header")
  if image[0] != HdrMagic0 or image[1] != HdrMagic1 or
     image[2] != HdrMagic2 or image[3] != HdrMagic3:
    return err("invalid namespace B-tree magic")
  if image.len mod PageSize != 0:
    return err("image not page-aligned")
  let descSize = case leafType
    of cltTypeA: 8
    of cltTypeB: 16
  let flags = image[OffFlags]
  var t = CowBTree(
    descriptorSize: descSize,
    leafType: leafType,
    skipSubBlocks: (flags and 0b10) != 0,
    order: orderFor(descSize),
    pages: @image,
    pageCount: rU64(image, OffPageCount),
    nextFreePage: rU64(image, OffNextFree),
    freeListHead: rU64(image, OffFreeHead),
    root0: rU64(image, OffRoot0),
    root1: rU64(image, OffRoot1),
    commit0: rU64(image, OffCommit0),
    commit1: rU64(image, OffCommit1),
    lastCommit: max(rU64(image, OffCommit0), rU64(image, OffCommit1)),
    count: 0,  # recomputed lazily if needed; not part of the wire format
    pendingFree: @[],
    readers: @[],
  )
  ok(t)

# ---------------------------------------------------------------------------
# Test-only helpers (mirror the production traversal so tests can assert on the
# page graph without exposing private fields).
# ---------------------------------------------------------------------------

proc lookupFromRootForTest*(t: CowBTree, root: uint64,
                            key: uint64): Result[seq[byte], string] =
  t.lookupFrom(root, key)

proc collectReachableForTest*(t: CowBTree, root: uint64,
                              into: var seq[uint64]) =
  t.collectReachable(root, into)

proc loadCowBTreeForTest*(image: openArray[byte],
                          leafType: CowLeafType): Result[CowBTree, string] =
  loadCowBTree(image, leafType)

proc rawPages*(t: CowBTree): lent seq[byte] {.inline.} =
  ## Borrow the page buffer without copying (for byte-equality assertions).
  t.pages

proc pageBytes*(t: CowBTree, page: uint64): seq[byte] =
  ## A copy of one page's bytes (test helper for CoW immutability proofs).
  let base = pageBase(page)
  t.pages[base ..< base + PageSize]
