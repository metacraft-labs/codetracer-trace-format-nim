when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions, sysatomics]

{.push raises: [].}

## B1: the B-link tree with in-place nodes, the `NSB2` node format
## (`ctfs-keyed-families.md` §5.8.3).
##
## Two members, `<p>.hdr` (a 256-byte header: `w0` the root's node number,
## `w1` the node count, `w1 = 0` an empty tree) and `<p>.nod` (node `n` is the
## 4096-byte block at `n * 4096`). A node:
##
## | Offset | Size | Field |
## |---|---|---|
## | 0 | 1 | `kind`: 1 leaf, 0 internal |
## | 2 | 2 | `count: u16`, at most 254 |
## | 8 | 8 | `high`: every key the node covers is below it; all ones: no bound |
## | 16 | 8 | `right`: the right sibling's node number plus 1; 0 for none |
## | 24 | 8 | `first`: internal, the child for keys below every separator |
## | 32 + 16·j | 8 | `key[j]` |
## | 40 + 16·j | 8 | `ref[j]`: leaf, a record number; internal, a child's node number |
##
## Entries are unsorted: each node keeps them in the order they arrived, so an
## insert shifts nothing a reader may already have read. References are
## record numbers of the family's record member; the family confirms each
## candidate against its record (§4.4).
##
## # The production form (§5.8.0), not the benchmark's shortcuts
##
## - **Entries, then count.** Appended slots are written first, and the count
##   after them, in a separate write.
## - **The hole fill.** A node that split shrinks to its left half by filling
##   each vacated slot below the new count with a surviving entry taken from
##   the highest slot at or above it, one 16-byte write per entry, and only
##   then storing the new count. Every surviving entry is, at every instant, at
##   a slot below whichever count a reader holds. The writer keeps each node's
##   physical slot order in memory to do that.
##
## # The seal (§5.8.3), after the family's records are published
##
## - **Phase A.** Every node created since the last seal, written whole; the
##   node member's `Size`.
## - **Phase B.** Every node that existed before: if it split, its new `high`
##   and `right` in one 16-byte write (link before shrink); otherwise, if it
##   gained entries, its new slots and then its count. Then the header.
## - **Phase C.** Each node that split shrinks by the hole fill.
##
## Each node change goes to the file first and to the node's mirror slot in
## the live coordination page after it (`seq` odd, payload, `seq` even); its
## in-place word goes to the file last.

import std/[algorithm, sets, tables]
import results
import ./types
import ./container
import ./keyed_store
import ./live_reader

const
  B1Capacity* = 254
    ## `(4096 - 32) / 16` entries per node.
  B1Mid = 127
  B1NoBound* = high(uint64)
  B1MirrorWords* = 3
    ## A node's mirrored words: word 0 (`kind`, `count`), `high`, `right`.
  B1HeaderMirrorWords* = 2

type
  B1Entry* = tuple[key: uint64, refv: uint64]

  B1Node = object
    leaf: bool
    slots: seq[B1Entry]
      ## In physical slot order; the node's count is its length.
    high: uint64
    right: int64      ## node number, or -1
    first: uint64     ## internal: the leftmost child's node number
    split: bool       ## split since the last seal

  B1Writer* = object
    prefix*: string
    hdr*: KHeader
    nod*: KMember
    nodes: seq[B1Node]
    root: int
    sealedNodes: int
      ## Nodes the file holds: those below were written at an earlier seal.
    extents: seq[int]
      ## Per range of 4096 nodes: the index into `extentList`, -1 not yet
      ## claimed, -2 refused (the page overflowed).
    extentList: seq[MirrorExtent]

proc b1Members*(prefix: string): tuple[hdr, nod: string] =
  (prefix & ".hdr", prefix & ".nod")

proc createB1*(c: var Ctfs, prefix: string): Result[B1Writer, string] =
  ## Create the family's two members. Node 0 is the initial root leaf; the
  ## tree is empty (`w1 = 0`) until the first seal.
  let names = b1Members(prefix)
  var w = B1Writer(prefix: prefix)
  w.hdr = ? c.createKHeader(names.hdr, B1HeaderMirrorWords)
  w.nod = ? c.createKMember(names.nod)
  w.nodes = @[B1Node(leaf: true, high: B1NoBound, right: -1)]
  w.root = 0
  ok(w)

proc nodeCount*(w: B1Writer): int = w.nodes.len

proc childFor(n: B1Node, key: uint64): uint64 =
  var found = false
  var best = 0'u64
  var child = n.first
  for e in n.slots:
    if e.key <= key and (not found or e.key > best):
      found = true
      best = e.key
      child = e.refv
  child

proc allocNode(w: var B1Writer, n: B1Node): int =
  w.nodes.add n
  w.nodes.len - 1

proc holeFill(phys: var seq[B1Entry], keep: HashSet[B1Entry], newCount: int) =
  ## Fill each slot below `newCount` whose entry is not kept with a kept
  ## entry from the highest slot at or above `newCount`, then cut to
  ## `newCount` (§5.8.0, "Shrinking an index block").
  var hi = phys.len - 1
  for j in 0 ..< newCount:
    if phys[j] in keep: continue
    while hi >= newCount and phys[hi] notin keep:
      dec hi
    doAssert hi >= newCount, "B1 hole fill: no survivor left to move"
    phys[j] = phys[hi]
    dec hi
  phys.setLen(newCount)

proc insertAt(w: var B1Writer, path: var seq[int], id: int, key, refv: uint64) =
  if w.nodes[id].slots.len < B1Capacity:
    w.nodes[id].slots.add (key: key, refv: refv)
    return
  # A split (§5.8.3, "Insert"): sort the 254 entries and the new one by
  # (key, ref); the left keeps [0, mid), a leaf gives [mid, 255) to a new
  # right node, an internal node gives (mid, 255) and makes the entry at mid's
  # ref the new node's `first`.
  var phys = w.nodes[id].slots
  phys.add (key: key, refv: refv)
  var sorted = phys
  sorted.sort(proc (a, b: B1Entry): int =
    if a.key != b.key: cmp(a.key, b.key) else: cmp(a.refv, b.refv))
  let sep = sorted[B1Mid].key
  let leaf = w.nodes[id].leaf
  var keep = initHashSet[B1Entry]()
  for j in 0 ..< B1Mid: keep.incl sorted[j]
  var rightEntries: seq[B1Entry]
  var rightFirst = 0'u64
  if leaf:
    rightEntries = sorted[B1Mid .. ^1]
  else:
    rightEntries = sorted[B1Mid + 1 .. ^1]
    rightFirst = sorted[B1Mid].refv
  holeFill(phys, keep, B1Mid)
  let r = w.allocNode(B1Node(leaf: leaf, slots: rightEntries,
    high: w.nodes[id].high, right: w.nodes[id].right, first: rightFirst))
  w.nodes[id].slots = phys
  w.nodes[id].high = sep
  w.nodes[id].right = int64(r)
  w.nodes[id].split = true
  if path.len > 0:
    let parent = path.pop()
    w.insertAt(path, parent, sep, uint64(r))
  else:
    let nr = w.allocNode(B1Node(leaf: false, slots: @[(key: sep, refv: uint64(r))],
      high: B1NoBound, right: -1, first: uint64(id)))
    w.root = nr

proc insert*(w: var B1Writer, key, refv: uint64) =
  ## Insert `(key, ref)` in memory; nothing reaches the file before the seal.
  var path: seq[int]
  var id = w.root
  while not w.nodes[id].leaf:
    path.add id
    id = int(childFor(w.nodes[id], key))
  w.insertAt(path, id, key, refv)

proc word0(leaf: bool, count: int): uint64 =
  (if leaf: 1'u64 else: 0'u64) or (uint64(count) shl 16)

proc rightWord(n: B1Node): uint64 =
  if n.right < 0: 0'u64 else: uint64(n.right) + 1

proc nodeBytes(n: B1Node): seq[byte] =
  result = newSeq[byte](NodeSize)
  writeU64LE(result, 0, word0(n.leaf, n.slots.len))
  writeU64LE(result, 8, n.high)
  writeU64LE(result, 16, n.rightWord())
  writeU64LE(result, 24, if n.leaf: 0'u64 else: n.first)
  for j, e in n.slots:
    writeU64LE(result, 32 + 16 * j, e.key)
    writeU64LE(result, 40 + 16 * j, e.refv)

proc entryBytes(e: B1Entry): array[16, byte] =
  writeU64LE(result, 0, e.key)
  writeU64LE(result, 8, e.refv)

proc nodeMirror(w: var B1Writer, c: Ctfs, n: int): Result[MirrorExtent, void] =
  if c.page == nil: return err()
  let k = n div int(MirrorExtentRecords)
  while w.extents.len <= k: w.extents.add -1
  if w.extents[k] == -1:
    let x = c.page.allocExtent(uint64(w.nod.entryIndex),
      uint64(k) * MirrorExtentRecords, MirrorExtentRecords, B1MirrorWords)
    if x.isOk:
      w.extentList.add x.get()
      w.extents[k] = w.extentList.len - 1
    else:
      w.extents[k] = -2
  if w.extents[k] < 0: return err()
  ok(w.extentList[w.extents[k]])

proc mirrorNode(w: var B1Writer, c: Ctfs, p: var SealPlan, n: int,
                count: int, high, right: uint64) =
  let x = w.nodeMirror(c, n)
  if x.isOk:
    p.mirror(x.get().slotOffset(uint64(n)),
      [word0(w.nodes[n].leaf, count), high, right])

proc planSeal*(w: var B1Writer, c: var Ctfs, p: var SealPlan): Result[void, string] =
  ## Add B1's seal (phases A, B and C, `ctfs-keyed-families.md` §5.8.3) to the
  ## plan. The family's records must already be published earlier in it.
  let nodeOff = proc (n: int): uint64 = uint64(n) * uint64(NodeSize)
  # --- Phase A: nodes created since the last seal, written whole, then the
  # node member's Size, then their mirrors.
  ? c.padTo(w.nod, uint64(NodeSize))
  if uint64(w.sealedNodes) * uint64(NodeSize) != w.nod.size:
    return err("B1 " & w.prefix & ": the node member holds " & $w.nod.size &
      " bytes for " & $w.sealedNodes & " sealed nodes")
  for n in w.sealedNodes ..< w.nodes.len:
    discard ? c.appendTo(w.nod, nodeBytes(w.nodes[n]))
  p.barrier()
  p.publishEntries(c, [w.nod])
  for n in w.sealedNodes ..< w.nodes.len:
    w.mirrorNode(c, p, n, w.nodes[n].slots.len, w.nodes[n].high,
      w.nodes[n].rightWord())
  # --- Phase B: in place, in nodes the file already holds.
  var shrink: seq[int]
  for n in 0 ..< w.sealedNodes:
    let disk = ? c.readMember(w.nod, nodeOff(n), NodeSize)
    let diskW0 = readU64LE(disk, 0)
    let diskCount = int((diskW0 shr 16) and 0xFFFF'u64)
    let node = w.nodes[n]
    if node.split:
      shrink.add n
      # Link before shrink: the right half is published (phase A); the old
      # node's high and right now send its moved keys there.
      w.mirrorNode(c, p, n, diskCount, node.high, node.rightWord())
      var hr: array[16, byte]
      writeU64LE(hr, 0, node.high)
      writeU64LE(hr, 8, node.rightWord())
      ? p.inPlaceItem(c, w.nod, nodeOff(n) + 8, hr, SealItem(kind: sikIndex,
        name: w.nod.name, record: nodeOff(n),
        words: @[(at: 8, v: node.high), (at: 16, v: node.rightWord())]))
    elif node.slots.len > diskCount:
      var slotBytes: seq[byte]
      var words: seq[SealWord]
      for j in diskCount ..< node.slots.len:
        let eb = entryBytes(node.slots[j])
        slotBytes.add eb
        words.add (at: 32 + 16 * j, v: node.slots[j].key)
        words.add (at: 40 + 16 * j, v: node.slots[j].refv)
      let w0 = word0(node.leaf, node.slots.len)
      words.add (at: 0, v: w0)
      var w0b: array[8, byte]
      writeU64LE(w0b, 0, w0)
      if mutated("b1-count-first"):
        # Mutation arm: the count before the entries it covers.
        w.mirrorNode(c, p, n, node.slots.len, node.high, node.rightWord())
        ? p.inPlaceItem(c, w.nod, nodeOff(n), w0b, SealItem(kind: sikIndex,
          name: w.nod.name, record: nodeOff(n), words: words))
        ? p.inPlace(c, w.nod, nodeOff(n) + 32 + 16 * uint64(diskCount), slotBytes)
      else:
        # Entries, then count (§5.8.0).
        ? p.inPlace(c, w.nod, nodeOff(n) + 32 + 16 * uint64(diskCount), slotBytes)
        w.mirrorNode(c, p, n, node.slots.len, node.high, node.rightWord())
        ? p.inPlaceItem(c, w.nod, nodeOff(n), w0b, SealItem(kind: sikIndex,
          name: w.nod.name, record: nodeOff(n), words: words))
  # The header: the root and the node count.
  w.hdr.w[0] = uint64(w.root)
  w.hdr.w[1] = uint64(w.nodes.len)
  ? w.hdr.planHeader(c, p)
  # --- Phase C: each node that split shrinks to its left half.
  for n in shrink:
    let disk = ? c.readMember(w.nod, nodeOff(n), NodeSize)
    let diskCount = int((readU64LE(disk, 0) shr 16) and 0xFFFF'u64)
    let node = w.nodes[n]
    let newCount = node.slots.len
    var fills: seq[tuple[j: int, e: B1Entry]]
    for j in 0 ..< min(newCount, diskCount):
      let de = (key: readU64LE(disk, 32 + 16 * j), refv: readU64LE(disk, 40 + 16 * j))
      if de != node.slots[j]:
        fills.add (j: j, e: node.slots[j])
    var words: seq[SealWord]
    let w0 = word0(node.leaf, newCount)
    var w0b: array[8, byte]
    writeU64LE(w0b, 0, w0)
    if mutated("b1-shrink-count-first"):
      # Mutation arm: the new count before the hole fill.
      w.mirrorNode(c, p, n, newCount, node.high, node.rightWord())
      ? p.inPlaceItem(c, w.nod, nodeOff(n), w0b, SealItem(kind: sikIndex,
        name: w.nod.name, record: nodeOff(n), words: @[(at: 0, v: w0)]))
    for f in fills:
      ? p.inPlace(c, w.nod, nodeOff(n) + 32 + 16 * uint64(f.j), entryBytes(f.e))
      words.add (at: 32 + 16 * f.j, v: f.e.key)
      words.add (at: 40 + 16 * f.j, v: f.e.refv)
    if newCount > diskCount:
      var slotBytes: seq[byte]
      for j in diskCount ..< newCount:
        slotBytes.add entryBytes(node.slots[j])
        words.add (at: 32 + 16 * j, v: node.slots[j].key)
        words.add (at: 40 + 16 * j, v: node.slots[j].refv)
      ? p.inPlace(c, w.nod, nodeOff(n) + 32 + 16 * uint64(diskCount), slotBytes)
    if not mutated("b1-shrink-count-first"):
      words.add (at: 0, v: w0)
      w.mirrorNode(c, p, n, newCount, node.high, node.rightWord())
      ? p.inPlaceItem(c, w.nod, nodeOff(n), w0b, SealItem(kind: sikIndex,
        name: w.nod.name, record: nodeOff(n), words: words))
  for n in 0 ..< w.nodes.len:
    w.nodes[n].split = false
  w.sealedNodes = w.nodes.len
  ok()

# ---------------------------------------------------------------------------
# The reader
# ---------------------------------------------------------------------------

type
  B1Node3* = object
    ## A node as a reader accepted it: its in-place words, and its slots.
    kind*: uint8
    count*: int
    high*: uint64
    right*: uint64
    first*: uint64
    blk*: seq[byte]

  B1Reader* = object
    prefix*: string
    hdrSlot*: int
    nodSlot*: int
    nodesRead*: int
    cache: Table[uint64, B1Node3]
    cacheRefresh: int
      ## Nodes accepted since the reader's last refresh, kept until the next
      ## one: an index block a realization updates in place is re-read at every
      ## refresh and may be kept between two (`ctfs-container.md` §6, "What
      ## Is Mutated In Place, And What A Cache May Keep").

  B1View* = object
    ## The header as a reader accepted it.
    root*: uint64
    nodes*: uint64
    empty*: bool

proc openB1Reader*(r: LiveReader, prefix: string): B1Reader =
  let names = b1Members(prefix)
  B1Reader(prefix: prefix, hdrSlot: r.findSlot(names.hdr),
           nodSlot: r.findSlot(names.nod))

proc readHeader*(b: var B1Reader, r: var LiveReader): Result[B1View, string] =
  ## Read `w0` and `w1`: from the page's mirror slot when attached (and
  ## published), else from the file read twice until both agree. A header
  ## member that is absent or has `Size` 0 is an empty tree.
  let names = b1Members(b.prefix)
  if b.hdrSlot < 0: b.hdrSlot = r.findSlot(names.hdr)
  if b.nodSlot < 0: b.nodSlot = r.findSlot(names.nod)
  if b.hdrSlot < 0 or r.memberSize(b.hdrSlot) == 0'u64:
    return ok(B1View(empty: true))
  if r.memberSize(b.hdrSlot) < uint64(KeyedHeaderSize):
    return err("B1 " & b.prefix & ": header member " & names.hdr & " holds " &
      $r.memberSize(b.hdrSlot) & " bytes, not 256")
  let x = r.mirrorExtent(b.hdrSlot, 0)
  if x.isOk:
    var words: array[2, uint64]
    for _ in 0 ..< RootReadRetries:
      case readSlot(r.page.view, x.get().slotOffset(0), words)
      of srOk:
        return ok(B1View(root: words[0], nodes: words[1], empty: words[1] == 0))
      of srNever: break
      of srRetry: discard
  for _ in 0 ..< RootReadRetries:
    let a = ? r.readMember(b.hdrSlot, 0, 16)
    let c = ? r.readMember(b.hdrSlot, 0, 16)
    if a == c:
      let nodes = readU64LE(a, 8)
      return ok(B1View(root: readU64LE(a, 0), nodes: nodes, empty: nodes == 0))
  err("B1 " & b.prefix & ": the header's words did not read the same twice in " &
    $RootReadRetries & " attempts")

proc readNode*(b: var B1Reader, r: var LiveReader, n: uint64): Result[B1Node3, string] =
  ## Node `n`: attached, its block read from the file between two loads of
  ## its mirror `seq`, and read again when they differ, with its in-place
  ## words from the mirror; file-only, its in-place words read twice until both
  ## reads agree. Refuses a `count` above 254 or a `kind` other than 0 or 1.
  if b.cacheRefresh != r.refreshes:
    b.cache.clear()
    b.cacheRefresh = r.refreshes
  if n in b.cache:
    return ok(b.cache.getOrDefault(n))
  let off = n * uint64(NodeSize)
  var res: B1Node3
  var accepted = false
  let x = r.mirrorExtent(b.nodSlot, n)
  if x.isOk:
    let so = x.get().slotOffset(n)
    for _ in 0 ..< RootReadRetries:
      let s1 = r.page.view.load(so)
      if s1 == 0'u64: break
      if (s1 and 1'u64) == 1'u64: continue
      let w0 = r.page.view.loadRelaxed(so + 8)
      let hi = r.page.view.loadRelaxed(so + 16)
      let rt = r.page.view.loadRelaxed(so + 24)
      let blk = ? r.readMember(b.nodSlot, off, NodeSize)
      atomicThreadFence(ATOMIC_ACQUIRE)
      if r.page.view.loadRelaxed(so) != s1: continue
      res = B1Node3(kind: uint8(w0 and 0xff), count: int((w0 shr 16) and 0xFFFF'u64),
        high: hi, right: rt, first: readU64LE(blk, 24), blk: blk)
      accepted = true
      break
  if not accepted:
    for _ in 0 ..< RootReadRetries:
      let blk = ? r.readMember(b.nodSlot, off, NodeSize)
      let again = ? r.readMember(b.nodSlot, off, 24)
      var same = true
      for k in 0 ..< 24:
        if blk[k] != again[k]:
          same = false
          break
      if not same: continue
      let w0 = readU64LE(blk, 0)
      res = B1Node3(kind: uint8(w0 and 0xff), count: int((w0 shr 16) and 0xFFFF'u64),
        high: readU64LE(blk, 8), right: readU64LE(blk, 16),
        first: readU64LE(blk, 24), blk: blk)
      accepted = true
      break
  if not accepted:
    return err("B1 " & b.prefix & ": node " & $n & "'s in-place words did not " &
      "read the same twice in " & $RootReadRetries & " attempts")
  if res.kind > 1'u8:
    return err("B1 " & b.prefix & ": node " & $n & " has kind " & $res.kind)
  if res.count > B1Capacity:
    return err("B1 " & b.prefix & ": node " & $n & " has count " & $res.count &
      ", above " & $B1Capacity)
  inc b.nodesRead
  b.cache[n] = res
  ok(res)

proc slotOf*(n: B1Node3, j: int): B1Entry =
  (key: readU64LE(n.blk, 32 + 16 * j), refv: readU64LE(n.blk, 40 + 16 * j))

type
  DescentOutcome = enum doFound, doStale

proc descend(b: var B1Reader, r: var LiveReader, key: uint64,
             cands: var seq[uint64]): Result[DescentOutcome, string] =
  ## One descent, safe for a key held by several entries (a truncated key,
  ## §5.8.3 "A leaf"). A split sorts by `(key, ref)` and cuts at entry 127, so
  ## entries of one key can lie on both sides of the cut, the left node's
  ## `high` then being that key. The descent therefore moves right only past a
  ## `high` strictly below the key, takes at an internal node the child of the
  ## largest separator strictly below the key (`first` when there is none),
  ## and at the leaves collects the key's entries and continues right while the
  ## key is at or above the leaf's `high`. For a key held once this reads at
  ## most one node more than §5.8.3's descent, and only when the key is a
  ## separator; the bytes it reads are the same.
  let h = ? b.readHeader(r)
  if h.empty: return ok(doFound)
  let visible = r.memberSize(b.nodSlot) div uint64(NodeSize)
  var n = h.root
  var visits = 0'u64
  while true:
    if n >= visible:
      return ok(doStale)
    inc visits
    if visits > visible:
      return ok(doStale)
    let node = ? b.readNode(r, n)
    if key > node.high and node.right != 0'u64:
      n = node.right - 1
      continue
    if node.kind == 1'u8:
      for j in 0 ..< node.count:
        let e = node.slotOf(j)
        if e.key == key: cands.add e.refv
      if key >= node.high and node.right != 0'u64:
        n = node.right - 1
        continue
      return ok(doFound)
    var found = false
    var best = 0'u64
    var child = node.first
    for j in 0 ..< node.count:
      let e = node.slotOf(j)
      if e.key < key and e.key < node.high and (not found or e.key > best):
        found = true
        best = e.key
        child = e.refv
    n = child

proc candidates*(b: var B1Reader, r: var LiveReader, key: uint64):
    Result[seq[uint64], string] =
  ## The references B1 holds under `key`, to be confirmed against their
  ## records. A descent that reaches a node past the node member's `Size` as
  ## last read, or visits more nodes than it holds, may only be a stale view:
  ## the reader refreshes and descends again, and only a second such descent is
  ## damage (§5.8.3, "Lookup").
  var cands: seq[uint64]
  for attempt in 0 ..< 2:
    cands.setLen(0)
    let o = ? b.descend(r, key, cands)
    if o == doFound:
      # A split leaves an entry in both halves until the left one shrinks:
      # each candidate is returned once.
      var once: seq[uint64]
      for x in cands:
        if x notin once: once.add x
      return ok(once)
    ? r.refresh()
  err("B1 " & b.prefix & ": a descent for key " & $key & " reached a node " &
    "past the node member's Size twice")

proc lookup*(b: var B1Reader, r: var LiveReader, key: uint64,
             confirm: proc (refv: uint64): Result[bool, string] {.closure,
               raises: [].}): Result[seq[uint64], string] =
  ## Every reference under `key` that `confirm` accepts. A lookup that finds
  ## nothing refreshes and descends once more before it answers "absent"
  ## (§5.3, "Live reader").
  for attempt in 0 ..< 2:
    let cands = ? b.candidates(r, key)
    var hits: seq[uint64]
    for x in cands:
      if ? confirm(x): hits.add x
    if hits.len > 0 or attempt == 1:
      return ok(hits)
    ? r.refresh()
  ok(newSeq[uint64]())

proc enumerate*(b: var B1Reader, r: var LiveReader): Result[seq[B1Entry], string] =
  ## Every leaf entry, by walking the leaves left to right from the leftmost
  ## leaf (a consistent view needs a closed or quiescent container).
  var res: seq[B1Entry]
  let h = ? b.readHeader(r)
  if h.empty: return ok(res)
  var n = h.root
  while true:
    let node = ? b.readNode(r, n)
    if node.kind == 1'u8: break
    n = node.first
  var visits = 0'u64
  let visible = r.memberSize(b.nodSlot) div uint64(NodeSize)
  while true:
    inc visits
    if visits > visible:
      return err("B1 " & b.prefix & ": the leaf chain is longer than the " &
        "node member")
    let node = ? b.readNode(r, n)
    for j in 0 ..< node.count:
      res.add node.slotOf(j)
    if node.right == 0'u64: break
    n = node.right - 1
  ok(res)
