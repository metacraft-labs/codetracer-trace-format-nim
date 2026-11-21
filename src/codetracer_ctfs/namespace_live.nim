when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## A namespace committed live (`ctfs-container.md` §8, "Live And Incremental
## Publication"): the `NSB1` copy-on-write tree of `cow_btree.nim` persisted
## as a member a reader may follow.
##
## **Writer, per commit:**
##
## 1. every new page, appended after the member's current end;
## 2. the member's entry published (`MapBlock` before `Size`), so every new
##    page lies below the published `Size`;
## 3. `next_free_page` and `page_count` stored in page 0;
## 4. the new root's page number stored in `root_block[s]`, `s` the slot with
##    the lower `commit_id`; then, in a separate write, `commit_id[s] =
##    commit_id[1 - s] + 1`.
##
## No superseded page is reused: a writer of a container file another process
## may read appends (§8, "Reclaiming superseded pages"); a tree published here
## refuses to have reclaimed pages.
##
## **Reader:** page 0 located through the entry; `commit_id[0]`,
## `commit_id[1]`, `root_block[0]`, `root_block[1]`, then both ids again,
## accepted only when neither id changed and the two non-zero ids differ by
## exactly 1; the higher slot taken; then the member's `Size`, and a root page
## at or past `Size / 4096` refused as damage, naming the namespace.

import results
import ./types
import ./container
import ./keyed_store
import ./live_reader
import ./cow_btree

const
  NsPageSize = 4096
  NsOffRoot0 = 4
  NsOffRoot1 = 12
  NsOffCommit0 = 20
  NsOffCommit1 = 28
  NsHeaderTotal = 61

type
  LiveNamespace* = object
    m*: KMember
    persisted*: uint64     ## pages the file holds
    commit0, commit1: uint64

proc createLiveNamespace*(c: var Ctfs, name: string): Result[LiveNamespace, string] =
  var ns: LiveNamespace
  ns.m = ? c.createKMember(name)
  ok(ns)

proc planCommit*(ns: var LiveNamespace, c: var Ctfs, t: CowBTree,
                 p: var SealPlan): Result[void, string] =
  ## Publish the tree's committed state: its new pages, the member's entry,
  ## page 0's allocation words, then the root slot and its commit id.
  if t.freeListLen() > 0:
    return err("namespace " & ns.m.name & ": a tree published live reuses no " &
      "superseded page, and this one has reclaimed " & $t.freeListLen())
  let img = t.serialize()
  let pages = uint64(img.len div NsPageSize)
  if pages < ns.persisted:
    return err("namespace " & ns.m.name & ": the tree has fewer pages than the file")
  # Pages below `persisted` are immutable once committed, except page 0.
  for pg in 1'u64 ..< ns.persisted:
    let disk = ? c.readMember(ns.m, pg * NsPageSize, NsPageSize)
    for k in 0 ..< NsPageSize:
      if disk[k] != img[int(pg) * NsPageSize + k]:
        return err("namespace " & ns.m.name & ": page " & $pg &
          " changed after it was committed")
  let flip = mutated("ns-commit-before-pages")
  var firstNew = ns.persisted
  if ns.persisted == 0:
    # The first commit: page 0 with both slots empty, then the pages.
    var p0 = img[0 ..< NsPageSize]
    for k in NsOffRoot0 ..< NsOffCommit1 + 8: p0[k] = 0
    discard ? c.appendTo(ns.m, p0)
    firstNew = 1
  if pages > firstNew:
    discard ? c.appendTo(ns.m, img[int(firstNew) * NsPageSize ..< int(pages) * NsPageSize])
  if not flip:
    p.barrier()
    p.publishEntries(c, [ns.m])
  # Page 0's allocation words (flags, free head, next free page, page count:
  # bytes 36 to 60), then the tree's committed root into the slot with the
  # lower commit id, then that slot's id, the other's plus one. The ids are
  # the publisher's: one per published commit, so the two valid ids differ by
  # exactly 1 whatever the tree committed in memory between two publications.
  let tailFrom = NsOffCommit1 + 8
  var tail = img[tailFrom ..< NsHeaderTotal]
  ? p.inPlace(c, ns.m, uint64(tailFrom), tail)
  let root = t.committedRoot()
  if root != 0'u64:
    let s = if ns.commit0 <= ns.commit1: 0 else: 1
    let id = max(ns.commit0, ns.commit1) + 1
    var rb: array[8, byte]
    writeU64LE(rb, 0, root)
    var cb: array[8, byte]
    writeU64LE(cb, 0, id)
    let rootOff = if s == 0: NsOffRoot0 else: NsOffRoot1
    let idOff = if s == 0: NsOffCommit0 else: NsOffCommit1
    ? p.inPlace(c, ns.m, uint64(rootOff), rb)
    ? p.inPlaceItem(c, ns.m, uint64(idOff), cb, SealItem(kind: sikIndex, name: ns.m.name,
      record: 0, words: @[(at: idOff, v: id)]))
    if s == 0: ns.commit0 = id else: ns.commit1 = id
  if flip:
    # Mutation arm: the root flipped before the pages it reaches are covered
    # by the member's published Size.
    p.barrier()
    p.publishEntries(c, [ns.m])
  ns.persisted = pages
  ok()

proc commit*(ns: var LiveNamespace, c: var Ctfs, t: CowBTree): Result[void, string] =
  var p = initSealPlan(c)
  ? ns.planCommit(c, t, p)
  c.executeSeal(p)

proc readLiveRoot*(r: var LiveReader, name: string):
    Result[tuple[empty: bool, root: uint64, commitId: uint64], string] =
  ## Reader steps 1 to 4: the committed root, double-read, validated against
  ## the member's `Size`.
  let slot = r.findSlot(name)
  if slot < 0 or r.memberSize(slot) < uint64(NsPageSize):
    return ok((empty: true, root: 0'u64, commitId: 0'u64))
  for _ in 0 ..< RootReadRetries:
    let a = ? r.readMember(slot, 0, 36)
    let b = ? r.readMember(slot, 0, 36)
    let ca0 = readU64LE(a, NsOffCommit0)
    let ca1 = readU64LE(a, NsOffCommit1)
    if ca0 != readU64LE(b, NsOffCommit0) or ca1 != readU64LE(b, NsOffCommit1) or
        readU64LE(a, NsOffRoot0) != readU64LE(b, NsOffRoot0) or
        readU64LE(a, NsOffRoot1) != readU64LE(b, NsOffRoot1):
      continue
    if ca0 != 0 and ca1 != 0 and max(ca0, ca1) - min(ca0, ca1) != 1:
      continue
    if ca0 == 0 and ca1 == 0:
      return ok((empty: true, root: 0'u64, commitId: 0'u64))
    let s = if ca0 > ca1: 0 else: 1
    let root = readU64LE(a, if s == 0: NsOffRoot0 else: NsOffRoot1)
    let pagesVisible = r.memberSize(slot) div uint64(NsPageSize)
    if root == 0 or root >= pagesVisible:
      return err("namespace " & name & ": its committed root page " & $root &
        " is not below its published Size (" & $pagesVisible & " pages)")
    return ok((empty: false, root: root, commitId: max(ca0, ca1)))
  err("namespace " & name & ": its root slots did not read the same twice")
