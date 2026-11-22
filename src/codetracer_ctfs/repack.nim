when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Dead space and repacking (`ctfs-container.md` §6, "Dead Space";
## `ctfs-keyed-families.md` §4.6), and the canonical repack
## (`ctfs-keyed-families.md` §8.3, O-12; owner 2026-10-26).
##
## **Dead space.** A block is dead when no published pointer reaches it.
## `deadSpace` walks a closed container from its root: the root region, every
## member's data and mapping blocks, and every keyed stream a keyed family's
## records name (`threads.rec`'s two streams per thread, `snap.rec`'s parts,
## `bundle.rec`'s data and index). Every allocated block it does not reach is
## dead, and is reported by number.
##
## **Repack.** `repackContainer` copies every live member of a closed container
## into a new one, which holds no dead block by construction. Its placement
## order is the canonical order of `ctfs-keyed-families.md` §8.3 O-12, so the
## output is a function of the container's content and not of the recording's
## timing:
##
## 1. The header declares what the source declares (`MaxRootEntries` as
##    declared, `0` staying `0`), version 5, no encryption, no shards.
## 2. The root members that belong to no keyed family, in ascending order of
##    name, each appended whole in one append (its blocks placed as §5 places
##    one append), published by one seal at the end of the step.
## 3. The keyed families, in ascending order of their record member's name
##    (`bundle.rec`, `corrmark.rec`, `cppages.rec`, `snap.rec`,
##    `threads.rec`), each rebuilt from its logical content in one seal:
##    - bundled files in `filemap.bin` order, each file's streams copied whole;
##    - the correlation index with its records in ascending key order and each
##      key's entries in their declared order, H1 built by inserting the
##      records in that order, S3 built at the end;
##    - the page store with its records in ascending `(key, digest)` order, the
##      heap in record order in frames of 4 MiB, B1 built by inserting the
##      records in that order;
##    - snapshots in ordinal order, each one's parts copied whole in part-slot
##      order, then its `positions`;
##    - threads in ascending `ct_tid` order: segments reserved in ascending
##      segment order, each thread's `events` then `index` stream copied whole.
##
## Every input to that order is content: names, keys, digests, ordinals,
## `ct_tid`s, and the order of a key's entries. Timing-dependent inputs (the
## source's slot assignment, its block placement, the page store's
## first-stored order, H1's split history, the seals the source was written in)
## are not read.

import std/[algorithm, tables, sets]
import results
import ./types
import ./base40
import ./container
import ./streaming
import ./keyed_store
import ./keyed_stream
import ./live_reader
import ./page_store
import ./thread_records
import ./snapshot_records
import ./corrmark_live
import ./h1

type
  DeadSpace* = object
    allocated*: uint64        ## whole blocks in the file
    reachable*: uint64
    dead*: seq[uint64]        ## the dead blocks, ascending

proc streamBlocks(r: var LiveReader, label: string, size, mapBlock: uint64,
                  into: var HashSet[uint64]): Result[void, string] =
  ## Every data and mapping block a stream reaches.
  if mapBlock == 0'u64 or size == 0'u64: return ok()
  if isDirectMapBlock(mapBlock):
    into.incl directDataBlock(mapBlock)
    return ok()
  let bs = uint64(r.blockSize)
  let usable = bs div 8 - 1
  let n = (size + bs - 1) div bs
  # Walk every data block index, recording the mapping blocks on the way.
  var idx = 0'u64
  while idx < n:
    var i = idx
    var level = 1
    var cap = usable
    var cur = mapBlock
    into.incl cur
    while i >= cap:
      i -= cap
      inc level
      let b = ? r.readRaw(cur * bs + usable * 8, 8)
      cur = readU64LE(b, 0)
      if cur == 0: return err(label & ": a missing chain pointer")
      into.incl cur
      cap = cap * usable
    var sub = cap div usable
    var l = level
    while l > 1:
      let b = ? r.readRaw(cur * bs + (i div sub) * 8, 8)
      cur = readU64LE(b, 0)
      if cur == 0: return err(label & ": a missing child pointer")
      into.incl cur
      i = i mod sub
      sub = sub div usable
      dec l
    let d = ? r.readRaw(cur * bs + i * 8, 8)
    let db = readU64LE(d, 0)
    if db == 0: return err(label & ": a missing data block")
    into.incl db
    inc idx
  ok()

proc deadSpace*(r: var LiveReader): Result[DeadSpace, string] =
  ## The reachability walk of a closed (or quiescent) container.
  var reach = initHashSet[uint64]()
  for b in 0'u64 ..< r.rootBlocks: reach.incl b
  for i in 0 ..< r.slots:
    let e = r.entries[i]
    if e.name == 0: continue
    ? streamBlocks(r, r.slotName(i), e.size, e.mapBlock, reach)
  # Keyed streams named by records.
  var t = openThreadsReader(r)
  let threads = ? t.enumerateThreads(r)
  for v in threads:
    ? streamBlocks(r, "thread events", v.events.size, v.events.mapBlock, reach)
    ? streamBlocks(r, "thread index", v.index.size, v.index.mapBlock, reach)
  for o in 0'u64 ..< snapshotCount(r):
    let s = ? readSnapshot(r, o)
    for p in s.snap.parts:
      ? streamBlocks(r, "snapshot part", p.data.size, p.data.mapBlock, reach)
      ? streamBlocks(r, "snapshot part index", p.index.size, p.index.mapBlock, reach)
  let bs = r.findSlot(BundleRec)
  if bs >= 0:
    for f in 0'u64 ..< recordCount(BundleRecordSize, r.memberSize(bs)):
      let b = ? readBundled(r, f)
      ? streamBlocks(r, "bundle data", b.data.size, b.data.mapBlock, reach)
      ? streamBlocks(r, "bundle index", b.index.size, b.index.mapBlock, reach)
  var res = DeadSpace(allocated: r.wholeBlocks, reachable: uint64(reach.len))
  for b in 0'u64 ..< r.wholeBlocks:
    if b notin reach: res.dead.add b
  ok(res)

# ---------------------------------------------------------------------------
# The canonical repack
# ---------------------------------------------------------------------------

const KeyedFamilyMembers = [
  "threads.rec", "threads.top", "snap.rec", "snap.pos", "bundle.rec",
  "corrmark.rec", "corrmark.ext", "corrmark.hdr", "corrmark.bkt", "corrmark.ovf",
  "corrmark.s3", "cppages.hdr", "cppages.nod", "cppages.rec", "cppages.hzd",
  "cppages.hzi"]

proc cmpDigest(a, b: PageDigest): int =
  for i in 0 ..< a.len:
    if a[i] != b[i]: return cmp(a[i], b[i])
  0

proc readWhole(r: var LiveReader, label: string, s: KStream): Result[seq[byte], string] =
  if s.size == 0: return ok(newSeq[byte]())
  r.readStream(label, s.size, s.mapBlock, 0, int(s.size))

proc copyStream(c: var Ctfs, r: var LiveReader, label: string, s: KStream): Result[KStream, string] =
  var o = KStream()
  let b = ? readWhole(r, label, s)
  ? c.appendStream(o, b, label)
  ok(o)

proc repackContainer*(src: string, dst: string): Result[void, string] =
  ## Copy every live member of the closed container `src` into a new
  ## container `dst`, in the canonical order of this module's doc.
  var r = ? openLiveReader(src, attach = false)
  defer: r.close()
  let hdr = ? r.readRaw(0, 16)
  let declared = readU32LE(hdr, 12)
  var c = ? createCtfsStreaming(dst, r.blockSize, declared)
  # 2. Plain members, by name.
  var plain: seq[(string, int)]
  for i in 0 ..< r.slots:
    if r.entries[i].name == 0: continue
    let n = r.slotName(i)
    if n notin KeyedFamilyMembers: plain.add (n, i)
  if mutated("repack-slot-order"):
    discard # Mutation arm: the members keep the source's slot order.
  else:
    plain.sort(proc (a, b: (string, int)): int = cmp(a[0], b[0]))
  for (n, i) in plain:
    var f = ? c.addFile(n)
    let bytes = ? r.readMember(i, 0, int(r.entries[i].size))
    if bytes.len > 0: ? c.writeToFile(f, bytes)
  c.publish()
  # 3a. Bundled files.
  let bslot = r.findSlot(BundleRec)
  if bslot >= 0:
    var w = ? c.createBundleFamily()
    for f in 0'u64 ..< recordCount(BundleRecordSize, r.memberSize(bslot)):
      let b = ? readBundled(r, f)
      var nb = BundledFile(form: b.form)
      if b.form != pfAbsent:
        nb.data = ? copyStream(c, r, "bundle data", b.data)
        nb.index = ? copyStream(c, r, "bundle index", b.index)
      discard w.addBundled(nb)
    ? w.seal(c)
  # 3b. The correlation index: records by key, entries in declared order.
  let cslot = r.findSlot(CorrRec)
  if cslot >= 0:
    var cr = openCorrmarkReader(r)
    var keys: seq[uint64]
    for rec in 0'u64 ..< recordCount(CorrRecordSize, r.memberSize(cslot)):
      let k = ? r.readMemberU64(cslot, recordOffset(CorrRecordSize, rec))
      keys.add k
    keys.sort()
    var w = ? c.createCorrmark()
    for k in keys:
      let es = ? cr.lookupEntries(r, k)
      for e in es:
        ? w.addMarker(k, e)
    if r.findSlot(CorrS3) >= 0:
      ? w.closeIndex(c)
    else:
      ? w.seal(c)
  # 3c. The page store: records by (key, digest), heap in that order.
  let pslot = r.findSlot(PageStoreRec)
  if pslot >= 0:
    var ps = openPageStoreReader(r)
    var recs: seq[(uint64, PageDigest, uint64)]
    for rec in 0'u64 ..< ps.recordCount(r):
      let b = ? r.readMember(pslot, recordOffset(32, rec), 32)
      var d: PageDigest
      for i in 0 ..< 16: d[i] = b[8 + i]
      recs.add (readU64LE(b, 0), d, rec)
    if not mutated("repack-page-store-order"):
      recs.sort(proc (a, b: (uint64, PageDigest, uint64)): int =
        if a[0] != b[0]: cmp(a[0], b[0]) else: cmpDigest(a[1], b[1]))
    var w = ? c.createPageStore()
    for (k, d, rec) in recs:
      let pg = ? ps.pageBytes(r, rec)
      discard ? w.storePage(d, pg)
    ? w.seal(c)
  # 3d. Snapshots, in ordinal order.
  let sslot = r.findSlot(SnapRec)
  if sslot >= 0:
    var w = ? c.createSnapshotFamily()
    for o in 0'u64 ..< snapshotCount(r):
      let v = ? readSnapshot(r, o)
      var s = v.snap
      for p in 0 ..< SnapParts:
        if s.parts[p].form != pfAbsent:
          s.parts[p].data = ? copyStream(c, r, "snapshot part", s.parts[p].data)
          s.parts[p].index = ? copyStream(c, r, "snapshot part index", s.parts[p].index)
      discard w.addSnapshot(s)
    ? w.seal(c)
  # 3e. Threads, by ct_tid.
  let tslot = r.findSlot(ThreadsRec)
  if tslot >= 0:
    var t = openThreadsReader(r)
    var all = ? t.enumerateThreads(r)
    all.sort(proc (a, b: ThreadView): int = cmp(a.info.ctTid, b.info.ctTid))
    var w = ? c.createThreadsFamily()
    for v in all:
      discard ? w.addThread(v.info)
      let ev = ? readWhole(r, "thread events", v.events)
      if ev.len > 0: discard ? c.appendEvents(w, v.info.ctTid, ev)
      let ix = ? readWhole(r, "thread index", v.index)
      if ix.len > 0: ? c.appendIndex(w, v.info.ctTid, ix)
    ? w.seal(c)
  ? c.closeCtfs()
  ok()
