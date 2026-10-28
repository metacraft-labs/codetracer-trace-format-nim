when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## H1, linear hashing (`ctfs-keyed-families.md` §5.8.4; F3, live).
##
## Three members: `<p>.hdr` (`w0` level `L`, `w1` split pointer `p`, `w2`
## bucket count `nb = 2^L + p`, `w3` key count; `nb = 0` an empty table),
## `<p>.bkt` (bucket `b`'s primary block at `b * 4096`) and `<p>.ovf`
## (overflow block `o` at `o * 4096`, numbered in allocation order; a block
## dropped before it was ever written still occupies its number, as zeros).
##
## Bucket block: `count: u16` at 0 (at most 255), `next` at 8 (the next
## overflow block's number plus 1, or 0), entries `(key, ref)` at `16 + 16·i`.
##
## Address: `hk = h(key)` (splitmix64), `b = hk mod 2^L`, and if `b < p`,
## `b = hk mod 2^(L+1)`. An insert appends to the first block of `b`'s chain
## with room, or allocates the next overflow block and links it; then, when
## the key count exceeds `0.75 * nb * 255`, bucket `p` splits: every entry
## whose `h(key) mod 2^(L+1)` is `p + 2^L` moves to that new bucket, trailing
## overflow blocks left empty are dropped (and become dead), and `p` advances.
##
## The production form (§5.8.0): an entry leaves a block by the hole fill
## (each vacated slot below the new count filled from the highest surviving
## slot at or above it, one entry per write, ascending), appended entries are
## written before the count, and the seal runs in phases:
##
## - **A.** Buckets created since the last seal, with their chains, and every
##   overflow block allocated since, written whole; the overflow member
##   extended to its allocated blocks. Publish.
## - **B.** The header. Publish.
## - **C.** In pre-existing blocks: appended entries, then counts; removals by
##   the hole fill; every `next` that changed. Publish.
##
## The page mirrors the header's four words as one slot, and each block's
## word 0 and `next` as one 2-word slot (extents keyed by the member's entry
## index and the block number).

import std/[sets, tables]
import results
import ./types
import ./container
import ./keyed_store
import ./live_reader

const
  H1Capacity* = 255
  H1HeaderMirrorWords* = 4
  H1BlockMirrorWords* = 2
  H1LoadFactor* = 0.75

proc splitmix64*(k: uint64): uint64 =
  ## `ctfs-keyed-families.md` §5.8.0's hash, over `u64` arithmetic.
  var z = k + 0x9E3779B97F4A7C15'u64
  z = (z xor (z shr 30)) * 0xBF58476D1CE4E5B9'u64
  z = (z xor (z shr 27)) * 0x94D049BB133111EB'u64
  z xor (z shr 31)

type
  H1Entry* = tuple[key: uint64, refv: uint64]

  H1Block = object
    slots: seq[H1Entry]   ## physical order; the count is its length
    ovf: int64            ## -1: the primary block; else its overflow number
    isNew: bool           ## not yet in the file

  H1Bucket = object
    blocks: seq[H1Block]  ## primary first, then the chain

  H1Writer* = object
    prefix*: string
    hdr*: KHeader
    bkt*, ovf*: KMember
    level: int
    split: uint64
    count*: uint64
    buckets: seq[H1Bucket]
    sealedBuckets: int
    ovfBlocks: uint64
    deadBytes*: uint64
    dropped: seq[tuple[bucket: int, fromBlock: int]]
      ## Chains whose tail was dropped since the last seal: the block whose
      ## `next` must become 0.
    extents: Table[(int, uint64), int]
    extentList: seq[MirrorExtent]

proc h1Members*(prefix: string): tuple[hdr, bkt, ovf: string] =
  (prefix & ".hdr", prefix & ".bkt", prefix & ".ovf")

proc createH1*(c: var Ctfs, prefix: string): Result[H1Writer, string] =
  let n = h1Members(prefix)
  var w = H1Writer(prefix: prefix)
  w.hdr = ? c.createKHeader(n.hdr, H1HeaderMirrorWords)
  w.bkt = ? c.createKMember(n.bkt)
  w.ovf = ? c.createKMember(n.ovf)
  w.buckets = @[H1Bucket(blocks: @[H1Block(ovf: -1, isNew: true)])]
  ok(w)

proc address*(level: int, split: uint64, key: uint64): uint64 =
  let hk = splitmix64(key)
  result = hk and ((1'u64 shl level) - 1)
  if result < split:
    result = hk and ((1'u64 shl (level + 1)) - 1)

proc put(w: var H1Writer, b: int, e: H1Entry) =
  for blk in w.buckets[b].blocks.mitems:
    if blk.slots.len < H1Capacity:
      blk.slots.add e
      return
  w.buckets[b].blocks.add H1Block(slots: @[e], ovf: int64(w.ovfBlocks), isNew: true)
  inc w.ovfBlocks

proc holeFillBlock(phys: var seq[H1Entry], keep: HashSet[H1Entry]) =
  ## Remove the entries not kept by the hole fill (§5.8.0).
  var n = 0
  for e in phys:
    if e in keep: inc n
  var hi = phys.len - 1
  for j in 0 ..< n:
    if phys[j] in keep: continue
    while hi >= n and phys[hi] notin keep: dec hi
    phys[j] = phys[hi]
    dec hi
  phys.setLen(n)

proc splitOne(w: var H1Writer) =
  let old = int(w.split)
  let newb = old + (1 shl w.level)
  let mask = (1'u64 shl (w.level + 1)) - 1
  w.buckets.add H1Bucket(blocks: @[H1Block(ovf: -1, isNew: true)])
  var moved: seq[H1Entry]
  for blk in w.buckets[old].blocks.mitems:
    var keep = initHashSet[H1Entry]()
    for e in blk.slots:
      if (splitmix64(e.key) and mask) == uint64(newb): moved.add e
      else: keep.incl e
    holeFillBlock(blk.slots, keep)
  let before = w.buckets[old].blocks.len
  while w.buckets[old].blocks.len > 1 and w.buckets[old].blocks[^1].slots.len == 0:
    let gone = w.buckets[old].blocks.pop()
    if not gone.isNew:
      w.deadBytes += uint64(NodeSize)
  if w.buckets[old].blocks.len < before:
    w.dropped.add (bucket: old, fromBlock: w.buckets[old].blocks.len - 1)
  for e in moved: w.put(newb, e)
  inc w.split
  if w.split == (1'u64 shl w.level):
    inc w.level
    w.split = 0

proc insert*(w: var H1Writer, key, refv: uint64) =
  ## In memory; at most one split per insert, so no insert does work
  ## proportional to the table.
  let b = int(address(w.level, w.split, key))
  w.put(b, (key: key, refv: refv))
  inc w.count
  let nb = (1'u64 shl w.level) + w.split
  if float(w.count) > H1LoadFactor * float(nb * H1Capacity):
    w.splitOne()

proc nextWord(bk: H1Bucket, i: int): uint64 =
  if i + 1 < bk.blocks.len: uint64(bk.blocks[i + 1].ovf) + 1 else: 0'u64

proc blockBytes(slots: seq[H1Entry], next: uint64): seq[byte] =
  result = newSeq[byte](NodeSize)
  result[0] = byte(slots.len and 0xff)
  result[1] = byte(slots.len shr 8)
  writeU64LE(result, 8, next)
  for i, e in slots:
    writeU64LE(result, 16 + 16 * i, e.key)
    writeU64LE(result, 24 + 16 * i, e.refv)

proc blockMirror(w: var H1Writer, c: Ctfs, m: KMember, blk: uint64): Result[MirrorExtent, void] =
  if c.page == nil: return err()
  let k = (m.entryIndex, blk div MirrorExtentRecords)
  if k notin w.extents:
    let x = c.page.allocExtent(uint64(m.entryIndex), (blk div MirrorExtentRecords) *
      MirrorExtentRecords, MirrorExtentRecords, H1BlockMirrorWords)
    if x.isOk:
      w.extentList.add x.get()
      w.extents[k] = w.extentList.len - 1
    else:
      w.extents[k] = -1
  let i = w.extents.getOrDefault(k)
  if i < 0: return err()
  ok(w.extentList[i])

proc locate(w: H1Writer, b, i: int): tuple[m: KMember, blk: uint64] =
  let o = w.buckets[b].blocks[i].ovf
  if o < 0: (w.bkt, uint64(b)) else: (w.ovf, uint64(o))

proc planSeal*(w: var H1Writer, c: var Ctfs, p: var SealPlan): Result[void, string] =
  ## Add H1's phases to the plan; the family's records are published earlier
  ## in it.
  # --- A: new buckets whole (primary blocks in bucket order), every new
  # overflow block whole, the overflow member extended to its blocks.
  for b in w.sealedBuckets ..< w.buckets.len:
    if w.bkt.size != uint64(b) * uint64(NodeSize):
      return err("H1 " & w.prefix & ": bucket " & $b & " would not land at its offset")
    discard ? c.appendTo(w.bkt, blockBytes(w.buckets[b].blocks[0].slots,
      w.buckets[b].nextWord(0)))
  var newOvf: seq[(int, int)]
  var byNumber: Table[uint64, (int, int)]
  for b in 0 ..< w.buckets.len:
    for i in 1 ..< w.buckets[b].blocks.len:
      if w.buckets[b].blocks[i].isNew:
        newOvf.add (b, i)
        byNumber[uint64(w.buckets[b].blocks[i].ovf)] = (b, i)
  # The overflow member grows to its allocated blocks: each new block whole,
  # a block dropped before it was ever written as zeros.
  var o = w.ovf.size div uint64(NodeSize)
  while o < w.ovfBlocks:
    if o in byNumber:
      let (b, i) = byNumber.getOrDefault(o)
      discard ? c.appendTo(w.ovf, blockBytes(w.buckets[b].blocks[i].slots,
        w.buckets[b].nextWord(i)))
    else:
      var z = newSeq[byte](NodeSize)
      discard ? c.appendTo(w.ovf, z)
    inc o
  p.barrier()
  p.publishEntries(c, [w.bkt, w.ovf])
  for b in w.sealedBuckets ..< w.buckets.len:
    for i in 0 ..< w.buckets[b].blocks.len:
      let loc = w.locate(b, i)
      let x = w.blockMirror(c, loc.m, loc.blk)
      if x.isOk and (i == 0 or w.buckets[b].blocks[i].isNew):
        p.mirror(x.get().slotOffset(loc.blk), [uint64(w.buckets[b].blocks[i].slots.len),
          w.buckets[b].nextWord(i)])
  for (b, i) in newOvf:
    if b < w.sealedBuckets:
      let loc = w.locate(b, i)
      let x = w.blockMirror(c, loc.m, loc.blk)
      if x.isOk:
        p.mirror(x.get().slotOffset(loc.blk), [uint64(w.buckets[b].blocks[i].slots.len),
          w.buckets[b].nextWord(i)])
  # --- B: the header.
  w.hdr.w[0] = uint64(w.level)
  w.hdr.w[1] = w.split
  w.hdr.w[2] = uint64(w.buckets.len)
  w.hdr.w[3] = w.count
  ? w.hdr.planHeader(c, p)
  # --- C: pre-existing blocks in place.
  for b in 0 ..< min(w.sealedBuckets, w.buckets.len):
    for i in 0 ..< w.buckets[b].blocks.len:
      let blk = w.buckets[b].blocks[i]
      if blk.isNew: continue
      let loc = w.locate(b, i)
      let base = loc.blk * uint64(NodeSize)
      let disk = ? c.readMember(loc.m, base, NodeSize)
      let diskCount = int(uint16(disk[0]) or (uint16(disk[1]) shl 8))
      let diskNext = readU64LE(disk, 8)
      let newCount = blk.slots.len
      let next = w.buckets[b].nextWord(i)
      var words: seq[SealWord]
      var slotOps: seq[tuple[off: uint64, bytes: seq[byte]]]
      for j in 0 ..< min(newCount, diskCount):
        let de = (key: readU64LE(disk, 16 + 16 * j), refv: readU64LE(disk, 24 + 16 * j))
        if de != blk.slots[j]:
          var eb = newSeq[byte](16)
          writeU64LE(eb, 0, blk.slots[j].key)
          writeU64LE(eb, 8, blk.slots[j].refv)
          slotOps.add (off: base + 16 + 16 * uint64(j), bytes: eb)
          words.add (at: 16 + 16 * j, v: blk.slots[j].key)
          words.add (at: 24 + 16 * j, v: blk.slots[j].refv)
      if newCount > diskCount:
        var sb: seq[byte]
        for j in diskCount ..< newCount:
          var eb: array[16, byte]
          writeU64LE(eb, 0, blk.slots[j].key)
          writeU64LE(eb, 8, blk.slots[j].refv)
          sb.add eb
          words.add (at: 16 + 16 * j, v: blk.slots[j].key)
          words.add (at: 24 + 16 * j, v: blk.slots[j].refv)
        slotOps.add (off: base + 16 + 16 * uint64(diskCount), bytes: sb)
      if newCount == diskCount and next == diskNext and slotOps.len == 0:
        continue
      var hb: array[16, byte]
      hb[0] = byte(newCount and 0xff)
      hb[1] = byte(newCount shr 8)
      writeU64LE(hb, 8, next)
      words.add (at: 0, v: readU64LE(hb, 0))
      words.add (at: 8, v: next)
      let x = w.blockMirror(c, loc.m, loc.blk)
      let countFirst = newCount < diskCount and mutated("h1-shrink-first")
      if countFirst:
        # Mutation arm: the shrunk count before the hole fill.
        if x.isOk: p.mirror(x.get().slotOffset(loc.blk), [uint64(newCount), next])
        ? p.inPlaceItem(c, loc.m, base, hb, SealItem(kind: sikIndex, name: loc.m.name,
          record: base, words: words))
      # Entries (the hole fill ascending, then the appended slots), then the
      # count and next: the mirror first, then the file.
      for op in slotOps:
        ? p.inPlace(c, loc.m, op.off, op.bytes)
      if not countFirst:
        if x.isOk: p.mirror(x.get().slotOffset(loc.blk), [uint64(newCount), next])
        ? p.inPlaceItem(c, loc.m, base, hb, SealItem(kind: sikIndex, name: loc.m.name,
          record: base, words: words))
  for bk in w.buckets.mitems:
    for blk in bk.blocks.mitems: blk.isNew = false
  w.sealedBuckets = w.buckets.len
  w.dropped.setLen(0)
  ok()

# ---------------------------------------------------------------------------
# The reader
# ---------------------------------------------------------------------------

type
  H1Reader* = object
    prefix*: string
    hdrSlot, bktSlot, ovfSlot: int

proc openH1Reader*(r: LiveReader, prefix: string): H1Reader =
  let n = h1Members(prefix)
  H1Reader(prefix: prefix, hdrSlot: r.findSlot(n.hdr), bktSlot: r.findSlot(n.bkt),
           ovfSlot: r.findSlot(n.ovf))

proc readWords(r: var LiveReader, slot: int, off: uint64, n: int,
               mirrorRec: uint64, mirrorWords: int): Result[seq[uint64], string] =
  ## `n` leading words at `off`: from the page's mirror slot when attached and
  ## published, else read twice from the file until both agree.
  let x = r.mirrorExtent(slot, mirrorRec)
  if x.isOk:
    var w = newSeq[uint64](mirrorWords)
    for _ in 0 ..< RootReadRetries:
      case readSlot(r.page.view, x.get().slotOffset(mirrorRec), w)
      of srOk: return ok(w)
      of srNever: break
      of srRetry: discard
  for _ in 0 ..< RootReadRetries:
    let a = ? r.readMember(slot, off, 8 * n)
    let b = ? r.readMember(slot, off, 8 * n)
    if a == b:
      var w = newSeq[uint64](n)
      for i in 0 ..< n: w[i] = readU64LE(a, 8 * i)
      return ok(w)
  err("H1: words at " & $off & " did not read the same twice")

proc candidates*(h: var H1Reader, r: var LiveReader, key: uint64): Result[seq[uint64], string] =
  ## §5.8.4, "Lookup": the header, the address, and `[0, count)` of the
  ## primary block and of every block its chain reaches. A chain longer than
  ## the overflow member's block count is damage.
  var res: seq[uint64]
  let n = h1Members(h.prefix)
  if h.hdrSlot < 0: h.hdrSlot = r.findSlot(n.hdr)
  if h.bktSlot < 0: h.bktSlot = r.findSlot(n.bkt)
  if h.ovfSlot < 0: h.ovfSlot = r.findSlot(n.ovf)
  if h.hdrSlot < 0 or r.memberSize(h.hdrSlot) == 0'u64: return ok(res)
  let hw = ? readWords(r, h.hdrSlot, 0, 4, 0, H1HeaderMirrorWords)
  if hw[2] == 0'u64: return ok(res)
  let b = address(int(hw[0]), hw[1], key)
  if b >= hw[2]:
    return err("H1 " & h.prefix & ": address " & $b & " past " & $hw[2] & " buckets")
  var slot = h.bktSlot
  var blk = b
  var hops = 0'u64
  let ovfBlocks = r.memberSize(h.ovfSlot) div uint64(NodeSize)
  while true:
    let base = blk * uint64(NodeSize)
    let hdrw = ? readWords(r, slot, base, 2, blk, H1BlockMirrorWords)
    let count = int(hdrw[0] and 0xFFFF'u64)
    if count > H1Capacity:
      return err("H1 " & h.prefix & ": a block's count " & $count & " is past " & $H1Capacity)
    let body = ? r.readMember(slot, base, 16 + 16 * count)
    for i in 0 ..< count:
      if readU64LE(body, 16 + 16 * i) == key:
        res.add readU64LE(body, 24 + 16 * i)
    if hdrw[1] == 0'u64: break
    inc hops
    if hops > ovfBlocks:
      return err("H1 " & h.prefix & ": a chain longer than the overflow member")
    slot = h.ovfSlot
    blk = hdrw[1] - 1
  # During a split an entry is in its old bucket and its new one until the
  # hole fill removes it from the old: each candidate is returned once.
  var seen: seq[uint64]
  for x in res:
    if x notin seen: seen.add x
  ok(seen)
