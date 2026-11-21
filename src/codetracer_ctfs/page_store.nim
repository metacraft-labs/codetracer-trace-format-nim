when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## The MCR page store, keyed and durable at every seal
## (`codetracer-specs` `CTFS-Binary-Format.md` §2.6; `ctfs-keyed-families.md`
## §5.7, the "live index" shape, realization B1, DR-2).
##
## Members (`CTFS-Binary-Format.md` §2.2, "Page CAS"):
##
## | Member | Holds |
## |---|---|
## | `cppages.hdr` | B1's header: `w0` the root's node number, `w1` the node count; bytes 64 to 79 (`w8`, `w9`) the `CASH` record (`MCR-Memory-Page-CAS.md` §4.2.3), written at the store's first seal and never changed |
## | `cppages.nod` | B1's nodes |
## | `cppages.rec` | the page records, in first-stored order |
## | `cppages.hzd`, `cppages.hzi` | the page heap: page bytes, compressed in frames that end at seals |
##
## A record is `S = 16 + w` bytes (`w` the digest width of `CASH.algo`: 16,
## so 32, for algorithm 1, xxh3-128): `key` (the digest's last 8 bytes read
## big-endian, stored as a little-endian `u64`), the full digest, and
## `heap_offset`, the page's first byte in the uncompressed heap. The store has
## no removal, so a record has no state: record `r` is present when it lies
## below `cppages.rec`'s `Size`.
##
## The heap is compressed (the production form; the benchmark stored pages
## raw): `cppages.hzd` holds one zstd frame at level 3 per index entry, with
## its content size declared, and a frame ends at every seal that publishes
## pages as well as at 4,194,304 bytes; `cppages.hzi` has `spans.idx`'s shape
## (an 8-byte header `chunk_size: u32 = 4194304`, `index_version: u16 = 2`,
## `reserved: u16 = 0`, then 16-byte entries `offset: u64, cumulative: u64`).
##
## A seal that publishes pages goes in this order, each step published before
## the next begins (§2.6, "A seal that publishes pages"):
##
## 1. the frame of the pages stored since the last seal, then its `.hzi` entry;
## 2. their records;
## 3. B1's phases A, B and C.
##
## The snapshots that reference those pages are published after this returns
## (`MCR-Memory-Page-CAS.md` §5.1.2 rule 3), by the caller.

import std/[tables]
import results
import ./types
import ./container
import ./keyed_store
import ./live_reader
import ./b1
import ./zstd_bindings

export b1

const
  PageStorePrefix* = "cppages"
  PageStoreHdr* = "cppages.hdr"
  PageStoreNod* = "cppages.nod"
  PageStoreRec* = "cppages.rec"
  PageStoreHzd* = "cppages.hzd"
  PageStoreHzi* = "cppages.hzi"
  CasAlgoXxh3x128* = 1'u16
  PageStoreFrameLimit* = 4_194_304
    ## A frame ends at this many uncompressed bytes, and at every seal.
  PageStoreZstdLevel* = 3
  HziHeaderSize* = 8
  HziEntrySize* = 16
  HziIndexVersion* = 2'u16

type
  PageDigest* = array[16, byte]
    ## An xxh3-128 digest, the canonical big-endian byte array.

  PageStoreWriter* = object
    b1*: B1Writer
    rec*, hzd*, hzi*: KMember
    algo*: uint16
    pageSize*: uint32
    pending: seq[seq[byte]]
    pendingRecs: seq[array[32, byte]]
    heapBytes: uint64
    stored: Table[PageDigest, uint64]
    nrec*: uint64
    sealedRecords*: uint64
    cashWritten: bool

proc pageKey*(d: PageDigest): uint64 =
  ## `MCR-Memory-Page-CAS.md` §5.4: the digest's LAST eight bytes, big-endian.
  for i in 8 ..< 16:
    result = (result shl 8) or uint64(d[i])

proc digestWidth*(algo: uint16): int =
  if algo == CasAlgoXxh3x128: 16 else: 0

proc createPageStore*(c: var Ctfs, algo = CasAlgoXxh3x128,
                      pageSize = 4096'u32): Result[PageStoreWriter, string] =
  ## Create the page store's five members, in the order of §2.6's table.
  if digestWidth(algo) != 16:
    return err("page store: CASH algorithm " & $algo & " is not one this " &
      "library writes (1, xxh3-128)")
  var w = PageStoreWriter(algo: algo, pageSize: pageSize)
  w.b1 = ? c.createB1(PageStorePrefix)
  w.rec = ? c.createKMember(PageStoreRec)
  w.hzd = ? c.createKMember(PageStoreHzd)
  w.hzi = ? c.createKMember(PageStoreHzi)
  ok(w)

proc storePage*(w: var PageStoreWriter, digest: PageDigest,
                page: openArray[byte]): Result[tuple[record: uint64, isNew: bool], string] =
  ## Store a page the first time it is seen: its record in first-stored order
  ## and its bytes in the heap, in memory until the next seal. A page whose
  ## digest is stored already is not stored again (§5.1.1: each page is stored
  ## once per trace); its record is returned.
  if page.len != int(w.pageSize):
    return err("page store: a page of " & $page.len & " bytes; every page is " &
      "a whole page of " & $w.pageSize & " bytes (sub-page tails stay in the " &
      "snapshot's memory part)")
  if digest in w.stored:
    return ok((record: w.stored.getOrDefault(digest), isNew: false))
  let r = w.nrec
  inc w.nrec
  var rec: array[32, byte]
  writeU64LE(rec, 0, pageKey(digest))
  for i in 0 ..< 16: rec[8 + i] = digest[i]
  writeU64LE(rec, 24, w.heapBytes)
  w.heapBytes += uint64(page.len)
  w.pending.add @page
  w.pendingRecs.add rec
  w.stored[digest] = r
  w.b1.insert(pageKey(digest), r)
  ok((record: r, isNew: true))

proc pendingPages*(w: PageStoreWriter): int = w.pendingRecs.len

proc compressFrame(data: openArray[byte]): Result[seq[byte], string] =
  let bound = ZSTD_compressBound(csize_t(data.len))
  var outp = newSeq[byte](int(bound))
  let n = ZSTD_compress(addr outp[0], bound, unsafeAddr data[0],
    csize_t(data.len), cint(PageStoreZstdLevel))
  if ZSTD_isError(n) != 0:
    return err("page store: zstd compression failed: " & $ZSTD_getErrorName(n))
  outp.setLen(int(n))
  ok(outp)

proc planSeal*(w: var PageStoreWriter, c: var Ctfs, p: var SealPlan): Result[void, string] =
  ## Add a seal of the pages stored since the last seal to the plan: their
  ## frames and index entries, their records, then B1's phases.
  if w.pendingRecs.len == 0:
    return ok()
  # 1. The frames, then their index entries.
  if not mutated("pagestore-frame-left-open"):
    if w.hzi.size == 0'u64:
      var h: array[HziHeaderSize, byte]
      writeU32LE(h, 0, uint32(PageStoreFrameLimit))
      h[4] = byte(HziIndexVersion and 0xff)
      h[5] = byte(HziIndexVersion shr 8)
      discard ? c.appendTo(w.hzi, h)
    let firstPending = w.heapBytes - uint64(w.pending.len) * uint64(w.pageSize)
    var cumulative = firstPending
    var i = 0
    while i < w.pending.len:
      var frame: seq[byte]
      while i < w.pending.len and frame.len + w.pending[i].len <= PageStoreFrameLimit:
        frame.add w.pending[i]
        inc i
      let z = ? compressFrame(frame)
      let at = ? c.appendTo(w.hzd, z)
      cumulative += uint64(frame.len)
      var e: array[HziEntrySize, byte]
      writeU64LE(e, 0, at)
      writeU64LE(e, 8, cumulative)
      discard ? c.appendTo(w.hzi, e)
    w.pending.setLen(0)
    p.barrier()
    p.publishEntries(c, [w.hzd, w.hzi])
  # 2. The records.
  for rec in w.pendingRecs:
    discard ? c.appendRecord(w.rec, 32, rec)
  w.sealedRecords += uint64(w.pendingRecs.len)
  w.pendingRecs.setLen(0)
  p.barrier()
  p.publishEntries(c, [w.rec])
  # 3. B1, whose header carries the CASH record from the first seal on.
  if not w.cashWritten:
    var cash: array[16, byte]
    cash[0] = byte('C'); cash[1] = byte('A'); cash[2] = byte('S'); cash[3] = byte('H')
    cash[4] = 1; cash[5] = 0
    cash[6] = byte(w.algo and 0xff); cash[7] = byte(w.algo shr 8)
    writeU32LE(cash, 8, w.pageSize)
    w.b1.hdr.w[8] = readU64LE(cash, 0)
    w.b1.hdr.w[9] = readU64LE(cash, 8)
    w.cashWritten = true
  ? w.b1.planSeal(c, p)
  ok()

proc seal*(w: var PageStoreWriter, c: var Ctfs): Result[void, string] =
  ## Publish every page stored since the last seal, as one seal of the
  ## container.
  var p = initSealPlan(c)
  ? w.planSeal(c, p)
  c.executeSeal(p)

# ---------------------------------------------------------------------------
# The reader
# ---------------------------------------------------------------------------

type
  PageStoreReader* = object
    b1*: B1Reader
    algo*: uint16
    pageSize*: uint32
    recSlot, hzdSlot, hziSlot: int
    frameCache: tuple[index: int, bytes: seq[byte]]
    misses*: int
      ## Records examined under a matching key whose digest did not match
      ## (§5.4's collision counter).

proc openPageStoreReader*(r: LiveReader, algo = CasAlgoXxh3x128,
                          pageSize = 4096'u32): PageStoreReader =
  PageStoreReader(b1: openB1Reader(r, PageStorePrefix), algo: algo,
    pageSize: pageSize, recSlot: r.findSlot(PageStoreRec),
    hzdSlot: r.findSlot(PageStoreHzd), hziSlot: r.findSlot(PageStoreHzi),
    frameCache: (index: -1, bytes: @[]))

proc resolveSlots(ps: var PageStoreReader, r: LiveReader) =
  if ps.recSlot < 0: ps.recSlot = r.findSlot(PageStoreRec)
  if ps.hzdSlot < 0: ps.hzdSlot = r.findSlot(PageStoreHzd)
  if ps.hziSlot < 0: ps.hziSlot = r.findSlot(PageStoreHzi)

proc recordCount*(ps: var PageStoreReader, r: LiveReader): uint64 =
  if ps.recSlot < 0: 0'u64
  else: recordCount(32, r.memberSize(ps.recSlot))

proc checkCash*(ps: var PageStoreReader, r: var LiveReader): Result[void, string] =
  ## The `CASH` record names the algorithm and the page size; a reader
  ## compares both with what it expects and refuses a mismatch
  ## (`MCR-Memory-Page-CAS.md` §4.2.3).
  let slot = ps.b1.hdrSlot
  if slot < 0 or r.memberSize(slot) < uint64(KeyedHeaderSize):
    return ok()
  let b = ? r.readMember(slot, 64, 16)
  if b[0] != byte('C') or b[1] != byte('A') or b[2] != byte('S') or b[3] != byte('H'):
    return err("page store: cppages.hdr carries no CASH record at byte 64")
  let version = uint16(b[4]) or (uint16(b[5]) shl 8)
  let algo = uint16(b[6]) or (uint16(b[7]) shl 8)
  let size = readU32LE(b, 8)
  if version != 1'u16:
    return err("page store: cppages.hdr's CASH record has version " & $version)
  if algo != ps.algo or size != ps.pageSize:
    return err("page store: cppages.hdr's CASH record names algorithm " &
      $algo & " and page size " & $size & ", not " & $ps.algo & " and " &
      $ps.pageSize)
  if readU32LE(b, 12) != 0'u32:
    return err("page store: cppages.hdr's CASH record has a nonzero reserved field")
  ok()

proc lookupPage*(ps: var PageStoreReader, r: var LiveReader,
                 digest: PageDigest): Result[tuple[found: bool, record: uint64], string] =
  ## §2.6, "Lookup": the key from the digest, a descent of B1 (through the
  ## page when attached), and every candidate record compared by key and full
  ## digest. A miss refreshes and descends once more before it answers.
  ps.resolveSlots(r)
  let h = ? ps.b1.readHeader(r)
  if not h.empty:
    ? ps.checkCash(r)
  let key = pageKey(digest)
  var psAddr = addr ps
  var rAddr = addr r
  let hits = ? ps.b1.lookup(r, key, proc (refv: uint64): Result[bool, string] =
    let count = recordCount(32, rAddr[].memberSize(psAddr[].recSlot))
    if psAddr[].recSlot < 0 or refv >= count:
      return ok(false)  # first seen after the last seal this reader read
    let b = ? rAddr[].readMember(psAddr[].recSlot, recordOffset(32, refv), 32)
    if readU64LE(b, 0) != key:
      return ok(false)
    for i in 0 ..< 16:
      if b[8 + i] != digest[i]:
        inc psAddr[].misses
        return ok(false)
    ok(true))
  if hits.len == 0:
    return ok((found: false, record: 0'u64))
  ok((found: true, record: hits[0]))

proc readHzi(ps: var PageStoreReader, r: var LiveReader):
    Result[seq[tuple[offset, cumulative: uint64]], string] =
  let size = r.memberSize(ps.hziSlot)
  if ps.hziSlot < 0 or size == 0'u64:
    return ok(newSeq[tuple[offset, cumulative: uint64]]())
  if size < uint64(HziHeaderSize):
    return err("cppages.hzi: shorter than its header")
  let all = ? r.readMember(ps.hziSlot, 0, int(size))
  if readU32LE(all, 0) == 0'u32:
    return err("cppages.hzi: chunk_size is 0")
  let version = uint16(all[4]) or (uint16(all[5]) shl 8)
  if version != HziIndexVersion:
    return err("cppages.hzi: index_version " & $version & ", not 2")
  if all[6] != 0 or all[7] != 0:
    return err("cppages.hzi: reserved is not 0")
  if (all.len - HziHeaderSize) mod HziEntrySize != 0:
    return err("cppages.hzi: the entry region is not a whole number of entries")
  let hzdSize = r.memberSize(ps.hzdSlot)
  var res: seq[tuple[offset, cumulative: uint64]]
  var o = HziHeaderSize
  while o < all.len:
    let e = (offset: readU64LE(all, o), cumulative: readU64LE(all, o + 8))
    if e.offset >= hzdSize:
      return err("cppages.hzi: an offset past the end of cppages.hzd")
    if res.len > 0 and (e.offset <= res[^1].offset or e.cumulative <= res[^1].cumulative):
      return err("cppages.hzi: offsets or cumulative counts do not increase")
    res.add e
    o += HziEntrySize
  ok(res)

proc frameBytes(ps: var PageStoreReader, r: var LiveReader,
                idx: seq[tuple[offset, cumulative: uint64]], i: int):
    Result[seq[byte], string] =
  if ps.frameCache.index == i:
    return ok(ps.frameCache.bytes)
  let start = idx[i].offset
  let stop =
    if i + 1 < idx.len: idx[i + 1].offset
    else: r.memberSize(ps.hzdSlot)
  var z = ? r.readMember(ps.hzdSlot, start, int(stop - start))
  if i + 1 >= idx.len:
    # The last frame ends at the end of its zstd frame, never at the data
    # member's Size (`ctfs-container.md` §6, step 4).
    let n = ZSTD_findFrameCompressedSize(addr z[0], csize_t(z.len))
    if ZSTD_isError(n) != 0:
      return err("cppages.hzd: frame " & $i & " is not a whole zstd frame")
    z.setLen(int(n))
  let want = idx[i].cumulative - (if i == 0: 0'u64 else: idx[i - 1].cumulative)
  let declared = ZSTD_getFrameContentSize(addr z[0], csize_t(z.len))
  if declared == ZSTD_CONTENTSIZE_UNKNOWN or declared == ZSTD_CONTENTSIZE_ERROR or
      uint64(declared) != want:
    return err("cppages.hzd: frame " & $i & " declares " & $uint64(declared) &
      " bytes, and its cppages.hzi step is " & $want)
  var outp = newSeq[byte](int(want))
  let n = ZSTD_decompress(addr outp[0], csize_t(want), addr z[0], csize_t(z.len))
  if ZSTD_isError(n) != 0 or uint64(n) != want:
    return err("cppages.hzd: frame " & $i & " inflates to " & $n &
      " bytes, not its cppages.hzi step " & $want)
  ps.frameCache = (index: i, bytes: outp)
  ok(outp)

proc pageBytes*(ps: var PageStoreReader, r: var LiveReader,
                record: uint64): Result[seq[byte], string] =
  ## The page of record `record`: its `heap_offset` mapped to its frame
  ## through `cppages.hzi` (the first frame whose `cumulative` exceeds it),
  ## the frame inflated, `page_size` bytes taken.
  ps.resolveSlots(r)
  if record >= ps.recordCount(r):
    return err("page store: record " & $record & " is past the published records")
  let rb = ? r.readMember(ps.recSlot, recordOffset(32, record), 32)
  let heapOff = readU64LE(rb, 24)
  let idx = ? ps.readHzi(r)
  var lo = 0
  var hi = idx.len
  while lo < hi:
    let mid = (lo + hi) div 2
    if idx[mid].cumulative > heapOff: hi = mid else: lo = mid + 1
  if lo >= idx.len:
    return err("page store: record " & $record & "'s heap offset " & $heapOff &
      " lies past the published frames")
  let frame = ? ps.frameBytes(r, idx, lo)
  let base = if lo == 0: 0'u64 else: idx[lo - 1].cumulative
  let at = int(heapOff - base)
  if at + int(ps.pageSize) > frame.len:
    return err("page store: record " & $record & "'s page runs past its frame")
  ok(frame[at ..< at + int(ps.pageSize)])
