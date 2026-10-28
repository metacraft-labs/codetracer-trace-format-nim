when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## MCR's live correlation index (`codetracer-specs` `CTFS-Binary-Format.md`
## §2.8, DR-6): H1 live (`h1.nim`) over records with V2 bucket extents
## (`ctfs-keyed-families.md` §5.8.6), and S3 (`s3.nim`) built at close.
##
## | Member | Holds |
## |---|---|
## | `corrmark.rec` | one 48-byte record per distinct key, in first-seen order |
## | `corrmark.ext` | the buckets: extent chains of 256-byte extents, four 60-byte entries each |
## | `corrmark.hdr`, `.bkt`, `.ovf` | H1 over `(key, record)` |
## | `corrmark.s3` | S3 over `(key, record)`, built at close |
##
## Record: `key` (0), `head` (8), `tail` (16, in place), `n` (24, in place),
## `bytes = 60 * n` (32, in place), 0 (40). Its mirror slot holds `tail`,
## `bytes` and `n`, stored in that order, `n` last: a reader trusts `n`, never
## an extent's `used`.
##
## Extent (256 bytes): `next` (0; the heap offset of the next extent, or 0),
## `used: u32` (8), 0 (12), entries from 16. A successor extent is always
## appended after its predecessor, so it never lies at offset 0.
##
## Per marker: a new key gets a new extent, written whole with the entry, a
## record and an H1 insert; a repeated key writes the entry into the tail
## extent's next free slot, or appends a new extent and links it from the old
## tail (a fill-once store), and then updates its record in place. At each
## seal: the extents, then the records, then H1's phases.

import std/[tables]
import results
import ./types
import ./container
import ./keyed_store
import ./live_reader
import ./h1
import ./s3

const
  CorrRec* = "corrmark.rec"
  CorrExt* = "corrmark.ext"
  CorrS3* = "corrmark.s3"
  CorrPrefix* = "corrmark"
  CorrRecordSize* = 48
  CorrExtentSize* = 256
  CorrEntrySize* = 60
  CorrPerExtent* = (CorrExtentSize - 16) div CorrEntrySize
  CorrMirrorWords* = 3

type
  CorrEntry* = array[CorrEntrySize, byte]

  CorrExtentMem = object
    off: uint64
    entries: seq[CorrEntry]
    next: uint64
    written: bool          ## in the file
    writtenEntries: int
    writtenNext: uint64

  CorrKey = object
    record: uint64
    key: uint64
    extents: seq[int]      ## indices into `extents`, head first
    n: uint64
    written: bool
    pubTail, pubN: uint64

  CorrmarkWriter* = object
    rec*, ext*: KMember
    h1*: H1Writer
    extents: seq[CorrExtentMem]
    keys: seq[CorrKey]
    byKey: Table[uint64, int]
    heapEnd: uint64
    mirrorExt: Table[uint64, int]
    mirrorList: seq[MirrorExtent]
    closed*: bool

proc createCorrmark*(c: var Ctfs): Result[CorrmarkWriter, string] =
  var w: CorrmarkWriter
  w.rec = ? c.createKMember(CorrRec)
  w.ext = ? c.createKMember(CorrExt)
  w.h1 = ? c.createH1(CorrPrefix)
  ok(w)

proc addMarker*(w: var CorrmarkWriter, key: uint64, entry: CorrEntry): Result[void, string] =
  ## Declare one entry under `key` (`XXH64` of the correlation key); in memory
  ## until the next seal.
  if w.closed: return err("corrmark: the index is closed")
  if key notin w.byKey:
    let r = uint64(w.keys.len)
    w.extents.add CorrExtentMem(off: w.heapEnd, entries: @[entry])
    w.heapEnd += CorrExtentSize
    w.keys.add CorrKey(record: r, key: key, extents: @[w.extents.len - 1], n: 1)
    w.byKey[key] = w.keys.len - 1
    w.h1.insert(key, r)
    return ok()
  let k = w.byKey.getOrDefault(key)
  let t = w.keys[k].extents[^1]
  if w.extents[t].entries.len < CorrPerExtent:
    w.extents[t].entries.add entry
  else:
    w.extents.add CorrExtentMem(off: w.heapEnd, entries: @[entry])
    w.heapEnd += CorrExtentSize
    w.extents[t].next = w.extents[^1].off
    w.keys[k].extents.add w.extents.len - 1
  inc w.keys[k].n
  ok()

proc extentBytes(e: CorrExtentMem): array[CorrExtentSize, byte] =
  writeU64LE(result, 0, e.next)
  writeU32LE(result, 8, uint32(e.entries.len))
  for i, en in e.entries:
    for b in 0 ..< CorrEntrySize: result[16 + CorrEntrySize * i + b] = en[b]

proc recordBytes(k: CorrKey, w: CorrmarkWriter): array[CorrRecordSize, byte] =
  writeU64LE(result, 0, k.key)
  writeU64LE(result, 8, w.extents[k.extents[0]].off)
  writeU64LE(result, 16, w.extents[k.extents[^1]].off)
  writeU64LE(result, 24, k.n)
  writeU64LE(result, 32, k.n * CorrEntrySize)

proc mirrorOf(w: var CorrmarkWriter, c: Ctfs, r: uint64): Result[MirrorExtent, void] =
  if c.page == nil: return err()
  let k = r div MirrorExtentRecords
  if k notin w.mirrorExt:
    let x = c.page.allocExtent(uint64(w.rec.entryIndex), k * MirrorExtentRecords,
      MirrorExtentRecords, CorrMirrorWords)
    w.mirrorExt[k] = if x.isOk: (w.mirrorList.add x.get(); w.mirrorList.len - 1) else: -1
  let i = w.mirrorExt.getOrDefault(k)
  if i < 0: return err()
  ok(w.mirrorList[i])

proc planSeal*(w: var CorrmarkWriter, c: var Ctfs, p: var SealPlan): Result[void, string] =
  ## The extents, then the records, then H1's phases.
  # 1. New extents appended whole (in heap order); in pre-existing extents,
  #    new entry slots and `used`, then each changed `next` (fill-once).
  for i in 0 ..< w.extents.len:
    if not w.extents[i].written:
      if w.ext.size != w.extents[i].off:
        return err("corrmark.ext: extent at " & $w.extents[i].off &
          " would not land at its offset")
      discard ? c.appendTo(w.ext, extentBytes(w.extents[i]))
  p.barrier()
  var links: seq[int]
  let countFirst = mutated("corr-count-before-entries")
  var deferredEntries: SealPlan
  for i in 0 ..< w.extents.len:
    let e = w.extents[i]
    if not e.written: continue
    if e.entries.len > e.writtenEntries and countFirst:
      # Mutation arm: the entries of a repeated key reach the extent only
      # after its record's `n` is published.
      var sb: seq[byte]
      for j in e.writtenEntries ..< e.entries.len: sb.add e.entries[j]
      ? deferredEntries.inPlace(c, w.ext, e.off + 16 + uint64(CorrEntrySize * e.writtenEntries), sb)
    elif e.entries.len > e.writtenEntries:
      var sb: seq[byte]
      for j in e.writtenEntries ..< e.entries.len: sb.add e.entries[j]
      ? p.inPlace(c, w.ext, e.off + 16 + uint64(CorrEntrySize * e.writtenEntries), sb)
      var u: array[8, byte]
      writeU32LE(u, 0, uint32(e.entries.len))
      ? p.inPlace(c, w.ext, e.off + 8, u)
    if e.next != e.writtenNext: links.add i
  for i in links:
    var nb: array[8, byte]
    writeU64LE(nb, 0, w.extents[i].next)
    ? p.inPlaceItem(c, w.ext, w.extents[i].off, nb, SealItem(kind: sikIndex,
      name: w.ext.name, record: w.extents[i].off, words: @[(at: 0, v: w.extents[i].next)]))
  p.publishEntries(c, [w.ext])
  # 2. New records, then the changed words of existing ones (mirror first).
  for k in w.keys.mitems:
    if not k.written:
      discard ? c.appendRecord(w.rec, CorrRecordSize, recordBytes(k, w))
  p.barrier()
  p.publishEntries(c, [w.rec])
  for k in w.keys.mitems:
    let tail = w.extents[k.extents[^1]].off
    if not k.written:
      k.written = true
    elif tail != k.pubTail or k.n != k.pubN:
      let x = w.mirrorOf(c, k.record)
      if x.isOk:
        p.mirror(x.get().slotOffset(k.record), [tail, k.n * CorrEntrySize, k.n])
      let rb = recordBytes(k, w)
      ? p.inPlaceItem(c, w.rec, recordOffset(CorrRecordSize, k.record) + 16,
        rb.toOpenArray(16, 39), SealItem(kind: sikRecord, name: w.rec.name,
        record: recordOffset(CorrRecordSize, k.record),
        words: @[(at: 16, v: tail), (at: 24, v: k.n), (at: 32, v: k.n * CorrEntrySize)]))
    k.pubTail = tail
    k.pubN = k.n
  for op in deferredEntries.ops: p.ops.add op
  for e in w.extents.mitems:
    e.written = true
    e.writtenEntries = e.entries.len
    e.writtenNext = e.next
  # 3. H1.
  ? w.h1.planSeal(c, p)
  ok()

proc seal*(w: var CorrmarkWriter, c: var Ctfs): Result[void, string] =
  var p = initSealPlan(c)
  ? w.planSeal(c, p)
  c.executeSeal(p)

proc closeIndex*(w: var CorrmarkWriter, c: var Ctfs): Result[void, string] =
  ## The last seal, then S3 over every `(key, record)`, written once.
  var p = initSealPlan(c)
  ? w.planSeal(c, p)
  var s3m = ? c.createKMember(CorrS3)
  var entries: seq[tuple[key, refv: uint64]]
  for k in w.keys: entries.add (key: k.key, refv: k.record)
  ? planS3(c, s3m, entries, p)
  w.closed = true
  c.executeSeal(p)

# ---------------------------------------------------------------------------
# The reader
# ---------------------------------------------------------------------------

type
  CorrmarkReader* = object
    h1*: H1Reader
    recSlot, extSlot, s3Slot: int

proc openCorrmarkReader*(r: LiveReader): CorrmarkReader =
  CorrmarkReader(h1: openH1Reader(r, CorrPrefix), recSlot: r.findSlot(CorrRec),
    extSlot: r.findSlot(CorrExt), s3Slot: r.findSlot(CorrS3))

proc indexed*(cr: CorrmarkReader): bool =
  ## A recording that declared no marker has no `corrmark.*` member: "not
  ## indexed", distinct from a miss.
  cr.recSlot >= 0

proc lookupEntries*(cr: var CorrmarkReader, r: var LiveReader, key: uint64):
    Result[seq[CorrEntry], string] =
  ## §2.8, "Lookup": S3 when `corrmark.s3` exists, H1 otherwise; for each
  ## candidate record whose key matches, its `n` entries from its extents.
  var res: seq[CorrEntry]
  if cr.recSlot < 0: return ok(res)
  let cands =
    if cr.s3Slot >= 0 and r.memberSize(cr.s3Slot) > 0'u64:
      ? s3Candidates(r, cr.s3Slot, key)
    else:
      ? cr.h1.candidates(r, key)
  let count = recordCount(CorrRecordSize, r.memberSize(cr.recSlot))
  for rec in cands:
    if rec >= count: continue
    let off = recordOffset(CorrRecordSize, rec)
    let b = ? r.readMember(cr.recSlot, off, CorrRecordSize)
    if readU64LE(b, 0) != key: continue
    let head = readU64LE(b, 8)
    var n = readU64LE(b, 24)
    let x = r.mirrorExtent(cr.recSlot, rec)
    if x.isOk:
      var w: array[CorrMirrorWords, uint64]
      for _ in 0 ..< RootReadRetries:
        case readSlot(r.page.view, x.get().slotOffset(rec), w)
        of srOk:
          n = w[2]
          break
        of srNever: break
        of srRetry: discard
    var e = head
    var taken = 0'u64
    var hops = 0'u64
    let extSize = r.memberSize(cr.extSlot)
    while taken < n:
      if e + CorrExtentSize > extSize:
        return err("corrmark.ext: record " & $rec & "'s chain leaves the heap")
      let eb = ? r.readMember(cr.extSlot, e, CorrExtentSize)
      let here = min(uint64(CorrPerExtent), n - taken)
      for i in 0'u64 ..< here:
        var en: CorrEntry
        for k in 0 ..< CorrEntrySize: en[k] = eb[16 + CorrEntrySize * int(i) + k]
        res.add en
      taken += here
      if taken < n:
        e = readU64LE(eb, 0)
        inc hops
        if e == 0'u64 or hops > extSize div CorrExtentSize:
          return err("corrmark.ext: record " & $rec & " has " & $n &
            " entries and its chain ends after " & $taken)
  ok(res)
