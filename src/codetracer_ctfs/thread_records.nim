when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## C1, the two-level table over a clustered key (`ctfs-keyed-families.md`
## §5.8.2), as MCR's per-thread family uses it (`codetracer-specs`
## `CTFS-Binary-Format.md` §2.3, DR-3, `k = 6`): `threads.rec`, 64-byte
## records, 64 to a block, and `threads.top`, one `u64` slot per segment of
## `2^k` records, holding the segment's first record number plus 1, or 0.
##
## Record (64 bytes):
##
## | Offset | Size | Field | Changes |
## |---|---|---|---|
## | 0 | 8 | `events.size` | in place |
## | 8 | 8 | `events.map` | in place |
## | 16 | 8 | `index.size` | in place |
## | 24 | 8 | `index.map` | in place |
## | 32 | 8 | `geid_floor` | once |
## | 40 | 4 | `ct_tid: u32` | once |
## | 44 | 4 | `ring_tid: u32` | once |
## | 48 | 4 | `process: u32` | once |
## | 52 | 4 | `image: u32` | once |
## | 56 | 4 | `state: u32`: 0 empty (a reserved slot), 1 present | once, 0 to 1 |
##
## `record(ct_tid) = (top[ct_tid >> k] - 1) + (ct_tid & (2^k - 1))`.
##
## The seal, in the order §5.8.2 and §2.3 give:
##
## 1. each segment reserved since the last seal: `2^k` zero records appended,
##    then `threads.rec`'s `Size`;
## 2. the top table extended with zero slots up to each new segment's slot,
##    its `Size`, then each new slot in one 8-byte write (the records before
##    the slot; every slot fill-once);
## 3. each new thread's record written whole into its reserved slot, and only
##    then its `state` set present (the page's mirror slot first);
## 4. each record whose streams grew: the page's mirror slot (`events.map`
##    then `events.size`, `index.map` then `index.size`), then the four stream
##    words in one 32-byte write. The streams' own bytes reach the file at the
##    seal's first barrier, before any of this.
##
## A record's mirror slot holds five words: `events.size`, `events.map`,
## `index.size`, `index.map` and `state`, claimed in extents of 4096 records.

import std/[tables, algorithm]
import results
import ./types
import ./container
import ./keyed_store
import ./keyed_stream
import ./live_reader

export keyed_stream

const
  ThreadsRec* = "threads.rec"
  ThreadsTop* = "threads.top"
  ThreadRecordSize* = 64
  ThreadSegmentBits* = 6
  ThreadMirrorWords* = 5
  ThreadMirrorStoreOrder* = [4, 1, 0, 3, 2]
    ## The mirror slot's stores (§2.3): `state`, then `events.map` before
    ## `events.size`, then `index.map` before `index.size`: a prefix-valid
    ## record (`ctfs-container.md` §6, "the crash rule").

type
  ThreadInfo* = object
    ctTid*: uint32
    ringTid*: uint32
    process*: uint32
    image*: uint32
    geidFloor*: uint64

  ThreadRec = object
    record: uint64
    info: ThreadInfo
    events, index: KStream
    written: bool            ## its record is in the file
    pubEvents, pubIndex: KStream  ## as last published

  ThreadsWriter* = object
    rec*, top*: KMember
    k*: int
    segments: Table[uint64, uint64]   ## segment -> first record number
    newSegments: seq[uint64]
    threads: Table[uint32, ThreadRec]
    order: seq[uint32]               ## ctTids in creation order
    extents: Table[uint64, int]
    extentList: seq[MirrorExtent]

proc createThreadsFamily*(c: var Ctfs, k = ThreadSegmentBits): Result[ThreadsWriter, string] =
  var w = ThreadsWriter(k: k)
  w.rec = ? c.createKMember(ThreadsRec)
  w.top = ? c.createKMember(ThreadsTop)
  ok(w)

proc addThread*(w: var ThreadsWriter, info: ThreadInfo): Result[uint64, string] =
  ## Learn of a thread: its record is created at the next seal, reserving its
  ## segment first when the segment is new. Returns its record number.
  if info.ctTid in w.threads:
    return err("threads.rec: ct_tid " & $info.ctTid & " already has a record")
  let seg = uint64(info.ctTid) shr w.k
  if seg notin w.segments:
    var first = 0'u64
    for _, f in w.segments: first = max(first, f + (1'u64 shl w.k))
    w.segments[seg] = first
    w.newSegments.add seg
  let r = w.segments.getOrDefault(seg) + (uint64(info.ctTid) and ((1'u64 shl w.k) - 1))
  w.threads[info.ctTid] = ThreadRec(record: r, info: info)
  w.order.add info.ctTid
  ok(r)

proc appendEvents*(c: var Ctfs, w: var ThreadsWriter, ctTid: uint32,
                   chunk: openArray[byte]): Result[uint64, string] =
  ## Append a sealed chunk's bytes to the thread's `events` stream; returns
  ## its offset (for the `index` entry). Published at the next seal.
  if ctTid notin w.threads:
    return err("threads.rec: no thread with ct_tid " & $ctTid)
  var t = w.threads.getOrDefault(ctTid)
  let at = t.events.size
  ? c.appendStream(t.events, chunk, "the events stream of ct_tid " & $ctTid)
  w.threads[ctTid] = t
  ok(at)

proc appendIndex*(c: var Ctfs, w: var ThreadsWriter, ctTid: uint32,
                  bytes: openArray[byte]): Result[void, string] =
  if ctTid notin w.threads:
    return err("threads.rec: no thread with ct_tid " & $ctTid)
  var t = w.threads.getOrDefault(ctTid)
  ? c.appendStream(t.index, bytes, "the index stream of ct_tid " & $ctTid)
  w.threads[ctTid] = t
  ok()

proc recordBytes(t: ThreadRec, state: uint32): array[ThreadRecordSize, byte] =
  writeU64LE(result, 0, t.events.size)
  writeU64LE(result, 8, t.events.mapBlock)
  writeU64LE(result, 16, t.index.size)
  writeU64LE(result, 24, t.index.mapBlock)
  writeU64LE(result, 32, t.info.geidFloor)
  writeU32LE(result, 40, t.info.ctTid)
  writeU32LE(result, 44, t.info.ringTid)
  writeU32LE(result, 48, t.info.process)
  writeU32LE(result, 52, t.info.image)
  writeU32LE(result, 56, state)

proc mirrorOf(w: var ThreadsWriter, c: Ctfs, r: uint64): Result[MirrorExtent, void] =
  if c.page == nil: return err()
  let k = r div MirrorExtentRecords
  if k notin w.extents:
    let x = c.page.allocExtent(uint64(w.rec.entryIndex), k * MirrorExtentRecords,
      MirrorExtentRecords, ThreadMirrorWords)
    if x.isOk:
      w.extentList.add x.get()
      w.extents[k] = w.extentList.len - 1
    else:
      w.extents[k] = -1
  let i = w.extents.getOrDefault(k)
  if i < 0: return err()
  ok(w.extentList[i])

proc planSeal*(w: var ThreadsWriter, c: var Ctfs, p: var SealPlan): Result[void, string] =
  ## Add the family's seal to the plan (the module's doc, steps 1 to 4).
  let segRecords = 1'u64 shl w.k
  # 1. Reserved segments: zero records, then the record member's Size.
  var newSegs: seq[tuple[first, seg: uint64]]
  for seg in w.newSegments: newSegs.add (first: w.segments.getOrDefault(seg), seg: seg)
  newSegs.sort()
  for (first, seg) in newSegs:
    if recordCount(ThreadRecordSize, w.rec.size) != first:
      return err("threads.rec: segment " & $seg & " starts at record " & $first &
        ", and the member holds " & $recordCount(ThreadRecordSize, w.rec.size))
    var z: array[ThreadRecordSize, byte]
    for _ in 0'u64 ..< segRecords:
      discard ? c.appendRecord(w.rec, ThreadRecordSize, z)
  # 2. The top table: zero slots up to each new slot, its Size, then each
  #    slot in one write.
  var maxSlot = -1
  for (_, seg) in newSegs: maxSlot = max(maxSlot, int(seg))
  while int(w.top.size div 8) <= maxSlot:
    var z: array[8, byte]
    discard ? c.appendTo(w.top, z)
  p.barrier()
  p.publishEntries(c, [w.rec, w.top])
  for (_, seg) in newSegs:
    var b: array[8, byte]
    let v = w.segments.getOrDefault(seg) + 1
    writeU64LE(b, 0, v)
    ? p.inPlaceItem(c, w.top, seg * 8, b, SealItem(kind: sikIndex, name: w.top.name,
      record: seg * 8, words: @[(at: 0, v: v)]))
  w.newSegments.setLen(0)
  # 3. New records, written whole, then state present.
  for ctTid in w.order:
    var t = w.threads.getOrDefault(ctTid)
    let off = recordOffset(ThreadRecordSize, t.record)
    if not t.written:
      let whole = recordBytes(t, 0)
      var st: array[8, byte]
      writeU32LE(st, 0, 1'u32)
      let full = recordBytes(t, 1)
      let stateFirst = mutated("c1-state-before-record")
      if stateFirst:
        # Mutation arm: the record present before its words are written.
        ? p.inPlaceItem(c, w.rec, off + 56, st, SealItem(kind: sikRecord,
          name: w.rec.name, record: off, isNew: true, bytes: @full,
          words: wordsOf(full, 0)))
      ? p.inPlace(c, w.rec, off, whole.toOpenArray(0, 55))
      let x = w.mirrorOf(c, t.record)
      if x.isOk:
        p.mirrorOrdered(x.get().slotOffset(t.record), [t.events.size, t.events.mapBlock,
          t.index.size, t.index.mapBlock, 1'u64], ThreadMirrorStoreOrder)
      if not stateFirst:
        ? p.inPlaceItem(c, w.rec, off + 56, st, SealItem(kind: sikRecord,
          name: w.rec.name, record: off, isNew: true, bytes: @full,
          words: wordsOf(full, 0)))
      t.written = true
      t.pubEvents = t.events
      t.pubIndex = t.index
    elif t.events != t.pubEvents or t.index != t.pubIndex:
      # 4. Stream words: the mirror first, then one 32-byte write.
      let x = w.mirrorOf(c, t.record)
      if x.isOk:
        p.mirrorOrdered(x.get().slotOffset(t.record), [t.events.size, t.events.mapBlock,
          t.index.size, t.index.mapBlock, 1'u64], ThreadMirrorStoreOrder)
      let b = recordBytes(t, 1)
      ? p.inPlaceItem(c, w.rec, off, b.toOpenArray(0, 31), SealItem(kind: sikRecord,
        name: w.rec.name, record: off, words: wordsOf(b.toOpenArray(0, 31), 0)))
      t.pubEvents = t.events
      t.pubIndex = t.index
    w.threads[ctTid] = t
  ok()

proc seal*(w: var ThreadsWriter, c: var Ctfs): Result[void, string] =
  var p = initSealPlan(c)
  ? w.planSeal(c, p)
  c.executeSeal(p)

# ---------------------------------------------------------------------------
# The reader
# ---------------------------------------------------------------------------

type
  ThreadView* = object
    record*: uint64
    info*: ThreadInfo
    events*, index*: KStream

  ThreadsReader* = object
    k*: int
    recSlot, topSlot: int
    topCache: Table[uint64, uint64]

proc openThreadsReader*(r: LiveReader, k = ThreadSegmentBits): ThreadsReader =
  ThreadsReader(k: k, recSlot: r.findSlot(ThreadsRec), topSlot: r.findSlot(ThreadsTop))

proc readRecord*(t: var ThreadsReader, r: var LiveReader, rec: uint64):
    Result[tuple[present: bool, v: ThreadView], string] =
  ## Record `rec`: its in-place words from the page's mirror when attached
  ## and published there, otherwise read twice from the file until both agree.
  if t.recSlot < 0: t.recSlot = r.findSlot(ThreadsRec)
  if t.recSlot < 0 or rec >= recordCount(ThreadRecordSize, r.memberSize(t.recSlot)):
    return ok((present: false, v: ThreadView()))
  let off = recordOffset(ThreadRecordSize, rec)
  var b: seq[byte]
  var accepted = false
  for _ in 0 ..< RootReadRetries:
    let a = ? r.readMember(t.recSlot, off, ThreadRecordSize)
    let a2 = ? r.readMember(t.recSlot, off, ThreadRecordSize)
    if a == a2:
      b = a
      accepted = true
      break
  if not accepted:
    return err("threads.rec: record " & $rec & " did not read the same twice")
  var v = ThreadView(record: rec,
    events: KStream(size: readU64LE(b, 0), mapBlock: readU64LE(b, 8)),
    index: KStream(size: readU64LE(b, 16), mapBlock: readU64LE(b, 24)),
    info: ThreadInfo(geidFloor: readU64LE(b, 32), ctTid: readU32LE(b, 40),
      ringTid: readU32LE(b, 44), process: readU32LE(b, 48), image: readU32LE(b, 52)))
  var state = readU32LE(b, 56)
  let x = r.mirrorExtent(t.recSlot, rec)
  if x.isOk:
    var words: array[ThreadMirrorWords, uint64]
    for _ in 0 ..< RootReadRetries:
      case readSlot(r.page.view, x.get().slotOffset(rec), words)
      of srOk:
        v.events = KStream(size: words[0], mapBlock: words[1])
        v.index = KStream(size: words[2], mapBlock: words[3])
        state = uint32(words[4])
        break
      of srNever: break
      of srRetry: discard
  if state > 1'u32:
    return err("threads.rec: record " & $rec & " has state " & $state)
  ok((present: state == 1'u32, v: v))

proc lookupThread*(t: var ThreadsReader, r: var LiveReader, ctTid: uint32):
    Result[tuple[found: bool, v: ThreadView], string] =
  ## C1's lookup: the top slot of `ct_tid >> k` (kept for good once nonzero),
  ## then the record, confirmed by its `ct_tid` and its state. A slot past the
  ## top table, a slot of 0, a record past the record count, a key mismatch or
  ## an empty state each mean "absent".
  if t.topSlot < 0: t.topSlot = r.findSlot(ThreadsTop)
  if t.topSlot < 0: return ok((found: false, v: ThreadView()))
  let seg = uint64(ctTid) shr t.k
  var slot = t.topCache.getOrDefault(seg)
  if slot == 0:
    if seg * 8 + 8 > r.memberSize(t.topSlot):
      return ok((found: false, v: ThreadView()))
    slot = ? r.readMemberU64(t.topSlot, seg * 8)
    if slot == 0: return ok((found: false, v: ThreadView()))
    t.topCache[seg] = slot
  let rec = slot - 1 + (uint64(ctTid) and ((1'u64 shl t.k) - 1))
  let x = ? t.readRecord(r, rec)
  if not x.present or x.v.info.ctTid != ctTid:
    return ok((found: false, v: ThreadView()))
  ok((found: true, v: x.v))

proc enumerateThreads*(t: var ThreadsReader, r: var LiveReader): Result[seq[ThreadView], string] =
  ## Every present record, in record order, skipping the empty ones.
  var res: seq[ThreadView]
  if t.recSlot < 0: t.recSlot = r.findSlot(ThreadsRec)
  if t.recSlot < 0: return ok(res)
  for rec in 0'u64 ..< recordCount(ThreadRecordSize, r.memberSize(t.recSlot)):
    let x = ? t.readRecord(r, rec)
    if x.present: res.add x.v
  ok(res)
