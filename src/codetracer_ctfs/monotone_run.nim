when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## M1, the monotone run (`ctfs-keyed-families.md` §5.8.7; F4 when keys are
## inserted in ascending order).
##
## Two members: `<name>.rec`, D1 records in key order, and `<name>.fen`, one
## `u64` per record block, the key of that block's first record, appended when
## that record is appended (fill-once). A record carries its key in its first
## word and its state in its second (`0` empty, `1` present, `2` tombstone,
## §5.8.1); the rest is the producer's value. A tombstone is an in-place
## update of the state word, through the page.
##
## Predecessor of `t`: binary-search the fences for the last one at or below
## `t`, scan that block for the last record whose key is at or below `t`, and
## if it is a tombstone continue with the predecessor of its key minus 1. An
## insert whose key does not exceed the last key is refused, naming the
## member. Dead space: none.

import std/[tables]
import results
import ./types
import ./container
import ./keyed_store
import ./live_reader

const
  MonotoneStatePresent* = 1'u64
  MonotoneStateTombstone* = 2'u64

type
  MonotoneWriter* = object
    name*: string
    rec*, fen*: KMember
    recSize*: int
    lastKey: uint64
    any: bool
    pending: seq[seq[byte]]
    count*: uint64
    tombstones: seq[uint64]
    mirrorExt: Table[uint64, int]
    mirrorList: seq[MirrorExtent]

proc createMonotoneRun*(c: var Ctfs, name: string, recSize: int): Result[MonotoneWriter, string] =
  if recSize < 16 or recSize mod 8 != 0:
    return err("M1 " & name & ": a record of " & $recSize & " bytes cannot carry a key and a state")
  var w = MonotoneWriter(name: name, recSize: recSize)
  w.rec = ? c.createKMember(name & ".rec")
  w.fen = ? c.createKMember(name & ".fen")
  ok(w)

proc insert*(w: var MonotoneWriter, key: uint64, value: openArray[byte]): Result[uint64, string] =
  ## Append the record of `key`; refused unless `key` exceeds the last key.
  if w.any and key <= w.lastKey:
    return err("M1 " & w.name & ".rec: key " & $key & " does not exceed the last key " &
      $w.lastKey & "; M1 applies only when keys arrive in ascending order")
  if value.len > w.recSize - 16:
    return err("M1 " & w.name & ".rec: a value of " & $value.len & " bytes")
  var r = newSeq[byte](w.recSize)
  writeU64LE(r, 0, key)
  writeU64LE(r, 8, MonotoneStatePresent)
  for i in 0 ..< value.len: r[16 + i] = value[i]
  w.pending.add r
  w.lastKey = key
  w.any = true
  result = ok(w.count)
  inc w.count

proc tombstone*(w: var MonotoneWriter, record: uint64): Result[void, string] =
  if record >= w.count:
    return err("M1 " & w.name & ".rec: no record " & $record)
  w.tombstones.add record
  ok()

proc planSeal*(w: var MonotoneWriter, c: var Ctfs, p: var SealPlan): Result[void, string] =
  let pb = perBlock(w.recSize)
  for r in w.pending:
    let n = ? c.appendRecord(w.rec, w.recSize, r)
    if n mod pb == 0:
      var f: array[8, byte]
      for i in 0 ..< 8: f[i] = r[i]
      discard ? c.appendTo(w.fen, f)
  w.pending.setLen(0)
  p.barrier()
  # The records before the fences that name their blocks.
  p.publishEntries(c, [w.rec])
  p.publishEntries(c, [w.fen])
  for t in w.tombstones:
    let off = recordOffset(w.recSize, t)
    if c.page != nil:
      let k = t div MirrorExtentRecords
      if k notin w.mirrorExt:
        let x = c.page.allocExtent(uint64(w.rec.entryIndex), k * MirrorExtentRecords,
          MirrorExtentRecords, 1)
        w.mirrorExt[k] = if x.isOk: (w.mirrorList.add x.get(); w.mirrorList.len - 1) else: -1
      let i = w.mirrorExt.getOrDefault(k)
      if i >= 0: p.mirror(w.mirrorList[i].slotOffset(t), [MonotoneStateTombstone])
    var s: array[8, byte]
    writeU64LE(s, 0, MonotoneStateTombstone)
    ? p.inPlaceItem(c, w.rec, off + 8, s, SealItem(kind: sikRecord, name: w.rec.name,
      record: off, words: @[(at: 8, v: MonotoneStateTombstone)]))
  w.tombstones.setLen(0)
  ok()

proc seal*(w: var MonotoneWriter, c: var Ctfs): Result[void, string] =
  var p = initSealPlan(c)
  ? w.planSeal(c, p)
  c.executeSeal(p)

proc predecessor*(r: var LiveReader, name: string, recSize: int, t: uint64):
    Result[tuple[found: bool, record: uint64, key: uint64], string] =
  ## The present record with the largest key at or below `t`.
  let rs = r.findSlot(name & ".rec")
  let fs = r.findSlot(name & ".fen")
  if rs < 0 or fs < 0: return ok((found: false, record: 0'u64, key: 0'u64))
  let nrec = recordCount(recSize, r.memberSize(rs))
  let pb = perBlock(recSize)
  let nfen = min(r.memberSize(fs) div 8, (nrec + pb - 1) div pb)
  var target = t
  while true:
    var lo = 0'u64
    var hi = nfen
    while lo < hi:
      let mid = (lo + hi) div 2
      if (? r.readMemberU64(fs, mid * 8)) <= target: lo = mid + 1 else: hi = mid
    if lo == 0: return ok((found: false, record: 0'u64, key: 0'u64))
    let blk = lo - 1
    var best = -1'i64
    var bestKey = 0'u64
    for i in 0'u64 ..< pb:
      let rec = blk * pb + i
      if rec >= nrec: break
      let k = ? r.readMemberU64(rs, recordOffset(recSize, rec))
      if k > target: break
      best = int64(rec)
      bestKey = k
    if best < 0: return ok((found: false, record: 0'u64, key: 0'u64))
    let off = recordOffset(recSize, uint64(best))
    var state = ? r.readMemberU64(rs, off + 8)
    let x = r.mirrorExtent(rs, uint64(best))
    if x.isOk:
      var w: array[1, uint64]
      if readSlot(r.page.view, x.get().slotOffset(uint64(best)), w) == srOk: state = w[0]
    if state == MonotoneStatePresent:
      return ok((found: true, record: uint64(best), key: bestKey))
    if state != MonotoneStateTombstone:
      return err("M1 " & name & ".rec: record " & $best & " has state " & $state)
    if bestKey == 0: return ok((found: false, record: 0'u64, key: 0'u64))
    target = bestKey - 1
