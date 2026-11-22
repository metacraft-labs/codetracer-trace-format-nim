when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## D1 and D2 (`ctfs-keyed-families.md` §5.8.1) as MCR's dense families use
## them (`codetracer-specs` `CTFS-Binary-Format.md` §2.4 and §2.7):
##
## - **Snapshots** (DR-4): 272-byte records in `snap.rec`, 15 per block, one
##   per snapshot in the order taken (the writer-assigned ordinal), with the
##   `positions` value in the D2 heap `snap.pos`, 8-byte aligned. A record is
##   written whole, once, after all its parts; nothing in it changes in place.
## - **Bundled files** (DR-5): 64-byte records in `bundle.rec`, record `i` the
##   bundled file of `filemap.bin` entry `i`, written whole, once.
##
## A snapshot's or bundled file's parts are keyed streams (`keyed_stream`)
## the caller appends first; a seal writes the `positions` values, then the
## records, each member's `Size` after the bytes it covers ("Publication
## order": a reader that finds a record finds every part).

import results
import ./types
import ./container
import ./keyed_store
import ./keyed_stream
import ./live_reader

export keyed_stream

const
  SnapRec* = "snap.rec"
  SnapPos* = "snap.pos"
  BundleRec* = "bundle.rec"
  SnapRecordSize* = 272
  BundleRecordSize* = 64
  SnapParts* = 7
  SnapUnknownGeid* = high(uint64)

type
  SnapKind* = enum
    skPeriodic = 0, skPrein = 1, skEntry = 2, skPostl = 3

  SnapPartSlot* = enum
    spLayout = 0, spHashes = 1, spRegs = 2, spMemory = 3, spRegsFull = 4,
    spSignals = 5, spState = 6

  PartForm* = enum
    pfAbsent = 0, pfRaw = 1, pfCompressed = 2

  SnapPart* = object
    form*: PartForm
    data*, index*: KStream

  Snapshot* = object
    kind*: SnapKind
    id*: uint64
    geid*: uint64
    positions*: seq[byte]
    parts*: array[SnapParts, SnapPart]

  SnapshotWriter* = object
    rec*, pos*: KMember
    pending: seq[Snapshot]
    count*: uint64

proc createSnapshotFamily*(c: var Ctfs): Result[SnapshotWriter, string] =
  var w: SnapshotWriter
  w.rec = ? c.createKMember(SnapRec)
  w.pos = ? c.createKMember(SnapPos)
  ok(w)

proc encodePositions*(positions: openArray[tuple[ctTid: uint32, position: uint64]],
                      parked: openArray[tuple[ctTid, parkKind: uint32, sysPre: uint64]]): seq[byte] =
  ## `positions`' layout (§2.4): `n_positions: u32 | 0 | (ct_tid: u32 | 0 |
  ## position: u64) * n | n_parked: u32 | 0 | (ct_tid | park_kind | sys_pre)
  ## * n_parked`.
  result = newSeq[byte](8 + 16 * positions.len + 8 + 16 * parked.len)
  writeU32LE(result, 0, uint32(positions.len))
  var o = 8
  for p in positions:
    writeU32LE(result, o, p.ctTid)
    writeU64LE(result, o + 8, p.position)
    o += 16
  writeU32LE(result, o, uint32(parked.len))
  o += 8
  for p in parked:
    writeU32LE(result, o, p.ctTid)
    writeU32LE(result, o + 4, p.parkKind)
    writeU64LE(result, o + 8, p.sysPre)
    o += 16

proc partsWord(s: Snapshot): uint32 =
  for p in 0 ..< SnapParts:
    result = result or (uint32(ord(s.parts[p].form)) shl (2 * p))

proc snapshotRecordBytes*(s: Snapshot, posOff, posLen: uint64): array[SnapRecordSize, byte] =
  writeU32LE(result, 0, uint32(ord(s.kind)))
  writeU32LE(result, 4, s.partsWord())
  writeU64LE(result, 8, s.id)
  writeU64LE(result, 16, s.geid)
  writeU64LE(result, 24, posOff)
  writeU64LE(result, 32, posLen)
  for p in 0 ..< SnapParts:
    let b = 48 + 32 * p
    writeU64LE(result, b, s.parts[p].data.size)
    writeU64LE(result, b + 8, s.parts[p].data.mapBlock)
    writeU64LE(result, b + 16, s.parts[p].index.size)
    writeU64LE(result, b + 24, s.parts[p].index.mapBlock)

proc addSnapshot*(w: var SnapshotWriter, s: Snapshot): uint64 =
  ## Buffer a snapshot whose parts are already appended; its ordinal is
  ## returned and it is published at the next seal.
  w.pending.add s
  result = w.count
  inc w.count

proc planSeal*(w: var SnapshotWriter, c: var Ctfs, p: var SealPlan): Result[void, string] =
  if w.pending.len == 0: return ok()
  var offs: seq[(uint64, uint64)]
  for s in w.pending:
    ? c.padTo(w.pos, 8)
    let at = if s.positions.len > 0: ? c.appendTo(w.pos, s.positions) else: 0'u64
    offs.add (at, uint64(s.positions.len))
  p.barrier()
  p.publishEntries(c, [w.pos])
  for i, s in w.pending:
    discard ? c.appendRecord(w.rec, SnapRecordSize,
      snapshotRecordBytes(s, offs[i][0], offs[i][1]))
  w.pending.setLen(0)
  p.barrier()
  p.publishEntries(c, [w.rec])
  ok()

proc seal*(w: var SnapshotWriter, c: var Ctfs): Result[void, string] =
  var p = initSealPlan(c)
  ? w.planSeal(c, p)
  c.executeSeal(p)

# --- Bundled files ---------------------------------------------------------

type
  BundledFile* = object
    form*: PartForm
    data*, index*: KStream

  BundleWriter* = object
    rec*: KMember
    pending: seq[BundledFile]
    count*: uint64

proc createBundleFamily*(c: var Ctfs): Result[BundleWriter, string] =
  var w: BundleWriter
  w.rec = ? c.createKMember(BundleRec)
  ok(w)

proc addBundled*(w: var BundleWriter, f: BundledFile): uint64 =
  ## The record of the next `filemap.bin` entry (an empty one, `form` absent,
  ## for a file whose bytes are not bundled).
  w.pending.add f
  result = w.count
  inc w.count

proc planSeal*(w: var BundleWriter, c: var Ctfs, p: var SealPlan): Result[void, string] =
  if w.pending.len == 0: return ok()
  for f in w.pending:
    let n = recordCount(BundleRecordSize, w.rec.size)
    var b: array[BundleRecordSize, byte]
    if f.form != pfAbsent:
      writeU64LE(b, 0, f.data.size)
      writeU64LE(b, 8, f.data.mapBlock)
      writeU64LE(b, 16, f.index.size)
      writeU64LE(b, 24, f.index.mapBlock)
    writeU64LE(b, 32, n)
    writeU32LE(b, 40, uint32(ord(f.form)))
    discard ? c.appendRecord(w.rec, BundleRecordSize, b)
  w.pending.setLen(0)
  p.barrier()
  p.publishEntries(c, [w.rec])
  ok()

proc seal*(w: var BundleWriter, c: var Ctfs): Result[void, string] =
  var p = initSealPlan(c)
  ? w.planSeal(c, p)
  c.executeSeal(p)

# --- Readers ---------------------------------------------------------------

type
  SnapshotView* = object
    ordinal*: uint64
    snap*: Snapshot
    posOffset*, posLength*: uint64

proc snapshotCount*(r: LiveReader): uint64 =
  let s = r.findSlot(SnapRec)
  if s < 0: 0'u64 else: recordCount(SnapRecordSize, r.memberSize(s))

proc partSetRefusal(ordinal: uint64, s: Snapshot): string =
  ## §2.4, "The admitted part sets": exactly one of P1 to P4 of `layout`,
  ## `hashes`, `regs`, `memory`, `regs_full`; `signals` and `state` only on a
  ## periodic record.
  var present: set[SnapPartSlot]
  for p in SnapPartSlot:
    if s.parts[ord(p)].form != pfAbsent: present.incl p
  let core = present - {spSignals, spState}
  let sets = [{spLayout, spHashes, spRegs}, {spLayout, spHashes, spRegs, spMemory},
              {spLayout, spRegs, spMemory}, {spMemory, spRegsFull}]
  if core notin sets:
    return "snapshot " & $ordinal & ": the parts " & $core & " are none of the sets P1 to P4"
  if s.kind != skPeriodic and (present * {spSignals, spState}).card > 0:
    return "snapshot " & $ordinal & ": signals or state on a record that is not periodic"
  ""

proc readSnapshot*(r: var LiveReader, ordinal: uint64): Result[SnapshotView, string] =
  ## Snapshot `ordinal`, with every reader check of §2.4, each refusal naming
  ## the ordinal (and the part).
  let slot = r.findSlot(SnapRec)
  if slot < 0 or ordinal >= recordCount(SnapRecordSize, r.memberSize(slot)):
    return err("snapshot " & $ordinal & " is not published")
  let b = ? r.readMember(slot, recordOffset(SnapRecordSize, ordinal), SnapRecordSize)
  let kind = readU32LE(b, 0)
  if kind > 3'u32:
    return err("snapshot " & $ordinal & ": kind " & $kind & " is not periodic, prein, entry or postl")
  let parts = readU32LE(b, 4)
  if (parts shr 14) != 0'u32:
    return err("snapshot " & $ordinal & ": parts bits 14 to 31 are set")
  var v = SnapshotView(ordinal: ordinal, posOffset: readU64LE(b, 24),
                       posLength: readU64LE(b, 32))
  v.snap.kind = SnapKind(kind)
  v.snap.id = readU64LE(b, 8)
  v.snap.geid = readU64LE(b, 16)
  if readU64LE(b, 40) != 0'u64:
    return err("snapshot " & $ordinal & ": bytes 40 to 47 are not 0")
  for p in 0 ..< SnapParts:
    let f = (parts shr (2 * p)) and 3'u32
    if f == 3'u32:
      return err("snapshot " & $ordinal & ": part " & $SnapPartSlot(p) & " has form 3")
    let o = 48 + 32 * p
    let part = SnapPart(form: PartForm(f),
      data: KStream(size: readU64LE(b, o), mapBlock: readU64LE(b, o + 8)),
      index: KStream(size: readU64LE(b, o + 16), mapBlock: readU64LE(b, o + 24)))
    if part.form == pfAbsent and (part.data != KStream() or part.index != KStream()):
      return err("snapshot " & $ordinal & ": absent part " & $SnapPartSlot(p) & " has a nonzero word")
    if part.form == pfRaw and part.index != KStream():
      return err("snapshot " & $ordinal & ": raw part " & $SnapPartSlot(p) & " has a nonzero index")
    v.snap.parts[p] = part
  let refusal = partSetRefusal(ordinal, v.snap)
  if refusal.len > 0: return err(refusal)
  if v.posLength > 0'u64:
    let ps = r.findSlot(SnapPos)
    if ps < 0 or v.posOffset + v.posLength > r.memberSize(ps):
      return err("snapshot " & $ordinal & ": positions extent past snap.pos's Size")
    v.snap.positions = ? r.readMember(ps, v.posOffset, int(v.posLength))
  ok(v)

proc readBundled*(r: var LiveReader, file: uint64): Result[BundledFile, string] =
  let slot = r.findSlot(BundleRec)
  if slot < 0 or file >= recordCount(BundleRecordSize, r.memberSize(slot)):
    return err("bundle.rec: no record for filemap.bin entry " & $file)
  let b = ? r.readMember(slot, recordOffset(BundleRecordSize, file), BundleRecordSize)
  if readU64LE(b, 32) != file:
    return err("bundle.rec: record " & $file & " names file " & $readU64LE(b, 32))
  let form = readU32LE(b, 40)
  if form > 2'u32:
    return err("bundle.rec: record " & $file & " has form " & $form)
  result = ok(BundledFile(form: PartForm(form),
    data: KStream(size: readU64LE(b, 0), mapBlock: readU64LE(b, 8)),
    index: KStream(size: readU64LE(b, 16), mapBlock: readU64LE(b, 24))))
  if form == 0'u32 and (result.get().data != KStream() or result.get().index != KStream()):
    return err("bundle.rec: record " & $file & " is not bundled and has a nonzero word")
  if form == 1'u32 and result.get().index != KStream():
    return err("bundle.rec: raw record " & $file & " has a nonzero index")
