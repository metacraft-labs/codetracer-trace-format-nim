## D1 with a D2 heap (`ctfs-keyed-families.md` §5.8.1) as MCR's snapshots
## (`codetracer-specs` `CTFS-Binary-Format.md` §2.4: `snap.rec`, 272-byte
## records, and `snap.pos`) and bundled files (§2.7: `bundle.rec`), and M1,
## the monotone run (§5.8.7).
##
## Checks, at every write prefix (file-only and attached), after real kills,
## and on round trips:
## 1. every published snapshot record is its snapshot: kind, id, geid, the
##    part set and every part's bytes through its keyed streams, and its
##    `positions` value from the heap (a reader that finds a record finds
##    every part, §2.4 "Publication order");
## 2. every snapshot a completed seal published is there;
## 3. the reader's refusals: a part set that is none of P1 to P4, signals on a
##    boundary record, a nonzero word in an absent slot, a nonzero index in a
##    raw slot, a kind past 3, each naming the ordinal;
## 4. bundled files: record `i` is file `i`, a mismatch refused;
## 5. M1: predecessor queries agree with a sorted model, tombstones skipped,
##    and an insert that does not exceed the last key refused, naming the
##    member.
##
## Null prediction (mutation arm, must be caught): `size-before-record`, a
## member's Size published before the bytes it covers (a record below the
## published Size that is not its snapshot, or a part that does not read).

import std/[os, osproc, strutils, tables, random, algorithm]
import results
import codetracer_ctfs/[types, container, streaming, keyed_store, live_reader,
                        snapshot_records, monotone_run, mutation_arms]
import ./keyed_harness

proc partBytes(ordinal, p: int): seq[byte] =
  result = newSeq[byte](100 + (ordinal * 37 + p * 911) mod 9000)
  for i in 0 ..< result.len: result[i] = byte((i * 3 + ordinal * 7 + p) and 0xff)

proc snapOf(ordinal: int, c: var Ctfs): Snapshot =
  ## Snapshot `ordinal`: its parts appended as keyed streams now. The kinds
  ## cycle through periodic and the three boundaries, and the part sets
  ## through P1 to P4.
  let kinds = [skPeriodic, skPrein, skEntry, skPostl]
  result.kind = kinds[ordinal mod 4]
  result.id = if result.kind == skPeriodic: uint64(ordinal) else: 0
  result.geid = if result.kind == skPostl: SnapUnknownGeid else: uint64(ordinal * 1000)
  let sets = [@[spLayout, spHashes, spRegs], @[spLayout, spHashes, spRegs, spMemory],
              @[spLayout, spRegs, spMemory], @[spMemory, spRegsFull]]
  var parts = sets[(ordinal div 4) mod 4]
  if result.kind == skPeriodic: parts.add spSignals
  for p in parts:
    var s = KStream()
    doAssert c.appendStream(s, partBytes(ordinal, ord(p))).isOk
    result.parts[ord(p)] = SnapPart(form: pfRaw, data: s)
  if result.kind == skPeriodic:
    result.positions = encodePositions([(ctTid: 1'u32, position: uint64(ordinal)),
      (ctTid: 1025'u32, position: 7'u64)], [(ctTid: 1025'u32, parkKind: 2'u32, sysPre: 3'u64)])

proc sealsOf(seed: uint64, n: int): seq[int] =
  var s = seed
  var i = 0
  while i < n:
    i = min(n, i + 1 + int(rng64(s) mod 6))
    result.add i

proc runWriter(c: var Ctfs, w: var SnapshotWriter, b: var BundleWriter, ends: seq[int],
               beforeSeal, onSeal: proc (k: int) {.closure.} = nil) =
  var i = 0
  for k, e in ends:
    while i < e:
      discard w.addSnapshot(snapOf(i, c))
      if i mod 3 == 0:
        var s = KStream()
        doAssert c.appendStream(s, partBytes(i, 99)).isOk
        discard b.addBundled(BundledFile(form: pfRaw, data: s))
      else:
        discard b.addBundled(BundledFile())
      inc i
    if beforeSeal != nil: beforeSeal(k + 1)
    var p = initSealPlan(c)
    doAssert w.planSeal(c, p).isOk
    doAssert b.planSeal(c, p).isOk
    doAssert c.executeSeal(p).isOk
    if onSeal != nil: onSeal(k + 1)

type CheckFailure = object of CatchableError
proc fail(m: string) {.raises: [CheckFailure].} = raise newException(CheckFailure, m)

proc checkSnaps(rd: var LiveReader, ends: seq[int], sealsDone: int, label: string) {.raises: [CheckFailure].} =
  var scratch = createCtfs()
  let n = int(snapshotCount(rd))
  let published = if sealsDone == 0: 0 else: ends[sealsDone - 1]
  if n < published: fail(label & ": " & $n & " snapshots, and seal " & $sealsDone & " published " & $published)
  for o in 0 ..< n:
    let v = readSnapshot(rd, uint64(o))
    if v.isErr: fail(label & ": " & v.error)
    let want = snapOf(o, scratch)
    let got = v.get().snap
    if got.kind != want.kind or got.id != want.id or got.geid != want.geid or
        got.positions != want.positions:
      fail(label & ": snapshot " & $o & " is not its snapshot")
    for p in 0 ..< SnapParts:
      if got.parts[p].form != want.parts[p].form:
        fail(label & ": snapshot " & $o & " part " & $SnapPartSlot(p) & " has the wrong form")
      if got.parts[p].form == pfRaw:
        let d = got.parts[p].data
        let b = rd.readStream("part", d.size, d.mapBlock, 0, int(d.size))
        if b.isErr: fail(label & ": snapshot " & $o & " part " & $SnapPartSlot(p) & ": " & b.error)
        if b.get() != partBytes(o, p): fail(label & ": snapshot " & $o & " part " & $SnapPartSlot(p) & " bytes differ")
  let bs = rd.findSlot(BundleRec)
  let nb = if bs < 0: 0'u64 else: recordCount(BundleRecordSize, rd.memberSize(bs))
  for f in 0'u64 ..< nb:
    let b = readBundled(rd, f)
    if b.isErr: fail(label & ": " & b.error)
    if (f mod 3 == 0) != (b.get().form == pfRaw): fail(label & ": bundled file " & $f & " has the wrong form")
    if b.get().form == pfRaw:
      let d = b.get().data
      let x = rd.readStream("bundle", d.size, d.mapBlock, 0, int(d.size))
      if x.isErr or x.get() != partBytes(int(f), 99): fail(label & ": bundled file " & $f & "'s bytes differ")

proc tmpPath(n: string): string = getTempDir() / ("ckf4n_snap_" & n & "_" & $getCurrentProcessId() & ".ct")

proc refusals(): string =
  ## Every reader refusal of §2.4, driven by a record the test damages.
  var c = createCtfs(maxRootEntries = 0)
  var w = c.createSnapshotFamily().get()
  var b = c.createBundleFamily().get()
  runWriter(c, w, b, @[8])
  let recAt = proc (o: int): int =
    let e = readU64LE(c.data, c.fileEntryOffset(w.rec.entryIndex) + 8)
    int(c.resolveFileBlock(e, recordOffset(SnapRecordSize, uint64(o)) div 4096)) * 4096 +
      int(recordOffset(SnapRecordSize, uint64(o)) mod 4096)
  type Damage = tuple[what: string, ordinal: int, apply: proc (img: var seq[byte], at: int) {.closure.}]
  let damages: seq[Damage] = @[
    (what: "is not periodic, prein", ordinal: 1, apply: proc (img: var seq[byte], at: int) = writeU32LE(img, at, 7'u32)),
    (what: "none of the sets", ordinal: 1, apply: proc (img: var seq[byte], at: int) =
      writeU32LE(img, at + 4, readU32LE(img, at + 4) and not 3'u32)
      for k in 0 ..< 4: writeU64LE(img, at + 48 + 8 * k, 0'u64)),
    (what: "signals or state", ordinal: 1, apply: proc (img: var seq[byte], at: int) =
      writeU32LE(img, at + 4, readU32LE(img, at + 4) or (1'u32 shl 10))),
    (what: "absent part", ordinal: 1, apply: proc (img: var seq[byte], at: int) = writeU64LE(img, at + 48 + 32 * 6, 5'u64)),
    (what: "nonzero index", ordinal: 1, apply: proc (img: var seq[byte], at: int) = writeU64LE(img, at + 48 + 16, 5'u64))]
  for d in damages:
    var img: ref seq[byte]
    new(img)
    img[] = c.data
    d.apply(img[], recAt(d.ordinal))
    var rd = openImageReader(img).get()
    let r = readSnapshot(rd, uint64(d.ordinal))
    if r.isOk: return "a record with " & d.what & " was not refused"
    if d.what notin r.error or ("snapshot " & $d.ordinal) notin r.error:
      return "the refusal of " & d.what & " does not name it and the ordinal: " & r.error
  ""

proc m1(): string =
  var c = createCtfs(maxRootEntries = 0)
  var w = c.createMonotoneRun("cov", 32).get()
  var keys: seq[uint64]
  var g = initRand(5)
  var k = 10'u64
  for i in 0 ..< 3000:
    k += uint64(1 + g.rand(50))
    keys.add k
    discard w.insert(k, [byte(i and 0xff)]).get()
    if i mod 97 == 96: doAssert w.seal(c).isOk
  let refused = w.insert(k, [])
  if refused.isOk or "cov.rec" notin refused.error: return "M1 accepted a key that does not exceed the last"
  var dead: seq[bool] = newSeq[bool](keys.len)
  for i in countup(5, keys.len - 1, 7):
    doAssert w.tombstone(uint64(i)).isOk
    dead[i] = true
  doAssert w.seal(c).isOk
  var img: ref seq[byte]
  new(img)
  img[] = c.data
  var rd = openImageReader(img).get()
  for _ in 0 ..< 3000:
    let t = uint64(g.rand(int(k) + 100))
    var want = -1
    for i in countdown(keys.len - 1, 0):
      if keys[i] <= t and not dead[i]:
        want = i
        break
    let got = predecessor(rd, "cov", 32, t)
    if got.isErr: return "M1 predecessor: " & got.error
    if (want < 0) == got.get().found or (want >= 0 and got.get().record != uint64(want)):
      return "M1 predecessor of " & $t & " is " & $got.get() & ", the model says record " & $want
  ""

proc matrix(seed: uint64): string =
  let path = tmpPath("mx")
  removeFile(path)
  var c = createCtfsStreaming(path, maxRootEntries = 0).get()
  var w = c.createSnapshotFamily().get()
  var b = c.createBundleFamily().get()
  let ends = sealsOf(seed, 60)
  let log = installTaps(c)
  runWriter(c, w, b, ends, onSeal = proc (k: int) = log.markSeal())
  c.removeTaps()
  discard c.closeCtfs()
  removeFile(path)
  try:
    for attached in [false, true]:
      var rep = initReplay(log, attached)
      var n = 0
      while true:
        let r0 = openImageReader(rep.file, if attached: rep.page else: nil)
        if r0.isErr: fail("prefix " & $rep.next & ": " & r0.error)
        var rd = r0.get()
        checkSnaps(rd, ends, rep.sealsDone, (if attached: "attached" else: "file-only") &
          " prefix " & $rep.next & "/" & $log.events.len)
        inc n
        if rep.atEnd: break
        rep.applyNext()
      echo "  matrix ", (if attached: "attached" else: "file-only"), ": ", n, " prefixes, ", rep.sealsDone, " seals"
  except CheckFailure as e: return e.msg
  ""

proc writerMain(path, logPath: string, seed: uint64) =
  var log: File
  doAssert open(log, logPath, fmAppend)
  var c = createCtfsStreaming(path, maxRootEntries = 0).get()
  var w = c.createSnapshotFamily().get()
  var b = c.createBundleFamily().get()
  runWriter(c, w, b, sealsOf(seed, 400),
    beforeSeal = proc (k: int) = logLine(log, "intent " & $k),
    onSeal = proc (k: int) = logLine(log, "done " & $k))
  logLine(log, "close")
  discard c.closeCtfs()

when isMainModule:
  let args = commandLineParams()
  if args.len > 0 and args[0] == "--writer":
    writerMain(args[1], args[2], uint64(parseInt(args[3])))
    quit(0)
  if args.len > 0 and args[0] == "--arm":
    let m = matrix(0xA4)
    echo "  arm ", args[1], ": fired ", keyedMutationFired, "; ", (if m.len > 0: "caught: " & m else: "NOT caught")
    quit(if keyedMutationFired == 0: 5 elif m.len > 0: 3 else: 4)
  when not defined(ctfsKeyedMutation):
    {.error: "build this test with -d:ctfsKeyedMutation".}
  var failures: seq[string]
  echo "refusals"
  let rf = refusals()
  if rf.len > 0: failures.add rf
  echo "M1"
  let mm = m1()
  if mm.len > 0: failures.add mm
  echo "every write prefix"
  let m = matrix(0x5EED)
  if m.len > 0: failures.add "matrix: " & m
  echo "mutation arms"
  if runArmChild("size-before-record") != 3: failures.add "arm size-before-record was not caught"
  echo "kills at random"
  let k = killLoop(200, "snap",
    proc (path, logPath: string, seed: uint64): seq[string] = @["--writer", path, logPath, $seed],
    proc (path, logPath: string, seed: uint64, d: int): string =
      if not fileExists(path): return ""
      let r0 = openLiveReader(path)
      if r0.isErr: return (if d == 0: "" else: "did not open: " & r0.error)
      var rd = r0.get()
      if rd.attached:
        rd.close()
        return "attached to a dead writer's page"
      try: checkSnaps(rd, sealsOf(seed, 400), d, "killed")
      except CheckFailure as e:
        rd.close()
        return e.msg
      rd.close()
      "")
  echo "  ", k.msg
  if not k.ok: failures.add "kills: " & k.msg
  if failures.len > 0:
    for f in failures: echo "FAIL: ", f
    quit(1)
  echo "PASS test_keyed_snapshots"
