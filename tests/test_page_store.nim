## The keyed page store (`codetracer-specs` `CTFS-Binary-Format.md` §2.6):
## `cppages.hdr`, `.nod`, `.rec`, `.hzd`, `.hzi`, durable at every seal.
##
## Null predictions (each driven by a mutation arm that must be CAUGHT):
##
## - `pagestore-frame-left-open` (the pages of a seal left in a heap frame the
##   seal does not publish, so a frame does not end at the seal): a record a
##   seal published names heap bytes no published frame holds, which the
##   check "every published record resolves its page" reports at the prefix
##   right after that seal.
## - `size-before-record` (a member's Size published before the bytes it
##   covers): a record, or a heap index entry, below its published Size is not
##   its final bytes.
##
## On the base this file does not compile (there is no page store), so it
## cannot pass there.

import std/[os, osproc, strutils, tables, random, times]
import results
import codetracer_ctfs/[types, container, streaming, keyed_store,
                        live_reader, b1, page_store]
import ./keyed_harness

proc digestOf(seed: uint64, i: int): PageDigest =
  var s = seed xor (uint64(i) * 0xD1B54A32D192ED03'u64)
  for k in 0 ..< 2:
    let x = rng64(s)
    for b in 0 ..< 8: result[8 * k + b] = byte((x shr (8 * b)) and 0xff)

proc pageOf(seed: uint64, i: int): seq[byte] =
  ## Distinct per page and compressible, as real pages mostly are.
  result = newSeq[byte](4096)
  let d = digestOf(seed, i)
  for k in 0 ..< 16: result[k] = d[k]
  for k in 16 ..< 4096: result[k] = byte((i + k div 64) and 0xff)

type
  PsWorkload = object
    seed: uint64
    sealEnd: seq[int]        ## pages published by seal k: [0, sealEnd[k-1])
    pages: int

proc makePsWorkload(seed: uint64, n, minSeal, maxSeal: int): PsWorkload =
  result.seed = seed
  result.pages = n
  var s = seed
  var i = 0
  while i < n:
    i = min(n, i + minSeal + int(rng64(s) mod uint64(maxSeal - minSeal + 1)))
    result.sealEnd.add i

proc runPsWriter(c: var Ctfs, ps: var PageStoreWriter, w: PsWorkload,
                 beforeSeal: proc (k: int) {.closure.} = nil,
                 onSeal: proc (k: int) {.closure.} = nil) =
  var i = 0
  for k, e in w.sealEnd:
    while i < e:
      let r = ps.storePage(digestOf(w.seed, i), pageOf(w.seed, i)).get()
      doAssert r.isNew and r.record == uint64(i)
      inc i
    # A page stored twice is not stored again.
    let again = ps.storePage(digestOf(w.seed, i - 1), pageOf(w.seed, i - 1)).get()
    doAssert not again.isNew and again.record == uint64(i - 1)
    if beforeSeal != nil: beforeSeal(k + 1)
    let r = ps.seal(c)
    doAssert r.isOk, r.error
    if onSeal != nil: onSeal(k + 1)

type CheckFailure = object of CatchableError
proc fail(msg: string) {.raises: [CheckFailure].} =
  raise newException(CheckFailure, msg)

proc checkStore(rd: var LiveReader, w: PsWorkload, sealsDone: int,
                sample: int, rng: var Rand, label: string) {.raises: [CheckFailure].} =
  ## 1. every published record is its page's record, and resolves to its
  ##    page's bytes through a published frame;
  ## 2. every page of a completed seal is found by digest, with its bytes;
  ## 3. a digest never stored is not found.
  var ps = openPageStoreReader(rd)
  let nrec = int(ps.recordCount(rd))
  if nrec > w.pages: fail(label & ": " & $nrec & " records, more than stored")
  if sealsDone > 0 and nrec < w.sealEnd[sealsDone - 1]:
    fail(label & ": " & $nrec & " records published after seal " & $sealsDone)
  let recSlot = rd.findSlot(PageStoreRec)
  for r in 0 ..< nrec:
    let b = rd.readMember(recSlot, recordOffset(32, uint64(r)), 32)
    if b.isErr: fail(label & ": record " & $r & ": " & b.error)
    let d = digestOf(w.seed, r)
    if readU64LE(b.get(), 0) != pageKey(d) or readU64LE(b.get(), 24) != uint64(r) * 4096:
      fail(label & ": record " & $r & " below the published Size is not its bytes")
    for k in 0 ..< 16:
      if b.get()[8 + k] != d[k]:
        fail(label & ": record " & $r & "'s digest is not its page's")
    if r mod 7 == 0 or r == nrec - 1:
      let pb = ps.pageBytes(rd, uint64(r))
      if pb.isErr: fail(label & ": published record " & $r & "'s page: " & pb.error)
      if pb.get() != pageOf(w.seed, r):
        fail(label & ": published record " & $r & "'s page bytes differ")
  if sealsDone > 0:
    let published = w.sealEnd[sealsDone - 1]
    var toCheck: seq[int]
    if sample <= 0 or sample >= published:
      for i in 0 ..< published: toCheck.add i
    else:
      for _ in 0 ..< sample: toCheck.add rng.rand(published - 1)
    for i in toCheck:
      let l = ps.lookupPage(rd, digestOf(w.seed, i))
      if l.isErr: fail(label & ": lookup of page " & $i & ": " & l.error)
      if not l.get().found or l.get().record != uint64(i):
        fail(label & ": page " & $i & ", published by seal " & $sealsDone &
          " or earlier, is not found")
  let absent = ps.lookupPage(rd, digestOf(w.seed xor 0xFFFF, 1))
  if absent.isErr: fail(label & ": lookup of an absent page: " & absent.error)
  if absent.get().found: fail(label & ": a digest never stored was found")

proc tmpPath(name: string): string =
  getTempDir() / ("ckf4n_ps_" & name & "_" & $getCurrentProcessId() & ".ct")

proc roundTrip(n: int, streaming: bool) =
  let path = tmpPath("rt")
  removeFile(path)
  var c =
    if streaming: createCtfsStreaming(path, maxRootEntries = 0).get()
    else: createCtfs(maxRootEntries = 0)
  var ps = c.createPageStore().get()
  let w = makePsWorkload(0x5A6E'u64 + uint64(n), n, 200, 3000)
  runPsWriter(c, ps, w)
  var image: ref seq[byte]
  new(image)
  if streaming:
    doAssert c.closeCtfs().isOk
    image[] = cast[seq[byte]](readFile(path))
    removeFile(path)
  else:
    image[] = c.data
  var rd = openImageReader(image).get()
  var rng = initRand(3)
  try:
    checkStore(rd, w, w.sealEnd.len, 0, rng, "round trip")
  except CheckFailure as e:
    doAssert false, e.msg
  # Every page's bytes, and the heap's frames end at every seal.
  var ps2 = openPageStoreReader(rd)
  for i in 0 ..< n:
    doAssert ps2.pageBytes(rd, uint64(i)).get() == pageOf(w.seed, i)
  let hzi = rd.readMember(rd.findSlot(PageStoreHzi), 0,
    int(rd.memberSize(rd.findSlot(PageStoreHzi)))).get()
  doAssert readU32LE(hzi, 0) == 4194304'u32 and hzi[4] == 2 and hzi[5] == 0
  let frames = (hzi.len - 8) div 16
  var ends: seq[uint64]
  for f in 0 ..< frames: ends.add readU64LE(hzi, 8 + 16 * f + 8)
  for k, e in w.sealEnd:
    doAssert uint64(e) * 4096 in ends, "seal " & $(k + 1) & " did not end a frame"
  for f in 0 ..< frames:
    let prev = if f == 0: 0'u64 else: ends[f - 1]
    doAssert ends[f] - prev <= 4194304'u64, "a frame holds more than 4 MiB"
  doAssert ends[^1] == uint64(n) * 4096
  # The CASH record: algorithm 1, 4096-byte pages, at bytes 64 to 79.
  let hdr = rd.readMember(rd.findSlot(PageStoreHdr), 0, 256).get()
  doAssert hdr[64 .. 67] == @[byte('C'), byte('A'), byte('S'), byte('H')]
  doAssert hdr[68] == 1 and hdr[70] == 1 and readU32LE(hdr, 72) == 4096'u32
  # A reader expecting another page size refuses the store, naming it.
  var wrong = openPageStoreReader(rd, pageSize = 8192)
  let refused = wrong.lookupPage(rd, digestOf(w.seed, 0))
  doAssert refused.isErr and "CASH" in refused.error and "8192" in refused.error
  echo "  round trip ", n, " pages", (if streaming: " streaming" else: " in memory"),
    ": ", frames, " frames, ", w.sealEnd.len, " seals, ", ps.b1.nodeCount, " nodes"

proc matrixScenario(n, minSeal, maxSeal, sample: int, seed: uint64): string =
  let path = tmpPath("matrix")
  removeFile(path)
  var c = createCtfsStreaming(path, maxRootEntries = 0).get()
  var ps = c.createPageStore().get()
  let w = makePsWorkload(seed, n, minSeal, maxSeal)
  let log = installTaps(c)
  runPsWriter(c, ps, w, onSeal = proc (k: int) = log.markSeal())
  c.removeTaps()
  discard c.closeCtfs()
  removeFile(path)
  try:
    for attached in [false, true]:
      var rep = initReplay(log, attached)
      var rng = initRand(5)
      var prefixes = 0
      while true:
        let rd0 = openImageReader(rep.file, if attached: rep.page else: nil)
        if rd0.isErr:
          fail("prefix " & $rep.next & ": the container did not open: " & rd0.error)
        var rd = rd0.get()
        checkStore(rd, w, rep.sealsDone, sample, rng,
          (if attached: "attached" else: "file-only") & " prefix " & $rep.next &
          "/" & $log.events.len & " (after seal " & $rep.sealsDone & ")")
        inc prefixes
        if rep.atEnd: break
        rep.applyNext()
      if rep.sealsDone != w.sealEnd.len:
        return "the replay saw " & $rep.sealsDone & " seals of " & $w.sealEnd.len
      echo "  matrix ", n, " pages, ", (if attached: "attached" else: "file-only"),
        ": ", prefixes, " prefixes of ", log.events.len, " events, ", rep.sealsDone, " seals"
  except CheckFailure as e:
    return e.msg
  ""

const KillPages = 6000

proc writerMain(path, logPath: string, seed: uint64) =
  var log: File
  doAssert open(log, logPath, fmAppend)
  var c = createCtfsStreaming(path, maxRootEntries = 0).get()
  var ps = c.createPageStore().get()
  let w = makePsWorkload(seed, KillPages, 20, 120)
  runPsWriter(c, ps, w,
    beforeSeal = proc (k: int) = logLine(log, "intent " & $k),
    onSeal = proc (k: int) = logLine(log, "done " & $k))
  logLine(log, "close")
  discard c.closeCtfs()

proc killScenario(kills: int): tuple[ok: bool, msg: string] =
  let path = tmpPath("kill")
  let logPath = path & ".seallog"
  removeFile(path); removeFile(logPath)
  let t0 = epochTime()
  var p = startProcess(selfExe(), args = ["--writer", path, logPath, "1"],
                       options = {poParentStreams})
  discard p.waitForExit()
  close(p)
  let full = epochTime() - t0
  let fullSeals = lastSealLogged(logPath)
  var inSeal = 0
  var rng = initRand(99)
  for i in 0 ..< kills:
    let seed = uint64(500 + i)
    removeFile(path); removeFile(logPath)
    var q = startProcess(selfExe(), args = ["--writer", path, logPath, $seed],
                         options = {poParentStreams})
    sleep(rng.rand(int(full * 1000.0)))
    q.killHard()
    discard q.waitForExit()
    close(q)
    let d = lastSealLogged(logPath)
    if inSealAtKill(logPath): inc inSeal
    if not fileExists(path): continue
    let w = makePsWorkload(seed, KillPages, 20, 120)
    let rd0 = openLiveReader(path)
    if rd0.isErr:
      if d == 0: continue
      return (false, "kill " & $i & ": the container did not open: " & rd0.error)
    var rd = rd0.get()
    if rd.attached:
      rd.close()
      return (false, "kill " & $i & ": a reader attached to a dead writer's page")
    var r2 = initRand(i)
    try:
      checkStore(rd, w, d, 150, r2, "kill " & $i & " (after seal " & $d & ")")
    except CheckFailure as e:
      rd.close()
      return (false, e.msg)
    rd.close()
  removeFile(path); removeFile(logPath)
  (true, $kills & " kills (a full run: " & $fullSeals & " seals in " &
    formatFloat(full, ffDecimal, 2) & " s), " & $inSeal & " inside a seal")

proc armMain(arm: string) =
  let msg = matrixScenario(1500, 50, 250, 0, 0xA2'u64)
  when defined(ctfsKeyedMutation):
    let fired = keyedMutationFired
  else:
    let fired = 0
  echo "  arm ", arm, ": fired ", fired, " times; ",
    (if msg.len > 0: "caught: " & msg else: "NOT caught")
  if fired == 0: quit(5)
  if msg.len > 0: quit(3)
  quit(4)

proc runArm(arm: string): bool =
  putEnv("CTFS_KEYED_MUTATE", arm)
  let p = startProcess(selfExe(), args = ["--arm", arm], options = {poParentStreams})
  let rc = p.waitForExit()
  close(p)
  delEnv("CTFS_KEYED_MUTATE")
  rc == 3

when isMainModule:
  let args = commandLineParams()
  if args.len > 0 and args[0] == "--writer":
    writerMain(args[1], args[2], uint64(parseInt(args[3])))
    quit(0)
  if args.len > 0 and args[0] == "--arm":
    armMain(args[1])
  when not defined(ctfsKeyedMutation):
    {.error: "build this test with -d:ctfsKeyedMutation: its mutation arms are part of it".}
  var failures: seq[string]
  echo "round trips"
  roundTrip(20_000, streaming = false)
  roundTrip(20_000, streaming = true)
  echo "every write prefix"
  let m = matrixScenario(3000, 50, 400, 120, 0x77'u64)
  if m.len > 0: failures.add "matrix: " & m
  echo "mutation arms"
  for arm in ["pagestore-frame-left-open", "size-before-record"]:
    if not runArm(arm):
      failures.add "mutation arm " & arm & " was not caught (or did not fire)"
  echo "kills at random"
  let k = killScenario(200)
  echo "  ", k.msg
  if not k.ok: failures.add "kill at random: " & k.msg
  if failures.len > 0:
    for f in failures: echo "FAIL: ", f
    quit(1)
  echo "PASS test_page_store"
