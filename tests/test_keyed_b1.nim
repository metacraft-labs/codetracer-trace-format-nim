## B1, the B-link tree with in-place nodes (`ctfs-keyed-families.md` §5.8.3),
## written and read through the live coordination page: round trip, every
## write prefix (the kill-at-every-write and stopped-reader test), real kills
## at random times, and a concurrent attached and file-only follower in another
## process.
##
## The family under test is a minimal one built on the library's B1: 16-byte
## records `(key, value)` in `kb.rec`, published before B1's phases, as the
## page store publishes its records (`CTFS-Binary-Format.md` §2.6).
##
## Null predictions (what each check reports on a library that lacks the rule
## it checks; each is driven by a mutation arm below, which must be CAUGHT):
##
## - `b1-count-first` (a node's count stored before the entries it covers,
##   the benchmark's flush order): the strict slot check finds a slot below a
##   count, in the file or in the page's mirror, that holds no inserted entry.
## - `b1-shrink-count-first` (a split node's new count stored before the hole
##   fill): a key published at an earlier seal is not found at some write
##   prefix, by the file-only reader and by the attached one.
## - `size-before-record` (a member's Size published before the records it
##   covers): a record below a published Size is not its final bytes.
##
## The production build must pass every check at every write prefix, in both
## reader modes. A test that would pass on the base (which has no keyed
## member) is not evidence: the base has no `createB1`, so this file does not
## compile there.

import std/[os, osproc, strutils, tables, sets, algorithm, times, random]
import results
import codetracer_ctfs/[types, container, streaming, keyed_store,
                        live_reader, b1]
import ./keyed_harness

const RecSize = 16

type
  TestFam = object
    rec: KMember
    b1: B1Writer
    pending: seq[array[RecSize, byte]]
    nrec: uint64

proc createFam(c: var Ctfs): TestFam =
  result.b1 = c.createB1("kb").get()
  result.rec = c.createKMember("kb.rec").get()

proc insert(f: var TestFam, key, value: uint64): uint64 =
  var r: array[RecSize, byte]
  writeU64LE(r, 0, key)
  writeU64LE(r, 8, value)
  f.pending.add r
  result = f.nrec
  inc f.nrec
  f.b1.insert(key, result)

proc seal(f: var TestFam, c: var Ctfs) =
  var p = initSealPlan(c)
  for r in f.pending:
    discard c.appendRecord(f.rec, RecSize, r).get()
  f.pending.setLen(0)
  p.barrier()
  p.publishEntries(c, [f.rec])
  f.b1.planSeal(c, p).get()
  let e = c.executeSeal(p)
  doAssert e.isOk, e.error

# ---------------------------------------------------------------------------
# The workload: deterministic from a seed, so a checker regenerates it.
# ---------------------------------------------------------------------------

type
  Workload = object
    keys: seq[uint64]          ## record r's key
    sealEnd: seq[int]          ## records published by seal k: [0, sealEnd[k-1])

const
  WriterMinSeal = 30
  WriterMaxSeal = 120
    ## The seal spans of the writer child, which every checker of its
    ## container regenerates.

proc makeWorkload(seed: uint64, n: int, minSeal, maxSeal: int,
                  dupPercent = 3): Workload =
  var s = seed
  var i = 0
  while i < n:
    let span = minSeal + int(rng64(s) mod uint64(maxSeal - minSeal + 1))
    for _ in 0 ..< span:
      if i >= n: break
      var k: uint64
      if i > 0 and int(rng64(s) mod 100) < dupPercent:
        k = result.keys[int(rng64(s) mod uint64(i))]   # a truncated-key collision
      else:
        k = rng64(s) or 1'u64                          # never 0
      result.keys.add k
      inc i
    result.sealEnd.add i

proc valueOf(r: int): uint64 = uint64(r) * 0x100000001B3'u64 + 7

proc runWriter(c: var Ctfs, f: var TestFam, w: Workload,
               onSeal: proc (k: int) {.closure.} = nil,
               beforeSeal: proc (k: int) {.closure.} = nil) =
  var r = 0
  for k, e in w.sealEnd:
    while r < e:
      discard f.insert(w.keys[r], valueOf(r))
      inc r
    if beforeSeal != nil: beforeSeal(k + 1)
    f.seal(c)
    if onSeal != nil: onSeal(k + 1)

# ---------------------------------------------------------------------------
# The checks a reader of any write prefix must pass
# ---------------------------------------------------------------------------

type
  Oracle = object
    w: Workload
    pairs: HashSet[(uint64, uint64)]
    keySet: HashSet[uint64]
    byKey: Table[uint64, seq[uint64]]

proc initOracle(w: Workload): Oracle =
  result.w = w
  for r, k in w.keys:
    result.pairs.incl (k, uint64(r))
    result.keySet.incl k
    result.byKey.mgetOrPut(k, @[]).add uint64(r)

proc recordBytes(r: int, w: Workload): seq[byte] =
  result = newSeq[byte](RecSize)
  writeU64LE(result, 0, w.keys[r])
  writeU64LE(result, 8, valueOf(r))

type CheckFailure = object of CatchableError

proc fail(msg: string) {.raises: [CheckFailure].} =
  raise newException(CheckFailure, msg)

type
  CheckScope = object
    ## What a check reads. `full`: every record, every node, every key. Else
    ## the records from `recFrom` on and `recSample` random ones, the nodes in
    ## `nodes` and `nodeSample` random ones, and `keySample` random keys.
    full: bool
    recFrom: int
    recSample: int
    nodes: seq[uint64]
    nodeSample: int
    keySample: int
    live: bool
      ## A file another process is writing: no node check (a reader may read a
      ## node's slots torn, §5.8.0, and a node may name nodes newer than the
      ## Size this reader read: a stale view a lookup refreshes past).

proc checkReader(rd: var LiveReader, o: Oracle, sealsDone: int,
                 scope: CheckScope, rng: var Rand, label: string) {.raises: [CheckFailure].} =
  ## 1. every record below the published Size is its final bytes;
  ## 2. every slot below a node's count (as this reader accepts the count) is
  ##    an inserted entry;
  ## 3. every key of a seal that completed is found, with its records, and
  ##    nothing but inserted records is returned.
  let recSlot = rd.findSlot("kb.rec")
  let size = if recSlot < 0: 0'u64 else: rd.memberSize(recSlot)
  let nrec = int(recordCount(RecSize, size))
  if nrec > o.w.keys.len: fail(label & ": " & $nrec & " records published, more than written")
  if sealsDone > 0 and nrec < o.w.sealEnd[sealsDone - 1]:
    fail(label & ": " & $nrec & " records published after seal " & $sealsDone &
      ", which published " & $o.w.sealEnd[sealsDone - 1])
  if nrec > 0:
    var recs: seq[int]
    if scope.full:
      for r in 0 ..< nrec: recs.add r
    else:
      for r in min(scope.recFrom, nrec) ..< nrec: recs.add r
      for _ in 0 ..< scope.recSample: recs.add rng.rand(nrec - 1)
    if scope.full:
      let all = rd.readMember(recSlot, 0, int(size))
      if all.isErr: fail(label & ": records: " & all.error)
      for r in recs:
        let off = int(recordOffset(RecSize, uint64(r)))
        if all.get()[off ..< off + RecSize] != recordBytes(r, o.w):
          fail(label & ": record " & $r & " below the published Size is not its bytes")
    else:
      for r in recs:
        let b = rd.readMember(recSlot, recordOffset(RecSize, uint64(r)), RecSize)
        if b.isErr: fail(label & ": record " & $r & ": " & b.error)
        if b.get() != recordBytes(r, o.w):
          fail(label & ": record " & $r & " below the published Size is not its bytes")
  var br = openB1Reader(rd, "kb")
  let h = br.readHeader(rd)
  if h.isErr: fail(label & ": header: " & h.error)
  if not h.get().empty and not scope.live:
    let visible = rd.memberSize(br.nodSlot) div 4096
    if h.get().root >= visible:
      fail(label & ": the header's root " & $h.get().root & " is past the " &
        $visible & " published nodes")
    var nodes: seq[uint64]
    if scope.full:
      for n in 0'u64 ..< visible: nodes.add n
    else:
      for n in scope.nodes:
        if n < visible: nodes.add n
      for _ in 0 ..< scope.nodeSample: nodes.add uint64(rng.rand(int(visible) - 1))
    for n in nodes:
      let node = br.readNode(rd, n)
      if node.isErr: fail(label & ": node " & $n & ": " & node.error)
      let nd = node.get()
      for j in 0 ..< nd.count:
        let e = nd.slotOf(j)
        if nd.kind == 1:
          if (e.key, e.refv) notin o.pairs:
            fail(label & ": leaf " & $n & " slot " & $j & " below count " &
              $nd.count & " holds (" & $e.key & ", " & $e.refv &
              "), which was never inserted")
        else:
          if e.key notin o.keySet or e.refv >= visible:
            fail(label & ": internal node " & $n & " slot " & $j & " below count " &
              $nd.count & " holds (" & $e.key & ", " & $e.refv & "), not a separator")
  if sealsDone == 0: return
  let published = o.w.sealEnd[sealsDone - 1]
  var toCheck: seq[int]
  if scope.full or scope.keySample >= published:
    for r in 0 ..< published: toCheck.add r
  else:
    for _ in 0 ..< scope.keySample: toCheck.add rng.rand(published - 1)
  var recRef = addr rd
  for r in toCheck:
    let key = o.w.keys[r]
    let hits = br.lookup(rd, key, proc (refv: uint64): Result[bool, string] =
      let cnt = recordCount(RecSize, recRef[].memberSize(recSlot))
      if refv >= cnt: return ok(false)
      let b = ? recRef[].readMember(recSlot, recordOffset(RecSize, refv), RecSize)
      ok(readU64LE(b, 0) == key))
    if hits.isErr: fail(label & ": lookup of record " & $r & "'s key: " & hits.error)
    if uint64(r) notin hits.get():
      fail(label & ": key " & $key & " of record " & $r & ", published by seal " &
        $sealsDone & " or earlier, is not found")
    for x in hits.get():
      if (key, x) notin o.pairs:
        fail(label & ": key " & $key & " answered record " & $x & ", which holds another key")

proc nodeOfBlock(final: ref seq[byte]): Table[uint64, uint64] =
  ## Physical block to node number, from the final image (blocks never move).
  var rd = openImageReader(final).get()
  let slot = rd.findSlot("kb.nod")
  for n in 0'u64 ..< rd.memberSize(slot) div 4096:
    result[rd.resolveBlock("kb.nod", rd.entries[slot].mapBlock, n).get()] = n

proc runMatrix(o: Oracle, log: TapLog, attached: bool,
               full: bool): tuple[prefixes, sealsSeen: int] =
  ## Rebuild the file (and the page) after every event and check a reader of
  ## it: every record, node and key when `full`; otherwise the records and
  ## nodes the seal in flight has written so far, samples of the rest, and
  ## everything at every completed seal. Raises the first failure.
  var rep = initReplay(log, attached)
  var rng = initRand(77)
  var prefixes = 0
  var final: ref seq[byte]
  new(final)
  block:
    var r2 = initReplay(log, false)
    while not r2.atEnd: r2.applyNext()
    final[] = r2.file[]
  let blockNode = nodeOfBlock(final)
  var touched: seq[uint64]
  var recFrom = 0
  while true:
    let atSeal = rep.next > 0 and rep.log.events[rep.next - 1].kind == teSeal
    let rd0 = openImageReader(rep.file, if attached: rep.page else: nil)
    if rd0.isErr:
      fail((if attached: "attached" else: "file-only") & " prefix " & $rep.next &
        ": the container did not open: " & rd0.error)
    var rd = rd0.get()
    if attached and not rd.attached:
      fail("prefix " & $rep.next & ": the replayed page did not attach")
    let scope =
      if full or atSeal or rep.atEnd: CheckScope(full: true)
      else: CheckScope(recFrom: recFrom, recSample: 32, nodes: touched,
                       nodeSample: 4, keySample: 60)
    checkReader(rd, o, rep.sealsDone, scope, rng,
      (if attached: "attached" else: "file-only") & " prefix " & $rep.next &
      "/" & $log.events.len & " (after seal " & $rep.sealsDone & ")")
    inc prefixes
    if rep.atEnd: break
    let e = rep.log.events[rep.next]
    if e.kind == teFile and e.bytes.len > 0:
      for b in uint64(e.off) div 4096 .. (uint64(e.off) + uint64(e.bytes.len) - 1) div 4096:
        if b in blockNode and blockNode.getOrDefault(b) notin touched:
          touched.add blockNode.getOrDefault(b)
    elif e.kind == teSeal:
      touched.setLen(0)
      recFrom = o.w.sealEnd[e.seal - 1]
    rep.applyNext()
  (prefixes, rep.sealsDone)

# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

proc tmpPath(name: string): string =
  getTempDir() / ("ckf4n_" & name & "_" & $getCurrentProcessId() & ".ct")

proc matrixScenario(n, minSeal, maxSeal: int, full: bool, seed: uint64): string =
  ## Write a workload with taps installed, then check every write prefix in
  ## both reader modes. Returns "" or the first failure.
  let path = tmpPath("matrix")
  removeFile(path)
  var c = createCtfsStreaming(path, maxRootEntries = 0).get()
  var f = createFam(c)
  let w = makeWorkload(seed, n, minSeal, maxSeal)
  let log = installTaps(c)
  runWriter(c, f, w, onSeal = proc (k: int) = log.markSeal())
  c.removeTaps()
  discard c.closeCtfs()
  removeFile(path)
  let o = initOracle(w)
  try:
    let a = runMatrix(o, log, attached = false, full)
    let b = runMatrix(o, log, attached = true, full)
    echo "  matrix n=", n, ": ", log.events.len, " events (", log.fileWrites,
      " file writes), ", a.prefixes, " file-only and ", b.prefixes,
      " attached prefixes checked, ", a.sealsSeen, " seals"
    if a.sealsSeen != w.sealEnd.len or b.sealsSeen != w.sealEnd.len:
      return "the replay saw " & $a.sealsSeen & " seals of " & $w.sealEnd.len
  except CheckFailure as e:
    return e.msg
  ""

proc roundTrip(n: int, streaming: bool) =
  let path = tmpPath("roundtrip")
  removeFile(path)
  var c =
    if streaming: createCtfsStreaming(path, maxRootEntries = 0).get()
    else: createCtfs(maxRootEntries = 0)
  var f = createFam(c)
  let w = makeWorkload(0xB1'u64 + uint64(n), n, 500, 4000, dupPercent = 5)
  runWriter(c, f, w)
  var image: ref seq[byte]
  new(image)
  if streaming:
    doAssert c.closeCtfs().isOk
    image[] = cast[seq[byte]](readFile(path))
    removeFile(path)
  else:
    image[] = c.data
  var rd = openImageReader(image).get()
  let o = initOracle(w)
  var rng = initRand(1)
  try:
    checkReader(rd, o, w.sealEnd.len, CheckScope(full: true), rng, "round trip n=" & $n)
  except CheckFailure as e:
    doAssert false, e.msg
  # Enumeration is the inserted multiset, exactly.
  var br = openB1Reader(rd, "kb")
  var got = br.enumerate(rd).get()
  var want: seq[B1Entry]
  for r, k in w.keys: want.add (key: k, refv: uint64(r))
  got.sort()
  want.sort()
  doAssert got == want, "enumeration differs from the inserted entries"
  echo "  round trip n=", n, (if streaming: " streaming" else: " in memory"),
    ": ", f.b1.nodeCount, " nodes, ", w.sealEnd.len, " seals"

# ---------------------------------------------------------------------------
# Real processes: a writer killed at random, and a concurrent follower
# ---------------------------------------------------------------------------

const KillN = 8000

proc writerMain(path, logPath: string, seed: uint64, n: int, slowSeals: bool) =
  ## The child: write the workload, logging `intent k` before seal k and
  ## `done k` after it.
  var log: File
  doAssert open(log, logPath, fmAppend)
  var c = createCtfsStreaming(path, maxRootEntries = 0).get()
  var f = createFam(c)
  let w = makeWorkload(seed, n, WriterMinSeal, WriterMaxSeal)
  runWriter(c, f, w,
    beforeSeal = proc (k: int) = logLine(log, "intent " & $k),
    onSeal = proc (k: int) =
      logLine(log, "done " & $k)
      if slowSeals: sleep(2))
  logLine(log, "close")
  discard c.closeCtfs()

proc checkKilled(path, logPath: string, seed: uint64, n: int): tuple[d: int, inSeal: bool, err: string] =
  let d = lastSealLogged(logPath)
  let inSeal = inSealAtKill(logPath)
  if not fileExists(path):
    return (d, inSeal, if d == 0: "" else: "no container after seal " & $d)
  let w = makeWorkload(seed, n, WriterMinSeal, WriterMaxSeal)
  let o = initOracle(w)
  var rd0 = openLiveReader(path, attach = true)
  if rd0.isErr:
    if d == 0: return (d, inSeal, "")
    return (d, inSeal, "the killed container did not open: " & rd0.error)
  var rd = rd0.get()
  if rd.attached:
    rd.close()
    return (d, inSeal, "a reader attached to the page of a killed writer")
  var rng = initRand(int(seed))
  try:
    checkReader(rd, o, d, CheckScope(recSample: 400, nodeSample: 64, keySample: 400),
      rng, "killed after seal " & $d)
  except CheckFailure as e:
    rd.close()
    return (d, inSeal, e.msg)
  rd.close()
  (d, inSeal, "")

proc killScenario(kills: int): tuple[ok: bool, msg: string] =
  # Calibrate: one uninterrupted run.
  let path = tmpPath("kill")
  let logPath = path & ".seallog"
  removeFile(path); removeFile(logPath)
  let t0 = epochTime()
  var p = startProcess(selfExe(), args = ["--writer", path, logPath, "1", $KillN, "1"],
                       options = {poParentStreams})
  discard p.waitForExit()
  let full = epochTime() - t0
  close(p)
  let fullSeals = lastSealLogged(logPath)
  removeFile(path); removeFile(logPath)
  var inSeal, afterLast, beforeFirst = 0
  var rng = initRand(4242)
  for i in 0 ..< kills:
    let seed = uint64(1000 + i)
    removeFile(path); removeFile(logPath)
    var q = startProcess(selfExe(), args = ["--writer", path, logPath, $seed, $KillN, "1"],
                         options = {poParentStreams})
    sleep(rng.rand(int(full * 1000.0)))
    q.killHard()
    discard q.waitForExit()
    close(q)
    let r = checkKilled(path, logPath, seed, KillN)
    if r.err.len > 0:
      return (false, "kill " & $i & " (seed " & $seed & ", after seal " & $r.d & "): " & r.err)
    if r.inSeal: inc inSeal
    if r.d == 0: inc beforeFirst
    if r.d >= fullSeals: inc afterLast
  removeFile(path); removeFile(logPath)
  (true, $kills & " kills (a full run: " & $fullSeals & " seals in " &
    formatFloat(full, ffDecimal, 2) & " s): " & $inSeal & " inside a seal, " &
    $beforeFirst & " before the first seal, " & $afterLast & " after the last")

proc followerScenario(seconds: float): tuple[ok: bool, msg: string] =
  ## A writer process seals continuously; this process follows it attached
  ## and file-only, and every key of every seal it knows completed must be
  ## found, and nothing but inserted records returned.
  let path = tmpPath("follow")
  let logPath = path & ".seallog"
  removeFile(path); removeFile(logPath)
  const n = 400_000
  var p = startProcess(selfExe(), args = ["--writer", path, logPath, "9", $n, "0"],
                       options = {poParentStreams})
  let w = makeWorkload(9, n, WriterMinSeal, WriterMaxSeal)
  let o = initOracle(w)
  var rng = initRand(9)
  var lookups, attachedRounds, fileRounds = 0
  let t0 = epochTime()
  var msg = ""
  while epochTime() - t0 < seconds and msg.len == 0:
    if not fileExists(path) or lastSealLogged(logPath) == 0:
      sleep(5)
      continue
    for attachMode in [true, false]:
      # The seals this reader may demand: those logged complete before it
      # opens (the writer logs `done k` after seal k returned).
      let d = lastSealLogged(logPath)
      var rd0 = openLiveReader(path, attach = attachMode)
      if rd0.isErr:
        msg = "open: " & rd0.error
        break
      var rd = rd0.get()
      if attachMode and not rd.attached:
        rd.close()
        if lastSealLogged(logPath) < w.sealEnd.len and fileExists(logPath) and
            not readFile(logPath).contains("close"):
          msg = "the follower did not attach to a live writer's page: " & rd.attachRefusal
          break
        continue
      try:
        checkReader(rd, o, min(d, w.sealEnd.len),
          CheckScope(recSample: 300, keySample: 300, live: true), rng,
          (if attachMode: "attached" else: "file-only") & " follower after seal " & $d)
        lookups += 300
        if attachMode: inc attachedRounds else: inc fileRounds
      except CheckFailure as e:
        msg = e.msg
      rd.close()
  p.killHard()
  discard p.waitForExit()
  close(p)
  removeFile(path); removeFile(logPath)
  if msg.len > 0: return (false, msg)
  if attachedRounds == 0 or fileRounds == 0:
    return (false, "the follower never completed a round in both modes")
  (true, $attachedRounds & " attached and " & $fileRounds &
    " file-only rounds, " & $lookups & " lookups, against a live writer")

# ---------------------------------------------------------------------------
# Mutation arms: this binary re-run with an arm armed must be caught
# ---------------------------------------------------------------------------

proc armMain(arm: string) =
  ## Exit 3: the arm was caught and fired; 4: it fired and nothing caught it;
  ## 5: it never fired (the control did not run its subject).
  let msg = matrixScenario(2500, 60, 300, true, 0xA11'u64)
  when defined(ctfsKeyedMutation):
    let fired = keyedMutationFired
  else:
    let fired = 0
  echo "  arm ", arm, ": fired ", fired, " times; ", (if msg.len > 0: "caught: " & msg else: "NOT caught")
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
    writerMain(args[1], args[2], uint64(parseInt(args[3])), parseInt(args[4]),
               args[5] == "1")
    quit(0)
  if args.len > 0 and args[0] == "--arm":
    armMain(args[1])
  when not defined(ctfsKeyedMutation):
    {.error: "build this test with -d:ctfsKeyedMutation: its mutation arms are part of it".}
  let only = if args.len > 1 and args[0] == "--only": args[1] else: ""
  proc part(name: string): bool = only.len == 0 or only == name
  var failures: seq[string]
  if part("roundtrip"):
    echo "round trips"
    roundTrip(100_000, streaming = false)
    roundTrip(100_000, streaming = true)
  if part("matrix"):
    echo "every write prefix"
    let m1 = matrixScenario(2500, 60, 300, true, 0x5EED'u64)
    if m1.len > 0: failures.add "matrix (small, every record, node and key): " & m1
    let m2 = matrixScenario(40_000, 500, 3000, false, 0xB16'u64)
    if m2.len > 0: failures.add "matrix (deep, what the seal in flight wrote): " & m2
  if part("arms"):
    echo "mutation arms"
    for arm in ["b1-count-first", "b1-shrink-count-first", "size-before-record"]:
      if not runArm(arm):
        failures.add "mutation arm " & arm & " was not caught (or did not fire)"
  if part("kill"):
    echo "kills at random"
    let k = killScenario(200)
    echo "  ", k.msg
    if not k.ok: failures.add "kill at random: " & k.msg
  if part("follow"):
    echo "a follower in another process"
    let fo = followerScenario(6.0)
    echo "  ", fo.msg
    if not fo.ok: failures.add "follower: " & fo.msg
  if failures.len > 0:
    for f in failures: echo "FAIL: ", f
    quit(1)
  echo "PASS test_keyed_b1", (if only.len > 0: " (" & only & " only)" else: "")
