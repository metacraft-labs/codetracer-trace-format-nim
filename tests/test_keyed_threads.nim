## C1, the two-level table over a clustered key, as MCR's per-thread family
## (`ctfs-keyed-families.md` §5.8.2; `codetracer-specs`
## `CTFS-Binary-Format.md` §2.3, `threads.rec` + `threads.top`, `k = 6`),
## with each thread's two keyed streams.
##
## Checks, at every write prefix (file-only and attached), after real kills,
## and on round trips:
## 1. every present record's once-written fields are its thread's, and its
##    stream words name a prefix of its streams' bytes, which read back
##    byte-exact through the stream's `MapBlock`;
## 2. every thread a completed seal created is found by `ct_tid`, with at
##    least the stream bytes that seal published;
## 3. a `ct_tid` never created is absent, and enumeration yields exactly the
##    present threads.
##
## Null predictions (mutation arms, each must be caught):
## - `c1-state-before-record`: `state` set present before the record's other
##   words are written: a present record with zero once-written fields.
## - `size-before-record`: a member's Size published before the bytes it
##   covers.

import std/[os, osproc, strutils, tables, random, times, sets]
import results
import codetracer_ctfs/[types, container, streaming, keyed_store, live_reader,
                        thread_records, mutation_arms]
import ./keyed_harness

type
  ThreadPlan = object
    info: ThreadInfo
    createdAt: int            ## the seal that creates its record (1-based)
    chunks: seq[seq[byte]]    ## event chunks, in order
    chunkSeal: seq[int]       ## the seal that publishes chunk i
  Workload = object
    threads: seq[ThreadPlan]
    seals: int

proc ctTidSet(seed: uint64, n: int): seq[uint32] =
  ## Clustered: a few processes at the 1024 stride, dense within each, with
  ## a hole now and then.
  var s = seed
  var procs = 1 + int(rng64(s) mod 3)
  var seen = initHashSet[uint32]()
  while result.len < n:
    let p = uint32(rng64(s) mod uint64(procs))
    let local = uint32(rng64(s) mod 600)
    let t = p * 1024 + local
    if t notin seen:
      seen.incl t
      result.add t

proc makeWorkload(seed: uint64, nThreads, seals: int): Workload =
  var s = seed xor 0x77
  let tids = ctTidSet(seed, nThreads)
  result.seals = seals
  for i, t in tids:
    var tp = ThreadPlan(info: ThreadInfo(ctTid: t, ringTid: uint32(i), process: t div 1024,
      image: uint32(rng64(s) mod 2), geidFloor: rng64(s) mod 1_000_000),
      createdAt: 1 + int(rng64(s) mod uint64(seals)))
    var k = tp.createdAt
    while k <= seals:
      if rng64(s) mod 3 == 0:
        var c = newSeq[byte](int(50 + rng64(s) mod 3000))
        for b in 0 ..< c.len: c[b] = byte((uint64(b) * 7 + uint64(t) + rng64(s)) and 0xff)
        tp.chunks.add c
        tp.chunkSeal.add k
      else:
        inc k
    result.threads.add tp

proc indexBytesOf(tp: ThreadPlan, upto: int): seq[byte] =
  ## The `index` stream after its first `upto` chunks: `chunk_size`, then
  ## each chunk's offset.
  result = newSeq[byte](4)
  writeU32LE(result, 0, 256'u32)
  var off = 0'u64
  for i in 0 ..< upto:
    var b: array[8, byte]
    writeU64LE(b, 0, off)
    result.add b
    off += uint64(tp.chunks[i].len)

proc runWriter(c: var Ctfs, w: var ThreadsWriter, wl: Workload,
               beforeSeal, onSeal: proc (k: int) {.closure.} = nil) =
  for k in 1 .. wl.seals:
    for tp in wl.threads:
      if tp.createdAt == k:
        doAssert w.addThread(tp.info).isOk
        doAssert c.appendIndex(w, tp.info.ctTid, indexBytesOf(tp, 0)).isOk
      for i, cs in tp.chunkSeal:
        if cs == k:
          discard c.appendEvents(w, tp.info.ctTid, tp.chunks[i]).get()
          var b: array[8, byte]
          var off = 0'u64
          for j in 0 ..< i: off += uint64(tp.chunks[j].len)
          writeU64LE(b, 0, off)
          doAssert c.appendIndex(w, tp.info.ctTid, b).isOk
    if beforeSeal != nil: beforeSeal(k)
    let r = w.seal(c)
    doAssert r.isOk, r.error
    if onSeal != nil: onSeal(k)

type CheckFailure = object of CatchableError
proc fail(m: string) {.raises: [CheckFailure].} = raise newException(CheckFailure, m)

proc eventsBytes(tp: ThreadPlan): seq[byte] =
  for c in tp.chunks: result.add c

proc checkThreads(rd: var LiveReader, wl: Workload, sealsDone: int, label: string) {.raises: [CheckFailure].} =
  var t = openThreadsReader(rd)
  let all = t.enumerateThreads(rd)
  if all.isErr: fail(label & ": enumeration: " & all.error)
  var byTid: Table[uint32, ThreadPlan]
  for tp in wl.threads: byTid[tp.info.ctTid] = tp
  var present = initHashSet[uint32]()
  for v in all.get():
    if v.info.ctTid notin byTid: fail(label & ": a present record names ct_tid " & $v.info.ctTid)
    let tp = byTid.getOrDefault(v.info.ctTid)
    if v.info != tp.info:
      fail(label & ": record " & $v.record & " (ct_tid " & $v.info.ctTid &
        ") carries once-written fields " & $v.info & ", not " & $tp.info)
    present.incl v.info.ctTid
    let ev = eventsBytes(tp)
    if v.events.size > uint64(ev.len): fail(label & ": ct_tid " & $v.info.ctTid & "'s events stream is too long")
    if v.events.size > 0:
      let b = rd.readStream("events of " & $v.info.ctTid, v.events.size, v.events.mapBlock, 0, int(v.events.size))
      if b.isErr: fail(label & ": ct_tid " & $v.info.ctTid & " events: " & b.error)
      if b.get() != ev[0 ..< int(v.events.size)]:
        fail(label & ": ct_tid " & $v.info.ctTid & "'s events stream is not its bytes")
    if v.index.size > 0:
      let b = rd.readStream("index of " & $v.info.ctTid, v.index.size, v.index.mapBlock, 0, int(v.index.size))
      if b.isErr: fail(label & ": ct_tid " & $v.info.ctTid & " index: " & b.error)
      let want = indexBytesOf(tp, tp.chunks.len)
      if v.index.size > uint64(want.len) or b.get() != want[0 ..< int(v.index.size)]:
        fail(label & ": ct_tid " & $v.info.ctTid & "'s index stream is not its bytes")
  for tp in wl.threads:
    if tp.createdAt <= sealsDone:
      let l = t.lookupThread(rd, tp.info.ctTid)
      if l.isErr: fail(label & ": lookup of " & $tp.info.ctTid & ": " & l.error)
      if not l.get().found:
        fail(label & ": ct_tid " & $tp.info.ctTid & ", created by seal " & $tp.createdAt & ", is not found")
      var want = 0'u64
      for i, cs in tp.chunkSeal:
        if cs <= sealsDone: want += uint64(tp.chunks[i].len)
      if l.get().v.events.size < want:
        fail(label & ": ct_tid " & $tp.info.ctTid & "'s events hold " & $l.get().v.events.size &
          " bytes, and seal " & $sealsDone & " published " & $want)
  let never = t.lookupThread(rd, 999_999)
  if never.isErr or never.get().found: fail(label & ": a ct_tid never created was found")

proc tmpPath(n: string): string = getTempDir() / ("ckf4n_thr_" & n & "_" & $getCurrentProcessId() & ".ct")

proc roundTrip(streaming: bool) =
  let path = tmpPath("rt")
  removeFile(path)
  var c = if streaming: createCtfsStreaming(path, maxRootEntries = 0).get() else: createCtfs(maxRootEntries = 0)
  var w = c.createThreadsFamily().get()
  let wl = makeWorkload(5, 300, 12)
  runWriter(c, w, wl)
  var img: ref seq[byte]
  new(img)
  if streaming:
    doAssert c.closeCtfs().isOk
    img[] = cast[seq[byte]](readFile(path))
    removeFile(path)
  else: img[] = c.data
  var rd = openImageReader(img).get()
  try: checkThreads(rd, wl, wl.seals, "round trip")
  except CheckFailure as e: doAssert false, e.msg
  var t = openThreadsReader(rd)
  doAssert t.enumerateThreads(rd).get().len == wl.threads.len
  echo "  round trip", (if streaming: " streaming" else: " in memory"), ": ", wl.threads.len, " threads"

proc matrix(seed: uint64): string =
  let path = tmpPath("mx")
  removeFile(path)
  var c = createCtfsStreaming(path, maxRootEntries = 0).get()
  var w = c.createThreadsFamily().get()
  let wl = makeWorkload(seed, 150, 8)
  let log = installTaps(c)
  runWriter(c, w, wl, onSeal = proc (k: int) = log.markSeal())
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
        checkThreads(rd, wl, rep.sealsDone, (if attached: "attached" else: "file-only") &
          " prefix " & $rep.next & "/" & $log.events.len & " (after seal " & $rep.sealsDone & ")")
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
  var w = c.createThreadsFamily().get()
  runWriter(c, w, makeWorkload(seed, 400, 60),
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
    let m = matrix(0xA1)
    echo "  arm ", args[1], ": fired ", keyedMutationFired, "; ", (if m.len > 0: "caught: " & m else: "NOT caught")
    quit(if keyedMutationFired == 0: 5 elif m.len > 0: 3 else: 4)
  when not defined(ctfsKeyedMutation):
    {.error: "build this test with -d:ctfsKeyedMutation".}
  var failures: seq[string]
  echo "round trips"
  roundTrip(false)
  roundTrip(true)
  echo "every write prefix"
  let m = matrix(0x5EED)
  if m.len > 0: failures.add "matrix: " & m
  echo "mutation arms"
  for arm in ["c1-state-before-record", "size-before-record"]:
    if runArmChild(arm) != 3: failures.add "arm " & arm & " was not caught (or did not fire)"
  echo "kills at random"
  let k = killLoop(200, "thr",
    proc (path, logPath: string, seed: uint64): seq[string] = @["--writer", path, logPath, $seed],
    proc (path, logPath: string, seed: uint64, d: int): string =
      if not fileExists(path): return ""
      let r0 = openLiveReader(path)
      if r0.isErr: return (if d == 0: "" else: "did not open: " & r0.error)
      var rd = r0.get()
      if rd.attached:
        rd.close()
        return "attached to a dead writer's page"
      try: checkThreads(rd, makeWorkload(seed, 400, 60), d, "killed")
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
  echo "PASS test_keyed_threads"
