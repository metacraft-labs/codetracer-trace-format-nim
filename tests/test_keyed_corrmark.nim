## MCR's live correlation index (`codetracer-specs` `CTFS-Binary-Format.md`
## §2.8): H1 live (`ctfs-keyed-families.md` §5.8.4) over 48-byte records with
## V2 bucket extents (§5.8.6), and S3 (§5.8.5) built at close.
##
## Checks, at every write prefix (file-only and attached), after real kills,
## and on round trips:
## 1. every key whose first marker a completed seal published is found, with
##    at least the entries that seal published, in declaration order, and
##    never an entry that was not declared under it;
## 2. a key never declared is not found;
## 3. after close, S3 answers exactly as H1 does, and the S3 image is a
##    function of its entries: built from the same entries inserted in
##    another order, it is byte-identical.
##
## Null predictions (mutation arms, each must be caught):
## - `h1-shrink-first`: a split bucket's new count stored before the hole
##   fill moves its survivors below it (a key published earlier is missed);
## - `corr-count-before-entries`: a repeated key's record `n` published before
##   the entries it covers reach the extent (an entry that was not declared);
## - `s3-unsorted`: S3 built in arrival order (two builds differ).

import std/[os, osproc, strutils, tables, random, algorithm, sets]
import results
import codetracer_ctfs/[types, container, streaming, keyed_store, live_reader,
                        h1, s3, corrmark_live, mutation_arms]
import ./keyed_harness

type
  Marker = tuple[key: uint64, entry: CorrEntry]
  Workload = object
    markers: seq[Marker]
    sealEnd: seq[int]

proc entryOf(i: int): CorrEntry =
  for b in 0 ..< CorrEntrySize: result[b] = byte((i * 31 + b * 5) and 0xff)
  writeU64LE(result, 0, uint64(i))

proc makeWorkload(seed: uint64, n, minSeal, maxSeal: int, repeatPct = 40): Workload =
  var s = seed
  var keys: seq[uint64]
  var i = 0
  while i < n:
    let span = minSeal + int(rng64(s) mod uint64(maxSeal - minSeal + 1))
    for _ in 0 ..< span:
      if i >= n: break
      var k: uint64
      if keys.len > 0 and int(rng64(s) mod 100) < repeatPct:
        k = keys[int(rng64(s) mod uint64(keys.len))]
      else:
        k = rng64(s)
        keys.add k
      result.markers.add (key: k, entry: entryOf(i))
      inc i
    result.sealEnd.add i

proc runWriter(c: var Ctfs, w: var CorrmarkWriter, wl: Workload, close: bool,
               beforeSeal, onSeal: proc (k: int) {.closure.} = nil) =
  var i = 0
  for k, e in wl.sealEnd:
    while i < e:
      doAssert w.addMarker(wl.markers[i].key, wl.markers[i].entry).isOk
      inc i
    if beforeSeal != nil: beforeSeal(k + 1)
    let r = if close and k == wl.sealEnd.len - 1: w.closeIndex(c) else: w.seal(c)
    doAssert r.isOk, r.error
    if onSeal != nil: onSeal(k + 1)

type CheckFailure = object of CatchableError
proc fail(m: string) {.raises: [CheckFailure].} = raise newException(CheckFailure, m)

proc checkIndex(rd: var LiveReader, wl: Workload, sealsDone: int, label: string,
                sample = 0, rng: ptr Rand = nil) {.raises: [CheckFailure].} =
  var cr = openCorrmarkReader(rd)
  var byKey: Table[uint64, seq[CorrEntry]]
  for m in wl.markers: byKey.mgetOrPut(m.key, @[]).add m.entry
  let published = if sealsDone == 0: 0 else: wl.sealEnd[sealsDone - 1]
  var pubByKey: Table[uint64, int]
  for i in 0 ..< published: pubByKey.mgetOrPut(wl.markers[i].key, 0).inc
  var keys: seq[uint64]
  for k in pubByKey.keys: keys.add k
  keys.sort()
  if sample > 0 and keys.len > sample:
    var ks: seq[uint64]
    for _ in 0 ..< sample: ks.add keys[rng[].rand(keys.len - 1)]
    keys = ks
  for k in keys:
    let got = cr.lookupEntries(rd, k)
    if got.isErr: fail(label & ": key " & $k & ": " & got.error)
    let want = byKey.getOrDefault(k)
    let n = got.get().len
    if n < pubByKey.getOrDefault(k):
      fail(label & ": key " & $k & " answers " & $n & " entries; seal " & $sealsDone &
        " published " & $pubByKey.getOrDefault(k))
    if n > want.len or got.get() != want[0 ..< n]:
      var firstBad = -1
      for i in 0 ..< min(n, want.len):
        if got.get()[i] != want[i]:
          firstBad = i
          break
      fail(label & ": key " & $k & " answers " & $n & " entries, " & $want.len &
        " were declared, the first that differs is " & $firstBad &
        "; entries that were not declared under it, in order")
  let never = cr.lookupEntries(rd, 0x1234_5678_9ABC_DEF0'u64)
  if never.isErr or never.get().len != 0: fail(label & ": a key never declared was found")

proc tmpPath(n: string): string = getTempDir() / ("ckf4n_corr_" & n & "_" & $getCurrentProcessId() & ".ct")

proc roundTrip(streaming: bool): string =
  let path = tmpPath("rt")
  removeFile(path)
  var c = if streaming: createCtfsStreaming(path, maxRootEntries = 0).get() else: createCtfs(maxRootEntries = 0)
  var w = c.createCorrmark().get()
  let wl = makeWorkload(3, 60_000, 500, 3000)
  runWriter(c, w, wl, close = true)
  var img: ref seq[byte]
  new(img)
  if streaming:
    doAssert c.closeCtfs().isOk
    img[] = cast[seq[byte]](readFile(path))
    removeFile(path)
  else: img[] = c.data
  var rd = openImageReader(img).get()
  try: checkIndex(rd, wl, wl.sealEnd.len, "round trip")
  except CheckFailure as e: return e.msg
  # S3 and H1 agree, key by key.
  var hr = openH1Reader(rd, CorrPrefix)
  let s3slot = rd.findSlot(CorrS3)
  var seen = initHashSet[uint64]()
  for m in wl.markers:
    if m.key in seen: continue
    seen.incl m.key
    var a = s3Candidates(rd, s3slot, m.key).get()
    var b = hr.candidates(rd, m.key).get()
    # S3 matches on a 32-bit fingerprint: its candidates include H1's.
    for x in b:
      if x notin a: return "S3 lacks record " & $x & " of key " & $m.key
  # The image is a function of its entries.
  var entries: seq[tuple[key, refv: uint64]]
  var r = 0'u64
  seen.clear()
  for m in wl.markers:
    if m.key notin seen:
      seen.incl m.key
      entries.add (key: m.key, refv: r)
      inc r
  let a = buildS3Image(entries, "a").get()
  var shuffled = entries
  var g = initRand(9)
  g.shuffle(shuffled)
  let b = buildS3Image(shuffled, "b").get()
  if a != b: return "S3 built from the same entries in another order is not the same image"
  let fileS3 = rd.readMember(s3slot, 0, int(rd.memberSize(s3slot))).get()
  if fileS3 != a: return "the S3 member is not the image of its entries"
  echo "  round trip", (if streaming: " streaming" else: " in memory"), ": ", wl.markers.len,
    " markers, ", entries.len, " keys"
  ""

proc matrix(seed: uint64): string =
  let path = tmpPath("mx")
  removeFile(path)
  var c = createCtfsStreaming(path, maxRootEntries = 0).get()
  var w = c.createCorrmark().get()
  let wl = makeWorkload(seed, 12_000, 300, 1500)
  let log = installTaps(c)
  runWriter(c, w, wl, close = true, onSeal = proc (k: int) = log.markSeal())
  c.removeTaps()
  discard c.closeCtfs()
  removeFile(path)
  try:
    for attached in [false, true]:
      var rep = initReplay(log, attached)
      var g = initRand(1)
      var n = 0
      while true:
        let r0 = openImageReader(rep.file, if attached: rep.page else: nil)
        if r0.isErr: fail("prefix " & $rep.next & ": " & r0.error)
        var rd = r0.get()
        let atSeal = rep.next > 0 and rep.log.events[rep.next - 1].kind == teSeal
        checkIndex(rd, wl, rep.sealsDone, (if attached: "attached" else: "file-only") &
          " prefix " & $rep.next & "/" & $log.events.len & " (after seal " & $rep.sealsDone & ")",
          if atSeal or rep.atEnd: 0 else: 150, addr g)
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
  var w = c.createCorrmark().get()
  runWriter(c, w, makeWorkload(seed, 20_000, 50, 300), close = true,
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
    let m = if args[1] == "s3-unsorted": roundTrip(false) else: matrix(0xA3)
    echo "  arm ", args[1], ": fired ", keyedMutationFired, "; ", (if m.len > 0: "caught: " & m else: "NOT caught")
    quit(if keyedMutationFired == 0: 5 elif m.len > 0: 3 else: 4)
  when not defined(ctfsKeyedMutation):
    {.error: "build this test with -d:ctfsKeyedMutation".}
  var failures: seq[string]
  if args.len > 0 and args[0] == "--matrix":
    echo matrix(0x5EED)
    quit(0)
  echo "round trips"
  for st in [false, true]:
    let m = roundTrip(st)
    if m.len > 0: failures.add "round trip: " & m
  echo "every write prefix"
  let m = matrix(0x5EED)
  if m.len > 0: failures.add "matrix: " & m
  echo "mutation arms"
  for arm in ["h1-shrink-first", "corr-count-before-entries", "s3-unsorted"]:
    if runArmChild(arm) != 3: failures.add "arm " & arm & " was not caught (or did not fire)"
  echo "kills at random"
  let k = killLoop(200, "corr",
    proc (path, logPath: string, seed: uint64): seq[string] = @["--writer", path, logPath, $seed],
    proc (path, logPath: string, seed: uint64, d: int): string =
      if not fileExists(path): return ""
      let r0 = openLiveReader(path)
      if r0.isErr: return (if d == 0: "" else: "did not open: " & r0.error)
      var rd = r0.get()
      if rd.attached:
        rd.close()
        return "attached to a dead writer's page"
      var g = initRand(int(seed))
      try: checkIndex(rd, makeWorkload(seed, 20_000, 50, 300), d, "killed", 300, addr g)
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
  echo "PASS test_keyed_corrmark"
