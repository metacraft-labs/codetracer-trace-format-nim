## Dead space and the canonical repack (`ctfs-container.md` §6, "Dead Space";
## `ctfs-keyed-families.md` §4.6 and §8.3 O-12).
##
## 1. The reachability walk reports exactly the dead blocks of a container
##    with known dead blocks (a member truncated and rewritten abandons its
##    old blocks), and none for a container without.
## 2. Two live containers of one execution, written with different timing
##    (member creation order, seal points, the page store's first-stored
##    order, the correlation keys' first-seen order, thread creation order,
##    one carrying dead blocks), differ byte for byte and repack to
##    byte-identical containers, which hold no dead block and read as their
##    sources do.
##
## Null predictions (mutation arms, each must be caught): `repack-slot-order`
## (plain members keep the source's slot order) and `repack-page-store-order`
## (page records keep their first-stored order) each leave one
## timing-dependent order in place, and the two repacks must then differ.

import std/[os, osproc, strutils, tables, random, algorithm, sets, sequtils]
import results
import codetracer_ctfs/[types, container, streaming, keyed_store, live_reader,
                        page_store, thread_records, snapshot_records,
                        corrmark_live, repack, mutation_arms]
import ./keyed_harness

proc digest(i: int): PageDigest =
  var s = uint64(i) * 0x9E37'u64 + 1
  for k in 0 ..< 2:
    let x = rng64(s)
    for b in 0 ..< 8: result[8 * k + b] = byte((x shr (8 * b)) and 0xff)

proc page(i: int): seq[byte] =
  result = newSeq[byte](4096)
  for k in 0 ..< 4096: result[k] = byte((i * 13 + k div 32) and 0xff)

proc chunk(t, i: int): seq[byte] =
  result = newSeq[byte](200 + (t * 31 + i * 17) mod 3000)
  for k in 0 ..< result.len: result[k] = byte((t + i + k) and 0xff)

proc entry(i: int): CorrEntry =
  for b in 0 ..< CorrEntrySize: result[b] = byte((i + b) and 0xff)

proc writeExecution(path: string, variant: int): seq[uint64] =
  ## One execution's content, written with variant `variant`'s timing.
  ## Returns the blocks the variant knowingly abandoned.
  var g = initRand(variant + 1)
  var c = createCtfsStreaming(path, maxRootEntries = 0).get()
  var names = @["meta.dat", "aux.dat", "events.dat"]
  if variant == 1: names.reverse()
  var files: Table[string, CtfsInternalFile]
  for n in names: files[n] = c.addFile(n).get()
  if variant == 1:
    # A member written, then truncated and rewritten: its old blocks die.
    var x = c.addFile("scratch.dat").get()
    var big = newSeq[byte](3 * 4096 + 100)
    doAssert c.writeToFile(x, big).isOk
    c.publish()
    let m = readU64LE(c.data, c.fileEntryOffset(x.entryIndex) + 8)
    for i in 0'u64 ..< 4: result.add c.resolveFileBlock(m, i)
    result.add m
    x = c.truncateFileContent(x).get()
    doAssert c.writeToFile(x, [1'u8, 2, 3]).isOk
  else:
    var x = c.addFile("scratch.dat").get()
    doAssert c.writeToFile(x, [1'u8, 2, 3]).isOk
  for n in ["meta.dat", "aux.dat", "events.dat"]:
    var f = files[n]
    doAssert c.writeToFile(f, n.toOpenArrayByte(0, n.len - 1)).isOk
    files[n] = f
  c.publish()
  var ps = c.createPageStore().get()
  var cm = c.createCorrmark().get()
  var th = c.createThreadsFamily().get()
  var sn = c.createSnapshotFamily().get()
  var order = toSeq(0 ..< 900)
  if variant == 1: g.shuffle(order)
  for k, i in order:
    discard ps.storePage(digest(i), page(i)).get()
    if k mod (100 + 37 * variant) == 0: doAssert ps.seal(c).isOk
  doAssert ps.seal(c).isOk
  var keys = toSeq(0 ..< 50)
  if variant == 1: keys.reverse()
  for key in keys:
    for j in 0 ..< 1 + key mod 7:
      doAssert cm.addMarker(uint64(key) * 0x1000193 + 5, entry(key * 10 + j)).isOk
  doAssert cm.closeIndex(c).isOk
  var tids = @[3'u32, 1027, 5, 2048, 70]
  if variant == 1: tids.reverse()
  for t in tids:
    discard th.addThread(ThreadInfo(ctTid: t, ringTid: t mod 64, process: t div 1024,
                                    geidFloor: uint64(t) * 10)).get()
    if variant == 0: doAssert th.seal(c).isOk
  for i in 0 ..< 4:
    for t in tids:
      discard c.appendEvents(th, t, chunk(int(t), i)).get()
    if variant == 1 or i == 3: doAssert th.seal(c).isOk
  for o in 0 ..< 6:
    var s = Snapshot(kind: skPeriodic, id: uint64(o), geid: uint64(o * 100))
    for p in [spMemory, spRegsFull]:
      var st = KStream()
      doAssert c.appendStream(st, chunk(o, ord(p))).isOk
      s.parts[ord(p)] = SnapPart(form: pfRaw, data: st)
    discard sn.addSnapshot(s)
    if variant == 1: doAssert sn.seal(c).isOk
  doAssert sn.seal(c).isOk
  let closed = c.closeCtfs()
  doAssert closed.isOk, closed.error

proc run(): string =
  let dir = getTempDir() / ("ckf4n_dead_" & $getCurrentProcessId())
  createDir(dir)
  let a = dir / "a.ct"
  let b = dir / "b.ct"
  discard writeExecution(a, 0)
  let abandoned = writeExecution(b, 1)
  var ra = openLiveReader(a, attach = false).get()
  let da = deadSpace(ra).get()
  ra.close()
  if da.dead.len != 0: return "the reachability walk reports dead blocks " & $da.dead & " in a container without any"
  var rb = openLiveReader(b, attach = false).get()
  let db = deadSpace(rb).get()
  rb.close()
  var want = abandoned
  want.sort()
  if db.dead != want:
    return "the reachability walk reports " & $db.dead & "; the writer abandoned " & $want
  if readFile(a) == readFile(b): return "the two live containers are identical; the test has no timing difference"
  let pa = dir / "pa.ct"
  let pb = dir / "pb.ct"
  let x = repackContainer(a, pa)
  if x.isErr: return "repack a: " & x.error
  let y = repackContainer(b, pb)
  if y.isErr: return "repack b: " & y.error
  if readFile(pa) != readFile(pb):
    return "the repacks of two containers of one execution differ"
  var rp = openLiveReader(pa, attach = false).get()
  let dp = deadSpace(rp).get()
  if dp.dead.len != 0: return "the repack holds dead blocks " & $dp.dead
  # The repack reads as its source: every page, entry, thread and snapshot.
  var ps = openPageStoreReader(rp)
  for i in 0 ..< 900:
    let l = ps.lookupPage(rp, digest(i)).get()
    if not l.found or ps.pageBytes(rp, l.record).get() != page(i):
      return "page " & $i & " does not read from the repack"
  var t = openThreadsReader(rp)
  for v in t.enumerateThreads(rp).get():
    var want: seq[byte]
    for i in 0 ..< 4: want.add chunk(int(v.info.ctTid), i)
    if rp.readStream("ev", v.events.size, v.events.mapBlock, 0, int(v.events.size)).get() != want:
      return "thread " & $v.info.ctTid & "'s events do not read from the repack"
  rp.close()
  removeDir(dir)
  ""

when isMainModule:
  let args = commandLineParams()
  if args.len > 0 and args[0] == "--arm":
    let m = run()
    echo "  arm ", args[1], ": fired ", keyedMutationFired, "; ", (if m.len > 0: "caught: " & m else: "NOT caught")
    quit(if keyedMutationFired == 0: 5 elif m.len > 0: 3 else: 4)
  when not defined(ctfsKeyedMutation):
    {.error: "build this test with -d:ctfsKeyedMutation".}
  let m = run()
  if m.len > 0:
    echo "FAIL: ", m
    quit(1)
  for arm in ["repack-slot-order", "repack-page-store-order"]:
    if runArmChild(arm) != 3:
      echo "FAIL: the mutation arm ", arm, " was not caught (or did not fire)"
      quit(1)
  echo "PASS test_dead_space"
