## A namespace committed live (`ctfs-container.md` §8, "Live And Incremental
## Publication"): a commit appends its pages, publishes the member, stores
## page 0's allocation words, then flips the root slot with `commit_id =
## other + 1`; a reader double-reads the slots; no superseded page is reused.
##
## At every write prefix of a run of commits, a reader of the rebuilt file
## finds the committed root below the member's published `Size`, and every key
## a completed commit inserted, with its descriptor; it never refuses.
##
## Null predictions:
## - `ns-commit-before-pages` (the root flipped before the member's `Size`
##   covers the pages it reaches): at the prefix after the flip a reader finds
##   the root past the published `Size` and refuses, or misses a key. Caught.
## - A tree that reclaimed a superseded page is refused by the publisher,
##   naming the namespace (driven below).

import std/[os, osproc, strutils, tables]
import results
import codetracer_ctfs/[types, container, streaming, keyed_store, live_reader,
                        cow_btree, namespace_live, mutation_arms]
import ./keyed_harness

proc desc(k: uint64): seq[byte] =
  result = newSeq[byte](16)
  writeU64LE(result, 0, k * 3)
  writeU64LE(result, 8, k xor 0x5555)

proc run(): string =
  let path = getTempDir() / ("ckf4n_ns_" & $getCurrentProcessId() & ".ct")
  removeFile(path)
  var c = createCtfsStreaming(path, maxRootEntries = 0).get()
  var ns = c.createLiveNamespace("cv.ns").get()
  var t = initCowBTree(cltTypeB)
  let log = installTaps(c)
  var keysBy: seq[seq[uint64]]
  var s = 77'u64
  var all: seq[uint64]
  for commitNo in 0 ..< 40:
    var ks: seq[uint64]
    for _ in 0 ..< 1 + int(rng64(s) mod 60):
      let k = rng64(s)
      doAssert t.insertAndCommit(k, desc(k)).isOk
      ks.add k
    let r = ns.commit(c, t)
    if r.isErr: return "commit: " & r.error
    log.markSeal()
    keysBy.add ks
  c.removeTaps()
  doAssert c.closeCtfs().isOk
  removeFile(path)
  var rep = initReplay(log, false)
  var prefixes = 0
  while true:
    var rd0 = openImageReader(rep.file)
    if rd0.isErr: return "prefix " & $rep.next & ": " & rd0.error
    var rd = rd0.get()
    let root = readLiveRoot(rd, "cv.ns")
    if root.isErr: return "prefix " & $rep.next & ": " & root.error
    if rep.sealsDone > 0:
      if root.get().empty: return "prefix " & $rep.next & ": no committed root after commit " & $rep.sealsDone
      let slot = rd.findSlot("cv.ns")
      let img = rd.readMember(slot, 0, int(rd.memberSize(slot))).get()
      let tree = loadCowBTreeForTest(img, cltTypeB)
      if tree.isErr: return "prefix " & $rep.next & ": " & tree.error
      for c in 0 ..< rep.sealsDone:
        for k in keysBy[c]:
          let v = tree.get().lookupFromRootForTest(root.get().root, k)
          if v.isErr or v.get() != desc(k):
            return "prefix " & $rep.next & ": key " & $k & " of commit " & $(c + 1) & " is not found"
    inc prefixes
    if rep.atEnd: break
    rep.applyNext()
  echo "  ", prefixes, " prefixes of 40 commits"
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
  # A tree that reclaimed a superseded page is refused.
  var c = createCtfs(maxRootEntries = 0)
  var ns = c.createLiveNamespace("re.ns").get()
  var t = initCowBTree(cltTypeB)
  for k in 1'u64 .. 300: doAssert t.insertAndCommit(k, desc(k)).isOk
  discard t.reclaimPending()
  let r = ns.commit(c, t)
  if r.isOk or "re.ns" notin r.error:
    echo "FAIL: a tree that reclaimed superseded pages was published live"
    quit(1)
  if runArmChild("ns-commit-before-pages") != 3:
    echo "FAIL: the arm ns-commit-before-pages was not caught"
    quit(1)
  echo "PASS test_namespace_live_commit"
