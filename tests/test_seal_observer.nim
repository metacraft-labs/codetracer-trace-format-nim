## The per-seal log observer (`codetracer-specs` `CTFS-Binary-Format.md`
## §2.15.2; `CTFS-Keyed-Families` CKF-4N): a test API of the library, fired
## from its one seal and nowhere else.
##
## 1. **It changes nothing.** A scripted single-threaded writer, with root
##    members and a keyed page store, issues the same container writes
##    (offset, length, bytes) with and without an observer registered.
## 2. **It sees every publication, and nothing else.** Folding every seal's
##    items (root entries, and the in-place words of records and index nodes)
##    gives exactly what the file shows after close: every non-empty root
##    entry, every B1 node word 0, `high`, `right` and slot below its count,
##    and the header's words (§2.15.5 step 7, "a closed recording, the
##    positive control"). Every item appears in the file, and every
##    publication the file shows appears in the items.
## 3. **No empty seal.** Every `beforeSeal` carries at least one item, and the
##    creation write fires nothing.
## 4. **afterSeal's epoch** is the writer slot's `seal_epoch`, equal to the
##    seal's ordinal, and seals are numbered 1, 2, 3, ... with no gap.
## 5. **Refusals.** While an observer is registered, the library refuses root
##    growth and `appendInternalFiles` on that container, naming it.
##
## Null predictions (mutation arms, each must be caught):
## - `observer-skips-close`: the observer is not called by `closeCtfs`'s final
##   seal; step 2 must find a publication the items lack.
## - `observer-empty-seal`: a publication that changes nothing fires the
##   observer; step 3 must report an empty seal.

import std/[os, osproc, strutils, tables]
import results
import codetracer_ctfs/[types, container, streaming, keyed_store, live_reader,
                        b1, page_store, container_append, mutation_arms]

type
  Write = tuple[off: int64, bytes: seq[byte]]
  Log = ref object
    seals: seq[tuple[ordinal: uint64, items: seq[SealItem]]]
    epochs: seq[tuple[ordinal, epoch: uint64]]

proc digest(i: int): PageDigest =
  for k in 0 ..< 16: result[k] = byte((i * 31 + k * 7 + (i shr 8) * 13) and 0xff)

proc script(path: string, observe: bool, log: Log): seq[Write] =
  ## The scripted writer: root members, then a page store, sealed several
  ## times, a publication that changes nothing, then close.
  var writes: seq[Write]
  var c = createCtfsStreaming(path, maxRootEntries = 0).get()
  c.writeTap = proc (offset: int64, bytes: openArray[byte]) {.closure, gcsafe,
      raises: [].} =
    writes.add (off: offset, bytes: @bytes)
  if observe:
    let before = proc (ordinal: uint64, items: seq[SealItem]) {.closure, gcsafe, raises: [].} =
      log.seals.add (ordinal: ordinal, items: items)
    let after = proc (ordinal, epoch: uint64) {.closure, gcsafe, raises: [].} =
      log.epochs.add (ordinal: ordinal, epoch: epoch)
    let annotate = proc (item: SealItem): seq[string] {.closure, gcsafe, raises: [].} =
      if item.kind == sikEntry and item.name == "events.dat":
        result = @["{\"t\":\"eventlog\",\"size\":" & $item.size & "}"]
    c.registerSealObserver(SealObserver(beforeSeal: before, afterSeal: after,
                                        annotate: annotate))
  var meta = c.addFile("meta.dat").get()
  doAssert c.writeToFile(meta, [1'u8, 2, 3, 4]).isOk
  c.publish()
  var ps = c.createPageStore().get()
  var ev = c.addFile("events.dat").get()
  var page = newSeq[byte](4096)
  var n = 0
  for s in 0 ..< 6:
    for k in 0 ..< 300:
      page[0] = byte(n and 0xff); page[1] = byte(n shr 8)
      discard ps.storePage(digest(n), page).get()
      inc n
    doAssert ps.seal(c).isOk
    doAssert c.writeToFile(ev, page[0 ..< 1000 + 100 * s]).isOk
    c.publish()
    c.publish()          # changes nothing: not a seal
  doAssert c.writeToFile(ev, page[0 ..< 77]).isOk   # left for close's final seal
  doAssert c.closeCtfs().isOk
  writes

proc compare(path: string, log: Log): string =
  ## Step 2: rebuild every member from the items alone (an entry item's bytes
  ## `[from, size)`, a `replace` restarting the member, then every in-place
  ## word an index or record item names, in order) and require the closed
  ## file's members to be exactly those bytes, and its root to be exactly the
  ## entries the items name.
  var members: Table[string, seq[byte]]
  var entries: Table[int, (uint64, uint64, string)]
  for s in log.seals:
    for it in s.items:
      case it.kind
      of sikEntry:
        entries[it.entry] = (it.size, it.mapBlock, it.name)
        var m = members.getOrDefault(it.name)
        if it.replace: m.setLen(0)
        if uint64(m.len) != it.fromSize:
          return "item for " & it.name & " starts at " & $it.fromSize &
            " where the items so far end at " & $m.len
        if uint64(it.bytes.len) != it.size - it.fromSize:
          return "item for " & it.name & " carries " & $it.bytes.len & " bytes for [" &
            $it.fromSize & ", " & $it.size & ")"
        m.add it.bytes
        members[it.name] = m
      of sikIndex, sikRecord:
        if it.name notin members:
          return "an in-place item names " & it.name & ", which no entry item published"
        var m = members.getOrDefault(it.name)
        for w in it.words:
          let at = int(it.record) + w.at
          if at + 8 > m.len:
            return "an in-place word of " & it.name & " at " & $at & " lies past its published bytes"
          for b in 0 ..< 8: m[at + b] = byte((w.v shr (8 * b)) and 0xff)
        members[it.name] = m
      of sikStream, sikSemantic: discard
  var r = openLiveReader(path).get()
  for i in 0 ..< r.slots:
    let e = r.entries[i]
    if e.name == 0 and i notin entries: continue
    if i notin entries:
      return "the file publishes entry " & $i & " (" & r.slotName(i) & "), which no item names"
    let x = entries.getOrDefault(i)
    if x != (e.size, e.mapBlock, r.slotName(i)):
      return "entry " & $i & " in the file is " & $(e.size, e.mapBlock, r.slotName(i)) &
        ", and the items say " & $x
    let bytes = r.readMember(i, 0, int(e.size)).get()
    if bytes != members.getOrDefault(r.slotName(i)):
      return "member " & r.slotName(i) & " in the file differs from the bytes its items publish"
  r.close()
  ""

proc checks(log: Log, path: string): string =
  for s in log.seals:
    if s.items.len == 0:
      return "seal " & $s.ordinal & " fired the observer with no item (an empty seal)"
    # The annotator's items follow the structural item they annotate.
    for i, it in s.items:
      if it.kind == sikEntry and it.name == "events.dat":
        if i + 1 >= s.items.len or s.items[i + 1].kind != sikSemantic or
            ("\"size\":" & $it.size) notin s.items[i + 1].semantic:
          return "seal " & $s.ordinal & ": the annotator's item does not follow events.dat's entry"
  for i, s in log.seals:
    if s.ordinal != uint64(i + 1):
      return "seal ordinals are not 1, 2, 3, ...: the " & $(i + 1) & "th is " & $s.ordinal
  if log.epochs.len != log.seals.len:
    return $log.seals.len & " beforeSeal calls and " & $log.epochs.len & " afterSeal calls"
  for e in log.epochs:
    if e.epoch != e.ordinal:
      return "seal " & $e.ordinal & " reported epoch " & $e.epoch
  compare(path, log)

proc run(): string =
  let p1 = getTempDir() / ("ckf4n_obs_a_" & $getCurrentProcessId() & ".ct")
  let p2 = getTempDir() / ("ckf4n_obs_b_" & $getCurrentProcessId() & ".ct")
  removeFile(p1); removeFile(p2)
  let log = Log()
  let without = script(p1, false, Log())
  let with = script(p2, true, log)
  if without != with:
    return "the observer changed the writes: " & $without.len & " without it, " &
      $with.len & " with it"
  let c = checks(log, p2)
  removeFile(p1)
  if c.len > 0: return c
  # Step 5: the refusals while an observer is registered.
  var cc = createCtfs(maxRootEntries = 31)
  cc.registerSealObserver(SealObserver())
  for i in 0 ..< 31: discard cc.addFile("m" & $i).get()
  let g = cc.growRootDirectory()
  if g.isOk or "observer" notin g.error:
    return "root growth was not refused with an observer registered"
  var oc = createCtfsStreaming(p2 & ".2").get()
  oc.registerSealObserver(SealObserver())
  discard oc.addFile("a.dat").get()
  oc.publish()
  let ap = appendInternalFiles(p2 & ".2", ["b.dat"], [@[1'u8, 2]])
  if ap.isOk or "observer" notin ap.error:
    return "appendInternalFiles was not refused on a container with an observer: " &
      (if ap.isErr: ap.error else: "appended")
  discard oc.closeCtfs()
  # Unregistered by close: the same append is accepted now.
  let ap2 = appendInternalFiles(p2 & ".2", ["b.dat"], [@[1'u8, 2]])
  if ap2.isErr: return "appendInternalFiles after close was refused: " & ap2.error
  removeFile(p2 & ".2")
  removeFile(p2)
  ""

when isMainModule:
  let args = commandLineParams()
  if args.len > 0 and args[0] == "--arm":
    let msg = run()
    echo "  arm ", args[1], ": fired ", keyedMutationFired, "; ",
      (if msg.len > 0: "caught: " & msg else: "NOT caught")
    quit(if keyedMutationFired == 0: 5 elif msg.len > 0: 3 else: 4)
  when not defined(ctfsKeyedMutation):
    {.error: "build this test with -d:ctfsKeyedMutation: its mutation arms are part of it".}
  let msg = run()
  if msg.len > 0:
    echo "FAIL: ", msg
    quit(1)
  for arm in ["observer-skips-close", "observer-empty-seal"]:
    putEnv("CTFS_KEYED_MUTATE", arm)
    let p = startProcess(getAppFilename(), args = ["--arm", arm], options = {poParentStreams})
    let rc = p.waitForExit()
    close(p)
    delEnv("CTFS_KEYED_MUTATE")
    if rc != 3:
      echo "FAIL: the mutation arm ", arm, " exited ", rc, ", not 3 (caught)"
      quit(1)
  echo "PASS test_seal_observer"
