## Writers of a live coordination page (`ctfs-container.md` §6, "Claiming a
## writer slot", W2, "A writer that dies"):
##
## 1. producer processes join the coordinator's page, each claiming one
##    writer slot; the 16th registration (the coordinator and 15 producers
##    hold all 16) is refused, naming the page;
## 2. a producer killed with a root mirror slot left odd is found dead by a
##    survivor, which marks its slot dead and settles the slot (an even
##    `seq`), after which an attached reader accepts the payload;
## 3. a live producer is not marked dead.
##
## Null prediction: without the settle, the odd `seq` makes every attached
## read of that slot retry and give up (asserted before the settle).

import std/[os, osproc, strutils]
import results
import codetracer_ctfs/[types, container, streaming, live_page]

proc childMain(path: string, leaveOdd: bool) =
  var f: File
  doAssert open(f, path, fmReadWriteExisting)
  let p = joinLivePage(f, 170)
  if p.isErr:
    writeFile(path & ".refused." & $getCurrentProcessId(), p.error)
    quit(0)
  if leaveOdd:
    # Die between the odd and the even store of root entry 5.
    let off = OffRootMirror + RootMirrorSlotSize * 5
    let s = p.get().view.loadRelaxed(off)
    p.get().view.store(off, s + 1)
    p.get().view.store(off + 8, 4096)
  writeFile(path & ".ready." & $getCurrentProcessId(), "1")
  while true: sleep(1000)

proc waitFile(prefix: string, n: int): bool =
  for _ in 0 ..< 500:
    var k = 0
    let dir = parentDir(prefix)
    for f in walkFiles(dir / (extractFilename(prefix) & ".*")): inc k
    if k >= n: return true
    sleep(20)
  false

when isMainModule:
  let args = commandLineParams()
  if args.len == 3 and args[0] == "--child":
    childMain(args[1], args[2] == "1")
  let path = getTempDir() / ("ckf4n_writers_" & $getCurrentProcessId() & ".ct")
  removeFile(path)
  for f in walkFiles(path & ".*"): removeFile(f)
  var c = createCtfsStreaming(path, maxRootEntries = 0).get()
  var procs: seq[Process]
  # One producer that will die mid-update, then 14 live ones: 16 slots taken.
  procs.add startProcess(getAppFilename(), args = ["--child", path, "1"], options = {poParentStreams})
  doAssert waitFile(path & ".ready", 1), "the first producer did not join"
  for i in 0 ..< 14:
    procs.add startProcess(getAppFilename(), args = ["--child", path, "0"], options = {poParentStreams})
  doAssert waitFile(path & ".ready", 15), "15 producers did not join"
  # The 16th producer is refused, naming the page.
  var extra = startProcess(getAppFilename(), args = ["--child", path, "0"], options = {poParentStreams})
  doAssert waitFile(path & ".refused", 1), "a 16th registration was not refused"
  discard extra.waitForExit()
  close(extra)
  for f in walkFiles(path & ".refused.*"):
    let msg = readFile(f)
    doAssert "writer slots" in msg and c.page.name in msg, "the refusal does not name the page: " & msg
  var reader = pageReaderOfView(c.page.view, c.page.rootEntries)
  # Before the producer dies: the odd slot is never accepted.
  doAssert reader.readRoot(5).state == srRetry, "an odd seq was accepted"
  procs[0].terminate()
  discard procs[0].waitForExit()
  close(procs[0])
  let dead = c.page.settleDeadWriters()
  doAssert dead.len == 1, "settled " & $dead & ", expected the one killed producer"
  let r = reader.readRoot(5)
  doAssert r.state == srOk and r.size == 4096, "the settled slot does not read: " & $r
  doAssert c.page.settleDeadWriters().len == 0, "a live producer was marked dead"
  for i in 1 ..< procs.len:
    procs[i].terminate()
    discard procs[i].waitForExit()
    close(procs[i])
  doAssert c.closeCtfs().isOk
  removeFile(path)
  for f in walkFiles(path & ".*"): removeFile(f)
  echo "PASS test_live_page_writers"
