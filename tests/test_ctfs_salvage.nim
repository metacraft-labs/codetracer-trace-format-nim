## `ctfs-salvage` (`ctfs-container.md` §6, "Every writer dies"): a writer
## that published a member's growth in its live coordination page, with the
## bytes in the file, and died before writing the entry, leaves a file one
## seal behind the page. Salvage copies the page's entry into the file; a page
## value whose bytes are not all in the file is not copied; a page with a
## live writer is refused.
##
## Null prediction: without salvage the file reads the member at its last
## sealed size (asserted first, so the advance is salvage's).

import std/[os, osproc, strutils]
import results
import codetracer_ctfs/[types, container, streaming, block_mapping, live_page,
                        live_reader, salvage]

proc childMain(path: string) =
  var c = createCtfsStreaming(path).get()
  var a = c.addFile("a.dat").get()
  var b = c.addFile("b.dat").get()
  doAssert c.writeToFile(a, newSeq[byte](1000)).isOk
  doAssert c.writeToFile(b, newSeq[byte](1000)).isOk
  c.publish()
  # a.dat grows: its bytes reach the file and the page, never the file entry.
  doAssert c.writeToFile(a, newSeq[byte](9000)).isOk
  c.flushDirtyData()
  let ea = c.fileEntryOffset(a.entryIndex)
  c.page.publishRoot(a.entryIndex, readU64LE(c.data, ea), readU64LE(c.data, ea + 8),
    readU64LE(c.data, ea + 16))
  # b.dat's page value claims bytes the file does not hold.
  let eb = c.fileEntryOffset(b.entryIndex)
  c.page.publishRoot(b.entryIndex, 50_000, readU64LE(c.data, eb + 8) and not CtfsDirect or 7,
    readU64LE(c.data, eb + 16))
  writeFile(path & ".ready", "1")
  while true: sleep(1000)

when isMainModule:
  let args = commandLineParams()
  if args.len == 2 and args[0] == "--child": childMain(args[1])
  let path = getTempDir() / ("ckf4n_salvage_" & $getCurrentProcessId() & ".ct")
  removeFile(path); removeFile(path & ".ready")
  var p = startProcess(getAppFilename(), args = ["--child", path], options = {poParentStreams})
  for _ in 0 ..< 500:
    if fileExists(path & ".ready"): break
    sleep(20)
  doAssert fileExists(path & ".ready")
  let live = salvageContainer(path)
  doAssert live.isErr and "live writer" in live.error, "salvage ran with a live writer"
  # Hold the page so it outlives the writer on Windows too.
  var holder = openLiveReader(path).get()
  doAssert holder.attached
  p.terminate()
  discard p.waitForExit()
  close(p)
  var before = openLiveReader(path, attach = false).get()
  doAssert before.memberSize(before.findSlot("a.dat")) == 1000, "the file was not one seal behind"
  before.close()
  let r = salvageContainer(path)
  doAssert r.isOk, r.error
  doAssert r.get().copied == @["a.dat"], "advanced " & $r.get().copied
  doAssert r.get().skipped == @["b.dat"], "skipped " & $r.get().skipped
  holder.close()
  var after = openLiveReader(path, attach = false).get()
  doAssert after.memberSize(after.findSlot("a.dat")) == 10000
  doAssert after.readMember(after.findSlot("a.dat"), 0, 10000).isOk
  doAssert after.memberSize(after.findSlot("b.dat")) == 1000
  after.close()
  removeFile(path); removeFile(path & ".ready")
  echo "PASS test_ctfs_salvage"
