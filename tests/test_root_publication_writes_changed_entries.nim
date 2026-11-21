## A publication writes what changed (`ctfs-container.md` §6, "Root
## Publication"): the entries that differ from what the file holds, each in
## one 24-byte write, through the live coordination page first, and never the
## header after the container's creation. A publication that changes nothing
## is not a seal (§6, "What counts as a seal"), and neither is the creation
## write.
##
## Null prediction: the base's `publish` wrote the whole root region from
## offset 0 at every call (`container.nim:526-529` at `d1d8e98`), header
## included, changed or not. The mutation arm `publish-whole-root` restores
## that write, and this test must fail on it (exit 3 from the arm's child).

import std/[os, osproc, strutils]
import results
import codetracer_ctfs/[types, container, streaming, live_page, mutation_arms]

type W = tuple[off: int64, len: int]

proc check(): string =
  ## "" or the first failure.
  let path = getTempDir() / ("ckf4n_rootpub_" & $getCurrentProcessId() & ".ct")
  removeFile(path)
  var c = createCtfsStreaming(path).get()
  var writes: seq[W]
  c.writeTap = proc (offset: int64, bytes: openArray[byte]) {.closure, gcsafe,
      raises: [].} =
    writes.add (off: offset, len: bytes.len)
  if c.page == nil: return "a streaming container hosts no live coordination page"
  if c.page.writerSealEpoch() != 0 or c.sealOrdinal != 0:
    return "the creation write counted as a seal"
  var a = c.addFile("a.dat").get()
  var b = c.addFile("b.dat").get()
  discard c.addFile("c.dat").get()
  var payload = newSeq[byte](5000)
  for i in 0 ..< payload.len: payload[i] = byte(i and 0xff)
  doAssert c.writeToFile(a, payload).isOk
  doAssert c.writeToFile(b, payload[0 ..< 100]).isOk
  c.publish()
  let rootEnd = int64(c.rootRegionBytes())
  var entryWrites: seq[int64]
  for w in writes:
    if w.off < rootEnd:
      if w.off < 16:
        return "a publication wrote the header (" & $w.len & " bytes at " & $w.off & ")"
      if w.len != FileEntrySize or (w.off - 16) mod FileEntrySize != 0:
        return "a root write of " & $w.len & " bytes at " & $w.off & " is not one entry"
      entryWrites.add w.off
  let want = @[int64(c.fileEntryOffset(0)), int64(c.fileEntryOffset(1)),
               int64(c.fileEntryOffset(2))]
  if entryWrites != want:
    return "the first seal wrote entries at " & $entryWrites & ", not " & $want
  if c.page.writerSealEpoch() != 1 or c.sealOrdinal != 1:
    return "the first seal's epoch is " & $c.page.writerSealEpoch()
  # Nothing changed: no write, no seal.
  writes.setLen(0)
  c.publish()
  if writes.len != 0:
    return "a publication that changed nothing wrote " & $writes.len & " times"
  if c.page.writerSealEpoch() != 1:
    return "a publication that changed nothing incremented seal_epoch"
  # One member grows: its data, then its one entry.
  doAssert c.writeToFile(b, payload[0 ..< 50]).isOk
  c.publish()
  var roots: seq[W]
  var dataBefore = true
  for w in writes:
    if w.off < rootEnd: roots.add w
    elif roots.len > 0: dataBefore = false
  if roots != @[(off: int64(c.fileEntryOffset(1)), len: FileEntrySize)]:
    return "growing b.dat wrote the root as " & $roots
  if not dataBefore:
    return "a data write followed the entry that publishes it"
  # The page's root mirror and the file both hold every entry.
  let onDisk = cast[seq[byte]](readFile(path))
  for i in 0 ..< 3:
    let off = c.fileEntryOffset(i)
    let r = pageReaderOfView(c.page.view, c.page.rootEntries).readRoot(i)
    if r.state != srOk or r.size != readU64LE(c.data, off) or
        r.mapBlock != readU64LE(c.data, off + 8) or r.name != readU64LE(c.data, off + 16):
      return "the page's root mirror of entry " & $i & " differs from the entry"
    for k in 0 ..< FileEntrySize:
      if onDisk[off + k] != c.data[off + k]:
        return "the file's entry " & $i & " differs from the writer's"
  if c.closeCtfs().isErr: return "close failed"
  removeFile(path)
  ""

when isMainModule:
  let args = commandLineParams()
  if args.len > 0 and args[0] == "--arm":
    let msg = check()
    let fired = keyedMutationFired
    echo "  arm ", args[1], ": fired ", fired, "; ", (if msg.len > 0: "caught: " & msg else: "NOT caught")
    quit(if fired == 0: 5 elif msg.len > 0: 3 else: 4)
  when not defined(ctfsKeyedMutation):
    {.error: "build this test with -d:ctfsKeyedMutation: its mutation arm is part of it".}
  let msg = check()
  if msg.len > 0:
    echo "FAIL: ", msg
    quit(1)
  putEnv("CTFS_KEYED_MUTATE", "publish-whole-root")
  let p = startProcess(getAppFilename(), args = ["--arm", "publish-whole-root"],
                       options = {poParentStreams})
  let rc = p.waitForExit()
  close(p)
  if rc != 3:
    echo "FAIL: the mutation arm publish-whole-root exited ", rc, ", not 3 (caught)"
    quit(1)
  echo "PASS test_root_publication_writes_changed_entries"
