## Closed-container appends (`ctfs-container.md` §5, "Adding Members To A
## Closed Container"):
##
## 1. A batch the directory cannot hold whole is refused before anything is
##    written, naming the first member that does not fit and the occupancy;
##    the file is byte-identical afterwards (the root does not grow).
## 2. An append writes the tail, then the changed entries in one write, and
##    never the header or anything in `[root_blocks, previous end of file)`.
## 3. The fault seam covers the entry write: a failed entry write is
##    reported, and the container keeps its old entries and stays readable.
##
## Null prediction: on the base, the batch of (1) grew the root directory
## (the appender rewrote everything from the new root to the old end of
## file) and succeeded; (2)'s writes included the whole root region, header
## included. Built with `-d:ctfsAppendFaultInjection`.

import std/[os, strutils]
import results
import codetracer_ctfs/[types, container, container_append]

proc build(path: string, members: int) =
  var c = createCtfs(maxRootEntries = 8)
  for i in 0 ..< members:
    var f = c.addFile("m" & $i).get()
    doAssert c.writeToFile(f, newSeq[byte](5000 + i)).isOk
  doAssert writeCtfsToFile(c, path).isOk

when isMainModule:
  when not defined(ctfsAppendFaultInjection):
    {.error: "build with -d:ctfsAppendFaultInjection".}
  let dir = getTempDir() / ("ckf4n_append_" & $getCurrentProcessId())
  createDir(dir)
  let p = dir / "c.ct"
  build(p, 6)
  # 1. Three members, two free slots: refused, nothing written.
  let before = readFile(p)
  let r = appendInternalFiles(p, ["x1", "x2", "x3"], [@[1'u8], @[2'u8], @[3'u8]])
  doAssert r.isErr, "a batch the directory cannot hold whole was appended"
  doAssert "x3" in r.error and "6 of 8" in r.error, "the refusal does not name the member and the occupancy: " & r.error
  doAssert readFile(p) == before, "a refused batch changed the file"
  # 2. Two members fit: the header and the old body are untouched.
  doAssert appendInternalFiles(p, ["x1", "x2"], [@[1'u8, 2, 3], newSeq[byte](9000)]).isOk
  let after = readFile(p)
  doAssert after[0 ..< 16] == before[0 ..< 16], "the append rewrote the header"
  doAssert after[4096 ..< before.len] == before[4096 ..< before.len],
    "the append rewrote a block of the previous body"
  # Only entries 6 and 7 changed in the root region.
  for k in 16 ..< 4096:
    let entry = (k - 16) div 24
    if entry != 6 and entry != 7:
      doAssert after[k] == before[k], "the append changed root byte " & $k & " (entry " & $entry & ")"
  let data = cast[seq[byte]](after)
  doAssert readInternalFile(data, "x2", maxEntries = 8).get().len == 9000
  # 3. A failed entry write: reported, old entries kept, still readable.
  build(p, 6)
  let b2 = readFile(p)
  ctfsAppendFailEntryWrite = true
  let f = appendInternalFiles(p, ["y1"], [@[9'u8, 9]])
  ctfsAppendFailEntryWrite = false
  doAssert f.isErr and "entry write" in f.error
  let a2 = readFile(p)
  doAssert a2[0 ..< 4096] == b2[0 ..< 4096], "a failed entry write changed the root region"
  let d2 = cast[seq[byte]](a2)
  doAssert readInternalFile(d2, "m5", maxEntries = 8).get().len == 5005
  doAssert not hasInternalFile(d2, "y1", 8)
  removeDir(dir)
  echo "PASS test_container_append_batch_refusal"
