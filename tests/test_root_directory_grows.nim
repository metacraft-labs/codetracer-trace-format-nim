## A container's root directory grows when its entries run out, and every
## member reads back byte for byte afterwards.
##
## WHY.  `addFile` used to fail with "no free file entry slots" once the
## entry array the container was created with was full: 170 members for a
## one-block root at the default block size.  A recorder stores two members
## per recorded thread, so a recording of more than ~80 threads was refused
## at finalize (measured 2026-10-05, codetracer-native-recorder: "root
## directory: 170 of 170 entries used" on a 256-thread program).  The format
## already lets the entry array continue into the blocks after block 0
## (`ctfs-container.md` §1, `root_blocks`); what was missing is a writer that
## enlarges it after data blocks have been allocated behind it.
## `growRootDirectory` moves those blocks to the end and rewrites every
## pointer to them.
##
## WHAT IS ASSERTED, for a streaming and an in-memory container:
##   * 1,500 members are added (about nine doublings from 170 entries), with
##     their writes interleaved so every member's blocks are scattered through
##     the container, including the blocks each growth moves;
##   * the members cover each mapping form: empty, direct (one block), mapped
##     at level 1, and one member past level 1's capacity (a level-2 chain);
##   * the header's MaxRootEntries grew and every member reads back exactly,
##     through the canonical reader, from the closed file;
##   * `appendInternalFiles` on a sealed container whose root is full grows it
##     too, and both the old and the appended members read back;
##   * the test-only `rootGrowthLimit` seam refuses growth past it, so the
##     writers that use it to induce a full directory really get one.
##
## No mocks: real containers, read back from disk by the canonical reader.

import std/[os, strutils]
import results
import ../src/codetracer_ctfs/types
import ../src/codetracer_ctfs/container
import ../src/codetracer_ctfs/streaming
import ../src/codetracer_ctfs/container_append

proc u32le(data: openArray[byte], off: int): uint32 =
  for i in 0 ..< 4:
    result = result or (uint32(data[off + i]) shl (i * 8))

proc payload(i, round, len: int): seq[byte] =
  result = newSeq[byte](len)
  for k in 0 ..< len:
    result[k] = byte((i * 131 + round * 17 + k * 7) and 0xFF)

const
  Members = 1500
  Rounds = 3

proc sizeFor(i: int): int =
  ## Bytes per round: empty, under one block, a few blocks, and member 7 grows
  ## past level 1 (4096-byte blocks hold 511 data pointers per mapping block).
  if i mod 50 == 3: 0
  elif i == 7: 4096 * 200
  elif i mod 3 == 0: 300
  else: 4096 + 900 * (i mod 5)

proc run(streaming: bool) =
  let tag = if streaming: "streaming" else: "memory"
  let path = getTempDir() / ("test_root_grows_" & tag & "_" & $getCurrentProcessId() & ".ct")
  removeFile(path)
  var c =
    if streaming:
      let r = createCtfsStreaming(path)
      doAssert r.isOk, r.error
      r.get()
    else:
      createCtfs()
  let startEntries = c.maxRootEntries
  var files: seq[CtfsInternalFile]
  var expected: seq[seq[byte]]
  for i in 0 ..< Members:
    let name = "m" & align($i, 11, '0')
    let f = c.addFile(name)
    doAssert f.isOk, tag & ": addFile " & name & ": " & (if f.isErr: f.error else: "")
    files.add f.get()
    expected.add @[]
    # Interleave: every few members, append a round to all earlier ones.
    if i mod 97 == 0:
      for j in 0 .. i:
        let p = payload(j, expected[j].len, sizeFor(j) div Rounds)
        if p.len > 0:
          let w = c.writeToFile(files[j], p)
          doAssert w.isOk, tag & ": write " & $j & ": " & w.error
          expected[j].add p
  for j in 0 ..< Members:
    let p = payload(j, expected[j].len, sizeFor(j))
    if p.len > 0:
      let w = c.writeToFile(files[j], p)
      doAssert w.isOk, tag & ": final write " & $j & ": " & w.error
      expected[j].add p
  doAssert c.maxRootEntries > startEntries and c.maxRootEntries >= uint32(Members),
    tag & ": the root did not grow (" & $startEntries & " -> " & $c.maxRootEntries & ")"
  if streaming:
    doAssert c.closeCtfs().isOk
  else:
    doAssert c.writeCtfsToFile(path).isOk
  let data = readCtfsFromFile(path)
  doAssert data.isOk, data.error
  let d = data.get()
  doAssert u32le(d, 12) == c.maxRootEntries, tag & ": header MaxRootEntries"
  var bad = 0
  for j in 0 ..< Members:
    let name = "m" & align($j, 11, '0')
    let r = readInternalFile(d, name, u32le(d, 8), u32le(d, 12))
    if expected[j].len == 0:
      if r.isOk and r.get().len != 0: inc bad
      continue
    if r.isErr or r.get() != expected[j]:
      inc bad
      if bad <= 3:
        echo tag, ": member ", name, " differs: ",
          (if r.isErr: r.error else: $r.get().len & " bytes vs " & $expected[j].len)
  doAssert bad == 0, tag & ": " & $bad & " members did not read back"
  echo "  ", tag, ": ", Members, " members, root ", startEntries, " -> ",
    c.maxRootEntries, " entries, all read back"
  removeFile(path)

proc runAppend() =
  let path = getTempDir() / ("test_root_grows_append_" & $getCurrentProcessId() & ".ct")
  removeFile(path)
  var c = createCtfs()
  var expected: seq[(string, seq[byte])]
  for i in 0 ..< int(c.maxRootEntries):
    let name = "o" & align($i, 11, '0')
    var f = c.addFile(name).get()
    let p = payload(i, 0, sizeFor(i))
    if p.len > 0: doAssert c.writeToFile(f, p).isOk
    expected.add (name, p)
  let full = c.maxRootEntries
  doAssert c.writeCtfsToFile(path).isOk
  var names: seq[string]
  var contents: seq[seq[byte]]
  for i in 0 ..< 200:
    names.add "a" & align($i, 11, '0')
    contents.add payload(i + 7, 1, sizeFor(i + 1))
    expected.add (names[^1], contents[^1])
  let a = appendInternalFiles(path, names, contents)
  doAssert a.isOk, "append: " & (if a.isErr: a.error else: "")
  let d = readCtfsFromFile(path).get()
  doAssert u32le(d, 12) > full, "append did not grow the root"
  var bad = 0
  for (name, p) in expected:
    let r = readInternalFile(d, name, u32le(d, 8), u32le(d, 12))
    if p.len == 0: continue
    if r.isErr or r.get() != p: inc bad
  doAssert bad == 0, "append: " & $bad & " members did not read back"
  echo "  append: ", full, " -> ", u32le(d, 12), " entries, ", expected.len,
    " members read back"
  removeFile(path)

proc runLimit() =
  var c = createCtfs()
  c.rootGrowthLimit = c.maxRootEntries
  for i in 0 ..< int(c.maxRootEntries):
    doAssert c.addFile("l" & align($i, 11, '0')).isOk
  let extra = c.addFile("one.more")
  doAssert extra.isErr and "rootGrowthLimit" in extra.error,
    "rootGrowthLimit did not stop the growth"
  echo "  limit: growth past rootGrowthLimit refused"

run(streaming = true)
run(streaming = false)
runAppend()
runLimit()
echo "PASS: test_root_directory_grows"
