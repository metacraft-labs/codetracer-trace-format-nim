## Readers refuse a block number in `[1, root_blocks)` (`ctfs-container.md`
## §6, Reader Protocol step 3; §4): in a container whose root region is
## several blocks, a member whose direct block, mapping root, chain pointer,
## child pointer or data block names a block of the root region is refused by
## `readInternalFile` (and so every reader built on it) and by the live
## reader, naming the member and the root region. Block 0 and a block past
## the end were already refused; the root region past block 0 was read as
## the member's bytes.
##
## Null prediction: on the base, `memberRuns` checked `0` and the whole-block
## bound only, so each damaged container below read back the root region's
## bytes as the member and returned success. The control: the undamaged
## container reads.

import std/[os, strutils]
import results
import codetracer_ctfs/[types, container, live_reader]

const Entries = 400'u32   # 16 + 400 * 24 bytes: a 3-block root region

proc build(size: int): (seq[byte], int) =
  var c = createCtfs(maxRootEntries = Entries)
  var f = c.addFile("x.dat").get()
  var b = newSeq[byte](size)
  for i in 0 ..< size: b[i] = byte(i and 0xff)
  doAssert c.writeToFile(f, b).isOk
  (c.data, c.fileEntryOffset(f.entryIndex))

var failures = 0
proc expectRefused(img: seq[byte], what: string) =
  let r = readInternalFile(img, "x.dat", maxEntries = Entries)
  if r.isOk:
    echo "FAIL: ", what, ": readInternalFile returned ", r.get().len, " bytes"
    inc failures
  elif "root region" notin r.error or "x.dat" notin r.error:
    echo "FAIL: ", what, ": the refusal does not name the member and the root region: ", r.error
    inc failures
  var image: ref seq[byte]
  new(image)
  image[] = img
  var rd = openImageReader(image).get()
  let slot = rd.findSlot("x.dat")
  let l = rd.readMember(slot, 0, int(rd.memberSize(slot)))
  if l.isOk or "root region" notin l.error:
    echo "FAIL: ", what, ": the live reader did not refuse it: ",
      (if l.isErr: l.error else: "read")
    inc failures

let rootBlocks = int(rootBlockCount(4096, Entries, 0))
doAssert rootBlocks == 3

block control:
  let (img, _) = build(10000)
  doAssert readInternalFile(img, "x.dat", maxEntries = Entries).isOk

block direct:
  var (img, e) = build(100)
  writeU64LE(img, e + 8, CtfsDirect or 2'u64)
  expectRefused(img, "a direct block in the root region")

block mappingRoot:
  var (img, e) = build(10000)
  writeU64LE(img, e + 8, 1'u64)
  expectRefused(img, "a mapping root in the root region")

block dataBlock:
  var (img, e) = build(10000)
  let m = int(readU64LE(img, e + 8))
  writeU64LE(img, m * 4096 + 8, 2'u64)
  expectRefused(img, "a data block in the root region")

block chainAndChild:
  # 512 blocks: past level 1's 511, so a chain pointer and a level-2 child.
  var (img, e) = build(512 * 4096 + 10)
  let m = int(readU64LE(img, e + 8))
  let chain = int(readU64LE(img, m * 4096 + 511 * 8))
  var img2 = img
  writeU64LE(img2, m * 4096 + 511 * 8, 1'u64)
  expectRefused(img2, "a chain pointer in the root region")
  writeU64LE(img, chain * 4096, 2'u64)
  expectRefused(img, "a child pointer in the root region")

if failures > 0: quit(1)
echo "PASS test_root_region_pointer_refused"
