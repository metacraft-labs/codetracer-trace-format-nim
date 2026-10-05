## A root directory larger than block 0 — `ctfs-container.md` §1's overflow.
##
## §1: when `MaxRootEntries * 24 + 16 + R > BlockSize` the file-entry array
## continues into the blocks after block 0,
##
##     root_blocks = ceil((16 + R + MaxRootEntries * 24) / BlockSize)
##
## and data block allocation begins at block `root_blocks`.  Until this test's
## change `createCtfs` allocated block 0 alone and started data at block 1, so a
## declared count past block 0 put entries on top of the first data block.
## That is why every writer in the workspace stopped at 170 entries (4096-byte
## blocks, R = 0), and why an MCR recording with more than ~50 periodic
## checkpoints (three members each) failed.
##
## Asserted here, writer and reader together:
##   * the spec's `root_blocks` arithmetic, at and around each boundary,
##     including the free-list roots `R`;
##   * a container declaring more entries than block 0 holds reserves
##     `root_blocks` blocks, allocates no data or mapping block inside them, and
##     stores and reads back EVERY member — including the ones whose entries
##     lie in block 1 and block 2, and one whose entry straddles the
##     block-1/block-2 boundary — through the canonical reader;
##   * (until 2026-10-05 also "the declared count is still a limit: one more
##     member is refused"; the directory now grows instead, asserted by
##     tests/test_root_directory_grows.nim);
##   * streaming mode publishes the overflow blocks too: a reader of the file
##     on disk sees an entry in block 2 before `closeCtfs`;
##   * a container that fits block 0 is byte-for-byte what it was (one root
##     block, data from block 1) — the change is invisible below the limit.
##
## No mocks: real containers, in memory and on a real filesystem, read back
## with `readInternalFile` / `hasInternalFile`.

{.push raises: [].}

import std/os
import results
import codetracer_ctfs

proc u32le(data: openArray[byte], off: int): uint32 =
  for i in 0 ..< 4:
    result = result or (uint32(data[off + i]) shl (i * 8))

proc u64le(data: openArray[byte], off: int): uint64 =
  for i in 0 ..< 8:
    result = result or (uint64(data[off + i]) shl (i * 8))

proc memberName(i: int): string =
  ## Distinct, valid base40, at most 12 characters.
  "m" & $i & ".bin"

proc memberContent(i: int): seq[byte] =
  ## Every third member spans more than one data block, so mapping blocks are
  ## allocated as well as data blocks.
  let n = if i mod 3 == 0: 4096 + 100 + i else: 1 + (i mod 50)
  result = newSeq[byte](n)
  for k in 0 ..< n:
    result[k] = byte((i * 31 + k) and 0xFF)

proc test_root_blocks_arithmetic() =
  ## `ctfs-container.md` §1, evaluated at the boundaries.
  doAssert rootBlockCount(4096, 170, 0) == 1, "170 entries fit block 0 (16 + 4080)"
  doAssert rootBlockCount(4096, 171, 0) == 2, "the 171st entry is in block 1"
  doAssert rootBlockCount(4096, 0, 0) == 1, "auto-fill (0) is block 0 alone"
  doAssert rootBlockCount(4096, 340, 0) == 2, "16 + 340*24 = 8176 <= 8192"
  doAssert rootBlockCount(4096, 341, 0) == 3, "16 + 341*24 = 8200 > 8192"
  # R = 7 * 16 * 6 = 672: 16 + 672 + 142*24 = 4096 exactly.
  doAssert rootBlockCount(4096, 142, 16) == 1
  doAssert rootBlockCount(4096, 143, 16) == 2
  doAssert rootBlockCount(0, 10, 0) == 0, "a zero block size has no root region"
  echo "PASS: test_root_blocks_arithmetic"

proc test_small_container_unchanged() =
  ## Below the limit nothing moves: one root block, data from block 1.
  var c = createCtfs(maxRootEntries = 170)
  doAssert c.rootBlockCount() == 1
  doAssert c.nextFreeBlock == 1, "data must still start at block 1"
  doAssert c.data.len == 4096
  var f = c.addFile("a.bin").get()
  doAssert c.writeToFile(f, [1'u8, 2, 3]).isOk
  let data = c.toBytes()
  doAssert u64le(data, 16 + 8) == (CtfsDirect or 1'u64),
    "the first member's only data block is block 1, stored direct"
  echo "PASS: test_small_container_unchanged"

const Declared = 400  # root_blocks = ceil((16 + 9600) / 4096) = 3

proc fillAndCheck(c: var Ctfs, label: string) =
  for i in 0 ..< Declared:
    let h = c.addFile(memberName(i))
    doAssert h.isOk, label & ": addFile #" & $i & " failed: " & h.error
    var f = h.get()
    doAssert c.writeToFile(f, memberContent(i)).isOk, label & ": write #" & $i
  # A member past the declared count no longer fails: the root directory
  # grows (`growRootDirectory`, 2026-10-05), which tests/
  # test_root_directory_grows.nim asserts.  This test pins the declared layout,
  # so it stops at the declared count.

proc checkContainer(data: seq[byte], label: string) =
  let blockSize = u32le(data, 8)
  let maxEntries = u32le(data, 12)
  doAssert blockSize == 4096 and maxEntries == uint32(Declared),
    label & ": header says " & $blockSize & " / " & $maxEntries
  let rootBlocks = rootBlockCount(blockSize, maxEntries, data[7])
  doAssert rootBlocks == 3, label & ": root_blocks " & $rootBlocks
  # Every entry is where §1 puts it, and no member's mapping block lies inside
  # the root region — the defect this overflow used to have.
  var inBlock1, inBlock2, straddles = 0
  for i in 0 ..< Declared:
    let off = 16 + i * 24
    let mapBlock = u64le(data, off + 8) and not CtfsDirect
    doAssert mapBlock >= rootBlocks,
      label & ": entry " & $i & "'s mapping block " & $mapBlock &
      " lies inside the " & $rootBlocks & "-block root region"
    if off div 4096 == 1: inc inBlock1
    if off div 4096 == 2: inc inBlock2
    if off div 4096 != (off + 23) div 4096: inc straddles
    let got = readInternalFile(data, memberName(i), blockSize, maxEntries)
    doAssert got.isOk, label & ": member " & $i & " (" & memberName(i) &
      ", entry at byte " & $off & ") did not read back: " & got.error
    doAssert got.get() == memberContent(i),
      label & ": member " & $i & " read back different bytes"
    doAssert hasInternalFile(data, memberName(i), maxEntries),
      label & ": hasInternalFile does not see member " & $i
  # The control: the checks above ran over entries PAST block 0, or they
  # would prove nothing about the overflow.
  doAssert inBlock1 > 0 and inBlock2 > 0 and straddles > 0,
    label & ": the fixture exercised block 1 (" & $inBlock1 & "), block 2 (" &
    $inBlock2 & ") and a straddling entry (" & $straddles & ")"

proc test_overflow_in_memory() =
  var c = createCtfs(maxRootEntries = uint32(Declared))
  doAssert c.rootBlockCount() == 3
  doAssert c.nextFreeBlock == 3, "data must start at block root_blocks (3), not " &
    $c.nextFreeBlock
  c.fillAndCheck("in-memory")
  checkContainer(c.toBytes(), "in-memory")
  echo "PASS: test_overflow_in_memory"

proc test_overflow_streaming() =
  let path = getTempDir() / "test_root_directory_overflow.ct"
  try: removeFile(path) except OSError: discard
  var c = createCtfsStreaming(path, maxRootEntries = uint32(Declared)).get()
  c.fillAndCheck("streaming")
  # Before close: the entries in blocks 1 and 2 must already be on disk.
  c.syncAllEntries()
  let live = readCtfsFromFile(path)
  doAssert live.isOk, live.error
  let last = readInternalFile(live.get(), memberName(Declared - 1), 4096,
                              uint32(Declared))
  doAssert last.isOk and last.get() == memberContent(Declared - 1),
    "a live reader cannot see the last member (its entry is in block 2) " &
    "before closeCtfs: the streaming writer published block 0 only"
  doAssert c.closeCtfs().isOk
  let data = readCtfsFromFile(path)
  doAssert data.isOk, data.error
  checkContainer(data.get(), "streaming")
  try: removeFile(path) except OSError: discard
  echo "PASS: test_overflow_streaming"

when isMainModule:
  test_root_blocks_arithmetic()
  test_small_container_unchanged()
  test_overflow_in_memory()
  test_overflow_streaming()
