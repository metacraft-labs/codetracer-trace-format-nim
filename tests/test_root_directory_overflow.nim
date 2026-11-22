## A root directory larger than block 0 — `ctfs-container.md` §1's overflow.
##
## §1: when `MaxRootEntries * 24 + 16 > BlockSize` the file-entry array
## continues into the blocks after block 0,
##
##     root_blocks = ceil((16 + MaxRootEntries * 24) / BlockSize)
##
## and data block allocation begins at block `root_blocks`.  Until this test's
## change `createCtfs` allocated block 0 alone and started data at block 1, so a
## declared count past block 0 put entries on top of the first data block.
## That is why every writer in the workspace stopped at 170 entries (4096-byte
## blocks), and why an MCR recording with more than ~50 periodic
## checkpoints (three members each) failed.
##
## Until 2026-10-08 the formula also carried a free-list root area
## `R = 7 * max_shards * 6` between the header and the entries; §1, "The free
## list root area is removed (2026-10-08)", makes `R = 0` for every shard
## count, so a sharded container is laid out exactly as an unsharded one.
##
## Asserted here, writer and reader together:
##   * the spec's `root_blocks` arithmetic, at and around each boundary,
##     for unsharded and sharded containers alike;
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
##     block, data from block 1) — the change is invisible below the limit;
##   * a sharded container (max_shards 1, 16, 255), in memory and streaming,
##     reserves the unsharded `root_blocks`, puts its first entry at byte 16,
##     reads every member back, and is byte-for-byte the unsharded container
##     except header byte 7;
##   * the negative control: an entry moved to the removed offset `16 + R` is
##     not found by the canonical reader;
##   * the entries start at the end of the header the image declares (§1a: 24
##     bytes at version 6), for `root_blocks`, an entry's offset and a grown
##     root alike.
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
  # Until 2026-10-08 this asserted `rootBlockCount(4096, 142, 16) == 1` and
  # `(4096, 143, 16) == 2`: a free-list root area of R = 7 * 16 * 6 = 672 bytes
  # before the entries.  `ctfs-container.md` §1, "The free list root area is
  # removed (2026-10-08)", makes R = 0 for every MaxShards, so the shard count
  # no longer moves a boundary.  Each boundary above is asserted again for
  # nonzero shard counts, the largest included.
  for shards in [1'u8, 4, 16, 255]:
    let s = " (max_shards " & $shards & ")"
    doAssert rootBlockCount(4096, 170, shards) == 1,
      "170 entries fit block 0 (16 + 4080) for any shard count" & s &
      ", got " & $rootBlockCount(4096, 170, shards)
    doAssert rootBlockCount(4096, 171, shards) == 2,
      "the 171st entry is in block 1" & s &
      ", got " & $rootBlockCount(4096, 171, shards)
    doAssert rootBlockCount(4096, 0, shards) == 1,
      "auto-fill (0) is block 0 alone" & s
    doAssert rootBlockCount(4096, 340, shards) == 2,
      "16 + 340*24 = 8176 <= 8192" & s &
      ", got " & $rootBlockCount(4096, 340, shards)
    doAssert rootBlockCount(4096, 341, shards) == 3,
      "16 + 341*24 = 8200 > 8192" & s &
      ", got " & $rootBlockCount(4096, 341, shards)
    doAssert rootBlockCount(4096, 400, shards) == 3,
      "16 + 400*24 = 9616 <= 12288" & s &
      ", got " & $rootBlockCount(4096, 400, shards)
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

proc fillAndCheck(c: var Ctfs, label: string, declared = Declared) =
  for i in 0 ..< declared:
    let h = c.addFile(memberName(i))
    doAssert h.isOk, label & ": addFile #" & $i & " failed: " & h.error
    var f = h.get()
    doAssert c.writeToFile(f, memberContent(i)).isOk, label & ": write #" & $i
  # A member past the declared count no longer fails: the root directory
  # grows (`growRootDirectory`, 2026-10-05), which tests/
  # test_root_directory_grows.nim asserts.  This test pins the declared layout,
  # so it stops at the declared count.

proc checkContainer(data: seq[byte], label: string, declared = Declared,
                    expectRoot = 3'u64) =
  let blockSize = u32le(data, 8)
  let maxEntries = u32le(data, 12)
  doAssert blockSize == 4096 and maxEntries == uint32(declared),
    label & ": header says " & $blockSize & " / " & $maxEntries
  let rootBlocks = rootBlockCount(blockSize, maxEntries, data[7])
  doAssert rootBlocks == expectRoot, label & ": root_blocks " & $rootBlocks &
    ", expected " & $expectRoot
  # Every entry is where §1 puts it, and no member's mapping block lies inside
  # the root region — the defect this overflow used to have.
  var inBlock1, inBlock2, straddles = 0
  for i in 0 ..< declared:
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
  # A three-block root must have reached block 2 (and straddled 1/2); a
  # two-block root must have reached block 1.  A one-block root is the
  # below-the-limit layout and has no overflow to exercise.
  if expectRoot >= 3'u64:
    doAssert inBlock1 > 0 and inBlock2 > 0 and straddles > 0,
      label & ": the fixture exercised block 1 (" & $inBlock1 & "), block 2 (" &
      $inBlock2 & ") and a straddling entry (" & $straddles & ")"
  elif expectRoot == 2'u64:
    doAssert inBlock1 > 0,
      label & ": the fixture exercised block 1 (" & $inBlock1 & ")"

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

# ---------------------------------------------------------------------------
# Sharded containers: `ctfs-container.md` §1, "The free list root area is
# removed (2026-10-08)".
#
# Until 2026-10-08 the root region of a container with `max_shards != 0`
# reserved `R = 7 * max_shards * 6` bytes between the header and the entries.
# No writer ever put an entry anywhere but `16 + 24 * i`, so the reservation
# only inflated `root_blocks` (a 170-entry sharded root took two blocks) and
# the readers that skipped `R` read entries that are not there.  R is now 0
# for every shard count: a sharded container is laid out exactly as an
# unsharded one, and only header byte 7 differs.
# ---------------------------------------------------------------------------

proc specRootBlocks(declared: int): uint64 =
  ## §1's `root_blocks = ceil((16 + MaxRootEntries * 24) / BlockSize)` at
  ## 4096-byte blocks, computed here rather than through `rootBlockCount` so a
  ## defect there cannot also be the expectation.
  uint64(max(1, (16 + declared * 24 + 4095) div 4096))

proc checkFreshRoot(c: Ctfs, label: string, declared: int) =
  let want = specRootBlocks(declared)
  doAssert c.rootBlockCount() == want,
    label & ": rootBlockCount " & $c.rootBlockCount() & ", expected " & $want
  doAssert c.nextFreeBlock == want,
    label & ": data must start at block root_blocks (" & $want & "), not " &
    $c.nextFreeBlock

proc buildContainer(shards: uint8, declared: int, streaming: bool,
                    label: string): seq[byte] =
  ## The container `fillAndCheck` writes, declaring `declared` entries and
  ## `shards` shards, as the bytes a reader sees once it is closed.
  if streaming:
    let path = getTempDir() / ("test_root_directory_overflow_s" & $shards &
                               "_" & $declared & ".ct")
    try: removeFile(path) except OSError: discard
    let created = createCtfsStreaming(path, maxRootEntries = uint32(declared),
                                      maxShards = shards)
    doAssert created.isOk, label & ": " & created.error
    var c = created.get()
    checkFreshRoot(c, label, declared)
    c.fillAndCheck(label, declared)
    doAssert c.closeCtfs().isOk, label & ": closeCtfs"
    let data = readCtfsFromFile(path)
    doAssert data.isOk, label & ": " & data.error
    try: removeFile(path) except OSError: discard
    result = data.get()
  else:
    var c = createCtfs(maxRootEntries = uint32(declared), maxShards = shards)
    checkFreshRoot(c, label, declared)
    c.fillAndCheck(label, declared)
    result = c.toBytes()

proc test_sharded_small_container() =
  ## A sharded container that fits block 0 is the unsharded layout: one root
  ## block, data from block 1, the first entry at byte 16.
  for streaming in [false, true]:
    for shards in [1'u8, 16, 255]:
      let label = (if streaming: "streaming" else: "in-memory") &
        " max_shards " & $shards
      let path = getTempDir() / ("test_root_directory_overflow_small_" &
                                 $shards & ".ct")
      var c: Ctfs
      if streaming:
        try: removeFile(path) except OSError: discard
        let created = createCtfsStreaming(path, maxRootEntries = 170,
                                          maxShards = shards)
        doAssert created.isOk, label & ": " & created.error
        c = created.get()
      else:
        c = createCtfs(maxRootEntries = 170, maxShards = shards)
      doAssert c.rootBlockCount() == 1,
        label & ": 16 + 170*24 = 4096 is one root block, got " &
        $c.rootBlockCount()
      doAssert c.nextFreeBlock == 1,
        label & ": data must start at block 1, not " & $c.nextFreeBlock
      if not streaming:
        doAssert c.data.len == 4096,
          label & ": the fresh root region is " & $c.data.len & " bytes, not one block"
      var f = c.addFile("a.bin").get()
      doAssert c.writeToFile(f, [1'u8, 2, 3]).isOk
      var data: seq[byte]
      if streaming:
        doAssert c.closeCtfs().isOk
        data = readCtfsFromFile(path).get()
        try: removeFile(path) except OSError: discard
      else:
        data = c.toBytes()
      doAssert data[7] == shards, label & ": header byte 7"
      doAssert data.len == 2 * 4096,
        label & ": block 0 and one data block, got " & $data.len & " bytes"
      # The bytes the removed rule reserved (16 ..< 16 + 7*shards*6) hold the
      # first entry: its size, its MapBlock and its name.
      doAssert u64le(data, 16) == 3'u64, label & ": entry 0's size at byte 16"
      doAssert u64le(data, 16 + 8) == (CtfsDirect or 1'u64),
        label & ": entry 0's only data block is block 1, stored direct"
      doAssert u64le(data, 16 + 16) == base40Encode("a.bin"),
        label & ": entry 0's name at byte 32"
      let got = readInternalFile(data, "a.bin", 4096, 170)
      doAssert got.isOk and got.get() == @[1'u8, 2, 3],
        label & ": a.bin did not read back"
  echo "PASS: test_sharded_small_container"

proc test_sharded_layout_is_the_unsharded_layout() =
  ## For each declared count around the root-block boundaries, in memory and
  ## streaming: the sharded container reserves `root_blocks` by §1's formula,
  ## every member reads back, and its bytes are the unsharded container's
  ## bytes except header byte 7 (max_shards).
  for streaming in [false, true]:
    let mode = if streaming: "streaming" else: "in-memory"
    for declared in [170, 340, 400]:
      let plain = buildContainer(0, declared, streaming,
                                 mode & " unsharded declared " & $declared)
      checkContainer(plain, mode & " unsharded", declared,
                     specRootBlocks(declared))
      for shards in [1'u8, 16, 255]:
        let label = mode & " max_shards " & $shards & " declared " & $declared
        let data = buildContainer(shards, declared, streaming, label)
        doAssert data[7] == shards, label & ": header byte 7 is " & $data[7]
        checkContainer(data, label, declared, specRootBlocks(declared))
        doAssert data.len == plain.len,
          label & ": " & $data.len & " bytes, the unsharded container has " &
          $plain.len
        for i in 0 ..< data.len:
          if i != 7:
            doAssert data[i] == plain[i],
              label & ": byte " & $i & " is " & $data[i] &
              ", the unsharded container's is " & $plain[i]
  echo "PASS: test_sharded_layout_is_the_unsharded_layout"

proc test_old_offset_is_not_read() =
  ## The negative control: an entry placed where the removed rule put it
  ## (`16 + 7 * max_shards * 6`) is not a member.  The canonical reader takes
  ## entry `i` at `16 + 24 * i` and skips nothing (§1: "a reader MUST NOT skip
  ## any"), so it must not find the moved entry.
  var c = createCtfs(maxRootEntries = 170, maxShards = 1)
  var f = c.addFile("a.bin").get()
  doAssert c.writeToFile(f, [1'u8, 2, 3]).isOk
  let good = c.toBytes()
  doAssert readInternalFile(good, "a.bin", 4096, 170).isOk,
    "control: the container as written reads back"
  var moved = good
  const R = 7 * 1 * 6
  for k in 0 ..< 24:
    moved[16 + R + k] = good[16 + k]
  for k in 0 ..< R:
    moved[16 + k] = 0
  doAssert u64le(moved, 16 + R + 16) == base40Encode("a.bin"),
    "the fixture moved the entry to byte " & $(16 + R)
  let got = readInternalFile(moved, "a.bin", 4096, 170)
  doAssert got.isErr,
    "an entry at the removed offset 16 + R = " & $(16 + R) & " was read as a member"
  doAssert not hasInternalFile(moved, "a.bin", 170),
    "hasInternalFile sees an entry at the removed offset " & $(16 + R)
  echo "PASS: test_old_offset_is_not_read"

proc test_header_size_follows_the_version() =
  ## §1 and §1a: the entry array starts at the end of the header, which is 16
  ## bytes through version 5 and 24 at version 6, so `root_blocks` and an
  ## entry's offset are computed from the header the image declares, not from
  ## a constant.
  doAssert containerHeaderSize(createCtfs().data) == 16
  var v6 = newSeq[byte](4096)
  v6[5] = CtfsVersionV6
  doAssert containerHeaderSize(v6) == 24
  doAssert containerHeaderSize(newSeq[byte](5)) == 16,
    "an image too short for byte 5 reads as the 16-byte layout"
  # 4096-byte blocks: version 5 fits 170 entries in block 0 (16 + 4080),
  # version 6 fits 169 (24 + 4056 = 4080; 24 + 4080 = 4104 > 4096).
  doAssert rootBlockCount(4096, 170, 0, 16) == 1
  doAssert rootBlockCount(4096, 169, 0, V6HeaderSize) == 1
  doAssert rootBlockCount(4096, 170, 0, V6HeaderSize) == 2,
    "version 6: the 170th entry is in block 1"
  doAssert rootBlockCount(4096, 340, 0, V6HeaderSize) == 2,
    "version 6: 24 + 340*24 = 8184 <= 8192"
  doAssert rootBlockCount(4096, 341, 0, V6HeaderSize) == 3,
    "version 6: 24 + 341*24 = 8208 > 8192"
  # The default is the version-5 header `createCtfs` writes.
  doAssert rootBlockCount(4096, 170, 0) == rootBlockCount(4096, 170, 0, 16)
  # A `Ctfs` sizes its root and finds its entries by its own header.
  var c = createCtfs(maxRootEntries = 170)
  doAssert c.fileEntryOffset(0) == 16 and c.fileEntryOffset(3) == 16 + 72
  doAssert c.rootBlockCount() == 1
  c.data[5] = CtfsVersionV6
  doAssert c.fileEntryOffset(0) == 24 and c.fileEntryOffset(3) == 24 + 72,
    "a version-6 image's entry 0 is at 24, got " & $c.fileEntryOffset(0)
  doAssert c.rootBlockCount() == 2,
    "170 entries after a 24-byte header overflow block 0"
  # Growing the root is the inverse of `rootBlockCount` for the same header:
  # 2 blocks double to 4, which hold (16384 - 24) div 24 = 681 entries after
  # a 24-byte header. Sizing the grown array by the 16-byte header would
  # declare 682, whose root is 5 blocks, past the 4 the growth claimed.
  doAssert c.growRootDirectory().isOk
  doAssert c.maxRootEntries == 681'u32,
    "the grown version-6 root declares " & $c.maxRootEntries & " entries"
  doAssert c.rootBlockCount() == 4,
    "the grown root is " & $c.rootBlockCount() & " blocks, not the 4 it claimed"
  echo "PASS: test_header_size_follows_the_version"

when isMainModule:
  test_root_blocks_arithmetic()
  test_small_container_unchanged()
  test_overflow_in_memory()
  test_overflow_streaming()
  test_sharded_small_container()
  test_sharded_layout_is_the_unsharded_layout()
  test_old_offset_is_not_read()
  test_header_size_follows_the_version()
