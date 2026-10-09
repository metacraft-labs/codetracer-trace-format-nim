## Container version 5: a member's `MapBlock` has three forms
## (`ctfs-container.md` §2, "`MapBlock` has three forms" and "Older versions
## are refused"; §5 "Creating a File" / "Appending Data").
##
## * creating a member claims no block: it is `(0, 0)` until written;
## * its first write claims one data block and stores it tagged
##   (`CtfsDirect or b`) — no mapping block while it fits one block;
## * the append that takes it past one block claims the level-1 mapping block
##   FIRST, puts the direct block in slot 0, then claims the new data blocks
##   in file order, and stores the untagged mapping block;
## * readers decide the layout from `MapBlock`, refuse a null pointer with a
##   non-zero size, refuse a direct member larger than a block, and refuse a
##   container whose version byte is not 5, naming it.
##
## No mocks: real containers, in memory and streamed to a real file.

import std/[os, strutils]
import results
import codetracer_ctfs

proc entryOf(c: Ctfs, idx: int): (uint64, uint64) =
  let off = c.fileEntryOffset(idx)
  (readU64LE(c.data, off), readU64LE(c.data, off + 8))

proc bytesOf(n: int, seed: int): seq[byte] =
  result = newSeq[byte](n)
  for i in 0 ..< n:
    result[i] = byte((i * 31 + seed) and 0xFF)

block creating_a_member_claims_no_block:
  var c = createCtfs()
  let before = c.nextFreeBlock
  let f = c.addFile("a.dat").get()
  doAssert c.nextFreeBlock == before,
    "addFile claimed " & $(c.nextFreeBlock - before) & " block(s); a member " &
    "that is never written must cost none"
  doAssert c.entryOf(f.entryIndex) == (0'u64, 0'u64)
  let img = c.toBytes()
  doAssert hasInternalFile(img, "a.dat"),
    "an empty member is present: presence is the name's, not the size's"
  let r = readInternalFile(img, "a.dat")
  doAssert r.isOk and r.get().len == 0, "an empty member reads as zero bytes"
  doAssert img[5] == 5'u8, "writers write version 5, got " & $img[5]
  echo "PASS creating_a_member_claims_no_block"

block first_write_is_one_tagged_data_block:
  var c = createCtfs()
  var f = c.addFile("a.dat").get()
  let before = c.nextFreeBlock
  doAssert c.writeToFile(f, bytesOf(100, 1)).isOk
  doAssert c.writeToFile(f, bytesOf(3996, 2)).isOk   # exactly one block
  doAssert c.nextFreeBlock == before + 1,
    "a member of at most one block owns one data block and no mapping block; " &
    "claimed " & $(c.nextFreeBlock - before)
  let (size, mapBlock) = c.entryOf(f.entryIndex)
  doAssert size == 4096
  doAssert (mapBlock and CtfsDirect) != 0, "MapBlock must carry the direct tag"
  doAssert (mapBlock and not CtfsDirect) == before
  let back = readInternalFile(c.toBytes(), "a.dat").get()
  doAssert back[0 ..< 100] == bytesOf(100, 1)
  doAssert back[100 ..< 4096] == bytesOf(3996, 2)
  echo "PASS first_write_is_one_tagged_data_block"

block growing_past_one_block_claims_the_mapping_block_first:
  var c = createCtfs()
  var f = c.addFile("a.dat").get()
  var other = c.addFile("b.dat").get()
  doAssert c.writeToFile(f, bytesOf(1000, 3)).isOk
  let b = c.nextFreeBlock - 1
  doAssert c.writeToFile(other, bytesOf(10, 4)).isOk   # interleave a claim
  let m0 = c.nextFreeBlock
  doAssert c.writeToFile(f, bytesOf(9000, 5)).isOk     # -> 10000 bytes, 3 blocks
  let (size, mapBlock) = c.entryOf(f.entryIndex)
  doAssert size == 10000
  doAssert (mapBlock and CtfsDirect) == 0, "a mapped member's MapBlock is untagged"
  doAssert mapBlock == m0,
    "the mapping block is claimed before the new data blocks: expected " &
    $m0 & ", got " & $mapBlock
  doAssert c.readPtr(mapBlock, 0) == b, "slot 0 holds the former direct block"
  doAssert c.readPtr(mapBlock, 1) == m0 + 1
  doAssert c.readPtr(mapBlock, 2) == m0 + 2
  doAssert c.nextFreeBlock == m0 + 3
  let back = readInternalFile(c.toBytes(), "a.dat").get()
  doAssert back[0 ..< 1000] == bytesOf(1000, 3)
  doAssert back[1000 ..< 10000] == bytesOf(9000, 5)
  doAssert readInternalFile(c.toBytes(), "b.dat").get() == bytesOf(10, 4)
  echo "PASS growing_past_one_block_claims_the_mapping_block_first"

block a_first_write_longer_than_a_block_is_mapped_at_once:
  var c = createCtfs()
  var f = c.addFile("a.dat").get()
  let m0 = c.nextFreeBlock
  doAssert c.writeToFile(f, bytesOf(5000, 6)).isOk
  let (_, mapBlock) = c.entryOf(f.entryIndex)
  doAssert mapBlock == m0, "mapping block first, then the data blocks"
  doAssert c.readPtr(mapBlock, 0) == m0 + 1
  doAssert c.readPtr(mapBlock, 1) == m0 + 2
  doAssert readInternalFile(c.toBytes(), "a.dat").get() == bytesOf(5000, 6)
  echo "PASS a_first_write_longer_than_a_block_is_mapped_at_once"

block a_streamed_container_reads_back_the_same:
  let path = getTempDir() / ("ctfs_v5_forms_" & $getCurrentProcessId() & ".ct")
  var c = createCtfsStreaming(path).get()
  var small = c.addFile("small.dat").get()
  var big = c.addFile("big.dat").get()
  discard c.addFile("never.dat").get()
  doAssert c.writeToFile(small, bytesOf(50, 7)).isOk
  doAssert c.writeToFile(big, bytesOf(4000, 8)).isOk
  doAssert c.writeToFile(big, bytesOf(700_000, 9)).isOk   # several hundred blocks
  doAssert closeCtfs(c).isOk
  let img = readCtfsFromFile(path).get()
  doAssert readInternalFile(img, "small.dat").get() == bytesOf(50, 7)
  let bigBack = readInternalFile(img, "big.dat").get()
  doAssert bigBack.len == 704_000
  doAssert bigBack[0 ..< 4000] == bytesOf(4000, 8)
  doAssert bigBack[4000 ..< 704_000] == bytesOf(700_000, 9)
  doAssert hasInternalFile(img, "never.dat")
  doAssert readInternalFile(img, "never.dat").get().len == 0
  let e = findFileEntry(img, "never.dat")
  doAssert e.found and e.size == 0 and e.mapBlock == 0
  removeFile(path)
  echo "PASS a_streamed_container_reads_back_the_same"

proc imageWithEntry(size, mapBlock: uint64): seq[byte] =
  var c = createCtfs()
  var f = c.addFile("x.dat").get()
  doAssert c.writeToFile(f, bytesOf(10, 1)).isOk
  var img = c.toBytes()
  let off = c.fileEntryOffset(f.entryIndex)
  writeU64LE(img, off, size)
  writeU64LE(img, off + 8, mapBlock)
  img

block readers_refuse_malformed_entries:
  # A non-zero size with a null MapBlock is a null pointer, not an empty member.
  let nullPtr = readInternalFile(imageWithEntry(10, 0), "x.dat")
  doAssert nullPtr.isErr and "null MapBlock" in nullPtr.error, $nullPtr
  # A direct member larger than one block cannot be held by its one block.
  let tooBig = readInternalFile(imageWithEntry(4097, CtfsDirect or 1), "x.dat")
  doAssert tooBig.isErr and "4097" in tooBig.error, $tooBig
  # A direct block outside the container.
  let oob = readInternalFile(imageWithEntry(10, CtfsDirect or 999), "x.dat")
  doAssert oob.isErr and "out of bounds" in oob.error, $oob
  # A direct pointer to block 0.
  let zero = readInternalFile(imageWithEntry(10, CtfsDirect), "x.dat")
  doAssert zero.isErr and "block 0" in zero.error, $zero
  echo "PASS readers_refuse_malformed_entries"

block readers_refuse_every_other_version:
  var c = createCtfs()
  var f = c.addFile("x.dat").get()
  doAssert c.writeToFile(f, bytesOf(10, 1)).isOk
  for v in [0'u8, 2, 3, 4, 6, 255]:
    var img = c.toBytes()
    img[5] = v
    let r = readInternalFile(img, "x.dat")
    doAssert r.isErr, "version " & $v & " was read"
    doAssert ("version " & $v) in r.error and "version 5" in r.error,
      "the refusal must name the version found and the one read: " & r.error
  echo "PASS readers_refuse_every_other_version"

echo "ALL PASS test_ctfs_v5_member_forms"
