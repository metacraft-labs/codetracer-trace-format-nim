## Genuine version-5 sharded root layout, writer/reader/append/analyzer controls.
## No mocks: actual serialized containers, original readers, real temporary files.
##
## `ctfs-container.md` §1, "The free list root area is removed (2026-10-08)":
## nothing lies between the header and the entry array for any `max_shards`,
## so a sharded container's entries start at byte 16, exactly as an unsharded
## one's. This test was written against the earlier rule (entries at
## `16 + 7 * max_shards * 6`); its oracles now state the current one, and the
## removed offset is kept as a negative control: an entry placed there is not
## read.
import std/[os, tempfiles]
import results
import codetracer_ctfs
import codetracer_ctfs/space_analyzer

proc add(c: var Ctfs, name: string, bytes: seq[byte]) =
  var f = c.addFile(name).get()
  doAssert c.writeToFile(f, bytes).isOk

proc verify(data: seq[byte], count: uint32, shards: uint8) =
  let prefix = 16
  doAssert data[5] == 5 and data[7] == shards
  doAssert rootDirectoryLayout(data).entryStart == prefix
  doAssert readU64LE(data, prefix + 16) == base40Encode("payload")
  let got = readInternalFile(data, "payload", 4096, count)
  doAssert got.isOk and got.get() == @[1'u8, 2, 3]
  let report = analyzeCtfs(data).get()
  doAssert report.headerBytes == 16
  doAssert report.files.len == int(count) and report.files[0].name == "payload"
  doAssert report.files[0].dataBytes == 3
  var beyondFirstRootBlock, straddles = false
  for i in 0 ..< int(count):
    let off = prefix + i * 24
    doAssert (readU64LE(data, off + 8) and not CtfsDirect) >= rootBlockCount(4096, count, shards)
    if off div 4096 > prefix div 4096: beyondFirstRootBlock = true
    if off div 4096 != (off + 23) div 4096: straddles = true
    if i > 0:
      let name = "m" & $i
      doAssert readU64LE(data, off + 16) == base40Encode(name)
      doAssert readInternalFile(data, name, 4096, count).get() == @[byte(i mod 256)]
  doAssert beyondFirstRootBlock and straddles, "fixture must exercise overflow and straddling entries"

let directory = createTempDir("ctfs-sharded-root-", "")
# Failure keeps the exclusively owned fixture directory as real evidence.
for shards in [1'u8, 2'u8, 16'u8, 255'u8]:
  var c = createCtfs(maxRootEntries = 400, maxShards = shards)
  c.add("payload", @[1'u8, 2, 3])
  for i in 1 ..< 400: c.add("m" & $i, @[byte(i mod 256)])
  verify(c.toBytes(), 400, shards)
  let path = directory / ("stream-" & $shards & ".ct")
  var streaming = createCtfsStreaming(path, maxRootEntries = 400, maxShards = shards).get()
  streaming.add("payload", @[1'u8, 2, 3])
  for i in 1 ..< 400: streaming.add("m" & $i, @[byte(i mod 256)])
  streaming.syncAllEntries()
  verify(readCtfsFromFile(path).get(), 400, shards)
  doAssert streaming.closeCtfs().isOk
  verify(readCtfsFromFile(path).get(), 400, shards)
  removeFile(path)
  echo "PASS: nonzero-shard memory/streaming/reserved-byte oracle " & $shards

# Header count zero really auto-fills block 0 after the header, whatever
# the shard count.
var automatic = createCtfs(maxRootEntries = 0, maxShards = 1)
doAssert readU32LE(automatic.toBytes(), 12) == 0
doAssert automatic.maxRootEntries == uint32((4096 - 16) div 24)
for i in 0 ..< int(automatic.maxRootEntries): automatic.add("a" & $i, @[byte(i mod 256)])
let autoFilled = automatic.maxRootEntries
let autoBytes = automatic.toBytes()
doAssert rootDirectoryLayout(autoBytes).entryCount == autoFilled
let lastName = "a" & $(autoFilled - 1)
doAssert readInternalFile(autoBytes, lastName, 4096, autoFilled).get() == @[byte((autoFilled - 1) mod 256)]
doAssert analyzeCtfs(autoBytes).get().files.len == int(autoFilled)
# One member more grows the root instead of being refused (`ctfs-container.md`
# §1: the entry array continues into the blocks after block 0).
var overflowFile = automatic.addFile("overflow")
doAssert overflowFile.isOk, overflowFile.error
doAssert automatic.writeToFile(overflowFile.get(), @[7'u8]).isOk
doAssert automatic.maxRootEntries > autoFilled
let grownBytes = automatic.toBytes()
doAssert rootDirectoryLayout(grownBytes).entryCount == automatic.maxRootEntries
doAssert readInternalFile(grownBytes, "overflow", 4096, automatic.maxRootEntries).get() == @[7'u8]
doAssert readInternalFile(grownBytes, lastName, 4096, automatic.maxRootEntries).get() == @[byte((autoFilled - 1) mod 256)]
doAssert analyzeCtfs(grownBytes).get().files.len == int(autoFilled) + 1
echo "PASS: header auto-fill starts after the header, and a full root grows"

# A caller's large scan limit cannot make a data block masquerade as a name.
# At a 1024-byte block, slot 42 of an unbounded scan (16 + 42 * 24 = 1024)
# lies over block 1, the data block of "real", whose bytes 16..23 carry the
# name word "fake".
var fake = newSeq[byte](24)
writeU64LE(fake, 16, base40Encode("fake"))
var small = createCtfs(blockSize = 1024, maxRootEntries = 1)
small.add("real", fake)
let smallBytes = small.toBytes()
doAssert readInternalFile(smallBytes, "real", 1024, 170).get() == fake
doAssert not hasInternalFile(smallBytes, "fake", 170)
doAssert analyzeCtfs(smallBytes, 1024).get().files.len == 1
echo "PASS: small header count excludes data-name collision"

var sharded = createCtfs(maxRootEntries = 2, maxShards = 1)
sharded.add("payload", @[1'u8, 2, 3])
let original = sharded.toBytes()
doAssert readInternalFile(original, "payload", 4096, 2).get() == @[1'u8, 2, 3]
# The negative control: the entry moved to the removed `16 + 42` offset.
var misplaced = original[0 .. ^1]
for i in 0 ..< 24: misplaced[58 + i] = original[16 + i]
for i in 0 ..< 24: misplaced[16 + i] = 0
doAssert readInternalFile(misplaced, "payload", 4096, 2).isErr
doAssert not hasInternalFile(misplaced, "payload", 2)
var truncated = original[0 .. ^1]
truncated.setLen(57)
doAssert rootDirectoryLayout(truncated).error.len > 0
doAssert readInternalFile(truncated, "payload", 4096, 2).isErr
doAssert analyzeCtfs(truncated).isErr
echo "PASS: an entry at the removed offset and a truncated root directory refuse"

let appendPath = directory / "append.ct"
doAssert sharded.writeCtfsToFile(appendPath).isOk
doAssert appendInternalFiles(appendPath, ["second"], [@[4'u8, 5]]).isOk
let appended = readCtfsFromFile(appendPath).get()
doAssert readInternalFile(appended, "payload", 4096, 2).get() == @[1'u8, 2, 3]
doAssert readInternalFile(appended, "second", 4096, 2).get() == @[4'u8, 5]
doAssert readU64LE(appended, 16 + 16) == base40Encode("payload")
doAssert readU64LE(appended, 16 + 24 + 16) == base40Encode("second")
removeFile(appendPath)
# A root that spills past block 0 is reopened and appended to like any other
# (`ctfs-container.md` §1: `root_blocks` contiguous blocks hold the entries).
var overflow = createCtfs(maxRootEntries = 400, maxShards = 1)
overflow.add("payload", @[1'u8, 2, 3])
let overflowPath = directory / "append-overflow.ct"
doAssert overflow.writeCtfsToFile(overflowPath).isOk
doAssert appendInternalFiles(overflowPath, ["second"], [@[4'u8, 5]]).isOk
let overflowed = readCtfsFromFile(overflowPath).get()
doAssert readInternalFile(overflowed, "payload", 4096, 400).get() == @[1'u8, 2, 3]
doAssert readInternalFile(overflowed, "second", 4096, 400).get() == @[4'u8, 5]
removeFile(overflowPath)
echo "PASS: real sharded append roundtrip, in block 0 and past it"

# Version 6 is read (`ctfs-container.md` §1a) and has its own header, so a
# version-5 image restamped 6 is not a refusal oracle; 2-4 are refused.
for version in [2'u8, 3'u8, 4'u8]:
  var unsupported = original[0 .. ^1]
  unsupported[5] = version
  doAssert readInternalFile(unsupported, "payload", 4096, 2).isErr
  doAssert not hasInternalFile(unsupported, "payload", 2)
  doAssert analyzeCtfs(unsupported).isErr
  let path = directory / ("unsupported-" & $version & ".ct")
  # writeCtfsToFile accepts Ctfs, so materialize these actual malformed bytes.
  var raw = newString(unsupported.len)
  for i, value in unsupported: raw[i] = char(value)
  writeFile(path, raw)
  doAssert openClosedCtfs(path).isErr
  removeFile(path)
echo "PASS: original unsupported full-profile version refusals"
removeDir(directory)
