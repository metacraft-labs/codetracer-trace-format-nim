{.push raises: [].}

## Tests for in-memory CTFS container operations.

import std/os
import results
import codetracer_ctfs

proc test_create_and_magic() {.raises: [].} =
  ## Test that creating a CTFS produces valid magic and version bytes.
  var c = createCtfs()
  let bytes = c.toBytes()
  doAssert hasCtfsMagic(bytes), "CTFS magic not found"
  doAssert hasValidVersion(bytes), "CTFS version not valid"
  doAssert bytes.len == int(DefaultBlockSize),
    "initial size should be one block"
  echo "PASS: test_create_and_magic"

proc test_encryption_max_shards_fields() {.raises: [].} =
  ## Test encryption and max_shards fields in the v4 header.
  var c = createCtfs(encryption = emAes256Gcm, maxShards = 4)
  let bytes = c.toBytes()
  doAssert bytes[5] == 4, "version should be 4"
  doAssert bytes[6] == uint8(emAes256Gcm), "byte 6 should be encryption"
  doAssert bytes[7] == 4, "byte 7 should be max_shards"
  doAssert readEncryptionMethod(bytes) == emAes256Gcm,
    "encryption method should be aes256gcm"
  doAssert readMaxShards(bytes) == 4,
    "max_shards should be 4"
  # Compression is not in the header for v4
  doAssert readCompressionMethod(bytes) == cmNone,
    "compression should be cmNone for v4 (not in header)"
  echo "PASS: test_encryption_max_shards_fields"

proc test_v3_backward_compat() {.raises: [].} =
  ## Test that v3 headers are read correctly with the old layout.
  ## V3 layout: [6] = compression, [7] = encryption
  var fakeV3 = newSeq[byte](16)
  fakeV3[0] = 0xC0; fakeV3[1] = 0xDE; fakeV3[2] = 0x72
  fakeV3[3] = 0xAC; fakeV3[4] = 0xE2
  fakeV3[5] = 3  # version 3
  fakeV3[6] = 1  # compression = cmZstd (old layout)
  fakeV3[7] = 1  # encryption = emAes256Gcm (old layout)
  doAssert hasCtfsMagic(fakeV3), "magic should be valid"
  doAssert hasValidVersion(fakeV3), "v3 should be accepted"
  doAssert readCompressionMethod(fakeV3) == cmZstd,
    "v3 byte 6 should be read as compression"
  doAssert readEncryptionMethod(fakeV3) == emAes256Gcm,
    "v3 byte 7 should be read as encryption"
  doAssert readMaxShards(fakeV3) == DefaultMaxShards,
    "v3 files should return default max_shards"
  echo "PASS: test_v3_backward_compat"

proc test_add_file_and_write() {.raises: [].} =
  ## Test adding a file and writing data to it.
  var c = createCtfs()
  doAssert not c.isStreaming, "default should not be streaming"

  let fileRes = c.addFile("test.dat")
  doAssert fileRes.isOk, "addFile failed"
  var f = fileRes.get()

  var data: array[100, byte]
  for i in 0 ..< data.len:
    data[i] = byte(i)

  let wRes = c.writeToFile(f, data)
  doAssert wRes.isOk, "writeToFile failed"

  let bytes = c.toBytes()
  doAssert hasCtfsMagic(bytes), "CTFS magic not found"

  # Should be at least root block + mapping block + data block = 3 blocks
  doAssert bytes.len >= int(DefaultBlockSize) * 3,
    "container too small: " & $bytes.len

  # syncEntry/syncAllEntries should be no-ops in non-streaming mode
  c.syncEntry(f)
  c.syncAllEntries()

  c.closeCtfs()
  echo "PASS: test_add_file_and_write"

proc test_multi_block_write() {.raises: [].} =
  ## Test writing data that spans multiple blocks.
  var c = createCtfs()

  let fileRes = c.addFile("big.dat")
  doAssert fileRes.isOk, "addFile failed"
  var f = fileRes.get()

  # Write 16KB = 4 blocks at 4096 bytes each
  var bigData = newSeq[byte](16384)
  for i in 0 ..< bigData.len:
    bigData[i] = byte(i mod 256)

  let wRes = c.writeToFile(f, bigData)
  doAssert wRes.isOk, "writeToFile failed for multi-block data"

  let bytes = c.toBytes()
  # At least: 1 root + 1 mapping + 4 data = 6 blocks
  let minSize = 6 * int(DefaultBlockSize)
  doAssert bytes.len >= minSize,
    "container too small: " & $bytes.len & " < " & $minSize

  c.closeCtfs()
  echo "PASS: test_multi_block_write"

proc test_multiple_files() {.raises: [].} =
  ## Test adding and writing to multiple internal files.
  var c = createCtfs()

  let f1Res = c.addFile("meta.json")
  doAssert f1Res.isOk, "addFile meta.json failed"
  var f1 = f1Res.get()

  let f2Res = c.addFile("events.log")
  doAssert f2Res.isOk, "addFile events.log failed"
  var f2 = f2Res.get()

  let metaJson = cast[seq[byte]]("{\"program\":\"test\"}")
  let mRes = c.writeToFile(f1, metaJson)
  doAssert mRes.isOk, "write meta.json failed"

  var eventData: array[512, byte]
  for i in 0 ..< eventData.len:
    eventData[i] = byte(i mod 256)
  let eRes = c.writeToFile(f2, eventData)
  doAssert eRes.isOk, "write events.log failed"

  let bytes = c.toBytes()
  doAssert hasCtfsMagic(bytes), "CTFS magic not found"

  c.closeCtfs()
  echo "PASS: test_multiple_files"

proc test_write_to_file_and_read_back() {.raises: [].} =
  ## Test writing a CTFS to disk and reading it back.
  let tmpDir = getTempDir()
  let path = tmpDir / "test_container_rw.ctfs"

  try:
    removeFile(path)
  except OSError:
    discard

  var c = createCtfs()
  let fileRes = c.addFile("data.bin")
  doAssert fileRes.isOk, "addFile failed"
  var f = fileRes.get()

  var data: array[200, byte]
  for i in 0 ..< data.len:
    data[i] = byte(i mod 256)
  let wRes = c.writeToFile(f, data)
  doAssert wRes.isOk, "writeToFile failed"

  let writeRes = c.writeCtfsToFile(path)
  doAssert writeRes.isOk, "writeCtfsToFile failed: " & writeRes.error

  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk, "readCtfsFromFile failed: " & readRes.error
  let diskData = readRes.get()
  doAssert hasCtfsMagic(diskData), "CTFS magic not found on re-read"
  doAssert hasValidVersion(diskData), "CTFS version not valid on re-read"

  c.closeCtfs()

  try:
    removeFile(path)
  except OSError:
    discard

  echo "PASS: test_write_to_file_and_read_back"

# Run all tests
test_create_and_magic()
test_encryption_max_shards_fields()
test_v3_backward_compat()
test_add_file_and_write()
test_multi_block_write()
test_multiple_files()
test_write_to_file_and_read_back()
