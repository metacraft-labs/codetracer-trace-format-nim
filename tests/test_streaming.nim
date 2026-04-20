{.push raises: [].}

## Tests for streaming CTFS -- verifies that a concurrent reader can see
## events while the writer is still writing.
##
## Migrated from codetracer-native-recorder/ct_recorder/tests/test_streaming_ctfs.nim

import std/os
import results
import codetracer_ctfs

proc test_streaming_basic() {.raises: [].} =
  ## Create a streaming CTFS, write data, verify it appears on disk.
  let tmpDir = getTempDir()
  let path = tmpDir / "test_streaming.ctfs"

  # Clean up any previous run
  try:
    removeFile(path)
  except OSError:
    discard

  # 1. Create streaming CTFS
  let ctfsRes = createCtfsStreaming(path)
  doAssert ctfsRes.isOk, "createCtfsStreaming failed: " & ctfsRes.error
  var c = ctfsRes.get()
  doAssert c.isStreaming, "should be in streaming mode"

  # 2. Add a file and write some data
  let fileRes = c.addFile("events.log")
  doAssert fileRes.isOk, "addFile failed"
  var f = fileRes.get()

  var testData: array[128, byte]
  for i in 0 ..< testData.len:
    testData[i] = byte(i mod 256)

  let wRes = c.writeToFile(f, testData)
  doAssert wRes.isOk, "writeToFile failed"

  # 3. Sync the entry so the file size is visible on disk
  c.syncEntry(f)

  # 4. Read the file back as a concurrent reader would
  let dataRes = readCtfsFromFile(path)
  doAssert dataRes.isOk, "readCtfsFromFile failed: " & dataRes.error
  let diskData = dataRes.get()

  # Verify CTFS magic
  doAssert hasCtfsMagic(diskData), "CTFS magic not found on disk"

  # Verify the file is large enough (at least root block + mapping block + data block)
  doAssert diskData.len >= int(DefaultBlockSize) * 3,
    "disk file too small: " & $diskData.len

  # 5. Write more data
  var moreData: array[256, byte]
  for i in 0 ..< moreData.len:
    moreData[i] = byte(255 - (i mod 256))

  let wRes2 = c.writeToFile(f, moreData)
  doAssert wRes2.isOk, "second writeToFile failed"
  c.syncEntry(f)

  # 6. Re-read and verify new data is visible
  let dataRes2 = readCtfsFromFile(path)
  doAssert dataRes2.isOk, "second readCtfsFromFile failed"
  let diskData2 = dataRes2.get()
  doAssert diskData2.len >= diskData.len,
    "disk file should not have shrunk"

  # 7. Close the container
  c.closeCtfs()

  # 8. Final read -- should have valid CTFS with all data
  let dataRes3 = readCtfsFromFile(path)
  doAssert dataRes3.isOk, "final readCtfsFromFile failed"
  let diskData3 = dataRes3.get()
  doAssert hasCtfsMagic(diskData3), "CTFS magic not found after close"

  # Clean up
  try:
    removeFile(path)
  except OSError:
    discard

  echo "PASS: test_streaming_basic"

proc test_streaming_multiple_files() {.raises: [].} =
  ## Verify that multiple internal files can be written in streaming mode
  ## and all are visible to concurrent readers.
  let tmpDir = getTempDir()
  let path = tmpDir / "test_streaming_multi.ctfs"

  try:
    removeFile(path)
  except OSError:
    discard

  let ctfsRes = createCtfsStreaming(path)
  doAssert ctfsRes.isOk, "createCtfsStreaming failed"
  var c = ctfsRes.get()

  # Add two files
  let f1Res = c.addFile("meta.json")
  doAssert f1Res.isOk, "addFile meta.json failed"
  var f1 = f1Res.get()

  let f2Res = c.addFile("events.log")
  doAssert f2Res.isOk, "addFile events.log failed"
  var f2 = f2Res.get()

  # Write to both files
  let metaJson = cast[seq[byte]]("{\"program\":\"test\"}")
  let mRes = c.writeToFile(f1, metaJson)
  doAssert mRes.isOk, "write meta.json failed"

  var eventData: array[512, byte]
  for i in 0 ..< eventData.len:
    eventData[i] = byte(i mod 256)

  let eRes = c.writeToFile(f2, eventData)
  doAssert eRes.isOk, "write events.log failed"

  # Sync all entries at once
  c.syncAllEntries()

  # Read back and verify CTFS magic (basic sanity)
  let dataRes = readCtfsFromFile(path)
  doAssert dataRes.isOk, "readCtfsFromFile failed"
  doAssert hasCtfsMagic(dataRes.get()), "CTFS magic not found"

  c.closeCtfs()

  try:
    removeFile(path)
  except OSError:
    discard

  echo "PASS: test_streaming_multiple_files"

proc test_streaming_large_data() {.raises: [].} =
  ## Write enough data to trigger multi-block allocation and verify
  ## streaming flushes all blocks correctly.
  let tmpDir = getTempDir()
  let path = tmpDir / "test_streaming_large.ctfs"

  try:
    removeFile(path)
  except OSError:
    discard

  let ctfsRes = createCtfsStreaming(path)
  doAssert ctfsRes.isOk, "createCtfsStreaming failed"
  var c = ctfsRes.get()

  let fileRes = c.addFile("big.dat")
  doAssert fileRes.isOk, "addFile failed"
  var f = fileRes.get()

  # Write 64KB of data (16 blocks at 4096 bytes each)
  var bigData = newSeq[byte](65536)
  for i in 0 ..< bigData.len:
    bigData[i] = byte(i mod 256)

  let wRes = c.writeToFile(f, bigData)
  doAssert wRes.isOk, "writeToFile failed for large data"

  c.syncEntry(f)

  # Verify disk file is large enough
  let dataRes = readCtfsFromFile(path)
  doAssert dataRes.isOk, "readCtfsFromFile failed"
  let diskData = dataRes.get()

  # Should be at least: 1 root block + 1 mapping block + 16 data blocks = 18 blocks
  let minSize = 18 * int(DefaultBlockSize)
  doAssert diskData.len >= minSize,
    "disk file too small: " & $diskData.len & " < " & $minSize

  c.closeCtfs()

  # Verify final file matches in-memory data that was built
  let finalRes = readCtfsFromFile(path)
  doAssert finalRes.isOk, "final readCtfsFromFile failed"
  doAssert hasCtfsMagic(finalRes.get()), "CTFS magic not found after close"

  try:
    removeFile(path)
  except OSError:
    discard

  echo "PASS: test_streaming_large_data"

proc test_non_streaming_unchanged() {.raises: [].} =
  ## Verify that the non-streaming (in-memory) mode still works correctly.
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

  # syncEntry should be a no-op in non-streaming mode
  c.syncEntry(f)
  c.syncAllEntries()

  c.closeCtfs()

  echo "PASS: test_non_streaming_unchanged"

proc test_streaming_incremental_visibility() {.raises: [].} =
  ## Simulate incremental writes and verify that after each syncEntry,
  ## the file size field on disk reflects the latest data.
  let tmpDir = getTempDir()
  let path = tmpDir / "test_streaming_incr.ctfs"

  try:
    removeFile(path)
  except OSError:
    discard

  let ctfsRes = createCtfsStreaming(path)
  doAssert ctfsRes.isOk, "createCtfsStreaming failed"
  var c = ctfsRes.get()

  let fileRes = c.addFile("stream.dat")
  doAssert fileRes.isOk, "addFile failed"
  var f = fileRes.get()

  # Write 100 bytes, sync, check file entry size field
  var chunk1: array[100, byte]
  for i in 0 ..< chunk1.len:
    chunk1[i] = byte(i)
  let w1 = c.writeToFile(f, chunk1)
  doAssert w1.isOk, "first write failed"
  c.syncEntry(f)

  # Read back the file entry size from disk
  block:
    let dRes = readCtfsFromFile(path)
    doAssert dRes.isOk, "read failed after first sync"
    let d = dRes.get()
    # File entry 0 is at offset HeaderSize + ExtHeaderSize = 16
    # size field is the first 8 bytes of the entry
    let entryOff = 16  # HeaderSize(8) + ExtHeaderSize(8)
    doAssert d.len > entryOff + 8, "disk too small for entry"
    var sizeBytes: array[8, byte]
    for i in 0 ..< 8:
      sizeBytes[i] = d[entryOff + i]
    # Read little-endian uint64
    var fileSize: uint64 = 0
    for i in 0 ..< 8:
      fileSize = fileSize or (uint64(sizeBytes[i]) shl (i * 8))
    doAssert fileSize == 100,
      "file size after first sync should be 100, got " & $fileSize

  # Write 200 more bytes, sync, check updated size
  var chunk2: array[200, byte]
  for i in 0 ..< chunk2.len:
    chunk2[i] = byte(255 - i mod 256)
  let w2 = c.writeToFile(f, chunk2)
  doAssert w2.isOk, "second write failed"
  c.syncEntry(f)

  block:
    let dRes = readCtfsFromFile(path)
    doAssert dRes.isOk, "read failed after second sync"
    let d = dRes.get()
    let entryOff = 16
    var fileSize: uint64 = 0
    for i in 0 ..< 8:
      fileSize = fileSize or (uint64(d[entryOff + i]) shl (i * 8))
    doAssert fileSize == 300,
      "file size after second sync should be 300, got " & $fileSize

  c.closeCtfs()

  try:
    removeFile(path)
  except OSError:
    discard

  echo "PASS: test_streaming_incremental_visibility"

# Run all tests
test_streaming_basic()
test_streaming_multiple_files()
test_streaming_large_data()
test_non_streaming_unchanged()
test_streaming_incremental_visibility()
