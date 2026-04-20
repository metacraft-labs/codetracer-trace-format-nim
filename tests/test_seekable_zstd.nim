{.push raises: [].}

## Comprehensive tests for the seekable Zstd encoder/decoder.

import results
import codetracer_ctfs/seekable_zstd

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc makeCompressibleData(size: int): seq[byte] =
  ## Generate compressible data with a repeating pattern.
  result = newSeq[byte](size)
  for i in 0 ..< size:
    result[i] = byte(i mod 251)  # prime mod for pattern variety

proc makeKnownData(size: int): seq[byte] =
  ## Generate data where each byte encodes its position (mod 256).
  result = newSeq[byte](size)
  for i in 0 ..< size:
    result[i] = byte(i mod 256)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

proc test_roundtrip_single_frame() =
  ## Write less than threshold data, finish, decode, verify.
  let threshold = 1024 * 1024  # 1 MiB
  var enc = newSeekableZstdEncoder(frameThreshold = threshold)

  let original = makeCompressibleData(1000)
  enc.write(original)
  let compressed = enc.finish()

  doAssert compressed.len > 0, "compressed output should not be empty"

  let decRes = initSeekableZstdDecoder(compressed)
  doAssert decRes.isOk, "decoder init failed: " & decRes.error

  let dec = decRes.get()
  doAssert dec.frameCount == 1, "expected 1 frame, got " & $dec.frameCount

  let decompRes = dec.decompressAll()
  doAssert decompRes.isOk, "decompressAll failed: " & decompRes.error

  let decompressed = decompRes.get()
  doAssert decompressed.len == original.len,
    "length mismatch: " & $decompressed.len & " vs " & $original.len
  doAssert decompressed == original, "data mismatch after roundtrip"

  echo "PASS: test_roundtrip_single_frame"

proc test_roundtrip_multi_frame() =
  ## Write more than 3x threshold data, verify 3+ frames, decode all, verify.
  let threshold = 4096
  var enc = newSeekableZstdEncoder(frameThreshold = threshold)

  let original = makeCompressibleData(threshold * 3 + 500)
  enc.write(original)
  let compressed = enc.finish()

  let decRes = initSeekableZstdDecoder(compressed)
  doAssert decRes.isOk, "decoder init failed: " & decRes.error

  let dec = decRes.get()
  doAssert dec.frameCount >= 3,
    "expected at least 3 frames, got " & $dec.frameCount

  let decompRes = dec.decompressAll()
  doAssert decompRes.isOk, "decompressAll failed: " & decompRes.error

  let decompressed = decompRes.get()
  doAssert decompressed.len == original.len,
    "length mismatch: " & $decompressed.len & " vs " & $original.len
  doAssert decompressed == original, "data mismatch after multi-frame roundtrip"

  echo "PASS: test_roundtrip_multi_frame"

proc test_seek_to_offset() =
  ## Write known data, seek to middle, verify correct frame and offset.
  let threshold = 1000
  var enc = newSeekableZstdEncoder(frameThreshold = threshold)

  let original = makeKnownData(5000)
  enc.write(original)
  let compressed = enc.finish()

  let decRes = initSeekableZstdDecoder(compressed)
  doAssert decRes.isOk, "decoder init failed: " & decRes.error

  let dec = decRes.get()

  # Seek to offset 2500 (middle of data)
  let seekRes = dec.seekToOffset(2500)
  doAssert seekRes.isOk, "seekToOffset failed: " & seekRes.error

  let (frameIdx, offsetInFrame) = seekRes.get()
  doAssert frameIdx >= 0 and frameIdx < dec.frameCount,
    "frame index out of range: " & $frameIdx

  # Verify: the decompressed start of frameIdx + offsetInFrame == 2500
  var decompStart: uint64 = 0
  for i in 0 ..< frameIdx:
    decompStart += uint64(dec.seekTable.entries[i].decompressedSize)
  doAssert decompStart + offsetInFrame == 2500,
    "seek offset mismatch: " & $(decompStart + offsetInFrame) & " != 2500"

  # Decompress that frame and verify the byte at offsetInFrame
  let frameRes = dec.decompressFrame(frameIdx)
  doAssert frameRes.isOk, "decompressFrame failed: " & frameRes.error
  let frameData = frameRes.get()
  doAssert frameData[int(offsetInFrame)] == byte(2500 mod 256),
    "byte at seek offset mismatch"

  echo "PASS: test_seek_to_offset"

proc test_decompress_individual_frames() =
  ## Decompress each frame separately, concatenate, verify matches original.
  let threshold = 2000
  var enc = newSeekableZstdEncoder(frameThreshold = threshold)

  let original = makeCompressibleData(10000)
  enc.write(original)
  let compressed = enc.finish()

  let decRes = initSeekableZstdDecoder(compressed)
  doAssert decRes.isOk, "decoder init failed: " & decRes.error

  let dec = decRes.get()
  var concatenated: seq[byte] = @[]

  for i in 0 ..< dec.frameCount:
    let frameRes = dec.decompressFrame(i)
    doAssert frameRes.isOk, "decompressFrame " & $i & " failed: " & frameRes.error
    concatenated.add(frameRes.get())

  doAssert concatenated.len == original.len,
    "concatenated length mismatch: " & $concatenated.len & " vs " & $original.len
  doAssert concatenated == original, "concatenated data mismatch"

  echo "PASS: test_decompress_individual_frames"

proc test_empty_data() =
  ## Encoder with no writes produces valid (empty) output.
  var enc = newSeekableZstdEncoder()
  let compressed = enc.finish()

  # Should be just the seek table with 0 entries
  let expectedSize = 8 + 9  # skippable header + footer, no entries
  doAssert compressed.len == expectedSize,
    "empty encoder output size should be " & $expectedSize & " but got " & $compressed.len

  let decRes = initSeekableZstdDecoder(compressed)
  doAssert decRes.isOk, "decoder init on empty data failed: " & decRes.error

  let dec = decRes.get()
  doAssert dec.frameCount == 0, "empty data should have 0 frames"

  let decompRes = dec.decompressAll()
  doAssert decompRes.isOk, "decompressAll on empty failed: " & decompRes.error
  doAssert decompRes.get().len == 0, "empty decompression should produce 0 bytes"

  echo "PASS: test_empty_data"

proc test_seek_table_parse() =
  ## Manually construct a seek table, serialize, parse back, verify.
  let table = SeekTable(entries: @[
    SeekTableEntry(compressedSize: 100, decompressedSize: 500),
    SeekTableEntry(compressedSize: 200, decompressedSize: 1000),
    SeekTableEntry(compressedSize: 150, decompressedSize: 750),
  ])

  let serialized = serializeSeekTable(table)
  let parseRes = parseSeekTable(serialized)
  doAssert parseRes.isOk, "parseSeekTable failed: " & parseRes.error

  let parsed = parseRes.get()
  doAssert parsed.entries.len == table.entries.len,
    "entry count mismatch: " & $parsed.entries.len & " vs " & $table.entries.len

  for i in 0 ..< table.entries.len:
    doAssert parsed.entries[i].compressedSize == table.entries[i].compressedSize,
      "compressed size mismatch at entry " & $i
    doAssert parsed.entries[i].decompressedSize == table.entries[i].decompressedSize,
      "decompressed size mismatch at entry " & $i

  doAssert parsed.totalCompressedSize() == 450,
    "total compressed size mismatch"
  doAssert parsed.totalDecompressedSize() == 2250,
    "total decompressed size mismatch"

  echo "PASS: test_seek_table_parse"

proc test_large_data() =
  ## Write 10 MB of compressible data, verify compression ratio < 1.0.
  let size = 10 * 1024 * 1024  # 10 MiB
  var enc = newSeekableZstdEncoder()

  let original = makeCompressibleData(size)
  enc.write(original)
  let compressed = enc.finish()

  doAssert compressed.len < original.len,
    "compression ratio should be < 1.0 for compressible data, got " &
    $compressed.len & " >= " & $original.len

  let decRes = initSeekableZstdDecoder(compressed)
  doAssert decRes.isOk, "decoder init failed: " & decRes.error

  let dec = decRes.get()
  let decompRes = dec.decompressAll()
  doAssert decompRes.isOk, "decompressAll failed: " & decompRes.error
  doAssert decompRes.get() == original, "data mismatch after large roundtrip"

  echo "PASS: test_large_data"

proc test_frame_count() =
  ## Verify the number of frames matches ceil(dataSize / threshold).
  let threshold = 5000
  let dataSize = 23456
  var enc = newSeekableZstdEncoder(frameThreshold = threshold)

  let original = makeCompressibleData(dataSize)
  enc.write(original)
  let compressed = enc.finish()

  let decRes = initSeekableZstdDecoder(compressed)
  doAssert decRes.isOk, "decoder init failed: " & decRes.error

  let dec = decRes.get()
  let expectedFrames = (dataSize + threshold - 1) div threshold  # ceil division
  doAssert dec.frameCount == expectedFrames,
    "frame count mismatch: expected " & $expectedFrames & " got " & $dec.frameCount

  echo "PASS: test_frame_count"

proc test_incremental_write() =
  ## Write data in small increments, verify same result as single write.
  let threshold = 4096
  let original = makeCompressibleData(20000)

  # Single write
  var enc1 = newSeekableZstdEncoder(frameThreshold = threshold)
  enc1.write(original)
  let compressed1 = enc1.finish()

  # Incremental writes of varying sizes
  var enc2 = newSeekableZstdEncoder(frameThreshold = threshold)
  var offset = 0
  let chunkSizes = [100, 500, 1000, 3000, 7000, 8400]
  var chunkIdx = 0
  while offset < original.len:
    let chunk = min(chunkSizes[chunkIdx mod chunkSizes.len], original.len - offset)
    enc2.write(original.toOpenArray(offset, offset + chunk - 1))
    offset += chunk
    chunkIdx += 1
  let compressed2 = enc2.finish()

  # Both should decompress to the same data
  let dec1 = initSeekableZstdDecoder(compressed1).get()
  let dec2 = initSeekableZstdDecoder(compressed2).get()

  doAssert dec1.decompressAll().get() == original, "single write roundtrip failed"
  doAssert dec2.decompressAll().get() == original, "incremental write roundtrip failed"

  # Frame counts should be equal (same threshold, same total data)
  doAssert dec1.frameCount == dec2.frameCount,
    "frame count mismatch between single and incremental: " &
    $dec1.frameCount & " vs " & $dec2.frameCount

  echo "PASS: test_incremental_write"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

test_roundtrip_single_frame()
test_roundtrip_multi_frame()
test_seek_to_offset()
test_decompress_individual_frames()
test_empty_data()
test_seek_table_parse()
test_large_data()
test_frame_count()
test_incremental_write()

echo "All seekable_zstd tests passed."
