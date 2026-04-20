{.push raises: [].}

## Comprehensive tests for the seekable Zstd encoder/decoder,
## including head format (v0.1.1) and cross-compatibility validation.

import results
import stew/endians2
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
# Head format tests (Task 2: M3)
# ---------------------------------------------------------------------------

proc test_head_format_roundtrip() =
  ## Serialize a seek table in head format, parse it back, verify.
  let table = SeekTable(entries: @[
    SeekTableEntry(compressedSize: 100, decompressedSize: 500),
    SeekTableEntry(compressedSize: 200, decompressedSize: 1000),
    SeekTableEntry(compressedSize: 150, decompressedSize: 750),
  ])

  let serialized = serializeSeekTableHead(table)
  let parseRes = parseSeekTableHead(serialized)
  doAssert parseRes.isOk, "parseSeekTableHead failed: " & parseRes.error

  let parsed = parseRes.get()
  doAssert parsed.entries.len == table.entries.len,
    "head format: entry count mismatch"

  for i in 0 ..< table.entries.len:
    doAssert parsed.entries[i].compressedSize == table.entries[i].compressedSize,
      "head format: compressed size mismatch at entry " & $i
    doAssert parsed.entries[i].decompressedSize == table.entries[i].decompressedSize,
      "head format: decompressed size mismatch at entry " & $i

  echo "PASS: test_head_format_roundtrip"

proc test_head_format_empty() =
  ## Head format with zero entries.
  let table = SeekTable(entries: @[])
  let serialized = serializeSeekTableHead(table)
  let parseRes = parseSeekTableHead(serialized)
  doAssert parseRes.isOk, "head format empty parse failed: " & parseRes.error
  doAssert parseRes.get().entries.len == 0, "head format empty: expected 0 entries"
  echo "PASS: test_head_format_empty"

proc test_head_format_auto_detect() =
  ## Auto-detect: parseSeekTable should detect head format when given head data.
  let table = SeekTable(entries: @[
    SeekTableEntry(compressedSize: 42, decompressedSize: 128),
  ])

  let headData = serializeSeekTableHead(table)
  let parseRes = parseSeekTable(headData)
  doAssert parseRes.isOk, "auto-detect head format failed: " & parseRes.error
  doAssert parseRes.get().entries.len == 1, "auto-detect: expected 1 entry"
  doAssert parseRes.get().entries[0].compressedSize == 42,
    "auto-detect: compressed size mismatch"

  echo "PASS: test_head_format_auto_detect"

proc test_foot_vs_head_equivalence() =
  ## Same seek table serialized in foot and head formats should parse to equal data.
  let table = SeekTable(entries: @[
    SeekTableEntry(compressedSize: 300, decompressedSize: 1500),
    SeekTableEntry(compressedSize: 400, decompressedSize: 2000),
  ])

  let footData = serializeSeekTable(table)
  let headData = serializeSeekTableHead(table)

  let footParsed = parseSeekTable(footData)
  let headParsed = parseSeekTable(headData)

  doAssert footParsed.isOk, "foot parse failed"
  doAssert headParsed.isOk, "head parse failed"

  let ft = footParsed.get()
  let ht = headParsed.get()

  doAssert ft.entries.len == ht.entries.len, "foot vs head: entry count mismatch"
  for i in 0 ..< ft.entries.len:
    doAssert ft.entries[i].compressedSize == ht.entries[i].compressedSize,
      "foot vs head: compressed size mismatch at " & $i
    doAssert ft.entries[i].decompressedSize == ht.entries[i].decompressedSize,
      "foot vs head: decompressed size mismatch at " & $i

  echo "PASS: test_foot_vs_head_equivalence"

# ---------------------------------------------------------------------------
# Cross-compatibility validation tests (Task 3: M3)
# ---------------------------------------------------------------------------

const ZstdFrameMagic = 0xFD2FB528'u32

proc test_zstd_frame_magic() =
  ## Verify each compressed frame starts with the Zstd magic number.
  let threshold = 2000
  var enc = newSeekableZstdEncoder(frameThreshold = threshold)

  let original = makeCompressibleData(10000)
  enc.write(original)
  let compressed = enc.finish()

  let decRes = initSeekableZstdDecoder(compressed)
  doAssert decRes.isOk, "decoder init failed: " & decRes.error
  let dec = decRes.get()

  # Check each frame starts with Zstd magic
  var frameStart = 0
  for i in 0 ..< dec.frameCount:
    let entry = dec.seekTable.entries[i]
    doAssert frameStart + 4 <= compressed.len,
      "frame " & $i & " magic check: not enough data"

    var magic4: array[4, byte]
    magic4[0] = compressed[frameStart + 0]
    magic4[1] = compressed[frameStart + 1]
    magic4[2] = compressed[frameStart + 2]
    magic4[3] = compressed[frameStart + 3]
    let frameMagic = fromBytesLE(uint32, magic4)
    doAssert frameMagic == ZstdFrameMagic,
      "frame " & $i & ": expected Zstd magic 0xFD2FB528, got 0x" & $frameMagic

    frameStart += int(entry.compressedSize)

  echo "PASS: test_zstd_frame_magic"

proc test_seek_table_footer_magic() =
  ## Verify the seek table footer magic is correct in the raw output.
  let threshold = 4096
  var enc = newSeekableZstdEncoder(frameThreshold = threshold)

  let original = makeCompressibleData(20000)
  enc.write(original)
  let compressed = enc.finish()

  # Last 4 bytes should be the seekable magic number
  doAssert compressed.len >= 4, "compressed data too small"
  var magic4: array[4, byte]
  magic4[0] = compressed[compressed.len - 4]
  magic4[1] = compressed[compressed.len - 3]
  magic4[2] = compressed[compressed.len - 2]
  magic4[3] = compressed[compressed.len - 1]
  let seekMagic = fromBytesLE(uint32, magic4)
  doAssert seekMagic == SeekableMagicNumber,
    "footer seekable magic mismatch: expected 0x8F92EAB1, got 0x" & $seekMagic

  echo "PASS: test_seek_table_footer_magic"

proc test_cumulative_compressed_offsets() =
  ## Verify cumulative compressed sizes in the seek table match actual frame offsets.
  let threshold = 3000
  var enc = newSeekableZstdEncoder(frameThreshold = threshold)

  let original = makeCompressibleData(15000)
  enc.write(original)
  let compressed = enc.finish()

  let decRes = initSeekableZstdDecoder(compressed)
  doAssert decRes.isOk, "decoder init failed: " & decRes.error
  let dec = decRes.get()

  # Walk frames and verify each starts with Zstd magic at the cumulative offset
  var cumulativeOffset: uint64 = 0
  for i in 0 ..< dec.frameCount:
    let entry = dec.seekTable.entries[i]
    let offset = int(cumulativeOffset)

    # Verify we can read the Zstd frame magic at this offset
    doAssert offset + 4 <= compressed.len,
      "cumulative offset " & $offset & " + 4 exceeds data len " & $compressed.len

    var magic4: array[4, byte]
    magic4[0] = compressed[offset + 0]
    magic4[1] = compressed[offset + 1]
    magic4[2] = compressed[offset + 2]
    magic4[3] = compressed[offset + 3]
    let frameMagic = fromBytesLE(uint32, magic4)
    doAssert frameMagic == ZstdFrameMagic,
      "frame " & $i & " at offset " & $offset & ": wrong magic"

    cumulativeOffset += uint64(entry.compressedSize)

  # After all frames, cumulativeOffset should point to the seek table skippable frame
  var skipMagic4: array[4, byte]
  let skipOffset = int(cumulativeOffset)
  doAssert skipOffset + 4 <= compressed.len,
    "seek table skippable frame not found at expected offset"
  skipMagic4[0] = compressed[skipOffset + 0]
  skipMagic4[1] = compressed[skipOffset + 1]
  skipMagic4[2] = compressed[skipOffset + 2]
  skipMagic4[3] = compressed[skipOffset + 3]
  let skipMagic = fromBytesLE(uint32, skipMagic4)
  doAssert skipMagic == SkippableFrameMagic,
    "expected skippable frame magic at offset " & $skipOffset

  echo "PASS: test_cumulative_compressed_offsets"

proc test_entry_count_matches_frames() =
  ## Verify the seek table entry count matches the number of Zstd frames.
  let threshold = 1500
  var enc = newSeekableZstdEncoder(frameThreshold = threshold)

  let original = makeCompressibleData(7500)
  enc.write(original)
  let compressed = enc.finish()

  let decRes = initSeekableZstdDecoder(compressed)
  doAssert decRes.isOk, "decoder init failed: " & decRes.error
  let dec = decRes.get()

  # Count actual Zstd frames by scanning for magic numbers
  var actualFrameCount = 0
  var scanPos = 0
  while scanPos + 4 <= compressed.len:
    var magic4: array[4, byte]
    magic4[0] = compressed[scanPos + 0]
    magic4[1] = compressed[scanPos + 1]
    magic4[2] = compressed[scanPos + 2]
    magic4[3] = compressed[scanPos + 3]
    let magic = fromBytesLE(uint32, magic4)
    if magic == ZstdFrameMagic:
      actualFrameCount += 1
      # Skip past this frame using the seek table entry
      if actualFrameCount <= dec.frameCount:
        scanPos += int(dec.seekTable.entries[actualFrameCount - 1].compressedSize)
      else:
        break
    elif magic == SkippableFrameMagic:
      break  # Reached the seek table
    else:
      scanPos += 1

  doAssert actualFrameCount == dec.frameCount,
    "frame count mismatch: seek table says " & $dec.frameCount &
    " but found " & $actualFrameCount & " Zstd frames"

  echo "PASS: test_entry_count_matches_frames"

proc test_write_read_roundtrip_validation() =
  ## Write with Nim encoder, read back with Nim decoder, verify full roundtrip
  ## including raw byte-level format validation.
  let threshold = 2048
  var enc = newSeekableZstdEncoder(frameThreshold = threshold)

  let original = makeKnownData(8192)
  enc.write(original)
  let compressed = enc.finish()

  # Parse the raw bytes manually to verify format structure
  # 1) First bytes should be Zstd frame magic (first compressed frame)
  doAssert compressed.len >= 4, "output too small"
  var magic4: array[4, byte]
  magic4[0] = compressed[0]
  magic4[1] = compressed[1]
  magic4[2] = compressed[2]
  magic4[3] = compressed[3]
  doAssert fromBytesLE(uint32, magic4) == ZstdFrameMagic,
    "first bytes should be Zstd frame magic"

  # 2) Last 4 bytes should be seekable magic
  magic4[0] = compressed[compressed.len - 4]
  magic4[1] = compressed[compressed.len - 3]
  magic4[2] = compressed[compressed.len - 2]
  magic4[3] = compressed[compressed.len - 1]
  doAssert fromBytesLE(uint32, magic4) == SeekableMagicNumber,
    "last 4 bytes should be seekable magic"

  # 3) Full decode should match
  let decRes = initSeekableZstdDecoder(compressed)
  doAssert decRes.isOk, "decoder init failed: " & decRes.error
  let dec = decRes.get()
  let decompRes = dec.decompressAll()
  doAssert decompRes.isOk, "decompressAll failed: " & decompRes.error
  doAssert decompRes.get() == original, "roundtrip data mismatch"

  echo "PASS: test_write_read_roundtrip_validation"

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

# Head format tests
test_head_format_roundtrip()
test_head_format_empty()
test_head_format_auto_detect()
test_foot_vs_head_equivalence()

# Cross-compatibility validation tests
test_zstd_frame_magic()
test_seek_table_footer_magic()
test_cumulative_compressed_offsets()
test_entry_count_matches_frames()
test_write_read_roundtrip_validation()

echo "All seekable_zstd tests passed."
