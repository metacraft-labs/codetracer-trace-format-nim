{.push raises: [].}

## Tests for ChunkIndexEntry encode/decode.

import codetracer_ctfs

proc test_chunk_header_roundtrip() {.raises: [].} =
  ## Test that encoding and decoding a chunk header produces the original values.
  let entry = ChunkIndexEntry(
    compressedSize: 1024,
    eventCount: 500,
    firstGeid: 42000
  )
  let encoded = encodeChunkHeader(entry)
  doAssert encoded.len == ChunkIndexEntrySize,
    "encoded size should be " & $ChunkIndexEntrySize

  let decoded = decodeChunkHeader(encoded, 0)
  doAssert decoded.compressedSize == entry.compressedSize,
    "compressedSize mismatch: " & $decoded.compressedSize
  doAssert decoded.eventCount == entry.eventCount,
    "eventCount mismatch: " & $decoded.eventCount
  doAssert decoded.firstGeid == entry.firstGeid,
    "firstGeid mismatch: " & $decoded.firstGeid
  echo "PASS: test_chunk_header_roundtrip"

proc test_chunk_header_zeros() {.raises: [].} =
  ## Test roundtrip with all-zero fields.
  let entry = ChunkIndexEntry(compressedSize: 0, eventCount: 0, firstGeid: 0)
  let encoded = encodeChunkHeader(entry)
  let decoded = decodeChunkHeader(encoded, 0)
  doAssert decoded.compressedSize == 0
  doAssert decoded.eventCount == 0
  doAssert decoded.firstGeid == 0
  echo "PASS: test_chunk_header_zeros"

proc test_chunk_header_large_values() {.raises: [].} =
  ## Test roundtrip with large values near type limits.
  let entry = ChunkIndexEntry(
    compressedSize: high(uint32),
    eventCount: high(uint32),
    firstGeid: high(uint64)
  )
  let encoded = encodeChunkHeader(entry)
  let decoded = decodeChunkHeader(encoded, 0)
  doAssert decoded.compressedSize == high(uint32),
    "compressedSize max mismatch"
  doAssert decoded.eventCount == high(uint32),
    "eventCount max mismatch"
  doAssert decoded.firstGeid == high(uint64),
    "firstGeid max mismatch"
  echo "PASS: test_chunk_header_large_values"

proc test_chunk_header_at_offset() {.raises: [].} =
  ## Test decoding a chunk header at a non-zero offset within a buffer.
  let entry = ChunkIndexEntry(
    compressedSize: 256,
    eventCount: 100,
    firstGeid: 9999
  )
  let encoded = encodeChunkHeader(entry)

  # Place encoded bytes at offset 10 in a larger buffer
  var buf = newSeq[byte](30)
  for i in 0 ..< ChunkIndexEntrySize:
    buf[10 + i] = encoded[i]

  let decoded = decodeChunkHeader(buf, 10)
  doAssert decoded.compressedSize == 256
  doAssert decoded.eventCount == 100
  doAssert decoded.firstGeid == 9999
  echo "PASS: test_chunk_header_at_offset"

proc test_decode_all_chunk_headers() {.raises: [].} =
  ## Test decodeAllChunkHeaders with a multi-chunk stream.
  ## Stream layout: [Header1][Data1][Header2][Data2][Header3][Data3]
  let entries = [
    ChunkIndexEntry(compressedSize: 8, eventCount: 10, firstGeid: 0),
    ChunkIndexEntry(compressedSize: 16, eventCount: 20, firstGeid: 10),
    ChunkIndexEntry(compressedSize: 4, eventCount: 5, firstGeid: 30),
  ]

  # Build the stream
  var stream = newSeq[byte]()
  for entry in entries:
    let hdr = encodeChunkHeader(entry)
    for b in hdr:
      stream.add(b)
    # Add fake compressed data of the specified size
    for i in 0 ..< int(entry.compressedSize):
      stream.add(byte(0xAA))

  let decoded = decodeAllChunkHeaders(stream)
  doAssert decoded.len == 3,
    "expected 3 chunks, got " & $decoded.len

  for i in 0 ..< 3:
    doAssert decoded[i].compressedSize == entries[i].compressedSize,
      "chunk " & $i & " compressedSize mismatch"
    doAssert decoded[i].eventCount == entries[i].eventCount,
      "chunk " & $i & " eventCount mismatch"
    doAssert decoded[i].firstGeid == entries[i].firstGeid,
      "chunk " & $i & " firstGeid mismatch"

  echo "PASS: test_decode_all_chunk_headers"

proc test_decode_all_chunk_headers_empty() {.raises: [].} =
  ## Test decodeAllChunkHeaders with empty input.
  let decoded = decodeAllChunkHeaders(@[])
  doAssert decoded.len == 0, "expected 0 chunks from empty input"
  echo "PASS: test_decode_all_chunk_headers_empty"

# Run all tests
test_chunk_header_roundtrip()
test_chunk_header_zeros()
test_chunk_header_large_values()
test_chunk_header_at_offset()
test_decode_all_chunk_headers()
test_decode_all_chunk_headers_empty()
