{.push raises: [].}

## ChunkIndexEntry encode/decode for inline chunk headers in compressed
## CTFS streams.

import ./types

proc encodeChunkHeader*(entry: ChunkIndexEntry): array[ChunkIndexEntrySize, byte] =
  ## Encode a single inline chunk header (16 bytes):
  ##   u32 compressed_size + u32 event_count + u64 first_geid
  writeU32LE(result, 0, entry.compressedSize)
  writeU32LE(result, 4, entry.eventCount)
  writeU64LE(result, 8, entry.firstGeid)

proc decodeChunkHeader*(data: openArray[byte], offset: int): ChunkIndexEntry =
  ## Decode a single inline chunk header at the given byte offset.
  var arr4: array[4, byte]
  for i in 0 ..< 4:
    arr4[i] = data[offset + i]
  result.compressedSize = fromBytesLE(uint32, arr4)
  for i in 0 ..< 4:
    arr4[i] = data[offset + 4 + i]
  result.eventCount = fromBytesLE(uint32, arr4)
  result.firstGeid = readU64LE(data, offset + 8)

proc decodeAllChunkHeaders*(data: openArray[byte]): seq[ChunkIndexEntry] =
  ## Scan a stream of inline chunk headers and return all entries.
  ## Stream layout: [Header1][Data1][Header2][Data2]...
  result = @[]
  var pos = 0
  while pos + ChunkIndexEntrySize <= data.len:
    let entry = decodeChunkHeader(data, pos)
    if entry.compressedSize == 0:
      break
    result.add(entry)
    pos += ChunkIndexEntrySize + int(entry.compressedSize)
