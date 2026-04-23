## Tests for the high-level TraceWriter API.

import std/os
import std/json
import results
import codetracer_ctfs
import codetracer_trace_writer

# ---------------------------------------------------------------------------
# Helpers: read back internal files from raw CTFS bytes
# ---------------------------------------------------------------------------

proc findInternalFile(data: openArray[byte], name: string): (uint64, uint64) =
  ## Search file entries in block 0 for the given name. Returns (size, mapBlock).
  ## Returns (0, 0) if not found.
  let encoded = base40Encode(name)
  let maxEntries = readU64LE(data, 12)  # maxRootEntries at offset 12
  for i in 0 ..< int(maxEntries):
    let off = HeaderSize + ExtHeaderSize + i * FileEntrySize
    if off + FileEntrySize > data.len:
      break
    let entrySize = readU64LE(data, off)
    let entryMap = readU64LE(data, off + 8)
    let entryName = readU64LE(data, off + 16)
    if entryName == encoded:
      return (entrySize, entryMap)
  (0'u64, 0'u64)

proc readInternalFileData(data: openArray[byte], name: string,
                          blockSize: uint32 = DefaultBlockSize): seq[byte] =
  ## Read the full contents of an internal file from raw CTFS bytes.
  let (fileSize, mapBlock) = findInternalFile(data, name)
  if fileSize == 0 and mapBlock == 0:
    return @[]

  result = newSeq[byte](int(fileSize))
  let usable = uint64(blockSize) div 8 - 1

  var remaining = int(fileSize)
  var destPos = 0
  var blockIdx: uint64 = 0

  # Simple level-1 only reader (sufficient for test sizes)
  while remaining > 0:
    var dataBlock: uint64
    if blockIdx < usable:
      # Level 1: direct pointer
      let off = int(mapBlock) * int(blockSize) + int(blockIdx) * 8
      dataBlock = readU64LE(data, off)
    else:
      # Level 2+ needed — for tests, files should fit in level 1
      break

    let blockOff = int(dataBlock) * int(blockSize)
    let toCopy = min(remaining, int(blockSize))
    for i in 0 ..< toCopy:
      result[destPos + i] = data[blockOff + i]
    destPos += toCopy
    remaining -= toCopy
    blockIdx += 1

proc readInternalFileStr(data: openArray[byte], name: string): string =
  ## Read an internal file as a string.
  let bytes = readInternalFileData(data, name)
  result = newString(bytes.len)
  for i in 0 ..< bytes.len:
    result[i] = char(bytes[i])

proc getTmpPath(name: string): string =
  getTempDir() / name

proc cleanupFile(path: string) =
  try:
    removeFile(path)
  except OSError:
    discard

# ---------------------------------------------------------------------------
# test_basic_trace
# ---------------------------------------------------------------------------

proc test_basic_trace() =
  let path = getTmpPath("test_basic_trace.ct")
  cleanupFile(path)

  var writerRes = newTraceWriter(path, "test_prog", @["arg1", "arg2"])
  doAssert writerRes.isOk, "newTraceWriter failed: " & writerRes.error
  var w = writerRes.get()

  # Write various event types
  doAssert w.writeStep(0, 1).isOk
  doAssert w.writePath("/src/main.nim").isOk
  doAssert w.writeFunction(0, 1, "main").isOk
  doAssert w.writeCall(0).isOk
  doAssert w.writeReturn().isOk

  let closeRes = w.close()
  doAssert closeRes.isOk, "close failed: " & closeRes.error

  # Verify .ct file exists and has CTFS magic
  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk, "failed to read .ct file: " & readRes.error
  let data = readRes.get()
  doAssert hasCtfsMagic(data), "CTFS magic not found"
  doAssert data.len > int(DefaultBlockSize), "file too small"

  cleanupFile(path)
  echo "PASS: test_basic_trace"

# ---------------------------------------------------------------------------
# test_metadata
# ---------------------------------------------------------------------------

proc test_metadata() =
  let path = getTmpPath("test_metadata.ct")
  cleanupFile(path)

  var writerRes = newTraceWriter(path, "my_program", @["--verbose", "input.txt"],
                                  workdir = "/home/user/project")
  doAssert writerRes.isOk, "newTraceWriter failed: " & writerRes.error
  var w = writerRes.get()

  doAssert w.writeStep(0, 1).isOk
  let closeRes = w.close()
  doAssert closeRes.isOk, "close failed: " & closeRes.error

  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk, "failed to read .ct file"
  let data = readRes.get()

  let metaStr = readInternalFileStr(data, "meta.json")
  doAssert metaStr.len > 0, "meta.json is empty"

  try:
    let node = parseJson(metaStr)
    doAssert node["program"].getStr() == "my_program",
      "program mismatch: " & node["program"].getStr()
    doAssert node["args"].len == 2, "args length mismatch"
    doAssert node["args"][0].getStr() == "--verbose"
    doAssert node["args"][1].getStr() == "input.txt"
    doAssert node["workdir"].getStr() == "/home/user/project",
      "workdir mismatch: " & node["workdir"].getStr()
  except JsonParsingError:
    doAssert false, "meta.json is not valid JSON: " & metaStr
  except KeyError:
    doAssert false, "meta.json missing expected key"

  cleanupFile(path)
  echo "PASS: test_metadata"

# ---------------------------------------------------------------------------
# test_paths
# ---------------------------------------------------------------------------

proc test_paths() =
  let path = getTmpPath("test_paths.ct")
  cleanupFile(path)

  var writerRes = newTraceWriter(path, "test", @[])
  doAssert writerRes.isOk
  var w = writerRes.get()

  let testPaths = @[
    "/src/main.nim",
    "/src/utils.nim",
    "/src/types.nim",
    "/tests/test_main.nim",
    "/lib/helpers.nim",
  ]
  for p in testPaths:
    doAssert w.writePath(p).isOk

  doAssert w.close().isOk

  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk
  let data = readRes.get()

  let pathsStr = readInternalFileStr(data, "paths.json")
  doAssert pathsStr.len > 0, "paths.json is empty"

  try:
    let arr = parseJson(pathsStr)
    doAssert arr.len == 5, "paths count mismatch: " & $arr.len
    for i in 0 ..< 5:
      doAssert arr[i].getStr() == testPaths[i],
        "path mismatch at " & $i & ": " & arr[i].getStr()
  except JsonParsingError:
    doAssert false, "paths.json is not valid JSON"
  except KeyError:
    doAssert false, "paths.json missing expected element"

  cleanupFile(path)
  echo "PASS: test_paths"

# ---------------------------------------------------------------------------
# test_events_fmt
# ---------------------------------------------------------------------------

proc test_events_fmt() =
  let path = getTmpPath("test_events_fmt.ct")
  cleanupFile(path)

  var writerRes = newTraceWriter(path, "test", @[])
  doAssert writerRes.isOk
  var w = writerRes.get()
  doAssert w.writeStep(0, 1).isOk
  doAssert w.close().isOk

  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk
  let data = readRes.get()

  let fmtStr = readInternalFileStr(data, "events.fmt")
  doAssert fmtStr == "split-binary",
    "events.fmt should be 'split-binary', got: '" & fmtStr & "'"

  cleanupFile(path)
  echo "PASS: test_events_fmt"

# ---------------------------------------------------------------------------
# test_large_trace
# ---------------------------------------------------------------------------

proc test_large_trace() =
  let path = getTmpPath("test_large_trace.ct")
  cleanupFile(path)

  var writerRes = newTraceWriter(path, "large_test", @[])
  doAssert writerRes.isOk
  var w = writerRes.get()

  for i in 0 ..< 50_000:
    doAssert w.writeStep(uint64(i mod 10), int64(i mod 500)).isOk

  doAssert w.close().isOk

  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk
  let data = readRes.get()

  doAssert hasCtfsMagic(data)
  # Compressed data should be significantly smaller than raw encoding
  # 50k step events * 17 bytes each = ~850KB raw; compressed should be much less
  # But CTFS has block overhead too. Just check it's reasonable.
  doAssert data.len > 0, "file is empty"
  doAssert data.len < 50_000 * 17 + 100_000,
    "file seems too large for compressed data: " & $data.len

  cleanupFile(path)
  echo "PASS: test_large_trace"

# ---------------------------------------------------------------------------
# test_chunk_compression
# ---------------------------------------------------------------------------

proc test_chunk_compression() =
  ## Write events exceeding chunk threshold and verify chunked format.
  let path = getTmpPath("test_chunk_compression.ct")
  cleanupFile(path)

  let chunkThreshold = 100
  var writerRes = newTraceWriter(path, "chunk_test", @[],
                                  chunkThreshold = chunkThreshold)
  doAssert writerRes.isOk
  var w = writerRes.get()

  # Write 350 events — should produce 3 full chunks + 1 partial
  for i in 0 ..< 350:
    doAssert w.writeStep(uint64(i mod 5), int64(i)).isOk

  doAssert w.close().isOk

  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk
  let data = readRes.get()

  # Read events.log and verify it contains multiple chunk headers
  let eventsData = readInternalFileData(data, "events.log")
  doAssert eventsData.len > 0, "events.log is empty"

  # Parse chunk headers — each chunk starts with a 16-byte header
  var chunks = decodeAllChunkHeaders(eventsData)
  doAssert chunks.len >= 3, "expected at least 3 chunks, got: " & $chunks.len

  # Verify first chunk has correct event count
  doAssert chunks[0].eventCount == uint32(chunkThreshold),
    "first chunk event count: " & $chunks[0].eventCount

  # Verify all chunks have non-zero compressed size
  for i, chunk in chunks:
    doAssert chunk.compressedSize > 0,
      "chunk " & $i & " has zero compressed size"

  cleanupFile(path)
  echo "PASS: test_chunk_compression"

# ---------------------------------------------------------------------------
# test_roundtrip_events
# ---------------------------------------------------------------------------

proc test_roundtrip_events() =
  ## Write known events, close, reopen, decompress, decode, verify match.
  let path = getTmpPath("test_roundtrip_events.ct")
  cleanupFile(path)

  # Use a small chunk threshold to test chunked compression
  let chunkThreshold = 10
  var writerRes = newTraceWriter(path, "roundtrip_test", @[],
                                  chunkThreshold = chunkThreshold)
  doAssert writerRes.isOk
  var w = writerRes.get()

  var originalEvents: seq[TraceLowLevelEvent]

  # Write a variety of events
  originalEvents.add(TraceLowLevelEvent(kind: tlePath, path: "/src/main.nim"))
  originalEvents.add(TraceLowLevelEvent(kind: tlePath, path: "/src/lib.nim"))
  originalEvents.add(TraceLowLevelEvent(kind: tleFunction,
    functionRecord: FunctionRecord(pathId: PathId(0), line: Line(1), name: "main")))
  originalEvents.add(TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(0), line: Line(1))))
  originalEvents.add(TraceLowLevelEvent(kind: tleCall,
    callRecord: codetracer_trace_types.CallRecord(functionId: FunctionId(0), args: @[])))
  originalEvents.add(TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(0), line: Line(5))))
  originalEvents.add(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(1),
      value: ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(7)))))
  originalEvents.add(TraceLowLevelEvent(kind: tleReturn,
    returnRecord: ReturnRecord(returnValue: NoneValue)))

  # Add more events to cross chunk boundaries
  for i in 0 ..< 25:
    originalEvents.add(TraceLowLevelEvent(kind: tleStep,
      step: StepRecord(pathId: PathId(uint64(i mod 2)), line: Line(int64(i + 10)))))

  for event in originalEvents:
    doAssert w.writeEvent(event).isOk

  doAssert w.close().isOk

  # Read back
  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk
  let data = readRes.get()

  let eventsData = readInternalFileData(data, "events.log")
  doAssert eventsData.len > 0, "events.log is empty"

  # Decode all chunks
  var allDecodedEvents: seq[TraceLowLevelEvent]
  var pos = 0
  while pos + ChunkIndexEntrySize <= eventsData.len:
    let chunk = decodeChunkHeader(eventsData, pos)
    if chunk.compressedSize == 0:
      break
    pos += ChunkIndexEntrySize

    # Decompress chunk
    let compressedData = eventsData[pos ..< pos + int(chunk.compressedSize)]
    let decompSize = ZSTD_getFrameContentSize(
      unsafeAddr compressedData[0], csize_t(compressedData.len))
    doAssert decompSize != ZSTD_CONTENTSIZE_ERROR,
      "failed to get decompressed size"

    var decompressed = newSeq[byte](int(decompSize))
    let actualSize = ZSTD_decompress(
      addr decompressed[0], csize_t(decompressed.len),
      unsafeAddr compressedData[0], csize_t(compressedData.len))
    doAssert ZSTD_isError(actualSize) == 0, "decompression failed"
    decompressed.setLen(int(actualSize))

    # Decode events from decompressed data
    let decoded = decodeAllEvents(decompressed)
    doAssert decoded.isOk, "failed to decode events"
    for event in decoded.get():
      allDecodedEvents.add(event)

    pos += int(chunk.compressedSize)

  # Verify all events match
  doAssert allDecodedEvents.len == originalEvents.len,
    "event count mismatch: " & $allDecodedEvents.len & " vs " & $originalEvents.len

  for i in 0 ..< originalEvents.len:
    doAssert allDecodedEvents[i] == originalEvents[i],
      "event mismatch at index " & $i

  cleanupFile(path)
  echo "PASS: test_roundtrip_events"

# ---------------------------------------------------------------------------
# test_ctfs_structure
# ---------------------------------------------------------------------------

proc test_ctfs_structure() =
  ## Verify the .ct file has all 4 internal files with correct names.
  let path = getTmpPath("test_ctfs_structure.ct")
  cleanupFile(path)

  var writerRes = newTraceWriter(path, "test", @[])
  doAssert writerRes.isOk
  var w = writerRes.get()
  doAssert w.writeStep(0, 1).isOk
  doAssert w.close().isOk

  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk
  let data = readRes.get()

  # Check all 4 expected internal files exist
  let expectedFiles = ["events.log", "events.fmt", "meta.json", "paths.json"]
  for name in expectedFiles:
    let (fileSize, mapBlock) = findInternalFile(data, name)
    doAssert mapBlock != 0,
      "internal file not found: " & name
    # events.log, meta.json, paths.json should have non-zero sizes
    # (events.fmt is small but non-zero)
    doAssert fileSize > 0,
      "internal file has zero size: " & name

  # Verify base40 encoding produces expected values
  let eventsLogEncoded = base40Encode("events.log")
  let eventsFmtEncoded = base40Encode("events.fmt")
  let metaJsonEncoded = base40Encode("meta.json")
  let pathsJsonEncoded = base40Encode("paths.json")

  # Verify roundtrip of names
  doAssert base40Decode(eventsLogEncoded) == "events.log"
  doAssert base40Decode(eventsFmtEncoded) == "events.fmt"
  doAssert base40Decode(metaJsonEncoded) == "meta.json"
  doAssert base40Decode(pathsJsonEncoded) == "paths.json"

  cleanupFile(path)
  echo "PASS: test_ctfs_structure"

# ---------------------------------------------------------------------------
# test_rust_reader_format
# ---------------------------------------------------------------------------

proc test_rust_reader_format() =
  ## Verify the .ct file structure matches what Rust codetracer_trace_reader
  ## expects: CTFS magic, version, correct internal file names.
  let path = getTmpPath("test_rust_reader_format.ct")
  cleanupFile(path)

  var writerRes = newTraceWriter(path, "test_prog", @["a1", "a2"],
                                  workdir = "/tmp")
  doAssert writerRes.isOk
  var w = writerRes.get()
  doAssert w.writePath("/src/main.rs").isOk
  doAssert w.writeStep(0, 1).isOk
  doAssert w.close().isOk

  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk
  let data = readRes.get()

  # 1. CTFS magic bytes
  doAssert data.len >= 16, "file too small"
  doAssert data[0] == 0xC0'u8
  doAssert data[1] == 0xDE'u8
  doAssert data[2] == 0x72'u8
  doAssert data[3] == 0xAC'u8
  doAssert data[4] == 0xE2'u8

  # 2. Version byte = 3
  doAssert data[5] == 3, "version should be 3, got: " & $data[5]

  # 3. Block size = 4096 at offset 8
  var bsArr: array[4, byte]
  for i in 0..3:
    bsArr[i] = data[8 + i]
  let blockSize = fromBytesLE(uint32, bsArr)
  doAssert blockSize == 4096, "block size should be 4096"

  # 4. events.fmt contains "split-binary"
  let fmtStr = readInternalFileStr(data, "events.fmt")
  doAssert fmtStr == "split-binary"

  # 5. meta.json is valid JSON with expected fields
  let metaStr = readInternalFileStr(data, "meta.json")
  try:
    let meta = parseJson(metaStr)
    doAssert meta.hasKey("program")
    doAssert meta.hasKey("args")
    doAssert meta.hasKey("workdir")
  except JsonParsingError:
    doAssert false, "meta.json is not valid JSON"
  except KeyError:
    doAssert false, "meta.json missing expected key"

  # 6. paths.json is a JSON array
  let pathsStr = readInternalFileStr(data, "paths.json")
  try:
    let paths = parseJson(pathsStr)
    doAssert paths.kind == JArray
  except JsonParsingError:
    doAssert false, "paths.json is not valid JSON"

  # 7. events.log has chunk header structure
  let eventsData = readInternalFileData(data, "events.log")
  doAssert eventsData.len > 0
  # First 16 bytes should be a valid chunk header
  doAssert eventsData.len >= ChunkIndexEntrySize
  let firstChunk = decodeChunkHeader(eventsData, 0)
  doAssert firstChunk.compressedSize > 0, "first chunk compressed size is 0"
  doAssert firstChunk.eventCount > 0, "first chunk event count is 0"
  doAssert firstChunk.firstGeid == 0, "first chunk should start at GEID 0"

  cleanupFile(path)
  echo "PASS: test_rust_reader_format"

# ---------------------------------------------------------------------------
# Run all tests
# ---------------------------------------------------------------------------

test_basic_trace()
test_metadata()
test_paths()
test_events_fmt()
test_large_trace()
test_chunk_compression()
test_roundtrip_events()
test_ctfs_structure()
test_rust_reader_format()
echo "ALL PASS: test_trace_writer"
