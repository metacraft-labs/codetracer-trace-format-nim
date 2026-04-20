## Cross-language compatibility test.
##
## Verifies that .ct files produced by the Nim writer have the exact
## byte layout the Rust `codetracer_trace_reader` expects:
##
##   - CTFS magic, version, block size match Rust defaults
##   - Internal file names match base40 encoding expected by Rust
##   - events.fmt = "split-binary"
##   - meta.json / paths.json are valid JSON with required fields
##   - Chunk headers in events.log use the expected 16-byte format
##   - Split-binary tag bytes match the Rust enum discriminant ordering
##   - Fixed-size events have exact byte layouts matching Rust spec
##   - CBOR-payload events have correct envelope structure

import std/[os, json, strutils]
import results
import codetracer_ctfs
import codetracer_trace_writer
import codetracer_trace_writer/split_binary

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc findInternalFile(data: openArray[byte], name: string): (uint64, uint64) =
  ## Search file entries in block 0 for the given name. Returns (size, mapBlock).
  let encoded = base40Encode(name)
  let maxEntries = readU64LE(data, 12)
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
  let (fileSize, mapBlock) = findInternalFile(data, name)
  if fileSize == 0 and mapBlock == 0:
    return @[]
  result = newSeq[byte](int(fileSize))
  let usable = uint64(blockSize) div 8 - 1
  var remaining = int(fileSize)
  var destPos = 0
  var blockIdx: uint64 = 0
  while remaining > 0:
    var dataBlock: uint64
    if blockIdx < usable:
      let off = int(mapBlock) * int(blockSize) + int(blockIdx) * 8
      dataBlock = readU64LE(data, off)
    else:
      break
    let blockOff = int(dataBlock) * int(blockSize)
    let toCopy = min(remaining, int(blockSize))
    for i in 0 ..< toCopy:
      result[destPos + i] = data[blockOff + i]
    destPos += toCopy
    remaining -= toCopy
    blockIdx += 1

proc readInternalFileStr(data: openArray[byte], name: string): string =
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

proc readU16LE(data: openArray[byte], offset: int): uint16 =
  uint16(data[offset]) or (uint16(data[offset + 1]) shl 8)

proc readU32LELocal(data: openArray[byte], offset: int): uint32 =
  uint32(data[offset]) or
    (uint32(data[offset + 1]) shl 8) or
    (uint32(data[offset + 2]) shl 16) or
    (uint32(data[offset + 3]) shl 24)

proc readU64LELocal(data: openArray[byte], offset: int): uint64 =
  uint64(data[offset]) or
    (uint64(data[offset + 1]) shl 8) or
    (uint64(data[offset + 2]) shl 16) or
    (uint64(data[offset + 3]) shl 24) or
    (uint64(data[offset + 4]) shl 32) or
    (uint64(data[offset + 5]) shl 40) or
    (uint64(data[offset + 6]) shl 48) or
    (uint64(data[offset + 7]) shl 56)

# ---------------------------------------------------------------------------
# Test: CTFS container structure matches Rust expectations
# ---------------------------------------------------------------------------

proc test_ctfs_magic_version_blocksize() =
  ## Verify CTFS magic bytes, version, and block size match Rust defaults.
  let path = getTmpPath("test_cross_compat_structure.ct")
  cleanupFile(path)

  var writerRes = newTraceWriter(path, "cross_test", @["--arg1"],
                                  workdir = "/tmp/test")
  doAssert writerRes.isOk, "newTraceWriter failed"
  var w = writerRes.get()
  doAssert w.writeStep(0, 1).isOk
  doAssert w.close().isOk

  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk, "failed to read .ct file"
  let data = readRes.get()

  # CTFS magic: [0xC0, 0xDE, 0x72, 0xAC, 0xE2] (5 bytes)
  # Rust: pub const CTFS_MAGIC: [u8; 5] = [0xC0, 0xDE, 0x72, 0xAC, 0xE2];
  doAssert data[0] == 0xC0'u8, "magic[0] mismatch"
  doAssert data[1] == 0xDE'u8, "magic[1] mismatch"
  doAssert data[2] == 0x72'u8, "magic[2] mismatch"
  doAssert data[3] == 0xAC'u8, "magic[3] mismatch"
  doAssert data[4] == 0xE2'u8, "magic[4] mismatch"

  # Version: 3
  # Rust: pub const CTFS_VERSION: u8 = 3;
  doAssert data[5] == 3'u8, "version should be 3, got: " & $data[5]

  # Compression method at offset 6: 0 = None at container level
  # (Zstd compression is applied per-chunk within events.log, not at CTFS level)
  doAssert data[6] == 0'u8, "compression should be 0 (None), got: " & $data[6]

  # Encryption method at offset 7: 0 = None
  doAssert data[7] == 0'u8, "encryption should be 0 (None), got: " & $data[7]

  # Block size at offset 8: 4096 as u32 LE
  # Rust: pub const DEFAULT_BLOCK_SIZE: u32 = 4096;
  let blockSize = readU32LELocal(data, 8)
  doAssert blockSize == 4096, "block size should be 4096, got: " & $blockSize

  cleanupFile(path)
  echo "PASS: test_ctfs_magic_version_blocksize"

# ---------------------------------------------------------------------------
# Test: Base40 internal file names match Rust expectations
# ---------------------------------------------------------------------------

proc test_base40_file_names() =
  ## Verify base40 encoding of internal file names matches Rust's base40.
  ## Rust expects: events.log, events.fmt, meta.json, paths.json
  let path = getTmpPath("test_cross_compat_names.ct")
  cleanupFile(path)

  var writerRes = newTraceWriter(path, "test", @[])
  doAssert writerRes.isOk
  var w = writerRes.get()
  doAssert w.writeStep(0, 1).isOk
  doAssert w.close().isOk

  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk
  let data = readRes.get()

  # All 4 internal files must be present and findable by base40 name
  let expectedFiles = ["events.log", "events.fmt", "meta.json", "paths.json"]
  for name in expectedFiles:
    let (fileSize, mapBlock) = findInternalFile(data, name)
    doAssert mapBlock != 0,
      "internal file not found via base40: " & name
    doAssert fileSize > 0,
      "internal file has zero size: " & name

  # Verify base40 roundtrip for all names
  for name in expectedFiles:
    let encoded = base40Encode(name)
    let decoded = base40Decode(encoded)
    doAssert decoded == name,
      "base40 roundtrip failed for '" & name & "': got '" & decoded & "'"

  cleanupFile(path)
  echo "PASS: test_base40_file_names"

# ---------------------------------------------------------------------------
# Test: events.fmt = "split-binary"
# ---------------------------------------------------------------------------

proc test_events_fmt_split_binary() =
  ## Rust reader expects events.fmt to contain exactly "split-binary".
  let path = getTmpPath("test_cross_compat_fmt.ct")
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
  echo "PASS: test_events_fmt_split_binary"

# ---------------------------------------------------------------------------
# Test: meta.json has required fields for Rust reader
# ---------------------------------------------------------------------------

proc test_meta_json_structure() =
  ## Rust reader expects meta.json with: program, args, workdir fields.
  let path = getTmpPath("test_cross_compat_meta.ct")
  cleanupFile(path)

  var writerRes = newTraceWriter(path, "my_program", @["--verbose", "input.txt"],
                                  workdir = "/home/user/project")
  doAssert writerRes.isOk
  var w = writerRes.get()
  doAssert w.writeStep(0, 1).isOk
  doAssert w.close().isOk

  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk
  let data = readRes.get()

  let metaStr = readInternalFileStr(data, "meta.json")
  doAssert metaStr.len > 0, "meta.json is empty"

  try:
    let meta = parseJson(metaStr)
    # Required by Rust: program field (string)
    doAssert meta.hasKey("program"), "meta.json missing 'program'"
    doAssert meta["program"].kind == JString
    doAssert meta["program"].getStr() == "my_program"
    # Required by Rust: args field (array of strings)
    doAssert meta.hasKey("args"), "meta.json missing 'args'"
    doAssert meta["args"].kind == JArray
    doAssert meta["args"].len == 2
    doAssert meta["args"][0].getStr() == "--verbose"
    doAssert meta["args"][1].getStr() == "input.txt"
    # Required by Rust: workdir field (string)
    doAssert meta.hasKey("workdir"), "meta.json missing 'workdir'"
    doAssert meta["workdir"].kind == JString
    doAssert meta["workdir"].getStr() == "/home/user/project"
  except JsonParsingError:
    doAssert false, "meta.json is not valid JSON: " & metaStr
  except KeyError:
    doAssert false, "meta.json missing expected key"

  cleanupFile(path)
  echo "PASS: test_meta_json_structure"

# ---------------------------------------------------------------------------
# Test: paths.json is a JSON array of strings
# ---------------------------------------------------------------------------

proc test_paths_json_structure() =
  ## Rust reader expects paths.json to be a JSON array of path strings.
  let path = getTmpPath("test_cross_compat_paths.ct")
  cleanupFile(path)

  var writerRes = newTraceWriter(path, "test", @[])
  doAssert writerRes.isOk
  var w = writerRes.get()
  doAssert w.writePath("/src/main.rs").isOk
  doAssert w.writePath("/src/lib.rs").isOk
  doAssert w.writeStep(0, 1).isOk
  doAssert w.close().isOk

  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk
  let data = readRes.get()

  let pathsStr = readInternalFileStr(data, "paths.json")
  doAssert pathsStr.len > 0, "paths.json is empty"

  try:
    let paths = parseJson(pathsStr)
    doAssert paths.kind == JArray, "paths.json should be a JSON array"
    doAssert paths.len == 2, "expected 2 paths, got: " & $paths.len
    doAssert paths[0].kind == JString
    doAssert paths[0].getStr() == "/src/main.rs"
    doAssert paths[1].getStr() == "/src/lib.rs"
  except JsonParsingError:
    doAssert false, "paths.json is not valid JSON"

  cleanupFile(path)
  echo "PASS: test_paths_json_structure"

# ---------------------------------------------------------------------------
# Test: Chunk header format (16 bytes)
# ---------------------------------------------------------------------------

proc test_chunk_header_format() =
  ## Rust reader expects chunk headers in events.log:
  ##   [4 bytes compressed_size LE][4 bytes event_count LE][8 bytes first_geid LE]
  ## Total: 16 bytes per chunk header (ChunkIndexEntrySize).
  let path = getTmpPath("test_cross_compat_chunks.ct")
  cleanupFile(path)

  var writerRes = newTraceWriter(path, "test", @[],
                                  chunkThreshold = 50)
  doAssert writerRes.isOk
  var w = writerRes.get()

  # Write 150 step events to get 3 chunks
  for i in 0 ..< 150:
    doAssert w.writeStep(uint64(i mod 5), int64(i)).isOk

  doAssert w.close().isOk

  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk
  let data = readRes.get()

  let eventsData = readInternalFileData(data, "events.log")
  doAssert eventsData.len > 0, "events.log is empty"

  # Verify ChunkIndexEntrySize = 16 (must match Rust)
  doAssert ChunkIndexEntrySize == 16,
    "ChunkIndexEntrySize should be 16, got: " & $ChunkIndexEntrySize

  # Parse first chunk header manually to verify byte layout
  doAssert eventsData.len >= 16, "events.log too small for chunk header"

  # Bytes 0..3: compressed_size (u32 LE)
  let compressedSize = readU32LELocal(eventsData, 0)
  doAssert compressedSize > 0, "first chunk compressed_size is 0"

  # Bytes 4..7: event_count (u32 LE)
  let eventCount = readU32LELocal(eventsData, 4)
  doAssert eventCount == 50, "first chunk event_count should be 50, got: " & $eventCount

  # Bytes 8..15: first_geid (u64 LE)
  let firstGeid = readU64LELocal(eventsData, 8)
  doAssert firstGeid == 0, "first chunk first_geid should be 0, got: " & $firstGeid

  # Verify second chunk starts at offset 16 + compressedSize
  let chunk2Offset = 16 + int(compressedSize)
  doAssert eventsData.len >= chunk2Offset + 16,
    "events.log too small for second chunk header"

  let eventCount2 = readU32LELocal(eventsData, chunk2Offset + 4)
  doAssert eventCount2 == 50,
    "second chunk event_count should be 50, got: " & $eventCount2

  let firstGeid2 = readU64LELocal(eventsData, chunk2Offset + 8)
  doAssert firstGeid2 == 50,
    "second chunk first_geid should be 50, got: " & $firstGeid2

  cleanupFile(path)
  echo "PASS: test_chunk_header_format"

# ---------------------------------------------------------------------------
# Test: Split-binary tag bytes match Rust enum discriminant ordering
# ---------------------------------------------------------------------------

proc test_split_binary_tag_ordering() =
  ## Verify that split-binary tag bytes match the Rust enum ordering:
  ##   0 = Step, 1 = Path, 2 = VariableName, 3 = Variable,
  ##   4 = Type, 5 = Value, 6 = Function, 7 = Call, 8 = Return,
  ##   9 = Event, 10 = Asm, 11 = BindVariable, 12 = Assignment,
  ##   13 = DropVariables, 14 = CompoundValue, 15 = CellValue,
  ##   16 = AssignCompoundItem, 17 = AssignCell, 18 = VariableCell,
  ##   19 = DropVariable, 20 = ThreadStart, 21 = ThreadExit,
  ##   22 = ThreadSwitch, 23 = DropLastStep
  var enc = SplitBinaryEncoder.init()

  # Step: tag 0
  enc.encodeEvent(TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(42), line: Line(100))))
  doAssert enc.getBytes()[0] == 0'u8, "Step tag should be 0"

  # Path: tag 1
  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tlePath, path: "/test"))
  doAssert enc.getBytes()[0] == 1'u8, "Path tag should be 1"

  # Value: tag 5
  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(1),
      value: ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(7)))))
  doAssert enc.getBytes()[0] == 5'u8, "Value tag should be 5"

  # Function: tag 6
  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleFunction,
    functionRecord: FunctionRecord(pathId: PathId(0), line: Line(1), name: "main")))
  doAssert enc.getBytes()[0] == 6'u8, "Function tag should be 6"

  # Call: tag 7
  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleCall,
    callRecord: CallRecord(functionId: FunctionId(0), args: @[])))
  doAssert enc.getBytes()[0] == 7'u8, "Call tag should be 7"

  # Return: tag 8
  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleReturn,
    returnRecord: ReturnRecord(returnValue: NoneValue)))
  doAssert enc.getBytes()[0] == 8'u8, "Return tag should be 8"

  # BindVariable: tag 11
  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleBindVariable,
    bindVar: BindVariableRecord(variableId: VariableId(1), place: Place(0))))
  doAssert enc.getBytes()[0] == 11'u8, "BindVariable tag should be 11"

  # ThreadStart: tag 20
  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleThreadStart,
    threadStartId: ThreadId(1)))
  doAssert enc.getBytes()[0] == 20'u8, "ThreadStart tag should be 20"

  # ThreadSwitch: tag 22
  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleThreadSwitch,
    threadSwitchId: ThreadId(2)))
  doAssert enc.getBytes()[0] == 22'u8, "ThreadSwitch tag should be 22"

  # DropLastStep: tag 23
  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleDropLastStep))
  doAssert enc.getBytes()[0] == 23'u8, "DropLastStep tag should be 23"

  echo "PASS: test_split_binary_tag_ordering"

# ---------------------------------------------------------------------------
# Test: Fixed-size event byte layouts match Rust spec
# ---------------------------------------------------------------------------

proc test_fixed_size_event_layouts() =
  ## Verify exact byte layouts for fixed-size events:
  ##   Step: [0x00][8 bytes path_id LE][8 bytes line LE] = 17 bytes
  ##   DropLastStep: [0x17] = 1 byte
  ##   ThreadStart: [0x14][8 bytes thread_id LE] = 9 bytes
  ##   ThreadSwitch: [0x16][8 bytes thread_id LE] = 9 bytes
  ##   BindVariable: [0x0B][8 bytes variable_id LE][8 bytes place LE] = 17 bytes
  var enc = SplitBinaryEncoder.init()

  # --- Step: 17 bytes ---
  enc.encodeEvent(TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(7), line: Line(42))))
  let stepBytes = enc.getBytes()
  doAssert stepBytes.len == 17,
    "Step should be 17 bytes, got: " & $stepBytes.len
  doAssert stepBytes[0] == 0x00'u8, "Step tag"
  # path_id = 7 as u64 LE
  doAssert readU64LELocal(stepBytes, 1) == 7'u64, "Step path_id"
  # line = 42 as i64 LE (same bits as u64 for positive values)
  doAssert readU64LELocal(stepBytes, 9) == 42'u64, "Step line"

  # --- DropLastStep: 1 byte ---
  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleDropLastStep))
  let dropBytes = enc.getBytes()
  doAssert dropBytes.len == 1,
    "DropLastStep should be 1 byte, got: " & $dropBytes.len
  doAssert dropBytes[0] == 0x17'u8, "DropLastStep tag (23 = 0x17)"

  # --- ThreadStart: 9 bytes ---
  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleThreadStart,
    threadStartId: ThreadId(5)))
  let tsBytes = enc.getBytes()
  doAssert tsBytes.len == 9,
    "ThreadStart should be 9 bytes, got: " & $tsBytes.len
  doAssert tsBytes[0] == 0x14'u8, "ThreadStart tag (20 = 0x14)"
  doAssert readU64LELocal(tsBytes, 1) == 5'u64, "ThreadStart thread_id"

  # --- ThreadSwitch: 9 bytes ---
  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleThreadSwitch,
    threadSwitchId: ThreadId(3)))
  let twBytes = enc.getBytes()
  doAssert twBytes.len == 9,
    "ThreadSwitch should be 9 bytes, got: " & $twBytes.len
  doAssert twBytes[0] == 0x16'u8, "ThreadSwitch tag (22 = 0x16)"
  doAssert readU64LELocal(twBytes, 1) == 3'u64, "ThreadSwitch thread_id"

  # --- BindVariable: 17 bytes ---
  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleBindVariable,
    bindVar: BindVariableRecord(variableId: VariableId(10), place: Place(99))))
  let bvBytes = enc.getBytes()
  doAssert bvBytes.len == 17,
    "BindVariable should be 17 bytes, got: " & $bvBytes.len
  doAssert bvBytes[0] == 0x0B'u8, "BindVariable tag (11 = 0x0B)"
  doAssert readU64LELocal(bvBytes, 1) == 10'u64, "BindVariable variable_id"
  # place is encoded as i64, 99 is positive so same as u64
  doAssert readU64LELocal(bvBytes, 9) == 99'u64, "BindVariable place"

  echo "PASS: test_fixed_size_event_layouts"

# ---------------------------------------------------------------------------
# Test: CBOR-payload events have correct envelope structure
# ---------------------------------------------------------------------------

proc test_cbor_payload_envelope() =
  ## For CBOR-payload events, verify:
  ##   [tag byte][fixed fields...][4-byte LE payload length][CBOR data]
  ## CBOR data should start with a map major type (0xA0..0xBF for small maps,
  ## or 0xBF for indefinite-length map).
  var enc = SplitBinaryEncoder.init()

  # --- Value event: tag=5, then 8-byte variable_id, then 4-byte payload len, then CBOR ---
  enc.encodeEvent(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(42),
      value: ValueRecord(kind: vrkInt, intVal: 123, intTypeId: TypeId(7)))))
  let valBytes = enc.getBytes()

  doAssert valBytes[0] == 5'u8, "Value tag"
  # variable_id at offset 1
  doAssert readU64LELocal(valBytes, 1) == 42'u64, "Value variable_id"
  # payload length at offset 9
  let payloadLen = readU32LELocal(valBytes, 9)
  doAssert payloadLen > 0, "Value CBOR payload length should be > 0"
  # Total size should be 1 (tag) + 8 (var_id) + 4 (len) + payloadLen
  doAssert valBytes.len == 13 + int(payloadLen),
    "Value total size mismatch: " & $valBytes.len & " vs expected " & $(13 + int(payloadLen))
  # CBOR data starts at offset 13; first byte should be map major type
  # CBOR map: major type 5 = 0b101xxxxx, so 0xA0..0xBF for definite, 0xBF for indefinite
  let cborFirstByte = valBytes[13]
  doAssert (cborFirstByte and 0xE0'u8) == 0xA0'u8,
    "Value CBOR should start with map major type (0xA_), got: 0x" &
    toHex(cborFirstByte)

  # --- Return event: tag=8, then 4-byte payload len, then CBOR ---
  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleReturn,
    returnRecord: ReturnRecord(
      returnValue: ValueRecord(kind: vrkInt, intVal: 999, intTypeId: TypeId(3)))))
  let retBytes = enc.getBytes()

  doAssert retBytes[0] == 8'u8, "Return tag"
  # payload length at offset 1
  let retPayloadLen = readU32LELocal(retBytes, 1)
  doAssert retPayloadLen > 0, "Return CBOR payload length should be > 0"
  # Total: 1 (tag) + 4 (len) + retPayloadLen
  doAssert retBytes.len == 5 + int(retPayloadLen),
    "Return total size mismatch"
  # CBOR map
  let retCborFirst = retBytes[5]
  doAssert (retCborFirst and 0xE0'u8) == 0xA0'u8,
    "Return CBOR should start with map major type, got: 0x" &
    toHex(retCborFirst)

  # --- Call event: tag=7, then 8-byte function_id, then 4-byte payload len, then CBOR ---
  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleCall,
    callRecord: CallRecord(functionId: FunctionId(5), args: @[])))
  let callBytes = enc.getBytes()

  doAssert callBytes[0] == 7'u8, "Call tag"
  doAssert readU64LELocal(callBytes, 1) == 5'u64, "Call function_id"
  let callPayloadLen = readU32LELocal(callBytes, 9)
  # For empty args, CBOR payload should still be valid (empty array)
  doAssert callBytes.len == 13 + int(callPayloadLen),
    "Call total size mismatch"
  # Call args is a CBOR array, major type 4 = 0b100xxxxx = 0x80..0x9F
  let callCborFirst = callBytes[13]
  doAssert (callCborFirst and 0xE0'u8) == 0x80'u8,
    "Call CBOR should start with array major type (0x8_), got: 0x" &
    toHex(callCborFirst)

  echo "PASS: test_cbor_payload_envelope"

# ---------------------------------------------------------------------------
# Test: Decode roundtrip verifies Rust-compatible encoding
# ---------------------------------------------------------------------------

proc test_encode_decode_roundtrip() =
  ## Encode events, decode them, verify they match — proves the wire format
  ## is self-consistent and decodable (same format Rust uses).
  var enc = SplitBinaryEncoder.init()

  let events = @[
    TraceLowLevelEvent(kind: tleStep,
      step: StepRecord(pathId: PathId(3), line: Line(77))),
    TraceLowLevelEvent(kind: tlePath, path: "/src/main.rs"),
    TraceLowLevelEvent(kind: tleFunction,
      functionRecord: FunctionRecord(pathId: PathId(3), line: Line(1), name: "main")),
    TraceLowLevelEvent(kind: tleCall,
      callRecord: CallRecord(functionId: FunctionId(0), args: @[])),
    TraceLowLevelEvent(kind: tleValue,
      fullValue: FullValueRecord(variableId: VariableId(1),
        value: ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(7)))),
    TraceLowLevelEvent(kind: tleReturn,
      returnRecord: ReturnRecord(returnValue: NoneValue)),
    TraceLowLevelEvent(kind: tleThreadStart, threadStartId: ThreadId(1)),
    TraceLowLevelEvent(kind: tleThreadSwitch, threadSwitchId: ThreadId(1)),
    TraceLowLevelEvent(kind: tleBindVariable,
      bindVar: BindVariableRecord(variableId: VariableId(5), place: Place(10))),
    TraceLowLevelEvent(kind: tleDropLastStep),
  ]

  for event in events:
    enc.encodeEvent(event)

  let bytes = enc.getBytes()
  let decoded = decodeAllEvents(bytes)
  doAssert decoded.isOk, "decodeAllEvents failed"
  let decodedEvents = decoded.get()

  doAssert decodedEvents.len == events.len,
    "event count mismatch: " & $decodedEvents.len & " vs " & $events.len

  for i in 0 ..< events.len:
    doAssert decodedEvents[i] == events[i],
      "event mismatch at index " & $i

  echo "PASS: test_encode_decode_roundtrip"

# ---------------------------------------------------------------------------
# Test: Full end-to-end .ct file is Rust-readable
# ---------------------------------------------------------------------------

proc test_full_ct_file_structure() =
  ## Generate a complete .ct file and verify all structural invariants
  ## that the Rust reader depends on, in one comprehensive check.
  let path = getTmpPath("test_cross_compat_full.ct")
  cleanupFile(path)

  var writerRes = newTraceWriter(path, "cross_compat_test",
    @["--flag", "value"], workdir = "/workspace")
  doAssert writerRes.isOk
  var w = writerRes.get()

  # Write a realistic trace
  doAssert w.writePath("/src/main.rs").isOk
  doAssert w.writePath("/src/lib.rs").isOk
  doAssert w.writeFunction(0, 1, "main").isOk
  doAssert w.writeStep(0, 1).isOk
  doAssert w.writeCall(0).isOk
  doAssert w.writeStep(1, 10).isOk
  doAssert w.writeReturn().isOk
  doAssert w.writeStep(0, 2).isOk

  doAssert w.close().isOk

  let readRes = readCtfsFromFile(path)
  doAssert readRes.isOk
  let data = readRes.get()

  # 1. Magic + version
  doAssert hasCtfsMagic(data)
  doAssert data[5] == 3'u8

  # 2. All internal files present
  for name in ["events.log", "events.fmt", "meta.json", "paths.json"]:
    let (sz, mb) = findInternalFile(data, name)
    doAssert mb != 0, "missing: " & name
    doAssert sz > 0, "empty: " & name

  # 3. events.fmt
  doAssert readInternalFileStr(data, "events.fmt") == "split-binary"

  # 4. meta.json
  let meta = parseJson(readInternalFileStr(data, "meta.json"))
  doAssert meta["program"].getStr() == "cross_compat_test"
  doAssert meta["args"].len == 2
  doAssert meta["workdir"].getStr() == "/workspace"

  # 5. paths.json
  let paths = parseJson(readInternalFileStr(data, "paths.json"))
  doAssert paths.kind == JArray
  doAssert paths.len == 2
  doAssert paths[0].getStr() == "/src/main.rs"
  doAssert paths[1].getStr() == "/src/lib.rs"

  # 6. events.log has valid chunk(s)
  let eventsData = readInternalFileData(data, "events.log")
  doAssert eventsData.len >= ChunkIndexEntrySize
  let firstChunk = decodeChunkHeader(eventsData, 0)
  doAssert firstChunk.compressedSize > 0
  doAssert firstChunk.eventCount > 0
  doAssert firstChunk.firstGeid == 0

  cleanupFile(path)
  echo "PASS: test_full_ct_file_structure"

# ---------------------------------------------------------------------------
# Run all tests
# ---------------------------------------------------------------------------

test_ctfs_magic_version_blocksize()
test_base40_file_names()
test_events_fmt_split_binary()
test_meta_json_structure()
test_paths_json_structure()
test_chunk_header_format()
test_split_binary_tag_ordering()
test_fixed_size_event_layouts()
test_cbor_payload_envelope()
test_encode_decode_roundtrip()
test_full_ct_file_structure()
echo "ALL PASS: test_cross_compat"
