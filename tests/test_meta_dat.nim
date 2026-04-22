{.push raises: [].}

## Tests for binary meta.dat writer and reader.

import std/[options, os, strutils]
import results
import codetracer_ctfs
import codetracer_trace_types
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/varint
import codetracer_trace_reader

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc extractFileBytes(c: Ctfs, f: CtfsInternalFile): seq[byte] {.raises: [].} =
  ## Read back the raw bytes written to an internal file.
  let bytes = c.toBytes()
  let entryOff = c.fileEntryOffset(f.entryIndex)
  let fileSize = int(readU64LE(bytes, entryOff))
  let mapBlock = readU64LE(bytes, entryOff + 8)

  result = newSeq[byte](fileSize)
  var pos = 0
  var blockIdx = 0'u64
  while pos < fileSize:
    let dataBlock = c.lookupDataBlock(mapBlock, blockIdx)
    let blockStart = c.blockOffset(dataBlock)
    let remaining = fileSize - pos
    let toCopy = min(remaining, int(c.blockSize))
    for i in 0 ..< toCopy:
      result[pos + i] = bytes[blockStart + i]
    pos += toCopy
    blockIdx += 1

proc readU16LEAt(data: openArray[byte], offset: int): uint16 =
  uint16(data[offset]) or (uint16(data[offset + 1]) shl 8)

proc decodeString(data: openArray[byte], pos: var int): Result[string, string] =
  let lenVal = ? decodeVarint(data, pos)
  let sLen = int(lenVal)
  if pos + sLen > data.len:
    return err("string extends past end of data")
  var s = newString(sLen)
  for i in 0 ..< sLen:
    s[i] = char(data[pos + i])
  pos += sLen
  ok(s)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

proc test_meta_dat_write_layout() {.raises: [].} =
  var c = createCtfs()
  let fileRes = c.addFile("meta.dat")
  doAssert fileRes.isOk, "addFile failed"
  var f = fileRes.get()

  let meta = TraceMetadata(
    program: "test_prog",
    args: @["--flag", "value"],
    workdir: "/tmp/test"
  )
  let paths = @["/src/a.nim", "/src/b.nim"]

  let wRes = c.writeMetaDat(f, meta, paths)
  doAssert wRes.isOk, "writeMetaDat failed: " & wRes.error

  let raw = extractFileBytes(c, f)
  var pos = 0

  # Magic
  doAssert raw[0] == 0x43 and raw[1] == 0x54 and raw[2] == 0x4D and raw[3] == 0x44,
    "magic mismatch"
  pos = 4

  # Version
  doAssert readU16LEAt(raw, pos) == 1, "version mismatch"
  pos += 2

  # Flags
  doAssert readU16LEAt(raw, pos) == 0, "flags should be 0 (no MCR)"
  pos += 2

  # Program
  let prog = decodeString(raw, pos)
  doAssert prog.isOk and prog.get() == "test_prog", "program mismatch"

  # Args count
  let argsCount = decodeVarint(raw, pos)
  doAssert argsCount.isOk and argsCount.get() == 2, "args count mismatch"

  let arg0 = decodeString(raw, pos)
  doAssert arg0.isOk and arg0.get() == "--flag", "arg0 mismatch"

  let arg1 = decodeString(raw, pos)
  doAssert arg1.isOk and arg1.get() == "value", "arg1 mismatch"

  # Workdir
  let wd = decodeString(raw, pos)
  doAssert wd.isOk and wd.get() == "/tmp/test", "workdir mismatch"

  # Recorder ID (empty)
  let rid = decodeString(raw, pos)
  doAssert rid.isOk and rid.get() == "", "recorder id should be empty"

  # Paths
  let pathsCount = decodeVarint(raw, pos)
  doAssert pathsCount.isOk and pathsCount.get() == 2, "paths count mismatch"

  let p0 = decodeString(raw, pos)
  doAssert p0.isOk and p0.get() == "/src/a.nim", "path0 mismatch"

  let p1 = decodeString(raw, pos)
  doAssert p1.isOk and p1.get() == "/src/b.nim", "path1 mismatch"

  # Should have consumed all bytes
  doAssert pos == raw.len, "trailing bytes: consumed " & $pos & " of " & $raw.len

  c.closeCtfs()
  echo "PASS: test_meta_dat_write_layout"


proc test_meta_dat_with_mcr_fields() {.raises: [].} =
  var c = createCtfs()
  let fileRes = c.addFile("meta.dat")
  doAssert fileRes.isOk, "addFile failed"
  var f = fileRes.get()

  let meta = TraceMetadata(
    program: "mcr_test",
    args: @["arg1"],
    workdir: "/work"
  )
  let paths = @["/src/main.c"]
  let mcr = McrMetaFields(
    tickSource: tsRdtsc,
    totalThreads: 4,
    atomicMode: amRelaxed
  )

  let wRes = c.writeMetaDat(f, meta, paths, mcrFields = some(mcr))
  doAssert wRes.isOk, "writeMetaDat failed: " & wRes.error

  let raw = extractFileBytes(c, f)
  var pos = 0

  # Magic
  doAssert raw[0] == 0x43 and raw[1] == 0x54 and raw[2] == 0x4D and raw[3] == 0x44,
    "magic mismatch"
  pos = 4

  # Version
  doAssert readU16LEAt(raw, pos) == 1, "version mismatch"
  pos += 2

  # Flags — bit 0 should be set
  let flags = readU16LEAt(raw, pos)
  doAssert (flags and 1) == 1, "flags bit 0 should be set for MCR fields"
  pos += 2

  # Skip program, args, workdir, recorder_id, paths
  let prog = decodeString(raw, pos)
  doAssert prog.isOk and prog.get() == "mcr_test"

  let argsCount = decodeVarint(raw, pos)
  doAssert argsCount.isOk and argsCount.get() == 1
  let a0 = decodeString(raw, pos)
  doAssert a0.isOk and a0.get() == "arg1"

  let wd = decodeString(raw, pos)
  doAssert wd.isOk and wd.get() == "/work"

  let rid = decodeString(raw, pos)
  doAssert rid.isOk and rid.get() == ""

  let pathsCount = decodeVarint(raw, pos)
  doAssert pathsCount.isOk and pathsCount.get() == 1
  let p0 = decodeString(raw, pos)
  doAssert p0.isOk and p0.get() == "/src/main.c"

  # MCR fields
  let tickSrc = decodeVarint(raw, pos)
  doAssert tickSrc.isOk and tickSrc.get() == 0, "tick_source should be 0 (rdtsc)"

  let threads = decodeVarint(raw, pos)
  doAssert threads.isOk and threads.get() == 4, "total_threads should be 4"

  let atomicMd = decodeVarint(raw, pos)
  doAssert atomicMd.isOk and atomicMd.get() == 0, "atomic_mode should be 0 (relaxed)"

  let totalEv = decodeVarint(raw, pos)
  doAssert totalEv.isOk and totalEv.get() == 0, "total_events should be 0"

  let totalCp = decodeVarint(raw, pos)
  doAssert totalCp.isOk and totalCp.get() == 0, "total_checkpoints should be 0"

  let startTime = decodeVarint(raw, pos)
  doAssert startTime.isOk and startTime.get() == 0, "start_time_unix_us should be 0"

  # platform (empty string)
  let platStr = decodeString(raw, pos)
  doAssert platStr.isOk and platStr.get() == "", "platform should be empty"

  # tickGranularity (empty string)
  let tgStr = decodeString(raw, pos)
  doAssert tgStr.isOk and tgStr.get() == "", "tickGranularity should be empty"

  # tickSourceStr (empty string)
  let tsStr = decodeString(raw, pos)
  doAssert tsStr.isOk and tsStr.get() == "", "tickSourceStr should be empty"

  # atomicModeStr (empty string)
  let amStr = decodeString(raw, pos)
  doAssert amStr.isOk and amStr.get() == "", "atomicModeStr should be empty"

  # startTimeStr (empty string)
  let stStr = decodeString(raw, pos)
  doAssert stStr.isOk and stStr.get() == "", "startTimeStr should be empty"

  doAssert pos == raw.len, "trailing bytes: consumed " & $pos & " of " & $raw.len

  c.closeCtfs()
  echo "PASS: test_meta_dat_with_mcr_fields"


proc test_meta_dat_empty_fields() {.raises: [].} =
  var c = createCtfs()
  let fileRes = c.addFile("meta.dat")
  doAssert fileRes.isOk, "addFile failed"
  var f = fileRes.get()

  let meta = TraceMetadata(
    program: "",
    args: @[],
    workdir: ""
  )
  let paths: seq[string] = @[]

  let wRes = c.writeMetaDat(f, meta, paths)
  doAssert wRes.isOk, "writeMetaDat failed: " & wRes.error

  let raw = extractFileBytes(c, f)
  var pos = 0

  # Magic + version + flags = 8 bytes
  doAssert raw[0] == 0x43 and raw[1] == 0x54 and raw[2] == 0x4D and raw[3] == 0x44
  pos = 4
  doAssert readU16LEAt(raw, pos) == 1
  pos += 2
  doAssert readU16LEAt(raw, pos) == 0
  pos += 2

  # Empty program (varint 0)
  let prog = decodeString(raw, pos)
  doAssert prog.isOk and prog.get() == ""

  # Args count = 0
  let argsCount = decodeVarint(raw, pos)
  doAssert argsCount.isOk and argsCount.get() == 0

  # Empty workdir
  let wd = decodeString(raw, pos)
  doAssert wd.isOk and wd.get() == ""

  # Empty recorder id
  let rid = decodeString(raw, pos)
  doAssert rid.isOk and rid.get() == ""

  # Paths count = 0
  let pathsCount = decodeVarint(raw, pos)
  doAssert pathsCount.isOk and pathsCount.get() == 0

  # 8 fixed bytes + 5 varint zeros (each 1 byte) = 13 bytes total
  doAssert pos == raw.len, "trailing bytes: consumed " & $pos & " of " & $raw.len
  doAssert raw.len == 13, "expected 13 bytes for empty meta.dat, got " & $raw.len

  c.closeCtfs()
  echo "PASS: test_meta_dat_empty_fields"


proc test_meta_dat_roundtrip() {.raises: [].} =
  ## Write meta.dat, then read it back with readMetaDat and verify all fields.
  var c = createCtfs()
  let fileRes = c.addFile("meta.dat")
  doAssert fileRes.isOk, "addFile failed"
  var f = fileRes.get()

  let meta = TraceMetadata(
    program: "/usr/bin/myapp",
    args: @["--verbose", "-o", "output.txt"],
    workdir: "/home/user/project"
  )
  let paths = @["/src/main.nim", "/src/utils.nim", "/src/lib.nim"]
  let recorderId = "nim-recorder-v1"

  let wRes = c.writeMetaDat(f, meta, paths, recorderId = recorderId)
  doAssert wRes.isOk, "writeMetaDat failed: " & wRes.unsafeError

  let raw = extractFileBytes(c, f)
  let parsed = readMetaDat(raw)
  doAssert parsed.isOk, "readMetaDat failed: " & parsed.unsafeError

  let contents = parsed.get()
  doAssert contents.version == 1, "version mismatch"
  doAssert contents.program == "/usr/bin/myapp", "program mismatch: " & contents.program
  doAssert contents.workdir == "/home/user/project", "workdir mismatch"
  doAssert contents.args.len == 3, "args count mismatch"
  doAssert contents.args[0] == "--verbose", "arg0 mismatch"
  doAssert contents.args[1] == "-o", "arg1 mismatch"
  doAssert contents.args[2] == "output.txt", "arg2 mismatch"
  doAssert contents.recorderId == "nim-recorder-v1", "recorderId mismatch"
  doAssert contents.paths.len == 3, "paths count mismatch"
  doAssert contents.paths[0] == "/src/main.nim", "path0 mismatch"
  doAssert contents.paths[1] == "/src/utils.nim", "path1 mismatch"
  doAssert contents.paths[2] == "/src/lib.nim", "path2 mismatch"
  doAssert contents.mcrFields.isNone, "mcrFields should be None"

  c.closeCtfs()
  echo "PASS: test_meta_dat_roundtrip"


proc test_meta_dat_roundtrip_with_mcr() {.raises: [].} =
  ## Roundtrip with MCR fields present.
  var c = createCtfs()
  let fileRes = c.addFile("meta.dat")
  doAssert fileRes.isOk, "addFile failed"
  var f = fileRes.get()

  let meta = TraceMetadata(
    program: "mcr_prog",
    args: @["a"],
    workdir: "/w"
  )
  let paths = @["/p1"]
  let mcr = McrMetaFields(
    tickSource: tsMonotonic,
    totalThreads: 8,
    atomicMode: amSeqCst,
    totalEvents: 42000,
    totalCheckpoints: 5,
    startTimeUnixUs: 1700000000_000000'u64,
  )

  let wRes = c.writeMetaDat(f, meta, paths, recorderId = "mcr-rec",
                            mcrFields = some(mcr))
  doAssert wRes.isOk, "writeMetaDat failed: " & wRes.unsafeError

  let raw = extractFileBytes(c, f)
  let parsed = readMetaDat(raw)
  doAssert parsed.isOk, "readMetaDat failed: " & parsed.unsafeError

  let contents = parsed.get()
  doAssert contents.program == "mcr_prog"
  doAssert contents.args == @["a"]
  doAssert contents.workdir == "/w"
  doAssert contents.recorderId == "mcr-rec"
  doAssert contents.paths == @["/p1"]
  doAssert contents.mcrFields.isSome, "mcrFields should be present"

  let m = contents.mcrFields.get()
  doAssert m.tickSource == tsMonotonic, "tickSource mismatch"
  doAssert m.totalThreads == 8, "totalThreads mismatch"
  doAssert m.atomicMode == amSeqCst, "atomicMode mismatch"
  doAssert m.totalEvents == 42000, "totalEvents mismatch"
  doAssert m.totalCheckpoints == 5, "totalCheckpoints mismatch"
  doAssert m.startTimeUnixUs == 1700000000_000000'u64, "startTimeUnixUs mismatch"

  c.closeCtfs()
  echo "PASS: test_meta_dat_roundtrip_with_mcr"


proc test_meta_dat_read_bad_magic() {.raises: [].} =
  ## readMetaDat should reject data with wrong magic.
  let badData: array[8, byte] = [0x00'u8, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00]
  let res = readMetaDat(badData)
  doAssert res.isErr, "should fail on bad magic"
  doAssert "bad magic" in res.unsafeError, "error should mention bad magic"
  echo "PASS: test_meta_dat_read_bad_magic"


proc test_meta_dat_read_too_short() {.raises: [].} =
  ## readMetaDat should reject data shorter than 8 bytes.
  let shortData: array[4, byte] = [0x43'u8, 0x54, 0x4D, 0x44]
  let res = readMetaDat(shortData)
  doAssert res.isErr, "should fail on short data"
  doAssert "too short" in res.unsafeError, "error should mention too short"
  echo "PASS: test_meta_dat_read_too_short"


proc writeJsonString(c: var Ctfs, f: var CtfsInternalFile,
                    s: string): Result[void, string] =
  ## Helper to write a string as bytes to an internal file.
  if s.len > 0:
    let bytes = cast[seq[byte]](s)
    ? c.writeToFile(f, bytes)
  ok()


proc test_meta_dat_backward_compat() {.raises: [].} =
  ## Create a CTFS container with meta.json + paths.json (old format),
  ## write to a temp file, open with openTrace, verify it reads correctly.
  var c = createCtfs()

  # Add meta.json
  let metaFileRes = c.addFile("meta.json")
  doAssert metaFileRes.isOk, "addFile meta.json failed"
  var metaFile = metaFileRes.get()

  let metaJson = """{"program":"/bin/old_prog","args":["--old","flag"],"workdir":"/old/dir"}"""
  let wRes1 = writeJsonString(c, metaFile, metaJson)
  doAssert wRes1.isOk, "write meta.json failed"

  # Add paths.json
  let pathsFileRes = c.addFile("paths.json")
  doAssert pathsFileRes.isOk, "addFile paths.json failed"
  var pathsFile = pathsFileRes.get()

  let pathsJson = """["/old/src/a.nim","/old/src/b.nim"]"""
  let wRes2 = writeJsonString(c, pathsFile, pathsJson)
  doAssert wRes2.isOk, "write paths.json failed"

  # Write to temp file
  let tmpPath = getTempDir() / "test_meta_dat_compat.ct"
  let saveRes = c.writeCtfsToFile(tmpPath)
  doAssert saveRes.isOk, "writeCtfsToFile failed: " & saveRes.unsafeError
  c.closeCtfs()

  # Open with openTrace — should fall back to JSON
  let traceRes = openTrace(tmpPath)
  doAssert traceRes.isOk, "openTrace failed: " & traceRes.unsafeError

  let reader = traceRes.get()
  doAssert reader.metadata.program == "/bin/old_prog",
    "program mismatch: " & reader.metadata.program
  doAssert reader.metadata.workdir == "/old/dir",
    "workdir mismatch: " & reader.metadata.workdir
  doAssert reader.metadata.args.len == 2, "args count mismatch"
  doAssert reader.metadata.args[0] == "--old", "arg0 mismatch"
  doAssert reader.metadata.args[1] == "flag", "arg1 mismatch"
  doAssert reader.paths.len == 2, "paths count mismatch"
  doAssert reader.paths[0] == "/old/src/a.nim", "path0 mismatch"
  doAssert reader.paths[1] == "/old/src/b.nim", "path1 mismatch"

  # Clean up
  try: removeFile(tmpPath)
  except OSError: discard

  echo "PASS: test_meta_dat_backward_compat"


proc test_meta_dat_openTrace_binary() {.raises: [].} =
  ## Create a CTFS container with meta.dat (new format),
  ## write to a temp file, open with openTrace, verify it reads correctly.
  ## Also compare toJson output with a JSON-based container to verify identical format.
  var c = createCtfs()

  let metaDatFileRes = c.addFile("meta.dat")
  doAssert metaDatFileRes.isOk, "addFile meta.dat failed"
  var metaDatFile = metaDatFileRes.get()

  let meta = TraceMetadata(
    program: "/bin/test_prog",
    args: @["--flag", "value"],
    workdir: "/tmp/test"
  )
  let paths = @["/src/main.nim", "/src/lib.nim"]

  let wRes = c.writeMetaDat(metaDatFile, meta, paths, recorderId = "test-rec")
  doAssert wRes.isOk, "writeMetaDat failed: " & wRes.unsafeError

  let tmpPath = getTempDir() / "test_meta_dat_binary.ct"
  let saveRes = c.writeCtfsToFile(tmpPath)
  doAssert saveRes.isOk, "writeCtfsToFile failed: " & saveRes.unsafeError
  c.closeCtfs()

  # Open with openTrace — should use meta.dat path
  let traceRes = openTrace(tmpPath)
  doAssert traceRes.isOk, "openTrace failed: " & traceRes.unsafeError

  let reader = traceRes.get()
  doAssert reader.metadata.program == "/bin/test_prog",
    "program mismatch: " & reader.metadata.program
  doAssert reader.metadata.workdir == "/tmp/test",
    "workdir mismatch: " & reader.metadata.workdir
  doAssert reader.metadata.args.len == 2, "args count mismatch"
  doAssert reader.metadata.args[0] == "--flag", "arg0 mismatch"
  doAssert reader.metadata.args[1] == "value", "arg1 mismatch"
  doAssert reader.paths.len == 2, "paths count mismatch"
  doAssert reader.paths[0] == "/src/main.nim", "path0 mismatch"
  doAssert reader.paths[1] == "/src/lib.nim", "path1 mismatch"

  # Now create an equivalent JSON-based container and compare toJson output
  var cJson = createCtfs()

  let metaJsonFileRes = cJson.addFile("meta.json")
  doAssert metaJsonFileRes.isOk
  var metaJsonFile = metaJsonFileRes.get()
  let metaJsonStr = """{"program":"/bin/test_prog","args":["--flag","value"],"workdir":"/tmp/test"}"""
  let w1 = writeJsonString(cJson, metaJsonFile, metaJsonStr)
  doAssert w1.isOk

  let pathsJsonFileRes = cJson.addFile("paths.json")
  doAssert pathsJsonFileRes.isOk
  var pathsJsonFile = pathsJsonFileRes.get()
  let pathsJsonStr = """["/src/main.nim","/src/lib.nim"]"""
  let w2 = writeJsonString(cJson, pathsJsonFile, pathsJsonStr)
  doAssert w2.isOk

  let tmpPathJson = getTempDir() / "test_meta_dat_json_compare.ct"
  let saveRes2 = cJson.writeCtfsToFile(tmpPathJson)
  doAssert saveRes2.isOk
  cJson.closeCtfs()

  let traceResJson = openTrace(tmpPathJson)
  doAssert traceResJson.isOk, "openTrace JSON failed: " & traceResJson.unsafeError

  let readerJson = traceResJson.get()

  # Compare toJson output — should be identical
  let jsonBinary = reader.toJson()
  let jsonFallback = readerJson.toJson()
  doAssert jsonBinary == jsonFallback,
    "toJson output differs between binary and JSON metadata"

  # Clean up
  try: removeFile(tmpPath)
  except OSError: discard
  try: removeFile(tmpPathJson)
  except OSError: discard

  echo "PASS: test_meta_dat_openTrace_binary"


# Run all tests
test_meta_dat_write_layout()
test_meta_dat_with_mcr_fields()
test_meta_dat_empty_fields()
test_meta_dat_roundtrip()
test_meta_dat_roundtrip_with_mcr()
test_meta_dat_read_bad_magic()
test_meta_dat_read_too_short()
test_meta_dat_backward_compat()
test_meta_dat_openTrace_binary()
