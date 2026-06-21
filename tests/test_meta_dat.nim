{.push raises: [].}

## Tests for binary meta.dat writer and reader.

import std/[hashes, options, os, strutils]
import results
import codetracer_ctfs
import codetracer_trace_types
import codetracer_trace_writer
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/varint
import codetracer_trace_writer/uuid_v7
import codetracer_trace_reader

# M-REC-1: a canonical UUIDv7 used in tests that don't care about
# id-generation behaviour.  Keeps each test's wire layout deterministic.
const TestRecordingId* = "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb"

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
    recordingId: TestRecordingId,
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
  doAssert readU16LEAt(raw, pos) == 3, "version mismatch (expected v3, M-REC-1)"
  pos += 2

  # Flags
  doAssert readU16LEAt(raw, pos) == 0, "flags should be 0 (no MCR)"
  pos += 2

  # Recording id (M-REC-1)
  let recId = decodeString(raw, pos)
  doAssert recId.isOk and recId.get() == TestRecordingId,
    "recording_id mismatch"

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
    recordingId: TestRecordingId,
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
  doAssert readU16LEAt(raw, pos) == 3, "version mismatch (v3 from M-REC-1)"
  pos += 2

  # Flags — bit 0 should be set
  let flags = readU16LEAt(raw, pos)
  doAssert (flags and 1) == 1, "flags bit 0 should be set for MCR fields"
  pos += 2

  # Skip recording_id, program, args, workdir, recorder_id, paths
  let recId = decodeString(raw, pos)
  doAssert recId.isOk and recId.get() == TestRecordingId,
    "recording_id mismatch"

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

  # hookProfile (empty string, v2)
  let hpStr = decodeString(raw, pos)
  doAssert hpStr.isOk and hpStr.get() == "", "hookProfile should be empty"

  # hookStrategies (count = 0, v2)
  let hsCount = decodeVarint(raw, pos)
  doAssert hsCount.isOk and hsCount.get() == 0,
    "hookStrategies count should be 0"

  doAssert pos == raw.len, "trailing bytes: consumed " & $pos & " of " & $raw.len

  c.closeCtfs()
  echo "PASS: test_meta_dat_with_mcr_fields"


proc test_meta_dat_empty_fields() {.raises: [].} =
  var c = createCtfs()
  let fileRes = c.addFile("meta.dat")
  doAssert fileRes.isOk, "addFile failed"
  var f = fileRes.get()

  # M-REC-1: recording_id is required even when every other field is
  # empty.  This test pins the minimum-size meta.dat layout.
  let meta = TraceMetadata(
    recordingId: TestRecordingId,
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
  doAssert readU16LEAt(raw, pos) == 3
  pos += 2
  doAssert readU16LEAt(raw, pos) == 0
  pos += 2

  # Recording id (varint 36 + 36 bytes of canonical UUIDv7) — M-REC-1
  let recId = decodeString(raw, pos)
  doAssert recId.isOk and recId.get() == TestRecordingId

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

  # 8 fixed bytes + 1 varint (=36) + 36 recording_id bytes + 5 varint
  # zeros (each 1 byte) = 50 bytes total.
  doAssert pos == raw.len, "trailing bytes: consumed " & $pos & " of " & $raw.len
  doAssert raw.len == 50,
    "expected 50 bytes for minimal v3 meta.dat, got " & $raw.len

  c.closeCtfs()
  echo "PASS: test_meta_dat_empty_fields"


proc test_meta_dat_roundtrip() {.raises: [].} =
  ## Write meta.dat, then read it back with readMetaDat and verify all fields.
  var c = createCtfs()
  let fileRes = c.addFile("meta.dat")
  doAssert fileRes.isOk, "addFile failed"
  var f = fileRes.get()

  let meta = TraceMetadata(
    recordingId: TestRecordingId,
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
  doAssert contents.version == 3, "version mismatch (expected v3)"
  doAssert contents.recordingId == TestRecordingId,
    "recording_id round-trip failed: got " & contents.recordingId
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
    recordingId: TestRecordingId,
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
    hookProfile: "dotnet",
    hookStrategies: @["ldpreload", "seccomp_unotify", "callsite_patch"],
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
  doAssert m.hookProfile == "dotnet", "hookProfile mismatch"
  doAssert m.hookStrategies ==
    @["ldpreload", "seccomp_unotify", "callsite_patch"],
    "hookStrategies mismatch"

  c.closeCtfs()
  echo "PASS: test_meta_dat_roundtrip_with_mcr"


proc test_meta_dat_roundtrip_with_filter_provenance() {.raises: [].} =
  ## TF-M7: round-trip the trace-filter provenance block through both
  ## the CTFS-based writer and the buffer-based writer, then re-parse
  ## with readMetaDat and assert the entries match byte-for-byte
  ## (including the raw 32-byte sha256 digests and the inline-default
  ## sentinel `<inline:builtin-default>`).
  var entries = newSeq[FilterProvenance](3)
  entries[0].path = "<inline:builtin-default>"
  for i in 0 ..< 32:
    entries[0].sha256[i] = byte(i)
  entries[1].path = "/project/.codetracer/trace-filter.toml"
  for i in 0 ..< 32:
    entries[1].sha256[i] = byte(0xA0 + (i mod 16))
  entries[2].path = "/home/user/override.toml"
  for i in 0 ..< 32:
    entries[2].sha256[i] = byte(255 - i)

  let meta = TraceMetadata(
    recordingId: TestRecordingId,
    program: "prog",
    args: @[],
    workdir: "/w",
  )
  let paths = @["/p"]

  # ----- CTFS-based writer -----
  block:
    var c = createCtfs()
    let fileRes = c.addFile("meta.dat")
    doAssert fileRes.isOk
    var f = fileRes.get()
    let wRes = c.writeMetaDat(f, meta, paths,
      filterProvenance = entries)
    doAssert wRes.isOk, "writeMetaDat failed: " & wRes.unsafeError

    let raw = extractFileBytes(c, f)
    # Header flags byte should have bit 3 set (FlagHasTraceFilterProvenance = 8).
    let flags = readU16LEAt(raw, 6)
    doAssert (flags and FlagHasTraceFilterProvenance) != 0,
      "FlagHasTraceFilterProvenance must be set when provenance is non-empty"
    doAssert (flags and FlagHasMcrFields) == 0,
      "MCR flag should be off"

    let parsed = readMetaDat(raw)
    doAssert parsed.isOk, "readMetaDat failed: " & parsed.unsafeError
    let contents = parsed.get()
    doAssert contents.hasFilterProvenance, "hasFilterProvenance must be true"
    doAssert contents.filterProvenance.len == 3,
      "expected 3 provenance entries, got " & $contents.filterProvenance.len
    doAssert contents.filterProvenance[0].path == "<inline:builtin-default>"
    doAssert contents.filterProvenance[1].path ==
      "/project/.codetracer/trace-filter.toml"
    doAssert contents.filterProvenance[2].path == "/home/user/override.toml"
    for entryIdx in 0 ..< 3:
      for i in 0 ..< 32:
        doAssert contents.filterProvenance[entryIdx].sha256[i] ==
          entries[entryIdx].sha256[i],
          "sha256 byte " & $i & " of entry " & $entryIdx & " mismatch"
    c.closeCtfs()

  # ----- Buffer-based writer -----
  block:
    let buf = writeMetaDatToBuffer(meta, paths,
      filterProvenance = entries)
    let flags = readU16LEAt(buf, 6)
    doAssert (flags and FlagHasTraceFilterProvenance) != 0,
      "buffer writer: FlagHasTraceFilterProvenance must be set"

    let parsed = readMetaDat(buf)
    doAssert parsed.isOk, "buffer readMetaDat failed: " & parsed.unsafeError
    let contents = parsed.get()
    doAssert contents.filterProvenance.len == 3
    doAssert contents.filterProvenance[0].path == "<inline:builtin-default>"
    doAssert contents.filterProvenance[2].path == "/home/user/override.toml"
    # Cross-check one sha256 digest byte-by-byte.
    for i in 0 ..< 32:
      doAssert contents.filterProvenance[2].sha256[i] ==
        entries[2].sha256[i],
        "buffer: sha256 byte " & $i & " of last entry mismatch"

  echo "PASS: test_meta_dat_roundtrip_with_filter_provenance"


proc test_meta_dat_roundtrip_empty_filter_provenance() {.raises: [].} =
  ## TF-M7: when `emitFilterProvenance` is set but the provenance list
  ## is empty, the flag bit must still be on and the reader must
  ## surface `hasFilterProvenance = true` with an empty list.  This
  ## preserves the spec §7 distinction between "did not record" (flag
  ## off) and "recorded an empty chain" (flag on, count 0).
  let meta = TraceMetadata(
    recordingId: TestRecordingId, program: "prog", args: @[], workdir: "/w")
  let paths: seq[string] = @[]

  let buf = writeMetaDatToBuffer(meta, paths,
    filterProvenance = [], emitFilterProvenance = true)
  let flags = readU16LEAt(buf, 6)
  doAssert (flags and FlagHasTraceFilterProvenance) != 0,
    "flag must be set even for empty provenance"

  let parsed = readMetaDat(buf)
  doAssert parsed.isOk
  let contents = parsed.get()
  doAssert contents.hasFilterProvenance
  doAssert contents.filterProvenance.len == 0
  echo "PASS: test_meta_dat_roundtrip_empty_filter_provenance"


proc test_meta_dat_no_filter_provenance_omits_flag() {.raises: [].} =
  ## TF-M7: when no provenance is passed and emitFilterProvenance is
  ## false (the default), the flag bit MUST stay off and the reader
  ## MUST report `hasFilterProvenance = false`.
  let meta = TraceMetadata(
    recordingId: TestRecordingId, program: "prog", args: @[], workdir: "/w")
  let paths: seq[string] = @[]
  let buf = writeMetaDatToBuffer(meta, paths)
  let flags = readU16LEAt(buf, 6)
  doAssert (flags and FlagHasTraceFilterProvenance) == 0,
    "flag must be off when caller did not record provenance"

  let parsed = readMetaDat(buf)
  doAssert parsed.isOk
  doAssert not parsed.get().hasFilterProvenance
  doAssert parsed.get().filterProvenance.len == 0
  echo "PASS: test_meta_dat_no_filter_provenance_omits_flag"


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

  # M-REC-1: meta.json fallback path now also requires recording_id.
  let metaJson = """{"recording_id":"01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb","program":"/bin/old_prog","args":["--old","flag"],"workdir":"/old/dir"}"""
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
    recordingId: TestRecordingId,
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
  let metaJsonStr = """{"recording_id":"01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb","program":"/bin/test_prog","args":["--flag","value"],"workdir":"/tmp/test"}"""
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


# ---------------------------------------------------------------------------
# M-REC-1 — recording_id behaviours
# ---------------------------------------------------------------------------

proc test_meta_dat_uuidv7_generation() {.raises: [].} =
  ## M-REC-1 acceptance: a freshly minted UUIDv7 is canonical-form, has
  ## the version-7 nibble, and a valid variant nibble.
  let r1 = newUuidV7()
  doAssert r1.isOk, "newUuidV7 should succeed on a healthy host"
  let u = r1.get()
  let s = $u
  doAssert s.len == 36, "canonical text form is 36 chars; got " & $s.len
  doAssert s[8] == '-' and s[13] == '-' and s[18] == '-' and s[23] == '-'
  doAssert s[14] == '7', "version nibble must be '7'; got '" & $s[14] & "'"
  doAssert s[19] in {'8', '9', 'a', 'b'},
    "variant nibble must be in {8,9,a,b}; got '" & $s[19] & "'"

  # Round-trip parse: the string we just printed must parse back to
  # the same bytes.
  let parsed = parseUuidV7(s)
  doAssert parsed.isOk, "parseUuidV7 failed: " & parsed.error
  for i in 0 ..< 16:
    doAssert parsed.get().bytes[i] == u.bytes[i],
      "byte " & $i & " differs after parse round-trip"

  echo "PASS: test_meta_dat_uuidv7_generation"


proc test_meta_dat_uuidv7_ms_monotonic_sortable() {.raises: [].} =
  ## M-REC-1 acceptance: two recordings minted on the same host one
  ## second apart sort by id lex-ascending, and the embedded ms
  ## timestamp of the later id is strictly greater.
  let aRes = newUuidV7()
  doAssert aRes.isOk
  let a = aRes.get()
  let aStr = $a
  let aMs = a.unixMs

  # Sleep ~1.05 seconds.  RFC 9562 §6.2: ms-granularity timestamps, so
  # a one-second gap is far more than enough to guarantee a strictly
  # larger unix_ts_ms prefix.
  try:
    os.sleep(1050)
  except CatchableError:
    discard  # On the practically-impossible failure path, just continue.

  let bRes = newUuidV7()
  doAssert bRes.isOk
  let b = bRes.get()
  let bStr = $b
  let bMs = b.unixMs

  doAssert aMs < bMs,
    "later recording's embedded ms must be > earlier; aMs=" &
    $aMs & " bMs=" & $bMs
  doAssert aStr < bStr,
    "canonical text form must sort ascending by creation time; " &
    "a=" & aStr & " b=" & bStr

  echo "PASS: test_meta_dat_uuidv7_ms_monotonic_sortable"


proc test_meta_dat_recording_id_required() {.raises: [].} =
  ## M-REC-1: writing meta.dat with an empty recording_id MUST fail
  ## (validation happens before the magic bytes are emitted).
  var c = createCtfs()
  let fileRes = c.addFile("meta.dat")
  doAssert fileRes.isOk
  var f = fileRes.get()

  let meta = TraceMetadata(
    recordingId: "",  # invalid
    program: "p", args: @[], workdir: "/w")
  let wRes = c.writeMetaDat(f, meta, @[])
  doAssert wRes.isErr,
    "writeMetaDat must reject empty recording_id"

  c.closeCtfs()
  echo "PASS: test_meta_dat_recording_id_required"


proc test_meta_dat_recording_id_malformed_rejected() {.raises: [].} =
  ## M-REC-1: malformed canonical form (wrong length, wrong version
  ## nibble, uppercase) is rejected by the writer and (for binary
  ## fixtures) by the reader.
  var c = createCtfs()
  let badCases = @[
    "not-a-uuid",                                  # too short
    "01949FCC-7D92-7E9C-AAAA-BBBBBBBBBBBB",        # uppercase
    "01949fcc-7d92-4e9c-aaaa-bbbbbbbbbbbb",        # version 4, not 7
    "01949fcc-7d92-7e9c-caaa-bbbbbbbbbbbb",        # variant 'c' (= 11b, wrong)
    "01949fcc7d927e9caaaabbbbbbbbbbbbbbbb",        # missing hyphens
  ]
  for bad in badCases:
    let fileRes = c.addFile("bad_" & $hash(bad))
    doAssert fileRes.isOk
    var f = fileRes.get()
    let meta = TraceMetadata(
      recordingId: bad, program: "p", args: @[], workdir: "/w")
    let wRes = c.writeMetaDat(f, meta, @[])
    doAssert wRes.isErr,
      "writeMetaDat must reject malformed recording_id: '" & bad & "'"

  c.closeCtfs()
  echo "PASS: test_meta_dat_recording_id_malformed_rejected"


proc test_meta_dat_reader_rejects_missing_recording_id() {.raises: [].} =
  ## M-REC-1: a hand-crafted v3 meta.dat with an empty recording_id
  ## string (varint length 0, no body) MUST be rejected at parse time.
  var buf = newSeq[byte](0)
  # Magic
  for b in [0x43'u8, 0x54, 0x4D, 0x44]:
    buf.add(b)
  # Version = 3
  buf.add(3'u8); buf.add(0'u8)
  # Flags = 0
  buf.add(0'u8); buf.add(0'u8)
  # Empty recording_id (varint 0 — zero-length string)
  buf.add(0'u8)
  # Empty program, args_count = 0, empty workdir, empty recorder_id,
  # paths_count = 0
  buf.add(0'u8)  # program
  buf.add(0'u8)  # args_count
  buf.add(0'u8)  # workdir
  buf.add(0'u8)  # recorder_id
  buf.add(0'u8)  # paths_count

  let res = readMetaDat(buf)
  doAssert res.isErr,
    "readMetaDat must reject meta.dat with empty recording_id"

  echo "PASS: test_meta_dat_reader_rejects_missing_recording_id"


proc test_meta_dat_writer_mints_when_blank() {.raises: [].} =
  ## newTraceWriter with empty recordingId mints a fresh UUIDv7.
  ## Two writers in a row produce different ids.
  let tmpDir = getTempDir() / "test_meta_dat_writer_mints"
  try:
    createDir(tmpDir)
  except OSError, IOError:
    discard

  let path1 = tmpDir / "a.ct"
  let path2 = tmpDir / "b.ct"

  let w1Res = newTraceWriter(path1, "p", @["arg"])
  doAssert w1Res.isOk, "newTraceWriter failed: " & w1Res.error
  var w1 = w1Res.get()
  let id1 = w1.metadata.recordingId
  doAssert validateRecordingIdStr(id1).isOk,
    "minted id1 is not a canonical UUIDv7: '" & id1 & "'"

  let w2Res = newTraceWriter(path2, "p", @["arg"])
  doAssert w2Res.isOk
  var w2 = w2Res.get()
  let id2 = w2.metadata.recordingId
  doAssert validateRecordingIdStr(id2).isOk
  doAssert id1 != id2,
    "two freshly-minted ids should differ; both were '" & id1 & "'"

  discard w1.close()
  discard w2.close()

  try:
    removeFile(path1)
    removeFile(path2)
    removeDir(tmpDir)
  except OSError, IOError:
    discard

  echo "PASS: test_meta_dat_writer_mints_when_blank"


proc test_meta_dat_strict_unknown_flag_rejection() {.raises: [].} =
  ## P6.5: ``readMetaDat`` MUST reject any meta.dat whose flag word
  ## carries a bit outside ``KnownFlags``.  This is the contract that
  ## makes future flag-bit extensions safely additive: an older reader
  ## that doesn't know a new bit refuses to open the trace cleanly,
  ## rather than silently misdecoding downstream streams.
  ##
  ## Tests three cases:
  ##   * flags = 0 (pre-extension trace) round-trips cleanly,
  ##   * flags = FlagHasColumnAwareSteps (bit 4, known) round-trips
  ##     cleanly with ``hasColumnAwareSteps = true``,
  ##   * flags = bit 4 + bit 10 fails because bit 10 is unknown.
  ##     (Bits 5..9 are now allocated — bit 5 to
  ##     ``FlagHasAlternateSourceViews``, bits 6 and 7 to the
  ##     M-capability-flags ``FlagSupportsColumnBreakpoints`` /
  ##     ``FlagSupportsColumnMotions``, bit 8 to M17a's
  ##     ``FlagHasCallStream``, and bit 9 to M23a's
  ##     ``FlagHasStepStream``.  Each time a new bit landed
  ##     this test was retargeted to the next unknown bit so the
  ##     strict-rejection contract stays exercised.)
  proc craft(flags: uint16): seq[byte] {.raises: [].} =
    var buf = newSeq[byte](0)
    for b in [0x43'u8, 0x54, 0x4D, 0x44]:
      buf.add(b)
    buf.add(3'u8); buf.add(0'u8)               # version 3
    buf.add(byte(flags and 0xFF))
    buf.add(byte((flags shr 8) and 0xFF))
    # recording_id (canonical UUIDv7)
    encodeVarint(uint64(TestRecordingId.len), buf)
    for c in TestRecordingId:
      buf.add(byte(c))
    # program, args_count, workdir, recorder_id, paths_count — all empty/zero.
    buf.add(0'u8); buf.add(0'u8); buf.add(0'u8); buf.add(0'u8); buf.add(0'u8)
    buf

  block:
    let res = readMetaDat(craft(0))
    doAssert res.isOk, "flags=0 must round-trip: " &
      (if res.isErr: res.error else: "ok")
    doAssert not res.get().hasColumnAwareSteps

  block:
    let res = readMetaDat(craft(FlagHasColumnAwareSteps))
    doAssert res.isOk,
      "bit 4 alone must round-trip: " &
      (if res.isErr: res.error else: "ok")
    doAssert res.get().hasColumnAwareSteps

  block:
    # M17a: bit 8 (FlagHasCallStream) is now a KNOWN flag and must
    # round-trip cleanly with ``hasCallStream = true``.
    let res = readMetaDat(craft(FlagHasCallStream))
    doAssert res.isOk,
      "bit 8 (FlagHasCallStream) must round-trip: " &
      (if res.isErr: res.error else: "ok")
    doAssert res.get().hasCallStream
    doAssert not res.get().hasColumnAwareSteps

  block:
    # M23a: bit 9 (FlagHasStepStream) is now a KNOWN flag and must
    # round-trip cleanly with ``hasStepStream = true``.  A real M23a
    # bundle sets it alongside ``FlagHasCallStream`` — assert both
    # round-trip together.
    let res = readMetaDat(craft(FlagHasStepStream))
    doAssert res.isOk,
      "bit 9 (FlagHasStepStream) must round-trip: " &
      (if res.isErr: res.error else: "ok")
    doAssert res.get().hasStepStream
    doAssert not res.get().hasCallStream

    let resBoth = readMetaDat(craft(FlagHasCallStream or FlagHasStepStream))
    doAssert resBoth.isOk,
      "bit 8 + bit 9 (call+step streams) must round-trip: " &
      (if resBoth.isErr: resBoth.error else: "ok")
    doAssert resBoth.get().hasCallStream
    doAssert resBoth.get().hasStepStream

  block:
    let res = readMetaDat(craft(FlagHasColumnAwareSteps or 0x400'u16))
    doAssert res.isErr,
      "bit 4 + bit 10 must reject because bit 10 is unknown"
    doAssert "unknown" in res.error,
      "rejection error must mention 'unknown'; got: " & res.error

  echo "PASS: test_meta_dat_strict_unknown_flag_rejection"


# Run all tests
test_meta_dat_uuidv7_generation()
test_meta_dat_uuidv7_ms_monotonic_sortable()
test_meta_dat_recording_id_required()
test_meta_dat_recording_id_malformed_rejected()
test_meta_dat_reader_rejects_missing_recording_id()
test_meta_dat_writer_mints_when_blank()
test_meta_dat_write_layout()
test_meta_dat_with_mcr_fields()
test_meta_dat_empty_fields()
test_meta_dat_roundtrip()
test_meta_dat_roundtrip_with_mcr()
test_meta_dat_roundtrip_with_filter_provenance()
test_meta_dat_roundtrip_empty_filter_provenance()
test_meta_dat_no_filter_provenance_omits_flag()
test_meta_dat_read_bad_magic()
test_meta_dat_read_too_short()
test_meta_dat_backward_compat()
test_meta_dat_openTrace_binary()
test_meta_dat_strict_unknown_flag_rejection()
