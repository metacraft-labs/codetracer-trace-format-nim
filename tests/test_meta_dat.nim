{.push raises: [].}

## Tests for binary meta.dat writer.

import std/options
import results
import codetracer_ctfs
import codetracer_trace_types
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/varint

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


# Run all tests
test_meta_dat_write_layout()
test_meta_dat_with_mcr_fields()
test_meta_dat_empty_fields()
