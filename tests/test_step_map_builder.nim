## Tests for the M26b prepopulated breakpoint index (`step-map.ns`).
##
## Two things are proven here:
##
## 1. The standalone `StepMapBuilder` serialises into the spec §4.1 `STMP`
##    layout and a Nim-side parser reads it back identically (Nim self
##    round-trip), with the byte stride / ordering the db-backend's
##    `StepMapNamespace::parse` expects.
## 2. A `.ct` produced by `MultiStreamTraceWriter` carries `step-map.ns` in the
##    CTFS container BY DEFAULT (line-only trace), and its `(path_id, line) ->
##    [step_id]` content equals the data the db-backend whole-table build would
##    derive (keyed by `unpack_global_line_index` of each step's gli).
##
## The authoritative write<->read loop (Nim writer -> Rust M26 consumer) lives in
## the db-backend's `m26_step_map_namespace_test.rs`; this file is the Nim-side
## guard that the bytes are well-formed and present.

import std/[tables, algorithm]
import results
import codetracer_trace_writer/step_map_builder
import codetracer_trace_writer/multi_stream_writer
import codetracer_ctfs/container

# ---------------------------------------------------------------------------
# A minimal, independent Nim STMP reader — mirrors the db-backend
# `StepMapNamespace::parse` byte layout so a Nim test can assert the writer's
# output without depending on the Rust crate.
# ---------------------------------------------------------------------------

type
  ParsedStepMap = object
    ## `path_id -> (line -> ascending step_ids)`.
    byPath: Table[uint64, Table[uint32, seq[int64]]]

proc readU16(buf: openArray[byte], off: int): uint16 =
  uint16(buf[off]) or (uint16(buf[off + 1]) shl 8)

proc readU32(buf: openArray[byte], off: int): uint32 =
  uint32(buf[off]) or (uint32(buf[off + 1]) shl 8) or
    (uint32(buf[off + 2]) shl 16) or (uint32(buf[off + 3]) shl 24)

proc readU64(buf: openArray[byte], off: int): uint64 =
  var v: uint64 = 0
  for i in 0 ..< 8:
    v = v or (uint64(buf[off + i]) shl (8 * i))
  v

proc readI64(buf: openArray[byte], off: int): int64 =
  cast[int64](readU64(buf, off))

proc parseStepMap(buf: seq[byte]): ParsedStepMap =
  ## Parse a `STMP` blob exactly as the M26 reader does (18-byte header,
  ## 20-byte path entries, 32-byte line entries, i64 step lists).
  result.byPath = initTable[uint64, Table[uint32, seq[int64]]]()
  doAssert buf.len >= 18, "STMP shorter than header"
  let magic = readU32(buf, 0)
  doAssert magic == StepMapMagic, "bad STMP magic: " & $magic
  let version = readU16(buf, 4)
  doAssert version == StepMapVersion, "bad STMP version: " & $version
  let pathCount = int(readU32(buf, 6))
  let pathTableOffset = int(readU64(buf, 10))

  for p in 0 ..< pathCount:
    let pbase = pathTableOffset + p * 20
    let pathId = readU64(buf, pbase)
    let lineCount = int(readU32(buf, pbase + 8))
    let linesOffset = int(readU64(buf, pbase + 12))

    var byLine = initTable[uint32, seq[int64]]()
    for l in 0 ..< lineCount:
      let lbase = linesOffset + l * 32
      let line = readU32(buf, lbase)
      let stepCount = int(readU32(buf, lbase + 4))
      let firstHint = readI64(buf, lbase + 8)
      let lastHint = readI64(buf, lbase + 16)
      let stepsOffset = int(readU64(buf, lbase + 24))

      var ids: seq[int64]
      for s in 0 ..< stepCount:
        ids.add(readI64(buf, stepsOffset + s * 8))
      if stepCount > 0:
        doAssert ids[0] == firstHint,
          "first hint mismatch on line " & $line
        doAssert ids[^1] == lastHint,
          "last hint mismatch on line " & $line
      byLine[line] = ids
    result.byPath[pathId] = byLine

# ---------------------------------------------------------------------------
# Test 1: standalone builder serialise -> parse round-trip.
# ---------------------------------------------------------------------------

proc test_builder_round_trip() =
  var b = initStepMapBuilder()
  # gli packs (path << 32) | line for line-only traces.  Record a non-trivial
  # spread across two paths so sort order and the path table are exercised.
  proc gli(path: uint64, line: uint64): uint64 = (path shl 32) or line

  # path 0, line 10: steps 2, 52, 102 (ascending)
  b.recordStep(gli(0, 10), 2)
  b.recordStep(gli(0, 10), 52)
  b.recordStep(gli(0, 10), 102)
  # path 0, line 11: step 3
  b.recordStep(gli(0, 11), 3)
  # path 2, line 7: steps 9, 8 (out of order on purpose -> serializer sorts)
  b.recordStep(gli(2, 7), 9)
  b.recordStep(gli(2, 7), 8)

  doAssert b.entryCount == 3, "entryCount: " & $b.entryCount

  let blob = b.serialize()
  let parsed = parseStepMap(blob)

  doAssert parsed.byPath[0][10] == @[2'i64, 52, 102]
  doAssert parsed.byPath[0][11] == @[3'i64]
  doAssert parsed.byPath[2][7] == @[8'i64, 9]  # sorted ascending
  doAssert not parsed.byPath.hasKey(5'u64)

  echo "PASS: test_builder_round_trip"

# ---------------------------------------------------------------------------
# Test 2: empty builder produces a well-formed zero-path blob.
# ---------------------------------------------------------------------------

proc test_empty_builder() =
  let b = initStepMapBuilder()
  doAssert b.entryCount == 0
  let blob = b.serialize()
  doAssert blob.len == 18, "empty STMP must be just the header"
  let parsed = parseStepMap(blob)
  doAssert parsed.byPath.len == 0
  echo "PASS: test_empty_builder"

# ---------------------------------------------------------------------------
# Test 3: writer emits step-map.ns by default (line-only trace), content
# matches the gli-derived (path, line) -> [step_id] map.
# ---------------------------------------------------------------------------

proc test_writer_emits_step_map() =
  let writerRes = initMultiStreamWriter("test_sm.ct", "step_map_test")
  doAssert writerRes.isOk, "initMultiStreamWriter failed: " & writerRes.error
  var w = writerRes.get()

  let p0 = w.registerPath("/src/main.py")
  doAssert p0.isOk and p0.get() == 0

  const NumSteps = 300
  const NumLines = 7
  # Build the expected (line -> step_ids) map, with lines 1..7 (gli == line for
  # path 0).
  var expected: array[NumLines, seq[int64]]
  for i in 0 ..< NumSteps:
    let lineIdx = i mod NumLines
    let line = uint64(lineIdx + 1)
    expected[lineIdx].add(int64(i))  # stepCount == i at registration
    let res = w.registerStep(0, line, @[])
    doAssert res.isOk, "registerStep " & $i & " failed: " & res.error

  let closeRes = w.close()
  doAssert closeRes.isOk, "close failed: " & closeRes.error

  let bytes = w.toBytes()

  # The container must carry step-map.ns by default.
  doAssert hasInternalFile(bytes, "step-map.ns"),
    "writer must emit step-map.ns by default on the line-only path"

  let blobRes = readInternalFile(bytes, "step-map.ns")
  doAssert blobRes.isOk, "reading step-map.ns failed: " & blobRes.error
  let parsed = parseStepMap(blobRes.get())

  # Path 0 must carry exactly NumLines entries.
  doAssert parsed.byPath.hasKey(0'u64), "path 0 missing from step-map"
  doAssert parsed.byPath[0].len == NumLines,
    "expected " & $NumLines & " lines, got " & $parsed.byPath[0].len

  for lineIdx in 0 ..< NumLines:
    let line = uint32(lineIdx + 1)
    var exp = expected[lineIdx]
    exp.sort()
    doAssert parsed.byPath[0][line] == exp,
      "line " & $line & " step set mismatch"

  w.closeCtfs()
  echo "PASS: test_writer_emits_step_map"

# ---------------------------------------------------------------------------
# Test 4: column-aware writers suppress step-map.ns (gated, documented).
# ---------------------------------------------------------------------------

proc test_column_aware_suppresses_step_map() =
  let writerRes = initMultiStreamWriter("test_sm_col.ct", "step_map_col_test")
  doAssert writerRes.isOk
  var w = writerRes.get()
  w.enableColumnAwareSteps()

  let p0 = w.registerPath("/src/main.py", @[10'u32, 10, 10])
  doAssert p0.isOk

  for i in 0 ..< 10:
    let res = w.registerStep(0, uint64((i mod 3) + 1), @[])
    doAssert res.isOk

  let closeRes = w.close()
  doAssert closeRes.isOk

  let bytes = w.toBytes()
  doAssert not hasInternalFile(bytes, "step-map.ns"),
    "column-aware traces must NOT carry step-map.ns"

  w.closeCtfs()
  echo "PASS: test_column_aware_suppresses_step_map"

when isMainModule:
  test_builder_round_trip()
  test_empty_builder()
  test_writer_emits_step_map()
  test_column_aware_suppresses_step_map()
  echo "All step-map builder tests passed."
