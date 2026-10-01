## `step-map.ns` version 2 (`internal-files.md` §"`step-map.ns`"): run-length
## gaps under zstd, keys delta-coded, chunked by the normative 64 KiB rule, and
## a step registered at line 0 keyed under line 1 (§"Global Line Index",
## "Line 0 is line 1, everywhere").
##
## The byte layout is pinned by hand against the specification's own example
## (`tools/ctfs-measure/src/stepmap.rs`
## `version_2_line_record_is_the_specified_bytes`), and the reader's refusals
## are driven with real maps that were damaged on purpose. No mocks.

import std/[strutils, tables]
import results
import codetracer_ctfs
import codetracer_trace_writer/step_map_builder
import codetracer_trace_writer/multi_stream_writer

proc rdU32(d: openArray[byte], o: int): uint32 =
  for i in 0 ..< 4: result = result or (uint32(d[o + i]) shl (8 * i))

proc rdU64(d: openArray[byte], o: int): uint64 =
  for i in 0 ..< 8: result = result or (uint64(d[o + i]) shl (8 * i))

proc inflate(frame: openArray[byte]): seq[byte] =
  let n = ZSTD_getFrameContentSize(unsafeAddr frame[0], csize_t(frame.len))
  doAssert n != ZSTD_CONTENTSIZE_UNKNOWN and n != ZSTD_CONTENTSIZE_ERROR,
    "every frame declares its content size"
  result = newSeq[byte](int(n))
  let got = ZSTD_decompress(addr result[0], csize_t(n), unsafeAddr frame[0],
    csize_t(frame.len))
  doAssert ZSTD_isError(got) == 0 and int(got) == int(n)

block the_specified_line_record_bytes:
  var b = initStepMapBuilder()
  for s in [0'u64, 2, 4, 6, 7]:
    b.recordStep(0, 3, s)
  let m = b.serialize()
  doAssert rdU32(m, 0) == 0x53544D50'u32
  doAssert m[4] == 2 and m[5] == 0, "version 2"
  doAssert rdU32(m, 6) == 1, "one chunk"
  doAssert rdU32(m, 10) == 1 and rdU32(m, 14) == 1 and rdU64(m, 18) == 5
  doAssert rdU64(m, 26) == 0 and rdU64(m, 34) == 0 and rdU32(m, 42) == 3
  let content = inflate(m.toOpenArray(46, m.high))
  #        dp line count (gap,rep) (gap,rep) (gap,rep)
  doAssert content == @[0'u8, 3, 5, 1, 1, 2, 3, 1, 1], $content
  let all = openStepMap(m).get().loadAll().get()
  doAssert all.len == 1 and all[0].steps == @[0'i64, 2, 4, 6, 7]
  echo "PASS the_specified_line_record_bytes"

block keys_are_delta_coded_and_line_0_is_line_1:
  var b = initStepMapBuilder()
  b.recordStep(2, 0, 4)     # line 0 -> keyed under line 1
  b.recordStep(2, 1, 9)
  b.recordStep(2, 7, 5)
  b.recordStep(5, 3, 6)
  let m = b.serialize()
  let content = inflate(m.toOpenArray(46, m.high))
  # (2,1): first record: dp 0, line 1, count 2, runs (5,1) (5,1) -> gaps
  #        from -1: 4+1=5, then 9-4=5 -> one run (5, 2)
  # (2,7): dp 0, line delta 6, count 1, run (6,1)
  # (5,3): dp 3, line 3 (absolute after a path change), count 1, run (7,1)
  doAssert content == @[0'u8, 1, 2, 5, 2, 0, 6, 1, 6, 1, 3, 3, 1, 7, 1],
    $content
  let r = openStepMap(m).get()
  doAssert r.pathCount == 2 and r.lineCount == 3 and r.stepCount == 4
  doAssert r.lookup(2, 0).get() == @[4'i64, 9],
    "a lookup of line 0 is a lookup of line 1"
  doAssert r.lookup(2, 1).get() == @[4'i64, 9]
  doAssert r.lookup(5, 3).get() == @[6'i64]
  doAssert r.lookup(5, 4).get().len == 0
  doAssert r.lookup(0, 1).get().len == 0
  echo "PASS keys_are_delta_coded_and_line_0_is_line_1"

block chunks_close_after_the_record_that_reaches_64_KiB:
  # 30,000 lines with irregular step ids, so each record is several bytes and
  # the content passes 64 KiB more than once.
  var b = initStepMapBuilder()
  var expect = initTable[(uint64, uint32), seq[int64]]()
  var step = 0'u64
  for line in 1'u64 .. 30_000:
    for k in 0 ..< 3:
      step += 1 + (line * 7 + uint64(k) * 13) mod 1000
      b.recordStep(line mod 3, line, step)
      expect.mgetOrPut((line mod 3, uint32(line)), @[]).add(int64(step))
  let m = b.serialize()
  let n = int(rdU32(m, 6))
  doAssert n >= 2, "expected several chunks, got " & $n
  let tableEnd = 26 + 20 * n
  for i in 0 ..< n:
    let s = tableEnd + int(rdU64(m, 26 + 20 * i))
    let e = if i + 1 < n: tableEnd + int(rdU64(m, 26 + 20 * (i + 1))) else: m.len
    let raw = inflate(m.toOpenArray(s, e - 1))
    if i + 1 < n:
      doAssert raw.len >= 65536, "chunk " & $i & " closed early at " & $raw.len
  let r = openStepMap(m).get()
  let all = r.loadAll().get()
  doAssert all.len == expect.len
  for ln in all:
    doAssert expect[(ln.pathId, ln.line)] == ln.steps
  for key in [(0'u64, 3'u32), (1'u64, 29_998'u32), (2'u64, 15_002'u32)]:
    doAssert r.lookup(key[0], uint64(key[1])).get() == expect[key]
  echo "PASS chunks_close_after_the_record_that_reaches_64_KiB"

block an_empty_map_is_the_header_alone:
  let m = initStepMapBuilder().serialize()
  doAssert m.len == 26
  doAssert rdU32(m, 6) == 0 and rdU32(m, 10) == 0 and rdU64(m, 18) == 0
  doAssert openStepMap(m).get().loadAll().get().len == 0
  echo "PASS an_empty_map_is_the_header_alone"

block readers_refuse_damaged_maps:
  var b = initStepMapBuilder()
  for s in [0'u64, 2, 4]:
    b.recordStep(0, 3, s)
  var m = b.serialize()
  var v1 = m
  v1[4] = 1
  let rv = openStepMap(v1)
  doAssert rv.isErr and "version 1" in rv.error, $rv
  var badCount = m
  badCount[18] = 4    # header says 4 steps, the lists hold 3
  let rc = openStepMap(badCount).get().loadAll()
  doAssert rc.isErr and "header" in rc.error, $rc
  var badKey = m
  badKey[42] = 9      # chunk table says the first line is 9, the record 3
  let rk = openStepMap(badKey).get().loadAll()
  doAssert rk.isErr and "table key" in rk.error, $rk
  echo "PASS readers_refuse_damaged_maps"

block the_writer_keys_exec_record_ids_and_line_0_as_1:
  var w = initMultiStreamWriter("", "stepmap").get()
  doAssert w.registerPath("/a.nim").isOk
  doAssert w.registerStep(0, 0, []).isOk          # record 0, line 0 -> 1
  doAssert w.registerThreadSwitch(1).isOk         # record 1, not a step
  doAssert w.registerStep(0, 1, []).isOk          # record 2
  doAssert w.registerStep(0, 4, []).isOk          # record 3
  doAssert w.close().isOk
  let m = readInternalFile(w.toBytes(), "step-map.ns").get()
  let r = openStepMap(m).get()
  doAssert r.lookup(0, 1).get() == @[0'i64, 2], $r.lookup(0, 1).get()
  doAssert r.lookup(0, 4).get() == @[3'i64]
  echo "PASS the_writer_keys_exec_record_ids_and_line_0_as_1"

echo "ALL PASS test_step_map_v2"
