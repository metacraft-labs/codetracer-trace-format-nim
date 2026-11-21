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
import codetracer_trace_writer/varint

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
  var r = openStepMap(m).get()
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
  var r = openStepMap(m).get()
  let all = r.loadAll().get()
  doAssert all.len == expect.len
  for ln in all:
    doAssert expect[(ln.pathId, ln.line)] == ln.steps
  for key in [(0'u64, 3'u32), (1'u64, 29_998'u32), (2'u64, 15_002'u32)]:
    doAssert r.lookup(key[0], uint64(key[1])).get() == expect[key]
  # Every key, visited in an order that leaves and re-enters chunks, and the
  # keys between them, answer as `loadAll` does.
  var probe = 1'u64
  for _ in 0 ..< 20_000:
    probe = (probe * 7919 + 13) mod 30_001
    for path in 0'u64 .. 2:
      let got = r.lookup(path, probe).get()
      let want = expect.getOrDefault((path, uint32(probe)), @[])
      doAssert got == want, "lookup (" & $path & ", " & $probe & ")"
  echo "PASS chunks_close_after_the_record_that_reaches_64_KiB"

block the_loaded_index_holds_every_id_in_one_list:
  var b = initStepMapBuilder()
  for (path, line, step) in [(0'u64, 3'u64, 1'u64), (0, 3, 4), (0, 9, 2),
      (4, 1, 0), (4, 1, 5), (4, 1, 6)]:
    b.recordStep(path, line, step)
  let idx = openStepMap(b.serialize()).get().loadAll().get()
  doAssert idx.len == 3
  doAssert idx.ids == @[1'i64, 4, 2, 0, 5, 6]
  doAssert (idx.pathId(2), idx.line(2)) == (4'u64, 1'u32)
  doAssert @(idx.steps(0)) == @[1'i64, 4] and @(idx.steps(1)) == @[2'i64]
  doAssert @(idx.steps(2)) == @[0'i64, 5, 6]
  doAssert idx.find(0, 9) == 1 and idx.find(4, 1) == 2
  doAssert idx.find(0, 4) == -1 and idx.find(3, 1) == -1
  doAssert idx.find(4, 0) == 2, "line 0 is looked up as line 1"
  doAssert idx.find(0, 1'u64 shl 33) == -1
  doAssert idx[0] == (0'u64, 3'u32, @[1'i64, 4])
  echo "PASS the_loaded_index_holds_every_id_in_one_list"

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

block a_lookup_checks_its_whole_chunk:
  # One chunk, two line records; the SECOND has a run with gap 0, which the
  # spec has a reader refuse. A lookup of the first line, before the defect,
  # is refused too: the chunk is checked whole when a lookup inflates it.
  let content = @[0'u8, 3, 1, 1, 1,   0, 2, 1, 0, 1]
  var frame = newSeq[byte](int(ZSTD_compressBound(csize_t(content.len))))
  let flen = ZSTD_compress(addr frame[0], csize_t(frame.len),
    unsafeAddr content[0], csize_t(content.len), 3)
  doAssert ZSTD_isError(flen) == 0
  frame.setLen(int(flen))
  var m: seq[byte]
  proc put(m: var seq[byte], v: uint64, n: int) =
    for i in 0 ..< n: m.add(byte((v shr (8 * i)) and 0xff))
  m.put(0x53544D50'u64, 4); m.put(2, 2); m.put(1, 4); m.put(1, 4)
  m.put(2, 4); m.put(2, 8)
  m.put(0, 8); m.put(0, 8); m.put(3, 4)
  m.add(frame)
  var r = openStepMap(m).get()
  let first = r.lookup(0, 3)
  doAssert first.isErr and "gap 0" in first.error, $first
  doAssert r.lookup(0, 3).isErr, "a refused chunk is not held"
  echo "PASS a_lookup_checks_its_whole_chunk"

block the_writer_keys_exec_record_ids_and_line_0_as_1:
  var w = initMultiStreamWriter("", "stepmap").get()
  doAssert w.registerPath("/a.nim").isOk
  doAssert w.registerStep(0, 0, []).isOk          # record 0, line 0 -> 1
  doAssert w.registerThreadSwitch(1).isOk         # record 1, not a step
  doAssert w.registerStep(0, 1, []).isOk          # record 2
  doAssert w.registerStep(0, 4, []).isOk          # record 3
  doAssert w.close().isOk
  let m = readInternalFile(w.toBytes(), "step-map.ns").get()
  var r = openStepMap(m).get()
  doAssert r.lookup(0, 1).get() == @[0'i64, 2], $r.lookup(0, 1).get()
  doAssert r.lookup(0, 4).get() == @[3'i64]
  echo "PASS the_writer_keys_exec_record_ids_and_line_0_as_1"

block step_ids_past_int64_are_refused:
  # A step id list holds `int64`s, so a run whose ids pass `high(int64)` is
  # refused by name, in a list read (`loadAll`, a lookup of that line) and in
  # a record a lookup only steps over, rather than wrapping or stopping the
  # process. The controls end exactly at `high(int64)`, and a run whose
  # `gap * repeat` passes 2^64 is refused without the product overflowing.
  proc mapOf(lines: openArray[(uint32, seq[(uint64, uint64)])]): seq[byte] =
    ## A one-chunk map of path 0 with these lines and runs; the header counts
    ## what the runs add up to.
    var content: seq[byte]
    var prevLine = 0'u32
    var steps = 0'u64
    for i, (line, runs) in lines:
      encodeVarint(0, content)
      encodeVarint(uint64(if i == 0: line else: line - prevLine), content)
      prevLine = line
      var count = 0'u64
      for (_, rep) in runs: count += rep
      steps += count
      encodeVarint(count, content)
      for (gap, rep) in runs:
        encodeVarint(gap, content)
        encodeVarint(rep, content)
    var frame = newSeq[byte](int(ZSTD_compressBound(csize_t(content.len))))
    let flen = ZSTD_compress(addr frame[0], csize_t(frame.len),
      unsafeAddr content[0], csize_t(content.len), 3)
    doAssert ZSTD_isError(flen) == 0
    frame.setLen(int(flen))
    proc put(m: var seq[byte], v: uint64, n: int) =
      for i in 0 ..< n: m.add(byte((v shr (8 * i)) and 0xff))
    result.put(0x53544D50'u64, 4); result.put(2, 2); result.put(1, 4)
    result.put(1, 4); result.put(uint64(lines.len), 4); result.put(steps, 8)
    result.put(0, 8); result.put(0, 8); result.put(uint64(lines[0][0]), 4)
    result.add(frame)

  const top = 1'u64 shl 63            # the gap from -1 to high(int64)
  let atTop = mapOf([(3'u32, @[(top, 1'u64)])])
  doAssert openStepMap(atTop).get().loadAll().get()[0].steps == @[high(int64)]
  var r = openStepMap(atTop).get()
  doAssert r.lookup(0, 3).get() == @[high(int64)]
  let runToTop = mapOf([(3'u32, @[(1'u64, 5'u64), (top - 5, 1'u64)])])
  doAssert openStepMap(runToTop).get().loadAll().get()[0].steps ==
    @[0'i64, 1, 2, 3, 4, high(int64)]

  for (what, m) in [
      ("one past", mapOf([(3'u32, @[(top, 1'u64), (1'u64, 1'u64)])])),
      ("a run past", mapOf([(3'u32, @[(1'u64 shl 61, 5'u64)])])),
      ("a product past 2^64", mapOf([(3'u32, @[(1'u64 shl 62, 4'u64)])]))]:
    let all = openStepMap(m).get().loadAll()
    doAssert all.isErr and "pass" in all.error, what & ": " & $all
    var lr = openStepMap(m).get()
    let one = lr.lookup(0, 3)
    doAssert one.isErr and "pass" in one.error, what & ": " & $one

  # Line 3 passes; a lookup of line 4 steps over it and is refused.
  let over = mapOf([(3'u32, @[(top, 1'u64), (1'u64, 1'u64)]),
    (4'u32, @[(1'u64, 1'u64)])])
  var lo = openStepMap(over).get()
  let four = lo.lookup(0, 4)
  doAssert four.isErr and "pass" in four.error, $four
  echo "PASS step_ids_past_int64_are_refused"

echo "ALL PASS test_step_map_v2"
