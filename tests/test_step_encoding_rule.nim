## `steps.dat`'s normative encoding rule (`trace-events.md` §"Encoding Rules"):
##
## 1. the first POSITION record of each chunk is an `AbsoluteStep`, whatever
##    records precede it in the chunk (a chunk that opens with a thread record
##    still anchors its first position);
## 2. otherwise a delta when the varint of `zigzag(p - cursor)` is strictly
##    shorter than the varint of `p`;
## 3. otherwise an `AbsoluteStep` — a tie goes to the absolute.
##
## and the reader half: a `DeltaStep` before a chunk's first `AbsoluteStep` is
## refused, naming the chunk, never resolved against 0 or a carried cursor.
##
## No mocks: real writer, real container, real reader.

import std/strutils
import results
import codetracer_ctfs
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/exec_stream
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/meta_dat

const ChunkSize = 8

proc writeTrace(body: proc (w: var MultiStreamTraceWriter)): seq[byte] =
  var w = initMultiStreamWriter("", "steps_rule", chunkSize = ChunkSize).get()
  doAssert w.registerPath("/a.nim").isOk   # path 0: lines at 0 .. 99_999
  body(w)
  doAssert w.close().isOk
  w.toBytes()

proc chunkEvents(img: seq[byte], chunk: int): seq[StepEvent] =
  var r = initExecStreamReader(img).get()
  discard r.readChunkEvents(chunk, result).get()

proc kinds(evs: seq[StepEvent]): string =
  for e in evs:
    result.add(case e.kind
      of sekAbsoluteStep: "A"
      of sekDeltaStep: "D"
      of sekDeltaColumn: "C"
      of sekThreadSwitch: "T"
      else: "?")

block a_chunk_opening_with_a_thread_record_anchors_its_first_position:
  # Records 0..7 fill chunk 0; chunk 1 opens with a thread switch and then a
  # step one line further on, whose delta fits a single byte.
  let img = writeTrace(proc (w: var MultiStreamTraceWriter) =
    for i in 0 ..< ChunkSize:
      doAssert w.registerStep(0, uint64(1000 + i), []).isOk
    doAssert w.registerThreadSwitch(2).isOk
    doAssert w.registerStep(0, 1008, []).isOk
    doAssert w.registerStep(0, 1009, []).isOk)
  let c1 = chunkEvents(img, 1)
  doAssert kinds(c1) == "TAD",
    "chunk 1 must anchor its first POSITION record, got " & kinds(c1)
  doAssert c1[1].globalLineIndex == 1007   # line 1008 of path 0
  echo "PASS a_chunk_opening_with_a_thread_record_anchors_its_first_position"

block each_record_takes_the_shorter_encoding_and_a_tie_goes_to_absolute:
  # Positions (line - 1): 0, 63, 64, 200, 210, 8200, 8100, 100.
  #   0     first position          -> A
  #   63    d=63  zz=126 (1B) vs 1B -> tie -> A
  #   64    d=1   (1B) vs 64 (1B)   -> tie -> A
  #   200   d=136 zz=272 (2B) vs 2B -> tie -> A
  #   210   d=10  (1B) vs 2B        -> D
  #   8200  d=7990 zz=15980 (2B) vs 8200 (2B) -> tie -> A
  #   8100  d=-100 zz=199 (2B) vs 2B -> tie -> A
  #   100   d=-8000 zz=15999 (2B) vs 1B -> A
  let img = writeTrace(proc (w: var MultiStreamTraceWriter) =
    for line in [1'u64, 64, 65, 201, 211, 8201, 8101, 101]:
      doAssert w.registerStep(0, line, []).isOk)
  let c0 = chunkEvents(img, 0)
  doAssert kinds(c0) == "AAAADAAA", "got " & kinds(c0)
  doAssert c0[4].lineDelta == 10
  echo "PASS each_record_takes_the_shorter_encoding_and_a_tie_goes_to_absolute"

block a_delta_is_written_only_when_strictly_shorter:
  # Large positions: 1_000_000 then 1_000_100 (delta 100 -> 2B vs 3B) -> D;
  # then 1_000_100 + 70 (delta 70: zz=140 -> 2B vs 3B) -> D;
  # then 1_000_170 - 5 (d=-5: 1B vs 3B) -> D.
  var w = initMultiStreamWriter("", "steps_rule", chunkSize = ChunkSize).get()
  doAssert w.registerPath("/a.nim").isOk
  doAssert w.registerPath("/b.nim").isOk
  doAssert w.registerPath("/c.nim").isOk
  # path 2 starts at 200_000 in the 100_000-per-file line space.
  for line in [1'u64, 101, 171, 166]:
    doAssert w.registerStep(2, line, []).isOk
  doAssert w.close().isOk
  let c0 = chunkEvents(w.toBytes(), 0)
  doAssert kinds(c0) == "ADDD", "got " & kinds(c0)
  doAssert c0[3].lineDelta == -5
  echo "PASS a_delta_is_written_only_when_strictly_shorter"

block readers_refuse_a_delta_before_the_chunks_anchor:
  # A chunk's resolution, and the same chunk with its anchor replaced by a
  # delta.
  var c = createCtfs()
  var ew = initExecStreamWriter(c, ChunkSize).get()
  for i in 0 ..< ChunkSize:
    doAssert c.writeEvent(ew, StepEvent(kind: sekAbsoluteStep,
      globalLineIndex: uint64(10 + i))).isOk
  doAssert c.flush(ew).isOk
  var img = c.toBytes()
  var r = initExecStreamReader(img).get()
  var evs: seq[StepEvent]
  discard r.readChunkEvents(0, evs).get()
  var positions: seq[uint64]
  doAssert resolveChunkPositions(evs, 0, positions).isOk
  doAssert positions[^1] == 17
  # The same chunk, its anchor replaced by a delta, must be refused by name.
  evs[0] = StepEvent(kind: sekDeltaStep, lineDelta: 3)
  let refused = resolveChunkPositions(evs, 0, positions)
  doAssert refused.isErr, "a delta before the chunk's anchor was resolved"
  doAssert "chunk 0" in refused.error and "AbsoluteStep" in refused.error,
    refused.error
  echo "PASS readers_refuse_a_delta_before_the_chunks_anchor"

block the_trace_reader_refuses_an_unanchored_chunk:
  # A container written by a writer that anchors only a chunk's first RECORD:
  # chunk 1 is [ThreadSwitch, DeltaStep]. Built with raw step events through
  # the exec stream, whose rule would re-anchor it, so the chunk bytes are
  # assembled here instead.
  var c = createCtfs()
  var ew = initExecStreamWriter(c, 2).get()
  doAssert c.writeEvent(ew, StepEvent(kind: sekAbsoluteStep,
    globalLineIndex: 5)).isOk
  doAssert c.writeEvent(ew, StepEvent(kind: sekAbsoluteStep,
    globalLineIndex: 6)).isOk
  doAssert c.flush(ew).isOk
  # chunk 1, hand-built: ThreadSwitch(1), DeltaStep(+1)
  var raw: seq[byte]
  encodeStepEvent(StepEvent(kind: sekThreadSwitch, threadId: 1), raw)
  encodeStepEvent(StepEvent(kind: sekDeltaStep, lineDelta: 1), raw)
  var frame = newSeq[byte](int(ZSTD_compressBound(csize_t(raw.len))))
  let n = ZSTD_compress(addr frame[0], csize_t(frame.len), addr raw[0],
    csize_t(raw.len), 3)
  frame.setLen(int(n))
  let datLen = readInternalFile(c.toBytes(), "steps.dat").get().len
  # Rebuild the container with the hand-built chunk appended.
  var c2 = createCtfs()
  var d2 = c2.addFile("steps.dat").get()
  var i2 = c2.addFile("steps.idx").get()
  var dat0 = readInternalFile(c.toBytes(), "steps.dat").get()
  var idx0 = readInternalFile(c.toBytes(), "steps.idx").get()
  doAssert c2.writeToFile(d2, dat0).isOk
  doAssert c2.writeToFile(d2, frame).isOk
  var off: array[8, byte]
  let o = uint64(datLen)
  for k in 0 ..< 8: off[k] = byte((o shr (8 * k)) and 0xFF)
  doAssert c2.writeToFile(i2, idx0).isOk
  doAssert c2.writeToFile(i2, off).isOk
  var m2 = c2.addFile("meta.dat").get()
  doAssert c2.writeMetaDat(m2, TraceMetadata(
    recordingId: "0192f8a0-1234-7abc-8def-0123456789ab", program: "p"),
    hasStepStream = true).isOk
  var nr = openNewTraceFromBytes(c2.toBytes()).get()
  doAssert nr.stepAbsoluteGlobalLineIndex(1).get() == 6
  let bad = nr.stepAbsoluteGlobalLineIndex(3)
  doAssert bad.isErr, "an unanchored delta resolved to " & $bad.get()
  doAssert "chunk 1" in bad.error, bad.error
  var bulk = newSeq[uint64](4)
  let badBulk = nr.stepAbsoluteGlobalLineIndices(0, 4, bulk)
  doAssert badBulk.isErr and "chunk 1" in badBulk.error, $badBulk
  echo "PASS the_trace_reader_refuses_an_unanchored_chunk"

echo "ALL PASS test_step_encoding_rule"
