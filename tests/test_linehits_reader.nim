## The READ side of `linehits.tc`.
##
## `test_linehits_builder.nim` covers the accumulator and its own in-memory
## lookups; this covers opening the file back out of a finished container,
## which is what a consumer that did not write the trace has to do. The two are
## separate because the builder's `lookupHits` answers from the `Table` it
## filled during recording — it never touches the serialised B-tree, so a
## container whose `linehits.tc` was wrong would still satisfy it.

import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/linehits_reader
import codetracer_trace_writer/value_stream

const
  RecordingId = "0192f8a0-1234-7abc-8def-0123456789ae"
  PathA = "/lh/a.nim"
  PathB = "/lh/b.nim"

proc buildTrace(withLinehits: bool): seq[byte] =
  var wr = initMultiStreamWriter("/lh/probe.ct", "linehits_probe",
                                 recordingId = RecordingId)
  doAssert wr.isOk, wr.error
  var w = wr.get()
  if withLinehits:
    w.enableLinehits()
  doAssert w.registerPath(PathA).isOk
  doAssert w.registerPath(PathB).isOk
  doAssert w.registerFunction("main").isOk
  doAssert w.registerVarname("v").isOk
  doAssert w.registerType("int64").isOk

  # Two files, a handful of lines each, revisited so several positions carry
  # more than one step id and the varint list is not always one element.
  for i in 0 ..< 60:
    let pathId = uint64(i mod 2)
    let line = uint64(i mod 3) + 1
    doAssert w.registerStep(pathId, line, newSeq[VariableValue]()).isOk
  doAssert w.close().isOk
  w.toBytes()

proc expectedHits(pathId: uint64, line: uint64): seq[uint64] =
  for i in 0 ..< 60:
    if uint64(i mod 2) == pathId and uint64(i mod 3) + 1 == line:
      result.add(uint64(i))

proc test_reads_back_every_recorded_hit() =
  let data = buildTrace(withLinehits = true)
  doAssert hasLinehits(data), "container should carry linehits.tc"
  let rRes = initLinehitsReader(data)
  doAssert rRes.isOk, "initLinehitsReader: " & rRes.error
  let r = rRes.get()

  let keysRes = r.positions()
  doAssert keysRes.isOk, keysRes.error
  let keys = keysRes.get()

  # Every (path, line) the recorder visited must be present exactly once, and
  # its hit list must be the step ids in observation order.
  var matched = 0
  for pathId in 0'u64 ..< 2'u64:
    for line in 1'u64 .. 3'u64:
      let want = expectedHits(pathId, line)
      doAssert want.len > 0
      # The reader addresses by global position index, which is what the
      # writer keyed on; find it by matching hit lists rather than by
      # recomputing the writer's index arithmetic here.
      var found = false
      for k in keys:
        let got = r.hits(k)
        doAssert got.isOk, got.error
        if got.get() == want:
          found = true
          break
      doAssert found,
        "no indexed position carries the hits for path " & $pathId &
        " line " & $line & ": " & $want
      matched += 1
  doAssert matched == 6

  doAssert r.positionCount() == uint64(keys.len),
    "positionCount " & $r.positionCount() & " disagrees with the key walk " &
    $keys.len
  doAssert r.positionCount() == 6'u64,
    "expected 6 distinct positions, got " & $r.positionCount()

  echo "PASS: test_reads_back_every_recorded_hit"

proc test_unexecuted_position_is_absent() =
  ## The index is sparse. A position nobody executed must come back as absent
  ## rather than as a neighbour's list — the latter reads as a real answer.
  let data = buildTrace(withLinehits = true)
  let r = initLinehitsReader(data).get()
  let keysRes = r.positions()
  doAssert keysRes.isOk
  var maxKey = 0'u64
  for k in keysRes.get():
    if k > maxKey: maxKey = k
  doAssert r.hits(maxKey + 1_000_000'u64).isErr,
    "an unexecuted position answered with a hit list"
  echo "PASS: test_unexecuted_position_is_absent"

proc test_trace_without_linehits() =
  ## A recorder that never called `enableLinehits` produces no `linehits.tc`,
  ## which is the pre-extension default and not a corruption.
  let data = buildTrace(withLinehits = false)
  doAssert not hasLinehits(data),
    "a trace with linehits disabled must not carry linehits.tc"
  doAssert initLinehitsReader(data).isErr,
    "opening an absent linehits.tc must fail rather than yield an empty index"
  echo "PASS: test_trace_without_linehits"

when isMainModule:
  test_reads_back_every_recorded_hit()
  test_unexecuted_position_is_absent()
  test_trace_without_linehits()
  echo "ALL PASS: test_linehits_reader"
