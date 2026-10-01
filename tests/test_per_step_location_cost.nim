## Resolving every step's location one call at a time costs about what one
## sequential decode of the same steps costs.
##
## `ct_reader_step_location` (the C ABI a host reads through) answers one step
## per call via `stepAbsoluteGlobalLineIndex` and `globalPositionSpace`. A
## host that walks a trace calls it once per step, so a per-call cost
## proportional to the chunk (decoding the whole containing chunk) or to the
## path count (rebuilding the position space) multiplies the walk by that
## size. Measured on a 150,000-iteration MIX-2 trace before the fix: 6.3 s
## through the C ABI against 3.4 ms for one sequential decode.
##
## Asserted: over a trace of 40,000 steps across 2,000 paths, resolving every
## step through `stepAbsoluteGlobalLineIndex` + `globalPositionSpace` costs
## under 10x one sequential decode through `step` (fastest of 3 each; the
## defect is ~1000x, so load cannot flip the verdict). And the per-call
## answers equal the sequential ones, step for step.
##
## No mocks: the real writer, the real reader, in memory.

import std/[monotimes, times, strutils]
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/global_line_index
import codetracer_trace_writer/step_encoding

const
  Steps = 40_000
  Paths = 2_000

proc buildTrace(): seq[byte] =
  var w = initMultiStreamWriter("", "per_step_cost").get()
  for i in 0 ..< Steps:
    let id = w.registerPath("/src/f" & $(i mod Paths) & ".nr").get()
    doAssert w.registerStep(id, uint64(1 + i mod 300), []).isOk
  doAssert w.close().isOk
  w.toBytes()

proc sequential(r: var NewTraceReader): seq[(int, uint64)] =
  let space = r.globalPositionSpace()
  var pos = 0'u64
  for i in 0'u64 ..< r.stepCount().get():
    let ev = r.step(i).get()
    case ev.kind
    of sekAbsoluteStep: pos = ev.globalLineIndex
    of sekDeltaStep: pos = uint64(int64(pos) + ev.lineDelta)
    else: discard
    result.add(space.tryResolve(pos).get())

proc perCall(r: var NewTraceReader): seq[(int, uint64)] =
  for i in 0'u64 ..< r.stepCount().get():
    let gli = r.stepAbsoluteGlobalLineIndex(i).get()
    result.add(r.globalPositionSpace().tryResolve(gli).get())

proc test_per_step_location_costs_about_a_sequential_decode() =
  let data = buildTrace()
  var seqBest, callBest = initDuration(days = 1)
  var seqAns, callAns: seq[(int, uint64)]
  for _ in 0 ..< 3:
    var r1 = openNewTraceFromBytes(data).get()
    let t1 = getMonoTime()
    seqAns = r1.sequential()
    seqBest = min(seqBest, getMonoTime() - t1)
    var r2 = openNewTraceFromBytes(data).get()
    let t2 = getMonoTime()
    callAns = r2.perCall()
    callBest = min(callBest, getMonoTime() - t2)
  doAssert seqAns.len == Steps and callAns == seqAns,
    "per-call locations differ from the sequential decode"
  let ratio = callBest.inNanoseconds.float / max(seqBest.inNanoseconds, 1).float
  echo "  sequential ", seqBest.inMicroseconds, " us; per call ",
    callBest.inMicroseconds, " us; ratio ", ratio.formatFloat(ffDecimal, 1)
  doAssert ratio < 10.0,
    "resolving each step by its own call took " & ratio.formatFloat(ffDecimal, 1) &
    "x one sequential decode (bound 10x): a per-call cost grows with the chunk " &
    "or with the path count"

when isMainModule:
  test_per_step_location_costs_about_a_sequential_decode()
  echo "PASS test_per_step_location_costs_about_a_sequential_decode"
