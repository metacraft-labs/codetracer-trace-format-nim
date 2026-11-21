{.push raises: [].}

## The production writer streaming to a real file: the cost of keeping a
## recording readable while it is written (`ctfs-container.md` §6,
## "Durability: a writer publishes every sealed chunk").
##
## Drives `MultiStreamTraceWriter` through the recorder-facing calls on the
## eight-phase mix the cross-writer parity test uses (step, call, return,
## value, path, function, a no-op slot, thread switch), and reports wall-clock
## records per second and the container size. Wall clock rather than CPU time,
## because the point is the I/O. Machine-dependent: part of `bench`, never of
## `test`.
##
## Must be compiled with -d:release.

import std/[monotimes, times, os, strutils]
import results
import codetracer_trace_writer/multi_stream_writer

const
  Iterations = 400_000
  PathCount = 50
  Samples = 5

proc runOnce(path: string): (float, int) =
  var w = initMultiStreamWriter(path, "bench_stream").get()
  let start = getMonoTime()
  var paths: seq[uint64]
  for i in 0 ..< PathCount:
    paths.add(w.registerPath("/src/file_" & $i & ".nr").get())
  for i in 0 ..< 100:
    discard w.registerFunction("fn_" & $i).get()
  var pending: seq[VariableValue]
  for i in 0 ..< Iterations:
    case i mod 8
    of 0:
      doAssert w.registerStep(paths[i mod PathCount],
        uint64(i mod 1000 + 1), pending).isOk
      pending.setLen(0)
    of 1: doAssert w.registerCall(uint64(i mod 100), []).isOk
    of 2: doAssert w.registerReturn(@[0xFF'u8]).isOk
    of 3:
      let vn = w.registerVarname("var_" & $(i mod 200)).get()
      pending.add(VariableValue(varnameId: vn,
        data: @[0xA2'u8, 0x61, 0x69, byte(i and 0x17), 0x61, 0x74, 0x07]))
    of 4: discard w.registerPath("/src/late_" & $i & ".nr").get()
    of 5: discard w.registerFunction("func_" & $i).get()
    of 6: discard
    else: doAssert w.registerThreadSwitch(uint64(i mod 4)).isOk
  doAssert w.close().isOk
  doAssert w.closeCtfs().isOk
  let elapsed = (getMonoTime() - start).inNanoseconds.float / 1e9
  var size = 0
  try: size = int(getFileSize(path))
  except OSError: discard
  (elapsed, size)

proc main() =
  let path = getTempDir() / ("bench_streaming_writer_" &
    $getCurrentProcessId() & ".ct")
  var best = 1e300
  var size = 0
  for s in 0 ..< Samples:
    let (t, sz) = runOnce(path)
    if t < best: best = t
    size = sz
  try: removeFile(path)
  except OSError: discard
  echo "{\"benchmark\": \"streaming_writer_mix\", \"iterations\": " &
    $Iterations & ", \"best_sec\": " & formatFloat(best, ffDecimal, 4) &
    ", \"calls_per_sec\": " & formatFloat(float(Iterations) / best,
      ffDecimal, 0) & ", \"container_bytes\": " & $size & "}"

main()
