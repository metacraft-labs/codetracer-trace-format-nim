## Registering paths interleaved with steps costs time linear in the number
## of paths.
##
## Every step addresses its line through the global line index, a prefix sum
## over the address counts of every registered path. A registration adds one
## file at the END of that space, so it needs one more prefix sum and changes
## none of the existing ones. An index rebuilt over every path after each
## registration costs O(paths) per registration and O(paths^2) over a trace,
## which is what a recorder that meets a new file every few steps pays.
##
## The assertion is on growth, not on a time: the same workload at N and 4N
## paths, taking the fastest of several repetitions of each so that a
## transient load on the machine inflates neither side. Linear work grows by
## about 4x; quadratic work by about 16x. The bound sits at 8x — the
## geometric midpoint — so a loaded machine has to distort the ratio by a
## factor of two in one direction before the verdict flips.
##
## Falsifiability: rebuild the whole index on every registration (the
## `gliDirty` flag set on append with nothing kept incrementally) and the
## ratio is well above 8.
##
## The second test asserts the addresses themselves: an index extended one
## registration at a time answers exactly what an index built in one pass
## over the same counts answers, across files of every sizing rule (sized by
## a line count, and the unsized `DefaultLinesPerFile` convention).
##
## No mocks: the real writer, writing a real container in memory, read back
## through the real reader.

import std/[monotimes, times, strutils]
import results
import codetracer_trace_types
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/global_line_index

proc registerPathsWithSteps(n: int): Duration =
  var w = initMultiStreamWriter("", "scaling").get()
  let t0 = getMonoTime()
  for i in 0 ..< n:
    let id = w.registerPath("/src/file_" & $i & ".nr").get()
    doAssert w.registerStep(id, 1, []).isOk
  result = getMonoTime() - t0
  doAssert w.close().isOk

proc fastest(n, reps: int): Duration =
  result = registerPathsWithSteps(n)
  for _ in 1 ..< reps:
    result = min(result, registerPathsWithSteps(n))

proc test_path_registration_is_linear_in_the_path_count() =
  const
    n = 4_000
    reps = 5
    bound = 8.0
  discard registerPathsWithSteps(n)  # warm the allocator and caches
  let small = fastest(n, reps)
  let large = fastest(4 * n, reps)
  let ratio = large.inNanoseconds.float / max(small.inNanoseconds, 1).float
  echo "  ", n, " paths: ", small.inMicroseconds, " us; ", 4 * n,
    " paths: ", large.inMicroseconds, " us; ratio ", ratio.formatFloat(ffDecimal, 2)
  doAssert ratio < bound,
    "registering 4x the paths took " & ratio.formatFloat(ffDecimal, 2) &
    "x as long (" & $small.inMicroseconds & " us -> " &
    $large.inMicroseconds & " us); linear work is ~4x and the bound is " &
    $bound & "x. Path registration is superlinear in the path count"

proc test_incremental_index_addresses_match_a_one_pass_build() =
  ## Files registered one at a time, each followed by a step at its last
  ## line, then steps back into earlier files: every address the writer
  ## emitted must be the one a one-pass prefix sum over the same counts gives,
  ## and must resolve on read to the (file, line) written.
  var w = initMultiStreamWriter("", "scaling").get()
  doAssert w.enableLineCountTable().isOk
  var counts: seq[uint64]
  var written: seq[(int, uint64)]
  for i in 0 ..< 200:
    let count = uint64(1 + (i * 37) mod 500)
    let id = w.registerPath("/src/f" & $i & ".nr", lineCount = count).get()
    doAssert id == uint64(i)
    counts.add(count)
    doAssert w.registerStep(id, count, []).isOk
    written.add((i, count))
  for i in countdown(199, 0, 13):
    doAssert w.registerStep(uint64(i), 1, []).isOk
    written.add((i, 1'u64))
  doAssert w.close().isOk

  let expected = buildGlobalLineIndex(counts)
  let r = openNewTraceFromBytes(w.toBytes())
  doAssert r.isOk, r.error
  var reader = r.get()
  let space = reader.globalPositionSpace()
  doAssert space.prefixSum == expected.prefixSum,
    "the reader's position space differs from a one-pass prefix sum"
  let n = reader.stepCount().get()
  doAssert n >= uint64(written.len),
    "read back " & $n & " steps, wrote " & $written.len
  # The written steps are the last `written.len` of the stream.
  let first = n - uint64(written.len)
  for k, (file, line) in written:
    let gli = reader.stepAbsoluteGlobalLineIndex(first + uint64(k)).get()
    doAssert gli == expected.globalIndex(file, line),
      "step " & $k & " (file " & $file & ", line " & $line & ") was written at " &
      $gli & "; a one-pass index puts it at " & $expected.globalIndex(file, line)
    doAssert space.tryResolve(gli).get() == (file, line)

when isMainModule:
  test_incremental_index_addresses_match_a_one_pass_build()
  echo "PASS test_incremental_index_addresses_match_a_one_pass_build"
  test_path_registration_is_linear_in_the_path_count()
  echo "PASS test_path_registration_is_linear_in_the_path_count"
