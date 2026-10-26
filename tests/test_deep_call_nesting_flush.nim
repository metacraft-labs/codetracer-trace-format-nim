## Writing a deeply nested call chain costs time near-linear in its depth.
##
## A call's record is buffered at its return and the buffer reaches the call
## stream, in call_key (entry) order, once the root call returns. Returns of a
## recursion arrive innermost first, so the buffer holds its keys in strictly
## DESCENDING order — the worst case for an order-restoring pass that moves
## records one position at a time: N nested calls cost about N^2/2 record
## moves, which at the depth of a real recursion (a recorder tracing a
## 100k-deep BEAM recursion) is hours of writer time inside one return.
##
## The first test asserts on growth, not on a time: the same recursion at N
## and 4N depth, taking the fastest of several repetitions of each so that a
## transient load on the machine inflates neither side. Near-linear work
## grows by about 4-5x; quadratic work by about 16x. The bound sits at 9x, so
## a loaded machine has to distort the ratio by a factor of almost two before
## the verdict flips.
##
## The second test is the depth recorders actually hit, 100k nested calls,
## under a wall-clock bound generous enough for a loaded CI host and far
## below what quadratic work at that depth takes.
##
## Falsifiability: restore the order with an insertion sort over the buffered
## (key, record) pairs and the ratio is well above 9 and the 100k case does
## not finish within its bound.
##
## The third test asserts what the writer wrote: recursion mixed with fan-out
## and several top-level calls, read back through the reader, has every record
## at the position of its entry-order key with the parent, depth and children
## the entry order gives it.
##
## No mocks: the real writer, writing a real container in memory, read back
## through the real reader.

import std/[monotimes, times, strutils]
import results
import codetracer_trace_types
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader

proc writeRecursion(depth: int): Duration =
  ## One step per frame on the way down and one on the way up: the shape a
  ## recorder emits for a non-tail recursion.
  var w = initMultiStreamWriter("", "deep-recursion").get()
  let path = w.registerPath("/src/rec.erl", lineCount = 10).get()
  let fn = w.registerFunctionAt("/src/rec.erl", 1, "rec").get()
  let t0 = getMonoTime()
  for _ in 0 ..< depth:
    doAssert w.registerStep(path, 2, []).isOk
    doAssert w.registerCall(fn, []).isOk
  for _ in 0 ..< depth:
    doAssert w.registerStep(path, 3, []).isOk
    doAssert w.registerReturn().isOk
  result = getMonoTime() - t0
  doAssert w.close().isOk

proc fastest(depth, reps: int): Duration =
  result = writeRecursion(depth)
  for _ in 1 ..< reps:
    result = min(result, writeRecursion(depth))

proc test_nested_call_flush_is_not_quadratic_in_depth() =
  const
    n = 5_000
    reps = 3
    bound = 9.0
  discard writeRecursion(n)  # warm the allocator and caches
  let small = fastest(n, reps)
  let large = fastest(4 * n, reps)
  let ratio = large.inNanoseconds.float / max(small.inNanoseconds, 1).float
  echo "  depth ", n, ": ", small.inMicroseconds, " us; depth ", 4 * n, ": ",
    large.inMicroseconds, " us; ratio ", ratio.formatFloat(ffDecimal, 2)
  doAssert ratio < bound,
    "writing a 4x deeper recursion took " & ratio.formatFloat(ffDecimal, 2) &
    "x as long (" & $small.inMicroseconds & " us -> " &
    $large.inMicroseconds & " us); near-linear work is ~4-5x and the bound is " &
    $bound & "x. Flushing nested calls is superlinear in the nesting depth"

proc test_a_100k_deep_recursion_is_written_in_seconds() =
  const
    depth = 100_000
    boundSeconds = 30
  let took = writeRecursion(depth)
  echo "  depth ", depth, ": ", took.inMilliseconds, " ms"
  doAssert took < initDuration(seconds = boundSeconds),
    "writing a " & $depth & "-deep recursion took " & $took.inMilliseconds &
    " ms; the bound is " & $boundSeconds & " s"

type Expected = object
  parent: int64
  depth: uint32
  children: seq[uint64]

proc test_records_land_at_their_entry_order_key() =
  ## Two top-level calls, each a recursion whose every frame also makes two
  ## leaf calls before descending, plus a top-level leaf. The model below
  ## assigns keys in entry order, which is the order the call stream must hold.
  var w = initMultiStreamWriter("", "mixed-calls").get()
  let path = w.registerPath("/src/mixed.erl", lineCount = 10).get()
  let fn = w.registerFunctionAt("/src/mixed.erl", 1, "f").get()
  var model: seq[Expected]
  var stack: seq[uint64]
  proc enter(w: var MultiStreamTraceWriter) =
    let key = uint64(model.len)
    let parent = if stack.len > 0: int64(stack[^1]) else: -1'i64
    if stack.len > 0:
      model[int(stack[^1])].children.add(key)
    model.add(Expected(parent: parent, depth: uint32(stack.len)))
    stack.add(key)
    doAssert w.registerStep(path, 2, []).isOk
    doAssert w.registerCall(fn, []).isOk
  proc leave(w: var MultiStreamTraceWriter) =
    doAssert w.registerStep(path, 3, []).isOk
    doAssert w.registerReturn().isOk
    stack.setLen(stack.len - 1)
  for _ in 0 ..< 2:
    for _ in 0 ..< 300:
      for _ in 0 ..< 2:
        w.enter()
        w.leave()
      w.enter()
    for _ in 0 ..< 300:
      w.leave()
  w.enter()
  w.leave()
  doAssert w.close().isOk

  var reader = openNewTraceFromBytes(w.toBytes()).get()
  doAssert reader.callCount().get() == uint64(model.len),
    "read back " & $reader.callCount().get() & " calls, wrote " & $model.len
  for key, e in model:
    let rec = reader.call(uint64(key)).get()
    doAssert rec.parentCallKey == e.parent and rec.depth == e.depth and
      rec.children == e.children,
      "call " & $key & " reads back as parent " & $rec.parentCallKey &
      ", depth " & $rec.depth & ", " & $rec.children.len &
      " children; entry order gives parent " & $e.parent & ", depth " &
      $e.depth & ", " & $e.children.len & " children"

when isMainModule:
  test_records_land_at_their_entry_order_key()
  echo "PASS test_records_land_at_their_entry_order_key"
  test_nested_call_flush_is_not_quadratic_in_depth()
  echo "PASS test_nested_call_flush_is_not_quadratic_in_depth"
  test_a_100k_deep_recursion_is_written_in_seconds()
  echo "PASS test_a_100k_deep_recursion_is_written_in_seconds"
