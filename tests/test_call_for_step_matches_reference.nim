## `NewTraceReader.callForStep` answers exactly its definition, on every step
## of a deep and wide call tree.
##
## `callForStep` (and `enclosingCallKey`, which it is built on) is defined in
## `new_trace_reader.nim` as: `k` = the largest call_key whose `entryStep` is
## at most the step (by binary search over the call stream), then the LARGEST
## call_key `<= k` whose `exitStep` reaches the step; refused with
## `no call records` on an empty stream and `step N not found in any call`
## when there is none. It used to compute the second part by walking back
## from `k` over every call entered before the step — O(calls) per step on a
## module body that calls thousands of functions, which is what a recorded
## `import` is (codetracer-pm
## `issues/2026-10-31-ct-print-full-uses-32-gb-to-decode-an-11-mb-python-trace.md`).
## It now answers from an index built once (a max-segment-tree over exit
## steps). This test holds the new answer to a brute-force reading of the
## definition, for EVERY step and a few past the end, including the refusal
## text.
##
## The tree is built by the production writer to carry every shape the
## definition has to get right:
##
## * depth (up to 7 frames) and width (siblings that return before a later
##   step of their caller, which the old walk crossed one at a time);
## * calls with no step of their own, which the writer clamps onto their
##   caller's latest step (`entryStep == exitStep ==` that step), so the
##   caller and the callee share it;
## * frames still open at close, which the writer finalizes with a SHARED
##   `exitStep` (the last step);
## * a frame entered after the last step, whose entry lies past the end (the
##   close-time drain), so no step of the trace is in it;
## * raise / catch records, which are exec records but not steps.
##
## The tree opens with a step before its first call, which no call encloses.
## A second container has no calls at all, for `no call records`.
##
## Falsifiability: answer part 2 with the leftmost instead of the rightmost
## qualifying leaf, or return `k` without checking its exit, and the first
## step after a returned sibling fails, naming the step and both keys. Drop
## the `lo > last` bound in `rightmostExitAtLeast` and steps after an open
## frame's later sibling are attributed to calls entered after them.
##
## No mocks: the writer and reader are the production ones.

import std/[os, tempfiles]
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/call_stream

let scratch = createTempDir("call_for_step_", "")

proc referenceKey(calls: seq[CallRecord], s: uint64): Result[uint64, string] =
  ## The definition, by brute force over every record. The writer stores
  ## calls in entry order, so the binary search's `k` is the largest key
  ## entered at or before `s`.
  if calls.len == 0:
    return err("no call records")
  var k = -1
  for i, c in calls:
    if c.entryStep <= s:
      k = i
  if k < 0:
    return err("step " & $s & " not found in any call")
  for i in countdown(k, 0):
    if calls[i].exitStep >= s:
      return ok(uint64(i))
  err("step " & $s & " not found in any call")

proc checkEveryStep(path, what: string): int =
  ## Compare `callForStep` and `enclosingCallKey` with the reference on every
  ## exec record of `path` and four past its end; return how many steps had
  ## an enclosing call.
  var reader = openNewTrace(path).get()
  var calls: seq[CallRecord]
  for i in 0'u64 ..< reader.callCount().get():
    calls.add(reader.call(i).get())
  # Entry order is what makes the brute-force `k` the binary search's `k`.
  for i in 1 ..< calls.len:
    doAssert calls[i].entryStep >= calls[i - 1].entryStep,
      what & ": the writer stored calls out of entry order"
  let n = reader.stepCount().get()
  for s in 0'u64 ..< n + 4:
    let want = referenceKey(calls, s)
    let gotKey = reader.enclosingCallKey(s)
    let got = reader.callForStep(s)
    if want.isOk:
      doAssert gotKey.isOk and gotKey.get() == want.get(),
        what & ": step " & $s & ": enclosingCallKey = " & $gotKey &
        ", the definition gives call_key " & $want.get()
      doAssert got.isOk and got.get() == calls[int(want.get())],
        what & ": step " & $s & ": callForStep did not return call_key " &
        $want.get() & "'s record"
      inc result
    else:
      doAssert gotKey.isErr and gotKey.error == want.error,
        what & ": step " & $s & ": expected the refusal `" & want.error &
        "`, got " & $gotKey
      doAssert got.isErr and got.error == want.error,
        what & ": step " & $s & ": callForStep: expected `" & want.error &
        "`, got " & (if got.isOk: "a record" else: got.error)

proc buildTree(path: string): tuple[calls, openAtClose: int] =
  var w = initMultiStreamWriter(path, "tree", chunkSize = 32).get()
  let p0 = w.registerPath("/wd/tree.py").get()
  var fns: seq[uint64]
  for name in ["main", "visit", "fold", "leaf", "empty"]:
    fns.add(w.registerFunction(name).get())
  var line = 1'u64
  proc step(w: var MultiStreamTraceWriter) =
    doAssert w.registerStep(p0, line, @[]).isOk
    inc line

  # A step before any call: enclosed by nothing.
  w.step()
  proc grow(w: var MultiStreamTraceWriter, depth, seed: int, count: var int) =
    ## One frame: a step, then children (more at shallow depths), a step
    ## after each child (so siblings return before their caller's next step),
    ## a callee with no steps now and then, and a raise/catch.
    doAssert w.registerCall(fns[min(depth, 3)], @[]).isOk
    inc count
    w.step()
    let children = if depth >= 6: 0 else: (seed mod 4) + (if depth < 2: 3 else: 0)
    for c in 0 ..< children:
      grow(w, depth + 1, seed * 7 + c * 3 + depth, count)
      if (seed + c) mod 3 != 0:
        w.step()
      if (seed + c) mod 5 == 1:
        doAssert w.registerCall(fns[4], @[]).isOk   # no step of its own
        inc count
        doAssert w.registerReturn().isOk
    if seed mod 11 == 4:
      doAssert w.registerRaise(1, cast[seq[byte]]("e")).isOk
      doAssert w.registerCatch(1).isOk
      w.step()
    doAssert w.registerReturn().isOk

  doAssert w.registerCall(fns[0], @[]).isOk
  inc result.calls
  w.step()
  for top in 0 ..< 6:
    grow(w, 1, top + 1, result.calls)
    w.step()
  # Leave three frames open: they close at the last step, sharing it.
  for d in 0 ..< 3:
    doAssert w.registerCall(fns[1 + d], @[]).isOk
    inc result.calls
    w.step()
  # And one entered after the last step: the close-time drain.
  doAssert w.registerCall(fns[3], @[]).isOk
  inc result.calls
  result.openAtClose = 5  # main, the three above, the drained one
  doAssert w.close().isOk
  doAssert w.closeCtfs().isOk

proc test_a_deep_and_wide_tree() =
  let path = scratch / "tree.ct"
  let shape = buildTree(path)
  var reader = openNewTrace(path).get()
  let n = reader.stepCount().get()
  doAssert reader.callCount().get() == uint64(shape.calls)
  # Non-degeneracy: the shapes the definition has to get right are present.
  var sharedLast, drained, empty, maxDepth = 0
  for i in 0'u64 ..< reader.callCount().get():
    let c = reader.call(i).get()
    if c.exitStep == n - 1: inc sharedLast
    if c.entryStep >= n: inc drained
    if reader.function(c.functionId).get() == "empty" and
        c.entryStep == c.exitStep: inc empty
    maxDepth = max(maxDepth, int(c.depth))
  doAssert sharedLast >= 4 and drained == 1 and empty > 0 and maxDepth >= 6,
    "the tree lost a shape: " & $sharedLast & " calls share the last step, " &
    $drained & " drained, " & $empty & " stepless, depth " & $maxDepth
  let enclosed = checkEveryStep(path, "deep-and-wide tree")
  doAssert enclosed > 0 and uint64(enclosed) < n,
    "every step, or none, had an enclosing call: " & $enclosed & " of " & $n
  echo "PASS: callForStep matches its definition on all " & $n &
    " exec records of a " & $shape.calls & "-call tree (depth " & $maxDepth & ")"

proc test_no_calls() =
  let path = scratch / "nocalls.ct"
  var w = initMultiStreamWriter(path, "flat", chunkSize = 8).get()
  let p0 = w.registerPath("/wd/flat.py").get()
  for l in 1'u64 .. 5:
    doAssert w.registerStep(p0, l, @[]).isOk
  doAssert w.close().isOk
  doAssert w.closeCtfs().isOk
  discard checkEveryStep(path, "no calls")
  var reader = openNewTrace(path).get()
  doAssert reader.callForStep(0).error == "no call records"
  echo "PASS: a container without calls refuses with `no call records`"

try:
  test_a_deep_and_wide_tree()
  test_no_calls()
finally:
  removeDir(scratch)
echo "ALL PASS: test_call_for_step_matches_reference"
