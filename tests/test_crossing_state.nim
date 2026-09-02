{.push raises: [].}

## MT7 piece 5a — the exported, ABI-stable, per-thread crossing block.
##
## Proves the crossing bookkeeping the trace writer keeps on its GC-heap
## `pendingCrossings` seq is ALSO mirrored into a fixed, exported, C-ABI block
## an out-of-process reader (MT7 5b) can read from RECREATED memory in O(1)
## (nested-trace-correlation.md §1.2; Mixed-Trace-Debugging.md §6.1).
##
## This test drives the writer's `beginCrossing` / `endCrossing` and then reads
## the state back **through the exported C symbols** — `ct_crossing_stack_count`
## / `ct_crossing_stacks[]` and the per-thread `ct_crossing_stack_t` — exactly
## the way MT7 5b will (registry enumeration + struct field offsets), so what is
## asserted is the EXTERNALLY OBSERVABLE ABI, not internal writer state.
##
## No mocks: it runs the real `MultiStreamTraceWriter` against the real C block.

import results
import codetracer_ctfs/crossing_state
import codetracer_trace_writer/multi_stream_writer

proc currentStack(): ptr CtCrossingStack {.raises: [].} =
  ## Read the calling thread's crossing stack the way the reader does: enumerate
  ## the exported registry.  This test is single-threaded and is the first (only)
  ## registrant, so its slot is index 0.
  doAssert ctCrossingStackCount >= 1'i32,
    "expected >= 1 registered crossing stack, got " & $ctCrossingStackCount
  result = ctCrossingStacks[0]
  doAssert result != nil, "ct_crossing_stacks[0] is NULL after a push"

proc test_crossing_block_mirrors_writer() {.raises: [].} =
  let writerRes = initMultiStreamWriter("test_crossing_state.ct", "test_cs")
  doAssert writerRes.isOk, "initMultiStreamWriter failed: " & writerRes.error
  var w = writerRes.get()
  let p0 = w.registerPath("/vm/prog.src")
  doAssert p0.isOk

  # Before any crossing opens, this thread has not registered a stack yet — the
  # exported count reads 0 (the block is strictly additive; a writer that never
  # crosses touches nothing).
  doAssert ctCrossingStackCount == 0'i32,
    "no crossing should be registered before the first beginCrossing, got " &
      $ctCrossingStackCount

  doAssert w.registerStep(0, 1, @[]).isOk        # step idx 0

  # ---- OUTER crossing opens at step idx 1 -------------------------------
  let outer = w.beginCrossing("gdscript-frame")
  doAssert outer == 1'u64, "first span_id must be 1, got " & $outer

  # First touch registered exactly one thread stack.
  doAssert ctCrossingStackCount == 1'i32,
    "one thread stack must be registered, got " & $ctCrossingStackCount
  block:
    let s = currentStack()
    doAssert s.sp == 0'i32, "after first begin sp must be 0, got " & $s.sp
    doAssert s.frames[0].spanId == 1'u64,
      "top frame span_id must be 1, got " & $s.frames[0].spanId
    doAssert s.frames[0].startStep == 1'u64,
      "top frame start_step must be 1, got " & $s.frames[0].startStep

  doAssert w.registerStep(0, 2, @[]).isOk        # step idx 1 (inside outer)

  # ---- INNER (nested) crossing opens at step idx 2 ----------------------
  let inner = w.beginCrossing("gdscript-call")
  doAssert inner == 2'u64, "second span_id must be 2, got " & $inner

  # Nesting deepens sp; the registry pointer is unchanged (same thread, no new
  # slot claimed).
  doAssert ctCrossingStackCount == 1'i32,
    "nested begin must not register a new thread, got " & $ctCrossingStackCount
  block:
    let s = currentStack()
    doAssert s.sp == 1'i32, "after nested begin sp must be 1, got " & $s.sp
    # LIFO: outer stays at frames[0], inner is the new top at frames[1].
    doAssert s.frames[0].spanId == 1'u64,
      "frames[0] must still be the outer crossing, got " & $s.frames[0].spanId
    doAssert s.frames[0].startStep == 1'u64, $s.frames[0].startStep
    doAssert s.frames[1].spanId == 2'u64,
      "frames[1] (top) must be the inner crossing, got " & $s.frames[1].spanId
    doAssert s.frames[1].startStep == 2'u64,
      "inner start_step must be 2, got " & $s.frames[1].startStep

  doAssert w.registerStep(0, 3, @[]).isOk        # step idx 2 (inside inner)

  # ---- Close inner (LIFO): sp pops back to the outer frame --------------
  doAssert w.endCrossing(inner).isOk
  block:
    let s = currentStack()
    doAssert s.sp == 0'i32, "after inner pop sp must be 0, got " & $s.sp
    doAssert s.frames[0].spanId == 1'u64,
      "outer must be the top again, got " & $s.frames[0].spanId

  doAssert w.registerStep(0, 4, @[]).isOk        # step idx 3 (still in outer)

  # ---- Close outer: fully closed → sp = -1 (native altitude) ------------
  doAssert w.endCrossing(outer).isOk
  block:
    let s = currentStack()
    doAssert s.sp == -1'i32,
      "fully closed crossings must leave sp = -1, got " & $s.sp

  # ---- Balance under misuse: a rejected endCrossing must NOT pop --------
  # The heap seq is empty now; endCrossing returns err and must leave the
  # exported block untouched (sp stays -1), so the mirror never underflows.
  doAssert w.endCrossing(inner).isErr, "re-ending a settled crossing must err"
  doAssert w.endCrossing(999'u64).isErr, "unknown span_id must err"
  doAssert currentStack().sp == -1'i32,
    "a rejected endCrossing must not pop the exported block"

  let closeRes = w.close()
  doAssert closeRes.isOk, "close failed: " & closeRes.error
  w.closeCtfs()

  # ---- A failed begin (closed writer) returns 0 and must NOT push -------
  doAssert w.beginCrossing("after-close") == 0'u64,
    "beginCrossing on a closed writer must return the 0 sentinel"
  doAssert currentStack().sp == -1'i32,
    "a begin that returned 0 must not have pushed onto the exported block"

  echo "PASS: test_crossing_block_mirrors_writer"

proc test_stack_depth_tracks_seq_length() {.raises: [].} =
  ## The exported block's stack depth (sp + 1) must equal the heap seq's length
  ## at every step — a three-deep open nest reads sp == 2.
  let writerRes = initMultiStreamWriter("test_crossing_state_depth.ct", "cs_d")
  doAssert writerRes.isOk, "initMultiStreamWriter failed: " & writerRes.error
  var w = writerRes.get()
  doAssert w.registerPath("/vm/prog.src").isOk

  let a = w.beginCrossing("a")
  doAssert a == 1'u64
  doAssert currentStack().sp == 0'i32
  let b = w.beginCrossing("b")
  doAssert b == 2'u64
  doAssert currentStack().sp == 1'i32
  let c = w.beginCrossing("c")
  doAssert c == 3'u64
  let s = currentStack()
  doAssert s.sp == 2'i32, "three-deep nest must read sp == 2, got " & $s.sp
  doAssert s.frames[0].spanId == 1'u64
  doAssert s.frames[1].spanId == 2'u64
  doAssert s.frames[2].spanId == 3'u64

  doAssert w.endCrossing(c).isOk
  doAssert currentStack().sp == 1'i32
  doAssert w.endCrossing(b).isOk
  doAssert currentStack().sp == 0'i32
  doAssert w.endCrossing(a).isOk
  doAssert currentStack().sp == -1'i32

  let closeRes = w.close()
  doAssert closeRes.isOk, "close failed: " & closeRes.error
  w.closeCtfs()

  echo "PASS: test_stack_depth_tracks_seq_length"

test_crossing_block_mirrors_writer()
test_stack_depth_tracks_seq_length()
echo "ALL PASS: test_crossing_state"

{.pop.}
