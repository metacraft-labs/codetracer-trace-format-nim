## An attached reader never accepts a torn value (`ctfs-container.md` §6,
## "Reading"), against the hostile injector of `CKF-2`'s KF-R: a writer that
## stores every payload word as single bytes, in random order, with pauses
## between them, while reader threads read the slot under its sequence
## counter. A second phase runs the natural writer (aligned word stores, no
## pauses), as KF-R does.
##
## Every payload the writer publishes is `(v, v * K1, v * K2)`; a payload is
## torn when its three words are not of one `v`.
##
## Null predictions (the mutation arms), one per writer mode, each the
## control KF-R found to see tears in that mode (tracing-formats-benchmarks
## `results/ctfs_keyed/REPORT.md` §12.1):
##
## - hostile writer: a reader that loads no `seq` at all
##   (`readSlotIgnoringSeq`) accepts torn payloads, so its count must be
##   above zero. If it were zero the injector would not be producing tears,
##   and the zero of the protocol's reader would mean nothing.
## - natural writer: a reader that skips the second `seq` load
##   (`readSlotSkippingSecondSeq`) accepts torn payloads, so its count must
##   be above zero. If it were zero the second load would be untested.
##
## The skip-second-seq reader is not the hostile mode's control: it tears
## only when the writer starts an update in the few nanoseconds between its
## `seq` load and its payload loads, which happens once per update at most,
## and a hostile update takes microseconds. On macOS arm64 under load, as
## the only arm of this test's earlier four-second form, it tore 0 or 1
## times; here it tears a few times per hostile phase, too few to rely on.
## Both readers run in both phases and their counts are printed.

import std/[atomics, random, os]
import codetracer_ctfs/live_page

const
  K1 = 0x9E3779B97F4A7C15'u64
  K2 = 0xC2B2AE3D27D4EB4F'u64
  SlotOff = 64
  Words = 3
  PhaseMs = 3000

type
  ReaderKind = enum
    rkProtocol = "the protocol's reader"
    rkSkipSecondSeq = "the arm that skips the second seq load"
    rkIgnoreSeq = "the arm that loads no seq"

  ReaderResult = object
    accepted, torn: int

var buf: array[256, uint64]
var stop: Atomic[bool]
var published: Atomic[int]
var results: array[ReaderKind, ReaderResult]

proc view(): PageView = PageView(base: addr buf[0], len: sizeof(buf))

proc payloadOf(x: uint64): array[Words, uint64] = [x, x * K1, x * K2]

proc storeByte(p: pointer, b: uint8) {.inline.} =
  atomicStoreN(cast[ptr uint8](p), b, ATOMIC_RELAXED)

proc hostileWriter() {.thread.} =
  ## The hostile writer: `seq` odd; each payload byte stored alone, in random
  ## order, with random pauses; `seq` even.
  var rng = initRand(11)
  let v = view()
  var x = 1'u64
  while not stop.load():
    inc x
    let payload = payloadOf(x)
    let s = v.loadRelaxed(SlotOff)
    v.storeRelaxed(SlotOff, s + 1)
    atomicThreadFence(ATOMIC_RELEASE)
    var order: seq[int]
    for i in 0 ..< 8 * Words: order.add i
    rng.shuffle(order)
    for i in order:
      let w = i div 8
      let b = i mod 8
      storeByte(cast[pointer](cast[uint](addr buf[0]) + uint(SlotOff + 8 + 8 * w + b)),
                uint8((payload[w] shr (8 * b)) and 0xff))
      if rng.rand(7) == 0:
        let t = monotonicNs() + uint64(rng.rand(2000))
        while monotonicNs() < t: discard
    v.store(SlotOff, s + 2)
    published.atomicInc()

proc naturalWriter() {.thread.} =
  ## The natural writer: `seq` odd; the payload as aligned word stores, with
  ## no pauses; `seq` even. Updates follow each other as fast as it can go,
  ## so a reader's first `seq` load is often followed by the next update.
  let v = view()
  var x = 1'u64
  while not stop.load():
    inc x
    let payload = payloadOf(x)
    let s = v.loadRelaxed(SlotOff)
    v.storeRelaxed(SlotOff, s + 1)
    atomicThreadFence(ATOMIC_RELEASE)
    for w in 0 ..< Words:
      v.storeRelaxed(SlotOff + 8 + 8 * w, payload[w])
    v.store(SlotOff, s + 2)
    published.atomicInc()

proc reader(kind: ReaderKind) {.thread.} =
  let v = view()
  var w: array[Words, uint64]
  var r: ReaderResult
  while not stop.load():
    let state =
      case kind
      of rkProtocol: readSlot(v, SlotOff, w)
      of rkSkipSecondSeq: readSlotSkippingSecondSeq(v, SlotOff, w)
      of rkIgnoreSeq: readSlotIgnoringSeq(v, SlotOff, w)
    if state == srOk:
      inc r.accepted
      if w[1] != w[0] * K1 or w[2] != w[0] * K2:
        inc r.torn
  results[kind] = r

proc runPhase(hostile: bool): bool =
  ## One writer mode against all three readers for `PhaseMs`; prints the
  ## counts and returns false when a check fails.
  let mode = if hostile: "hostile" else: "natural"
  # A published first value, so a reader never sees seq 0.
  let first = payloadOf(1)
  for i in 0 ..< Words: view().store(SlotOff + 8 + 8 * i, first[i])
  view().store(SlotOff, 2)
  stop.store(false)
  published.store(0)
  var tw: Thread[void]
  var tr: array[ReaderKind, Thread[ReaderKind]]
  createThread(tw, if hostile: hostileWriter else: naturalWriter)
  for k in ReaderKind: createThread(tr[k], reader, k)
  sleep(PhaseMs)
  stop.store(true)
  joinThread(tw)
  for k in ReaderKind: joinThread(tr[k])
  echo mode, " writer: published ", published.load(), " payloads"
  for k in ReaderKind:
    echo "  ", k, " accepted ", results[k].accepted, " (", results[k].torn, " torn)"
  result = true
  if published.load() < 1000 or results[rkProtocol].accepted < 1000:
    echo "FAIL (", mode, "): too little traffic to judge"; result = false
  if results[rkProtocol].torn != 0:
    echo "FAIL (", mode, "): the attached reader accepted ",
      results[rkProtocol].torn, " torn values"; result = false
  let control = if hostile: rkIgnoreSeq else: rkSkipSecondSeq
  if results[control].torn == 0:
    echo "FAIL (", mode, "): ", control, " accepted no torn value: ",
      (if hostile: "the injector tore nothing" else: "the second seq load is untested")
    result = false

when isMainModule:
  let hostileOk = runPhase(hostile = true)
  let naturalOk = runPhase(hostile = false)
  if not (hostileOk and naturalOk): quit(1)
  echo "PASS test_live_page_torn"
