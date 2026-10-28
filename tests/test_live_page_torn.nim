## An attached reader never accepts a torn value (`ctfs-container.md` §6,
## "Reading"), against the hostile injector of `CKF-2`'s KF-R: a writer that
## stores every payload word as single bytes, in random order, with pauses
## between them, while reader threads read the slot under its sequence
## counter.
##
## Every payload the writer publishes is `(v, v * K1, v * K2)`; a payload is
## torn when its three words are not of one `v`.
##
## Null prediction (the mutation arm): a reader that skips the second `seq`
## load (`readSlotSkippingSecondSeq`) accepts torn payloads, so the arm's
## count must be above zero. If it were zero the injector would not be
## producing tears, and the zero of the protocol's reader would mean nothing.

import std/[atomics, random, times, os]
import codetracer_ctfs/live_page

const
  K1 = 0x9E3779B97F4A7C15'u64
  K2 = 0xC2B2AE3D27D4EB4F'u64
  SlotOff = 64
  Words = 3

var buf: array[256, uint64]
var stop: Atomic[bool]
var published: Atomic[int]

proc view(): PageView = PageView(base: addr buf[0], len: sizeof(buf))

proc storeByte(p: pointer, b: uint8) {.inline.} =
  atomicStoreN(cast[ptr uint8](p), b, ATOMIC_RELAXED)

proc writer() {.thread.} =
  ## The hostile writer: `seq` odd; each payload byte stored alone, in random
  ## order, with random pauses; `seq` even.
  var rng = initRand(11)
  let v = view()
  var x = 1'u64
  while not stop.load():
    inc x
    let payload = [x, x * K1, x * K2]
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

type ReaderResult = object
  accepted, torn: int

var protocolResult, skippingResult: ReaderResult

proc readerLoop(skip: bool): ReaderResult =
  let v = view()
  var w: array[Words, uint64]
  while not stop.load():
    let r =
      if skip: readSlotSkippingSecondSeq(v, SlotOff, w)
      else: readSlot(v, SlotOff, w)
    if r == srOk:
      inc result.accepted
      if w[1] != w[0] * K1 or w[2] != w[0] * K2:
        inc result.torn

proc protocolReader() {.thread.} = protocolResult = readerLoop(false)
proc skippingReader() {.thread.} = skippingResult = readerLoop(true)

when isMainModule:
  # A published first value, so a reader never sees seq 0.
  view().store(SlotOff + 8, 1); view().store(SlotOff + 16, K1)
  view().store(SlotOff + 24, K2); view().store(SlotOff, 2)
  var tw, tr1, tr2: Thread[void]
  createThread(tw, writer)
  createThread(tr1, protocolReader)
  createThread(tr2, skippingReader)
  sleep(4000)
  stop.store(true)
  joinThreads(tw, tr1, tr2)
  echo "published ", published.load(), " payloads; the protocol's reader accepted ",
    protocolResult.accepted, " (", protocolResult.torn, " torn); the arm that skips ",
    "the second seq load accepted ", skippingResult.accepted, " (",
    skippingResult.torn, " torn)"
  var bad = false
  if published.load() < 1000 or protocolResult.accepted < 1000:
    echo "FAIL: too little traffic to judge"; bad = true
  if protocolResult.torn != 0:
    echo "FAIL: the attached reader accepted ", protocolResult.torn, " torn values"; bad = true
  if skippingResult.torn == 0:
    echo "FAIL: the mutation arm accepted no torn value: the injector tore nothing"; bad = true
  if bad: quit(1)
  echo "PASS test_live_page_torn"
