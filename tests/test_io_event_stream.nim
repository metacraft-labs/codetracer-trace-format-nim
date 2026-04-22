when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Tests for the IO event stream writer/reader.

import std/times
import results
import codetracer_ctfs/container
import codetracer_trace_writer/io_event_stream

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

type Rng = object
  state: uint64

proc initRng(seed: uint64): Rng = Rng(state: seed)

proc next(r: var Rng): uint64 =
  r.state = r.state xor (r.state shl 13)
  r.state = r.state xor (r.state shr 7)
  r.state = r.state xor (r.state shl 17)
  r.state

proc makeData(rng: var Rng, length: int): seq[byte] =
  var d = newSeq[byte](length)
  for i in 0 ..< length:
    d[i] = byte(rng.next() mod 256)
  d

proc makeIOEvent(rng: var Rng, idx: int): IOEvent =
  let kind = IOEventKind(rng.next() mod 4)
  let stepId = rng.next() mod 100000
  let dataLen = int(rng.next() mod 50) + 1
  IOEvent(
    kind: kind,
    stepId: stepId,
    data: makeData(rng, dataLen),
  )

# ---------------------------------------------------------------------------
# test_io_event_stream_write_read
# ---------------------------------------------------------------------------

proc test_io_event_stream_write_read() {.raises: [].} =
  const numEvents = 1000
  const numChecks = 100

  var ctfs = createCtfs()
  let writerRes = initIOEventStreamWriter(ctfs)
  doAssert writerRes.isOk, "initIOEventStreamWriter failed: " & writerRes.error
  var writer = writerRes.get()

  var writeRng = initRng(42)
  for i in 0 ..< numEvents:
    let ev = makeIOEvent(writeRng, i)
    let r = writeEvent(ctfs, writer, ev)
    doAssert r.isOk, "writeEvent failed at index " & $i & ": " & r.error

  let rawBytes = ctfs.toBytes()
  let readerRes = initIOEventStreamReader(rawBytes)
  doAssert readerRes.isOk, "initIOEventStreamReader failed: " & readerRes.error
  let reader = readerRes.get()
  doAssert reader.count == uint64(numEvents),
    "count mismatch: got " & $reader.count & " expected " & $numEvents

  # Verify random subset
  var checkRng = initRng(99)
  for check in 0 ..< numChecks:
    let idx = int(checkRng.next() mod uint64(numEvents))

    # Replay to get expected event
    var replayRng = initRng(42)
    for s in 0 ..< idx:
      discard makeIOEvent(replayRng, s)
    let expected = makeIOEvent(replayRng, idx)

    let readRes = readEvent(reader, uint64(idx))
    doAssert readRes.isOk, "readEvent failed at index " & $idx & ": " & readRes.error
    let got = readRes.get()

    doAssert got.kind == expected.kind,
      "event " & $idx & ": kind mismatch: got " & $got.kind & " expected " & $expected.kind
    doAssert got.stepId == expected.stepId,
      "event " & $idx & ": stepId mismatch"
    doAssert got.data == expected.data,
      "event " & $idx & ": data mismatch"

  echo "PASS: test_io_event_stream_write_read"

# ---------------------------------------------------------------------------
# test_io_event_stream_page_load
# ---------------------------------------------------------------------------

proc test_io_event_stream_page_load() {.raises: [].} =
  const numEvents = 500

  var ctfs = createCtfs()
  let writerRes = initIOEventStreamWriter(ctfs)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  var writeRng = initRng(55)
  for i in 0 ..< numEvents:
    let ev = makeIOEvent(writeRng, i)
    let r = writeEvent(ctfs, writer, ev)
    doAssert r.isOk

  let rawBytes = ctfs.toBytes()
  let readerRes = initIOEventStreamReader(rawBytes)
  doAssert readerRes.isOk
  let reader = readerRes.get()

  # Read events 100-149 (50 events)
  var replayRng = initRng(55)
  for s in 0 ..< 100:
    discard makeIOEvent(replayRng, s)

  for i in 100 ..< 150:
    let expected = makeIOEvent(replayRng, i)
    let readRes = readEvent(reader, uint64(i))
    doAssert readRes.isOk, "readEvent failed at index " & $i & ": " & readRes.error
    let got = readRes.get()

    doAssert got.kind == expected.kind,
      "page event " & $i & ": kind mismatch"
    doAssert got.stepId == expected.stepId,
      "page event " & $i & ": stepId mismatch"
    doAssert got.data == expected.data,
      "page event " & $i & ": data mismatch"

  echo "PASS: test_io_event_stream_page_load"

# ---------------------------------------------------------------------------
# bench_io_event_page_load
# ---------------------------------------------------------------------------

proc bench_io_event_page_load() {.raises: [].} =
  const totalEvents = 1000
  const pageSize = 50

  var ctfs = createCtfs()
  let writerRes = initIOEventStreamWriter(ctfs)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  var rng = initRng(77)
  for i in 0 ..< totalEvents:
    let ev = makeIOEvent(rng, i)
    let r = writeEvent(ctfs, writer, ev)
    doAssert r.isOk

  let rawBytes = ctfs.toBytes()
  let readerRes = initIOEventStreamReader(rawBytes)
  doAssert readerRes.isOk
  let reader = readerRes.get()

  # Time loading 50 events (simulating a page)
  let startTime = cpuTime()

  for i in 100 ..< 100 + pageSize:
    let readRes = readEvent(reader, uint64(i))
    doAssert readRes.isOk

  let elapsed = cpuTime() - startTime
  let elapsedMs = elapsed * 1000.0

  echo "bench_io_event_page_load: " & $pageSize &
    " events in " & $elapsedMs & " ms"
  doAssert elapsedMs < 1.0,
    "page load took " & $elapsedMs & " ms, expected < 1ms"

  echo "PASS: bench_io_event_page_load"

# Run all tests
test_io_event_stream_write_read()
test_io_event_stream_page_load()
bench_io_event_page_load()
