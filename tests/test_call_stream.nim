when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Tests for the call stream writer/reader.

import std/[monotimes, times, os]
import results
import codetracer_ctfs/container
import codetracer_ctfs/streaming
import codetracer_trace_writer/call_stream

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

proc makeCallRecord(rng: var Rng, idx: int): CallRecord =
  let numArgs = int(rng.next() mod 4) + 1  # 1 to 4 args
  var args = newSeq[CallArg](numArgs)
  for a in 0 ..< numArgs:
    let argLen = int(rng.next() mod 20) + 1
    args[a] = CallArg(varnameId: uint64(a), value: makeData(rng, argLen))

  let retLen = int(rng.next() mod 16) + 1
  let retVal = makeData(rng, retLen)

  let numChildren = int(rng.next() mod 5)
  var children = newSeq[uint64](numChildren)
  for c in 0 ..< numChildren:
    children[c] = rng.next() mod 10000

  CallRecord(
    functionId: rng.next() mod 5000,
    parentCallKey: int64(rng.next() mod 1000) - 500,  # can be negative
    entryStep: rng.next() mod 100000,
    exitStep: rng.next() mod 100000,
    depth: uint32(rng.next() mod 50),
    args: args,
    returnValue: retVal,
    exception: @[],  # no exception by default
    children: children,
  )

# ---------------------------------------------------------------------------
# test_call_stream_write_read
# ---------------------------------------------------------------------------

proc test_call_stream_write_read() {.raises: [].} =
  const numRecords = 1000
  const numChecks = 100

  var ctfs = createCtfs()
  let writerRes = initCallStreamWriter(ctfs)
  doAssert writerRes.isOk, "initCallStreamWriter failed: " & writerRes.error
  var writer = writerRes.get()

  var writeRng = initRng(42)
  for i in 0 ..< numRecords:
    let rec = makeCallRecord(writeRng, i)
    let r = writeCall(ctfs, writer, rec)
    doAssert r.isOk, "writeCall failed at index " & $i & ": " & r.error

  let finRes = finalizeCallStream(ctfs, writer)
  doAssert finRes.isOk, "finalizeCallStream failed: " & finRes.error
  let rawBytes = ctfs.toBytes()
  let readerRes = initCallStreamReader(rawBytes)
  doAssert readerRes.isOk, "initCallStreamReader failed: " & readerRes.error
  var reader = readerRes.get()
  doAssert reader.count == uint64(numRecords),
    "count mismatch: got " & $reader.count & " expected " & $numRecords

  # Verify random subset
  var checkRng = initRng(99)
  for check in 0 ..< numChecks:
    let idx = int(checkRng.next() mod uint64(numRecords))

    # Replay to get expected record
    var replayRng = initRng(42)
    for s in 0 ..< idx:
      discard makeCallRecord(replayRng, s)
    let expected = makeCallRecord(replayRng, idx)

    let readRes = readCall(reader, uint64(idx))
    doAssert readRes.isOk, "readCall failed at index " & $idx & ": " & readRes.error
    let got = readRes.get()

    doAssert got.functionId == expected.functionId,
      "call " & $idx & ": functionId mismatch"
    doAssert got.parentCallKey == expected.parentCallKey,
      "call " & $idx & ": parentCallKey mismatch"
    doAssert got.entryStep == expected.entryStep,
      "call " & $idx & ": entryStep mismatch"
    doAssert got.exitStep == expected.exitStep,
      "call " & $idx & ": exitStep mismatch"
    doAssert got.depth == expected.depth,
      "call " & $idx & ": depth mismatch"
    doAssert got.args.len == expected.args.len,
      "call " & $idx & ": args count mismatch"
    for a in 0 ..< got.args.len:
      doAssert got.args[a] == expected.args[a],
        "call " & $idx & " arg " & $a & ": data mismatch"
    doAssert got.returnValue == expected.returnValue,
      "call " & $idx & ": returnValue mismatch"
    doAssert got.exception == expected.exception,
      "call " & $idx & ": exception mismatch"
    doAssert got.children == expected.children,
      "call " & $idx & ": children mismatch"

  echo "PASS: test_call_stream_write_read"

# ---------------------------------------------------------------------------
# test_call_stream_void_return
# ---------------------------------------------------------------------------

proc test_call_stream_void_return() {.raises: [].} =
  var ctfs = createCtfs()
  let writerRes = initCallStreamWriter(ctfs)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  let rec = CallRecord(
    functionId: 42,
    parentCallKey: -1,
    entryStep: 100,
    exitStep: 200,
    depth: 0,
    args: @[CallArg(varnameId: 0, value: @[byte 1, 2, 3])],
    returnValue: @[VoidReturnMarker],
    exception: @[],
    children: @[],
  )
  let r = writeCall(ctfs, writer, rec)
  doAssert r.isOk

  let finRes = finalizeCallStream(ctfs, writer)
  doAssert finRes.isOk
  let rawBytes = ctfs.toBytes()
  let readerRes = initCallStreamReader(rawBytes)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  let got = readCall(reader, 0)
  doAssert got.isOk
  let call = got.get()
  doAssert call.returnValue.len == 1
  doAssert call.returnValue[0] == VoidReturnMarker,
    "VoidReturnMarker not preserved: got " & $call.returnValue[0]
  doAssert call.parentCallKey == -1,
    "parentCallKey should be -1, got " & $call.parentCallKey

  echo "PASS: test_call_stream_void_return"

# ---------------------------------------------------------------------------
# test_call_stream_exception_exit
# ---------------------------------------------------------------------------

proc test_call_stream_exception_exit() {.raises: [].} =
  var ctfs = createCtfs()
  let writerRes = initCallStreamWriter(ctfs)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  let excData = @[byte 0xCA, 0xFE, 0xBA, 0xBE]  # mock CBOR exception
  let rec = CallRecord(
    functionId: 99,
    parentCallKey: 0,
    entryStep: 500,
    exitStep: 600,
    depth: 1,
    args: @[
      CallArg(varnameId: 0, value: @[byte 10]),
      CallArg(varnameId: 1, value: @[byte 20, 30]),
    ],
    returnValue: @[],  # no return value on exception
    exception: excData,
    children: @[],
  )
  let r = writeCall(ctfs, writer, rec)
  doAssert r.isOk

  let finRes = finalizeCallStream(ctfs, writer)
  doAssert finRes.isOk
  let rawBytes = ctfs.toBytes()
  let readerRes = initCallStreamReader(rawBytes)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  let got = readCall(reader, 0)
  doAssert got.isOk
  let call = got.get()
  doAssert call.returnValue.len == 0,
    "exception call should have empty returnValue"
  doAssert call.exception == excData,
    "exception data mismatch"
  doAssert call.args.len == 2
  doAssert call.args[0].value == @[byte 10]
  doAssert call.args[1].value == @[byte 20, 30]

  echo "PASS: test_call_stream_exception_exit"

# ---------------------------------------------------------------------------
# test_call_stream_nested_calls
# ---------------------------------------------------------------------------

proc test_call_stream_nested_calls() {.raises: [].} =
  var ctfs = createCtfs()
  let writerRes = initCallStreamWriter(ctfs)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  # Write parent (call_key 0) with 5 children (call_keys 1..5)
  let parent = CallRecord(
    functionId: 1,
    parentCallKey: -1,
    entryStep: 0,
    exitStep: 1000,
    depth: 0,
    args: @[CallArg(varnameId: 0, value: @[byte 0xFF])],
    returnValue: @[byte 42],
    exception: @[],
    children: @[uint64 1, 2, 3, 4, 5],
  )
  let rp = writeCall(ctfs, writer, parent)
  doAssert rp.isOk

  # Write 5 children
  for i in 1 .. 5:
    let child = CallRecord(
      functionId: uint64(10 + i),
      parentCallKey: 0,  # parent is call_key 0
      entryStep: uint64(i * 100),
      exitStep: uint64(i * 100 + 50),
      depth: 1,
      args: @[CallArg(varnameId: 0, value: @[byte(i)])],
      returnValue: @[byte(i * 10)],
      exception: @[],
      children: @[],
    )
    let rc = writeCall(ctfs, writer, child)
    doAssert rc.isOk

  let finRes = finalizeCallStream(ctfs, writer)
  doAssert finRes.isOk
  let rawBytes = ctfs.toBytes()
  let readerRes = initCallStreamReader(rawBytes)
  doAssert readerRes.isOk
  var reader = readerRes.get()
  doAssert reader.count == 6

  # Verify parent
  let gotParent = readCall(reader, 0)
  doAssert gotParent.isOk
  let p = gotParent.get()
  doAssert p.children.len == 5
  doAssert p.parentCallKey == -1
  for i in 0 ..< 5:
    doAssert p.children[i] == uint64(i + 1),
      "child " & $i & " key mismatch: got " & $p.children[i]

  # Verify each child
  for i in 1 .. 5:
    let gotChild = readCall(reader, uint64(i))
    doAssert gotChild.isOk
    let c = gotChild.get()
    doAssert c.parentCallKey == 0,
      "child " & $i & " parentCallKey should be 0, got " & $c.parentCallKey
    doAssert c.functionId == uint64(10 + i)
    doAssert c.depth == 1
    doAssert c.children.len == 0

  echo "PASS: test_call_stream_nested_calls"

# ---------------------------------------------------------------------------
# test_call_stream_readable_mid_run
#
# F1 regression guard: `calls.idx` must be written INCREMENTALLY as chunks
# seal, so a still-recording container is readable BEFORE `finalizeCallStream`.
# Previously the index was created only at finalize, so mid-run every call
# reader saw an empty (or absent) `calls.idx` and the whole call tree was dead
# while recording.
# ---------------------------------------------------------------------------

proc test_call_stream_readable_mid_run() {.raises: [].} =
  # Small chunk size so multiple chunks seal during the writes below.
  const chunkSize = 4
  const numWritten = 10   # 2 full chunks (8 calls) seal; 2 remain buffered.
  const sealedExpected = 8

  var ctfs = createCtfs()
  let writerRes = initCallStreamWriter(ctfs, chunkSize)
  doAssert writerRes.isOk, "initCallStreamWriter failed: " & writerRes.error
  var writer = writerRes.get()

  var rng = initRng(1234)
  for i in 0 ..< numWritten:
    let rec = makeCallRecord(rng, i)
    let r = writeCall(ctfs, writer, rec)
    doAssert r.isOk, "writeCall failed at index " & $i & ": " & r.error

  # BEFORE finalize: calls.idx must already exist and expose the sealed chunks.
  let midBytes = ctfs.toBytes()
  let midReaderRes = initCallStreamReader(midBytes)
  doAssert midReaderRes.isOk,
    "mid-run initCallStreamReader failed (calls.idx missing?): " &
    midReaderRes.error
  var midReader = midReaderRes.get()
  doAssert midReader.count == uint64(sealedExpected),
    "mid-run count mismatch: got " & $midReader.count & " expected " &
    $sealedExpected & " (calls.idx not written incrementally)"

  # Every sealed call must decode correctly mid-run, matching a fresh replay.
  var replayRng = initRng(1234)
  for i in 0 ..< sealedExpected:
    let expected = makeCallRecord(replayRng, i)
    let got = readCall(midReader, uint64(i))
    doAssert got.isOk, "mid-run readCall failed at " & $i & ": " & got.error
    doAssert got.get().functionId == expected.functionId,
      "mid-run call " & $i & ": functionId mismatch"
    doAssert got.get().entryStep == expected.entryStep,
      "mid-run call " & $i & ": entryStep mismatch"

  # Now finalize: the trailing partial chunk seals and the full stream reads.
  let finRes = finalizeCallStream(ctfs, writer)
  doAssert finRes.isOk, "finalizeCallStream failed: " & finRes.error
  let finalBytes = ctfs.toBytes()
  let finalReaderRes = initCallStreamReader(finalBytes)
  doAssert finalReaderRes.isOk, "final initCallStreamReader failed: " &
    finalReaderRes.error
  var finalReader = finalReaderRes.get()
  doAssert finalReader.count == uint64(numWritten),
    "final count mismatch: got " & $finalReader.count & " expected " &
    $numWritten

  var replay2 = initRng(1234)
  for i in 0 ..< numWritten:
    let expected = makeCallRecord(replay2, i)
    let got = readCall(finalReader, uint64(i))
    doAssert got.isOk, "final readCall failed at " & $i & ": " & got.error
    doAssert got.get().functionId == expected.functionId,
      "final call " & $i & ": functionId mismatch"

  echo "PASS: test_call_stream_readable_mid_run"

# ---------------------------------------------------------------------------
# test_call_stream_streaming_tail_from_disk
#
# The on-disk counterpart of the mid-run guard: with a real streaming CTFS
# (`createCtfsStreaming`), a reader that reopens the growing file mid-recording
# sees the sealed calls via the incrementally-synced `calls.idx` — this is the
# path `syncEntry` publishes.
# ---------------------------------------------------------------------------

proc test_call_stream_streaming_tail_from_disk() {.raises: [].} =
  const chunkSize = 4
  const numWritten = 10
  const sealedExpected = 8

  var path: string
  try:
    path = getTempDir() / "ct_call_stream_tail_test.ct"
    removeFile(path)
  except OSError, IOError:
    doAssert false, "failed to prepare temp path"

  let ctfsRes = createCtfsStreaming(path)
  doAssert ctfsRes.isOk, "createCtfsStreaming failed: " & ctfsRes.error
  var ctfs = ctfsRes.get()
  let writerRes = initCallStreamWriter(ctfs, chunkSize)
  doAssert writerRes.isOk, "initCallStreamWriter failed: " & writerRes.error
  var writer = writerRes.get()

  var rng = initRng(555)
  for i in 0 ..< numWritten:
    let rec = makeCallRecord(rng, i)
    let r = writeCall(ctfs, writer, rec)
    doAssert r.isOk, "writeCall failed at index " & $i & ": " & r.error

  # Reopen the growing file from disk WITHOUT closing the writer.
  var diskBytes: seq[byte] = @[]
  try:
    diskBytes = cast[seq[byte]](readFile(path))
  except IOError, OSError:
    doAssert false, "failed to read streaming file mid-run"

  let readerRes = initCallStreamReader(diskBytes)
  doAssert readerRes.isOk,
    "mid-run disk initCallStreamReader failed (calls.idx not synced?): " &
    readerRes.error
  var reader = readerRes.get()
  doAssert reader.count == uint64(sealedExpected),
    "mid-run disk count mismatch: got " & $reader.count & " expected " &
    $sealedExpected

  var replayRng = initRng(555)
  for i in 0 ..< sealedExpected:
    let expected = makeCallRecord(replayRng, i)
    let got = readCall(reader, uint64(i))
    doAssert got.isOk, "mid-run disk readCall failed at " & $i & ": " & got.error
    doAssert got.get().functionId == expected.functionId,
      "mid-run disk call " & $i & ": functionId mismatch"

  doAssert finalizeCallStream(ctfs, writer).isOk, "finalize failed"
  ctfs.closeCtfs()
  try:
    removeFile(path)
  except OSError, IOError:
    discard
  echo "PASS: test_call_stream_streaming_tail_from_disk"

# ---------------------------------------------------------------------------
# bench_call_tree_viewport_load
# ---------------------------------------------------------------------------

proc bench_call_tree_viewport_load() {.raises: [].} =
  const totalCalls = 1000
  const viewportSize = 30

  var ctfs = createCtfs()
  let writerRes = initCallStreamWriter(ctfs)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  var rng = initRng(77)
  for i in 0 ..< totalCalls:
    let rec = makeCallRecord(rng, i)
    let r = writeCall(ctfs, writer, rec)
    doAssert r.isOk

  let finRes = finalizeCallStream(ctfs, writer)
  doAssert finRes.isOk
  let rawBytes = ctfs.toBytes()
  let readerRes = initCallStreamReader(rawBytes)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  # Time loading 30 calls (simulating a viewport)
  let startTime = getMonoTime()

  for i in 0 ..< viewportSize:
    let readRes = readCall(reader, uint64(i))
    doAssert readRes.isOk

  let elapsedNs = (getMonoTime() - startTime).inNanoseconds
  let elapsedMs = float(elapsedNs) / 1_000_000.0

  echo "bench_call_tree_viewport_load: " & $viewportSize &
    " calls in " & $elapsedMs & " ms"
  doAssert elapsedMs < 1.0,
    "viewport load took " & $elapsedMs & " ms, expected < 1ms"

  echo "PASS: bench_call_tree_viewport_load"

# Run all tests
test_call_stream_write_read()
test_call_stream_void_return()
test_call_stream_exception_exit()
test_call_stream_nested_calls()
test_call_stream_readable_mid_run()
test_call_stream_streaming_tail_from_disk()
bench_call_tree_viewport_load()
