{.push raises: [].}

## Tests for the execution stream writer/reader.

import std/times
import results
import codetracer_ctfs/container
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/exec_stream

proc test_exec_stream_write_read() {.raises: [].} =
  ## Write 10K events with mixed types, read back each by index and verify.
  var ctfs = createCtfs()
  var writerRes = initExecStreamWriter(ctfs, chunkSize = 256)
  doAssert writerRes.isOk, "init writer failed: " & writerRes.error
  var writer = writerRes.get()

  var events: seq[StepEvent]
  let totalSteps = 10_000

  for i in 0 ..< totalSteps:
    var ev: StepEvent
    if i == 0:
      ev = StepEvent(kind: sekAbsoluteStep, globalLineIndex: 1000)
    elif i mod 500 == 0:
      let msg = @[byte('e'), byte('r'), byte('r')]
      ev = StepEvent(kind: sekRaise, exceptionTypeId: uint64(i mod 10), message: msg)
    elif i mod 500 == 1 and i > 1:
      ev = StepEvent(kind: sekCatch, catchExceptionTypeId: uint64(i mod 10))
    elif i mod 1000 == 250:
      ev = StepEvent(kind: sekThreadSwitch, threadId: uint64(i mod 4))
    elif i mod 100 == 0:
      ev = StepEvent(kind: sekAbsoluteStep, globalLineIndex: uint64(i * 3))
    else:
      ev = StepEvent(kind: sekDeltaStep, lineDelta: 1)
    events.add(ev)
    let writeRes = ctfs.writeEvent(writer, ev)
    doAssert writeRes.isOk, "writeEvent failed at " & $i & ": " & writeRes.error

  let flushRes = ctfs.flush(writer)
  doAssert flushRes.isOk, "flush failed: " & flushRes.error
  doAssert writer.totalEvents == uint64(totalSteps),
    "totalEvents mismatch: " & $writer.totalEvents

  # Serialize and read back
  let ctfsBytes = ctfs.toBytes()
  var readerRes = initExecStreamReader(ctfsBytes)
  doAssert readerRes.isOk, "init reader failed: " & readerRes.error
  var reader = readerRes.get()

  doAssert reader.totalEvents == uint64(totalSteps),
    "reader totalEvents mismatch: " & $reader.totalEvents

  # Track absolute position to verify semantics (not bit-exact encoding,
  # since the writer may convert DeltaStep to AbsoluteStep at chunk boundaries)
  var currentPos: uint64 = 0

  for i in 0 ..< totalSteps:
    let readRes = reader.readEvent(uint64(i))
    doAssert readRes.isOk, "readEvent failed at " & $i & ": " & readRes.error
    let got = readRes.get()
    let orig = events[i]

    # At chunk boundaries, DeltaStep may be converted to AbsoluteStep
    # So we compare semantically: the line position must be consistent
    case orig.kind
    of sekAbsoluteStep:
      currentPos = orig.globalLineIndex
      # The reader should return AbsoluteStep with the same index
      doAssert got.kind == sekAbsoluteStep,
        "expected AbsoluteStep at " & $i & ", got " & $got.kind
      doAssert got.globalLineIndex == orig.globalLineIndex,
        "globalLineIndex mismatch at " & $i & ": expected " &
        $orig.globalLineIndex & " got " & $got.globalLineIndex
    of sekDeltaStep:
      let expectedPos = uint64(int64(currentPos) + orig.lineDelta)
      currentPos = expectedPos
      # Could be AbsoluteStep at chunk boundary or DeltaStep
      case got.kind
      of sekAbsoluteStep:
        doAssert got.globalLineIndex == expectedPos,
          "converted AbsoluteStep mismatch at " & $i & ": expected " &
          $expectedPos & " got " & $got.globalLineIndex
      of sekDeltaStep:
        doAssert got.lineDelta == orig.lineDelta,
          "lineDelta mismatch at " & $i
      else:
        doAssert false, "unexpected event kind at " & $i & ": " & $got.kind
    of sekRaise:
      doAssert got.kind == sekRaise, "expected Raise at " & $i
      doAssert got.exceptionTypeId == orig.exceptionTypeId,
        "exceptionTypeId mismatch at " & $i
      doAssert got.message == orig.message, "message mismatch at " & $i
    of sekCatch:
      doAssert got.kind == sekCatch, "expected Catch at " & $i
      doAssert got.catchExceptionTypeId == orig.catchExceptionTypeId,
        "catchExceptionTypeId mismatch at " & $i
    of sekThreadSwitch:
      doAssert got.kind == sekThreadSwitch, "expected ThreadSwitch at " & $i
      doAssert got.threadId == orig.threadId,
        "threadId mismatch at " & $i

  echo "PASS: test_exec_stream_write_read"

proc test_exec_stream_raise_catch() {.raises: [].} =
  ## Write AbsoluteStep, steps, Raise, Catch, step — read back and verify.
  var ctfs = createCtfs()
  var writerRes = initExecStreamWriter(ctfs, chunkSize = 64)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  let events = @[
    StepEvent(kind: sekAbsoluteStep, globalLineIndex: 100),
    StepEvent(kind: sekDeltaStep, lineDelta: 1),
    StepEvent(kind: sekDeltaStep, lineDelta: 1),
    StepEvent(kind: sekRaise, exceptionTypeId: 1,
              message: @[byte('e'), byte('r'), byte('r'), byte('o'), byte('r')]),
    StepEvent(kind: sekCatch, catchExceptionTypeId: 1),
    StepEvent(kind: sekDeltaStep, lineDelta: 2),
  ]

  for ev in events:
    let r = ctfs.writeEvent(writer, ev)
    doAssert r.isOk, "writeEvent failed: " & r.error

  let flushRes = ctfs.flush(writer)
  doAssert flushRes.isOk

  let ctfsBytes = ctfs.toBytes()
  var readerRes = initExecStreamReader(ctfsBytes)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  doAssert reader.totalEvents == uint64(events.len)

  # Read back and verify exact match (all in one chunk, no boundary conversions)
  for i in 0 ..< events.len:
    let got = reader.readEvent(uint64(i))
    doAssert got.isOk, "readEvent failed at " & $i & ": " & got.error
    let ev = got.get()
    let orig = events[i]
    doAssert ev.kind == orig.kind, "kind mismatch at " & $i &
      ": expected " & $orig.kind & " got " & $ev.kind

    case ev.kind
    of sekAbsoluteStep:
      doAssert ev.globalLineIndex == orig.globalLineIndex
    of sekDeltaStep:
      doAssert ev.lineDelta == orig.lineDelta
    of sekRaise:
      doAssert ev.exceptionTypeId == orig.exceptionTypeId
      doAssert ev.message == orig.message
    of sekCatch:
      doAssert ev.catchExceptionTypeId == orig.catchExceptionTypeId
    of sekThreadSwitch:
      doAssert ev.threadId == orig.threadId

  echo "PASS: test_exec_stream_raise_catch"

proc test_exec_stream_thread_switch() {.raises: [].} =
  ## Write steps for thread 0, ThreadSwitch(1), steps for thread 1,
  ## ThreadSwitch(0), more steps — verify ThreadSwitch events preserved.
  var ctfs = createCtfs()
  var writerRes = initExecStreamWriter(ctfs, chunkSize = 32)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  let events = @[
    StepEvent(kind: sekAbsoluteStep, globalLineIndex: 50),
    StepEvent(kind: sekDeltaStep, lineDelta: 1),
    StepEvent(kind: sekDeltaStep, lineDelta: 1),
    StepEvent(kind: sekThreadSwitch, threadId: 1),
    StepEvent(kind: sekAbsoluteStep, globalLineIndex: 200),
    StepEvent(kind: sekDeltaStep, lineDelta: 3),
    StepEvent(kind: sekDeltaStep, lineDelta: -1),
    StepEvent(kind: sekThreadSwitch, threadId: 0),
    StepEvent(kind: sekAbsoluteStep, globalLineIndex: 53),
    StepEvent(kind: sekDeltaStep, lineDelta: 1),
  ]

  for ev in events:
    let r = ctfs.writeEvent(writer, ev)
    doAssert r.isOk

  let flushRes = ctfs.flush(writer)
  doAssert flushRes.isOk

  let ctfsBytes = ctfs.toBytes()
  var readerRes = initExecStreamReader(ctfsBytes)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  doAssert reader.totalEvents == uint64(events.len)

  for i in 0 ..< events.len:
    let got = reader.readEvent(uint64(i))
    doAssert got.isOk, "readEvent failed at " & $i
    let ev = got.get()
    let orig = events[i]
    doAssert ev.kind == orig.kind, "kind mismatch at " & $i

    case ev.kind
    of sekThreadSwitch:
      doAssert ev.threadId == orig.threadId,
        "threadId mismatch at " & $i & ": expected " &
        $orig.threadId & " got " & $ev.threadId
    of sekAbsoluteStep:
      doAssert ev.globalLineIndex == orig.globalLineIndex
    of sekDeltaStep:
      doAssert ev.lineDelta == orig.lineDelta
    else:
      discard

  echo "PASS: test_exec_stream_thread_switch"

proc bench_exec_stream_write_throughput() {.raises: [].} =
  ## Write 1M step events (90% DeltaStep, 10% AbsoluteStep), measure throughput.
  let totalSteps = 1_000_000

  var ctfs = createCtfs()
  var writerRes = initExecStreamWriter(ctfs)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  let startTime = cpuTime()

  for i in 0 ..< totalSteps:
    var ev: StepEvent
    if i mod 10 == 0:
      ev = StepEvent(kind: sekAbsoluteStep, globalLineIndex: uint64(i * 2))
    else:
      ev = StepEvent(kind: sekDeltaStep, lineDelta: 1)
    let r = ctfs.writeEvent(writer, ev)
    doAssert r.isOk

  let flushRes = ctfs.flush(writer)
  doAssert flushRes.isOk

  let elapsed = cpuTime() - startTime
  let eventsPerSec = float(totalSteps) / elapsed
  let ctfsBytes = ctfs.toBytes()
  let datSize = ctfsBytes.len  # approximate, includes container overhead

  echo "{\"total_events\": " & $totalSteps &
    ", \"elapsed_sec\": " & $elapsed &
    ", \"events_per_sec\": " & $eventsPerSec &
    ", \"container_bytes\": " & $datSize & "}"

  echo "PASS: bench_exec_stream_write_throughput"

test_exec_stream_write_read()
test_exec_stream_raise_catch()
test_exec_stream_thread_switch()
bench_exec_stream_write_throughput()
echo "ALL PASS: test_exec_stream"
