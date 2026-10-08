{.push raises: [].}

## Tests for the execution stream writer/reader.

import std/times
import results
import codetracer_ctfs/container
import codetracer_ctfs/types
import codetracer_ctfs/zstd_bindings
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

  # Compare positions, not encodings: the writer re-encodes every position
  # record by the normative rule (`trace-events.md` §"Encoding Rules"), so a
  # caller's AbsoluteStep may be written as a DeltaStep and vice versa. What
  # must hold is that each position decodes, within its chunk, to the one the
  # caller gave.
  var currentPos: uint64 = 0
  var decodedPos: uint64 = 0

  for i in 0 ..< totalSteps:
    let readRes = reader.readEvent(uint64(i))
    doAssert readRes.isOk, "readEvent failed at " & $i & ": " & readRes.error
    let got = readRes.get()
    let orig = events[i]

    if i mod 256 == 0:
      decodedPos = high(uint64)    # each chunk starts without a cursor
    case got.kind
    of sekAbsoluteStep: decodedPos = got.globalLineIndex
    of sekDeltaStep:
      doAssert decodedPos != high(uint64), "unanchored delta at " & $i
      decodedPos = uint64(int64(decodedPos) + got.lineDelta)
    else: discard

    case orig.kind
    of sekAbsoluteStep, sekDeltaStep:
      currentPos =
        if orig.kind == sekAbsoluteStep: orig.globalLineIndex
        else: uint64(int64(currentPos) + orig.lineDelta)
      doAssert got.kind in {sekAbsoluteStep, sekDeltaStep},
        "expected a position record at " & $i & ", got " & $got.kind
      doAssert decodedPos == currentPos, "position mismatch at " & $i &
        ": expected " & $currentPos & " got " & $decodedPos
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
    of sekThreadStart:
      doAssert got.kind == sekThreadStart, "expected ThreadStart at " & $i
      doAssert got.startThreadId == orig.startThreadId,
        "startThreadId mismatch at " & $i
    of sekThreadExit:
      doAssert got.kind == sekThreadExit, "expected ThreadExit at " & $i
      doAssert got.exitThreadId == orig.exitThreadId,
        "exitThreadId mismatch at " & $i
    of sekDeltaColumn:
      # Not exercised in this test's event generator, but the case must
      # be present for exhaustiveness.
      discard
    of sekSourceReload:
      # GDH-M2's tag 0x08. Not exercised here on purpose: this reader is
      # constructed WITHOUT `allowSourceReload`, so a container carrying
      # the tag would be refused at open — which is the contract, and
      # which `tests/test_gdh2_reload_marker.nim` measures.
      discard

  echo "PASS: test_exec_stream_write_read"

proc test_exec_stream_raise_catch() {.raises: [].} =
  ## Write AbsoluteStep, steps, Raise, Catch, step — read back and verify.
  var ctfs = createCtfs()
  var writerRes = initExecStreamWriter(ctfs, chunkSize = 64)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  # Position 1000 takes two varint bytes and each delta one, so by the
  # normative rule (shorter encoding, a tie to the absolute) every record is
  # written in the form given here.
  let events = @[
    StepEvent(kind: sekAbsoluteStep, globalLineIndex: 1000),
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
    of sekThreadStart:
      doAssert ev.startThreadId == orig.startThreadId
    of sekThreadExit:
      doAssert ev.exitThreadId == orig.exitThreadId
    of sekDeltaColumn:
      discard  # not generated by this test
    of sekSourceReload:
      discard  # GDH-M2's tag 0x08; not generated by this test

  echo "PASS: test_exec_stream_raise_catch"

proc test_exec_stream_thread_switch() {.raises: [].} =
  ## Write steps for thread 0, ThreadSwitch(1), steps for thread 1,
  ## ThreadSwitch(0), more steps — verify ThreadSwitch events preserved.
  var ctfs = createCtfs()
  var writerRes = initExecStreamWriter(ctfs, chunkSize = 32)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  # Positions chosen so the normative rule writes every record in the form
  # given here: each absolute is no longer than its delta, each delta shorter
  # than its position.
  let events = @[
    StepEvent(kind: sekAbsoluteStep, globalLineIndex: 5000),
    StepEvent(kind: sekDeltaStep, lineDelta: 1),
    StepEvent(kind: sekDeltaStep, lineDelta: 1),
    StepEvent(kind: sekThreadSwitch, threadId: 1),
    StepEvent(kind: sekAbsoluteStep, globalLineIndex: 20000),
    StepEvent(kind: sekDeltaStep, lineDelta: 3),
    StepEvent(kind: sekDeltaStep, lineDelta: -1),
    StepEvent(kind: sekThreadSwitch, threadId: 0),
    StepEvent(kind: sekAbsoluteStep, globalLineIndex: 5003),
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

proc test_exec_stream_delta_column_chunk_boundary() {.raises: [].} =
  ## P6.4: verify chunk-boundary semantics when ``sekDeltaColumn``
  ## events are present.
  ##
  ## Spec rule (§"Column Encoding — `DeltaColumn` (chosen)"): each
  ## chunk must be independently decodable, so the first event of every
  ## chunk must be an ``AbsoluteStep``.  The exec-stream writer
  ## auto-promotes ``sekDeltaStep`` to ``sekAbsoluteStep`` at chunk
  ## boundaries today; this test asserts the same auto-promotion holds
  ## for ``sekDeltaColumn``.
  ##
  ## We use a small chunk size so we force several boundaries with a
  ## mix of column and line deltas in between, then assert:
  ##   * total event count round-trips,
  ##   * the reader returns ``sekAbsoluteStep`` for the first event of
  ##     every chunk regardless of whether the writer was fed a column
  ##     or line delta there,
  ##   * the running ``global_position_index`` matches what the writer
  ##     should have tracked.

  let chunkSize = 4
  var ctfs = createCtfs()
  var writerRes = initExecStreamWriter(ctfs, chunkSize = chunkSize)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  # Carefully construct so the chunk-boundary event (every 4th) is
  # alternately a line delta and a column delta.  Both should be
  # promoted to AbsoluteStep at the boundary.
  var events: seq[StepEvent]
  events.add(StepEvent(kind: sekAbsoluteStep, globalLineIndex: 1000))
  for i in 1 ..< 16:
    if i mod 2 == 0:
      events.add(StepEvent(kind: sekDeltaColumn, columnDelta: 1))
    else:
      events.add(StepEvent(kind: sekDeltaStep, lineDelta: 1))

  for ev in events:
    let r = ctfs.writeEvent(writer, ev)
    doAssert r.isOk, "writeEvent failed: " & r.error

  doAssert ctfs.flush(writer).isOk

  let ctfsBytes = ctfs.toBytes()
  var readerRes = initExecStreamReader(ctfsBytes)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  doAssert reader.totalEvents == uint64(events.len),
    "totalEvents mismatch: got " & $reader.totalEvents &
    " expected " & $events.len

  # Walk back, tracking the position the writer would have tracked.
  # Boundary events (index 0, 4, 8, 12) must come back as AbsoluteStep
  # because the writer promotes them.
  var pos: uint64 = 0
  for i in 0 ..< events.len:
    let got = reader.readEvent(uint64(i))
    doAssert got.isOk, "readEvent failed at " & $i & ": " & got.error
    let ev = got.get()

    # Update expected pos against the ORIGINAL event the writer was fed.
    let orig = events[i]
    case orig.kind
    of sekAbsoluteStep:
      pos = orig.globalLineIndex
    of sekDeltaStep:
      pos = uint64(int64(pos) + orig.lineDelta)
    of sekDeltaColumn:
      pos = uint64(int64(pos) + orig.columnDelta)
    else:
      discard

    if i mod chunkSize == 0:
      doAssert ev.kind == sekAbsoluteStep,
        "chunk-boundary event " & $i & " should be AbsoluteStep, got " & $ev.kind
      doAssert ev.globalLineIndex == pos,
        "AbsoluteStep at boundary " & $i & " has wrong position: got " &
        $ev.globalLineIndex & " expected " & $pos
    else:
      # Non-boundary events should preserve their original kind.
      doAssert ev.kind == orig.kind,
        "event " & $i & " kind mismatch: got " & $ev.kind &
        " expected " & $orig.kind
      case ev.kind
      of sekDeltaStep:
        doAssert ev.lineDelta == orig.lineDelta
      of sekDeltaColumn:
        doAssert ev.columnDelta == orig.columnDelta
      else:
        discard

  echo "PASS: test_exec_stream_delta_column_chunk_boundary"

proc test_exec_stream_reads_in_any_order() {.raises: [].} =
  ## A chunk is decoded only as far as reads reach, so a read can find its
  ## chunk partly decoded. Reads in order, backwards, in a pseudo-random walk
  ## that crosses chunks, and position reads interleaved with event reads,
  ## must agree with the whole-chunk decoders (`readChunkEvents`,
  ## `resolveChunkPositions`) on every record, from fresh readers each time,
  ## and with a cache that holds one chunk as with one that holds them all.
  proc enc(ev: StepEvent): seq[byte] =
    encodeStepEvent(ev, result)
  var ctfs = createCtfs()
  var writer = initExecStreamWriter(ctfs, chunkSize = 97).get()
  for i in 0 ..< 1000:
    let ev =
      if i mod 37 == 5: StepEvent(kind: sekThreadSwitch, threadId: uint64(i))
      elif i mod 11 == 0: StepEvent(kind: sekAbsoluteStep,
        globalLineIndex: uint64(10_000 + i * 7))
      else: StepEvent(kind: sekDeltaStep, lineDelta: int64(i mod 5) - 2)
    doAssert ctfs.writeEvent(writer, ev).isOk
  doAssert ctfs.flush(writer).isOk
  let bytes = ctfs.toBytes()

  var whole = initExecStreamReader(bytes).get()
  var events: seq[StepEvent]
  var positions: seq[uint64]
  var allEvents: seq[StepEvent]
  var allPositions: seq[uint64]
  for c in 0 ..< whole.chunkCount:
    discard whole.readChunkEvents(c, events).get()
    doAssert resolveChunkPositions(events, c, positions).isOk
    allEvents.add(events)
    allPositions.add(positions)
  doAssert allEvents.len == 1000

  var orders: seq[seq[int]]
  orders.add(@[])
  for i in 0 ..< 1000: orders[^1].add(i)
  orders.add(@[])
  for i in countdown(999, 0): orders[^1].add(i)
  orders.add(@[])
  var x = 7
  for _ in 0 ..< 3000:
    x = (x * 1103 + 12345) mod 1000
    orders[^1].add(x)
  # The default budget holds every chunk; a budget of one byte holds one, so
  # every read of another chunk evicts the chunk the reader last read. The
  # last walk reads events alone, with no position read between them.
  const all = 8'u64 shl 20
  for (order, budget, withPositions) in [(orders[0], all, true),
      (orders[1], all, true), (orders[2], all, true), (orders[1], 1'u64, true),
      (orders[2], 1'u64, true), (orders[2], 1'u64, false)]:
    var r = initExecStreamReader(bytes, cacheBytes = budget).get()
    var k = 0
    for i in order:
      if withPositions and k mod 2 == 0:
        doAssert r.eventPosition(uint64(i)).get() == allPositions[i],
          "position of record " & $i
      let ev = r.readEvent(uint64(i))
      doAssert ev.isOk, ev.error
      doAssert enc(ev.get()) == enc(allEvents[i]), "record " & $i
      if withPositions:
        doAssert r.eventPosition(uint64(i)).get() == allPositions[i],
          "position of record " & $i
      inc k
    doAssert r.readEvent(1000).isErr
  echo "PASS: test_exec_stream_reads_in_any_order"

test_exec_stream_write_read()
test_exec_stream_reads_in_any_order()
test_exec_stream_raise_catch()
test_exec_stream_thread_switch()
test_exec_stream_delta_column_chunk_boundary()
bench_exec_stream_write_throughput()
echo "ALL PASS: test_exec_stream"
