{.push raises: [].}

## Tests for global line index and step event encoding.

import codetracer_trace_writer/global_line_index
import codetracer_trace_writer/step_encoding

proc test_global_line_index_roundtrip() {.raises: [].} =
  ## Create line index with 5 files, verify every (fileId, line) roundtrips.
  let lineCounts = [100'u64, 200, 50, 300, 150]
  let gli = buildGlobalLineIndex(lineCounts)

  doAssert gli.totalLines == 800

  for fileId in 0 ..< lineCounts.len:
    for line in 0'u64 ..< lineCounts[fileId]:
      let gi = gli.globalIndex(fileId, line)
      let (resolvedFileId, resolvedLine) = gli.resolve(gi)
      doAssert resolvedFileId == fileId,
        "fileId mismatch: expected " & $fileId & " got " & $resolvedFileId &
        " for globalIndex " & $gi
      doAssert resolvedLine == line,
        "line mismatch: expected " & $line & " got " & $resolvedLine &
        " for fileId " & $fileId

  echo "PASS: test_global_line_index_roundtrip"

proc test_delta_step_encode_decode() {.raises: [].} =
  ## Encode 10K steps with mixed event types, decode and verify exact match.
  var events: seq[StepEvent]
  let totalSteps = 10_000

  for i in 0 ..< totalSteps:
    if i == 0:
      # First event is always AbsoluteStep
      events.add(StepEvent(kind: sekAbsoluteStep, globalLineIndex: 1000))
    elif i mod 500 == 0:
      # Intersperse Raise events
      let msg = @[byte('e'), byte('r'), byte('r')]
      events.add(StepEvent(kind: sekRaise, exceptionTypeId: uint64(i mod 10), message: msg))
    elif i mod 500 == 1:
      # Catch after Raise
      events.add(StepEvent(kind: sekCatch, catchExceptionTypeId: uint64(i mod 10)))
    elif i mod 1000 == 250:
      # ThreadSwitch
      events.add(StepEvent(kind: sekThreadSwitch, threadId: uint64(i mod 4)))
    elif i mod 100 == 0:
      # Occasional AbsoluteStep
      events.add(StepEvent(kind: sekAbsoluteStep, globalLineIndex: uint64(i * 3)))
    else:
      # Most steps are DeltaStep with small delta
      events.add(StepEvent(kind: sekDeltaStep, lineDelta: 1))

  # Encode all events
  var buf: seq[byte]
  for event in events:
    encodeStepEvent(event, buf)

  # Decode all events and verify
  var pos = 0
  for i in 0 ..< events.len:
    let decoded = decodeStepEvent(buf, pos)
    doAssert decoded.isOk, "decode failed at event " & $i & ": " & decoded.error
    let ev = decoded.get
    let orig = events[i]
    doAssert ev.kind == orig.kind, "kind mismatch at event " & $i

    case ev.kind
    of sekAbsoluteStep:
      doAssert ev.globalLineIndex == orig.globalLineIndex,
        "globalLineIndex mismatch at event " & $i
    of sekDeltaStep:
      doAssert ev.lineDelta == orig.lineDelta,
        "lineDelta mismatch at event " & $i
    of sekRaise:
      doAssert ev.exceptionTypeId == orig.exceptionTypeId,
        "exceptionTypeId mismatch at event " & $i
      doAssert ev.message == orig.message,
        "message mismatch at event " & $i
    of sekCatch:
      doAssert ev.catchExceptionTypeId == orig.catchExceptionTypeId,
        "catchExceptionTypeId mismatch at event " & $i
    of sekThreadSwitch:
      doAssert ev.threadId == orig.threadId,
        "threadId mismatch at event " & $i
    of sekThreadStart:
      doAssert ev.startThreadId == orig.startThreadId,
        "startThreadId mismatch at event " & $i
    of sekThreadExit:
      doAssert ev.exitThreadId == orig.exitThreadId,
        "exitThreadId mismatch at event " & $i
    of sekDeltaColumn:
      doAssert ev.columnDelta == orig.columnDelta,
        "columnDelta mismatch at event " & $i

  doAssert pos == buf.len, "did not consume all bytes"

  echo "PASS: test_delta_step_encode_decode"

proc test_delta_column_tag_byte() {.raises: [].} =
  ## P6.4: tag byte allocation check.  ``sekDeltaColumn`` encodes the
  ## first byte as 0x07 — see spec §"Column Encoding —
  ## `DeltaColumn` (chosen)".  Tags 0x00..0x06 are already taken
  ## (AbsoluteStep, DeltaStep, Raise, Catch, ThreadSwitch, ThreadStart,
  ## ThreadExit) so this guards against accidental re-allocation.
  var buf: seq[byte]
  encodeStepEvent(StepEvent(kind: sekDeltaColumn, columnDelta: 1), buf)
  doAssert buf.len >= 1, "encoded DeltaColumn should have at least the tag byte"
  doAssert buf[0] == 0x07'u8,
    "DeltaColumn tag byte should be 0x07, got 0x" & $buf[0].uint
  # Tag(1) + signed zigzag varint(1) for delta=±1 → 2 bytes total.
  doAssert buf.len == 2,
    "encoded DeltaColumn(±1) should be exactly 2 bytes, got " & $buf.len
  echo "PASS: test_delta_column_tag_byte"

proc test_delta_column_roundtrip() {.raises: [].} =
  ## P6.4: round-trip ``sekDeltaColumn`` events across a representative
  ## range of column deltas — including the sign-bit corner case
  ## (negative) and the boundary that promotes from 1 to 2 zigzag varint
  ## bytes (±63 vs ±64).
  let deltas = [1'i64, -1, 7, -7, 63, -64, 64, -1000, 1048575, -1048576]
  for d in deltas:
    var buf: seq[byte]
    encodeStepEvent(StepEvent(kind: sekDeltaColumn, columnDelta: d), buf)
    var pos = 0
    let decoded = decodeStepEvent(buf, pos)
    doAssert decoded.isOk, "decode failed for delta " & $d & ": " & decoded.error
    let ev = decoded.get
    doAssert ev.kind == sekDeltaColumn,
      "expected sekDeltaColumn for delta " & $d & ", got " & $ev.kind
    doAssert ev.columnDelta == d,
      "columnDelta mismatch for delta " & $d & ": got " & $ev.columnDelta
    doAssert pos == buf.len,
      "did not consume all bytes for delta " & $d
  echo "PASS: test_delta_column_roundtrip"

proc test_mixed_event_sequence_roundtrip() {.raises: [].} =
  ## P6.4: a mixed sequence — AbsoluteStep + DeltaStep + DeltaColumn +
  ## DeltaStep + DeltaColumn — exercises that the new tag interleaves
  ## with the existing tags correctly.  This mirrors what a real
  ## column-aware recorder would emit: an absolute open, a line move, a
  ## column nudge within the new line, another line move, another
  ## column nudge.
  let events = @[
    StepEvent(kind: sekAbsoluteStep, globalLineIndex: 42),
    StepEvent(kind: sekDeltaStep,    lineDelta: 1),
    StepEvent(kind: sekDeltaColumn,  columnDelta: 5),
    StepEvent(kind: sekDeltaStep,    lineDelta: 1),
    StepEvent(kind: sekDeltaColumn,  columnDelta: -3),
  ]
  var buf: seq[byte]
  for ev in events:
    encodeStepEvent(ev, buf)
  var pos = 0
  for i in 0 ..< events.len:
    let decoded = decodeStepEvent(buf, pos)
    doAssert decoded.isOk, "decode failed at index " & $i & ": " & decoded.error
    let got = decoded.get
    let exp = events[i]
    doAssert got.kind == exp.kind,
      "kind mismatch at index " & $i & ": got " & $got.kind &
      ", expected " & $exp.kind
    case exp.kind
    of sekAbsoluteStep:
      doAssert got.globalLineIndex == exp.globalLineIndex
    of sekDeltaStep:
      doAssert got.lineDelta == exp.lineDelta
    of sekDeltaColumn:
      doAssert got.columnDelta == exp.columnDelta
    else:
      doAssert false, "unexpected kind in mixed-event test"
  doAssert pos == buf.len, "did not consume all bytes in mixed-event test"
  echo "PASS: test_mixed_event_sequence_roundtrip"

proc bench_delta_step_bytes_per_step() {.raises: [].} =
  ## Encode 100K steps (90% DeltaStep, 10% AbsoluteStep), measure bytes/step.
  let totalSteps = 100_000
  var buf: seq[byte]

  for i in 0 ..< totalSteps:
    if i mod 10 == 0:
      encodeStepEvent(StepEvent(kind: sekAbsoluteStep, globalLineIndex: uint64(i * 2)), buf)
    else:
      encodeStepEvent(StepEvent(kind: sekDeltaStep, lineDelta: 1), buf)

  let bytesPerStep = float(buf.len) / float(totalSteps)

  echo "{\"total_bytes\": " & $buf.len &
    ", \"total_steps\": " & $totalSteps &
    ", \"bytes_per_step\": " & $bytesPerStep & "}"

  doAssert bytesPerStep < 3.0,
    "average bytes per step too high: " & $bytesPerStep & " (expected < 3)"

  echo "PASS: bench_delta_step_bytes_per_step"

test_global_line_index_roundtrip()
test_delta_step_encode_decode()
test_delta_column_tag_byte()
test_delta_column_roundtrip()
test_mixed_event_sequence_roundtrip()
bench_delta_step_bytes_per_step()
echo "ALL PASS: test_step_encoding"
