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

  doAssert pos == buf.len, "did not consume all bytes"

  echo "PASS: test_delta_step_encode_decode"

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
bench_delta_step_bytes_per_step()
echo "ALL PASS: test_step_encoding"
