{.push raises: [].}

## Tests for global line index and step event encoding.

import std/strutils
import codetracer_trace_writer/global_line_index
import codetracer_trace_writer/step_encoding

proc test_global_line_index_roundtrip() {.raises: [].} =
  ## Create line index with 5 files, verify every (fileId, line) roundtrips.
  ##
  ## Lines are 1-based, so a file of N lines holds lines ``1 .. N`` — the
  ## whole of its N-address slot, first line at its own base and last line
  ## at ``base + N - 1``.
  let lineCounts = [100'u64, 200, 50, 300, 150]
  let gli = buildGlobalLineIndex(lineCounts)

  doAssert gli.totalLines == 800

  for fileId in 0 ..< lineCounts.len:
    for line in 1'u64 .. lineCounts[fileId]:
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
    of sekSourceReload:
      doAssert false,
        "the 10K mixed sequence emits no source-reload markers; tag 0x08 " &
        "has its own round-trip below because it is the one tag the " &
        "decoder refuses unless the container declares it"

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

proc test_source_reload_tag_byte() {.raises: [].} =
  ## GDH-M2: tag byte allocation check.  ``sekSourceReload`` encodes the
  ## first byte as 0x08.  Tags 0x00..0x07 are already taken, so this
  ## guards against accidental re-allocation the same way the
  ## ``DeltaColumn`` check above does.
  var buf: seq[byte]
  encodeStepEvent(StepEvent(kind: sekSourceReload, reloadOrdinal: 1,
    changed: @[SourceReloadChange(oldPathId: 0, newPathId: 2, generation: 2)],
    inFlightFrames: 0), buf)
  doAssert buf.len >= 1, "encoded SourceReload should have at least a tag byte"
  doAssert buf[0] == 0x08'u8,
    "SourceReload tag byte should be 0x08, got 0x" & $buf[0].uint
  echo "PASS: test_source_reload_tag_byte"

proc test_source_reload_roundtrip() {.raises: [].} =
  ## GDH-M2: the marker round-trips with every field, including a
  ## multi-file change list and a non-zero in-flight count, and — the
  ## point of the tag's gating — it is REFUSED BY NAME when the caller
  ## does not say the container declares it.
  let ev = StepEvent(kind: sekSourceReload,
    reloadOrdinal: 7,
    changed: @[
      SourceReloadChange(oldPathId: 0, newPathId: 4, generation: 2),
      SourceReloadChange(oldPathId: 1, newPathId: 5, generation: 3),
    ],
    inFlightFrames: 12)
  var buf: seq[byte]
  encodeStepEvent(ev, buf)

  var pos = 0
  let ok = decodeStepEvent(buf, pos, allowSourceReload = true)
  doAssert ok.isOk, "decode failed: " & ok.error
  let got = ok.get
  doAssert got.kind == sekSourceReload, "kind mismatch: " & $got.kind
  doAssert got.reloadOrdinal == 7, "ordinal mismatch: " & $got.reloadOrdinal
  doAssert got.changed.len == 2, "changed count: " & $got.changed.len
  doAssert got.changed[0].oldPathId == 0 and got.changed[0].newPathId == 4 and
    got.changed[0].generation == 2, "changed[0] mismatch"
  doAssert got.changed[1].oldPathId == 1 and got.changed[1].newPathId == 5 and
    got.changed[1].generation == 3, "changed[1] mismatch"
  doAssert got.inFlightFrames == 12, "inFlightFrames: " & $got.inFlightFrames
  doAssert pos == buf.len, "did not consume all bytes"

  # The gating, and its diagnostic.  A decoder that SKIPPED the tag would
  # re-read the payload varints as further events, so the stream would
  # decode shorter and plausibly rather than fail — which is why the
  # default is refusal and why the message has to name the tag.
  var pos2 = 0
  let refused = decodeStepEvent(buf, pos2)
  doAssert refused.isErr,
    "tag 0x08 was ACCEPTED over a container that does not declare it"
  doAssert refused.error.contains("tag: 8"),
    "the refusal does not name the tag: " & refused.error
  doAssert refused.error.contains("FlagExtHasSourceReload"),
    "the refusal does not name the missing flag: " & refused.error
  echo "PASS: test_source_reload_roundtrip"

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
test_source_reload_tag_byte()
test_source_reload_roundtrip()
bench_delta_step_bytes_per_step()
echo "ALL PASS: test_step_encoding"
