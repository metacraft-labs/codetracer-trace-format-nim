when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Tests for the IO event stream writer/reader (M24a-3 SPEC chunked layout).
##
## The on-disk format is now the SPEC-canonical chunked Zstd ``events.dat`` +
## ``events.idx`` (byte-compatible with the Rust ``IoEventStreamReader``).  Each
## record is ``u8 kind (EventLogKind ordinal), varint step_id, len+metadata,
## len+content`` — byte-identical to the Rust ``IoEventRecord::encode``.  These
## tests exercise multi-chunk streams, the per-chunk independent decode, the
## metadata round-trip, and the legacy ``.off`` VRT back-compat path.

import std/times
import results
import codetracer_ctfs/container
import codetracer_ctfs/variable_record_table
import codetracer_trace_writer/io_event_stream
import codetracer_trace_writer/varint

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
  let metaLen = int(rng.next() mod 8)  # 0..7 metadata bytes (incl. empty)
  let dataLen = int(rng.next() mod 50) + 1
  IOEvent(
    kind: kind,
    stepId: stepId,
    metadata: makeData(rng, metaLen),
    data: makeData(rng, dataLen),
  )

proc assertEqualEvent(got, expected: IOEvent, ctx: string) =
  doAssert got.kind == expected.kind,
    ctx & ": kind mismatch: got " & $got.kind & " expected " & $expected.kind
  doAssert got.stepId == expected.stepId, ctx & ": stepId mismatch"
  doAssert got.metadata == expected.metadata, ctx & ": metadata mismatch"
  doAssert got.data == expected.data, ctx & ": data mismatch"

# ---------------------------------------------------------------------------
# test_io_event_stream_write_read — multi-chunk round trip
# ---------------------------------------------------------------------------

proc test_io_event_stream_write_read() {.raises: [].} =
  const numEvents = 1000
  const numChecks = 100

  var ctfs = createCtfs()
  # Small chunk size so the stream spans many chunks (exercises per-chunk seek
  # + the multi-chunk record-count recovery).
  let writerRes = initIOEventStreamWriter(ctfs, chunkSize = 16)
  doAssert writerRes.isOk, "initIOEventStreamWriter failed: " & writerRes.error
  var writer = writerRes.get()

  var writeRng = initRng(42)
  for i in 0 ..< numEvents:
    let ev = makeIOEvent(writeRng, i)
    let r = writeEvent(ctfs, writer, ev)
    doAssert r.isOk, "writeEvent failed at index " & $i & ": " & r.error
  doAssert io_event_stream.flush(ctfs, writer).isOk

  let rawBytes = ctfs.toBytes()
  let readerRes = initIOEventStreamReader(rawBytes)
  doAssert readerRes.isOk, "initIOEventStreamReader failed: " & readerRes.error
  var reader = readerRes.get()
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
    assertEqualEvent(readRes.get(), expected, "event " & $idx)

  echo "PASS: test_io_event_stream_write_read"

# ---------------------------------------------------------------------------
# test_io_event_stream_page_load — sequential page (within / across chunks)
# ---------------------------------------------------------------------------

proc test_io_event_stream_page_load() {.raises: [].} =
  const numEvents = 500

  var ctfs = createCtfs()
  let writerRes = initIOEventStreamWriter(ctfs, chunkSize = 16)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  var writeRng = initRng(55)
  for i in 0 ..< numEvents:
    let ev = makeIOEvent(writeRng, i)
    let r = writeEvent(ctfs, writer, ev)
    doAssert r.isOk
  doAssert io_event_stream.flush(ctfs, writer).isOk

  let rawBytes = ctfs.toBytes()
  let readerRes = initIOEventStreamReader(rawBytes)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  # Read events 100-149 (50 events) — spans multiple chunks.
  var replayRng = initRng(55)
  for s in 0 ..< 100:
    discard makeIOEvent(replayRng, s)

  for i in 100 ..< 150:
    let expected = makeIOEvent(replayRng, i)
    let readRes = readEvent(reader, uint64(i))
    doAssert readRes.isOk, "readEvent failed at index " & $i & ": " & readRes.error
    assertEqualEvent(readRes.get(), expected, "page event " & $i)

  echo "PASS: test_io_event_stream_page_load"

# ---------------------------------------------------------------------------
# test_io_event_kind_roundtrip — the IOEventKind ↔ EventLogKind-ordinal map
# ---------------------------------------------------------------------------

proc test_io_event_kind_roundtrip() {.raises: [].} =
  # Every coarse IOEventKind must survive the on-disk EventLogKind-ordinal
  # encoding so ct-print's io_kind output is unchanged across the format flip.
  for k in IOEventKind:
    let ord = ioEventKindToOrdinal(k)
    doAssert ordinalToIOEventKind(ord) == k,
      "kind round-trip broken for " & $k & " (ord " & $ord & ")"

  # And the canonical ordinals are exactly the spec EventLogKind values used by
  # the cross-read sidecar.
  doAssert ioEventKindToOrdinal(ioStdout) == 0'u8
  doAssert ioEventKindToOrdinal(ioStderr) == 12'u8
  doAssert ioEventKindToOrdinal(ioFileOp) == 4'u8
  doAssert ioEventKindToOrdinal(ioError) == 11'u8

  echo "PASS: test_io_event_kind_roundtrip"

# ---------------------------------------------------------------------------
# test_io_event_stream_legacy_back_compat — old .off VRT bundles still read
# ---------------------------------------------------------------------------

proc encodeLegacyIOEvent(ev: IOEvent): seq[byte] =
  ## Encode one event in the PRE-M24a-3 legacy framing:
  ## ``u8 kind (IOEventKind ordinal), varint stepId, varint data_len, data``.
  ## Used to synthesize an old-format ``events.dat``/``events.off`` VRT so we
  ## can prove the reader's legacy path still works.  Note: legacy framing had
  ## no metadata field and the kind byte was the IOEventKind ordinal.
  var rec: seq[byte] = @[]
  rec.add(byte(ev.kind))
  encodeVarint(ev.stepId, rec)
  encodeVarint(uint64(ev.data.len), rec)
  rec.add(ev.data)
  rec

proc test_io_event_stream_legacy_back_compat() {.raises: [].} =
  var ctfs = createCtfs()
  let tableRes = initVariableRecordTableWriter(ctfs, "events")
  doAssert tableRes.isOk
  var table = tableRes.get()

  var rng = initRng(555)
  var events: seq[IOEvent] = @[]
  for i in 0 ..< 50:
    # Legacy events carry no metadata.
    let ev = IOEvent(
      kind: IOEventKind(rng.next() mod 4),
      stepId: rng.next() mod 100000,
      metadata: @[],
      data: makeData(rng, int(rng.next() mod 30) + 1))
    events.add(ev)
    let appendRes = ctfs.append(table, encodeLegacyIOEvent(ev))
    doAssert appendRes.isOk, "append legacy record failed: " & appendRes.error

  let rawBytes = ctfs.toBytes()
  let readerRes = initIOEventStreamReader(rawBytes, legacy = true)
  doAssert readerRes.isOk, "legacy reader init failed: " & readerRes.error
  var reader = readerRes.get()
  doAssert reader.count == 50, "legacy count mismatch: got " & $reader.count

  for i in 0 ..< 50:
    let got = readEvent(reader, uint64(i))
    doAssert got.isOk, "legacy readEvent failed at " & $i & ": " & got.error
    assertEqualEvent(got.get(), events[i], "legacy event " & $i)

  echo "PASS: test_io_event_stream_legacy_back_compat"

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
  doAssert io_event_stream.flush(ctfs, writer).isOk

  let rawBytes = ctfs.toBytes()
  let readerRes = initIOEventStreamReader(rawBytes)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  # Time loading 50 events (simulating a page)
  let startTime = cpuTime()

  for i in 100 ..< 100 + pageSize:
    let readRes = readEvent(reader, uint64(i))
    doAssert readRes.isOk

  let elapsed = cpuTime() - startTime
  let elapsedMs = elapsed * 1000.0

  echo "bench_io_event_page_load: " & $pageSize &
    " events in " & $elapsedMs & " ms"
  doAssert elapsedMs < 5.0,
    "page load took " & $elapsedMs & " ms, expected < 5ms"

  echo "PASS: bench_io_event_page_load"

# Run all tests
test_io_event_stream_write_read()
test_io_event_stream_page_load()
test_io_event_kind_roundtrip()
test_io_event_stream_legacy_back_compat()
bench_io_event_page_load()
