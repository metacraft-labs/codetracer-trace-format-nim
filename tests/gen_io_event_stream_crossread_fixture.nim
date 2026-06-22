{.push raises: [].}

## M24a-3 cross-read fixture generator.
##
## Writes a real production-shape `.ct` bundle via the multi-stream writer
## (the SPEC-canonical events.dat/events.idx chunked layout, with the
## `has_io_event_stream` meta.dat flag set) plus a sidecar
## `<bundle>.events.txt` describing, one line per I/O event (in stream order),
## the record that the Nim FFI I/O-event reader decoded back out of the bundle.
##
## Sidecar line format (one per I/O event, in stream order):
##   ``kind=<u8>;step_id=<u64>;metadata=<hex>;content=<hex>``
## ``kind`` is the on-disk ``EventLogKind`` ordinal (what the Rust reader sees),
## reconstructed from the decoded coarse ``IOEventKind`` via
## ``ioEventKindToOrdinal`` (the mapping round-trips for every IOEventKind).
##
## The companion Rust test
## (`codetracer-trace-format/codetracer_trace_reader/tests/nim_io_event_stream_crossread.rs`)
## opens the bundle with the canonical Rust `IoEventStreamReader` and asserts the
## decoded `(kind, step_id, metadata, content)` records equal this sidecar — the
## load-bearing proof that the Nim writer's events.dat is BYTE-COMPATIBLE with
## the Rust reader.
##
## Usage: `gen_io_event_stream_crossread_fixture <out.ct>`.  The writer uses the
## default events chunk size (64); the fixture emits enough I/O events that the
## stream spans multiple chunks so the round-trip exercises per-chunk independent
## decode.

import std/os
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/io_event_stream
import codetracer_trace_writer/new_trace_reader

proc fail(msg: string) {.raises: [].} =
  try:
    stderr.writeLine("gen_io_event_stream_crossread_fixture: " & msg)
  except IOError, ValueError:
    discard
  quit(1)

proc toBytes(s: string): seq[byte] {.raises: [].} =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

proc toHex(data: openArray[byte]): string {.raises: [].} =
  const digits = "0123456789abcdef"
  result = newStringOfCap(data.len * 2)
  for b in data:
    result.add(digits[int(b shr 4)])
    result.add(digits[int(b and 0x0f)])

proc main() {.raises: [].} =
  let args = commandLineParams()
  if args.len < 1:
    fail("usage: gen_io_event_stream_crossread_fixture <out.ct>")
  let outPath = args[0]

  var wRes = initMultiStreamWriter(outPath, "io_event_crossread",
    recordingId = "01949fcc-7d92-7e9c-bccc-eeeeeeeeeeee")
  if wRes.isErr: fail("init writer: " & wRes.error)
  var w = wRes.get()

  let p0Res = w.registerPath("/test/prog.py")
  if p0Res.isErr: fail("registerPath: " & p0Res.error)
  let pathId = p0Res.get()

  proc step(line: uint64) {.raises: [].} =
    let r = w.registerStep(pathId, line, [])
    if r.isErr: fail("registerStep: " & r.error)

  # Emit a generous interleaving of steps and I/O events so the I/O event stream
  # (default chunk size 64) crosses several chunk boundaries.  Cycle through all
  # four IOEventKinds and vary metadata (incl. empty) + content (incl. binary).
  const kinds = [ioStdout, ioStderr, ioFileOp, ioError]
  var ioCount = 0
  for ln in 1'u64 .. 250'u64:
    step(ln)
    # Two I/O events per step on average → ~500 events across multiple chunks.
    let n = int(ln mod 3)  # 0..2 events this step
    for j in 0 ..< n:
      let kind = kinds[(int(ln) + j) mod kinds.len]
      # Vary metadata: empty for some, a small descriptor for others.
      let meta =
        if (int(ln) + j) mod 4 == 0: newSeq[byte](0)
        else: toBytes("/tmp/f" & $ln & "_" & $j)
      # Vary content: occasionally embed NUL / high bytes to prove raw fidelity.
      var content = toBytes("line " & $ln & "." & $j & "\n")
      if (int(ln) + j) mod 5 == 0:
        content.add(@[byte(0x00), byte(0xff), byte(0x10)])
      let r = w.registerIOEvent(kind, content, metadata = meta)
      if r.isErr: fail("registerIOEvent: " & r.error)
      inc ioCount

  if ioCount == 0:
    fail("fixture produced no I/O events")

  let closeRes = w.close()
  if closeRes.isErr: fail("close: " & closeRes.error)

  let bytes = w.toBytes()
  w.closeCtfs()

  try:
    writeFile(outPath, bytes)
  except IOError:
    fail("failed to write " & outPath)

  # Sidecar: re-decode the just-written bundle with the Nim FFI I/O-event reader
  # so the Rust cross-read asserts "Rust reader decode == Nim reader decode" of
  # the SAME events.dat bytes.
  var rRes = openNewTraceFromBytes(bytes)
  if rRes.isErr: fail("reopen for sidecar: " & rRes.error)
  var reader = rRes.get()
  let countRes = reader.ioEventCount()
  if countRes.isErr: fail("ioEventCount: " & countRes.error)
  let count = countRes.get()
  if count != uint64(ioCount):
    fail("reader I/O event count " & $count & " != recorded " & $ioCount)

  var sidecar = ""
  for i in 0'u64 ..< count:
    let evRes = reader.ioEvent(i)
    if evRes.isErr: fail("ioEvent: " & evRes.error)
    let ev = evRes.get()
    sidecar.add("kind=" & $ioEventKindToOrdinal(ev.kind) &
      ";step_id=" & $ev.stepId &
      ";metadata=" & toHex(ev.metadata) &
      ";content=" & toHex(ev.data) & "\n")
  try:
    writeFile(outPath & ".events.txt", sidecar)
  except IOError:
    fail("failed to write sidecar")

  echo "wrote " & outPath & " (" & $bytes.len & " bytes), " &
    $count & " I/O events"

main()
