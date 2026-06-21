{.push raises: [].}

## M24a-2 cross-read fixture generator.
##
## Writes a real production-shape `.ct` bundle via the multi-stream writer
## (the SPEC-canonical values.dat/values.idx chunked layout, with the
## `has_value_stream` meta.dat flag set) plus a sidecar
## `<bundle>.values.txt` describing, one line per step, the per-step value
## record that the Nim FFI value reader decoded back out of the bundle.
##
## Sidecar line format (one per step, in step order):
##   ``<name_id>=<hex(cbor)>;<name_id>=<hex(cbor)>;...``
## A value-less step is an EMPTY line.  This captures the parallel-index
## invariant (record N ↔ step N, empty for value-less steps).
##
## The companion Rust test
## (`codetracer-trace-format/codetracer_trace_reader/tests/nim_value_stream_crossread.rs`)
## opens the bundle with the canonical Rust `ValueStreamReader` and asserts the
## decoded per-step `StepValues` `(name_id, CBOR bytes)` pairs equal this
## sidecar — the load-bearing proof that the Nim writer's values.dat is
## BYTE-COMPATIBLE with the Rust reader.
##
## Usage: `gen_value_stream_crossread_fixture <out.ct>`.  Chunk size is small
## (4) so the stream spans multiple value chunks and the round-trip exercises
## per-chunk independent decode.

import std/os
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/cbor
import codetracer_trace_types

proc fail(msg: string) {.raises: [].} =
  try:
    stderr.writeLine("gen_value_stream_crossread_fixture: " & msg)
  except IOError, ValueError:
    discard
  quit(1)

proc encInt(value: int64, typeId: uint64): seq[byte] {.raises: [].} =
  var enc = CborEncoder.init()
  enc.encodeCborValueRecord(ValueRecord(
    kind: vrkInt, intVal: value, intTypeId: TypeId(typeId)))
  enc.getBytes()

proc encStr(text: string, typeId: uint64): seq[byte] {.raises: [].} =
  var enc = CborEncoder.init()
  enc.encodeCborValueRecord(ValueRecord(
    kind: vrkString, text: text, strTypeId: TypeId(typeId)))
  enc.getBytes()

proc toHex(data: openArray[byte]): string {.raises: [].} =
  const digits = "0123456789abcdef"
  result = newStringOfCap(data.len * 2)
  for b in data:
    result.add(digits[int(b shr 4)])
    result.add(digits[int(b and 0x0f)])

proc main() {.raises: [].} =
  let args = commandLineParams()
  if args.len < 1:
    fail("usage: gen_value_stream_crossread_fixture <out.ct>")
  let outPath = args[0]

  # Small chunk size so the value stream spans multiple chunks.  The exec
  # chunkSize is independent of the value chunkSize (256 default), so make the
  # step stream large enough that the value stream crosses chunk boundaries.
  var wRes = initMultiStreamWriter(outPath, "value_crossread", chunkSize = 4,
    recordingId = "01949fcc-7d92-7e9c-bbbb-dddddddddddd")
  if wRes.isErr: fail("init writer: " & wRes.error)
  var w = wRes.get()

  let p0Res = w.registerPath("/test/prog.py")
  if p0Res.isErr: fail("registerPath: " & p0Res.error)
  let pathId = p0Res.get()

  let tIntRes = w.registerType("int")
  if tIntRes.isErr: fail("registerType int: " & tIntRes.error)
  let tInt = tIntRes.get()
  let tStrRes = w.registerType("str")
  if tStrRes.isErr: fail("registerType str: " & tStrRes.error)
  let tStr = tStrRes.get()

  let vnXRes = w.registerVarname("x")
  if vnXRes.isErr: fail("registerVarname x: " & vnXRes.error)
  let vnX = vnXRes.get()
  let vnSRes = w.registerVarname("s")
  if vnSRes.isErr: fail("registerVarname s: " & vnSRes.error)
  let vnS = vnSRes.get()

  # Emit a generous number of steps so the value stream (chunk size 256, but we
  # force several chunks by also writing many steps) crosses chunk boundaries.
  # We additionally call initValueStreamWriter with a small chunk via the
  # writer's default; to truly cross chunks here, interleave many steps.
  proc step(line: uint64, vals: openArray[VariableValue]) {.raises: [].} =
    let r = w.registerStep(pathId, line, vals)
    if r.isErr: fail("registerStep: " & r.error)

  # Mix value-bearing and value-less steps to verify the parallel index.
  for ln in 1'u64 .. 600'u64:
    if ln mod 5 == 0:
      step(ln, [])  # value-less step (empty record)
    elif ln mod 3 == 0:
      step(ln, [
        VariableValue(varnameId: vnX, typeId: tInt, data: encInt(int64(ln), tInt)),
        VariableValue(varnameId: vnS, typeId: tStr,
          data: encStr("v" & $ln, tStr)),
      ])
    else:
      step(ln, [
        VariableValue(varnameId: vnX, typeId: tInt, data: encInt(int64(ln), tInt)),
      ])

  let closeRes = w.close()
  if closeRes.isErr: fail("close: " & closeRes.error)

  let bytes = w.toBytes()
  w.closeCtfs()

  try:
    writeFile(outPath, bytes)
  except IOError:
    fail("failed to write " & outPath)

  # Sidecar: re-decode the just-written bundle with the Nim FFI value reader so
  # the Rust cross-read asserts "Rust reader decode == Nim reader decode" of
  # the SAME values.dat bytes.
  var rRes = openNewTraceFromBytes(bytes)
  if rRes.isErr: fail("reopen for sidecar: " & rRes.error)
  var reader = rRes.get()
  let scRes = reader.stepCount()
  if scRes.isErr: fail("stepCount: " & scRes.error)
  let stepCount = scRes.get()

  var sidecar = ""
  for n in 0'u64 ..< stepCount:
    let valsRes = reader.values(n)
    if valsRes.isErr: fail("values: " & valsRes.error)
    var line = ""
    var first = true
    for v in valsRes.get():
      if not first: line.add(";")
      first = false
      line.add($v.varnameId & "=" & toHex(v.data))
    sidecar.add(line & "\n")
  try:
    writeFile(outPath & ".values.txt", sidecar)
  except IOError:
    fail("failed to write sidecar")

  echo "wrote " & outPath & " (" & $bytes.len & " bytes), " &
    $stepCount & " steps"

main()
