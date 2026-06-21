{.push raises: [].}

## M24a-1 cross-read fixture generator.
##
## Writes a real production-shape `.ct` bundle via the multi-stream writer
## (the SPEC-canonical steps.dat/steps.idx layout, with the `has_step_stream`
## meta.dat flag set) plus a sidecar `<bundle>.steps-glis.txt` listing, one per
## line, the absolute `global_line_index` (u64) decoded back out of the bundle
## by the Nim FFI step reader (`stepAbsoluteGlobalLineIndex`).
##
## The companion Rust test
## (`codetracer-trace-format/codetracer_trace_reader/tests/nim_step_stream_crossread.rs`)
## opens the bundle with the canonical Rust `StepStreamReader` and asserts the
## decoded Step `global_line_index` sequence equals this sidecar — the
## load-bearing proof that the Nim writer's steps.dat is BYTE-COMPATIBLE with
## the Rust reader.
##
## Usage: `gen_step_stream_crossread_fixture <out.ct>`.  The chunk size is
## small (4) so the stream spans several chunks and the round-trip exercises
## per-chunk independent decode (each chunk's first Step is AbsoluteStep).

import std/os
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader

proc fail(msg: string) {.raises: [].} =
  try:
    stderr.writeLine("gen_step_stream_crossread_fixture: " & msg)
  except IOError, ValueError:
    discard
  quit(1)

proc main() {.raises: [].} =
  let args = commandLineParams()
  if args.len < 1:
    fail("usage: gen_step_stream_crossread_fixture <out.ct>")
  let outPath = args[0]

  # Small chunk size so the ~36-step stream spans multiple chunks.
  var wRes = initMultiStreamWriter(outPath, "crossread_prog", chunkSize = 4,
    recordingId = "01949fcc-7d92-7e9c-aaaa-cccccccccccc")
  if wRes.isErr: fail("init writer: " & wRes.error)
  var w = wRes.get()

  let p0Res = w.registerPath("/test/prog.py")
  if p0Res.isErr: fail("registerPath: " & p0Res.error)
  let pathId = p0Res.get()

  proc step(line: uint64) {.raises: [].} =
    let r = w.registerStep(pathId, line, [])
    if r.isErr: fail("registerStep: " & r.error)

  # A run of sequential lines (small +1 deltas), a large jump that forces an
  # AbsoluteStep, a call/return that forces AbsoluteStep, enough to cross
  # several chunks.
  for ln in 1'u64 .. 12'u64:
    step(ln)
  # A large jump well beyond the small-delta window -> AbsoluteStep.
  step(900'u64)
  step(901'u64)

  let fnRes = w.registerFunction("helper")
  if fnRes.isErr: fail("registerFunction: " & fnRes.error)
  let callRes = w.registerCall(fnRes.get(), [])
  if callRes.isErr: fail("registerCall: " & callRes.error)
  for ln in 50'u64 .. 60'u64:
    step(ln)
  let retRes = w.registerReturn(@[])
  if retRes.isErr: fail("registerReturn: " & retRes.error)
  step(13'u64)

  let closeRes = w.close()
  if closeRes.isErr: fail("close: " & closeRes.error)

  let bytes = w.toBytes()
  w.closeCtfs()

  try:
    writeFile(outPath, bytes)
  except IOError:
    fail("failed to write " & outPath)

  # Sidecar: re-decode the just-written bundle with the Nim FFI step reader so
  # the Rust cross-read asserts "Rust reader decode == Nim reader decode" of
  # the SAME steps.dat bytes.
  var rRes = openNewTraceFromBytes(bytes)
  if rRes.isErr: fail("reopen for sidecar: " & rRes.error)
  var reader = rRes.get()
  let scRes = reader.stepCount()
  if scRes.isErr: fail("stepCount: " & scRes.error)
  let stepCount = scRes.get()

  var sidecar = ""
  for n in 0'u64 ..< stepCount:
    let gliRes = reader.stepAbsoluteGlobalLineIndex(n)
    if gliRes.isErr: fail("stepAbsoluteGlobalLineIndex: " & gliRes.error)
    sidecar.add($gliRes.get() & "\n")
  try:
    writeFile(outPath & ".steps-glis.txt", sidecar)
  except IOError:
    fail("failed to write sidecar")

  echo "wrote " & outPath & " (" & $bytes.len & " bytes), " &
    $stepCount & " steps"

main()
