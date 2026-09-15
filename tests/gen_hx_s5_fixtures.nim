{.push raises: [].}

## Fixture generator for Milestone HX-S-5:
## "Value-stream forward compatibility, at the format level"
##
## Generates real CTFS containers for testing forward compatibility:
## - "control": clean trace with 10 steps and variables (tags 0 only).
## - "forward_compat": same trace, but step 0 carries unknown tag 10 with length prefix.
## - "malformed": same trace, but step 0 carries tag 10 with truncated payload.

import std/os
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/cbor
import codetracer_trace_types

proc fail(msg: string) {.raises: [].} =
  try:
    stderr.writeLine("gen_hx_s5_fixtures: " & msg)
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

proc main() {.raises: [].} =
  let args = commandLineParams()
  if args.len < 2:
    fail("usage: gen_hx_s5_fixtures <control|forward_compat|malformed> <out.ct>")
  let mode = args[0]
  let outPath = args[1]

  var wRes = initMultiStreamWriter(outPath, "hx_s5_fixture", chunkSize = 4,
    recordingId = "01949fcc-7d92-7e9c-bbbb-000000000005")
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

  for ln in 1'u64 .. 10'u64:
    let vals = @[
      VariableValue(varnameId: vnX, typeId: tInt, data: encInt(int64(ln * 10), tInt)),
      VariableValue(varnameId: vnS, typeId: tStr, data: encStr("step_" & $ln, tStr))
    ]
    var extra: seq[byte] = @[]
    if ln == 1'u64:
      if mode == "forward_compat":
        # Forward-compatible event tag 10 with length prefix and 4 payload bytes
        encodeLengthPrefixedEvent(10'u8, [0x01'u8, 0x02'u8, 0x03'u8, 0x04'u8], extra)
      elif mode == "malformed":
        # Malformed: tag 10 with declared payload length of 100 bytes, but only 2 bytes provided
        extra = @[10'u8, 100'u8, 0xAA'u8, 0xBB'u8]
    let r = w.registerStep(pathId, ln, vals, extra)
    if r.isErr: fail("registerStep: " & r.error)

  let closeRes = w.close()
  if closeRes.isErr: fail("close: " & closeRes.error)

  let bytes = w.toBytes()
  w.closeCtfs()

  try:
    writeFile(outPath, bytes)
  except IOError:
    fail("failed to write " & outPath)

main()
