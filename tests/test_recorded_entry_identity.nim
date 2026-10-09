## Real writer/reader/container boundaries; no mock objects are used.
import std/options
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/entry_identity

proc checked(res: Result[void, string]) =
  doAssert res.isOk, res.error

proc makeWriter(): MultiStreamTraceWriter =
  let initialized = initMultiStreamWriter("", "real-entry-control")
  doAssert initialized.isOk, initialized.error
  result = initialized.get()
  doAssert result.registerPath("/src/program.sol").isOk
  doAssert result.registerFunctionAt("/src/program.sol", 1, "<toplevel>").get() == 0
  doAssert result.registerFunctionAt("/src/program.sol", 2, "compute").get() == 1

block:
  var w = makeWriter()
  doAssert w.markCurrentCallAsEntry().isErr
  checked w.registerCall(0, @[])
  checked w.registerStep(0, 1, @[])
  checked w.registerCall(1, @[])
  checked w.markCurrentCallAsEntry()
  checked w.markCurrentCallAsEntry()
  checked w.registerStep(0, 2, @[])
  checked w.registerReturn()
  checked w.registerReturn()
  checked w.close()
  let opened = openNewTraceFromBytes(w.toBytes())
  doAssert opened.isOk, opened.error
  let entry = opened.get().recordedEntryIdentity()
  doAssert entry.isSome
  doAssert entry.get() == RecordedEntryIdentity(callKey: 1, functionId: 1, entryStep: 1)
  echo "PASS real finalized entry, repeated mark and same-image reader"

block:
  var w = makeWriter()
  checked w.registerCall(0, @[])
  checked w.registerStep(0, 1, @[])
  checked w.registerCall(1, @[])
  checked w.markCurrentCallAsEntry()
  checked w.registerStep(0, 2, @[])
  # Real close finalizes still-open calls exactly as existing trap/revert path.
  checked w.close()
  let opened = openNewTraceFromBytes(w.toBytes())
  doAssert opened.isOk, opened.error
  doAssert opened.get().recordedEntryIdentity().isSome
  echo "PASS genuine existing partial finalization"

block:
  var w = makeWriter()
  checked w.registerCall(0, @[])
  checked w.markCurrentCallAsEntry()
  checked w.registerStep(0, 1, @[])
  checked w.registerReturn()
  checked w.registerCall(1, @[])
  doAssert w.markCurrentCallAsEntry().isErr
  doAssert w.registerReturn().isErr
  echo "PASS changed second entry and later empty-root ownership refusal"

block:
  var w = makeWriter()
  checked w.registerCall(0, @[])
  checked w.registerStep(0, 1, @[])
  checked w.registerReturn()
  checked w.close()
  let opened = openNewTraceFromBytes(w.toBytes())
  doAssert opened.isOk, opened.error
  doAssert opened.get().recordedEntryIdentity().isNone
  echo "PASS genuine unmarked absence"

block:
  let valid = encodeEntryIdentity(RecordedEntryIdentity(callKey: 1, functionId: 1, entryStep: 1)).get()
  doAssert decodeEntryIdentity(valid).isOk
  for length in 0 ..< valid.len:
    doAssert decodeEntryIdentity(valid[0 ..< length]).isErr
  var extra = valid
  extra.add(0)
  doAssert decodeEntryIdentity(extra).isErr
  var reserved = valid
  reserved[6] = 1
  doAssert decodeEntryIdentity(reserved).isErr
  var overlong = valid[0 .. 7]
  overlong.add(@[0x81.byte, 0x00.byte, 0x01.byte, 0x01.byte])
  doAssert decodeEntryIdentity(overlong).isErr
  echo "PASS canonical truncation, reserved, trailing and overlong refusal"
