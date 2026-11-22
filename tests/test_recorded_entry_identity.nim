## Real writer/reader/container boundaries; no mock objects are used.
import std/[options, os]
import results
import codetracer_ctfs/types
import codetracer_ctfs/base40
import codetracer_ctfs/container
import codetracer_ctfs/container_append
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

block:
  # A caller's scan limit larger than the root the header declares must not
  # count a member's data as a second `entry.dat`: the duplicate check reads
  # only the header-declared slots. Slot `k` of an unbounded scan starts at
  # byte `16 + 24 * k`; for a data block `b` with `b mod 3 == 1` that is the
  # block's first byte when `k = (4096 * b - 16) div 24`, so the member's
  # bytes 16..23 are that slot's name word.
  var w = makeWriter()
  checked w.registerCall(0, @[])
  checked w.registerStep(0, 1, @[])
  checked w.registerCall(1, @[])
  checked w.markCurrentCallAsEntry()
  checked w.registerStep(0, 2, @[])
  checked w.registerReturn()
  checked w.registerReturn()
  checked w.close()
  let traced = w.toBytes()
  doAssert readU32LE(traced, 8) == 4096'u32
  var poisoned = newSeq[byte](24)
  writeU64LE(poisoned, 16, base40Encode("entry.dat"))
  # One-byte fillers take one direct block each, so the poisoned member's
  # data block is the next block number `b` with `b mod 3 == 1`.
  let firstFree = traced.len div 4096
  var names: seq[string] = @[]
  var contents: seq[seq[byte]] = @[]
  for i in 0 ..< (4 - firstFree mod 3) mod 3:
    names.add("pad" & $i)
    contents.add(@[byte(i)])
  names.add("x")
  contents.add(poisoned)
  let path = getTempDir() / ("entry-dat-overscan-" & $getCurrentProcessId() & ".ct")
  var raw = newString(traced.len)
  for i, b in traced: raw[i] = char(b)
  writeFile(path, raw)
  doAssert appendInternalFiles(path, names, contents).isOk
  let image = readCtfsFromFile(path).get()
  removeFile(path)
  let x = findFileEntry(image, "x", 0'u32)
  doAssert x.found and isDirectMapBlock(x.mapBlock)
  let dataBlock = int(directDataBlock(x.mapBlock))
  doAssert dataBlock mod 3 == 1, "the poisoned data block is " & $dataBlock
  let overscanSlot = (dataBlock * 4096 - 16) div 24
  doAssert readU64LE(image, 16 + overscanSlot * 24 + 16) == base40Encode("entry.dat"),
    "the poisoned word is not where an unbounded scan reads a name"
  let opened = openNewTraceFromBytes(image, 4096, uint32(overscanSlot + 1))
  doAssert opened.isOk, opened.error
  doAssert opened.get().recordedEntryIdentity() ==
    some(RecordedEntryIdentity(callKey: 1, functionId: 1, entryStep: 1))
  echo "PASS a caller scan limit past the declared root counts no data as entry.dat"
