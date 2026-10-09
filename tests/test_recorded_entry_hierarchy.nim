## Modify only finalized call records in a genuine retained CTFS image.
## Negative protocol corruption is intentional; no mock objects or substitute positive trace.
import std/[os, json, strutils]
import results
import codetracer_ctfs/[types, container, base40]
import codetracer_trace_writer/[call_stream, new_trace_reader, full_document_json, multi_stream_writer]
proc makeImage(): seq[byte] =
  var w = initMultiStreamWriter("", "hierarchy-real-entry").get()
  doAssert w.registerPath("/src/program.sol").isOk
  doAssert w.registerFunctionAt("/src/program.sol", 1, "<toplevel>").get() == 0
  doAssert w.registerFunctionAt("/src/program.sol", 2, "compute").get() == 1
  doAssert w.registerCall(0, @[]).isOk
  doAssert w.registerStep(0, 1, @[]).isOk
  doAssert w.registerCall(1, @[]).isOk
  doAssert w.markCurrentCallAsEntry().isOk
  doAssert w.registerStep(0, 2, @[]).isOk
  doAssert w.registerReturn().isOk
  doAssert w.registerReturn().isOk
  doAssert w.close().isOk
  result = w.toBytes()
let original = makeImage()
var calls = initCallStreamReader(original).get()
doAssert calls.count() == 2
var records: seq[CallRecord] = @[]
for key in 0'u64 ..< calls.count(): records.add(calls.readCall(key).get())
doAssert records[0].functionId == 0 and records[1].functionId == 1
proc copyRecords(): seq[CallRecord] =
  result = newSeq[CallRecord](records.len)
  for i,r in records: result[i] = r
proc rebuild(changed: seq[CallRecord]): seq[byte] =
  var c = createCtfs()
  for i in 0 ..< int(DefaultMaxRootEntries):
    let off = HeaderSize + ExtHeaderSize + i * FileEntrySize
    let nameCode = readU64LE(original, off+16)
    if nameCode == 0: continue
    let name = base40Decode(nameCode)
    if name in ["calls.dat", "calls.idx", "entry.dat"]: continue
    var file = c.addFile(name).get()
    let bytes = readInternalFile(original,name).get()
    doAssert c.writeToFile(file,bytes).isOk
  var stream = initCallStreamWriter(c).get()
  for record in changed: doAssert c.writeCall(stream,record).isOk
  doAssert c.finalizeCallStream(stream).isOk
  var entry = c.addFile("entry.dat").get()
  doAssert c.writeToFile(entry,readInternalFile(original,"entry.dat").get()).isOk
  result = c.toBytes()
proc document(data: seq[byte]): JsonNode =
  var r = openNewTraceFromBytes(data).get()
  buildFullDocument(r,FullOpts())
doAssert document(rebuild(records)) == document(original)
echo "PASS actual reconstructed call stream whole document unchanged"
proc refused(changed: seq[CallRecord], name: string) =
  let opened = openNewTraceFromBytes(rebuild(changed))
  doAssert opened.isErr, name & " accepted"
  doAssert "entry.dat" in opened.error, name & ": " & opened.error
  echo "PASS hierarchy " & name & ": " & opened.error
block:
  var changed=copyRecords();changed[1].parentCallKey=1
  refused(changed,"parent-cycle")
block:
  var changed=copyRecords();changed[1].parentCallKey=5
  refused(changed,"parent-out-of-range")
block:
  var changed=copyRecords();changed[1].depth=2
  refused(changed,"parent-depth")
block:
  var changed=copyRecords();changed[0].exitStep=0
  refused(changed,"parent-range")
block:
  var changed=copyRecords();changed[0].children = @[]
  refused(changed,"missing-child-link")
block:
  var changed=copyRecords();changed[0].children = @[1'u64,1'u64]
  refused(changed,"duplicate-child-link")
block:
  var changed=copyRecords();changed[0].functionId=1
  refused(changed,"literal-root-binding")
block:
  var changed=copyRecords()
  var later=changed[1]
  later.depth=2;later.parentCallKey=1;later.children = @[]
  changed[1].children = @[2'u64]
  changed.add(later)
  refused(changed,"higher-key-step-owner")
