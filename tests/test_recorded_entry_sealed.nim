## Genuine finalized CTFS images and targeted structural corruption; no mocks.
import std/[strutils, json]
import results
import codetracer_ctfs/[types, container, base40]
import codetracer_trace_writer/[multi_stream_writer, new_trace_reader]

proc copyImage(data: seq[byte]): seq[byte] =
  result = newSeq[byte](data.len)
  for i, b in data: result[i] = b
proc saveImage(name: string, data: seq[byte]) =
  var s = newString(data.len)
  for i,b in data: s[i] = char(b)
  writeFile(name, s)
proc makeImage(marked: bool): seq[byte] =
  var w = initMultiStreamWriter("", "sealed-real-entry").get()
  doAssert w.registerPath("/src/program.sol").isOk
  doAssert w.registerFunctionAt("/src/program.sol", 1, "<toplevel>").get() == 0
  doAssert w.registerFunctionAt("/src/program.sol", 2, "compute").get() == 1
  doAssert w.registerCall(0, @[]).isOk
  doAssert w.registerStep(0, 1, @[]).isOk
  doAssert w.registerCall(1, @[]).isOk
  if marked: doAssert w.markCurrentCallAsEntry().isOk
  doAssert w.registerStep(0, 2, @[]).isOk
  doAssert w.registerReturn().isOk
  doAssert w.registerReturn().isOk
  doAssert w.close().isOk
  result = w.toBytes()
proc refuse(data: seq[byte], name: string) =
  let opened = openNewTraceFromBytes(data)
  doAssert opened.isErr, name & " accepted"
  doAssert "entry.dat" in opened.error, name & ": " & opened.error
  echo "PASS sealed " & name & ": " & opened.error

let image = makeImage(true)
saveImage("marked.ct", image)
saveImage("unmarked.ct", makeImage(false))
let entry = findFileEntry(image, "entry.dat")
doAssert entry.found and entry.size == 11 and isDirectMapBlock(entry.mapBlock)
let payload = int(directDataBlock(entry.mapBlock)) * int(DefaultBlockSize)
let directory = HeaderSize + ExtHeaderSize + entry.index * FileEntrySize
for (index, value, name) in [(0, 0.byte, "magic"), (4, 2.byte, "version"),
    (6, 1.byte, "reserved"), (8, 2.byte, "out-of-range-call"),
    (9, 2.byte, "out-of-range-function"), (10, 0.byte, "entry-step-binding")]:
  var altered = copyImage(image)
  altered[payload + index] = value
  refuse(altered, name)
block:
  var altered = copyImage(image)
  writeU64LE(altered, directory, 0)
  writeU64LE(altered, directory+8, 0)
  refuse(altered, "present-empty")
block:
  var altered = copyImage(image)
  writeU64LE(altered, directory, entry.size-1)
  refuse(altered, "present-truncated")
block:
  var altered = copyImage(image)
  writeU64LE(altered, directory, entry.size+1)
  refuse(altered, "present-trailing")
block:
  var altered = copyImage(image)
  var slot = -1
  for i in 0 ..< int(DefaultMaxRootEntries):
    let off = HeaderSize + ExtHeaderSize + i * FileEntrySize
    if readU64LE(altered, off+16) == 0:
      slot = off
      break
  doAssert slot >= 0
  writeU64LE(altered, slot+16, base40Encode("entry.dat"))
  refuse(altered, "duplicate-member")
echo "PASS finalized marked and unmarked images written"
