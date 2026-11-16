## ct-print shows a read's FULL content when `eventlog.dat` carries only a
## preview of it (codetracer-pm spec/Trace-Files/MCR-Event-Log-Stream.md
## §2.2).
##
## The native recorder may store a read entry's content as a prefix of the
## bytes read, the entry's return value being the whole length; the bytes are
## in the thread-stream record with the same geid on the same thread.  ct-print
## pairs the two (`native_decoder.fillPreviewContent`), so what it prints does
## not change.  Each a hard failure:
##   (1) a preview entry (16 of 1,000 bytes) prints content_len 1000 and the
##       record's 1,000 bytes;
##   (2) an entry that carries its whole content is printed as before;
##   (3) a preview whose thread-stream record is missing is refused, naming
##       the geid -- never printed as if the preview were the content;
##   (4) a preview that is not a prefix of its record's data is refused,
##       naming the geid and the byte.
##
## FAILING BEFORE THE FIX: at agents `2e29ce44` (1) prints content_len 16 and
## (3) / (4) decode without an error.
##
## No mocks: every bundle is a real CTFS container decoded by the real decoder.
## The thread-stream and event-log bytes follow the recorder's layout,
## written by hand (`native_bundle_encoding`): the 26-byte header, then the OS
## event's fixed record (returnValue u64, errorCode u32, dataLen u32,
## savedErrno u32) and its data.

import std/[json, options, base64, strutils]
import results
import stew/endians2
import codetracer_trace_types
import codetracer_ctfs
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/interning_table
import native_decoder
import native_bundle_encoding

const
  Tid = 1'u32
  ReadGeid = 7'u64
  FullLen = 1000

proc full(): seq[byte] =
  result = newSeq[byte](FullLen)
  for i in 0 ..< FullLen: result[i] = byte((i * 31 + 7) and 0xFF)

proc osRead(geid: uint64, data: openArray[byte]): seq[byte] =
  ## An `evOsRead` (type 19) record on thread `Tid`.
  result = encodeEventHeader(19'u16, uint32(26 + 20 + data.len), Tid, geid,
                             1000'u64 + geid)
  result.add toBytesLE(uint64(data.len))   # returnValue
  result.add toBytesLE(4'u32)              # errorCode: the descriptor
  result.add toBytesLE(uint32(data.len))   # dataLen
  result.add toBytesLE(0'u32)              # savedErrno
  for b in data: result.add b

proc bundle(streamData: openArray[byte], entryContent: openArray[byte],
            withRecord = true): seq[byte] =
  var ctfs = createCtfs()
  let meta = writeMetaDatToBuffer(
    TraceMetadata(recordingId: "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
                  workdir: "/synthetic", program: "/synthetic/program",
                  args: @[]),
    recorderId = "mcr",
    mcrFields = some(McrMetaFields(tickSource: tsMonotonic, totalThreads: 1,
      atomicMode: amRelaxed, totalEvents: 1, platform: "linux",
      tickGranularity: "edge", tickSourceStr: "none",
      atomicModeStr: "relaxed", hookProfile: "test",
      hookStrategies: @["inprocess"])))
  var metaFile = ctfs.addFile("meta.dat").get()
  doAssert ctfs.writeToFile(metaFile, meta).isOk
  var tables = initTraceInterningTables(ctfs).get()
  doAssert ctfs.ensurePathId(tables, "/synthetic/src/main.c").get() == 0
  # The thread stream: a record at another geid when `withRecord` is false.
  let g = if withRecord: ReadGeid else: ReadGeid + 1
  var tFile = ctfs.addFile(threadFileName(Tid)).get()
  doAssert ctfs.writeToFile(tFile, osRead(g, streamData)).isOk
  let entry = encodeOsEntry(geid = ReadGeid, tick = 1000'u64 + ReadGeid,
                            tid = Tid, kind = 3'u8, fd = 4'i32,
                            returnValue = int64(FullLen), metadata = "",
                            content = entryContent)
  var datFile = ctfs.addFile(EventLogDat).get()
  doAssert ctfs.writeToFile(datFile, encodeOsEventLogChunk(@[entry])).isOk
  var idxFile = ctfs.addFile(EventLogIdx).get()
  doAssert ctfs.writeToFile(idxFile, encodeOsEventLogIndex(
    entryCount = 1'u32, chunkOffsets = [0'u64], chunkGeids = [ReadGeid])).isOk
  result = ctfs.toBytes()
  ctfs.closeCtfs()

proc osEvent(doc: JsonNode): JsonNode =
  for ev in doc["events"].elems:
    if ev["kind"].getStr == "os_event": return ev
  doAssert false, "no os_event in the document"

let data = full()

block preview:
  let r = buildNativeFullDocument(bundle(data, data[0 ..< 16]),
                                  NativeOpts(stripPaths: false))
  doAssert r.isOk, "(1) " & r.error
  let ev = osEvent(r.get())
  doAssert ev["content_len"].getInt == FullLen,
    "(1) a preview entry prints content_len " & $ev["content_len"].getInt &
    "; the read was " & $FullLen & " bytes"
  doAssert ev["content_b64"].getStr == base64.encode(data),
    "(1) the printed content is not the thread-stream record's bytes"
  echo "[ok] (1) a 16-byte preview prints the read's 1,000 bytes"

block wholeContent:
  let r = buildNativeFullDocument(bundle(data, data),
                                  NativeOpts(stripPaths: false))
  doAssert r.isOk, "(2) " & r.error
  let ev = osEvent(r.get())
  doAssert ev["content_len"].getInt == FullLen
  doAssert ev["content_b64"].getStr == base64.encode(data)
  echo "[ok] (2) an entry with its whole content prints as before"

block missingRecord:
  let r = buildNativeFullDocument(bundle(data, data[0 ..< 16],
                                         withRecord = false),
                                  NativeOpts(stripPaths: false))
  doAssert r.isErr, "(3) a preview with no thread-stream record decoded"
  doAssert ("geid " & $ReadGeid) in r.error, "(3) " & r.error
  echo "[ok] (3) ", r.error

block notAPrefix:
  var other = data
  other[5] = other[5] xor 0xFF'u8
  let r = buildNativeFullDocument(bundle(other, data[0 ..< 16]),
                                  NativeOpts(stripPaths: false))
  doAssert r.isErr, "(4) a preview that is not a prefix of its record decoded"
  doAssert ("geid " & $ReadGeid) in r.error and "byte 5" in r.error,
    "(4) " & r.error
  echo "[ok] (4) ", r.error

echo "All ct-print event log preview tests passed."
