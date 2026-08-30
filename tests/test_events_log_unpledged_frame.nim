## An `events.log` chunk whose Zstd frame does not pledge its content size
## must be read, not crashed on.
##
## # The defect this guards
##
## `ZSTD_getFrameContentSize` returns one of three things: a size, the
## sentinel `ZSTD_CONTENTSIZE_ERROR`, or the sentinel
## `ZSTD_CONTENTSIZE_UNKNOWN` (`0xFFFFFFFFFFFFFFFF`) for a frame whose header
## carries no size field.  The last is not an error — it is what every
## STREAMING encoder produces, because a streaming encoder cannot know the
## length in advance.
##
## `codetracer_trace_reader.readEvents` tested only for `_ERROR` and let
## `_UNKNOWN` fall through into `int(decompSize)`.  Converting
## `0xFFFFFFFFFFFFFFFF` to a signed `int` raises `RangeDefect`, which nothing
## on the path catches, so the process DIED:
##
##     fatal.nim(62)  sysFatal
##     Error: unhandled exception: value out of range [RangeDefect]
##
## `ct-print` reaches this reader for EVERY container carrying an
## `events.log` (`codetracer_ct_print.nim`: `hasEventsLog` forces
## `preferSplit = false`), so this was a hard crash of the shipped inspector
## on a class of container that is entirely valid.
##
## Every other stream module in this repo already tests both sentinels —
## `exec_stream`, `value_stream`, `call_stream`, `io_event_stream`,
## `span_stream`, `chunked_compressed_table` and `native_decoder`.  This
## reader was the sole exception.
##
## # Who writes such a container
##
## The sibling Rust `CtfsTraceWriter` when it cannot reach libzstd — notably
## every container written from `wasm32-unknown-unknown`, where the pure-Rust
## encoder emits `frame_content_size: None` unconditionally.  It is reachable
## from any producer that streams rather than one-shots.
##
## # Why this is a hard failure, never a skip
##
## The fixture is CONSTRUCTED from this repo's own writer and container
## primitives, so there is no prerequisite that could be missing and
## therefore no honest reason to skip.  Nothing is mocked: the recording is
## produced by the REAL `codetracer_trace_writer`, the container is
## assembled with the REAL `codetracer_ctfs/container` primitives, and it is
## read back through the REAL `openTrace` / `readEvents` — the same entry
## points `ct-print` calls.
##
## The only synthesised step is `stripFrameContentSize`, which rewrites a
## finished Zstd frame header so it no longer pledges.  That is the exact
## inverse of the Rust writer's `zstd_frame::pledge_frame_content_size`, and
## it is a conformant edit for the same reason: RFC 8878 §3.1.1 lays the
## frame header out as
##
##     Magic(4) Frame_Header_Descriptor(1) [Window_Descriptor(1)]
##     [Dictionary_ID(0..4)] [Frame_Content_Size(0..8)] Block... [Checksum]
##
## and nothing in the blocks depends on the header's length.
##
## A recorded fixture blob was considered and rejected: a committed binary
## that silently stops representing a streaming encoder is the same class of
## lie this test exists to prevent.

import std/os
import results
import codetracer_trace_writer
import codetracer_trace_reader
import codetracer_trace_types
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_ctfs/chunk_index
import codetracer_ctfs/zstd_bindings

const
  tmpDir = "/tmp/ctfs_unpledged_frame_test"
  pledgedBundle = tmpDir / "pledged.ct"
  unpledgedBundle = tmpDir / "unpledged.ct"

  mandatoryFiles = ["meta.dat", "events.log"]
  optionalFiles = ["meta.json", "paths.json", "paths.dat"]

  ZstdMagic: array[4, byte] = [0x28'u8, 0xB5, 0x2F, 0xFD]

# ---------------------------------------------------------------------------
# Frame surgery: remove the Frame_Content_Size field from a finished frame
# ---------------------------------------------------------------------------

proc stripFrameContentSize(frame: openArray[byte]): seq[byte] =
  ## Return `frame` rewritten so its header carries no `Frame_Content_Size`,
  ## i.e. so `ZSTD_getFrameContentSize` answers `ZSTD_CONTENTSIZE_UNKNOWN`.
  ##
  ## Two header bits have to move together.  Clearing the FCS flag alone is
  ## not enough when `Single_Segment_flag` is set, because with that flag a
  ## content size is MANDATORY and there is no `Window_Descriptor` byte.  So
  ## the flag is cleared and a `Window_Descriptor` wide enough for the
  ## payload is inserted in its place.
  doAssert frame.len >= 6, "not a Zstd frame: too short"
  for i in 0 ..< 4:
    doAssert frame[i] == ZstdMagic[i], "not a Zstd frame: bad magic"

  let descriptor = frame[4]
  let fcsFlag = int(descriptor shr 6)
  let singleSegment = (int(descriptor shr 5) and 1) == 1
  let dictIdFlag = int(descriptor and 0b11)
  let dictIdLen = case dictIdFlag
    of 0: 0
    of 1: 1
    of 2: 2
    else: 4

  var pos = 5
  var windowByte: byte = 0
  if singleSegment:
    # No Window_Descriptor present; we must synthesise one below.
    discard
  else:
    windowByte = frame[pos]
    inc pos

  let dictIdStart = pos
  pos += dictIdLen

  let fcsLen = case fcsFlag
    of 0: (if singleSegment: 1 else: 0)
    of 1: 2
    of 2: 4
    else: 8

  # Read the pledged size so a synthesised window can cover it.
  var contentSize: uint64 = 0
  for i in 0 ..< fcsLen:
    contentSize = contentSize or (uint64(frame[pos + i]) shl (8 * i))
  if fcsFlag == 1:
    # Per RFC 8878 the 2-byte form is stored biased by 256.
    contentSize += 256

  if fcsLen == 0:
    # Already unpledged and not single-segment: nothing to do.
    result = @[]
    for b in frame: result.add(b)
    return result

  let bodyStart = pos + fcsLen

  if singleSegment:
    # windowLog must be >= the content size. Exponent 0 => 1 KiB.
    var exponent = 0
    while exponent < 31 and (uint64(1) shl (10 + exponent)) < contentSize:
      inc exponent
    windowByte = byte(exponent shl 3)  # Mantissa 0

  # Clear FCS flag (bits 7:6) and Single_Segment_flag (bit 5); keep the
  # Content_Checksum_flag (bit 2) and Dictionary_ID_flag (bits 1:0).
  # Bits 4 and 3 are the Unused and Reserved bits and must stay zero.
  let newDescriptor = byte(int(descriptor) and 0b0000_0111)

  result = @[]
  for i in 0 ..< 4: result.add(frame[i])
  result.add(newDescriptor)
  result.add(windowByte)
  for i in 0 ..< dictIdLen: result.add(frame[dictIdStart + i])
  for i in bodyStart ..< frame.len: result.add(frame[i])

proc frameReportsUnknownSize(frame: openArray[byte]): bool =
  doAssert frame.len > 0
  ZSTD_getFrameContentSize(unsafeAddr frame[0], csize_t(frame.len)) ==
    ZSTD_CONTENTSIZE_UNKNOWN

proc inflateWithGenerousBuffer(frame: openArray[byte]): seq[byte] =
  ## Decode a frame independently of the reader under test, so the surgery
  ## itself can be validated rather than assumed.
  var dst = newSeq[byte](1 shl 20)
  let n = ZSTD_decompress(addr dst[0], csize_t(dst.len),
                          unsafeAddr frame[0], csize_t(frame.len))
  doAssert ZSTD_isError(n) == 0,
    "stripped frame does not decode: " & $ZSTD_getErrorName(n)
  dst.setLen(int(n))
  dst

# ---------------------------------------------------------------------------
# Fixture construction
# ---------------------------------------------------------------------------

proc buildPledgedBundle(path: string) =
  ## A real recording from this repo's writer.  `ZSTD_compress` always
  ## pledges, so every chunk frame here carries a content size.
  removeFile(path)
  var w = newTraceWriter(path, "demo", @["--x"], "/wd").get()
  doAssert w.writePath("/wd/main.py").isOk
  doAssert w.writeFunction(0, 1, "main").isOk
  doAssert w.writeStep(0, 1).isOk
  doAssert w.writeCall(0).isOk
  doAssert w.writeStep(0, 2).isOk
  doAssert w.writeValue(0,
    ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(0))).isOk
  doAssert w.writeStep(0, 3).isOk
  doAssert w.writeValue(0,
    ValueRecord(kind: vrkInt, intVal: 43, intTypeId: TypeId(0))).isOk
  doAssert w.writeReturn().isOk
  doAssert w.writeMetaDat().isOk
  doAssert w.close().isOk

proc rewriteEventsLogUnpledged(payload: openArray[byte]):
    tuple[data: seq[byte], chunks: int] =
  ## Walk `events.log`'s inline chunk layout and replace every chunk's frame
  ## with an unpledged equivalent, fixing up each 16-byte chunk header's
  ## `compressedSize` (the frame changes length).
  var outData: seq[byte] = @[]
  var pos = 0
  var chunks = 0
  while pos + ChunkIndexEntrySize <= payload.len:
    let entry = decodeChunkHeader(payload, pos)
    if entry.compressedSize == 0:
      break
    let bodyStart = pos + ChunkIndexEntrySize
    let bodyEnd = bodyStart + int(entry.compressedSize)
    doAssert bodyEnd <= payload.len, "chunk overruns events.log"

    var frame: seq[byte] = @[]
    for i in bodyStart ..< bodyEnd: frame.add(payload[i])

    let stripped = stripFrameContentSize(frame)
    doAssert frameReportsUnknownSize(stripped),
      "surgery failed: the rewritten frame still pledges its size, so this " &
      "suite would prove nothing"
    doAssert inflateWithGenerousBuffer(stripped) ==
             inflateWithGenerousBuffer(frame),
      "surgery corrupted the payload: the stripped frame decodes to " &
      "different bytes than the original"

    var newEntry = entry
    newEntry.compressedSize = uint32(stripped.len)
    let header = encodeChunkHeader(newEntry)
    for b in header: outData.add(b)
    for b in stripped: outData.add(b)

    inc chunks
    pos = bodyEnd
  (outData, chunks)

proc buildUnpledgedBundle(srcPath, outPath: string): int =
  ## Rebuild `srcPath` with an `events.log` whose frames pledge nothing,
  ## every other stream copied verbatim.  Returns the chunk count rewritten.
  let src = readCtfsFromFile(srcPath).get()
  var chunkCount = 0

  var c = createCtfs()
  for name in mandatoryFiles:
    let payload = readInternalFile(src, name).get()
    doAssert payload.len > 0, name & " is empty in the source bundle"
    var f = c.addFile(name).get()
    if name == "events.log":
      let rewritten = rewriteEventsLogUnpledged(payload)
      chunkCount = rewritten.chunks
      doAssert c.writeToFile(f, rewritten.data).isOk
    else:
      doAssert c.writeToFile(f, payload).isOk

  for name in optionalFiles:
    if not hasInternalFile(src, name):
      continue
    let payload = readInternalFile(src, name).get()
    var f = c.addFile(name).get()
    doAssert c.writeToFile(f, payload).isOk

  doAssert writeCtfsToFile(c, outPath).isOk
  chunkCount

# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------

proc readAllEvents(path: string): seq[TraceLowLevelEvent] =
  let readerRes = openTrace(path)
  doAssert readerRes.isOk,
    "openTrace(" & path & ") failed: " & readerRes.unsafeError
  var reader = readerRes.get()
  let evRes = reader.readEvents()
  doAssert evRes.isOk,
    "readEvents(" & path & ") failed: " & evRes.unsafeError &
    "\n  A frame that does not pledge its content size is VALID Zstd; " &
    "refusing it is this repo's bug, not the container's."
  reader.events

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

var rewrittenChunks = 0

proc testFixtureIsReallyUnpledged() =
  ## Guard the guard.  If the reconstruction still pledged, every assertion
  ## below would pass while testing nothing — which is precisely how this
  ## defect survived: the pledged path was always exercised and the
  ## unpledged one never was.
  doAssert rewrittenChunks > 0,
    "the fixture rewrote ZERO chunks, so it exercises nothing"
  let bytes = readCtfsFromFile(unpledgedBundle).get()
  let payload = readInternalFile(bytes, "events.log").get()
  var pos = 0
  var seen = 0
  while pos + ChunkIndexEntrySize <= payload.len:
    let entry = decodeChunkHeader(payload, pos)
    if entry.compressedSize == 0: break
    let bodyStart = pos + ChunkIndexEntrySize
    var frame: seq[byte] = @[]
    for i in bodyStart ..< bodyStart + int(entry.compressedSize):
      frame.add(payload[i])
    doAssert frameReportsUnknownSize(frame),
      "chunk " & $seen & " in the reconstructed bundle still pledges its " &
      "size — the fixture is not exercising the unpledged path"
    inc seen
    pos = bodyStart + int(entry.compressedSize)
  doAssert seen == rewrittenChunks
  echo "PASS: fixture carries ", seen,
       " chunk(s), every one reporting ZSTD_CONTENTSIZE_UNKNOWN"

proc testPledgedBundleStillReads() =
  ## The ordinary path must be untouched.
  let events = readAllEvents(pledgedBundle)
  doAssert events.len > 0, "the pledged bundle decoded to zero events"
  echo "PASS: pledged events.log still reads (", events.len, " events)"

proc testUnpledgedBundleReadsIdentically() =
  ## The whole point.  Before the fix this line did not fail — it killed the
  ## process with a RangeDefect.
  let pledged = readAllEvents(pledgedBundle)
  let unpledged = readAllEvents(unpledgedBundle)
  doAssert unpledged.len == pledged.len,
    "unpledged bundle decoded " & $unpledged.len & " events, pledged " &
    $pledged.len
  doAssert unpledged.len > 0, "both bundles decoded to zero events"
  for i in 0 ..< pledged.len:
    doAssert unpledged[i] == pledged[i],
      "event " & $i & " differs between the pledged and unpledged bundles"
  echo "PASS: unpledged events.log decodes identically (",
       pledged.len, " events)"

proc testJsonOutputMatches() =
  ## `ct-print --json-events` is exactly `openTrace` + `readEvents` + this
  ## serialization, so pinning the JSON pins the user-visible behaviour
  ## without rebuilding the binary.
  var a = openTrace(pledgedBundle).get()
  doAssert a.readEvents().isOk
  var b = openTrace(unpledgedBundle).get()
  doAssert b.readEvents().isOk
  let ja = a.toJsonEvents()
  let jb = b.toJsonEvents()
  doAssert ja == jb,
    "JSON event streams differ:\n  pledged:   " & ja &
    "\n  unpledged: " & jb
  doAssert ja.len > 2, "JSON event stream is empty"
  echo "PASS: --json-events output is byte-identical across both layouts"

when isMainModule:
  createDir(tmpDir)
  buildPledgedBundle(pledgedBundle)
  rewrittenChunks = buildUnpledgedBundle(pledgedBundle, unpledgedBundle)

  testFixtureIsReallyUnpledged()
  testPledgedBundleStillReads()
  testUnpledgedBundleReadsIdentically()
  testJsonOutputMatches()

  echo "All unpledged-frame events.log tests passed"
