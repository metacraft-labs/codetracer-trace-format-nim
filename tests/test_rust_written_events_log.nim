## Reading an `events.log` written by the Rust `CtfsTraceWriter`.
##
## The sibling Rust writer and this Nim writer produce combined-stream
## bundles that differ in two ways the reader used to trip over. Both are
## real: `ct-print` could not read *any* container the pure-Rust
## `CtfsTraceWriter` produced — on a host or from WebAssembly — even though
## `codetracer_ct_print.nim` documents that path as working.
##
##   1. **The 8-byte CodeTracer file header.** Rust prefixes `events.log`
##      with `HEADERV1` (`C0 DE 72 AC E2 01 00 00`) and its own reader skips
##      it. The Nim writer emits no such prefix, and the Nim reader used to
##      parse the magic as an inline chunk header — yielding a
##      `compressedSize` of `0xAC72DEC0`, about 2.9 GB, and the error
##      "chunk compressed data extends beyond events.log".
##
##   2. **A frame that does not pledge its decompressed size.** Rust
##      compresses each chunk with `zstd::encode_all`, a *streaming* encoder,
##      whose frame header omits `Frame_Content_Size`. Nim's one-shot
##      `ZSTD_compress` always writes it. `readEvents` checked only for
##      `ZSTD_CONTENTSIZE_ERROR`, so `ZSTD_CONTENTSIZE_UNKNOWN`
##      (`0xFFFF_FFFF_FFFF_FFFF`) reached `int(...)` and raised a
##      `RangeDefect`.
##
## No mocks and no fixtures on disk: the container is assembled here with the
## production CTFS container writer, and the unpledged frame is a real one
## emitted by `zstd::encode_all` and inlined as bytes (Nim's bindings expose
## only the one-shot compressor, which cannot produce such a frame).

import std/[os, json]
import results
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_ctfs/chunk_index
import codetracer_ctfs/zstd_bindings
import codetracer_trace_types
import codetracer_trace_writer/split_binary
import codetracer_trace_writer/meta_dat
import codetracer_trace_reader

const
  TmpDir = getTempDir() / "ct_rust_events_log"
  RecordingId = "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb"

  ## A Zstandard frame emitted by Rust's `zstd::encode_all(.., 3)`, whose
  ## frame-header descriptor byte is `0x00`: `Frame_Content_Size_flag = 0`
  ## and `Single_Segment_flag = 0`, i.e. no pledged decompressed size. This
  ## is byte-for-byte what the Rust `CtfsTraceWriter` puts in a chunk.
  UnpledgedFrame = [
    byte 0x28, 0xB5, 0x2F, 0xFD, 0x00, 0x58, 0x15, 0x01, 0x00, 0xD8, 0x63,
    0x6F, 0x64, 0x65, 0x74, 0x72, 0x61, 0x63, 0x65, 0x72, 0x20, 0x75, 0x6E,
    0x70, 0x6C, 0x65, 0x64, 0x67, 0x65, 0x64, 0x20, 0x66, 0x72, 0x61, 0x6D,
    0x65, 0x20, 0x01, 0x00, 0xAB, 0x9C, 0x9A, 0x63
  ]
  UnpledgedPlaintextUnit = "codetracer unpledged frame "
  UnpledgedPlaintextLen = 200

proc compressChunk(raw: openArray[byte]): seq[byte] =
  ## One-shot compression, matching what the Nim writer emits per chunk.
  let bound = ZSTD_compressBound(csize_t(raw.len))
  var buf = newSeq[byte](int(bound))
  let n = ZSTD_compress(addr buf[0], csize_t(buf.len),
                        unsafeAddr raw[0], csize_t(raw.len), cint(3))
  doAssert ZSTD_isError(n) == 0
  buf.setLen(int(n))
  buf

proc buildBundle(path: string, withHeaderPrefix: bool): seq[TraceLowLevelEvent] =
  ## Assemble a combined-stream `.ct` by hand so the `events.log` bytes are
  ## exactly what we want to test — with or without the Rust file-header
  ## prefix ahead of the first chunk header.
  let events = @[
    TraceLowLevelEvent(kind: tlePath, path: "/wd/main.rs"),
    TraceLowLevelEvent(kind: tleFunction,
      functionRecord: FunctionRecord(pathId: PathId(0), line: Line(1), name: "main")),
    TraceLowLevelEvent(kind: tleStep,
      step: StepRecord(pathId: PathId(0), line: Line(1))),
    TraceLowLevelEvent(kind: tleStep,
      step: StepRecord(pathId: PathId(0), line: Line(2))),
    TraceLowLevelEvent(kind: tleStep,
      step: StepRecord(pathId: PathId(0), line: Line(3))),
  ]

  var enc = SplitBinaryEncoder.init()
  for e in events:
    enc.encodeEvent(e)
  let raw = enc.getBytes()
  let compressed = compressChunk(raw)

  var eventsLog: seq[byte] = @[]
  if withHeaderPrefix:
    for b in codetracer_trace_reader.EventsLogHeaderV1:
      eventsLog.add(b)
  let header = encodeChunkHeader(ChunkIndexEntry(
    compressedSize: uint32(compressed.len),
    eventCount: uint32(events.len),
    firstGeid: 0'u64))
  for b in header:
    eventsLog.add(b)
  eventsLog.add(compressed)

  var c = createCtfs()

  var evFile = c.addFile("events.log").get()
  doAssert c.writeToFile(evFile, eventsLog).isOk

  var fmtFile = c.addFile("events.fmt").get()
  doAssert c.writeToFile(fmtFile, cast[seq[byte]]("split-binary")).isOk

  let meta = TraceMetadata(recordingId: RecordingId, workdir: "/wd",
                           program: "rust-writer", args: @[])

  var metaJson = c.addFile("meta.json").get()
  let metaText = $ %*{"recording_id": RecordingId, "workdir": "/wd",
                      "program": "rust-writer", "args": newJArray()}
  doAssert c.writeToFile(metaJson, cast[seq[byte]](metaText)).isOk

  var pathsJson = c.addFile("paths.json").get()
  doAssert c.writeToFile(pathsJson, cast[seq[byte]]($ %*["/wd/main.rs"])).isOk

  # NB: Nim identifiers are style-insensitive, so a local named `metaDat`
  # would shadow the `meta_dat` module and turn the qualified call below into
  # a method call on the local.
  var metaDatFile = c.addFile("meta.dat").get()
  doAssert writeMetaDat(c, metaDatFile, meta, ["/wd/main.rs"]).isOk

  doAssert c.writeCtfsToFile(path).isOk
  events

proc readBack(path: string): seq[TraceLowLevelEvent] =
  var reader = openTrace(path).get()
  let res = reader.readEvents()
  doAssert res.isOk, "readEvents failed: " & res.error
  reader.events

# ---------------------------------------------------------------------------

createDir(TmpDir)

block a_bundle_without_the_rust_header_prefix_still_reads:
  ## The Nim writer's own layout must be completely unaffected by the
  ## prefix-skipping added for Rust-written containers.
  let path = TmpDir / "no-prefix.ct"
  let written = buildBundle(path, withHeaderPrefix = false)
  let got = readBack(path)
  doAssert got.len == written.len,
    "expected " & $written.len & " events, got " & $got.len
  doAssert got[2].kind == tleStep and got[2].step.line == Line(1)
  doAssert got[4].step.line == Line(3)

block a_bundle_with_the_rust_header_prefix_reads_identically:
  ## The regression: this used to fail with "chunk compressed data extends
  ## beyond events.log", because the 8-byte magic was read as a chunk header.
  let path = TmpDir / "with-prefix.ct"
  let written = buildBundle(path, withHeaderPrefix = true)
  let got = readBack(path)
  doAssert got.len == written.len,
    "expected " & $written.len & " events, got " & $got.len
  doAssert got[2].kind == tleStep and got[2].step.line == Line(1)
  doAssert got[4].step.line == Line(3)

block a_frame_with_no_pledged_size_inflates:
  ## The other regression: `int(ZSTD_CONTENTSIZE_UNKNOWN)` raised a
  ## `RangeDefect` and took the whole process down.
  var expected = ""
  while expected.len < UnpledgedPlaintextLen:
    expected.add(UnpledgedPlaintextUnit)
  expected.setLen(UnpledgedPlaintextLen)

  doAssert ZSTD_getFrameContentSize(unsafeAddr UnpledgedFrame[0],
                                    csize_t(UnpledgedFrame.len)) ==
           ZSTD_CONTENTSIZE_UNKNOWN,
    "the inlined frame is supposed to omit its content size"

  let got = decompressFrameOfUnknownSize(UnpledgedFrame)
  doAssert got.isOk, "decompression failed: " & got.error
  doAssert cast[string](got.get()) == expected

block an_empty_input_inflates_to_nothing:
  let got = decompressFrameOfUnknownSize([])
  doAssert got.isOk
  doAssert got.get().len == 0

block a_damaged_frame_reports_an_error_rather_than_looping:
  ## Only `dstSize_tooSmall` may be retried; every other error must fail
  ## fast, or a damaged frame would drive the buffer up to the allocation
  ## ceiling one doubling at a time.  A truncated frame is the cheap way to
  ## produce a non-retryable error: flipping payload bytes is not, because
  ## this frame's literals are stored raw and no checksum is present, so a
  ## bit-flip decodes happily into different bytes.
  let truncated = @UnpledgedFrame[0 ..< 20]
  let got = decompressFrameOfUnknownSize(truncated)
  doAssert got.isErr, "a truncated frame must not decode"

removeDir(TmpDir)
echo "test_rust_written_events_log: OK"
