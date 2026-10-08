{.push raises: [].}

## Call stream (`calls.dat` + `calls.idx`): stores complete call records
## (function invocations) indexed by call_key, for seekable random access.
##
## # Wire format per record
##
## Each record matches `codetracer-trace-format-spec/trace-events.md`
## §"Call Stream Records" and is byte-identical to the Rust
## `codetracer_trace_writer/call_stream.rs` encoding so the two
## implementations interoperate:
##
##   varint functionId
##   signed_varint parentCallKey  (-1 = root)
##   varint entryStep
##   varint exitStep
##   varint depth
##   varint args_count
##     for each arg: varint varname_id, varint value_len + bytes
##   varint return_value_len + bytes (single byte 0xFF for VoidReturn)
##   varint exception_len + bytes (0 if no exception)
##   varint children_count
##     for each child: varint call_key
##
## # Storage (`calls.dat` + `calls.idx`)
##
## CTFS-M20: records are grouped into chunks of `chunkSize` records, each
## independently Zstd-compressed and concatenated into `calls.dat` with no
## inline headers. Inside a chunk every record is length-prefixed with a
## varint so the reader can walk variable-length records. The companion
## `calls.idx` follows `codetracer-trace-format-spec/seekable-zstd.md` and is
## byte-compatible with the Rust `CallStreamReader`'s `calls.idx` parser
## (`codetracer_trace_reader/src/call_stream_reader.rs`):
##
##   calls.dat:  [zstd(chunk_0)][zstd(chunk_1)]...
##   calls.idx:  [chunk_size: u32 LE][offset_0: u64 LE][offset_1: u64 LE]...
##
## `offset_i` is the byte offset of chunk `i` within `calls.dat`. To seek to
## call record `N`: `chunk = N div chunkSize`, decompress `dat[offset[chunk] ..
## offset[chunk+1])` (or to end for the last chunk), and index `N mod chunkSize`
## within it — O(1), no whole-stream decompression.
##
## A Nim-written container is seekable by the Rust `CallStreamReader` exactly
## like a Rust-written one. The PUBLIC reader/writer
## API (`initCallStreamWriter`, `writeCall`, `finalizeCallStream`,
## `initCallStreamReader`, `readCall`, `count`) is unchanged so callers
## (`multi_stream_writer`, `new_trace_reader`) need only the close-time
## finalize.

import std/options
import results
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ../codetracer_ctfs/streaming
import ../codetracer_ctfs/zstd_bindings
import ./varint
import ./record_chunk

const
  VoidReturnMarker*: byte = 0xFF  ## 1-byte marker for void returns
  DefaultCallsChunkSize* = 256
    ## Records per `calls.dat` chunk. Matches the Rust writer's
    ## `DEFAULT_CALLS_CHUNK_SIZE` so seek granularity is identical.
  DefaultCallsZstdLevel = 3
    ## Zstd compression level for `calls.dat` chunks. The exact bytes need
    ## not match the Rust writer (any valid zstd frame decodes); only the
    ## CHUNK LAYOUT and `calls.idx` structure must be byte-compatible.

type
  CallArg* = object
    varnameId*: uint64        ## interned variable name id
    value*: seq[byte]         ## CBOR-encoded argument value (may be empty)

  CallRecord* = object
    functionId*: uint64
    parentCallKey*: int64   ## -1 for root calls
    entryStep*: uint64
    exitStep*: uint64
    depth*: uint32
    args*: seq[CallArg]        ## per-argument (varname_id, CBOR value) pairs
    returnValue*: seq[byte]    ## CBOR-encoded return value, or [VoidReturnMarker]
    exception*: seq[byte]      ## CBOR-encoded exception, empty if none
    children*: seq[uint64]     ## child call_keys

  CallStreamWriter* = object
    ## Buffers encoded records and flushes them to `calls.dat` one Zstd chunk
    ## at a time. Each sealed chunk publishes its byte offset to `calls.idx`
    ## INCREMENTALLY (header written by `initCallStreamWriter`, one offset per
    ## sealed chunk by `flushChunk`), so a still-recording container is already
    ## seekable by a follow reader. `finalizeCallStream` only seals the last
    ## partial chunk.
    datFile: CtfsInternalFile      ## calls.dat handle
    indexFile: CtfsInternalFile    ## calls.idx handle
    chunkSize: int                 ## records per chunk
    zstdLevel: int                 ## zstd compression level
    pending: seq[byte]             ## current chunk's length-prefixed records
    pendingCount: int              ## records buffered in `pending`
    datOffset: uint64              ## running byte offset within calls.dat
    recordCount: uint64            ## total records appended
    finalized: bool

  CallStreamReader* = object
    ## Reads the dedicated call stream: chunked-Zstd `calls.dat` and its
    ## companion `calls.idx`.
    spec: ChunkedRecords
    recordCount: uint64

# ---------------------------------------------------------------------------
# Record encode/decode (unchanged wire format)
# ---------------------------------------------------------------------------

proc encodeCallRecord*(rec: CallRecord): seq[byte] {.raises: [].} =
  ## Encode a CallRecord into its wire format.
  var buf: seq[byte]

  encodeVarint(rec.functionId, buf)
  encodeSignedVarint(rec.parentCallKey, buf)
  encodeVarint(rec.entryStep, buf)
  encodeVarint(rec.exitStep, buf)
  encodeVarint(uint64(rec.depth), buf)

  # args
  encodeVarint(uint64(rec.args.len), buf)
  for arg in rec.args:
    encodeVarint(arg.varnameId, buf)
    encodeVarint(uint64(arg.value.len), buf)
    buf.add(arg.value)

  # return value
  encodeVarint(uint64(rec.returnValue.len), buf)
  buf.add(rec.returnValue)

  # exception
  encodeVarint(uint64(rec.exception.len), buf)
  buf.add(rec.exception)

  # children
  encodeVarint(uint64(rec.children.len), buf)
  for child in rec.children:
    encodeVarint(child, buf)

  buf

type
  CallHead = object
    ## A call record's fixed-size fields, decoded before the record is built.
    functionId: uint64
    parentCallKey: int64
    entryStep: uint64
    exitStep: uint64
    depth: uint32

proc decodeCallFields(data: openArray[byte], head: var CallHead,
    args: var seq[CallArg], returnValue, exception: var seq[byte],
    children: var seq[uint64], why: var string): bool =
  ## Every field of a call record's wire format. False, with `why` set, where
  ## the record does not decode: one walker answering `bool`, so a refusal
  ## costs one `Result` for the record rather than one per field.
  var pos = 0
  template varint(): uint64 = varintOrFail(data, pos, why)
  template refuse(message: string) =
    why = message
    return false
  template blob(dest: var seq[byte], what: string) =
    let n = int(varint())
    if n < 0 or pos + n > data.len:
      refuse("truncated " & what & " data")
    dest = fieldBytes(data, pos, n)
    pos += n

  head.functionId = varint()
  head.parentCallKey = signedVarintOrFail(data, pos, why)
  head.entryStep = varint()
  head.exitStep = varint()
  head.depth = uint32(varint())

  # The lists grow as they are read, sized at most by the bytes left: a count
  # read from a damaged record must not size an allocation by itself.
  let argsCount = varint()
  args = newSeqOfCap[CallArg](int(min(argsCount, uint64(data.len - pos))))
  for i in 0'u64 ..< argsCount:
    var arg = CallArg(varnameId: varint())
    blob(arg.value, "arg")
    args.add(move arg)
  blob(returnValue, "return value")
  blob(exception, "exception")

  let childrenCount = varint()
  children = newSeqOfCap[uint64](
    int(min(childrenCount, uint64(data.len - pos))))
  for i in 0'u64 ..< childrenCount:
    children.add(varint())

  # `trace-events.md` §"Call Stream": a record's fields fill its
  # `record_len` exactly. Bytes left over mean the record is not the one its
  # frame claims, and the fields decoded above are not trustworthy either.
  if pos != data.len:
    refuse("call record's fields end at byte " & $pos & " of its " &
      $data.len & "-byte frame")
  true

template decodeCallInto(data: openArray[byte], refusalPrefix: string) =
  ## Decode a call record into the enclosing proc's
  ## `Result[CallRecord, string]`, or its refusal, prefixed. The record is
  ## built once, from its decoded fields, in the result: a call record is 104
  ## bytes (72 on wasm32), and building it any other way zeroes it first,
  ## which a WebAssembly build makes a call into the host.
  var head: CallHead
  var args: seq[CallArg]
  var returnValue, exception: seq[byte]
  var children: seq[uint64]
  var why: string
  if decodeCallFields(data, head, args, returnValue, exception, children, why):
    result.ok(CallRecord(functionId: head.functionId,
      parentCallKey: head.parentCallKey, entryStep: head.entryStep,
      exitStep: head.exitStep, depth: head.depth, args: move args,
      returnValue: move returnValue, exception: move exception,
      children: move children))
  else:
    result = err(refusalPrefix & why)

proc decodeCallRecord*(data: openArray[byte]): Result[CallRecord, string] {.raises: [].} =
  ## Decode a CallRecord from its wire format.
  decodeCallInto(data, "")

# ---------------------------------------------------------------------------
# Zstd helpers
# ---------------------------------------------------------------------------

proc zstdCompress(src: openArray[byte], level: int): Result[seq[byte], string] {.raises: [].} =
  ## Compress `src` into a single Zstd frame.
  if src.len == 0:
    # An empty chunk is never flushed (we only flush when pendingCount > 0),
    # but be defensive: compress the empty input so the frame is still valid.
    var dst = newSeq[byte](64)
    let written = ZSTD_compress(addr dst[0], csize_t(dst.len), nil, 0, cint(level))
    if ZSTD_isError(written) != 0:
      return err("zstd compress (empty) failed: " & $ZSTD_getErrorName(written))
    dst.setLen(int(written))
    return ok(dst)
  let bound = ZSTD_compressBound(csize_t(src.len))
  var dst = newSeq[byte](int(bound))
  let written = ZSTD_compress(addr dst[0], csize_t(dst.len),
                              unsafeAddr src[0], csize_t(src.len), cint(level))
  if ZSTD_isError(written) != 0:
    return err("zstd compress failed: " & $ZSTD_getErrorName(written))
  dst.setLen(int(written))
  ok(dst)

# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

proc initCallStreamWriter*(ctfs: var Ctfs,
    chunkSize: int = DefaultCallsChunkSize): Result[CallStreamWriter, string] =
  ## Create the `calls.dat` / `calls.idx` stream pair and write the index
  ## header. `calls.idx` is grown INCREMENTALLY (one offset per sealed chunk,
  ## in `flushChunk`) so a still-recording container is seekable mid-run, exactly
  ## like the sibling `steps.idx` / `values.idx` / `events.idx` / `spans.idx`
  ## streams. `chunkSize` is the records-per-chunk seek granularity (matches the
  ## Rust writer's default).
  let cs = max(chunkSize, 1)
  let datFileRes = ctfs.addFile("calls.dat")
  if datFileRes.isErr:
    return err("failed to create calls.dat: " & datFileRes.unsafeError)
  let idxFileRes = ctfs.addFile("calls.idx")
  if idxFileRes.isErr:
    return err("failed to create calls.idx: " & idxFileRes.unsafeError)

  var w = CallStreamWriter(
    datFile: datFileRes.get(),
    indexFile: idxFileRes.get(),
    chunkSize: cs,
    zstdLevel: DefaultCallsZstdLevel,
  )

  # calls.idx header: [chunk_size: u32 LE]  (seekable-zstd.md).  The Rust
  # `CallStreamReader::parse` reads exactly this 4-byte header before the u64
  # chunk offsets, so writing it at init makes the index parseable the moment
  # the first chunk seals — the on-disk FORMAT is unchanged, only WHEN the
  # bytes appear.
  var hdr: array[4, byte]
  writeU32LE(hdr, 0, uint32(cs))
  let hdrRes = ctfs.writeToFile(w.indexFile, hdr)
  if hdrRes.isErr:
    return err("failed to write calls.idx header: " & hdrRes.unsafeError)
  ctfs.syncEntry(w.indexFile)

  ok(w)

proc flushChunk(ctfs: var Ctfs, w: var CallStreamWriter): Result[void, string] {.raises: [].} =
  ## Compress the buffered chunk, append it to calls.dat, and publish its byte
  ## offset in calls.idx. No-op when nothing is pending.
  ##
  ## The chunk's bytes, then its offset in `calls.idx`, then one publish of
  ## both, data before the root entries (`ctfs-container.md` §6,
  ## "Durability", rule 2).
  if w.pendingCount == 0:
    return ok()
  let chunkStart = w.datOffset
  let compressed = ?zstdCompress(w.pending, w.zstdLevel)

  # 1. Chunk body.
  let writeRes = ctfs.writeToFile(w.datFile, compressed)
  if writeRes.isErr:
    return err("calls.dat chunk write failed: " & writeRes.unsafeError)

  # 2. Its offset, then the publish.
  var offBuf: array[8, byte]
  writeU64LE(offBuf, 0, chunkStart)
  let idxRes = ctfs.writeToFile(w.indexFile, offBuf)
  if idxRes.isErr:
    return err("calls.idx offset write failed: " & idxRes.unsafeError)
  ctfs.syncEntry(w.indexFile)

  w.datOffset += uint64(compressed.len)
  w.pending.setLen(0)
  w.pendingCount = 0
  ok()

proc writeCall*(ctfs: var Ctfs, w: var CallStreamWriter,
    rec: CallRecord): Result[void, string] =
  ## Append a call record. Records are indexed by call_key (sequential, the
  ## record's position). Buffered into the current chunk; a full chunk is
  ## flushed to calls.dat immediately.
  let encoded = encodeCallRecord(rec)
  encodeVarint(uint64(encoded.len), w.pending)
  w.pending.add(encoded)
  w.pendingCount += 1
  w.recordCount += 1
  if w.pendingCount >= w.chunkSize:
    return flushChunk(ctfs, w)
  ok()

proc finalizeCallStream*(ctfs: var Ctfs, w: var CallStreamWriter): Result[void, string] =
  ## Seal the final partial chunk. The companion `calls.idx` is written
  ## INCREMENTALLY by `flushChunk` (header at init, one offset per sealed
  ## chunk), so a still-recording container is already seekable; this only
  ## flushes whatever remains buffered. MUST be called once after the last
  ## `writeCall`, before serializing the container. Idempotent.
  ##
  ## The on-disk `calls.idx` is byte-identical to the pre-incremental layout —
  ## `[chunk_size: u32 LE][offset_0: u64 LE]...` — because the same header and
  ## the same offsets are written, only earlier; a fully-closed container is
  ## unchanged.
  if w.finalized:
    return ok()
  ?flushChunk(ctfs, w)
  w.finalized = true
  ok()

proc count*(w: CallStreamWriter): uint64 = w.recordCount

# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

proc openCallStream(dat: sink MemberView, idx: openArray[byte],
    stored: bool): Result[CallStreamReader, string] =
  # A reader of a container still being written can meet a partial index
  # entry after the last whole one; it is not read.
  var spec = ? openChunkedRecords(dat, idx, "calls", "call", stored,
    trailingIndexBytes = true)
  let count = spec.count
  ok(CallStreamReader(spec: move spec, recordCount: count))

proc initCallStreamReader*(ctfsBytes: openArray[byte],
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): Result[CallStreamReader, string] =
  ## Initialize a seekable reader from raw CTFS container bytes. Reads
  ## calls.dat + calls.idx. Computes the total record count by decoding only
  ## the last chunk.
  var datRes = readInternalFile(ctfsBytes, "calls.dat", blockSize, maxEntries)
  if datRes.isErr:
    return err("failed to read calls.dat: " & datRes.unsafeError)
  let idxRes = readInternalFile(ctfsBytes, "calls.idx", blockSize, maxEntries)
  if idxRes.isErr:
    return err("failed to read calls.idx: " & idxRes.unsafeError)
  openCallStream(viewBytes(move datRes.get()), idxRes.get(),
    isCompactContainer(ctfsBytes))

proc initCallStreamReader*(image: ContainerImage,
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): Result[CallStreamReader, string] =
  ## As above, over a container image it shares: `calls.dat` is read in place.
  var datRes = viewMember(image, "calls.dat", blockSize, maxEntries)
  if datRes.isErr:
    return err("failed to read calls.dat: " & datRes.unsafeError)
  let idxRes = viewMember(image, "calls.idx", blockSize, maxEntries)
  if idxRes.isErr:
    return err("failed to read calls.idx: " & idxRes.unsafeError)
  openCallStream(move datRes.get(), ? idxRes.get().contents(),
    isCompactContainer(image.bytes))

proc refresh*(r: var CallStreamReader, image: ContainerImage,
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): Result[void, string] =
  ## Extend the reader by the chunks the container in `image` has published
  ## since it was opened or last refreshed (`ctfs-container.md` §6).
  let dat = viewMember(image, "calls.dat", blockSize, maxEntries)
  if dat.isErr:
    return err("failed to read calls.dat: " & dat.unsafeError)
  let idx = viewMember(image, "calls.idx", blockSize, maxEntries)
  if idx.isErr:
    return err("failed to read calls.idx: " & idx.unsafeError)
  ? r.spec.refresh(dat.unsafeGet(), ? idx.unsafeGet().contents(), "calls")
  r.recordCount = r.spec.count
  ok()

template readCallInto*(r: var CallStreamReader, callKey: uint64) =
  ## `readCall`'s body, for a proc returning `Result[CallRecord, string]` that
  ## reads a call record and returns it: the record is built in that proc's
  ## `result` rather than in each layer it would pass through.
  block readCallBody:
    if callKey >= r.recordCount:
      result = err("call_key " & $callKey & " out of range (count " &
        $r.recordCount & ")")
      break readCallBody
    var within: int
    var whyNot: string
    if not r.spec.locate(callKey, within, whyNot):
      result = err(whyNot)
      break readCallBody
    decodeCallInto(r.spec.record(within), "calls.dat record " & $callKey & ": ")

proc readCall*(r: var CallStreamReader,
    callKey: uint64): Result[CallRecord, string] =
  ## Read the call record at the given call_key, decompressing only its chunk.
  ## A one-chunk cache avoids re-decompressing clustered reads.
  r.readCallInto(callKey)

proc count*(r: CallStreamReader): uint64 = r.recordCount
