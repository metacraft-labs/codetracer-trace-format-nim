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
## This storage replaces the pre-M20 `calls.dat` + `calls.off`
## VariableRecordTable layout (which the Rust seekable reader could not index),
## so a Nim-written split bundle is now SEEKABLE by the db-backend's
## `CallStreamReader` exactly like a Rust-writer one. The PUBLIC reader/writer
## API (`initCallStreamWriter`, `writeCall`, `finalizeCallStream`,
## `initCallStreamReader`, `readCall`, `count`) is unchanged so callers
## (`multi_stream_writer`, `new_trace_reader`) need only the close-time
## finalize.

import std/options
import results
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ../codetracer_ctfs/variable_record_table
import ../codetracer_ctfs/zstd_bindings
import ./varint

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
    ## at a time. `finalizeCallStream` flushes the last partial chunk and
    ## writes `calls.idx`.
    datFile: CtfsInternalFile      ## calls.dat handle
    chunkSize: int                 ## records per chunk
    zstdLevel: int                 ## zstd compression level
    pending: seq[byte]             ## current chunk's length-prefixed records
    pendingCount: int              ## records buffered in `pending`
    datOffset: uint64              ## running byte offset within calls.dat
    chunkOffsets: seq[uint64]      ## byte offset of each flushed chunk
    recordCount: uint64            ## total records appended
    finalized: bool

  CallStreamReader* = object
    ## Reads the dedicated call stream. Supports BOTH on-disk layouts so a
    ## reader works on any bundle, old or new:
    ##   * NEW (M20): chunked-Zstd `calls.dat` + companion `calls.idx`
    ##     (Rust-`CallStreamReader`-compatible, seekable). Selected when
    ##     `calls.idx` is present.
    ##   * LEGACY (pre-M20): `calls.dat` + `calls.off` VariableRecordTable.
    ##     Selected when `calls.idx` is absent. This keeps bundles recorded by
    ##     the pre-M20 Nim writer (flag clear) reading byte-for-byte unchanged.
    chunkSize: int
    chunkOffsets: seq[uint64]      ## byte offset of each chunk within calls.dat
    dat: seq[byte]                 ## raw calls.dat content (new format)
    recordCount: uint64
    cachedChunk: int               ## -1 = none
    cachedRecords: seq[seq[byte]]  ## decompressed records of cachedChunk
    legacy: Option[VariableRecordTableReader]
      ## Present iff the bundle uses the legacy `calls.dat` + `calls.off`
      ## VariableRecordTable layout (no `calls.idx`).

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

proc decodeCallRecord*(data: openArray[byte]): Result[CallRecord, string] {.raises: [].} =
  ## Decode a CallRecord from its wire format.
  var pos = 0
  var rec: CallRecord

  rec.functionId = ?decodeVarint(data, pos)
  rec.parentCallKey = ?decodeSignedVarint(data, pos)
  rec.entryStep = ?decodeVarint(data, pos)
  rec.exitStep = ?decodeVarint(data, pos)
  rec.depth = uint32(?decodeVarint(data, pos))

  # args
  let argsCount = int(?decodeVarint(data, pos))
  rec.args = newSeq[CallArg](argsCount)
  for i in 0 ..< argsCount:
    let varnameId = ?decodeVarint(data, pos)
    let argLen = int(?decodeVarint(data, pos))
    if pos + argLen > data.len:
      return err("truncated arg data")
    var value = newSeq[byte](argLen)
    for j in 0 ..< argLen:
      value[j] = data[pos + j]
    pos += argLen
    rec.args[i] = CallArg(varnameId: varnameId, value: value)

  # return value
  let retLen = int(?decodeVarint(data, pos))
  if pos + retLen > data.len:
    return err("truncated return value data")
  rec.returnValue = newSeq[byte](retLen)
  for j in 0 ..< retLen:
    rec.returnValue[j] = data[pos + j]
  pos += retLen

  # exception
  let excLen = int(?decodeVarint(data, pos))
  if pos + excLen > data.len:
    return err("truncated exception data")
  rec.exception = newSeq[byte](excLen)
  for j in 0 ..< excLen:
    rec.exception[j] = data[pos + j]
  pos += excLen

  # children
  let childrenCount = int(?decodeVarint(data, pos))
  rec.children = newSeq[uint64](childrenCount)
  for i in 0 ..< childrenCount:
    rec.children[i] = ?decodeVarint(data, pos)

  ok(rec)

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

proc zstdDecompress(src: openArray[byte]): Result[seq[byte], string] {.raises: [].} =
  ## Decompress a single Zstd frame.
  if src.len == 0:
    return ok(newSeq[byte](0))
  let contentSize = ZSTD_getFrameContentSize(unsafeAddr src[0], csize_t(src.len))
  if contentSize == ZSTD_CONTENTSIZE_ERROR:
    return err("zstd: invalid frame")
  if contentSize == ZSTD_CONTENTSIZE_UNKNOWN:
    return err("zstd: unknown frame content size")
  if contentSize == 0:
    return ok(newSeq[byte](0))
  var dst = newSeq[byte](int(contentSize))
  let written = ZSTD_decompress(addr dst[0], csize_t(dst.len),
                                unsafeAddr src[0], csize_t(src.len))
  if ZSTD_isError(written) != 0:
    return err("zstd decompress failed: " & $ZSTD_getErrorName(written))
  dst.setLen(int(written))
  ok(dst)

# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

proc initCallStreamWriter*(ctfs: var Ctfs,
    chunkSize: int = DefaultCallsChunkSize): Result[CallStreamWriter, string] =
  ## Create the `calls.dat` file. `calls.idx` is written by
  ## `finalizeCallStream`. `chunkSize` is the records-per-chunk seek
  ## granularity (matches the Rust writer's default).
  let cs = max(chunkSize, 1)
  let datFileRes = ctfs.addFile("calls.dat")
  if datFileRes.isErr:
    return err("failed to create calls.dat: " & datFileRes.error)
  ok(CallStreamWriter(
    datFile: datFileRes.get(),
    chunkSize: cs,
    zstdLevel: DefaultCallsZstdLevel,
    chunkOffsets: @[],
  ))

proc flushChunk(ctfs: var Ctfs, w: var CallStreamWriter): Result[void, string] {.raises: [].} =
  ## Compress the buffered chunk, append it to calls.dat, and record its
  ## offset. No-op when nothing is pending.
  if w.pendingCount == 0:
    return ok()
  w.chunkOffsets.add(w.datOffset)
  let compressed = ?zstdCompress(w.pending, w.zstdLevel)
  let writeRes = ctfs.writeToFile(w.datFile, compressed)
  if writeRes.isErr:
    return err("calls.dat chunk write failed: " & writeRes.error)
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
  ## Flush the final partial chunk and write the companion `calls.idx`.
  ## MUST be called once after the last `writeCall`, before serializing the
  ## container. Idempotent.
  if w.finalized:
    return ok()
  ?flushChunk(ctfs, w)

  # calls.idx: [chunk_size: u32 LE][offset_0: u64 LE]...  (seekable-zstd.md)
  var idx: seq[byte] = @[]
  var u32buf: array[4, byte]
  writeU32LE(u32buf, 0, uint32(w.chunkSize))
  for b in u32buf: idx.add(b)
  for off in w.chunkOffsets:
    var u64buf: array[8, byte]
    writeU64LE(u64buf, 0, off)
    for b in u64buf: idx.add(b)

  let idxFileRes = ctfs.addFile("calls.idx")
  if idxFileRes.isErr:
    return err("failed to create calls.idx: " & idxFileRes.error)
  var idxFile = idxFileRes.get()
  if idx.len > 0:
    let writeRes = ctfs.writeToFile(idxFile, idx)
    if writeRes.isErr:
      return err("calls.idx write failed: " & writeRes.error)
  w.finalized = true
  ok()

proc count*(w: CallStreamWriter): uint64 = w.recordCount

# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

proc parseCallsIdx(idx: openArray[byte]): Result[(int, seq[uint64]), string] {.raises: [].} =
  ## Parse calls.idx → (chunkSize, chunkOffsets).
  if idx.len < 4:
    return err("calls.idx: too short for chunk_size header")
  let chunkSize = int(uint32(idx[0]) or (uint32(idx[1]) shl 8) or
                      (uint32(idx[2]) shl 16) or (uint32(idx[3]) shl 24))
  if chunkSize == 0:
    return err("calls.idx: chunk_size is zero")
  var offsets: seq[uint64] = @[]
  var pos = 4
  while pos + 8 <= idx.len:
    offsets.add(readU64LE(idx, pos))
    pos += 8
  ok((chunkSize, offsets))

proc decodeChunkRecords(raw: openArray[byte]): Result[seq[seq[byte]], string] {.raises: [].} =
  ## Split a decompressed chunk into its length-prefixed records.
  var records: seq[seq[byte]] = @[]
  var pos = 0
  while pos < raw.len:
    let recLen = int(?decodeVarint(raw, pos))
    if pos + recLen > raw.len:
      return err("calls.dat: record extends past end of chunk")
    var rec = newSeq[byte](recLen)
    for i in 0 ..< recLen:
      rec[i] = raw[pos + i]
    records.add(rec)
    pos += recLen
  ok(records)

proc initCallStreamReader*(ctfsBytes: openArray[byte],
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): Result[CallStreamReader, string] =
  ## Initialize a seekable reader from raw CTFS container bytes. Reads
  ## calls.dat + calls.idx. Computes the total record count by decoding only
  ## the last chunk.
  let datRes = readInternalFile(ctfsBytes, "calls.dat", blockSize, maxEntries)
  if datRes.isErr:
    return err("failed to read calls.dat: " & datRes.error)
  let idxRes = readInternalFile(ctfsBytes, "calls.idx", blockSize, maxEntries)
  if idxRes.isErr:
    # No calls.idx ⇒ a pre-M20 legacy bundle whose calls.dat is a
    # VariableRecordTable (calls.dat + calls.off). Fall back to the legacy
    # reader so old (flag-clear) bundles keep reading byte-for-byte unchanged.
    let legacyRes = initVariableRecordTableReader(ctfsBytes, "calls",
        blockSize, maxEntries)
    if legacyRes.isErr:
      return err("failed to read legacy calls table: " & legacyRes.error)
    let lr = legacyRes.get()
    return ok(CallStreamReader(
      chunkSize: 1,
      recordCount: lr.count(),
      cachedChunk: -1,
      legacy: some(lr)))

  let parsed = ?parseCallsIdx(idxRes.get())
  let (chunkSize, chunkOffsets) = parsed
  let dat = datRes.get()

  var recordCount: uint64 = 0
  if chunkOffsets.len > 0:
    let lastChunk = chunkOffsets.len - 1
    let start = int(chunkOffsets[lastChunk])
    if start > dat.len:
      return err("calls.idx: last chunk offset past end of calls.dat")
    let raw = ?zstdDecompress(dat.toOpenArray(start, dat.len - 1))
    let lastRecords = ?decodeChunkRecords(raw)
    recordCount = uint64(lastChunk * chunkSize + lastRecords.len)

  ok(CallStreamReader(
    chunkSize: chunkSize,
    chunkOffsets: chunkOffsets,
    dat: dat,
    recordCount: recordCount,
    cachedChunk: -1,
    cachedRecords: @[],
  ))

proc readCall*(r: var CallStreamReader,
    callKey: uint64): Result[CallRecord, string] =
  ## Read the call record at the given call_key, decompressing only its chunk.
  ## A one-chunk cache avoids re-decompressing clustered reads. Legacy bundles
  ## read directly from the VariableRecordTable.
  if r.legacy.isSome:
    let dataRes = r.legacy.get().read(callKey)
    if dataRes.isErr:
      return err(dataRes.error)
    return decodeCallRecord(dataRes.get())
  if callKey >= r.recordCount:
    return err("call_key " & $callKey & " out of range (count " & $r.recordCount & ")")
  let chunkNumber = int(callKey) div r.chunkSize
  let within = int(callKey) mod r.chunkSize

  if r.cachedChunk != chunkNumber:
    let start = int(r.chunkOffsets[chunkNumber])
    let endOff =
      if chunkNumber + 1 < r.chunkOffsets.len: int(r.chunkOffsets[chunkNumber + 1])
      else: r.dat.len
    if start > endOff or endOff > r.dat.len:
      return err("calls.dat: chunk offsets out of range")
    let raw = ?zstdDecompress(r.dat.toOpenArray(start, endOff - 1))
    r.cachedRecords = ?decodeChunkRecords(raw)
    r.cachedChunk = chunkNumber

  if within >= r.cachedRecords.len:
    return err("call record " & $within & " missing in chunk " & $chunkNumber)
  decodeCallRecord(r.cachedRecords[within])

proc count*(r: CallStreamReader): uint64 = r.recordCount
