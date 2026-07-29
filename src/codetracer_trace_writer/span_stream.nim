{.push raises: [].}

## Request/interval span streams (RS-M1) — `spans.dat` / `spans.idx` /
## `spantype.ns`.
##
## Implements the container side of
## ``codetracer-specs/Trace-Files/CTFS-Request-Span-Streams.md``: the two CTFS
## internal files that carry a recording's *spans*, plus the span-type index.
##
## A span is a bounded, labeled interval of execution named by the coordinate
## *(process_ord, thread_id, step range)* — an HTTP request, a process, a test.
## The stream replaces the `session_manifest.jsonl` / `codetracer_spans.jsonl`
## sidecars the PHP / Ruby / Python recorders write today, so that a recording
## is **one artifact**: nothing else has to be found, polled or uploaded for a
## consumer to see the request list.
##
## # Files
##
## | File          | Type               | Contents                              |
## | ------------- | ------------------ | ------------------------------------- |
## | `spans.dat`   | Chunked compressed | Span records in append order          |
## | `spans.idx`   | Companion index    | `[chunk_size u32][offset u64]...`     |
## | `spantype.ns` | Namespace          | interned `span_type` id -> span ids   |
##
## All three are gated by `meta.dat` bit 13 `FlagHasSpanStream`.
##
## # Data layout (`spans.dat`) — CTFS §9c chunked compressed table
##
## ```text
## [zstd(chunk 0)][zstd(chunk 1)]...
## ```
##
## Each chunk groups up to ``chunkSize`` span records; a chunk's uncompressed
## payload is the concatenation of LENGTH-PREFIXED records
## (``[varint rec_len][rec_bytes]``...), exactly as `events.dat` / `calls.dat`
## do, so a reader can index the ``N % chunk_size``-th record of a chunk
## without re-deriving sizes.  Chunks are independently decompressible.
##
## # Index layout (`spans.idx`) — CTFS §7 companion index
##
## ```text
## [chunk_size: u32 LE][offset_0: u64 LE][offset_1: u64 LE]...
## ```
##
## There is no `total_spans` header or trailer, and — unlike a table whose
## chunks are all full but the last — a span chunk may be SHORT ANYWHERE in the
## stream: `flush` seals whatever is buffered, and a live recorder calls it
## per request so an in-flight span becomes visible immediately.  `chunk_size`
## is therefore an upper bound per chunk, never a guarantee.
##
## That costs nothing on the wire, because a chunk is self-describing: its
## payload is a sequence of length-prefixed records, so its true record count
## falls out of decoding it.  The reader builds a per-chunk cumulative-count
## table at init (see `initSpanStreamReader`) and uses it for `count` and for
## `readSpan`'s random access, instead of assuming uniformity.  The index is
## appended to as each chunk is sealed and is therefore always current during
## recording — this is the property that makes the stream tailable (see
## "Tailing" below).
##
## # Record model (spec §"Record Model", wire format v1)
##
## ```text
## span_id:          varint u64   # 1-based, monotonic within the container
## parent_span_id:   varint u64   # 0 = none (reserved; v1 spans are flat)
## flags:            u8           # bit 0: open record; bit 1: external binding
## status:           u8           # 0 unknown | 1 ok | 2 error
## start_wall_ns:    varint u64   # UNIX epoch nanoseconds at span start
## end_wall_ns:      varint u64   # 0 when flags.open is set
## process_ord:      varint u64   # ordinal into the process table; 0 = primary
## thread_id:        varint u64
## start_step:       varint u64   # first step id inside the span
## end_step:         varint u64   # last step id (0 when open)
## # flags.external ONLY — span lives in a different container:
## external_recording: string     # UUIDv7 recording_id
## external_path:      string     # path relative to this container's directory
## span_type:        string       # "web-request" | "process" | "test" | ...
## label:            string       # e.g. "GET /api/users", or an exe path
## structural:       u8           # bit 0: contiguous_on_one_thread
##                                # bit 1: shares_timeline
##                                # bit 2: concurrent_with_siblings
## metadata_count:   varint
## metadata:         (key: string, value: string) * metadata_count
## ```
##
## Strings are varint-length-prefixed UTF-8, matching the varint conventions
## the rest of the trace writer uses.  Metadata is a flat ordered key/value
## list — deliberately NOT interned (span counts are thousands per session
## against millions of steps, so interning would only add a cross-stream
## dependency; Zstd chunk compression already collapses the repetition) and
## deliberately NOT a `Table`, because **write order is part of the contract**:
## a consumer renders metadata in the order the recorder emitted it.
##
## The two external-binding strings are present ONLY when `flags.external` is
## set, per the spec's `# flags.external only` annotation.
##
## # Append-only, last-record-wins
##
## A writer MAY append a record with `flags.open` when a request starts, and
## appends a normal record with the SAME `span_id` when it completes.  Readers
## apply **last record wins per `span_id`** (`settledSpans` / `pageSpans`), so
## the stream stays strictly append-only — no record is ever rewritten, and a
## truncated or mid-upload container is always a valid prefix.  `readSpan`
## exposes the raw, unresolved record sequence for callers that need it.
##
## # Fail-closed decoding
##
## `decodeSpanRecord` NEVER silently drops or repairs a span.  It rejects:
## truncated fields, trailing bytes inside a record, unknown `flags` bits,
## unknown `structural` bits, a `status` outside 0..2, and an open record whose
## `end_wall_ns` / `end_step` are non-zero (the spec requires them to be 0 when
## `flags.open` is set).  Any of these fail the whole read with an error rather
## than yielding a partial span list.
##
## # Tailing during active writing
##
## `chunkCount` is the reader's cursor: it is exactly the number of `u64`
## entries in `spans.idx`.  A live consumer remembers the count it has already
## consumed and calls `readSpansSince(reader, knownChunkCount)`, which decodes
## ONLY the chunks sealed since then.  There is no finalization step — the
## reader re-opens the growing container, observes `spans.idx` having grown,
## and range-reads just the new chunks.  This is the primitive RS-M3's
## `CtUpdatedHttpRequests` delta is built on.
##
## Two details make that safe against a writer that is mid-chunk:
##
## 1. **The writer appends chunk data BEFORE the index entry** and syncs the
##    data file entry before the index file entry.  An index entry therefore
##    always means "this chunk is complete and starts here".  The spec's §7
##    "Writer Protocol" lists the index append (step c) before the data write
##    (step d); taken literally that publishes an offset for bytes that are not
##    on disk yet, which contradicts the same section's own guarantee that
##    "concurrent readers see new chunks as soon as the index entry is synced".
##    The on-disk *layout* is identical either way — ordering is purely a
##    runtime concern — so we use the safe order.
## 2. **The last indexed chunk's end offset is found from the zstd frame**, via
##    `ZSTD_findFrameCompressedSize`, not from the `spans.dat` file size.
##    While writing, `spans.dat` may already carry the leading bytes of the
##    next, not-yet-sealed chunk, and feeding those trailing bytes to
##    `ZSTD_decompress` is an error.

import std/[algorithm, sets, tables]
import results
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ../codetracer_ctfs/streaming
import ../codetracer_ctfs/zstd_bindings
import ./varint

export results

const
  SpansDataFileName* = "spans.dat"
  SpansIndexFileName* = "spans.idx"
  SpanTypeNamespaceFileName* = "spantype.ns"

  DefaultSpansChunkSize* = 64
    ## Records per chunk.  Span records are moderately sized — a web-request
    ## span carries a label plus ~5-9 metadata pairs, so a few hundred bytes,
    ## the same order as an I/O event record and well below a call record.
    ## 64 gives useful page granularity (a live panel wants the newest spans
    ## visible soon after they are written, which argues for small chunks)
    ## without paying a decompression per span.
  SpansCompressionLevel = 3
    ## Zstd level.  Compatibility does not depend on the level (zstd decode is
    ## level-agnostic), only on the chunk codec.

  # `flags` byte (spec §"Record Model")
  SpanFlagOpen* = 0x01'u8       ## bit 0 — open record, completion still to come
  SpanFlagExternal* = 0x02'u8   ## bit 1 — span lives in a different container
  SpanFlagsKnown = SpanFlagOpen or SpanFlagExternal

  # `structural` byte (Trace-Spans.md §2.4)
  SpanStructuralContiguous* = 0x01'u8
    ## bit 0 — the interval is an uninterrupted run on a single thread.
  SpanStructuralSharesTimeline* = 0x02'u8
    ## bit 1 — ordering (GEID/tick) is comparable with sibling intervals.
  SpanStructuralConcurrent* = 0x04'u8
    ## bit 2 — sibling intervals may overlap in time.
  SpanStructuralKnown = SpanStructuralContiguous or
    SpanStructuralSharesTimeline or SpanStructuralConcurrent

  SpanTypeNsMagic*: uint32 = 0x5350_5459'u32
    ## ASCII "SPTY" read as a little-endian u32 — the `spantype.ns` magic.
  SpanTypeNsVersion*: uint16 = 1

type
  SpanStatus* = enum
    ## Spec §"Record Model" `status` byte.  The ordinals are the wire values.
    spanStatusUnknown = 0
    spanStatusOk = 1
    spanStatusError = 2

  SpanRecord* = object
    ## One interval record.  Field order mirrors the spec's wire layout.
    spanId*: uint64
      ## 1-based, monotonic within the container.  The last-record-wins key.
    parentSpanId*: uint64
      ## 0 = none.  Reserved: v1 spans are flat, so nesting can land later
      ## without a version bump.
    isOpen*: bool
      ## `flags` bit 0.  The request has started but not finished; `endWallNs`
      ## and `endStep` are 0 and `status` is normally `spanStatusUnknown`.
    isExternal*: bool
      ## `flags` bit 1.  The span's execution lives in a DIFFERENT container,
      ## named by `externalRecording` / `externalPath`.  This is the fallback
      ## for recordings that genuinely stay separate (it replaces what
      ## `session_manifest.jsonl`'s `trace_dir` does today), not the primary
      ## model — an inline span names a coordinate in THIS container.
    status*: SpanStatus
    startWallNs*: uint64        ## UNIX epoch nanoseconds at span start
    endWallNs*: uint64          ## 0 when `isOpen`
    processOrd*: uint64         ## ordinal into the process table; 0 = primary
    threadId*: uint64
    startStep*: uint64          ## first step id inside the span
    endStep*: uint64            ## last step id; 0 when `isOpen`
    externalRecording*: string  ## UUIDv7 recording_id; only when `isExternal`
    externalPath*: string       ## relative path;      only when `isExternal`
    spanType*: string           ## "web-request" | "process" | "test" | ...
    label*: string              ## e.g. "GET /api/users", or an exe path
    contiguousOnOneThread*: bool   ## `structural` bit 0
    sharesTimeline*: bool          ## `structural` bit 1
    concurrentWithSiblings*: bool  ## `structural` bit 2
    metadata*: seq[(string, string)]
      ## Flat ordered key/value metadata.  ORDER IS PRESERVED on the wire and
      ## by every reader entry point — consumers render metadata in emission
      ## order, so this is a `seq` of pairs and never a `Table`.

  SpanStreamWriter* = object
    dataFile: CtfsInternalFile
    indexFile: CtfsInternalFile
    chunkSize: int
    buffer: seq[byte]        ## length-prefixed records for the current chunk
    recordCount: int         ## records buffered in the current chunk
    totalRecords: uint64
    dataOffset: uint64       ## running byte offset in spans.dat
    typeIds: Table[string, uint32]   ## span_type -> interned id
    typeOrder: seq[string]           ## interned id -> span_type
    spanIdsByType: Table[uint32, HashSet[uint64]]
      ## interned type id -> the DISTINCT span ids of that type.  A set, not a
      ## seq: an open record and its later completion share a `span_id` and
      ## must contribute that id exactly once, and the two records need not be
      ## adjacent in the stream (other spans interleave), so deduplication
      ## cannot rely on comparing against the previous append.

  SpanStreamReader* = object
    data: seq[byte]          ## raw spans.dat content
    chunkSize: uint32
    offsets: seq[uint64]     ## chunk byte offsets from spans.idx
    chunkFirstRecord: seq[uint64]
      ## `chunkFirstRecord[i]` is the append-order index of chunk `i`'s first
      ## record — a cumulative-count table, parallel to `offsets`.  Chunks may
      ## be SHORT anywhere in the stream (a live `flush` seals a partial
      ## chunk), so record index cannot be derived from `chunkSize`.
    totalRecordsVal: uint64
    cachedChunkIdx: int      ## -1 means no cache
    cachedRecords: seq[seq[byte]]

  SpanTypeEntry* = object
    ## One `spantype.ns` entry: an interned span-type id, its name, and the
    ## span ids of that type in ascending order.
    typeId*: uint32
    name*: string
    spanIds*: seq[uint64]

# ---------------------------------------------------------------------------
# Per-record encode / decode
# ---------------------------------------------------------------------------

proc hexByte(b: uint8): string =
  ## Two-digit uppercase hex for a byte, for decode-error messages.  Local so
  ## this module does not pull in `std/strutils` under `{.push raises: [].}`.
  const digits = "0123456789ABCDEF"
  result = newString(2)
  result[0] = digits[int(b shr 4)]
  result[1] = digits[int(b and 0x0F)]

proc appendVarintStr(buf: var seq[byte], s: string) =
  encodeVarint(uint64(s.len), buf)
  for i in 0 ..< s.len:
    buf.add(byte(s[i]))

proc readVarintStr(data: openArray[byte], pos: var int,
    what: string): Result[string, string] =
  let lenVal = ?decodeVarint(data, pos)
  let sLen = int(lenVal)
  if sLen < 0 or pos + sLen > data.len:
    return err("span record: " & what & " extends past end of record")
  var s = newString(sLen)
  for i in 0 ..< sLen:
    s[i] = char(data[pos + i])
  pos += sLen
  ok(s)

proc spanFlagsByte*(s: SpanRecord): uint8 =
  ## The `flags` byte for a span, per the spec's bit assignment.
  result = 0
  if s.isOpen: result = result or SpanFlagOpen
  if s.isExternal: result = result or SpanFlagExternal

proc spanStructuralByte*(s: SpanRecord): uint8 =
  ## The `structural` byte for a span (Trace-Spans.md §2.4).
  result = 0
  if s.contiguousOnOneThread: result = result or SpanStructuralContiguous
  if s.sharesTimeline: result = result or SpanStructuralSharesTimeline
  if s.concurrentWithSiblings: result = result or SpanStructuralConcurrent

proc encodeSpanRecord*(s: SpanRecord): Result[seq[byte], string] =
  ## Encode a span into its v1 wire format (no length prefix).
  ##
  ## Fails rather than silently normalising when the record contradicts the
  ## spec: an open record must carry `end_wall_ns == 0` and `end_step == 0`,
  ## and the external-binding strings only exist when `flags.external` is set,
  ## so a non-external span carrying them would lose data on the wire.
  if s.spanId == 0:
    return err("span record: span_id must be 1-based (got 0)")
  if s.isOpen and (s.endWallNs != 0 or s.endStep != 0):
    return err("span record: open span " & $s.spanId &
      " must have end_wall_ns and end_step == 0")
  if not s.isExternal and
      (s.externalRecording.len > 0 or s.externalPath.len > 0):
    return err("span record: span " & $s.spanId & " carries external " &
      "binding fields but flags.external is not set")

  var buf: seq[byte] = @[]
  encodeVarint(s.spanId, buf)
  encodeVarint(s.parentSpanId, buf)
  buf.add(spanFlagsByte(s))
  buf.add(uint8(ord(s.status)))
  encodeVarint(s.startWallNs, buf)
  encodeVarint(s.endWallNs, buf)

  # Execution binding.
  encodeVarint(s.processOrd, buf)
  encodeVarint(s.threadId, buf)
  encodeVarint(s.startStep, buf)
  encodeVarint(s.endStep, buf)

  # flags.external only.
  if s.isExternal:
    buf.appendVarintStr(s.externalRecording)
    buf.appendVarintStr(s.externalPath)

  # Common.
  buf.appendVarintStr(s.spanType)
  buf.appendVarintStr(s.label)
  buf.add(spanStructuralByte(s))
  encodeVarint(uint64(s.metadata.len), buf)
  for (k, v) in s.metadata:
    buf.appendVarintStr(k)
    buf.appendVarintStr(v)
  ok(buf)

proc decodeSpanRecord*(data: openArray[byte]): Result[SpanRecord, string] =
  ## Decode a span from its v1 wire format (the whole record, no length
  ## prefix).  FAIL-CLOSED: every malformation is an error, never a dropped or
  ## partially-populated span.  See the module header for the full list.
  var pos = 0
  var s = SpanRecord()

  s.spanId = ?decodeVarint(data, pos)
  if s.spanId == 0:
    return err("span record: span_id must be 1-based (got 0)")
  s.parentSpanId = ?decodeVarint(data, pos)

  if pos + 2 > data.len:
    return err("span record: truncated before flags/status")
  let flags = data[pos]
  pos += 1
  if (flags and not SpanFlagsKnown) != 0:
    return err("span record: unknown flags bits set: 0x" &
      hexByte(flags))
  s.isOpen = (flags and SpanFlagOpen) != 0
  s.isExternal = (flags and SpanFlagExternal) != 0

  let statusByte = data[pos]
  pos += 1
  if statusByte > uint8(ord(high(SpanStatus))):
    return err("span record: invalid status value " & $statusByte)
  s.status = SpanStatus(statusByte)

  s.startWallNs = ?decodeVarint(data, pos)
  s.endWallNs = ?decodeVarint(data, pos)

  s.processOrd = ?decodeVarint(data, pos)
  s.threadId = ?decodeVarint(data, pos)
  s.startStep = ?decodeVarint(data, pos)
  s.endStep = ?decodeVarint(data, pos)

  if s.isOpen and (s.endWallNs != 0 or s.endStep != 0):
    return err("span record: open span " & $s.spanId &
      " must have end_wall_ns and end_step == 0")

  if s.isExternal:
    s.externalRecording = ?readVarintStr(data, pos, "external_recording")
    s.externalPath = ?readVarintStr(data, pos, "external_path")

  s.spanType = ?readVarintStr(data, pos, "span_type")
  s.label = ?readVarintStr(data, pos, "label")

  if pos + 1 > data.len:
    return err("span record: truncated before structural byte")
  let structural = data[pos]
  pos += 1
  if (structural and not SpanStructuralKnown) != 0:
    return err("span record: unknown structural bits set: 0x" &
      hexByte(structural))
  s.contiguousOnOneThread = (structural and SpanStructuralContiguous) != 0
  s.sharesTimeline = (structural and SpanStructuralSharesTimeline) != 0
  s.concurrentWithSiblings = (structural and SpanStructuralConcurrent) != 0

  let metaCount = ?decodeVarint(data, pos)
  for i in 0'u64 ..< metaCount:
    let k = ?readVarintStr(data, pos, "metadata key")
    let v = ?readVarintStr(data, pos, "metadata value")
    s.metadata.add((k, v))

  if pos != data.len:
    return err("span record: " & $(data.len - pos) &
      " trailing bytes after record")
  ok(s)

# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

proc initSpanStreamWriter*(ctfs: var Ctfs,
    chunkSize: int = DefaultSpansChunkSize): Result[SpanStreamWriter, string] =
  ## Create the `spans.dat` / `spans.idx` stream pair and write the index
  ## header.  `spantype.ns` is written later, by `writeSpanTypeNamespace`.
  if chunkSize <= 0:
    return err("spans chunkSize must be positive")

  let datRes = ctfs.addFile(SpansDataFileName)
  if datRes.isErr:
    return err("failed to add " & SpansDataFileName & ": " & datRes.error)
  let idxRes = ctfs.addFile(SpansIndexFileName)
  if idxRes.isErr:
    return err("failed to add " & SpansIndexFileName & ": " & idxRes.error)

  var w = SpanStreamWriter(
    dataFile: datRes.get(),
    indexFile: idxRes.get(),
    chunkSize: chunkSize,
    buffer: @[],
    recordCount: 0,
    totalRecords: 0,
    dataOffset: 0,
    typeIds: initTable[string, uint32](),
    typeOrder: @[],
    spanIdsByType: initTable[uint32, HashSet[uint64]](),
  )

  # Index header: the u32 records-per-chunk count (CTFS §7).  No total count.
  var hdr: array[4, byte]
  let csLE = toBytesLE(uint32(chunkSize))
  for i in 0 ..< 4:
    hdr[i] = csLE[i]
  let hdrRes = ctfs.writeToFile(w.indexFile, hdr)
  if hdrRes.isErr:
    return err("failed to write " & SpansIndexFileName & " header: " &
      hdrRes.error)
  ctfs.syncEntry(w.indexFile)

  ok(w)

proc flushChunk(ctfs: var Ctfs, w: var SpanStreamWriter): Result[void, string] =
  ## Seal the buffered records into one chunk.
  ##
  ## Order matters for tailing readers: the compressed chunk is appended to
  ## `spans.dat` and that file entry synced FIRST, and only then is the
  ## chunk's byte offset appended to `spans.idx` and synced.  A reader that
  ## observes N index entries can therefore always assume chunks 0..N-1 are
  ## complete on disk.  See the module header for why this deviates from the
  ## literal step ordering in CTFS §7's "Writer Protocol".
  if w.recordCount == 0:
    return ok()

  let bound = ZSTD_compressBound(csize_t(w.buffer.len))
  var compressed = newSeq[byte](int(bound))
  let compressedSize = ZSTD_compress(
    addr compressed[0], csize_t(bound),
    addr w.buffer[0], csize_t(w.buffer.len),
    cint(SpansCompressionLevel))
  if ZSTD_isError(compressedSize) != 0:
    return err("zstd compress failed for span chunk: " &
      $ZSTD_getErrorName(compressedSize))

  let chunkStart = w.dataOffset

  # 1. Chunk body first, then publish its size to concurrent readers.
  let datRes = ctfs.writeToFile(w.dataFile,
      compressed.toOpenArray(0, int(compressedSize) - 1))
  if datRes.isErr:
    return err("failed to write span chunk: " & datRes.error)
  ctfs.syncEntry(w.dataFile)

  # 2. Only now does the index entry appear — it means "chunk complete".
  var offBytes: array[8, byte]
  let offLE = toBytesLE(chunkStart)
  for i in 0 ..< 8:
    offBytes[i] = offLE[i]
  let offRes = ctfs.writeToFile(w.indexFile, offBytes)
  if offRes.isErr:
    return err("failed to write " & SpansIndexFileName & " offset: " &
      offRes.error)
  ctfs.syncEntry(w.indexFile)

  w.dataOffset += uint64(compressedSize)
  w.buffer.setLen(0)
  w.recordCount = 0
  ok()

proc internSpanType(w: var SpanStreamWriter, spanType: string): uint32 =
  ## Intern a span-type name, assigning ids in first-appearance order.
  if w.typeIds.hasKey(spanType):
    return w.typeIds.getOrDefault(spanType)
  let id = uint32(w.typeOrder.len)
  w.typeIds[spanType] = id
  w.typeOrder.add(spanType)
  id

proc writeSpan*(ctfs: var Ctfs, w: var SpanStreamWriter,
    span: SpanRecord): Result[void, string] =
  ## Append one span record.  The stream is append-only: writing a completion
  ## record for a `span_id` that already has an open record appends a SECOND
  ## record, and readers resolve the pair by last-record-wins.
  let rec = ?encodeSpanRecord(span)
  # Length-prefix the record within the chunk so the reader can index it.
  encodeVarint(uint64(rec.len), w.buffer)
  w.buffer.add(rec)
  inc w.recordCount
  inc w.totalRecords

  # spantype.ns: record the span id under its interned type.  The set
  # deduplicates an open record against its later completion.
  let typeId = w.internSpanType(span.spanType)
  var ids = addr w.spanIdsByType.mgetOrPut(typeId, initHashSet[uint64]())
  ids[].incl(span.spanId)

  if w.recordCount >= w.chunkSize:
    return flushChunk(ctfs, w)
  ok()

proc flush*(ctfs: var Ctfs, w: var SpanStreamWriter): Result[void, string] =
  ## Seal any buffered records as a chunk and publish its offset in
  ## `spans.idx`.  Safe to call repeatedly during a live recording — that is
  ## how a writer commits in-flight spans before the container is closed (with
  ## a streaming `Ctfs` the chunk and its index entry reach the filesystem
  ## here) — and must be called once before serializing the CTFS.
  ##
  ## A repeated call deliberately leaves a SHORT chunk in the middle of the
  ## stream; publishing in-flight spans is the whole point of the API.  The
  ## reader tolerates that by construction: it derives each chunk's record
  ## count from the chunk itself rather than from `chunkSize`.
  flushChunk(ctfs, w)

proc count*(w: SpanStreamWriter): uint64 =
  ## Total span RECORDS appended (open records counted separately from their
  ## completions), including any still buffered in an unsealed chunk.
  w.totalRecords

# ---------------------------------------------------------------------------
# spantype.ns — interned span-type id -> span ids
# ---------------------------------------------------------------------------
#
# Flat, self-contained namespace image, in the style of `step-map.ns` (§4.1
# `STMP`).  It exists so a reader that only wants the processes (or only the
# tests) fetches a handful of ids instead of scanning a million request
# records — the same trick `linehits.tc` uses for source lines.
#
# ```text
# Header (18 bytes):
#   [magic u32 = "SPTY"][version u16 = 1][type_count u32][type_table_offset u64]
# Type table (at type_table_offset), sorted by span_type_id, 28 bytes each:
#   [span_type_id u32][name_len u32][name_offset u64]
#   [span_count u32][spans_offset u64]
# Name bytes (at name_offset): name_len UTF-8 bytes, no terminator.
# Span-id lists (at spans_offset): span_count x u64 LE, ascending.
# ```
# All integers little-endian.

proc putU16LE(buf: var seq[byte], off: int, v: uint16) =
  buf[off] = byte(v and 0xFF)
  buf[off + 1] = byte((v shr 8) and 0xFF)

proc putU32LE(buf: var seq[byte], off: int, v: uint32) =
  for i in 0 ..< 4:
    buf[off + i] = byte((v shr (i * 8)) and 0xFF'u32)

proc putU64LE(buf: var seq[byte], off: int, v: uint64) =
  for i in 0 ..< 8:
    buf[off + i] = byte((v shr (i * 8)) and 0xFF'u64)

proc getU16LE(data: openArray[byte], off: int): uint16 =
  uint16(data[off]) or (uint16(data[off + 1]) shl 8)

proc getU32LE(data: openArray[byte], off: int): uint32 =
  for i in countdown(3, 0):
    result = (result shl 8) or uint32(data[off + i])

proc getU64LE(data: openArray[byte], off: int): uint64 =
  for i in countdown(7, 0):
    result = (result shl 8) or uint64(data[off + i])

proc serializeSpanTypeNamespace*(w: SpanStreamWriter): seq[byte] =
  ## Serialise the accumulated span-type index into the `SPTY` wire format.
  ## Types are emitted sorted by interned id (which is first-appearance
  ## order), and each span-id list ascending.
  const
    HeaderSize = 18
    TypeEntrySize = 28

  let typeCount = w.typeOrder.len

  var idLists: seq[seq[uint64]] = @[]
  for id in 0 ..< typeCount:
    var ids: seq[uint64] = @[]
    for spanId in w.spanIdsByType.getOrDefault(uint32(id)):
      ids.add(spanId)
    ids.sort()
    idLists.add(ids)

  # Plan offsets: header | type table | name bytes | span-id lists.
  var cursor = HeaderSize + typeCount * TypeEntrySize

  var nameOffsets: seq[int] = @[]
  for id in 0 ..< typeCount:
    nameOffsets.add(cursor)
    cursor += w.typeOrder[id].len

  var listOffsets: seq[int] = @[]
  for id in 0 ..< typeCount:
    listOffsets.add(cursor)
    cursor += idLists[id].len * 8

  var buf = newSeq[byte](cursor)
  buf.putU32LE(0, SpanTypeNsMagic)
  buf.putU16LE(4, SpanTypeNsVersion)
  buf.putU32LE(6, uint32(typeCount))
  buf.putU64LE(10, uint64(HeaderSize))

  for id in 0 ..< typeCount:
    let base = HeaderSize + id * TypeEntrySize
    let name = w.typeOrder[id]
    buf.putU32LE(base, uint32(id))
    buf.putU32LE(base + 4, uint32(name.len))
    buf.putU64LE(base + 8, uint64(nameOffsets[id]))
    buf.putU32LE(base + 16, uint32(idLists[id].len))
    buf.putU64LE(base + 20, uint64(listOffsets[id]))

    for j in 0 ..< name.len:
      buf[nameOffsets[id] + j] = byte(name[j])
    for j in 0 ..< idLists[id].len:
      buf.putU64LE(listOffsets[id] + j * 8, idLists[id][j])

  buf

proc writeSpanTypeNamespace*(ctfs: var Ctfs,
    w: SpanStreamWriter): Result[void, string] =
  ## Add `spantype.ns` to the container with the current span-type index.
  ## Call after the final `flush`.
  let image = serializeSpanTypeNamespace(w)
  let fileRes = ctfs.addFile(SpanTypeNamespaceFileName)
  if fileRes.isErr:
    return err("failed to add " & SpanTypeNamespaceFileName & ": " &
      fileRes.error)
  var f = fileRes.get()
  let wr = ctfs.writeToFile(f, image)
  if wr.isErr:
    return err("failed to write " & SpanTypeNamespaceFileName & ": " &
      wr.error)
  ctfs.syncEntry(f)
  ok()

proc parseSpanTypeNamespace*(data: openArray[byte]):
    Result[seq[SpanTypeEntry], string] =
  ## Parse a `spantype.ns` image.  Fail-closed on any out-of-bounds offset.
  const
    HeaderSize = 18
    TypeEntrySize = 28
  if data.len < HeaderSize:
    return err("spantype.ns too short: " & $data.len & " bytes")
  if getU32LE(data, 0) != SpanTypeNsMagic:
    return err("spantype.ns: bad magic")
  let version = getU16LE(data, 4)
  if version != SpanTypeNsVersion:
    return err("spantype.ns: unsupported version " & $version)
  let typeCount = int(getU32LE(data, 6))
  let tableOff = int(getU64LE(data, 10))
  if tableOff < HeaderSize or
      tableOff + typeCount * TypeEntrySize > data.len:
    return err("spantype.ns: type table out of bounds")

  var entries: seq[SpanTypeEntry] = @[]
  for i in 0 ..< typeCount:
    let base = tableOff + i * TypeEntrySize
    let typeId = getU32LE(data, base)
    let nameLen = int(getU32LE(data, base + 4))
    let nameOff = int(getU64LE(data, base + 8))
    let spanCount = int(getU32LE(data, base + 16))
    let spansOff = int(getU64LE(data, base + 20))
    if nameOff < 0 or nameOff + nameLen > data.len:
      return err("spantype.ns: name out of bounds for type " & $typeId)
    if spansOff < 0 or spansOff + spanCount * 8 > data.len:
      return err("spantype.ns: span id list out of bounds for type " & $typeId)
    var name = newString(nameLen)
    for j in 0 ..< nameLen:
      name[j] = char(data[nameOff + j])
    var ids = newSeq[uint64](spanCount)
    for j in 0 ..< spanCount:
      ids[j] = getU64LE(data, spansOff + j * 8)
    entries.add(SpanTypeEntry(typeId: typeId, name: name, spanIds: ids))
  ok(entries)

proc readSpanTypeNamespace*(ctfsBytes: openArray[byte],
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries):
    Result[seq[SpanTypeEntry], string] =
  ## Read and parse `spantype.ns` out of a container.
  let raw = readInternalFile(ctfsBytes, SpanTypeNamespaceFileName,
    blockSize, maxEntries)
  if raw.isErr:
    return err("failed to read " & SpanTypeNamespaceFileName & ": " &
      raw.error)
  parseSpanTypeNamespace(raw.get())

proc spanIdsOfType*(entries: openArray[SpanTypeEntry],
    spanType: string): seq[uint64] =
  ## The span ids recorded under `spanType`, or an empty seq when the
  ## container holds no spans of that type.
  for e in entries:
    if e.name == spanType:
      return e.spanIds
  @[]

# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

proc decompressChunkRecords(compressed: openArray[byte]):
    Result[seq[seq[byte]], string] =
  ## Decompress one chunk and split it into its length-prefixed records.
  if compressed.len == 0:
    return ok(newSeq[seq[byte]]())
  let frameSize = ZSTD_getFrameContentSize(
    unsafeAddr compressed[0], csize_t(compressed.len))
  if frameSize == ZSTD_CONTENTSIZE_UNKNOWN or frameSize == ZSTD_CONTENTSIZE_ERROR:
    return err("cannot determine decompressed size for span chunk")
  var raw = newSeq[byte](int(frameSize))
  if frameSize > 0:
    let decompSize = ZSTD_decompress(
      addr raw[0], csize_t(frameSize),
      unsafeAddr compressed[0], csize_t(compressed.len))
    if ZSTD_isError(decompSize) != 0:
      return err("zstd decompress failed for span chunk: " &
        $ZSTD_getErrorName(decompSize))
    raw.setLen(int(decompSize))

  var records: seq[seq[byte]] = @[]
  var pos = 0
  while pos < raw.len:
    let recLen = int(?decodeVarint(raw, pos))
    if recLen < 0 or pos + recLen > raw.len:
      return err("span record length extends past chunk")
    var rec = newSeq[byte](recLen)
    for j in 0 ..< recLen:
      rec[j] = raw[pos + j]
    pos += recLen
    records.add(rec)
  ok(records)

proc chunkByteRange(r: SpanStreamReader,
    chunkNumber: int): Result[(int, int), string] =
  ## The `[start, end)` byte range of chunk `chunkNumber` in `spans.dat`.
  ##
  ## For any chunk but the last indexed one the end is simply the next index
  ## entry.  For the LAST indexed chunk the file size is NOT a safe end: while
  ## the container is still being written, `spans.dat` may already hold the
  ## leading bytes of the next, not-yet-sealed chunk.  Ask zstd for the exact
  ## frame length instead, so a tailing read decodes the same bytes a
  ## finalized read would.
  if chunkNumber < 0 or chunkNumber >= r.offsets.len:
    return err("span chunk " & $chunkNumber & " out of range (have " &
      $r.offsets.len & " chunks)")
  let startOff = int(r.offsets[chunkNumber])
  if startOff > r.data.len:
    return err("span chunk offset past end of " & SpansDataFileName)
  if chunkNumber + 1 < r.offsets.len:
    let endOff = int(r.offsets[chunkNumber + 1])
    if endOff < startOff or endOff > r.data.len:
      return err("span chunk offsets out of range")
    return ok((startOff, endOff))
  if startOff == r.data.len:
    return ok((startOff, startOff))
  let frameLen = ZSTD_findFrameCompressedSize(
    unsafeAddr r.data[startOff], csize_t(r.data.len - startOff))
  if ZSTD_isError(frameLen) != 0:
    return err("cannot determine span chunk frame size: " &
      $ZSTD_getErrorName(frameLen))
  let endOff = startOff + int(frameLen)
  if endOff > r.data.len:
    return err("span chunk frame extends past end of " & SpansDataFileName)
  ok((startOff, endOff))

proc decodeChunk(r: SpanStreamReader,
    chunkNumber: int): Result[seq[seq[byte]], string] =
  let (startOff, endOff) = ?r.chunkByteRange(chunkNumber)
  if startOff == endOff:
    return ok(newSeq[seq[byte]]())
  decompressChunkRecords(r.data.toOpenArray(startOff, endOff - 1))

proc initSpanStreamReader*(ctfsBytes: openArray[byte],
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries):
    Result[SpanStreamReader, string] =
  ## Open the span stream in a container.  Works equally on a finalized
  ## container and on one that is still being written — re-opening a growing
  ## container is exactly how a live consumer observes new chunks.
  let datRes = readInternalFile(ctfsBytes, SpansDataFileName, blockSize,
    maxEntries)
  if datRes.isErr:
    return err("failed to read " & SpansDataFileName & ": " & datRes.error)
  let datData = datRes.get()

  let idxRes = readInternalFile(ctfsBytes, SpansIndexFileName, blockSize,
    maxEntries)
  if idxRes.isErr:
    return err("failed to read " & SpansIndexFileName & ": " & idxRes.error)
  let idxData = idxRes.get()

  if idxData.len < 4:
    return err(SpansIndexFileName & " too small for chunk_size header")
  var cs4: array[4, byte]
  for i in 0 ..< 4:
    cs4[i] = idxData[i]
  let chunkSize = fromBytesLE(uint32, cs4)
  if chunkSize == 0:
    return err("chunkSize in " & SpansIndexFileName & " is 0")

  let offsetRegionBytes = idxData.len - 4
  if offsetRegionBytes mod 8 != 0:
    return err(SpansIndexFileName & " has trailing bytes in offset region")
  let numChunks = offsetRegionBytes div 8
  var offsets = newSeq[uint64](numChunks)
  for i in 0 ..< numChunks:
    var o8: array[8, byte]
    for j in 0 ..< 8:
      o8[j] = idxData[4 + i * 8 + j]
    offsets[i] = fromBytesLE(uint64, o8)

  var r = SpanStreamReader(
    data: datData,
    chunkSize: chunkSize,
    offsets: offsets,
    chunkFirstRecord: newSeq[uint64](numChunks),
    totalRecordsVal: 0,
    cachedChunkIdx: -1,
    cachedRecords: @[])

  # Recover the record count by asking each chunk how many records it actually
  # holds.  `chunk_size` is an UPPER BOUND, not an invariant: `flush` seals a
  # partial chunk whenever a live recorder wants an in-flight span published,
  # so a short chunk can sit anywhere in the stream.  Deriving the count (or a
  # record's chunk) from `chunk_size` would silently mis-address every record
  # after the first such flush.
  #
  # Cost: init decompresses the whole stream once, O(total records).  That is
  # affordable precisely for spans — thousands per session, against the
  # millions `steps.dat` carries — and `settledSpans` / `pageSpans` already
  # decode everything anyway.  Only the record COUNTS are retained; the record
  # bytes are dropped again except for the last chunk, which stays cached
  # because a tailing consumer reads the newest spans first.
  for c in 0 ..< numChunks:
    let recs = ?r.decodeChunk(c)
    r.chunkFirstRecord[c] = r.totalRecordsVal
    r.totalRecordsVal += uint64(recs.len)
    if c == numChunks - 1:
      r.cachedChunkIdx = c
      r.cachedRecords = recs

  ok(r)

proc count*(r: SpanStreamReader): uint64 =
  ## Number of span RECORDS committed to sealed chunks.  An open record and
  ## its later completion are two records; use `settledSpans` for the
  ## last-record-wins view.
  r.totalRecordsVal

proc chunkCount*(r: SpanStreamReader): int =
  ## Number of sealed chunks — equivalently, the number of `u64` entries in
  ## `spans.idx`.  This is the cursor a live consumer remembers between polls
  ## and hands back to `readSpansSince`.
  r.offsets.len

proc chunkSizeRecords*(r: SpanStreamReader): uint32 =
  ## Records per chunk, from the `spans.idx` header.  This is the writer's
  ## seal-at threshold and therefore an UPPER BOUND — a chunk sealed early by
  ## `flush` holds fewer.  Use `recordsInChunk` for a chunk's actual count.
  r.chunkSize

proc recordsInChunk*(r: SpanStreamReader, chunkNumber: int): int =
  ## Records actually held by chunk `chunkNumber`, from the cumulative table
  ## built at init.  0 for an out-of-range chunk.
  if chunkNumber < 0 or chunkNumber >= r.chunkFirstRecord.len:
    return 0
  let nextStart =
    if chunkNumber + 1 < r.chunkFirstRecord.len:
      r.chunkFirstRecord[chunkNumber + 1]
    else:
      r.totalRecordsVal
  int(nextStart - r.chunkFirstRecord[chunkNumber])

proc readSpan*(r: var SpanStreamReader,
    index: uint64): Result[SpanRecord, string] =
  ## Read the raw span record at `index` in append order, decompressing only
  ## its chunk.  Records are NOT resolved by last-record-wins here.
  ##
  ## The owning chunk comes from the cumulative-count table, NOT from
  ## `index div chunkSize` — a `flush` mid-recording seals a short chunk, after
  ## which the two disagree for every later record.
  if index >= r.totalRecordsVal:
    return err("span index " & $index & " out of range (count " &
      $r.totalRecordsVal & ")")
  # Largest chunk whose first-record index is <= `index`.  `upperBound` returns
  # the first entry strictly greater, so the predecessor is the owner; empty
  # chunks (equal consecutive entries) are skipped over correctly because the
  # LAST of a run of equal starts is the one that holds the record.
  let chunkNumber = upperBound(r.chunkFirstRecord, index) - 1
  if chunkNumber < 0 or chunkNumber >= r.chunkFirstRecord.len:
    return err("span index " & $index & " has no owning chunk")
  let within = int(index - r.chunkFirstRecord[chunkNumber])

  if r.cachedChunkIdx != chunkNumber:
    let recs = ?r.decodeChunk(chunkNumber)
    r.cachedRecords = recs
    r.cachedChunkIdx = chunkNumber

  if within >= r.cachedRecords.len:
    return err("span record " & $within & " missing in chunk " & $chunkNumber)
  decodeSpanRecord(r.cachedRecords[within])

proc readSpansInChunks*(r: SpanStreamReader, fromChunk: int,
    toChunk: int): Result[seq[SpanRecord], string] =
  ## Decode every record in chunks `[fromChunk, toChunk)`, in append order.
  ## Records are raw — apply `resolveSpans` for the settled view.
  if fromChunk < 0 or toChunk > r.offsets.len or fromChunk > toChunk:
    return err("span chunk range [" & $fromChunk & ", " & $toChunk &
      ") out of range (have " & $r.offsets.len & " chunks)")
  var spans: seq[SpanRecord] = @[]
  for c in fromChunk ..< toChunk:
    let recs = ?r.decodeChunk(c)
    for rec in recs:
      spans.add(?decodeSpanRecord(rec))
  ok(spans)

proc readSpansSince*(r: SpanStreamReader,
    knownChunkCount: int): Result[seq[SpanRecord], string] =
  ## The tailing primitive (RS-M3): decode ONLY the chunks sealed since the
  ## caller last looked, identified by the companion-index length it saw then.
  ## Returns them in append order; the new cursor is `chunkCount(r)`.
  ##
  ## No finalization step is involved — the caller re-opens the growing
  ## container, compares `chunkCount` with its cursor, and asks for the delta.
  if knownChunkCount < 0:
    return err("knownChunkCount must not be negative")
  if knownChunkCount > r.offsets.len:
    return err("knownChunkCount " & $knownChunkCount &
      " exceeds current chunk count " & $r.offsets.len &
      " (the index cannot shrink)")
  r.readSpansInChunks(knownChunkCount, r.offsets.len)

proc readAllSpanRecords*(r: SpanStreamReader): Result[seq[SpanRecord], string] =
  ## Every committed span record, raw, in append order.
  r.readSpansInChunks(0, r.offsets.len)

proc resolveSpans*(records: openArray[SpanRecord]): seq[SpanRecord] =
  ## Apply **last record wins per `span_id`** to a raw record sequence and
  ## return the settled spans in ascending `span_id` order.  An open record
  ## followed by its completion yields exactly one span — the completion.
  var bySpanId = initTable[uint64, SpanRecord]()
  var order: seq[uint64] = @[]
  for rec in records:
    if not bySpanId.hasKey(rec.spanId):
      order.add(rec.spanId)
    bySpanId[rec.spanId] = rec
  order.sort()
  for id in order:
    result.add(bySpanId.getOrDefault(id))

proc settledSpans*(r: SpanStreamReader): Result[seq[SpanRecord], string] =
  ## Every span in the container, last-record-wins applied, ascending by
  ## `span_id`.
  ok(resolveSpans(?r.readAllSpanRecords()))

proc pageSpans*(r: SpanStreamReader, fromSpanId: uint64,
    limit: int): Result[seq[SpanRecord], string] =
  ## Page through settled spans by span id: up to `limit` spans with
  ## `span_id >= fromSpanId`, ascending.  Backs `ct/load-request-spans`.
  ## `limit <= 0` means "no limit".
  let settled = ?r.settledSpans()
  var page: seq[SpanRecord] = @[]
  for s in settled:
    if s.spanId < fromSpanId:
      continue
    page.add(s)
    if limit > 0 and page.len >= limit:
      break
  ok(page)

proc hasSpanStreamFiles*(ctfsBytes: openArray[byte],
    maxEntries: uint32 = DefaultMaxRootEntries): bool =
  ## Whether the container carries `spans.dat`.  Callers should gate on the
  ## `meta.dat` bit 13 `FlagHasSpanStream` instead; this is for diagnostics
  ## and for tests that assert a span-free container gained no new files.
  hasInternalFile(ctfsBytes, SpansDataFileName, maxEntries)
