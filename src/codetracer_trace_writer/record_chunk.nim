{.push raises: [].}

## One chunk of a stream whose chunks are zstd frames of varint-length-prefixed
## records (`values.dat`, `calls.dat`, `events.dat`), held for repeated reads.
##
## A stream reader keeps one `RecordChunk`. Loading a chunk inflates its frame
## into a buffer the `RecordChunk` keeps from one load to the next, through the
## thread's shared decompression context. A chunk of a compact container
## (`ctfs-container.md` §1d) is stored as its content rather than as a frame,
## and `loadStored` takes it as it is.
##
## Records are framed lazily: `frame` notes where records start and end only
## as far as the record asked for, so a point read near the start of a chunk
## does not walk the rest of it, and a walk over the chunk frames each record
## once. A record is then read in place with `record`, so a read decodes the
## one record it wants and allocates nothing to reach it.

import results
import ../codetracer_ctfs/zstd_bindings
import ./varint
import ../codetracer_ctfs/member_view

export results, member_view

type
  RecordChunk* = object
    index: int          ## the chunk held, -1 when none
    raw: seq[byte]      ## its inflated bytes
    bounds: seq[int]    ## record `i` is `raw[bounds[2*i] ..< bounds[2*i + 1]]`,
                        ## for `i` below `framed`; the rest is spare capacity
    framed: int         ## how many records are framed
    framedTo: int       ## where the next record's length prefix starts

proc initRecordChunk*(): RecordChunk =
  RecordChunk(index: -1)

proc held*(c: RecordChunk): int {.inline.} =
  ## The chunk this holds, or -1.
  c.index

proc framed*(c: RecordChunk): int {.inline.} =
  ## How many records of the held chunk are framed so far: `record(i)` may be
  ## read for every `i` below it.
  c.framed

proc reset(c: var RecordChunk) =
  c.index = -1
  c.framed = 0
  c.framedTo = 0

proc loadStored*(c: var RecordChunk, index: int, content: openArray[byte])

proc load*(c: var RecordChunk, index: int, frame: openArray[byte],
    what: string, stored = false): Result[void, string] =
  ## Inflate `frame`, chunk `index` of a stream of `what` records — or, when
  ## the chunk is `stored` as its content (a compact container,
  ## `ctfs-container.md` §1f), hold it as it is. On failure nothing is held.
  ## An empty frame is a chunk with no records.
  if stored:
    c.loadStored(index, frame)
    return ok()
  c.reset()
  c.raw.setLen(0)
  if frame.len > 0:
    let size = ZSTD_getFrameContentSize(unsafeAddr frame[0], csize_t(frame.len))
    if size == ZSTD_CONTENTSIZE_UNKNOWN or size == ZSTD_CONTENTSIZE_ERROR:
      return err("cannot determine decompressed size for " & what & " chunk")
    c.raw.setLenUninit(int(size))  # every byte is written by the inflate
    if size > 0:
      let got = zstdDecompressShared(addr c.raw[0], csize_t(size),
        unsafeAddr frame[0], csize_t(frame.len))
      if ZSTD_isError(got) != 0:
        c.raw.setLen(0)
        return err("zstd decompress failed for " & what & " chunk: " &
          $ZSTD_getErrorName(got))
      c.raw.setLen(int(got))
  c.index = index
  ok()

proc loadStored*(c: var RecordChunk, index: int, content: openArray[byte]) =
  ## Hold chunk `index` whose records are `content` as stored, uncompressed.
  c.reset()
  c.raw.setLenUninit(content.len)
  if content.len > 0:
    copyMem(addr c.raw[0], unsafeAddr content[0], content.len)
  c.index = index

type
  FrameOutcome* = enum
    foHas          ## the record is framed
    foEnded        ## the chunk ends before it
    foBadLength    ## a length prefix does not decode
    foOverrun      ## a record's length runs past the chunk

proc frameTo*(c: var RecordChunk, i: int): FrameOutcome =
  ## Frame the held chunk's records up to record `i`, and say how that went.
  ## `frame` is the same, as a `Result`; this is the shape for a reader's hot
  ## path, which builds a refusal (`refusal`) only when there is one.
  while c.framed <= i:
    if c.framedTo >= c.raw.len:
      return foEnded
    var pos = c.framedTo
    var recLen: uint64
    if not readVarint(c.raw, pos, recLen):
      return foBadLength
    if recLen > uint64(c.raw.len - pos):
      return foOverrun
    if 2 * c.framed + 2 > c.bounds.len:
      c.bounds.setLenUninit(max(128, 2 * c.bounds.len))
    c.bounds[2 * c.framed] = pos
    c.framedTo = pos + int(recLen)
    c.bounds[2 * c.framed + 1] = c.framedTo
    inc c.framed
  foHas

proc refusal*(c: RecordChunk, outcome: FrameOutcome, i: int,
    what: string): string =
  ## Why record `i` could not be framed, for an outcome other than `foHas`.
  case outcome
  of foHas: ""
  of foEnded: what & " record " & $i & " missing in chunk " & $c.index
  of foBadLength:
    var pos = c.framedTo
    decodeVarint(c.raw, pos).error
  of foOverrun: what & " record length extends past chunk"

proc frame*(c: var RecordChunk, i: int, what: string): Result[bool, string] =
  ## Frame the held chunk's records up to record `i`. True when the chunk has
  ## a record `i`, false when it ends before it. A record whose length runs
  ## past the chunk, or whose length prefix does not decode, is refused when
  ## framing reaches it.
  let o = c.frameTo(i)
  case o
  of foHas: ok(true)
  of foEnded: ok(false)
  else: err(c.refusal(o, i, what))

proc count*(c: var RecordChunk, what: string): Result[int, string] =
  ## How many records the held chunk holds, framing all of them.
  discard ? c.frame(high(int) - 1, what)
  ok(c.framed)

template record*(c: RecordChunk, i: int): untyped =
  ## Record `i` of the held chunk, in place. `i` must be below `framed`.
  c.raw.toOpenArray(c.bounds[2 * i], c.bounds[2 * i + 1] - 1)

proc fieldBytes*(data: openArray[byte], first, len: int): seq[byte] =
  ## `data[first ..< first + len]`, copied out of a record into a value of
  ## its own. The caller has checked the range. A short field — most CBOR
  ## values a record carries are a few bytes — is copied byte by byte, which
  ## a WebAssembly build compiles to plain loads and stores where `copyMem`
  ## becomes a call into the host's `memory.copy`.
  if len <= 0:
    return
  result = newSeqUninit[byte](len)
  let src = cast[ptr UncheckedArray[byte]](unsafeAddr data[first])
  doAssert first + len <= data.len
  if len <= 32:
    let dst = cast[ptr UncheckedArray[byte]](addr result[0])
    for i in 0 ..< len:
      dst[i] = src[i]
  else:
    copyMem(addr result[0], src, len)

# ---------------------------------------------------------------------------
# A stream of records in chunks (`values.dat`, `calls.dat`, `events.dat`)
# ---------------------------------------------------------------------------

type
  ChunkedRecords* = object
    ## A chunked compressed table of length-prefixed records
    ## (`ctfs-container.md` §7): its data member read in place, its index
    ## parsed, and the chunk the last read reached held inflated.
    data: MemberView
    scratch: seq[byte]       ## a chunk's frame when it straddles two runs
    offsets: seq[uint64]     ## each chunk's first byte in the data member
    chunkSize*: int          ## records per chunk, every chunk but the last
    count*: uint64           ## records in the stream
    stored: bool             ## chunks are their content (compact, §1f)
    chunk: RecordChunk
    what: string             ## "value", "call", ... for refusals

proc loadChunk(r: var ChunkedRecords, c: int): Result[void, string] =
  let startOff = int(r.offsets[c])
  let endOff =
    if c + 1 < r.offsets.len: int(r.offsets[c + 1])
    else: r.data.len
  if startOff > endOff or endOff > r.data.len:
    return err(r.what & " chunk offsets out of range")
  r.data.ensureLoaded(startOff, endOff - startOff)
  r.data.withSpan(startOff, endOff - startOff, r.scratch, frame):
    ? r.chunk.load(c, frame, r.what, r.stored)
  ok()

proc openChunkedRecords*(data: sink MemberView, idx: openArray[byte],
    name, what: string, stored: bool,
    trailingIndexBytes = false): Result[ChunkedRecords, string] =
  ## The stream whose data member is `data` and whose index member is `idx`
  ## (`[chunk_size: u32][offset: u64]...`), named `name` (`"values"`) in
  ## refusals. All chunks but the last hold `chunk_size` records; the last is
  ## inflated to count its own, and stays held for the reads that follow.
  ## `trailingIndexBytes` tolerates a partial entry after the last whole one,
  ## which a reader of a container still being written can meet.
  if idx.len < 4:
    return err(name & ".idx too small for chunk_size header")
  let chunkSize = int(uint32(idx[0]) or (uint32(idx[1]) shl 8) or
    (uint32(idx[2]) shl 16) or (uint32(idx[3]) shl 24))
  if chunkSize == 0:
    return err("chunkSize in " & name & ".idx is 0")
  if (idx.len - 4) mod 8 != 0 and not trailingIndexBytes:
    return err(name & ".idx has trailing bytes in offset region")
  let numChunks = (idx.len - 4) div 8
  var r = ChunkedRecords(data: data, chunkSize: chunkSize, stored: stored,
    chunk: initRecordChunk(), what: what)
  r.offsets = newSeqUninit[uint64](numChunks)  # every entry written below
  for i in 0 ..< numChunks:
    var v = 0'u64
    for j in 0 ..< 8:
      v = v or (uint64(idx[4 + i * 8 + j]) shl (8 * j))
    r.offsets[i] = v
  if numChunks > 0:
    let last = numChunks - 1
    if int(r.offsets[last]) > r.data.len:
      return err("last " & what & " chunk offset past end of " & name & ".dat")
    ? r.loadChunk(last)
    r.count = uint64(last) * uint64(chunkSize) + uint64(? r.chunk.count(what))
  ok(r)

proc reach(r: var ChunkedRecords, c, within: int, why: var string): bool =
  ## `locate` past its fast path: load chunk `c` unless it is held, and frame
  ## it as far as record `within`.
  if r.chunk.held != c:
    let loaded = r.loadChunk(c)
    if loaded.isErr:
      why = loaded.unsafeError
      return false
  if within >= r.chunk.framed:
    let framing = r.chunk.frameTo(within)
    if framing != foHas:
      why = r.chunk.refusal(framing, within, r.what)
      return false
  true

proc locate*(r: var ChunkedRecords, index: uint64, within: var int,
    why: var string): bool {.inline.} =
  ## Hold the chunk with record `index`, which is below `count`, framed as far
  ## as the record, and set `within` to the record's index within the chunk.
  ## False, with `why` set, where that chunk cannot be read as far. A reader
  ## calls this once per record, so it builds no `Result`, and a record of
  ## the chunk held, already framed, is found without a call.
  let c = int(index div uint64(r.chunkSize))
  within = int(index mod uint64(r.chunkSize))
  (r.chunk.held == c and within < r.chunk.framed) or r.reach(c, within, why)

proc locate*(r: var ChunkedRecords, index: uint64): Result[int, string] =
  ## `locate`, as a `Result`: the record's index within its chunk.
  var within: int
  var why: string
  if r.locate(index, within, why): ok(within)
  else: err(why)

template record*(r: ChunkedRecords, within: int): untyped =
  ## Record `within` of the held chunk, in place (see `locate`).
  r.chunk.record(within)
