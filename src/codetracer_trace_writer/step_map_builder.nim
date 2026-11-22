{.push raises: [].}

## Step-map namespace builder (M26b).
##
## Accumulates the `(path_id, line) -> [step_id]` BREAKPOINT index during
## recording and serialises it, at finalize time, into the spec's flat `STMP`
## namespace (`step-map.ns`).  This is the on-disk, computed-at-recording-time
## equivalent of the db-backend's in-memory `path -> line -> [step]` map: when a
## `.ct` carries `step-map.ns`, BREAKPOINT line->step resolution is an
## O(unique-lines) index lookup that never materialises the whole step table.
##
## ## Why this keys off the recorded `(path_id, line)`
##
## The key side of this index is fixed by its query side. A BREAKPOINT request
## names a source file and a line; the debugger interns the file to a
## `path_id` and looks up `(path_id, line)`. So the index must be keyed by the
## coordinates the recorder registered the step at — the arguments of
## `registerStep(pathId, line, ...)` — and by nothing else.
##
## In particular it must not be keyed by inverting the `global_line_index` the
## writer packed those coordinates into. That integer's apportionment between
## files is a writer convention the container does not record, and the two
## writers of this format disagree about it: this repo packs
## `prefixSum[path_id] + (line - 1)`, the Rust `codetracer_trace_writer` packs
## `(path_id shl 32) or line` (`step_stream.rs pack_global_line_index`). See
## `global_line_index.nim`'s module header. Inverting one packing's integer
## with the other's formula files every step of every path above 0 under a
## path and line that nothing executed, and a breakpoint set anywhere in such
## a file then resolves against an empty entry.
##
## ## On-disk format — `internal-files.md` §"`step-map.ns`", version 2
##
## ```text
## Header (26 bytes):
##   magic u32 = 0x53544D50 ("STMP"), version u16 = 2, chunk_count u32,
##   path_count u32, line_count u32, step_count u64
## Chunk table, chunk_count x 20 bytes, in key order:
##   frame_offset u64 (from the end of the table), first_path_id u64,
##   first_line u32
## Frames: one zstd frame per chunk (level 3, one shot, content size declared).
## Chunk content: line records in ascending (path_id, line) order:
##   path_delta varint, line varint (absolute after a path change or at a
##   chunk's first record, else the delta), count varint, then
##   (gap varint, repeat varint) runs until their repeats add up to count;
##   the id before a list's first is -1, and runs are maximal.
## ```
##
## A chunk closes after the record that brings its decompressed size to
## 65,536 bytes or more. Step ids are exec-record indices; a step registered
## at line 0 is keyed under line 1 (§"Global Line Index", "Line 0 is line 1,
## everywhere"). All integers outside the frames are little-endian.

import std/[tables, algorithm]
import results
import ../codetracer_ctfs/zstd_bindings
import ../codetracer_ctfs/container
import ../codetracer_ctfs/compact
import ../codetracer_ctfs/types
import ./varint

export results

const
  StepMapMagic*: uint32 = 0x5354_4D50'u32
    ## ASCII "STMP" read as a little-endian u32 — the namespace magic.
  StepMapVersion*: uint16 = 2
    ## The only version written or read; version 1 is refused.
  StepMapHeaderSize* = 26
  StepMapChunkEntrySize* = 20
  StepMapChunkTarget* = 65536
    ## A chunk closes after the record that brings it to this many bytes.
  StepMapCompressionLevel = 3
  StepMapFileName*: string = "step-map.ns"
    ## The CTFS container-internal file name for the prepopulated index.

type
  StepMapBuilder* = object
    ## Accumulates `(path_id, line) -> [step_id]` during recording.  Step ids
    ## arrive in ascending order (the writer's monotonic `stepCount`), so each
    ## per-line list is already sorted; the serializer re-sorts defensively to
    ## stay byte-identical to the canonical Rust path regardless of insertion
    ## order.
    byPath: Table[uint64, Table[uint32, seq[int64]]]

proc initStepMapBuilder*(): StepMapBuilder =
  ## A fresh, empty step-map builder.
  StepMapBuilder(byPath: initTable[uint64, Table[uint32, seq[int64]]]())

proc recordStep*(b: var StepMapBuilder, pathId: uint64, line: uint64,
    stepId: uint64) =
  ## Record that step `stepId` executed at `(pathId, line)` — the coordinates
  ## the recorder registered, which are also the coordinates a breakpoint
  ## request arrives as.  `line` is narrowed to the u32 wire width of the
  ## `STMP` line field.
  # `default`, not `initTable`: the default is evaluated on every call, and
  # an empty table allocates nothing until its first insert.
  var byLine = addr b.byPath.mgetOrPut(pathId, default(Table[uint32, seq[int64]]))
  let line = if line == 0: 1'u32 else: uint32(line)
  var ids = addr byLine[].mgetOrPut(line, newSeq[int64]())
  ids[].add(int64(stepId))

proc entryCount*(b: StepMapBuilder): int =
  ## Total number of distinct `(path_id, line)` keys recorded.
  for byLine in b.byPath.values:
    result += byLine.len

proc putU16(buf: var seq[byte], v: uint16) =
  buf.add(byte(v and 0xFF)); buf.add(byte(v shr 8))

proc putU32(buf: var seq[byte], v: uint32) =
  for i in 0 ..< 4: buf.add(byte((v shr (8 * i)) and 0xFF))

proc putU64(buf: var seq[byte], v: uint64) =
  for i in 0 ..< 8: buf.add(byte((v shr (8 * i)) and 0xFF))

proc encodeRuns(ids: openArray[int64], buf: var seq[byte]) =
  ## The list as maximal (gap, repeat) runs; the id before the first is -1.
  var prev = -1'i64
  var runGap = 0'u64
  var runLen = 0'u64
  for id in ids:
    let g = uint64(id - prev)
    prev = id
    if runLen > 0 and g == runGap:
      inc runLen
    else:
      if runLen > 0:
        encodeVarint(runGap, buf)
        encodeVarint(runLen, buf)
      runGap = g
      runLen = 1
  if runLen > 0:
    encodeVarint(runGap, buf)
    encodeVarint(runLen, buf)

proc zstdOneShot(raw: openArray[byte]): Result[seq[byte], string] =
  ## One frame, level 3, compressed in one shot so it declares its content size.
  let bound = ZSTD_compressBound(csize_t(raw.len))
  var outBuf = newSeq[byte](int(bound))
  let n = ZSTD_compress(addr outBuf[0], bound,
    if raw.len > 0: unsafeAddr raw[0] else: nil, csize_t(raw.len),
    cint(StepMapCompressionLevel))
  if ZSTD_isError(n) != 0:
    return err("step-map.ns: zstd compression failed: " & $ZSTD_getErrorName(n))
  outBuf.setLen(int(n))
  ok(outBuf)

proc serialize*(b: StepMapBuilder): seq[byte] =
  ## Serialise the accumulated map as `step-map.ns` version 2 — byte for byte
  ## the layout `internal-files.md` §"`step-map.ns`" specifies, chunked by its
  ## normative 64 KiB rule so two writers produce the same bytes.
  var paths: seq[uint64]
  for pathId in b.byPath.keys:
    paths.add(pathId)
  paths.sort()

  type Chunk = tuple[path: uint64, line: uint32, raw: seq[byte]]
  var chunks: seq[Chunk]
  var cur: seq[byte]
  var open = false
  var firstPath = 0'u64
  var firstLine = 0'u32
  var prevPath = 0'u64
  var prevLine = 0'u32
  var lineCount = 0'u32
  var stepCount = 0'u64
  for pathId in paths:
    let byLine = b.byPath.getOrDefault(pathId)
    var lineKeys: seq[uint32]
    for line in byLine.keys:
      lineKeys.add(line)
    lineKeys.sort()
    for line in lineKeys:
      var ids = byLine.getOrDefault(line)
      ids.sort()
      if not open:
        open = true
        firstPath = pathId
        firstLine = line
        prevPath = pathId
        prevLine = 0
      let dp = pathId - prevPath
      encodeVarint(dp, cur)
      encodeVarint(if dp > 0: uint64(line) else: uint64(line - prevLine), cur)
      encodeVarint(uint64(ids.len), cur)
      encodeRuns(ids, cur)
      prevPath = pathId
      prevLine = line
      inc lineCount
      stepCount += uint64(ids.len)
      if cur.len >= StepMapChunkTarget:
        chunks.add((firstPath, firstLine, move(cur)))
        cur = @[]
        open = false
  if open:
    chunks.add((firstPath, firstLine, move(cur)))

  var frames: seq[seq[byte]]
  for c in chunks:
    let f = zstdOneShot(c.raw)
    # Level-3 one-shot compression of an in-memory buffer cannot fail short
    # of allocation failure; an empty frame would be refused by every reader.
    frames.add(if f.isOk: f.get() else: @[])

  result = @[]
  result.putU32(StepMapMagic)
  result.putU16(StepMapVersion)
  result.putU32(uint32(chunks.len))
  result.putU32(uint32(paths.len))
  result.putU32(lineCount)
  result.putU64(stepCount)
  var off = 0'u64
  for i, c in chunks:
    result.putU64(off)
    result.putU64(c.path)
    result.putU32(c.line)
    off += uint64(frames[i].len)
  for f in frames:
    result.add(f)

# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

type
  StepMapChunkRef = object
    frameStart: int
    frameEnd: int
    firstPath: uint64
    firstLine: uint32

  StepMapReader* = object
    ## A version 2 `step-map.ns`, opened over its bytes. `lookup` inflates one
    ## chunk and keeps it for the next lookup; `loadAll` inflates every chunk
    ## and verifies the header's counts.
    data: seq[byte]
    chunks: seq[StepMapChunkRef]
    pathCount*: uint32
    lineCount*: uint32
    stepCount*: uint64
    stored: bool
      ## Chunks are stored as their content, not as zstd frames: the map is a
      ## member of a compact container (`ctfs-container.md` §1f).
    held: int            ## the chunk `raw` holds, -1 when none
    raw: seq[byte]       ## its inflated content
    heldKeys: seq[StepMapKey]
      ## Every line record of the held chunk, in key order: the index the
      ## first lookup into the chunk builds while it checks the whole chunk.
    heldStarts: seq[int]
      ## Where each of those records' `count` field starts in `raw`.

  StepMapLine* = tuple[pathId: uint64, line: uint32, steps: seq[int64]]

  StepMapKey = (uint64, uint32)

  StepMapIndex* = object
    ## Every line of a `step-map.ns` and its step ids, as `loadAll` reads
    ## them: the keys in order, and every line's ids in one list, each line's
    ## ending where the next begins. One allocation holds every id.
    keys: seq[StepMapKey]
    ends: seq[int]      ## where each line's ids end in `ids`
    ids: seq[int64]

proc rdU16(d: openArray[byte], o: int): uint16 =
  uint16(d[o]) or (uint16(d[o + 1]) shl 8)

proc rdU32(d: openArray[byte], o: int): uint32 =
  for i in 0 ..< 4: result = result or (uint32(d[o + i]) shl (8 * i))

proc rdU64(d: openArray[byte], o: int): uint64 =
  for i in 0 ..< 8: result = result or (uint64(d[o + i]) shl (8 * i))

proc openStepMap*(data: sink seq[byte],
    stored = false): Result[StepMapReader, string] =
  ## Parse a `step-map.ns` header and chunk table. Refuses any version but 2.
  ## The reader keeps `data`; pass it by its last use and it is not copied.
  ## `stored` says the chunks are their content rather than zstd frames, as
  ## in a compact container (`ctfs-container.md` §1f); `openStepMapIn` reads
  ## the member and the profile out of a container.
  if data.len < StepMapHeaderSize:
    return err("step-map.ns: " & $data.len & " bytes, shorter than the " &
      $StepMapHeaderSize & "-byte header")
  if rdU32(data, 0) != StepMapMagic:
    return err("step-map.ns: bad magic")
  let version = rdU16(data, 4)
  if version != StepMapVersion:
    return err("step-map.ns: version " & $version & " is not supported; " &
      "this reader reads version " & $StepMapVersion & " only")
  let n = int(rdU32(data, 6))
  var r = StepMapReader(held: -1, stored: stored)
  r.pathCount = rdU32(data, 10)
  r.lineCount = rdU32(data, 14)
  r.stepCount = rdU64(data, 18)
  let tableEnd = StepMapHeaderSize + n * StepMapChunkEntrySize
  if n < 0 or tableEnd > data.len:
    return err("step-map.ns: chunk table of " & $n & " entries runs past the " &
      "member's " & $data.len & " bytes")
  for i in 0 ..< n:
    let o = StepMapHeaderSize + i * StepMapChunkEntrySize
    let fo = rdU64(data, o)
    if fo > uint64(data.len - tableEnd):
      return err("step-map.ns: chunk " & $i & " frame offset " & $fo &
        " is past the end of the member")
    r.chunks.add(StepMapChunkRef(frameStart: tableEnd + int(fo),
      firstPath: rdU64(data, o + 8), firstLine: rdU32(data, o + 16)))
  for i in 0 ..< n:
    r.chunks[i].frameEnd =
      if i + 1 < n: r.chunks[i + 1].frameStart else: data.len
    if r.chunks[i].frameEnd <= r.chunks[i].frameStart:
      return err("step-map.ns: chunk " & $i & " has an empty or inverted frame")
    if i > 0 and (r.chunks[i].firstPath, r.chunks[i].firstLine) <=
        (r.chunks[i - 1].firstPath, r.chunks[i - 1].firstLine):
      return err("step-map.ns: chunk table keys do not ascend at chunk " & $i)
  if n == 0 and (r.pathCount != 0 or r.lineCount != 0 or r.stepCount != 0):
    return err("step-map.ns: no chunks, but the header counts " &
      $r.pathCount & " paths, " & $r.lineCount & " lines, " & $r.stepCount &
      " steps")
  r.data = data
  ok(r)

proc openStepMap*(data: openArray[byte],
    stored = false): Result[StepMapReader, string] =
  ## As above, over a copy of `data`.
  openStepMap(@data, stored)

proc openStepMapIn*(container: openArray[byte]): Result[StepMapReader, string] =
  ## The `step-map.ns` of a container, in either profile, and stored under a
  ## whole-file scheme or not (`ctfs-container.md` §1a).
  if container.len > V6CompressionOffset and container[5] == CtfsVersionV6 and
      container[V6CompressionOffset] != uint8(ord(wfcNone)):
    var image = ? reconstructImage(container)
    image[V6CompressionOffset] = uint8(ord(wfcNone))
    return openStepMapIn(image)
  var bytes = readInternalFile(container, StepMapFileName)
  if bytes.isErr:
    return err(StepMapFileName & ": " & bytes.unsafeError)
  openStepMap(move bytes.get(), isCompactContainer(container))

proc inflateChunk(r: StepMapReader, c: int,
    raw: var seq[byte]): Result[void, string] =
  ## Inflate chunk `c` into `raw` — or copy it, when it is stored as content.
  let ch = r.chunks[c]
  if r.stored:
    raw.setLenUninit(ch.frameEnd - ch.frameStart)
    copyMem(addr raw[0], unsafeAddr r.data[ch.frameStart], raw.len)
    return ok()
  let src = unsafeAddr r.data[ch.frameStart]
  let srcLen = csize_t(ch.frameEnd - ch.frameStart)
  let size = ZSTD_getFrameContentSize(src, srcLen)
  if size == ZSTD_CONTENTSIZE_UNKNOWN or size == ZSTD_CONTENTSIZE_ERROR:
    return err("step-map.ns: chunk " & $c & " frame does not declare its size")
  raw.setLenUninit(int(size))  # every byte is written by the inflate
  let got = zstdDecompressShared(if raw.len > 0: addr raw[0] else: nil,
    csize_t(raw.len), src, srcLen)
  if ZSTD_isError(got) != 0:
    return err("step-map.ns: chunk " & $c & " does not decode: " &
      $ZSTD_getErrorName(got))
  if int(got) != raw.len:
    return err("step-map.ns: chunk " & $c & " decodes to " & $got &
      " bytes, not its declared " & $raw.len)
  ok()

proc runFits(prev: int64, gap, rep: uint64): bool {.inline.} =
  ## Whether a run of `rep` ids `gap` apart after `prev` ends at or below
  ## `high(int64)`, the largest id a step id list holds: `gap * rep` is at
  ## most what is left above `prev`, compared without forming the product.
  ## `gap` and `rep` are at least 1, `prev` at least -1.
  # `high(int64) - prev`, in unsigned arithmetic, which wraps to 2^63 for
  # the -1 a list starts from.
  let room = uint64(high(int64)) - cast[uint64](prev)
  gap <= room div rep

proc fillRun(ids: var seq[int64], at: int, prev: var int64,
    gap, rep: uint64) {.inline.} =
  ## Write a run's `rep` ids, `gap` apart after `prev`, to `ids[at ..]`, and
  ## leave `prev` at the last. The caller has sized `ids` and checked the run
  ## with `runFits`, so the ids are written without a check per id; the sum is
  ## wrapping only because `gap` may be 2^63, which `int64` cannot hold, and
  ## lands on an id `runFits` allowed.
  let dst = cast[ptr UncheckedArray[int64]](addr ids[at])
  let g = cast[int64](gap)
  var v = prev
  for k in 0 ..< int(rep):
    v = v +% g
    dst[k] = v
  prev = v

proc lineRefusal(path: uint64, line: uint32, what: string): string =
  ## A refusal of line `(path, line)`'s record, for `what` is wrong with it.
  "step-map.ns: line (" & $path & ", " & $line & ")" & what

proc varintRefusal(c, pos: int): string =
  "step-map.ns: chunk " & $c & ": a varint at byte " & $pos &
    " is truncated or longer than ten bytes"

proc scanChunk(r: StepMapReader, c: int, raw: openArray[byte],
    prevKey: var StepMapKey,
    havePrev: var bool, collect: bool,
    index: var StepMapIndex,
    keys: var seq[StepMapKey], starts: var seq[int]): Result[void, string] =
  ## Decode the inflated chunk `c`, checking everything the spec has a reader
  ## refuse. With `collect`, every line record is appended to `index`.
  ## Without it, no step id is written, and each record's key and the
  ## position of its `count` field are appended to `keys` and `starts`: the
  ## index `lookup` answers from.
  var pos = 0
  template next(v: var uint64) =
    if not readVarint(raw, pos, v):
      return err(varintRefusal(c, pos))
  var path = r.chunks[c].firstPath
  var line = 0'u32
  var first = true
  var dp, dl, count, gap, rep: uint64
  while pos < raw.len:
    next(dp)
    next(dl)
    if first:
      if dp != 0:
        return err("step-map.ns: chunk " & $c & "'s first record has a path " &
          "delta of " & $dp & "; it must be 0")
      line = uint32(dl)
      if (path, line) != (r.chunks[c].firstPath, r.chunks[c].firstLine):
        return err("step-map.ns: chunk " & $c & "'s first record key (" &
          $path & ", " & $line & ") is not its table key (" &
          $r.chunks[c].firstPath & ", " & $r.chunks[c].firstLine & ")")
    elif dp > 0:
      path += dp
      line = uint32(dl)
    else:
      line += uint32(dl)
    first = false
    let key = (path, line)
    if havePrev and key <= prevKey:
      return err("step-map.ns: keys do not ascend strictly at (" & $path &
        ", " & $line & ")")
    prevKey = key
    havePrev = true
    if not collect:
      keys.add(key)
      starts.add(pos)
    next(count)
    if count == 0:
      return err(lineRefusal(path, line, " has count 0"))
    let base = index.ids.len
    var n = 0'u64
    var prev = -1'i64
    while n < count:
      next(gap)
      next(rep)
      if gap == 0 or rep == 0:
        return err(lineRefusal(path, line, " has a run with gap " & $gap &
          " and repeat " & $rep))
      if rep > count - n:
        return err(lineRefusal(path, line, "'s runs overshoot its count " &
          $count))
      if not runFits(prev, gap, rep):
        return err(lineRefusal(path, line,
          " has a run whose step ids pass " & $high(int64)))
      if collect:
        # Grown a run at a time: `rep` was just checked against what is left
        # of `count`.
        index.ids.setLenUninit(base + int(n + rep))
        fillRun(index.ids, base + int(n), prev, gap, rep)
      else:
        # `runFits` bounds the product by 2^63, wrapping as `fillRun` does.
        prev = prev +% cast[int64](gap * rep)
      n += rep
    if collect:
      index.keys.add(key)
      index.ends.add(index.ids.len)
  ok()

proc decodeIds(raw: openArray[byte], pos: int): seq[int64] =
  ## The step ids of the record whose `count` field starts at `pos`, in a
  ## chunk `scanChunk` has already checked.
  var p = pos
  var count, gap, rep: uint64
  discard readVarint(raw, p, count)
  # The scan checked that the runs' repeats add up to `count` exactly.
  # A run at a time: the scan checked each run's ids against `high(int64)`.
  result = newSeqUninit[int64](int(count))
  var prev = -1'i64
  var n = 0
  while n < int(count):
    discard readVarint(raw, p, gap)
    discard readVarint(raw, p, rep)
    fillRun(result, n, prev, gap, rep)
    n += int(rep)

proc len*(idx: StepMapIndex): int =
  ## How many lines the index holds.
  idx.keys.len

proc pathId*(idx: StepMapIndex, i: int): uint64 = idx.keys[i][0]
proc line*(idx: StepMapIndex, i: int): uint32 = idx.keys[i][1]

proc ids*(idx: StepMapIndex): lent seq[int64] =
  ## Every line's step ids, line after line in key order.
  idx.ids

proc stepsStart*(idx: StepMapIndex, i: int): int =
  ## Where line `i`'s step ids begin in `ids`.
  if i == 0: 0 else: idx.ends[i - 1]

proc stepsEnd*(idx: StepMapIndex, i: int): int =
  ## Where line `i`'s step ids end in `ids` (exclusive).
  idx.ends[i]

template steps*(idx: StepMapIndex, i: int): openArray[int64] =
  ## Line `i`'s step ids, in place.
  idx.ids.toOpenArray(idx.stepsStart(i), idx.stepsEnd(i) - 1)

proc `[]`*(idx: StepMapIndex, i: int): StepMapLine =
  ## Line `i`, its step ids copied out.
  (idx.pathId(i), idx.line(i), @(idx.steps(i)))

iterator items*(idx: StepMapIndex): StepMapLine =
  ## Every line in key order, each one's step ids copied out.
  for i in 0 ..< idx.len:
    yield idx[i]

proc find*(idx: StepMapIndex, pathId: uint64, line: uint64): int =
  ## The position of `(pathId, line)` in the index, or -1. Line 0 is looked up
  ## as line 1, as `lookup` does.
  if line > high(uint32):
    return -1
  let key = (pathId, if line == 0: 1'u32 else: uint32(line))
  var lo = 0
  var hi = idx.keys.len
  while lo < hi:
    let mid = (lo + hi) div 2
    if idx.keys[mid] < key: lo = mid + 1
    else: hi = mid
  if lo < idx.keys.len and idx.keys[lo] == key: lo else: -1

proc loadAll*(r: StepMapReader): Result[StepMapIndex, string] =
  ## Every line's step ids, in key order. Refuses a map whose decoded counts
  ## disagree with the header.
  var index = StepMapIndex(
    keys: newSeqOfCap[StepMapKey](int(r.lineCount)),
    ends: newSeqOfCap[int](int(r.lineCount)),
    # The header's count, as far as a list grown on demand would reach on
    # its own: a count larger than the map holds is refused below, after
    # the ids are read, and must not size an allocation first.
    ids: newSeqOfCap[int64](int(min(r.stepCount, 1'u64 shl 20))))
  var prevKey = (0'u64, 0'u32)
  var havePrev = false
  var raw: seq[byte]
  var noKeys: seq[StepMapKey]
  var noStarts: seq[int]
  for c in 0 ..< r.chunks.len:
    ? r.inflateChunk(c, raw)
    ? r.scanChunk(c, raw, prevKey, havePrev, true, index, noKeys, noStarts)
  var paths = 0'u32
  var last = high(uint64)
  for key in index.keys:
    if key[0] != last:
      inc paths
      last = key[0]
  let steps = uint64(index.ids.len)
  if paths != r.pathCount or uint32(index.len) != r.lineCount or
      steps != r.stepCount:
    return err("step-map.ns: decodes to " & $paths & " paths, " & $index.len &
      " lines and " & $steps & " steps; the header says " & $r.pathCount &
      ", " & $r.lineCount & " and " & $r.stepCount)
  ok(index)

proc lookup*(r: var StepMapReader, pathId: uint64,
    line: uint64): Result[seq[int64], string] =
  ## The step ids of one `(path_id, line)`, inflating only the chunk that can
  ## hold it; the chunk stays inflated for the next lookup. Line 0 is looked
  ## up as line 1 (`internal-files.md` §"`step-map.ns`", "Reading"). Empty
  ## when no step has the key.
  let key = (pathId, if line == 0: 1'u32 else: uint32(line))
  var lo = 0
  var hi = r.chunks.len
  while lo < hi:
    let mid = (lo + hi) div 2
    if (r.chunks[mid].firstPath, r.chunks[mid].firstLine) <= key:
      lo = mid + 1
    else:
      hi = mid
  if lo == 0:
    return ok(newSeq[int64]())
  let c = lo - 1
  if r.held != c:
    # The whole chunk is checked once, when it is inflated, and indexed; a
    # defect anywhere in it refuses every lookup into it.
    r.held = -1
    r.heldKeys.setLen(0)
    r.heldStarts.setLen(0)
    ? r.inflateChunk(c, r.raw)
    var prevKey = (0'u64, 0'u32)
    var havePrev = false
    var noLines: StepMapIndex
    let scanned = r.scanChunk(c, r.raw, prevKey, havePrev, false, noLines,
      r.heldKeys, r.heldStarts)
    if scanned.isErr:
      r.heldKeys.setLen(0)
      r.heldStarts.setLen(0)
      return err(scanned.unsafeError)
    r.held = c
  var a = 0
  var b = r.heldKeys.len
  while a < b:
    let mid = (a + b) div 2
    if r.heldKeys[mid] < key: a = mid + 1
    else: b = mid
  if a == r.heldKeys.len or r.heldKeys[a] != key:
    return ok(newSeq[int64]())
  ok(decodeIds(r.raw, r.heldStarts[a]))
