{.push raises: [].}

## Seek-based trace reader (M18 + M19).
##
## Opens a multi-stream CTFS trace and provides random access to all data.
## Interning tables are loaded eagerly at startup; execution, value, call,
## and IO-event streams are initialized lazily on first access.

import results
import std/[os, json, options]
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ./meta_dat
import ./interning_table
import ./exec_stream
import ./value_stream
import ./call_stream
import ./io_event_stream
import ./step_encoding
import ./varint

type
  NewTraceReader* = object
    data: seq[byte]            ## raw .ct file bytes (mmap later)
    blockSize: uint32
    maxEntries: uint32

    # Metadata
    meta*: MetaDatContents

    # Interning tables (loaded at startup)
    pathReader: InterningTableReader
    funcReader: InterningTableReader
    typeReader: InterningTableReader
    varnameReader: InterningTableReader

    # paths.json fallback for traces that don't carry a binary paths
    # interning table yet (the M13 ct_recorder writer populates
    # paths.json but the binary paths.dat / paths.off table is still
    # an open TODO per the meta-json-retirement work tracked in
    # codetracer-specs/Planned-Work/Legacy-CTFS-Format-Cleanup.md).
    # When pathReader is empty we fall back to this list so callers
    # get the source paths they actually recorded.
    pathsJson: seq[string]

    # P6.5 / Layout A — per-file line-length tables, parsed from the
    # column-aware paths.dat records when `meta.hasColumnAwareSteps`
    # is set.  ``lineLengths[fileId][line]`` is the addressable column
    # count of line (0-indexed) in file ``fileId``.  When the trace is
    # not column-aware, this stays empty and column queries return
    # ``none``.
    lineLengths: seq[seq[uint32]]
    # Per-file cumulative line-base table (prefix sum of lineLengths).
    # Built lazily on first column resolution.  ``lineBase[fileId][l]``
    # is the in-file offset where line ``l`` starts in the file's
    # contiguous position range.
    lineBase: seq[seq[uint64]]
    # Per-file base in the global position space (prefix sum of each
    # file's ``sum(lineLengths)`` in column-aware mode, or
    # ``line_count`` in line-only mode).  ``fileBase[fileId]`` is the
    # ``global_position_index`` of the first position in that file.
    fileBase: seq[uint64]
    # Per-file size (sum of lineLengths in column-aware mode) used by
    # ``decodeGlobalPositionIndex``'s binary search.
    fileSize: seq[uint64]
    posTablesBuilt: bool

    # Stream readers (lazy, loaded on first access)
    execReader: ExecStreamReader
    valueReader: ValueStreamReader
    callReader: CallStreamReader
    ioEventReader: IOEventStreamReader

    # Flags for lazy initialization
    execLoaded: bool
    valueLoaded: bool
    callLoaded: bool
    ioEventLoaded: bool

# ---------------------------------------------------------------------------
# Opening
# ---------------------------------------------------------------------------

proc openNewTraceFromBytes*(data: seq[byte],
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): Result[NewTraceReader, string] =
  ## Open a trace from in-memory bytes. Used for testing.

  var reader: NewTraceReader
  reader.data = data
  reader.blockSize = blockSize
  reader.maxEntries = maxEntries

  # Read meta.dat
  let metaDataRes = readInternalFile(data, "meta.dat", blockSize, maxEntries)
  if metaDataRes.isOk:
    let metaRes = readMetaDat(metaDataRes.get())
    if metaRes.isOk:
      reader.meta = metaRes.get()

  # Load interning tables (these are small, load at startup)
  let pathRes = initInterningTableReader(data, "paths", blockSize, maxEntries)
  if pathRes.isOk: reader.pathReader = pathRes.get()

  let funcRes = initInterningTableReader(data, "funcs", blockSize, maxEntries)
  if funcRes.isOk: reader.funcReader = funcRes.get()

  let typeRes = initInterningTableReader(data, "types", blockSize, maxEntries)
  if typeRes.isOk: reader.typeReader = typeRes.get()

  let vnRes = initInterningTableReader(data, "varnames", blockSize, maxEntries)
  if vnRes.isOk: reader.varnameReader = vnRes.get()

  # paths.json fallback: when no binary paths interning table is
  # present, try the JSON form ct_recorder writes (M13).  The
  # binary table is preferred when both exist — see pathCount / path.
  if reader.pathReader.count() == 0:
    let pathsJsonRes = readInternalFile(data, "paths.json", blockSize, maxEntries)
    if pathsJsonRes.isOk:
      let pathsBytes = pathsJsonRes.get()
      if pathsBytes.len > 0:
        var pathsTxt = newString(pathsBytes.len)
        for i, b in pathsBytes:
          pathsTxt[i] = char(b)
        try:
          let parsed = parseJson(pathsTxt)
          if parsed.kind == JArray:
            for item in parsed.elems:
              if item.kind == JString:
                reader.pathsJson.add(item.getStr(""))
        except CatchableError:
          discard  # malformed paths.json — leave the fallback empty

  # P6.5 / Layout A — when the trace is column-aware, parse each
  # paths.dat record as
  # ``path_len + path_bytes + line_count + line_lengths`` and cache
  # the per-file line-length table.  Pre-extension traces (column flag
  # clear) skip this step and ``lineLengths`` stays empty.
  if reader.meta.hasColumnAwareSteps and reader.pathReader.count() > 0:
    let pathTotal = reader.pathReader.count()
    reader.lineLengths = newSeq[seq[uint32]](int(pathTotal))
    for i in 0'u64 ..< pathTotal:
      let rawRes = reader.pathReader.readRawById(i)
      if rawRes.isErr:
        return err("paths.dat[" & $i & "]: " & rawRes.error)
      let raw = rawRes.get()
      var pos = 0
      let pathLenRes = decodeVarint(raw, pos)
      if pathLenRes.isErr:
        return err("paths.dat[" & $i & "]: column-aware path_len varint: " &
          pathLenRes.error)
      let pathLen = int(pathLenRes.get())
      if pos + pathLen > raw.len:
        return err("paths.dat[" & $i & "]: path_bytes truncated")
      pos += pathLen
      let lineCountRes = decodeVarint(raw, pos)
      if lineCountRes.isErr:
        return err("paths.dat[" & $i & "]: column-aware line_count varint: " &
          lineCountRes.error)
      let lineCount = int(lineCountRes.get())
      var lls = newSeq[uint32](lineCount)
      var prev: int64 = 0
      for l in 0 ..< lineCount:
        let dRes = decodeSignedVarint(raw, pos)
        if dRes.isErr:
          return err("paths.dat[" & $i & "]: line_length[" & $l & "]: " &
            dRes.error)
        let d = dRes.get()
        let current = if l == 0: d else: prev + d
        if current < 0:
          return err("paths.dat[" & $i & "]: line_length[" & $l &
            "] negative: " & $current)
        lls[l] = uint32(current)
        prev = current
      reader.lineLengths[i] = lls

  ok(reader)

proc openNewTrace*(path: string): Result[NewTraceReader, string] =
  ## Open a multi-stream trace file from disk.
  ## Loads meta.dat and interning tables at startup.
  ## All other streams are loaded lazily on first access.

  if not fileExists(path):
    return err("file not found: " & path)

  var data: seq[byte]
  try:
    let f = open(path, fmRead)
    let size = f.getFileSize()
    data = newSeq[byte](size)
    discard f.readBytes(data, 0, size)
    f.close()
  except:
    return err("failed to read file: " & path)

  openNewTraceFromBytes(data)

# ---------------------------------------------------------------------------
# Interning table accessors
# ---------------------------------------------------------------------------

proc path*(r: NewTraceReader, id: uint64): Result[string, string] =
  if r.pathReader.count() > 0:
    if r.meta.hasColumnAwareSteps:
      # P6.5 / Layout A: paths.dat record is
      # ``path_len + path_bytes + line_count + line_lengths``.  Decode
      # only the path prefix to surface the legacy string-shaped API.
      let rawRes = r.pathReader.readRawById(id)
      if rawRes.isErr:
        return err(rawRes.error)
      let raw = rawRes.get()
      var pos = 0
      let pathLenRes = decodeVarint(raw, pos)
      if pathLenRes.isErr:
        return err("paths.dat[" & $id & "]: " & pathLenRes.error)
      let pathLen = int(pathLenRes.get())
      if pos + pathLen > raw.len:
        return err("paths.dat[" & $id & "]: path_bytes truncated")
      var s = newString(pathLen)
      for k in 0 ..< pathLen:
        s[k] = char(raw[pos + k])
      ok(s)
    else:
      r.pathReader.readById(id)
  elif r.pathsJson.len > 0 and id < uint64(r.pathsJson.len):
    ok(r.pathsJson[int(id)])
  else:
    r.pathReader.readById(id)  # error path — preserve the original error

proc function*(r: NewTraceReader, id: uint64): Result[string, string] =
  r.funcReader.readById(id)

proc typeName*(r: NewTraceReader, id: uint64): Result[string, string] =
  r.typeReader.readById(id)

proc varname*(r: NewTraceReader, id: uint64): Result[string, string] =
  r.varnameReader.readById(id)

proc pathCount*(r: NewTraceReader): uint64 =
  let binary = r.pathReader.count()
  if binary > 0: binary
  else: uint64(r.pathsJson.len)
proc functionCount*(r: NewTraceReader): uint64 = r.funcReader.count()
proc typeCount*(r: NewTraceReader): uint64 = r.typeReader.count()
proc varnameCount*(r: NewTraceReader): uint64 = r.varnameReader.count()

# ---------------------------------------------------------------------------
# P6.5 — column-aware position decoding (spec §"Source Location
#                                            Addressing")
# ---------------------------------------------------------------------------

proc lineLength*(r: NewTraceReader, fileId: uint64,
    lineIndex0: uint32): Option[uint32] =
  ## Return the addressable column count of ``lineIndex0`` (0-indexed,
  ## so line 1 of the file is ``lineIndex0 = 0``) in the file with id
  ## ``fileId``.  Returns ``none`` when the trace is not column-aware,
  ## when ``fileId`` is out of range, when the line index is past the
  ## file's known line table, or when the recorder did not surface a
  ## per-line table (``line_count = 0`` in paths.dat).  The back-compat
  ## default is "no per-line data" → ``none``, matching the spec
  ## contract for pre-extension traces.
  ##
  ## Note: callers that have a 1-indexed line number (per the spec
  ## convention used by AbsoluteStep / DeltaStep cursor tracking) must
  ## subtract 1 before calling.
  if not r.meta.hasColumnAwareSteps:
    return none(uint32)
  if fileId >= uint64(r.lineLengths.len):
    return none(uint32)
  let lls = r.lineLengths[fileId]
  if int(lineIndex0) >= lls.len:
    return none(uint32)
  some(lls[int(lineIndex0)])

proc ensurePositionTables(r: var NewTraceReader) =
  ## Build per-file cumulative tables used by ``decodeGlobalPositionIndex``.
  ## Idempotent: callable from every per-step resolution.
  if r.posTablesBuilt:
    return
  let fileCount = r.lineLengths.len
  r.lineBase = newSeq[seq[uint64]](fileCount)
  r.fileBase = newSeq[uint64](fileCount)
  r.fileSize = newSeq[uint64](fileCount)
  var runningGlobal: uint64 = 0
  for fid in 0 ..< fileCount:
    let lls = r.lineLengths[fid]
    var lb = newSeq[uint64](lls.len)
    var sum: uint64 = 0
    for i in 0 ..< lls.len:
      lb[i] = sum
      sum += uint64(lls[i])
    r.lineBase[fid] = lb
    r.fileBase[fid] = runningGlobal
    r.fileSize[fid] = sum
    runningGlobal += sum
  r.posTablesBuilt = true

proc decodeGlobalPositionIndex*(r: var NewTraceReader,
    p: uint64): Result[tuple[file: uint64, line: uint32, column: uint32],
    string] =
  ## P6.5 — resolve a ``global_position_index`` to ``(file, line,
  ## column)`` using the per-file / per-line cumulative tables built
  ## from the column-aware paths.dat records.  Implements the spec
  ## algorithm at ``codetracer-trace-format-spec/trace-events.md``
  ## §"Decoding ``global_position_index``": ``O(log F)`` file-table
  ## binary search + ``O(log L)`` line-table binary search.
  ##
  ## Only valid on column-aware traces.  ``line`` and ``column`` are
  ## 1-based to match the spec.
  if not r.meta.hasColumnAwareSteps:
    return err("decodeGlobalPositionIndex requires a column-aware trace")
  r.ensurePositionTables()
  if r.fileBase.len == 0:
    return err("trace has no paths registered")

  # Binary search for the file: largest fid with fileBase[fid] <= p.
  var lo = 0
  var hi = r.fileBase.len - 1
  var fid = -1
  while lo <= hi:
    let mid = (lo + hi) div 2
    if r.fileBase[mid] <= p:
      fid = mid
      lo = mid + 1
    else:
      hi = mid - 1
  if fid < 0:
    return err("global_position_index " & $p &
      " precedes the first file's base")
  if p >= r.fileBase[fid] + r.fileSize[fid]:
    return err("global_position_index " & $p &
      " out of range for file " & $fid)

  let q = p - r.fileBase[fid]
  let lb = r.lineBase[fid]
  if lb.len == 0:
    return err("file " & $fid & " has no line-length table")

  # Binary search for the line: largest l with lb[l] <= q.
  lo = 0
  hi = lb.len - 1
  var l = -1
  while lo <= hi:
    let mid = (lo + hi) div 2
    if lb[mid] <= q:
      l = mid
      lo = mid + 1
    else:
      hi = mid - 1
  if l < 0:
    return err("in-file offset " & $q & " precedes the first line")

  let column = uint32(q - lb[l] + 1)
  ok((file: uint64(fid), line: uint32(l + 1), column: column))

# ---------------------------------------------------------------------------
# Step access (lazy init exec reader)
# ---------------------------------------------------------------------------

proc ensureExecReader(r: var NewTraceReader): Result[void, string] =
  if not r.execLoaded:
    let res = initExecStreamReader(r.data, int(r.blockSize), int(r.maxEntries))
    if res.isErr: return err(res.error)
    r.execReader = res.get()
    r.execLoaded = true
  ok()

proc step*(r: var NewTraceReader, n: uint64): Result[StepEvent, string] =
  ?r.ensureExecReader()
  r.execReader.readEvent(n)

proc stepAbsoluteGlobalLineIndex*(r: var NewTraceReader,
    n: uint64): Result[uint64, string] =
  ## Return the absolute global line index for step N.
  ##
  ## The exec stream stores steps as a mix of AbsoluteStep and DeltaStep
  ## events. Each chunk starts with an AbsoluteStep, and subsequent events
  ## may be DeltaStep (relative to the previous). This method scans from
  ## the start of the chunk containing step N, accumulating deltas, to
  ## produce the absolute global line index.
  ?r.ensureExecReader()

  let chunkSize = uint64(r.execReader.chunkSize)
  let chunkStart = (n div chunkSize) * chunkSize
  var currentGli: uint64 = 0

  for i in chunkStart .. n:
    let ev = ?r.execReader.readEvent(i)
    case ev.kind
    of sekAbsoluteStep:
      currentGli = ev.globalLineIndex
    of sekDeltaStep:
      currentGli = uint64(int64(currentGli) + ev.lineDelta)
    of sekDeltaColumn:
      # P6.5: in column-aware traces ``global_position_index`` is
      # one-dimensional, so a column-only delta is also a position
      # delta.  Apply it to the running GLI so callers that decode the
      # absolute position see the post-column-delta cursor.  In
      # line-only traces this branch never fires because writers
      # cannot emit tag 0x07 without the column flag and the meta-dat
      # strict-rejection check guards against mismatches.
      currentGli = uint64(int64(currentGli) + ev.columnDelta)
    else:
      # Non-step events (raise, catch, thread_switch) don't change GLI
      discard

  ok(currentGli)

proc stepCount*(r: var NewTraceReader): Result[uint64, string] =
  ?r.ensureExecReader()
  ok(r.execReader.totalEvents)

proc stepAbsoluteGlobalLineIndices*(r: var NewTraceReader,
    startN: uint64, count: uint64,
    output: var openArray[uint64]): Result[uint64, string] =
  ## Bulk variant of [stepAbsoluteGlobalLineIndex].
  ##
  ## Resolves the absolute global line index for steps in
  ## ``[startN, startN + count)`` and writes them into ``output``.  Returns
  ## the number of step entries actually written (always equal to
  ## ``min(count, total_events - startN, output.len)``).
  ##
  ## Why this helper exists: the per-step accessor re-scans from the start
  ## of the chunk containing each requested step, which gives the loop
  ## ``for n in 0 ..< N: stepAbsoluteGlobalLineIndex(n)`` an O(N²/chunk)
  ## decode cost — every step inside a chunk re-decodes every prior step
  ## of that chunk.  This bulk routine streams events through each chunk
  ## exactly once, accumulating the running ``currentGli`` across delta
  ## events, which is O(N) and removes the per-step Rust→Nim FFI overhead
  ## entirely.  Non-step events (Raise, Catch, ThreadStart/Exit/Switch)
  ## carry no GLI delta so the running GLI is left untouched, mirroring
  ## [stepAbsoluteGlobalLineIndex].
  ?r.ensureExecReader()

  if count == 0'u64 or output.len == 0:
    return ok(0'u64)

  let totalEvents = r.execReader.totalEvents
  if startN >= totalEvents:
    return ok(0'u64)

  let endN = min(startN + count, totalEvents)
  let want = endN - startN
  let writable = min(uint64(output.len), want)
  if writable == 0'u64:
    return ok(0'u64)
  let stopN = startN + writable

  let chunkSize = uint64(r.execReader.chunkSize)
  if chunkSize == 0'u64:
    return err("execReader has zero chunkSize")

  var currentGli: uint64 = 0
  var events: seq[StepEvent] = @[]
  var n = startN
  while n < stopN:
    let chunkIdx = int(n div chunkSize)
    # Stream all events of the chunk through the cache exactly once.
    # ``readChunkEvents`` returns the chunk's first global event index
    # so we can map seq positions back to absolute step indices.
    let firstIdxRes = r.execReader.readChunkEvents(chunkIdx, events)
    if firstIdxRes.isErr:
      return err(firstIdxRes.error)
    let firstIdx = firstIdxRes.get()

    for offset, ev in events:
      let absIdx = firstIdx + uint64(offset)
      case ev.kind
      of sekAbsoluteStep:
        currentGli = ev.globalLineIndex
      of sekDeltaStep:
        currentGli = uint64(int64(currentGli) + ev.lineDelta)
      of sekDeltaColumn:
        # P6.5: see ``stepAbsoluteGlobalLineIndex`` for rationale —
        # column deltas advance ``global_position_index`` in the
        # one-dimensional column-aware position space.
        currentGli = uint64(int64(currentGli) + ev.columnDelta)
      else:
        discard
      if absIdx >= n and absIdx < stopN:
        output[int(absIdx - startN)] = currentGli

    # Advance ``n`` to the next chunk boundary so the outer loop picks
    # the correct chunk on the next iteration.
    n = firstIdx + uint64(events.len)
    if events.len == 0:
      # Defensive: should never happen on a well-formed trace, but break
      # rather than spin if a chunk decodes to zero events.
      break

  ok(writable)

# ---------------------------------------------------------------------------
# Value access (lazy init)
# ---------------------------------------------------------------------------

proc ensureValueReader(r: var NewTraceReader): Result[void, string] =
  if not r.valueLoaded:
    let res = initValueStreamReader(r.data, r.blockSize, r.maxEntries)
    if res.isErr: return err(res.error)
    r.valueReader = res.get()
    r.valueLoaded = true
  ok()

proc values*(r: var NewTraceReader, n: uint64): Result[seq[VariableValue], string] =
  ?r.ensureValueReader()
  r.valueReader.readStepValues(n)

iterator valuesIter*(r: var NewTraceReader, n: uint64): VariableValue =
  ## Yields variable values one at a time for a given step.
  let vals = r.values(n)
  if vals.isOk:
    for v in vals.get():
      yield v

proc values*(r: var NewTraceReader, n: uint64, output: var openArray[VariableValue]): int =
  ## Fill output buffer with values for step n. Returns the number of values written.
  let vals = r.values(n)
  if vals.isErr: return 0
  let vs = vals.get()
  let count = min(vs.len, output.len)
  for i in 0 ..< count:
    output[i] = vs[i]
  count

proc valueCount*(r: var NewTraceReader): Result[uint64, string] =
  ?r.ensureValueReader()
  ok(r.valueReader.count())

# ---------------------------------------------------------------------------
# Call access (lazy init)
# ---------------------------------------------------------------------------

proc ensureCallReader(r: var NewTraceReader): Result[void, string] =
  if not r.callLoaded:
    let res = initCallStreamReader(r.data, r.blockSize, r.maxEntries)
    if res.isErr: return err(res.error)
    r.callReader = res.get()
    r.callLoaded = true
  ok()

proc call*(r: var NewTraceReader, callKey: uint64): Result[CallRecord, string] =
  ?r.ensureCallReader()
  r.callReader.readCall(callKey)

proc callCount*(r: var NewTraceReader): Result[uint64, string] =
  ?r.ensureCallReader()
  ok(r.callReader.count())

proc callForStep*(r: var NewTraceReader, stepId: uint64): Result[CallRecord, string] =
  ## Find the innermost enclosing call record for the given step.
  ##
  ## Call records are sorted by `entryStep` ascending (= entry order, which
  ## also matches call_key allocation order after CTFS-M-CallKeyOrder).
  ## Because calls NEST, child entries follow their parent's entry but a
  ## parent's `exitStep` is far larger than any of its children's: child
  ## ranges `[entryStep, exitStep]` are strictly contained in the parent's.
  ##
  ## Strategy (correct for arbitrary nesting):
  ##   1. Binary-search for the largest index `k` with
  ##      `calls[k].entryStep <= stepId`. All calls at index > k were
  ##      entered after `stepId` and cannot contain it.
  ##   2. Walk back from `k` and return the FIRST call whose
  ##      `exitStep >= stepId`. That call is, by construction, the
  ##      most-recently-entered frame still open at `stepId`, hence the
  ##      deepest enclosing call. Earlier indices in the walk are either
  ##      siblings that already returned (their parent eventually has
  ##      `exitStep >= stepId`) or the matching parent itself.
  ##
  ## CTFS-M-FunctionAttrTemplate: the previous implementation interleaved
  ## an interpolation search with an early-exit on `stepId > hi.exitStep`
  ## and used the lo/hi-contains shortcut to record matches. Both pieces
  ## broke for steps that sit in a caller's body AFTER a nested call
  ## returned: the interpolation could jump past the parent (whose
  ## `exitStep` extends far beyond a sibling child's `exitStep`), and the
  ## hi-side early-exit truncated the search even when `lo` itself still
  ## covered `stepId`. The net effect was that every post-return step of
  ## any caller -- and every step in code emitted via template inlining
  ## that physically sits after the inlined call returns -- was reported
  ## as "not found in any call", emitting a step with no `function`,
  ## `function_id`, or `depth` attribution.
  ?r.ensureCallReader()
  let totalCalls = r.callReader.count()
  if totalCalls == 0:
    return err("no call records")

  # Step 1: binary search for largest index k with entryStep <= stepId.
  # If no such index exists (stepId precedes the first call), bail out.
  var lo: uint64 = 0
  var hi: uint64 = totalCalls - 1
  var k: int64 = -1
  while lo <= hi:
    let mid = lo + (hi - lo) div 2
    let midCall = ?r.callReader.readCall(mid)
    if midCall.entryStep <= stepId:
      k = int64(mid)
      if mid == high(uint64):  # defensive, can't happen with reasonable trace
        break
      lo = mid + 1
    else:
      if mid == 0:
        break
      hi = mid - 1
  if k < 0:
    return err("step " & $stepId & " not found in any call")

  # Step 2: walk backwards looking for the first enclosing call. The
  # walk traverses sibling subtrees that already returned; their parent
  # (or grandparent, etc.) is the answer. Worst case is O(N) but the
  # expected cost on well-formed traces is O(call-depth-at-stepId)
  # because each backward hop skips at most one returned subtree before
  # landing on the enclosing frame.
  var i = uint64(k)
  while true:
    let c = ?r.callReader.readCall(i)
    if c.exitStep >= stepId:
      return ok(c)
    if i == 0:
      break
    i -= 1
  err("step " & $stepId & " not found in any call")

iterator callRange*(r: var NewTraceReader, start, count: uint64): CallRecord =
  ## Yields call records in [start, start+count).
  let _ = r.ensureCallReader()
  for i in start ..< start + count:
    let res = r.callReader.readCall(i)
    if res.isOk:
      yield res.get()

proc callRange*(r: var NewTraceReader, start, count: uint64,
                output: var openArray[CallRecord]): int =
  ## Fill output buffer with call records starting at `start`.
  ## Returns the number of records written.
  let _ = r.ensureCallReader()
  var written = 0
  for i in start ..< start + count:
    if written >= output.len: break
    let res = r.callReader.readCall(i)
    if res.isOk:
      output[written] = res.get()
      written += 1
  written

# ---------------------------------------------------------------------------
# IO event access (lazy init)
# ---------------------------------------------------------------------------

proc ensureIOEventReader(r: var NewTraceReader): Result[void, string] =
  if not r.ioEventLoaded:
    let res = initIOEventStreamReader(r.data, r.blockSize, r.maxEntries)
    if res.isErr: return err(res.error)
    r.ioEventReader = res.get()
    r.ioEventLoaded = true
  ok()

proc ioEvent*(r: var NewTraceReader, index: uint64): Result[IOEvent, string] =
  ?r.ensureIOEventReader()
  r.ioEventReader.readEvent(index)

proc ioEventCount*(r: var NewTraceReader): Result[uint64, string] =
  ?r.ensureIOEventReader()
  ok(r.ioEventReader.count())

iterator events*(r: var NewTraceReader, start, count: uint64): IOEvent =
  ## Yields IO events in [start, start+count).
  let _ = r.ensureIOEventReader()
  for i in start ..< start + count:
    let res = r.ioEventReader.readEvent(i)
    if res.isOk:
      yield res.get()

proc events*(
    r: var NewTraceReader, start, count: uint64,
    output: var openArray[IOEvent]): int =
  ## Fill output buffer with IO events starting at `start`.
  ## Returns the number of events written.
  let _ = r.ensureIOEventReader()
  var written = 0
  for i in start ..< start + count:
    if written >= output.len: break
    let res = r.ioEventReader.readEvent(i)
    if res.isOk:
      output[written] = res.get()
      written += 1
  written
