when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## High-level trace reader API for .ct files produced by TraceWriter.
##
## Opens a .ct file, parses the CTFS container, decompresses seekable Zstd
## chunks from events.log, decodes split-binary events, and provides
## JSON and text output.

import std/[json, strutils]
import results
import codetracer_ctfs/types
import codetracer_ctfs/base40
import codetracer_ctfs/container
import codetracer_ctfs/chunk_index
import codetracer_ctfs/zstd_bindings
import codetracer_trace_types
import codetracer_trace_writer/split_binary
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/uuid_v7
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/value_stream as v4_value_stream
import codetracer_trace_writer/call_stream as v4_call_stream
import codetracer_trace_writer/io_event_stream as v4_io_event_stream
import codetracer_trace_writer/cbor as v4_cbor
import codetracer_trace_writer/global_line_index as v4_gli

export results, codetracer_trace_types

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

type
  TraceReaderMetadata* = object
    ## Per-trace metadata as surfaced by `openTrace`.
    ##
    ## `recordingId` is the canonical UUIDv7 (M-REC-1, RFC 9562); the
    ## reader rejects traces whose metadata file lacks a valid id.
    recordingId*: string
    program*: string
    args*: seq[string]
    workdir*: string

  TraceReader* = object
    ctfsData: seq[byte]          ## Raw CTFS container
    blockSize: uint32
    maxRootEntries: uint32
    metadata*: TraceReaderMetadata
    paths*: seq[string]
    events*: seq[TraceLowLevelEvent]
    eventCount*: int
    isV4*: bool                  ## true if this is a multi-stream v4 trace

# ---------------------------------------------------------------------------
# Internal helpers: CTFS file reading
# ---------------------------------------------------------------------------

proc findInternalFileEntry(data: openArray[byte], name: string,
    maxEntries: uint32): tuple[size: uint64, mapBlock: uint64] =
  ## Search file entries in block 0 for the given name. Returns (size, mapBlock).
  let encoded = base40Encode(name)
  for i in 0 ..< int(maxEntries):
    let off = HeaderSize + ExtHeaderSize + i * FileEntrySize
    if off + FileEntrySize > data.len:
      break
    let entrySize = readU64LE(data, off)
    let entryMap = readU64LE(data, off + 8)
    let entryName = readU64LE(data, off + 16)
    if entryName == encoded:
      return (entrySize, entryMap)
  (0'u64, 0'u64)

proc readInternalFile(data: openArray[byte], name: string,
                      blockSize: uint32,
                      maxEntries: uint32): Result[seq[byte], string] =
  ## Read the complete content of an internal CTFS file by following the block mapping.
  let (fileSize, mapBlock) = findInternalFileEntry(data, name, maxEntries)
  if fileSize == 0 and mapBlock == 0:
    return err("internal file not found: " & name)

  var fileBytes = newSeq[byte](int(fileSize))
  let usable = uint64(blockSize) div 8 - 1

  var remaining = int(fileSize)
  var destPos = 0
  var blockIdx: uint64 = 0

  # Walk through data blocks using the mapping. Supports multi-level mapping
  # by using the same chain-walking logic as the writer.
  while remaining > 0:
    # Use lookupDataBlock logic inline for the reader (no Ctfs object available)
    var idx = blockIdx
    var currentLevelBlock = mapBlock
    var level: uint32 = 1

    # Walk up through levels
    block findLevel:
      while true:
        var cap: uint64 = 1
        for l in 0'u32 ..< level:
          cap = cap * usable
        if idx < cap:
          break findLevel
        idx -= cap
        level += 1
        if level > MaxChainLevels:
          return err("block index too large for mapping")
        # Follow chain pointer (last entry in current level block)
        let chainOff = int(currentLevelBlock) * int(blockSize) + int(usable) * 8
        if chainOff + 8 > data.len:
          return err("chain pointer out of bounds")
        let chainPtr = readU64LE(data, chainOff)
        if chainPtr == 0:
          return err("missing chain pointer at level " & $level)
        currentLevelBlock = chainPtr

    # Navigate down from level to find the data block
    var navBlock = currentLevelBlock
    var navLevel = level
    var navIdx = idx
    while navLevel > 1:
      var subCap: uint64 = 1
      for l in 0'u32 ..< (navLevel - 1):
        subCap = subCap * usable
      let entryIdx = navIdx div subCap
      let subIdx = navIdx mod subCap
      let childOff = int(navBlock) * int(blockSize) + int(entryIdx) * 8
      if childOff + 8 > data.len:
        return err("child pointer out of bounds")
      let childBlock = readU64LE(data, childOff)
      if childBlock == 0:
        return err("missing child block at level " & $navLevel)
      navBlock = childBlock
      navIdx = subIdx
      navLevel -= 1

    # Level 1: read direct pointer
    let ptrOff = int(navBlock) * int(blockSize) + int(navIdx) * 8
    if ptrOff + 8 > data.len:
      return err("data block pointer out of bounds")
    let dataBlock = readU64LE(data, ptrOff)
    if dataBlock == 0:
      return err("null data block at index " & $blockIdx)

    let blockOff = int(dataBlock) * int(blockSize)
    let toCopy = min(remaining, int(blockSize))
    if blockOff + toCopy > data.len:
      return err("data block content out of bounds")
    for i in 0 ..< toCopy:
      fileBytes[destPos + i] = data[blockOff + i]
    destPos += toCopy
    remaining -= toCopy
    blockIdx += 1

  ok(fileBytes)

proc bytesToString(data: seq[byte]): string =
  result = newString(data.len)
  for i in 0 ..< data.len:
    result[i] = char(data[i])

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

proc openTrace*(path: string): Result[TraceReader, string] =
  ## Open a .ct file and parse its contents (metadata and paths).
  let readRes = readCtfsFromFile(path)
  if readRes.isErr:
    return err("failed to read file: " & readRes.error)

  let data = readRes.get()

  # Validate CTFS magic
  if not hasCtfsMagic(data):
    return err("not a valid CTFS file (bad magic)")
  if not hasValidVersion(data):
    return err("unsupported CTFS version")

  # Read block size and max entries from extended header
  if data.len < 16:
    return err("file too small for CTFS header")

  var bs4: array[4, byte]
  bs4[0] = data[8]; bs4[1] = data[9]; bs4[2] = data[10]; bs4[3] = data[11]
  let blockSize = fromBytesLE(uint32, bs4)

  var mr4: array[4, byte]
  mr4[0] = data[12]; mr4[1] = data[13]; mr4[2] = data[14]; mr4[3] = data[15]
  let maxEntries = fromBytesLE(uint32, mr4)

  # Detect v4 (multi-stream) by absence of events.log.  v3 traces always
  # contain events.log; v4 traces never do (they use per-kind streams).
  let isV4 = findInternalFileEntry(data, "events.log", maxEntries).size == 0 and
             findInternalFileEntry(data, "events.log", maxEntries).mapBlock == 0

  var reader = TraceReader(
    ctfsData: data,
    blockSize: blockSize,
    maxRootEntries: maxEntries,
    metadata: TraceReaderMetadata(),
    paths: @[],
    events: @[],
    eventCount: 0,
    isV4: isV4,
  )

  # Try meta.dat first (new binary format), fall back to meta.json + paths.json
  let metaDatRes = readInternalFile(data, "meta.dat", blockSize, maxEntries)
  if metaDatRes.isOk:
    let parsed = readMetaDat(metaDatRes.get())
    if parsed.isOk:
      let contents = parsed.get()
      # M-REC-1: surface recording_id from the parsed metadata.
      reader.metadata.recordingId = contents.recordingId
      reader.metadata.program = contents.program
      reader.metadata.workdir = contents.workdir
      reader.metadata.args = contents.args
      reader.paths = contents.paths
    else:
      return err("meta.dat present but corrupt: " & parsed.error)
  else:
    # Fall back to meta.json + paths.json (legacy JSON sidecar).
    # M-REC-1: pre-1.0 the spec rejects metadata without recording_id,
    # so we require the JSON to carry one too.
    let metaRes = readInternalFile(data, "meta.json", blockSize, maxEntries)
    if metaRes.isOk:
      let metaStr = bytesToString(metaRes.get())
      var recordingIdFromJson = ""
      try:
        let node = parseJson(metaStr)
        recordingIdFromJson =
          node.getOrDefault("recording_id").getStr("")
        reader.metadata.program = node.getOrDefault("program").getStr("")
        reader.metadata.workdir = node.getOrDefault("workdir").getStr("")
        let argsNode = node.getOrDefault("args")
        if argsNode != nil and argsNode.kind == JArray:
          for item in argsNode:
            reader.metadata.args.add(item.getStr(""))
      except JsonParsingError:
        return err("failed to parse meta.json")
      except KeyError:
        return err("unexpected key error in meta.json")
      except IOError:
        return err("IO error parsing meta.json")
      except OSError:
        return err("OS error parsing meta.json")
      except ValueError:
        return err("value error parsing meta.json")
      except Exception:
        return err("unexpected error parsing meta.json")
      let valRes = validateRecordingIdStr(recordingIdFromJson)
      if valRes.isErr:
        return err("meta.json: invalid or missing recording_id: " &
                   valRes.error)
      reader.metadata.recordingId = recordingIdFromJson

    let pathsRes = readInternalFile(data, "paths.json", blockSize, maxEntries)
    if pathsRes.isOk:
      let pathsStr = bytesToString(pathsRes.get())
      try:
        let arr = parseJson(pathsStr)
        if arr.kind == JArray:
          for item in arr:
            reader.paths.add(item.getStr(""))
      except JsonParsingError:
        return err("failed to parse paths.json")
      except KeyError:
        return err("unexpected key error in paths.json")
      except IOError:
        return err("IO error parsing paths.json")
      except OSError:
        return err("OS error parsing paths.json")
      except ValueError:
        return err("value error parsing paths.json")
      except Exception:
        return err("unexpected error parsing paths.json")

  ok(reader)

proc decodeValueRecordBytes(data: openArray[byte]): ValueRecord =
  ## Decode a CBOR-encoded ValueRecord, returning NoneValue on failure.
  ## v4 attaches values as CBOR ValueRecord byte blobs in the value stream.
  if data.len == 0:
    return NoneValue
  var dec = CborDecoder.init(data)
  let r = dec.decodeCborValueRecord()
  if r.isOk:
    r.get()
  else:
    ValueRecord(kind: vrkRaw, rawStr: "<decode error: " & r.unsafeError & ">",
                rawTypeId: NoneTypeId)

proc decodeReturnValueBytes(data: openArray[byte]): ValueRecord =
  ## Decode a v4 return-value byte blob.  A single 0xFF byte is the
  ## VoidReturnMarker (→ NoneValue); otherwise the bytes are CBOR.
  if data.len == 0:
    return NoneValue
  if data.len == 1 and data[0] == 0xFF:
    return NoneValue
  decodeValueRecordBytes(data)

proc readEventsV4(reader: var TraceReader): Result[void, string] =
  ## Read a v4 (multi-stream) trace and reconstruct a chronological
  ## seq[TraceLowLevelEvent] that matches the v3 reader API surface.
  ##
  ## Event ordering produced:
  ##   1. tlePath for each interned path (in interning order).
  ##   2. tleFunction for each interned function (in interning order).
  ##      v4 only stores function names — pathId/line are emitted as 0.
  ##   3. For each step n in [0, stepCount):
  ##        a. If n is the entryStep of one or more pending call records,
  ##           emit tleCall (outer-first / shallower depth first, matching
  ##           v3 chronological call-entry order).
  ##        b. tleStep for the step itself (decoded from execution stream).
  ##        c. tleValue for each variable value attached to this step
  ##           (decoded from value stream, CBOR ValueRecord blobs).
  ##        d. If n is the exitStep of any pending call records,
  ##           emit tleReturn (innermost-first / LIFO — deeper exits first).
  ##   4. tleEvent for each IO event (in stream order).
  let trRes = openNewTraceFromBytes(reader.ctfsData, reader.blockSize,
                                    reader.maxRootEntries)
  if trRes.isErr:
    return err("failed to open v4 trace: " & trRes.error)
  var nr = trRes.get()

  # 1. tlePath for each interned path.
  for i in 0 ..< int(nr.pathCount()):
    let p = nr.path(uint64(i))
    if p.isOk:
      reader.events.add(TraceLowLevelEvent(kind: tlePath, path: p.get()))

  # 2. tleFunction for each interned function name.
  for i in 0 ..< int(nr.functionCount()):
    let fname = nr.function(uint64(i))
    if fname.isOk:
      reader.events.add(TraceLowLevelEvent(
        kind: tleFunction,
        functionRecord: FunctionRecord(
          pathId: PathId(0),
          line: Line(0),
          name: fname.get())))

  # Pre-load all call records so we can find entry/exit step boundaries.
  var calls: seq[v4_call_stream.CallRecord]
  let callCountRes = nr.callCount()
  if callCountRes.isOk:
    let total = callCountRes.get()
    for i in 0 ..< int(total):
      let cr = nr.call(uint64(i))
      if cr.isOk:
        calls.add(cr.get())

  # Build a map from entryStep → list of call-record indices (sorted by depth
  # so outer/shallower calls emit first, matching call-stack order).
  let stepCountRes = nr.stepCount()
  if stepCountRes.isErr:
    return err("failed to read step count: " & stepCountRes.error)
  let totalSteps = stepCountRes.get()

  # Build a GLI matching the writer (DefaultLinesPerFile per file).
  # IMPORTANT: this MUST stay in lock-step with
  # `codetracer_trace_writer/multi_stream_writer.DefaultLinesPerFile`.
  # If a future writer revision changes the assumed density (or starts
  # writing per-file true line counts into the trace), this reader will
  # silently misinterpret the (fileId, line) of every step. When that
  # happens the writer should expose the counts via trace metadata and
  # this code should read them back instead of assuming a constant.
  const DefaultLinesPerFile: uint64 = 100_000
  var lineCounts = newSeq[uint64](reader.paths.len)
  for i in 0 ..< reader.paths.len:
    lineCounts[i] = DefaultLinesPerFile
  let gli = buildGlobalLineIndex(lineCounts)

  # P6.5 / Piece B — column-tracking cursor.
  #
  # When the trace is column-aware (``meta.hasColumnAwareSteps``) we
  # surface a running ``column`` slot on every emitted ``StepRecord``.
  # The cursor is a tiny state machine fed by each step event:
  #
  #   * ``sekAbsoluteStep(G)`` — decode G via the per-file line-length
  #     tables (when populated) to land on (line, column).  Without
  #     per-line data the column slot is unknown; default to 1 — the
  #     spec's "column = 1 means the start of the line" convention
  #     covers the common "recorder didn't fill in lineLengths yet"
  #     case.
  #   * ``sekDeltaStep(D)`` — D is a delta over ``global_position_index``.
  #     In the current writer (which still encodes line-only GLIs even
  #     in column-aware mode) D ≠ 0 always crosses a line, so per spec
  #     "DeltaStep that crosses a line boundary resets column to 1".
  #     A future column-aware writer that emits within-line position
  #     deltas SHOULD use ``sekDeltaColumn`` for those, keeping this
  #     branch's reset semantics correct.
  #   * ``sekDeltaColumn(D)`` — column += D, line unchanged.  Spec
  #     §"Column Encoding — DeltaColumn (chosen)".
  #
  # Subtle cases the reviewer can spot-check:
  #
  #   * ``DeltaColumn`` after ``DeltaStep`` with ``lineDelta = 0`` (rare;
  #     current writer can't emit it because every ``registerStep`` call
  #     bumps GLI by at least 1).  In that case the cursor would skip
  #     the reset and keep the previous column, then apply the column
  #     delta — matches spec.
  #   * ``AbsoluteStep`` with ``column = 0`` per spec is illegal (columns
  #     are 1-based) — we never emit it because
  #     ``decodeGlobalPositionIndex`` produces 1-based columns by
  #     construction.  If a future writer encodes a position past
  #     ``sum(line_lengths)`` for a file, ``decodeGlobalPositionIndex``
  #     errors and we fall back to ``column = 1``.
  #   * The cursor only fires when ``meta.hasColumnAwareSteps`` is true.
  #     For legacy traces every ``StepRecord.hasColumn`` stays ``false``
  #     and the emitted events are byte-for-byte identical to the
  #     pre-P6.5 output.
  let columnAware = nr.meta.hasColumnAwareSteps
  var cursorColumn: uint32 = 1
  var cursorHasColumn = false

  # 3. Walk steps, weaving Call/Return events at entry/exit boundaries.
  for n in 0'u64 ..< totalSteps:
    # 3a. Calls entering at this step (outer→inner = shallower depth first).
    var entering: seq[int]
    for ci in 0 ..< calls.len:
      if calls[ci].entryStep == n:
        entering.add(ci)
    # Sort by depth ascending so outer call emits first.
    for i in 0 ..< entering.len:
      for j in i + 1 ..< entering.len:
        if calls[entering[j]].depth < calls[entering[i]].depth:
          let tmp = entering[i]
          entering[i] = entering[j]
          entering[j] = tmp
    for ci in entering:
      let c = calls[ci]
      var args = newSeq[FullValueRecord](c.args.len)
      for ai in 0 ..< c.args.len:
        args[ai] = FullValueRecord(
          variableId: VariableId(c.args[ai].varnameId),
          value: decodeValueRecordBytes(c.args[ai].value))
      reader.events.add(TraceLowLevelEvent(
        kind: tleCall,
        callRecord: codetracer_trace_types.CallRecord(
          functionId: FunctionId(c.functionId),
          args: args)))

    # 3b. tleStep — resolve GLI back to (pathId, line) where possible.
    let stepRes = nr.step(n)
    if stepRes.isErr:
      return err("failed to read step " & $n & ": " & stepRes.error)
    let stepEv = stepRes.get()
    case stepEv.kind
    of sekAbsoluteStep, sekDeltaStep, sekDeltaColumn:
      # P6.5: update the column cursor.  See the long comment above
      # the step-walk loop for the state machine's spec basis.
      #
      # In column-aware mode the on-wire ``global_position_index`` is a
      # byte-offset (sum of preceding line_lengths) — NOT a line count.
      # The line-only ``gli.resolve`` does line-count-based prefix-sum
      # search and therefore produces nonsense values on column-aware
      # traces.  Use ``decodeGlobalPositionIndex`` to recover both line
      # and column in one O(log F) + O(log L) pass; the per-file
      # line-length tables it consults are populated from paths.dat at
      # construction time.
      var resolvedLine: int64 = 0
      var resolvedPathId: uint64 = 0
      var haveResolvedLine = false

      if columnAware:
        case stepEv.kind
        of sekAbsoluteStep:
          let absGli = nr.stepAbsoluteGlobalLineIndex(n)
          if absGli.isOk:
            let posRes = nr.decodeGlobalPositionIndex(absGli.get())
            if posRes.isOk:
              cursorColumn = posRes.get().column
              resolvedPathId = posRes.get().file
              resolvedLine = int64(posRes.get().line)
              haveResolvedLine = true
            else:
              # No per-line data — column slot defaults to column 1.
              cursorColumn = 1
          else:
            cursorColumn = 1
          cursorHasColumn = true
        of sekDeltaStep:
          # Line transition resets column to 1 (spec §"Column Encoding
          # — DeltaColumn (chosen)").
          if stepEv.lineDelta != 0:
            cursorColumn = 1
          cursorHasColumn = true
          let absGli = nr.stepAbsoluteGlobalLineIndex(n)
          if absGli.isOk:
            let posRes = nr.decodeGlobalPositionIndex(absGli.get())
            if posRes.isOk:
              resolvedPathId = posRes.get().file
              resolvedLine = int64(posRes.get().line)
              haveResolvedLine = true
        of sekDeltaColumn:
          let nextCol = int64(cursorColumn) + stepEv.columnDelta
          if nextCol < 1:
            cursorColumn = 1
          else:
            cursorColumn = uint32(nextCol)
          cursorHasColumn = true
          let absGli = nr.stepAbsoluteGlobalLineIndex(n)
          if absGli.isOk:
            let posRes = nr.decodeGlobalPositionIndex(absGli.get())
            if posRes.isOk:
              resolvedPathId = posRes.get().file
              resolvedLine = int64(posRes.get().line)
              haveResolvedLine = true
        else:
          discard

      # sekDeltaColumn: column-aware traces only (tag 0x07).  At the v3
      # legacy projection layer we surface column motion as a tleStep at
      # the running (path, line); the column slot is populated above
      # when the trace is column-aware.
      if haveResolvedLine and reader.paths.len > 0:
        var rec = StepRecord(pathId: PathId(resolvedPathId),
                             line: Line(resolvedLine))
        if cursorHasColumn:
          rec.hasColumn = true
          rec.column = Line(int64(cursorColumn))
        reader.events.add(TraceLowLevelEvent(kind: tleStep, step: rec))
      else:
        let absGli = nr.stepAbsoluteGlobalLineIndex(n)
        if absGli.isOk and reader.paths.len > 0:
          let (fileId, line) = gli.resolve(absGli.get())
          var rec = StepRecord(pathId: PathId(uint64(fileId)),
                               line: Line(int64(line)))
          if cursorHasColumn:
            rec.hasColumn = true
            rec.column = Line(int64(cursorColumn))
          reader.events.add(TraceLowLevelEvent(kind: tleStep, step: rec))
        else:
          var rec = StepRecord(pathId: PathId(0), line: Line(0))
          if cursorHasColumn:
            rec.hasColumn = true
            rec.column = Line(int64(cursorColumn))
          reader.events.add(TraceLowLevelEvent(kind: tleStep, step: rec))
    of sekRaise, sekCatch:
      # No direct v3 mapping for raise/catch — skip (or could synthesize
      # a tleEvent).  v3 didn't expose these via tleStep either.
      discard
    of sekThreadStart:
      reader.events.add(TraceLowLevelEvent(
        kind: tleThreadStart,
        threadStartId: ThreadId(stepEv.startThreadId)))
    of sekThreadExit:
      reader.events.add(TraceLowLevelEvent(
        kind: tleThreadExit,
        threadExitId: ThreadId(stepEv.exitThreadId)))
    of sekThreadSwitch:
      reader.events.add(TraceLowLevelEvent(
        kind: tleThreadSwitch,
        threadSwitchId: ThreadId(stepEv.threadId)))

    # 3c. tleValue for each attached variable value.
    let valsRes = nr.values(n)
    if valsRes.isOk:
      for vv in valsRes.get():
        reader.events.add(TraceLowLevelEvent(
          kind: tleValue,
          fullValue: FullValueRecord(
            variableId: VariableId(vv.varnameId),
            value: decodeValueRecordBytes(vv.data))))

    # 3d. Returns exiting at this step (LIFO — innermost / deepest first).
    var exiting: seq[int]
    for ci in 0 ..< calls.len:
      if calls[ci].exitStep == n:
        exiting.add(ci)
    for i in 0 ..< exiting.len:
      for j in i + 1 ..< exiting.len:
        if calls[exiting[j]].depth > calls[exiting[i]].depth:
          let tmp = exiting[i]
          exiting[i] = exiting[j]
          exiting[j] = tmp
    for ci in exiting:
      let c = calls[ci]
      reader.events.add(TraceLowLevelEvent(
        kind: tleReturn,
        returnRecord: ReturnRecord(
          returnValue: decodeReturnValueBytes(c.returnValue))))

  # 4. IO events.
  let ioCountRes = nr.ioEventCount()
  if ioCountRes.isOk:
    let total = ioCountRes.get()
    for i in 0 ..< int(total):
      let evRes = nr.ioEvent(uint64(i))
      if evRes.isOk:
        let io = evRes.get()
        let kind =
          case io.kind
          of ioStdout, ioStderr, ioFileOp: elkWrite
          of ioError: elkError
        var content = newString(io.data.len)
        for j in 0 ..< io.data.len:
          content[j] = char(io.data[j])
        reader.events.add(TraceLowLevelEvent(
          kind: tleEvent,
          recordEvent: RecordEvent(
            kind: kind,
            metadata: "",
            content: content)))

  reader.eventCount = reader.events.len
  ok()

proc readEvents*(reader: var TraceReader): Result[void, string] =
  ## Decompress and decode all events. Dispatches to v3 (events.log) or
  ## v4 (multi-stream) based on the file layout detected at openTrace time.
  if reader.isV4:
    return readEventsV4(reader)

  let eventsRes = readInternalFile(reader.ctfsData, "events.log",
                                    reader.blockSize, reader.maxRootEntries)
  if eventsRes.isErr:
    return err("failed to read events.log: " & eventsRes.error)

  let eventsData = eventsRes.get()
  if eventsData.len == 0:
    reader.events = @[]
    reader.eventCount = 0
    return ok()

  # Decode all chunks: [16-byte header][compressed data]...
  var pos = 0
  while pos + ChunkIndexEntrySize <= eventsData.len:
    let chunk = decodeChunkHeader(eventsData, pos)
    if chunk.compressedSize == 0:
      break
    pos += ChunkIndexEntrySize

    if pos + int(chunk.compressedSize) > eventsData.len:
      return err("chunk compressed data extends beyond events.log")

    # Decompress the chunk
    let compressedSlice = eventsData[pos ..< pos + int(chunk.compressedSize)]
    let decompSize = ZSTD_getFrameContentSize(
      unsafeAddr compressedSlice[0], csize_t(compressedSlice.len))

    if decompSize == ZSTD_CONTENTSIZE_ERROR:
      return err("failed to get decompressed size for chunk")

    var decompressed = newSeq[byte](int(decompSize))
    if decompSize > 0:
      let actualSize = ZSTD_decompress(
        addr decompressed[0], csize_t(decompressed.len),
        unsafeAddr compressedSlice[0], csize_t(compressedSlice.len))
      if ZSTD_isError(actualSize) != 0:
        return err("zstd decompression failed")
      decompressed.setLen(int(actualSize))

    # Decode split-binary events
    let decoded = decodeAllEvents(decompressed)
    if decoded.isErr:
      return err("failed to decode events: " & decoded.unsafeError)
    for event in decoded.get():
      reader.events.add(event)

    pos += int(chunk.compressedSize)

  reader.eventCount = reader.events.len
  ok()

# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------

proc valueRecordToJson(v: ValueRecord): JsonNode {.raises: [].}

proc valueRecordToJson(v: ValueRecord): JsonNode =
  result = newJObject()
  case v.kind
  of vrkInt:
    result["kind"] = newJString("Int")
    result["i"] = newJInt(v.intVal)
    result["type_id"] = newJInt(int64(uint64(v.intTypeId)))
  of vrkFloat:
    result["kind"] = newJString("Float")
    result["f"] = newJFloat(v.floatVal)
    result["type_id"] = newJInt(int64(uint64(v.floatTypeId)))
  of vrkBool:
    result["kind"] = newJString("Bool")
    result["b"] = newJBool(v.boolVal)
    # `text` carries the printed boolean ("true"|"false"), matching the
    # 4-key CBOR map produced by writeBool / encodeCborValueRecord.
    result["text"] = newJString(if v.boolVal: "true" else: "false")
    result["type_id"] = newJInt(int64(uint64(v.boolTypeId)))
  of vrkString:
    result["kind"] = newJString("String")
    result["text"] = newJString(v.text)
    result["type_id"] = newJInt(int64(uint64(v.strTypeId)))
  of vrkSequence:
    result["kind"] = newJString("Sequence")
    var elems = newJArray()
    for e in v.seqElements:
      elems.add(valueRecordToJson(e))
    result["elements"] = elems
    result["is_slice"] = newJBool(v.isSlice)
    result["type_id"] = newJInt(int64(uint64(v.seqTypeId)))
  of vrkTuple:
    result["kind"] = newJString("Tuple")
    var elems = newJArray()
    for e in v.tupleElements:
      elems.add(valueRecordToJson(e))
    result["elements"] = elems
    result["type_id"] = newJInt(int64(uint64(v.tupleTypeId)))
  of vrkStruct:
    result["kind"] = newJString("Struct")
    var fields = newJArray()
    for e in v.fieldValues:
      fields.add(valueRecordToJson(e))
    result["field_values"] = fields
    if v.fieldNames.len > 0 and v.fieldNames.len == v.fieldValues.len:
      var pairs = newJArray()
      for i in 0 ..< v.fieldValues.len:
        var pair = newJArray()
        pair.add(newJString(v.fieldNames[i]))
        pair.add(valueRecordToJson(v.fieldValues[i]))
        pairs.add(pair)
      result["fields"] = pairs
    result["type_id"] = newJInt(int64(uint64(v.structTypeId)))
  of vrkVariant:
    result["kind"] = newJString("Variant")
    result["discriminator"] = newJString(v.discriminator)
    if v.contents.len > 0:
      result["contents"] = valueRecordToJson(v.contents[0])
    else:
      result["contents"] = newJNull()
    result["type_id"] = newJInt(int64(uint64(v.variantTypeId)))
  of vrkReference:
    result["kind"] = newJString("Reference")
    if v.dereferenced.len > 0:
      result["dereferenced"] = valueRecordToJson(v.dereferenced[0])
    else:
      result["dereferenced"] = newJNull()
    result["address"] = newJInt(int64(v.address))
    result["mutable"] = newJBool(v.mutable)
    result["type_id"] = newJInt(int64(uint64(v.refTypeId)))
  of vrkRaw:
    result["kind"] = newJString("Raw")
    result["r"] = newJString(v.rawStr)
    result["type_id"] = newJInt(int64(uint64(v.rawTypeId)))
  of vrkError:
    result["kind"] = newJString("Error")
    result["msg"] = newJString(v.errorMsg)
    result["type_id"] = newJInt(int64(uint64(v.errorTypeId)))
  of vrkNone:
    result["kind"] = newJString("None")
    result["type_id"] = newJInt(int64(uint64(v.noneTypeId)))
  of vrkCell:
    result["kind"] = newJString("Cell")
    result["place"] = newJInt(int64(v.cellPlace))
  of vrkBigInt:
    result["kind"] = newJString("BigInt")
    result["negative"] = newJBool(v.negative)
    result["type_id"] = newJInt(int64(uint64(v.bigIntTypeId)))
  of vrkChar:
    result["kind"] = newJString("Char")
    result["c"] = newJString($v.charVal)
    result["type_id"] = newJInt(int64(uint64(v.charTypeId)))
  of vrkValueRef:
    result["kind"] = newJString("ValueRef")
    result["ref_id"] = newJInt(int64(v.refId))
  of vrkSet:
    result["kind"] = newJString("Set")
    var members = newJArray()
    for e in v.setMembers:
      members.add(valueRecordToJson(e))
    result["members"] = members
    result["type_id"] = newJInt(int64(uint64(v.setTypeId)))
  of vrkEnum:
    result["kind"] = newJString("Enum")
    result["name"] = newJString(v.enumName)
    result["ordinal"] = newJInt(v.enumOrdinal)
    result["type_id"] = newJInt(int64(uint64(v.enumTypeId)))

proc eventToJson(event: TraceLowLevelEvent): JsonNode =
  result = newJObject()
  case event.kind
  of tleStep:
    result["type"] = newJString("Step")
    result["path_id"] = newJInt(int64(uint64(event.step.pathId)))
    result["line"] = newJInt(int64(event.step.line))
    # P1.4: surface the column on column-aware traces so JSON-events
    # consumers can observe the per-step column without re-decoding the
    # global position index themselves.  Legacy (non-column-aware)
    # traces have ``hasColumn = false`` and the field is omitted to
    # keep the JSON output bit-for-bit compatible with pre-extension
    # readers.
    if event.step.hasColumn:
      result["column"] = newJInt(int64(event.step.column))
  of tlePath:
    result["type"] = newJString("Path")
    result["name"] = newJString(event.path)
  of tleVariableName:
    result["type"] = newJString("VariableName")
    result["name"] = newJString(event.varName)
  of tleVariable:
    result["type"] = newJString("Variable")
    result["name"] = newJString(event.variable)
  of tleType:
    result["type"] = newJString("Type")
    result["kind"] = newJString($event.typeRecord.kind)
    result["lang_type"] = newJString(event.typeRecord.langType)
  of tleValue:
    result["type"] = newJString("Value")
    result["variable_id"] = newJInt(int64(uint64(event.fullValue.variableId)))
    result["value"] = valueRecordToJson(event.fullValue.value)
  of tleFunction:
    result["type"] = newJString("Function")
    result["path_id"] = newJInt(int64(uint64(event.functionRecord.pathId)))
    result["line"] = newJInt(int64(event.functionRecord.line))
    result["name"] = newJString(event.functionRecord.name)
  of tleCall:
    result["type"] = newJString("Call")
    result["function_id"] = newJInt(int64(uint64(event.callRecord.functionId)))
    var args = newJArray()
    for arg in event.callRecord.args:
      var argNode = newJObject()
      argNode["variable_id"] = newJInt(int64(uint64(arg.variableId)))
      argNode["value"] = valueRecordToJson(arg.value)
      args.add(argNode)
    result["args"] = args
  of tleReturn:
    result["type"] = newJString("Return")
    result["value"] = valueRecordToJson(event.returnRecord.returnValue)
  of tleEvent:
    result["type"] = newJString("Event")
    result["event_kind"] = newJString($event.recordEvent.kind)
    result["metadata"] = newJString(event.recordEvent.metadata)
    result["content"] = newJString(event.recordEvent.content)
  of tleAsm:
    result["type"] = newJString("Asm")
    var lines = newJArray()
    for line in event.asmLines:
      lines.add(newJString(line))
    result["lines"] = lines
  of tleBindVariable:
    result["type"] = newJString("BindVariable")
    result["variable_id"] = newJInt(int64(uint64(event.bindVar.variableId)))
    result["place"] = newJInt(int64(event.bindVar.place))
  of tleAssignment:
    result["type"] = newJString("Assignment")
    result["to"] = newJInt(int64(uint64(event.assignment.to)))
    result["pass_by"] = newJString($event.assignment.passBy)
  of tleDropVariables:
    result["type"] = newJString("DropVariables")
    var ids = newJArray()
    for id in event.dropVarIds:
      ids.add(newJInt(int64(uint64(id))))
    result["ids"] = ids
  of tleCompoundValue:
    result["type"] = newJString("CompoundValue")
    result["place"] = newJInt(int64(event.compoundValue.place))
    result["value"] = valueRecordToJson(event.compoundValue.value)
  of tleCellValue:
    result["type"] = newJString("CellValue")
    result["place"] = newJInt(int64(event.cellValue.place))
    result["value"] = valueRecordToJson(event.cellValue.value)
  of tleAssignCompoundItem:
    result["type"] = newJString("AssignCompoundItem")
    result["place"] = newJInt(int64(event.assignCompoundItem.place))
    result["index"] = newJInt(int64(event.assignCompoundItem.index))
    result["item_place"] = newJInt(int64(event.assignCompoundItem.itemPlace))
  of tleAssignCell:
    result["type"] = newJString("AssignCell")
    result["place"] = newJInt(int64(event.assignCell.place))
    result["value"] = valueRecordToJson(event.assignCell.newValue)
  of tleVariableCell:
    result["type"] = newJString("VariableCell")
    result["variable_id"] = newJInt(int64(uint64(event.variableCell.variableId)))
    result["place"] = newJInt(int64(event.variableCell.place))
  of tleDropVariable:
    result["type"] = newJString("DropVariable")
    result["variable_id"] = newJInt(int64(uint64(event.dropVarId)))
  of tleThreadStart:
    result["type"] = newJString("ThreadStart")
    result["thread_id"] = newJInt(int64(uint64(event.threadStartId)))
  of tleThreadExit:
    result["type"] = newJString("ThreadExit")
    result["thread_id"] = newJInt(int64(uint64(event.threadExitId)))
  of tleThreadSwitch:
    result["type"] = newJString("ThreadSwitch")
    result["thread_id"] = newJInt(int64(uint64(event.threadSwitchId)))
  of tleDropLastStep:
    result["type"] = newJString("DropLastStep")

proc toJson*(reader: TraceReader): string =
  ## Serialize the entire trace to JSON (metadata + paths + events).
  var root = newJObject()

  # Metadata
  var meta = newJObject()
  meta["program"] = newJString(reader.metadata.program)
  var argsArr = newJArray()
  for arg in reader.metadata.args:
    argsArr.add(newJString(arg))
  meta["args"] = argsArr
  meta["workdir"] = newJString(reader.metadata.workdir)
  root["metadata"] = meta

  # Paths
  var pathsArr = newJArray()
  for p in reader.paths:
    pathsArr.add(newJString(p))
  root["paths"] = pathsArr

  # Events
  var eventsArr = newJArray()
  for event in reader.events:
    eventsArr.add(eventToJson(event))
  root["events"] = eventsArr

  try:
    result = pretty(root)
  except ValueError:
    result = $root

proc toJsonEvents*(reader: TraceReader): string =
  ## Serialize just the events to JSON array.
  var eventsArr = newJArray()
  for event in reader.events:
    eventsArr.add(eventToJson(event))
  try:
    result = pretty(eventsArr)
  except ValueError:
    result = $eventsArr

# ---------------------------------------------------------------------------
# Pretty text output
# ---------------------------------------------------------------------------

proc escapeStr(s: string): string =
  result = "\""
  for c in s:
    case c
    of '\n': result.add("\\n")
    of '\r': result.add("\\r")
    of '\t': result.add("\\t")
    of '\\': result.add("\\\\")
    of '"': result.add("\\\"")
    else:
      if ord(c) < 32:
        result.add("\\x" & toHex(ord(c), 2).toLowerAscii())
      else:
        result.add(c)
  result.add("\"")

proc prettyPrintEvent(event: TraceLowLevelEvent, index: int): string =
  let prefix = "  event[" & $index & "]: "
  case event.kind
  of tleStep:
    prefix & "Step path_id=" & $uint64(event.step.pathId) &
      " line=" & $int64(event.step.line)
  of tlePath:
    prefix & "Path " & escapeStr(event.path)
  of tleVariableName:
    prefix & "VariableName " & escapeStr(event.varName)
  of tleVariable:
    prefix & "Variable " & escapeStr(event.variable)
  of tleType:
    prefix & "Type kind=" & $event.typeRecord.kind &
      " lang_type=" & escapeStr(event.typeRecord.langType)
  of tleValue:
    var s = prefix & "Value variable_id=" & $uint64(event.fullValue.variableId)
    case event.fullValue.value.kind
    of vrkInt:
      s &= " kind=Int i=" & $event.fullValue.value.intVal &
        " type_id=" & $uint64(event.fullValue.value.intTypeId)
    of vrkFloat:
      s &= " kind=Float f=" & $event.fullValue.value.floatVal &
        " type_id=" & $uint64(event.fullValue.value.floatTypeId)
    of vrkBool:
      s &= " kind=Bool b=" & $event.fullValue.value.boolVal &
        " type_id=" & $uint64(event.fullValue.value.boolTypeId)
    of vrkString:
      s &= " kind=String text=" & escapeStr(event.fullValue.value.text) &
        " type_id=" & $uint64(event.fullValue.value.strTypeId)
    of vrkNone:
      s &= " kind=None type_id=" & $uint64(event.fullValue.value.noneTypeId)
    of vrkChar:
      s &= " kind=Char c=" & escapeStr($event.fullValue.value.charVal) &
        " type_id=" & $uint64(event.fullValue.value.charTypeId)
    of vrkRaw:
      s &= " kind=Raw text=" & escapeStr(event.fullValue.value.rawStr) &
        " type_id=" & $uint64(event.fullValue.value.rawTypeId)
    of vrkError:
      s &= " kind=Error msg=" & escapeStr(event.fullValue.value.errorMsg) &
        " type_id=" & $uint64(event.fullValue.value.errorTypeId)
    of vrkCell:
      s &= " kind=Cell place=" & $int64(event.fullValue.value.cellPlace)
    of vrkBigInt:
      s &= " kind=BigInt negative=" & $event.fullValue.value.negative &
        " type_id=" & $uint64(event.fullValue.value.bigIntTypeId)
    else:
      s &= " kind=" & $event.fullValue.value.kind
    s
  of tleFunction:
    prefix & "Function path_id=" & $uint64(event.functionRecord.pathId) &
      " line=" & $int64(event.functionRecord.line) &
      " name=" & escapeStr(event.functionRecord.name)
  of tleCall:
    prefix & "Call function_id=" & $uint64(event.callRecord.functionId) &
      " args_count=" & $event.callRecord.args.len
  of tleReturn:
    var s = prefix & "Return"
    case event.returnRecord.returnValue.kind
    of vrkInt:
      s &= " kind=Int i=" & $event.returnRecord.returnValue.intVal &
        " type_id=" & $uint64(event.returnRecord.returnValue.intTypeId)
    of vrkNone:
      s &= " kind=None"
    of vrkFloat:
      s &= " kind=Float f=" & $event.returnRecord.returnValue.floatVal
    of vrkBool:
      s &= " kind=Bool b=" & $event.returnRecord.returnValue.boolVal
    of vrkString:
      s &= " kind=String text=" & escapeStr(event.returnRecord.returnValue.text)
    else:
      s &= " kind=" & $event.returnRecord.returnValue.kind
    s
  of tleEvent:
    prefix & "Event kind=" & $event.recordEvent.kind &
      " metadata=" & escapeStr(event.recordEvent.metadata) &
      " content=" & escapeStr(event.recordEvent.content)
  of tleAsm:
    prefix & "Asm lines=" & $event.asmLines.len
  of tleBindVariable:
    prefix & "BindVariable variable_id=" & $uint64(event.bindVar.variableId) &
      " place=" & $int64(event.bindVar.place)
  of tleAssignment:
    prefix & "Assignment to=" & $uint64(event.assignment.to) &
      " pass_by=" & $event.assignment.passBy
  of tleDropVariables:
    prefix & "DropVariables count=" & $event.dropVarIds.len
  of tleCompoundValue:
    prefix & "CompoundValue place=" & $int64(event.compoundValue.place)
  of tleCellValue:
    prefix & "CellValue place=" & $int64(event.cellValue.place)
  of tleAssignCompoundItem:
    prefix & "AssignCompoundItem place=" & $int64(event.assignCompoundItem.place) &
      " index=" & $event.assignCompoundItem.index
  of tleAssignCell:
    prefix & "AssignCell place=" & $int64(event.assignCell.place)
  of tleVariableCell:
    prefix & "VariableCell variable_id=" & $uint64(event.variableCell.variableId) &
      " place=" & $int64(event.variableCell.place)
  of tleDropVariable:
    prefix & "DropVariable variable_id=" & $uint64(event.dropVarId)
  of tleThreadStart:
    prefix & "ThreadStart id=" & $uint64(event.threadStartId)
  of tleThreadExit:
    prefix & "ThreadExit id=" & $uint64(event.threadExitId)
  of tleThreadSwitch:
    prefix & "ThreadSwitch id=" & $uint64(event.threadSwitchId)
  of tleDropLastStep:
    prefix & "DropLastStep"

proc toPrettyText*(reader: TraceReader): string =
  ## Human-readable text format.
  var lines: seq[string]
  lines.add("=== Trace ===")
  lines.add("program: " & reader.metadata.program)
  if reader.metadata.args.len > 0:
    lines.add("args: " & reader.metadata.args.join(" "))
  if reader.metadata.workdir.len > 0:
    lines.add("workdir: " & reader.metadata.workdir)
  lines.add("paths: " & $reader.paths.len)
  lines.add("events: " & $reader.eventCount)
  lines.add("")
  for i, event in reader.events:
    lines.add(prettyPrintEvent(event, i))
  result = lines.join("\n") & "\n"

proc toSummary*(reader: TraceReader): string =
  ## Print metadata and event counts only.
  var lines: seq[string]
  lines.add("program: " & reader.metadata.program)
  if reader.metadata.args.len > 0:
    lines.add("args: " & reader.metadata.args.join(" "))
  if reader.metadata.workdir.len > 0:
    lines.add("workdir: " & reader.metadata.workdir)
  lines.add("paths: " & $reader.paths.len)
  lines.add("events: " & $reader.eventCount)

  # Count events by type
  var stepCount = 0
  var pathCount = 0
  var functionCount = 0
  var callCount = 0
  var returnCount = 0
  var valueCount = 0
  var otherCount = 0
  for event in reader.events:
    case event.kind
    of tleStep: stepCount += 1
    of tlePath: pathCount += 1
    of tleFunction: functionCount += 1
    of tleCall: callCount += 1
    of tleReturn: returnCount += 1
    of tleValue: valueCount += 1
    else: otherCount += 1

  lines.add("")
  lines.add("breakdown:")
  if stepCount > 0: lines.add("  steps: " & $stepCount)
  if pathCount > 0: lines.add("  paths: " & $pathCount)
  if functionCount > 0: lines.add("  functions: " & $functionCount)
  if callCount > 0: lines.add("  calls: " & $callCount)
  if returnCount > 0: lines.add("  returns: " & $returnCount)
  if valueCount > 0: lines.add("  values: " & $valueCount)
  if otherCount > 0: lines.add("  other: " & $otherCount)

  result = lines.join("\n") & "\n"
