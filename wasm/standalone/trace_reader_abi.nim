## The read-side C ABI of the freestanding reader probe, `include`d by both
## `trace_reader_only_standalone.nim` (reader alone) and
## `trace_reader_standalone.nim` (reader plus the writer that feeds the
## in-module round trip), so the two modules answer host queries with one
## implementation and their sizes differ only by what they actually link.
##
## The scalar and string queries are funnelled through `ct_num` / `ct_str`
## rather than given one export each. A wasm export is a name the host has to
## know either way, and a table of kinds keeps the module's export list — which
## IS its interface — small enough to read.

var
  input: seq[byte]
    ## The container the HOST hands in, through `ct_input_alloc`.
  reader: NewTraceReader
  readerOpen: bool
  spanReader: SpanStreamReader
  spanReaderOpen: bool
  spanSettled: seq[SpanRecord]
  spanTypes: seq[SpanTypeEntry]
  linehits: LinehitsReader
  linehitsOpen: bool
  strBuf: seq[byte]
    ## Landing pad for a string result, so the host can read it out of memory.

proc nimMain() {.importc: "NimMain", cdecl.}

proc ctInit() {.exportc: "ct_init", cdecl.} =
  ## `--noMain` means the host runs module-level initialisation itself.
  nimMain()

# ---------------------------------------------------------------------------
# Reading bytes the host supplies
# ---------------------------------------------------------------------------

proc ctInputAlloc(n: int32): pointer {.exportc: "ct_input_alloc", cdecl.} =
  ## Reserve `n` writable bytes for the host to copy a container into, and
  ## return their address in linear memory.
  if n <= 0:
    input = @[]
    return nil
  input = newSeq[byte](int(n))
  addr input[0]

proc ctInputLen(): int32 {.exportc: "ct_input_len", cdecl.} =
  int32(input.len)

proc ctVerifyInput(): int32 {.exportc: "ct_verify_input", cdecl.} =
  ## Run the corpus verifier over the host-supplied bytes. 0 means every
  ## decoded field matched; anything else names the first check that failed.
  verifyCorpus(input)

proc ctVerifyLegacyInput(): int32 {.exportc: "ct_verify_legacy_input",
    cdecl.} =
  ## The same, for a LEGACY-framed v4 container: no `events.log`, and a
  ## meta.dat that leaves the three stream bits clear so the reader picks the
  ## pre-M24a decoders.
  verifyLegacyCorpus(input)

proc ctProbeMisframed(): int32 {.exportc: "ct_probe_misframed", cdecl.} =
  ## Read legacy-framed bytes whose meta.dat claims the SPEC framing, and
  ## report what came back as a bit set. See `probeMisframedLegacy`.
  probeMisframedLegacy(input)

# ---------------------------------------------------------------------------
# Queries, so the host can check the decode rather than trust a return code
# ---------------------------------------------------------------------------

proc ctOpenInput(): int32 {.exportc: "ct_open_input", cdecl.} =
  ## Open the host-supplied bytes as a trace. 0 on success.
  ##
  ## The span stream and the line-hit index are separate files with separate
  ## readers, so they are opened here too — and their absence is NOT an error:
  ## a container that carries neither is the pre-extension shape.
  readerOpen = false
  spanReaderOpen = false
  linehitsOpen = false
  spanSettled = @[]
  spanTypes = @[]
  let rr = openNewTraceFromBytes(input)
  if rr.isErr: return 1
  reader = rr.get()
  readerOpen = true

  let sr = initSpanStreamReader(input)
  if sr.isOk:
    spanReader = sr.get()
    spanReaderOpen = true
    let settled = spanReader.settledSpans()
    if settled.isOk: spanSettled = settled.get()
    let ns = readSpanTypeNamespace(input)
    if ns.isOk: spanTypes = ns.get()

  let lh = initLinehitsReader(input)
  if lh.isOk:
    linehits = lh.get()
    linehitsOpen = true
  0

proc ctStepCount(): int64 {.exportc: "ct_step_count", cdecl.} =
  if not readerOpen: return -1
  let r = reader.stepCount()
  if r.isErr: -2 else: int64(r.get())

proc ctPathCount(): int64 {.exportc: "ct_path_count", cdecl.} =
  if not readerOpen: return -1
  int64(reader.pathCount())

proc ctFunctionCount(): int64 {.exportc: "ct_function_count", cdecl.} =
  if not readerOpen: return -1
  int64(reader.functionCount())

proc ctTypeCount(): int64 {.exportc: "ct_type_count", cdecl.} =
  if not readerOpen: return -1
  int64(reader.typeCount())

proc ctVarnameCount(): int64 {.exportc: "ct_varname_count", cdecl.} =
  if not readerOpen: return -1
  int64(reader.varnameCount())

proc ctCallCount(): int64 {.exportc: "ct_call_count", cdecl.} =
  if not readerOpen: return -1
  let r = reader.callCount()
  if r.isErr: -2 else: int64(r.get())

proc ctColumnAware(): int32 {.exportc: "ct_column_aware", cdecl.} =
  if not readerOpen: return -1
  if reader.meta.hasColumnAwareSteps: 1 else: 0

proc ctStepPosition(n: int64): int64 {.exportc: "ct_step_position", cdecl.} =
  ## The absolute `global_position_index` of step `n`.
  if not readerOpen: return -1
  let r = reader.stepAbsoluteGlobalLineIndex(uint64(n))
  if r.isErr: -2 else: int64(r.get())

proc ctPosFile(p: int64): int64 {.exportc: "ct_pos_file", cdecl.} =
  if not readerOpen: return -1
  let r = reader.decodeGlobalPositionIndex(uint64(p))
  if r.isErr: -2 else: int64(r.get().file)

proc ctPosLine(p: int64): int64 {.exportc: "ct_pos_line", cdecl.} =
  if not readerOpen: return -1
  let r = reader.decodeGlobalPositionIndex(uint64(p))
  if r.isErr: -2 else: int64(r.get().line)

proc ctPosColumn(p: int64): int64 {.exportc: "ct_pos_column", cdecl.} =
  if not readerOpen: return -1
  let r = reader.decodeGlobalPositionIndex(uint64(p))
  if r.isErr: -2 else: int64(r.get().column)

# ---------------------------------------------------------------------------
# Scalar queries
# ---------------------------------------------------------------------------
#
# `ct_num(kind, a, b)`. Every kind returns -1 for "nothing open" and -2 for
# "the reader refused" — distinct values, because a surface that FAILS and a
# surface that returns a wrong number are different findings and the host has
# to be able to tell them apart.

const
  NumSourceViewCount* = 0'i32
  NumSourceViewPath* = 1'i32
  NumSourceViewKind* = 2'i32
  NumSourceViewsForPathCount* = 3'i32
  NumSourceViewsForPathAt* = 4'i32
  NumIoEventCount* = 5'i32
  NumIoEventKind* = 6'i32
  NumIoEventStep* = 7'i32
  NumIoEventDecodes* = 8'i32
  NumSpanRecordCount* = 9'i32
  NumSpanSettledCount* = 10'i32
  NumSpanId* = 11'i32
  NumSpanParent* = 12'i32
  NumSpanIsOpen* = 13'i32
  NumSpanIsExternal* = 14'i32
  NumSpanStatus* = 15'i32
  NumSpanStartStep* = 16'i32
  NumSpanEndStep* = 17'i32
  NumSpanStructural* = 18'i32
  NumSpanTypeEntryCount* = 19'i32
  NumSpanTypeIdCount* = 20'i32
  NumSpanTypeIdAt* = 21'i32
  NumLinehitPositionCount* = 22'i32
  NumLinehitPresent* = 23'i32
  NumLinehitCount* = 24'i32
  NumLinehitAt* = 25'i32
  NumLinehitSum* = 26'i32
  NumValueCount* = 27'i32
  NumValueByte* = 28'i32
  NumHasSourceViewsFlag* = 29'i32
  NumHasSpanStreamFlag* = 30'i32
  NumHasStepStreamFlag* = 31'i32
  NumHasValueStreamFlag* = 32'i32
  NumHasIoStreamFlag* = 33'i32

proc ctNum(kind: int32, a: int64, b: int64): int64 {.exportc: "ct_num",
    cdecl.} =
  if not readerOpen: return -1
  case kind
  of NumSourceViewCount:
    int64(reader.sourceViewCount())
  of NumSourceViewPath:
    let r = reader.sourceView(uint64(a))
    if r.isErr: -2 else: int64(r.get().pathId)
  of NumSourceViewKind:
    let r = reader.sourceView(uint64(a))
    if r.isErr: -2 else: int64(r.get().viewKind)
  of NumSourceViewsForPathCount:
    int64(reader.sourceViewsForPath(uint64(a)).len)
  of NumSourceViewsForPathAt:
    let idx = reader.sourceViewsForPath(uint64(a))
    if b < 0 or b >= int64(idx.len): -2 else: int64(idx[int(b)])
  of NumIoEventCount:
    let r = reader.ioEventCount()
    if r.isErr: -2 else: int64(r.get())
  of NumIoEventKind:
    let r = reader.ioEvent(uint64(a))
    if r.isErr: -2 else: int64(ord(r.get().kind))
  of NumIoEventStep:
    let r = reader.ioEvent(uint64(a))
    if r.isErr: -2 else: int64(r.get().stepId)
  of NumIoEventDecodes:
    if reader.ioEvent(uint64(a)).isErr: 0'i64 else: 1'i64
  of NumSpanRecordCount:
    if not spanReaderOpen: -2 else: int64(spanReader.count())
  of NumSpanSettledCount:
    if not spanReaderOpen: -2 else: int64(spanSettled.len)
  of NumSpanId:
    if not spanReaderOpen or a < 0 or a >= int64(spanSettled.len): -2
    else: int64(spanSettled[int(a)].spanId)
  of NumSpanParent:
    if not spanReaderOpen or a < 0 or a >= int64(spanSettled.len): -2
    else: int64(spanSettled[int(a)].parentSpanId)
  of NumSpanIsOpen:
    if not spanReaderOpen or a < 0 or a >= int64(spanSettled.len): -2
    elif spanSettled[int(a)].isOpen: 1'i64 else: 0'i64
  of NumSpanIsExternal:
    if not spanReaderOpen or a < 0 or a >= int64(spanSettled.len): -2
    elif spanSettled[int(a)].isExternal: 1'i64 else: 0'i64
  of NumSpanStatus:
    if not spanReaderOpen or a < 0 or a >= int64(spanSettled.len): -2
    else: int64(ord(spanSettled[int(a)].status))
  of NumSpanStartStep:
    if not spanReaderOpen or a < 0 or a >= int64(spanSettled.len): -2
    else: int64(spanSettled[int(a)].startStep)
  of NumSpanEndStep:
    if not spanReaderOpen or a < 0 or a >= int64(spanSettled.len): -2
    else: int64(spanSettled[int(a)].endStep)
  of NumSpanStructural:
    # The three structural bits packed the way the wire byte packs them, so a
    # reader that dropped one is visible as a single number.
    if not spanReaderOpen or a < 0 or a >= int64(spanSettled.len): -2
    else:
      let s = spanSettled[int(a)]
      var v = 0'i64
      if s.contiguousOnOneThread: v = v or 1
      if s.sharesTimeline: v = v or 2
      if s.concurrentWithSiblings: v = v or 4
      v
  of NumSpanTypeEntryCount:
    if not spanReaderOpen: -2 else: int64(spanTypes.len)
  of NumSpanTypeIdCount:
    if not spanReaderOpen or a < 0 or a >= int64(spanTypes.len): -2
    else: int64(spanTypes[int(a)].spanIds.len)
  of NumSpanTypeIdAt:
    if not spanReaderOpen or a < 0 or a >= int64(spanTypes.len): -2
    elif b < 0 or b >= int64(spanTypes[int(a)].spanIds.len): -2
    else: int64(spanTypes[int(a)].spanIds[int(b)])
  of NumLinehitPositionCount:
    if not linehitsOpen: -2 else: int64(linehits.positionCount())
  of NumLinehitPresent:
    if not linehitsOpen: -2
    elif linehits.hits(uint64(a)).isOk: 1'i64 else: 0'i64
  of NumLinehitCount:
    if not linehitsOpen: return -2
    let r = linehits.hits(uint64(a))
    if r.isErr: -2 else: int64(r.get().len)
  of NumLinehitAt:
    if not linehitsOpen: return -2
    let r = linehits.hits(uint64(a))
    if r.isErr: return -2
    let hs = r.get()
    if b < 0 or b >= int64(hs.len): -2 else: int64(hs[int(b)])
  of NumLinehitSum:
    if not linehitsOpen: return -2
    let r = linehits.hits(uint64(a))
    if r.isErr: return -2
    var total = 0'i64
    for h in r.get(): total += int64(h)
    total
  of NumValueCount:
    let r = reader.values(uint64(a))
    if r.isErr: -2 else: int64(r.get().len)
  of NumValueByte:
    let r = reader.values(uint64(a))
    if r.isErr: return -2
    let vs = r.get()
    if vs.len == 0: return -2
    if b < 0 or b >= int64(vs[0].data.len): -2 else: int64(vs[0].data[int(b)])
  of NumHasSourceViewsFlag:
    if reader.meta.hasAlternateSourceViews: 1 else: 0
  of NumHasSpanStreamFlag:
    if reader.meta.hasSpanStream: 1 else: 0
  of NumHasStepStreamFlag:
    if reader.meta.hasStepStream: 1 else: 0
  of NumHasValueStreamFlag:
    if reader.meta.hasValueStream: 1 else: 0
  of NumHasIoStreamFlag:
    if reader.meta.hasIoEventStream: 1 else: 0
  else:
    -3

# ---------------------------------------------------------------------------
# String / byte-buffer queries
# ---------------------------------------------------------------------------

const
  StrKindPath = 0'i32
  StrKindFunction = 1'i32
  StrKindType = 2'i32
  StrKindVarname = 3'i32
  StrKindSourceViewName = 4'i32
  StrKindSourceViewContent = 5'i32
  StrKindSourceViewMap = 6'i32
  StrKindIoData = 7'i32
  StrKindIoMeta = 8'i32
  StrKindSpanLabel = 9'i32
  StrKindSpanType = 10'i32
  StrKindSpanMetadata = 11'i32
  StrKindSpanExternalRecording = 12'i32
  StrKindSpanExternalPath = 13'i32
  StrKindSpanTypeName = 14'i32

  MetaKeySep = 0x1F'u8   ## between a metadata key and its value
  MetaPairSep = 0x1E'u8  ## between metadata pairs; the ORDER is the payload

proc setStrBufFromString(s: string) =
  strBuf = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    strBuf[i] = byte(s[i])

proc setStrBufFromBytes(bs: seq[byte]) =
  strBuf = newSeq[byte](bs.len)
  for i in 0 ..< bs.len:
    strBuf[i] = bs[i]

proc ctStr(kind: int32, id: int64): int32 {.exportc: "ct_str", cdecl.} =
  ## Decode one string or byte run into `strBuf` and return its length, or -1.
  ## The host then reads `ct_str_len` bytes from `ct_str_ptr`.
  ##
  ## Zero is a legitimate length here — an empty sourcemap and empty IO-event
  ## content are both shapes the corpus carries on purpose — so the host must
  ## distinguish 0 from the -1 that means "the reader refused".
  strBuf = @[]
  if not readerOpen: return -1
  case kind
  of StrKindPath, StrKindFunction, StrKindType, StrKindVarname:
    let res =
      case kind
      of StrKindPath: reader.path(uint64(id))
      of StrKindFunction: reader.function(uint64(id))
      of StrKindType: reader.typeName(uint64(id))
      else: reader.varname(uint64(id))
    if res.isErr: return -1
    setStrBufFromString(res.get())
  of StrKindSourceViewName, StrKindSourceViewContent, StrKindSourceViewMap:
    let res = reader.sourceView(uint64(id))
    if res.isErr: return -1
    let sv = res.get()
    case kind
    of StrKindSourceViewName: setStrBufFromString(sv.viewName)
    of StrKindSourceViewContent: setStrBufFromBytes(sv.content)
    else: setStrBufFromBytes(sv.sourcemapV3)
  of StrKindIoData, StrKindIoMeta:
    let res = reader.ioEvent(uint64(id))
    if res.isErr: return -1
    let ev = res.get()
    if kind == StrKindIoData: setStrBufFromBytes(ev.data)
    else: setStrBufFromBytes(ev.metadata)
  of StrKindSpanLabel, StrKindSpanType, StrKindSpanMetadata,
     StrKindSpanExternalRecording, StrKindSpanExternalPath:
    if not spanReaderOpen: return -1
    if id < 0 or id >= int64(spanSettled.len): return -1
    let s = spanSettled[int(id)]
    case kind
    of StrKindSpanLabel: setStrBufFromString(s.label)
    of StrKindSpanType: setStrBufFromString(s.spanType)
    of StrKindSpanExternalRecording:
      setStrBufFromString(s.externalRecording)
    of StrKindSpanExternalPath: setStrBufFromString(s.externalPath)
    else:
      var flat = ""
      for i in 0 ..< s.metadata.len:
        if i > 0: flat.add(char(MetaPairSep))
        flat.add(s.metadata[i][0])
        flat.add(char(MetaKeySep))
        flat.add(s.metadata[i][1])
      setStrBufFromString(flat)
  of StrKindSpanTypeName:
    if not spanReaderOpen: return -1
    if id < 0 or id >= int64(spanTypes.len): return -1
    setStrBufFromString(spanTypes[int(id)].name)
  else:
    return -1
  int32(strBuf.len)

proc ctStrPtr(): pointer {.exportc: "ct_str_ptr", cdecl.} =
  if strBuf.len == 0: nil else: addr strBuf[0]

proc ctStrLen(): int32 {.exportc: "ct_str_len", cdecl.} =
  int32(strBuf.len)
