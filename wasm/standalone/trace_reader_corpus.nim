## One corpus, one verifier, compiled for both a freestanding target and the
## host — so "the reader decoded it" means the same thing on both.
##
## `trace_reader_standalone.nim` builds this for `wasm32-unknown-unknown` and
## `trace_reader_native.nim` builds it for the host.  The corpus is WRITTEN by
## `trace_reader_corpus_build.nim`, which is a separate module so this one
## pulls in no writer. The container the native
## build emits is fed byte-for-byte to the wasm module by
## `trace_reader_host.mjs`, which is why the corpus must be deterministic:
## `CorpusRecordingId` is supplied rather than minted, so no clock and no
## entropy source enters the bytes.
##
## The verifier asserts DECODED CONTENT, not that a handle was obtained.
## Every expectation below is recomputed from `CorpusPaths` / `CorpusLineLens`
## rather than read out of the container, so a reader that returned a
## plausible-looking wrong answer fails.
##
## The corpus covers every read surface this repo's writer can produce:
## steps, positions, interning tables, values and calls; and — because a path
## that compiles and links can still fault on a target with no libc behind it —
## source views, IO events, spans and the line-hit index. A surface that is
## never EXECUTED on the target is not verified there, whatever the link log
## says.

{.push raises: [].}

import std/options
import results
import ../../src/codetracer_trace_writer/new_trace_reader
import ../../src/codetracer_trace_writer/linehits_reader
import ../../src/codetracer_trace_writer/value_stream
import ../../src/codetracer_trace_writer/call_stream
import ../../src/codetracer_trace_writer/io_event_stream
import ../../src/codetracer_trace_writer/span_stream

const
  CorpusRecordingId* = "0192f8a0-1234-7abc-8def-0123456789ab"
  CorpusProgram* = "ct_browser_reader_probe"

  CorpusSteps* = 5000
    ## Above the writer's default 4096-event chunk, so the read path seeks
    ## into a sealed seekable-Zstd chunk instead of decoding a single one.

  CorpusPath0* = "/ct/browser/main.nim"
  CorpusPath1* = "/ct/browser/lib.nim"

  CorpusFunc0* = "browserMain"
  CorpusFunc1* = "decodeStep"
  CorpusFunc2* = "renderFlow"

  CorpusType0* = "int64"
  CorpusType1* = "string"
  CorpusType2* = "StepEvent"

  CorpusVar0* = "acc"
  CorpusVar1* = "idx"
  CorpusVar2* = "path"

  CorpusValueEvery* = 500
    ## One variable value every N steps, so the value stream is non-empty and
    ## its per-step association is checkable.
  CorpusValueByte* = 42'u8

let
  CorpusLineLens0* = @[10'u32, 20'u32, 30'u32, 40'u32, 50'u32]
  CorpusLineLens1* = @[5'u32, 15'u32, 25'u32]

proc corpusFile*(i: int): uint64 =
  uint64(i mod 2)

proc corpusLine*(i: int): uint64 =
  ## 1-based, and always within the file's registered line count.
  if corpusFile(i) == 0'u64: uint64(i mod 5) + 1 else: uint64(i mod 3) + 1

proc corpusColumnDelta*(i: int): int64 =
  ## Offset from column 1, kept strictly below the shortest line of the file
  ## it lands on so a decoded position cannot spill into the next line.
  if corpusFile(i) == 0'u64: int64(i mod 7) else: int64(i mod 4)

proc corpusPositionIndex*(i: int): uint64 =
  ## The `global_position_index` the column-aware writer must emit for step
  ## `i`: file base + the byte offset of column 1 on the line + the column
  ## delta. Recomputed here rather than read back, so it is an expectation.
  let f = corpusFile(i)
  let line = corpusLine(i)
  let lens = if f == 0'u64: CorpusLineLens0 else: CorpusLineLens1
  var base: uint64 = 0
  if f == 1'u64:
    for L in CorpusLineLens0:
      base += uint64(L)
  var off: uint64 = 0
  for k in 0 ..< int(line) - 1:
    off += uint64(lens[k])
  base + off + uint64(corpusColumnDelta(i))

# ---------------------------------------------------------------------------
# Alternate source views
# ---------------------------------------------------------------------------
#
# Three views over two paths, so `sourceViewsForPath` has both a multi-view
# and a single-view answer to give.  The shapes are chosen to hit the record
# decoder's boundaries rather than to look realistic: view 1 carries the
# spec-allowed zero-length sourcemap, view 2 carries zero-length CONTENT and a
# vendor-range view kind (>= 128), and view 2 targets path 0 again so the
# reverse index cannot be a 1:1 mapping that happens to work.

const
  CorpusViewCount* = 3

  CorpusView0Path* = 0'u64
  CorpusView0Kind* = 1'u8            ## prettier_format
  CorpusView0Name* = "main.fmt.nim"
  CorpusView0Content* = "let x = 1\nlet y = 2\n"
  CorpusView0Map* = "{\"version\":3,\"mappings\":\"AAAA;AACA\"}"

  CorpusView1Path* = 1'u64
  CorpusView1Kind* = 2'u8            ## black_format
  CorpusView1Name* = "lib.fmt.nim"
  CorpusView1Content* = "proc f() = discard\n"
  CorpusView1Map* = ""               ## spec-allowed "no sourcemap"

  CorpusView2Path* = 0'u64
  CorpusView2Kind* = 200'u8          ## vendor-specific range
  CorpusView2Name* = "main.vendor.nim"
  CorpusView2Content* = ""           ## zero-length content
  CorpusView2Map* = "{\"version\":3,\"mappings\":\"\"}"

proc corpusViewPath*(i: int): uint64 =
  case i
  of 0: CorpusView0Path
  of 1: CorpusView1Path
  else: CorpusView2Path

proc corpusViewKind*(i: int): uint8 =
  case i
  of 0: CorpusView0Kind
  of 1: CorpusView1Kind
  else: CorpusView2Kind

proc corpusViewName*(i: int): string =
  case i
  of 0: CorpusView0Name
  of 1: CorpusView1Name
  else: CorpusView2Name

proc corpusViewContent*(i: int): string =
  case i
  of 0: CorpusView0Content
  of 1: CorpusView1Content
  else: CorpusView2Content

proc corpusViewMap*(i: int): string =
  case i
  of 0: CorpusView0Map
  of 1: CorpusView1Map
  else: CorpusView2Map

proc corpusViewsForPath*(pathId: uint64): seq[uint64] =
  ## The view indices the reader's reverse index must return for `pathId`,
  ## derived from the corpus definition rather than read back out of it.
  for i in 0 ..< CorpusViewCount:
    if corpusViewPath(i) == pathId:
      result.add(uint64(i))

# ---------------------------------------------------------------------------
# IO events
# ---------------------------------------------------------------------------
#
# The step ids are supplied explicitly rather than left to default to
# `stepCount - 1`, so each event's attribution is an expectation and not a
# consequence of when the writer happened to be called.  Event 3 carries empty
# CONTENT and non-empty metadata, and event 4 the reverse, because the SPEC
# record's two length-prefixed fields are decoded by the same varint path and
# a swap between them reads back as plausible data.

const CorpusIoCount* = 6

proc corpusIoKind*(i: int): IOEventKind =
  ## Cycles all four kinds, so the EventLogKind ordinal round trip
  ## (`ioEventKindToOrdinal` -> byte -> `ordinalToIOEventKind`) is exercised
  ## for each of them rather than for stdout alone.
  IOEventKind(i mod 4)

proc corpusIoStep*(i: int): uint64 =
  uint64(i * 811 + 7)

proc corpusIoData*(i: int): seq[byte] =
  if i == 3: return @[]
  for k in 0 ..< (i + 1) * 3:
    result.add(byte((i * 31 + k * 7) and 0xFF))

proc corpusIoMeta*(i: int): seq[byte] =
  if i == 4: return @[]
  for k in 0 ..< (i mod 3) + 1:
    result.add(byte(0xA0 + i * 5 + k))

# ---------------------------------------------------------------------------
# Spans
# ---------------------------------------------------------------------------
#
# Four RECORDS resolving to three spans: span 1 is registered open and then
# completed, which is the append-only stream's whole point — `count` must see
# four and `settledSpans` three, and the settled record for span 1 must be the
# COMPLETION.  A reader that returned the open record instead answers with a
# plausible span that is simply the wrong one.

const CorpusSpanRecords* = 4
const CorpusSettledSpans* = 3

proc corpusSpanRecord*(i: int): SpanRecord =
  case i
  of 0:
    SpanRecord(
      spanId: 1'u64, parentSpanId: 0'u64, isOpen: true, isExternal: false,
      status: spanStatusUnknown,
      startWallNs: 1_700_000_000_000_000_000'u64, endWallNs: 0'u64,
      processOrd: 0'u64, threadId: 1'u64,
      startStep: 10'u64, endStep: 0'u64,
      spanType: "web-request", label: "GET /api/users",
      contiguousOnOneThread: true, sharesTimeline: true,
      concurrentWithSiblings: false,
      metadata: @[("method", "GET"), ("status", "pending")])
  of 1:
    SpanRecord(
      spanId: 2'u64, parentSpanId: 0'u64, isOpen: false, isExternal: false,
      status: spanStatusOk,
      startWallNs: 1_700_000_000_000_000_100'u64,
      endWallNs: 1_700_000_000_000_009_100'u64,
      processOrd: 0'u64, threadId: 1'u64,
      startStep: 100'u64, endStep: 4200'u64,
      spanType: "process", label: "/usr/bin/ct-probe",
      contiguousOnOneThread: true, sharesTimeline: false,
      concurrentWithSiblings: true,
      # Order is part of the contract: a reader that sorted these would
      # produce the same SET and the wrong sequence.
      metadata: @[("zeta", "1"), ("alpha", "2"), ("mu", "3")])
  of 2:
    SpanRecord(
      spanId: 1'u64, parentSpanId: 0'u64, isOpen: false, isExternal: false,
      status: spanStatusError,
      startWallNs: 1_700_000_000_000_000_000'u64,
      endWallNs: 1_700_000_000_000_500_000'u64,
      processOrd: 0'u64, threadId: 1'u64,
      startStep: 10'u64, endStep: 4900'u64,
      spanType: "web-request", label: "GET /api/users",
      contiguousOnOneThread: true, sharesTimeline: true,
      concurrentWithSiblings: false,
      metadata: @[("method", "GET"), ("status", "500")])
  else:
    SpanRecord(
      spanId: 3'u64, parentSpanId: 1'u64, isOpen: false, isExternal: true,
      status: spanStatusOk,
      startWallNs: 1_700_000_000_000_100_000'u64,
      endWallNs: 1_700_000_000_000_200_000'u64,
      processOrd: 1'u64, threadId: 2'u64,
      startStep: 0'u64, endStep: 0'u64,
      externalRecording: "0192f8a0-1234-7abc-8def-0123456789ac",
      externalPath: "sub/child.ct",
      spanType: "test", label: "spec::browser_read",
      contiguousOnOneThread: false, sharesTimeline: false,
      concurrentWithSiblings: true,
      metadata: @[])

proc corpusSettledSpan*(i: int): SpanRecord =
  ## Last-record-wins, ascending by span id: span 1's COMPLETION (record 2),
  ## then span 2 (record 1), then span 3 (record 3).
  case i
  of 0: corpusSpanRecord(2)
  of 1: corpusSpanRecord(1)
  else: corpusSpanRecord(3)

proc corpusSpanIdsOfType*(spanType: string): seq[uint64] =
  ## The distinct span ids `spantype.ns` must carry for a type, ascending.
  var seen: seq[uint64] = @[]
  for i in 0 ..< CorpusSpanRecords:
    let r = corpusSpanRecord(i)
    if r.spanType == spanType and r.spanId notin seen:
      seen.add(r.spanId)
  for id in seen:
    result.add(id)

# ---------------------------------------------------------------------------
# Line hits
# ---------------------------------------------------------------------------
#
# The writer records one hit per step at the step's `global_position_index`,
# so the whole index is a pure function of the step definition above.

proc corpusLinehitSteps*(positionIndex: uint64): seq[uint64] =
  ## The step ids the index must hold for `positionIndex` — derived from the
  ## corpus definition, in the order the writer observed them.
  for i in 0 ..< CorpusSteps:
    if corpusPositionIndex(i) == positionIndex:
      result.add(uint64(i))

proc corpusLinehitPositionCount*(): uint64 =
  ## Distinct positions the index must carry.
  var seen: seq[uint64] = @[]
  for i in 0 ..< CorpusSteps:
    let p = corpusPositionIndex(i)
    if p notin seen:
      seen.add(p)
  uint64(seen.len)

const
  LinehitProbePositions* = [0, 3, 7, 13, 29]
    ## Indices into the corpus's STEP space; the probe asks the index about
    ## `corpusPositionIndex` of each.  Chosen to land on both files and on
    ## several distinct (line, column) pairs.

  LinehitAbsentPosition* = 100_000'u64
    ## Past every registered file, so the index must report it as absent
    ## rather than return someone else's hit list.

# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------

const
  ProbeIndices* = [0, 1, 2, 7, 63, 64, 65, 4095, 4096, 4097, 4999]
    ## Deliberately straddles the 4096-event chunk boundary and the ±63
    ## DeltaStep encoding window, so the checked steps are not all reached by
    ## the same decode path.

proc verifySourceViews(r: NewTraceReader): int32 =
  ## Codes 100..119.
  if not r.meta.hasAlternateSourceViews: return 100
  if r.sourceViewCount() != uint64(CorpusViewCount): return 101
  for i in 0 ..< CorpusViewCount:
    let svRes = r.sourceView(uint64(i))
    if svRes.isErr: return 102
    let sv = svRes.get()
    if sv.pathId != corpusViewPath(i): return 103
    if sv.viewKind != corpusViewKind(i): return 104
    if sv.viewName != corpusViewName(i): return 105
    let content = corpusViewContent(i)
    if sv.content.len != content.len: return 106
    for k in 0 ..< content.len:
      if sv.content[k] != byte(content[k]): return 107
    let smap = corpusViewMap(i)
    if sv.sourcemapV3.len != smap.len: return 108
    for k in 0 ..< smap.len:
      if sv.sourcemapV3[k] != byte(smap[k]): return 109
  # Out-of-range index must fail rather than wrap to a real record.
  if r.sourceView(uint64(CorpusViewCount)).isOk: return 110
  for pathId in 0'u64 ..< 2'u64:
    let want = corpusViewsForPath(pathId)
    let got = r.sourceViewsForPath(pathId)
    if got.len != want.len: return 111
    for k in 0 ..< want.len:
      if got[k] != want[k]: return 112
  # A path id past the table is "no views", not an error and not someone
  # else's list.
  if r.sourceViewsForPath(99'u64).len != 0: return 113
  0'i32

proc verifyIoEvents(r: var NewTraceReader): int32 =
  ## Codes 120..139.
  let cnt = r.ioEventCount()
  if cnt.isErr: return 120
  if cnt.get() != uint64(CorpusIoCount): return 121
  for i in 0 ..< CorpusIoCount:
    let evRes = r.ioEvent(uint64(i))
    if evRes.isErr: return 122
    let ev = evRes.get()
    if ev.kind != corpusIoKind(i): return 123
    if ev.stepId != corpusIoStep(i): return 124
    let data = corpusIoData(i)
    if ev.data.len != data.len: return 125
    for k in 0 ..< data.len:
      if ev.data[k] != data[k]: return 126
    let meta = corpusIoMeta(i)
    if ev.metadata.len != meta.len: return 127
    for k in 0 ..< meta.len:
      if ev.metadata[k] != meta[k]: return 128
  if r.ioEvent(uint64(CorpusIoCount)).isOk: return 129
  0'i32

proc spansEqual(a, b: SpanRecord): bool =
  if a.spanId != b.spanId: return false
  if a.parentSpanId != b.parentSpanId: return false
  if a.isOpen != b.isOpen: return false
  if a.isExternal != b.isExternal: return false
  if a.status != b.status: return false
  if a.startWallNs != b.startWallNs: return false
  if a.endWallNs != b.endWallNs: return false
  if a.processOrd != b.processOrd: return false
  if a.threadId != b.threadId: return false
  if a.startStep != b.startStep: return false
  if a.endStep != b.endStep: return false
  if a.externalRecording != b.externalRecording: return false
  if a.externalPath != b.externalPath: return false
  if a.spanType != b.spanType: return false
  if a.label != b.label: return false
  if a.contiguousOnOneThread != b.contiguousOnOneThread: return false
  if a.sharesTimeline != b.sharesTimeline: return false
  if a.concurrentWithSiblings != b.concurrentWithSiblings: return false
  if a.metadata.len != b.metadata.len: return false
  for i in 0 ..< a.metadata.len:
    if a.metadata[i][0] != b.metadata[i][0]: return false
    if a.metadata[i][1] != b.metadata[i][1]: return false
  true

proc verifySpans(data: seq[byte], hasFlag: bool): int32 =
  ## Codes 140..159.
  if not hasFlag: return 140
  let srRes = initSpanStreamReader(data)
  if srRes.isErr: return 141
  var sr = srRes.get()
  if sr.count() != uint64(CorpusSpanRecords): return 142
  for i in 0 ..< CorpusSpanRecords:
    let recRes = sr.readSpan(uint64(i))
    if recRes.isErr: return 143
    if not spansEqual(recRes.get(), corpusSpanRecord(i)): return int32(144)
  if sr.readSpan(uint64(CorpusSpanRecords)).isOk: return 145

  let settledRes = sr.settledSpans()
  if settledRes.isErr: return 146
  let settled = settledRes.get()
  if settled.len != CorpusSettledSpans: return 147
  for i in 0 ..< CorpusSettledSpans:
    if not spansEqual(settled[i], corpusSettledSpan(i)): return int32(148)

  let nsRes = readSpanTypeNamespace(data)
  if nsRes.isErr: return 149
  let ns = nsRes.get()
  for spanType in ["web-request", "process", "test"]:
    let want = corpusSpanIdsOfType(spanType)
    let got = spanIdsOfType(ns, spanType)
    if got.len != want.len: return 150
    for k in 0 ..< want.len:
      if got[k] != want[k]: return 151
  if spanIdsOfType(ns, "no-such-type").len != 0: return 152
  0'i32

proc verifyLinehits(data: seq[byte]): int32 =
  ## Codes 160..179.
  if not hasLinehits(data): return 160
  let lhRes = initLinehitsReader(data)
  if lhRes.isErr: return 161
  let lh = lhRes.get()
  if lh.positionCount() != corpusLinehitPositionCount(): return 162
  for stepIdx in LinehitProbePositions:
    let pos = corpusPositionIndex(stepIdx)
    let want = corpusLinehitSteps(pos)
    if want.len == 0: return 163
    let gotRes = lh.hits(pos)
    if gotRes.isErr: return 164
    let got = gotRes.get()
    if got.len != want.len: return 165
    for k in 0 ..< want.len:
      if got[k] != want[k]: return 166
  # An unexecuted position must be reported as absent, not answered with a
  # neighbour's hit list.
  if lh.hits(LinehitAbsentPosition).isOk: return 167
  let keysRes = lh.positions()
  if keysRes.isErr: return 168
  if uint64(keysRes.get().len) != corpusLinehitPositionCount(): return 169
  0'i32

proc verifyCorpus*(data: seq[byte]): int32 =
  ## Open `data` as a CTFS container and check what comes back out.
  ## Returns 0, or a code identifying the first check that failed.
  if data.len == 0: return 1

  let rr = openNewTraceFromBytes(data)
  if rr.isErr: return 2
  var r = rr.get()

  if not r.meta.hasColumnAwareSteps: return 3

  # --- interning tables ----------------------------------------------------
  if r.pathCount() != 2'u64: return 10
  if r.functionCount() != 3'u64: return 11
  if r.typeCount() != 3'u64: return 12
  if r.varnameCount() != 3'u64: return 13

  let path0 = r.path(0'u64)
  if path0.isErr or path0.get() != CorpusPath0: return 20
  let path1 = r.path(1'u64)
  if path1.isErr or path1.get() != CorpusPath1: return 21

  let f0 = r.function(0'u64)
  if f0.isErr or f0.get() != CorpusFunc0: return 22
  let f1 = r.function(1'u64)
  if f1.isErr or f1.get() != CorpusFunc1: return 23
  let f2 = r.function(2'u64)
  if f2.isErr or f2.get() != CorpusFunc2: return 24

  let t0 = r.typeName(0'u64)
  if t0.isErr or t0.get() != CorpusType0: return 25
  let t1 = r.typeName(1'u64)
  if t1.isErr or t1.get() != CorpusType1: return 26
  let t2 = r.typeName(2'u64)
  if t2.isErr or t2.get() != CorpusType2: return 27

  let v0 = r.varname(0'u64)
  if v0.isErr or v0.get() != CorpusVar0: return 28
  let v1 = r.varname(1'u64)
  if v1.isErr or v1.get() != CorpusVar1: return 29
  let v2 = r.varname(2'u64)
  if v2.isErr or v2.get() != CorpusVar2: return 30

  # --- per-line tables -----------------------------------------------------
  if r.lineCountRaw(0'u64) != uint64(CorpusLineLens0.len): return 35
  if r.lineCountRaw(1'u64) != uint64(CorpusLineLens1.len): return 36
  for k in 0 ..< CorpusLineLens0.len:
    let ll = r.lineLength(0'u64, uint32(k))
    if ll.isNone or ll.get() != CorpusLineLens0[k]: return 37
  for k in 0 ..< CorpusLineLens1.len:
    let ll = r.lineLength(1'u64, uint32(k))
    if ll.isNone or ll.get() != CorpusLineLens1[k]: return 38

  # --- steps ---------------------------------------------------------------
  let sc = r.stepCount()
  if sc.isErr or sc.get() != uint64(CorpusSteps): return 40

  for i in ProbeIndices:
    let gliRes = r.stepAbsoluteGlobalLineIndex(uint64(i))
    if gliRes.isErr: return 50
    if gliRes.get() != corpusPositionIndex(i): return int32(51)
    let posRes = r.decodeGlobalPositionIndex(gliRes.get())
    if posRes.isErr: return 52
    let pos = posRes.get()
    if pos.file != corpusFile(i): return 53
    if pos.line != uint32(corpusLine(i)): return 54
    if pos.column != uint32(corpusColumnDelta(i) + 1): return 55

  # The bulk accessor must agree with the per-step one across a chunk seam.
  var bulk = newSeq[uint64](8)
  let nb = r.stepAbsoluteGlobalLineIndices(4092'u64, 8'u64, bulk)
  if nb.isErr or nb.get() != 8'u64: return 60
  for k in 0 ..< 8:
    if bulk[k] != corpusPositionIndex(4092 + k): return 61

  # --- values --------------------------------------------------------------
  let vals = r.values(0'u64)
  if vals.isErr: return 70
  if vals.get().len != 1: return 71
  if vals.get()[0].data.len != 2: return 72
  if vals.get()[0].data[1] != CorpusValueByte: return 73

  # --- calls ---------------------------------------------------------------
  let cc = r.callCount()
  if cc.isErr or cc.get() != 1'u64: return 80
  let c0 = r.call(0'u64)
  if c0.isErr or c0.get().functionId != 1'u64: return 81

  # --- the surfaces that only linking had covered --------------------------
  let svRc = verifySourceViews(r)
  if svRc != 0: return svRc
  let ioRc = verifyIoEvents(r)
  if ioRc != 0: return ioRc
  let spanRc = verifySpans(data, r.meta.hasSpanStream)
  if spanRc != 0: return spanRc
  let lhRc = verifyLinehits(data)
  if lhRc != 0: return lhRc

  0'i32

# ---------------------------------------------------------------------------
# The legacy Nim-v4 framing
# ---------------------------------------------------------------------------
#
# A pre-M24a bundle is a SPLIT container — no `events.log` — whose meta.dat
# leaves the three stream bits clear, so the reader picks its framing from the
# flags rather than from the bytes.  That makes it the one shape where a wrong
# answer is guaranteed to look right: the container opens, the counts are
# plausible and the positions are in range whichever framing is chosen.
#
# `verifyLegacyCorpus` therefore checks decoded CONTENT against the corpus
# definition, and `LegacyMisframedExpectation` below pins what happens when the
# flags LIE — see `trace_reader_corpus_build.nim` for how that container is
# produced.

const
  LegacyRecordingId* = "0192f8a0-1234-7abc-8def-0123456789ad"
  LegacyProgram* = "ct_browser_legacy_probe"

  LegacySteps* = 40
  LegacyChunkSize* = 8
    ## Five chunks, so the legacy per-chunk `u32 count` header is crossed
    ## several times and the `total_events` trailer is not the only thing that
    ## has to be right.

  LegacyPath0* = "/legacy/v4/alpha.nim"
  LegacyPath1* = "/legacy/v4/beta.nim"
  LegacyFunc0* = "legacyMain"
  LegacyVar0* = "counter"
  LegacyType0* = "uint64"

  LegacyIoCount* = 5
  LegacyValueByte* = 0x5A'u8

proc legacyStepGli*(i: int): uint64 =
  ## Line-only addressing: `(path_id << 32) | line`, which is what
  ## `DefaultLinesPerFile`-based `GlobalLineIndex` produces and what a
  ## pre-column-aware bundle carries.  Kept inside one file's slot so the
  ## deltas stay in the ±63 DeltaStep window for most steps and cross it for
  ## some.
  1000'u64 + uint64(i) * 3'u64

proc legacyValueBytes*(i: int): seq[byte] =
  ## A minimal CBOR-ish payload; the legacy value record carries a verbatim
  ## type id alongside it, which is exactly what the SPEC framing dropped.
  @[0x18'u8, byte((int(LegacyValueByte) + i) and 0xFF)]

proc legacyIoKind*(i: int): IOEventKind =
  IOEventKind(i mod 4)

proc legacyIoStep*(i: int): uint64 =
  uint64(i * 5 + 1)

proc legacyIoData*(i: int): seq[byte] =
  for k in 0 ..< i + 2:
    result.add(byte((0x40 + i * 11 + k) and 0xFF))

proc verifyLegacyCorpus*(data: seq[byte]): int32 =
  ## Codes 200..249.  Open a LEGACY-framed v4 container and check what the
  ## reader decodes, against values the corpus definition computes.
  if data.len == 0: return 200

  let rr = openNewTraceFromBytes(data)
  if rr.isErr: return 201
  var r = rr.get()

  # The framing discriminator itself.  If any of these is set the container is
  # not the legacy shape and the rest of this proc would be testing the SPEC
  # reader under a legacy name.
  if r.meta.hasStepStream: return 202
  if r.meta.hasValueStream: return 203
  if r.meta.hasIoEventStream: return 204
  # A legacy bundle is line-only.  The reader speculatively parses paths.dat
  # as column-aware Layout A and PROMOTES the flag when that parse succeeds;
  # a promotion here would silently reinterpret every position as a byte
  # offset, so it is checked rather than assumed.
  if r.meta.hasColumnAwareSteps: return 205

  if r.pathCount() != 2'u64: return 210
  let p0 = r.path(0'u64)
  if p0.isErr or p0.get() != LegacyPath0: return 211
  let p1 = r.path(1'u64)
  if p1.isErr or p1.get() != LegacyPath1: return 212
  let lf0 = r.function(0'u64)
  if lf0.isErr or lf0.get() != LegacyFunc0: return 213
  let lt0 = r.typeName(0'u64)
  if lt0.isErr or lt0.get() != LegacyType0: return 214
  let lv0 = r.varname(0'u64)
  if lv0.isErr or lv0.get() != LegacyVar0: return 215

  let sc = r.stepCount()
  if sc.isErr: return 220
  if sc.get() != uint64(LegacySteps): return 221

  for i in 0 ..< LegacySteps:
    let gliRes = r.stepAbsoluteGlobalLineIndex(uint64(i))
    if gliRes.isErr: return 222
    if gliRes.get() != legacyStepGli(i): return int32(223)

  var bulk = newSeq[uint64](LegacySteps)
  let nb = r.stepAbsoluteGlobalLineIndices(0'u64, uint64(LegacySteps), bulk)
  if nb.isErr or nb.get() != uint64(LegacySteps): return 224
  for i in 0 ..< LegacySteps:
    if bulk[i] != legacyStepGli(i): return int32(225)

  # Legacy `.off` VRT values: one record per step, carrying a verbatim type id.
  for i in 0 ..< LegacySteps:
    let vRes = r.values(uint64(i))
    if vRes.isErr: return 230
    let vs = vRes.get()
    if vs.len != 1: return 231
    if vs[0].varnameId != 0'u64: return 232
    if vs[0].typeId != 0'u64: return 233
    let want = legacyValueBytes(i)
    if vs[0].data.len != want.len: return 234
    for k in 0 ..< want.len:
      if vs[0].data[k] != want[k]: return 235

  # Legacy `.off` VRT IO events: the kind byte is the IOEventKind ordinal, not
  # an EventLogKind ordinal, and there is no metadata field at all.
  let ioCnt = r.ioEventCount()
  if ioCnt.isErr: return 240
  if ioCnt.get() != uint64(LegacyIoCount): return 241
  for i in 0 ..< LegacyIoCount:
    let evRes = r.ioEvent(uint64(i))
    if evRes.isErr: return 242
    let ev = evRes.get()
    if ev.kind != legacyIoKind(i): return 243
    if ev.stepId != legacyIoStep(i): return 244
    if ev.metadata.len != 0: return 245
    let want = legacyIoData(i)
    if ev.data.len != want.len: return 246
    for k in 0 ..< want.len:
      if ev.data[k] != want[k]: return 247

  0'i32

proc probeMisframedLegacy*(data: seq[byte]): int32 =
  ## Read a legacy-framed container whose meta.dat CLAIMS the SPEC framing.
  ##
  ## This is not a container any writer produces; it is the mis-discrimination
  ## itself, isolated.  The question it answers is the one that matters for a
  ## browser: when the flag and the bytes disagree, does the reader FAIL, or
  ## does it hand back a step count and positions that look like a trace?
  ##
  ## Returns a bit set, so one call distinguishes the outcomes:
  ##   1 — the container opened
  ##   2 — a step count came back
  ##   4 — that step count was WRONG (the legacy value is `LegacySteps`)
  ##   8 — step 0's position resolved at all
  ##  16 — that position was WRONG
  ## A reader that fails loudly returns 1 or 3; a reader that returns
  ## wrong-but-plausible data sets bits 4 and/or 16.
  let rr = openNewTraceFromBytes(data)
  if rr.isErr: return 0
  var r = rr.get()
  result = 1
  let sc = r.stepCount()
  if sc.isOk:
    result = result or 2
    if sc.get() != uint64(LegacySteps):
      result = result or 4
  let gli = r.stepAbsoluteGlobalLineIndex(0'u64)
  if gli.isOk:
    result = result or 8
    if gli.get() != legacyStepGli(0):
      result = result or 16

{.pop.}
