## The write half of the reader probe's corpus.
##
## Split out of `trace_reader_corpus.nim` so a module that only READS is not
## forced to link a writer it never calls: `trace_reader_only_standalone.nim`
## imports the verifier alone, and its size is what a read-only embedding
## actually pays.

{.push raises: [].}

import std/options
import results
import ../../src/codetracer_ctfs/types
import ../../src/codetracer_ctfs/container
import ../../src/codetracer_ctfs/variable_record_table
import ../../src/codetracer_ctfs/zstd_bindings
import ../../src/codetracer_trace_writer/multi_stream_writer
import ../../src/codetracer_trace_writer/interning_table
import ../../src/codetracer_trace_writer/step_encoding
import ../../src/codetracer_trace_writer/meta_dat
import ../../src/codetracer_trace_writer/value_stream
import ../../src/codetracer_trace_writer/io_event_stream
import ../../src/codetracer_trace_writer/varint
import ../../src/codetracer_trace_types
import ./trace_reader_corpus

# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

proc buildCorpus*(): Result[seq[byte], string] =
  ## Build the corpus container in linear memory. No path is opened: the
  ## `path` argument of `initMultiStreamWriter` is metadata only.
  var wr = initMultiStreamWriter("/ct/browser/probe.ct", CorpusProgram,
                                 recordingId = CorpusRecordingId)
  if wr.isErr: return err("initMultiStreamWriter: " & wr.error)
  var w = wr.get()

  w.enableColumnAwareSteps()
  # Must precede the first step: the builder accumulates a hit per step as it
  # is registered, so enabling it later would index a suffix of the trace.
  w.enableLinehits()

  let p0 = w.registerPath(CorpusPath0, CorpusLineLens0)
  if p0.isErr: return err("registerPath 0: " & p0.error)
  let p1 = w.registerPath(CorpusPath1, CorpusLineLens1)
  if p1.isErr: return err("registerPath 1: " & p1.error)

  for i in 0 ..< CorpusViewCount:
    let content = corpusViewContent(i)
    let smap = corpusViewMap(i)
    var contentBytes = newSeq[byte](content.len)
    for k in 0 ..< content.len:
      contentBytes[k] = byte(content[k])
    var mapBytes = newSeq[byte](smap.len)
    for k in 0 ..< smap.len:
      mapBytes[k] = byte(smap[k])
    let svRes = w.registerSourceView(corpusViewPath(i), corpusViewKind(i),
                                     corpusViewName(i), contentBytes, mapBytes)
    if svRes.isErr: return err("registerSourceView: " & svRes.error)
    if svRes.get() != uint64(i):
      return err("registerSourceView returned index " & $svRes.get() &
        ", expected " & $i)

  for name in [CorpusFunc0, CorpusFunc1, CorpusFunc2]:
    let r = w.registerFunction(name)
    if r.isErr: return err("registerFunction: " & r.error)
  for name in [CorpusType0, CorpusType1, CorpusType2]:
    let r = w.registerType(name)
    if r.isErr: return err("registerType: " & r.error)
  for name in [CorpusVar0, CorpusVar1, CorpusVar2]:
    let r = w.registerVarname(name)
    if r.isErr: return err("registerVarname: " & r.error)

  let callRes = w.registerCall(1'u64, [])
  if callRes.isErr: return err("registerCall: " & callRes.error)

  for i in 0 ..< CorpusSteps:
    var values: seq[VariableValue] = @[]
    if i mod CorpusValueEvery == 0:
      values.add(VariableValue(varnameId: 0'u64, typeId: 0'u64,
                               data: @[0x18'u8, CorpusValueByte]))
    let r = w.registerStepWithColumn(corpusFile(i), corpusLine(i),
                                     corpusColumnDelta(i), values)
    if r.isErr: return err("registerStepWithColumn: " & r.error)

  # Attributed to explicit step ids rather than to "the last step written",
  # so the association the reader surfaces is an expectation and not an
  # artefact of call order.
  for i in 0 ..< CorpusIoCount:
    let r = w.registerIOEvent(corpusIoKind(i), corpusIoData(i),
                              corpusIoMeta(i), some(corpusIoStep(i)))
    if r.isErr: return err("registerIOEvent: " & r.error)

  for i in 0 ..< CorpusSpanRecords:
    let r = w.registerSpan(corpusSpanRecord(i))
    if r.isErr: return err("registerSpan: " & r.error)

  let retRes = w.registerReturn()
  if retRes.isErr: return err("registerReturn: " & retRes.error)

  let cl = w.close()
  if cl.isErr: return err("close: " & cl.error)
  ok(w.toBytes())

# ---------------------------------------------------------------------------
# The legacy Nim-v4 framing
# ---------------------------------------------------------------------------
#
# The writer in `src/` cannot emit this: `close()` stamps `has_step_stream`,
# `has_value_stream` and `has_io_event_stream` unconditionally, so every
# container it produces is SPEC-framed.  The legacy shape still exists in the
# wild — it is what pre-M24a Nim-v4 bundles are, and `new_trace_reader` selects
# its decoders from those three flags — so the only way to EXECUTE those
# decoders is to lay the container out by hand, which is what follows.
#
# The framings are transcribed from the readers they have to satisfy
# (`exec_stream.nim`, `value_stream.nim`, `io_event_stream.nim`), and the
# per-stream shapes match the ones `tests/test_*_stream.nim` already build for
# the host-side back-compat tests.

proc buildLegacyExecStream(ctfs: var Ctfs, events: seq[StepEvent],
    chunkSize: int): Result[uint64, string] =
  ## Legacy `steps.dat` / `steps.idx`:
  ##   * `steps.idx`: `[chunk_size:u32][total_events placeholder:u64]`
  ##     `[offset_0:u64]...[total_events trailer:u64]`
  ##   * each chunk's uncompressed payload: `[event_count:u32][events...]`
  ## The SPEC framing has neither the per-chunk count nor the trailer.
  let datRes = ctfs.addFile("steps.dat")
  if datRes.isErr: return err(datRes.error)
  var datFile = datRes.get()
  let idxRes = ctfs.addFile("steps.idx")
  if idxRes.isErr: return err(idxRes.error)
  var idxFile = idxRes.get()

  var hdr: array[12, byte]
  let csLE = toBytesLE(uint32(chunkSize))
  for i in 0 ..< 4: hdr[i] = csLE[i]
  let hdrW = ctfs.writeToFile(idxFile, hdr)
  if hdrW.isErr: return err(hdrW.error)

  var dataOffset: uint64 = 0
  var total: uint64 = 0
  var i = 0
  while i < events.len:
    let endIdx = min(i + chunkSize, events.len)
    let count = endIdx - i
    var payload = newSeq[byte](4)
    let ecLE = toBytesLE(uint32(count))
    for k in 0 ..< 4: payload[k] = ecLE[k]
    for j in i ..< endIdx:
      encodeStepEvent(events[j], payload)
    let bound = ZSTD_compressBound(csize_t(payload.len))
    var compressed = newSeq[byte](int(bound))
    let cs = ZSTD_compress(addr compressed[0], csize_t(bound),
      addr payload[0], csize_t(payload.len), cint(3))
    if ZSTD_isError(cs) != 0:
      return err("legacy zstd compress failed")
    var off: array[8, byte]
    let offLE = toBytesLE(dataOffset)
    for k in 0 ..< 8: off[k] = offLE[k]
    let offW = ctfs.writeToFile(idxFile, off)
    if offW.isErr: return err(offW.error)
    let datW = ctfs.writeToFile(datFile, compressed.toOpenArray(0, int(cs) - 1))
    if datW.isErr: return err(datW.error)
    dataOffset += uint64(cs)
    total += uint64(count)
    i = endIdx

  var teBytes: array[8, byte]
  let teLE = toBytesLE(total)
  for k in 0 ..< 8: teBytes[k] = teLE[k]
  let teW = ctfs.writeToFile(idxFile, teBytes)
  if teW.isErr: return err(teW.error)
  ok(total)

proc encodeLegacyValueRecord(values: openArray[VariableValue]): seq[byte] =
  ## Legacy `.off` VRT value record:
  ## `varint count, count x (varint varnameId, varint typeId, varint len, data)`.
  ## The type id is stored verbatim here; the SPEC framing reconstructs it.
  var rec: seq[byte] = @[]
  encodeVarint(uint64(values.len), rec)
  for v in values:
    encodeVarint(v.varnameId, rec)
    encodeVarint(v.typeId, rec)
    encodeVarint(uint64(v.data.len), rec)
    rec.add(v.data)
  rec

proc encodeLegacyIoRecord(ev: IOEvent): seq[byte] =
  ## Legacy `.off` VRT IO record: `u8 kind, varint stepId, varint len, data`.
  ## The kind byte is the four-value `IOEventKind` ordinal, NOT the
  ## `EventLogKind` ordinal the SPEC record carries, and there is no metadata
  ## field — which is why reading one framing as the other yields a decode
  ## that succeeds and means something else.
  var rec: seq[byte] = @[]
  rec.add(byte(ev.kind))
  encodeVarint(ev.stepId, rec)
  encodeVarint(uint64(ev.data.len), rec)
  rec.add(ev.data)
  rec

proc buildLegacyCorpusFlagged(claimSpecFraming: bool):
    Result[seq[byte], string] =
  ## Lay out a legacy-framed v4 container.
  ##
  ## `claimSpecFraming` stamps meta.dat with the three stream bits SET while
  ## leaving the bytes in the legacy framing — a container no writer produces,
  ## used only by `probeMisframedLegacy` to ask what the reader does when the
  ## discriminator and the bytes disagree.
  var ctfs = createCtfs()

  # Interning tables, in the pre-Layout-A form: a paths.dat record is the raw
  # path string, with no length prefix and no per-line table.
  let itRes = initTraceInterningTables(ctfs)
  if itRes.isErr: return err("initTraceInterningTables: " & itRes.error)
  var it = itRes.get()
  for p in [LegacyPath0, LegacyPath1]:
    let r = ctfs.ensurePathId(it, p)
    if r.isErr: return err("ensurePathId: " & r.error)
  let fRes = ctfs.ensureFunctionId(it, LegacyFunc0)
  if fRes.isErr: return err("ensureFunctionId: " & fRes.error)
  let tRes = ctfs.ensureTypeId(it, LegacyType0)
  if tRes.isErr: return err("ensureTypeId: " & tRes.error)
  let vRes = ctfs.ensureVarnameId(it, LegacyVar0)
  if vRes.isErr: return err("ensureVarnameId: " & vRes.error)

  # Steps.  The first event of every chunk must be absolute, because a legacy
  # chunk is independently decodable; in between, deltas inside the +-63 window
  # and absolutes outside it, which is what the writer of the day emitted.
  var events: seq[StepEvent] = @[]
  var prev: uint64 = 0
  for i in 0 ..< LegacySteps:
    let gli = legacyStepGli(i)
    if i mod LegacyChunkSize == 0:
      events.add(StepEvent(kind: sekAbsoluteStep, globalLineIndex: gli))
    else:
      let delta = int64(gli) - int64(prev)
      if delta >= -64 and delta <= 63:
        events.add(StepEvent(kind: sekDeltaStep, lineDelta: delta))
      else:
        events.add(StepEvent(kind: sekAbsoluteStep, globalLineIndex: gli))
    prev = gli
  let stepRes = buildLegacyExecStream(ctfs, events, LegacyChunkSize)
  if stepRes.isErr: return err("buildLegacyExecStream: " & stepRes.error)
  if stepRes.get() != uint64(LegacySteps):
    return err("legacy exec stream wrote " & $stepRes.get() & " events")

  # Values: one legacy `.off` VRT record per step.
  let valTableRes = initVariableRecordTableWriter(ctfs, "values")
  if valTableRes.isErr: return err("values table: " & valTableRes.error)
  var valTable = valTableRes.get()
  for i in 0 ..< LegacySteps:
    let rec = encodeLegacyValueRecord(
      [VariableValue(varnameId: 0'u64, typeId: 0'u64,
                     data: legacyValueBytes(i))])
    let a = ctfs.append(valTable, rec)
    if a.isErr: return err("append legacy value: " & a.error)

  # IO events: legacy `.off` VRT, no metadata field.
  let ioTableRes = initVariableRecordTableWriter(ctfs, "events")
  if ioTableRes.isErr: return err("events table: " & ioTableRes.error)
  var ioTable = ioTableRes.get()
  for i in 0 ..< LegacyIoCount:
    let ev = IOEvent(kind: legacyIoKind(i), stepId: legacyIoStep(i),
                     metadata: @[], data: legacyIoData(i))
    let a = ctfs.append(ioTable, encodeLegacyIoRecord(ev))
    if a.isErr: return err("append legacy io event: " & a.error)

  let metaFileRes = ctfs.addFile("meta.dat")
  if metaFileRes.isErr: return err("meta.dat: " & metaFileRes.error)
  var metaFile = metaFileRes.get()
  let meta = TraceMetadata(recordingId: LegacyRecordingId,
                           program: LegacyProgram, args: @[], workdir: "")
  let mRes = ctfs.writeMetaDat(metaFile, meta, [LegacyPath0, LegacyPath1],
    hasStepStream = claimSpecFraming,
    hasValueStream = claimSpecFraming,
    hasIoEventStream = claimSpecFraming)
  if mRes.isErr: return err("writeMetaDat: " & mRes.error)

  ok(ctfs.toBytes())

proc buildLegacyCorpus*(): Result[seq[byte], string] =
  ## A legacy-framed v4 container whose meta.dat describes it honestly.
  buildLegacyCorpusFlagged(false)

proc buildMisframedLegacyCorpus*(): Result[seq[byte], string] =
  ## The same bytes with meta.dat claiming the SPEC framing.
  buildLegacyCorpusFlagged(true)

{.pop.}
