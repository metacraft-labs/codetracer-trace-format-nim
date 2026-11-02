when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

## ct-print: Convert .ct trace files to human-readable formats.
##
## Usage:
##   ct-print <file.ct>                     # Print as text (default)
##   ct-print --json <file.ct>              # Print as JSON (legacy: no value content)
##   ct-print --json-events <file.ct>       # Print only events as JSON array (legacy)
##   ct-print --summary <file.ct>           # Print metadata and event counts only
##   ct-print --follow <file.ct>            # Follow mode (NDJSON, polls for new events)
##   ct-print --full <file.ct>              # Pretty JSON dump with full value content
##   ct-print --events <file.ct>            # JSONL (one event per line) with full values
##   ct-print --full --strip-paths <file.ct># Strip absolute path prefixes for portability
##
## --full and --events modes decode CBOR-encoded variable values into a
## structured JSON form suitable for golden-snapshot verification:
##   - integers: {"kind": "Int", "i": 42, "type_id": 7}
##   - strings: {"kind": "String", "text": "hello", "type_id": 9}
##   - structs/sequences/tuples/variants/refs: full nested decode
##   - all variants of ValueRecord are surfaced; nothing is hex-blob'd.
##   - a reference's "address", every "type_id", "exception_type_id" and
##     "thread_id" are unsigned 64-bit decimal numbers and may exceed 2^63
##     (e.g. "address": 18446744073709551615); read them as unsigned.

import std/[os, parseopt, json, strutils, base64, algorithm]
import results
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/meta_flags_json
import codetracer_trace_writer/source_reload_json
import codetracer_trace_writer/full_document_json
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/call_stream as v4calls
import codetracer_trace_writer/io_event_stream
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/global_line_index
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/span_stream          # for --spans
import codetracer_trace_writer/cbor
import codetracer_trace_types
import codetracer_ctfs/container as ctfs_container
import codetracer_trace_writer/corrmark_builder
import codetracer_ctfs/types as ctfs_types  # header geometry for --spans
import native_decoder

proc escapeJsonStr(s: string): string =
  ## Escape a string for JSON output without using stdlib json procs.
  result = "\""
  for c in s:
    case c
    of '"': result.add("\\\"")
    of '\\': result.add("\\\\")
    of '\n': result.add("\\n")
    of '\r': result.add("\\r")
    of '\t': result.add("\\t")
    else:
      if ord(c) < 32:
        result.add("\\u00" & toHex(ord(c), 2).toLowerAscii())
      else:
        result.add(c)
  result.add("\"")

proc dataToJsonValue(data: seq[byte]): string =
  ## Best-effort conversion of CBOR-encoded value bytes to a JSON-friendly
  ## string. Used by legacy --json mode only.
  if data.len == 0:
    return "\"\""
  var allPrintable = true
  for b in data:
    if b < 0x20 and b != 0x0A and b != 0x0D and b != 0x09:
      allPrintable = false
      break
  if allPrintable:
    return escapeJsonStr(bytesToUtf8(data))
  else:
    var hex = "\"0x"
    for b in data:
      hex.add(toHex(int(b), 2).toLowerAscii())
    hex.add("\"")
    return hex

# ---------------------------------------------------------------------------
proc printSummaryV4(reader: var NewTraceReader) =
  var lines: seq[string]
  lines.add("program: " & reader.meta.program)
  if reader.meta.args.len > 0:
    lines.add("args: " & reader.meta.args.join(" "))
  if reader.meta.workdir.len > 0:
    lines.add("workdir: " & reader.meta.workdir)
  if reader.meta.recorderId.len > 0:
    lines.add("recorder: " & reader.meta.recorderId)

  # The step count is the number of step records; raise, catch, thread,
  # column-only and reload records occupy exec records but are not steps.
  let sc = reader.logicalStepCount()
  let cc = reader.callCount()
  let vc = reader.valueCount()
  let ic = reader.ioEventCount()

  lines.add("")
  lines.add("counts:")
  if sc.isOk: lines.add("  steps: " & $sc.get())
  else: lines.add("  steps: (unavailable)")
  if cc.isOk: lines.add("  calls: " & $cc.get())
  else: lines.add("  calls: (unavailable)")
  if vc.isOk: lines.add("  values: " & $vc.get())
  else: lines.add("  values: (unavailable)")
  if ic.isOk: lines.add("  io_events: " & $ic.get())
  else: lines.add("  io_events: (unavailable)")

  lines.add("  paths: " & $reader.pathCount())
  lines.add("  functions: " & $reader.functionCount())
  lines.add("  types: " & $reader.typeCount())
  lines.add("  varnames: " & $reader.varnameCount())

  # GDH-M1 — versioned paths. A file that was hot-reloaded mid-session has
  # MORE THAN ONE paths.dat entry carrying the identical path string, and
  # only the index tells the versions apart. This block is printed on every
  # trace, with an explicit "none" when there are no versioned paths,
  # because a section that appears only when there is something to report
  # cannot be told apart from a `ct-print` that does not know how to look.
  lines.add("")
  var versioned: seq[string]
  for i in 0'u64 ..< reader.pathCount():
    let vc = reader.pathVersionCount(i)
    let vo = reader.pathVersionOrdinal(i)
    if vc.isOk and vc.get() > 1'u64 and vo.isOk:
      let p = reader.path(i)
      versioned.add("  [" & $i & "] v" & $vo.get() & " of " & $vc.get() &
        ", " &
        (if reader.recordedLineCount(i) > 0'u64:
           $reader.recordedLineCount(i) & " lines"
         else: "size not stated") &
        "  " & (if p.isOk: p.get() else: "(error)"))
  if versioned.len == 0:
    lines.add("versioned paths: none")
  else:
    lines.add("versioned paths: " & $versioned.len & " entr" &
      (if versioned.len == 1: "y" else: "ies"))
    for v in versioned:
      lines.add(v)

  echo lines.join("\n")

# ---------------------------------------------------------------------------
# V4 meta-json — metadata + counts only, no event decode
# ---------------------------------------------------------------------------

proc printMetaJsonV4(reader: var NewTraceReader) =
  ## Emit a compact JSON document with the CTFS `meta.dat` metadata plus
  ## interning-table / event counts.  Unlike `--full` / `--json` this
  ## does NOT decode the per-event streams, so it stays O(meta.dat) fast
  ## even on multi-GB traces.  Shape (deterministic ordering):
  ##   { metadata: { program, args[], workdir, recorder,
  ##                 trace_filter?: { filters: [{path, sha256}] } },
  ##     counts:  { steps, calls, values, io_events,
  ##                paths, functions, types, varnames } }
  ##
  ## This is the canonical fast path for callers that only need the
  ## recorder-stamped metadata (program identity, args, the composed
  ## trace-filter provenance chain) without paying the event-decode
  ## cost — e.g. recorder integration tests asserting on a recorded
  ## trace's metadata.  `metadata.trace_filter` is materialized exactly
  ## as `--full` does (TF-M7, Trace-Filters.md § 7): present-but-empty
  ## when the recorder recorded an empty chain, absent when the recorder
  ## did not record provenance at all.
  var root = newJObject()

  var meta = newJObject()
  meta["program"] = newJString(reader.meta.program)
  var argsArr = newJArray()
  for a in reader.meta.args:
    argsArr.add(newJString(a))
  meta["args"] = argsArr
  meta["workdir"] = newJString(reader.meta.workdir)
  meta["recorder"] = newJString(reader.meta.recorderId)
  # P1.4: surface the meta.dat flag bits so consumers (e.g. the
  # ``test_column_aware_steps.py`` smoke test) can assert on
  # ``has_column_aware_steps`` without re-reading the raw header.
  # We expose the bool directly rather than as a flags array so the
  # field name is stable across future flag additions (each known flag
  # becomes its own boolean keyed by its meta.dat constant name).
  # ONE rendering, shared with `codetracer_ct_print_lib` — see
  # `meta_flags_json`'s header for the drift this replaces.
  meta["flags"] = metaFlagsJson(reader.meta)
  # Reader diagnostic, deliberately outside `flags` because it is not a
  # meta.dat bit: true when the trace declares line-only steps yet every
  # paths.dat record also decodes as a column-aware Layout A record. Either
  # the resemblance is coincidental and harmless, or the recorder emitted
  # Layout A records without setting bit 4 (a writer bug fixed in 708ee44).
  # The reader keeps reading the trace as its meta.dat declares; this field
  # is how an operator finds out there is a question to answer.
  meta["column_aware_paths_suspected"] = newJBool(
    reader.columnAwarePathsSuspected)

  if reader.meta.hasFilterProvenance:
    var filtersArr = newJArray()
    for entry in reader.meta.filterProvenance:
      var entryObj = newJObject()
      entryObj["path"] = newJString(entry.path)
      var hex = newStringOfCap(64)
      for k in 0 ..< 32:
        hex.add(toHex(int(entry.sha256[k]), 2).toLowerAscii())
      entryObj["sha256"] = newJString(hex)
      filtersArr.add(entryObj)
    var traceFilterObj = newJObject()
    traceFilterObj["filters"] = filtersArr
    meta["trace_filter"] = traceFilterObj

  root["metadata"] = meta

  var counts = newJObject()
  # Use logicalStepCount so user-facing "steps" excludes DeltaColumn
  # nudges (column-aware traces interleave them with line moves but
  # they are not logical source-line steps).
  let sc = reader.logicalStepCount()
  let cc = reader.callCount()
  let vc = reader.valueCount()
  let ic = reader.ioEventCount()
  if sc.isOk: counts["steps"] = unsignedJson(sc.get())
  if cc.isOk: counts["calls"] = unsignedJson(cc.get())
  if vc.isOk: counts["values"] = unsignedJson(vc.get())
  if ic.isOk: counts["io_events"] = unsignedJson(ic.get())
  counts["paths"] = unsignedJson(reader.pathCount())
  counts["functions"] = unsignedJson(reader.functionCount())
  counts["types"] = unsignedJson(reader.typeCount())
  counts["varnames"] = unsignedJson(reader.varnameCount())
  # GDH-M2: how many source-reload markers the container carries.
  # ALWAYS emitted, including the `0` every trace without a reload
  # answers with, for the reason `path_version_ordinal` is always
  # emitted: a key that appears only when a marker exists makes a scan
  # for it pass on a trace that has none AND on a build that cannot see
  # one. It is also what lets a caller check an `--events` dump is
  # COMPLETE: the step entries plus the source_reload entries must
  # account for every exec record.
  let srR = reader.sourceReloadCount()
  counts["source_reloads"] = if srR.isOk: unsignedJson(srR.get()) else: newJInt(-1)
  root["counts"] = counts

  echo pretty(root)

# ---------------------------------------------------------------------------
# V4 JSON (full dump - legacy, value bytes are passed through as text)
# ---------------------------------------------------------------------------

proc printJsonV4(reader: var NewTraceReader) =
  ## The legacy `--json` document: `metadata`, `paths`, `functions`, then
  ## `steps`, `calls`, `values` and `ioEvents`, one entry per record.
  ##
  ## Streamed: the four per-record arrays are produced one element at a
  ## time and written as they come (`streamed_json`), so neither the tree
  ## nor its rendering is held; the text is byte-identical to
  ## `pretty(root)` of the tree this used to build.
  let gli = reader.globalPositionSpace()

  # Metadata
  var meta = newJObject()
  meta["program"] = newJString(reader.meta.program)
  var argsArr = newJArray()
  for arg in reader.meta.args:
    argsArr.add(newJString(arg))
  meta["args"] = argsArr
  meta["workdir"] = newJString(reader.meta.workdir)

  # Paths
  var pathsArr = newJArray()
  for i in 0'u64 ..< reader.pathCount():
    let p = reader.path(i)
    if p.isOk: pathsArr.add(newJString(p.get()))
    else: pathsArr.add(newJString("(error)"))

  # Functions
  var funcsArr = newJArray()
  for i in 0'u64 ..< reader.functionCount():
    let f = reader.function(i)
    if f.isOk: funcsArr.add(newJString(f.get()))
    else: funcsArr.add(newJString("(error)"))

  let rp = addr reader
  var members = @[held("metadata", meta), held("paths", pathsArr),
                  held("functions", funcsArr)]

  # Steps — pre-fetch all GLIs in one O(N) bulk pass so the per-step
  # loop is a flat seq lookup; using stepAbsoluteGlobalLineIndex(i)
  # in the loop is O(N^2) per call (and O(N^3) end-to-end).
  members.add(streamedArray("steps", proc(emit: EventSink) =
    template reader: untyped = rp[]
    let allGlis = precomputeStepGlis(reader)
    for i in 0'u64 ..< uint64(allGlis.len):
      var stepObj = newJObject()
      stepObj["index"] = unsignedJson(i)
      let loc = resolveStepLocation(reader, gli, allGlis[int(i)])
      if loc.isErr:
        # No `path_id` / `line` / `path` key at all: a consumer that reads
        # them gets a missing key rather than a position that was never
        # in the trace.
        stepObj["position_error"] = newJString(loc.error)
      else:
        let (pathId, line) = loc.get()
        stepObj["path_id"] = newJInt(int64(pathId))
        stepObj["line"] = unsignedJson(line)
        let pathStr = reader.path(uint64(pathId))
        if pathStr.isOk:
          stepObj["path"] = newJString(pathStr.get())
      let ev = reader.step(i)
      if ev.isOk:
        stepObj["kind"] = newJString($ev.get().kind)
      emit(stepObj)))

  # Calls
  members.add(streamedArray("calls", proc(emit: EventSink) =
    template reader: untyped = rp[]
    let cc = reader.callCount()
    if cc.isOk:
      for i in 0'u64 ..< cc.get():
        let c = reader.call(i)
        if c.isOk:
          let rec = c.get()
          var callObj = newJObject()
          callObj["call_key"] = unsignedJson(i)
          callObj["function_id"] = unsignedJson(rec.functionId)
          let fn = reader.function(rec.functionId)
          if fn.isOk:
            callObj["function"] = newJString(fn.get())
          callObj["entry_step"] = unsignedJson(rec.entryStep)
          callObj["exit_step"] = unsignedJson(rec.exitStep)
          callObj["depth"] = newJInt(int64(rec.depth))
          callObj["arg_count"] = newJInt(int64(rec.args.len))
          callObj["children_count"] = newJInt(int64(rec.children.len))
          emit(callObj)))

  # Values (per step)
  members.add(streamedArray("values", proc(emit: EventSink) =
    template reader: untyped = rp[]
    let scForValues = reader.stepCount()
    if scForValues.isOk:
      for i in 0'u64 ..< scForValues.get():
        let vals = reader.valuesForStep(i)
        if vals.isOk:
          for v in vals.get():
            var valObj = newJObject()
            valObj["step"] = unsignedJson(i)
            valObj["varname_id"] = unsignedJson(v.varnameId)
            let vn = reader.varname(v.varnameId)
            if vn.isOk:
              valObj["varname"] = newJString(vn.get())
            valObj["type_id"] = unsignedJson(v.typeId)
            let tn = reader.typeName(v.typeId)
            if tn.isOk:
              valObj["type"] = newJString(tn.get())
            emit(valObj)))

  # IO Events
  members.add(streamedArray("ioEvents", proc(emit: EventSink) =
    template reader: untyped = rp[]
    let ic = reader.ioEventCount()
    if ic.isOk:
      for i in 0'u64 ..< ic.get():
        let ev = reader.ioEvent(i)
        if ev.isOk:
          let e = ev.get()
          var ioObj = newJObject()
          ioObj["index"] = unsignedJson(i)
          ioObj["kind"] = newJString($e.kind)
          ioObj["step_id"] = unsignedJson(e.stepId)
          ioObj["data"] = newJString(bytesToUtf8(e.data))
          emit(ioObj)))

  writePrettyDocument(stdout, members)

# ---------------------------------------------------------------------------
# V4 JSON events (legacy, interleaved by step, no full values)
# ---------------------------------------------------------------------------

proc stepEventToJson(reader: var NewTraceReader, gli: GlobalLineIndex,
    stepIdx: uint64, stepGli: uint64,
    ioEvents: seq[IOEvent], ioIndices: seq[uint64]): seq[JsonNode] =
  ## Produce JSON nodes for a single step: the step itself, its values,
  ## and any IO events at this step.  ``stepGli`` is the precomputed
  ## absolute global_position_index for this step (from
  ## ``precomputeStepGlis``) so the caller's per-step loop stays O(N).
  var nodes: seq[JsonNode]

  # Step event
  var stepObj = newJObject()
  stepObj["type"] = newJString("step")
  stepObj["step_index"] = unsignedJson(stepIdx)

  block resolveStep:
    let loc = resolveStepLocation(reader, gli, stepGli)
    if loc.isErr:
      stepObj["position_error"] = newJString(loc.error)
      break resolveStep
    let (pathId, line) = loc.get()
    stepObj["path_id"] = newJInt(int64(pathId))
    stepObj["line"] = unsignedJson(line)
    let pathStr = reader.path(uint64(pathId))
    if pathStr.isOk:
      stepObj["path"] = newJString(pathStr.get())
    # P1.4: surface the per-step column for column-aware traces so
    # JSON-events consumers (e.g. the
    # ``tests/python/test_column_aware_steps.py`` acceptance harness)
    # can assert that each step in a single-line multi-statement program
    # lands at a distinct column.  We resolve the absolute global
    # position index → ``(file, line, column)`` via the spec-canonical
    # ``decodeGlobalPositionIndex`` algorithm (spec §"Decoding
    # ``global_position_index``").  The decoder is no-op on legacy traces
    # — it errors when the column-aware flag is clear, in which case we
    # leave the column field absent so the JSON output stays bit-for-bit
    # compatible with pre-column-aware consumers.
    if reader.meta.hasColumnAwareSteps:
      let posRes = reader.decodeGlobalPositionIndex(stepGli)
      if posRes.isOk:
        stepObj["column"] = newJInt(int64(posRes.get().column))

  let ev = reader.step(stepIdx)
  if ev.isOk:
    stepObj["kind"] = newJString($ev.get().kind)

  # Resolve enclosing call
  let callRes = reader.callForStep(stepIdx)
  if callRes.isOk:
    let c = callRes.get()
    stepObj["function_id"] = unsignedJson(c.functionId)
    let fn = reader.function(c.functionId)
    if fn.isOk:
      stepObj["function"] = newJString(fn.get())
    stepObj["depth"] = newJInt(int64(c.depth))

  nodes.add(stepObj)

  # Values for this step
  let vals = reader.valuesForStep(stepIdx)
  if vals.isOk:
    for v in vals.get():
      var valObj = newJObject()
      valObj["type"] = newJString("value")
      valObj["step_index"] = unsignedJson(stepIdx)
      valObj["varname_id"] = unsignedJson(v.varnameId)
      let vn = reader.varname(v.varnameId)
      if vn.isOk:
        valObj["varname"] = newJString(vn.get())
      valObj["type_id"] = unsignedJson(v.typeId)
      let tn = reader.typeName(v.typeId)
      if tn.isOk:
        valObj["type_name"] = newJString(tn.get())
      # Decode the CBOR-encoded value bytes into a structured JSON object
      # matching the ValueRecord variant layout.  Consumers that want the
      # raw bytes can still get them under `data_bytes_utf8`.
      valObj["value"] = decodeValueBytesToJson(v.data)
      valObj["data"] = newJString(bytesToUtf8(v.data))
      nodes.add(valObj)

  # IO events at this step
  for idx in 0 ..< ioEvents.len:
    let io = ioEvents[idx]
    var ioObj = newJObject()
    ioObj["type"] = newJString("io")
    ioObj["io_index"] = unsignedJson(ioIndices[idx])
    ioObj["kind"] = newJString($io.kind)
    ioObj["step_id"] = unsignedJson(io.stepId)
    ioObj["data"] = newJString(bytesToUtf8(io.data))
    # The metadata slot, and a correlation marker hoisted out of it when the
    # slot holds one.  `--json-events` used to drop the slot entirely, which
    # made every correlation marker INVISIBLE in this mode while `--full`
    # showed it — so a recorder's own test written against `--json-events`
    # would report a perfectly good marker as missing and send the reader
    # looking for a recorder bug that was not there.  A marker's whole failure
    # mode is being invisible rather than broken; a decoder that hides one is
    # the same defect one level up.
    addEventMetadata(ioObj, io.metadata)
    nodes.add(ioObj)

  nodes

proc printJsonEventsV4(reader: var NewTraceReader) =
  ## The legacy `--json-events` array: declaration events for every path,
  ## function, varname and type, then per exec record its call entries
  ## (call_key ascending), the step, its values and its IO events. Calls
  ## entered, and IO events placed, past the last record are not printed
  ## (unlike `--full`, which drains them).
  ##
  ## Streamed (`streamed_json`), byte-identical to `pretty(eventsArr)` of the
  ## array this used to build. The call entries and IO events at each
  ## record come from indexes sorted once and walked by cursors; the
  ## per-record scan of every call and every IO event this replaced was
  ## O(records x calls).
  let gli = reader.globalPositionSpace()

  # IO events in index order, and positions into them by (stepId, index).
  var ios: seq[(uint64, IOEvent)]  # (index, event)
  let ic = reader.ioEventCount()
  if ic.isOk:
    for i in 0'u64 ..< ic.get():
      let ev = reader.ioEvent(i)
      if ev.isOk:
        ios.add((i, ev.get()))
  var ioByStep = newSeq[int](ios.len)
  for i in 0 ..< ios.len: ioByStep[i] = i
  ioByStep.sort(proc(a, b: int): int =
    let sa = ios[a][1].stepId
    let sb = ios[b][1].stepId
    if sa < sb: -1 elif sa > sb: 1 else: cmp(a, b))

  # Call records in call_key order, and positions by (entryStep, call_key).
  var calls: seq[(uint64, v4calls.CallRecord)]  # (callKey, record)
  let cc = reader.callCount()
  if cc.isOk:
    for i in 0'u64 ..< cc.get():
      let c = reader.call(i)
      if c.isOk:
        calls.add((i, c.get()))
  var byEntry = newSeq[int](calls.len)
  for i in 0 ..< calls.len: byEntry[i] = i
  byEntry.sort(proc(a, b: int): int =
    let ea = calls[a][1].entryStep
    let eb = calls[b][1].entryStep
    if ea < eb: -1 elif ea > eb: 1 else: cmp(a, b))

  let rp = addr reader
  writePrettyArray(stdout, proc(emit: EventSink) =
    template reader: untyped = rp[]
    # Declaration events for paths / functions / varnames / types.  These
    # are emitted up-front so consumers (e.g. ruby's recorder-comparison
    # test) can observe every interned identifier the .ct file carries, in
    # the same order the writer assigned IDs.  Without these,
    # `--json-events` only emits steps/calls/values/io and the consumer has
    # to reverse-engineer the interning tables from inlined name strings —
    # which loses information when the same name is re-used at multiple
    # sites.
    for i in 0'u64 ..< reader.pathCount():
      var obj = newJObject()
      obj["type"] = newJString("path")
      obj["path_id"] = unsignedJson(i)
      let p = reader.path(i)
      if p.isOk:
        obj["name"] = newJString(p.get())
      emit(obj)

    for i in 0'u64 ..< reader.functionCount():
      var obj = newJObject()
      obj["type"] = newJString("function")
      obj["function_id"] = unsignedJson(i)
      let f = reader.function(i)
      if f.isOk:
        obj["name"] = newJString(f.get())
      emit(obj)

    for i in 0'u64 ..< reader.varnameCount():
      var obj = newJObject()
      obj["type"] = newJString("varname")
      obj["varname_id"] = unsignedJson(i)
      let v = reader.varname(i)
      if v.isOk:
        obj["name"] = newJString(v.get())
      emit(obj)

    for i in 0'u64 ..< reader.typeCount():
      var obj = newJObject()
      obj["type"] = newJString("type")
      obj["type_id"] = unsignedJson(i)
      let t = reader.typeName(i)
      if t.isOk:
        obj["name"] = newJString(t.get())
      emit(obj)

    # Pre-fetch all step GLIs in one O(N) pass so the per-step loop
    # below stays linear instead of calling stepAbsoluteGlobalLineIndex
    # per step (which is O(N) each ⇒ O(N²) total).
    let allGlis = precomputeStepGlis(reader)
    var entryCur, ioCur = 0
    for stepIdx in 0'u64 ..< uint64(allGlis.len):
      # Every cursor's remaining items lie at or after `stepIdx`, so the
      # items AT it are a prefix of what remains.
      var stepIo: seq[IOEvent]
      var stepIoIndices: seq[uint64]
      while ioCur < ioByStep.len and ios[ioByStep[ioCur]][1].stepId == stepIdx:
        stepIo.add(ios[ioByStep[ioCur]][1])
        stepIoIndices.add(ios[ioByStep[ioCur]][0])
        inc ioCur

      # Emit call entry events at this step
      while entryCur < byEntry.len and
          calls[byEntry[entryCur]][1].entryStep == stepIdx:
        let (ck, rec) = calls[byEntry[entryCur]]
        inc entryCur
        var callObj = newJObject()
        callObj["type"] = newJString("call")
        callObj["call_key"] = unsignedJson(ck)
        callObj["function_id"] = unsignedJson(rec.functionId)
        let fn = reader.function(rec.functionId)
        if fn.isOk:
          callObj["function"] = newJString(fn.get())
        callObj["entry_step"] = unsignedJson(rec.entryStep)
        callObj["exit_step"] = unsignedJson(rec.exitStep)
        callObj["depth"] = newJInt(int64(rec.depth))
        emit(callObj)

      for n in stepEventToJson(reader, gli, stepIdx, allGlis[int(stepIdx)],
          stepIo, stepIoIndices):
        emit(n))

# ---------------------------------------------------------------------------
# V4 FULL: complete content-faithful JSON dump (with decoded values).
# ---------------------------------------------------------------------------

proc printFullV4(reader: var NewTraceReader, opts: FullOpts) =
  ## Streamed: the document is never held whole, in either form — see
  ## `full_document_json`'s "Streaming the document" section for why, and for
  ## the byte-identity with `pretty(buildFullDocument(...), indent = 2)`.
  writeFullDocument(reader, opts, stdout)

proc printMarkersV4(reader: var NewTraceReader, opts: FullOpts) =
  ## List the correlation markers a trace carries.
  ##
  ## Correlation markers are the substrate of cross-process debugging:
  ## each one records that a value left or entered this recording at a
  ## given source location, tagged with a key that pairs it with the
  ## matching marker in another recording. When a cross-process origin
  ## chain fails to cross a boundary, the cause is almost always visible
  ## here — a missing marker, a mismatched key, or a direction recorded
  ## the wrong way round.
  ##
  ## Without a view like this the failure is silent: the debugger simply
  ## reports a chain that stops early, with nothing to say why.
  # Only the marker events are built: filtering the whole stream decodes
  # every step's variables just to drop them.
  var markers: seq[JsonNode] = @[]
  reader.forEachMarkerEvent(opts, proc(ev: JsonNode) =
    markers.add(ev))

  if opts.jsonOut:
    var arr = newJArray()
    for m in markers:
      arr.add(m)
    echo pretty(arr, indent = 2)
    return

  echo "program: " & reader.meta.program
  echo "correlation markers: " & $markers.len
  if markers.len == 0:
    echo ""
    echo "  (none — this recording declares no boundary crossings, so no"
    echo "   cross-process origin chain can enter or leave it)"
    return
  echo ""
  echo "  #  direction  boundary                  key                  step  location"
  echo "-".repeat(94)
  for i, m in markers:
    let payload = m{"correlation_marker"}
    let direction = payload{"direction"}.getStr("?")
    let boundary = payload{"boundary_id"}.getStr("?")
    let key = payload{"key_value"}.getStr("?")
    let stepId = m{"step_id"}.getInt(-1)
    let showValue =
      if payload{"show_value"} != nil and payload{"show_value"}.kind == JString:
        payload{"show_value"}.getStr()
      else:
        ""
    echo align($(i + 1), 3) & "  " & alignLeft(direction, 9) & "  " &
      alignLeft(boundary, 24) & "  " & alignLeft(key, 19) & "  " &
      align($stepId, 4) & "  " & showValue

proc printCorrelationIndexV4(filePath: string, opts: FullOpts) =
  ## Report the recording's correlation index — `corrmark.ns`.
  ##
  ## THE ONLY VIEW OF SPAN COVERAGE. A boundary crossing also writes a
  ## `MarkerPayload` into the event stream, so `--markers` can show it; a
  ## span-coverage declaration deliberately writes no payload and no event
  ## (contract §10.2), so nothing outside the index records it. Without this
  ## view a recorder that declared coverage and a recorder that dropped the
  ## call on the floor produce byte-identical observable output, which is the
  ## precise failure class this campaign exists to remove.
  ##
  ## THE ABSENT CASE IS REPORTED, NOT ELIDED. A recording with no index has
  ## never been indexed and says nothing about any span; a recording with an
  ## empty index has been indexed and covers none. They are different answers
  ## (contract §9), and reporting both as "0 entries" is what made the
  ## original lookup failure so hard to place.
  ## Dispatched for EVERY container layout, before the native / v4 split.
  ## `corrmark.ns` is a container member, not a stream, so which decoder owns
  ## the event streams is irrelevant here — and routing this through the v4
  ## branch alone would have made the index invisible on exactly the
  ## recordings (native, multi-threaded) whose lookups this campaign is about.
  let dataRes = readContainerImage(filePath)
  if dataRes.isErr:
    quit("Error: " & dataRes.error)
  let data = dataRes.get()
  proc u32le(buf: openArray[byte], off: int): uint32 =
    for i in 0 ..< 4:
      result = result or (uint32(buf[off + i]) shl (i * 8))
  let blockSize = u32le(data, 8)
  # A declared `0` is the auto-filled root (`ctfs-container.md` §1).
  let maxEntries = headerRootEntryCount(data)
  # The program name is a nicety; a container whose meta.dat this build cannot
  # parse must still report its index rather than refusing.
  var program = filePath
  let metaBytes = ctfs_container.readInternalFile(
    data, "meta.dat", blockSize, maxEntries)
  if metaBytes.isOk:
    let parsed = readMetaDat(metaBytes.get())
    if parsed.isOk:
      program = parsed.get().program
  let nsRes = ctfs_container.readInternalFile(
    data, CorrmarkNamespaceName, blockSize, maxEntries)

  if nsRes.isErr:
    if opts.jsonOut:
      var root = newJObject()
      root["correlation_index"] = newJString("absent")
      root["entries"] = newJArray()
      echo pretty(root, indent = 2)
      return
    echo "program: " & program
    echo "correlation index: ABSENT"
    echo ""
    echo "  This recording was never indexed. That is NOT the same as"
    echo "  covering no span: a consumer must report it as unindexed rather"
    echo "  than as a recording that does not cover the span it was asked"
    echo "  about."
    return

  var idxRes = openCorrmarkIndex(nsRes.get())
  if idxRes.isErr:
    quit("Error: corrmark.ns is present but unreadable: " & idxRes.error)
  var idx = idxRes.get()
  let entriesRes = idx.allEntries()
  if entriesRes.isErr:
    quit("Error: " & entriesRes.error)
  let entries = entriesRes.get()

  if opts.jsonOut:
    var arr = newJArray()
    for e in entries:
      var obj = newJObject()
      if e.kind == MarkerKindSpan:
        obj["kind"] = newJString("span_coverage")
        obj["trace_id"] = newJString(e.traceIdHexOf())
        obj["span_id"] = newJString(e.spanIdHexOf())
      else:
        obj["kind"] = newJString("boundary")
        obj["marker_id"] = unsignedJson(e.markerIdOf())
      obj["wall_time_unix_ns"] = unsignedJson(e.wallTimeUnixNs)
      obj["monotonic_time_ns"] = unsignedJson(e.monotonicTimeNs)
      obj["geid"] = unsignedJson(e.geid)
      obj["thread_id"] = unsignedJson(e.threadId)
      obj["exit"] = newJBool((e.flags and MarkerFlagExit) != 0)
      arr.add(obj)
    var root = newJObject()
    root["correlation_index"] = newJString("present")
    root["entries"] = arr
    echo pretty(root, indent = 2)
    return

  echo "program: " & program
  echo "correlation index: present, " & $entries.len & " entr" &
    (if entries.len == 1: "y" else: "ies")
  if entries.len == 0:
    echo ""
    echo "  Indexed, and covering nothing. A lookup against this recording is"
    echo "  a definitive no, unlike one against a recording with no index."
    return
  echo ""
  echo "  #  kind            trace_id / marker                 span_id            geid  wall_time_unix_ns"
  echo "-".repeat(104)
  for i, e in entries:
    let kindText = if e.kind == MarkerKindSpan: "span_coverage" else: "boundary"
    let left =
      if e.kind == MarkerKindSpan: e.traceIdHexOf()
      else: "marker_id=" & $e.markerIdOf()
    let right = if e.kind == MarkerKindSpan: e.spanIdHexOf() else: ""
    echo align($(i + 1), 3) & "  " & alignLeft(kindText, 14) & "  " &
      alignLeft(left, 32) & "  " & alignLeft(right, 17) & "  " &
      align($e.geid, 4) & "  " & $e.wallTimeUnixNs

proc printSpans(filePath: string, jsonOut: bool) =
  ## List the container's SPAN STREAM — the intervals `spans.dat` / `spans.idx`
  ## hold, with `spantype.ns`'s type name resolved onto each record.
  ##
  ## The span stream is the only stream ct-print could not show. It is not an
  ## event kind, so it never appears in `--full` / `--events`, and the two
  ## consumers that need to see it — the request-span panel and the mixed-trace
  ## native<->VM crossings — had no way to inspect a container from a shell.
  ## Without this, "the recording carries N crossing spans" could only be
  ## asserted from inside a Rust or Nim test, which is precisely the kind of
  ## claim that ends up unverified.
  ##
  ## Reads the raw container rather than going through a trace reader: the span
  ## stream is self-describing (`initSpanStreamReader` parses `spans.idx` and
  ## decompresses only the chunks it is asked for), so this works on a
  ## still-growing container and on bundles either reader flavour would reject.
  ##
  ## Records are shown SETTLED (last-record-wins per `span_id`), so a crossing
  ## that was opened and later completed is one row carrying its final
  ## `end_step`, not two.
  let dataR = readContainerImage(filePath)
  if dataR.isErr:
    quit("ct-print: cannot read container: " & dataR.error)
  let bytes = dataR.get()

  # The container's OWN root geometry, from its extended header (block size at
  # byte 8, root-entry count at byte 12), exactly as `native_decoder.nim` reads
  # it. Taking `initSpanStreamReader`'s defaults instead is a silent-wrong-answer
  # bug, and it was one: `DefaultMaxRootEntries` is 31, while a real MCR
  # recording of a 30-thread process declares 128 and parks `spans.dat` in root
  # slot 67 (past the per-thread `tNNN`/`iNNN` pairs). Scanning 31 slots misses
  # it, `readInternalFile` says "internal file not found", and the tool that
  # exists so a span count need not be taken on trust prints `spans: 0` for a
  # container that has 450 bytes of them.
  if bytes.len < HeaderSize + ExtHeaderSize:
    quit("ct-print: file too small to be a CTFS container: " & filePath)
  let blockSize = readU32LE(bytes, 8)
  # A declared `0` is the auto-filled root (`ctfs-container.md` §1,
  # "Auto-fill"), not a nonsensical header.
  let maxRoot = headerRootEntryCount(bytes)
  if blockSize == 0'u32 or maxRoot == 0'u32 or int(maxRoot) > 4096:
    quit("ct-print: nonsensical CTFS root header (blockSize=" & $blockSize &
      " maxRootEntries=" & $maxRoot & ") in " & filePath)

  # "No span stream" and "the span stream is there but would not open" are
  # different answers and must not share an exit. Decide which one it is from
  # the ROOT DIRECTORY — the presence of the member — before attributing an
  # open failure to absence.
  let hasSpanMember = ctfs_container.hasInternalFile(bytes, "spans.dat", maxRoot)

  let rRes = initSpanStreamReader(bytes, blockSize, maxRoot)
  if rRes.isErr:
    if hasSpanMember:
      # The member exists and the reader still refused it: that is damage or a
      # format mismatch, never "zero spans". Fail loudly.
      quit("ct-print: container carries a spans.dat member but its span " &
        "stream would not open: " & rRes.error)
    # A container without `spans.dat` is ordinary — most recordings have no
    # spans. Say so, with the reason, and report zero rather than pretending
    # to have looked.
    if jsonOut:
      echo "[]"
    else:
      echo "spans: 0"
      echo "  (no span stream in this container: " & rRes.error & ")"
    return
  let reader = rRes.get()

  let settledRes = reader.settledSpans()
  if settledRes.isErr:
    quit("ct-print: failed to read the span stream: " & settledRes.error)
  let spans = settledRes.get()

  if jsonOut:
    var arr = newJArray()
    for s in spans:
      var o = newJObject()
      o["span_id"] = %(s.spanId)
      o["parent_span_id"] = %(s.parentSpanId)
      o["span_type"] = %(s.spanType)
      o["label"] = %(s.label)
      o["open"] = %(s.isOpen)
      o["external"] = %(s.isExternal)
      o["status"] = %($s.status)
      o["thread_id"] = %(s.threadId)
      o["process_ord"] = %(s.processOrd)
      o["start_step"] = %(s.startStep)
      o["end_step"] = %(s.endStep)
      o["start_wall_ns"] = %(s.startWallNs)
      o["end_wall_ns"] = %(s.endWallNs)
      var meta = newJObject()
      for (k, v) in s.metadata:
        meta[k] = %v
      o["metadata"] = meta
      arr.add(o)
    try:
      echo pretty(arr, indent = 2)
    except ValueError:
      echo $arr
    return

  echo "spans: " & $spans.len & " settled (" & $reader.count() &
    " records in " & $reader.chunkCount() & " chunk(s))"
  if spans.len == 0:
    return
  echo ""
  echo "  #  span_id  parent  type                 open  start_step  end_step  label"
  echo "-".repeat(94)
  for i, s in spans:
    echo align($(i + 1), 3) & "  " & align($s.spanId, 7) & "  " &
      align($s.parentSpanId, 6) & "  " & alignLeft(s.spanType, 19) & "  " &
      alignLeft((if s.isOpen: "yes" else: "no"), 4) & "  " &
      align($s.startStep, 10) & "  " & align($s.endStep, 8) & "  " & s.label

proc printEventsJsonlV4(reader: var NewTraceReader, opts: FullOpts) =
  ## Emit one JSON object per line. The first line is the header
  ## (metadata + interning tables). Subsequent lines are one event each,
  ## written as each is produced.
  writeEventsJsonl(reader, opts, stdout)

# ---------------------------------------------------------------------------
# V4 text output (human-readable)
# ---------------------------------------------------------------------------

proc printTextV4(reader: var NewTraceReader) =
  let gli = reader.globalPositionSpace()

  echo "=== Trace (v4 multi-stream) ==="
  echo "program: " & reader.meta.program
  if reader.meta.args.len > 0:
    echo "args: " & reader.meta.args.join(" ")
  if reader.meta.workdir.len > 0:
    echo "workdir: " & reader.meta.workdir
  echo ""

  # Pre-load IO events by step
  var ioByStep: seq[(uint64, IOEvent)]
  let ic = reader.ioEventCount()
  if ic.isOk:
    for i in 0'u64 ..< ic.get():
      let ev = reader.ioEvent(i)
      if ev.isOk:
        ioByStep.add((ev.get().stepId, ev.get()))

  # Pre-fetch all step GLIs in one O(N) pass — calling
  # stepAbsoluteGlobalLineIndex per step inside the loop is O(N²).
  let allGlis = precomputeStepGlis(reader)
  if allGlis.len == 0:
    echo "(no steps)"
    return
  let totalSteps = uint64(allGlis.len)
  # Only step records are printed as `Step N`; the other exec records are
  # printed as what they are (trace-events.md §"Which exec records are
  # steps"), each still followed by the I/O events anchored to it.
  let records = reader.execRecords()

  for stepIdx in 0'u64 ..< totalSteps:
    if records.isOk and int(stepIdx) < records.get().len and
        not records.get()[int(stepIdx)].isStepRecord:
      let ev = records.get()[int(stepIdx)]
      case ev.kind
      of sekRaise:
        var tn = $ev.exceptionTypeId
        let t = reader.typeName(ev.exceptionTypeId)
        if t.isOk: tn = t.get()
        echo "Raise (exec " & $stepIdx & "): " & tn & ": " & bytesToUtf8(ev.message)
      of sekCatch:
        var tn = $ev.catchExceptionTypeId
        let t = reader.typeName(ev.catchExceptionTypeId)
        if t.isOk: tn = t.get()
        echo "Catch (exec " & $stepIdx & "): " & tn
      of sekThreadSwitch:
        echo "ThreadSwitch (exec " & $stepIdx & "): thread " & $ev.threadId
      of sekThreadStart:
        echo "ThreadStart (exec " & $stepIdx & "): thread " & $ev.startThreadId
      of sekThreadExit:
        echo "ThreadExit (exec " & $stepIdx & "): thread " & $ev.exitThreadId
      of sekDeltaColumn:
        echo "Column move (exec " & $stepIdx & ")"
      of sekSourceReload:
        echo "SourceReload (exec " & $stepIdx & "): reload " & $ev.reloadOrdinal
      else:
        discard
      for (sid, io) in ioByStep:
        if sid == stepIdx:
          echo "  [" & eventLogKindName(io.kind) & "] " & bytesToUtf8(io.data)
      continue
    var pathStr = "?"
    var lineNum: uint64 = 0
    var posError = ""
    block resolveStep:
      let loc = resolveStepLocation(reader, gli, allGlis[int(stepIdx)])
      if loc.isErr:
        posError = loc.error
        break resolveStep
      let (pathId, line) = loc.get()
      lineNum = line
      let p = reader.path(uint64(pathId))
      if p.isOk:
        pathStr = p.get()

    # Resolve function name
    var funcStr = "?"
    let callRes = reader.callForStep(stepIdx)
    if callRes.isOk:
      let fn = reader.function(callRes.get().functionId)
      if fn.isOk:
        funcStr = fn.get()

    if posError.len > 0:
      echo "Step " & $stepIdx & ": <unresolved position> (" & funcStr & ")"
      echo "  " & posError
    else:
      echo "Step " & $stepIdx & ": " & pathStr & ":" & $lineNum &
        " (" & funcStr & ")"

    # Print values
    let vals = reader.valuesForStep(stepIdx)
    if vals.isOk:
      for v in vals.get():
        var vnStr = "?"
        let vn = reader.varname(v.varnameId)
        if vn.isOk:
          vnStr = vn.get()
        var tnStr = ""
        let tn = reader.typeName(v.typeId)
        if tn.isOk:
          tnStr = tn.get()
        let dataJson = decodeValueBytesToJson(v.data)
        var dataStr: string
        try:
          dataStr = $dataJson
        except ValueError:
          dataStr = bytesToUtf8(v.data)
        if tnStr.len > 0:
          echo "  " & vnStr & ": " & tnStr & " = " & dataStr
        else:
          echo "  " & vnStr & " = " & dataStr

    # Print IO events at this step
    for (sid, ev) in ioByStep:
      if sid == stepIdx:
        echo "  [" & eventLogKindName(ev.kind) & "] " & bytesToUtf8(ev.data)

# ---------------------------------------------------------------------------
# V4 follow mode
# ---------------------------------------------------------------------------

proc followV4(filePath: string, pollMs: int) =
  ## Poll the trace file and print new events as NDJSON.
  var lastStepCount: uint64 = 0
  var lastIoCount: uint64 = 0
  var lastCallCount: uint64 = 0
  var noNewEventPolls = 0
  let maxIdlePolls = 50  # stop after 50 consecutive idle polls (10s at 200ms)

  while true:
    let readerRes = openNewTrace(filePath)
    if readerRes.isErr:
      try:
        sleep(pollMs)
      except:
        discard
      noNewEventPolls += 1
      if noNewEventPolls >= maxIdlePolls:
        break
      continue

    var reader = readerRes.get()
    let gli = reader.globalPositionSpace()
    var hadNewEvents = false

    let sc = reader.stepCount()
    if sc.isOk and sc.get() > lastStepCount:
      # Bulk-fetch GLIs for just the new steps so this tail tick stays
      # linear in the delta size (per-step accessor would be O(K²) for
      # K new events).
      let newCount = sc.get() - lastStepCount
      var newGlis = newSeq[uint64](newCount)
      let fetched = reader.stepAbsoluteGlobalLineIndices(
        lastStepCount, newCount, newGlis)
      let usableGlis = fetched.isOk and fetched.get() == newCount
      for stepIdx in lastStepCount ..< sc.get():
        var stepObj = newJObject()
        stepObj["type"] = newJString("step")
        stepObj["step_index"] = unsignedJson(stepIdx)
        if usableGlis:
          let loc = resolveGli(gli, newGlis[int(stepIdx - lastStepCount)])
          if loc.isErr:
            stepObj["position_error"] = newJString(loc.error)
          else:
            let (pathId, line) = loc.get()
            stepObj["path_id"] = newJInt(int64(pathId))
            stepObj["line"] = unsignedJson(line)
            let pathStr = reader.path(uint64(pathId))
            if pathStr.isOk:
              stepObj["path"] = newJString(pathStr.get())

        let callRes = reader.callForStep(stepIdx)
        if callRes.isOk:
          let c = callRes.get()
          stepObj["function_id"] = unsignedJson(c.functionId)
          let fn = reader.function(c.functionId)
          if fn.isOk:
            stepObj["function"] = newJString(fn.get())

        let vals = reader.valuesForStep(stepIdx)
        if vals.isOk and vals.get().len > 0:
          var valArr = newJArray()
          for v in vals.get():
            var valObj = newJObject()
            valObj["varname_id"] = unsignedJson(v.varnameId)
            let vn = reader.varname(v.varnameId)
            if vn.isOk:
              valObj["varname"] = newJString(vn.get())
            valObj["type_id"] = unsignedJson(v.typeId)
            valObj["value"] = decodeValueBytesToJson(v.data)
            valArr.add(valObj)
          stepObj["values"] = valArr

        echo $stepObj
        hadNewEvents = true
      lastStepCount = sc.get()

    let ic = reader.ioEventCount()
    if ic.isOk and ic.get() > lastIoCount:
      for i in lastIoCount ..< ic.get():
        let ev = reader.ioEvent(i)
        if ev.isOk:
          let e = ev.get()
          var ioObj = newJObject()
          ioObj["type"] = newJString("io")
          ioObj["io_index"] = unsignedJson(i)
          ioObj["kind"] = newJString($e.kind)
          ioObj["step_id"] = unsignedJson(e.stepId)
          ioObj["data"] = newJString(bytesToUtf8(e.data))
          echo $ioObj
          hadNewEvents = true
      lastIoCount = ic.get()

    let cc = reader.callCount()
    if cc.isOk and cc.get() > lastCallCount:
      for i in lastCallCount ..< cc.get():
        let c = reader.call(i)
        if c.isOk:
          let rec = c.get()
          var callObj = newJObject()
          callObj["type"] = newJString("call")
          callObj["call_key"] = unsignedJson(i)
          callObj["function_id"] = unsignedJson(rec.functionId)
          let fn = reader.function(rec.functionId)
          if fn.isOk:
            callObj["function"] = newJString(fn.get())
          callObj["entry_step"] = unsignedJson(rec.entryStep)
          callObj["exit_step"] = unsignedJson(rec.exitStep)
          callObj["depth"] = newJInt(int64(rec.depth))
          echo $callObj
          hadNewEvents = true
      lastCallCount = cc.get()

    if hadNewEvents:
      noNewEventPolls = 0
    else:
      noNewEventPolls += 1

    if noNewEventPolls >= maxIdlePolls:
      break

    try:
      sleep(pollMs)
    except:
      discard

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------

proc printHelp() =
  echo """
ct-print: Convert .ct trace files to human-readable formats.

Usage:
  ct-print <file.ct>                     Print trace as text (default)
  ct-print --summary <file.ct>           Print metadata + counts only
  ct-print --meta-json <file.ct>         JSON dump of meta.dat metadata
                                          (program, args, workdir, recorder,
                                          trace_filter chain) + counts only.
                                          Does NOT decode event streams, so
                                          it stays fast on multi-GB traces.
  ct-print --json <file.ct>              JSON dump (legacy: variable values
                                          are NOT decoded; use --full instead)
  ct-print --json-events <file.ct>       JSON events array (legacy)
  ct-print --full <file.ct>              Pretty-printed JSON dump with FULL
                                          decoded variable values, call args,
                                          return values, IO bytes, etc.
                                          Suitable for golden snapshots.
  ct-print --events <file.ct>            JSONL: header line + one event per
                                          line (compact, diff-friendly).
  ct-print --correlation-index <file.ct> Report the recording's correlation
                                          index: which distributed-trace spans
                                          it covers, and whether it was indexed
                                          at all (an unindexed recording says
                                          nothing about any span, which is not
                                          the same as covering none)
  ct-print --markers <file.ct>           List the trace's correlation
                                          markers — the boundary crossings
                                          that let a cross-process origin
                                          chain enter or leave this
                                          recording. Add --json-out for
                                          machine-readable output.
  ct-print --spans <file.ct>             List the container's span stream
                                          (spans.dat/spans.idx), settled
                                          last-record-wins: request spans,
                                          process spans, and the native<->VM
                                          crossing spans a mixed trace
                                          carries. Add --json-out for
                                          machine-readable output.
  ct-print --follow <file.ct>            Tail the trace as it is written
                                          (NDJSON output).
  ct-print --strip-paths --full <f.ct>   Replace absolute workdir/tmp prefixes
                                          with placeholders for diff-stable
                                          snapshots across machines.
  ct-print --native <file.ct>            Force the native MCR shard decoder
                                          (multi-thread CTFS bundles produced
                                          by codetracer-native-recorder).
                                          Without this flag the same layout
                                          is auto-detected; pass --no-native
                                          to disable auto-detection.

Output for --full / --events:
  Single JSON document with these top-level keys (deterministic ordering):
    metadata, paths, functions, varnames, types, counts, events
  Each entry of `events` has a `kind` field:
    - "call_entry":   call_key, function_id, function, entry_step, exit_step,
                      depth, parent_call_key, args[], children[]
    - "step":         step_index, path_id, line, path, step_kind,
                      function_id, function, depth, vars[]
    - "io":           io_kind (the EventLogKind name: Write, ReadFile, Error, EvmEvent, ...), io_index, step_id,
                      bytes_b64, bytes_len, [text]
    - "call_exit":    call_key, function_id, function, exit_step, depth,
                      return_value, [exception]
  Variable values, call args, and return values are decoded from CBOR into
  structured JSON: {"kind":"Int","i":42,"type_id":7},
  {"kind":"String","text":"hello",...}, structs/sequences/tuples nested,
  references with addresses, BigInts as base64+hex, etc. Addresses, type ids,
  exception type ids and thread ids are unsigned 64-bit numbers and may
  exceed 2^63.
"""

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

proc emitNativeFull(filePath: string, format: string, stripPaths: bool) =
  ## Decode a CodeTracer **native recorder** multi-thread shard bundle (the
  ## layout written by `codetracer-native-recorder/ct_recorder/trace_writer`).
  ## On any decode failure we `quit` with a specific error so callers see
  ## what the decoder didn't understand — never a silent empty document.
  ##
  ## Streamed like the split-stream modes: the decoded records are held, the
  ## document and its rendering are not (`native_decoder.NativeDocument`,
  ## `streamed_json`). The text is byte-identical to `pretty` / `$` of
  ## `buildNativeFullDocument`'s tree.
  var docRes = decodeNativeDocumentFromFile(filePath,
    NativeOpts(stripPaths: stripPaths))
  if docRes.isErr:
    quit("ct-print: native decode failed: " & docRes.error)
  let dp = addr docRes
  let produce = proc(emit: EventSink) = dp[].get().forEachNativeEvent(emit)
  case format
  of "events":
    writeJsonLines(stdout, docRes.get().header, produce)
  else:
    var members = membersOf(docRes.get().header)
    members.add(streamedArray("events", produce))
    writePrettyDocument(stdout, members)

type
  AutoDetectKind = enum
    adkUnreadable    ## file does not exist / cannot be opened
    adkNotCtfs       ## file exists but lacks CTFS magic — let v2/v3 path try
    adkCtfsCorrupt   ## CTFS magic OK but root header looks broken — hard error
    adkNative        ## CTFS with `tNNN` thread streams → native MCR decoder
    adkV4MultiStream ## CTFS with the split per-kind streams → v4 path

proc detectAutoKind(filePath: string): (AutoDetectKind, string) =
  ## Inspect the file once and decide which decode path should claim it.
  ## Returning a (kind, detail) pair lets the caller emit a precise error
  ## message instead of silently falling through to an empty document.
  let dataR = readContainerImage(filePath)
  if dataR.isErr:
    return (adkUnreadable, dataR.error)
  let data = dataR.get()
  if data.len < 16 or not ctfs_container.hasCtfsMagic(data):
    return (adkNotCtfs, "no CTFS magic — not a .ct bundle")
  # Both layouts carry `meta.dat`, so the metadata document does not route
  # anything; `tNNN` per-thread streams are what identify an MCR bundle.
  # A container missing `meta.dat` entirely is left to the v4 reader, which
  # reports the absence with its own message.
  let infoR = probeContainerLayout(data)
  if infoR.isErr:
    if infoR.error.startsWith("meta.dat missing"):
      # A bundle with no `meta.dat` was handed to the v4 reader on the stated
      # understanding that it "reports the absence with its own message". It
      # does not: it opens on anything carrying the magic and prints an empty
      # program with zero counts, exit 0.
      #
      # That was survivable only while the v4 path was selected by the ABSENCE
      # of `events.log`, because a legacy bundle still had one to be absent.
      # No writer emits that stream now, so absence stopped distinguishing
      # anything and every truncated or malformed file became "a v4 recording
      # with nothing in it". Measured on a 512-byte head of a real container.
      #
      # `steps.dat` is the stream the v4 path actually reads, so it is the
      # positive test: no `meta.dat` AND no `steps.dat` is a broken bundle.
      # A presence test on `steps.dat` is NOT enough and was tried: block 0
      # holds the file ENTRIES, so a 512-byte head of a real container still
      # lists them — the entries are there and nothing they point at is. What
      # separates a bundle from its own truncated head is `meta.dat`, which
      # every writer emits and which `detectNativeBundle` has just failed to
      # find.
      return (adkCtfsCorrupt,
        "meta.dat is missing — the bundle is truncated, or it is not a trace container")
    return (adkCtfsCorrupt, infoR.error)
  if infoR.get().threadStreams.len > 0:
    let versionErr = ctfs_types.ctfsVersionError(data)
    if versionErr.len > 0:
      return (adkCtfsCorrupt, "native MCR decoder: " & versionErr)
    return (adkNative, "")
  (adkV4MultiStream, "")

proc main() =
  var format = "text"
  var filePath = ""
  var follow = false
  var pollMs = 200
  var stripPaths = false
  var jsonOut = false
  var nativeMode = "auto"  # "auto" | "force" | "off"

  for kind, key, val in getopt():
    case kind
    of cmdArgument: filePath = key
    of cmdLongOption:
      case key
      of "json": format = "json"
      of "json-events": format = "json-events"
      of "summary": format = "summary"
      of "meta-json": format = "meta-json"
      of "full": format = "full"
      of "events": format = "events"
      of "markers": format = "markers"
      of "correlation-index": format = "correlation-index"
      of "spans": format = "spans"
      of "follow": follow = true
      of "strip-paths": stripPaths = true
      of "json-out": jsonOut = true
      of "native": nativeMode = "force"
      of "no-native": nativeMode = "off"
      of "help", "h": printHelp(); return
      of "poll-interval":
        try:
          pollMs = parseInt(val)
        except ValueError:
          quit("Invalid --poll-interval value: " & val)
      else: quit("Unknown option: " & key)
    of cmdShortOption:
      case key
      of "j": format = "json"
      of "s": format = "summary"
      of "f": follow = true
      of "h": printHelp(); return
      else: quit("Unknown option: " & key)
    of cmdEnd: discard

  if filePath == "":
    printHelp()
    quit(1)

  let opts = FullOpts(stripPaths: stripPaths, jsonOut: jsonOut)

  if format == "correlation-index":
    printCorrelationIndexV4(filePath, opts)
    return

  # ----- Span stream -----
  # Handled before the reader selection below because the span stream is read
  # straight off the container and is therefore independent of which event
  # reader (split streams / native shard) the bundle needs.
  if format == "spans":
    printSpans(filePath, jsonOut)
    return

  # ----- Native MCR shard path (auto-detect or --native) -----
  # The native recorder writes a CTFS shard with per-thread `tNNNN` streams
  # (vs the v4 layout's split per-kind streams + interning tables).
  # Without this branch ct-print used to silently emit `-1` sentinel counts.
  if nativeMode == "force":
    emitNativeFull(filePath, format, stripPaths)
    return

  if nativeMode == "auto" and not follow:
    let (autoKind, detail) = detectAutoKind(filePath)
    case autoKind
    of adkNative:
      emitNativeFull(filePath, format, stripPaths)
      return
    of adkCtfsCorrupt:
      # CTFS magic OK but this build cannot decode the bundle — refuse instead
      # of letting the reader produce a degenerate empty document.
      #
      # NOT "broken". This arm also catches a container from a NEWER writer,
      # whose version this build does not read but which is perfectly
      # well-formed — a version-6 compact container is exactly that
      # (`ctfs-container.md` §1a). `detail` carries the accurate account either
      # way, and asserting brokenness over it would be the diagnosis being
      # wrong in exactly the direction §1c warns about.
      quit("ct-print: refusing to decode this CTFS bundle: " & detail)
    of adkNotCtfs:
      # File exists but is not a CTFS .ct bundle. The legacy v2/v3 reader
      # used to accept arbitrary bytes here and produce a degenerate empty
      # document — refuse instead. (Legacy callers that want the old text
      # path can still use --no-native + a real v2/v3 file.)
      if format == "full" or format == "events" or format == "summary" or
         format == "meta-json":
        quit(
          "ct-print: not a recognised .ct file: " & detail &
          " (path=" & filePath & ")")
    of adkV4MultiStream, adkUnreadable:
      discard  # fall through to v4 reader (or v2/v3 reader if v4 fails)

  # A container that carries a member that is not part of the trace format
  # (`events.log`, `events.fmt`) is refused by name, and one with no
  # `steps.dat` -- the stream every recording is read from -- is refused rather
  # than printed as an empty program with zero counts.
  #
  # Follow mode polls a growing container whose streams may not exist yet; it
  # refuses retired members through the reader's own check at each open.
  block decideLayout:
    if follow:
      break decideLayout
    let dataR = readContainerImage(filePath)
    if dataR.isErr:
      break decideLayout  # unreadable here ⇒ let openNewTrace surface the error
    let layoutBytes = dataR.get()
    if not ctfs_container.hasCtfsMagic(layoutBytes):
      break decideLayout  # non-CTFS ⇒ openNewTrace reports it
    let retired = refuseRetiredMembers(layoutBytes, DefaultMaxRootEntries)
    if retired.isErr:
      quit("ct-print: " & filePath & ": " & retired.error)
    if not ctfs_container.hasInternalFile(layoutBytes, "steps.dat"):
      stderr.writeLine("ct-print: " & filePath &
        " carries no `steps.dat` (the execution stream). It is truncated, or " &
        "it is not a trace container.")
      quit(1)

  let newReaderRes = openNewTrace(filePath)
  if newReaderRes.isErr:
    quit("Error: " & newReaderRes.unsafeError)
  if follow:
    followV4(filePath, pollMs)
  else:
    var reader = newReaderRes.get()
    case format
    of "summary": printSummaryV4(reader)
    of "markers": printMarkersV4(reader, opts)
    of "meta-json": printMetaJsonV4(reader)
    of "json": printJsonV4(reader)
    of "json-events": printJsonEventsV4(reader)
    of "full": printFullV4(reader, opts)
    of "events": printEventsJsonlV4(reader, opts)
    else: printTextV4(reader)

main()
