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

import std/[os, parseopt, json, strutils, base64, algorithm]
import results
import codetracer_trace_reader
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/call_stream as v4calls
import codetracer_trace_writer/io_event_stream
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/global_line_index
import codetracer_trace_writer/multi_stream_writer  # for DefaultLinesPerFile
import codetracer_trace_writer/cbor
import codetracer_trace_types
import codetracer_ctfs/container as ctfs_container
import native_decoder

# ---------------------------------------------------------------------------
# Global line index resolution for v4 traces
# ---------------------------------------------------------------------------

proc buildGliFromMeta(meta: MetaDatContents): GlobalLineIndex =
  ## Rebuild the global line index from the meta.dat paths list
  ## using the same DefaultLinesPerFile the writer uses.
  var counts = newSeq[uint64](meta.paths.len)
  for i in 0 ..< meta.paths.len:
    counts[i] = DefaultLinesPerFile
  buildGlobalLineIndex(counts)

proc resolveGli(gli: GlobalLineIndex, globalIdx: uint64): (int, uint64) =
  ## Convert global line index to (pathId, line).
  gli.resolve(globalIdx)

proc resolveStepLocation(reader: var NewTraceReader,
    gli: GlobalLineIndex, stepGli: uint64): (int, uint64) =
  ## Resolve a step's absolute ``global_position_index`` to ``(pathId,
  ## line)``.  Column-aware traces encode GLI as a byte-offset (cumulative
  ## sum of preceding line_lengths), so the legacy line-count-based
  ## ``gli.resolve`` returns garbage on them.  Route through the spec-
  ## canonical ``decodeGlobalPositionIndex`` when the column-aware flag is
  ## set; fall back to the line-count resolver for legacy traces.
  if reader.meta.hasColumnAwareSteps:
    let posRes = reader.decodeGlobalPositionIndex(stepGli)
    if posRes.isOk:
      return (int(posRes.get().file), uint64(posRes.get().line))
  gli.resolve(stepGli)

proc precomputeStepGlis(reader: var NewTraceReader): seq[uint64] =
  ## Walk the exec stream once and return a seq mapping step_index →
  ## absolute global_position_index.  ct-print's per-step JSON loops
  ## use this so they stay O(N) — calling
  ## ``stepAbsoluteGlobalLineIndex(i)`` per step is O(N²) (and the
  ## outer loop made the whole emission O(N³) before this helper).
  ## Empty seq on any failure; callers should fall back to omitting
  ## path / line / column data for the step in that case.
  let scR = reader.stepCount()
  if scR.isErr:
    return @[]
  let n = scR.get()
  if n == 0:
    return @[]
  result = newSeq[uint64](n)
  let fetched = reader.stepAbsoluteGlobalLineIndices(0'u64, n, result)
  if fetched.isErr or fetched.get() != n:
    return @[]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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

proc bytesToUtf8(data: seq[byte]): string =
  result = newString(data.len)
  for i in 0 ..< data.len:
    result[i] = char(data[i])

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
# ValueRecord -> JsonNode
# ---------------------------------------------------------------------------

proc valueRecordToJson*(v: ValueRecord): JsonNode =
  ## Convert a decoded ValueRecord into a structured JSON node, surfacing
  ## every variant of the tagged union. Output is deterministic — keys
  ## appear in a fixed order so golden snapshots stay stable.
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
    # `text` carries the printed boolean ("true"|"false") so consumers
    # querying `value.text` get the printed form regardless of `kind`.
    # Matches the 4-key CBOR map produced by writeBool / encodeCborValueRecord.
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
    result["b"] = newJString(base64.encode(v.bigIntBytes))
    var biHex = ""
    for byteVal in v.bigIntBytes:
      biHex.add(toHex(int(byteVal), 2).toLowerAscii())
    result["b_hex"] = newJString(biHex)
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

proc decodeValueBytesToJson*(data: seq[byte]): JsonNode =
  ## Decode CBOR-encoded value bytes into a structured JSON node.
  ## On decode error, returns a fallback {"kind":"Undecodable","raw":...}.
  if data.len == 0:
    var node = newJObject()
    node["kind"] = newJString("Empty")
    return node
  # Special case: void return marker (single 0xFF byte) used in call_stream.
  if data.len == 1 and data[0] == VoidReturnMarker:
    var node = newJObject()
    node["kind"] = newJString("Void")
    return node
  var dec = CborDecoder.init(data)
  let res = decodeCborValueRecord(dec)
  if res.isOk:
    return valueRecordToJson(res.get())
  else:
    var node = newJObject()
    node["kind"] = newJString("Undecodable")
    # Avoid `.error` (results' getter may have side-effect-permitting raise).
    # Use `errorOr`-equivalent pattern via unsafeError which is plain readonly.
    node["error"] = newJString(res.unsafeError)
    var hex = ""
    for b in data:
      hex.add(toHex(int(b), 2).toLowerAscii())
    node["raw_hex"] = newJString(hex)
    return node

# ---------------------------------------------------------------------------
# Path normalization
# ---------------------------------------------------------------------------

proc normalizePath(s: string, stripWorkdir: string, stripPaths: bool): string =
  ## If --strip-paths is set, strip leading workdir prefix and any
  ## /tmp/... or absolute path prefixes so traces are diff-friendly.
  if not stripPaths:
    return s
  if stripWorkdir.len > 0 and s.startsWith(stripWorkdir):
    var rest = s[stripWorkdir.len .. ^1]
    if rest.len > 0 and rest[0] == '/':
      rest = rest[1 .. ^1]
    return "<workdir>/" & rest
  # Strip /tmp/<random>/ prefix
  if s.startsWith("/tmp/"):
    let parts = s.split('/')
    if parts.len >= 4:
      return "<tmp>/" & parts[3 .. ^1].join("/")
  s

# ---------------------------------------------------------------------------
# V4 summary
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

  let sc = reader.stepCount()
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
  var flagsObj = newJObject()
  flagsObj["has_column_aware_steps"] = newJBool(reader.meta.hasColumnAwareSteps)
  flagsObj["has_alternate_source_views"] = newJBool(
    reader.meta.hasAlternateSourceViews)
  flagsObj["supports_column_breakpoints"] = newJBool(
    reader.meta.supportsColumnBreakpoints)
  flagsObj["supports_column_motions"] = newJBool(
    reader.meta.supportsColumnMotions)
  flagsObj["has_call_stream"] = newJBool(reader.meta.hasCallStream)
  flagsObj["has_step_stream"] = newJBool(reader.meta.hasStepStream)
  meta["flags"] = flagsObj

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
  if sc.isOk: counts["steps"] = newJInt(int64(sc.get()))
  if cc.isOk: counts["calls"] = newJInt(int64(cc.get()))
  if vc.isOk: counts["values"] = newJInt(int64(vc.get()))
  if ic.isOk: counts["io_events"] = newJInt(int64(ic.get()))
  counts["paths"] = newJInt(int64(reader.pathCount()))
  counts["functions"] = newJInt(int64(reader.functionCount()))
  counts["types"] = newJInt(int64(reader.typeCount()))
  counts["varnames"] = newJInt(int64(reader.varnameCount()))
  root["counts"] = counts

  echo pretty(root)

# ---------------------------------------------------------------------------
# V4 JSON (full dump - legacy, value bytes are passed through as text)
# ---------------------------------------------------------------------------

proc printJsonV4(reader: var NewTraceReader) =
  let gli = buildGliFromMeta(reader.meta)

  var root = newJObject()

  # Metadata
  var meta = newJObject()
  meta["program"] = newJString(reader.meta.program)
  var argsArr = newJArray()
  for arg in reader.meta.args:
    argsArr.add(newJString(arg))
  meta["args"] = argsArr
  meta["workdir"] = newJString(reader.meta.workdir)
  root["metadata"] = meta

  # Paths
  var pathsArr = newJArray()
  for i in 0'u64 ..< reader.pathCount():
    let p = reader.path(i)
    if p.isOk: pathsArr.add(newJString(p.get()))
    else: pathsArr.add(newJString("(error)"))
  root["paths"] = pathsArr

  # Functions
  var funcsArr = newJArray()
  for i in 0'u64 ..< reader.functionCount():
    let f = reader.function(i)
    if f.isOk: funcsArr.add(newJString(f.get()))
    else: funcsArr.add(newJString("(error)"))
  root["functions"] = funcsArr

  # Steps — pre-fetch all GLIs in one O(N) bulk pass so the per-step
  # loop is a flat seq lookup; using stepAbsoluteGlobalLineIndex(i)
  # in the loop is O(N^2) per call (and O(N^3) end-to-end).
  var stepsArr = newJArray()
  let allGlis = precomputeStepGlis(reader)
  if allGlis.len > 0:
    for i in 0'u64 ..< uint64(allGlis.len):
      var stepObj = newJObject()
      stepObj["index"] = newJInt(int64(i))
      let (pathId, line) = resolveStepLocation(reader, gli, allGlis[int(i)])
      stepObj["path_id"] = newJInt(int64(pathId))
      stepObj["line"] = newJInt(int64(line))
      let pathStr = reader.path(uint64(pathId))
      if pathStr.isOk:
        stepObj["path"] = newJString(pathStr.get())
      let ev = reader.step(i)
      if ev.isOk:
        stepObj["kind"] = newJString($ev.get().kind)
      stepsArr.add(stepObj)
  root["steps"] = stepsArr

  # Calls
  var callsArr = newJArray()
  let cc = reader.callCount()
  if cc.isOk:
    for i in 0'u64 ..< cc.get():
      let c = reader.call(i)
      if c.isOk:
        let rec = c.get()
        var callObj = newJObject()
        callObj["call_key"] = newJInt(int64(i))
        callObj["function_id"] = newJInt(int64(rec.functionId))
        let fn = reader.function(rec.functionId)
        if fn.isOk:
          callObj["function"] = newJString(fn.get())
        callObj["entry_step"] = newJInt(int64(rec.entryStep))
        callObj["exit_step"] = newJInt(int64(rec.exitStep))
        callObj["depth"] = newJInt(int64(rec.depth))
        callObj["arg_count"] = newJInt(int64(rec.args.len))
        callObj["children_count"] = newJInt(int64(rec.children.len))
        callsArr.add(callObj)
  root["calls"] = callsArr

  # Values (per step)
  var valuesArr = newJArray()
  let scForValues = reader.stepCount()
  if scForValues.isOk:
    for i in 0'u64 ..< scForValues.get():
      let vals = reader.values(i)
      if vals.isOk:
        for v in vals.get():
          var valObj = newJObject()
          valObj["step"] = newJInt(int64(i))
          valObj["varname_id"] = newJInt(int64(v.varnameId))
          let vn = reader.varname(v.varnameId)
          if vn.isOk:
            valObj["varname"] = newJString(vn.get())
          valObj["type_id"] = newJInt(int64(v.typeId))
          let tn = reader.typeName(v.typeId)
          if tn.isOk:
            valObj["type"] = newJString(tn.get())
          valuesArr.add(valObj)
  root["values"] = valuesArr

  # IO Events
  var ioArr = newJArray()
  let ic = reader.ioEventCount()
  if ic.isOk:
    for i in 0'u64 ..< ic.get():
      let ev = reader.ioEvent(i)
      if ev.isOk:
        let e = ev.get()
        var ioObj = newJObject()
        ioObj["index"] = newJInt(int64(i))
        ioObj["kind"] = newJString($e.kind)
        ioObj["step_id"] = newJInt(int64(e.stepId))
        ioObj["data"] = newJString(bytesToUtf8(e.data))
        ioArr.add(ioObj)
  root["ioEvents"] = ioArr

  try:
    echo pretty(root)
  except ValueError:
    echo $root

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
  stepObj["step_index"] = newJInt(int64(stepIdx))

  block resolveStep:
    let (pathId, line) = resolveStepLocation(reader, gli, stepGli)
    stepObj["path_id"] = newJInt(int64(pathId))
    stepObj["line"] = newJInt(int64(line))
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
    stepObj["function_id"] = newJInt(int64(c.functionId))
    let fn = reader.function(c.functionId)
    if fn.isOk:
      stepObj["function"] = newJString(fn.get())
    stepObj["depth"] = newJInt(int64(c.depth))

  nodes.add(stepObj)

  # Values for this step
  let vals = reader.values(stepIdx)
  if vals.isOk:
    for v in vals.get():
      var valObj = newJObject()
      valObj["type"] = newJString("value")
      valObj["step_index"] = newJInt(int64(stepIdx))
      valObj["varname_id"] = newJInt(int64(v.varnameId))
      let vn = reader.varname(v.varnameId)
      if vn.isOk:
        valObj["varname"] = newJString(vn.get())
      valObj["type_id"] = newJInt(int64(v.typeId))
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
    ioObj["io_index"] = newJInt(int64(ioIndices[idx]))
    ioObj["kind"] = newJString($io.kind)
    ioObj["step_id"] = newJInt(int64(io.stepId))
    ioObj["data"] = newJString(bytesToUtf8(io.data))
    nodes.add(ioObj)

  nodes

proc printJsonEventsV4(reader: var NewTraceReader) =
  let gli = buildGliFromMeta(reader.meta)

  # Pre-load all IO events indexed by stepId for quick lookup
  var ioByStep: seq[(uint64, IOEvent, uint64)]  # (stepId, event, index)
  let ic = reader.ioEventCount()
  if ic.isOk:
    for i in 0'u64 ..< ic.get():
      let ev = reader.ioEvent(i)
      if ev.isOk:
        ioByStep.add((ev.get().stepId, ev.get(), i))

  # Also emit call events at their entry steps
  var callEntries: seq[(uint64, v4calls.CallRecord, uint64)]  # (entryStep, record, callKey)
  let cc = reader.callCount()
  if cc.isOk:
    for i in 0'u64 ..< cc.get():
      let c = reader.call(i)
      if c.isOk:
        callEntries.add((c.get().entryStep, c.get(), i))

  var eventsArr = newJArray()

  # Declaration events for paths / functions / varnames / types.  These are
  # emitted up-front so consumers (e.g. ruby's recorder-comparison test) can
  # observe every interned identifier the .ct file carries, in the same
  # order the writer assigned IDs.  Without these, `--json-events` only
  # emits steps/calls/values/io and the consumer has to reverse-engineer
  # the interning tables from inlined name strings — which loses information
  # when the same name is re-used at multiple sites.
  for i in 0'u64 ..< reader.pathCount():
    var obj = newJObject()
    obj["type"] = newJString("path")
    obj["path_id"] = newJInt(int64(i))
    let p = reader.path(i)
    if p.isOk:
      obj["name"] = newJString(p.get())
    eventsArr.add(obj)

  for i in 0'u64 ..< reader.functionCount():
    var obj = newJObject()
    obj["type"] = newJString("function")
    obj["function_id"] = newJInt(int64(i))
    let f = reader.function(i)
    if f.isOk:
      obj["name"] = newJString(f.get())
    eventsArr.add(obj)

  for i in 0'u64 ..< reader.varnameCount():
    var obj = newJObject()
    obj["type"] = newJString("varname")
    obj["varname_id"] = newJInt(int64(i))
    let v = reader.varname(i)
    if v.isOk:
      obj["name"] = newJString(v.get())
    eventsArr.add(obj)

  for i in 0'u64 ..< reader.typeCount():
    var obj = newJObject()
    obj["type"] = newJString("type")
    obj["type_id"] = newJInt(int64(i))
    let t = reader.typeName(i)
    if t.isOk:
      obj["name"] = newJString(t.get())
    eventsArr.add(obj)

  # Pre-fetch all step GLIs in one O(N) pass so the per-step loop
  # below stays linear instead of calling stepAbsoluteGlobalLineIndex
  # per step (which is O(N) each ⇒ O(N²) total).
  let allGlis = precomputeStepGlis(reader)
  if allGlis.len > 0:
    for stepIdx in 0'u64 ..< uint64(allGlis.len):
      # Collect IO events for this step
      var stepIo: seq[IOEvent]
      var stepIoIndices: seq[uint64]
      for (sid, ev, idx) in ioByStep:
        if sid == stepIdx:
          stepIo.add(ev)
          stepIoIndices.add(idx)

      # Emit call entry events at this step
      for (es, rec, ck) in callEntries:
        if es == stepIdx:
          var callObj = newJObject()
          callObj["type"] = newJString("call")
          callObj["call_key"] = newJInt(int64(ck))
          callObj["function_id"] = newJInt(int64(rec.functionId))
          let fn = reader.function(rec.functionId)
          if fn.isOk:
            callObj["function"] = newJString(fn.get())
          callObj["entry_step"] = newJInt(int64(rec.entryStep))
          callObj["exit_step"] = newJInt(int64(rec.exitStep))
          callObj["depth"] = newJInt(int64(rec.depth))
          eventsArr.add(callObj)

      let nodes = stepEventToJson(reader, gli, stepIdx, allGlis[int(stepIdx)],
        stepIo, stepIoIndices)
      for n in nodes:
        eventsArr.add(n)

  try:
    echo pretty(eventsArr)
  except ValueError:
    echo $eventsArr

# ---------------------------------------------------------------------------
# V4 FULL: complete content-faithful JSON dump (with decoded values).
# ---------------------------------------------------------------------------

type FullOpts = object
  stripPaths: bool

proc buildFullDocument(reader: var NewTraceReader,
    opts: FullOpts): JsonNode =
  ## Build the deterministic JSON document for `--full` and `--events` modes.
  ## The shape is:
  ##   { metadata, paths, functions, varnames, types,
  ##     events: [ {kind: "...", ...}, ... ] }
  ## All variable values and call args/returns are decoded from CBOR into
  ## structured JSON objects matching the ValueRecord variant layout.
  let gli = buildGliFromMeta(reader.meta)
  var root = newJObject()

  # ----- metadata -----
  var meta = newJObject()
  # TF-M4d / TF-M5-Prep-2 (Blocker 3): route `metadata.program` through
  # the same `normalizePath` walk that `paths[]` and `metadata.workdir`
  # go through. Without this, `--strip-paths` output retained the
  # absolute `/home/<user>/...` form for the `program` field alone,
  # leaking the developer's filesystem layout into snapshots.
  meta["program"] = newJString(
    normalizePath(reader.meta.program, reader.meta.workdir, opts.stripPaths))
  var argsArr = newJArray()
  for a in reader.meta.args:
    argsArr.add(newJString(a))
  meta["args"] = argsArr
  meta["workdir"] = newJString(
    if opts.stripPaths and reader.meta.workdir.len > 0: "<workdir>"
    else: reader.meta.workdir)
  meta["recorder"] = newJString(reader.meta.recorderId)

  # ----- meta.dat flag bits surfaced under `metadata.flags` -----
  # Stable JSON anchor for golden tests: every flag bit gets its
  # own boolean field, defaulting false on traces written before
  # the flag was introduced.
  var flagsObj = newJObject()
  flagsObj["has_column_aware_steps"] = newJBool(reader.meta.hasColumnAwareSteps)
  flagsObj["has_alternate_source_views"] = newJBool(
    reader.meta.hasAlternateSourceViews)
  flagsObj["supports_column_breakpoints"] = newJBool(
    reader.meta.supportsColumnBreakpoints)
  flagsObj["supports_column_motions"] = newJBool(
    reader.meta.supportsColumnMotions)
  flagsObj["has_call_stream"] = newJBool(reader.meta.hasCallStream)
  flagsObj["has_step_stream"] = newJBool(reader.meta.hasStepStream)
  meta["flags"] = flagsObj

  # ----- trace_filter provenance (TF-M7, spec §7) -----
  # Materialized as `metadata.trace_filter.filters[].{path,sha256}` per
  # Trace-Filters.md § 7.  Emitted only when the meta.dat header had
  # FlagHasTraceFilterProvenance set; absent (vs present-but-empty)
  # distinguishes "did not record" from "recorded an empty chain".
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

  # ----- paths -----
  var pathsArr = newJArray()
  for i in 0'u64 ..< reader.pathCount():
    let p = reader.path(i)
    let s = if p.isOk: p.get() else: "(error)"
    pathsArr.add(newJString(
      normalizePath(s, reader.meta.workdir, opts.stripPaths)))
  root["paths"] = pathsArr

  # ----- functions -----
  var funcsArr = newJArray()
  for i in 0'u64 ..< reader.functionCount():
    let f = reader.function(i)
    funcsArr.add(newJString(if f.isOk: f.get() else: "(error)"))
  root["functions"] = funcsArr

  # ----- varnames -----
  var vnArr = newJArray()
  for i in 0'u64 ..< reader.varnameCount():
    let vn = reader.varname(i)
    vnArr.add(newJString(if vn.isOk: vn.get() else: "(error)"))
  root["varnames"] = vnArr

  # ----- types -----
  var typesArr = newJArray()
  for i in 0'u64 ..< reader.typeCount():
    let tn = reader.typeName(i)
    typesArr.add(newJString(if tn.isOk: tn.get() else: "(error)"))
  root["types"] = typesArr

  # ----- source_views (Alternate Source Views, Deminification Support) -----
  # Inline content/sourcemap bytes would blow up the JSON, so we surface
  # only their lengths — golden tests anchor on the per-view metadata
  # and the recorded byte counts.
  var sourceViewsArr = newJArray()
  for i in 0'u64 ..< reader.sourceViewCount():
    let svRes = reader.sourceView(i)
    if svRes.isOk:
      let sv = svRes.get()
      var svObj = newJObject()
      svObj["path_id"] = newJInt(int64(sv.pathId))
      svObj["view_kind"] = newJInt(int64(sv.viewKind))
      svObj["view_name"] = newJString(sv.viewName)
      svObj["content_len"] = newJInt(int64(sv.content.len))
      svObj["map_len"] = newJInt(int64(sv.sourcemapV3.len))
      sourceViewsArr.add(svObj)
  root["source_views"] = sourceViewsArr

  # ----- counts (for golden anchoring) -----
  var counts = newJObject()
  counts["paths"] = newJInt(int64(reader.pathCount()))
  counts["functions"] = newJInt(int64(reader.functionCount()))
  counts["varnames"] = newJInt(int64(reader.varnameCount()))
  counts["types"] = newJInt(int64(reader.typeCount()))
  counts["source_views"] = newJInt(int64(reader.sourceViewCount()))
  # Use logicalStepCount so user-facing "steps" excludes DeltaColumn
  # nudges — column-aware traces interleave them with line moves but
  # they are not logical source-line steps.
  let scR = reader.logicalStepCount()
  counts["steps"] = newJInt(if scR.isOk: int64(scR.get()) else: -1)
  let ccR = reader.callCount()
  counts["calls"] = newJInt(if ccR.isOk: int64(ccR.get()) else: -1)
  let vcR = reader.valueCount()
  counts["values"] = newJInt(if vcR.isOk: int64(vcR.get()) else: -1)
  let icR = reader.ioEventCount()
  counts["io_events"] = newJInt(if icR.isOk: int64(icR.get()) else: -1)
  root["counts"] = counts

  # ----- events (interleaved, source-order) -----
  # Collection helpers: pre-load IO events and call entries indexed by step.
  var ioByStep: seq[(uint64, IOEvent, uint64)]
  let icRes = reader.ioEventCount()
  if icRes.isOk:
    for i in 0'u64 ..< icRes.get():
      let ev = reader.ioEvent(i)
      if ev.isOk:
        ioByStep.add((ev.get().stepId, ev.get(), i))

  var callsByEntry: seq[(uint64, v4calls.CallRecord, uint64)]
  var callsByExit: seq[(uint64, v4calls.CallRecord, uint64)]
  let ccRes = reader.callCount()
  if ccRes.isOk:
    for i in 0'u64 ..< ccRes.get():
      let c = reader.call(i)
      if c.isOk:
        callsByEntry.add((c.get().entryStep, c.get(), i))
        callsByExit.add((c.get().exitStep, c.get(), i))
  # call_exit ordering: at the same exit_step, LIFO (innermost frame
  # closes first).  The natural iteration order is call_key ASC (i.e.
  # the order calls were registered), which is FIFO — wrong when the
  # writer's close()-time drain places parent and child at the same
  # exitStep (parent has no post-recursion body step, so both share
  # the last step).  Sort by (exitStep ASC, call_key DESC) so the
  # innermost call's exit comes first in events at any shared step.
  callsByExit.sort(proc(a, b: (uint64, v4calls.CallRecord, uint64)): int =
    if a[0] < b[0]: -1
    elif a[0] > b[0]: 1
    elif a[2] > b[2]: -1
    elif a[2] < b[2]: 1
    else: 0)

  var eventsArr = newJArray()

  # Pre-fetch all step GLIs in one O(N) pass — calling
  # stepAbsoluteGlobalLineIndex per step inside the loop is O(N²).
  let allGlis = precomputeStepGlis(reader)
  if allGlis.len > 0:
    for stepIdx in 0'u64 ..< uint64(allGlis.len):
      let stepGli = allGlis[int(stepIdx)]
      # 1. Emit call entry events at this step (deterministic depth-asc order).
      for (es, rec, ck) in callsByEntry:
        if es == stepIdx:
          var callObj = newJObject()
          callObj["kind"] = newJString("call_entry")
          callObj["call_key"] = newJInt(int64(ck))
          callObj["function_id"] = newJInt(int64(rec.functionId))
          let fn = reader.function(rec.functionId)
          if fn.isOk:
            callObj["function"] = newJString(fn.get())
          callObj["entry_step"] = newJInt(int64(rec.entryStep))
          callObj["exit_step"] = newJInt(int64(rec.exitStep))
          callObj["depth"] = newJInt(int64(rec.depth))
          callObj["parent_call_key"] = newJInt(rec.parentCallKey)
          var argsJson = newJArray()
          for arg in rec.args:
            var argObj = newJObject()
            argObj["varname_id"] = newJInt(int64(arg.varnameId))
            let argVn = reader.varname(arg.varnameId)
            if argVn.isOk:
              argObj["varname"] = newJString(argVn.get())
            argObj["value"] = decodeValueBytesToJson(arg.value)
            argsJson.add(argObj)
          callObj["args"] = argsJson
          var childrenJson = newJArray()
          for c in rec.children:
            childrenJson.add(newJInt(int64(c)))
          callObj["children"] = childrenJson
          eventsArr.add(callObj)

      # 2. Emit the step event itself.
      var stepObj = newJObject()
      stepObj["kind"] = newJString("step")
      stepObj["step_index"] = newJInt(int64(stepIdx))
      block emitStep:
        let (pathId, line) = resolveStepLocation(reader, gli, stepGli)
        stepObj["path_id"] = newJInt(int64(pathId))
        stepObj["line"] = newJInt(int64(line))
        let pStr = reader.path(uint64(pathId))
        if pStr.isOk:
          stepObj["path"] = newJString(
            normalizePath(pStr.get(), reader.meta.workdir, opts.stripPaths))
        # Column-aware traces: surface the step's column by decoding the
        # absolute global_position_index per the spec.  Pre-extension
        # traces leave the field absent so the JSON output stays
        # bit-for-bit compatible with pre-column-aware consumers.
        if reader.meta.hasColumnAwareSteps:
          let posRes = reader.decodeGlobalPositionIndex(stepGli)
          if posRes.isOk:
            stepObj["column"] = newJInt(int64(posRes.get().column))
      let stepEv = reader.step(stepIdx)
      if stepEv.isOk:
        let se = stepEv.get()
        stepObj["step_kind"] = newJString($se.kind)
        case se.kind
        of sekRaise:
          stepObj["exception_type_id"] = newJInt(int64(se.exceptionTypeId))
          stepObj["exception_message"] = newJString(bytesToUtf8(se.message))
        of sekCatch:
          stepObj["catch_exception_type_id"] = newJInt(int64(se.catchExceptionTypeId))
        of sekThreadStart:
          stepObj["thread_id"] = newJInt(int64(se.startThreadId))
        of sekThreadExit:
          stepObj["thread_id"] = newJInt(int64(se.exitThreadId))
        of sekThreadSwitch:
          stepObj["thread_id"] = newJInt(int64(se.threadId))
        else:
          discard
      let callForStep = reader.callForStep(stepIdx)
      if callForStep.isOk:
        let cs = callForStep.get()
        stepObj["function_id"] = newJInt(int64(cs.functionId))
        let fn = reader.function(cs.functionId)
        if fn.isOk:
          stepObj["function"] = newJString(fn.get())
        stepObj["depth"] = newJInt(int64(cs.depth))
      # Variable values (decoded)
      var valsArr = newJArray()
      let vals = reader.values(stepIdx)
      if vals.isOk:
        for v in vals.get():
          var vObj = newJObject()
          vObj["varname_id"] = newJInt(int64(v.varnameId))
          let vn = reader.varname(v.varnameId)
          if vn.isOk:
            vObj["varname"] = newJString(vn.get())
          vObj["type_id"] = newJInt(int64(v.typeId))
          let tn = reader.typeName(v.typeId)
          if tn.isOk:
            vObj["type_name"] = newJString(tn.get())
          vObj["value"] = decodeValueBytesToJson(v.data)
          valsArr.add(vObj)
      stepObj["vars"] = valsArr
      eventsArr.add(stepObj)

      # 3. Emit IO events at this step.
      for (sid, ev, idx) in ioByStep:
        if sid == stepIdx:
          var ioObj = newJObject()
          ioObj["kind"] = newJString("io")
          ioObj["io_kind"] = newJString($ev.kind)
          ioObj["io_index"] = newJInt(int64(idx))
          ioObj["step_id"] = newJInt(int64(ev.stepId))
          # Surface both UTF-8 (best-effort) and base64 (exact bytes) so
          # binary payloads are diff-friendly without losing fidelity.
          var allPrintable = true
          for b in ev.data:
            if b < 0x20 and b != 0x0A and b != 0x0D and b != 0x09:
              allPrintable = false
              break
          if allPrintable:
            ioObj["text"] = newJString(bytesToUtf8(ev.data))
          ioObj["bytes_b64"] = newJString(base64.encode(ev.data))
          ioObj["bytes_len"] = newJInt(int64(ev.data.len))
          eventsArr.add(ioObj)

      # 4. Emit call exit events at this step.
      for (es, rec, ck) in callsByExit:
        if es == stepIdx:
          var exitObj = newJObject()
          exitObj["kind"] = newJString("call_exit")
          exitObj["call_key"] = newJInt(int64(ck))
          exitObj["function_id"] = newJInt(int64(rec.functionId))
          let fn = reader.function(rec.functionId)
          if fn.isOk:
            exitObj["function"] = newJString(fn.get())
          exitObj["exit_step"] = newJInt(int64(rec.exitStep))
          exitObj["depth"] = newJInt(int64(rec.depth))
          exitObj["return_value"] = decodeValueBytesToJson(rec.returnValue)
          if rec.exception.len > 0:
            exitObj["exception"] = decodeValueBytesToJson(rec.exception)
          eventsArr.add(exitObj)

    # Post-loop drain: synthesize missing call_entry events for records
    # whose entryStep landed past the last step (entryStep >= allGlis.len).
    # This happens when the writer's close() drain finalizes still-open
    # frames whose entry was registered at w.stepCount but no further
    # registerStep ever produced a real step at that index. The matching
    # call_exit was already emitted in the loop above because the writer
    # clamps exitStep to stepCount - 1, so without this drain the events
    # array is unbalanced (an exit with no entry).
    #
    # We anchor the synthesized entry on the last step (entry_step is
    # preserved as the original — possibly out-of-range — value so the
    # CallRecord round-trips faithfully). Records are scanned in
    # entryStep-ascending, then call_key (storage) order so deeper
    # frames synthesized in close() appear after their parents.
    let lastStep = uint64(allGlis.len) - 1
    var pending: seq[(uint64, v4calls.CallRecord, uint64)]
    for entry in callsByEntry:
      if entry[0] >= uint64(allGlis.len):
        pending.add(entry)
    # Stable-sort by entryStep so parent-before-child holds (call_key
    # order is already the entry order for ties).
    pending.sort(proc(a, b: (uint64, v4calls.CallRecord, uint64)): int =
      if a[0] < b[0]: -1
      elif a[0] > b[0]: 1
      elif a[2] < b[2]: -1
      elif a[2] > b[2]: 1
      else: 0)
    for (es, rec, ck) in pending:
      var callObj = newJObject()
      callObj["kind"] = newJString("call_entry")
      callObj["call_key"] = newJInt(int64(ck))
      callObj["function_id"] = newJInt(int64(rec.functionId))
      let fn = reader.function(rec.functionId)
      if fn.isOk:
        callObj["function"] = newJString(fn.get())
      callObj["entry_step"] = newJInt(int64(rec.entryStep))
      callObj["exit_step"] = newJInt(int64(rec.exitStep))
      callObj["depth"] = newJInt(int64(rec.depth))
      callObj["parent_call_key"] = newJInt(rec.parentCallKey)
      # Flag synthesized entries so downstream consumers can spot the
      # writer-close drain case (entry at last-step, exit already past).
      callObj["synthesized_at_step"] = newJInt(int64(lastStep))
      var argsJson = newJArray()
      for arg in rec.args:
        var argObj = newJObject()
        argObj["varname_id"] = newJInt(int64(arg.varnameId))
        let argVn = reader.varname(arg.varnameId)
        if argVn.isOk:
          argObj["varname"] = newJString(argVn.get())
        argObj["value"] = decodeValueBytesToJson(arg.value)
        argsJson.add(argObj)
      callObj["args"] = argsJson
      var childrenJson = newJArray()
      for c in rec.children:
        childrenJson.add(newJInt(int64(c)))
      callObj["children"] = childrenJson
      eventsArr.add(callObj)

  root["events"] = eventsArr
  return root

proc printFullV4(reader: var NewTraceReader, opts: FullOpts) =
  let root = buildFullDocument(reader, opts)
  try:
    echo pretty(root, indent = 2)
  except ValueError:
    echo $root

proc printEventsJsonlV4(reader: var NewTraceReader, opts: FullOpts) =
  ## Emit one JSON object per line. The first line is the header
  ## (metadata + interning tables). Subsequent lines are one event each.
  let root = buildFullDocument(reader, opts)

  # Header line: everything except `events`
  var header = newJObject()
  for k, v in root.pairs:
    if k != "events":
      header[k] = v
  echo $header

  let events = root["events"]
  if events.kind == JArray:
    for ev in events.elems:
      echo $ev

# ---------------------------------------------------------------------------
# V4 text output (human-readable)
# ---------------------------------------------------------------------------

proc printTextV4(reader: var NewTraceReader) =
  let gli = buildGliFromMeta(reader.meta)

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

  for stepIdx in 0'u64 ..< totalSteps:
    var pathStr = "?"
    var lineNum: uint64 = 0
    block resolveStep:
      let (pathId, line) = resolveStepLocation(reader, gli, allGlis[int(stepIdx)])
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

    echo "Step " & $stepIdx & ": " & pathStr & ":" & $lineNum & " (" & funcStr & ")"

    # Print values
    let vals = reader.values(stepIdx)
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
        let kindStr = case ev.kind
          of ioStdout: "stdout"
          of ioStderr: "stderr"
          of ioFileOp: "file"
          of ioError: "error"
        echo "  [" & kindStr & "] " & bytesToUtf8(ev.data)

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
    let gli = buildGliFromMeta(reader.meta)
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
        stepObj["step_index"] = newJInt(int64(stepIdx))
        if usableGlis:
          let (pathId, line) = resolveGli(
            gli, newGlis[int(stepIdx - lastStepCount)])
          stepObj["path_id"] = newJInt(int64(pathId))
          stepObj["line"] = newJInt(int64(line))
          let pathStr = reader.path(uint64(pathId))
          if pathStr.isOk:
            stepObj["path"] = newJString(pathStr.get())

        let callRes = reader.callForStep(stepIdx)
        if callRes.isOk:
          let c = callRes.get()
          stepObj["function_id"] = newJInt(int64(c.functionId))
          let fn = reader.function(c.functionId)
          if fn.isOk:
            stepObj["function"] = newJString(fn.get())

        let vals = reader.values(stepIdx)
        if vals.isOk and vals.get().len > 0:
          var valArr = newJArray()
          for v in vals.get():
            var valObj = newJObject()
            valObj["varname_id"] = newJInt(int64(v.varnameId))
            let vn = reader.varname(v.varnameId)
            if vn.isOk:
              valObj["varname"] = newJString(vn.get())
            valObj["type_id"] = newJInt(int64(v.typeId))
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
          ioObj["io_index"] = newJInt(int64(i))
          ioObj["kind"] = newJString($e.kind)
          ioObj["step_id"] = newJInt(int64(e.stepId))
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
          callObj["call_key"] = newJInt(int64(i))
          callObj["function_id"] = newJInt(int64(rec.functionId))
          let fn = reader.function(rec.functionId)
          if fn.isOk:
            callObj["function"] = newJString(fn.get())
          callObj["entry_step"] = newJInt(int64(rec.entryStep))
          callObj["exit_step"] = newJInt(int64(rec.exitStep))
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
    - "io":           io_kind (stdout/stderr/file/error), io_index, step_id,
                      bytes_b64, bytes_len, [text]
    - "call_exit":    call_key, function_id, function, exit_step, depth,
                      return_value, [exception]
  Variable values, call args, and return values are decoded from CBOR into
  structured JSON: {"kind":"Int","i":42,"type_id":7},
  {"kind":"String","text":"hello",...}, structs/sequences/tuples nested,
  references with addresses, BigInts as base64+hex, etc.
"""

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

proc emitNativeFull(filePath: string, format: string, stripPaths: bool) =
  ## Decode a CodeTracer **native recorder** multi-thread shard bundle (the
  ## layout written by `codetracer-native-recorder/ct_recorder/trace_writer`).
  ## On any decode failure we `quit` with a specific error so callers see
  ## what the decoder didn't understand — never a silent empty document.
  let docRes = decodeNativeFromFile(filePath,
    NativeOpts(stripPaths: stripPaths))
  if docRes.isErr:
    quit("ct-print: native decode failed: " & docRes.error)
  let doc = docRes.get()
  case format
  of "events":
    var header = newJObject()
    for k, v in doc.pairs:
      if k != "events":
        header[k] = v
    echo $header
    let events = doc{"events"}
    if events != nil and events.kind == JArray:
      for ev in events.elems:
        echo $ev
  else:
    try:
      echo pretty(doc, indent = 2)
    except ValueError:
      echo $doc

type
  AutoDetectKind = enum
    adkUnreadable    ## file does not exist / cannot be opened
    adkNotCtfs       ## file exists but lacks CTFS magic — let v2/v3 path try
    adkCtfsCorrupt   ## CTFS magic OK but root header looks broken — hard error
    adkNative        ## CTFS + meta.json with recordingMode → native decoder
    adkV4MultiStream ## CTFS without meta.json → existing v4 path

proc detectAutoKind(filePath: string): (AutoDetectKind, string) =
  ## Inspect the file once and decide which decode path should claim it.
  ## Returning a (kind, detail) pair lets the caller emit a precise error
  ## message instead of silently falling through to an empty document.
  let dataR = ctfs_container.readCtfsFromFile(filePath)
  if dataR.isErr:
    return (adkUnreadable, dataR.error)
  let data = dataR.get()
  if data.len < 16 or not ctfs_container.hasCtfsMagic(data):
    return (adkNotCtfs, "no CTFS magic — not a .ct bundle")
  let infoR = detectNativeBundle(data)
  if infoR.isErr:
    # A v4 multi-stream bundle has `meta.dat` and no `meta.json`, so
    # detectNativeBundle errs with "meta.json missing" — that's the normal
    # signal to route to the v4 reader, not a corruption indicator.
    if infoR.error.startsWith("meta.json missing"):
      return (adkV4MultiStream, "")
    return (adkCtfsCorrupt, infoR.error)
  if isNativeBundle(data):
    return (adkNative, "")
  (adkV4MultiStream, "")

proc main() =
  var format = "text"
  var filePath = ""
  var follow = false
  var pollMs = 200
  var stripPaths = false
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
      of "follow": follow = true
      of "strip-paths": stripPaths = true
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

  let opts = FullOpts(stripPaths: stripPaths)

  # ----- Native MCR shard path (auto-detect or --native) -----
  # The native recorder writes a CTFS shard with per-thread `tNNNN` streams
  # and a `meta.json` (vs the v4 layout's `meta.dat` + interning tables).
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
      # CTFS magic OK but the bundle is broken — refuse instead of letting
      # the v4 reader produce a degenerate empty document.
      quit("ct-print: refusing to decode broken CTFS bundle: " & detail)
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

  # Try v4 multi-stream reader first
  let newReaderRes = openNewTrace(filePath)
  if newReaderRes.isOk:
    if follow:
      followV4(filePath, pollMs)
    else:
      var reader = newReaderRes.get()
      case format
      of "summary": printSummaryV4(reader)
      of "meta-json": printMetaJsonV4(reader)
      of "json": printJsonV4(reader)
      of "json-events": printJsonEventsV4(reader)
      of "full": printFullV4(reader, opts)
      of "events": printEventsJsonlV4(reader, opts)
      else: printTextV4(reader)
    return

  # Fall back to old v2/v3 reader
  let readerRes = openTrace(filePath)
  if readerRes.isErr:
    quit("Error: " & readerRes.unsafeError)
  var reader = readerRes.get()

  let readRes = reader.readEvents()
  if readRes.isErr:
    quit("Error reading events: " & readRes.unsafeError)

  case format
  of "json": echo reader.toJson()
  of "json-events": echo reader.toJsonEvents()
  of "summary": echo reader.toSummary()
  of "full":
    # The legacy v2/v3 reader's toJson already includes full event content.
    echo reader.toJson()
  of "events":
    echo reader.toJsonEvents()
  else: echo reader.toPrettyText()

main()
