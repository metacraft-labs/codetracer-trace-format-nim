## codetracer_ct_print_lib: Reusable helpers for the `ct-print` CLI.
##
## Splitting the heavy lifting out of `codetracer_ct_print.nim` keeps the
## CLI entry point thin and makes the JSON-rendering logic accessible from
## the test suite without compiling and shelling out to the binary.
##
## Public surface:
##   - `valueRecordToJson(v)` — render a decoded ValueRecord as JsonNode.
##   - `decodeValueBytesToJson(bytes)` — decode CBOR + render to JsonNode.
##   - `FullOpts` and `buildFullDocument(reader, opts)` — produce the full
##     content-faithful dump used by `ct-print --full` and `--events`.
##   - `buildGliFromMeta`, `resolveGli` — global-line-index helpers shared
##     with the legacy text/JSON paths.
##
## All output is deterministic: stable key order, no timestamps, no PIDs,
## no machine-specific paths unless the input itself contained them. The
## `FullOpts.stripPaths` flag substitutes `<workdir>` and `<tmp>` placeholders
## for cross-machine snapshots.

import std/[json, strutils, base64, algorithm]
import results
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/call_stream as v4calls
import codetracer_trace_writer/io_event_stream
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/global_line_index
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/cbor
import codetracer_trace_types

export json, results

# ---------------------------------------------------------------------------
# GLI helpers
# ---------------------------------------------------------------------------

proc buildGliFromMeta*(meta: MetaDatContents): GlobalLineIndex =
  ## Rebuild the global line index from the meta.dat paths list using the
  ## same DefaultLinesPerFile as the writer.
  var counts = newSeq[uint64](meta.paths.len)
  for i in 0 ..< meta.paths.len:
    counts[i] = DefaultLinesPerFile
  buildGlobalLineIndex(counts)

proc resolveGli*(gli: GlobalLineIndex, globalIdx: uint64): (int, uint64) =
  gli.resolve(globalIdx)

proc precomputeStepGlis*(reader: var NewTraceReader): seq[uint64] =
  ## Walk the exec stream once and return a seq mapping step_index →
  ## absolute global_position_index.  Used by ct-print's --json /
  ## --full / --events emission so the per-step loop becomes a flat
  ## seq lookup instead of an O(N²) ``stepAbsoluteGlobalLineIndex(i)``
  ## call per step (which itself walks O(N) events per call ⇒ O(N³)
  ## for ct-print's outer loop).  Empty seq when the reader has no
  ## exec stream or the bulk fetch fails — callers fall back to the
  ## per-step accessor in that case.
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
# Byte/string helpers
# ---------------------------------------------------------------------------

proc bytesToUtf8*(data: seq[byte]): string =
  result = newString(data.len)
  for i in 0 ..< data.len:
    result[i] = char(data[i])

proc bytesToHexLower*(data: seq[byte]): string =
  result = newStringOfCap(data.len * 2)
  for b in data:
    result.add(toHex(int(b), 2).toLowerAscii())

# ---------------------------------------------------------------------------
# ValueRecord -> JsonNode
# ---------------------------------------------------------------------------

proc valueRecordToJson*(v: ValueRecord): JsonNode =
  ## Convert a decoded ValueRecord into a structured JSON node, surfacing
  ## every variant of the tagged union with deterministic key order.
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
    # CTFS-M-TypeSchema: when the struct carries `fieldNames`, surface
    # `fields` as `[[name, value], ...]` pairs in addition to keeping
    # the positional `field_values` array. Consumers that already key
    # off `field_values` keep working; new consumers can prefer
    # `fields` for named rendering.
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
    result["b"] = newJString(base64.encode(v.bigIntBytes))
    result["b_hex"] = newJString(bytesToHexLower(v.bigIntBytes))
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
  ## Decode CBOR-encoded value bytes into structured JSON. On decode
  ## failure, emits a fallback {"kind":"Undecodable","raw_hex":...}. The
  ## one-byte VoidReturnMarker is surfaced as {"kind":"Void"}.
  if data.len == 0:
    var node = newJObject()
    node["kind"] = newJString("Empty")
    return node
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
    node["error"] = newJString(res.unsafeError)
    node["raw_hex"] = newJString(bytesToHexLower(data))
    return node

# ---------------------------------------------------------------------------
# Path normalization
# ---------------------------------------------------------------------------

proc normalizePath*(s: string, stripWorkdir: string, stripPaths: bool): string =
  if not stripPaths:
    return s
  if stripWorkdir.len > 0 and s.startsWith(stripWorkdir):
    var rest = s[stripWorkdir.len .. ^1]
    if rest.len > 0 and rest[0] == '/':
      rest = rest[1 .. ^1]
    return "<workdir>/" & rest
  if s.startsWith("/tmp/"):
    let parts = s.split('/')
    if parts.len >= 4:
      return "<tmp>/" & parts[3 .. ^1].join("/")
  s

# ---------------------------------------------------------------------------
# FullOpts + buildFullDocument
# ---------------------------------------------------------------------------

type
  FullOpts* = object
    stripPaths*: bool

proc buildFullDocument*(reader: var NewTraceReader,
    opts: FullOpts): JsonNode =
  ## Build the deterministic JSON document for `--full` and `--events`.
  ## Top-level shape:
  ##   { metadata, paths, functions, varnames, types, counts, events: [...] }
  ## Each event entry has a `kind` field: "step" | "call_entry" | "call_exit"
  ## | "io". Variable values, call args, and return values are decoded from
  ## CBOR into structured JSON matching the ValueRecord variant layout.
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
  # the flag was introduced.  Currently only the column-aware flag
  # is exposed here; further flags follow the same pattern.
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
  # FlagHasTraceFilterProvenance set, so post-trace audit can
  # distinguish "no provenance recorded" (key absent) from "provenance
  # recorded but the chain happens to be empty" (key present, empty
  # `filters` array).
  if reader.meta.hasFilterProvenance:
    var filtersArr = newJArray()
    for entry in reader.meta.filterProvenance:
      var entryObj = newJObject()
      entryObj["path"] = newJString(entry.path)
      var shaBytes = newSeq[byte](32)
      for k in 0 ..< 32:
        shaBytes[k] = entry.sha256[k]
      entryObj["sha256"] = newJString(bytesToHexLower(shaBytes))
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
  # Inline content/sourcemap bytes would blow up the JSON; surface only
  # the per-view metadata + byte counts so golden tests anchor on the
  # structural fields without storing the formatted source in the
  # diff.
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

  # ----- counts (deterministic anchors for golden tests) -----
  var counts = newJObject()
  counts["paths"] = newJInt(int64(reader.pathCount()))
  counts["functions"] = newJInt(int64(reader.functionCount()))
  counts["varnames"] = newJInt(int64(reader.varnameCount()))
  counts["types"] = newJInt(int64(reader.typeCount()))
  counts["source_views"] = newJInt(int64(reader.sourceViewCount()))
  # User-facing "step count" is the number of logical line-bearing
  # events (AbsoluteStep + DeltaStep) — stable across the addition of
  # new event types and unaffected by the writer's column-aware mode
  # (which interleaves DeltaColumn nudges into the events stream).
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

  var eventsArr = newJArray()
  # Pre-fetch all step GLIs in one O(N) pass — calling
  # stepAbsoluteGlobalLineIndex per step inside the loop is O(N²).
  let allGlis = precomputeStepGlis(reader)
  if allGlis.len > 0:
    for stepIdx in 0'u64 ..< uint64(allGlis.len):
      let stepGli = allGlis[int(stepIdx)]
      # 1) call entries at this step
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

      # 2) the step event itself
      var stepObj = newJObject()
      stepObj["kind"] = newJString("step")
      stepObj["step_index"] = newJInt(int64(stepIdx))
      block emitStep:
        let (pathId, line) = resolveGli(gli, stepGli)
        stepObj["path_id"] = newJInt(int64(pathId))
        stepObj["line"] = newJInt(int64(line))
        let pStr = reader.path(uint64(pathId))
        if pStr.isOk:
          stepObj["path"] = newJString(
            normalizePath(pStr.get(), reader.meta.workdir, opts.stripPaths))
        # P1.4: surface the per-step column for column-aware traces so
        # JSON-events / --full consumers can read the resolved
        # ``(file, line, column)`` directly without having to walk the
        # exec stream themselves.  The decoder errors on legacy traces;
        # we leave the field absent in that case to keep the JSON
        # bit-for-bit compatible with pre-column-aware tooling.
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

      # 3) IO events at this step
      for (sid, ev, idx) in ioByStep:
        if sid == stepIdx:
          var ioObj = newJObject()
          ioObj["kind"] = newJString("io")
          ioObj["io_kind"] = newJString($ev.kind)
          ioObj["io_index"] = newJInt(int64(idx))
          ioObj["step_id"] = newJInt(int64(ev.stepId))
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

      # 4) call exits at this step
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
    # Happens when writer's close() drain finalizes still-open frames whose
    # entry was registered at w.stepCount with no further registerStep
    # producing a real step at that index. The matching call_exit was
    # already emitted in the loop above (writer clamps exitStep to
    # stepCount - 1), so without this drain the events array is unbalanced
    # (an exit with no entry). Anchored at the last step; entry_step keeps
    # the original — possibly out-of-range — value for fidelity.
    let lastStep = uint64(allGlis.len) - 1
    var pending: seq[(uint64, v4calls.CallRecord, uint64)]
    for entry in callsByEntry:
      if entry[0] >= uint64(allGlis.len):
        pending.add(entry)
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
