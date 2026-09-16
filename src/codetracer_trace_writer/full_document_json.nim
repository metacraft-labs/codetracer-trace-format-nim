## The ONE assembly of a CTFS container into the JSON document that
## ``ct-print --full`` / ``--events`` print.
##
## It lives here, beside ``meta_flags_json`` and ``source_reload_json``, for the
## reason those modules' headers record — and it is the block those two were
## carved out of. ``ct-print`` had TWO implementations of this document: the
## shipped CLI (``src/codetracer_ct_print.nim``) and a near-copy
## (``src/codetracer_ct_print_lib.nim``) that the test corpus links so it can
## call the builder in-process without shelling out to a binary.
##
## The tests asserted against the copy. Over the following months the shipped
## binary gained an ordering rule for same-step ``call_exit`` events, the
## exclusion of ``DeltaColumn`` nudges from ``events[]``, a column-aware
## position decoder, and a diagnostic channel for undecodable value records;
## the copy the tests exercised gained none of them, and separately grew a
## ``fields`` key the binary has never printed. Every one of those was a
## passing test suite describing a program nobody runs.
##
## Carving out one block at a time left the rest of the class open, so this is
## the whole document rather than one more block. ``codetracer_ct_print_lib``
## now re-exports this module, which makes the two entry points the same code
## by construction; ``tests/test_ct_print_agreement.nim`` checks that the
## shipped binary has not grown a third.
##
## All output is deterministic: stable key order, no timestamps, no PIDs, no
## machine-specific paths unless the input itself contained them.
## ``FullOpts.stripPaths`` substitutes ``<workdir>`` and ``<tmp>`` placeholders
## for cross-machine snapshots.

import std/[json, strutils, base64, algorithm]
import results
import ./new_trace_reader
import ./meta_dat
import ./meta_flags_json
import ./source_reload_json
import ./step_encoding
import ./call_stream as v4calls
import ./io_event_stream
import ./value_stream
import ./global_line_index
import ./multi_stream_writer
import ./cbor
import ../codetracer_trace_types

export json, results

# ---------------------------------------------------------------------------
# Global line index resolution for v4 traces
# ---------------------------------------------------------------------------

proc addEventMetadata*(obj: JsonNode, metadata: seq[byte]) =
  ## Attach an IO event's ``metadata`` slot to its JSON representation.
  ##
  ## The metadata slot is opaque to the trace format — it carries whatever
  ## the recorder put there. Two consumers matter today:
  ##
  ## * ordinary program output tags it with the sink name ("stdout"),
  ## * **correlation markers** put a complete JSON ``MarkerPayload`` there,
  ##   which is what lets the debugger pair a value's departure from one
  ##   recording with its arrival in another.
  ##
  ## Because the second case is the substrate of every cross-process
  ## origin chain, it is worth surfacing structurally rather than as an
  ## opaque string: a marker that fails to decode is invisible to the
  ## debugger, and "invisible" is exactly the failure mode that is hard
  ## to notice without a tool that shows it. When the slot parses as a
  ## marker payload the event is additionally tagged
  ## ``correlation_marker`` with its boundary, direction and key hoisted
  ## to the top level.
  if metadata.len == 0:
    return
  var text = newString(metadata.len)
  for i, b in metadata:
    text[i] = char(b)
  obj["metadata"] = newJString(text)
  try:
    let parsed = parseJson(text)
    if parsed.kind == JObject and parsed.hasKey("boundary_id") and
        parsed.hasKey("direction") and parsed.hasKey("key_value"):
      obj["correlation_marker"] = parsed
      obj["boundary_id"] = parsed["boundary_id"]
      obj["direction"] = parsed["direction"]
      obj["key_value"] = parsed["key_value"]
  except CatchableError:
    # Not JSON, or not a marker — the raw string above is all we can say.
    discard

proc resolveGli*(gli: GlobalLineIndex,
    globalIdx: uint64): Result[(int, uint64), string] =
  ## Invert a line-only ``global_position_index`` to ``(pathId, line)``,
  ## or say why it cannot be inverted.
  ##
  ## Through ``tryResolve``, not ``resolve``: the packing is a writer
  ## convention the container does not record and the writers of this
  ## format disagree about it, so an index the space cannot address is
  ## reported rather than clamped into a file that exists (see
  ## ``global_line_index``'s module header). ct-print's business is
  ## saying what the container holds, and "this position is not one this
  ## trace can hold" is part of that.
  gli.tryResolve(globalIdx)

proc resolveStepLocation*(reader: var NewTraceReader,
    gli: GlobalLineIndex, stepGli: uint64): Result[(int, uint64), string] =
  ## Resolve a step's absolute ``global_position_index`` to ``(pathId,
  ## line)``.  Column-aware traces encode GLI as a byte-offset (cumulative
  ## sum of preceding line_lengths), so the legacy line-count-based
  ## resolver returns garbage on them.  Route through the spec-canonical
  ## ``decodeGlobalPositionIndex`` when the column-aware flag is set; fall
  ## back to the line-only space for legacy traces and for the
  ## column-aware files that carry no per-line table.
  if reader.meta.hasColumnAwareSteps:
    let posRes = reader.decodeGlobalPositionIndex(stepGli)
    if posRes.isOk:
      return ok((int(posRes.get().file), uint64(posRes.get().line)))
  gli.tryResolve(stepGli)

proc precomputeStepGlis*(reader: var NewTraceReader): seq[uint64] =
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

var valueDecodeFailures = 0
  ## How many steps this run could not decode a value record for.  Module-level
  ## so the warning below is emitted once instead of once per step.
var warnedSkippedTags: seq[uint8] = @[]
  ## Distinct unknown value-stream event tags >= 10 that have already been warned about.

proc valuesForStep*(reader: var NewTraceReader, stepIdx: uint64):
    Result[seq[VariableValue], string] =
  ## A step's decoded variable values, REPORTING a decode failure instead of
  ## rendering the step as though it had none.
  ##
  ## Forward-compatibility (HX-S-5 / HX-OQ-8):
  ## Tags >= 10 carry a self-delimiting length prefix.  When an unknown tag
  ## >= 10 is encountered, it is skipped while preserving known variable values
  ## at this step.  A one-shot warning naming the skipped tag and count is
  ## emitted to stderr so the skip is not silent.
  ##
  ## Legacy / malformed records with unknown tags < 10 or truncated payloads
  ## continue to fail the record, emitting a one-shot warning and rendering
  ## the step with no variables.
  let vals = reader.values(stepIdx)
  if vals.isOk:
    let skipped = reader.lastSkippedValueTags()
    for tag in skipped:
      if tag notin warnedSkippedTags:
        warnedSkippedTags.add(tag)
        var countInStep = 0
        for t in skipped:
          if t == tag: inc countInStep
        when not defined(silentSkipForwardCompat):
          stderr.writeLine("ct-print: WARNING: step " & $stepIdx &
            ": unknown value-stream event tag " & $tag &
            " skipped (" & $countInStep & " skipped; self-delimiting length prefix preserved visible variables; " &
            "further occurrences suppressed)")
    return vals
  inc valueDecodeFailures
  if valueDecodeFailures == 1:
    stderr.writeLine("ct-print: WARNING: step " & $stepIdx &
      ": this step's value record did not decode, so the step is printed " &
      "with NO variables even though the record is present in the " &
      "container: " & vals.error &
      "  (further occurrences are suppressed)")
  ok(newSeq[VariableValue]())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc isCorrelationMarker*(event: JsonNode): bool =
  ## Does this event (as produced by ``buildFullDocument``) carry a
  ## decoded correlation-marker payload?
  event.kind == JObject and event.hasKey("correlation_marker")

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

proc normalizePath*(s: string, stripWorkdir: string, stripPaths: bool): string =
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


# ---------------------------------------------------------------------------
# The full document
# ---------------------------------------------------------------------------

type FullOpts* = object
  stripPaths*: bool
  ## Emit machine-readable JSON instead of the human-readable table.
  ## Only consulted by `--markers`; the other modes are JSON already.
  jsonOut*: bool

proc buildFullDocument*(reader: var NewTraceReader,
    opts: FullOpts): JsonNode =
  ## Build the deterministic JSON document for `--full` and `--events` modes.
  ## The shape is:
  ##   { metadata, paths, functions, varnames, types,
  ##     events: [ {kind: "...", ...}, ... ] }
  ## All variable values and call args/returns are decoded from CBOR into
  ## structured JSON objects matching the ValueRecord variant layout.
  let gli = reader.globalPositionSpace()
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

  # ----- path_versions (GDH-M1, design §6.1 / §7.0) -----
  # Parallel to `paths`, one entry per path id: the file's own line count
  # as the container states it, the 0-based version ordinal of this entry
  # among the entries sharing its string, and how many such entries there
  # are. `paths` stays a plain array of strings so every existing golden
  # and consumer is untouched; this is where the index-is-the-version
  # property becomes readable without linking the reader.
  var pathVersionsArr = newJArray()
  for i in 0'u64 ..< reader.pathCount():
    var vObj = newJObject()
    vObj["path_id"] = newJInt(int64(i))
    let vo = reader.pathVersionOrdinal(i)
    vObj["version_ordinal"] = if vo.isOk: newJInt(int64(vo.get()))
                              else: newJInt(-1)
    let vc = reader.pathVersionCount(i)
    vObj["version_count"] = if vc.isOk: newJInt(int64(vc.get()))
                            else: newJInt(-1)
    # 0 means "this container does not state this file's size", not "the
    # file has no lines" — see `recordedLineCount`.
    vObj["recorded_line_count"] = newJInt(int64(reader.recordedLineCount(i)))
    pathVersionsArr.add(vObj)
  root["path_versions"] = pathVersionsArr

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
  # GDH-M2: how many source-reload markers the container carries.
  # ALWAYS emitted, including the `0` every trace without a reload
  # answers with, for the reason `path_version_ordinal` is always
  # emitted: a key that appears only when a marker exists makes a scan
  # for it pass on a trace that has none AND on a build that cannot see
  # one. It is also what lets a caller check an `--events` dump is
  # COMPLETE: the step entries plus the source_reload entries must
  # account for every exec record.
  let srR = reader.sourceReloadCount()
  counts["source_reloads"] = newJInt(if srR.isOk: int64(srR.get()) else: -1)
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
      #
      # ``sekDeltaColumn`` events are column-only NUDGES on the preceding
      # real step (Column-Aware-Tracing spec), not standalone logical
      # steps.  ``logicalStepCount`` (new_trace_reader.nim:766-773)
      # excludes them from ``counts.steps`` for exactly this reason, so we
      # must mirror that exclusion here: a DeltaColumn nudge must NOT be
      # materialized as a ``kind="step"`` entry, otherwise ``events[]``
      # step entries would be inconsistent with ``counts.steps`` (and
      # inflated relative to the true logical-step count).  Column-aware
      # REAL steps (TagStepWithColumn ⇒ AbsoluteStep/DeltaStep events)
      # are counted by ``logicalStepCount`` and therefore stay.  We still
      # walk the raw index so call/IO correlation (keyed on the raw step
      # index) is unaffected.
      let stepEv = reader.step(stepIdx)
      let isDeltaColumnNudge = stepEv.isOk and stepEv.get().kind == sekDeltaColumn
      # GDH-M2 / design §7.3: the reload marker is a timeline ANNOTATION,
      # not a step.  It has no source location, so rendering it as a
      # `kind="step"` entry would attach it to whatever position the
      # running absolute address happened to hold — the previous step's —
      # and invite a consumer to navigate to it.  It gets its own kind,
      # and `counts.steps` (logicalStepCount) excludes it, so the two
      # stay consistent.
      let isSourceReload = stepEv.isOk and stepEv.get().kind == sekSourceReload
      if isSourceReload:
        eventsArr.add(sourceReloadEventJson(stepEv.get(), stepIdx))
      # A record that could not be DECODED gets its own kind, and never
      # falls through into the `kind="step"` branch below.  Both booleans
      # above are `isOk and ...`, so without this arm a failed decode
      # reads as "not a nudge and not a marker" and is rendered as an
      # ordinary step — at whatever position the running absolute address
      # happens to hold, i.e. the PREVIOUS step's.  That is exactly the
      # mis-attribution the marker branch above exists to prevent, and it
      # is reachable by the refusal this milestone added: a container
      # carrying tag 0x08 without declaring it fails HERE, and would
      # otherwise be dumped as a plausible step stream with no error in it
      # anywhere.
      let isUnreadable = stepEv.isErr
      if isUnreadable:
        var errObj = newJObject()
        errObj["kind"] = newJString("step_error")
        errObj["step_index"] = newJInt(int64(stepIdx))
        errObj["error"] = newJString(stepEv.error)
        eventsArr.add(errObj)
      if not isDeltaColumnNudge and not isSourceReload and not isUnreadable:
        var stepObj = newJObject()
        stepObj["kind"] = newJString("step")
        stepObj["step_index"] = newJInt(int64(stepIdx))
        block emitStep:
          let loc = resolveStepLocation(reader, gli, stepGli)
          if loc.isErr:
            stepObj["position_error"] = newJString(loc.error)
            break emitStep
          let (pathId, line) = loc.get()
          stepObj["path_id"] = newJInt(int64(pathId))
          stepObj["line"] = newJInt(int64(line))
          # GDH-M1 — which VERSION of that file the step ran in: the
          # 0-based ordinal of `path_id` among the paths.dat entries
          # carrying its string. Always emitted, including the `0`
          # every legacy trace answers with, because a consumer must be
          # able to tell "this file was never reloaded" from "this
          # ct-print predates versioned paths" — a key that appears only
          # when a version exists makes a scan for it pass on a trace
          # that has none AND on a build that cannot see one.
          let vOrd = reader.pathVersionOrdinal(uint64(pathId))
          stepObj["path_version_ordinal"] =
            if vOrd.isOk: newJInt(int64(vOrd.get())) else: newJInt(-1)
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
        let vals = reader.valuesForStep(stepIdx)
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
          addEventMetadata(ioObj, ev.metadata)
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

