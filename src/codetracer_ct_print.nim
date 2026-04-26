when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

## ct-print: Convert .ct trace files to human-readable formats.
##
## Usage:
##   ct-print <file.ct>                  # Print as text (default)
##   ct-print --json <file.ct>           # Print as JSON
##   ct-print --json-events <file.ct>    # Print only events as JSON array
##   ct-print --summary <file.ct>        # Print metadata and event counts only
##   ct-print --follow <file.ct>         # Follow mode (NDJSON, polls for new events)
##   ct-print --follow --poll-interval=500 <file.ct>  # Follow with custom poll interval

import std/[os, parseopt, json, strutils]
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
  ## Best-effort conversion of CBOR-encoded value bytes to a JSON-friendly string.
  ## For now, try to interpret as UTF-8; otherwise hex-encode.
  if data.len == 0:
    return "\"\""
  # Try as UTF-8 string
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
# V4 JSON (full dump)
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

  # Steps
  var stepsArr = newJArray()
  let sc = reader.stepCount()
  if sc.isOk:
    for i in 0'u64 ..< sc.get():
      var stepObj = newJObject()
      stepObj["index"] = newJInt(int64(i))
      let absGli = reader.stepAbsoluteGlobalLineIndex(i)
      if absGli.isOk:
        let (pathId, line) = resolveGli(gli, absGli.get())
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
  if sc.isOk:
    for i in 0'u64 ..< sc.get():
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
# V4 JSON events (unified, interleaved by step)
# ---------------------------------------------------------------------------

proc stepEventToJson(reader: var NewTraceReader, gli: GlobalLineIndex,
    stepIdx: uint64,
    ioEvents: seq[IOEvent], ioIndices: seq[uint64]): seq[JsonNode] =
  ## Produce JSON nodes for a single step: the step itself, its values,
  ## and any IO events at this step.
  var nodes: seq[JsonNode]

  # Step event
  var stepObj = newJObject()
  stepObj["type"] = newJString("step")
  stepObj["step_index"] = newJInt(int64(stepIdx))

  let absGli = reader.stepAbsoluteGlobalLineIndex(stepIdx)
  if absGli.isOk:
    let (pathId, line) = resolveGli(gli, absGli.get())
    stepObj["path_id"] = newJInt(int64(pathId))
    stepObj["line"] = newJInt(int64(line))
    let pathStr = reader.path(uint64(pathId))
    if pathStr.isOk:
      stepObj["path"] = newJString(pathStr.get())

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

  let sc = reader.stepCount()
  if sc.isOk:
    for stepIdx in 0'u64 ..< sc.get():
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

      let nodes = stepEventToJson(reader, gli, stepIdx, stepIo, stepIoIndices)
      for n in nodes:
        eventsArr.add(n)

  try:
    echo pretty(eventsArr)
  except ValueError:
    echo $eventsArr

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

  let sc = reader.stepCount()
  if sc.isErr:
    echo "(no steps)"
    return

  let totalSteps = sc.get()
  if totalSteps == 0:
    echo "(no steps)"
    return

  for stepIdx in 0'u64 ..< totalSteps:
    var pathStr = "?"
    var lineNum: uint64 = 0
    let absGli = reader.stepAbsoluteGlobalLineIndex(stepIdx)
    if absGli.isOk:
      let (pathId, line) = resolveGli(gli, absGli.get())
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
        var dataStr = bytesToUtf8(v.data)
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
      # File might not exist yet during recording startup
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

    # New steps
    let sc = reader.stepCount()
    if sc.isOk and sc.get() > lastStepCount:
      for stepIdx in lastStepCount ..< sc.get():
        var stepObj = newJObject()
        stepObj["type"] = newJString("step")
        stepObj["step_index"] = newJInt(int64(stepIdx))
        let absGli = reader.stepAbsoluteGlobalLineIndex(stepIdx)
        if absGli.isOk:
          let (pathId, line) = resolveGli(gli, absGli.get())
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

        # Values
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
            valObj["data"] = newJString(bytesToUtf8(v.data))
            valArr.add(valObj)
          stepObj["values"] = valArr

        echo $stepObj
        hadNewEvents = true
      lastStepCount = sc.get()

    # New IO events
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

    # New calls
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
# Main
# ---------------------------------------------------------------------------

proc main() =
  var format = "text"
  var filePath = ""
  var follow = false
  var pollMs = 200

  for kind, key, val in getopt():
    case kind
    of cmdArgument: filePath = key
    of cmdLongOption:
      case key
      of "json": format = "json"
      of "json-events": format = "json-events"
      of "summary": format = "summary"
      of "follow": follow = true
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
      else: quit("Unknown option: " & key)
    of cmdEnd: discard

  if filePath == "":
    quit("Usage: ct-print [--json|--json-events|--summary|--follow] <file.ct>")

  # Try v4 multi-stream reader first
  let newReaderRes = openNewTrace(filePath)
  if newReaderRes.isOk:
    if follow:
      followV4(filePath, pollMs)
    else:
      var reader = newReaderRes.get()
      case format
      of "summary": printSummaryV4(reader)
      of "json": printJsonV4(reader)
      of "json-events": printJsonEventsV4(reader)
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
  else: echo reader.toPrettyText()

main()
