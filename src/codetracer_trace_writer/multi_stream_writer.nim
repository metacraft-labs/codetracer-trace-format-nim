{.push raises: [].}

## Multi-stream TraceWriter (M25).
##
## High-level writer that produces multi-stream CTFS traces.
## Delegates to exec_stream, value_stream, call_stream, io_event_stream,
## interning_table, and meta_dat for the actual encoding.
##
## This module is the new replacement for the old TraceWriter that produced
## single-stream events.log + meta.json + paths.json.

import std/options
import results
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ./meta_dat
import ./interning_table
import ./exec_stream
import ./value_stream
import ./call_stream
import ./io_event_stream
import ./step_encoding
import ./global_line_index
import ./varint
import ../codetracer_trace_types

export results, value_stream.VariableValue, io_event_stream.IOEventKind

const
  DefaultLinesPerFile*: uint64 = 100_000
    ## Default assumed line count per file for GlobalLineIndex.
    ## The real line counts would come from source files, which we
    ## don't have at this level.

type
  PendingCall = object
    functionId: uint64
    entryStep: uint64
    depth: uint32
    parentCallKey: int64
    args: seq[seq[byte]]
    children: seq[uint64]

  MultiStreamTraceWriter* = object
    ctfs: Ctfs
    execWriter: ExecStreamWriter
    valueWriter: ValueStreamWriter
    callWriter: CallStreamWriter
    ioEventWriter: IOEventStreamWriter
    interning: TraceInterningTables
    metadata*: TraceMetadata
    paths*: seq[string]

    # Global line index (rebuilt when paths change)
    gli: GlobalLineIndex
    gliDirty: bool

    # State tracking
    stepCount*: uint64
    callCount: uint64
    lastGlobalLineIndex: uint64
    lastPathId: uint64
    lastLine: uint64
    callStack: seq[PendingCall]
    currentDepth: uint32
    closed: bool
    filePath: string

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc rebuildGli(w: var MultiStreamTraceWriter) =
  ## Rebuild the global line index from the current set of paths.
  var counts = newSeq[uint64](w.paths.len)
  for i in 0 ..< w.paths.len:
    counts[i] = DefaultLinesPerFile
  w.gli = buildGlobalLineIndex(counts)
  w.gliDirty = false

proc toGlobalLineIndex(w: var MultiStreamTraceWriter,
    pathId: uint64, line: uint64): uint64 =
  if w.gliDirty:
    w.rebuildGli()
  w.gli.globalIndex(int(pathId), line)

# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

proc initMultiStreamWriter*(path: string, program: string,
    chunkSize: int = 4096): Result[MultiStreamTraceWriter, string] =
  ## Create a new multi-stream trace writer.
  ## Produces an in-memory CTFS container (call close() to finalize).
  var w: MultiStreamTraceWriter
  w.ctfs = createCtfs()
  w.metadata = TraceMetadata(program: program, args: @[], workdir: "")
  w.paths = @[]
  w.gliDirty = true
  w.filePath = path

  # Meta.dat placeholder - will be written at close time
  # Init interning tables
  let intRes = initTraceInterningTables(w.ctfs)
  if intRes.isErr:
    return err("failed to init interning tables: " & intRes.error)
  w.interning = intRes.get()

  # Init stream writers
  let execRes = initExecStreamWriter(w.ctfs, chunkSize)
  if execRes.isErr:
    return err("failed to init exec stream: " & execRes.error)
  w.execWriter = execRes.get()

  let valRes = initValueStreamWriter(w.ctfs)
  if valRes.isErr:
    return err("failed to init value stream: " & valRes.error)
  w.valueWriter = valRes.get()

  let callRes = initCallStreamWriter(w.ctfs)
  if callRes.isErr:
    return err("failed to init call stream: " & callRes.error)
  w.callWriter = callRes.get()

  let ioRes = initIOEventStreamWriter(w.ctfs)
  if ioRes.isErr:
    return err("failed to init io event stream: " & ioRes.error)
  w.ioEventWriter = ioRes.get()

  ok(w)

# ---------------------------------------------------------------------------
# Path registration
# ---------------------------------------------------------------------------

proc registerPath*(w: var MultiStreamTraceWriter,
    path: string): Result[uint64, string] =
  ## Register a source path and return its interned ID.
  let idRes = w.ctfs.ensurePathId(w.interning, path)
  if idRes.isErr:
    return err(idRes.error)
  let id = idRes.get()
  # Track paths list for meta.dat (only add if new)
  if id == uint64(w.paths.len):
    w.paths.add(path)
    w.gliDirty = true
  ok(id)

# ---------------------------------------------------------------------------
# Step registration
# ---------------------------------------------------------------------------

proc registerStep*(w: var MultiStreamTraceWriter, pathId: uint64,
    line: uint64,
    values: openArray[VariableValue]): Result[void, string] =
  ## Register a step event with its variable values.
  ## Automatically uses DeltaStep encoding when the new global line index
  ## is within a small delta of the previous one.
  if w.closed:
    return err("writer is closed")

  let gli = w.toGlobalLineIndex(pathId, line)

  var ev: StepEvent
  if w.stepCount == 0:
    # First step must be absolute
    ev = StepEvent(kind: sekAbsoluteStep, globalLineIndex: gli)
  else:
    let delta = int64(gli) - int64(w.lastGlobalLineIndex)
    # Use delta encoding for small deltas (fits in 1-2 varint bytes)
    if delta >= -64 and delta <= 63:
      ev = StepEvent(kind: sekDeltaStep, lineDelta: delta)
    else:
      ev = StepEvent(kind: sekAbsoluteStep, globalLineIndex: gli)

  let evRes = w.ctfs.writeEvent(w.execWriter, ev)
  if evRes.isErr:
    return err("failed to write step event: " & evRes.error)

  # Write values parallel to this step
  let valRes = w.ctfs.writeStepValues(w.valueWriter, values)
  if valRes.isErr:
    return err("failed to write step values: " & valRes.error)

  w.lastGlobalLineIndex = gli
  w.lastPathId = pathId
  w.lastLine = line
  w.stepCount += 1
  ok()

# ---------------------------------------------------------------------------
# Call / Return
# ---------------------------------------------------------------------------

proc registerCall*(w: var MultiStreamTraceWriter, functionId: uint64,
    args: openArray[seq[byte]]): Result[void, string] =
  ## Register a function call entry. Pushes onto the internal call stack.
  ## The call record is written when registerReturn is called.
  if w.closed:
    return err("writer is closed")

  let parentKey =
    if w.callStack.len > 0:
      int64(w.callCount) - 1  # approximate: use the parent's eventual callKey
    else:
      -1'i64

  var argsSeq = newSeq[seq[byte]](args.len)
  for i in 0 ..< args.len:
    argsSeq[i] = args[i]

  w.callStack.add(PendingCall(
    functionId: functionId,
    entryStep: w.stepCount,
    depth: w.currentDepth,
    parentCallKey: parentKey,
    args: argsSeq,
    children: @[],
  ))
  w.currentDepth += 1
  ok()

proc registerReturn*(w: var MultiStreamTraceWriter,
    returnValue: seq[byte] = @[]): Result[void, string] =
  ## Register a function return. Pops the call stack and writes the call record.
  if w.closed:
    return err("writer is closed")
  if w.callStack.len == 0:
    return err("call stack underflow: return without matching call")

  let pending = w.callStack[^1]
  w.callStack.setLen(w.callStack.len - 1)
  w.currentDepth -= 1

  let retVal = if returnValue.len == 0: @[VoidReturnMarker] else: returnValue

  let callKey = w.callCount

  # If there's a parent on the stack, register this as a child
  if w.callStack.len > 0:
    w.callStack[^1].children.add(callKey)

  let rec = call_stream.CallRecord(
    functionId: pending.functionId,
    parentCallKey: pending.parentCallKey,
    entryStep: pending.entryStep,
    exitStep: if w.stepCount > 0: w.stepCount - 1 else: 0,
    depth: pending.depth,
    args: pending.args,
    returnValue: retVal,
    exception: @[],
    children: pending.children,
  )

  let res = w.ctfs.writeCall(w.callWriter, rec)
  if res.isErr:
    return err("failed to write call record: " & res.error)

  w.callCount += 1
  ok()

# ---------------------------------------------------------------------------
# IO events
# ---------------------------------------------------------------------------

proc registerIOEvent*(w: var MultiStreamTraceWriter, kind: IOEventKind,
    data: openArray[byte]): Result[void, string] =
  ## Register an IO event (stdout, stderr, etc.) at the current step.
  if w.closed:
    return err("writer is closed")

  var dataSeq = newSeq[byte](data.len)
  for i in 0 ..< data.len:
    dataSeq[i] = data[i]

  let ev = IOEvent(
    kind: kind,
    stepId: if w.stepCount > 0: w.stepCount - 1 else: 0,
    data: dataSeq,
  )

  let res = w.ctfs.writeEvent(w.ioEventWriter, ev)
  if res.isErr:
    return err("failed to write IO event: " & res.error)
  ok()

# ---------------------------------------------------------------------------
# Exception events
# ---------------------------------------------------------------------------

proc registerRaise*(w: var MultiStreamTraceWriter, exceptionTypeId: uint64,
    message: openArray[byte]): Result[void, string] =
  ## Register a raise event in the execution stream.
  ## Also writes an empty value record to keep the value stream in sync.
  if w.closed:
    return err("writer is closed")

  var msgSeq = newSeq[byte](message.len)
  for i in 0 ..< message.len:
    msgSeq[i] = message[i]

  let ev = StepEvent(kind: sekRaise,
    exceptionTypeId: exceptionTypeId, message: msgSeq)
  let res = w.ctfs.writeEvent(w.execWriter, ev)
  if res.isErr:
    return err("failed to write raise event: " & res.error)

  # Write empty values to keep streams in sync
  let valRes = w.ctfs.writeStepValues(w.valueWriter, @[])
  if valRes.isErr:
    return err("failed to write raise values: " & valRes.error)

  w.stepCount += 1
  ok()

proc registerCatch*(w: var MultiStreamTraceWriter,
    exceptionTypeId: uint64): Result[void, string] =
  ## Register a catch event in the execution stream.
  ## Also writes an empty value record to keep the value stream in sync.
  if w.closed:
    return err("writer is closed")

  let ev = StepEvent(kind: sekCatch, catchExceptionTypeId: exceptionTypeId)
  let res = w.ctfs.writeEvent(w.execWriter, ev)
  if res.isErr:
    return err("failed to write catch event: " & res.error)

  # Write empty values to keep streams in sync
  let valRes = w.ctfs.writeStepValues(w.valueWriter, @[])
  if valRes.isErr:
    return err("failed to write catch values: " & valRes.error)

  w.stepCount += 1
  ok()

# ---------------------------------------------------------------------------
# Close
# ---------------------------------------------------------------------------

proc close*(w: var MultiStreamTraceWriter): Result[void, string] =
  ## Flush all streams, write meta.dat, and finalize.
  ## After close, the CTFS bytes can be retrieved via toBytes().
  if w.closed:
    return ok()

  # Flush exec stream
  let flushRes = w.ctfs.flush(w.execWriter)
  if flushRes.isErr:
    return err("failed to flush exec stream: " & flushRes.error)

  # Write meta.dat
  let metaFileRes = w.ctfs.addFile("meta.dat")
  if metaFileRes.isErr:
    return err("failed to add meta.dat: " & metaFileRes.error)
  var metaFile = metaFileRes.get()

  let metaRes = w.ctfs.writeMetaDat(metaFile, w.metadata, w.paths)
  if metaRes.isErr:
    return err("failed to write meta.dat: " & metaRes.error)

  w.closed = true
  ok()

proc toBytes*(w: var MultiStreamTraceWriter): seq[byte] =
  ## Get the serialized CTFS bytes. Must call close() first.
  w.ctfs.toBytes()

proc closeCtfs*(w: var MultiStreamTraceWriter) =
  ## Release any resources held by the underlying CTFS container.
  w.ctfs.closeCtfs()
