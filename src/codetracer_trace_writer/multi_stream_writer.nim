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
import ./linehits_builder
import ./uuid_v7
import ../codetracer_trace_types

export results, value_stream.VariableValue, io_event_stream.IOEventKind,
       codetracer_trace_types.FilterProvenance, uuid_v7

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
    callKey: uint64
      ## call_key allocated at entry time. CTFS-M-CallKeyOrder: keys are
      ## assigned monotonically at `registerCall` so that parent
      ## call_key < child call_key and entry order matches key order.
      ## The matching CallRecord is buffered in `completedCalls` at this
      ## index and flushed to the call stream in key order (see close()).
    args: seq[CallArg]
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

    # Optional linehits builder
    linehitsBuilder: Option[LinehitsBuilder]

    # State tracking
    stepCount*: uint64
    callCount: uint64
      ## Total number of CallRecords already written to the call stream.
      ## With CTFS-M-CallKeyOrder this advances as buffered records flush
      ## in entry order (not as registerReturn fires).
    nextCallKey: uint64
      ## Monotonic call_key generator. Incremented at each `registerCall`
      ## so call_keys reflect entry order across nested calls.
    lastGlobalLineIndex: uint64
    lastPathId: uint64
    lastLine: uint64
    callStack: seq[PendingCall]
    completedCalls: seq[(uint64, call_stream.CallRecord)]
      ## CTFS-M-CallKeyOrder: finished CallRecords waiting to be written
      ## to the call stream. Filled in registerReturn (in exit order) and
      ## drained in call_key (entry) order. The stream is the CTFS
      ## VariableRecordTable "calls"; record position == call_key, so we
      ## must write in key order. When `callStack` returns to empty, we
      ## know every key issued so far has a completed record and flush
      ## all of them at once; close() also drains any leftovers.
    currentDepth: uint32
    closed: bool
    filePath: string

    # TF-M7: trace-filter chain provenance (spec § 7).  When non-empty
    # OR when `recordEmptyFilterProvenance` is set, the close() path
    # emits FlagHasTraceFilterProvenance on meta.dat and writes the
    # per-entry (path, sha256) block.  Recorders integrating the
    # trace-filter library set this from their composed Classifier
    # before close().
    filterProvenance*: seq[FilterProvenance]
    recordEmptyFilterProvenance*: bool
      ## When true and `filterProvenance` is empty, the writer still
      ## emits an empty provenance block.  Use this for recorders that
      ## implement trace filters but ended up with a zero-length chain
      ## (spec § 7 distinguishes "no provenance recorded" from
      ## "provenance recorded but empty").

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
    chunkSize: int = 4096,
    recordingId: string = ""): Result[MultiStreamTraceWriter, string] =
  ## Create a new multi-stream trace writer.
  ## Produces an in-memory CTFS container (call close() to finalize).
  ##
  ## ~recordingId~ defaults to a freshly-minted UUIDv7 (M-REC-1).  Pass
  ## an explicit canonical-form id to pin the recording's identity
  ## (e.g. on the import path where the source recording's id must be
  ## preserved).
  var resolvedId = recordingId
  if resolvedId.len == 0:
    let uuidRes = newUuidV7()
    if uuidRes.isErr:
      return err("failed to mint recording_id: " & uuidRes.error)
    resolvedId = $uuidRes.get()
  else:
    let valRes = validateRecordingIdStr(resolvedId)
    if valRes.isErr:
      return err("recordingId is not a canonical UUIDv7: " & valRes.error)

  var w: MultiStreamTraceWriter
  w.ctfs = createCtfs()
  w.metadata = TraceMetadata(
    recordingId: resolvedId, program: program, args: @[], workdir: "")
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
# Linehits (optional)
# ---------------------------------------------------------------------------

proc enableLinehits*(w: var MultiStreamTraceWriter) =
  ## Enable the linehits builder. Must be called before writing steps.
  w.linehitsBuilder = some(initLinehitsBuilder())

# ---------------------------------------------------------------------------
# Filter provenance (TF-M7 — spec §7 / Trace-Filters.md §7)
# ---------------------------------------------------------------------------

proc setFilterProvenance*(w: var MultiStreamTraceWriter,
                          entries: openArray[FilterProvenance];
                          recordEvenIfEmpty: bool = false) =
  ## Record the active trace-filter chain in composition order.
  ##
  ## When the resulting sequence is non-empty, OR when
  ## `recordEvenIfEmpty` is true, `close()` sets
  ## `FlagHasTraceFilterProvenance` on the meta.dat header and emits
  ## the per-entry block.  Recorders that implement trace filters
  ## SHOULD pass `recordEvenIfEmpty = true` so the flag distinguishes
  ## "implements filters but chain happens to be empty" from "doesn't
  ## record provenance at all" (spec §7).
  ##
  ## Calling this proc replaces any previously set provenance — there
  ## is no append API by design: the caller composes the full
  ## composition chain (builtin default → auto-discovered → env →
  ## CLI) once before close().
  w.filterProvenance = @[]
  for e in entries:
    w.filterProvenance.add(e)
  w.recordEmptyFilterProvenance = recordEvenIfEmpty

proc linehits*(w: var MultiStreamTraceWriter): var LinehitsBuilder =
  ## Access the linehits builder. Raises if not enabled.
  w.linehitsBuilder.get()

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
# Function / Type / Varname registration (interning)
# ---------------------------------------------------------------------------

proc registerFunction*(w: var MultiStreamTraceWriter,
    name: string): Result[uint64, string] =
  ## Register a function name and return its interned ID.
  w.ctfs.ensureFunctionId(w.interning, name)

proc registerType*(w: var MultiStreamTraceWriter,
    name: string): Result[uint64, string] =
  ## Register a type name and return its interned ID.
  w.ctfs.ensureTypeId(w.interning, name)

proc registerVarname*(w: var MultiStreamTraceWriter,
    name: string): Result[uint64, string] =
  ## Register a variable name and return its interned ID.
  w.ctfs.ensureVarnameId(w.interning, name)

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

  # Record linehit if enabled
  if w.linehitsBuilder.isSome:
    w.linehitsBuilder.get().recordHit(gli, w.stepCount)

  w.lastGlobalLineIndex = gli
  w.lastPathId = pathId
  w.lastLine = line
  w.stepCount += 1
  ok()

# ---------------------------------------------------------------------------
# Call / Return
# ---------------------------------------------------------------------------

proc flushCompletedCalls(w: var MultiStreamTraceWriter): Result[void, string] =
  ## CTFS-M-CallKeyOrder: drain `completedCalls` in call_key order to the
  ## call stream. Called when `callStack` empties (every key issued so far
  ## has a finished record) and from close() for any leftovers.
  ##
  ## Records were buffered in registerReturn in exit order; their call_keys
  ## were assigned at entry time so a child key > parent key. Sorting by
  ## call_key and appending in that order makes the on-disk record index
  ## equal to the entry-order call_key.
  if w.completedCalls.len == 0:
    return ok()
  # Insertion-sort by callKey: typical fan-out is small (1..few siblings
  # per parent), so this is effectively linear and avoids pulling in a
  # generic sort over a tuple type.
  for i in 1 ..< w.completedCalls.len:
    var j = i
    while j > 0 and w.completedCalls[j - 1][0] > w.completedCalls[j][0]:
      let tmp = w.completedCalls[j - 1]
      w.completedCalls[j - 1] = w.completedCalls[j]
      w.completedCalls[j] = tmp
      dec j
  for entry in w.completedCalls:
    let res = w.ctfs.writeCall(w.callWriter, entry[1])
    if res.isErr:
      return err("failed to write call record: " & res.error)
    w.callCount += 1
  w.completedCalls.setLen(0)
  ok()

proc registerCall*(w: var MultiStreamTraceWriter, functionId: uint64,
    args: openArray[CallArg]): Result[void, string] =
  ## Register a function call entry. Pushes onto the internal call stack
  ## and allocates the call_key immediately so entry order matches key
  ## order (CTFS-M-CallKeyOrder).
  ##
  ## The matching CallRecord is materialized in `registerReturn` and
  ## buffered in `completedCalls`; it reaches the call stream once the
  ## enclosing root call returns (or at close() for partial traces),
  ## with all entries written in call_key order.
  ##
  ## ``args`` carries one (varname_id, CBOR value) entry per parameter so the
  ## frontend can render the call's argument names alongside their values.
  if w.closed:
    return err("writer is closed")

  let parentKey =
    if w.callStack.len > 0:
      int64(w.callStack[^1].callKey)
    else:
      -1'i64

  let callKey = w.nextCallKey
  w.nextCallKey += 1

  # If there's a parent on the stack, register this as a child now —
  # the parent's CallRecord won't be assembled until its own return
  # fires, by which time all child keys are already in its `children`.
  if w.callStack.len > 0:
    w.callStack[^1].children.add(callKey)

  var argsSeq = newSeq[CallArg](args.len)
  for i in 0 ..< args.len:
    argsSeq[i] = args[i]

  w.callStack.add(PendingCall(
    functionId: functionId,
    entryStep: w.stepCount,
    depth: w.currentDepth,
    parentCallKey: parentKey,
    callKey: callKey,
    args: argsSeq,
    children: @[],
  ))
  w.currentDepth += 1
  ok()

proc registerReturn*(w: var MultiStreamTraceWriter,
    returnValue: seq[byte] = @[]): Result[void, string] =
  ## Register a function return. Pops the call stack and buffers the
  ## CallRecord under its entry-allocated call_key. The buffer flushes
  ## (in call_key order) once `callStack` becomes empty, ensuring the
  ## record position in the call stream equals its entry-order call_key.
  if w.closed:
    return err("writer is closed")
  if w.callStack.len == 0:
    return err("call stack underflow: return without matching call")

  let pending = w.callStack[^1]
  w.callStack.setLen(w.callStack.len - 1)
  w.currentDepth -= 1

  let retVal = if returnValue.len == 0: @[VoidReturnMarker] else: returnValue

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

  w.completedCalls.add((pending.callKey, rec))

  # When the root call returns, every key issued so far has a buffered
  # record. Flush them now in key order so memory stays bounded for
  # long traces composed of many top-level calls.
  if w.callStack.len == 0:
    let flushRes = w.flushCompletedCalls()
    if flushRes.isErr:
      return err(flushRes.error)
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
# Thread events
# ---------------------------------------------------------------------------
#
# ThreadStart / ThreadExit / ThreadSwitch are emitted as exec-stream step
# events (parallel to Raise / Catch).  Each thread event is paired with an
# empty values record so the value stream stays in lock-step with the exec
# stream.  This mirrors registerRaise / registerCatch and lets readers walk
# `step(n)` / `values(n)` without special-casing the thread events.
#
# Recorders that route TraceLowLevelEvent::ThreadStart / ThreadExit /
# ThreadSwitch through TraceWriter::add_event end up here via the FFI's
# trace_writer_register_thread_start / _exit / _switch entry points.  Before
# the dedicated entry points existed, add_event was a silent no-op on the
# Nim multi-stream backend — the cause of the 1.21 / 1.22 / 1.27 incidents
# and the reason the Ruby recorder's three add_event call sites could not
# capture thread lifecycle events.

proc registerThreadSwitch*(w: var MultiStreamTraceWriter,
    threadId: uint64): Result[void, string] =
  ## Register a thread-switch event in the execution stream.
  ## Also writes an empty value record to keep the value stream in sync.
  if w.closed:
    return err("writer is closed")

  let ev = StepEvent(kind: sekThreadSwitch, threadId: threadId)
  let res = w.ctfs.writeEvent(w.execWriter, ev)
  if res.isErr:
    return err("failed to write thread_switch event: " & res.error)

  let valRes = w.ctfs.writeStepValues(w.valueWriter, @[])
  if valRes.isErr:
    return err("failed to write thread_switch values: " & valRes.error)

  w.stepCount += 1
  ok()

proc registerThreadStart*(w: var MultiStreamTraceWriter,
    threadId: uint64): Result[void, string] =
  ## Register a thread-start event (a new thread came into existence).
  ## Also writes an empty value record to keep the value stream in sync.
  if w.closed:
    return err("writer is closed")

  let ev = StepEvent(kind: sekThreadStart, startThreadId: threadId)
  let res = w.ctfs.writeEvent(w.execWriter, ev)
  if res.isErr:
    return err("failed to write thread_start event: " & res.error)

  let valRes = w.ctfs.writeStepValues(w.valueWriter, @[])
  if valRes.isErr:
    return err("failed to write thread_start values: " & valRes.error)

  w.stepCount += 1
  ok()

proc registerThreadExit*(w: var MultiStreamTraceWriter,
    threadId: uint64): Result[void, string] =
  ## Register a thread-exit event (a thread terminated).
  ## Also writes an empty value record to keep the value stream in sync.
  if w.closed:
    return err("writer is closed")

  let ev = StepEvent(kind: sekThreadExit, exitThreadId: threadId)
  let res = w.ctfs.writeEvent(w.execWriter, ev)
  if res.isErr:
    return err("failed to write thread_exit event: " & res.error)

  let valRes = w.ctfs.writeStepValues(w.valueWriter, @[])
  if valRes.isErr:
    return err("failed to write thread_exit values: " & valRes.error)

  w.stepCount += 1
  ok()

# ---------------------------------------------------------------------------
# Close
# ---------------------------------------------------------------------------

proc close*(w: var MultiStreamTraceWriter): Result[void, string] =
  ## Flush all streams, write meta.dat, and finalize.
  ## After close, the CTFS bytes can be retrieved via toBytes().
  ##
  ## Drains any unclosed PendingCalls left on the call stack (LIFO,
  ## innermost-first) so partial-trace recordings (panic, trap,
  ## exit-without-return) still produce balanced call_entry/call_exit
  ## pairs in the call stream rather than silently losing the deepest
  ## un-popped frames.
  if w.closed:
    return ok()

  # Finalize linehits if enabled
  if w.linehitsBuilder.isSome:
    let lhRes = w.linehitsBuilder.get().finalize()
    if lhRes.isErr:
      return err("failed to finalize linehits: " & lhRes.error)

  # Drain any unclosed call frames before flushing the exec stream and
  # writing meta. We mirror registerReturn's semantics: exitStep is the
  # last produced step, returnValue is VoidReturnMarker, and child links
  # are propagated up the stack so callKey ordering stays valid.
  #
  # CTFS-M-CallKeyOrder: call_keys are already allocated (at entry time)
  # and child links were registered against the parent when each child
  # was entered, so here we just synthesize the missing CallRecords for
  # the still-open frames and buffer them. The final flushCompletedCalls
  # below writes everything in call_key (entry) order.
  while w.callStack.len > 0:
    let pending = w.callStack[^1]
    w.callStack.setLen(w.callStack.len - 1)
    if w.currentDepth > 0:
      w.currentDepth -= 1

    let rec = call_stream.CallRecord(
      functionId: pending.functionId,
      parentCallKey: pending.parentCallKey,
      entryStep: pending.entryStep,
      exitStep: if w.stepCount > 0: w.stepCount - 1 else: 0,
      depth: pending.depth,
      args: pending.args,
      returnValue: @[VoidReturnMarker],
      exception: @[],
      children: pending.children,
    )

    w.completedCalls.add((pending.callKey, rec))

  # Flush any buffered call records in call_key order. This covers both
  # the records synthesized above for unclosed frames and any leftover
  # buffered records (e.g. if the outermost call never returned, the
  # incremental flush at the empty-stack point never fired).
  let drainRes = w.flushCompletedCalls()
  if drainRes.isErr:
    return err("failed to flush unclosed call records: " & drainRes.error)

  # Flush exec stream
  let flushRes = w.ctfs.flush(w.execWriter)
  if flushRes.isErr:
    return err("failed to flush exec stream: " & flushRes.error)

  # Write meta.dat
  let metaFileRes = w.ctfs.addFile("meta.dat")
  if metaFileRes.isErr:
    return err("failed to add meta.dat: " & metaFileRes.error)
  var metaFile = metaFileRes.get()

  let metaRes = w.ctfs.writeMetaDat(
    metaFile, w.metadata, w.paths,
    filterProvenance = w.filterProvenance,
    emitFilterProvenance = w.recordEmptyFilterProvenance)
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
