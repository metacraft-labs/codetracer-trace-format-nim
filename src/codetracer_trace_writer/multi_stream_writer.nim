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
import ../codetracer_ctfs/variable_record_table
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
  SourceViewRecord* = object
    ## In-memory shape of one ``source_views.dat`` record, mirroring the
    ## on-disk encoding documented at
    ## ``codetracer-trace-format-spec/internal-files.md`` §
    ## "Alternate Source Views (Deminification Support)".  The record
    ## carries one formatted view of one source path together with a
    ## sourcemap V3 that translates positions in ``content`` back to
    ## positions in the original (typically minified) source at
    ## ``pathId``.
    pathId*: uint64
      ## Index into ``paths.dat`` — the original source this view
      ## applies to.  Writers MUST validate the id against the
      ## currently-registered paths before appending so a malformed
      ## index can never reach the on-disk record.
    viewKind*: uint8
      ## 0 = raw (rarely emitted), 1 = prettier_format, 2 = black_format,
      ## 3-127 reserved, 128+ vendor-specific.
    viewName*: string
      ## Human-readable name shown in the UI (e.g.
      ## ``"lodash.fmt.js"``).
    content*: seq[byte]
      ## The formatted source as UTF-8 bytes.
    sourcemapV3*: seq[byte]
      ## Sourcemap V3 JSON (UTF-8), translating
      ## ``(generated_line, generated_column)`` in ``content`` →
      ## ``(original_line, original_column)`` in the source at
      ## ``pathId``.  Length-zero is the spec-allowed "no sourcemap"
      ## marker.

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
    pathLineLengths: seq[seq[uint32]]
      ## P6 follow-up — per-path line-length tables, used in column-aware
      ## mode to compute byte-offset-based ``global_position_index`` values
      ## that match the reader's ``decodeGlobalPositionIndex`` expectation
      ## per spec §"Source Location Addressing".  Parallel to ``paths``;
      ## empty seq for files whose line_lengths the caller didn't
      ## supply.  Ignored when ``columnAwareSteps`` is false.

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

    # P6.3 / P6.4 — column-aware step mode.  When set:
    #  * `writeColumnStep` is permitted (emits tag 0x07, sekDeltaColumn);
    #  * `meta.dat` flags include `FlagHasColumnAwareSteps` (bit 4) so
    #    column-unaware readers reject the trace cleanly instead of
    #    silently misdecoding the step stream.
    columnAwareSteps*: bool
      ## True iff this writer is producing a column-aware trace.  Gates
      ## tag 0x07 emission and the bit-4 flag on meta.dat.  Defaults to
      ## false so existing callers keep producing line-only traces
      ## byte-for-byte identical to the pre-P6.4 output.

    # Deminification / alternate source views (spec §
    # "Alternate Source Views (Deminification Support)").  Buffered
    # in memory and serialized into ``source_views.dat`` /
    # ``source_views.off`` on close() only when at least one view has
    # been registered — pre-extension writers (no registerSourceView
    # call) leave the CTFS container untouched so their output stays
    # byte-for-byte identical to pre-deminification traces.
    sourceViews*: seq[SourceViewRecord]
      ## In-order list of formatted-source views.  Index in this seq
      ## becomes the on-disk record index.  Empty until a recorder
      ## opts in via ``registerSourceView``.

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc rebuildGli(w: var MultiStreamTraceWriter) =
  ## Rebuild the global line index from the current set of paths.
  ##
  ## In column-aware mode each file's slot is sized to the file's total
  ## byte capacity (sum of per-line lengths) so the resulting
  ## ``global_position_index`` matches the spec's byte-offset-based
  ## addressing.  Files whose ``lineLengths`` weren't supplied fall back
  ## to the legacy ``DefaultLinesPerFile`` allocation.
  ##
  ## In line-only mode every file gets the legacy ``DefaultLinesPerFile``
  ## allocation, preserving byte-for-byte output of pre-P6 traces.
  var counts = newSeq[uint64](w.paths.len)
  for i in 0 ..< w.paths.len:
    if w.columnAwareSteps and i < w.pathLineLengths.len and
       w.pathLineLengths[i].len > 0:
      var total: uint64 = 0
      for L in w.pathLineLengths[i]:
        total += uint64(L)
      counts[i] = max(total, 1'u64)
    else:
      counts[i] = DefaultLinesPerFile
  w.gli = buildGlobalLineIndex(counts)
  w.gliDirty = false

proc toGlobalLineIndex(w: var MultiStreamTraceWriter,
    pathId: uint64, line: uint64): uint64 =
  ## In column-aware mode, returns the byte-offset-based
  ## ``global_position_index`` of column 1 on ``line`` (the spec's
  ## reset-on-line-change semantic: after a ``register_step`` the cursor
  ## column is 1, and subsequent ``DeltaColumn`` events advance it
  ## within the line).
  ##
  ## In line-only mode, returns the legacy ``file_base + line`` value
  ## so traces produced without column data are byte-for-byte identical
  ## to pre-P6 output.
  if w.gliDirty:
    w.rebuildGli()
  if w.columnAwareSteps and pathId < uint64(w.pathLineLengths.len) and
     w.pathLineLengths[int(pathId)].len > 0:
    # Cumulative byte offset of column 1 on ``line``: sum of the lengths
    # of preceding lines.  ``line`` is 1-based per the cursor convention;
    # line 1 sits at offset 0 within the file.  When ``line`` exceeds the
    # known line count we clamp to the file's total capacity (the reader's
    # ``decodeGlobalPositionIndex`` handles past-end addresses the same
    # way).
    let lls = w.pathLineLengths[int(pathId)]
    var lineOffset: uint64 = 0
    let upTo = min(int(line) - 1, lls.len)
    for i in 0 ..< upTo:
      lineOffset += uint64(lls[i])
    return w.gli.prefixSum[int(pathId)] + lineOffset
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
# Column-aware step mode (P6.3 / P6.4)
# ---------------------------------------------------------------------------

proc enableColumnAwareSteps*(w: var MultiStreamTraceWriter) =
  ## Opt this writer into column-aware step encoding.  After calling
  ## this, ``writeColumnStep`` is permitted, and ``close()`` will set
  ## ``FlagHasColumnAwareSteps`` (bit 4) on ``meta.dat`` so
  ## column-unaware readers reject the trace cleanly via the reserved
  ## bits-4-15 check (see spec §"Reader Behaviour and Back-Compat").
  ##
  ## Must be called before any step events are written — the flag is
  ## trace-global; the writer MUST NOT mix column-aware and line-only
  ## step records within a single trace.
  w.columnAwareSteps = true

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
    path: string,
    lineLengths: openArray[uint32] = []): Result[uint64, string] =
  ## Register a source path and return its interned ID.
  ##
  ## P6.5 / Layout A — ``lineLengths`` is the per-line addressable
  ## column count, used only when the writer has opted into
  ## column-aware mode (``enableColumnAwareSteps``).  When the trace is
  ## column-aware, the on-disk paths.dat record is extended to carry
  ## ``path_len + path_bytes + line_count + zigzag-delta line_lengths``
  ## (spec §"paths.dat per-line offset table — Layout A").  When the
  ## trace is line-only, ``lineLengths`` is ignored and the legacy
  ## bare-path-bytes record format is preserved byte-for-byte.
  ##
  ## Recorders that don't yet surface per-line column counts can leave
  ## ``lineLengths`` at its default empty value.  Column-aware traces
  ## still write the ``path_len`` and ``line_count = 0`` framing so
  ## the reader can decode the record uniformly — empty
  ## ``lineLengths`` just signals "no per-line data available yet"
  ## and column resolution falls back to surfacing ``None``.
  let idRes =
    if w.columnAwareSteps:
      w.ctfs.ensurePathIdColumnAware(w.interning, path, lineLengths)
    else:
      w.ctfs.ensurePathId(w.interning, path)
  if idRes.isErr:
    return err(idRes.error)
  let id = idRes.get()
  # Track paths list for meta.dat (only add if new)
  if id == uint64(w.paths.len):
    w.paths.add(path)
    # Mirror the per-file line-lengths so ``toGlobalLineIndex`` can
    # compute byte-offset positions in column-aware mode.  When line
    # lengths weren't supplied, store an empty seq so ``rebuildGli``
    # falls back to the legacy ``DefaultLinesPerFile`` allocation for
    # that path.
    if w.columnAwareSteps:
      var lls = newSeq[uint32](lineLengths.len)
      for i in 0 ..< lineLengths.len:
        lls[i] = lineLengths[i]
      w.pathLineLengths.add(lls)
    else:
      w.pathLineLengths.add(@[])
    w.gliDirty = true
  ok(id)

# ---------------------------------------------------------------------------
# Alternate source views (Deminification Support).  Spec §
# "Alternate Source Views (Deminification Support)" in
# ``codetracer-trace-format-spec/internal-files.md``.
# ---------------------------------------------------------------------------

proc registerSourceView*(w: var MultiStreamTraceWriter,
    pathId: uint64,
    viewKind: uint8,
    viewName: string,
    content: seq[byte],
    sourcemapV3: seq[byte]): Result[uint64, string] =
  ## Buffer a formatted-view record for emission into
  ## ``source_views.dat`` at ``close()`` time.  Returns the new view's
  ## 0-based index in the (per-trace) source-views table.
  ##
  ## ``pathId`` MUST refer to a path already registered via
  ## ``registerPath``.  Validating up front lets us reject a malformed
  ## index at the call site rather than at serialization time when the
  ## trace is being finalized.
  ##
  ## Emitting any record flips ``FlagHasAlternateSourceViews`` (bit 5)
  ## on meta.dat at close time — pre-extension readers reject the
  ## trace cleanly via the strict-rejection contract (spec §
  ## "Reader Behaviour and Back-Compat").  Writers that never call
  ## this proc keep producing pre-extension-compatible traces
  ## byte-for-byte (no source_views files, no flag bit).
  if w.closed:
    return err("writer is closed")
  if pathId >= uint64(w.paths.len):
    return err("registerSourceView: path_id " & $pathId &
      " is out of range (only " & $w.paths.len & " path(s) registered)")
  let idx = uint64(w.sourceViews.len)
  w.sourceViews.add(SourceViewRecord(
    pathId: pathId,
    viewKind: viewKind,
    viewName: viewName,
    content: content,
    sourcemapV3: sourcemapV3,
  ))
  ok(idx)

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

proc registerColumnStep*(w: var MultiStreamTraceWriter,
    columnDelta: int64,
    values: openArray[VariableValue]): Result[void, string] =
  ## Emit a column-only step (sekDeltaColumn, tag 0x07) that advances
  ## the cursor's column within the current line.  ``columnDelta`` is a
  ## signed zigzag varint on the wire; magnitudes ≤ ±63 cost two bytes
  ## (1 tag + 1 varint) — see spec §"Column Encoding — `DeltaColumn`
  ## (chosen)".
  ##
  ## Only callable on a writer that has opted into column-aware mode via
  ## ``enableColumnAwareSteps``.  The first step in a trace must still
  ## be a line-aware ``registerStep`` so the running
  ## ``global_position_index`` is well-defined before column deltas are
  ## applied.
  if w.closed:
    return err("writer is closed")
  if not w.columnAwareSteps:
    return err("registerColumnStep called on a writer that has not " &
      "opted into column-aware mode (call enableColumnAwareSteps first)")
  if w.stepCount == 0:
    return err("registerColumnStep cannot be the first step — emit an " &
      "AbsoluteStep (registerStep) first so the cursor position is defined")

  # In column-aware mode `global_position_index` is one-dimensional, so
  # a column delta is also a position delta.  The exec-stream writer
  # picks up the running index from `lastGlobalLineIndex` already.
  let ev = StepEvent(kind: sekDeltaColumn, columnDelta: columnDelta)
  let evRes = w.ctfs.writeEvent(w.execWriter, ev)
  if evRes.isErr:
    return err("failed to write delta-column event: " & evRes.error)

  let valRes = w.ctfs.writeStepValues(w.valueWriter, values)
  if valRes.isErr:
    return err("failed to write step values: " & valRes.error)

  # Update running line/position index by the column delta.  Path /
  # line slots are unchanged (column-only motion stays within the
  # current line by construction).
  w.lastGlobalLineIndex = uint64(int64(w.lastGlobalLineIndex) + columnDelta)

  if w.linehitsBuilder.isSome:
    w.linehitsBuilder.get().recordHit(w.lastGlobalLineIndex, w.stepCount)

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

  # CTFS-M entry_step convention: the call_entry event MUST be emitted at
  # a step index that lies within the trace's [0, stepCount) range so the
  # downstream reader (ct-print) can place it during its single-pass walk.
  # The FFI flushes any pending step BEFORE calling registerCall (so the
  # step that produced the call's argument context is already written),
  # which means `w.stepCount` here is the count AFTER that flush — i.e.
  # the index of the NEXT step.  Capturing that value works for non-leaf
  # callees (the first body step of the callee fills the slot), but for
  # leaf calls (snforge contract_call / storage_read / etc. that have no
  # further body steps before register_return) it points past the last
  # emitted step and the call_entry is silently dropped by ct-print.
  # Mirror registerReturn's symmetric convention: use the just-flushed
  # step's index (`stepCount - 1`), which is always within range for any
  # caller that emitted at least one step before the call.  For the very
  # first registerCall in a trace (before any step has been flushed), fall
  # back to 0 so the call_entry still places at the trace's initial step.
  w.callStack.add(PendingCall(
    functionId: functionId,
    entryStep: if w.stepCount > 0: w.stepCount - 1 else: 0,
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

  # Emit source_views.dat / source_views.off when the writer has any
  # alternate-view records buffered.  Skipped entirely when none have
  # been registered so pre-extension traces remain byte-for-byte
  # identical to the pre-deminification output (back-compat contract
  # for the bit-5 meta.dat flag).
  #
  # NOTE on file naming: the spec section uses ``source_views.dat`` /
  # ``source_views.off``, but the CTFS base40 filename encoding caps
  # internal-file names at 12 characters (see
  # ``codetracer_ctfs/base40.nim``).  ``source_views.dat`` is 16 chars
  # and silently truncates to ``source_views`` — colliding with the
  # ``.off`` entry — so we use the 12-char abbreviation
  # ``srcviews.dat`` / ``srcviews.off`` on disk.  Readers must look
  # for these abbreviated names; the spec text is the conceptual
  # reference and the abbreviation is the wire-format reality.
  const SourceViewsBaseName = "srcviews"
  let hasSourceViews = w.sourceViews.len > 0
  if hasSourceViews:
    let svTableRes = initVariableRecordTableWriter(
      w.ctfs, SourceViewsBaseName)
    if svTableRes.isErr:
      return err("failed to init source_views table: " & svTableRes.error)
    var svTable = svTableRes.get()
    for sv in w.sourceViews:
      var rec: seq[byte] = @[]
      encodeVarint(sv.pathId, rec)
      rec.add(sv.viewKind)
      encodeVarint(uint64(sv.viewName.len), rec)
      for i in 0 ..< sv.viewName.len:
        rec.add(byte(sv.viewName[i]))
      encodeVarint(uint64(sv.content.len), rec)
      for b in sv.content:
        rec.add(b)
      encodeVarint(uint64(sv.sourcemapV3.len), rec)
      for b in sv.sourcemapV3:
        rec.add(b)
      let appendRes = w.ctfs.append(svTable, rec)
      if appendRes.isErr:
        return err("failed to write source_views record: " & appendRes.error)

  # Write meta.dat
  let metaFileRes = w.ctfs.addFile("meta.dat")
  if metaFileRes.isErr:
    return err("failed to add meta.dat: " & metaFileRes.error)
  var metaFile = metaFileRes.get()

  let metaRes = w.ctfs.writeMetaDat(
    metaFile, w.metadata, w.paths,
    filterProvenance = w.filterProvenance,
    emitFilterProvenance = w.recordEmptyFilterProvenance,
    columnAwareSteps = w.columnAwareSteps,
    alternateSourceViews = hasSourceViews)
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
