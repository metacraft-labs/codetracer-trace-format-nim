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
import std/tables
import results
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ../codetracer_ctfs/streaming
import ../codetracer_ctfs/variable_record_table
import ../codetracer_ctfs/crossing_state
import ./meta_dat
import ./corrmark_builder
import ./interning_table
import ./exec_stream
import ./value_stream
import ./call_stream
import ./io_event_stream
import ./span_stream
import ./step_encoding
import ./global_line_index
import ./varint
import ./linehits_builder
import ./step_map_builder
import ./uuid_v7
import ../codetracer_trace_types

export results, value_stream.VariableValue, io_event_stream.IOEventKind,
       codetracer_trace_types.FilterProvenance, uuid_v7

# The line-only address-space stride, re-exported for the consumers that
# reach it through this module. It is defined beside the prefix-sum
# arithmetic in `global_line_index` because encoding with one value and
# inverting with another produces plausible wrong positions that no
# container check can catch.
export global_line_index.DefaultLinesPerFile

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

  PendingCrossing = object
    ## An OPEN native↔VM crossing span (Mixed-Trace-Debugging.md §3), stashed
    ## between `beginCrossing` and `endCrossing`.  `startStep` is snapshotted
    ## from `w.stepCount` at `beginCrossing` — the index of the first
    ## materialized step that will run inside the crossing — mirroring the
    ## `entryStep` capture in `registerCall`.  Crossings NEST (a VM frame can
    ## enter another VM frame before returning) and close strictly LIFO, so
    ## pending entries live on a flat stack (see `pendingCrossings`); each
    ## carries its own minted `spanId` so `endCrossing` can check that the caller
    ## is closing the innermost open crossing.
    spanId: uint64
    spanType: string
    startStep: uint64

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
    # Stage C — container ownership.  A writer either OWNS its container
    # (the classic `initMultiStreamWriter` path: `ownedCtfs` holds the
    # streaming CTFS created for `filePath`) or is ATTACHED to a container
    # that ANOTHER writer created and owns (`initMultiStreamWriterAttached`:
    # `sharedCtfs` points at the external container; `ownedCtfs` is unused).
    #
    # All stream writers and register procs go through the `container`
    # accessor rather than touching a field directly, so the two modes share
    # one data path.  Crucially the accessor computes the target lazily on
    # every call — it NEVER stores `addr ownedCtfs` in the object — so the
    # writer stays movable/returnable-by-value without a dangling self
    # pointer, and the owned path drives the exact same inline `ownedCtfs`
    # object it always did (byte-identical output; guarded by the corpus +
    # cross-read proofs).
    ownedCtfs: Ctfs
      ## Backing storage for an OWNED writer.  Left default-initialised and
      ## untouched when `attached` is true.
    sharedCtfs: ptr Ctfs
      ## Non-nil only when `attached`.  Points at the externally-owned
      ## container; the writer writes its streams here but never frees it or
      ## writes `meta.dat` into it (see `close`).
    attached: bool
      ## False (default) = OWNED; true = attached to an external container.
    execWriter: ExecStreamWriter
    valueWriter: ValueStreamWriter
    callWriter: CallStreamWriter
    ioEventWriter: IOEventStreamWriter
    spanWriter: SpanStreamWriter
      ## RS-M1 span stream.  Created LAZILY on the first `registerSpan` call —
      ## see `hasSpans`.
    markerLabels: InterningTableWriter
      ## `markers.dat` + `markers.off` — the correlation-marker label table,
      ## the same Variable-Size Record Table shape as paths/funcs/types/
      ## varnames.  Created LAZILY on the first `ensureMarkerId`, so a
      ## recording that declares no marker gains no files.
    hasMarkerLabels: bool
    correlationMarkers: seq[CorrelationMarker]
      ## Correlation markers declared during this recording
      ## (`Correlation-Markers.md` §2.4).  Accumulated here and bulk-loaded
      ## into `corrmark.ns` at close, because the index is a constructor over a
      ## known set rather than something inserted into per marker.
      ##
      ## A recording with no markers writes no namespace at all: its ABSENCE is
      ## a distinct, meaningful answer ("never indexed") from an empty index
      ## ("indexed, covers nothing").  See the contract §9.
    nextSpanId: uint64
      ## Monotonic span-id generator for spans this writer MINTS itself — i.e.
      ## native↔VM crossing spans (`beginCrossing`/`endCrossing`,
      ## Mixed-Trace-Debugging.md §3).  Span ids are 1-based (matching the
      ## process-span convention `process_ord + 1`), so a 0 here means "no id
      ## issued yet" and the first crossing gets id 1.  The externally-supplied
      ## `registerSpan` path (web-request / process spans) carries its OWN
      ## span_id and does NOT draw from this counter; a writer must not mix the
      ## two id spaces.
    pendingCrossings: seq[PendingCrossing]
      ## Open crossings awaiting their `endCrossing`, as a flat LIFO stack: a
      ## crossing is a call frame, so the innermost open crossing is always the
      ## one that closes next (`beginCrossing` pushes, `endCrossing` pops the
      ## top).  A stack rather than a `Table` keyed by span_id removes the
      ## per-crossing hash lookup and — more importantly — is a contiguous,
      ## memory-readable structure, the shape MCR reads from recreated memory on
      ## the replay path (nested-trace-correlation.md §1.2).  Empty for every
      ## writer that never opens a crossing.
    hasSpans: bool
      ## True once at least one span has been registered, which is also when
      ## `spans.dat` / `spans.idx` were added to the container.
      ##
      ## The span stream is the ONLY stream this writer does not always emit.
      ## `meta.dat` bit 13 is a REJECTING flag for readers that predate it
      ## (`KnownFlags`), so stamping it unconditionally would make every
      ## container this writer produces unreadable by older readers, for a
      ## feature almost no recording uses.  Gating on actual span registration
      ## keeps a span-free container byte-for-byte identical to what this
      ## writer produced before RS-M1 — same files, flag word unchanged — which
      ## is the same back-compat contract `source_views.dat` (bit 5) follows.
    interning: TraceInterningTables
      ## The writer's OWN interning tables.  Used only when
      ## ``sharedInterning`` is nil (the classic owned/standalone path, and an
      ## attach that was NOT handed an owner's tables).
    sharedInterning: ptr TraceInterningTables
      ## IC-M2 — non-nil when this writer interns THROUGH tables another
      ## producer created and owns (the Stage C shared writer: MCR owns the four
      ## tables, this attached materialized writer binds to them).  When set,
      ## ``initMultiStreamWriterAttached`` did NOT create its own tables, so only
      ## ONE ``addFile("paths.dat")`` per kind ever happens across both producers
      ## and the single ``nextId``/``lookup`` per kind is shared — ids are unique
      ## across producers by construction.  Resolved via the ``interningPtr``
      ## accessor (never cached as ``addr w.interning``) so the writer stays
      ## movable, mirroring the ``container`` accessor.
    qualifier*: string
      ## IC-M2 — the fully-qualified-key origin namespace this producer stamps
      ## on every interned string (spec ``Interning-Table-Coexistence.md`` §2):
      ## "" for a standalone trace (byte-identical to the pre-qualifier format),
      ## the VM language (e.g. "gdscript") for a materialized writer sharing a
      ## container with MCR.  Empty by default, so every existing caller keeps
      ## producing bare payloads.
    metadata*: TraceMetadata
    paths*: seq[string]
    pathLineLengths: seq[seq[uint32]]
      ## P6 follow-up — per-path line-length tables, used in column-aware
      ## mode to compute byte-offset-based ``global_position_index`` values
      ## that match the reader's ``decodeGlobalPositionIndex`` expectation
      ## per spec §"Source Location Addressing".  Parallel to ``paths``;
      ## empty seq for files whose line_lengths the caller didn't
      ## supply.  Ignored when ``columnAwareSteps`` is false.
    pathLineCounts: seq[uint64]
      ## Per-path line counts, used in line-only mode to size each file's
      ## slot in the global position space at exactly the number of lines
      ## the file has.  Parallel to ``paths``.  Populated only when
      ## ``lineCountTable`` is on, in which case ``registerPath``
      ## requires a count for every path and no entry is ever zero.
    currentPathVersions: Table[string, uint64]
      ## GDH-M1 / design §6.1 — per path STRING, the id of the most
      ## recently registered version of that file.
      ##
      ## Populated only by ``registerPathVersion``; a writer that never
      ## registers a second version leaves it empty and every lookup
      ## through it falls back to ``registerPath``, which is why an
      ## existing recorder's output is unchanged byte-for-byte.
      ##
      ## It exists so the STRING-taking step path stays version-unaware:
      ## the Godot fork's hot path is
      ## ``trace_writer_register_step(handle, path_string, line)``, and
      ## after a reload that string must resolve to the NEWEST index
      ## without the caller knowing a reload happened. Only the reload
      ## path is version-aware.
      ##
      ## Keyed on the bare path string rather than on the interning
      ## payload: the qualifier is a per-writer producer namespace
      ## (§6.2), constant for the lifetime of this writer, so within one
      ## writer the two keys are in bijection — and the callers that ask
      ## this question hold a path, not a payload.

    # Global line index (rebuilt when paths change)
    gli: GlobalLineIndex
    gliDirty: bool

    # Optional linehits builder
    linehitsBuilder: Option[LinehitsBuilder]

    # M26b — prepopulated breakpoint index (`step-map.ns`).  Accumulates
    # `(path_id, line) -> [step_id]` during recording and is serialised into
    # the CTFS container at close() so production `.ct` bundles carry the
    # spec's `STMP` namespace.  The db-backend's M26 consumer attaches it and
    # answers breakpoint line->step resolution with an O(unique-lines) index
    # lookup, WITHOUT materialising the whole step table.
    #
    # Enabled BY DEFAULT for line-only traces (the production write path every
    # non-column-aware recorder drives).  Gated OFF for column-aware traces:
    # there the exec stream's `global_position_index` is a byte offset, not a
    # `(path_id << 32) | line` packing, so the reader decodes step locations via
    # the column-aware path rather than `unpack_global_line_index`, and a
    # gli-derived `step-map.ns` would not agree with that derivation.  See
    # `enableColumnAwareSteps` (which clears this) and `step_map_builder.nim`.
    stepMapBuilder: StepMapBuilder
    emitStepMap: bool

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

    # M-capability-flags — column-aware capability bits.  Recorders
    # that not only emit column data but also support the GUI's
    # per-column affordances (breakpoints, motions) flip these so the
    # GUI can show/hide UI accordingly.  Both bits MUST imply
    # ``columnAwareSteps``; ``close()`` enforces this when serialising
    # the meta.dat header.  See spec § "Column-Aware Capability
    # Flags".
    supportsColumnBreakpoints*: bool
      ## True iff this writer's recorder guarantees columns sharp
      ## enough for per-column breakpoint placement.
    supportsColumnMotions*: bool
      ## True iff this writer's recorder guarantees the step predicate
      ## fires per-statement so the GUI can offer per-column motions
      ## (step-over / step-in / step-out at sub-statement granularity).

    lineCountTable*: bool
      ## True iff this writer records a per-file line count in every
      ## ``paths.dat`` record and sizes each file's slot in the global
      ## position space from it.  Sets ``FlagHasLineCountTable`` (bit 14)
      ## on ``meta.dat`` at ``close()``.
      ##
      ## Defaults to false, so a writer that does not opt in produces a
      ## container byte-identical to what it produced before the table
      ## existed: bare ``paths.dat`` records, no bit 14, and every file
      ## sized ``DefaultLinesPerFile`` by convention.  The default is not
      ## a preference — bit 14 is rejecting at every reader that predates
      ## it (``meta_dat.KnownFlags``), so reader support has to ship
      ## everywhere before a recorder flips this.
      ##
      ## Mutually exclusive with ``columnAwareSteps``; see
      ## ``enableLineCountTable``.

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

proc container(w: var MultiStreamTraceWriter): var Ctfs =
  ## The CTFS container this writer writes into — its own inline `ownedCtfs`
  ## when owned, or the externally-owned container when attached.
  ##
  ## Resolved fresh on every call (never cached as `addr w.ownedCtfs` in the
  ## object) so a by-value move of the writer can never leave a dangling self
  ## pointer.  In owned mode this returns the very same field the writer used
  ## before Stage C, so the owned write path is byte-for-byte unchanged.
  if w.attached:
    return w.sharedCtfs[]
  else:
    return w.ownedCtfs

proc interningPtr(w: var MultiStreamTraceWriter): ptr TraceInterningTables =
  ## The interning tables this writer interns into — the externally-owned tables
  ## when bound to a shared owner (IC-M2), else its own inline ``interning``.
  ##
  ## Returned as a raw ``ptr`` (not ``var``) and dereferenced at the call site so
  ## the borrow of ``w`` ends here: a by-value move of the writer can never leave
  ## a dangling ``addr w.interning``, exactly as ``container`` guards ``ownedCtfs``.
  if w.sharedInterning != nil:
    w.sharedInterning
  else:
    addr w.interning

proc rebuildGli(w: var MultiStreamTraceWriter) =
  ## Rebuild the global line index from the current set of paths.
  ##
  ## Each file's slot is sized to the number of positions the file has,
  ## which is what the trace's addressing mode makes a position:
  ##
  ## * column-aware — the file's total byte capacity (sum of its per-line
  ##   lengths), so the resulting ``global_position_index`` matches the
  ##   spec's byte-offset-based addressing;
  ## * line-only with the line-count table — the file's line count, which
  ##   ``registerPath`` required and ``paths.dat`` records;
  ## * line-only without it — the ``DefaultLinesPerFile`` convention,
  ##   preserving byte-for-byte output of pre-table traces.
  ##
  ## The sizing rule lives in ``global_line_index.positionSpaceCounts``
  ## because the reader has to apply the same one — a file sized
  ## differently there shifts the base of every file after it. Under the
  ## line-count table the reader recovers the sizes from the container;
  ## without it, neither the sizes nor the rule are recorded.
  w.gli = buildGlobalLineIndex(
    positionSpaceCounts(w.pathLineLengths, w.pathLineCounts,
      w.paths.len, w.columnAwareSteps))
  w.gliDirty = false

proc toGlobalLineIndex(w: var MultiStreamTraceWriter,
    pathId: uint64, line: uint64): uint64 =
  ## In column-aware mode, returns the byte-offset-based
  ## ``global_position_index`` of column 1 on ``line`` (the spec's
  ## reset-on-line-change semantic: after a ``register_step`` the cursor
  ## column is 1, and subsequent ``DeltaColumn`` events advance it
  ## within the line).
  ##
  ## In line-only mode, returns ``file_base + (line - 1)`` — the same
  ## 0-based in-file offset, with one address per line instead of one per
  ## (line, column) position, so both modes put line 1 at the file's own
  ## base. See ``global_line_index.globalIndex``.
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

proc checkLineWithinFile(w: var MultiStreamTraceWriter,
    pathId: uint64, line: uint64): Result[void, string] =
  ## Refuse a step whose line is past the end of the file's recorded slot.
  ##
  ## With a file sized to its real line count, a step one line past the
  ## end addresses the FIRST line of the next file. That address is
  ## inside the trace's space, so nothing downstream can refuse it:
  ## ``tryResolve`` has no bound to test it against, and the reader
  ## answers a ``(file, line)`` pair that the recorder never registered.
  ## The only party that knows the file's size is the writer, which was
  ## given the count, so the check is here.
  ##
  ## It applies exactly where a size is recorded. Without the line-count
  ## table there is no per-file bound in the container to enforce, and
  ## sizes come from the ``DefaultLinesPerFile`` convention — the check
  ## would be enforcing a number the trace does not carry.
  if not w.lineCountTable:
    return ok()
  if pathId >= uint64(w.pathLineCounts.len):
    return err("step at path id " & $pathId & " line " & $line &
      ": no path with that id has been registered (" &
      $w.pathLineCounts.len & " registered)")
  let count = w.pathLineCounts[int(pathId)]
  if line > count:
    return err("step at line " & $line & " of " & w.paths[int(pathId)] &
      ", which this trace records as having " & $count & " line(s). The " &
      "line is past the end of the file's slot in the global position " &
      "space, so its address would be inside the NEXT file's range and " &
      "read back as a location that was never recorded. Register the " &
      "path with the file's real line count")
  ok()

# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

proc initMultiStreamWriter*(path: string, program: string,
    chunkSize: int = 4096,
    recordingId: string = ""): Result[MultiStreamTraceWriter, string] =
  ## Create a new multi-stream trace writer.
  ##
  ## A writer with a ``path`` builds its container on
  ## ``createCtfsStreaming(path)``: the exec/value/call/io/span stream chunks
  ## flush to ``path`` on disk as they are written, so a producer does not have
  ## to hold the whole trace in RAM until close, and the container is
  ## durable/observable mid-run.  ``closeCtfs`` finalizes the on-disk image.
  ## Streaming is not a mode a file-backed producer may opt out of: buffering
  ## the trace and dumping it at close balloons RAM and loses everything if a
  ## long-running producer is killed first.
  ##
  ## An EMPTY ``path`` selects the in-memory container instead.  That is not
  ## the retired opt-out — it is the case where there is nothing to stream to:
  ## the freestanding WebAssembly writer has no filesystem, so its host reads
  ## the finished image out of ``toBytes`` (see ``trace_writer_begin_in_memory``
  ## on the C ABI).  ``toBytes`` returns the same bytes either way; a streamed
  ## container is byte-identical to a buffered one.
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
  if path.len == 0:
    w.ownedCtfs = createCtfs()
  else:
    let sres = createCtfsStreaming(path)
    if sres.isErr:
      return err("failed to create streaming CTFS container at " & path & ": " & sres.error)
    w.ownedCtfs = sres.get()
  w.metadata = TraceMetadata(
    recordingId: resolvedId, program: program, args: @[], workdir: "")
  w.paths = @[]
  w.gliDirty = true
  w.filePath = path
  # M26b — emit the prepopulated `step-map.ns` breakpoint index by default on
  # the production (line-only) write path.  `enableColumnAwareSteps` clears
  # this because the column-aware gli is a byte offset the reader decodes
  # differently (see the field docs / step_map_builder.nim).
  w.emitStepMap = true
  w.stepMapBuilder = initStepMapBuilder()

  # Meta.dat placeholder - will be written at close time
  # Init interning tables
  let intRes = initTraceInterningTables(w.container)
  if intRes.isErr:
    return err("failed to init interning tables: " & intRes.error)
  w.interning = intRes.get()

  # Init stream writers
  let execRes = initExecStreamWriter(w.container, chunkSize)
  if execRes.isErr:
    return err("failed to init exec stream: " & execRes.error)
  w.execWriter = execRes.get()

  let valRes = initValueStreamWriter(w.container)
  if valRes.isErr:
    return err("failed to init value stream: " & valRes.error)
  w.valueWriter = valRes.get()

  let callRes = initCallStreamWriter(w.container)
  if callRes.isErr:
    return err("failed to init call stream: " & callRes.error)
  w.callWriter = callRes.get()

  let ioRes = initIOEventStreamWriter(w.container)
  if ioRes.isErr:
    return err("failed to init io event stream: " & ioRes.error)
  w.ioEventWriter = ioRes.get()

  ok(w)

proc initMultiStreamWriterAttached*(ctfs: ptr Ctfs, program: string,
    chunkSize: int = 4096,
    recordingId: string = "",
    sharedInterning: ptr TraceInterningTables = nil,
    qualifier: string = ""): Result[MultiStreamTraceWriter, string] =
  ## Create a multi-stream trace writer that ATTACHES to a container another
  ## writer already created and OWNS (Stage C step 1).
  ##
  ## Unlike ``initMultiStreamWriter``, this does NOT call
  ## ``createCtfsStreaming`` — it writes its exec/value/call/io/span streams
  ## and interning tables into the caller-provided ``ctfs`` exactly as the
  ## owned path writes into its own container.  Two producers can therefore
  ## share ONE ``.ct`` as long as they emit DISTINCT stream names; only the
  ## container's creator owns ``meta.dat`` and the container's lifetime.
  ##
  ## Ownership contract this constructor sets up (enforced in ``close``):
  ##  * ``close`` FLUSHES and finalizes this writer's OWN stream indices
  ##    (calls.idx, values.idx, events.idx, steps.idx, spantype.ns …) so its
  ##    streams are complete and seekable, but
  ##  * it does NOT write ``meta.dat`` (the owner does), and
  ##  * it does NOT ``closeCtfs`` / free the shared container (the owner does).
  ##
  ## ``ctfs`` MUST outlive the returned writer (the writer stores the raw
  ## pointer and never copies the container).  Passing ``nil`` is rejected.
  ##
  ## ``recordingId`` behaves as in ``initMultiStreamWriter`` — it feeds this
  ## writer's ``metadata`` for callers that read it back, even though the
  ## attached writer never serialises ``meta.dat`` itself.
  if ctfs == nil:
    return err("initMultiStreamWriterAttached: ctfs pointer is nil")

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
  w.attached = true
  w.sharedCtfs = ctfs
  w.qualifier = qualifier
  w.metadata = TraceMetadata(
    recordingId: resolvedId, program: program, args: @[], workdir: "")
  w.paths = @[]
  w.gliDirty = true
  w.filePath = ""
  # Same M26b default as the owned path — line-only writers emit step-map.ns.
  # It is written into the SHARED container at close(); it is one of this
  # writer's own additive files, not meta.dat, so it is allowed on the attach
  # path.  A column-aware attached writer clears it via enableColumnAwareSteps.
  w.emitStepMap = true
  w.stepMapBuilder = initStepMapBuilder()

  # IC-M2 — interning ownership.  When the caller hands us the owner's tables
  # (the Stage C shared writer: MCR created + owns them over the SAME container),
  # BIND to them rather than creating our own: this is what prevents the second
  # `addFile("paths.dat")` the M61 dup-name guard would reject, and it makes the
  # single per-kind `nextId`/`lookup` shared so ids are unique across producers.
  # Absent (a plain attach, e.g. an attach test or a materialized writer not
  # sharing with MCR), create our own tables exactly as before — byte-identical.
  if sharedInterning != nil:
    w.sharedInterning = sharedInterning
  else:
    let intRes = initTraceInterningTables(w.container)
    if intRes.isErr:
      return err("failed to init interning tables: " & intRes.error)
    w.interning = intRes.get()

  let execRes = initExecStreamWriter(w.container, chunkSize)
  if execRes.isErr:
    return err("failed to init exec stream: " & execRes.error)
  w.execWriter = execRes.get()

  let valRes = initValueStreamWriter(w.container)
  if valRes.isErr:
    return err("failed to init value stream: " & valRes.error)
  w.valueWriter = valRes.get()

  let callRes = initCallStreamWriter(w.container)
  if callRes.isErr:
    return err("failed to init call stream: " & callRes.error)
  w.callWriter = callRes.get()

  let ioRes = initIOEventStreamWriter(w.container)
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
  # M26b — the column-aware exec stream stores a byte-offset
  # `global_position_index`, which the db-backend decodes via the
  # column-aware path rather than `unpack_global_line_index`.  A
  # gli-derived `step-map.ns` would not match that decode, so suppress
  # emission for column-aware traces (line-only traces keep it on).
  w.emitStepMap = false

proc enableColumnBreakpointsSupport*(w: var MultiStreamTraceWriter) =
  ## Capability opt-in: declare that this trace's recorder emits
  ## columns precise enough for the GUI to set per-column breakpoints.
  ## Sets ``FlagSupportsColumnBreakpoints`` (bit 6) on the meta.dat
  ## header at ``close()`` time.
  ##
  ## Capability bits are meaningless without column data on the wire,
  ## so this proc auto-enables ``columnAwareSteps`` if it isn't already
  ## on — this mirrors the spec contract that capability flags
  ## presuppose ``FlagHasColumnAwareSteps``.  Call before any step is
  ## emitted; the underlying ``columnAwareSteps`` flip is trace-global.
  ##
  ## See ``codetracer-trace-format-spec/internal-files.md`` §
  ## "Column-Aware Capability Flags".
  w.columnAwareSteps = true
  w.supportsColumnBreakpoints = true
  w.emitStepMap = false  # M26b — see enableColumnAwareSteps.

proc enableColumnMotionsSupport*(w: var MultiStreamTraceWriter) =
  ## Capability opt-in: declare that this trace's recorder supports
  ## per-column step-over / step-in / step-out.  Sets
  ## ``FlagSupportsColumnMotions`` (bit 7) on the meta.dat header at
  ## ``close()`` time and — like ``enableColumnBreakpointsSupport`` —
  ## auto-enables ``columnAwareSteps`` because capability bits without
  ## wire-format column data is undefined behaviour per spec.
  w.columnAwareSteps = true
  w.supportsColumnMotions = true
  w.emitStepMap = false  # M26b — see enableColumnAwareSteps.

proc enableLineCountTable*(w: var MultiStreamTraceWriter): Result[void, string] =
  ## Opt this writer into recording a per-file line count.
  ##
  ## After this call every ``registerPath`` REQUIRES the file's line
  ## count, each file's slot in the global position space is sized to
  ## that count, ``registerStep`` refuses a line past a file's count, and
  ## ``close()`` sets ``FlagHasLineCountTable`` (bit 14) on ``meta.dat``
  ## so a reader lays the space out from the recorded sizes instead of
  ## assuming ``DefaultLinesPerFile`` for every file.
  ##
  ## The count is required rather than optional because an omitted one
  ## puts the reader back to assuming a size for that file — the defect
  ## the table removes, per file instead of per trace. A recorder that
  ## cannot count a file's lines passes ``DefaultLinesPerFile``, which
  ## records the ceiling it used rather than leaving it to be inferred.
  ##
  ## Must be called before the first ``registerPath``: the table covers
  ## every record in ``paths.dat``, and a path already interned under the
  ## bare layout cannot grow a count.
  ##
  ## Refused on a column-aware writer. A Layout A record already carries
  ## the file's ``line_count`` as the length of its per-line table, and
  ## that mode sizes a file in addressable columns rather than lines, so
  ## the two tables are not additive — they are two spellings of the same
  ## field under two different units.
  if w.columnAwareSteps:
    return err("enableLineCountTable: this writer is column-aware, whose " &
      "paths.dat records already carry the file's line_count and whose " &
      "files are sized in addressable columns rather than lines")
  if w.paths.len > 0:
    return err("enableLineCountTable: " & $w.paths.len & " path(s) are " &
      "already interned under the bare paths.dat layout and cannot grow " &
      "a line count; enable the table before the first registerPath")
  w.lineCountTable = true
  ok()

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
    lineLengths: openArray[uint32] = [],
    lineCount: uint64 = 0): Result[uint64, string] =
  ## Register a source path and return its interned ID.
  ##
  ## ``lineCount`` is the number of lines the file has, and it sizes the
  ## file's slot in the line-only global position space.  It is REQUIRED
  ## on a writer that called ``enableLineCountTable`` and ignored on one
  ## that did not — a bare ``paths.dat`` record has nowhere to put it,
  ## and a column-aware record derives the same field from
  ## ``lineLengths``.  A recorder that cannot count a file's lines passes
  ## ``DefaultLinesPerFile`` so that the size the space was laid out with
  ## is the size the container states.
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
      # Column-aware paths key their dedup on the bare path (the on-disk record
      # is the self-describing Layout A form); IC-M2's qualifier applies to the
      # line-only producers (MCR, the GDScript VM), so column-aware attach keeps
      # the bare record.
      w.container.ensurePathIdColumnAware(w.interningPtr[], path, lineLengths)
    elif w.lineCountTable:
      w.container.ensureQualifiedPathIdWithLineCount(
        w.interningPtr[], w.qualifier, path, lineCount)
    else:
      w.container.ensureQualifiedPathId(w.interningPtr[], w.qualifier, path)
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
    # Mirror the line count so ``rebuildGli`` sizes the file's slot to it.
    # Zero outside the line-count table: no count was recorded, and
    # ``positionSpaceCounts`` reads that as the pre-table convention.
    w.pathLineCounts.add(if w.lineCountTable: lineCount else: 0'u64)
    w.gliDirty = true
  ok(id)

# ---------------------------------------------------------------------------
# Versioned paths (GDH-M1 — design §6.1 of
# ``codetracer-specs/Planned-Features/GDScript-Hot-Reload-Multi-Version-Sources.md``)
#
# The fault-injection arms below are what the campaign's falsifier clauses
# name, compiled in ONLY when BOTH ``-d:gdh1FalsifierArms`` (the master
# switch) and the arm's own define are given. Two defines rather than one so
# that no single stray ``-d:`` can arm a mutation in a shipped build, and so
# that a build carrying one is self-evident: the master switch emits a
# compile-time warning. ``tests/test_gdh1_path_versions.nim`` asserts its own
# inertness — a green run refuses to report PASS unless every arm is off.
# ---------------------------------------------------------------------------

when defined(gdh1FalsifierArms):
  {.warning: "gdh1FalsifierArms: versioned-path fault injection is COMPILED IN. This build must never be shipped or measured as a green result.".}

template gdh1Arm*(name: untyped): bool =
  ## True iff the named falsifier arm is armed — the master switch AND the
  ## arm's own define. Always a compile-time constant, so an unarmed build
  ## contains none of the mutated code.
  when defined(gdh1FalsifierArms): defined(name) else: false

proc activeGdh1FalsifierArm*(): string =
  ## The name of the armed falsifier mutation, or "" when none is.
  ## Used by the gate to prove its own green run measured the real writer.
  if gdh1Arm(gdh1FalsifyDedup): "gdh1FalsifyDedup"
  elif gdh1Arm(gdh1FalsifyDefaultSlot): "gdh1FalsifyDefaultSlot"
  elif gdh1Arm(gdh1FalsifyNoMirror): "gdh1FalsifyNoMirror"
  elif gdh1Arm(gdh1FalsifyMangle): "gdh1FalsifyMangle"
  elif gdh1Arm(gdh1FalsifyOverwrite): "gdh1FalsifyOverwrite"
  elif gdh1Arm(gdh1FalsifyPrepend): "gdh1FalsifyPrepend"
  elif gdh1Arm(gdh1FalsifyNoBoundsCheck): "gdh1FalsifyNoBoundsCheck"
  elif gdh1Arm(gdh1FalsifyGlobalCurrent): "gdh1FalsifyGlobalCurrent"
  elif gdh1Arm(gdh1FalsifyVersionWithoutTable): "gdh1FalsifyVersionWithoutTable"
  else: ""

template gdh1VersionKey(path: string): string =
  ## The key ``currentPathVersions`` is indexed by.
  ##
  ## FALSIFIER (``gdh1FalsifyGlobalCurrent``, GDH-G1): collapse it to a
  ## single global "most recently registered version" instead of one entry
  ## per path string. That is the cheapest way to write the map, it makes
  ## every assertion about the RELOADED file pass, and it silently
  ## re-points every OTHER file's steps at the reloaded file's newest id.
  ## The gate's `pathIdForStep` on an unrelated path is what catches it.
  when gdh1Arm(gdh1FalsifyGlobalCurrent): ""
  else: path

proc registerPathVersion*(w: var MultiStreamTraceWriter,
    path: string, lineCount: uint64): Result[uint64, string] =
  ## Register a NEW VERSION of an already-registered path, and return the
  ## new version's path id. Design §6.1.
  ##
  ## Unlike ``registerPath`` this **bypasses the interning lookup and
  ## always appends** a ``paths.dat`` record. The payload is
  ## byte-identical to the earlier version's — the virtual path string is
  ## the same file, and only the INDEX discriminates the version. Nothing
  ## is appended to, prefixed to, or interposed into the string; a
  ## consumer that resolves a user-supplied ``res://foo.gd`` must keep
  ## resolving it after a reload.
  ##
  ## The new version gets its OWN slot in the global position space,
  ## sized to its own line count, appended after every existing file. That
  ## is the arithmetic the feature rests on: v1's slot keeps its base and
  ## its size, so every address already emitted against v1 still decodes
  ## to v1, while v2's lines — which may lie past the end of v1 — get
  ## addresses of their own instead of spilling into the next file.
  ##
  ## Requires the line-count table. Without it a ``paths.dat`` record has
  ## nowhere to put a size, both versions would be laid out at the
  ## ``DefaultLinesPerFile`` stride, and neither the writer's
  ## ``checkLineWithinFile`` nor any reader could bound a version against
  ## its own count — the mis-attribution would be silent, which is the
  ## defect this milestone exists to remove rather than to inherit.
  ##
  ## Refused on a column-aware writer for the same reason
  ## ``enableLineCountTable`` is: a Layout A record sizes its file in
  ## addressable columns and carries its own count, so versioning there
  ## is a different (and unbuilt) arithmetic.
  if w.closed:
    return err("writer is closed")
  if w.columnAwareSteps:
    return err("registerPathVersion: this writer is column-aware, whose " &
      "paths.dat records size a file in addressable columns; versioned " &
      "paths are defined for the line-only line-count-table layout")
  # FALSIFIER (``gdh1FalsifyVersionWithoutTable``, 4th gate): drop the
  # requirement that the writer states file sizes. A versioned record then
  # has nowhere to put its count, both versions are laid out at the
  # ``DefaultLinesPerFile`` stride, and no bound can be enforced against
  # either — GDH-M0's silent mis-attribution mode, re-created one layer up.
  if not w.lineCountTable and not gdh1Arm(gdh1FalsifyVersionWithoutTable):
    return err("registerPathVersion: this writer has no line-count table, " &
      "so a second record for " & path & " would carry no size and both " &
      "versions would be laid out at the DefaultLinesPerFile stride. " &
      "Neither the writer nor a reader could then bound a version " &
      "against its own line count and the mis-attribution would be " &
      "silent. Call enableLineCountTable before the first registerPath")

  let registerName =
    when gdh1Arm(gdh1FalsifyMangle):
      # FALSIFIER (GDH-G2): disambiguate by mangling the string. This is
      # the cheapest way to make "two entries" pass while breaking every
      # consumer that resolves a path by name.
      path & "#" & $(w.paths.len + 1)
    else:
      path
  let recordedCount =
    when gdh1Arm(gdh1FalsifyDefaultSlot):
      # FALSIFIER (GDH-G1, arm 2): let the new version's slot fall back to
      # the ceiling instead of stating the file's real size.
      DefaultLinesPerFile
    else:
      lineCount

  when gdh1Arm(gdh1FalsifyOverwrite):
    # FALSIFIER (GDH-G4, arm 1): resize the EXISTING slot in place instead
    # of appending a new one — the "just fix the size" cheat. Every file
    # after the resized one then has its base moved under addresses that
    # were already emitted.
    let prevId = w.currentPathVersions.getOrDefault(path, high(uint64))
    let existing =
      if prevId != high(uint64): prevId
      else:
        var found = high(uint64)
        for i in 0 ..< w.paths.len:
          if w.paths[i] == path:
            found = uint64(i)
        found
    if existing != high(uint64):
      w.pathLineCounts[int(existing)] = lineCount
      w.gliDirty = true
      w.currentPathVersions[path] = existing
      return ok(existing)

  let idRes =
    when gdh1Arm(gdh1FalsifyDedup):
      # FALSIFIER (GDH-G1, arm 1): restore dedup — route the versioned
      # registration back through the interning lookup.
      w.container.ensureQualifiedPathIdWithLineCount(
        w.interningPtr[], w.qualifier, registerName, recordedCount)
    else:
      w.container.appendQualifiedPathWithLineCount(
        w.interningPtr[], w.qualifier, registerName, recordedCount)
  if idRes.isErr:
    return err(idRes.error)
  let id = idRes.get()

  if id == uint64(w.paths.len):
    when gdh1Arm(gdh1FalsifyPrepend):
      # FALSIFIER (GDH-G4, arm 2): put the new version at index 0 instead
      # of appending. v1's base moves, so every address emitted against
      # v1 after this point lands in a different file.
      w.paths.insert(registerName, 0)
      w.pathLineLengths.insert(@[], 0)
      w.pathLineCounts.insert(recordedCount, 0)
    else:
      w.paths.add(registerName)
      w.pathLineLengths.add(@[])
      when gdh1Arm(gdh1FalsifyNoMirror):
        # FALSIFIER (GDH-G1, arm 2, literal form): append the record but
        # NOT the writer's own count, so the slot silently falls back to
        # DefaultLinesPerFile in the space the steps are encoded in.
        discard
      else:
        w.pathLineCounts.add(recordedCount)
    w.gliDirty = true

  w.currentPathVersions[gdh1VersionKey(path)] = id
  ok(id)

proc currentPathId*(w: MultiStreamTraceWriter,
    path: string): Option[uint64] =
  ## The id a bare ``registerStep(path, …)`` resolves ``path`` to today —
  ## the id of its newest registered version — or ``none`` when no
  ## version of this path has been registered through
  ## ``registerPathVersion``.
  ##
  ## ``none`` is not "unknown": it is "this path has at most one entry,
  ## so the interning lookup is the answer". Keeping the two apart is
  ## what lets the FFI mirror be DELETED rather than kept in sync — a
  ## caller asking this question gets the writer's own state or an
  ## explicit "ask the interning table", never a re-derivation.
  let found = w.currentPathVersions.getOrDefault(gdh1VersionKey(path), high(uint64))
  if found == high(uint64):
    none(uint64)
  else:
    some(found)

proc pathIdForStep*(w: var MultiStreamTraceWriter,
    path: string): Result[uint64, string] =
  ## Resolve a path STRING to the id a step recorded now belongs to.
  ##
  ## This is the string-taking step path's entry point (design §6.1): the
  ## newest version when the file has been reloaded, and otherwise
  ## exactly what ``registerPath(path)`` has always returned — same call,
  ## same interning, same bytes — so a recorder that never registers a
  ## version is unaffected.
  let current = w.currentPathId(path)
  if current.isSome:
    return ok(current.get())
  w.registerPath(path)

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
  w.container.ensureQualifiedFunctionId(w.interningPtr[], w.qualifier, name)

proc registerType*(w: var MultiStreamTraceWriter,
    name: string): Result[uint64, string] =
  ## Register a type name and return its interned ID.
  w.container.ensureQualifiedTypeId(w.interningPtr[], w.qualifier, name)

proc registerVarname*(w: var MultiStreamTraceWriter,
    name: string): Result[uint64, string] =
  ## Register a variable name and return its interned ID.
  w.container.ensureQualifiedVarnameId(w.interningPtr[], w.qualifier, name)

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
  when gdh1Arm(gdh1FalsifyNoBoundsCheck):
    # FALSIFIER (GDH-M1's fourth gate): drop the per-file bound. The step
    # is then accepted, encodes into the NEXT file's range, and reads back
    # as a location that was never recorded.
    discard
  else:
    ? w.checkLineWithinFile(pathId, line)

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

  let evRes = w.container.writeEvent(w.execWriter, ev)
  if evRes.isErr:
    return err("failed to write step event: " & evRes.error)

  # Write values parallel to this step
  let valRes = w.container.writeStepValues(w.valueWriter, values)
  if valRes.isErr:
    return err("failed to write step values: " & valRes.error)

  # Record linehit if enabled
  if w.linehitsBuilder.isSome:
    w.linehitsBuilder.get().recordHit(gli, w.stepCount)

  # M26b — record into the prepopulated breakpoint index, keyed by the
  # coordinates registered here.  A breakpoint request arrives as
  # `(path_id, line)`; the packed `gli` is a writer convention the container
  # does not record, so it is not a key (see step_map_builder's header).
  if w.emitStepMap:
    w.stepMapBuilder.recordStep(pathId, line, w.stepCount)

  w.lastGlobalLineIndex = gli
  w.lastPathId = pathId
  w.lastLine = line
  w.stepCount += 1
  ok()

proc registerStepWithColumn*(w: var MultiStreamTraceWriter,
    pathId: uint64,
    line: uint64,
    columnDelta: int64,
    values: openArray[VariableValue]): Result[void, string] =
  ## Register a step at (pathId, line, column) as a SINGLE wire event.
  ##
  ## ``columnDelta`` is the column offset *from column 1* on the
  ## requested line — the value an FFI caller would otherwise pass to
  ## a subsequent ``registerColumnStep``.  Folding both into one event
  ## means line-granular step-over readers don't see an intermediate
  ## column-1 step boundary that has no variable values attached, so
  ## ``variables_at(step_id)`` on the resulting step sees the values
  ## the caller attached to (pathId, line, column).
  ##
  ## FU-Writer-redux follow-up: before this proc existed, the FFI
  ## buffered a line step in ``hasPendingStep`` and (on
  ## ``register_delta_column``) flushed it as an empty
  ## ``sekAbsoluteStep`` before buffering a fresh column step.  The
  ## resulting (line step, column step) pair broke
  ## ``Replay::load_locals`` because line-granular step-over from the
  ## call_entry landed on the empty line step.  The combined event
  ## eliminates that intermediate.
  ##
  ## Only callable on writers that have opted into column-aware mode
  ## (``enableColumnAwareSteps``).  When ``columnDelta == 0`` the
  ## emitted event is bit-for-bit identical to ``registerStep`` —
  ## ``globalLineIndex`` is the same offset and the value record
  ## doesn't carry per-event column information either way.
  if w.closed:
    return err("writer is closed")
  if not w.columnAwareSteps and columnDelta != 0:
    return err("registerStepWithColumn(columnDelta != 0) called on a " &
      "writer that has not opted into column-aware mode " &
      "(call enableColumnAwareSteps first)")
  ? w.checkLineWithinFile(pathId, line)

  let baseGli = w.toGlobalLineIndex(pathId, line)
  let combinedGli = uint64(int64(baseGli) + columnDelta)

  var ev: StepEvent
  if w.stepCount == 0:
    # First step must be absolute.
    ev = StepEvent(kind: sekAbsoluteStep, globalLineIndex: combinedGli)
  else:
    let delta = int64(combinedGli) - int64(w.lastGlobalLineIndex)
    if delta >= -64 and delta <= 63:
      ev = StepEvent(kind: sekDeltaStep, lineDelta: delta)
    else:
      ev = StepEvent(kind: sekAbsoluteStep, globalLineIndex: combinedGli)

  let evRes = w.container.writeEvent(w.execWriter, ev)
  if evRes.isErr:
    return err("failed to write step event: " & evRes.error)

  let valRes = w.container.writeStepValues(w.valueWriter, values)
  if valRes.isErr:
    return err("failed to write step values: " & valRes.error)

  if w.linehitsBuilder.isSome:
    w.linehitsBuilder.get().recordHit(combinedGli, w.stepCount)

  # M26b — index into the breakpoint map at the registered coordinates.
  # `emitStepMap` is only on for line-only writers, where `columnDelta == 0`
  # and this step is the line's first position, so the column the request
  # cannot name is not lost by keying on the line alone.
  if w.emitStepMap:
    w.stepMapBuilder.recordStep(pathId, line, w.stepCount)

  w.lastGlobalLineIndex = combinedGli
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
  let evRes = w.container.writeEvent(w.execWriter, ev)
  if evRes.isErr:
    return err("failed to write delta-column event: " & evRes.error)

  let valRes = w.container.writeStepValues(w.valueWriter, values)
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
    let res = w.container.writeCall(w.callWriter, entry[1])
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

  # CTFS-M entry_step convention: `entryStep` is the index of the FIRST
  # step that belongs to the callee's body — i.e. the next step that the
  # writer will emit after this `registerCall`.  At the moment of this
  # call, `w.stepCount` is the count of already-flushed steps (the FFI
  # flushed any pending caller step before calling us), so it is exactly
  # the index of that next step.
  #
  # This is the "next-step" semantic.  It puts non-leaf callee bodies on
  # the correct call frame (the call's range `[entryStep, exitStep]`
  # covers the callee body's steps, NOT the caller's call-site step).
  #
  # Leaf calls (snforge `register_step → register_call → register_return`
  # with no callee body) need separate handling: at `registerReturn`
  # time we detect the empty range (`w.stepCount == entryStep`, meaning
  # no body step was emitted) and clamp `entryStep` to the just-flushed
  # caller step so the call_entry still surfaces in the trace.  See the
  # leaf-clamp branch in `registerReturn`.
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

  # Leaf-call entry-step clamp.  `registerCall` captured `entryStep` as
  # `stepCount` (the index of the next step to be emitted, i.e. the
  # first step of the callee's body).  If no body step was emitted
  # before this `registerReturn` (the snforge `register_step →
  # register_call → register_return` pattern), `entryStep` ends up
  # pointing past the last emitted step — the call_entry would land
  # outside the trace's `[0, stepCount)` walk range and downstream
  # readers (ct-print, the DAP trace_processor) would silently drop it
  # or attach it to the wrong frame.
  #
  # In that case the callee has no body of its own and the
  # semantically-meaningful step for the call IS the caller's
  # just-flushed call-site step (typically the snforge
  # `contract_call` / `storage_read` / etc. line that produced the
  # call's argument context).  Clamp `entryStep` to that step so the
  # call_entry surfaces at the right frame.
  #
  # For non-leaf callees `stepCount > pending.entryStep` and we keep
  # the original `entryStep` so the call's range covers the callee's
  # body steps without bleeding back into the caller.
  let entryStep =
    if w.stepCount > pending.entryStep:
      pending.entryStep
    elif w.stepCount > 0:
      w.stepCount - 1
    else:
      0'u64

  let rec = call_stream.CallRecord(
    functionId: pending.functionId,
    parentCallKey: pending.parentCallKey,
    entryStep: entryStep,
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

proc registerSpan*(w: var MultiStreamTraceWriter,
    span: SpanRecord): Result[void, string] =
  ## Append a span (RS-M1) — a bounded, labeled interval of execution: an HTTP
  ## request, a process, a test.  See `span_stream.nim` for the record model.
  ##
  ## The `spans.dat` / `spans.idx` pair is created on the FIRST call, not at
  ## writer init, so a recording that never registers a span produces a
  ## container byte-for-byte identical to the pre-RS-M1 output (see `hasSpans`).
  ##
  ## The stream is append-only: to publish an in-flight request, register a
  ## record with `isOpen = true`, then register the completion later with the
  ## SAME `spanId`.  Readers resolve the pair by last-record-wins; nothing is
  ## rewritten.
  if w.closed:
    return err("writer is closed")

  if not w.hasSpans:
    let initRes = initSpanStreamWriter(w.container)
    if initRes.isErr:
      return err("failed to init span stream: " & initRes.error)
    w.spanWriter = initRes.get()
    w.hasSpans = true

  let res = writeSpan(w.container, w.spanWriter, span)
  if res.isErr:
    return err("failed to write span: " & res.error)
  ok()

proc flushSpans*(w: var MultiStreamTraceWriter): Result[void, string] =
  ## Seal the current partial span chunk, without closing the writer: the
  ## buffered records are compressed, appended to `spans.dat` and published in
  ## `spans.idx`, so they are committed to the container rather than sitting in
  ## the writer's record buffer.  `close()` calls it anyway, so a batch
  ## recorder never needs to.  A no-op when no span has been registered.
  ##
  ## On an IN-MEMORY writer (an empty `path`) this makes nothing visible to a
  ## concurrent reader: there is no file, and the bytes exist only once the
  ## caller serialises them (`toBytes`, which the FFI does at
  ## `trace_writer_close`).  Sealing a chunk there changes what an
  ## already-materialised image would contain, not what is on disk.
  ##
  ## On a file-backed writer the container is `createCtfsStreaming(path)`, so
  ## `syncEntry` lands the chunk and its index entry on disk as they are
  ## sealed and a concurrent reader does see them.  `flushChunk` writes the
  ## chunk body before the index entry and syncs both, and
  ## `initSpanStreamReader` / `readSpansSince` read a growing container (see
  ## `span_stream_tail_during_active_write`, which drives exactly that path
  ## against a streaming container).
  if not w.hasSpans:
    return ok()
  span_stream.flush(w.container, w.spanWriter)

proc spanCount*(w: MultiStreamTraceWriter): uint64 =
  ## Number of span RECORDS registered (an open record and its completion count
  ## as two).
  if not w.hasSpans: 0'u64 else: span_stream.count(w.spanWriter)

# ---------------------------------------------------------------------------
# Native↔VM crossing spans (Mixed-Trace-Debugging.md §3)
# ---------------------------------------------------------------------------
#
# A host process enters and leaves an embedded VM many times; each crossing is
# recorded as an ordinary `SpanRecord` whose `[start_step, end_step]` bound the
# materialized steps executed inside the VM frame (§3: "the span IS a coordinate
# in the container — (process, thread, step range)").  These two helpers emit
# that span INCREMENTALLY into the live span stream at VM-frame boundaries,
# replacing the earlier plan of reopening a finalized container after the fact.
#
# The step space they index is THIS writer's own — the same counter
# `registerCall`/`registerReturn` capture for a call's `[entryStep, exitStep]`
# — so the crossing helper is writer-native and the span_id is minted here,
# where the step space is owned.  The FFI wrapper flushes any buffered step
# BEFORE `beginCrossing` (exactly as `trace_writer_register_call` does), so at
# snapshot time `w.stepCount` is the index of the first step of the frame's
# body, aligning a crossing's `start_step` with the enclosed call's `entryStep`.
#
# SCOPE: crossings index the materialized step space, so they are only
# meaningful on the multi-stream writer (the FFI rejects other backends).  In
# the COMBINED native+VM recording the attached VM writer is the SOLE
# span-stream owner — MCR does not also emit into the shared span stream (the
# M61 dup-name guard would fire), so these ids never collide with an MCR span.

proc beginCrossing*(w: var MultiStreamTraceWriter, spanType: string): uint64 =
  ## Open a native↔VM crossing span and return its minted `span_id` (the
  ## caller's handle to pass back to `endCrossing`).  `start_step` is snapshotted
  ## now from `w.stepCount` — the index of the next materialized step, i.e. the
  ## first step that will run inside the crossing.
  ##
  ## Streaming correctness (nested-trace-correlation.md §1.4): the crossing is
  ## written **open-at-begin, settled-at-close**.  This proc IMMEDIATELY appends
  ## an OPEN `SpanRecord` (`flags.open`, `end_step = 0`, `status` unknown) and
  ## flushes it, so a reader sees the in-flight frame BEFORE `endCrossing`;
  ## `endCrossing` later appends the settled record with the SAME `span_id`
  ## (last-record-wins).  The open record MUST carry `end_step = 0` /
  ## `end_wall_ns = 0` — the span decoder rejects an open record otherwise
  ## (`span_stream.nim` `decodeSpanRecord`).
  ##
  ## Returns 0 (never a valid 1-based span id) if the writer is closed or if
  ## appending/flushing the open record fails — the error is not raised.  The
  ## open crossing is pushed onto the LIFO stack only after the open record is
  ## committed, so a failed begin leaves no dangling pending entry.
  if w.closed:
    return 0'u64
  if w.nextSpanId == 0'u64:
    w.nextSpanId = 1'u64
  let spanId = w.nextSpanId
  w.nextSpanId += 1

  let openSpan = SpanRecord(
    spanId: spanId,
    parentSpanId: 0'u64,
    isOpen: true,
    isExternal: false,
    status: spanStatusUnknown,
    startWallNs: 0'u64,
    endWallNs: 0'u64,
    processOrd: 0'u64,
    threadId: 0'u64,
    startStep: w.stepCount,
    endStep: 0'u64,
    spanType: spanType,
    label: "",
    contiguousOnOneThread: true,
    sharesTimeline: true,
    concurrentWithSiblings: false,
  )
  # Seal-immediately, exactly as `endCrossing` does for the settled record, so
  # the open frame is visible mid-run.  On any error return the 0 sentinel
  # rather than raising — the FFI contract callers rely on.
  let regRes = w.registerSpan(openSpan)
  if regRes.isErr:
    return 0'u64
  let flushRes = w.flushSpans()
  if flushRes.isErr:
    return 0'u64

  w.pendingCrossings.add(PendingCrossing(
    spanId: spanId, spanType: spanType, startStep: w.stepCount))

  # MT7-5a: mirror the just-opened crossing into the exported, C-ABI, per-thread
  # crossing block so a replay-time reader can read the current crossing /
  # altitude from RECREATED memory in O(1) (nested-trace-correlation.md §1.2).
  # Pushed AFTER the open span record is committed and the heap seq is grown, so
  # a failed begin (returns 0 above) never pushes and the block's stack depth
  # equals `pendingCrossings.len` at all times.  Same (spanId, startStep) the
  # heap entry carries.
  ctCrossingPush(spanId, w.stepCount)
  spanId

proc endCrossing*(w: var MultiStreamTraceWriter,
    spanId: uint64): Result[void, string] =
  ## Settle the crossing opened as `spanId`: build its `SpanRecord` with
  ## `end_step = stepCount - 1` (the last materialized step inside the frame,
  ## clamped to 0 when nothing was recorded — the same clamp `registerReturn`
  ## uses for `exitStep`), append it via `registerSpan` with the SAME `span_id`
  ## as the open record `beginCrossing` wrote (last-record-wins settles the
  ## pair), and `flushSpans` so it is sealed into the container's span stream
  ## immediately (mid-run visibility per §3).
  ##
  ## Crossings close strictly LIFO — a crossing is a call frame, so `spanId`
  ## MUST be the innermost still-open crossing (the top of the stack).  Closing
  ## anything else, or closing when nothing is open, is an error, never a silent
  ## no-op: it catches genuine misuse (mismatched begin/end nesting).
  if w.closed:
    return err("writer is closed")
  if w.pendingCrossings.len == 0:
    return err("endCrossing: span_id " & $spanId &
      " is not the innermost open crossing (no open crossings)")
  let top = w.pendingCrossings[^1]
  if top.spanId != spanId:
    return err("endCrossing: span_id " & $spanId &
      " is not the innermost open crossing (" & $top.spanId & ")")
  w.pendingCrossings.setLen(w.pendingCrossings.len - 1)

  # MT7-5a: mirror the close into the exported crossing block (see beginCrossing).
  # Popped only after the LIFO validation passed and the heap seq was shortened,
  # so an out-of-order / no-op endCrossing (which returns err above) never pops,
  # keeping the block balanced with what the writer actually closed.
  ctCrossingPop()

  let span = SpanRecord(
    spanId: spanId,
    parentSpanId: 0'u64,
    isOpen: false,
    isExternal: false,
    status: spanStatusOk,
    startWallNs: 0'u64,
    endWallNs: 0'u64,
    processOrd: 0'u64,
    threadId: 0'u64,
    startStep: top.startStep,
    endStep: (if w.stepCount > 0'u64: w.stepCount - 1'u64 else: 0'u64),
    spanType: top.spanType,
    label: "",
    contiguousOnOneThread: true,
    sharesTimeline: true,
    concurrentWithSiblings: false,
  )
  ?w.registerSpan(span)
  ?w.flushSpans()
  ok()

proc registerIOEvent*(w: var MultiStreamTraceWriter, kind: IOEventKind,
    data: openArray[byte],
    metadata: openArray[byte] = [],
    stepId: Option[uint64] = none(uint64)): Result[void, string] =
  ## Register an IO event (stdout, stderr, etc.) at the current step.
  ##
  ## ``metadata`` is carried verbatim into the SPEC ``events.dat`` record's
  ## metadata field (``trace-events.md`` §"IO Event Stream Records"); it defaults
  ## to empty for callers that only have content bytes.
  ##
  ## ``stepId`` names the exec-stream step the event belongs to.  When it is
  ## ``none`` the event is attributed to the LAST step this writer emitted
  ## (``stepCount - 1``), which is correct only for callers that write their
  ## steps eagerly.  Callers that BUFFER a step before emitting it — the C FFI
  ## does, so that variable values registered after
  ## ``trace_writer_register_step`` still attach to that step
  ## (``flushPendingStep``) — must pass the id explicitly: at the moment a
  ## write is registered the step for the writing line has not been emitted
  ## yet, so ``stepCount - 1`` would name the PREVIOUS step and the debugger
  ## would render the output one line too high (issue #601).
  if w.closed:
    return err("writer is closed")

  var dataSeq = newSeq[byte](data.len)
  for i in 0 ..< data.len:
    dataSeq[i] = data[i]

  var metaSeq = newSeq[byte](metadata.len)
  for i in 0 ..< metadata.len:
    metaSeq[i] = metadata[i]

  let ev = IOEvent(
    kind: kind,
    stepId:
      if stepId.isSome: stepId.get()
      elif w.stepCount > 0: w.stepCount - 1
      else: 0,
    metadata: metaSeq,
    data: dataSeq,
  )

  let res = w.container.writeEvent(w.ioEventWriter, ev)
  if res.isErr:
    return err("failed to write IO event: " & res.error)
  ok()

# ---------------------------------------------------------------------------
# Exception events
# ---------------------------------------------------------------------------

proc jsonEscape(s: string): string =
  ## Minimal JSON string escaping for the MarkerPayload document.
  result = newStringOfCap(s.len + 8)
  for c in s:
    case c
    of '"': result.add("\\\"")
    of '\\': result.add("\\\\")
    of '\n': result.add("\\n")
    of '\r': result.add("\\r")
    of '\t': result.add("\\t")
    else:
      if c < ' ':
        const hex = "0123456789abcdef"
        result.add("\\u00")
        result.add(hex[(int(c) shr 4) and 0xF])
        result.add(hex[int(c) and 0xF])
      else:
        result.add(c)

proc ensureMarkerId*(w: var MultiStreamTraceWriter, label: string):
    Result[uint64, string] =
  ## Intern a correlation-marker label and return its numeric id.
  ##
  ## THE PRIMARY OPERATION, mirroring `ensure_path_id`.  A caller hoists this
  ## out of its hot path — once per boundary, not once per crossing — and then
  ## passes the integer to `registerCorrelationMarkerById`, so the per-marker
  ## call does no string lookup.  Interning is the only part that touches a
  ## hash map, which is the same reason a binding must keep it off the hot
  ## path as it keeps host-language conversion off it.
  ##
  ## `markers.dat` / `markers.off` are created LAZILY here, not in
  ## `initTraceInterningTables`: the four standard tables are made for every
  ## trace, and a fifth there would put marker files into every container ever
  ## written.  A recording that declares no marker gains no files.
  if w.closed:
    return err("writer is closed")
  if not w.hasMarkerLabels:
    w.markerLabels = ?initInterningTableWriter(w.container, "markers")
    w.hasMarkerLabels = true
  ensureMarkerLabelId(w.container, w.markerLabels, label)

proc registerCorrelationMarkerById*(w: var MultiStreamTraceWriter,
    direction: string, markerId: uint64, boundaryLabel: string,
    keyValue: string, showValue: string = "", description: string = "",
    keyText: string = "key", showText: string = "",
    stepId: Option[uint64] = none(uint64)):
    Result[void, string] =
  ## Declare a correlation marker against an already-interned label id.
  ##
  ## THE PRIMARY HOT-PATH ENTRY POINT (`Correlation-Markers.md` §2.4),
  ## implemented once here so the ~20 CTFS recorders bind to it rather than
  ## each building the payload — a recorder whose field names drifted would
  ## write markers that are INVISIBLE rather than broken.
  ##
  ## `boundaryLabel` is passed alongside the id because the on-disk
  ## `MarkerPayload` carries `boundary_id` as TEXT for the debugger, while the
  ## index keys on the id.  A caller that has hoisted `ensureMarkerId` already
  ## holds the label, so this costs it nothing.
  ##
  ## Two things happen, and both matter:
  ##
  ## 1. The `MarkerPayload` goes into an IO event's metadata slot, where every
  ##    existing consumer looks.  Field names must match `MarkerPayload` in
  ##    `db-backend/src/correlation_markers.rs`.
  ## 2. It is indexed into `corrmark.ns` for lookup without decoding the
  ##    event stream.
  ##
  ## NO STEP IS MINTED.  The marker attaches to the enclosing step — the line
  ## the call sits on.  Minting one would insert an exec-stream event no user
  ## code executed and shift every later step index, which is what spans'
  ## `start_step`/`end_step` are measured in.
  ##
  ## `stepId` names that enclosing step, and **one value serves both halves**:
  ## the `MarkerPayload` event's `step_id` and the index entry's `geid`. They
  ## MUST be the same number. They were not: the event defaulted to
  ## `stepCount - 1` while the index recorded `stepCount`, so a single marker
  ## carried two different coordinates and a consumer that resumed from the
  ## index landed one step away from where `ct print` showed the marker. Found
  ## by the JavaScript recorder while becoming a thin binding, which is
  ## precisely the review §11a.2 asks a binding to perform on this API.
  ##
  ## `none` means "the last step this writer emitted", which is right for a
  ## caller that writes its steps eagerly. A caller that BUFFERS a step — the
  ## C ABI does, so values registered after `trace_writer_register_step` still
  ## attach to it — must pass the id explicitly, because at marker time the
  ## step for the marker's own line has not been emitted yet and
  ## `stepCount - 1` names the previous one (issue #601, the same accounting
  ## `trace_writer_register_special_event` documents).
  ##
  ## `keyText` and `showText` are the NAMES the two values were read under —
  ## `key_text` is the textual form of the `key=<expr>` declaration, and
  ## `showText` is the binding the shown value came from. `showText` is
  ## load-bearing rather than cosmetic: a cross-process origin chain resumes
  ## its walk on that name in the sending recording, so a marker that drops it
  ## is visible with its history unreachable. They were hard-coded to "key" and
  ## "show" here until the JavaScript recorder — the only end-to-end
  ## implementation that predates this shared API — turned out to pass a real
  ## binding name, which is exactly the kind of signal contract §11a.2 says to
  ## treat as the API's shape being wrong rather than to work around.
  ##
  ## `keyValue` / `showValue` are ALREADY-STRINGIFIED UTF-8: this library never
  ## calls back into the host to render a value, because a conversion that can
  ## raise must not run under the writer lock — a Ruby exception `longjmp`s
  ## past Rust destructors and strands the guard.
  if w.closed:
    return err("writer is closed")

  let dir = if direction == "recv" or direction == "receive": "recv" else: "send"
    ## An unrecognised direction becomes "send" rather than an error, matching
    ## the JS recorder: a marker with no side is unpairable, and an unpairable
    ## marker is worse than one that picked a side.

  var payload = "{\"marker_id\":" & $markerId
  payload.add(",\"boundary_id\":\"" & jsonEscape(boundaryLabel) & "\"")
  payload.add(",\"direction\":\"" & dir & "\"")
  payload.add(",\"key_text\":\"" &
    jsonEscape(if keyText.len > 0: keyText else: "key") & "\"")
  payload.add(",\"key_value\":\"" & jsonEscape(keyValue) & "\"")
  if showValue.len > 0 or showText.len > 0:
    payload.add(",\"show_text\":\"" &
      jsonEscape(if showText.len > 0: showText else: "show") & "\"")
    payload.add(",\"show_value\":\"" & jsonEscape(showValue) & "\"")
  if description.len > 0:
    payload.add(",\"description\":\"" & jsonEscape(description) & "\"")
  payload.add("}")

  var metaBytes = newSeq[byte](payload.len)
  for k, c in payload:
    metaBytes[k] = byte(c)
  let enclosingStep =
    if stepId.isSome: stepId.get()
    elif w.stepCount > 0: w.stepCount - 1
    else: 0'u64

  ?w.registerIOEvent(ioStdout, [], metaBytes, stepId = some(enclosingStep))

  w.correlationMarkers.add(initBoundaryMarker(
    markerId, keyValue, isRecv = dir == "recv", geid = enclosingStep))
  ok()

proc registerCorrelationMarker*(w: var MultiStreamTraceWriter,
    direction: string, boundaryId: string, keyValue: string,
    showValue: string = "", description: string = "",
    keyText: string = "key", showText: string = "",
    stepId: Option[uint64] = none(uint64)):
    Result[void, string] =
  ## String-label convenience: interns `boundaryId`, then forwards.
  ##
  ## A WRAPPER OVER THE NUMERIC PATH, never the reverse.  If the string form
  ## were primary, every binding would grow its own label cache and they would
  ## drift — which is the reason this moved into the shared writer at all.
  let id = ?w.ensureMarkerId(boundaryId)
  w.registerCorrelationMarkerById(
    direction, id, boundaryId, keyValue, showValue, description,
    keyText, showText, stepId)

proc registerSpanCoverage*(w: var MultiStreamTraceWriter,
    traceIdBe: openArray[byte], spanIdBe: openArray[byte],
    wallTimeUnixNs: uint64, monotonicTimeNs: uint64,
    threadId: uint64 = 0, isExit: bool = false,
    stepId: Option[uint64] = none(uint64)): Result[void, string] =
  ## Declare that this recording covers a distributed-trace span
  ## (`corrmark.ns` kind 0).
  ##
  ## THE ENTRY POINT AN OBSERVABILITY RECORDER CALLS per served request, so a
  ## consumer holding an OTel `(trace_id, span_id)` can decide whether this
  ## recording covers it with one B-tree lookup instead of decoding the event
  ## stream.
  ##
  ## Ids are the WIRE bytes — 16 and 8 — not a hex rendering.  Hashing the hex
  ## would key the index on something no consumer computes; `decodeHexId` is
  ## provided for bindings whose host hands them hex.
  ##
  ## **This mints no `MarkerPayload` and no IO event**, unlike
  ## `registerCorrelationMarkerById`.  A span-coverage marker has no send/recv
  ## sense and no pairing domain, so forcing it into a `MarkerPayload` would
  ## make the pairing index try to pair spans with each other (contract
  ## §10.2).  One index, two kinds; two payload shapes.
  ##
  ## `geid` is the ENCLOSING step — the coordinate a consumer resumes replay
  ## from — and follows the same rule as a boundary marker's: `none` means the
  ## last step this writer emitted, and a caller that buffers its steps (the C
  ## ABI) passes the id explicitly. No step is minted, for the reason in
  ## §11a.6.
  if w.closed:
    return err("writer is closed")
  let enclosingStep =
    if stepId.isSome: stepId.get()
    elif w.stepCount > 0: w.stepCount - 1
    else: 0'u64
  let m = ?initSpanMarker(traceIdBe, spanIdBe, wallTimeUnixNs,
    monotonicTimeNs, geid = enclosingStep, threadId = threadId,
    isExit = isExit)
  w.correlationMarkers.add(m)
  ok()

proc registerSpanCoverageHex*(w: var MultiStreamTraceWriter,
    traceIdHex: string, spanIdHex: string,
    wallTimeUnixNs: uint64, monotonicTimeNs: uint64,
    threadId: uint64 = 0, isExit: bool = false,
    stepId: Option[uint64] = none(uint64)): Result[void, string] =
  ## Hex convenience over `registerSpanCoverage`, for callers whose OTel API
  ## hands them the canonical 32/16-character hex ids.  A WRAPPER: the byte
  ## form stays primary so the conversion has exactly one implementation.
  let traceId = ?decodeHexId(traceIdHex, 16)
  let spanId = ?decodeHexId(spanIdHex, 8)
  w.registerSpanCoverage(traceId, spanId, wallTimeUnixNs, monotonicTimeNs,
    threadId, isExit, stepId)

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
  let res = w.container.writeEvent(w.execWriter, ev)
  if res.isErr:
    return err("failed to write raise event: " & res.error)

  # Write empty values to keep streams in sync
  let valRes = w.container.writeStepValues(w.valueWriter, @[])
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
  let res = w.container.writeEvent(w.execWriter, ev)
  if res.isErr:
    return err("failed to write catch event: " & res.error)

  # Write empty values to keep streams in sync
  let valRes = w.container.writeStepValues(w.valueWriter, @[])
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
  let res = w.container.writeEvent(w.execWriter, ev)
  if res.isErr:
    return err("failed to write thread_switch event: " & res.error)

  let valRes = w.container.writeStepValues(w.valueWriter, @[])
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
  let res = w.container.writeEvent(w.execWriter, ev)
  if res.isErr:
    return err("failed to write thread_start event: " & res.error)

  let valRes = w.container.writeStepValues(w.valueWriter, @[])
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
  let res = w.container.writeEvent(w.execWriter, ev)
  if res.isErr:
    return err("failed to write thread_exit event: " & res.error)

  let valRes = w.container.writeStepValues(w.valueWriter, @[])
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

  # CTFS-M20: flush the last partial calls.dat chunk and write the companion
  # calls.idx seek index. This makes the Nim-written `calls.dat` SEEKABLE by
  # the Rust `CallStreamReader` (db-backend seekable path), byte-compatible
  # with the Rust writer's calls.dat/calls.idx layout. Must run after the
  # final flushCompletedCalls (the last writeCall) and before meta.dat.
  let finalizeRes = call_stream.finalizeCallStream(w.container, w.callWriter)
  if finalizeRes.isErr:
    return err("failed to finalize call stream: " & finalizeRes.error)

  # Flush exec stream
  let flushRes = w.container.flush(w.execWriter)
  if flushRes.isErr:
    return err("failed to flush exec stream: " & flushRes.error)

  # M24a-2: flush the last partial values.dat chunk and finalize the companion
  # values.idx.  This makes the Nim-written `values.dat` byte-compatible with
  # the Rust `ValueStreamReader` (the SPEC chunked layout); it must run before
  # meta.dat so the has_value_stream flag (set below) is accurate.
  let valFlushRes = value_stream.flush(w.container, w.valueWriter)
  if valFlushRes.isErr:
    return err("failed to flush value stream: " & valFlushRes.error)

  # M24a-3: flush the last partial events.dat chunk and finalize the companion
  # events.idx.  This makes the Nim-written `events.dat` byte-compatible with
  # the Rust `IoEventStreamReader` (the SPEC chunked layout); it must run before
  # meta.dat so the has_io_event_stream flag (set below) is accurate.
  let ioFlushRes = io_event_stream.flush(w.container, w.ioEventWriter)
  if ioFlushRes.isErr:
    return err("failed to flush io event stream: " & ioFlushRes.error)

  # RS-M1: flush the last partial spans.dat chunk and emit the spantype.ns
  # span-type index.  Skipped entirely when no span was ever registered, so a
  # span-free container gains no new files and keeps bit 13 clear.  Must run
  # before meta.dat so the has_span_stream flag below is accurate.
  if w.hasSpans:
    let spanFlushRes = span_stream.flush(w.container, w.spanWriter)
    if spanFlushRes.isErr:
      return err("failed to flush span stream: " & spanFlushRes.error)
    let spanNsRes = writeSpanTypeNamespace(w.container, w.spanWriter)
    if spanNsRes.isErr:
      return err("failed to write spantype.ns: " & spanNsRes.error)

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
      w.container, SourceViewsBaseName)
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
      let appendRes = w.container.append(svTable, rec)
      if appendRes.isErr:
        return err("failed to write source_views record: " & appendRes.error)

  # M26b — serialise the prepopulated breakpoint index into the CTFS container
  # as `step-map.ns` (spec §4.1 `STMP`).  Emitted by default for line-only
  # traces (suppressed for column-aware ones; see `emitStepMap`).  The file is
  # additive: only the M26 db-backend consumer reads it; every other reader
  # ignores the extra root entry, so other streams stay byte-identical and
  # legacy bundles (no `step-map.ns`) still read via the M26 whole-table
  # fallback.  An empty trace (no recorded steps) still writes a well-formed
  # zero-path `STMP` blob so the namespace is always parseable when present.
  if w.emitStepMap:
    let stepMapBytes = w.stepMapBuilder.serialize()
    let smFileRes = w.container.addFile(StepMapFileName)
    if smFileRes.isErr:
      return err("failed to add step-map.ns: " & smFileRes.error)
    var smFile = smFileRes.get()
    let smWriteRes = w.container.writeToFile(smFile, stepMapBytes)
    if smWriteRes.isErr:
      return err("failed to write step-map.ns: " & smWriteRes.error)

  # M8 Production CoW namespaces — when linehits are enabled, persist the
  # production builder's line-hit index as a CoW namespace image. The old
  # LinehitsBuilder used the legacy in-memory Namespace object and never wrote
  # it into the container; this writes `linehits.tc` as an `NSB1` CowBTree whose
  # descriptors point at the varint step-id payload appended to the namespace
  # image. Rust opens the index via CowNamespaceReader.
  if w.linehitsBuilder.isSome:
    let lhBytesRes = w.linehitsBuilder.get().serializeCowNamespace()
    if lhBytesRes.isErr:
      return err("failed to serialize linehits.tc: " & lhBytesRes.error)
    let lhFileRes = w.container.addFile("linehits.tc")
    if lhFileRes.isErr:
      return err("failed to add linehits.tc: " & lhFileRes.error)
    var lhFile = lhFileRes.get()
    let lhWriteRes = w.container.writeToFile(lhFile, lhBytesRes.get())
    if lhWriteRes.isErr:
      return err("failed to write linehits.tc: " & lhWriteRes.error)

  # Stage C — attached writers stop here.  Everything above finalized THIS
  # writer's own streams and additive namespaces (calls.idx/values.idx/
  # events.idx/steps.idx, spantype.ns, srcviews, step-map.ns, linehits.tc) into
  # the shared container, so its streams are complete and seekable.  But
  # `meta.dat` belongs to the container's OWNER, and the shared container's
  # lifetime is the owner's too, so an attached `close` must not write meta.dat
  # or `closeCtfs` — it just marks itself closed and returns.
  if w.attached:
    # Publish this writer's entry sizes before handing the container back to
    # its owner: `writeToFile` only updates them in block 0's in-memory image.
    w.container.syncRootBlock()
    w.closed = true
    return ok()

  # Write corrmark.ns — the correlation index — when this recording declared
  # any markers.  A recording with none writes NO namespace: its absence says
  # "never indexed", which a consumer must not report as "does not cover that
  # span" (contract §9).  That is why this is conditional rather than always
  # emitting an empty index.
  if w.correlationMarkers.len > 0:
    let imageRes = serializeCorrmarkNamespace(w.correlationMarkers)
    if imageRes.isErr:
      return err("failed to build corrmark.ns: " & imageRes.error)
    let image = imageRes.get()
    let nsFileRes = w.container.addFile(CorrmarkNamespaceName)
    if nsFileRes.isErr:
      return err("failed to add corrmark.ns: " & nsFileRes.error)
    var nsFile = nsFileRes.get()
    let writeRes = w.container.writeToFile(nsFile, image)
    if writeRes.isErr:
      return err("failed to write corrmark.ns: " & writeRes.error)

  # Write meta.dat
  let metaFileRes = w.container.addFile("meta.dat")
  if metaFileRes.isErr:
    return err("failed to add meta.dat: " & metaFileRes.error)
  var metaFile = metaFileRes.get()

  let metaRes = w.container.writeMetaDat(
    metaFile, w.metadata, w.paths,
    filterProvenance = w.filterProvenance,
    emitFilterProvenance = w.recordEmptyFilterProvenance,
    columnAwareSteps = w.columnAwareSteps,
    alternateSourceViews = hasSourceViews,
    supportsColumnBreakpoints = w.supportsColumnBreakpoints,
    supportsColumnMotions = w.supportsColumnMotions,
    # M17a: the multi-stream writer ALWAYS emits a dedicated calls.dat call
    # stream (initCallStreamWriter above), so stamp the has_call_stream
    # capability flag.  Readers may then load the call tree from calls.dat
    # directly; the flag is the M17a gate for that on-demand path.
    hasCallStream = true,
    # M24a-1: the writer ALWAYS emits a dedicated steps.dat/steps.idx execution
    # stream (initExecStreamWriter above) in the SPEC-canonical layout, so stamp
    # the has_step_stream capability flag (bit 9).  The flag both gates the
    # Rust/db-backend seekable step path AND, for the Nim FFI reader, marks the
    # bundle as SPEC-framed (vs the legacy Nim-v4 framing that never set it).
    hasStepStream = true,
    # M24a-2: the writer ALWAYS emits a dedicated values.dat/values.idx value
    # stream (initValueStreamWriter above) in the SPEC-canonical chunked layout,
    # so stamp the has_value_stream capability flag (bit 10).  The flag both
    # gates the Rust/db-backend seekable value path AND, for the Nim FFI reader,
    # marks the bundle as SPEC-framed (vs the legacy Nim-v4 .off VRT framing
    # that never set it).
    hasValueStream = true,
    # M24a-3: the writer ALWAYS emits a dedicated events.dat/events.idx I/O event
    # stream (initIOEventStreamWriter above) in the SPEC-canonical chunked
    # layout, so stamp the has_io_event_stream capability flag (bit 11).  The
    # flag both gates the Rust/db-backend event-log path AND, for the Nim FFI
    # reader, marks the bundle as SPEC-framed (vs the legacy Nim-v4 .off VRT
    # framing that never set it).
    hasIoEventStream = true,
    # RS-M1: unlike the four stream bits above, bit 13 is stamped ONLY when a
    # span was actually registered.  See `hasSpans` for why this one is
    # conditional: bit 13 is rejecting, not additive, at the reader.
    hasSpanStream = w.hasSpans,
    # Bit 14 is stamped only when the writer opted into recording per-file
    # line counts.  Like bit 13 it is rejecting, not additive, at the
    # reader, so a writer that did not opt in must leave it clear — see
    # `enableLineCountTable`.
    hasLineCountTable = w.lineCountTable,
    # WTCI (bit 15): stamped only when the recording actually declared a
    # marker, i.e. exactly when `corrmark.ns` was written above.  The bit is
    # a hint and the file entry is the authority, so the two must never be
    # able to disagree: a bit set over a container with no index would
    # reintroduce the exact ambiguity the contract's §9 removes.
    hasCorrelationIndex = w.correlationMarkers.len > 0)
  if metaRes.isErr:
    return err("failed to write meta.dat: " & metaRes.error)

  # Publish block 0 — the root entry array, which carries every internal
  # file's size.  `writeToFile` maintains those sizes in the in-memory image
  # and flushes only the data block it filled, so without this the container
  # on disk reports each member's size as of the last `addFile`: `meta.dat`,
  # written last, reads back as 0 bytes and the whole recording decodes as an
  # empty program.  One 4 KiB write per recording.
  w.container.syncRootBlock()

  w.closed = true
  ok()

proc isClosed*(w: MultiStreamTraceWriter): bool =
  ## True once `close` has run.  Exposed so a caller that finalizes on a
  ## teardown path (the C ABI's `trace_writer_free`) can tell "already closed,
  ## nothing to flush" from "still open", instead of driving buffered writes
  ## into a closed writer and reporting the resulting error as a real one.
  w.closed

proc toBytes*(w: var MultiStreamTraceWriter): seq[byte] =
  ## Get the serialized CTFS bytes. Must call close() first.
  w.container.toBytes()

proc closeCtfs*(w: var MultiStreamTraceWriter): Result[void, string]
    {.discardable.} =
  ## Release any resources held by the underlying CTFS container.
  ##
  ## No-op for an ATTACHED writer: the container is owned by another writer,
  ## which is responsible for finalizing and closing it.  Only the owner's
  ## `closeCtfs` may write the final root block and close the stream file.
  ##
  ## Returns the container's finalization error instead of dropping it: this
  ## is the write that makes the recording durable, so a failure here means
  ## the trace on disk is incomplete.
  if w.attached:
    return ok()
  w.container.closeCtfs()
