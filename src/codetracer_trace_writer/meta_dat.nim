when defined(nimPreviewSlimSystem):
  import std/assertions  # doAssert is not in `system` under slim-system

{.push raises: [].}

## Binary meta.dat writer for CTFS trace metadata.
##
## Layout (version 4):
##   [4] magic "CTMD"
##   [2] version u16 LE
##   [2] flags u16 LE (bit 0: has_mcr_fields,
##                    bit 1: has_replay_launch_fields,
##                    bit 2: has_layout_snapshot,
##                    bit 3: has_trace_filter_provenance)
##   varint-prefixed recording_id string  (M-REC-1; required, UUIDv7,
##                                         lowercase hyphenated 36-char form)
##   varint-prefixed program string
##   varint args_count, then varint-prefixed arg strings
##   varint-prefixed workdir string
##   varint-prefixed recorder_id string
##   varint paths_count, then varint-prefixed path strings
##   if has_mcr_fields:
##     varint tick_source
##     varint total_threads
##     varint atomic_mode
##     varint total_events
##     varint total_checkpoints
##     varint start_time_unix_us
##     varint-prefixed platform string
##     varint-prefixed tick_granularity string
##     varint-prefixed tick_source_str string
##     varint-prefixed atomic_mode_str string
##     varint-prefixed start_time_str string
##     varint-prefixed hook_profile string                      (v2)
##     varint hook_strategies_count, then strings               (v2)
##   if has_replay_launch_fields:                                (M-RLP-1)
##     u8 aslr_disabled (0 = false, 1 = true)
##   if has_layout_snapshot:                                     (M-RLP-2)
##     u64 layout_hash (XXH64 over the fingerprint bytes, seed 0)
##     varint fingerprint_len
##     bytes fingerprint[fingerprint_len]
##   if has_trace_filter_provenance:                             (TF-M7)
##     varint trace_filter_count
##     for i in 0 ..< trace_filter_count:
##       varint path_len, then UTF-8 path bytes
##       32 raw bytes: SHA-256 of the filter source (no length prefix)
##
## Version history:
##   v1 — initial release (no hook fields).  Removed before any external
##        consumer shipped: F5a Phase A dual-wrote meta.json alongside
##        meta.dat, and the meta.json carried hookProfile/hookStrategies
##        until the schema gained them in v2.
##   v2 — appended hookProfile + hookStrategies inside the MCR-fields
##        block so meta.dat reaches parity with the legacy meta.json.
##   v3 — M-REC-1 (2026-05-18): prepended a required `recording_id`
##        UUIDv7 string before the existing `program` field.  Pre-1.0:
##        no backcompat shim — v2 fixtures must be regenerated.  Spec:
##        ~codetracer-specs/Refactoring-Plans/Recording-Identifier-Migration.md~
##        §3 and the M-REC-1 milestone in the companion `.status.org`.
##        M-RLP-1 (2026-05-12) added FlagHasReplayLaunchFields (bit 1)
##        and a one-byte aslr_disabled block appended after the MCR
##        block; readers that don't know about the bit simply stop after
##        the MCR block, so this is a forward-compatible extension at
##        version 2 (no schema bump needed — the flag bit gates parsing).
##        M-RLP-2 (2026-05-13) added FlagHasLayoutSnapshot (bit 2) and a
##        separate block after the replay-launch block.  Choosing a
##        separate flag bit (rather than extending the replay-launch
##        block) preserves binary compatibility for traces recorded
##        between M-RLP-1 and M-RLP-2, which carry the replay-launch
##        block but no layout snapshot.
##        TF-M7 (2026-05-14) added FlagHasTraceFilterProvenance (bit 3)
##        and a separate block after the layout-snapshot block.  Spec:
##        `codetracer-trace-format-spec/internal-files.md` §
##        "Flag bit 3 — Trace filter provenance" and
##        `codetracer-trace-format-spec/Trace-Filters.md` § 7.  Bit 3
##        was chosen (rather than the spec-suggested bit 1) because
##        bits 1 and 2 had already shipped as FlagHasReplayLaunchFields
##        / FlagHasLayoutSnapshot in M-RLP-1/M-RLP-2; reusing them
##        would break the in-flight trace fixtures from those
##        milestones.  The spec was updated in the same TF-M7 commit
##        series to match.
##   v4 — (2026-09-08): the line-only ``global_position_index`` encode
##        changed from ``prefixSum[path_id] + line`` to
##        ``prefixSum[path_id] + (line - 1)``, making it the exact
##        inverse of the decode the spec states.  Spec:
##        ``codetracer-trace-format-spec`` branch ``zk/gli-off-by-one``
##        (commit ``becd9e1``), ``internal-files.md`` §"Global Line
##        Index" and ``trace-events.md`` §"Decoding
##        ``global_position_index``".
##
##        The bump exists because the two encodes are INDISTINGUISHABLE
##        in the bytes.  Both put every step at an address the space can
##        address, so a v3 container read under the v4 decode yields a
##        ``(path, line)`` pair for every step and reports each one
##        exactly one line high — silently, with nothing for
##        ``tryResolve`` to refuse.  (The Rust writer's rival
##        ``(path_id shl 32) or line`` is catchable only because it
##        lands OUTSIDE the space.)  The schema version is the sole
##        discriminator: ``recorder_id`` names the producer, not its
##        address packing.
##
##        Pre-1.0: no backcompat shim — v3 fixtures must be regenerated.
##        A shim is not possible in principle here, only in practice:
##        subtracting one from every address would fix the steps of a
##        trace the writer of which used this packing, but the version
##        is what says it did, and that is exactly what a v3 container
##        does not record.  ``readMetaDat`` therefore refuses v3 and
##        below by name rather than decoding them one line high.

import std/options
import std/strutils
import results
import ../codetracer_trace_types
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ./varint
import ./uuid_v7

const
  MetaDatMagic*: array[4, byte] = [0x43'u8, 0x54, 0x4D, 0x44]  # "CTMD"
  MetaDatVersion*: uint16 = 4
  LastShiftedGlobalIndexVersion*: uint16 = 3
    ## The highest schema version whose writer packed a line-only
    ## ``global_position_index`` as ``prefixSum[path_id] + line``.
    ##
    ## Containers at or below it are refused by ``readMetaDat``: their
    ## addresses are one above what the current decode inverts, and
    ## nothing else in the container says so.  It is named rather than
    ## written as a literal `3` at the refusal because the refusal and
    ## this definition have to move together — a later version that
    ## changed the packing again would raise the bound, and a reader
    ## comparing against a stale literal would answer such a container
    ## instead of refusing it.
  FlagHasMcrFields*: uint16 = 1                  # bit 0
  FlagHasReplayLaunchFields*: uint16 = 2         # bit 1 — M-RLP-1 (spec §6A.5)
  FlagHasLayoutSnapshot*: uint16 = 4             # bit 2 — M-RLP-2 (spec §6B.7)
  FlagHasTraceFilterProvenance*: uint16 = 8      # bit 3 — TF-M7 (spec §7)
  FlagHasColumnAwareSteps*: uint16 = 0x10        # bit 4 — P6.3 / P6.4
    ## When set, the exec stream is permitted to contain tag 0x07
    ## (sekDeltaColumn) and ``global_position_index`` addresses
    ## ``(line, column)`` tuples instead of lines.  See
    ## ``codetracer-trace-format-spec/trace-events.md``
    ## §"Reader Behaviour and Back-Compat" and
    ## ``codetracer-trace-format-spec/internal-files.md``
    ## §"Metadata (meta.dat)".
  FlagHasAlternateSourceViews*: uint16 = 0x20    # bit 5 — Deminification Support
    ## When set, the trace carries one or more ``source_views.dat``
    ## records: alternate (formatted) views of source paths registered
    ## in ``paths.dat``, used to deminify minified JS/Python sources at
    ## record time.  Each record carries
    ## ``(path_id, view_kind, view_name, content, sourcemapV3)``.
    ## See ``codetracer-trace-format-spec/internal-files.md`` §
    ## "Alternate Source Views (Deminification Support)".
  FlagSupportsColumnBreakpoints*: uint16 = 0x40  # bit 6 — Capability Flag
    ## Capability bit: when set, the recorder's columns are sharp enough
    ## for the GUI to place per-column breakpoints (M6 Alt+click).  The
    ## bit MUST only be set in combination with
    ## ``FlagHasColumnAwareSteps`` — capability flags presuppose column
    ## data on the wire.  Recorders that emit columns purely as display
    ## hints (no runtime distinguishability between same-line statements)
    ## MUST leave this bit clear; the GUI then disables the per-column
    ## breakpoint affordance and falls back to line-only breakpoints.
    ## See ``codetracer-trace-format-spec/internal-files.md``
    ## §"Column-Aware Capability Flags".
  FlagSupportsColumnMotions*: uint16 = 0x80      # bit 7 — Capability Flag
    ## Capability bit: when set, the recorder's step predicate fires
    ## per statement-start (not per line) so the GUI can offer
    ## column-aware step-over / step-in / step-out.  Like
    ## ``FlagSupportsColumnBreakpoints``, this bit MUST only be set
    ## together with ``FlagHasColumnAwareSteps``.  When clear the GUI
    ## hides the per-column motion buttons; legacy line-only motions
    ## remain available.
  FlagHasStepStream*: uint16 = 0x200             # bit 9 — M23a
    ## When set, the materialized `.ct` carries a dedicated compact
    ## execution stream (`steps.dat` + its companion seekable index
    ## `steps.idx`) in addition to the unified event stream.  The split
    ## lets a reader load the step timeline independently / on-demand
    ## without scanning the unified stream
    ## (trace-events.md §"Execution Stream (`steps.dat`)").  Like
    ## ``FlagHasCallStream`` this is ADDITIVE: the unified stream is
    ## unchanged, and a reader that does not know the bit simply ignores
    ## `steps.dat`/`steps.idx`.  The flag is the M23a gate; the dedicated
    ## db-backend seekable step reader that consumes `steps.dat` lands in
    ## M22.  Must match
    ## ``codetracer_trace_writer::meta_dat::FLAG_HAS_STEP_STREAM`` (Rust)
    ## and the db-backend ``FLAG_HAS_STEP_STREAM`` bit 9.
  FlagHasValueStream*: uint16 = 0x400            # bit 10 — M23b
    ## When set, the materialized `.ct` carries a dedicated parallel value
    ## stream (`values.dat` + its companion seekable index `values.idx`)
    ## in addition to the unified event stream.  The value stream is
    ## parallel-indexed to the execution stream — value record N
    ## corresponds to step N (`steps.dat` record N), with an empty record
    ## for steps that have no variable activity — so a reader can load a
    ## step's variable values independently / on-demand without scanning
    ## the unified stream (trace-events.md §"Value Stream").  Like
    ## ``FlagHasStepStream`` this is ADDITIVE: the unified stream is
    ## unchanged, and a reader that does not know the bit simply ignores
    ## `values.dat`/`values.idx`.  The value stream lives in its OWN CTFS
    ## file pair (NOT `steps.dat`) because value records are large
    ## (50-500B) with different Zstd chunk sizing than the tiny (2-4B)
    ## execution records, and a CTFS internal file is a single seekable
    ## byte range with one companion index.  The flag is the M23b gate; the
    ## db-backend seekable value reader that consumes `values.dat` lands in
    ## M22.  Must match
    ## ``codetracer_trace_writer::meta_dat::FLAG_HAS_VALUE_STREAM`` (Rust)
    ## and the db-backend ``FLAG_HAS_VALUE_STREAM`` bit 10.
  FlagHasIoEventStream*: uint16 = 0x800          # bit 11 — M23c
    ## When set, the materialized `.ct` carries a dedicated I/O event
    ## stream (`events.dat` + its companion seekable index `events.idx`)
    ## in addition to the unified event stream.  It holds the
    ## ``EventLogKind``-tagged I/O / log events
    ## (stdout/stderr/file/network/error/log) split out of the unified
    ## stream; each record carries ``kind`` (u8) / ``step_id`` (varint
    ## cross-reference to the execution stream) / ``metadata`` / ``content``
    ## (trace-events.md §"IO Event Stream (`events.dat`)").  Like
    ## ``FlagHasValueStream`` this is ADDITIVE: the unified stream is
    ## unchanged, and a reader that does not know the bit simply ignores
    ## `events.dat`/`events.idx`.  NOTE the file naming — the legacy
    ## combined stream file is `events.log`; this NEW I/O stream is the
    ## distinct `events.dat` (do not collide the two).  The flag is the
    ## M23c gate; the event-log pane that paginates `events.dat` consumes
    ## it later.  Must match
    ## ``codetracer_trace_writer::meta_dat::FLAG_HAS_IO_EVENT_STREAM``
    ## (Rust) and the db-backend ``FLAG_HAS_IO_EVENT_STREAM`` bit 11.
  FlagHasInterningTables*: uint16 = 0x1000       # bit 12 — M23d
    ## When set, the materialized `.ct` carries the binary varint
    ## interning tables (`paths.dat`+`paths.off`, `funcs.dat`+`funcs.off`,
    ## `types.dat`+`types.off`, `varnames.dat`+`varnames.off`) in addition
    ## to the legacy `events.log` / `paths.json` interning.  These use the
    ## Variable-Size Record Table (`.dat` + `.off`) pattern — a `.dat` of
    ## serialized records plus a u64-LE offset index for O(1) random
    ## access by id (internal-files.md §"Interning Tables").  Like
    ## ``FlagHasIoEventStream`` this is ADDITIVE: `events.log` / `paths.json`
    ## are unchanged, and a reader that does not know the bit simply
    ## ignores the eight new files.  The flag is the M23d gate; the
    ## consumer migration off the legacy interning lands later.  Must
    ## match ``codetracer_trace_writer::meta_dat::FLAG_HAS_INTERNING_TABLES``
    ## (Rust) and the db-backend ``FLAG_HAS_INTERNING_TABLES`` bit 12.
  FlagHasSpanStream*: uint16 = 0x2000            # bit 13 — RS-M1
    ## When set, the materialized `.ct` carries the request/interval span
    ## streams: `spans.dat` (chunked compressed span records) + its companion
    ## seekable index `spans.idx`, plus the `spantype.ns` namespace that maps
    ## an interned `span_type` id to the span ids of that type.  A span is a
    ## bounded, labeled interval of execution named by the coordinate
    ## `(process_ord, thread_id, step range)` — HTTP requests, processes,
    ## tests — replacing the `session_manifest.jsonl` /
    ## `codetracer_spans.jsonl` sidecars.  See
    ## ``codetracer-specs/Trace-Files/CTFS-Request-Span-Streams.md``.
    ##
    ## **This bit is NOT additive at the reader.**  Unlike the flag-bit notes
    ## on bits 8-12 above, which describe their streams as "a reader that does
    ## not know the bit simply ignores the files", ``KnownFlags`` +
    ## ``readMetaDat`` REJECT any container carrying a bit outside the known
    ## mask.  A reader built before this constant existed therefore refuses a
    ## span-bearing container outright rather than ignoring `spans.dat`.  The
    ## rollout consequence: reader support (this constant, in ``KnownFlags``)
    ## must ship everywhere before any writer sets the bit.  Accordingly no
    ## existing writer call site sets it — `hasSpanStream` defaults to false
    ## on both `writeMetaDat` and `writeMetaDatToBuffer`, so a recorder must
    ## opt in explicitly and containers without spans are byte-identical to
    ## what the writer produced before this bit existed.
    ##
    ## Must match ``codetracer_trace_writer::meta_dat::FLAG_HAS_SPAN_STREAM``
    ## (Rust) and the db-backend ``FLAG_HAS_SPAN_STREAM`` bit 13 (RS-M2).
  FlagHasLineCountTable*: uint16 = 0x4000        # bit 14 — per-file line counts
    ## When set, every ``paths.dat`` record carries the file's line count
    ## after the path bytes, and the line-only global position space is
    ## laid out from those counts rather than from the
    ## ``DefaultLinesPerFile`` convention.  The record layout is
    ## ``path_len + path_bytes + line_count`` — the first three fields of
    ## the column-aware Layout A record, without its trailing per-line
    ## table.  See ``codetracer-trace-format-spec/internal-files.md``
    ## §"``paths.dat`` line-count table".
    ##
    ## **The counts are mandatory under this bit**, not per-file
    ## optional.  A file that omitted its count would put the reader back
    ## to assuming a size for it, which is the defect the table exists to
    ## remove; ``registerPath`` therefore refuses a path with no count
    ## once the table is enabled, and a writer that cannot determine a
    ## file's real line count records the ``DefaultLinesPerFile`` ceiling
    ## it used.  Every file's size is then a number the container states.
    ##
    ## Mutually exclusive with ``FlagHasColumnAwareSteps``: a
    ## column-aware record already carries ``line_count`` as the length of
    ## its per-line table, so setting both would declare the same field
    ## twice under two layouts.  ``writeMetaDat`` refuses the combination.
    ##
    ## **This bit is NOT additive at the reader**, for the same reason
    ## bit 13 is not: ``KnownFlags`` + ``readMetaDat`` reject any
    ## container carrying a bit outside the known mask, so a reader built
    ## before this constant existed refuses a count-bearing container
    ## outright.  The rollout consequence is the one recorded on bit 13 —
    ## reader support (this constant, in ``KnownFlags``) must ship
    ## everywhere BEFORE any writer sets the bit.  Accordingly no
    ## recorder sets it by default: ``enableLineCountTable`` is an
    ## explicit opt-in and a container without it is byte-identical to
    ## what the writer produced before this bit existed.
    ##
    ## Bit 15 is the last unallocated bit of the ``u16``, and there are
    ## more queued consumers than that.  Whether it becomes an "extended
    ## flag word follows" escape or the field is widened by a meta.dat
    ## version bump is a format decision that needs its own milestone,
    ## with reader support landed first.  See CTFS-Binary-Format.md
    ## §"Flag-space exhaustion".
    ##
    ## Must match ``codetracer_trace_writer::meta_dat::FLAG_HAS_LINE_COUNT_TABLE``
    ## (Rust) and the db-backend ``FLAG_HAS_LINE_COUNT_TABLE`` bit 14.
  FlagHasCallStream*: uint16 = 0x100             # bit 8 — M17a
    ## When set, the materialized `.ct` carries a dedicated call stream
    ## (`calls.dat` + its companion seekable index `calls.idx`) in
    ## addition to the unified event stream.  The split lets a reader
    ## load the call tree independently / on-demand without scanning the
    ## step+value stream (trace-events.md §"Call Stream (`calls.dat`)").
    ## This is ADDITIVE: the unified stream is unchanged, and a reader
    ## that does not know the bit simply ignores `calls.dat`/`calls.idx`.
    ## The flag is the M17a gate; the dedicated db-backend seekable
    ## reader that consumes `calls.dat` lands in M17b.  See
    ## ``codetracer-trace-format-spec/internal-files.md`` §"Metadata
    ## (meta.dat)" and §"Runtime Tracing (DB Traces)".

  KnownFlags*: uint16 = (
    FlagHasMcrFields or
    FlagHasReplayLaunchFields or
    FlagHasLayoutSnapshot or
    FlagHasTraceFilterProvenance or
    FlagHasColumnAwareSteps or
    FlagHasAlternateSourceViews or
    FlagSupportsColumnBreakpoints or
    FlagSupportsColumnMotions or
    FlagHasCallStream or
    FlagHasStepStream or
    FlagHasValueStream or
    FlagHasIoEventStream or
    FlagHasInterningTables or
    FlagHasSpanStream or
    FlagHasLineCountTable)
    ## P6.5 (column-extension back-compat): every flag bit this reader
    ## understands.  ``readMetaDat`` rejects any meta.dat whose flag
    ## word has bits outside this mask set, per
    ## ``codetracer-trace-format-spec/internal-files.md``
    ## §"Metadata (meta.dat)" ("bits 4-15 reserved; readers reject when
    ## set" — generalised here to "all unknown bits reject").  This is
    ## the contract the column extension (and every future flag-bit
    ## extension) relies on: when a future writer sets a bit this
    ## reader has not learned about, the reader refuses to open the
    ## trace cleanly rather than silently misdecoding downstream
    ## streams (e.g. the column-aware step stream).

type
  MetaDatContents* = object
    version*: uint16
    recordingId*: string
      ## M-REC-1: UUIDv7 identifying this recording.  Required in v3+.
    program*: string
    workdir*: string
    args*: seq[string]
    recorderId*: string
    paths*: seq[string]
    mcrFields*: Option[McrMetaFields]
    replayLaunchFields*: Option[ReplayLaunchFields]
    layoutSnapshotFields*: Option[LayoutSnapshotFields]
    filterProvenance*: seq[FilterProvenance]
      ## TF-M7: trace-filter chain entries.  Empty when the writer did
      ## not record provenance (the flag bit is clear) AND when the
      ## writer recorded a deliberately-empty chain (the flag bit is
      ## set with `trace_filter_count = 0`).  Use `hasFilterProvenance`
      ## to distinguish the two cases.
    hasFilterProvenance*: bool
      ## True iff FlagHasTraceFilterProvenance was set on the meta.dat
      ## header.  Distinguishes "no provenance recorded" (false) from
      ## "provenance recorded but empty" (true with empty
      ## `filterProvenance`).
    hasColumnAwareSteps*: bool
      ## True iff FlagHasColumnAwareSteps was set on the meta.dat header.
      ## Readers must surface column data from sekDeltaColumn / column-aware
      ## global_position_index only when this is set.  Pre-extension
      ## traces always have it clear and readers must surface columns as
      ## ``None``.
    hasAlternateSourceViews*: bool
      ## True iff FlagHasAlternateSourceViews was set on the meta.dat
      ## header.  When set, the trace carries ``source_views.dat`` /
      ## ``source_views.off`` records (formatted views of minified
      ## sources for the replay-server's deminification path).  Pre-
      ## extension traces always have it clear; readers should not look
      ## for the source_views files when this is false.
    supportsColumnBreakpoints*: bool
      ## True iff FlagSupportsColumnBreakpoints was set on the meta.dat
      ## header.  GUI consumers gate per-column breakpoint affordances
      ## (M6 Alt+click) on this; legacy / non-statement-precise
      ## recorders surface the bit as false and the GUI falls back to
      ## line-only breakpoints.
    supportsColumnMotions*: bool
      ## True iff FlagSupportsColumnMotions was set on the meta.dat
      ## header.  GUI consumers gate per-column step-over / step-in /
      ## step-out affordances on this; clear means the recorder's step
      ## predicate is line-granular and only line-only motions are
      ## meaningful.
    hasCallStream*: bool
      ## M17a: True iff FlagHasCallStream was set on the meta.dat header.
      ## When set, the trace carries a dedicated `calls.dat` call stream
      ## (plus its companion `calls.idx`) alongside the unified event
      ## stream, and readers may load the call tree from it directly.
      ## Pre-extension traces always have it clear; the unified-stream
      ## call tree remains the source of truth when it is clear.
    hasStepStream*: bool
      ## M23a: True iff FlagHasStepStream was set on the meta.dat header.
      ## When set, the trace carries a dedicated `steps.dat` compact
      ## execution stream (plus its companion `steps.idx`) alongside the
      ## unified event stream, and readers may load the step timeline from
      ## it directly.  Pre-extension traces always have it clear; the
      ## unified-stream step sequence remains the source of truth when it
      ## is clear.
    hasValueStream*: bool
      ## M23b: True iff FlagHasValueStream was set on the meta.dat header.
      ## When set, the trace carries a dedicated `values.dat` parallel
      ## value stream (plus its companion `values.idx`) alongside the
      ## unified event stream, parallel-indexed to the execution stream
      ## (value record N ↔ step N), and readers may load a step's variable
      ## values from it directly.  Pre-extension traces always have it
      ## clear; the unified-stream value events remain the source of truth
      ## when it is clear.
    hasIoEventStream*: bool
      ## M23c: True iff FlagHasIoEventStream was set on the meta.dat header.
      ## When set, the trace carries a dedicated `events.dat` I/O event
      ## stream (plus its companion `events.idx`) alongside the unified
      ## event stream, holding the ``EventLogKind``-tagged I/O / log events
      ## (each record: kind / step_id / metadata / content), and the
      ## event-log pane may paginate it directly.  NOTE: `events.dat` is
      ## DISTINCT from the legacy combined `events.log`.  Pre-extension
      ## traces always have it clear; the unified-stream `Event` records
      ## remain the source of truth when it is clear.
    hasInterningTables*: bool
      ## M23d: True iff FlagHasInterningTables was set on the meta.dat
      ## header.  When set, the trace carries the binary varint interning
      ## tables (`paths.dat`+`paths.off`, `funcs.dat`+`funcs.off`,
      ## `types.dat`+`types.off`, `varnames.dat`+`varnames.off`) alongside
      ## the legacy `events.log` / `paths.json` interning, resolvable by id
      ## with O(1) random access via the `.off` offset indices.  Pre-
      ## extension traces always have it clear; the legacy interning remains
      ## the source of truth when it is clear.
    hasSpanStream*: bool
      ## RS-M1: True iff FlagHasSpanStream was set on the meta.dat header.
      ## When set, the trace carries `spans.dat` + `spans.idx` (interval
      ## records for every `span_type` — web requests, processes, tests) plus
      ## the `spantype.ns` span-type index, and readers may enumerate spans
      ## without the retired JSONL sidecars.  Clear means the container has no
      ## span streams and a span reader must report zero spans (it must not go
      ## looking for the files).  NOTE: unlike bits 8-12, this bit is not
      ## additive for readers that predate it — see `FlagHasSpanStream`.
    hasLineCountTable*: bool
      ## True iff FlagHasLineCountTable was set on the meta.dat header.
      ## When set, every `paths.dat` record carries the file's line count
      ## after the path bytes and the line-only global position space is
      ## laid out from those counts.  Clear means the container states no
      ## per-file size and every file occupies `DefaultLinesPerFile`
      ## addresses by convention.  Like bit 13 this bit is not additive
      ## for readers that predate it — see `FlagHasLineCountTable`.

proc writeRawBytes(
    c: var Ctfs, f: var CtfsInternalFile,
    data: openArray[byte]): Result[void, string] =
  c.writeToFile(f, data)

proc writeU16LE(
    c: var Ctfs, f: var CtfsInternalFile,
    val: uint16): Result[void, string] =
  let bytes = [byte(val and 0xFF), byte((val shr 8) and 0xFF)]
  c.writeToFile(f, bytes)

proc writeVarint(
    c: var Ctfs, f: var CtfsInternalFile,
    val: uint64): Result[void, string] =
  var buf: seq[byte]
  encodeVarint(val, buf)
  c.writeToFile(f, buf)

proc writeVarintString(
    c: var Ctfs, f: var CtfsInternalFile,
    s: string): Result[void, string] =
  ? c.writeVarint(f, uint64(s.len))
  if s.len > 0:
    let bytes = cast[seq[byte]](s)
    ? c.writeToFile(f, bytes)
  ok()

proc writeMetaDat*(
    c: var Ctfs, f: var CtfsInternalFile,
    meta: TraceMetadata,
    paths: openArray[string],
    recorderId: string = "",
    mcrFields: Option[McrMetaFields] = none(McrMetaFields),
    replayLaunchFields: Option[ReplayLaunchFields] =
      none(ReplayLaunchFields),
    layoutSnapshotFields: Option[LayoutSnapshotFields] =
      none(LayoutSnapshotFields),
    filterProvenance: openArray[FilterProvenance] = [],
    emitFilterProvenance: bool = false,
    columnAwareSteps: bool = false,
    alternateSourceViews: bool = false,
    supportsColumnBreakpoints: bool = false,
    supportsColumnMotions: bool = false,
    hasCallStream: bool = false,
    hasStepStream: bool = false,
    hasValueStream: bool = false,
    hasIoEventStream: bool = false,
    hasInterningTables: bool = false,
    hasSpanStream: bool = false,
    hasLineCountTable: bool = false,
): Result[void, string] =
  ## Write binary meta.dat to a CTFS internal file.
  ##
  ## `filterProvenance` records the active trace-filter chain (TF-M7,
  ## spec § 7).  The flag bit is set whenever `emitFilterProvenance` is
  ## true OR `filterProvenance.len > 0`; an explicit
  ## `emitFilterProvenance = true` with an empty sequence is the spec's
  ## "recorder implements filters but the chain is empty" signal.

  # Recording id must be present and syntactically valid.  Pre-1.0
  # the spec forbids backcompat: a missing or malformed id is a write
  # error here so that no caller can accidentally produce a v3 trace
  # without the M-REC-1 spine.
  ? validateRecordingIdStr(meta.recordingId)

  # Magic
  ? c.writeRawBytes(f, MetaDatMagic)

  # Version
  ? c.writeU16LE(f, MetaDatVersion)

  # Flags
  var flags: uint16 = 0
  if mcrFields.isSome:
    flags = flags or FlagHasMcrFields
  if replayLaunchFields.isSome:
    flags = flags or FlagHasReplayLaunchFields
  if layoutSnapshotFields.isSome:
    flags = flags or FlagHasLayoutSnapshot
  let emitProvenance = emitFilterProvenance or filterProvenance.len > 0
  if emitProvenance:
    flags = flags or FlagHasTraceFilterProvenance
  if columnAwareSteps:
    flags = flags or FlagHasColumnAwareSteps
  if alternateSourceViews:
    flags = flags or FlagHasAlternateSourceViews
  # Capability bits only make sense on top of the wire-format bit;
  # silently dropping them when columnAwareSteps is false would be a
  # misleading round-trip.  Surface the contract explicitly so an
  # accidental misuse fails the write rather than producing a header
  # that the reader's invariant check will later reject.
  if (supportsColumnBreakpoints or supportsColumnMotions) and
      not columnAwareSteps:
    return err(
      "meta.dat: capability flags (column breakpoints / motions) " &
      "require columnAwareSteps to be enabled")
  if supportsColumnBreakpoints:
    flags = flags or FlagSupportsColumnBreakpoints
  if supportsColumnMotions:
    flags = flags or FlagSupportsColumnMotions
  if hasCallStream:
    flags = flags or FlagHasCallStream
  if hasStepStream:
    flags = flags or FlagHasStepStream
  if hasValueStream:
    flags = flags or FlagHasValueStream
  if hasIoEventStream:
    flags = flags or FlagHasIoEventStream
  if hasInterningTables:
    flags = flags or FlagHasInterningTables
  if hasSpanStream:
    flags = flags or FlagHasSpanStream
  # A column-aware paths.dat record already carries `line_count` as the
  # length of its per-line table, so bit 14 on top of bit 4 would declare
  # the same field twice under two incompatible record layouts and leave
  # the reader to pick one.  Refuse rather than write a header no reader
  # can interpret unambiguously.
  if hasLineCountTable and columnAwareSteps:
    return err(
      "meta.dat: hasLineCountTable and columnAwareSteps are mutually " &
      "exclusive — a Layout A record already carries the file's " &
      "line_count as the length of its per-line table")
  if hasLineCountTable:
    flags = flags or FlagHasLineCountTable
  ? c.writeU16LE(f, flags)

  # Recording id (UUIDv7, canonical 36-char form).  M-REC-1.
  ? c.writeVarintString(f, meta.recordingId)

  # Program
  ? c.writeVarintString(f, meta.program)

  # Args
  ? c.writeVarint(f, uint64(meta.args.len))
  for arg in meta.args:
    ? c.writeVarintString(f, arg)

  # Workdir
  ? c.writeVarintString(f, meta.workdir)

  # Recorder ID
  ? c.writeVarintString(f, recorderId)

  # Paths
  ? c.writeVarint(f, uint64(paths.len))
  for p in paths:
    ? c.writeVarintString(f, p)

  # MCR fields
  if mcrFields.isSome:
    let mcr = mcrFields.get()
    ? c.writeVarint(f, uint64(ord(mcr.tickSource)))
    ? c.writeVarint(f, uint64(mcr.totalThreads))
    ? c.writeVarint(f, uint64(ord(mcr.atomicMode)))
    ? c.writeVarint(f, mcr.totalEvents)
    ? c.writeVarint(f, uint64(mcr.totalCheckpoints))
    ? c.writeVarint(f, mcr.startTimeUnixUs)
    ? c.writeVarintString(f, mcr.platform)
    ? c.writeVarintString(f, mcr.tickGranularity)
    ? c.writeVarintString(f, mcr.tickSourceStr)
    ? c.writeVarintString(f, mcr.atomicModeStr)
    ? c.writeVarintString(f, mcr.startTimeStr)
    ? c.writeVarintString(f, mcr.hookProfile)
    ? c.writeVarint(f, uint64(mcr.hookStrategies.len))
    for s in mcr.hookStrategies:
      ? c.writeVarintString(f, s)

  # Replay-launch fields (M-RLP-1, spec §6A.5).  One u8 flag.
  if replayLaunchFields.isSome:
    let rl = replayLaunchFields.get()
    let aslrByte: array[1, byte] = [byte(if rl.aslrDisabled: 1 else: 0)]
    ? c.writeRawBytes(f, aslrByte)

  # Layout snapshot (M-RLP-2, spec §6B.7).  u64 hash, varint len, bytes.
  if layoutSnapshotFields.isSome:
    let ls = layoutSnapshotFields.get()
    var hashBytes: array[8, byte]
    let h = ls.layoutHash
    for i in 0 ..< 8:
      hashBytes[i] = byte((h shr (i * 8)) and 0xFF'u64)
    ? c.writeRawBytes(f, hashBytes)
    ? c.writeVarint(f, uint64(ls.layoutFingerprint.len))
    if ls.layoutFingerprint.len > 0:
      ? c.writeRawBytes(f, ls.layoutFingerprint)

  # Trace filter provenance (TF-M7, spec §7).  varint count, then for
  # each entry: (varint-length path string, 32 raw sha256 bytes).
  if emitProvenance:
    ? c.writeVarint(f, uint64(filterProvenance.len))
    for entry in filterProvenance:
      ? c.writeVarintString(f, entry.path)
      var shaBytes = newSeq[byte](32)
      for i in 0 ..< 32:
        shaBytes[i] = entry.sha256[i]
      ? c.writeRawBytes(f, shaBytes)

  ok()

# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

proc readU16LE(data: openArray[byte], offset: int): uint16 =
  uint16(data[offset]) or (uint16(data[offset + 1]) shl 8)

proc readString(data: openArray[byte], pos: var int): Result[string, string] =
  let lenVal = ? decodeVarint(data, pos)
  let sLen = int(lenVal)
  if pos + sLen > data.len:
    return err("meta.dat: string extends past end of data")
  var s = newString(sLen)
  for i in 0 ..< sLen:
    s[i] = char(data[pos + i])
  pos += sLen
  ok(s)

proc readMetaDat*(data: openArray[byte]): Result[MetaDatContents, string] =
  ## Parse binary meta.dat from raw bytes.
  ## Validates magic and version, returns MetaDatContents or an error.
  if data.len < 8:
    return err("meta.dat too short: need at least 8 bytes, got " & $data.len)

  # Check magic
  if data[0] != MetaDatMagic[0] or data[1] != MetaDatMagic[1] or
      data[2] != MetaDatMagic[2] or data[3] != MetaDatMagic[3]:
    return err("meta.dat: bad magic bytes")

  let version = readU16LE(data, 4)
  if version <= LastShiftedGlobalIndexVersion:
    # Refused by name, not by the generic version mismatch below, because
    # the consequence of reading one anyway is not a parse failure — it is
    # a plausible wrong answer at every step.  See the v4 note in the
    # version history above.
    # Phrased about the WRITER, not about this container's contents: the
    # gate is on the schema version, so it also refuses a v3 container that
    # holds no steps at all (a ct-mcr recording, say). Saying "its steps
    # were packed as" would be a claim about such a trace that is not true.
    return err("meta.dat: schema version " & $version &
      " predates the global line index correction, and this trace cannot " &
      "be read. Writers at that version packed a line-only step position " &
      "as prefixSum[path_id] + line; version " & $MetaDatVersion &
      " packs prefixSum[path_id] + (line - 1). Both land inside the " &
      "trace's address space, so a step read under the current decode " &
      "would come back one line high rather than fail, and the container " &
      "records nothing else that tells the two apart. Re-record the trace " &
      "with a current recorder. Spec: " &
      "codetracer-trace-format-spec/internal-files.md \"Global Line Index\"")
  if version != MetaDatVersion:
    return err("meta.dat: unsupported version " & $version & ", expected " & $MetaDatVersion)

  let flags = readU16LE(data, 6)

  # P6.5: strict back-compat rejection.  Any flag bit outside this
  # reader's ``KnownFlags`` set causes the open to fail cleanly rather
  # than silently misdecoding downstream streams.  This is the
  # mechanism that lets the column extension's wire-format break (tag
  # 0x07 in the step stream when bit 4 is set) be safely additive: an
  # older reader compiled without bit 4 in its ``KnownFlags`` mask
  # rejects column-aware traces at meta-parse time, before any step
  # stream is touched.  See spec
  # `codetracer-trace-format-spec/internal-files.md`
  # §"Metadata (meta.dat)" and `trace-events.md`
  # §"Reader Behaviour and Back-Compat".
  let unknownBits = flags and (not KnownFlags)
  if unknownBits != 0:
    return err("meta.dat: unknown flag bits set: 0x" &
      toHex(unknownBits.BiggestInt, 4))

  # Two KNOWN bits that cannot both be honoured. Each selects a paths.dat
  # record layout and a record is in one or the other; a column-aware
  # record already carries the file's line_count as the length of its
  # per-line table. Refused rather than resolved by preference, because
  # the wrong choice is not a parse failure downstream — it is a path
  # string with its own framing inside it and a per-file size that was
  # never written. `writeMetaDat` refuses to produce such a header, so
  # this catches one from another producer.
  if (flags and FlagHasColumnAwareSteps) != 0 and
     (flags and FlagHasLineCountTable) != 0:
    return err("meta.dat: flags 0x" & toHex(flags.BiggestInt, 4) &
      " set both FlagHasColumnAwareSteps (bit 4) and FlagHasLineCountTable " &
      "(bit 14). Each selects a paths.dat record layout and a record is in " &
      "one or the other; a column-aware record already carries the file's " &
      "line_count as the length of its per-line table. Re-record the trace " &
      "with a current recorder")

  var pos = 8

  var contents = MetaDatContents(version: version)
  contents.hasColumnAwareSteps = (flags and FlagHasColumnAwareSteps) != 0
  contents.hasAlternateSourceViews =
    (flags and FlagHasAlternateSourceViews) != 0
  contents.supportsColumnBreakpoints =
    (flags and FlagSupportsColumnBreakpoints) != 0
  contents.supportsColumnMotions =
    (flags and FlagSupportsColumnMotions) != 0
  contents.hasCallStream = (flags and FlagHasCallStream) != 0
  contents.hasStepStream = (flags and FlagHasStepStream) != 0
  contents.hasValueStream = (flags and FlagHasValueStream) != 0
  contents.hasIoEventStream = (flags and FlagHasIoEventStream) != 0
  contents.hasInterningTables = (flags and FlagHasInterningTables) != 0
  contents.hasSpanStream = (flags and FlagHasSpanStream) != 0
  contents.hasLineCountTable = (flags and FlagHasLineCountTable) != 0

  # Recording id (UUIDv7, canonical 36-char form).  M-REC-1, required
  # in v3+: a malformed or missing id rejects the trace at parse time.
  contents.recordingId = ? readString(data, pos)
  ? validateRecordingIdStr(contents.recordingId)

  # Program
  contents.program = ? readString(data, pos)

  # Args
  let argsCount = ? decodeVarint(data, pos)
  for i in 0'u64 ..< argsCount:
    contents.args.add(? readString(data, pos))

  # Workdir
  contents.workdir = ? readString(data, pos)

  # Recorder ID
  contents.recorderId = ? readString(data, pos)

  # Paths
  let pathsCount = ? decodeVarint(data, pos)
  for i in 0'u64 ..< pathsCount:
    contents.paths.add(? readString(data, pos))

  # MCR fields
  if (flags and FlagHasMcrFields) != 0:
    let tickSourceVal = ? decodeVarint(data, pos)
    let totalThreadsVal = ? decodeVarint(data, pos)
    let atomicModeVal = ? decodeVarint(data, pos)

    if tickSourceVal > uint64(high(TickSource).ord):
      return err("meta.dat: invalid tick_source value " & $tickSourceVal)
    if atomicModeVal > uint64(high(AtomicMode).ord):
      return err("meta.dat: invalid atomic_mode value " & $atomicModeVal)

    let totalEventsVal = ? decodeVarint(data, pos)
    let totalCheckpointsVal = ? decodeVarint(data, pos)
    let startTimeUnixUsVal = ? decodeVarint(data, pos)
    let platformStr = ? readString(data, pos)
    let tickGranularityStr = ? readString(data, pos)
    let tickSourceStr = ? readString(data, pos)
    let atomicModeStr = ? readString(data, pos)
    let startTimeStr = ? readString(data, pos)
    let hookProfileStr = ? readString(data, pos)
    let hookStrategiesCount = ? decodeVarint(data, pos)
    var hookStrategies: seq[string] = @[]
    for i in 0'u64 ..< hookStrategiesCount:
      hookStrategies.add(? readString(data, pos))

    contents.mcrFields = some(McrMetaFields(
      tickSource: TickSource(tickSourceVal),
      totalThreads: uint32(totalThreadsVal),
      atomicMode: AtomicMode(atomicModeVal),
      totalEvents: totalEventsVal,
      totalCheckpoints: uint32(totalCheckpointsVal),
      startTimeUnixUs: startTimeUnixUsVal,
      platform: platformStr,
      tickGranularity: tickGranularityStr,
      tickSourceStr: tickSourceStr,
      atomicModeStr: atomicModeStr,
      startTimeStr: startTimeStr,
      hookProfile: hookProfileStr,
      hookStrategies: hookStrategies,
    ))

  # Replay-launch fields (M-RLP-1, spec §6A.5).
  if (flags and FlagHasReplayLaunchFields) != 0:
    if pos + 1 > data.len:
      return err("meta.dat: replay_launch_fields aslr_disabled byte missing")
    let aslr = data[pos] != 0
    pos += 1
    contents.replayLaunchFields = some(ReplayLaunchFields(
      aslrDisabled: aslr,
    ))

  # Layout snapshot (M-RLP-2, spec §6B.7).
  if (flags and FlagHasLayoutSnapshot) != 0:
    if pos + 8 > data.len:
      return err("meta.dat: layout_snapshot hash bytes missing")
    var h: uint64 = 0
    for i in 0 ..< 8:
      h = h or (uint64(data[pos + i]) shl (i * 8))
    pos += 8
    let fpLen = ? decodeVarint(data, pos)
    if pos + int(fpLen) > data.len:
      return err("meta.dat: layout_snapshot fingerprint extends past end")
    var fp = newSeq[byte](int(fpLen))
    for i in 0 ..< int(fpLen):
      fp[i] = data[pos + i]
    pos += int(fpLen)
    contents.layoutSnapshotFields = some(LayoutSnapshotFields(
      layoutHash: h,
      layoutFingerprint: fp,
    ))

  # Trace filter provenance (TF-M7, spec §7).
  if (flags and FlagHasTraceFilterProvenance) != 0:
    contents.hasFilterProvenance = true
    let countVal = ? decodeVarint(data, pos)
    for i in 0'u64 ..< countVal:
      let path = ? readString(data, pos)
      if pos + 32 > data.len:
        return err("meta.dat: trace_filter sha256 bytes extend past end")
      var sha: array[32, byte]
      for k in 0 ..< 32:
        sha[k] = data[pos + k]
      pos += 32
      contents.filterProvenance.add(FilterProvenance(path: path, sha256: sha))

  ok(contents)

# ---------------------------------------------------------------------------
# Buffer-based writer (for FFI / standalone use)
# ---------------------------------------------------------------------------

proc appendU16LE(buf: var seq[byte], val: uint16) =
  buf.add(byte(val and 0xFF))
  buf.add(byte((val shr 8) and 0xFF))

proc appendVarintStr(buf: var seq[byte], s: string) =
  encodeVarint(uint64(s.len), buf)
  for i in 0 ..< s.len:
    buf.add(byte(s[i]))

proc writeMetaDatToBuffer*(
    meta: TraceMetadata,
    paths: openArray[string],
    recorderId: string = "",
    mcrFields: Option[McrMetaFields] = none(McrMetaFields),
    replayLaunchFields: Option[ReplayLaunchFields] =
      none(ReplayLaunchFields),
    layoutSnapshotFields: Option[LayoutSnapshotFields] =
      none(LayoutSnapshotFields),
    filterProvenance: openArray[FilterProvenance] = [],
    emitFilterProvenance: bool = false,
    columnAwareSteps: bool = false,
    alternateSourceViews: bool = false,
    supportsColumnBreakpoints: bool = false,
    supportsColumnMotions: bool = false,
    hasCallStream: bool = false,
    hasStepStream: bool = false,
    hasValueStream: bool = false,
    hasIoEventStream: bool = false,
    hasInterningTables: bool = false,
    hasSpanStream: bool = false,
    hasLineCountTable: bool = false,
): seq[byte] =
  ## Serialize meta.dat to an in-memory byte buffer.
  ## This is the same format as writeMetaDat but without needing a CTFS container.
  ##
  ## A malformed `meta.recordingId` aborts via `doAssert`.  Callers
  ## must pass a syntactically valid UUIDv7 (M-REC-1, spec §3); this
  ## proc has no `Result` return type so we cannot surface a recoverable
  ## error.  Use `writeMetaDat` (CTFS-based) when you need that.
  doAssert validateRecordingIdStr(meta.recordingId).isOk,
    "writeMetaDatToBuffer: meta.recordingId is not a canonical UUIDv7"

  result = newSeq[byte]()

  # Magic
  for b in MetaDatMagic:
    result.add(b)

  # Version
  result.appendU16LE(MetaDatVersion)

  # Flags
  var flags: uint16 = 0
  if mcrFields.isSome:
    flags = flags or FlagHasMcrFields
  if replayLaunchFields.isSome:
    flags = flags or FlagHasReplayLaunchFields
  if layoutSnapshotFields.isSome:
    flags = flags or FlagHasLayoutSnapshot
  let emitProvenance = emitFilterProvenance or filterProvenance.len > 0
  if emitProvenance:
    flags = flags or FlagHasTraceFilterProvenance
  if columnAwareSteps:
    flags = flags or FlagHasColumnAwareSteps
  if alternateSourceViews:
    flags = flags or FlagHasAlternateSourceViews
  # ``writeMetaDatToBuffer`` is the buffer-side mirror of
  # ``writeMetaDat`` and has no Result return type — surface the
  # capability/columnAwareSteps invariant via ``doAssert`` (already the
  # convention used for the recordingId validation above) so the test
  # suite catches the misuse loud and clear.
  doAssert (not supportsColumnBreakpoints and not supportsColumnMotions) or
      columnAwareSteps,
    "writeMetaDatToBuffer: capability flags require columnAwareSteps"
  if supportsColumnBreakpoints:
    flags = flags or FlagSupportsColumnBreakpoints
  if supportsColumnMotions:
    flags = flags or FlagSupportsColumnMotions
  if hasCallStream:
    flags = flags or FlagHasCallStream
  if hasStepStream:
    flags = flags or FlagHasStepStream
  if hasValueStream:
    flags = flags or FlagHasValueStream
  if hasIoEventStream:
    flags = flags or FlagHasIoEventStream
  if hasInterningTables:
    flags = flags or FlagHasInterningTables
  if hasSpanStream:
    flags = flags or FlagHasSpanStream
  # See writeMetaDat for why the two bits are mutually exclusive.  This
  # entry point has no Result return, so the contract is a doAssert.
  doAssert not (hasLineCountTable and columnAwareSteps),
    "writeMetaDatToBuffer: hasLineCountTable and columnAwareSteps are " &
    "mutually exclusive"
  if hasLineCountTable:
    flags = flags or FlagHasLineCountTable
  result.appendU16LE(flags)

  # Recording id (UUIDv7, canonical 36-char form).  M-REC-1.
  result.appendVarintStr(meta.recordingId)

  # Program
  result.appendVarintStr(meta.program)

  # Args
  encodeVarint(uint64(meta.args.len), result)
  for arg in meta.args:
    result.appendVarintStr(arg)

  # Workdir
  result.appendVarintStr(meta.workdir)

  # Recorder ID
  result.appendVarintStr(recorderId)

  # Paths
  encodeVarint(uint64(paths.len), result)
  for p in paths:
    result.appendVarintStr(p)

  # MCR fields
  if mcrFields.isSome:
    let mcr = mcrFields.get()
    encodeVarint(uint64(ord(mcr.tickSource)), result)
    encodeVarint(uint64(mcr.totalThreads), result)
    encodeVarint(uint64(ord(mcr.atomicMode)), result)
    encodeVarint(mcr.totalEvents, result)
    encodeVarint(uint64(mcr.totalCheckpoints), result)
    encodeVarint(mcr.startTimeUnixUs, result)
    result.appendVarintStr(mcr.platform)
    result.appendVarintStr(mcr.tickGranularity)
    result.appendVarintStr(mcr.tickSourceStr)
    result.appendVarintStr(mcr.atomicModeStr)
    result.appendVarintStr(mcr.startTimeStr)
    result.appendVarintStr(mcr.hookProfile)
    encodeVarint(uint64(mcr.hookStrategies.len), result)
    for s in mcr.hookStrategies:
      result.appendVarintStr(s)

  # Replay-launch fields (M-RLP-1, spec §6A.5).  One u8 flag.
  if replayLaunchFields.isSome:
    let rl = replayLaunchFields.get()
    result.add(byte(if rl.aslrDisabled: 1 else: 0))

  # Layout snapshot (M-RLP-2, spec §6B.7).  u64 hash + varint len + bytes.
  if layoutSnapshotFields.isSome:
    let ls = layoutSnapshotFields.get()
    let h = ls.layoutHash
    for i in 0 ..< 8:
      result.add(byte((h shr (i * 8)) and 0xFF'u64))
    encodeVarint(uint64(ls.layoutFingerprint.len), result)
    for b in ls.layoutFingerprint:
      result.add(b)

  # Trace filter provenance (TF-M7, spec §7).
  if emitProvenance:
    encodeVarint(uint64(filterProvenance.len), result)
    for entry in filterProvenance:
      result.appendVarintStr(entry.path)
      for i in 0 ..< 32:
        result.add(entry.sha256[i])
