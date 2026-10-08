when defined(nimPreviewSlimSystem):
  import std/assertions  # doAssert is not in `system` under slim-system

{.push raises: [].}

## Binary meta.dat writer for CTFS trace metadata.
##
## Layout (version 6, `internal-files.md` §"Metadata (meta.dat)"):
##   [4] magic "CTMD"
##   [2] version u16 LE (6)
##   [2] flags u16 LE (bit 0: has_mcr_fields,
##                    bit 1: has_replay_launch_fields,
##                    bit 2: has_layout_snapshot,
##                    bit 3: has_trace_filter_provenance, ...)
##   [4] flags_ext u32 LE   -- always present (bit 0: has_source_reload)
##   varint-prefixed recording_id string  (M-REC-1; required, UUIDv7,
##                                         lowercase hyphenated 36-char form)
##   varint-prefixed program string
##   varint args_count, then varint-prefixed arg strings
##   varint-prefixed workdir string
##   varint-prefixed recorder_id string
##   (no path list: a trace's source paths are `paths.dat`'s records)
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
##        ``codetracer-trace-format-spec`` ``internal-files.md`` §"Global
##        Line Index" and ``trace-events.md`` §"Decoding
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
##   v5 — GDH-M2 (2026-09-10): inserts a ``[4] flags_ext u32 LE`` word
##        after the u16 flags, because bits 0..15 are all assigned and
##        no spare one is left to gate step-stream tag ``0x08``
##        (``TagSourceReload``).  **Written ONLY when an extended flag
##        is actually set** — a recording with no reload is still a v4
##        header, byte for byte.  ``readMetaDat`` accepts 4 and 5; a
##        reader that predates v5 refuses a v5 container by name at
##        metadata-parse time, which is the strict-rejection rollout
##        rule bits 13-15 record, obtained from the version field
##        instead of from a flag bit that does not exist.  See
##        ``MetaDatVersionExtendedFlags`` and
##        ``codetracer-specs/Planned-Features/GDScript-Hot-Reload-Multi-Version-Sources.md``
##        §6.3 / GDH-OQ-2.
##   v6 — (2026-10-01): the path list after ``recorder_id`` is gone —
##        ``paths.dat`` is the only list of source paths — and
##        ``flags_ext`` is always present, so there is one header length.
##        Readers refuse every other version, naming it: the bytes after
##        ``recorder_id`` mean something different in v5 and below.
##        ``meta.dat`` is written once, complete, at open; every flag is
##        fixed then, so ``FlagExtHasSourceReload`` is a capability the
##        recorder declares up front rather than a record of what happened.

import std/options
import std/strutils
import std/unicode
import results
import ../codetracer_trace_types
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ./varint
import ./uuid_v7

const
  MetaDatMagic*: array[4, byte] = [0x43'u8, 0x54, 0x4D, 0x44]  # "CTMD"
  MetaDatVersion*: uint16 = 6
    ## The only schema version this library writes or reads
    ## (`internal-files.md` §"Metadata (meta.dat)", "Version History": v6).
    ## Every other version is refused by name; pre-1.0 there is no
    ## compatibility path, and older containers are re-recorded.
  MetaDatHeaderSize* = 12
    ## magic (4) + version (2) + flags (2) + flags_ext (4). Version 6 always
    ## carries `flags_ext`, so this is the one header length.
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
    ## A stream-presence HINT, never a read gate (`internal-files.md`
    ## §"Stream-presence flags are a hint, not a gate"): a reader finds the
    ## span stream by the presence of `spans.dat`.  The split-stream writer
    ## never sets it — `meta.dat` is written at the first record, before any
    ## span — so `hasSpanStream` defaults to false on both `writeMetaDat` and
    ## `writeMetaDatToBuffer`.  A reader that predates this constant refuses a
    ## container whose flag word carries it (``KnownFlags`` + ``readMetaDat``).
    ##
    ## Bit 14 is ``FlagHasLineCountTable`` and bit 15 is
    ## ``FlagHasCorrelationIndex``, so the ``u16`` is now fully allocated and
    ## there are more queued consumers than bits.  Whether the field grows an
    ## "extended flag word follows" escape or is widened by a meta.dat version
    ## bump is a format decision that needs its own milestone, with reader
    ## support landed first.  See CTFS-Binary-Format.md
    ## §"Flag-space exhaustion".
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
    ## **This bit is NOT additive at the reader**: ``KnownFlags`` +
    ## ``readMetaDat`` reject any container carrying a bit outside the
    ## known mask, so a reader built before this constant existed refuses
    ## a count-bearing container outright.  The rollout consequence:
    ## reader support (this constant, in ``KnownFlags``) must ship
    ## everywhere BEFORE any writer sets the bit.  Accordingly no
    ## recorder sets it by default: ``enableLineCountTable`` is an
    ## explicit opt-in and a container without it is byte-identical to
    ## what the writer produced before this bit existed.
    ##
    ## Bit 15 is ``FlagHasCorrelationIndex``, which spends the last one.
    ## Whether the field grows an "extended flag word follows" escape or is
    ## widened by a meta.dat version bump is a format decision that needs
    ## its own milestone, with reader support landed first.  See
    ## CTFS-Binary-Format.md §"Flag-space exhaustion".
    ##
    ## Must match ``codetracer_trace_writer::meta_dat::FLAG_HAS_LINE_COUNT_TABLE``
    ## (Rust) and the db-backend ``FLAG_HAS_LINE_COUNT_TABLE`` bit 14.
  FlagHasCorrelationIndex*: uint16 = 0x8000      # bit 15 — WTCI
    ## When set, the container carries `corrmark.ns` — the record-time B-tree
    ## index of the distributed-trace spans and boundary crossings this
    ## recording covers — together with the `markers.dat` / `markers.off`
    ## interning table its boundary labels resolve through.  It rides on ONE
    ## bit rather than joining bit 12's set because the label table is
    ## meaningless without the index, and because bit 12's meaning is a
    ## settled three-way agreement describing four tables two of those readers
    ## have no use for.
    ##
    ## **A HINT, NOT A GATE, and not the authority.**  Whether a recording was
    ## indexed is answered by the presence of the `corrmark.ns` FILE ENTRY,
    ## which a consumer already parses to find anything at all.  That matters
    ## because the distinction the contract requires — "never indexed" versus
    ## "indexed and covering nothing" — is a statement about the recording
    ## rather than about any span, and a flag that could disagree with the
    ## entry array would make it ambiguous again.  See
    ## ``codetracer-specs/Testing/CTFS-Correlation-Marker-Contract.md`` §9.
    ##
    ## Recognising the bit is nonetheless load-bearing on the READ side:
    ## ``KnownFlags`` + ``readMetaDat`` refuse a container carrying any bit
    ## outside the known mask, so a reader without this constant rejects every
    ## marker-bearing recording outright instead of ignoring an index it has
    ## no use for.  Reader support therefore ships before the writer sets it.
    ##
    ## Must match ``codetracer_trace_writer::meta_dat::FLAG_HAS_CORRELATION_INDEX``
    ## (Rust) and the db-backend ``FLAG_HAS_CORRELATION_INDEX`` bit 15.
    ##
    ## Drafted against bit 14, which ``FlagHasLineCountTable`` took first.
    ## Both describe the container, so they could not share a bit; neither
    ## had shipped, so moving this one cost no compatibility.
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
    FlagHasLineCountTable or
    FlagHasCorrelationIndex)

  FlagExtHasSourceReload*: uint32 = 1          # ext bit 0 (global bit 16)
    ## A capability declared at open: the execution stream MAY contain
    ## step-event tag ``0x08`` (``TagSourceReload``). A trace that declared it
    ## and recorded no reload is well-formed; a writer refuses a reload in a
    ## trace that did not declare it, and a reader refuses tag 0x08 in a
    ## container that does not declare it (`internal-files.md` §"Extended
    ## flags (`flags_ext`)", `trace-events.md` §"Source Reload Marker").

  KnownExtFlags*: uint32 = FlagExtHasSourceReload
    ## Every ``flags_ext`` bit this reader understands.  ``readMetaDat``
    ## rejects a v5 header whose extended word has bits outside this
    ## mask, the same strict-rejection contract ``KnownFlags`` enforces
    ## for the u16.  31 bits remain, so the next flag after this one is
    ## an ordinary addition rather than another format decision.
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
    hasCorrelationIndex*: bool
      ## WTCI: True iff FlagHasCorrelationIndex (bit 15) was set.  A HINT that
      ## the container carries `corrmark.ns` + `markers.dat`/`.off`; the file
      ## entry, not this bit, is what a consumer must consult to tell "never
      ## indexed" from "indexed and covering nothing" (contract §9).
    hasSpanStream*: bool
      ## RS-M1: True iff FlagHasSpanStream was set on the meta.dat header —
      ## a hint that the trace carries `spans.dat`, never a read gate: a span
      ## reader looks for `spans.dat` whatever this says, since the writer
      ## leaves the bit clear (see `FlagHasSpanStream`).
    hasLineCountTable*: bool
      ## True iff FlagHasLineCountTable was set on the meta.dat header.
      ## When set, every `paths.dat` record carries the file's line count
      ## after the path bytes and the line-only global position space is
      ## laid out from those counts.  Clear means the container states no
      ## per-file size and every file occupies `DefaultLinesPerFile`
      ## addresses by convention.  Like bit 13 this bit is not additive
      ## for readers that predate it — see `FlagHasLineCountTable`.
    hasSourceReload*: bool
      ## GDH-M2: True iff the `flags_ext` word carries
      ## `FlagExtHasSourceReload`.  When set,
      ## the execution stream may contain step-event tag `0x08`
      ## (`TagSourceReload`) and a reader must pass
      ## `allowSourceReload = true` down to `decodeStepEvent`.  Clear
      ## means the tag must be REFUSED, not skipped: its record length
      ## is not recoverable without decoding it, so a skip re-reads the
      ## payload varints as further events and the stream decodes
      ## shorter and plausibly.
    flagsExt*: uint32
      ## The raw `flags_ext` word.  Surfaced so a consumer can report
      ## what a container DECLARED, not only what this reader knows how
      ## to act on.

proc writeRawBytes(buf: var seq[byte],
    data: openArray[byte]): Result[void, string] =
  buf.add(data)
  ok()

proc writeU16LE(buf: var seq[byte], val: uint16): Result[void, string] =
  buf.add([byte(val and 0xFF), byte((val shr 8) and 0xFF)])
  ok()

proc writeU32LE(buf: var seq[byte], val: uint32): Result[void, string] =
  buf.add([byte(val and 0xFF), byte((val shr 8) and 0xFF),
           byte((val shr 16) and 0xFF), byte((val shr 24) and 0xFF)])
  ok()

proc writeVarint(buf: var seq[byte], val: uint64): Result[void, string] =
  encodeVarint(val, buf)
  ok()

proc writeVarintString(buf: var seq[byte], s: string): Result[void, string] =
  ? buf.writeVarint(uint64(s.len))
  buf.add(s.toOpenArrayByte(0, s.high))
  ok()

type
  MetaDatFlagsInput* = object
    ## Every flag and flag-gated block of a `meta.dat`, as a writer decides it
    ## at open. Grouped so the CTFS-backed and the buffer-backed writer cannot
    ## drift apart: both go through `encodeMetaDat`.
    recorderId*: string
    mcrFields*: Option[McrMetaFields]
    replayLaunchFields*: Option[ReplayLaunchFields]
    layoutSnapshotFields*: Option[LayoutSnapshotFields]
    filterProvenance*: seq[FilterProvenance]
    emitFilterProvenance*: bool
    columnAwareSteps*: bool
    alternateSourceViews*: bool
    supportsColumnBreakpoints*: bool
    supportsColumnMotions*: bool
    hasCallStream*: bool
    hasStepStream*: bool
    hasValueStream*: bool
    hasIoEventStream*: bool
    hasInterningTables*: bool
    hasSpanStream*: bool
    hasLineCountTable*: bool
    hasCorrelationIndex*: bool
    hasSourceReload*: bool

proc metaTextRefusal*(s: string, what: string): string =
  ## Empty when `s` is UTF-8, as every string meta.dat carries is
  ## (`internal-files.md` §"Metadata (meta.dat)"); otherwise the refusal,
  ## naming the field.
  if validateUtf8(s) < 0: ""
  else: "meta.dat: " & what & " is not UTF-8"

template refuseNonUtf8(s: string, what: string) =
  block:
    let refusal = metaTextRefusal(s, what)
    if refusal.len > 0:
      return err(refusal)

proc encodeMetaDat*(meta: TraceMetadata,
    input: MetaDatFlagsInput): Result[seq[byte], string] =
  ## Serialize a version 6 `meta.dat`.
  ##
  ## `filterProvenance` records the active trace-filter chain (TF-M7,
  ## spec § 7).  The flag bit is set whenever `emitFilterProvenance` is
  ## true OR `filterProvenance.len > 0`; an explicit
  ## `emitFilterProvenance = true` with an empty sequence is the spec's
  ## "recorder implements filters but the chain is empty" signal.

  # Recording id must be present and syntactically valid (M-REC-1).
  ? validateRecordingIdStr(meta.recordingId)
  refuseNonUtf8(meta.program, "program")
  for arg in meta.args:
    refuseNonUtf8(arg, "an argument")
  refuseNonUtf8(meta.workdir, "workdir")
  refuseNonUtf8(input.recorderId, "recorder_id")
  if input.mcrFields.isSome:
    let m = input.mcrFields.get()
    for (s, what) in [(m.platform, "platform"),
        (m.tickGranularity, "tick_granularity"),
        (m.tickSourceStr, "tick_source_str"),
        (m.atomicModeStr, "atomic_mode_str"),
        (m.startTimeStr, "start_time_str"), (m.hookProfile, "hook_profile")]:
      refuseNonUtf8(s, what)
    for st in m.hookStrategies:
      refuseNonUtf8(st, "a hook strategy")
  for entry in input.filterProvenance:
    refuseNonUtf8(entry.path, "a filter-provenance path")

  var buf: seq[byte]
  ? buf.writeRawBytes(MetaDatMagic)
  ? buf.writeU16LE(MetaDatVersion)

  var extFlags: uint32 = 0
  if input.hasSourceReload:
    extFlags = extFlags or FlagExtHasSourceReload

  var flags: uint16 = 0
  if input.mcrFields.isSome:
    flags = flags or FlagHasMcrFields
  if input.replayLaunchFields.isSome:
    flags = flags or FlagHasReplayLaunchFields
  if input.layoutSnapshotFields.isSome:
    flags = flags or FlagHasLayoutSnapshot
  let emitProvenance = input.emitFilterProvenance or
    input.filterProvenance.len > 0
  if emitProvenance:
    flags = flags or FlagHasTraceFilterProvenance
  if input.columnAwareSteps:
    flags = flags or FlagHasColumnAwareSteps
  if input.alternateSourceViews:
    flags = flags or FlagHasAlternateSourceViews
  # Capability bits only make sense on top of the wire-format bit;
  # silently dropping them when columnAwareSteps is false would be a
  # misleading round-trip.
  if (input.supportsColumnBreakpoints or input.supportsColumnMotions) and
      not input.columnAwareSteps:
    return err(
      "meta.dat: capability flags (column breakpoints / motions) " &
      "require columnAwareSteps to be enabled")
  if input.supportsColumnBreakpoints:
    flags = flags or FlagSupportsColumnBreakpoints
  if input.supportsColumnMotions:
    flags = flags or FlagSupportsColumnMotions
  if input.hasCallStream:
    flags = flags or FlagHasCallStream
  if input.hasStepStream:
    flags = flags or FlagHasStepStream
  if input.hasValueStream:
    flags = flags or FlagHasValueStream
  if input.hasIoEventStream:
    flags = flags or FlagHasIoEventStream
  if input.hasInterningTables:
    flags = flags or FlagHasInterningTables
  if input.hasSpanStream:
    flags = flags or FlagHasSpanStream
  # A column-aware paths.dat record already carries `line_count` as the
  # length of its per-line table, so bit 14 on top of bit 4 would declare
  # the same field twice under two incompatible record layouts.
  if input.hasLineCountTable and input.columnAwareSteps:
    return err(
      "meta.dat: hasLineCountTable and columnAwareSteps are mutually " &
      "exclusive — a Layout A record already carries the file's " &
      "line_count as the length of its per-line table")
  if input.hasLineCountTable:
    flags = flags or FlagHasLineCountTable
  if input.hasCorrelationIndex:
    flags = flags or FlagHasCorrelationIndex
  ? buf.writeU16LE(flags)
  ? buf.writeU32LE(extFlags)

  ? buf.writeVarintString(meta.recordingId)
  ? buf.writeVarintString(meta.program)
  ? buf.writeVarint(uint64(meta.args.len))
  for arg in meta.args:
    ? buf.writeVarintString(arg)
  ? buf.writeVarintString(meta.workdir)
  ? buf.writeVarintString(input.recorderId)

  # MCR fields
  if input.mcrFields.isSome:
    let mcr = input.mcrFields.get()
    ? buf.writeVarint(uint64(ord(mcr.tickSource)))
    ? buf.writeVarint(uint64(mcr.totalThreads))
    ? buf.writeVarint(uint64(ord(mcr.atomicMode)))
    ? buf.writeVarint(mcr.totalEvents)
    ? buf.writeVarint(uint64(mcr.totalCheckpoints))
    ? buf.writeVarint(mcr.startTimeUnixUs)
    ? buf.writeVarintString(mcr.platform)
    ? buf.writeVarintString(mcr.tickGranularity)
    ? buf.writeVarintString(mcr.tickSourceStr)
    ? buf.writeVarintString(mcr.atomicModeStr)
    ? buf.writeVarintString(mcr.startTimeStr)
    ? buf.writeVarintString(mcr.hookProfile)
    ? buf.writeVarint(uint64(mcr.hookStrategies.len))
    for st in mcr.hookStrategies:
      ? buf.writeVarintString(st)

  # Replay-launch fields (M-RLP-1, spec §6A.5).  One u8 flag.
  if input.replayLaunchFields.isSome:
    let rl = input.replayLaunchFields.get()
    let aslrByte: array[1, byte] = [byte(if rl.aslrDisabled: 1 else: 0)]
    ? buf.writeRawBytes(aslrByte)

  # Layout snapshot (M-RLP-2, spec §6B.7).  u64 hash, varint len, bytes.
  if input.layoutSnapshotFields.isSome:
    let ls = input.layoutSnapshotFields.get()
    var hashBytes: array[8, byte]
    let h = ls.layoutHash
    for i in 0 ..< 8:
      hashBytes[i] = byte((h shr (i * 8)) and 0xFF'u64)
    ? buf.writeRawBytes(hashBytes)
    ? buf.writeVarint(uint64(ls.layoutFingerprint.len))
    if ls.layoutFingerprint.len > 0:
      ? buf.writeRawBytes(ls.layoutFingerprint)

  # Trace filter provenance (TF-M7, spec §7).  varint count, then for
  # each entry: (varint-length path string, 32 raw sha256 bytes).
  if emitProvenance:
    ? buf.writeVarint(uint64(input.filterProvenance.len))
    for entry in input.filterProvenance:
      ? buf.writeVarintString(entry.path)
      ? buf.writeRawBytes(entry.sha256)

  ok(buf)

proc writeMetaDat*(
    c: var Ctfs, f: var CtfsInternalFile,
    meta: TraceMetadata,
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
    hasCorrelationIndex: bool = false,
    hasSourceReload: bool = false,
): Result[void, string] =
  ## Write a version 6 `meta.dat` to a CTFS internal file, in one append.
  let buf = ? encodeMetaDat(meta, MetaDatFlagsInput(
    recorderId: recorderId, mcrFields: mcrFields,
    replayLaunchFields: replayLaunchFields,
    layoutSnapshotFields: layoutSnapshotFields,
    filterProvenance: @filterProvenance,
    emitFilterProvenance: emitFilterProvenance,
    columnAwareSteps: columnAwareSteps,
    alternateSourceViews: alternateSourceViews,
    supportsColumnBreakpoints: supportsColumnBreakpoints,
    supportsColumnMotions: supportsColumnMotions,
    hasCallStream: hasCallStream, hasStepStream: hasStepStream,
    hasValueStream: hasValueStream, hasIoEventStream: hasIoEventStream,
    hasInterningTables: hasInterningTables, hasSpanStream: hasSpanStream,
    hasLineCountTable: hasLineCountTable,
    hasCorrelationIndex: hasCorrelationIndex,
    hasSourceReload: hasSourceReload))
  ? c.writeToFile(f, buf)
  ok()

# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

proc readU16LE(data: openArray[byte], offset: int): uint16 =
  uint16(data[offset]) or (uint16(data[offset + 1]) shl 8)

template refuse(message: string) {.dirty.} =
  ## `return err(message)`, for the walkers below, which answer `bool` and
  ## name their refusal in `why`.
  why = message
  return false

proc readStringInto(data: openArray[byte], pos: var int, dest: var string,
    why: var string): bool =
  ## A varint-length-prefixed string at `pos`, into `dest`.
  let sLen = int(varintOrFail(data, pos, why))
  if sLen < 0 or sLen > data.len - pos:
    refuse("meta.dat: string extends past end of data")
  dest = newString(sLen)
  for i in 0 ..< sLen:
    dest[i] = char(data[pos + i])
  if validateUtf8(dest) >= 0:
    refuse("meta.dat: a string at byte " & $pos & " is not UTF-8")
  pos += sLen
  true

proc readBody(data: openArray[byte], flags: uint16, pos: var int,
    c: var MetaDatContents, why: var string): bool =
  ## Every field after the fixed header, in order, into `c`: the strings, then
  ## each flag-gated block the header declares. One walker answering `bool`
  ## rather than a `Result` per field, which costs a reader that ships to a
  ## browser several kilobytes of code for the same refusals.
  template str(dest: var string) =
    if not readStringInto(data, pos, dest, why): return false
  template varint(): uint64 = varintOrFail(data, pos, why)

  # Recording id (UUIDv7, canonical 36-char form).  M-REC-1, required
  # in v3+: a malformed or missing id rejects the trace at parse time.
  str(c.recordingId)
  let idCheck = validateRecordingIdStr(c.recordingId)
  if idCheck.isErr:
    refuse(idCheck.unsafeError)
  str(c.program)
  let argsCount = varint()
  for i in 0'u64 ..< argsCount:
    var a: string
    str(a)
    c.args.add(move a)
  str(c.workdir)
  str(c.recorderId)

  # MCR fields
  if (flags and FlagHasMcrFields) != 0:
    var m: McrMetaFields
    let tickSourceVal = varint()
    let totalThreadsVal = varint()
    let atomicModeVal = varint()
    if tickSourceVal > uint64(high(TickSource).ord):
      refuse("meta.dat: invalid tick_source value " & $tickSourceVal)
    if atomicModeVal > uint64(high(AtomicMode).ord):
      refuse("meta.dat: invalid atomic_mode value " & $atomicModeVal)
    m.tickSource = TickSource(tickSourceVal)
    m.atomicMode = AtomicMode(atomicModeVal)
    m.totalEvents = varint()
    let totalCheckpointsVal = varint()
    # The spec gives both counts as a varint with no narrower bound; the
    # fields are 32 bits, so a larger count is refused rather than cut to
    # its low half (the Rust reader's `decode_u32`, word for word).
    for (field, v) in [("total_threads", totalThreadsVal),
        ("total_checkpoints", totalCheckpointsVal)]:
      if v > uint64(high(uint32)):
        refuse("meta.dat: " & field & " value " & $v & " does not fit 32 bits")
    m.totalThreads = uint32(totalThreadsVal)
    m.totalCheckpoints = uint32(totalCheckpointsVal)
    m.startTimeUnixUs = varint()
    str(m.platform)
    str(m.tickGranularity)
    str(m.tickSourceStr)
    str(m.atomicModeStr)
    str(m.startTimeStr)
    str(m.hookProfile)
    let strategies = varint()
    for i in 0'u64 ..< strategies:
      var h: string
      str(h)
      m.hookStrategies.add(move h)
    c.mcrFields = some(move m)

  # Replay-launch fields (M-RLP-1, spec §6A.5).
  if (flags and FlagHasReplayLaunchFields) != 0:
    if pos + 1 > data.len:
      refuse("meta.dat: replay_launch_fields aslr_disabled byte missing")
    c.replayLaunchFields = some(ReplayLaunchFields(aslrDisabled: data[pos] != 0))
    pos += 1

  # Layout snapshot (M-RLP-2, spec §6B.7).
  if (flags and FlagHasLayoutSnapshot) != 0:
    if pos + 8 > data.len:
      refuse("meta.dat: layout_snapshot hash bytes missing")
    var l: LayoutSnapshotFields
    for i in 0 ..< 8:
      l.layoutHash = l.layoutHash or (uint64(data[pos + i]) shl (i * 8))
    pos += 8
    let fpLen = varint()
    if fpLen > uint64(data.len - pos):
      refuse("meta.dat: layout_snapshot fingerprint extends past end")
    l.layoutFingerprint = newSeq[byte](int(fpLen))
    for i in 0 ..< l.layoutFingerprint.len:
      l.layoutFingerprint[i] = data[pos + i]
    pos += int(fpLen)
    c.layoutSnapshotFields = some(move l)

  # Trace filter provenance (TF-M7, spec §7).
  if (flags and FlagHasTraceFilterProvenance) != 0:
    c.hasFilterProvenance = true
    let count = varint()
    for i in 0'u64 ..< count:
      var e: FilterProvenance
      str(e.path)
      if pos + 32 > data.len:
        refuse("meta.dat: trace_filter sha256 bytes extend past end")
      for k in 0 ..< 32:
        e.sha256[k] = data[pos + k]
      pos += 32
      c.filterProvenance.add(move e)
  true

proc readMetaDat*(data: openArray[byte]): Result[MetaDatContents, string] =
  ## Parse a version 6 `meta.dat`.
  ##
  ## Every other version is refused, naming it (`internal-files.md`
  ## §"Version History", v6): the bytes after `recorder_id` mean something
  ## different in version 5 and below — a path list — and a reader that
  ## guessed would read a path count as an MCR field. Versions 3 and below
  ## also packed line-only step positions one line higher than version 4 on.
  if data.len >= 4 and (data[0] != MetaDatMagic[0] or
      data[1] != MetaDatMagic[1] or data[2] != MetaDatMagic[2] or
      data[3] != MetaDatMagic[3]):
    return err("meta.dat: bad magic bytes")
  if data.len < MetaDatHeaderSize:
    return err("meta.dat too short: a version " & $MetaDatVersion &
      " header is " & $MetaDatHeaderSize & " bytes, got " & $data.len)

  let version = readU16LE(data, 4)
  if version != MetaDatVersion:
    return err("meta.dat: schema version " & $version & " is not supported; " &
      "this reader reads version " & $MetaDatVersion & " only. Re-record " &
      "the trace with a current recorder (internal-files.md \"Metadata " &
      "(meta.dat)\", Version History)")

  let flags = readU16LE(data, 6)

  let flagsExt = uint32(data[8]) or (uint32(data[9]) shl 8) or
    (uint32(data[10]) shl 16) or (uint32(data[11]) shl 24)
  let unknownExt = flagsExt and (not KnownExtFlags)
  if unknownExt != 0:
    return err("meta.dat: flags_ext carries bits this reader does not " &
      "implement: 0x" & toHex(BiggestInt(unknownExt), 8))

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

  result.ok(MetaDatContents(version: version, flagsExt: flagsExt,
    hasSourceReload: (flagsExt and FlagExtHasSourceReload) != 0,
    hasColumnAwareSteps: (flags and FlagHasColumnAwareSteps) != 0,
    hasCorrelationIndex: (flags and FlagHasCorrelationIndex) != 0,
    hasAlternateSourceViews: (flags and FlagHasAlternateSourceViews) != 0,
    supportsColumnBreakpoints: (flags and FlagSupportsColumnBreakpoints) != 0,
    supportsColumnMotions: (flags and FlagSupportsColumnMotions) != 0,
    hasCallStream: (flags and FlagHasCallStream) != 0,
    hasStepStream: (flags and FlagHasStepStream) != 0,
    hasValueStream: (flags and FlagHasValueStream) != 0,
    hasIoEventStream: (flags and FlagHasIoEventStream) != 0,
    hasInterningTables: (flags and FlagHasInterningTables) != 0,
    hasSpanStream: (flags and FlagHasSpanStream) != 0,
    hasLineCountTable: (flags and FlagHasLineCountTable) != 0))
  var pos = MetaDatHeaderSize
  var why: string
  if not readBody(data, flags, pos, result.unsafeGet(), why):
    result = err(why)

# ---------------------------------------------------------------------------
# Buffer-based writer (for FFI / standalone use)
# ---------------------------------------------------------------------------

proc appendU16LE(buf: var seq[byte], val: uint16) =
  buf.add(byte(val and 0xFF))
  buf.add(byte((val shr 8) and 0xFF))

proc appendU32LE(buf: var seq[byte], val: uint32) =
  buf.add(byte(val and 0xFF))
  buf.add(byte((val shr 8) and 0xFF))
  buf.add(byte((val shr 16) and 0xFF))
  buf.add(byte((val shr 24) and 0xFF))

proc appendVarintStr(buf: var seq[byte], s: string) =
  encodeVarint(uint64(s.len), buf)
  for i in 0 ..< s.len:
    buf.add(byte(s[i]))

proc writeMetaDatToBuffer*(
    meta: TraceMetadata,
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
    hasCorrelationIndex: bool = false,
    hasSourceReload: bool = false,
): seq[byte] =
  ## Serialize meta.dat to an in-memory byte buffer — the same bytes
  ## `writeMetaDat` writes, from the same `encodeMetaDat`.
  ##
  ## A malformed `meta.recordingId` or a contradictory flag set aborts via
  ## `doAssert`: this proc has no `Result` return type. Use `encodeMetaDat`
  ## when you need a recoverable error.
  let res = encodeMetaDat(meta, MetaDatFlagsInput(
    recorderId: recorderId, mcrFields: mcrFields,
    replayLaunchFields: replayLaunchFields,
    layoutSnapshotFields: layoutSnapshotFields,
    filterProvenance: @filterProvenance,
    emitFilterProvenance: emitFilterProvenance,
    columnAwareSteps: columnAwareSteps,
    alternateSourceViews: alternateSourceViews,
    supportsColumnBreakpoints: supportsColumnBreakpoints,
    supportsColumnMotions: supportsColumnMotions,
    hasCallStream: hasCallStream, hasStepStream: hasStepStream,
    hasValueStream: hasValueStream, hasIoEventStream: hasIoEventStream,
    hasInterningTables: hasInterningTables, hasSpanStream: hasSpanStream,
    hasLineCountTable: hasLineCountTable,
    hasCorrelationIndex: hasCorrelationIndex,
    hasSourceReload: hasSourceReload))
  doAssert res.isOk, "writeMetaDatToBuffer: " & (if res.isErr: res.unsafeError else: "")
  res.get()
