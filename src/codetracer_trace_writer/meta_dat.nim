{.push raises: [].}

## Binary meta.dat writer for CTFS trace metadata.
##
## Layout (version 3):
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
  MetaDatVersion*: uint16 = 3
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

  KnownFlags*: uint16 = (
    FlagHasMcrFields or
    FlagHasReplayLaunchFields or
    FlagHasLayoutSnapshot or
    FlagHasTraceFilterProvenance or
    FlagHasColumnAwareSteps or
    FlagHasAlternateSourceViews or
    FlagSupportsColumnBreakpoints or
    FlagSupportsColumnMotions)
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

  var pos = 8

  var contents = MetaDatContents(version: version)
  contents.hasColumnAwareSteps = (flags and FlagHasColumnAwareSteps) != 0
  contents.hasAlternateSourceViews =
    (flags and FlagHasAlternateSourceViews) != 0
  contents.supportsColumnBreakpoints =
    (flags and FlagSupportsColumnBreakpoints) != 0
  contents.supportsColumnMotions =
    (flags and FlagSupportsColumnMotions) != 0

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
