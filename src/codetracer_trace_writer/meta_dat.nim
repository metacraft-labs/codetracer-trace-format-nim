{.push raises: [].}

## Binary meta.dat writer for CTFS trace metadata.
##
## Layout (version 2):
##   [4] magic "CTMD"
##   [2] version u16 LE
##   [2] flags u16 LE (bit 0: has_mcr_fields, bit 1: has_replay_launch_fields)
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
##
## Version history:
##   v1 — initial release (no hook fields).  Removed before any external
##        consumer shipped: F5a Phase A dual-wrote meta.json alongside
##        meta.dat, and the meta.json carried hookProfile/hookStrategies
##        until the schema gained them in v2.
##   v2 — appended hookProfile + hookStrategies inside the MCR-fields
##        block so meta.dat reaches parity with the legacy meta.json.
##        M-RLP-1 (2026-05-12) added FlagHasReplayLaunchFields (bit 1)
##        and a one-byte aslr_disabled block appended after the MCR
##        block; readers that don't know about the bit simply stop after
##        the MCR block, so this is a forward-compatible extension at
##        version 2 (no schema bump needed — the flag bit gates parsing).

import std/options
import results
import ../codetracer_trace_types
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ./varint

const
  MetaDatMagic*: array[4, byte] = [0x43'u8, 0x54, 0x4D, 0x44]  # "CTMD"
  MetaDatVersion*: uint16 = 2
  FlagHasMcrFields*: uint16 = 1            # bit 0
  FlagHasReplayLaunchFields*: uint16 = 2   # bit 1 — M-RLP-1 (spec §6A.5)

type
  MetaDatContents* = object
    version*: uint16
    program*: string
    workdir*: string
    args*: seq[string]
    recorderId*: string
    paths*: seq[string]
    mcrFields*: Option[McrMetaFields]
    replayLaunchFields*: Option[ReplayLaunchFields]

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
      none(ReplayLaunchFields)
): Result[void, string] =
  ## Write binary meta.dat to a CTFS internal file.

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
  ? c.writeU16LE(f, flags)

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
  var pos = 8

  var contents = MetaDatContents(version: version)

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
      none(ReplayLaunchFields)
): seq[byte] =
  ## Serialize meta.dat to an in-memory byte buffer.
  ## This is the same format as writeMetaDat but without needing a CTFS container.
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
  result.appendU16LE(flags)

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
