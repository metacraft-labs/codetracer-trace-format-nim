{.push raises: [].}

## Native MCR shard decoder for ct-print.
##
## The CodeTracer **native recorder** (`codetracer-native-recorder/ct_recorder/
## trace_writer.nim`) writes a *multi-thread CTFS shard* with a different
## internal layout from the regular v4 multi-stream traces this repo's
## `NewTraceReader` understands. Concretely, a native bundle contains:
##
##   - `meta.json`             — JSON header (program / args / recordingMode /
##                               platform / tickSource / hookProfile / ...)
##   - `tNNNNNNNNNNN`          — one per-thread event stream, each a sequence
##                               of `EventHeader (26 bytes)` + payload records;
##                               may be zstd-compressed (auto-detected).
##   - `event_log.dat` + `.idx`— chunk-indexed OS-event log with structured
##                               (geid, tick, tid, kind, fd, returnValue,
##                               metadata, content) entries.
##   - `paths.json`            — empty array placeholder (M1 writer)
##   - `cpidx.idx` + `cpdata.bin` (optional) — checkpoint records
##
## Without this decoder, `ct-print --full` on a native bundle silently produced
## an empty document with `-1` sentinel counts (the v4 reader simply did not
## find `meta.dat` / interning tables and returned a degenerate reader).
## That violates the user's "no silent skips" rule. This module decodes the
## native layout into the same JSON document shape used by `--full`/`--events`,
## with a `thread_id` field on each event so per-thread interleaving stays
## visible in the golden snapshot.
##
## Design notes:
## * We re-implement a small CTFS reader on top of `codetracer_ctfs/container`
##   helpers (`readCtfsFromFile`, `readInternalFile`, `hasCtfsMagic`) — the
##   sibling reader in `codetracer-native-recorder/ct_replayer/trace_loader.nim`
##   is the reference implementation but lives in another repo and is not a
##   public Nim package; reimplementing here keeps ct-print's dependency
##   footprint unchanged.
## * Thread stream names are `t` + 11-zero-padded uint32 TID (see writer's
##   `threadFileName`). We enumerate the root-block file entries directly to
##   discover them — `readInternalFile` looks up by base40-encoded name, so
##   we still go through it to read the bytes once we know the names.
## * Event headers use a 26-byte little-endian record (eventType:u16,
##   size:u32, ctTid:u32, geid:u64, tick:u64). Payload bytes follow until
##   `size` is reached. We surface a synthetic `event` JSON record per
##   header — type / size / geid / tick / thread_id / payload_bytes / payload
##   length — without trying to fully decode every event-type subform (that
##   would duplicate the native replayer's domain knowledge).
## * `event_log.dat` is decoded into structured OS-event records (one entry
##   per write/read/open/close/...) using the writer's serializer format.
## * The output document is byte-deterministic given the same input bytes:
##   thread streams are emitted in ascending CtTid order, OS events in their
##   stored order, and counts are computed up front.
## * Mutation behaviour: any byte corruption that breaks CTFS magic, the
##   meta.json blob, or an event-stream header is reported via a hard
##   `quit("ct-print: native decode failed: ...")` from the caller — this
##   module returns `Result[..., string]` and the CLI treats `isErr` as a
##   user-visible error rather than a silent fallback.

import std/[json, base64, strutils, algorithm, options]
import results
import stew/endians2
import codetracer_ctfs/base40
import codetracer_ctfs/container
import codetracer_ctfs/types
import codetracer_ctfs/zstd_bindings

# ---------------------------------------------------------------------------
# Wire-format constants (must match the native recorder)
# ---------------------------------------------------------------------------

const
  HeaderSize = 8
  ExtHeaderSize = 8
  FileEntrySize = 24  # 8 (size) + 8 (mapBlock) + 8 (encoded name)

  ## Native event header (ct_events/header.nim — EventHeaderSize = 26):
  ##   off 0..1   : eventType (u16 LE)
  ##   off 2..5   : size      (u32 LE — total bytes including header)
  ##   off 6..9   : ctTid     (u32 LE)
  ##   off 10..17 : geid      (u64 LE)
  ##   off 18..25 : tick      (u64 LE)
  EventHeaderSize* = 26

  ## OS event log magic ("EIDX") — see ct_recorder/event_log_writer.nim.
  EventLogMagic* = [byte(0x45), byte(0x49), byte(0x44), byte(0x58)]

# ---------------------------------------------------------------------------
# Small endian helpers (kept local to avoid a stew dep at the call site).
# ---------------------------------------------------------------------------

proc readU16LE(data: openArray[byte], offset: int): uint16 =
  var arr: array[2, byte]
  arr[0] = data[offset]
  arr[1] = data[offset + 1]
  fromBytesLE(uint16, arr)

proc readU32LE(data: openArray[byte], offset: int): uint32 =
  var arr: array[4, byte]
  for i in 0 ..< 4: arr[i] = data[offset + i]
  fromBytesLE(uint32, arr)

proc readU64LE(data: openArray[byte], offset: int): uint64 =
  var arr: array[8, byte]
  for i in 0 ..< 8: arr[i] = data[offset + i]
  fromBytesLE(uint64, arr)

# ---------------------------------------------------------------------------
# Minimal JSON extraction — we don't use std/json on the meta blob because the
# native writer hand-rolls a JSON string and `meta.json` is the *only* source
# of metadata. The helpers mirror those in trace_loader.nim (the reference
# reader) so behaviour stays consistent across implementations.
# ---------------------------------------------------------------------------

proc skipWhitespaceColon(s: string, i: int): int =
  result = i
  while result < s.len and (s[result] == ' ' or s[result] == ':' or
                            s[result] == '\t' or s[result] == '\n' or
                            s[result] == '\r'):
    result += 1

proc keyMatch(json: string, i: int, needle: string): bool =
  if i + needle.len > json.len:
    return false
  for j in 0 ..< needle.len:
    if json[i + j] != needle[j]:
      return false
  true

proc findJsonStringValue(json: string, key: string): string =
  let needle = "\"" & key & "\""
  var i = 0
  while i + needle.len <= json.len:
    if keyMatch(json, i, needle):
      var pos = skipWhitespaceColon(json, i + needle.len)
      if pos < json.len and json[pos] == '"':
        pos += 1
        var val = ""
        while pos < json.len and json[pos] != '"':
          if json[pos] == '\\' and pos + 1 < json.len:
            val.add(json[pos + 1])
            pos += 2
          else:
            val.add(json[pos])
            pos += 1
        return val
    i += 1
  ""

proc findJsonIntValue(json: string, key: string): int =
  let needle = "\"" & key & "\""
  var i = 0
  while i + needle.len <= json.len:
    if keyMatch(json, i, needle):
      var pos = skipWhitespaceColon(json, i + needle.len)
      if pos < json.len and json[pos] >= '0' and json[pos] <= '9':
        var val = 0
        while pos < json.len and json[pos] >= '0' and json[pos] <= '9':
          val = val * 10 + (ord(json[pos]) - ord('0'))
          pos += 1
        return val
    i += 1
  0

proc findJsonStringArray(json: string, key: string): seq[string] =
  result = @[]
  let needle = "\"" & key & "\""
  var i = 0
  while i + needle.len <= json.len:
    if keyMatch(json, i, needle):
      var pos = skipWhitespaceColon(json, i + needle.len)
      if pos < json.len and json[pos] == '[':
        pos += 1
        while pos < json.len:
          while pos < json.len and (json[pos] == ' ' or json[pos] == ',' or
                                    json[pos] == '\t' or json[pos] == '\n' or
                                    json[pos] == '\r'):
            pos += 1
          if pos >= json.len or json[pos] == ']':
            return result
          if json[pos] == '"':
            pos += 1
            var s = ""
            while pos < json.len and json[pos] != '"':
              if json[pos] == '\\' and pos + 1 < json.len:
                s.add(json[pos + 1])
                pos += 2
              else:
                s.add(json[pos])
                pos += 1
            if pos < json.len: pos += 1
            result.add(s)
          else:
            while pos < json.len and json[pos] != ',' and json[pos] != ']':
              pos += 1
        return
    i += 1

# ---------------------------------------------------------------------------
# Detection — is this a native MCR bundle?
# ---------------------------------------------------------------------------

type
  NativeBundleInfo* = object
    blockSize*: uint32
    maxRoot*: uint32
    metaJson*: string
    threadStreams*: seq[(uint32, string)]  # (ctTid, encodedName-string)
    hasEventLog*: bool
    hasCheckpointIndex*: bool

proc isThreadStreamName(name: string): bool =
  ## Thread stream names are `t` followed by 1+ digits.
  if name.len < 2 or name[0] != 't':
    return false
  for i in 1 ..< name.len:
    if name[i] < '0' or name[i] > '9':
      return false
  true

proc parseTidFromName(name: string): uint32 =
  var v: uint32 = 0
  for i in 1 ..< name.len:
    v = v * 10 + uint32(ord(name[i]) - ord('0'))
  v

proc detectNativeBundle*(data: openArray[byte]): Result[NativeBundleInfo, string] =
  ## Inspect a CTFS container's root block and decide whether it looks like a
  ## native MCR shard (multi-thread streams + meta.json with `recordingMode`).
  ## Returns the discovered structure on success, or an error message on
  ## inputs that aren't a valid CTFS file.
  if data.len < HeaderSize + ExtHeaderSize:
    return err("file too small to be a CTFS container")
  if not hasCtfsMagic(data):
    return err("invalid CTFS magic")
  if not hasValidVersion(data):
    return err("unsupported CTFS version: byte 0x" & toHex(int(data[5]), 2))

  let blockSize = readU32LE(data, 8)
  let maxRoot = readU32LE(data, 12)
  if blockSize == 0 or maxRoot == 0 or int(maxRoot) > 4096:
    return err(
      "nonsensical CTFS root header (blockSize=" & $blockSize &
      " maxRoot=" & $maxRoot & ")")

  var info = NativeBundleInfo(blockSize: blockSize, maxRoot: maxRoot)

  # Pull meta.json — required for native bundles.
  let metaR = readInternalFile(data, "meta.json", blockSize, maxRoot)
  if metaR.isErr:
    return err("meta.json missing: " & metaR.error)
  var metaStr = newString(metaR.get().len)
  for i in 0 ..< metaR.get().len:
    metaStr[i] = char(metaR.get()[i])
  info.metaJson = metaStr

  # Enumerate root-block entries to find tNNNN streams and event_log files.
  for i in 0 ..< int(maxRoot):
    let off = HeaderSize + ExtHeaderSize + i * FileEntrySize
    if off + FileEntrySize > data.len:
      break
    let nameVal = readU64LE(data, off + 16)
    if nameVal == 0:
      continue
    let name = base40Decode(nameVal)
    if isThreadStreamName(name):
      info.threadStreams.add((parseTidFromName(name), name))
    elif (nameVal == base40Encode("event_log.dat")) or
        (nameVal == base40Encode("event_log.idx")):
      # base40 truncates names >12 chars and maps `_` to `\0`, so the
      # decoded string contains a NUL — match by encoded value instead.
      info.hasEventLog = true
    elif nameVal == base40Encode("cpidx.idx"):
      info.hasCheckpointIndex = true

  # Sort thread streams by ctTid for deterministic output.
  info.threadStreams.sort(proc(a, b: (uint32, string)): int =
    cmp(a[0], b[0]))

  ok(info)

proc isNativeBundle*(data: openArray[byte]): bool =
  ## True when the container looks like a native MCR shard. We accept any
  ## CTFS container that has a `meta.json` file (the native writer's
  ## metadata blob) — that's enough to distinguish from the v4 multi-stream
  ## layout, which uses `meta.dat`. An empty native bundle (no `tNNNN`
  ## streams) is still routed here so the native decoder's structured
  ## error message surfaces, rather than the v4 reader silently emitting a
  ## degenerate `{counts: {steps: -1, ...}}` document.
  let infoR = detectNativeBundle(data)
  if infoR.isErr:
    return false
  let info = infoR.get()
  # Has a non-empty meta.json AND has either a `recordingMode` field or
  # at least one thread stream — both are unique to the native layout.
  if info.metaJson.len == 0:
    return false
  if info.threadStreams.len > 0:
    return true
  findJsonStringValue(info.metaJson, "recordingMode").len > 0

# ---------------------------------------------------------------------------
# Per-thread event stream decoding
# ---------------------------------------------------------------------------

type
  NativeEvent* = object
    threadId*: uint32
    eventType*: uint16
    size*: uint32
    ctTid*: uint32
    geid*: uint64
    tick*: uint64
    payload*: seq[byte]   ## bytes after the 26-byte header (size - 26)
    streamOffset*: int    ## byte offset into the per-thread stream

proc decompressZstdFrame(raw: openArray[byte]): Result[seq[byte], string] =
  ## Single-frame zstd decompression. We use `ZSTD_getFrameContentSize` to
  ## learn the decoded length up front; if the frame doesn't carry a content
  ## size we fall back to a 16x growth heuristic.
  if raw.len == 0:
    var empty: seq[byte] = @[]
    return ok(empty)
  let srcPtr = unsafeAddr raw[0]
  let cs = ZSTD_getFrameContentSize(srcPtr, csize_t(raw.len))
  var dstSize: int
  if cs == ZSTD_CONTENTSIZE_ERROR:
    return err("zstd: not a valid frame")
  elif cs == ZSTD_CONTENTSIZE_UNKNOWN:
    dstSize = raw.len * 16
    if dstSize < 4096: dstSize = 4096
  else:
    dstSize = int(cs)
  var dst = newSeq[byte](dstSize)
  let n = ZSTD_decompress(addr dst[0], csize_t(dstSize),
                          srcPtr, csize_t(raw.len))
  if ZSTD_isError(n) != 0:
    return err("zstd decompress failed: " & $ZSTD_getErrorName(n))
  dst.setLen(int(n))
  ok(dst)

proc readThreadStream(data: openArray[byte], blockSize, maxRoot: uint32,
                      streamName: string): Result[seq[byte], string] =
  ## Read bytes for a single per-thread stream and zstd-decompress if needed.
  let r = readInternalFile(data, streamName, blockSize, maxRoot)
  if r.isErr:
    return err("thread stream `" & streamName & "` read failed: " & r.error)
  let raw = r.get()
  if raw.len >= 4:
    # zstd frame magic 0xFD2FB528 LE
    if raw[0] == 0x28'u8 and raw[1] == 0xB5'u8 and raw[2] == 0x2F'u8 and
        raw[3] == 0xFD'u8:
      let dec = decompressZstdFrame(raw)
      if dec.isErr:
        return err("thread stream `" & streamName & "` " & dec.error)
      return ok(dec.get())
  ok(raw)

proc decodeThreadEvents(threadId: uint32, stream: seq[byte]):
    Result[seq[NativeEvent], string] =
  ## Walk a per-thread stream, slicing on the size field of each event header.
  ## Any header whose `size` would overrun the buffer is treated as a hard
  ## decode error — we never silently skip past corrupt regions.
  var events: seq[NativeEvent] = @[]
  var pos = 0
  while pos < stream.len:
    if pos + EventHeaderSize > stream.len:
      return err(
        "thread " & $threadId &
        ": truncated event header at offset " & $pos &
        " (need " & $EventHeaderSize & " bytes, have " &
        $(stream.len - pos) & ")")
    let etype = readU16LE(stream, pos)
    let size = readU32LE(stream, pos + 2)
    if size < uint32(EventHeaderSize):
      return err(
        "thread " & $threadId & ": event size " & $size &
        " < EventHeaderSize at offset " & $pos)
    if pos + int(size) > stream.len:
      return err(
        "thread " & $threadId & ": event size " & $size &
        " overruns stream end at offset " & $pos &
        " (stream len " & $stream.len & ")")
    let ctTid = readU32LE(stream, pos + 6)
    let geid = readU64LE(stream, pos + 10)
    let tick = readU64LE(stream, pos + 18)
    var payload: seq[byte] = @[]
    let payloadLen = int(size) - EventHeaderSize
    if payloadLen > 0:
      payload = newSeq[byte](payloadLen)
      for i in 0 ..< payloadLen:
        payload[i] = stream[pos + EventHeaderSize + i]
    events.add(NativeEvent(
      threadId: threadId,
      eventType: etype,
      size: size,
      ctTid: ctTid,
      geid: geid,
      tick: tick,
      payload: payload,
      streamOffset: pos,
    ))
    pos += int(size)
  ok(events)

# ---------------------------------------------------------------------------
# Event-type name table — kept here so we don't drag a dep on `ct_events`.
# Mirrors codetracer-native-recorder/ct_events/event_types.nim. Unknown
# values surface as the integer string so corrupted streams stay diff-stable.
# ---------------------------------------------------------------------------

proc nativeEventTypeName*(et: uint16): string =
  case et
  of 0: "evSyncLockAcquireBegin"
  of 1: "evSyncLockAcquireSuccess"
  of 2: "evSyncLockRelease"
  of 3: "evSyncCvWaitBegin"
  of 4: "evSyncCvWaitEnd"
  of 5: "evSyncCvSignal"
  of 6: "evSyncCvBroadcast"
  of 7: "evSyncSemWaitBegin"
  of 8: "evSyncSemWaitEnd"
  of 9: "evSyncSemPost"
  of 10: "evSyncEventWaitBegin"
  of 11: "evSyncEventWaitEnd"
  of 12: "evSyncEventSet"
  of 13: "evSyncEventReset"
  of 14: "evSyncAtomicRmw"
  of 15: "evSyncAtomicFence"
  of 16: "evSyncAtomicLoadAcquire"
  of 17: "evSyncAtomicStoreRelease"
  of 18: "evOsSyscall"
  of 19: "evOsRead"
  of 20: "evOsWrite"
  of 21: "evOsOpen"
  of 22: "evOsClose"
  of 23: "evOsMmap"
  of 24: "evOsPoll"
  of 25: "evOsSelect"
  of 26: "evOsSocket"
  of 27: "evOsConnect"
  of 28: "evOsAccept"
  of 29: "evOsSend"
  of 30: "evOsRecv"
  of 31: "evTimeClockGettime"
  of 32: "evTimeGettimeofday"
  of 33: "evRandGetrandom"
  of 34: "evRandGetentropy"
  of 35: "evThreadStart"
  of 36: "evThreadEnd"
  of 37: "evThreadJoin"
  of 38: "evCheckpointPeriodic"
  of 39: "evCheckpointCrash"
  of 40: "evCheckpointUser"
  of 41: "evProcessFork"
  of 42: "evProcessExec"
  of 43: "evData"
  of 44: "evRegisterSnapshot"
  of 45: "evOsLoadLibrary"
  of 46: "evOsGetCurrentProcessor"
  of 47: "evSyncTryLockAcquire"
  of 48: "evOsCreateEvent"
  of 49: "evOsCreateFileMapping"
  of 50: "evVerifyArgHash"
  of 51: "evVerifyMemHash"
  of 52: "evVerifyRegSnapshot"
  of 53: "evOsWait"
  of 54: "evOsSleep"
  of 55: "evOsIocp"
  of 56: "evOsGetAffinity"
  of 60: "evGfxGeneric"
  of 61: "evGfxDrawCall"
  of 62: "evGfxPresent"
  of 63: "evGfxResourceCreate"
  of 64: "evGfxDataUpload"
  of 70: "evVkGeneric"
  of 71: "evVkDraw"
  of 72: "evVkPresent"
  of 73: "evVkResourceCreate"
  of 74: "evVkDataUpload"
  of 80: "evTraceContextSpanStart"
  of 81: "evTraceContextSpanEnd"
  of 90: "evReplayBlockForever"
  else: "ev_unknown_" & $et

# ---------------------------------------------------------------------------
# event_log.dat / .idx decoding
# ---------------------------------------------------------------------------

type
  OsEventLogEntry* = object
    geid*: uint64
    tick*: uint64
    tid*: uint32
    kind*: uint8
    fd*: int32
    returnValue*: int64
    metadata*: string
    content*: seq[byte]

proc osEventKindName*(k: uint8): string =
  case k
  of 0: "elkWrite"
  of 1: "elkWriteFile"
  of 2: "elkRead"
  of 3: "elkReadFile"
  of 4: "elkOpen"
  of 5: "elkClose"
  of 6: "elkMmap"
  of 7: "elkSocket"
  of 8: "elkConnect"
  of 9: "elkAccept"
  of 10: "elkSend"
  of 11: "elkRecv"
  of 12: "elkPoll"
  of 13: "elkClockGettime"
  of 14: "elkGetrandom"
  else: "elk_unknown_" & $k

proc decodeOsEventLog(data: openArray[byte], blockSize, maxRoot: uint32):
    Result[seq[OsEventLogEntry], string] =
  ## Decode `event_log.dat` (chunk-prefixed records) using `event_log.idx`
  ## for the chunk count and offsets. Returns entries in stored order.
  let datR = readInternalFile(data, "event_log.dat", blockSize, maxRoot)
  if datR.isErr:
    var empty: seq[OsEventLogEntry] = @[]
    return ok(empty)  # event log file is optional (older bundles)
  let idxR = readInternalFile(data, "event_log.idx", blockSize, maxRoot)
  if idxR.isErr:
    return err("event_log.dat present but event_log.idx missing: " & idxR.error)
  let dat = datR.get()
  let idx = idxR.get()
  if idx.len < 4 + 2 + 4 + 4 + 4:
    return err("event_log.idx truncated (len=" & $idx.len & ")")
  for i in 0 ..< 4:
    if idx[i] != EventLogMagic[i]:
      return err("event_log.idx: bad magic at byte " & $i)
  let entryCount = readU32LE(idx, 4 + 2)
  let chunkCount = readU32LE(idx, 4 + 2 + 4 + 4)
  let chunkOffsetsStart = 4 + 2 + 4 + 4 + 4
  if idx.len < chunkOffsetsStart + int(chunkCount) * 8:
    return err("event_log.idx: truncated chunk-offsets table")

  var entries: seq[OsEventLogEntry] = @[]
  for c in 0 ..< int(chunkCount):
    let chunkOff = readU64LE(idx, chunkOffsetsStart + c * 8)
    if int(chunkOff) + 4 > dat.len:
      return err("event_log.dat: chunk " & $c & " offset out of bounds")
    let chunkRawSize = readU32LE(dat, int(chunkOff))
    let bodyStart = int(chunkOff) + 4
    if bodyStart + int(chunkRawSize) > dat.len:
      return err("event_log.dat: chunk " & $c & " size overruns end")
    if chunkRawSize < 4:
      return err("event_log.dat: chunk " & $c & " too small for entry count")
    let nEntries = readU32LE(dat, bodyStart)
    var pos = bodyStart + 4
    let bodyEnd = bodyStart + int(chunkRawSize)
    for e in 0 ..< int(nEntries):
      if pos + 8 + 8 + 4 + 1 + 4 + 8 + 2 > bodyEnd:
        return err(
          "event_log.dat: chunk " & $c & " entry " & $e &
          " truncated header")
      let geid = readU64LE(dat, pos); pos += 8
      let tick = readU64LE(dat, pos); pos += 8
      let tid = readU32LE(dat, pos); pos += 4
      let kind = dat[pos]; pos += 1
      let fdRaw = readU32LE(dat, pos); pos += 4
      let rvRaw = readU64LE(dat, pos); pos += 8
      let mdLen = int(readU16LE(dat, pos)); pos += 2
      if pos + mdLen + 4 > bodyEnd:
        return err(
          "event_log.dat: chunk " & $c & " entry " & $e &
          " truncated metadata")
      var md = newString(mdLen)
      for i in 0 ..< mdLen:
        md[i] = char(dat[pos + i])
      pos += mdLen
      let ctLen = int(readU32LE(dat, pos)); pos += 4
      if pos + ctLen > bodyEnd:
        return err(
          "event_log.dat: chunk " & $c & " entry " & $e &
          " truncated content (len=" & $ctLen & ")")
      var content = newSeq[byte](ctLen)
      for i in 0 ..< ctLen:
        content[i] = dat[pos + i]
      pos += ctLen
      entries.add(OsEventLogEntry(
        geid: geid, tick: tick, tid: tid, kind: kind,
        fd: cast[int32](fdRaw),
        returnValue: cast[int64](rvRaw),
        metadata: md,
        content: content,
      ))
  if uint32(entries.len) != entryCount:
    return err(
      "event_log: index says " & $entryCount &
      " entries, decoded " & $entries.len)
  ok(entries)

# ---------------------------------------------------------------------------
# Top-level decode + JSON document
# ---------------------------------------------------------------------------

type
  NativeOpts* = object
    stripPaths*: bool

proc bytesToUtf8(data: openArray[byte]): string =
  result = newString(data.len)
  for i in 0 ..< data.len:
    result[i] = char(data[i])

proc isPrintable(data: openArray[byte]): bool =
  ## Conservative ASCII check: every byte must be in the 0x20..0x7E range
  ## (or one of the standard control whitespace bytes \t \n \r). High-bit
  ## bytes are rejected so the JSON string we emit is always valid UTF-8.
  for b in data:
    if b >= 0x80:
      return false
    if b < 0x20 and b != 0x0A and b != 0x0D and b != 0x09:
      return false
  true

proc payloadJson(payload: seq[byte]): JsonNode =
  ## Surface the per-event payload bytes in a diff-friendly form: base64
  ## for exact fidelity, plus a UTF-8 best-effort string when printable.
  var node = newJObject()
  node["len"] = newJInt(int64(payload.len))
  node["b64"] = newJString(base64.encode(payload))
  if payload.len > 0 and isPrintable(payload):
    node["text"] = newJString(bytesToUtf8(payload))
  return node

proc buildNativeFullDocument*(data: openArray[byte], opts: NativeOpts):
    Result[JsonNode, string] =
  ## Decode a native MCR bundle and produce the same shape document the v4
  ## `--full` mode emits, with per-thread events interleaved by (geid, tid).
  let infoR = detectNativeBundle(data)
  if infoR.isErr:
    return err(infoR.error)
  let info = infoR.get()

  var root = newJObject()

  # ----- metadata -----
  var meta = newJObject()
  let program = findJsonStringValue(info.metaJson, "program")
  let args = findJsonStringArray(info.metaJson, "args")
  let platform = findJsonStringValue(info.metaJson, "platform")
  let recordingMode = findJsonStringValue(info.metaJson, "recordingMode")
  let tickSource = findJsonStringValue(info.metaJson, "tickSource")
  let tickDef = findJsonStringValue(info.metaJson, "tickDefinition")
  let hookProfile = findJsonStringValue(info.metaJson, "hookProfile")
  let metaVersion = findJsonStringValue(info.metaJson, "version")
  let hookStrats = findJsonStringArray(info.metaJson, "hookStrategies")
  meta["program"] = newJString(
    if opts.stripPaths and program.len > 0: "<program>" else: program)
  var argsArr = newJArray()
  for a in args:
    let av =
      if opts.stripPaths and a.len > 0 and (a.startsWith("/") or a.startsWith("C:")):
        "<arg>"
      else:
        a
    argsArr.add(newJString(av))
  meta["args"] = argsArr
  meta["workdir"] = newJString("")  # native bundle has no workdir field
  meta["recorder"] = newJString("native-mcr")
  meta["recording_mode"] = newJString(recordingMode)
  meta["platform"] = newJString(platform)
  meta["tick_source"] = newJString(tickSource)
  meta["tick_definition"] = newJString(tickDef)
  meta["hook_profile"] = newJString(hookProfile)
  meta["ctfs_version"] = newJString(metaVersion)
  var hsArr = newJArray()
  for h in hookStrats: hsArr.add(newJString(h))
  meta["hook_strategies"] = hsArr
  root["metadata"] = meta

  # paths/functions/varnames/types — native bundles do not interning-table
  # these (paths.json is always `[]` in M1). Surface as empty arrays so the
  # output schema matches the v4 `--full` output.
  root["paths"] = newJArray()
  root["functions"] = newJArray()
  root["varnames"] = newJArray()
  root["types"] = newJArray()

  # ----- decode each thread stream into events -----
  var allEvents: seq[NativeEvent] = @[]
  for (tid, name) in info.threadStreams:
    let streamR = readThreadStream(data, info.blockSize, info.maxRoot, name)
    if streamR.isErr:
      return err(streamR.error)
    let evR = decodeThreadEvents(tid, streamR.get())
    if evR.isErr:
      return err(evR.error)
    for ev in evR.get():
      allEvents.add(ev)

  # Stable interleave: by (geid, threadId, streamOffset). geid is the
  # global event ID assigned at recording time, so this matches replay order.
  allEvents.sort(proc(a, b: NativeEvent): int =
    if a.geid != b.geid:
      cmp(a.geid, b.geid)
    elif a.threadId != b.threadId:
      cmp(a.threadId, b.threadId)
    else:
      cmp(a.streamOffset, b.streamOffset))

  # ----- decode OS event log -----
  let osR = decodeOsEventLog(data, info.blockSize, info.maxRoot)
  if osR.isErr:
    return err(osR.error)
  let osEvents = osR.get()

  # ----- counts -----
  var counts = newJObject()
  counts["paths"] = newJInt(0)
  counts["functions"] = newJInt(0)
  counts["varnames"] = newJInt(0)
  counts["types"] = newJInt(0)
  counts["steps"] = newJInt(0)
  counts["calls"] = newJInt(0)
  counts["values"] = newJInt(0)
  counts["io_events"] = newJInt(int64(osEvents.len))
  counts["thread_events"] = newJInt(int64(allEvents.len))
  counts["thread_streams"] = newJInt(int64(info.threadStreams.len))
  root["counts"] = counts

  # ----- thread list -----
  var threadsArr = newJArray()
  for (tid, name) in info.threadStreams:
    var tObj = newJObject()
    tObj["thread_id"] = newJInt(int64(tid))
    tObj["stream_name"] = newJString(name)
    threadsArr.add(tObj)
  root["threads"] = threadsArr

  # ----- events: thread_event records first (geid-ordered), then os events -----
  var eventsArr = newJArray()
  for ev in allEvents:
    var obj = newJObject()
    obj["kind"] = newJString("thread_event")
    obj["thread_id"] = newJInt(int64(ev.threadId))
    obj["event_type"] = newJString(nativeEventTypeName(ev.eventType))
    obj["event_type_id"] = newJInt(int64(ev.eventType))
    obj["geid"] = newJInt(int64(ev.geid))
    obj["tick"] = newJInt(int64(ev.tick))
    obj["ct_tid"] = newJInt(int64(ev.ctTid))
    obj["size"] = newJInt(int64(ev.size))
    obj["payload"] = payloadJson(ev.payload)
    eventsArr.add(obj)

  for e in osEvents:
    var obj = newJObject()
    obj["kind"] = newJString("os_event")
    obj["os_kind"] = newJString(osEventKindName(e.kind))
    obj["os_kind_id"] = newJInt(int64(e.kind))
    obj["thread_id"] = newJInt(int64(e.tid))
    obj["geid"] = newJInt(int64(e.geid))
    obj["tick"] = newJInt(int64(e.tick))
    obj["fd"] = newJInt(int64(e.fd))
    obj["return_value"] = newJInt(e.returnValue)
    obj["metadata"] = newJString(e.metadata)
    obj["content_len"] = newJInt(int64(e.content.len))
    obj["content_b64"] = newJString(base64.encode(e.content))
    if e.content.len > 0 and isPrintable(e.content):
      obj["content_text"] = newJString(bytesToUtf8(e.content))
    eventsArr.add(obj)

  root["events"] = eventsArr
  ok(root)

proc decodeNativeFromFile*(path: string, opts: NativeOpts):
    Result[JsonNode, string] =
  ## Convenience wrapper used by the CLI: read the file, then decode.
  let dataR = readCtfsFromFile(path)
  if dataR.isErr:
    return err(dataR.error)
  buildNativeFullDocument(dataR.get(), opts)
