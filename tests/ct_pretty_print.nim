## Pretty-printer for CTFS containers and split-binary event streams.
##
## Produces deterministic text output suitable for golden fixture comparisons.
## Non-deterministic fields (compressed sizes) are omitted.

{.push raises: [].}

import std/strutils
import results
import stew/endians2
import codetracer_ctfs

{.pop.}  # Restore default raises for procs below

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc escapeStr(s: string): string =
  ## Escape a string for display (show control chars).
  result = "\""
  for c in s:
    case c
    of '\n': result.add("\\n")
    of '\r': result.add("\\r")
    of '\t': result.add("\\t")
    of '\\': result.add("\\\\")
    of '"': result.add("\\\"")
    else:
      if ord(c) < 32:
        result.add("\\x" & toHex(ord(c), 2).toLowerAscii())
      else:
        result.add(c)
  result.add("\"")

# ---------------------------------------------------------------------------
# Event pretty-printing
# ---------------------------------------------------------------------------

proc prettyPrintEvent*(event: TraceLowLevelEvent, index: int): string =
  ## Pretty-print a single event.
  let prefix = "  event[" & $index & "]: "
  case event.kind
  of tleStep:
    prefix & "Step path_id=" & $uint64(event.step.pathId) &
      " line=" & $int64(event.step.line)
  of tlePath:
    prefix & "Path " & escapeStr(event.path)
  of tleVariableName:
    prefix & "VariableName " & escapeStr(event.varName)
  of tleVariable:
    prefix & "Variable " & escapeStr(event.variable)
  of tleType:
    prefix & "Type kind=" & $event.typeRecord.kind &
      " lang_type=" & escapeStr(event.typeRecord.langType)
  of tleValue:
    var s = prefix & "Value variable_id=" & $uint64(event.fullValue.variableId)
    case event.fullValue.value.kind
    of vrkInt:
      s &= " kind=Int i=" & $event.fullValue.value.intVal &
        " type_id=" & $uint64(event.fullValue.value.intTypeId)
    of vrkFloat:
      s &= " kind=Float f=" & $event.fullValue.value.floatVal &
        " type_id=" & $uint64(event.fullValue.value.floatTypeId)
    of vrkBool:
      s &= " kind=Bool b=" & $event.fullValue.value.boolVal &
        " type_id=" & $uint64(event.fullValue.value.boolTypeId)
    of vrkString:
      s &= " kind=String text=" & escapeStr(event.fullValue.value.text) &
        " type_id=" & $uint64(event.fullValue.value.strTypeId)
    of vrkNone:
      s &= " kind=None type_id=" & $uint64(event.fullValue.value.noneTypeId)
    of vrkChar:
      s &= " kind=Char c=" & escapeStr($event.fullValue.value.charVal) &
        " type_id=" & $uint64(event.fullValue.value.charTypeId)
    of vrkRaw:
      s &= " kind=Raw text=" & escapeStr(event.fullValue.value.rawStr) &
        " type_id=" & $uint64(event.fullValue.value.rawTypeId)
    of vrkError:
      s &= " kind=Error msg=" & escapeStr(event.fullValue.value.errorMsg) &
        " type_id=" & $uint64(event.fullValue.value.errorTypeId)
    of vrkCell:
      s &= " kind=Cell place=" & $int64(event.fullValue.value.cellPlace)
    of vrkBigInt:
      s &= " kind=BigInt negative=" & $event.fullValue.value.negative &
        " type_id=" & $uint64(event.fullValue.value.bigIntTypeId)
    else:
      s &= " kind=" & $event.fullValue.value.kind
    s
  of tleFunction:
    prefix & "Function path_id=" & $uint64(event.functionRecord.pathId) &
      " line=" & $int64(event.functionRecord.line) &
      " name=" & escapeStr(event.functionRecord.name)
  of tleCall:
    prefix & "Call function_id=" & $uint64(event.callRecord.functionId) &
      " args_count=" & $event.callRecord.args.len
  of tleReturn:
    var s = prefix & "Return"
    case event.returnRecord.returnValue.kind
    of vrkInt:
      s &= " kind=Int i=" & $event.returnRecord.returnValue.intVal &
        " type_id=" & $uint64(event.returnRecord.returnValue.intTypeId)
    of vrkNone:
      s &= " kind=None"
    of vrkFloat:
      s &= " kind=Float f=" & $event.returnRecord.returnValue.floatVal
    of vrkBool:
      s &= " kind=Bool b=" & $event.returnRecord.returnValue.boolVal
    of vrkString:
      s &= " kind=String text=" & escapeStr(event.returnRecord.returnValue.text)
    else:
      s &= " kind=" & $event.returnRecord.returnValue.kind
    s
  of tleEvent:
    prefix & "Event kind=" & $event.recordEvent.kind &
      " metadata=" & escapeStr(event.recordEvent.metadata) &
      " content=" & escapeStr(event.recordEvent.content)
  of tleAsm:
    prefix & "Asm lines=" & $event.asmLines.len
  of tleBindVariable:
    prefix & "BindVariable variable_id=" & $uint64(event.bindVar.variableId) &
      " place=" & $int64(event.bindVar.place)
  of tleAssignment:
    prefix & "Assignment to=" & $uint64(event.assignment.to) &
      " pass_by=" & $event.assignment.passBy
  of tleDropVariables:
    prefix & "DropVariables count=" & $event.dropVarIds.len
  of tleCompoundValue:
    prefix & "CompoundValue place=" & $int64(event.compoundValue.place)
  of tleCellValue:
    prefix & "CellValue place=" & $int64(event.cellValue.place)
  of tleAssignCompoundItem:
    prefix & "AssignCompoundItem place=" & $int64(event.assignCompoundItem.place) &
      " index=" & $event.assignCompoundItem.index
  of tleAssignCell:
    prefix & "AssignCell place=" & $int64(event.assignCell.place)
  of tleVariableCell:
    prefix & "VariableCell variable_id=" & $uint64(event.variableCell.variableId) &
      " place=" & $int64(event.variableCell.place)
  of tleDropVariable:
    prefix & "DropVariable variable_id=" & $uint64(event.dropVarId)
  of tleThreadStart:
    prefix & "ThreadStart id=" & $uint64(event.threadStartId)
  of tleThreadExit:
    prefix & "ThreadExit id=" & $uint64(event.threadExitId)
  of tleThreadSwitch:
    prefix & "ThreadSwitch id=" & $uint64(event.threadSwitchId)
  of tleDropLastStep:
    prefix & "DropLastStep"

# ---------------------------------------------------------------------------
# Pretty-print raw split-binary events (no CTFS container)
# ---------------------------------------------------------------------------

proc prettyPrintEvents*(data: openArray[byte]): string =
  ## Pretty-print a raw split-binary event stream.
  let decoded = decodeAllEvents(data)
  if decoded.isErr:
    return "ERROR: failed to decode events: " & decoded.unsafeError & "\n"
  let events = decoded.get()
  var lines: seq[string]
  lines.add("=== Split-Binary Events ===")
  lines.add("event_count: " & $events.len)
  lines.add("")
  for i, ev in events:
    lines.add(prettyPrintEvent(ev, i))
  result = lines.join("\n") & "\n"

# ---------------------------------------------------------------------------
# Pretty-print seekable Zstd
# ---------------------------------------------------------------------------

proc prettyPrintSeekableZstd*(data: openArray[byte]): string =
  ## Pretty-print a seekable Zstd file's structure and content.
  let decRes = initSeekableZstdDecoder(data)
  if decRes.isErr:
    return "ERROR: failed to init seekable Zstd decoder: " & decRes.unsafeError & "\n"
  let dec = decRes.get()

  var lines: seq[string]
  lines.add("=== Seekable Zstd ===")
  lines.add("frame_count: " & $dec.frameCount)

  var totalDecomp: uint64 = 0
  for entry in dec.seekTable.entries:
    totalDecomp += uint64(entry.decompressedSize)
  lines.add("total_decompressed: " & $totalDecomp)
  lines.add("")

  for i in 0 ..< dec.frameCount:
    let entry = dec.seekTable.entries[i]
    lines.add("[frame " & $i & "] decompressed_size: " & $entry.decompressedSize)
    let frameRes = dec.decompressFrame(i)
    if frameRes.isOk:
      let frameData = frameRes.get()
      # Show first 32 bytes as hex for content identification
      var hexStr = ""
      let showLen = min(32, frameData.len)
      for j in 0 ..< showLen:
        hexStr.add(toHex(frameData[j]).toLowerAscii())
      if frameData.len > 32:
        hexStr.add("...")
      lines.add("  content_preview: " & hexStr)
    else:
      lines.add("  content_preview: ERROR")
  lines.add("")

  lines.add("=== Seek Table ===")
  lines.add("entries: " & $dec.frameCount)
  for i in 0 ..< dec.frameCount:
    let entry = dec.seekTable.entries[i]
    lines.add("[" & $i & "] decompressed_size: " & $entry.decompressedSize)

  result = lines.join("\n") & "\n"

# ---------------------------------------------------------------------------
# Pretty-print CTFS container
# ---------------------------------------------------------------------------

proc prettyPrintCtfsHeader(data: openArray[byte]): seq[string] =
  ## Pretty-print the CTFS header fields.
  result = @[]
  result.add("=== CTFS Header ===")
  # Magic
  var magicHex = ""
  for i in 0 ..< 5:
    magicHex.add(toHex(data[i]).toLowerAscii())
  result.add("magic: " & magicHex)
  # Version
  result.add("version: " & $data[5])
  # Compression
  let comp = readCompressionMethod(data)
  result.add("compression: " & $comp)
  # Encryption
  let enc = readEncryptionMethod(data)
  result.add("encryption: " & $enc)
  # Block size
  var bs4: array[4, byte]
  bs4[0] = data[8]; bs4[1] = data[9]; bs4[2] = data[10]; bs4[3] = data[11]
  let blockSize = fromBytesLE(uint32, bs4)
  result.add("block_size: " & $blockSize)
  # Max root entries
  var mr4: array[4, byte]
  mr4[0] = data[12]; mr4[1] = data[13]; mr4[2] = data[14]; mr4[3] = data[15]
  let maxEntries = fromBytesLE(uint32, mr4)
  result.add("max_root_entries: " & $maxEntries)

proc findAllFileEntries(data: openArray[byte]): seq[tuple[name: string, size: uint64, mapBlock: uint64]] =
  ## Find all non-empty file entries in block 0.
  var mr4: array[4, byte]
  mr4[0] = data[12]; mr4[1] = data[13]; mr4[2] = data[14]; mr4[3] = data[15]
  let maxEntries = fromBytesLE(uint32, mr4)
  result = @[]
  for i in 0 ..< int(maxEntries):
    let off = HeaderSize + ExtHeaderSize + i * FileEntrySize
    if off + FileEntrySize > data.len:
      break
    let entrySize = readU64LE(data, off)
    let entryMap = readU64LE(data, off + 8)
    let entryName = readU64LE(data, off + 16)
    if entrySize == 0 and entryMap == 0 and entryName == 0:
      continue
    let name = base40Decode(entryName)
    result.add((name: name, size: entrySize, mapBlock: entryMap))

proc readInternalFileData(data: openArray[byte], mapBlock: uint64,
                          fileSize: uint64,
                          blockSize: uint32 = DefaultBlockSize): seq[byte] =
  ## Read internal file data given map block and size.
  result = newSeq[byte](int(fileSize))
  let usable = uint64(blockSize) div 8 - 1
  var remaining = int(fileSize)
  var destPos = 0
  var blockIdx: uint64 = 0
  while remaining > 0:
    var dataBlock: uint64
    if blockIdx < usable:
      let off = int(mapBlock) * int(blockSize) + int(blockIdx) * 8
      dataBlock = readU64LE(data, off)
    else:
      break
    let blockOff = int(dataBlock) * int(blockSize)
    let toCopy = min(remaining, int(blockSize))
    for i in 0 ..< toCopy:
      result[destPos + i] = data[blockOff + i]
    destPos += toCopy
    remaining -= toCopy
    blockIdx += 1

proc prettyPrintCtFile*(data: openArray[byte]): string =
  ## Pretty-print a complete CTFS .ct file.
  var lines: seq[string]

  # Header
  lines.add(prettyPrintCtfsHeader(data))

  # File entries
  let entries = findAllFileEntries(data)
  lines.add("")
  lines.add("=== File Entries ===")
  for i, entry in entries:
    lines.add("[" & $i & "] name: " & escapeStr(entry.name) &
              " size: " & $entry.size &
              " map_block: " & $entry.mapBlock)

  # Read block size for data access
  var bs4: array[4, byte]
  bs4[0] = data[8]; bs4[1] = data[9]; bs4[2] = data[10]; bs4[3] = data[11]
  let blockSize = fromBytesLE(uint32, bs4)

  # Print each internal file's content
  for entry in entries:
    let fileData = readInternalFileData(data, entry.mapBlock, entry.size, blockSize)
    lines.add("")

    if entry.name == "events.log":
      # Decode chunked compressed events
      lines.add("=== Internal File: events.log ===")
      let chunks = decodeAllChunkHeaders(fileData)
      var pos = 0
      var chunkIdx = 0
      for chunk in chunks:
        pos += ChunkIndexEntrySize
        let compressedData = fileData[pos ..< pos + int(chunk.compressedSize)]
        # Decompress
        let decompSize = ZSTD_getFrameContentSize(
          unsafeAddr compressedData[0], csize_t(compressedData.len))
        if decompSize == ZSTD_CONTENTSIZE_ERROR:
          lines.add("[chunk " & $chunkIdx & "] ERROR: bad frame content size")
          pos += int(chunk.compressedSize)
          chunkIdx += 1
          continue
        var decompressed = newSeq[byte](int(decompSize))
        let actualSize = ZSTD_decompress(
          addr decompressed[0], csize_t(decompressed.len),
          unsafeAddr compressedData[0], csize_t(compressedData.len))
        if ZSTD_isError(actualSize) != 0:
          lines.add("[chunk " & $chunkIdx & "] ERROR: decompression failed")
          pos += int(chunk.compressedSize)
          chunkIdx += 1
          continue
        decompressed.setLen(int(actualSize))
        let decoded = decodeAllEvents(decompressed)
        if decoded.isErr:
          lines.add("[chunk " & $chunkIdx & "] ERROR: decode failed: " & decoded.unsafeError)
        else:
          let events = decoded.get()
          lines.add("[chunk " & $chunkIdx & "] event_count: " &
                    $chunk.eventCount & " first_geid: " & $chunk.firstGeid)
          for i, ev in events:
            lines.add(prettyPrintEvent(ev, i))
        pos += int(chunk.compressedSize)
        chunkIdx += 1
    elif entry.name == "events.fmt" or entry.name == "meta.json" or
        entry.name == "paths.json":
      lines.add("=== Internal File: " & entry.name & " ===")
      var content = newString(fileData.len)
      for i in 0 ..< fileData.len:
        content[i] = char(fileData[i])
      lines.add(content)
    else:
      # Generic binary file -- show hex preview
      lines.add("=== Internal File: " & entry.name & " ===")
      var hexStr = ""
      let showLen = min(64, fileData.len)
      for i in 0 ..< showLen:
        hexStr.add(toHex(fileData[i]).toLowerAscii())
        if (i + 1) mod 16 == 0 and i + 1 < showLen:
          hexStr.add("\n")
      if fileData.len > 64:
        hexStr.add("... (" & $fileData.len & " bytes total)"  )
      lines.add(hexStr)

  result = lines.join("\n") & "\n"

proc prettyPrintCtFile*(path: string): string {.raises: [IOError, OSError].} =
  ## Pretty-print a .ct file from disk.
  let content = readFile(path)
  var data = newSeq[byte](content.len)
  for i in 0 ..< content.len:
    data[i] = byte(content[i])
  prettyPrintCtFile(data)
