when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## High-level trace reader API for .ct files produced by TraceWriter.
##
## Opens a .ct file, parses the CTFS container, decompresses seekable Zstd
## chunks from events.log, decodes split-binary events, and provides
## JSON and text output.

import std/[json, strutils]
import results
import codetracer_ctfs/types
import codetracer_ctfs/base40
import codetracer_ctfs/container
import codetracer_ctfs/chunk_index
import codetracer_ctfs/zstd_bindings
import codetracer_trace_types
import codetracer_trace_writer/split_binary

export results, codetracer_trace_types

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

type
  TraceReaderMetadata* = object
    program*: string
    args*: seq[string]
    workdir*: string

  TraceReader* = object
    ctfsData: seq[byte]          ## Raw CTFS container
    blockSize: uint32
    maxRootEntries: uint32
    metadata*: TraceReaderMetadata
    paths*: seq[string]
    events*: seq[TraceLowLevelEvent]
    eventCount*: int

# ---------------------------------------------------------------------------
# Internal helpers: CTFS file reading
# ---------------------------------------------------------------------------

proc findInternalFileEntry(data: openArray[byte], name: string,
    maxEntries: uint32): tuple[size: uint64, mapBlock: uint64] =
  ## Search file entries in block 0 for the given name. Returns (size, mapBlock).
  let encoded = base40Encode(name)
  for i in 0 ..< int(maxEntries):
    let off = HeaderSize + ExtHeaderSize + i * FileEntrySize
    if off + FileEntrySize > data.len:
      break
    let entrySize = readU64LE(data, off)
    let entryMap = readU64LE(data, off + 8)
    let entryName = readU64LE(data, off + 16)
    if entryName == encoded:
      return (entrySize, entryMap)
  (0'u64, 0'u64)

proc readInternalFile(data: openArray[byte], name: string,
                      blockSize: uint32,
                      maxEntries: uint32): Result[seq[byte], string] =
  ## Read the complete content of an internal CTFS file by following the block mapping.
  let (fileSize, mapBlock) = findInternalFileEntry(data, name, maxEntries)
  if fileSize == 0 and mapBlock == 0:
    return err("internal file not found: " & name)

  var fileBytes = newSeq[byte](int(fileSize))
  let usable = uint64(blockSize) div 8 - 1

  var remaining = int(fileSize)
  var destPos = 0
  var blockIdx: uint64 = 0

  # Walk through data blocks using the mapping. Supports multi-level mapping
  # by using the same chain-walking logic as the writer.
  while remaining > 0:
    # Use lookupDataBlock logic inline for the reader (no Ctfs object available)
    var idx = blockIdx
    var currentLevelBlock = mapBlock
    var level: uint32 = 1

    # Walk up through levels
    block findLevel:
      while true:
        var cap: uint64 = 1
        for l in 0'u32 ..< level:
          cap = cap * usable
        if idx < cap:
          break findLevel
        idx -= cap
        level += 1
        if level > MaxChainLevels:
          return err("block index too large for mapping")
        # Follow chain pointer (last entry in current level block)
        let chainOff = int(currentLevelBlock) * int(blockSize) + int(usable) * 8
        if chainOff + 8 > data.len:
          return err("chain pointer out of bounds")
        let chainPtr = readU64LE(data, chainOff)
        if chainPtr == 0:
          return err("missing chain pointer at level " & $level)
        currentLevelBlock = chainPtr

    # Navigate down from level to find the data block
    var navBlock = currentLevelBlock
    var navLevel = level
    var navIdx = idx
    while navLevel > 1:
      var subCap: uint64 = 1
      for l in 0'u32 ..< (navLevel - 1):
        subCap = subCap * usable
      let entryIdx = navIdx div subCap
      let subIdx = navIdx mod subCap
      let childOff = int(navBlock) * int(blockSize) + int(entryIdx) * 8
      if childOff + 8 > data.len:
        return err("child pointer out of bounds")
      let childBlock = readU64LE(data, childOff)
      if childBlock == 0:
        return err("missing child block at level " & $navLevel)
      navBlock = childBlock
      navIdx = subIdx
      navLevel -= 1

    # Level 1: read direct pointer
    let ptrOff = int(navBlock) * int(blockSize) + int(navIdx) * 8
    if ptrOff + 8 > data.len:
      return err("data block pointer out of bounds")
    let dataBlock = readU64LE(data, ptrOff)
    if dataBlock == 0:
      return err("null data block at index " & $blockIdx)

    let blockOff = int(dataBlock) * int(blockSize)
    let toCopy = min(remaining, int(blockSize))
    if blockOff + toCopy > data.len:
      return err("data block content out of bounds")
    for i in 0 ..< toCopy:
      fileBytes[destPos + i] = data[blockOff + i]
    destPos += toCopy
    remaining -= toCopy
    blockIdx += 1

  ok(fileBytes)

proc bytesToString(data: seq[byte]): string =
  result = newString(data.len)
  for i in 0 ..< data.len:
    result[i] = char(data[i])

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

proc openTrace*(path: string): Result[TraceReader, string] =
  ## Open a .ct file and parse its contents (metadata and paths).
  let readRes = readCtfsFromFile(path)
  if readRes.isErr:
    return err("failed to read file: " & readRes.error)

  let data = readRes.get()

  # Validate CTFS magic
  if not hasCtfsMagic(data):
    return err("not a valid CTFS file (bad magic)")
  if not hasValidVersion(data):
    return err("unsupported CTFS version")

  # Read block size and max entries from extended header
  if data.len < 16:
    return err("file too small for CTFS header")

  var bs4: array[4, byte]
  bs4[0] = data[8]; bs4[1] = data[9]; bs4[2] = data[10]; bs4[3] = data[11]
  let blockSize = fromBytesLE(uint32, bs4)

  var mr4: array[4, byte]
  mr4[0] = data[12]; mr4[1] = data[13]; mr4[2] = data[14]; mr4[3] = data[15]
  let maxEntries = fromBytesLE(uint32, mr4)

  var reader = TraceReader(
    ctfsData: data,
    blockSize: blockSize,
    maxRootEntries: maxEntries,
    metadata: TraceReaderMetadata(),
    paths: @[],
    events: @[],
    eventCount: 0,
  )

  # Parse meta.json
  let metaRes = readInternalFile(data, "meta.json", blockSize, maxEntries)
  if metaRes.isOk:
    let metaStr = bytesToString(metaRes.get())
    try:
      let node = parseJson(metaStr)
      reader.metadata.program = node.getOrDefault("program").getStr("")
      reader.metadata.workdir = node.getOrDefault("workdir").getStr("")
      let argsNode = node.getOrDefault("args")
      if argsNode != nil and argsNode.kind == JArray:
        for item in argsNode:
          reader.metadata.args.add(item.getStr(""))
    except JsonParsingError:
      return err("failed to parse meta.json")
    except KeyError:
      return err("unexpected key error in meta.json")
    except IOError:
      return err("IO error parsing meta.json")
    except OSError:
      return err("OS error parsing meta.json")
    except ValueError:
      return err("value error parsing meta.json")
    except Exception:
      return err("unexpected error parsing meta.json")

  # Parse paths.json
  let pathsRes = readInternalFile(data, "paths.json", blockSize, maxEntries)
  if pathsRes.isOk:
    let pathsStr = bytesToString(pathsRes.get())
    try:
      let arr = parseJson(pathsStr)
      if arr.kind == JArray:
        for item in arr:
          reader.paths.add(item.getStr(""))
    except JsonParsingError:
      return err("failed to parse paths.json")
    except KeyError:
      return err("unexpected key error in paths.json")
    except IOError:
      return err("IO error parsing paths.json")
    except OSError:
      return err("OS error parsing paths.json")
    except ValueError:
      return err("value error parsing paths.json")
    except Exception:
      return err("unexpected error parsing paths.json")

  ok(reader)

proc readEvents*(reader: var TraceReader): Result[void, string] =
  ## Decompress and decode all events from events.log.
  let eventsRes = readInternalFile(reader.ctfsData, "events.log",
                                    reader.blockSize, reader.maxRootEntries)
  if eventsRes.isErr:
    return err("failed to read events.log: " & eventsRes.error)

  let eventsData = eventsRes.get()
  if eventsData.len == 0:
    reader.events = @[]
    reader.eventCount = 0
    return ok()

  # Decode all chunks: [16-byte header][compressed data]...
  var pos = 0
  while pos + ChunkIndexEntrySize <= eventsData.len:
    let chunk = decodeChunkHeader(eventsData, pos)
    if chunk.compressedSize == 0:
      break
    pos += ChunkIndexEntrySize

    if pos + int(chunk.compressedSize) > eventsData.len:
      return err("chunk compressed data extends beyond events.log")

    # Decompress the chunk
    let compressedSlice = eventsData[pos ..< pos + int(chunk.compressedSize)]
    let decompSize = ZSTD_getFrameContentSize(
      unsafeAddr compressedSlice[0], csize_t(compressedSlice.len))

    if decompSize == ZSTD_CONTENTSIZE_ERROR:
      return err("failed to get decompressed size for chunk")

    var decompressed = newSeq[byte](int(decompSize))
    if decompSize > 0:
      let actualSize = ZSTD_decompress(
        addr decompressed[0], csize_t(decompressed.len),
        unsafeAddr compressedSlice[0], csize_t(compressedSlice.len))
      if ZSTD_isError(actualSize) != 0:
        return err("zstd decompression failed")
      decompressed.setLen(int(actualSize))

    # Decode split-binary events
    let decoded = decodeAllEvents(decompressed)
    if decoded.isErr:
      return err("failed to decode events: " & decoded.unsafeError)
    for event in decoded.get():
      reader.events.add(event)

    pos += int(chunk.compressedSize)

  reader.eventCount = reader.events.len
  ok()

# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------

proc valueRecordToJson(v: ValueRecord): JsonNode {.raises: [].}

proc valueRecordToJson(v: ValueRecord): JsonNode =
  result = newJObject()
  case v.kind
  of vrkInt:
    result["kind"] = newJString("Int")
    result["i"] = newJInt(v.intVal)
    result["type_id"] = newJInt(int64(uint64(v.intTypeId)))
  of vrkFloat:
    result["kind"] = newJString("Float")
    result["f"] = newJFloat(v.floatVal)
    result["type_id"] = newJInt(int64(uint64(v.floatTypeId)))
  of vrkBool:
    result["kind"] = newJString("Bool")
    result["b"] = newJBool(v.boolVal)
    result["type_id"] = newJInt(int64(uint64(v.boolTypeId)))
  of vrkString:
    result["kind"] = newJString("String")
    result["text"] = newJString(v.text)
    result["type_id"] = newJInt(int64(uint64(v.strTypeId)))
  of vrkSequence:
    result["kind"] = newJString("Sequence")
    var elems = newJArray()
    for e in v.seqElements:
      elems.add(valueRecordToJson(e))
    result["elements"] = elems
    result["is_slice"] = newJBool(v.isSlice)
    result["type_id"] = newJInt(int64(uint64(v.seqTypeId)))
  of vrkTuple:
    result["kind"] = newJString("Tuple")
    var elems = newJArray()
    for e in v.tupleElements:
      elems.add(valueRecordToJson(e))
    result["elements"] = elems
    result["type_id"] = newJInt(int64(uint64(v.tupleTypeId)))
  of vrkStruct:
    result["kind"] = newJString("Struct")
    var fields = newJArray()
    for e in v.fieldValues:
      fields.add(valueRecordToJson(e))
    result["field_values"] = fields
    result["type_id"] = newJInt(int64(uint64(v.structTypeId)))
  of vrkVariant:
    result["kind"] = newJString("Variant")
    result["discriminator"] = newJString(v.discriminator)
    if v.contents.len > 0:
      result["contents"] = valueRecordToJson(v.contents[0])
    else:
      result["contents"] = newJNull()
    result["type_id"] = newJInt(int64(uint64(v.variantTypeId)))
  of vrkReference:
    result["kind"] = newJString("Reference")
    if v.dereferenced.len > 0:
      result["dereferenced"] = valueRecordToJson(v.dereferenced[0])
    else:
      result["dereferenced"] = newJNull()
    result["address"] = newJInt(int64(v.address))
    result["mutable"] = newJBool(v.mutable)
    result["type_id"] = newJInt(int64(uint64(v.refTypeId)))
  of vrkRaw:
    result["kind"] = newJString("Raw")
    result["r"] = newJString(v.rawStr)
    result["type_id"] = newJInt(int64(uint64(v.rawTypeId)))
  of vrkError:
    result["kind"] = newJString("Error")
    result["msg"] = newJString(v.errorMsg)
    result["type_id"] = newJInt(int64(uint64(v.errorTypeId)))
  of vrkNone:
    result["kind"] = newJString("None")
    result["type_id"] = newJInt(int64(uint64(v.noneTypeId)))
  of vrkCell:
    result["kind"] = newJString("Cell")
    result["place"] = newJInt(int64(v.cellPlace))
  of vrkBigInt:
    result["kind"] = newJString("BigInt")
    result["negative"] = newJBool(v.negative)
    result["type_id"] = newJInt(int64(uint64(v.bigIntTypeId)))
  of vrkChar:
    result["kind"] = newJString("Char")
    result["c"] = newJString($v.charVal)
    result["type_id"] = newJInt(int64(uint64(v.charTypeId)))

proc eventToJson(event: TraceLowLevelEvent): JsonNode =
  result = newJObject()
  case event.kind
  of tleStep:
    result["type"] = newJString("Step")
    result["path_id"] = newJInt(int64(uint64(event.step.pathId)))
    result["line"] = newJInt(int64(event.step.line))
  of tlePath:
    result["type"] = newJString("Path")
    result["name"] = newJString(event.path)
  of tleVariableName:
    result["type"] = newJString("VariableName")
    result["name"] = newJString(event.varName)
  of tleVariable:
    result["type"] = newJString("Variable")
    result["name"] = newJString(event.variable)
  of tleType:
    result["type"] = newJString("Type")
    result["kind"] = newJString($event.typeRecord.kind)
    result["lang_type"] = newJString(event.typeRecord.langType)
  of tleValue:
    result["type"] = newJString("Value")
    result["variable_id"] = newJInt(int64(uint64(event.fullValue.variableId)))
    result["value"] = valueRecordToJson(event.fullValue.value)
  of tleFunction:
    result["type"] = newJString("Function")
    result["path_id"] = newJInt(int64(uint64(event.functionRecord.pathId)))
    result["line"] = newJInt(int64(event.functionRecord.line))
    result["name"] = newJString(event.functionRecord.name)
  of tleCall:
    result["type"] = newJString("Call")
    result["function_id"] = newJInt(int64(uint64(event.callRecord.functionId)))
    var args = newJArray()
    for arg in event.callRecord.args:
      var argNode = newJObject()
      argNode["variable_id"] = newJInt(int64(uint64(arg.variableId)))
      argNode["value"] = valueRecordToJson(arg.value)
      args.add(argNode)
    result["args"] = args
  of tleReturn:
    result["type"] = newJString("Return")
    result["value"] = valueRecordToJson(event.returnRecord.returnValue)
  of tleEvent:
    result["type"] = newJString("Event")
    result["event_kind"] = newJString($event.recordEvent.kind)
    result["metadata"] = newJString(event.recordEvent.metadata)
    result["content"] = newJString(event.recordEvent.content)
  of tleAsm:
    result["type"] = newJString("Asm")
    var lines = newJArray()
    for line in event.asmLines:
      lines.add(newJString(line))
    result["lines"] = lines
  of tleBindVariable:
    result["type"] = newJString("BindVariable")
    result["variable_id"] = newJInt(int64(uint64(event.bindVar.variableId)))
    result["place"] = newJInt(int64(event.bindVar.place))
  of tleAssignment:
    result["type"] = newJString("Assignment")
    result["to"] = newJInt(int64(uint64(event.assignment.to)))
    result["pass_by"] = newJString($event.assignment.passBy)
  of tleDropVariables:
    result["type"] = newJString("DropVariables")
    var ids = newJArray()
    for id in event.dropVarIds:
      ids.add(newJInt(int64(uint64(id))))
    result["ids"] = ids
  of tleCompoundValue:
    result["type"] = newJString("CompoundValue")
    result["place"] = newJInt(int64(event.compoundValue.place))
    result["value"] = valueRecordToJson(event.compoundValue.value)
  of tleCellValue:
    result["type"] = newJString("CellValue")
    result["place"] = newJInt(int64(event.cellValue.place))
    result["value"] = valueRecordToJson(event.cellValue.value)
  of tleAssignCompoundItem:
    result["type"] = newJString("AssignCompoundItem")
    result["place"] = newJInt(int64(event.assignCompoundItem.place))
    result["index"] = newJInt(int64(event.assignCompoundItem.index))
    result["item_place"] = newJInt(int64(event.assignCompoundItem.itemPlace))
  of tleAssignCell:
    result["type"] = newJString("AssignCell")
    result["place"] = newJInt(int64(event.assignCell.place))
    result["value"] = valueRecordToJson(event.assignCell.newValue)
  of tleVariableCell:
    result["type"] = newJString("VariableCell")
    result["variable_id"] = newJInt(int64(uint64(event.variableCell.variableId)))
    result["place"] = newJInt(int64(event.variableCell.place))
  of tleDropVariable:
    result["type"] = newJString("DropVariable")
    result["variable_id"] = newJInt(int64(uint64(event.dropVarId)))
  of tleThreadStart:
    result["type"] = newJString("ThreadStart")
    result["thread_id"] = newJInt(int64(uint64(event.threadStartId)))
  of tleThreadExit:
    result["type"] = newJString("ThreadExit")
    result["thread_id"] = newJInt(int64(uint64(event.threadExitId)))
  of tleThreadSwitch:
    result["type"] = newJString("ThreadSwitch")
    result["thread_id"] = newJInt(int64(uint64(event.threadSwitchId)))
  of tleDropLastStep:
    result["type"] = newJString("DropLastStep")

proc toJson*(reader: TraceReader): string =
  ## Serialize the entire trace to JSON (metadata + paths + events).
  var root = newJObject()

  # Metadata
  var meta = newJObject()
  meta["program"] = newJString(reader.metadata.program)
  var argsArr = newJArray()
  for arg in reader.metadata.args:
    argsArr.add(newJString(arg))
  meta["args"] = argsArr
  meta["workdir"] = newJString(reader.metadata.workdir)
  root["metadata"] = meta

  # Paths
  var pathsArr = newJArray()
  for p in reader.paths:
    pathsArr.add(newJString(p))
  root["paths"] = pathsArr

  # Events
  var eventsArr = newJArray()
  for event in reader.events:
    eventsArr.add(eventToJson(event))
  root["events"] = eventsArr

  try:
    result = pretty(root)
  except ValueError:
    result = $root

proc toJsonEvents*(reader: TraceReader): string =
  ## Serialize just the events to JSON array.
  var eventsArr = newJArray()
  for event in reader.events:
    eventsArr.add(eventToJson(event))
  try:
    result = pretty(eventsArr)
  except ValueError:
    result = $eventsArr

# ---------------------------------------------------------------------------
# Pretty text output
# ---------------------------------------------------------------------------

proc escapeStr(s: string): string =
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

proc prettyPrintEvent(event: TraceLowLevelEvent, index: int): string =
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

proc toPrettyText*(reader: TraceReader): string =
  ## Human-readable text format.
  var lines: seq[string]
  lines.add("=== Trace ===")
  lines.add("program: " & reader.metadata.program)
  if reader.metadata.args.len > 0:
    lines.add("args: " & reader.metadata.args.join(" "))
  if reader.metadata.workdir.len > 0:
    lines.add("workdir: " & reader.metadata.workdir)
  lines.add("paths: " & $reader.paths.len)
  lines.add("events: " & $reader.eventCount)
  lines.add("")
  for i, event in reader.events:
    lines.add(prettyPrintEvent(event, i))
  result = lines.join("\n") & "\n"

proc toSummary*(reader: TraceReader): string =
  ## Print metadata and event counts only.
  var lines: seq[string]
  lines.add("program: " & reader.metadata.program)
  if reader.metadata.args.len > 0:
    lines.add("args: " & reader.metadata.args.join(" "))
  if reader.metadata.workdir.len > 0:
    lines.add("workdir: " & reader.metadata.workdir)
  lines.add("paths: " & $reader.paths.len)
  lines.add("events: " & $reader.eventCount)

  # Count events by type
  var stepCount = 0
  var pathCount = 0
  var functionCount = 0
  var callCount = 0
  var returnCount = 0
  var valueCount = 0
  var otherCount = 0
  for event in reader.events:
    case event.kind
    of tleStep: stepCount += 1
    of tlePath: pathCount += 1
    of tleFunction: functionCount += 1
    of tleCall: callCount += 1
    of tleReturn: returnCount += 1
    of tleValue: valueCount += 1
    else: otherCount += 1

  lines.add("")
  lines.add("breakdown:")
  if stepCount > 0: lines.add("  steps: " & $stepCount)
  if pathCount > 0: lines.add("  paths: " & $pathCount)
  if functionCount > 0: lines.add("  functions: " & $functionCount)
  if callCount > 0: lines.add("  calls: " & $callCount)
  if returnCount > 0: lines.add("  returns: " & $returnCount)
  if valueCount > 0: lines.add("  values: " & $valueCount)
  if otherCount > 0: lines.add("  other: " & $otherCount)

  result = lines.join("\n") & "\n"
