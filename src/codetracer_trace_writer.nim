{.push raises: [].}

## High-level trace writer API combining CTFS container + split-binary
## encoding + seekable Zstd compression.
##
## Produces .ct files with:
##   events.log  — split-binary events compressed with seekable Zstd
##   events.fmt  — the string "split-binary"
##   meta.json   — {"program": "...", "args": [...], "workdir": "..."}
##   paths.json  — ["/path/to/file1.nim", ...]

import std/json
import results
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_ctfs/streaming
import codetracer_ctfs/seekable_zstd
import codetracer_ctfs/chunk_index
import codetracer_ctfs/zstd_bindings
import codetracer_trace_types
import codetracer_trace_writer/split_binary

export results, codetracer_trace_types

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  DefaultChunkThreshold* = 4096  ## Events per chunk before compression
  TraceWriterCompressionLevel = 3

# ---------------------------------------------------------------------------
# TraceWriter type
# ---------------------------------------------------------------------------

type
  TraceWriter* = object
    ctfs: Ctfs                       ## CTFS container
    eventsFile: CtfsInternalFile     ## Handle for events.log
    encoder: SplitBinaryEncoder      ## Event serializer
    paths: seq[string]               ## Registered paths (for paths.json)
    metadata: TraceMetadata          ## Program name, args, workdir
    eventCount: uint64               ## Total events written
    chunkEventCount: int             ## Events in current chunk
    chunkThreshold: int              ## Events per chunk
    closed: bool
    filePath: string                 ## Path to .ct file

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

proc flushChunk(w: var TraceWriter): Result[void, string] =
  ## Compress the current encoder buffer as a single Zstd frame, write
  ## a chunk header + compressed data to the CTFS events.log file.
  if w.chunkEventCount == 0:
    return ok()

  let rawData = w.encoder.getBytes()
  if rawData.len == 0:
    w.encoder.clear()
    w.chunkEventCount = 0
    return ok()

  # Compress with Zstd
  let srcSize = csize_t(rawData.len)
  let bound = ZSTD_compressBound(srcSize)
  var compressed = newSeq[byte](int(bound))

  let compressedSize = ZSTD_compress(
    addr compressed[0], csize_t(compressed.len),
    unsafeAddr rawData[0], srcSize,
    cint(TraceWriterCompressionLevel),
  )

  if ZSTD_isError(compressedSize) != 0:
    return err("zstd compression failed")

  compressed.setLen(int(compressedSize))

  # Write inline chunk header (16 bytes)
  let firstGeid = w.eventCount - uint64(w.chunkEventCount)
  let header = encodeChunkHeader(ChunkIndexEntry(
    compressedSize: uint32(compressedSize),
    eventCount: uint32(w.chunkEventCount),
    firstGeid: firstGeid,
  ))

  let headerRes = w.ctfs.writeToFile(w.eventsFile, header)
  if headerRes.isErr:
    return err("failed to write chunk header: " & headerRes.error)

  # Write compressed data
  let dataRes = w.ctfs.writeToFile(w.eventsFile, compressed)
  if dataRes.isErr:
    return err("failed to write compressed chunk: " & dataRes.error)

  # Sync the entry for concurrent readers
  w.ctfs.syncEntry(w.eventsFile)

  # Reset for next chunk
  w.encoder.clear()
  w.chunkEventCount = 0
  ok()

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

proc newTraceWriter*(path: string, program: string, args: seq[string],
    workdir: string = "",
    chunkThreshold: int = DefaultChunkThreshold): Result[TraceWriter, string] =
  ## Create a new TraceWriter that writes a .ct file at the given path.
  ## Uses streaming CTFS mode so the file can be read concurrently.
  let ctfsRes = createCtfsStreaming(path)
  if ctfsRes.isErr:
    return err("failed to create CTFS container: " & ctfsRes.error)

  var w = TraceWriter(
    ctfs: ctfsRes.get(),
    encoder: SplitBinaryEncoder.init(),
    paths: @[],
    metadata: TraceMetadata(
      program: program,
      args: args,
      workdir: workdir,
    ),
    eventCount: 0,
    chunkEventCount: 0,
    chunkThreshold: chunkThreshold,
    closed: false,
    filePath: path,
  )

  # Add events.log internal file
  let eventsRes = w.ctfs.addFile("events.log")
  if eventsRes.isErr:
    return err("failed to add events.log: " & eventsRes.error)
  w.eventsFile = eventsRes.get()

  ok(w)

proc writeEvent*(w: var TraceWriter, event: TraceLowLevelEvent): Result[void, string] =
  ## Write a single event. Events are buffered and compressed in chunks.
  if w.closed:
    return err("TraceWriter is already closed")

  # Track paths
  if event.kind == tlePath:
    w.paths.add(event.path)

  w.encoder.encodeEvent(event)
  w.eventCount += 1
  w.chunkEventCount += 1

  # Flush chunk when threshold is reached
  if w.chunkEventCount >= w.chunkThreshold:
    return w.flushChunk()

  ok()

# ---------------------------------------------------------------------------
# Convenience procs
# ---------------------------------------------------------------------------

proc writeStep*(w: var TraceWriter, pathId: uint64, line: int64): Result[void, string] =
  w.writeEvent(TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(pathId), line: Line(line))))

proc writePath*(w: var TraceWriter, path: string): Result[void, string] =
  w.writeEvent(TraceLowLevelEvent(kind: tlePath, path: path))

proc writeCall*(w: var TraceWriter, functionId: uint64): Result[void, string] =
  w.writeEvent(TraceLowLevelEvent(kind: tleCall,
    callRecord: CallRecord(functionId: FunctionId(functionId), args: @[])))

proc writeReturn*(w: var TraceWriter): Result[void, string] =
  w.writeEvent(TraceLowLevelEvent(kind: tleReturn,
    returnRecord: ReturnRecord(returnValue: NoneValue)))

proc writeFunction*(w: var TraceWriter, pathId: uint64, line: int64,
                    name: string): Result[void, string] =
  w.writeEvent(TraceLowLevelEvent(kind: tleFunction,
    functionRecord: FunctionRecord(
      pathId: PathId(pathId), line: Line(line), name: name)))

proc writeValue*(w: var TraceWriter, variableId: uint64,
    value: ValueRecord): Result[void, string] =
  w.writeEvent(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(
      variableId: VariableId(variableId), value: value)))

# ---------------------------------------------------------------------------
# Close
# ---------------------------------------------------------------------------

proc close*(w: var TraceWriter): Result[void, string] =
  ## Flush remaining events, write metadata files, and close the container.
  if w.closed:
    return ok()

  # Flush remaining events as final chunk
  let flushRes = w.flushChunk()
  if flushRes.isErr:
    return err("failed to flush final chunk: " & flushRes.error)

  # Write events.fmt
  block:
    let fmtRes = w.ctfs.addFile("events.fmt")
    if fmtRes.isErr:
      return err("failed to add events.fmt: " & fmtRes.error)
    var fmtFile = fmtRes.get()
    let fmtContent = "split-binary"
    var fmtBytes = newSeq[byte](fmtContent.len)
    for i in 0 ..< fmtContent.len:
      fmtBytes[i] = byte(fmtContent[i])
    let writeRes = w.ctfs.writeToFile(fmtFile, fmtBytes)
    if writeRes.isErr:
      return err("failed to write events.fmt: " & writeRes.error)

  # Write meta.json
  block:
    let metaRes = w.ctfs.addFile("meta.json")
    if metaRes.isErr:
      return err("failed to add meta.json: " & metaRes.error)
    var metaFile = metaRes.get()
    var metaJson: string
    try:
      var node = newJObject()
      node["program"] = newJString(w.metadata.program)
      var argsArr = newJArray()
      for arg in w.metadata.args:
        argsArr.add(newJString(arg))
      node["args"] = argsArr
      node["workdir"] = newJString(w.metadata.workdir)
      metaJson = $node
    except ValueError:
      return err("failed to serialize meta.json")
    var metaBytes = newSeq[byte](metaJson.len)
    for i in 0 ..< metaJson.len:
      metaBytes[i] = byte(metaJson[i])
    let writeRes = w.ctfs.writeToFile(metaFile, metaBytes)
    if writeRes.isErr:
      return err("failed to write meta.json: " & writeRes.error)

  # Write paths.json
  block:
    let pathsRes = w.ctfs.addFile("paths.json")
    if pathsRes.isErr:
      return err("failed to add paths.json: " & pathsRes.error)
    var pathsFile = pathsRes.get()
    var pathsJson: string
    try:
      var arr = newJArray()
      for p in w.paths:
        arr.add(newJString(p))
      pathsJson = $arr
    except ValueError:
      return err("failed to serialize paths.json")
    var pathsBytes = newSeq[byte](pathsJson.len)
    for i in 0 ..< pathsJson.len:
      pathsBytes[i] = byte(pathsJson[i])
    let writeRes = w.ctfs.writeToFile(pathsFile, pathsBytes)
    if writeRes.isErr:
      return err("failed to write paths.json: " & writeRes.error)

  # Close CTFS container
  w.ctfs.closeCtfs()
  w.closed = true
  ok()
