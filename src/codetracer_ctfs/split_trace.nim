{.push raises: [].}

## Split-by-time trace directories for CTFS.
##
## Long-running recordings can be split into multiple .ct files within a
## single trace directory. Each file is named with a zero-padded 6-digit
## index: 000000.ct, 000001.ct, etc. Each file is a self-contained CTFS
## container with its own block numbering.
##
## Individual split files can be deleted without affecting the remaining
## files — each is independently readable.

import std/[os, strutils, algorithm]
import results
import ./types
import ./container

const
  SplitNameDigits = 6
  SplitExtension = ".ct"
  DefaultMaxSplitSize*: uint64 = 256 * 1024 * 1024  ## 256 MiB per split

type
  SplitTraceWriter* = object
    directory*: string
    currentSplit*: int
    currentCtfs*: Ctfs
    maxSplitSize*: uint64
    splitStarted*: bool  ## true if currentCtfs has been initialized

  SplitTraceReader* = object
    directory*: string
    files*: seq[string]  ## sorted: 000000.ct, 000001.ct, ...

proc splitFileName*(index: int): string =
  ## Generate the filename for a split at the given index.
  ## E.g. index 0 => "000000.ct", index 1 => "000001.ct".
  align($index, SplitNameDigits, '0') & SplitExtension

proc currentSplitPath*(w: SplitTraceWriter): string =
  ## Return the full path to the current split file.
  w.directory / splitFileName(w.currentSplit)

proc initSplitTraceWriter*(directory: string,
    maxSplitSize: uint64 = DefaultMaxSplitSize): Result[SplitTraceWriter, string] =
  ## Initialize a split trace writer. Creates the directory if it doesn't exist.
  ## Does not create the first split file — call startNewSplit() to begin writing.
  try:
    createDir(directory)
  except IOError, OSError:
    return err("failed to create directory: " & directory)

  ok(SplitTraceWriter(
    directory: directory,
    currentSplit: -1,
    maxSplitSize: maxSplitSize,
    splitStarted: false,
  ))

proc startNewSplit*(w: var SplitTraceWriter): Result[void, string] =
  ## Finalize the current split file (if any) and start a new one.
  ## The new file uses the next sequential index.

  # Close/write the current split if one is open.
  if w.splitStarted:
    let path = w.currentSplitPath()
    let writeRes = w.currentCtfs.writeCtfsToFile(path)
    if writeRes.isErr:
      return err("failed to write split: " & writeRes.error)
    w.currentCtfs.closeCtfs()

  w.currentSplit += 1
  w.currentCtfs = createCtfs()
  w.splitStarted = true
  ok()

proc finalize*(w: var SplitTraceWriter): Result[void, string] =
  ## Finalize the writer: write the current split file to disk and close it.
  if w.splitStarted:
    let path = w.currentSplitPath()
    let writeRes = w.currentCtfs.writeCtfsToFile(path)
    if writeRes.isErr:
      return err("failed to write final split: " & writeRes.error)
    w.currentCtfs.closeCtfs()
    w.splitStarted = false
  ok()

proc currentSize*(w: SplitTraceWriter): uint64 =
  ## Return the current size of the in-memory CTFS container (approximate).
  if w.splitStarted:
    uint64(w.currentCtfs.data.len)
  else:
    0'u64

proc shouldSplit*(w: SplitTraceWriter): bool =
  ## Return true if the current split has exceeded the max split size.
  w.splitStarted and w.currentSize() >= w.maxSplitSize

proc splitCount*(w: SplitTraceWriter): int =
  ## Return the number of splits created so far (including current).
  if w.currentSplit < 0: 0
  else: w.currentSplit + 1

# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

proc openSplitTrace*(directory: string): Result[SplitTraceReader, string] =
  ## Open a trace directory. Discovers all .ct files in sorted order.
  if not dirExists(directory):
    return err("not a directory: " & directory)

  var files: seq[string]
  try:
    for entry in walkDir(directory):
      if entry.kind == pcFile and entry.path.endsWith(SplitExtension):
        files.add(entry.path)
  except OSError:
    return err("failed to read directory: " & directory)

  files.sort()

  if files.len == 0:
    return err("no .ct files in: " & directory)

  ok(SplitTraceReader(directory: directory, files: files))

proc splitCount*(r: SplitTraceReader): int =
  ## Return the number of split files discovered.
  r.files.len

proc splitPath*(r: SplitTraceReader, index: int): Result[string, string] =
  ## Return the path to a specific split file.
  if index < 0 or index >= r.files.len:
    return err("split index out of range: " & $index)
  ok(r.files[index])

proc readSplitBytes*(r: SplitTraceReader, index: int): Result[seq[byte], string] =
  ## Read the raw bytes of a specific split file.
  if index < 0 or index >= r.files.len:
    return err("split index out of range: " & $index)

  try:
    let path = r.files[index]
    let f = open(path, fmRead)
    let size = f.getFileSize()
    var data = newSeq[byte](size)
    if size > 0:
      discard f.readBytes(data, 0, size)
    f.close()
    ok(data)
  except IOError, OSError:
    err("failed to read split file: " & r.files[index])

proc isValidCtfs*(data: openArray[byte]): bool =
  ## Check if the given bytes start with a valid CTFS magic header.
  if data.len < int(HeaderSize):
    return false
  data[0] == CtfsMagic[0] and
    data[1] == CtfsMagic[1] and
    data[2] == CtfsMagic[2] and
    data[3] == CtfsMagic[3] and
    data[4] == CtfsMagic[4]

proc deleteSplit*(r: var SplitTraceReader, index: int): Result[void, string] =
  ## Delete a split file. Remaining files stay playable per spec.
  if index < 0 or index >= r.files.len:
    return err("split index out of range: " & $index)
  try:
    removeFile(r.files[index])
    r.files.delete(index)
    ok()
  except OSError:
    err("failed to delete: " & r.files[index])
