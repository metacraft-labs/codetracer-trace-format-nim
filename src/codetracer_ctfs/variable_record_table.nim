when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## VariableRecordTable: stores variable-length records using two CTFS files:
##   - baseName.dat — data file, records appended sequentially
##   - baseName.off — offset file, a FixedRecordTable of u64 LE offsets
##
## To read record i: read offset[i] from the offset file (O(1)),
## compute length as offset[i+1] - offset[i], then read from the data file.

import results
import ./types
import ./container
import ./fixed_record_table

type
  VariableRecordTableWriter* = object
    dataFile: CtfsInternalFile          ## baseName.dat
    offsetWriter: FixedRecordTableWriter ## baseName.off (u64 offsets)
    currentOffset: uint64               ## running offset in data file
    recordCount: uint64                 ## number of records written

  VariableRecordTableReader* = object
    data: seq[byte]        ## raw baseName.dat content
    offsets: seq[uint64]   ## parsed from baseName.off (each is u64 LE)

proc initVariableRecordTableWriter*(ctfs: var Ctfs,
    baseName: string): Result[VariableRecordTableWriter, string] =
  ## Create a new variable-record table in the CTFS container.
  ## Creates baseName.dat (data) and baseName.off (offsets).
  let dataFileRes = ctfs.addFile(baseName & ".dat")
  if dataFileRes.isErr:
    return err("failed to create data file: " & dataFileRes.error)

  let offsetWriterRes = initFixedRecordTableWriter(ctfs, baseName & ".off", 8)
  if offsetWriterRes.isErr:
    return err("failed to create offset file: " & offsetWriterRes.error)

  var writer = VariableRecordTableWriter(
    dataFile: dataFileRes.get(),
    offsetWriter: offsetWriterRes.get(),
    currentOffset: 0,
    recordCount: 0,
  )

  # Write initial offset 0
  var offsetBytes: array[8, byte]
  writeU64LE(offsetBytes, 0, 0'u64)
  let appendRes = ctfs.append(writer.offsetWriter, offsetBytes)
  if appendRes.isErr:
    return err("failed to write initial offset: " & appendRes.error)

  ok(writer)

proc append*(ctfs: var Ctfs, w: var VariableRecordTableWriter,
    record: openArray[byte]): Result[void, string] =
  ## Append a variable-length record. May be zero-length.
  if record.len > 0:
    let writeRes = ctfs.writeToFile(w.dataFile, record)
    if writeRes.isErr:
      return err("data write failed: " & writeRes.error)

  w.currentOffset += uint64(record.len)
  w.recordCount += 1

  # Write the new cumulative offset to the offset file
  var offsetBytes: array[8, byte]
  writeU64LE(offsetBytes, 0, w.currentOffset)
  let appendRes = ctfs.append(w.offsetWriter, offsetBytes)
  if appendRes.isErr:
    return err("offset write failed: " & appendRes.error)

  ok()

proc count*(w: VariableRecordTableWriter): uint64 = w.recordCount

proc initVariableRecordTableReader*(ctfsBytes: openArray[byte],
    baseName: string,
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): Result[VariableRecordTableReader, string] =
  ## Initialize a reader from raw CTFS container bytes.
  ## Reads both baseName.dat and baseName.off.
  let dataRes = readInternalFile(ctfsBytes, baseName & ".dat", blockSize, maxEntries)
  if dataRes.isErr:
    return err("failed to read data file: " & dataRes.error)

  let offsetDataRes = readInternalFile(ctfsBytes, baseName & ".off", blockSize, maxEntries)
  if offsetDataRes.isErr:
    return err("failed to read offset file: " & offsetDataRes.error)

  let offsetData = offsetDataRes.get()
  if offsetData.len mod 8 != 0:
    return err("offset file size not a multiple of 8")
  if offsetData.len < 8:
    return err("offset file too small (needs at least initial offset)")

  let numOffsets = offsetData.len div 8
  var offsets = newSeq[uint64](numOffsets)
  for i in 0 ..< numOffsets:
    offsets[i] = readU64LE(offsetData, i * 8)

  ok(VariableRecordTableReader(
    data: dataRes.get(),
    offsets: offsets,
  ))

proc count*(r: VariableRecordTableReader): uint64 =
  ## Number of records. There are N+1 offsets for N records.
  if r.offsets.len == 0:
    return 0
  uint64(r.offsets.len - 1)

proc read*(r: VariableRecordTableReader,
    index: uint64): Result[seq[byte], string] =
  ## Read the record at the given index, returning its bytes.
  if index >= r.count:
    return err("index out of range: " & $index)

  let startOff = r.offsets[index]
  let endOff = r.offsets[index + 1]
  let length = int(endOff - startOff)

  if length == 0:
    return ok(newSeq[byte](0))

  let start = int(startOff)
  if start + length > r.data.len:
    return err("record data out of bounds")

  var record = newSeq[byte](length)
  for i in 0 ..< length:
    record[i] = r.data[start + i]
  ok(record)
