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
import ./member_view

export member_view.ContainerImage, member_view.newContainerImage

type
  VariableRecordTableWriter* = object
    dataFile: CtfsInternalFile          ## baseName.dat
    offsetWriter: FixedRecordTableWriter ## baseName.off (u64 offsets)
    currentOffset: uint64               ## running offset in data file
    recordCount: uint64                 ## number of records written

  VariableRecordTableReader* = object
    data: MemberView       ## baseName.dat, in place
    offsets: MemberView    ## baseName.off: u64 LE offsets, read where they
                           ## are used rather than parsed up front

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

proc checkOffsets(offsetLen: int): Result[void, string] =
  if offsetLen mod 8 != 0:
    return err("offset file size not a multiple of 8")
  if offsetLen < 8:
    return err("offset file too small (needs at least initial offset)")
  ok()

proc initVariableRecordTableReader*(ctfsBytes: openArray[byte],
    baseName: string,
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): Result[VariableRecordTableReader, string] =
  ## Initialize a reader from raw CTFS container bytes.
  ## Reads both baseName.dat and baseName.off, copied out of `ctfsBytes`.
  var dataRes = readInternalFile(ctfsBytes, baseName & ".dat", blockSize, maxEntries)
  if dataRes.isErr:
    return err("failed to read data file: " & dataRes.error)

  var offsetDataRes = readInternalFile(ctfsBytes, baseName & ".off", blockSize, maxEntries)
  if offsetDataRes.isErr:
    return err("failed to read offset file: " & offsetDataRes.error)

  ? checkOffsets(offsetDataRes.get().len)
  ok(VariableRecordTableReader(
    data: viewBytes(move dataRes.get()),
    offsets: viewBytes(move offsetDataRes.get()),
  ))

proc initVariableRecordTableReader*(image: ContainerImage,
    baseName: string,
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): Result[VariableRecordTableReader, string] =
  ## Initialize a reader over a container image it shares: both members are
  ## read in place, nothing is copied.
  var dataRes = viewMember(image, baseName & ".dat", blockSize, maxEntries)
  if dataRes.isErr:
    return err("failed to read data file: " & dataRes.error)
  var offsetsRes = viewMember(image, baseName & ".off", blockSize, maxEntries)
  if offsetsRes.isErr:
    return err("failed to read offset file: " & offsetsRes.error)
  ? checkOffsets(offsetsRes.get().len)
  ok(VariableRecordTableReader(data: move dataRes.get(),
    offsets: move offsetsRes.get()))

proc count*(r: VariableRecordTableReader): uint64 =
  ## Number of records. There are N+1 offsets for N records.
  if r.offsets.len < 8:
    return 0
  uint64(r.offsets.len div 8 - 1)

proc read*(r: VariableRecordTableReader,
    index: uint64): Result[seq[byte], string] =
  ## Read the record at the given index, returning its bytes.
  if index >= r.count:
    return err("index out of range: " & $index)

  let startOff = r.offsets.readU64LE(int(index) * 8)
  let endOff = r.offsets.readU64LE(int(index + 1) * 8)
  if endOff < startOff or endOff > uint64(r.data.len):
    return err("record data out of bounds")
  ok(r.data.copyOut(int(startOff), int(endOff - startOff)))
