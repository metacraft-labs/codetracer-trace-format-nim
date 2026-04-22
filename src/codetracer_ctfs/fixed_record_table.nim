when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## FixedRecordTable: stores N records of constant size S in a CTFS internal file.
## Record i is at byte offset i*S, so seeking is O(1).
## Records can straddle block boundaries — the reader reads across blocks.

import results
import ./types
import ./container

type
  FixedRecordTableWriter* = object
    file: CtfsInternalFile
    recordSize: int
    count: uint64

  FixedRecordTableReader* = object
    data: seq[byte]  # raw file data
    recordSize: int

proc initFixedRecordTableWriter*(ctfs: var Ctfs, name: string,
                                  recordSize: int): Result[FixedRecordTableWriter, string] =
  ## Create a new fixed-record table in the CTFS container.
  if recordSize <= 0:
    return err("recordSize must be positive")
  let fileRes = ctfs.addFile(name)
  if fileRes.isErr:
    return err("failed to create file: " & fileRes.error)
  ok(FixedRecordTableWriter(file: fileRes.get(), recordSize: recordSize, count: 0))

proc append*(ctfs: var Ctfs, w: var FixedRecordTableWriter,
    record: openArray[byte]): Result[void, string] =
  ## Append a fixed-size record. record.len must equal recordSize.
  if record.len != w.recordSize:
    return err("record size mismatch: expected " & $w.recordSize & ", got " & $record.len)
  let res = ctfs.writeToFile(w.file, record)
  if res.isErr:
    return err("write failed: " & res.error)
  w.count += 1
  ok()

proc count*(w: FixedRecordTableWriter): uint64 = w.count

# Reader - works on raw bytes from readInternalFile
proc initFixedRecordTableReader*(data: seq[byte],
                                  recordSize: int): Result[FixedRecordTableReader, string] =
  if recordSize <= 0:
    return err("recordSize must be positive")
  ok(FixedRecordTableReader(data: data, recordSize: recordSize))

proc count*(r: FixedRecordTableReader): uint64 =
  uint64(r.data.len div r.recordSize)

proc read*(r: FixedRecordTableReader, index: uint64,
    output: var openArray[byte]): Result[void, string] =
  ## Read record at index into output buffer. output.len must be >= recordSize.
  let offset = int(index) * r.recordSize
  if offset + r.recordSize > r.data.len:
    return err("index out of range: " & $index)
  if output.len < r.recordSize:
    return err("output buffer too small")
  for i in 0 ..< r.recordSize:
    output[i] = r.data[offset + i]
  ok()
