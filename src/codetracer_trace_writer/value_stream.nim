{.push raises: [].}

## Value stream: stores variable values per step, parallel-indexed with
## the execution stream.  Record N contains all variables visible at step N.
## Uses VariableRecordTable for variable-length records with O(1) access by
## step index.
##
## Wire format per record:
##   varint count
##   for each variable:
##     varint varnameId
##     varint typeId
##     varint dataLen
##     <dataLen bytes>

import results
import ../codetracer_ctfs/types
import ../codetracer_ctfs/variable_record_table
import ./varint

type
  VariableValue* = object
    varnameId*: uint64
    typeId*: uint64
    data*: seq[byte]  ## CBOR-encoded value bytes

  ValueStreamWriter* = object
    table: VariableRecordTableWriter

  ValueStreamReader* = object
    table: VariableRecordTableReader

proc initValueStreamWriter*(ctfs: var Ctfs): Result[ValueStreamWriter, string] =
  ## Creates "values" VariableRecordTable (values.dat + values.off).
  let tableRes = initVariableRecordTableWriter(ctfs, "values")
  if tableRes.isErr:
    return err(tableRes.error)
  ok(ValueStreamWriter(table: tableRes.get()))

proc writeStepValues*(ctfs: var Ctfs, w: var ValueStreamWriter,
    values: openArray[VariableValue]): Result[void, string] =
  ## Write all variable values for one step.  Call exactly once per step event.
  ## For steps with no values (ThreadSwitch, etc.), pass an empty array.

  # Encode: varint count, then for each: varint varnameId, varint typeId,
  # varint dataLen, data bytes
  var record: seq[byte]
  encodeVarint(uint64(values.len), record)
  for v in values:
    encodeVarint(v.varnameId, record)
    encodeVarint(v.typeId, record)
    encodeVarint(uint64(v.data.len), record)
    record.add(v.data)

  ctfs.append(w.table, record)

proc readStepValues*(r: ValueStreamReader,
    stepIndex: uint64): Result[seq[VariableValue], string] =
  ## Read all variable values for a given step.
  let dataRes = r.table.read(stepIndex)
  if dataRes.isErr:
    return err(dataRes.error)
  let data = dataRes.get()

  if data.len == 0:
    return ok(newSeq[VariableValue]())

  var pos = 0
  let countRes = decodeVarint(data, pos)
  if countRes.isErr:
    return err("corrupt value record: " & countRes.error)
  let count = int(countRes.get())

  var values = newSeq[VariableValue](count)
  for i in 0 ..< count:
    let vnId = ?decodeVarint(data, pos)
    let tId = ?decodeVarint(data, pos)
    let dLen = ?decodeVarint(data, pos)
    if pos + int(dLen) > data.len:
      return err("truncated value data")
    var d = newSeq[byte](int(dLen))
    for j in 0 ..< int(dLen):
      d[j] = data[pos + j]
    pos += int(dLen)
    values[i] = VariableValue(varnameId: vnId, typeId: tId, data: d)

  ok(values)

proc initValueStreamReader*(ctfsBytes: openArray[byte],
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): Result[ValueStreamReader, string] =
  ## Initialize a reader from raw CTFS container bytes.
  let tableRes = initVariableRecordTableReader(ctfsBytes, "values",
      blockSize, maxEntries)
  if tableRes.isErr:
    return err(tableRes.error)
  ok(ValueStreamReader(table: tableRes.get()))

proc count*(r: ValueStreamReader): uint64 = r.table.count()
