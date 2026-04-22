{.push raises: [].}

## Call stream: stores call records (function invocations) indexed by call_key.
## Uses VariableRecordTable for variable-length records with O(1) access.
##
## Wire format per record:
##   varint functionId
##   signed_varint parentCallKey  (-1 = root)
##   varint entryStep
##   varint exitStep
##   varint depth
##   varint args_count
##     for each arg: varint len + bytes
##   varint return_value_len + bytes (single byte 0xFF for VoidReturn)
##   varint exception_len + bytes (0 if no exception)
##   varint children_count
##     for each child: varint call_key

import results
import ../codetracer_ctfs/types
import ../codetracer_ctfs/variable_record_table
import ./varint

const VoidReturnMarker*: byte = 0xFF  ## 1-byte marker for void returns

type
  CallRecord* = object
    functionId*: uint64
    parentCallKey*: int64   ## -1 for root calls
    entryStep*: uint64
    exitStep*: uint64
    depth*: uint32
    args*: seq[seq[byte]]      ## CBOR-encoded argument values
    returnValue*: seq[byte]    ## CBOR-encoded return value, or [VoidReturnMarker]
    exception*: seq[byte]      ## CBOR-encoded exception, empty if none
    children*: seq[uint64]     ## child call_keys

  CallStreamWriter* = object
    table: VariableRecordTableWriter

  CallStreamReader* = object
    table: VariableRecordTableReader

proc encodeCallRecord*(rec: CallRecord): seq[byte] {.raises: [].} =
  ## Encode a CallRecord into its wire format.
  var buf: seq[byte]

  encodeVarint(rec.functionId, buf)
  encodeSignedVarint(rec.parentCallKey, buf)
  encodeVarint(rec.entryStep, buf)
  encodeVarint(rec.exitStep, buf)
  encodeVarint(uint64(rec.depth), buf)

  # args
  encodeVarint(uint64(rec.args.len), buf)
  for arg in rec.args:
    encodeVarint(uint64(arg.len), buf)
    buf.add(arg)

  # return value
  encodeVarint(uint64(rec.returnValue.len), buf)
  buf.add(rec.returnValue)

  # exception
  encodeVarint(uint64(rec.exception.len), buf)
  buf.add(rec.exception)

  # children
  encodeVarint(uint64(rec.children.len), buf)
  for child in rec.children:
    encodeVarint(child, buf)

  buf

proc decodeCallRecord*(data: openArray[byte]): Result[CallRecord, string] {.raises: [].} =
  ## Decode a CallRecord from its wire format.
  var pos = 0
  var rec: CallRecord

  rec.functionId = ?decodeVarint(data, pos)
  rec.parentCallKey = ?decodeSignedVarint(data, pos)
  rec.entryStep = ?decodeVarint(data, pos)
  rec.exitStep = ?decodeVarint(data, pos)
  rec.depth = uint32(?decodeVarint(data, pos))

  # args
  let argsCount = int(?decodeVarint(data, pos))
  rec.args = newSeq[seq[byte]](argsCount)
  for i in 0 ..< argsCount:
    let argLen = int(?decodeVarint(data, pos))
    if pos + argLen > data.len:
      return err("truncated arg data")
    var arg = newSeq[byte](argLen)
    for j in 0 ..< argLen:
      arg[j] = data[pos + j]
    pos += argLen
    rec.args[i] = arg

  # return value
  let retLen = int(?decodeVarint(data, pos))
  if pos + retLen > data.len:
    return err("truncated return value data")
  rec.returnValue = newSeq[byte](retLen)
  for j in 0 ..< retLen:
    rec.returnValue[j] = data[pos + j]
  pos += retLen

  # exception
  let excLen = int(?decodeVarint(data, pos))
  if pos + excLen > data.len:
    return err("truncated exception data")
  rec.exception = newSeq[byte](excLen)
  for j in 0 ..< excLen:
    rec.exception[j] = data[pos + j]
  pos += excLen

  # children
  let childrenCount = int(?decodeVarint(data, pos))
  rec.children = newSeq[uint64](childrenCount)
  for i in 0 ..< childrenCount:
    rec.children[i] = ?decodeVarint(data, pos)

  ok(rec)

proc initCallStreamWriter*(ctfs: var Ctfs): Result[CallStreamWriter, string] =
  ## Creates "calls" VariableRecordTable (calls.dat + calls.off).
  let tableRes = initVariableRecordTableWriter(ctfs, "calls")
  if tableRes.isErr:
    return err(tableRes.error)
  ok(CallStreamWriter(table: tableRes.get()))

proc writeCall*(ctfs: var Ctfs, w: var CallStreamWriter,
    rec: CallRecord): Result[void, string] =
  ## Write a call record. Records are indexed by call_key (sequential).
  let encoded = encodeCallRecord(rec)
  ctfs.append(w.table, encoded)

proc count*(w: CallStreamWriter): uint64 = w.table.count()

proc initCallStreamReader*(ctfsBytes: openArray[byte],
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): Result[CallStreamReader, string] =
  ## Initialize a reader from raw CTFS container bytes.
  let tableRes = initVariableRecordTableReader(ctfsBytes, "calls",
      blockSize, maxEntries)
  if tableRes.isErr:
    return err(tableRes.error)
  ok(CallStreamReader(table: tableRes.get()))

proc readCall*(r: CallStreamReader,
    callKey: uint64): Result[CallRecord, string] =
  ## Read the call record at the given call_key.
  let dataRes = r.table.read(callKey)
  if dataRes.isErr:
    return err(dataRes.error)
  let data = dataRes.get()
  decodeCallRecord(data)

proc count*(r: CallStreamReader): uint64 = r.table.count()
