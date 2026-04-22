{.push raises: [].}

## IO event stream: stores IO events (stdout, stderr, file ops, errors)
## indexed by event index.  Uses VariableRecordTable for variable-length
## records with O(1) access.
##
## Wire format per event:
##   u8 kind
##   varint stepId
##   varint data_len + bytes

import results
import ../codetracer_ctfs/types
import ../codetracer_ctfs/variable_record_table
import ./varint

type
  IOEventKind* = enum
    ioStdout = 0
    ioStderr = 1
    ioFileOp = 2
    ioError = 3

  IOEvent* = object
    kind*: IOEventKind
    stepId*: uint64
    data*: seq[byte]  ## content bytes

  IOEventStreamWriter* = object
    table: VariableRecordTableWriter

  IOEventStreamReader* = object
    table: VariableRecordTableReader

proc encodeIOEvent*(ev: IOEvent): seq[byte] {.raises: [].} =
  ## Encode an IOEvent into its wire format.
  var buf: seq[byte]
  buf.add(byte(ev.kind))
  encodeVarint(ev.stepId, buf)
  encodeVarint(uint64(ev.data.len), buf)
  buf.add(ev.data)
  buf

proc decodeIOEvent*(data: openArray[byte]): Result[IOEvent, string] {.raises: [].} =
  ## Decode an IOEvent from its wire format.
  if data.len < 1:
    return err("IO event data too short")

  var pos = 0
  let kindByte = data[pos]
  pos += 1

  if kindByte > byte(high(IOEventKind)):
    return err("invalid IO event kind: " & $kindByte)
  let kind = IOEventKind(kindByte)

  let stepId = ?decodeVarint(data, pos)
  let dataLen = int(?decodeVarint(data, pos))

  if pos + dataLen > data.len:
    return err("truncated IO event data")

  var evData = newSeq[byte](dataLen)
  for i in 0 ..< dataLen:
    evData[i] = data[pos + i]

  ok(IOEvent(kind: kind, stepId: stepId, data: evData))

proc initIOEventStreamWriter*(ctfs: var Ctfs): Result[IOEventStreamWriter, string] =
  ## Creates "events" VariableRecordTable (events.dat + events.off).
  let tableRes = initVariableRecordTableWriter(ctfs, "events")
  if tableRes.isErr:
    return err(tableRes.error)
  ok(IOEventStreamWriter(table: tableRes.get()))

proc writeEvent*(ctfs: var Ctfs, w: var IOEventStreamWriter,
    ev: IOEvent): Result[void, string] =
  ## Write an IO event. Events are indexed sequentially.
  let encoded = encodeIOEvent(ev)
  ctfs.append(w.table, encoded)

proc count*(w: IOEventStreamWriter): uint64 = w.table.count()

proc initIOEventStreamReader*(ctfsBytes: openArray[byte],
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): Result[IOEventStreamReader, string] =
  ## Initialize a reader from raw CTFS container bytes.
  let tableRes = initVariableRecordTableReader(ctfsBytes, "events",
      blockSize, maxEntries)
  if tableRes.isErr:
    return err(tableRes.error)
  ok(IOEventStreamReader(table: tableRes.get()))

proc readEvent*(r: IOEventStreamReader,
    index: uint64): Result[IOEvent, string] =
  ## Read the IO event at the given index.
  let dataRes = r.table.read(index)
  if dataRes.isErr:
    return err(dataRes.error)
  decodeIOEvent(dataRes.get())

proc count*(r: IOEventStreamReader): uint64 = r.table.count()
