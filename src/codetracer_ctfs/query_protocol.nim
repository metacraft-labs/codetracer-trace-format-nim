{.push raises: [].}

## Storage Query Protocol definitions (M49).
##
## Defines message types, request/response enums, and serialization
## helpers for the CTFS remote query protocol.
##
## This module contains only the protocol definition and serialization.
## No networking code is included.

import results
import codetracer_trace_writer/varint

# ---------------------------------------------------------------------------
# Message type codes
# ---------------------------------------------------------------------------

type
  RequestKind* = enum
    ## Wire values: use `requestKindToByte` / `byteToRequestKind` for mapping.
    rkFetchBlock
    rkFetchRange
    rkQueryMetadata
    rkQueryStep
    rkQueryValues
    rkQueryCall
    rkQueryEvents
    rkAuthenticate
    rkPing

  ResponseType* = enum
    rsBlockData      = 0x81
    rsRangeData      = 0x82
    rsMetadataResult = 0x83
    rsStepResult     = 0x84
    rsValuesResult   = 0x85
    rsCallResult     = 0x86
    rsEventsResult   = 0x87
    rsAuthResult     = 0x88
    rsPong           = 0x89
    rsError          = 0xFE

  ErrorCode* = enum
    ecNone             = 0
    ecNotAuthenticated = 1
    ecUnknownRequest   = 2
    ecOutOfRange       = 3
    ecInternal         = 255

  CompressionFlag* = enum
    cfNone = 0x00
    cfZstd = 0x01

const
  RequestKindWireBytes*: array[RequestKind, byte] = [
    rkFetchBlock:    0x01'u8,
    rkFetchRange:    0x02,
    rkQueryMetadata: 0x03,
    rkQueryStep:     0x04,
    rkQueryValues:   0x05,
    rkQueryCall:     0x06,
    rkQueryEvents:   0x07,
    rkAuthenticate:  0x08,
    rkPing:          0x09,
  ]

proc requestKindToByte*(k: RequestKind): byte =
  RequestKindWireBytes[k]

proc byteToRequestKind*(b: byte): Result[RequestKind, string] =
  for k in RequestKind:
    if RequestKindWireBytes[k] == b:
      return ok(k)
  err("unknown request type: 0x" & $int(b))

# ---------------------------------------------------------------------------
# Request structures
# ---------------------------------------------------------------------------

type
  FetchBlockRequest* = object
    blockId*: uint64

  FetchRangeRequest* = object
    blockId*: uint64
    count*: uint64

  QueryStepRequest* = object
    stepIndex*: uint64

  QueryValuesRequest* = object
    stepIndex*: uint64

  QueryCallRequest* = object
    callKey*: uint64

  QueryEventsRequest* = object
    start*: uint64
    count*: uint64

  AuthenticateRequest* = object
    token*: seq[byte]

  PingRequest* = object
    nonce*: uint64

  Request* = object
    case kind*: RequestKind
    of rkFetchBlock:
      fetchBlock*: FetchBlockRequest
    of rkFetchRange:
      fetchRange*: FetchRangeRequest
    of rkQueryMetadata:
      discard
    of rkQueryStep:
      queryStep*: QueryStepRequest
    of rkQueryValues:
      queryValues*: QueryValuesRequest
    of rkQueryCall:
      queryCall*: QueryCallRequest
    of rkQueryEvents:
      queryEvents*: QueryEventsRequest
    of rkAuthenticate:
      authenticate*: AuthenticateRequest
    of rkPing:
      ping*: PingRequest

# ---------------------------------------------------------------------------
# Response structures
# ---------------------------------------------------------------------------

type
  ValueEntry* = object
    varnameId*: uint64
    typeId*: uint64
    data*: seq[byte]

  ErrorResponse* = object
    requestType*: uint64
    errorCode*: uint64
    message*: string

  MetadataResponse* = object
    program*: string
    workdir*: string
    args*: seq[string]
    paths*: seq[string]
    totalSteps*: uint64
    recorderId*: string

  PongResponse* = object
    nonce*: uint64

  AuthResponse* = object
    status*: uint64
    message*: string

# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

proc encodeBytes*(data: openArray[byte], buf: var seq[byte]) =
  ## Encode a length-prefixed byte sequence.
  encodeVarint(uint64(data.len), buf)
  let start = buf.len
  buf.setLen(start + data.len)
  for i in 0 ..< data.len:
    buf[start + i] = data[i]

proc encodeString*(s: string, buf: var seq[byte]) =
  ## Encode a length-prefixed string.
  encodeVarint(uint64(s.len), buf)
  let start = buf.len
  buf.setLen(start + s.len)
  for i in 0 ..< s.len:
    buf[start + i] = byte(s[i])

proc decodeBytes*(buf: openArray[byte], pos: var int): Result[seq[byte], string] =
  ## Decode a length-prefixed byte sequence.
  let lenRes = decodeVarint(buf, pos)
  if lenRes.isErr:
    return err("failed to decode bytes length: " & lenRes.error)
  let length = int(lenRes.get())
  if pos + length > buf.len:
    return err("bytes length exceeds buffer: need " & $length &
      " have " & $(buf.len - pos))
  var data = newSeq[byte](length)
  for i in 0 ..< length:
    data[i] = buf[pos + i]
  pos += length
  ok(data)

proc decodeString*(buf: openArray[byte], pos: var int): Result[string, string] =
  ## Decode a length-prefixed string.
  let bytesRes = decodeBytes(buf, pos)
  if bytesRes.isErr:
    return err(bytesRes.error)
  let data = bytesRes.get()
  var s = newString(data.len)
  for i in 0 ..< data.len:
    s[i] = char(data[i])
  ok(s)

# ---------------------------------------------------------------------------
# Frame encoding/decoding
# ---------------------------------------------------------------------------

proc encodeFrame*(payload: openArray[byte]): seq[byte] =
  ## Wrap a payload in a length-prefixed frame.
  ## Frame format: [length: 4 bytes LE uint32][payload]
  let length = uint32(payload.len)
  result = newSeq[byte](4 + payload.len)
  result[0] = byte(length and 0xFF)
  result[1] = byte((length shr 8) and 0xFF)
  result[2] = byte((length shr 16) and 0xFF)
  result[3] = byte((length shr 24) and 0xFF)
  for i in 0 ..< payload.len:
    result[4 + i] = payload[i]

proc decodeFrameLength*(header: openArray[byte]): Result[uint32, string] =
  ## Decode the 4-byte frame length header.
  if header.len < 4:
    return err("frame header too short")
  let length =
    uint32(header[0]) or
    (uint32(header[1]) shl 8) or
    (uint32(header[2]) shl 16) or
    (uint32(header[3]) shl 24)
  ok(length)

# ---------------------------------------------------------------------------
# Request serialization
# ---------------------------------------------------------------------------

proc encodeRequest*(req: Request): seq[byte] =
  ## Serialize a request to bytes (without frame).
  var buf: seq[byte]
  buf.add(requestKindToByte(req.kind))

  case req.kind
  of rkFetchBlock:
    encodeVarint(req.fetchBlock.blockId, buf)
  of rkFetchRange:
    encodeVarint(req.fetchRange.blockId, buf)
    encodeVarint(req.fetchRange.count, buf)
  of rkQueryMetadata:
    discard
  of rkQueryStep:
    encodeVarint(req.queryStep.stepIndex, buf)
  of rkQueryValues:
    encodeVarint(req.queryValues.stepIndex, buf)
  of rkQueryCall:
    encodeVarint(req.queryCall.callKey, buf)
  of rkQueryEvents:
    encodeVarint(req.queryEvents.start, buf)
    encodeVarint(req.queryEvents.count, buf)
  of rkAuthenticate:
    encodeBytes(req.authenticate.token, buf)
  of rkPing:
    encodeVarint(req.ping.nonce, buf)

  result = buf

proc decodeRequest*(buf: openArray[byte]): Result[Request, string] =
  ## Deserialize a request from bytes (without frame).
  if buf.len < 1:
    return err("empty request")

  var pos = 0
  let typeByte = buf[pos]
  pos += 1

  let kindRes = byteToRequestKind(typeByte)
  if kindRes.isErr:
    return err(kindRes.error)
  let kind = kindRes.get()

  case kind
  of rkFetchBlock:
    let v = decodeVarint(buf, pos)
    if v.isErr: return err("FetchBlock: " & v.error)
    var req = Request(kind: rkFetchBlock)
    req.fetchBlock = FetchBlockRequest(blockId: v.get())
    ok(req)
  of rkFetchRange:
    let v1 = decodeVarint(buf, pos)
    if v1.isErr: return err("FetchRange blockId: " & v1.error)
    let v2 = decodeVarint(buf, pos)
    if v2.isErr: return err("FetchRange count: " & v2.error)
    var req = Request(kind: rkFetchRange)
    req.fetchRange = FetchRangeRequest(blockId: v1.get(), count: v2.get())
    ok(req)
  of rkQueryMetadata:
    ok(Request(kind: rkQueryMetadata))
  of rkQueryStep:
    let v = decodeVarint(buf, pos)
    if v.isErr: return err("QueryStep: " & v.error)
    var req = Request(kind: rkQueryStep)
    req.queryStep = QueryStepRequest(stepIndex: v.get())
    ok(req)
  of rkQueryValues:
    let v = decodeVarint(buf, pos)
    if v.isErr: return err("QueryValues: " & v.error)
    var req = Request(kind: rkQueryValues)
    req.queryValues = QueryValuesRequest(stepIndex: v.get())
    ok(req)
  of rkQueryCall:
    let v = decodeVarint(buf, pos)
    if v.isErr: return err("QueryCall: " & v.error)
    var req = Request(kind: rkQueryCall)
    req.queryCall = QueryCallRequest(callKey: v.get())
    ok(req)
  of rkQueryEvents:
    let v1 = decodeVarint(buf, pos)
    if v1.isErr: return err("QueryEvents start: " & v1.error)
    let v2 = decodeVarint(buf, pos)
    if v2.isErr: return err("QueryEvents count: " & v2.error)
    var req = Request(kind: rkQueryEvents)
    req.queryEvents = QueryEventsRequest(start: v1.get(), count: v2.get())
    ok(req)
  of rkAuthenticate:
    let tokenRes = decodeBytes(buf, pos)
    if tokenRes.isErr: return err("Authenticate: " & tokenRes.error)
    var req = Request(kind: rkAuthenticate)
    req.authenticate = AuthenticateRequest(token: tokenRes.get())
    ok(req)
  of rkPing:
    let v = decodeVarint(buf, pos)
    if v.isErr: return err("Ping: " & v.error)
    var req = Request(kind: rkPing)
    req.ping = PingRequest(nonce: v.get())
    ok(req)

# ---------------------------------------------------------------------------
# Error response serialization
# ---------------------------------------------------------------------------

proc encodeError*(reqType: uint64, code: ErrorCode, message: string): seq[byte] =
  ## Encode an error response.
  var buf: seq[byte]
  buf.add(byte(rsError))
  encodeVarint(reqType, buf)
  encodeVarint(uint64(code), buf)
  encodeString(message, buf)
  result = buf

proc decodeErrorResponse*(buf: openArray[byte], pos: var int): Result[ErrorResponse, string] =
  ## Decode an error response body (after the type byte has been consumed).
  let reqTypeRes = decodeVarint(buf, pos)
  if reqTypeRes.isErr: return err("error reqType: " & reqTypeRes.error)
  let codeRes = decodeVarint(buf, pos)
  if codeRes.isErr: return err("error code: " & codeRes.error)
  let msgRes = decodeString(buf, pos)
  if msgRes.isErr: return err("error message: " & msgRes.error)
  ok(ErrorResponse(
    requestType: reqTypeRes.get(),
    errorCode: codeRes.get(),
    message: msgRes.get()
  ))

proc encodeAuthResult*(status: uint64, message: string): seq[byte] =
  ## Encode an authentication result.
  var buf: seq[byte]
  buf.add(byte(rsAuthResult))
  encodeVarint(status, buf)
  encodeString(message, buf)
  result = buf

proc decodeAuthResponse*(buf: openArray[byte], pos: var int): Result[AuthResponse, string] =
  ## Decode an auth response body (after the type byte has been consumed).
  let statusRes = decodeVarint(buf, pos)
  if statusRes.isErr: return err("auth status: " & statusRes.error)
  let msgRes = decodeString(buf, pos)
  if msgRes.isErr: return err("auth message: " & msgRes.error)
  ok(AuthResponse(status: statusRes.get(), message: msgRes.get()))

proc encodePong*(nonce: uint64): seq[byte] =
  ## Encode a pong response.
  var buf: seq[byte]
  buf.add(byte(rsPong))
  encodeVarint(nonce, buf)
  result = buf

proc decodePongResponse*(buf: openArray[byte], pos: var int): Result[PongResponse, string] =
  ## Decode a pong response body (after the type byte has been consumed).
  let nonceRes = decodeVarint(buf, pos)
  if nonceRes.isErr: return err("pong nonce: " & nonceRes.error)
  ok(PongResponse(nonce: nonceRes.get()))

proc encodeBlockData*(blockId: uint64, data: openArray[byte]): seq[byte] =
  ## Encode a block data response.
  var buf: seq[byte]
  buf.add(byte(rsBlockData))
  encodeVarint(blockId, buf)
  encodeBytes(data, buf)
  result = buf

proc decodeBlockDataResponse*(buf: openArray[byte], pos: var int): Result[(uint64, seq[byte]), string] =
  ## Decode a block data response body (after the type byte).
  ## Returns (blockId, data).
  let blockIdRes = decodeVarint(buf, pos)
  if blockIdRes.isErr: return err("blockData blockId: " & blockIdRes.error)
  let dataRes = decodeBytes(buf, pos)
  if dataRes.isErr: return err("blockData data: " & dataRes.error)
  ok((blockIdRes.get(), dataRes.get()))

proc responseTypeByte*(buf: openArray[byte]): Result[ResponseType, string] =
  ## Read the response type byte from a response payload.
  if buf.len < 1:
    return err("empty response")
  let b = buf[0]
  case b
  of 0x81: ok(rsBlockData)
  of 0x82: ok(rsRangeData)
  of 0x83: ok(rsMetadataResult)
  of 0x84: ok(rsStepResult)
  of 0x85: ok(rsValuesResult)
  of 0x86: ok(rsCallResult)
  of 0x87: ok(rsEventsResult)
  of 0x88: ok(rsAuthResult)
  of 0x89: ok(rsPong)
  of 0xFE: ok(rsError)
  else: err("unknown response type: 0x" & $int(b))
