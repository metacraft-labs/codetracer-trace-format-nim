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
  RequestType* = enum
    rtFetchBlock     = 0x01
    rtFetchRange     = 0x02
    rtQueryMetadata  = 0x03
    rtQueryStep      = 0x04
    rtQueryValues    = 0x05
    rtQueryCall      = 0x06
    rtQueryEvents    = 0x07
    rtAuthenticate   = 0x08
    rtPing           = 0x09

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
    case kind*: RequestType
    of rtFetchBlock:
      fetchBlock*: FetchBlockRequest
    of rtFetchRange:
      fetchRange*: FetchRangeRequest
    of rtQueryMetadata:
      discard
    of rtQueryStep:
      queryStep*: QueryStepRequest
    of rtQueryValues:
      queryValues*: QueryValuesRequest
    of rtQueryCall:
      queryCall*: QueryCallRequest
    of rtQueryEvents:
      queryEvents*: QueryEventsRequest
    of rtAuthenticate:
      authenticate*: AuthenticateRequest
    of rtPing:
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
  encodeVarintTo(uint64(data.len), buf)
  let start = buf.len
  buf.setLen(start + data.len)
  for i in 0 ..< data.len:
    buf[start + i] = data[i]

proc encodeString*(s: string, buf: var seq[byte]) =
  ## Encode a length-prefixed string.
  encodeVarintTo(uint64(s.len), buf)
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
  buf.add(byte(req.kind))

  case req.kind
  of rtFetchBlock:
    encodeVarintTo(req.fetchBlock.blockId, buf)
  of rtFetchRange:
    encodeVarintTo(req.fetchRange.blockId, buf)
    encodeVarintTo(req.fetchRange.count, buf)
  of rtQueryMetadata:
    discard
  of rtQueryStep:
    encodeVarintTo(req.queryStep.stepIndex, buf)
  of rtQueryValues:
    encodeVarintTo(req.queryValues.stepIndex, buf)
  of rtQueryCall:
    encodeVarintTo(req.queryCall.callKey, buf)
  of rtQueryEvents:
    encodeVarintTo(req.queryEvents.start, buf)
    encodeVarintTo(req.queryEvents.count, buf)
  of rtAuthenticate:
    encodeBytes(req.authenticate.token, buf)
  of rtPing:
    encodeVarintTo(req.ping.nonce, buf)

  result = buf

proc decodeRequest*(buf: openArray[byte]): Result[Request, string] =
  ## Deserialize a request from bytes (without frame).
  if buf.len < 1:
    return err("empty request")

  var pos = 0
  let typeByte = buf[pos]
  pos += 1

  # Validate request type
  var kind: RequestType
  case typeByte
  of 0x01: kind = rtFetchBlock
  of 0x02: kind = rtFetchRange
  of 0x03: kind = rtQueryMetadata
  of 0x04: kind = rtQueryStep
  of 0x05: kind = rtQueryValues
  of 0x06: kind = rtQueryCall
  of 0x07: kind = rtQueryEvents
  of 0x08: kind = rtAuthenticate
  of 0x09: kind = rtPing
  else:
    return err("unknown request type: 0x" & $typeByte)

  case kind
  of rtFetchBlock:
    let v = decodeVarint(buf, pos)
    if v.isErr: return err("FetchBlock: " & v.error)
    var req = Request(kind: rtFetchBlock)
    req.fetchBlock = FetchBlockRequest(blockId: v.get())
    ok(req)
  of rtFetchRange:
    let v1 = decodeVarint(buf, pos)
    if v1.isErr: return err("FetchRange blockId: " & v1.error)
    let v2 = decodeVarint(buf, pos)
    if v2.isErr: return err("FetchRange count: " & v2.error)
    var req = Request(kind: rtFetchRange)
    req.fetchRange = FetchRangeRequest(blockId: v1.get(), count: v2.get())
    ok(req)
  of rtQueryMetadata:
    ok(Request(kind: rtQueryMetadata))
  of rtQueryStep:
    let v = decodeVarint(buf, pos)
    if v.isErr: return err("QueryStep: " & v.error)
    var req = Request(kind: rtQueryStep)
    req.queryStep = QueryStepRequest(stepIndex: v.get())
    ok(req)
  of rtQueryValues:
    let v = decodeVarint(buf, pos)
    if v.isErr: return err("QueryValues: " & v.error)
    var req = Request(kind: rtQueryValues)
    req.queryValues = QueryValuesRequest(stepIndex: v.get())
    ok(req)
  of rtQueryCall:
    let v = decodeVarint(buf, pos)
    if v.isErr: return err("QueryCall: " & v.error)
    var req = Request(kind: rtQueryCall)
    req.queryCall = QueryCallRequest(callKey: v.get())
    ok(req)
  of rtQueryEvents:
    let v1 = decodeVarint(buf, pos)
    if v1.isErr: return err("QueryEvents start: " & v1.error)
    let v2 = decodeVarint(buf, pos)
    if v2.isErr: return err("QueryEvents count: " & v2.error)
    var req = Request(kind: rtQueryEvents)
    req.queryEvents = QueryEventsRequest(start: v1.get(), count: v2.get())
    ok(req)
  of rtAuthenticate:
    let tokenRes = decodeBytes(buf, pos)
    if tokenRes.isErr: return err("Authenticate: " & tokenRes.error)
    var req = Request(kind: rtAuthenticate)
    req.authenticate = AuthenticateRequest(token: tokenRes.get())
    ok(req)
  of rtPing:
    let v = decodeVarint(buf, pos)
    if v.isErr: return err("Ping: " & v.error)
    var req = Request(kind: rtPing)
    req.ping = PingRequest(nonce: v.get())
    ok(req)

# ---------------------------------------------------------------------------
# Error response serialization
# ---------------------------------------------------------------------------

proc encodeError*(reqType: uint64, code: ErrorCode, message: string): seq[byte] =
  ## Encode an error response.
  var buf: seq[byte]
  buf.add(byte(rsError))
  encodeVarintTo(reqType, buf)
  encodeVarintTo(uint64(code), buf)
  encodeString(message, buf)
  result = buf

proc encodeAuthResult*(status: uint64, message: string): seq[byte] =
  ## Encode an authentication result.
  var buf: seq[byte]
  buf.add(byte(rsAuthResult))
  encodeVarintTo(status, buf)
  encodeString(message, buf)
  result = buf

proc encodePong*(nonce: uint64): seq[byte] =
  ## Encode a pong response.
  var buf: seq[byte]
  buf.add(byte(rsPong))
  encodeVarintTo(nonce, buf)
  result = buf
