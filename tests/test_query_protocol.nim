{.push raises: [].}

## M23c: Integration test for the smart query protocol (M49).
##
## Tests round-trip serialization/deserialization for all request/response
## types, error responses, and frame encoding/decoding.
## No networking — pure protocol serialization tests.

import results
import codetracer_ctfs/query_protocol

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc assertEq[T](a, b: T, msg: string) =
  doAssert a == b, msg & ": expected " & $b & ", got " & $a

# ---------------------------------------------------------------------------
# test_fetch_block_roundtrip
# ---------------------------------------------------------------------------

proc test_fetch_block_roundtrip() {.raises: [].} =
  var req = Request(kind: rkFetchBlock)
  req.fetchBlock = FetchBlockRequest(blockId: 42)

  let encoded = encodeRequest(req)
  let decoded = decodeRequest(encoded)
  doAssert decoded.isOk, "decode failed: " & decoded.error
  let r = decoded.get()
  assertEq(r.kind, rkFetchBlock, "kind")
  assertEq(r.fetchBlock.blockId, 42'u64, "blockId")

  echo "PASS: test_fetch_block_roundtrip"

# ---------------------------------------------------------------------------
# test_fetch_range_roundtrip
# ---------------------------------------------------------------------------

proc test_fetch_range_roundtrip() {.raises: [].} =
  var req = Request(kind: rkFetchRange)
  req.fetchRange = FetchRangeRequest(blockId: 100, count: 10)

  let encoded = encodeRequest(req)
  let decoded = decodeRequest(encoded)
  doAssert decoded.isOk, "decode failed: " & decoded.error
  let r = decoded.get()
  assertEq(r.kind, rkFetchRange, "kind")
  assertEq(r.fetchRange.blockId, 100'u64, "blockId")
  assertEq(r.fetchRange.count, 10'u64, "count")

  echo "PASS: test_fetch_range_roundtrip"

# ---------------------------------------------------------------------------
# test_query_metadata_roundtrip
# ---------------------------------------------------------------------------

proc test_query_metadata_roundtrip() {.raises: [].} =
  let req = Request(kind: rkQueryMetadata)
  let encoded = encodeRequest(req)
  let decoded = decodeRequest(encoded)
  doAssert decoded.isOk, "decode failed: " & decoded.error
  assertEq(decoded.get().kind, rkQueryMetadata, "kind")

  echo "PASS: test_query_metadata_roundtrip"

# ---------------------------------------------------------------------------
# test_query_step_roundtrip
# ---------------------------------------------------------------------------

proc test_query_step_roundtrip() {.raises: [].} =
  var req = Request(kind: rkQueryStep)
  req.queryStep = QueryStepRequest(stepIndex: 999999)

  let encoded = encodeRequest(req)
  let decoded = decodeRequest(encoded)
  doAssert decoded.isOk, "decode failed: " & decoded.error
  let r = decoded.get()
  assertEq(r.kind, rkQueryStep, "kind")
  assertEq(r.queryStep.stepIndex, 999999'u64, "stepIndex")

  echo "PASS: test_query_step_roundtrip"

# ---------------------------------------------------------------------------
# test_query_values_roundtrip
# ---------------------------------------------------------------------------

proc test_query_values_roundtrip() {.raises: [].} =
  var req = Request(kind: rkQueryValues)
  req.queryValues = QueryValuesRequest(stepIndex: 12345)

  let encoded = encodeRequest(req)
  let decoded = decodeRequest(encoded)
  doAssert decoded.isOk, "decode failed: " & decoded.error
  let r = decoded.get()
  assertEq(r.kind, rkQueryValues, "kind")
  assertEq(r.queryValues.stepIndex, 12345'u64, "stepIndex")

  echo "PASS: test_query_values_roundtrip"

# ---------------------------------------------------------------------------
# test_query_call_roundtrip
# ---------------------------------------------------------------------------

proc test_query_call_roundtrip() {.raises: [].} =
  var req = Request(kind: rkQueryCall)
  req.queryCall = QueryCallRequest(callKey: 0xDEADBEEF'u64)

  let encoded = encodeRequest(req)
  let decoded = decodeRequest(encoded)
  doAssert decoded.isOk, "decode failed: " & decoded.error
  let r = decoded.get()
  assertEq(r.kind, rkQueryCall, "kind")
  assertEq(r.queryCall.callKey, 0xDEADBEEF'u64, "callKey")

  echo "PASS: test_query_call_roundtrip"

# ---------------------------------------------------------------------------
# test_query_events_roundtrip
# ---------------------------------------------------------------------------

proc test_query_events_roundtrip() {.raises: [].} =
  var req = Request(kind: rkQueryEvents)
  req.queryEvents = QueryEventsRequest(start: 500, count: 100)

  let encoded = encodeRequest(req)
  let decoded = decodeRequest(encoded)
  doAssert decoded.isOk, "decode failed: " & decoded.error
  let r = decoded.get()
  assertEq(r.kind, rkQueryEvents, "kind")
  assertEq(r.queryEvents.start, 500'u64, "start")
  assertEq(r.queryEvents.count, 100'u64, "count")

  echo "PASS: test_query_events_roundtrip"

# ---------------------------------------------------------------------------
# test_authenticate_roundtrip
# ---------------------------------------------------------------------------

proc test_authenticate_roundtrip() {.raises: [].} =
  var req = Request(kind: rkAuthenticate)
  req.authenticate = AuthenticateRequest(token: @[0x01'u8, 0x02, 0x03, 0xFF])

  let encoded = encodeRequest(req)
  let decoded = decodeRequest(encoded)
  doAssert decoded.isOk, "decode failed: " & decoded.error
  let r = decoded.get()
  assertEq(r.kind, rkAuthenticate, "kind")
  doAssert r.authenticate.token == @[0x01'u8, 0x02, 0x03, 0xFF],
    "token mismatch"

  echo "PASS: test_authenticate_roundtrip"

# ---------------------------------------------------------------------------
# test_ping_roundtrip
# ---------------------------------------------------------------------------

proc test_ping_roundtrip() {.raises: [].} =
  var req = Request(kind: rkPing)
  req.ping = PingRequest(nonce: 0xCAFEBABE'u64)

  let encoded = encodeRequest(req)
  let decoded = decodeRequest(encoded)
  doAssert decoded.isOk, "decode failed: " & decoded.error
  let r = decoded.get()
  assertEq(r.kind, rkPing, "kind")
  assertEq(r.ping.nonce, 0xCAFEBABE'u64, "nonce")

  echo "PASS: test_ping_roundtrip"

# ---------------------------------------------------------------------------
# test_error_response
# ---------------------------------------------------------------------------

proc test_error_response() {.raises: [].} =
  let encoded = encodeError(
    reqType = uint64(requestKindToByte(rkFetchBlock)),
    code = ecOutOfRange,
    message = "block 999 out of range"
  )

  # First byte should be the error response type
  assertEq(encoded[0], byte(rsError), "response type byte")

  # Decode it
  let rtRes = responseTypeByte(encoded)
  doAssert rtRes.isOk, "responseTypeByte failed"
  assertEq(rtRes.get(), rsError, "response type")

  var pos = 1  # skip type byte
  let errRes = decodeErrorResponse(encoded, pos)
  doAssert errRes.isOk, "decodeErrorResponse failed: " & errRes.error
  let e = errRes.get()
  assertEq(e.requestType, uint64(requestKindToByte(rkFetchBlock)), "reqType")
  assertEq(e.errorCode, uint64(ecOutOfRange), "errorCode")
  doAssert e.message == "block 999 out of range", "message mismatch: " & e.message

  echo "PASS: test_error_response"

# ---------------------------------------------------------------------------
# test_auth_response
# ---------------------------------------------------------------------------

proc test_auth_response() {.raises: [].} =
  let encoded = encodeAuthResult(status = 1, message = "authenticated")

  let rtRes = responseTypeByte(encoded)
  doAssert rtRes.isOk
  assertEq(rtRes.get(), rsAuthResult, "response type")

  var pos = 1
  let authRes = decodeAuthResponse(encoded, pos)
  doAssert authRes.isOk, "decodeAuthResponse failed: " & authRes.error
  let a = authRes.get()
  assertEq(a.status, 1'u64, "status")
  doAssert a.message == "authenticated", "message mismatch: " & a.message

  echo "PASS: test_auth_response"

# ---------------------------------------------------------------------------
# test_pong_response
# ---------------------------------------------------------------------------

proc test_pong_response() {.raises: [].} =
  let encoded = encodePong(nonce = 0xCAFEBABE'u64)

  let rtRes = responseTypeByte(encoded)
  doAssert rtRes.isOk
  assertEq(rtRes.get(), rsPong, "response type")

  var pos = 1
  let pongRes = decodePongResponse(encoded, pos)
  doAssert pongRes.isOk, "decodePongResponse failed: " & pongRes.error
  assertEq(pongRes.get().nonce, 0xCAFEBABE'u64, "nonce")

  echo "PASS: test_pong_response"

# ---------------------------------------------------------------------------
# test_block_data_response
# ---------------------------------------------------------------------------

proc test_block_data_response() {.raises: [].} =
  let blockData = @[0xAA'u8, 0xBB, 0xCC, 0xDD]
  let encoded = encodeBlockData(blockId = 7, data = blockData)

  let rtRes = responseTypeByte(encoded)
  doAssert rtRes.isOk
  assertEq(rtRes.get(), rsBlockData, "response type")

  var pos = 1
  let bdRes = decodeBlockDataResponse(encoded, pos)
  doAssert bdRes.isOk, "decodeBlockDataResponse failed: " & bdRes.error
  let (bid, bdata) = bdRes.get()
  assertEq(bid, 7'u64, "blockId")
  doAssert bdata == blockData, "block data mismatch"

  echo "PASS: test_block_data_response"

# ---------------------------------------------------------------------------
# test_frame_encoding
# ---------------------------------------------------------------------------

proc test_frame_encoding() {.raises: [].} =
  let payload = @[0x01'u8, 0x02, 0x03, 0x04, 0x05]
  let frame = encodeFrame(payload)

  # Frame should be 4 (header) + 5 (payload) = 9 bytes
  assertEq(frame.len, 9, "frame length")

  # Decode the length from the header
  let lenRes = decodeFrameLength(frame)
  doAssert lenRes.isOk, "decodeFrameLength failed"
  assertEq(lenRes.get(), 5'u32, "payload length")

  # Verify payload bytes are intact
  for i in 0 ..< payload.len:
    assertEq(frame[4 + i], payload[i], "payload byte " & $i)

  echo "PASS: test_frame_encoding"

# ---------------------------------------------------------------------------
# test_frame_short_header
# ---------------------------------------------------------------------------

proc test_frame_short_header() {.raises: [].} =
  let short = @[0x01'u8, 0x02]
  let lenRes = decodeFrameLength(short)
  doAssert lenRes.isErr, "should fail on short header"

  echo "PASS: test_frame_short_header"

# ---------------------------------------------------------------------------
# test_empty_request_decode
# ---------------------------------------------------------------------------

proc test_empty_request_decode() {.raises: [].} =
  let empty: seq[byte] = @[]
  let res = decodeRequest(empty)
  doAssert res.isErr, "should fail on empty buffer"

  echo "PASS: test_empty_request_decode"

# ---------------------------------------------------------------------------
# test_unknown_request_type
# ---------------------------------------------------------------------------

proc test_unknown_request_type() {.raises: [].} =
  let bad = @[0xFF'u8, 0x00]
  let res = decodeRequest(bad)
  doAssert res.isErr, "should fail on unknown type byte"

  echo "PASS: test_unknown_request_type"

# ---------------------------------------------------------------------------
# test_large_varint_roundtrip
# ---------------------------------------------------------------------------

proc test_large_varint_roundtrip() {.raises: [].} =
  ## Test with max uint64 value to ensure varint handles large values.
  var req = Request(kind: rkFetchBlock)
  req.fetchBlock = FetchBlockRequest(blockId: high(uint64))

  let encoded = encodeRequest(req)
  let decoded = decodeRequest(encoded)
  doAssert decoded.isOk, "decode failed: " & decoded.error
  assertEq(decoded.get().fetchBlock.blockId, high(uint64), "max uint64 blockId")

  echo "PASS: test_large_varint_roundtrip"

# ---------------------------------------------------------------------------
# test_framed_request_roundtrip
# ---------------------------------------------------------------------------

proc test_framed_request_roundtrip() {.raises: [].} =
  ## Encode a request, wrap in frame, decode frame length, extract payload,
  ## decode request — full protocol path.
  var req = Request(kind: rkQueryEvents)
  req.queryEvents = QueryEventsRequest(start: 1000, count: 50)

  let payload = encodeRequest(req)
  let frame = encodeFrame(payload)

  # Decode frame
  let lenRes = decodeFrameLength(frame)
  doAssert lenRes.isOk
  let payloadLen = int(lenRes.get())

  # Extract payload from frame
  var extractedPayload = newSeq[byte](payloadLen)
  for i in 0 ..< payloadLen:
    extractedPayload[i] = frame[4 + i]

  # Decode request from payload
  let decoded = decodeRequest(extractedPayload)
  doAssert decoded.isOk, "decode failed: " & decoded.error
  let r = decoded.get()
  assertEq(r.kind, rkQueryEvents, "kind")
  assertEq(r.queryEvents.start, 1000'u64, "start")
  assertEq(r.queryEvents.count, 50'u64, "count")

  echo "PASS: test_framed_request_roundtrip"

# ---------------------------------------------------------------------------
# Run all
# ---------------------------------------------------------------------------

test_fetch_block_roundtrip()
test_fetch_range_roundtrip()
test_query_metadata_roundtrip()
test_query_step_roundtrip()
test_query_values_roundtrip()
test_query_call_roundtrip()
test_query_events_roundtrip()
test_authenticate_roundtrip()
test_ping_roundtrip()
test_error_response()
test_auth_response()
test_pong_response()
test_block_data_response()
test_frame_encoding()
test_frame_short_header()
test_empty_request_decode()
test_unknown_request_type()
test_large_varint_roundtrip()
test_framed_request_roundtrip()
