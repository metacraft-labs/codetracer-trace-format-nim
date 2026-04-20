{.push raises: [].}

## Tests for LEB128 varint encoding/decoding.

import codetracer_trace_writer/varint

proc roundtripUnsigned(val: uint64) {.raises: [].} =
  var buf: seq[byte]
  encodeVarint(val, buf)
  var pos = 0
  let decoded = decodeVarint(buf, pos)
  doAssert decoded.isOk, "decode failed for " & $val
  doAssert decoded.get == val, "roundtrip mismatch for " & $val &
    ": got " & $decoded.get

proc roundtripSigned(val: int64) {.raises: [].} =
  var buf: seq[byte]
  encodeSignedVarint(val, buf)
  var pos = 0
  let decoded = decodeSignedVarint(buf, pos)
  doAssert decoded.isOk, "decode failed for " & $val
  doAssert decoded.get == val, "roundtrip mismatch for " & $val &
    ": got " & $decoded.get

proc test_roundtrip_basic() {.raises: [].} =
  roundtripUnsigned(0)
  roundtripUnsigned(1)
  roundtripUnsigned(127)
  roundtripUnsigned(128)
  roundtripUnsigned(16383)
  roundtripUnsigned(16384)
  roundtripUnsigned(uint64(high(uint32)))  # 2^32 - 1
  roundtripUnsigned(high(uint64))          # 2^64 - 1
  echo "PASS: test_roundtrip_basic"

proc test_roundtrip_signed() {.raises: [].} =
  roundtripSigned(0)
  roundtripSigned(1)
  roundtripSigned(-1)
  roundtripSigned(63)
  roundtripSigned(-64)
  roundtripSigned(64)
  roundtripSigned(-65)
  roundtripSigned(high(int64))
  roundtripSigned(low(int64))
  echo "PASS: test_roundtrip_signed"

proc test_encoding_sizes() {.raises: [].} =
  # 0..127 should encode as 1 byte
  var buf: seq[byte]
  encodeVarint(0, buf)
  doAssert buf.len == 1
  doAssert buf[0] == 0

  buf.setLen(0)
  encodeVarint(127, buf)
  doAssert buf.len == 1
  doAssert buf[0] == 127

  # 128 should encode as 2 bytes
  buf.setLen(0)
  encodeVarint(128, buf)
  doAssert buf.len == 2

  # 16383 should encode as 2 bytes
  buf.setLen(0)
  encodeVarint(16383, buf)
  doAssert buf.len == 2

  # 16384 should encode as 3 bytes
  buf.setLen(0)
  encodeVarint(16384, buf)
  doAssert buf.len == 3

  # max uint64 should encode as 10 bytes
  buf.setLen(0)
  encodeVarint(high(uint64), buf)
  doAssert buf.len == 10

  echo "PASS: test_encoding_sizes"

proc test_empty_input() {.raises: [].} =
  let empty: seq[byte] = @[]
  var pos = 0
  let result_val = decodeVarint(empty, pos)
  doAssert result_val.isErr
  echo "PASS: test_empty_input"

proc test_truncated_input() {.raises: [].} =
  # A varint with continuation bit set but no following byte
  let truncated = @[0x80'u8]
  var pos = 0
  let result_val = decodeVarint(truncated, pos)
  doAssert result_val.isErr
  echo "PASS: test_truncated_input"

proc test_multiple_varints_in_sequence() {.raises: [].} =
  var buf: seq[byte]
  encodeVarint(42, buf)
  encodeVarint(300, buf)
  encodeVarint(0, buf)
  encodeVarint(high(uint64), buf)

  var pos = 0
  doAssert decodeVarint(buf, pos).get == 42
  doAssert decodeVarint(buf, pos).get == 300
  doAssert decodeVarint(buf, pos).get == 0
  doAssert decodeVarint(buf, pos).get == high(uint64)
  doAssert pos == buf.len
  echo "PASS: test_multiple_varints_in_sequence"

proc test_pos_advances_correctly() {.raises: [].} =
  var buf: seq[byte]
  encodeVarint(1, buf)    # 1 byte
  encodeVarint(128, buf)  # 2 bytes
  encodeVarint(16384, buf) # 3 bytes

  var pos = 0
  discard decodeVarint(buf, pos)
  doAssert pos == 1
  discard decodeVarint(buf, pos)
  doAssert pos == 3
  discard decodeVarint(buf, pos)
  doAssert pos == 6
  echo "PASS: test_pos_advances_correctly"

test_roundtrip_basic()
test_roundtrip_signed()
test_encoding_sizes()
test_empty_input()
test_truncated_input()
test_multiple_varints_in_sequence()
test_pos_advances_correctly()
echo "ALL PASS: test_varint"
