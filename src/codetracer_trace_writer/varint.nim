{.push raises: [].}

## LEB128 varint encoding/decoding.
##
## Used only internally for encoding dynamic payloads (ValueRecord, etc.)
## in the Nim-native binary format.

import results
export results

proc encodeVarint*(val: uint64, output: var seq[byte]) {.raises: [].} =
  ## Encode an unsigned 64-bit integer as LEB128.
  var v = val
  while true:
    var b = byte(v and 0x7F)
    v = v shr 7
    if v != 0:
      b = b or 0x80
    output.add(b)
    if v == 0:
      break

proc encodeVarintTo*(val: uint64, output: var openArray[byte], pos: var int) {.raises: [].} =
  ## Encode an unsigned 64-bit integer as LEB128 into a pre-allocated buffer.
  ## Advances pos past the written bytes. Caller must ensure enough space
  ## (max 10 bytes per varint).
  var v = val
  while true:
    var b = byte(v and 0x7F)
    v = v shr 7
    if v != 0:
      b = b or 0x80
    output[pos] = b
    pos += 1
    if v == 0:
      break

proc decodeVarint*(data: openArray[byte], pos: var int): Result[uint64, string] {.raises: [].} =
  ## Decode a LEB128 unsigned varint from data starting at pos.
  ## Advances pos past the consumed bytes.
  var result_val: uint64 = 0
  var shift: int = 0
  while true:
    if pos >= data.len:
      return err("varint: unexpected end of input")
    let b = data[pos]
    pos += 1
    result_val = result_val or (uint64(b and 0x7F) shl shift)
    if (b and 0x80) == 0:
      return ok(result_val)
    shift += 7
    if shift >= 64:
      return err("varint: too many bytes (>10)")

proc encodeSignedVarint*(val: int64, output: var seq[byte]) {.raises: [].} =
  ## Encode a signed 64-bit integer using zigzag encoding + LEB128.
  let zigzag = if val >= 0: uint64(val) shl 1
              else: (uint64(not val) shl 1) or 1
  encodeVarint(zigzag, output)

proc decodeSignedVarint*(data: openArray[byte], pos: var int): Result[int64, string] {.raises: [].} =
  ## Decode a zigzag-encoded signed varint.
  let v = ?decodeVarint(data, pos)
  if (v and 1) == 0:
    ok(int64(v shr 1))
  else:
    ok(not int64(v shr 1))
