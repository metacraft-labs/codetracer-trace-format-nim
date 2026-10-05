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

proc readVarintMultiByte*(data: openArray[byte], pos: var int,
    value: var uint64): bool {.raises: [].} =
  ## `readVarint` past its one-byte case.
  let p = pos
  if p < 0 or p >= data.len:
    return false
  if data.len - p >= 10:
    # Every byte a varint may take is in range: read them unchecked.
    let d = cast[ptr UncheckedArray[byte]](unsafeAddr data[p])
    var v = 0'u64
    for k in 0 ..< 10:
      let b = d[k]
      v = v or (uint64(b and 0x7F) shl (7 * k))
      if (b and 0x80) == 0:
        value = v
        pos = p + k + 1
        return true
    return false
  var v = 0'u64
  var shift = 0
  var q = p
  while q < data.len:
    let b = data[q]
    inc q
    v = v or (uint64(b and 0x7F) shl shift)
    if (b and 0x80) == 0:
      value = v
      pos = q
      return true
    shift += 7
    if shift >= 64:
      return false
  false

template readVarint*(data: openArray[byte], pos: var int,
    value: var uint64): bool =
  ## `decodeVarint` for a hot loop: no `Result` to build. True with `value`
  ## set and `pos` advanced; false, with `pos` unchanged, where
  ## `decodeVarint` refuses (a truncated varint or one over ten bytes).
  ##
  ## A template, so the one-byte case — most lengths, ids and deltas — is
  ## decided in the decoder that reads it; longer varints take
  ## `readVarintMultiByte`. `data` and `pos` must be plain locations: they
  ## are evaluated more than once.
  if pos >= 0 and pos < data.len and data[pos] < 0x80'u8:
    value = uint64(data[pos])
    inc pos
    true
  else:
    readVarintMultiByte(data, pos, value)

proc decodeVarintMultiByte(data: openArray[byte],
    pos: var int): Result[uint64, string] {.raises: [].} =
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

proc decodeVarint*(data: openArray[byte],
    pos: var int): Result[uint64, string] {.inline, raises: [].} =
  ## Decode a LEB128 unsigned varint from data starting at pos.
  ## Advances pos past the consumed bytes.
  ##
  ## Inline for the one-byte case, values below 128, which is most of the
  ## lengths, ids and deltas a reader decodes.
  if pos >= 0 and pos < data.len and data[pos] < 0x80'u8:
    result = ok(uint64(data[pos]))
    inc pos
  else:
    result = decodeVarintMultiByte(data, pos)

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

template varintOrReturn*(data: openArray[byte], pos: var int): uint64 =
  ## `?decodeVarint(data, pos)` for a hot decoder: the same value, or the same
  ## `err` returned from the enclosing proc, without a `Result` built for
  ## every varint that decodes.
  var v {.gensym.}: uint64
  if not readVarint(data, pos, v):
    return err(decodeVarint(data, pos).error)
  v

template varintOrFail*(data: openArray[byte], pos: var int,
    why: var string): uint64 =
  ## `varintOrReturn` for a decoder that answers `bool` and names its refusal
  ## in `why`: the same value, or `why` set to `decodeVarint`'s refusal and
  ## `false` returned from the enclosing proc.
  var v {.gensym.}: uint64
  if not readVarint(data, pos, v):
    why = decodeVarint(data, pos).error
    return false
  v

template signedVarintOrFail*(data: openArray[byte], pos: var int,
    why: var string): int64 =
  ## `decodeSignedVarint`, as `varintOrFail`.
  let z {.gensym.} = varintOrFail(data, pos, why)
  if (z and 1) == 0: int64(z shr 1) else: not int64(z shr 1)

template signedVarintOrReturn*(data: openArray[byte], pos: var int): int64 =
  ## `?decodeSignedVarint(data, pos)`, as `varintOrReturn`.
  let z {.gensym.} = varintOrReturn(data, pos)
  if (z and 1) == 0: int64(z shr 1) else: not int64(z shr 1)

