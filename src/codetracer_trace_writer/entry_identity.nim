## Optional finalized entry.dat identity, format specification 4fc5486.
## Codec only: callers must additionally validate actual same-image call,
## function, step ownership and parent-chain bindings before using the result.
{.push raises: [].}
import results
import ./varint

type RecordedEntryIdentity* = object
  callKey*: uint64
  functionId*: uint64
  entryStep*: uint64

proc encodeEntryIdentity*(entry: RecordedEntryIdentity): Result[seq[byte], string] =
  if entry.callKey > uint64(high(int64)) or entry.entryStep > uint64(high(int64)):
    return err("entry.dat: call/step id outside signed-64-bit domain")
  var data = @[byte('C'), byte('T'), byte('E'), byte('I'), 1'u8, 0, 0, 0]
  encodeVarint(entry.callKey, data)
  encodeVarint(entry.functionId, data)
  encodeVarint(entry.entryStep, data)
  ok(data)

proc readCanonicalVarint(data: openArray[byte], pos: var int): Result[uint64, string] =
  var value = 0'u64
  for i in 0 ..< 10:
    if pos >= data.len: return err("entry.dat: truncated varint")
    let b = data[pos]
    inc pos
    if i == 9 and b > 1: return err("entry.dat: overflowing varint")
    value = value or (uint64(b and 0x7f) shl (i * 7))
    if (b and 0x80) == 0:
      if i > 0 and b == 0: return err("entry.dat: noncanonical varint")
      return ok(value)
  err("entry.dat: overflowing varint")

proc decodeEntryIdentity*(data: openArray[byte]): Result[RecordedEntryIdentity, string] =
  if data.len < 8: return err("entry.dat: truncated header")
  if data[0] != byte('C') or data[1] != byte('T') or data[2] != byte('E') or data[3] != byte('I'):
    return err("entry.dat: invalid magic")
  if data[4] != 1 or data[5] != 0: return err("entry.dat: unsupported version")
  if data[6] != 0 or data[7] != 0: return err("entry.dat: nonzero reserved bytes")
  var pos = 8
  let key = ? readCanonicalVarint(data, pos)
  let functionId = ? readCanonicalVarint(data, pos)
  let step = ? readCanonicalVarint(data, pos)
  if pos != data.len: return err("entry.dat: trailing bytes")
  if key > uint64(high(int64)) or step > uint64(high(int64)):
    return err("entry.dat: call/step id outside signed-64-bit domain")
  ok(RecordedEntryIdentity(callKey: key, functionId: functionId, entryStep: step))
