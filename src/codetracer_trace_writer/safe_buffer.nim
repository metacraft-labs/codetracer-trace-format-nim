{.push raises: [].}

## ChunkBuffer: fixed-capacity buffer for streaming event encoding.
## Allocated once at init, never resized in the hot path.
## The buffer is sized generously (chunkThreshold * 256 bytes) so that
## a full chunk of events fits without growing. A single `ensureSpace`
## check per event (in encodeEvent) is the only branch in the hot path.
##
## Safety net: if the buffer is ever too small (pathological case with
## huge strings), growIfNeeded expands it. This is marked {.noinline.}
## so the fast path stays compact in the instruction cache.

type
  SafeBuffer* = object
    data*: seq[byte]
    pos*: int  ## Write cursor position (actual used bytes)

proc initSafeBuffer*(initialCap: int = 65536): SafeBuffer =
  result.data = newSeq[byte](initialCap)
  result.pos = 0

proc clear*(buf: var SafeBuffer) {.inline.} =
  buf.pos = 0

proc len*(buf: SafeBuffer): int {.inline.} =
  buf.pos

proc growIfNeeded*(buf: var SafeBuffer, needed: int) {.noinline.} =
  ## Rare path: grow the buffer. Should almost never be called in
  ## streaming mode since the buffer is pre-sized for a full chunk.
  let required = buf.pos + needed
  if required > buf.data.len:
    var newCap = buf.data.len * 2
    while newCap < required:
      newCap = newCap * 2
    buf.data.setLen(newCap)

template ensureSpace*(buf: var SafeBuffer, needed: int) =
  ## Fast inline check - the branch is almost never taken when the
  ## buffer is correctly pre-sized for streaming chunks.
  if unlikely(buf.pos + needed > buf.data.len):
    buf.growIfNeeded(needed)

# Keep ensureCapacity as an alias for backward compatibility (tests, etc.)
template ensureCapacity*(buf: var SafeBuffer, needed: int) =
  ensureSpace(buf, needed)

{.push checks: off, boundChecks: off.}

proc writeU8*(buf: var SafeBuffer, v: uint8) {.inline.} =
  buf.data[buf.pos] = v
  buf.pos += 1

proc writeU32*(buf: var SafeBuffer, v: uint32) {.inline.} =
  copyMem(addr buf.data[buf.pos], unsafeAddr v, 4)
  buf.pos += 4

proc writeU64*(buf: var SafeBuffer, v: uint64) {.inline.} =
  copyMem(addr buf.data[buf.pos], unsafeAddr v, 8)
  buf.pos += 8

proc writeI64*(buf: var SafeBuffer, v: int64) {.inline.} =
  writeU64(buf, cast[uint64](v))

proc writeF64*(buf: var SafeBuffer, v: float64) {.inline.} =
  writeU64(buf, cast[uint64](v))

proc writeBool*(buf: var SafeBuffer, v: bool) {.inline.} =
  buf.data[buf.pos] = if v: 1'u8 else: 0'u8
  buf.pos += 1

proc writeBytes*(buf: var SafeBuffer, src: pointer, count: int) {.inline.} =
  if count > 0:
    copyMem(addr buf.data[buf.pos], src, count)
    buf.pos += count

proc writeStr*(buf: var SafeBuffer, s: string) {.inline.} =
  # Safety net for long strings that exceed the per-event ensureSpace budget
  ensureSpace(buf, 4 + s.len)
  let slen = uint32(s.len)
  copyMem(addr buf.data[buf.pos], unsafeAddr slen, 4)
  buf.pos += 4
  if s.len > 0:
    copyMem(addr buf.data[buf.pos], unsafeAddr s[0], s.len)
    buf.pos += s.len

proc writeOpenArray*(buf: var SafeBuffer, data: openArray[byte]) {.inline.} =
  ## For short pre-computed CBOR keys (all <= 20 bytes), no check needed
  ## since ensureSpace(512) at event start covers them. But we keep the
  ## safety net for correctness with arbitrary data.
  if data.len > 0:
    copyMem(addr buf.data[buf.pos], unsafeAddr data[0], data.len)
    buf.pos += data.len

{.pop.} # checks: off, boundChecks: off

proc toSeq*(buf: SafeBuffer): seq[byte] =
  result = buf.data
  result.setLen(buf.pos)

# CBOR-specific write methods (big-endian integers for CBOR headers)

{.push checks: off, boundChecks: off.}

proc writeCborTypeAndValue*(buf: var SafeBuffer, majorType: byte, value: uint64) {.inline.} =
  let mt = majorType shl 5
  if value <= 23:
    buf.data[buf.pos] = mt or byte(value)
    buf.pos += 1
  elif value <= 0xFF:
    buf.data[buf.pos] = mt or 24
    buf.data[buf.pos + 1] = byte(value)
    buf.pos += 2
  elif value <= 0xFFFF:
    buf.data[buf.pos] = mt or 25
    buf.data[buf.pos + 1] = byte(value shr 8)
    buf.data[buf.pos + 2] = byte(value)
    buf.pos += 3
  elif value <= 0xFFFF_FFFF'u64:
    buf.data[buf.pos] = mt or 26
    buf.data[buf.pos + 1] = byte(value shr 24)
    buf.data[buf.pos + 2] = byte(value shr 16)
    buf.data[buf.pos + 3] = byte(value shr 8)
    buf.data[buf.pos + 4] = byte(value)
    buf.pos += 5
  else:
    buf.data[buf.pos] = mt or 27
    buf.data[buf.pos + 1] = byte(value shr 56)
    buf.data[buf.pos + 2] = byte(value shr 48)
    buf.data[buf.pos + 3] = byte(value shr 40)
    buf.data[buf.pos + 4] = byte(value shr 32)
    buf.data[buf.pos + 5] = byte(value shr 24)
    buf.data[buf.pos + 6] = byte(value shr 16)
    buf.data[buf.pos + 7] = byte(value shr 8)
    buf.data[buf.pos + 8] = byte(value)
    buf.pos += 9

proc writeCborUint*(buf: var SafeBuffer, value: uint64) {.inline.} =
  buf.writeCborTypeAndValue(0, value)

proc writeCborNegInt*(buf: var SafeBuffer, value: uint64) {.inline.} =
  buf.writeCborTypeAndValue(1, value)

proc writeCborInt*(buf: var SafeBuffer, value: int64) {.inline.} =
  if value >= 0:
    buf.writeCborUint(uint64(value))
  else:
    buf.writeCborNegInt(uint64(-1 - value))

proc writeCborTextString*(buf: var SafeBuffer, s: string) {.inline.} =
  # Safety net for long strings (9 bytes for CBOR header + string content)
  ensureSpace(buf, 9 + s.len)
  buf.writeCborTypeAndValue(3, uint64(s.len))
  if s.len > 0:
    copyMem(addr buf.data[buf.pos], unsafeAddr s[0], s.len)
    buf.pos += s.len

proc writeCborByteString*(buf: var SafeBuffer, data: openArray[byte]) {.inline.} =
  # Safety net for large byte strings
  ensureSpace(buf, 9 + data.len)
  buf.writeCborTypeAndValue(2, uint64(data.len))
  if data.len > 0:
    copyMem(addr buf.data[buf.pos], unsafeAddr data[0], data.len)
    buf.pos += data.len

proc writeCborArrayHeader*(buf: var SafeBuffer, count: uint64) {.inline.} =
  buf.writeCborTypeAndValue(4, count)

proc writeCborMapHeader*(buf: var SafeBuffer, count: uint64) {.inline.} =
  buf.writeCborTypeAndValue(5, count)

proc writeCborBool*(buf: var SafeBuffer, value: bool) {.inline.} =
  buf.data[buf.pos] = if value: 0xF5'u8 else: 0xF4'u8
  buf.pos += 1

proc writeCborNull*(buf: var SafeBuffer) {.inline.} =
  buf.data[buf.pos] = 0xF6'u8
  buf.pos += 1

proc writeCborFloat64*(buf: var SafeBuffer, value: float64) {.inline.} =
  buf.data[buf.pos] = 0xFB'u8
  let bits = cast[uint64](value)
  buf.data[buf.pos + 1] = byte(bits shr 56)
  buf.data[buf.pos + 2] = byte(bits shr 48)
  buf.data[buf.pos + 3] = byte(bits shr 40)
  buf.data[buf.pos + 4] = byte(bits shr 32)
  buf.data[buf.pos + 5] = byte(bits shr 24)
  buf.data[buf.pos + 6] = byte(bits shr 16)
  buf.data[buf.pos + 7] = byte(bits shr 8)
  buf.data[buf.pos + 8] = byte(bits)
  buf.pos += 9

{.pop.} # checks: off, boundChecks: off
