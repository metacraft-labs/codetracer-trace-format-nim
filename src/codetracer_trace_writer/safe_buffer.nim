{.push raises: [].}

## Safe buffer with cursor for high-performance binary writing.
## Uses seq[byte] (GC-managed) with a separate write position cursor.
## Key insight: pre-allocate full capacity, write via indexed access,
## only truncate (setLen) at the end. This avoids per-write setLen overhead.

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

proc ensureCapacity*(buf: var SafeBuffer, needed: int) {.inline.} =
  let required = buf.pos + needed
  if required > buf.data.len:
    var newCap = buf.data.len * 2
    while newCap < required:
      newCap = newCap * 2
    buf.data.setLen(newCap)

{.push checks: off, boundChecks: off.}

proc writeU8*(buf: var SafeBuffer, v: uint8) {.inline.} =
  ensureCapacity(buf, 1)
  buf.data[buf.pos] = v
  buf.pos += 1

proc writeU32*(buf: var SafeBuffer, v: uint32) {.inline.} =
  ensureCapacity(buf, 4)
  copyMem(addr buf.data[buf.pos], unsafeAddr v, 4)
  buf.pos += 4

proc writeU64*(buf: var SafeBuffer, v: uint64) {.inline.} =
  ensureCapacity(buf, 8)
  copyMem(addr buf.data[buf.pos], unsafeAddr v, 8)
  buf.pos += 8

proc writeI64*(buf: var SafeBuffer, v: int64) {.inline.} =
  writeU64(buf, cast[uint64](v))

proc writeF64*(buf: var SafeBuffer, v: float64) {.inline.} =
  writeU64(buf, cast[uint64](v))

proc writeBool*(buf: var SafeBuffer, v: bool) {.inline.} =
  ensureCapacity(buf, 1)
  buf.data[buf.pos] = if v: 1'u8 else: 0'u8
  buf.pos += 1

proc writeBytes*(buf: var SafeBuffer, src: pointer, count: int) {.inline.} =
  if count > 0:
    ensureCapacity(buf, count)
    copyMem(addr buf.data[buf.pos], src, count)
    buf.pos += count

proc writeStr*(buf: var SafeBuffer, s: string) {.inline.} =
  let slen = uint32(s.len)
  ensureCapacity(buf, 4 + s.len)
  copyMem(addr buf.data[buf.pos], unsafeAddr slen, 4)
  buf.pos += 4
  if s.len > 0:
    copyMem(addr buf.data[buf.pos], unsafeAddr s[0], s.len)
    buf.pos += s.len

proc writeOpenArray*(buf: var SafeBuffer, data: openArray[byte]) {.inline.} =
  if data.len > 0:
    ensureCapacity(buf, data.len)
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
    ensureCapacity(buf, 1)
    buf.data[buf.pos] = mt or byte(value)
    buf.pos += 1
  elif value <= 0xFF:
    ensureCapacity(buf, 2)
    buf.data[buf.pos] = mt or 24
    buf.data[buf.pos + 1] = byte(value)
    buf.pos += 2
  elif value <= 0xFFFF:
    ensureCapacity(buf, 3)
    buf.data[buf.pos] = mt or 25
    buf.data[buf.pos + 1] = byte(value shr 8)
    buf.data[buf.pos + 2] = byte(value)
    buf.pos += 3
  elif value <= 0xFFFF_FFFF'u64:
    ensureCapacity(buf, 5)
    buf.data[buf.pos] = mt or 26
    buf.data[buf.pos + 1] = byte(value shr 24)
    buf.data[buf.pos + 2] = byte(value shr 16)
    buf.data[buf.pos + 3] = byte(value shr 8)
    buf.data[buf.pos + 4] = byte(value)
    buf.pos += 5
  else:
    ensureCapacity(buf, 9)
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
  buf.writeCborTypeAndValue(3, uint64(s.len))
  if s.len > 0:
    ensureCapacity(buf, s.len)
    copyMem(addr buf.data[buf.pos], unsafeAddr s[0], s.len)
    buf.pos += s.len

proc writeCborByteString*(buf: var SafeBuffer, data: openArray[byte]) {.inline.} =
  buf.writeCborTypeAndValue(2, uint64(data.len))
  if data.len > 0:
    ensureCapacity(buf, data.len)
    copyMem(addr buf.data[buf.pos], unsafeAddr data[0], data.len)
    buf.pos += data.len

proc writeCborArrayHeader*(buf: var SafeBuffer, count: uint64) {.inline.} =
  buf.writeCborTypeAndValue(4, count)

proc writeCborMapHeader*(buf: var SafeBuffer, count: uint64) {.inline.} =
  buf.writeCborTypeAndValue(5, count)

proc writeCborBool*(buf: var SafeBuffer, value: bool) {.inline.} =
  ensureCapacity(buf, 1)
  buf.data[buf.pos] = if value: 0xF5'u8 else: 0xF4'u8
  buf.pos += 1

proc writeCborNull*(buf: var SafeBuffer) {.inline.} =
  ensureCapacity(buf, 1)
  buf.data[buf.pos] = 0xF6'u8
  buf.pos += 1

proc writeCborFloat64*(buf: var SafeBuffer, value: float64) {.inline.} =
  ensureCapacity(buf, 9)
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
