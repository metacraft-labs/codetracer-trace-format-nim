{.push raises: [].}

## Raw buffer with cursor for high-performance binary writing.
## Uses manual memory management (alloc/realloc/dealloc) - no GC involvement.
## Single capacity check per write operation, direct pointer arithmetic.

type
  FastBuffer* = object
    data*: ptr UncheckedArray[byte]
    len*: int
    cap*: int

proc initFastBuffer*(initialCap: int = 65536): FastBuffer =
  result.data = cast[ptr UncheckedArray[byte]](alloc(initialCap))
  result.len = 0
  result.cap = initialCap

proc destroy*(buf: var FastBuffer) =
  if buf.data != nil:
    dealloc(buf.data)
    buf.data = nil
    buf.len = 0
    buf.cap = 0

proc clear*(buf: var FastBuffer) {.inline.} =
  buf.len = 0

proc ensureCapacity*(buf: var FastBuffer, needed: int) {.inline.} =
  let required = buf.len + needed
  if required > buf.cap:
    var newCap = buf.cap * 2
    while newCap < required:
      newCap = newCap * 2
    buf.data = cast[ptr UncheckedArray[byte]](realloc(buf.data, newCap))
    buf.cap = newCap

{.push checks: off, boundChecks: off.}

proc writeU8*(buf: var FastBuffer, v: uint8) {.inline.} =
  ensureCapacity(buf, 1)
  buf.data[buf.len] = v
  buf.len += 1

proc writeU32*(buf: var FastBuffer, v: uint32) {.inline.} =
  ensureCapacity(buf, 4)
  copyMem(addr buf.data[buf.len], unsafeAddr v, 4)
  buf.len += 4

proc writeU64*(buf: var FastBuffer, v: uint64) {.inline.} =
  ensureCapacity(buf, 8)
  copyMem(addr buf.data[buf.len], unsafeAddr v, 8)
  buf.len += 8

proc writeI64*(buf: var FastBuffer, v: int64) {.inline.} =
  writeU64(buf, cast[uint64](v))

proc writeF64*(buf: var FastBuffer, v: float64) {.inline.} =
  writeU64(buf, cast[uint64](v))

proc writeBool*(buf: var FastBuffer, v: bool) {.inline.} =
  ensureCapacity(buf, 1)
  buf.data[buf.len] = if v: 1'u8 else: 0'u8
  buf.len += 1

proc writeBytes*(buf: var FastBuffer, src: pointer, count: int) {.inline.} =
  if count > 0:
    ensureCapacity(buf, count)
    copyMem(addr buf.data[buf.len], src, count)
    buf.len += count

proc writeStr*(buf: var FastBuffer, s: string) {.inline.} =
  let slen = uint32(s.len)
  ensureCapacity(buf, 4 + s.len)
  copyMem(addr buf.data[buf.len], unsafeAddr slen, 4)
  buf.len += 4
  if s.len > 0:
    copyMem(addr buf.data[buf.len], unsafeAddr s[0], s.len)
    buf.len += s.len

proc writeOpenArray*(buf: var FastBuffer, data: openArray[byte]) {.inline.} =
  if data.len > 0:
    ensureCapacity(buf, data.len)
    copyMem(addr buf.data[buf.len], unsafeAddr data[0], data.len)
    buf.len += data.len

{.pop.} # checks: off, boundChecks: off

proc toSeq*(buf: FastBuffer): seq[byte] =
  result = newSeq[byte](buf.len)
  if buf.len > 0:
    copyMem(addr result[0], buf.data, buf.len)

# CBOR-specific write methods (big-endian integers for CBOR headers)

{.push checks: off, boundChecks: off.}

proc writeCborTypeAndValue*(buf: var FastBuffer, majorType: byte, value: uint64) {.inline.} =
  let mt = majorType shl 5
  if value <= 23:
    ensureCapacity(buf, 1)
    buf.data[buf.len] = mt or byte(value)
    buf.len += 1
  elif value <= 0xFF:
    ensureCapacity(buf, 2)
    buf.data[buf.len] = mt or 24
    buf.data[buf.len + 1] = byte(value)
    buf.len += 2
  elif value <= 0xFFFF:
    ensureCapacity(buf, 3)
    buf.data[buf.len] = mt or 25
    buf.data[buf.len + 1] = byte(value shr 8)
    buf.data[buf.len + 2] = byte(value)
    buf.len += 3
  elif value <= 0xFFFF_FFFF'u64:
    ensureCapacity(buf, 5)
    buf.data[buf.len] = mt or 26
    buf.data[buf.len + 1] = byte(value shr 24)
    buf.data[buf.len + 2] = byte(value shr 16)
    buf.data[buf.len + 3] = byte(value shr 8)
    buf.data[buf.len + 4] = byte(value)
    buf.len += 5
  else:
    ensureCapacity(buf, 9)
    buf.data[buf.len] = mt or 27
    buf.data[buf.len + 1] = byte(value shr 56)
    buf.data[buf.len + 2] = byte(value shr 48)
    buf.data[buf.len + 3] = byte(value shr 40)
    buf.data[buf.len + 4] = byte(value shr 32)
    buf.data[buf.len + 5] = byte(value shr 24)
    buf.data[buf.len + 6] = byte(value shr 16)
    buf.data[buf.len + 7] = byte(value shr 8)
    buf.data[buf.len + 8] = byte(value)
    buf.len += 9

proc writeCborUint*(buf: var FastBuffer, value: uint64) {.inline.} =
  buf.writeCborTypeAndValue(0, value)

proc writeCborNegInt*(buf: var FastBuffer, value: uint64) {.inline.} =
  buf.writeCborTypeAndValue(1, value)

proc writeCborInt*(buf: var FastBuffer, value: int64) {.inline.} =
  if value >= 0:
    buf.writeCborUint(uint64(value))
  else:
    buf.writeCborNegInt(uint64(-1 - value))

proc writeCborTextString*(buf: var FastBuffer, s: string) {.inline.} =
  buf.writeCborTypeAndValue(3, uint64(s.len))
  if s.len > 0:
    ensureCapacity(buf, s.len)
    copyMem(addr buf.data[buf.len], unsafeAddr s[0], s.len)
    buf.len += s.len

proc writeCborByteString*(buf: var FastBuffer, data: openArray[byte]) {.inline.} =
  buf.writeCborTypeAndValue(2, uint64(data.len))
  if data.len > 0:
    ensureCapacity(buf, data.len)
    copyMem(addr buf.data[buf.len], unsafeAddr data[0], data.len)
    buf.len += data.len

proc writeCborArrayHeader*(buf: var FastBuffer, count: uint64) {.inline.} =
  buf.writeCborTypeAndValue(4, count)

proc writeCborMapHeader*(buf: var FastBuffer, count: uint64) {.inline.} =
  buf.writeCborTypeAndValue(5, count)

proc writeCborBool*(buf: var FastBuffer, value: bool) {.inline.} =
  ensureCapacity(buf, 1)
  buf.data[buf.len] = if value: 0xF5'u8 else: 0xF4'u8
  buf.len += 1

proc writeCborNull*(buf: var FastBuffer) {.inline.} =
  ensureCapacity(buf, 1)
  buf.data[buf.len] = 0xF6'u8
  buf.len += 1

proc writeCborFloat64*(buf: var FastBuffer, value: float64) {.inline.} =
  ensureCapacity(buf, 9)
  buf.data[buf.len] = 0xFB'u8
  let bits = cast[uint64](value)
  buf.data[buf.len + 1] = byte(bits shr 56)
  buf.data[buf.len + 2] = byte(bits shr 48)
  buf.data[buf.len + 3] = byte(bits shr 40)
  buf.data[buf.len + 4] = byte(bits shr 32)
  buf.data[buf.len + 5] = byte(bits shr 24)
  buf.data[buf.len + 6] = byte(bits shr 16)
  buf.data[buf.len + 7] = byte(bits shr 8)
  buf.data[buf.len + 8] = byte(bits)
  buf.len += 9

{.pop.} # checks: off, boundChecks: off
