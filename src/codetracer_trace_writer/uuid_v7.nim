{.push raises: [].}

## UUIDv7 generator and validator (RFC 9562, May 2024).
##
## UUIDv7 is the canonical recording-id format for CodeTracer traces, per
## ~codetracer-specs/Refactoring-Plans/Recording-Identifier-Migration.md~
## §3.  The layout (RFC 9562 §5.7):
##
## ```
##  0                   1                   2                   3
##  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
## +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
## |                           unix_ts_ms                          |
## +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
## |          unix_ts_ms           |  ver  |       rand_a          |
## +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
## |var|                        rand_b                             |
## +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
## |                            rand_b                             |
## +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
## ```
##
## - bits 0..47  : `unix_ts_ms` — current wall-clock milliseconds since
##                  the Unix epoch (big-endian).
## - bits 48..51 : version (= 7).
## - bits 52..63 : `rand_a` — 12 random bits (we use this for sub-ms
##                  entropy; we do NOT use it as a sub-ms timestamp
##                  because Nim's `std/times` does not portably expose
##                  sub-ms resolution and a CSPRNG draw is just as good).
## - bits 64..65 : variant (= 10b).
## - bits 66..127: `rand_b` — 62 random bits.
##
## The canonical text form is the lowercase 36-char hyphenated form
## documented in RFC 9562 §4: `xxxxxxxx-xxxx-7xxx-yxxx-xxxxxxxxxxxx`
## where the `7` is the version nibble and `y` ∈ {8,9,a,b} per the
## two-bit variant prefix.
##
## ## Sortability
##
## Two UUIDv7s generated in different milliseconds are guaranteed to
## sort lex-ascending in their text form, because the high 48 bits
## (the timestamp) are written big-endian and the canonical text form
## is hexadecimal MSB-first.  Within a single millisecond, ordering is
## random — this module does not attempt monotonic generation, which
## would require a process-global lock for the per-process counter
## that RFC 9562 §6.2 describes.  The recorder writes at most one
## recording_id per record-start, so sub-ms ties are not a real
## concern; if a future caller needs strict monotonicity, layer it on
## top.

import std/[sysrand, times]
import results

const
  UuidV7TextLen* = 36
    ## Length of the canonical hyphenated text form (8-4-4-4-12).
  UuidV7HexLen* = 32
    ## Length without hyphens.

type
  UuidV7* = object
    ## Big-endian 16-byte UUIDv7.  Stored as raw bytes; convert with
    ## `$` for the canonical text form.
    bytes*: array[16, byte]

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

proc unixMs(): uint64 =
  ## Return the current Unix epoch in whole milliseconds.  Uses
  ## `std/times.epochTime()` so the value matches `date +%s%3N`.
  uint64(epochTime() * 1000.0)

# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------

proc newUuidV7*(): Result[UuidV7, string] =
  ## Generate a fresh UUIDv7.  Returns `err` only if the OS CSPRNG
  ## (`/dev/urandom`, `getentropy`, `BCryptGenRandom`, ...) refuses to
  ## yield 10 bytes — a condition that practically never occurs on a
  ## healthy host.
  ##
  ## RFC 9562 §6.2 lists several monotonicity strategies; this
  ## implementation picks "Method 1: Random" — the rand_a / rand_b
  ## fields are purely random — because the recorder mints at most
  ## one id per record-start and we do not need a process-global
  ## counter.

  var randomBytes: array[10, byte]
  if not urandom(randomBytes):
    return err("uuidv7: OS CSPRNG returned no entropy")

  let ms = unixMs()

  var u = UuidV7()

  # Bytes 0..5 — 48-bit unix_ts_ms, big-endian.
  u.bytes[0] = byte((ms shr 40) and 0xFF'u64)
  u.bytes[1] = byte((ms shr 32) and 0xFF'u64)
  u.bytes[2] = byte((ms shr 24) and 0xFF'u64)
  u.bytes[3] = byte((ms shr 16) and 0xFF'u64)
  u.bytes[4] = byte((ms shr 8) and 0xFF'u64)
  u.bytes[5] = byte(ms and 0xFF'u64)

  # Bytes 6..7 — version (4 bits = 0b0111) + rand_a (12 bits).
  u.bytes[6] = byte((0x70'u8) or (randomBytes[0] and 0x0F'u8))
  u.bytes[7] = randomBytes[1]

  # Bytes 8..15 — variant (2 bits = 0b10) + rand_b (62 bits).
  u.bytes[8] = byte((randomBytes[2] and 0x3F'u8) or 0x80'u8)
  u.bytes[9] = randomBytes[3]
  for i in 0 ..< 6:
    u.bytes[10 + i] = randomBytes[4 + i]

  ok(u)

# ---------------------------------------------------------------------------
# Text form
# ---------------------------------------------------------------------------

const HexLower = "0123456789abcdef"

proc `$`*(u: UuidV7): string =
  ## Render the canonical lowercase hyphenated form
  ## `xxxxxxxx-xxxx-7xxx-yxxx-xxxxxxxxxxxx`.
  result = newString(UuidV7TextLen)
  var dest = 0
  for i in 0 ..< 16:
    if i == 4 or i == 6 or i == 8 or i == 10:
      result[dest] = '-'
      inc dest
    let b = u.bytes[i]
    result[dest] = HexLower[int(b shr 4)]
    inc dest
    result[dest] = HexLower[int(b and 0x0F'u8)]
    inc dest

proc unixMs*(u: UuidV7): uint64 =
  ## Extract the embedded 48-bit Unix-epoch-ms timestamp.  Useful in
  ## tests that need to assert two recordings were stamped with
  ## advancing wall-clock time.
  (uint64(u.bytes[0]) shl 40) or
  (uint64(u.bytes[1]) shl 32) or
  (uint64(u.bytes[2]) shl 24) or
  (uint64(u.bytes[3]) shl 16) or
  (uint64(u.bytes[4]) shl 8) or
   uint64(u.bytes[5])

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

proc parseHexDigit(c: char): int =
  case c
  of '0' .. '9': ord(c) - ord('0')
  of 'a' .. 'f': 10 + ord(c) - ord('a')
  of 'A' .. 'F': 10 + ord(c) - ord('A')
  else: -1

proc validateRecordingIdStr*(s: string): Result[void, string] =
  ## Validate that `s` is a syntactically correct canonical-form
  ## UUIDv7 string (lowercase, hyphenated, 36 chars, version nibble 7,
  ## variant bits 10).  Used by readers to reject malformed
  ## `recording_id` values in trace metadata.
  ##
  ## Casing: per the parent spec §3 ("lowercase hyphenated 36-char
  ## form"), we reject uppercase A-F to keep the on-disk
  ## representation byte-for-byte canonical.  Callers that need to
  ## accept user input should lowercase first.

  if s.len != UuidV7TextLen:
    return err("recording_id: expected " & $UuidV7TextLen &
               " chars, got " & $s.len)

  # Hyphen positions.
  for hyphenPos in [8, 13, 18, 23]:
    if s[hyphenPos] != '-':
      return err("recording_id: expected '-' at position " &
                 $hyphenPos & ", got '" & $s[hyphenPos] & "'")

  # All other positions must be lowercase hex digits.
  for i, ch in s:
    if i == 8 or i == 13 or i == 18 or i == 23:
      continue
    case ch
    of '0' .. '9', 'a' .. 'f':
      discard
    else:
      return err("recording_id: non-lowercase-hex character '" &
                 $ch & "' at position " & $i)

  # Version nibble — first hex char of the 3rd group (position 14)
  # MUST be '7'.  Position breakdown:
  # xxxxxxxx-xxxx-Vxxx-Yxxx-xxxxxxxxxxxx
  # 01234567 9 11 14 16 19 20    33 34 35
  if s[14] != '7':
    return err("recording_id: expected version nibble '7' at " &
               "position 14, got '" & $s[14] & "' (not a UUIDv7)")

  # Variant nibble — first hex char of the 4th group (position 19)
  # MUST be in {8, 9, a, b} so the top two bits are 10.
  case s[19]
  of '8', '9', 'a', 'b':
    discard
  else:
    return err("recording_id: expected variant nibble in " &
               "{8,9,a,b} at position 19, got '" & $s[19] & "'")

  ok()

proc parseUuidV7*(s: string): Result[UuidV7, string] =
  ## Parse a canonical-form UUIDv7 string into raw bytes.  Validates
  ## syntax first, then materializes the byte array.
  ? validateRecordingIdStr(s)
  var u = UuidV7()
  var byteIdx = 0
  var i = 0
  while i < UuidV7TextLen:
    if s[i] == '-':
      inc i
      continue
    let hi = parseHexDigit(s[i])
    let lo = parseHexDigit(s[i + 1])
    # validateRecordingIdStr already rejected non-hex digits, so the
    # parses cannot fail; the asserts document the invariant.
    doAssert hi >= 0 and lo >= 0,
      "uuidv7: validator passed but hex parse failed; should be unreachable"
    u.bytes[byteIdx] = byte((hi shl 4) or lo)
    inc byteIdx
    i += 2
  ok(u)
