{.push raises: [].}

## XXH64 — the hash CTFS uses to derive 64-bit namespace keys from keys that
## are wider than 64 bits.
##
## Why this algorithm and not `std/hashes`: a namespace key is part of the
## on-disk format, so Nim, Rust and C# must agree on it byte-for-byte forever.
## `std/hashes` is explicitly not stable across Nim versions, and the two other
## languages have no way to reproduce it. XXH64 is specified, has published
## reference vectors, and ships in both other ecosystems already
## (`xxhash-rust`; `System.IO.Hashing.XxHash64` in .NET).
##
## The reference vectors in `tests/test_xxh64.nim` are the contract. If this
## implementation and another language's disagree, a recording written by one
## becomes unreadable by the other with no error — the B-tree lookup simply
## misses — so the vectors are what keep the three honest.

const
  Prime1 = 0x9E3779B185EBCA87'u64
  Prime2 = 0xC2B2AE3D27D4EB4F'u64
  Prime3 = 0x165667B19E3779F9'u64
  Prime4 = 0x85EBCA77C2B2AE63'u64
  Prime5 = 0x27D4EB2F165667C5'u64

func rotl(x: uint64, r: int): uint64 {.inline.} =
  (x shl r) or (x shr (64 - r))

func readU64LE(data: openArray[byte], off: int): uint64 {.inline.} =
  for i in 0 ..< 8:
    result = result or (uint64(data[off + i]) shl (i * 8))

func readU32LE(data: openArray[byte], off: int): uint32 {.inline.} =
  for i in 0 ..< 4:
    result = result or (uint32(data[off + i]) shl (i * 8))

func round(acc, input: uint64): uint64 {.inline.} =
  var a = acc + input * Prime2
  a = rotl(a, 31)
  a * Prime1

func mergeRound(acc, val: uint64): uint64 {.inline.} =
  let v = round(0'u64, val)
  var a = acc xor v
  a = a * Prime1 + Prime4
  a

func xxh64*(data: openArray[byte], seed: uint64 = 0): uint64 =
  ## XXH64 over `data`. `seed` defaults to 0, which is what CTFS namespace key
  ## derivation uses.
  let len = data.len
  var h: uint64
  var idx = 0

  if len >= 32:
    var v1 = seed + Prime1 + Prime2
    var v2 = seed + Prime2
    var v3 = seed
    var v4 = seed - Prime1
    while idx + 32 <= len:
      v1 = round(v1, readU64LE(data, idx));      idx += 8
      v2 = round(v2, readU64LE(data, idx));      idx += 8
      v3 = round(v3, readU64LE(data, idx));      idx += 8
      v4 = round(v4, readU64LE(data, idx));      idx += 8
    h = rotl(v1, 1) + rotl(v2, 7) + rotl(v3, 12) + rotl(v4, 18)
    h = mergeRound(h, v1)
    h = mergeRound(h, v2)
    h = mergeRound(h, v3)
    h = mergeRound(h, v4)
  else:
    h = seed + Prime5

  h = h + uint64(len)

  while idx + 8 <= len:
    let k1 = round(0'u64, readU64LE(data, idx))
    h = h xor k1
    h = rotl(h, 27) * Prime1 + Prime4
    idx += 8

  if idx + 4 <= len:
    h = h xor (uint64(readU32LE(data, idx)) * Prime1)
    h = rotl(h, 23) * Prime2 + Prime3
    idx += 4

  while idx < len:
    h = h xor (uint64(data[idx]) * Prime5)
    h = rotl(h, 11) * Prime1
    idx += 1

  h = h xor (h shr 33)
  h = h * Prime2
  h = h xor (h shr 29)
  h = h * Prime3
  h = h xor (h shr 32)
  h

func correlationKey*(traceIdBe: openArray[byte],
                     spanIdBe: openArray[byte]): uint64 =
  ## The `corrmark.ns` key for a distributed-trace span (`kind = 0`).
  ##
  ## `traceIdBe` is the 16 big-endian bytes of the trace id and `spanIdBe` the
  ## 8 big-endian bytes of the span id — WIRE ORDER, not a lowercase-hex
  ## rendering. Hashing the hex text instead would produce a different key for
  ## the same span, which is the kind of mismatch that shows up as a lookup
  ## that silently finds nothing.
  var buf: array[24, byte]
  let tn = min(traceIdBe.len, 16)
  for i in 0 ..< tn:
    buf[i] = traceIdBe[i]
  let sn = min(spanIdBe.len, 8)
  for i in 0 ..< sn:
    buf[16 + i] = spanIdBe[i]
  xxh64(buf, 0)
