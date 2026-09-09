## XXH64 reference vectors.
##
## These are the cross-language contract for `corrmark.ns` keys. A namespace
## key is on-disk format: if Nim, Rust and C# disagree on it, a recording
## written by one is unreadable by another with NO error — the B-tree lookup
## just misses, and the consumer reports "this recording does not cover that
## span". That failure is silent by construction, so it has to be pinned by
## vectors rather than by a round-trip through a single implementation (which
## would agree with itself no matter what it computed).
##
## Every constant below was produced by an INDEPENDENT implementation — the C
## XXH64 in `codetracer-native-recorder/ct_interpose/src/ct_interpose/xxh64.c`
## — and not by this one. That distinction did real work: the seeded-corpus
## vector was originally written from memory as 0xE86CF509D5A0E27B, this
## implementation disagreed, and the C oracle showed the IMPLEMENTATION was
## right and the remembered constant wrong. Had the constant been trusted, the
## "fix" would have been to break a correct hash into agreeing with it.
##
## Re-derive with:
##   cc -O2 -I<ct_interpose_src> -o oracle oracle.c <ct_interpose_src>/xxh64.c

import std/strutils

const PRIME = 2654435761'u64
import ../src/codetracer_ctfs/xxh64

proc bytesOf(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i, c in s:
    result[i] = byte(c)

const Corpus = "Nobody inspects the spammish repetition"

proc test_reference_vectors() =
  # Canonical xxHash reference vectors.
  let empty: seq[byte] = @[]
  doAssert xxh64(empty, 0) == 0xEF46DB3751D8E999'u64,
    "empty/seed0 mismatch: " & toHex(xxh64(empty, 0))
  doAssert xxh64(empty, PRIME) == 0xAC75FDA2929B17EF'u64,
    "empty/seeded mismatch: " & toHex(xxh64(empty, PRIME))

  let c = bytesOf(Corpus)
  doAssert xxh64(c, 0) == 0xFBCEA83C8A378BF1'u64,
    "corpus/seed0 mismatch: " & toHex(xxh64(c, 0))
  doAssert xxh64(c, PRIME) == 0x56DB22DD5B051147'u64,
    "corpus/seeded mismatch: " & toHex(xxh64(c, PRIME))

  echo "PASS: test_reference_vectors"

proc test_length_boundaries() =
  ## Pin every branch of the tail handler against the oracle: the 32-byte
  ## striped loop, the 8-byte lane, the 4-byte lane and the single-byte lane,
  ## plus the boundaries either side of 32 and 64. A hash that is right on
  ## aligned input and wrong on a 5-byte tail passes a round-trip test and
  ## fails on real keys.
  const OracleVectors = [
    (1, 0x2078E1AD38AD738B'u64), (4, 0x6BB99866CB63C0A8'u64),
    (7, 0x31365618AD874893'u64), (8, 0x3BA000679FBEE7B5'u64),
    (15, 0xA3666D452D79E70D'u64), (31, 0x7231380363BB4388'u64),
    (32, 0x56699A69DA28FD3B'u64), (33, 0xD477447593124012'u64),
    (63, 0x8898CD8219F457FC'u64), (64, 0xBAD331060E4CD79A'u64),
    (70, 0xF50882C553DFB471'u64),
  ]
  for (n, expected) in OracleVectors:
    var buf = newSeq[byte](n)
    for i in 0 ..< n:
      buf[i] = byte((i * 7 + 13) and 0xFF)
    doAssert xxh64(buf, 0) == expected,
      "length " & $n & " mismatch: got " & toHex(xxh64(buf, 0))

  # And no two lengths of the same generator collide, which catches a tail
  # handler that silently ignores the trailing bytes.
  var seen: seq[uint64] = @[]
  for n in 0 .. 70:
    var buf = newSeq[byte](n)
    for i in 0 ..< n:
      buf[i] = byte((i * 7 + 13) and 0xFF)
    let h = xxh64(buf, 0)
    doAssert h notin seen, "length " & $n & " collided with a shorter input"
    seen.add(h)
  echo "PASS: test_length_boundaries (11 oracle vectors, 0..70 distinct)"

proc test_correlation_key_is_wire_order() =
  ## The key must be derived from the 24 wire-order bytes, NOT from the hex
  ## text. Both are 'the trace id', and hashing the wrong one yields a stable,
  ## plausible, entirely different key.
  var traceId: array[16, byte]
  var spanId: array[8, byte]
  for i in 0 ..< 16: traceId[i] = byte(0x10 + i)
  for i in 0 ..< 8: spanId[i] = byte(0xA0 + i)

  var buf: seq[byte] = @[]
  for b in traceId: buf.add(b)
  for b in spanId: buf.add(b)

  doAssert correlationKey(traceId, spanId) == xxh64(buf, 0),
    "correlationKey must equal XXH64 over trace_id_be || span_id_be"

  # The hex rendering of the same ids must NOT produce the same key — if it
  # did, this test could not tell the two derivations apart.
  var hexText = ""
  for b in buf: hexText.add(toHex(b, 2).toLowerAscii)
  doAssert correlationKey(traceId, spanId) != xxh64(bytesOf(hexText), 0),
    "wire-order and hex-text derivations must differ"

  # Swapping the two fields must change the key: concatenation order is part
  # of the contract, and a symmetric combiner would hide a field-order bug.
  var swapped: array[8, byte]
  for i in 0 ..< 8: swapped[i] = traceId[i]
  doAssert correlationKey(traceId, spanId) != correlationKey(spanId, swapped),
    "field order must matter"

  echo "PASS: test_correlation_key_is_wire_order"

when isMainModule:
  test_reference_vectors()
  test_length_boundaries()
  test_correlation_key_is_wire_order()
  echo "=== xxh64 tests passed ==="
