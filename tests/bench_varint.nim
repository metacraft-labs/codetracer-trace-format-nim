{.push raises: [].}

## Varint throughput benchmark: encode/decode 10M values, verify > 100M ops/sec.
## Must be compiled with -d:release.

import std/monotimes
import codetracer_trace_writer/varint

const N = 10_000_000

proc getMonoNanos(): int64 {.raises: [].} =
  getMonoTime().ticks

proc bench_varint_throughput() {.raises: [].} =
  # Generate 10M pseudo-random values using a simple LCG.
  # Mix of value ranges to approximate real-world trace ID distributions:
  #   ~50% small (1-2 byte varints), ~30% medium (3-4 bytes), ~20% large (5+ bytes)
  var values: seq[uint64]
  values.setLen(N)
  var state: uint64 = 0xDEAD_BEEF_CAFE_1234'u64
  for i in 0 ..< N:
    state = state * 6364136223846793005'u64 + 1442695040888963407'u64
    let bucket = state shr 62  # top 2 bits: 0,1,2,3
    case bucket
    of 0, 1:  # ~50%: 0..16383 (1-2 byte varints)
      values[i] = (state shr 2) and 0x3FFF
    of 2:     # ~25%: 0..2^28-1 (3-4 byte varints)
      values[i] = (state shr 2) and 0xFFF_FFFF
    else:     # ~25%: full range (5-10 byte varints)
      values[i] = state

  # --- Encode benchmark ---
  # Pre-allocate buffer: max 10 bytes per varint
  var buf = newSeq[byte](N * 10)
  var writePos = 0

  let encStart = getMonoNanos()
  for i in 0 ..< N:
    encodeVarintTo(values[i], buf, writePos)
  let encEnd = getMonoNanos()

  let totalBytes = writePos

  let encDurationNs = float64(encEnd - encStart)
  let encOpsPerSec = float64(N) / (encDurationNs / 1e9)

  echo "{\"name\": \"varint_encode_throughput\", \"unit\": \"ops/sec\", \"value\": " &
    $int(encOpsPerSec) & "}"

  # --- Decode benchmark ---
  # Shrink buffer to actual size so bounds check in decodeVarint is tight
  buf.setLen(totalBytes)
  var readPos = 0
  let decStart = getMonoNanos()
  for i in 0 ..< N:
    let r = decodeVarint(buf, readPos)
    doAssert r.isOk, "decode failed at index " & $i
    doAssert r.get == values[i], "mismatch at index " & $i
  let decEnd = getMonoNanos()

  doAssert readPos == totalBytes, "did not consume entire encoded region"

  let decDurationNs = float64(decEnd - decStart)
  let decOpsPerSec = float64(N) / (decDurationNs / 1e9)

  echo "{\"name\": \"varint_decode_throughput\", \"unit\": \"ops/sec\", \"value\": " &
    $int(decOpsPerSec) & "}"

  # Assert > 100M ops/sec (this benchmark must be compiled with -d:release)
  doAssert encOpsPerSec > 100_000_000.0,
    "encode throughput too low: " & $int(encOpsPerSec) & " ops/sec (need > 100M)"
  doAssert decOpsPerSec > 100_000_000.0,
    "decode throughput too low: " & $int(decOpsPerSec) & " ops/sec (need > 100M)"

  echo "PASS: bench_varint_throughput"

bench_varint_throughput()
