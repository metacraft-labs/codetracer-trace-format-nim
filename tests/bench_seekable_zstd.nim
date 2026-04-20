{.push raises: [].}

## Benchmarks for seekable Zstd encoder/decoder.
##
## Output is in a parseable format:
##   BENCH: <name> <value> <unit>

import std/times
import results
import codetracer_ctfs/seekable_zstd

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc makeCompressibleData(size: int): seq[byte] =
  result = newSeq[byte](size)
  for i in 0 ..< size:
    result[i] = byte(i mod 251)

proc nowMs(): float64 =
  ## Current time in milliseconds (monotonic).
  let t = cpuTime()
  t * 1000.0

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

proc bench_encode() =
  ## Benchmark encoding: compress 10 MB of data with seekable Zstd.
  let size = 10 * 1024 * 1024  # 10 MiB
  let data = makeCompressibleData(size)

  let startTime = nowMs()
  var enc = newSeekableZstdEncoder()
  enc.write(data)
  let compressed = enc.finish()
  let elapsed = nowMs() - startTime

  let throughputMBs = (float64(size) / (1024.0 * 1024.0)) / (elapsed / 1000.0)
  let ratio = float64(compressed.len) / float64(size)

  echo "BENCH: encode_time_ms " & $elapsed & " ms"
  echo "BENCH: encode_throughput " & $throughputMBs & " MB/s"
  echo "BENCH: encode_input_size " & $size & " bytes"
  echo "BENCH: encode_output_size " & $compressed.len & " bytes"
  echo "BENCH: encode_ratio " & $ratio & " ratio"

proc bench_decode() =
  ## Benchmark decoding: decompress 10 MB of seekable Zstd data.
  let size = 10 * 1024 * 1024
  let data = makeCompressibleData(size)

  var enc = newSeekableZstdEncoder()
  enc.write(data)
  let compressed = enc.finish()

  let startTime = nowMs()
  let decRes = initSeekableZstdDecoder(compressed)
  doAssert decRes.isOk, "decoder init failed: " & decRes.error
  let dec = decRes.get()
  let decompRes = dec.decompressAll()
  doAssert decompRes.isOk, "decompressAll failed: " & decompRes.error
  let decompressed = decompRes.get()
  let elapsed = nowMs() - startTime

  doAssert decompressed.len == size, "decoded size mismatch"
  let throughputMBs = (float64(size) / (1024.0 * 1024.0)) / (elapsed / 1000.0)

  echo "BENCH: decode_time_ms " & $elapsed & " ms"
  echo "BENCH: decode_throughput " & $throughputMBs & " MB/s"

proc bench_seek() =
  ## Benchmark seeking: seek to 100 random offsets and decompress the target frame.
  let size = 10 * 1024 * 1024
  let data = makeCompressibleData(size)

  var enc = newSeekableZstdEncoder()
  enc.write(data)
  let compressed = enc.finish()

  let decRes = initSeekableZstdDecoder(compressed)
  doAssert decRes.isOk, "decoder init failed: " & decRes.error
  let dec = decRes.get()

  let totalDecomp = dec.seekTable.totalDecompressedSize()
  let numSeeks = 100

  # Generate deterministic "random" offsets using a simple LCG
  var offsets = newSeq[uint64](numSeeks)
  var rng: uint64 = 12345  # seed
  for i in 0 ..< numSeeks:
    rng = rng * 6364136223846793005'u64 + 1442695040888963407'u64
    offsets[i] = rng mod totalDecomp

  let startTime = nowMs()
  for i in 0 ..< numSeeks:
    let seekRes = dec.seekToOffset(offsets[i])
    doAssert seekRes.isOk, "seek failed at offset " & $offsets[i]
    let (frameIdx, _) = seekRes.get()
    let frameRes = dec.decompressFrame(frameIdx)
    doAssert frameRes.isOk, "decompressFrame failed for seek"
  let elapsed = nowMs() - startTime

  let avgMs = elapsed / float64(numSeeks)

  echo "BENCH: seek_total_ms " & $elapsed & " ms"
  echo "BENCH: seek_count " & $numSeeks & " seeks"
  echo "BENCH: seek_avg_ms " & $avgMs & " ms/seek"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

echo "--- Seekable Zstd Benchmarks ---"
bench_encode()
bench_decode()
bench_seek()
echo "--- Done ---"
