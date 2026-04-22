{.push raises: [].}

## Tests and benchmarks for Namespace (B-tree + sub-block pool integration).

import std/[monotimes, times, algorithm]
import results
import codetracer_ctfs/namespace
import codetracer_ctfs/namespace_descriptor

# ---------------------------------------------------------------------------
# Simple LCG PRNG (deterministic, no crypto needed)
# ---------------------------------------------------------------------------

type Rng = object
  state: uint64

proc next(r: var Rng): uint64 =
  r.state = r.state * 6364136223846793005'u64 + 1442695040888963407'u64
  r.state

proc nextInRange(r: var Rng, lo, hi: uint64): uint64 =
  ## Return a value in [lo, hi].
  let range = hi - lo + 1
  lo + (r.next() mod range)

proc randomData(r: var Rng, minLen, maxLen: int): seq[byte] =
  ## Generate a random byte sequence with length in [minLen, maxLen].
  let length = int(r.nextInRange(uint64(minLen), uint64(maxLen)))
  result = newSeq[byte](length)
  for i in 0 ..< length:
    result[i] = byte(r.next() and 0xFF)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

proc test_namespace_create_append_lookup() =
  ## Create Type A namespace, append 100K entries (key = i, data = random
  ## 8-32 bytes), lookup 1000 random keys, verify data matches.
  const N = 100_000
  const Q = 1_000
  var ns = initNamespace("test_ns", ltTypeA)
  var rng = Rng(state: 42'u64)

  # Generate and insert all entries, keeping a copy of the data.
  var entries = newSeq[seq[byte]](N)
  for i in 0 ..< N:
    let data = rng.randomData(8, 32)
    entries[i] = data
    let res = ns.append(uint64(i), data)
    doAssert res.isOk, "append failed at i=" & $i & ": " & res.error

  doAssert ns.count == uint64(N)

  # Lookup 1000 random keys and verify.
  var lookupRng = Rng(state: 99'u64)
  for q in 0 ..< Q:
    let key = uint64(lookupRng.next() mod uint64(N))
    let res = ns.lookup(key)
    doAssert res.isOk, "lookup failed for key=" & $key & ": " & res.error
    let got = res.get()
    doAssert got == entries[int(key)],
      "data mismatch for key=" & $key &
      " got.len=" & $got.len & " want.len=" & $entries[int(key)].len

  # Verify missing key returns error.
  let miss = ns.lookup(uint64(N + 1))
  doAssert miss.isErr, "expected miss for key beyond range"

  echo "PASS: test_namespace_create_append_lookup"

proc test_namespace_type_b() =
  ## Same test with Type B descriptors.
  const N = 1_000
  var ns = initNamespace("test_ns_b", ltTypeB)
  var rng = Rng(state: 77'u64)

  var entries = newSeq[seq[byte]](N)
  for i in 0 ..< N:
    let data = rng.randomData(8, 32)
    entries[i] = data
    let res = ns.append(uint64(i), data)
    doAssert res.isOk, "append failed at i=" & $i & ": " & res.error

  doAssert ns.count == uint64(N)

  for i in 0 ..< N:
    let res = ns.lookup(uint64(i))
    doAssert res.isOk, "lookup failed for key=" & $i & ": " & res.error
    doAssert res.get() == entries[i],
      "data mismatch for key=" & $i

  echo "PASS: test_namespace_type_b"

proc test_namespace_range_scan() =
  ## Create namespace, insert 10K keys (0, 10, 20, ..., 99990).
  ## Range scan [500, 5000], verify returns correct sorted subset.
  ## Verify both iterator and openArray overload return same results.
  const N = 10_000
  var ns = initNamespace("range_ns", ltTypeA)
  var rng = Rng(state: 55'u64)

  var entries: seq[(uint64, seq[byte])]
  for i in 0 ..< N:
    let key = uint64(i * 10)
    let data = rng.randomData(8, 32)
    entries.add((key, data))
    let res = ns.append(key, data)
    doAssert res.isOk, "append failed at key=" & $key & ": " & res.error

  # Collect via iterator.
  var iterResults: seq[NamespaceEntry]
  for entry in ns.items(500'u64, 5000'u64):
    iterResults.add(NamespaceEntry(key: entry.key, data: entry.data))

  # Verify sorted.
  for i in 1 ..< iterResults.len:
    doAssert iterResults[i].key > iterResults[i - 1].key, "range scan not sorted"

  # Verify bounds.
  doAssert iterResults.len > 0, "range scan returned nothing"
  doAssert iterResults[0].key >= 500'u64
  doAssert iterResults[^1].key <= 5000'u64

  # Expected keys: multiples of 10 in [500, 5000].
  # First: 500, last: 5000. Count = (5000 - 500) / 10 + 1 = 451.
  let expectedCount = 451
  doAssert iterResults.len == expectedCount,
    "range scan count: got " & $iterResults.len & " want " & $expectedCount
  doAssert iterResults[0].key == 500'u64
  doAssert iterResults[^1].key == 5000'u64

  # Verify data matches original entries.
  for entry in iterResults:
    let idx = int(entry.key div 10)
    doAssert entry.data == entries[idx][1],
      "data mismatch in range scan for key=" & $entry.key

  # Verify openArray overload returns same results.
  var buf = newSeq[NamespaceEntry](expectedCount + 10)
  let written = ns.rangeScan(500'u64, 5000'u64, buf)
  doAssert written == expectedCount,
    "rangeScan count: got " & $written & " want " & $expectedCount
  for i in 0 ..< written:
    doAssert buf[i].key == iterResults[i].key,
      "rangeScan key mismatch at i=" & $i
    doAssert buf[i].data == iterResults[i].data,
      "rangeScan data mismatch at i=" & $i

  echo "PASS: test_namespace_range_scan"

proc test_namespace_empty_data_rejected() =
  ## Verify that appending empty data returns an error.
  var ns = initNamespace("empty_ns", ltTypeA)
  var emptyData: seq[byte]
  let res = ns.append(0'u64, emptyData)
  doAssert res.isErr, "expected error for empty data"

  echo "PASS: test_namespace_empty_data_rejected"

proc test_namespace_large_data_rejected() =
  ## Verify that data exceeding 2048 bytes is rejected.
  var ns = initNamespace("large_ns", ltTypeA)
  var bigData = newSeq[byte](2049)
  let res = ns.append(0'u64, bigData)
  doAssert res.isErr, "expected error for oversized data"

  echo "PASS: test_namespace_large_data_rejected"

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

proc bench_namespace_create_append_1m() =
  ## Create Type A namespace, append 1M entries (8-32 bytes each).
  ## Measure total time, assert < 10 seconds.
  const N = 1_000_000
  var ns = initNamespace("bench_ns", ltTypeA)
  var rng = Rng(state: 123'u64)

  let start = cpuTime()
  for i in 0 ..< N:
    let data = rng.randomData(8, 32)
    let res = ns.append(uint64(i), data)
    doAssert res.isOk
  let elapsed = cpuTime() - start

  let throughput = float(N) / elapsed
  echo "{\"benchmark\": \"namespace_create_append_1m\", " &
    "\"entries\": " & $N & ", " &
    "\"elapsed_sec\": " & $elapsed & ", " &
    "\"entries_per_sec\": " & $throughput & "}"
  doAssert elapsed < 10.0,
    "append 1M took " & $elapsed & "s, exceeds 10s threshold"

  echo "PASS: bench_namespace_create_append_1m"

proc bench_namespace_lookup_latency() =
  ## After creating 1M entries, lookup 10K random keys.
  ## Measure median latency, assert < 10000ns (10us).
  const N = 1_000_000
  const Q = 10_000
  var ns = initNamespace("bench_lookup_ns", ltTypeA)
  var rng = Rng(state: 456'u64)

  # Build namespace.
  for i in 0 ..< N:
    let data = rng.randomData(8, 32)
    let res = ns.append(uint64(i), data)
    doAssert res.isOk

  # Generate random lookup keys.
  var lookupRng = Rng(state: 789'u64)
  var queryKeys = newSeq[uint64](Q)
  for i in 0 ..< Q:
    queryKeys[i] = lookupRng.next() mod uint64(N)

  # Time each lookup.
  var durations = newSeq[int64](Q)
  for i in 0 ..< Q:
    let t0 = getMonoTime()
    let res = ns.lookup(queryKeys[i])
    let t1 = getMonoTime()
    doAssert res.isOk
    durations[i] = (t1 - t0).inNanoseconds

  sort(durations)
  let medianNs = durations[Q div 2]
  let p99Ns = durations[Q * 99 div 100]

  echo "{\"benchmark\": \"namespace_lookup_latency\", " &
    "\"median_ns\": " & $medianNs & ", " &
    "\"p99_ns\": " & $p99Ns & ", " &
    "\"count\": " & $Q & "}"
  doAssert medianNs < 10000,
    "median lookup latency " & $medianNs & "ns exceeds 10000ns threshold"

  echo "PASS: bench_namespace_lookup_latency"

proc bench_namespace_space_utilization() =
  ## After creating 100K entries (sizes 8-31 bytes), compute:
  ##   total_data_bytes = sum of all entry sizes
  ##   total_allocated_bytes = sum of poolSize(poolClass) for each entry
  ##   utilization = total_data_bytes / total_allocated_bytes
  ## Assert > 80%.
  ## Note: sizes capped at 31 because pool class 0 (32B) can only represent
  ## usedBytes up to 31 in its 5-bit descriptor field.
  const N = 100_000
  var rng = Rng(state: 321'u64)

  var totalDataBytes: uint64 = 0
  var totalAllocatedBytes: uint64 = 0

  for i in 0 ..< N:
    let data = rng.randomData(24, 31)
    let dataLen = data.len
    totalDataBytes += uint64(dataLen)

    # Determine which pool class this entry would use (must fit both
    # the data bytes and the usedBytes descriptor field).
    var poolClass: uint8 = 0
    while poolClass < 6 and (poolSize(poolClass) < dataLen or
          dataLen > int((1'u16 shl usedBytesBits(poolClass)) - 1)):
      poolClass += 1
    totalAllocatedBytes += uint64(poolSize(poolClass))

  let utilization = float(totalDataBytes) / float(totalAllocatedBytes)

  echo "{\"benchmark\": \"namespace_space_utilization\", " &
    "\"total_data_bytes\": " & $totalDataBytes & ", " &
    "\"total_allocated_bytes\": " & $totalAllocatedBytes & ", " &
    "\"utilization\": " & $utilization & "}"
  doAssert utilization > 0.80,
    "space utilization " & $utilization & " below 80% threshold"

  echo "PASS: bench_namespace_space_utilization"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

when isMainModule:
  test_namespace_create_append_lookup()
  test_namespace_type_b()
  test_namespace_range_scan()
  test_namespace_empty_data_rejected()
  test_namespace_large_data_rejected()
  bench_namespace_create_append_1m()
  bench_namespace_lookup_latency()
  bench_namespace_space_utilization()
  echo "All namespace tests passed."
