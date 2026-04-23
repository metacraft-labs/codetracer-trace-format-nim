{.push raises: [].}

## Tests for the in-memory B-tree namespace key index.

import std/monotimes
import std/times
import std/algorithm

import codetracer_ctfs/btree

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc keyToDescriptor(key: uint64, size: int): seq[byte] =
  ## Produce a deterministic descriptor from a key. The first 8 bytes are
  ## the key in little-endian; remaining bytes (if size=16) are zero-padded.
  result = newSeq[byte](size)
  var k = key
  for i in 0 ..< 8:
    result[i] = byte(k and 0xFF)
    k = k shr 8

## Simple LCG PRNG (no need for crypto quality).
type Rng = object
  state: uint64

proc next(r: var Rng): uint64 =
  r.state = r.state * 6364136223846793005'u64 + 1442695040888963407'u64
  r.state

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

proc test_btree_insert_lookup_1m() =
  ## Insert 1M sorted keys (key = i*10), lookup each, verify correct descriptor.
  const N = 1_000_000
  var tree = initBTree(8)
  for i in 0'u64 ..< N:
    let key = i * 10
    tree.insert(key, keyToDescriptor(key, 8))

  doAssert tree.count == N

  for i in 0'u64 ..< N:
    let key = i * 10
    let res = tree.lookup(key)
    doAssert res.isOk, "lookup failed for key=" & $key
    doAssert res.get() == keyToDescriptor(key, 8),
      "descriptor mismatch for key=" & $key

  # Verify missing key
  let miss = tree.lookup(5'u64)
  doAssert miss.isErr, "expected miss for key=5"

  echo "PASS: test_btree_insert_lookup_1m"

proc test_btree_range_scan() =
  ## Insert 10K keys, range scan [1000, 5000], verify returns correct sorted
  ## subset.
  const N = 10_000
  var tree = initBTree(8)
  for i in 0'u64 ..< N:
    let key = i * 3
    tree.insert(key, keyToDescriptor(key, 8))

  # Collect via iterator
  var results: seq[BTreeEntry]
  for entry in tree.rangeIter(1000'u64, 5000'u64):
    results.add(entry)

  # Verify sorted
  for i in 1 ..< results.len:
    doAssert results[i].key > results[i - 1].key, "range scan not sorted"

  # Verify bounds
  doAssert results.len > 0, "range scan returned nothing"
  doAssert results[0].key >= 1000'u64
  doAssert results[^1].key <= 5000'u64

  # Verify we got the right count: keys are multiples of 3 in [1000, 5000]
  # first key >= 1000 that's a multiple of 3: 1002 (334*3)
  # last key <= 5000 that's a multiple of 3: 4998 (1666*3)
  # count = 1666 - 334 + 1 = 1333
  let expectedFirst = 1002'u64  # ceil(1000/3)*3
  let expectedLast = 4998'u64   # floor(5000/3)*3
  let expectedCount = int((expectedLast - expectedFirst) div 3) + 1
  doAssert results.len == expectedCount,
    "range scan count: got " & $results.len & " want " & $expectedCount
  doAssert results[0].key == expectedFirst
  doAssert results[^1].key == expectedLast

  # Also test rangeScan with openArray
  var buf = newSeq[BTreeEntry](expectedCount + 10)
  let written = tree.rangeScan(1000'u64, 5000'u64, buf)
  doAssert written == expectedCount

  echo "PASS: test_btree_range_scan"

proc test_btree_unsorted_insert() =
  ## Insert 10K keys in random order, verify all lookups return correct results.
  const N = 10_000
  var rng = Rng(state: 42'u64)

  # Generate shuffled keys
  var keys = newSeq[uint64](N)
  for i in 0 ..< N:
    keys[i] = uint64(i) * 7 + 1

  # Fisher-Yates shuffle
  for i in countdown(N - 1, 1):
    let j = int(rng.next() mod uint64(i + 1))
    swap(keys[i], keys[j])

  var tree = initBTree(16)  # Use Type B size
  for key in keys:
    tree.insert(key, keyToDescriptor(key, 16))

  # Verify all
  for key in keys:
    let res = tree.lookup(key)
    doAssert res.isOk, "lookup failed for key=" & $key
    doAssert res.get() == keyToDescriptor(key, 16),
      "descriptor mismatch for key=" & $key

  echo "PASS: test_btree_unsorted_insert"

proc bench_btree_lookup_latency() =
  ## 1M keys, 100K random lookups, measure median latency.
  const N = 1_000_000
  const Q = 100_000

  var tree = initBTree(8)
  for i in 0'u64 ..< N:
    let key = i * 10
    tree.insert(key, keyToDescriptor(key, 8))

  var rng = Rng(state: 12345'u64)

  # Generate random lookup keys (all valid)
  var queryKeys = newSeq[uint64](Q)
  for i in 0 ..< Q:
    queryKeys[i] = (rng.next() mod N) * 10

  # Time all lookups
  var durations = newSeq[int64](Q)
  for i in 0 ..< Q:
    let t0 = getMonoTime()
    let res = tree.lookup(queryKeys[i])
    let t1 = getMonoTime()
    doAssert res.isOk
    durations[i] = (t1 - t0).inNanoseconds

  # Sort for median
  sort(durations)
  let medianNs = durations[Q div 2]
  let p99Ns = durations[Q * 99 div 100]

  echo "{\"test\":\"bench_btree_lookup_latency\",\"median_ns\":" &
    $medianNs & ",\"p99_ns\":" & $p99Ns & ",\"count\":" & $Q & "}"

  doAssert medianNs < 10000,
    "median lookup latency " & $medianNs & "ns exceeds 10000ns threshold"

  echo "PASS: bench_btree_lookup_latency"

proc test_btree_serialize_deserialize() =
  ## Insert 10K keys, serialize, deserialize, verify all lookups still work.
  const N = 10_000
  var rng = Rng(state: 99'u64)

  # Test with both Type A (8-byte) and Type B (16-byte) descriptors.
  for descSize in [8, 16]:
    var tree = initBTree(descSize)

    # Insert keys in random order.
    var keys = newSeq[uint64](N)
    for i in 0 ..< N:
      keys[i] = uint64(i) * 13 + 7
    # Shuffle.
    for i in countdown(N - 1, 1):
      let j = int(rng.next() mod uint64(i + 1))
      swap(keys[i], keys[j])

    for key in keys:
      tree.insert(key, keyToDescriptor(key, descSize))

    doAssert tree.count == uint64(N)

    # Serialize.
    let data = tree.serialize()
    doAssert data.len > 0, "serialize produced empty output"

    # Verify block alignment: payload after 16-byte header should be multiple
    # of 4096.
    doAssert (data.len - 16) mod 4096 == 0,
      "serialized data not block-aligned"

    # Deserialize.
    let res = deserialize(data, descSize)
    doAssert res.isOk, "deserialize failed: " & res.error

    let restored = res.get()
    doAssert restored.count == uint64(N),
      "count mismatch: " & $restored.count & " vs " & $N

    # Verify every key lookup returns the correct descriptor.
    for key in keys:
      let lres = restored.lookup(key)
      doAssert lres.isOk, "lookup failed for key=" & $key & " (descSize=" & $descSize & ")"
      doAssert lres.get() == keyToDescriptor(key, descSize),
        "descriptor mismatch for key=" & $key

    # Verify a missing key is still missing.
    let miss = restored.lookup(1'u64)  # 1 is not in the key set (keys are 7 + 13*i)
    doAssert miss.isErr, "expected miss for key=1 (descSize=" & $descSize & ")"

    # Verify range scan works on deserialized tree.
    var rangeResults: seq[BTreeEntry]
    for entry in restored.rangeIter(100'u64, 500'u64):
      rangeResults.add(entry)
    doAssert rangeResults.len > 0, "range scan on deserialized tree returned nothing"
    for i in 1 ..< rangeResults.len:
      doAssert rangeResults[i].key > rangeResults[i - 1].key,
        "range scan not sorted after deserialize"

  echo "PASS: test_btree_serialize_deserialize"

when isMainModule:
  test_btree_insert_lookup_1m()
  test_btree_range_scan()
  test_btree_unsorted_insert()
  test_btree_serialize_deserialize()
  bench_btree_lookup_latency()
  echo "All B-tree tests passed."
