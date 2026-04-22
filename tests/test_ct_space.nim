{.push raises: [].}

## Tests for the CTFS space analyzer.

import std/[json, math, strutils]
import results
import codetracer_ctfs/container
import codetracer_ctfs/types
import codetracer_ctfs/namespace
import codetracer_ctfs/btree
import codetracer_ctfs/space_analyzer

# ---------------------------------------------------------------------------
# Simple LCG PRNG (deterministic, no crypto needed)
# ---------------------------------------------------------------------------

type Rng = object
  state: uint64

proc next(r: var Rng): uint64 =
  r.state = r.state * 6364136223846793005'u64 + 1442695040888963407'u64
  r.state

# ---------------------------------------------------------------------------
# test_ct_space_basic
# ---------------------------------------------------------------------------

proc test_ct_space_basic() =
  ## Create a CTFS container, add several files with known sizes.
  ## Create a Namespace with 10K entries of known sizes (mix of 8, 16, 24, 31 bytes).
  ## Analyze both the CTFS and the namespace.
  echo "--- test_ct_space_basic ---"

  # -- CTFS container analysis --
  var c = createCtfs()

  # Add file 1: 100 bytes
  let f1Res = c.addFile("meta.json")
  doAssert f1Res.isOk, "addFile meta.json failed"
  var f1 = f1Res.get()
  var data1 = newSeq[byte](100)
  for i in 0 ..< data1.len:
    data1[i] = byte(i mod 256)
  let w1 = c.writeToFile(f1, data1)
  doAssert w1.isOk, "write meta.json failed"

  # Add file 2: 8000 bytes (spans 2 blocks)
  let f2Res = c.addFile("events.dat")
  doAssert f2Res.isOk, "addFile events.dat failed"
  var f2 = f2Res.get()
  var data2 = newSeq[byte](8000)
  for i in 0 ..< data2.len:
    data2[i] = byte(i mod 256)
  let w2 = c.writeToFile(f2, data2)
  doAssert w2.isOk, "write events.dat failed"

  # Add file 3: 512 bytes
  let f3Res = c.addFile("index.bin")
  doAssert f3Res.isOk, "addFile index.bin failed"
  var f3 = f3Res.get()
  var data3 = newSeq[byte](512)
  for i in 0 ..< data3.len:
    data3[i] = byte(i mod 256)
  let w3 = c.writeToFile(f3, data3)
  doAssert w3.isOk, "write index.bin failed"

  let bytes = c.toBytes()
  let reportRes = analyzeCtfs(bytes)
  doAssert reportRes.isOk, "analyzeCtfs failed: " & reportRes.error
  let report = reportRes.get()

  # Verify file count.
  doAssert report.files.len == 3,
    "expected 3 files, got " & $report.files.len

  # Find files by name and verify data bytes.
  var foundMeta, foundEvents, foundIndex: bool
  for f in report.files:
    case f.name
    of "meta.json":
      doAssert f.dataBytes == 100, "meta.json: expected 100 bytes, got " & $f.dataBytes
      doAssert f.blockCount == 1, "meta.json: expected 1 block, got " & $f.blockCount
      foundMeta = true
    of "events.dat":
      doAssert f.dataBytes == 8000, "events.dat: expected 8000 bytes, got " & $f.dataBytes
      doAssert f.blockCount == 2, "events.dat: expected 2 blocks, got " & $f.blockCount
      foundEvents = true
    of "index.bin":
      doAssert f.dataBytes == 512, "index.bin: expected 512 bytes, got " & $f.dataBytes
      doAssert f.blockCount == 1, "index.bin: expected 1 block, got " & $f.blockCount
      foundIndex = true
    else:
      doAssert false, "unexpected file: " & f.name

  doAssert foundMeta and foundEvents and foundIndex, "not all files found"

  # -- Namespace analysis --
  const N = 10_000
  var ns = initNamespace("test_ns", ltTypeA)
  var rng = Rng(state: 42'u64)

  # Insert entries with known sizes: cycle through 8, 16, 24, 31.
  let sizes = [8, 16, 24, 31]
  var entrySizes: seq[int]
  for i in 0 ..< N:
    let sz = sizes[i mod sizes.len]
    var data = newSeq[byte](sz)
    for j in 0 ..< sz:
      data[j] = byte(rng.next() and 0xFF)
    let res = ns.append(uint64(i), data)
    doAssert res.isOk, "namespace append failed at i=" & $i & ": " & res.error
    entrySizes.add(sz)

  let nsStats = analyzeNamespace(ns)
  doAssert nsStats.entryCount == uint64(N),
    "expected " & $N & " entries, got " & $nsStats.entryCount
  doAssert nsStats.sizeMin == 8, "expected min=8, got " & $nsStats.sizeMin
  doAssert nsStats.sizeMax == 31, "expected max=31, got " & $nsStats.sizeMax

  # Median: sorted sizes of 10K entries cycling [8,16,24,31].
  # 2500 of each. Sorted: 2500x8, 2500x16, 2500x24, 2500x31.
  # Median (index 5000) = 24.
  doAssert nsStats.sizeMedian == 24,
    "expected median=24, got " & $nsStats.sizeMedian

  # Pool analysis with entry sizes for accurate fragmentation.
  let poolStats = analyzePoolWithSizes(ns.pool, entrySizes)
  doAssert poolStats.len > 0, "expected at least one pool class"

  # All entries fit in pool class 0 (32B) since max size is 31
  # and 31 fits in usedBytesBits(0) = 5 bits -> max 31.
  doAssert poolStats.len == 1, "expected 1 pool class, got " & $poolStats.len
  doAssert poolStats[0].poolClass == 0, "expected pool class 0"
  doAssert poolStats[0].allocatedSlots == N,
    "expected " & $N & " allocated slots, got " & $poolStats[0].allocatedSlots

  # B-tree analysis
  let btStats = analyzeBTree(ns.tree)
  doAssert btStats.totalEntries == uint64(N),
    "expected " & $N & " entries in btree"
  doAssert btStats.depth >= 1, "btree depth should be >= 1"
  doAssert btStats.nodeCount >= 1, "btree node count should be >= 1"

  echo "PASS: test_ct_space_basic"

# ---------------------------------------------------------------------------
# test_ct_space_fragmentation
# ---------------------------------------------------------------------------

proc test_ct_space_fragmentation() =
  ## Create a Namespace with 1000 entries all 33 bytes (forces pool class 1 = 64B).
  ## Analyze pool utilization.
  ## Verify internal fragmentation is approximately (64-33)/64 = 48.4%.
  echo "--- test_ct_space_fragmentation ---"

  const N = 1000
  const EntrySize = 33
  var ns = initNamespace("frag_ns", ltTypeA)
  var rng = Rng(state: 99'u64)

  var entrySizes: seq[int]
  for i in 0 ..< N:
    var data = newSeq[byte](EntrySize)
    for j in 0 ..< EntrySize:
      data[j] = byte(rng.next() and 0xFF)
    let res = ns.append(uint64(i), data)
    doAssert res.isOk, "append failed at i=" & $i & ": " & res.error
    entrySizes.add(EntrySize)

  let poolStats = analyzePoolWithSizes(ns.pool, entrySizes)

  # 33 bytes doesn't fit pool class 0 (32B), so goes to class 1 (64B).
  doAssert poolStats.len == 1, "expected 1 pool class, got " & $poolStats.len
  doAssert poolStats[0].poolClass == 1,
    "expected pool class 1 (64B), got " & $poolStats[0].poolClass
  doAssert poolStats[0].allocatedSlots == N,
    "expected " & $N & " allocated slots"

  # Internal fragmentation: (64 - 33) / 64 = 0.484375
  let expectedFrag = float(64 - EntrySize) / float(64)
  let actualFrag = poolStats[0].internalFragmentation
  let diff = abs(actualFrag - expectedFrag)
  doAssert diff < 0.01,
    "fragmentation mismatch: expected ~" & $expectedFrag &
    " got " & $actualFrag

  echo "PASS: test_ct_space_fragmentation"

# ---------------------------------------------------------------------------
# test_ct_space_json_output
# ---------------------------------------------------------------------------

proc test_ct_space_json_output() {.raises: [JsonParsingError, ValueError, IOError, OSError].} =
  ## Generate a report, convert to JSON, parse and verify fields.
  echo "--- test_ct_space_json_output ---"

  var c = createCtfs()
  let fRes = c.addFile("test.dat")
  doAssert fRes.isOk
  var f = fRes.get()
  var data = newSeq[byte](200)
  for i in 0 ..< data.len:
    data[i] = byte(i mod 256)
  let wRes = c.writeToFile(f, data)
  doAssert wRes.isOk

  let bytes = c.toBytes()
  let reportRes = analyzeCtfs(bytes)
  doAssert reportRes.isOk
  var report = reportRes.get()

  # Add namespace stats to the report for richer JSON.
  var ns = initNamespace("json_ns", ltTypeA)
  var smallData = newSeq[byte](10)
  for i in 0 ..< 10:
    smallData[i] = byte(i)
  for i in 0 ..< 100:
    let res = ns.append(uint64(i), smallData)
    doAssert res.isOk

  report.namespaces.add(analyzeNamespace(ns))

  var entrySizes: seq[int]
  for i in 0 ..< 100:
    entrySizes.add(10)
  report.pools = analyzePoolWithSizes(ns.pool, entrySizes)
  report.btreeStats.add(analyzeBTree(ns.tree))

  let jsonStr = report.toJson()

  # Parse JSON to verify it's valid.
  let parsed = parseJson(jsonStr)

  # Verify all expected top-level fields.
  doAssert parsed.hasKey("totalBlocks"), "missing totalBlocks"
  doAssert parsed.hasKey("totalBytes"), "missing totalBytes"
  doAssert parsed.hasKey("headerBytes"), "missing headerBytes"
  doAssert parsed.hasKey("fileEntryBytes"), "missing fileEntryBytes"
  doAssert parsed.hasKey("files"), "missing files"
  doAssert parsed.hasKey("namespaces"), "missing namespaces"
  doAssert parsed.hasKey("pools"), "missing pools"
  doAssert parsed.hasKey("btreeStats"), "missing btreeStats"

  # Verify files array.
  let files = parsed["files"]
  doAssert files.kind == JArray, "files should be array"
  doAssert files.len == 1, "expected 1 file"
  doAssert files[0].hasKey("name"), "file missing name"
  doAssert files[0].hasKey("blockCount"), "file missing blockCount"
  doAssert files[0].hasKey("dataBytes"), "file missing dataBytes"
  doAssert files[0].hasKey("allocatedBytes"), "file missing allocatedBytes"
  doAssert files[0].hasKey("utilization"), "file missing utilization"

  # Verify namespaces array.
  let namespaces = parsed["namespaces"]
  doAssert namespaces.kind == JArray, "namespaces should be array"
  doAssert namespaces.len == 1, "expected 1 namespace"
  doAssert namespaces[0].hasKey("name"), "namespace missing name"
  doAssert namespaces[0].hasKey("entryCount"), "namespace missing entryCount"

  # Verify pools array.
  let pools = parsed["pools"]
  doAssert pools.kind == JArray, "pools should be array"
  doAssert pools.len >= 1, "expected at least 1 pool"
  doAssert pools[0].hasKey("poolClass"), "pool missing poolClass"
  doAssert pools[0].hasKey("internalFragmentation"), "pool missing internalFragmentation"

  # Verify btreeStats array.
  let btrees = parsed["btreeStats"]
  doAssert btrees.kind == JArray, "btreeStats should be array"
  doAssert btrees.len == 1, "expected 1 btree"
  doAssert btrees[0].hasKey("depth"), "btree missing depth"
  doAssert btrees[0].hasKey("nodeCount"), "btree missing nodeCount"
  doAssert btrees[0].hasKey("totalEntries"), "btree missing totalEntries"

  echo "PASS: test_ct_space_json_output"

# ---------------------------------------------------------------------------
# test_ct_space_text_output
# ---------------------------------------------------------------------------

proc test_ct_space_text_output() =
  ## Verify text output contains expected sections.
  echo "--- test_ct_space_text_output ---"

  var c = createCtfs()
  let fRes = c.addFile("test.dat")
  doAssert fRes.isOk
  var f = fRes.get()
  var data = newSeq[byte](200)
  for i in 0 ..< data.len:
    data[i] = byte(i mod 256)
  let wRes = c.writeToFile(f, data)
  doAssert wRes.isOk

  let bytes = c.toBytes()
  let reportRes = analyzeCtfs(bytes)
  doAssert reportRes.isOk
  let report = reportRes.get()

  let text = report.toText()
  doAssert text.len > 0, "text output should not be empty"

  # Check that expected sections appear.
  var hasTotalBlocks = false
  var hasFiles = false
  for line in text.split('\n'):
    if "Total blocks" in line:
      hasTotalBlocks = true
    if "test.dat" in line:
      hasFiles = true

  doAssert hasTotalBlocks, "text missing 'Total blocks'"
  doAssert hasFiles, "text missing file name"

  echo "PASS: test_ct_space_text_output"

# ---------------------------------------------------------------------------
# test_ct_space_empty_container
# ---------------------------------------------------------------------------

proc test_ct_space_empty_container() =
  ## Analyze an empty container (no files added).
  echo "--- test_ct_space_empty_container ---"

  var c = createCtfs()
  let bytes = c.toBytes()
  let reportRes = analyzeCtfs(bytes)
  doAssert reportRes.isOk, "analyzeCtfs failed on empty container"
  let report = reportRes.get()

  doAssert report.files.len == 0, "expected 0 files"
  doAssert report.totalBlocks == 1, "expected 1 block (root)"
  doAssert report.headerBytes == HeaderSize + ExtHeaderSize

  echo "PASS: test_ct_space_empty_container"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

when isMainModule:
  test_ct_space_basic()
  test_ct_space_fragmentation()
  try:
    test_ct_space_json_output()
  except JsonParsingError:
    doAssert false, "JSON parsing failed"
  except ValueError:
    doAssert false, "JSON value error"
  except IOError:
    doAssert false, "JSON IO error"
  except OSError:
    doAssert false, "JSON OS error"
  test_ct_space_text_output()
  test_ct_space_empty_container()
  echo "All ct-space tests passed."
