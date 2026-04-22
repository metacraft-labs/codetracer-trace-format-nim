when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Tests for InterningTable writer and reader.

import results
import codetracer_ctfs
import codetracer_trace_writer/interning_table

# ---------------------------------------------------------------------------
# test_interning_table_roundtrip
# ---------------------------------------------------------------------------

proc test_interning_table_roundtrip() {.raises: [].} =
  const numPaths = 10_000

  var ctfs = createCtfs()
  let tablesRes = initTraceInterningTables(ctfs)
  doAssert tablesRes.isOk, "initTraceInterningTables failed: " & tablesRes.error
  var tables = tablesRes.get()

  # Intern 10K paths
  for i in 0 ..< numPaths:
    let path = "path_" & $i
    let idRes = ctfs.ensurePathId(tables, path)
    doAssert idRes.isOk, "ensurePathId failed for " & path & ": " & idRes.error
    doAssert idRes.get() == uint64(i), "path ID mismatch: got " & $idRes.get() & " expected " & $i

  # Intern some functions
  let funcNames = ["main", "foo", "bar", "baz", "helper"]
  for i in 0 ..< funcNames.len:
    let idRes = ctfs.ensureFunctionId(tables, funcNames[i])
    doAssert idRes.isOk, "ensureFunctionId failed: " & idRes.error
    doAssert idRes.get() == uint64(i)

  # Intern some types
  let typeNames = ["int", "string", "float", "bool", "seq[int]", "Table[string, int]"]
  for i in 0 ..< typeNames.len:
    let idRes = ctfs.ensureTypeId(tables, typeNames[i])
    doAssert idRes.isOk, "ensureTypeId failed: " & idRes.error
    doAssert idRes.get() == uint64(i)

  # Intern some varnames
  let varNames = ["x", "y", "result", "counter", "tmp"]
  for i in 0 ..< varNames.len:
    let idRes = ctfs.ensureVarnameId(tables, varNames[i])
    doAssert idRes.isOk, "ensureVarnameId failed: " & idRes.error
    doAssert idRes.get() == uint64(i)

  # Verify counts
  doAssert tables.paths.count() == uint64(numPaths),
    "paths count: " & $tables.paths.count()
  doAssert tables.funcs.count() == uint64(funcNames.len),
    "funcs count: " & $tables.funcs.count()
  doAssert tables.types.count() == uint64(typeNames.len),
    "types count: " & $tables.types.count()
  doAssert tables.varnames.count() == uint64(varNames.len),
    "varnames count: " & $tables.varnames.count()

  # Serialize to bytes
  let rawBytes = ctfs.toBytes()

  # Create readers
  let pathsReaderRes = initInterningTableReader(rawBytes, "paths")
  doAssert pathsReaderRes.isOk, "paths reader failed: " & pathsReaderRes.error
  let pathsReader = pathsReaderRes.get()

  let funcsReaderRes = initInterningTableReader(rawBytes, "funcs")
  doAssert funcsReaderRes.isOk, "funcs reader failed: " & funcsReaderRes.error
  let funcsReader = funcsReaderRes.get()

  let typesReaderRes = initInterningTableReader(rawBytes, "types")
  doAssert typesReaderRes.isOk, "types reader failed: " & typesReaderRes.error
  let typesReader = typesReaderRes.get()

  let varnamesReaderRes = initInterningTableReader(rawBytes, "varnames")
  doAssert varnamesReaderRes.isOk, "varnames reader failed: " & varnamesReaderRes.error
  let varnamesReader = varnamesReaderRes.get()

  # Verify reader counts
  doAssert pathsReader.count() == uint64(numPaths),
    "paths reader count: " & $pathsReader.count()
  doAssert funcsReader.count() == uint64(funcNames.len)
  doAssert typesReader.count() == uint64(typeNames.len)
  doAssert varnamesReader.count() == uint64(varNames.len)

  # Read back all paths and verify
  for i in 0 ..< numPaths:
    let expected = "path_" & $i
    let readRes = pathsReader.readById(uint64(i))
    doAssert readRes.isOk, "readById failed for path " & $i & ": " & readRes.error
    doAssert readRes.get() == expected,
      "path mismatch at " & $i & ": got '" & readRes.get() & "' expected '" & expected & "'"

  # Read back functions
  for i in 0 ..< funcNames.len:
    let readRes = funcsReader.readById(uint64(i))
    doAssert readRes.isOk, "readById failed for func " & $i & ": " & readRes.error
    doAssert readRes.get() == funcNames[i],
      "func mismatch at " & $i & ": got '" & readRes.get() & "'"

  # Read back types
  for i in 0 ..< typeNames.len:
    let readRes = typesReader.readById(uint64(i))
    doAssert readRes.isOk
    doAssert readRes.get() == typeNames[i],
      "type mismatch at " & $i & ": got '" & readRes.get() & "'"

  # Read back varnames
  for i in 0 ..< varNames.len:
    let readRes = varnamesReader.readById(uint64(i))
    doAssert readRes.isOk
    doAssert readRes.get() == varNames[i],
      "varname mismatch at " & $i & ": got '" & readRes.get() & "'"

  echo "PASS: test_interning_table_roundtrip"

# ---------------------------------------------------------------------------
# test_interning_deduplication
# ---------------------------------------------------------------------------

proc test_interning_deduplication() {.raises: [].} =
  var ctfs = createCtfs()
  let writerRes = initInterningTableWriter(ctfs, "dedup")
  doAssert writerRes.isOk, "initInterningTableWriter failed: " & writerRes.error
  var writer = writerRes.get()

  # Intern "hello" 5 times — should get the same ID each time
  var helloId: uint64
  for i in 0 ..< 5:
    let idRes = ctfs.ensureId(writer, "hello")
    doAssert idRes.isOk, "ensureId failed: " & idRes.error
    if i == 0:
      helloId = idRes.get()
    else:
      doAssert idRes.get() == helloId,
        "dedup failed: got " & $idRes.get() & " expected " & $helloId

  # Intern "world" — should get a different ID
  let worldRes = ctfs.ensureId(writer, "world")
  doAssert worldRes.isOk
  doAssert worldRes.get() != helloId,
    "world got same ID as hello: " & $worldRes.get()
  doAssert worldRes.get() == 1,
    "world should have ID 1, got " & $worldRes.get()

  # Count should be 2, not 6
  doAssert writer.count() == 2,
    "count should be 2, got " & $writer.count()

  echo "PASS: test_interning_deduplication"

# ---------------------------------------------------------------------------
# test_interning_table_persistence
# ---------------------------------------------------------------------------

proc test_interning_table_persistence() {.raises: [].} =
  var ctfs = createCtfs()
  let writerRes = initInterningTableWriter(ctfs, "persist")
  doAssert writerRes.isOk
  var writer = writerRes.get()

  # Intern several entries
  let entries = [
    "alpha", "beta", "gamma", "delta", "epsilon",
    "zeta", "eta", "theta", "iota", "kappa"]
  for i in 0 ..< entries.len:
    let idRes = ctfs.ensureId(writer, entries[i])
    doAssert idRes.isOk, "ensureId failed for " & entries[i] & ": " & idRes.error
    doAssert idRes.get() == uint64(i)

  doAssert writer.count() == uint64(entries.len)

  # Serialize to bytes
  let rawBytes = ctfs.toBytes()

  # Create reader from bytes
  let readerRes = initInterningTableReader(rawBytes, "persist")
  doAssert readerRes.isOk, "initInterningTableReader failed: " & readerRes.error
  let reader = readerRes.get()

  # Verify count matches
  doAssert reader.count() == uint64(entries.len),
    "reader count mismatch: " & $reader.count() & " vs " & $entries.len

  # Verify all entries readable and correct
  for i in 0 ..< entries.len:
    let readRes = reader.readById(uint64(i))
    doAssert readRes.isOk, "readById failed for " & $i & ": " & readRes.error
    doAssert readRes.get() == entries[i],
      "entry mismatch at " & $i & ": got '" & readRes.get() & "' expected '" & entries[i] & "'"

  # Out of range read should fail
  let badRes = reader.readById(uint64(entries.len))
  doAssert badRes.isErr, "reading out-of-range ID should fail"

  echo "PASS: test_interning_table_persistence"

# Run all tests
test_interning_table_roundtrip()
test_interning_deduplication()
test_interning_table_persistence()
