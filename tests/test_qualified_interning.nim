when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Tests for the fully-qualified interning-key primitive (IC-M1).
##
## Spec: codetracer-specs/Planned-Features/Interning-Table-Coexistence.md §§2,3,5.
##
## No mocks: every assertion runs against a real Ctfs container, the real
## VariableRecordTable backing, and the real reader read-path.

import results
import codetracer_ctfs
import codetracer_trace_writer/interning_table

# ---------------------------------------------------------------------------
# test_qualified_ids_are_distinct_by_construction
# ---------------------------------------------------------------------------

proc test_qualified_ids_are_distinct_by_construction() {.raises: [].} =
  var ctfs = createCtfs()
  let writerRes = initInterningTableWriter(ctfs, "funcs")
  doAssert writerRes.isOk, "initInterningTableWriter failed: " & writerRes.error
  var w = writerRes.get()

  # Two producers minting the same bare name under different qualifiers.
  let nativeRes = ctfs.ensureQualifiedId(w, "native", "main")
  doAssert nativeRes.isOk, "ensureQualifiedId(native) failed: " & nativeRes.error
  let nativeId = nativeRes.get()

  let gdscriptRes = ctfs.ensureQualifiedId(w, "gdscript", "main")
  doAssert gdscriptRes.isOk, "ensureQualifiedId(gdscript) failed: " & gdscriptRes.error
  let gdscriptId = gdscriptRes.get()

  doAssert nativeId != gdscriptId,
    "two qualifiers of the same bare name must be distinct ids: " &
    $nativeId & " vs " & $gdscriptId

  # A genuine repeat of (native, main) dedups to the FIRST id.
  let nativeAgainRes = ctfs.ensureQualifiedId(w, "native", "main")
  doAssert nativeAgainRes.isOk
  doAssert nativeAgainRes.get() == nativeId,
    "repeat of (native,main) must dedup to the first id: got " &
    $nativeAgainRes.get() & " expected " & $nativeId

  doAssert w.count() == 2,
    "only two distinct entries expected, got " & $w.count()

  echo "PASS: test_qualified_ids_are_distinct_by_construction"

# ---------------------------------------------------------------------------
# test_empty_qualifier_is_byte_identical
# ---------------------------------------------------------------------------

proc internedBytes(baseName: string,
    fill: proc(ctfs: var Ctfs, w: var InterningTableWriter) {.raises: [].}):
    seq[byte] {.raises: [].} =
  var ctfs = createCtfs()
  let writerRes = initInterningTableWriter(ctfs, baseName)
  doAssert writerRes.isOk, "initInterningTableWriter failed: " & writerRes.error
  var w = writerRes.get()
  fill(ctfs, w)
  ctfs.toBytes()

proc test_empty_qualifier_is_byte_identical() {.raises: [].} =
  # The composite payload for an empty qualifier is the BARE name, containing
  # no unit separator.
  let payload = qualifiedPayload("", "main")
  doAssert payload == "main",
    "empty-qualifier payload must be the bare name, got '" & payload & "'"
  doAssert payload.find(InterningUnitSeparator) < 0,
    "empty-qualifier payload must contain no 0x1f byte"

  # ensureId(name) and ensureQualifiedId("", name) must intern identically.
  let entries = ["main", "foo", "bar", "helper", "res://x.gd"]

  proc fillBare(ctfs: var Ctfs, w: var InterningTableWriter) {.raises: [].} =
    for e in entries:
      let r = ctfs.ensureId(w, e)
      doAssert r.isOk, "ensureId failed: " & r.error

  proc fillEmptyQualified(ctfs: var Ctfs, w: var InterningTableWriter) {.raises: [].} =
    for e in entries:
      let r = ctfs.ensureQualifiedId(w, "", e)
      doAssert r.isOk, "ensureQualifiedId failed: " & r.error

  let bareBytes = internedBytes("funcs", fillBare)
  let emptyQualBytes = internedBytes("funcs", fillEmptyQualified)

  # Back-compat proof: a table built only with empty-qualifier interns is
  # byte-identical to the table built with the pre-change bare `ensureId`.
  doAssert bareBytes == emptyQualBytes,
    "empty-qualifier container must be byte-identical to the bare-name container: " &
    $bareBytes.len & " vs " & $emptyQualBytes.len & " bytes"

  echo "PASS: test_empty_qualifier_is_byte_identical"

# ---------------------------------------------------------------------------
# test_split_interning_payload
# ---------------------------------------------------------------------------

proc test_split_interning_payload() {.raises: [].} =
  let q1 = splitInterningPayload("native\x1fmain")
  doAssert q1.qualifier == "native", "qualifier: '" & q1.qualifier & "'"
  doAssert q1.name == "main", "name: '" & q1.name & "'"

  let q2 = splitInterningPayload("main")
  doAssert q2.qualifier == "", "bare payload must yield empty qualifier"
  doAssert q2.name == "main", "bare payload name: '" & q2.name & "'"

  # A qualifier with a sub-origin, and a name that is itself empty.
  let q3 = splitInterningPayload("native:libfoo\x1f")
  doAssert q3.qualifier == "native:libfoo"
  doAssert q3.name == ""

  echo "PASS: test_split_interning_payload"

# ---------------------------------------------------------------------------
# test_roundtrip_through_variable_record_table
# ---------------------------------------------------------------------------

proc test_roundtrip_through_variable_record_table() {.raises: [].} =
  var ctfs = createCtfs()
  let writerRes = initInterningTableWriter(ctfs, "funcs")
  doAssert writerRes.isOk, "initInterningTableWriter failed: " & writerRes.error
  var w = writerRes.get()

  # A mix of qualified and unqualified keys, in intern order.
  let keys = @[
    ("native", "main"),
    ("gdscript", "main"),
    ("native:libfoo", "main"),
    ("", "standalone"),
    ("gdscript:autoload/Game", "_ready"),
  ]
  var ids: seq[uint64] = @[]
  for (qual, name) in keys:
    let r = ctfs.ensureQualifiedId(w, qual, name)
    doAssert r.isOk, "ensureQualifiedId failed for (" & qual & "," & name & "): " & r.error
    ids.add(r.get())

  # Serialize and read back through the real VariableRecordTable read-path.
  let rawBytes = ctfs.toBytes()
  let readerRes = initInterningTableReader(rawBytes, "funcs")
  doAssert readerRes.isOk, "initInterningTableReader failed: " & readerRes.error
  let reader = readerRes.get()

  doAssert reader.count() == uint64(keys.len),
    "reader count: " & $reader.count() & " expected " & $keys.len

  for i in 0 ..< keys.len:
    let readRes = reader.readById(ids[i])
    doAssert readRes.isOk, "readById failed for id " & $ids[i] & ": " & readRes.error
    let payload = readRes.get()

    # The stored payload must equal the composite payload we intended.
    let (expectedQual, expectedName) = keys[i]
    doAssert payload == qualifiedPayload(expectedQual, expectedName),
      "payload mismatch at id " & $ids[i] & ": got '" & payload & "'"

    # And splitting it must recover the original (qualifier, name).
    let (gotQual, gotName) = splitInterningPayload(payload)
    doAssert gotQual == expectedQual,
      "recovered qualifier mismatch at id " & $ids[i] & ": '" & gotQual & "'"
    doAssert gotName == expectedName,
      "recovered name mismatch at id " & $ids[i] & ": '" & gotName & "'"

  echo "PASS: test_roundtrip_through_variable_record_table"

# ---------------------------------------------------------------------------
# test_qualified_wrappers_on_trace_interning_tables
# ---------------------------------------------------------------------------

proc test_qualified_wrappers_on_trace_interning_tables() {.raises: [].} =
  var ctfs = createCtfs()
  let tablesRes = initTraceInterningTables(ctfs)
  doAssert tablesRes.isOk, "initTraceInterningTables failed: " & tablesRes.error
  var tables = tablesRes.get()

  let a = ctfs.ensureQualifiedFunctionId(tables, "native", "main")
  doAssert a.isOk
  let b = ctfs.ensureQualifiedFunctionId(tables, "gdscript", "main")
  doAssert b.isOk
  doAssert a.get() != b.get(),
    "qualified wrapper must keep the two mains distinct"

  # Bare wrapper stays available and shares the same table/id space.
  let c = ctfs.ensureFunctionId(tables, "main")
  doAssert c.isOk
  doAssert c.get() != a.get() and c.get() != b.get(),
    "bare 'main' is a third, empty-qualifier entry"

  doAssert tables.funcs.count() == 3

  echo "PASS: test_qualified_wrappers_on_trace_interning_tables"

# Run all tests
test_qualified_ids_are_distinct_by_construction()
test_empty_qualifier_is_byte_identical()
test_split_interning_payload()
test_roundtrip_through_variable_record_table()
test_qualified_wrappers_on_trace_interning_tables()
