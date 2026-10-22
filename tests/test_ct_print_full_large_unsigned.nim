## `ct-print --full` renders every representable unsigned 64-bit value.
##
## A `ValueRecord::Reference` carries its `address` as an unsigned 64-bit
## field, and the value records' `type_id`, a raise's or catch's exception
## type id and a thread record's thread id are unsigned 64-bit too. The full
## document used to render each of them as `newJInt(int64(x))`, so any value
## of 2^63 or more raised `RangeDefect` and `ct-print --full` exited without
## printing anything — one value in the upper half of the address space lost
## the whole dump (codetracer-pm
## `issues/2026-10-20-ct-print-full-aborts-on-reference-address-at-or-above-2-63.md`).
##
## The contract checked here:
##
## * a value below 2^63 is still an ordinary JSON integer (`JInt`), so every
##   existing consumer and golden is unchanged;
## * a value of 2^63 or more is printed as the exact unsigned decimal number,
##   unquoted — not a string, not a wrapped negative number.
##
## The serialized TEXT is what is asserted, not a parsed node: `std/json`
## parses an integer that does not fit `BiggestInt` into a `JString` marked
## unquoted, so a check on the parsed node alone could not tell a correct
## `18446744073709551615` from a quoted `"18446744073709551615"`.
##
## Both entry points are exercised: the in-process builder the test corpus
## links (`valueRecordToJson`, `decodeValueBytesToJson`, `buildFullDocument`)
## and the shipped `ct-print` binary on a real container, because the defect
## was observed as the binary aborting.
##
## No mocks: the container is written by the production writer and read by
## the production reader.
##
## Falsifiability: restore `newJInt(int64(v.address))` in
## `full_document_json.nim` and the first upper-half case aborts with
## `RangeDefect`; the binary case then fails with ct-print's non-zero exit.
## Restore `newJInt(int64(v.typeId))` in one of `codetracer_ct_print.nim`'s
## own JSON modes (`--json`, `--json-events`, `--follow`), or `newJInt(int64(
## e.markerIdOf()))` in `--correlation-index`, and the last test fails naming
## that mode and its exit code.

import std/[os, osproc, strutils, json, tempfiles, options]
import results
import codetracer_trace_types
import codetracer_trace_writer/cbor
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/full_document_json
import ct_print_binary

const
  lowAddress = 4096'u64
  int64HighAddress = uint64(int64.high)       ## 2^63 - 1: still a JInt
  bit63Address = 0x8000_0000_0000_0000'u64    ## 2^63: the first that aborted
  maxAddress = uint64.high                    ## 2^64 - 1
  maxText = "18446744073709551615"
  bit63Text = "9223372036854775808"

proc reference(address: uint64, typeId = TypeId(3)): ValueRecord =
  ValueRecord(kind: vrkReference,
    dereferenced: @[ValueRecord(kind: vrkInt, intVal: 7,
      intTypeId: TypeId(1))],
    address: address, mutable: true, refTypeId: typeId)

proc encodeValue(v: ValueRecord): seq[byte] =
  var enc = CborEncoder.init()
  enc.encodeCborValueRecord(v)
  enc.getBytes()

proc expectUnquoted(node: JsonNode, key, digits, context: string) =
  ## `node[key]` serializes as the bare decimal `digits`.
  let text = $node
  doAssert ("\"" & key & "\":" & digits) in text,
    context & ": expected " & key & " to print as the unquoted number " &
    digits & ", got " & text
  doAssert ("\"" & key & "\":\"" & digits & "\"") notin text,
    context & ": " & key & " printed as a string: " & text

proc expectJInt(node: JsonNode, key: string, value: int64, context: string) =
  ## `node[key]` is an ordinary JSON integer holding `value`.
  doAssert node[key].kind == JInt,
    context & ": " & key & " must stay a JInt below 2^63, got " &
    $node[key].kind & " in " & $node
  doAssert node[key].getBiggestInt() == value,
    context & ": " & key & " = " & $node[key] & ", expected " & $value

proc test_an_address_below_2_63_is_an_ordinary_integer() =
  expectJInt(valueRecordToJson(reference(lowAddress)), "address",
    int64(lowAddress), "address 4096")
  expectJInt(valueRecordToJson(reference(int64HighAddress)), "address",
    int64.high, "address 2^63-1")
  expectJInt(valueRecordToJson(reference(lowAddress)), "type_id", 3,
    "type_id 3")
  echo "PASS: an address below 2^63 is an ordinary JSON integer"

proc test_an_address_at_or_above_2_63_renders_unsigned() =
  expectUnquoted(valueRecordToJson(reference(bit63Address)), "address",
    bit63Text, "address 2^63")
  expectUnquoted(valueRecordToJson(reference(maxAddress)), "address",
    maxText, "address 2^64-1")
  # Through the CBOR decode `buildFullDocument` uses for every value.
  expectUnquoted(decodeValueBytesToJson(encodeValue(reference(maxAddress))),
    "address", maxText, "decoded address 2^64-1")
  echo "PASS: an address at or above 2^63 renders as an unsigned number"

proc test_a_type_id_at_or_above_2_63_renders_unsigned() =
  let big = TypeId(uint64.high)
  expectUnquoted(valueRecordToJson(reference(lowAddress, big)), "type_id",
    maxText, "Reference type_id 2^64-1")
  let intVal = ValueRecord(kind: vrkInt, intVal: -1, intTypeId: big)
  expectUnquoted(decodeValueBytesToJson(encodeValue(intVal)), "type_id",
    maxText, "decoded Int type_id 2^64-1")
  echo "PASS: a type_id at or above 2^63 renders as an unsigned number"

proc buildBundle(path: string) =
  ## One step whose variables carry upper-half values, then a raise, a catch
  ## and a thread switch whose ids are 2^64-1.
  var w = initMultiStreamWriter(path, "demo").get()
  w.metadata.workdir = "/wd"
  let p0 = w.registerPath("/wd/main.c").get()
  let fnMain = w.registerFunction("main").get()
  let vnPtr = w.registerVarname("ptr").get()
  let vnLow = w.registerVarname("low").get()
  let vnTagged = w.registerVarname("tagged").get()
  doAssert w.registerStep(p0, 1'u64, @[]).isOk
  doAssert w.registerCall(fnMain, @[]).isOk
  doAssert w.registerStep(p0, 2'u64, @[
    VariableValue(varnameId: vnPtr, data: encodeValue(reference(maxAddress))),
    VariableValue(varnameId: vnLow, data: encodeValue(reference(lowAddress))),
    VariableValue(varnameId: vnTagged, data: encodeValue(
      ValueRecord(kind: vrkInt, intVal: 1, intTypeId: TypeId(uint64.high))))
  ]).isOk
  doAssert w.registerRaise(uint64.high, cast[seq[byte]]("boom")).isOk
  doAssert w.registerCatch(uint64.high).isOk
  doAssert w.registerThreadSwitch(uint64.high).isOk
  # Correlation index entries: a boundary marker whose id has bit 63 set and
  # a span-coverage entry whose thread id, timestamps and step coordinate
  # (`geid`, taken from `stepId` as given -- the C ABI passes it explicitly
  # for a buffered step) are 2^64-1.
  doAssert w.registerCorrelationMarkerById("send", uint64.high, "edge",
    "k").isOk
  var traceId = newSeq[byte](16)
  var spanId = newSeq[byte](8)
  for i in 0 ..< traceId.len: traceId[i] = byte(i + 1)
  for i in 0 ..< spanId.len: spanId[i] = byte(0xA0 + i)
  doAssert w.registerSpanCoverage(traceId, spanId, uint64.high, uint64.high,
    threadId = uint64.high, stepId = some(uint64.high)).isOk
  doAssert w.registerStep(p0, 3'u64, @[]).isOk
  doAssert w.close().isOk
  w.closeCtfs()

proc varsByName(doc: JsonNode): seq[(string, JsonNode)] =
  for ev in doc["events"].elems:
    if ev["kind"].getStr() == "step" and ev.hasKey("vars"):
      for v in ev["vars"].elems:
        result.add((v["varname"].getStr(), v))

proc recordsOfKind(doc: JsonNode, kind: string): seq[JsonNode] =
  for ev in doc["events"].elems:
    if ev["kind"].getStr() == kind:
      result.add(ev)

proc checkDocument(doc: JsonNode, context: string) =
  var seen: seq[string]
  for (name, v) in varsByName(doc):
    case name
    of "ptr":
      expectUnquoted(v["value"], "address", maxText, context & " ptr")
    of "low":
      expectJInt(v["value"], "address", int64(lowAddress), context & " low")
    of "tagged":
      expectUnquoted(v["value"], "type_id", maxText, context & " tagged")
      # The variable's own `type_id`, read off the same CBOR.
      expectUnquoted(v, "type_id", maxText, context & " tagged var")
    else: discard
    seen.add(name)
  doAssert "ptr" in seen and "low" in seen and "tagged" in seen,
    context & ": the step's variables are missing from the document: " & $seen
  for (kind, key) in [("raise", "exception_type_id"),
                      ("catch", "exception_type_id"),
                      ("thread_switch", "thread_id")]:
    let recs = recordsOfKind(doc, kind)
    doAssert recs.len == 1,
      context & ": expected one " & kind & " record, got " & $recs.len
    expectUnquoted(recs[0], key, maxText, context & " " & kind)

proc test_ct_print_full_prints_a_container_with_upper_half_values() =
  let dir = createTempDir("ct_print_large_unsigned_", "")
  defer: removeDir(dir)
  let bundle = dir / "large.ct"
  buildBundle(bundle)

  var reader = openNewTrace(bundle).get()
  checkDocument(buildFullDocument(reader, FullOpts(stripPaths: true)),
    "in-process")

  ensureCtPrint()
  let (output, code) = execCmdEx(
    quoteShell(ctPrintBin) & " --full --strip-paths " & quoteShell(bundle))
  doAssert code == 0,
    "ct-print --full exited " & $code & " on a container holding an " &
    "address of 2^64-1:\n" & output
  # The binary pretty-prints; check its exact digits in the raw text too, so
  # the assertion does not depend on how `parseJson` stores a big integer.
  doAssert (": " & maxText) in output,
    "ct-print --full did not print " & maxText & " as a number:\n" & output
  checkDocument(parseJson(output), "ct-print binary")
  echo "PASS: ct-print --full prints a container with upper-half values"

proc runCtPrint(args, bundle: string): string =
  ## Run the shipped binary with `args`; it must exit 0. Returns stdout with
  ## pretty-printing's `": "` folded to `":"`, so one spelling of a key/value
  ## pair matches compact and pretty output alike.
  let (output, code) = execCmdEx(
    quoteShell(ctPrintBin) & " " & args & " " & quoteShell(bundle))
  doAssert code == 0,
    "ct-print " & args & " exited " & $code & " on a container holding " &
    "2^64-1 values:\n" & output
  output.replace("\": ", "\":")

proc expectInOutput(output, key, args: string) =
  doAssert ("\"" & key & "\":" & maxText) in output,
    "ct-print " & args & " did not print " & key & " as the unsigned " &
    "number " & maxText & ":\n" & output

proc test_every_ct_print_mode_prints_upper_half_values() =
  ## The other JSON modes render a variable's `type_id` with code of their
  ## own rather than through the full-document builder, and
  ## `--correlation-index` renders the index's ids and timestamps; each
  ## aborted the same way before it used `unsignedJson`.
  let dir = createTempDir("ct_print_large_unsigned_modes_", "")
  defer: removeDir(dir)
  let bundle = dir / "large.ct"
  buildBundle(bundle)
  ensureCtPrint()

  # `--follow` stops after 50 idle polls; a 1 ms interval ends it promptly
  # on a container nobody is writing.
  for args in ["--json", "--json-events", "--events",
               "--follow --poll-interval=1"]:
    expectInOutput(runCtPrint(args, bundle), "type_id", args)
    echo "PASS: ct-print " & args & " prints a type_id of 2^64-1"

  let args = "--correlation-index --json-out"
  let index = runCtPrint(args, bundle)
  for key in ["marker_id", "thread_id", "wall_time_unix_ns",
              "monotonic_time_ns", "geid"]:
    expectInOutput(index, key, args)
  echo "PASS: ct-print " & args & " prints ids and times of 2^64-1"

test_an_address_below_2_63_is_an_ordinary_integer()
test_an_address_at_or_above_2_63_renders_unsigned()
test_a_type_id_at_or_above_2_63_renders_unsigned()
test_ct_print_full_prints_a_container_with_upper_half_values()
test_every_ct_print_mode_prints_upper_half_values()

echo "ALL PASS: test_ct_print_full_large_unsigned"
