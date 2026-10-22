## Tests for the high-level TraceReader API: a recording written by the
## split-stream writer, read back as events and rendered as JSON, text and a
## summary.

import std/[os, json, strutils]
import results
import codetracer_trace_reader
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/cbor

proc getTmpPath(name: string): string =
  getTempDir() / name

proc cleanupFile(path: string) =
  try:
    removeFile(path)
  except OSError:
    discard

proc writeTrace(path, program: string, args: seq[string], workdir: string,
    source: string, lines: openArray[uint64]) =
  cleanupFile(path)
  var w = initMultiStreamWriter(path, program).get()
  w.metadata.args = args
  w.metadata.workdir = workdir
  let p = w.registerPath(source).get()
  for line in lines:
    doAssert w.registerStep(p, line, []).isOk
  doAssert w.close().isOk
  doAssert w.closeCtfs().isOk

proc test_reader_basic() =
  let path = getTmpPath("test_reader_basic.ct")
  writeTrace(path, "test_prog", @["arg1", "arg2"], "/tmp/work",
    "/src/main.nim", [10'u64, 11])
  var reader = openTrace(path).get()
  doAssert reader.metadata.program == "test_prog",
    "program mismatch: " & reader.metadata.program
  doAssert reader.metadata.args == @["arg1", "arg2"]
  doAssert reader.metadata.workdir == "/tmp/work"
  doAssert reader.paths == @["/src/main.nim"]
  doAssert reader.readEvents().isOk
  var lines: seq[int64]
  for e in reader.events:
    if e.kind == tleStep:
      doAssert e.step.pathId == PathId(0)
      lines.add(int64(e.step.line))
  doAssert lines == @[10'i64, 11], "steps: " & $lines
  cleanupFile(path)
  echo "PASS: test_reader_basic"

proc test_reader_json_output() =
  let path = getTmpPath("test_reader_json.ct")
  writeTrace(path, "json_test", @["--flag"], "/home", "/test.py", [1'u64])
  var reader = openTrace(path).get()
  doAssert reader.readEvents().isOk
  try:
    let node = parseJson(reader.toJson())
    doAssert node["metadata"]["program"].getStr() == "json_test"
    doAssert node["metadata"]["args"][0].getStr() == "--flag"
    doAssert node["metadata"]["workdir"].getStr() == "/home"
    doAssert node["paths"][0].getStr() == "/test.py"
    var kinds: seq[string]
    for e in node["events"]:
      kinds.add(e["type"].getStr())
    doAssert "Path" in kinds and "Step" in kinds, $kinds
    let arr = parseJson(reader.toJsonEvents())
    doAssert arr.kind == JArray
    doAssert arr.len == node["events"].len
  except JsonParsingError, KeyError:
    doAssert false, "toJson output is not the expected JSON: " &
      getCurrentExceptionMsg()
  cleanupFile(path)
  echo "PASS: test_reader_json_output"

proc test_reader_text_output() =
  let path = getTmpPath("test_reader_text.ct")
  writeTrace(path, "text_test", @[], "/workspace", "/src/app.nim", [42'u64])
  var reader = openTrace(path).get()
  doAssert reader.readEvents().isOk
  let text = reader.toPrettyText()
  doAssert "=== Trace ===" in text
  doAssert "program: text_test" in text
  doAssert "workdir: /workspace" in text
  doAssert "Step" in text
  doAssert "line=42" in text
  cleanupFile(path)
  echo "PASS: test_reader_text_output"

proc test_reader_summary() =
  let path = getTmpPath("test_reader_summary.ct")
  writeTrace(path, "summary_test", @["a", "b"], "", "/p.nim", [1'u64, 2, 3])
  var reader = openTrace(path).get()
  doAssert reader.readEvents().isOk
  let summary = reader.toSummary()
  doAssert "program: summary_test" in summary
  doAssert "steps: 3" in summary, summary
  doAssert "paths: 1" in summary, summary
  cleanupFile(path)
  echo "PASS: test_reader_summary"

proc test_reader_error_handling() =
  doAssert openTrace("/nonexistent/path/file.ct").isErr,
    "should fail on non-existent file"
  let path = getTmpPath("test_reader_bad.ct")
  try:
    writeFile(path, "not a ctfs file")
  except IOError, OSError:
    discard
  doAssert openTrace(path).isErr, "should fail on non-CTFS file"
  cleanupFile(path)
  echo "PASS: test_reader_error_handling"

proc encodeValue(v: ValueRecord): seq[byte] =
  var enc = CborEncoder.init()
  enc.encodeCborValueRecord(v)
  enc.getBytes()

proc test_reader_json_prints_upper_half_values() =
  ## `toJson` / `toJsonEvents` render a Reference's `address`, every value
  ## `type_id` and a thread record's `thread_id` as unsigned 64-bit numbers.
  ## Each is chosen by the recorder (a pointer in the upper half of the
  ## address space, a tag in bit 63), and each used to be `newJInt(int64(x))`,
  ## which raised RangeDefect at 2^63 — under `{.push raises: [].}` that is
  ## an abort, not an error the caller can handle. Below 2^63 a value stays
  ## an ordinary JSON integer.
  ##
  ## The serialized text is asserted, not a parsed node: `std/json` parses an
  ## integer too big for `BiggestInt` into a `JString`, so a parsed check
  ## could not tell `18446744073709551615` from `"18446744073709551615"`.
  const maxText = "18446744073709551615"
  let path = getTmpPath("test_reader_json_upper_half.ct")
  cleanupFile(path)
  var w = initMultiStreamWriter(path, "upper_half").get()
  let p = w.registerPath("/src/main.c").get()
  let vnPtr = w.registerVarname("ptr").get()
  let vnLow = w.registerVarname("low").get()
  let deref = @[ValueRecord(kind: vrkInt, intVal: 7, intTypeId: TypeId(1))]
  doAssert w.registerStep(p, 1'u64, [
    VariableValue(varnameId: vnPtr, data: encodeValue(ValueRecord(
      kind: vrkReference, dereferenced: deref, address: uint64.high,
      mutable: true, refTypeId: TypeId(uint64.high)))),
    VariableValue(varnameId: vnLow, data: encodeValue(ValueRecord(
      kind: vrkReference, dereferenced: deref, address: 4096'u64,
      mutable: false, refTypeId: TypeId(3))))
  ]).isOk
  doAssert w.registerThreadStart(uint64.high).isOk
  doAssert w.registerThreadSwitch(uint64.high).isOk
  doAssert w.registerThreadExit(uint64.high).isOk
  doAssert w.registerStep(p, 2'u64, []).isOk
  doAssert w.close().isOk
  doAssert w.closeCtfs().isOk

  var reader = openTrace(path).get()
  doAssert reader.readEvents().isOk
  for (name, rendered) in [("toJson", reader.toJson()),
                           ("toJsonEvents", reader.toJsonEvents())]:
    # Pretty-printing writes `"key": value`; fold it to one spelling.
    let text = rendered.replace("\": ", "\":")
    for key in ["address", "type_id"]:
      doAssert ("\"" & key & "\":" & maxText) in text,
        name & " did not print " & key & " as the unsigned number " &
        maxText & ":\n" & rendered
    doAssert text.count("\"thread_id\":" & maxText) == 3,
      name & " did not print the ThreadStart, ThreadSwitch and ThreadExit " &
      "ids as " & maxText & ":\n" & rendered
    doAssert ("\"" & maxText & "\"") notin text,
      name & " printed an unsigned value as a string:\n" & rendered
    # Below 2^63 the address is still an ordinary JSON integer.
    try:
      let node = parseJson(rendered)
      let events = if node.kind == JArray: node.getElems()
                   else: node["events"].getElems()
      var lowAddresses: seq[JsonNode]
      for ev in events:
        if ev["type"].getStr() == "Value" and
            ev["value"]["kind"].getStr() == "Reference" and
            ev["value"]["address"].kind == JInt:
          lowAddresses.add(ev["value"]["address"])
      doAssert lowAddresses.len == 1 and
        lowAddresses[0].getBiggestInt() == 4096,
        name & ": the address 4096 must be a JInt, got " & $lowAddresses
    except JsonParsingError, KeyError, ValueError, IOError, OSError:
      doAssert false, name & " output is not JSON: " & getCurrentExceptionMsg()
  cleanupFile(path)
  echo "PASS: test_reader_json_prints_upper_half_values"

test_reader_basic()
test_reader_json_output()
test_reader_text_output()
test_reader_summary()
test_reader_error_handling()
test_reader_json_prints_upper_half_values()
echo "ALL PASS: test_trace_reader"
