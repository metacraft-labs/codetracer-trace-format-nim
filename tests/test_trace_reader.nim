## Tests for the high-level TraceReader API: a recording written by the
## split-stream writer, read back as events and rendered as JSON, text and a
## summary.

import std/[os, json, strutils]
import results
import codetracer_trace_reader
import codetracer_trace_writer/multi_stream_writer

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

test_reader_basic()
test_reader_json_output()
test_reader_text_output()
test_reader_summary()
test_reader_error_handling()
echo "ALL PASS: test_trace_reader"
