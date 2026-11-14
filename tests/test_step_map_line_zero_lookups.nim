## The Nim reader answers every `step-map.ns` lookup, a lookup of line 0
## included, with the steps registered there.
##
## `internal-files.md` §"`step-map.ns`", "Reading": a lookup of line 0 is a
## lookup of line 1, because a step registered at line 0 is keyed under line 1
## (§"Global Line Index", "Line 0 is line 1, everywhere"). Answering it with
## nothing, as a key no writer stores would be answered, misses the steps the
## caller means.
##
## The test writes a container with the Nim writer — steps registered at line
## 0 and line 1 of one file, at line 0 only in a second, and away from lines
## 0 and 1 in a third — and checks the Nim reader's answers against what was
## recorded. The same container and answers are held against the Rust reader
## by codetracer-trace-format-conformance's `test_nim_step_map_crossread`.
##
## No mocks: the production writer and reader.

import std/os
import results
import codetracer_ctfs/container
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/step_map_builder

proc fail(msg: string) =
  echo "FAIL: ", msg
  quit(1)

proc main() =
  let tmp = getTempDir() / ("ct_step_map_line_zero_" & $getCurrentProcessId())
  createDir(tmp)
  let bundle = tmp / "trace.ct"

  # --- write ---------------------------------------------------------------
  var w = initMultiStreamWriter(bundle, "step_map_crossread",
    recordingId = "01949fcc-7d92-7e9c-aaaa-dddddddddddd").get()
  let a = w.registerPath("/src/a.nr").get()
  let b = w.registerPath("/src/b.nr").get()
  let c = w.registerPath("/src/c.nr").get()
  # Step ids are exec-record indices, in the order registered.
  for (path, line) in [(a, 0'u64), (a, 1'u64), (a, 0'u64), (b, 0'u64),
                       (c, 4'u64), (a, 2'u64), (b, 0'u64), (a, 1'u64)]:
    if w.registerStep(path, line, []).isErr:
      fail("registerStep(" & $path & ", " & $line & ")")
  if w.close().isErr: fail("close")
  writeFile(bundle, w.toBytes())
  w.closeCtfs()

  # --- the Nim answers -----------------------------------------------------
  let data = readCtfsFromFile(bundle).get()
  var map = openStepMap(readInternalFile(data, StepMapFileName).get()).get()
  let expected = [
    (a, 0'u64, @[0'i64, 1, 2, 7]),   # line 0 is line 1: the steps registered at
    (a, 1'u64, @[0'i64, 1, 2, 7]),   # either, in step order
    (a, 2'u64, @[5'i64]),
    (b, 0'u64, @[3'i64, 6]),         # registered at line 0 only
    (b, 1'u64, @[3'i64, 6]),
    (c, 0'u64, newSeq[int64]()),     # line 1 of c never ran
    (c, 4'u64, @[4'i64]),
    (c, 5'u64, newSeq[int64]()),
  ]
  for (path, line, want) in expected:
    let got = map.lookup(path, line).get()
    if got != want:
      fail("Nim lookup (" & $path & ", " & $line & ") is " & $got &
        ", the steps registered there are " & $want)
  echo "PASS: the Nim reader answers line 0 as line 1"

  removeDir(tmp)

main()
