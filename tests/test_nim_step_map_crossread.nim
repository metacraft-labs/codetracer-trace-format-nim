## The Nim and Rust readers answer every `step-map.ns` lookup alike, a lookup
## of line 0 included.
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
## recorded. It then hands the container and those answers to the sibling Rust
## repo's `codetracer_trace_reader/tests/nim_step_map_crossread.rs`, which
## reads the same bytes with the Rust `StepMapReader` and requires the same
## answer to every lookup.
##
## The Rust repo is `../codetracer-trace-format`, or the checkout
## `CODETRACER_TRACE_FORMAT_DIR` names. Without it, or without `cargo`, the
## cross-read is skipped with a message; the Nim half still runs.
##
## No mocks: the production writer and both production readers.

import std/[os, osproc, strutils, strtabs, streams]
import results
import codetracer_ctfs/container
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/step_map_builder

proc fail(msg: string) =
  echo "FAIL: ", msg
  quit(1)

proc run(cmd: string, args: seq[string], workdir: string,
    extraEnv: seq[(string, string)]): tuple[code: int, output: string] =
  try:
    var env = newStringTable(modeStyleInsensitive)
    for k, v in envPairs(): env[k] = v
    for (k, v) in extraEnv: env[k] = v
    let p = startProcess(cmd, workingDir = workdir, args = args, env = env,
      options = {poStdErrToStdOut, poUsePath})
    let outp = p.outputStream.readAll()
    let code = p.waitForExit()
    p.close()
    (code, outp)
  except OSError, IOError:
    (-1, "process error: " & getCurrentExceptionMsg())

proc main() =
  let tmp = getTempDir() / ("ct_step_map_crossread_" & $getCurrentProcessId())
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
  var answers = ""
  for (path, line, want) in expected:
    let got = map.lookup(path, line).get()
    if got != want:
      fail("Nim lookup (" & $path & ", " & $line & ") is " & $got &
        ", the steps registered there are " & $want)
    answers.add($path & " " & $line & " " &
      (if got.len == 0: "-" else: got.join(",")) & "\n")
  let answersPath = bundle & ".answers.txt"
  writeFile(answersPath, answers)
  echo "PASS: the Nim reader answers line 0 as line 1"

  # --- the Rust answers ----------------------------------------------------
  let rustRepo = absolutePath(getEnv("CODETRACER_TRACE_FORMAT_DIR",
    "../codetracer-trace-format"))
  if not dirExists(rustRepo):
    echo "SKIP: no Rust repo at " & rustRepo & " to cross-read with"
  elif findExe("cargo").len == 0 and findExe("direnv").len == 0:
    echo "SKIP: neither cargo nor direnv on PATH"
  else:
    let cargoArgs = @["test", "-p", "codetracer_trace_reader",
      "--test", "nim_step_map_crossread", "--", "--nocapture"]
    let env = @[("CT_NIM_STEP_MAP_FIXTURE", bundle),
      ("CT_NIM_STEP_MAP_ANSWERS", answersPath)]
    let res =
      if findExe("cargo").len > 0:
        run("cargo", cargoArgs, rustRepo, env)
      else:
        run("direnv", @["exec", rustRepo, "cargo"] & cargoArgs, rustRepo, env)
    if res.code != 0:
      fail("the Rust StepMapReader answers differently:\n" & res.output)
    echo "PASS: the Rust reader answers every lookup as the Nim reader does"
  removeDir(tmp)

main()
