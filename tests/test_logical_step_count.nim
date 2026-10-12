## A trace's step count is the number of step records, and no other exec
## record (`trace-events.md` §"Multi-Stream Architecture" and §"Which exec
## records are steps"): `Raise`, `Catch` and the thread records occupy an exec
## record each but are not steps.
##
## Checked through every place a step count is reported: the reader's
## `logicalStepCount`, the C ABI's `ct_reader_logical_step_count`, and
## `ct-print`'s `--summary`, text, `--meta-json` and `--full` output.  The exec
## record count, which the other streams are indexed by, stays the total.
## The fixture holds three steps and five non-step records, so a reader that
## counts any of them reports a number other than 3.
##
## No mocks: the real writer, the real reader, the shipped ct-print binary.

include codetracer_trace_writer_ffi

{.pop.}

import std/[os, osproc, strutils, json]
import ct_print_binary

const
  outDir = "/tmp/ct_logical_step_count"
  bundlePath = outDir / "raise_catch.ct"
  stepRecords = 3'u64
  execRecords = 8'u64

proc buildBundle(path: string) =
  createDir(path.parentDir)
  removeFile(path)
  var w = initMultiStreamWriter(path, "raise_catch").get()
  let p0 = w.registerPath("/src/rc.php").get()
  let fnMain = w.registerFunction("main").get()
  doAssert w.registerStep(p0, 1'u64, @[]).isOk          # exec 0: step
  doAssert w.registerCall(fnMain, @[]).isOk
  doAssert w.registerThreadStart(2).isOk                 # exec 1
  doAssert w.registerThreadSwitch(2).isOk                # exec 2
  doAssert w.registerStep(p0, 2'u64, @[]).isOk           # exec 3: step
  doAssert w.registerRaise(0, cast[seq[byte]]("boom")).isOk  # exec 4
  doAssert w.registerCatch(0).isOk                       # exec 5
  doAssert w.registerThreadExit(2).isOk                  # exec 6
  doAssert w.registerStep(p0, 3'u64, @[]).isOk           # exec 7: step
  doAssert w.close().isOk
  w.closeCtfs()

proc ctPrint(args: string): string =
  let (output, code) = execCmdEx(quoteShell(ctPrintBin) & " " & args & " " &
    quoteShell(bundlePath))
  doAssert code == 0, "ct-print " & args & " failed (" & $code & "):\n" & output
  output

proc test_the_reader_counts_step_records_only() =
  buildBundle(bundlePath)
  var r = openNewTrace(bundlePath).get()
  doAssert r.stepCount().get() == execRecords,
    "the exec-record count is " & $r.stepCount().get()
  doAssert r.logicalStepCount().get() == stepRecords,
    "logicalStepCount is " & $r.logicalStepCount().get() & ", not " &
    $stepRecords & ": it counts raise, catch or thread records as steps"
  let h = ct_reader_open(cstring(bundlePath))
  doAssert h != nil, $trace_writer_last_error()
  doAssert ct_reader_step_count(h) == execRecords
  doAssert ct_reader_logical_step_count(h) == stepRecords,
    "ct_reader_logical_step_count is " & $ct_reader_logical_step_count(h)
  ct_reader_close(h)
  echo "PASS: the reader counts step records only"

proc test_ct_print_reports_three_steps_everywhere() =
  ensureCtPrint()
  buildBundle(bundlePath)
  let summary = ctPrint("--summary")
  doAssert ("steps: " & $stepRecords) in summary,
    "--summary does not report " & $stepRecords & " steps:\n" & summary
  let json = parseJson(ctPrint("--meta-json"))
  doAssert json["counts"]["steps"].getInt() == int(stepRecords),
    "--meta-json counts.steps is " & $json["counts"]["steps"]
  let full = parseJson(ctPrint("--full"))
  doAssert full["counts"]["steps"].getInt() == int(stepRecords),
    "--full counts.steps is " & $full["counts"]["steps"]
  var stepEntries = 0
  var kinds: seq[string]
  for ev in full["events"].elems:
    let k = ev["kind"].getStr()
    if k == "step": inc stepEntries
    elif k != "call_entry" and k != "call_exit": kinds.add(k)
  doAssert stepEntries == int(stepRecords),
    "--full has " & $stepEntries & " step entries; counts.steps says " &
    $stepRecords
  doAssert kinds == @["thread_start", "thread_switch", "raise", "catch",
    "thread_exit"], "--full renders the non-step records as " & $kinds
  let text = ctPrint("")
  var stepLines: seq[string]
  for line in text.splitLines():
    if line.startsWith("Step "): stepLines.add(line)
  doAssert stepLines.len == int(stepRecords),
    "the text view prints " & $stepLines.len & " Step lines:\n" & text
  doAssert "Raise" in text and "Catch" in text,
    "the text view does not show the raise and the catch:\n" & text
  echo "PASS: ct-print reports three steps everywhere"

test_the_reader_counts_step_records_only()
test_ct_print_reports_three_steps_everywhere()
removeDir(outDir)
