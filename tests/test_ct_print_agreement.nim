## `ct-print --full` has one implementation, and the events it assembles obey
## the ordering rule the format states.
##
## Two properties, and the second is why the first is not enough.
##
## **Agreement.** The shipped binary and the in-process builder the test corpus
## links must produce the same document for the same container. They used to be
## two near-copies of one function: the binary gained an ordering rule for
## same-step `call_exit` events, the exclusion of `DeltaColumn` nudges from
## `events[]`, a column-aware position decoder and a diagnostic channel, while
## the copy the tests exercised gained none of them and separately grew a key
## the binary has never printed. Nothing compared them, so every one of those
## was a passing suite describing a program nobody runs. They are one module
## now; this is what fails if a second one reappears.
##
## **The rule.** Agreement between two things that are the same code is worth
## little on its own — it would hold just as well if both were wrong. So the
## order is also checked against what `trace-events.md` §"Assembling an event
## stream: storage order is not event order" requires: an assembler MUST NOT
## emit a call's `call_exit` before the `call_exit` of any call in its subtree.
##
## The fixture is built to make that question answerable. Every existing
## ct-print fixture closes its calls at *different* steps, where storage order
## and event order happen to coincide and any ordering passes. Here `main` and
## `compute` are both still open when the recording ends, so both are finalized
## with the same `last_step_id` and the order is the only thing that tells them
## apart — which is exactly the case the writer's close-time drain produces and
## the one that was rendering a caller as closing before its own callee.
##
## Falsifiability: sort `callsByExit` by ascending `call_key` (the copy's
## behaviour) and `test_a_call_closes_after_the_calls_inside_it` fails, naming
## the pair it got backwards.

import std/[os, osproc, strutils, json]
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/full_document_json
import ct_print_binary

const
  outDir = "/tmp/ct_print_agreement"
  bundlePath = outDir / "shared_exit.ct"

proc buildSharedExitBundle(path: string) =
  ## `main` calls `compute`, and neither returns before the recording ends.
  ## The close-time drain finalizes both at the last step, so the two records
  ## carry the same `last_step_id`.
  createDir(path.parentDir)
  removeFile(path)
  var w = initMultiStreamWriter(path, "demo").get()
  w.metadata.workdir = "/wd"
  let p0 = w.registerPath("/wd/main.py").get()
  let fnMain = w.registerFunction("main").get()
  let fnCompute = w.registerFunction("compute").get()

  doAssert w.registerStep(p0, 1'u64, @[]).isOk
  doAssert w.registerCall(fnMain, @[]).isOk
  doAssert w.registerStep(p0, 2'u64, @[]).isOk
  doAssert w.registerCall(fnCompute, @[]).isOk
  doAssert w.registerStep(p0, 3'u64, @[]).isOk
  # No returns: both frames are open at close.
  doAssert w.close().isOk
  w.closeCtfs()

proc binaryDocument(path: string): JsonNode =
  let (output, code) = execCmdEx(
    quoteShell(ctPrintBin) & " --full --strip-paths " & quoteShell(path))
  doAssert code == 0, "ct-print --full failed (" & $code & "):\n" & output
  parseJson(output)

proc inProcessDocument(path: string): JsonNode =
  var readerRes = openNewTrace(path)
  doAssert readerRes.isOk, "openNewTrace failed: " & readerRes.error
  var reader = readerRes.get()
  buildFullDocument(reader, FullOpts(stripPaths: true))

proc exitSequence(doc: JsonNode): seq[string] =
  for ev in doc["events"].elems:
    if ev["kind"].getStr() == "call_exit":
      result.add(ev["function"].getStr())

proc test_the_two_entry_points_agree() =
  ensureCtPrint()
  buildSharedExitBundle(bundlePath)

  let fromBinary = binaryDocument(bundlePath)
  let fromLibrary = inProcessDocument(bundlePath)

  doAssert fromBinary == fromLibrary,
    "the shipped binary and the in-process builder disagree on the same " &
    "container.\nbinary:\n" & fromBinary.pretty() &
    "\nlibrary:\n" & fromLibrary.pretty()

  # Non-degeneracy: two empty documents are equal too. This fixture has
  # calls in it, and the comparison is only evidence if it looked at them.
  doAssert exitSequence(fromBinary).len == 2,
    "the fixture must carry two call_exit events for the comparison to " &
    "mean anything; it carries " & $exitSequence(fromBinary)

  echo "PASS: the shipped binary and the in-process builder agree"

proc test_a_call_closes_after_the_calls_inside_it() =
  ensureCtPrint()
  buildSharedExitBundle(bundlePath)
  let doc = binaryDocument(bundlePath)

  # Both records report the same exit step — otherwise the step index alone
  # would decide the order and the ordering rule would not be under test.
  var exitSteps: seq[int]
  for ev in doc["events"].elems:
    if ev["kind"].getStr() == "call_exit":
      exitSteps.add(int(ev["exit_step"].getInt()))
  doAssert exitSteps.len == 2 and exitSteps[0] == exitSteps[1],
    "the fixture must put both exits on one step; got " & $exitSteps

  doAssert exitSequence(doc) == @["compute", "main"],
    "`compute` is called from inside `main`, so its range lies inside " &
    "main's and its exit is the earlier event. Got " & $exitSequence(doc) &
    ", which renders main as closing while its own callee is still open."

  # Derived from the records rather than from this fixture's names, so it
  # holds for every container: no call closes while a child is open.
  var closed: seq[int]
  var entryChildren: seq[(int, seq[int])]
  for ev in doc["events"].elems:
    if ev["kind"].getStr() == "call_entry":
      var kids: seq[int]
      for k in ev["children"].elems:
        kids.add(int(k.getInt()))
      entryChildren.add((int(ev["call_key"].getInt()), kids))
  for ev in doc["events"].elems:
    if ev["kind"].getStr() != "call_exit": continue
    let key = int(ev["call_key"].getInt())
    for (k, kids) in entryChildren:
      if k != key: continue
      for child in kids:
        doAssert child in closed,
          "call_key " & $key & " (" & ev["function"].getStr() &
          ") exits while its child call_key " & $child & " is still open"
    closed.add(key)

  echo "PASS: a call closes after the calls inside it"

test_the_two_entry_points_agree()
test_a_call_closes_after_the_calls_inside_it()

echo "ALL PASS: test_ct_print_agreement"
