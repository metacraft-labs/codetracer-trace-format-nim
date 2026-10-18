## Every test file in `tests/` is reachable from a runner.
##
## A test that is never invoked cannot fail, so it stops describing the code
## and nobody finds out. Two files in this directory sat unlisted for months:
##
## * `test_pending_value_after_delta_column.nim` demanded the two separate
##   wire events that a later change folded into one, and drove its whole
##   sequence against a path registered with no per-line table — the one shape
##   where a column may not be applied at all. Both would have failed on the
##   first run.
## * `test_reader_ffi.nim` declared a container it had not written, so the
##   reader refused every value lookup; the test turned the refusal into an
##   empty string and its assertions read that as "no values".
##
## Each was written by someone who understood the format, each was correct on
## the day it was written, and each was wrong within weeks. The common factor
## is not the code in them — it is that nothing ran them. See
## `conformance-testing.md` §"A gate that cannot pass": a gate everyone
## bypasses is worse than no gate, and an unlisted test is a gate nobody can
## even reach.
##
## **There is deliberately no exemption list.** An allowlist is how this class
## survives: the first entry is always justified, and it is the mechanism by
## which the second and third are never questioned. A file that genuinely
## cannot be run should be deleted, and its reason recorded in the commit that
## deletes it.
##
## Falsifiability: add a `tests/test_*.nim` that no runner mentions and this
## goes red naming it.
##
## **Both runners, not either.** Being named by *a* runner is not enough:
## `repro.nim` models the corpus as its own build graph, so a file the nimble
## `test` task runs but no `repro.nim` `TestSpec` models is a test that a
## reprobuild `test` run never executes. Forty such files accumulated while
## the first check stayed green, because a mention anywhere in `repro.nim`,
## a comment included, satisfied it (codetracer-pm
## `issues/2026-10-18-trace-format-nim-repro-test-edges-miss-40-nimble-tests.md`).
## The second check reads both lists structurally: every `tests/test_*.nim` on
## an executed line of the nimble `test` task must be the `source:` of a
## `TestSpec`. The reverse is not required, because `repro.nim` also models
## suites the nimble task does not run. The no-exemption rule holds here too.

import std/[os, strutils, algorithm, sets]

const
  repoRoot = currentSourcePath().parentDir.parentDir
  testsDir = repoRoot / "tests"
  # `nimble test` is the task CI runs; `repro.nim` is the reprobuild manifest,
  # which drives a second set. A file counts as reachable when either names it.
  runners = ["codetracer_trace_format.nimble", "repro.nim"]

proc runnerText(): string =
  ## Every runner's source, concatenated. A file counts as reachable when any
  ## of them names it — the two do not overlap and neither is authoritative
  ## on its own.
  for runner in runners:
    let path = repoRoot / runner
    doAssert fileExists(path),
      "runner " & runner & " does not exist at " & path &
      "; this check is looking in the wrong place and would pass vacuously"
    result.add(readFile(path))
    result.add("\n")

proc test_every_test_file_is_named_by_a_runner() =
  let text = runnerText()

  var testFiles: seq[string]
  for path in walkFiles(testsDir / "test_*.nim"):
    testFiles.add(path.extractFilename)
  testFiles.sort()

  # Non-degeneracy: an empty directory listing satisfies the loop below
  # without looking at anything, and a wrong `testsDir` produces exactly that.
  doAssert testFiles.len > 50,
    "expected the tests directory to hold well over 50 `test_*.nim` files; " &
    "found " & $testFiles.len & " in " & testsDir & ". The check is " &
    "pointed somewhere wrong and would otherwise pass without examining a " &
    "single file."

  var unlisted: seq[string]
  for name in testFiles:
    if name notin text:
      unlisted.add(name)

  # Echoed, not carried in the assertion message: see the note in
  # `test_every_nimble_test_is_a_repro_test_spec`.
  if unlisted.len > 0:
    echo "FAIL: these test files are named by no runner, so nothing ever ",
      "runs them:\n  ", unlisted.join("\n  "),
      "\nAdd each to the `test` task in codetracer_trace_format.nimble, and ",
      "a `TestSpec` for it to repro.nim, and expect it to fail the first ",
      "time — a test that has not run in months is describing code that has ",
      "moved on. Do not add an exemption here: an unrunnable test should be ",
      "deleted, with the reason in the commit that deletes it."
  doAssert unlisted.len == 0,
    $unlisted.len & " test file(s) named by no runner; the list is printed above"

  echo "PASS: all ", testFiles.len, " test files are named by a runner"

proc nimbleTestTaskFiles(nimble: string): HashSet[string] =
  ## The `tests/test_*.nim` files the nimble `test` task compiles: every such
  ## name on a non-comment line between `task test,` and the next `task`.
  ## Comment lines are skipped, because a comment that names a file does not
  ## run it.
  var inTask = false
  for raw in nimble.splitLines():
    if raw.startsWith("task "):
      inTask = raw.startsWith("task test,")
      continue
    let line = raw.strip()
    if not inTask or line.len == 0 or line.startsWith("#"):
      continue
    var at = line.find("tests/test_")
    while at >= 0:
      let stop = line.find(".nim", at)
      if stop < 0:
        break
      result.incl(line["tests/".len + at ..< stop + ".nim".len])
      at = line.find("tests/test_", stop)

proc reproModelledFiles(repro: string): HashSet[string] =
  ## The files `repro.nim` models: the `source: "tests/<file>"` of every
  ## `TestSpec`. A mention in a comment is not a `source:` field and does not
  ## count, which is the whole difference from the first check.
  const needle = "source: \"tests/"
  for raw in repro.splitLines():
    let line = raw.strip()
    if line.startsWith("#"):
      continue
    let at = line.find(needle)
    if at < 0:
      continue
    let start = at + needle.len
    let stop = line.find('"', start)
    if stop > start:
      result.incl(line[start ..< stop])

proc unmodelled(nimble, repro: string): seq[string] =
  ## The one predicate the rule and its control both call: the files the
  ## nimble `test` task runs that no `TestSpec` models.
  let modelled = reproModelledFiles(repro)
  for name in nimbleTestTaskFiles(nimble):
    if name notin modelled:
      result.add(name)
  result.sort()

proc test_every_nimble_test_is_a_repro_test_spec() =
  let nimble = readFile(repoRoot / "codetracer_trace_format.nimble")
  let repro = readFile(repoRoot / "repro.nim")

  # Non-degeneracy: an extractor that matches nothing leaves the task "empty"
  # and the check below passes without looking at anything.
  let ran = nimbleTestTaskFiles(nimble)
  let modelled = reproModelledFiles(repro)
  doAssert ran.len > 100,
    "expected the nimble `test` task to name well over 100 test files; " &
    "found " & $ran.len & ". The task parser is broken and would pass " &
    "vacuously."
  doAssert modelled.len > 100,
    "expected repro.nim to model well over 100 test files; found " &
    $modelled.len & ". The `source:` parser is broken."
  # A `TestSpec` whose source was deleted is an edge that cannot build, so a
  # reprobuild `test` run fails before it reaches anything it should test.
  var stale: seq[string]
  for name in modelled:
    if not fileExists(testsDir / name):
      stale.add(name)
  stale.sort()

  let missing = unmodelled(nimble, repro)
  # The lists are echoed rather than carried in the assertion message: under
  # -d:release (the nimble task's flag for this file) a defect message this
  # long reaches the terminal EMPTY, so the failure would name nothing.
  if missing.len > 0:
    echo "FAIL: these test files are run by the nimble `test` task but are ",
      "not the `source:` of any repro.nim `TestSpec`, so a reprobuild `test` ",
      "run never executes them:\n  ", missing.join("\n  "),
      "\nAdd a `TestSpec` for each, with the flags its nimble line passes. ",
      "Naming a file in a repro.nim comment does not model it."
  if stale.len > 0:
    echo "FAIL: these repro.nim `TestSpec` sources do not exist:\n  ",
      stale.join("\n  ")
  doAssert missing.len == 0 and stale.len == 0,
    $missing.len & " nimble test(s) unmodelled and " & $stale.len &
    " stale TestSpec(s) in repro.nim; the lists are printed above"

  echo "PASS: all ", ran.len, " files the nimble test task runs are ",
    "repro.nim test specs"

proc test_the_modelled_check_can_see_a_missing_spec() =
  ## The same predicate, run on a repro.nim with one real `TestSpec` removed,
  ## must report exactly that file, even though the file is still named in a
  ## comment where the `TestSpec` was.
  let nimble = readFile(repoRoot / "codetracer_trace_format.nimble")
  let repro = readFile(repoRoot / "repro.nim")
  const victim = "test_base40.nim"
  const specLine = "TestSpec(source: \"tests/" & victim & "\""
  doAssert victim in nimbleTestTaskFiles(nimble),
    "the control's victim " & victim & " is not in the nimble test task; " &
    "pick another"
  var mutated: seq[string]
  var removed = false
  for line in repro.splitLines():
    if specLine in line:
      removed = true
      mutated.add("  # no longer modelled: tests/" & victim)
    else:
      mutated.add(line)
  doAssert removed, "the control could not find the TestSpec for " & victim
  let reported = unmodelled(nimble, mutated.join("\n"))
  doAssert victim in reported and victim notin unmodelled(nimble, repro),
    "removing the TestSpec for " & victim & " was not reported (got " &
    $reported & "): the modelled check cannot fail"
  echo "PASS: the modelled check reports a TestSpec removed from repro.nim"

proc test_the_check_can_see_a_missing_file() =
  ## The needle has to be able to miss. This confirms the containment test
  ## reports absence for a name no runner carries, so the check above is
  ## capable of failing rather than merely capable of passing.
  let text = runnerText()
  const absent = "test_a_file_that_does_not_exist_anywhere.nim"
  doAssert absent notin text,
    "a name invented for this check appears in a runner, so the check " &
    "cannot distinguish listed from unlisted"
  echo "PASS: the check reports a name no runner carries"

test_the_check_can_see_a_missing_file()
test_every_test_file_is_named_by_a_runner()
test_the_modelled_check_can_see_a_missing_spec()
test_every_nimble_test_is_a_repro_test_spec()

echo "ALL PASS: test_every_test_is_listed"
