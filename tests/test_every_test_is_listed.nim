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

import std/[os, strutils, algorithm]

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

  doAssert unlisted.len == 0,
    "these test files are named by no runner, so nothing ever runs them:\n  " &
    unlisted.join("\n  ") &
    "\n\nAdd each to the `test` task in codetracer_trace_format.nimble (or to " &
    "repro.nim), and expect it to fail the first time — a test that has not " &
    "run in months is describing code that has moved on. Do not add an " &
    "exemption here: an unrunnable test should be deleted, with the reason " &
    "in the commit that deletes it."

  echo "PASS: all ", testFiles.len, " test files are named by a runner"

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

echo "ALL PASS: test_every_test_is_listed"
