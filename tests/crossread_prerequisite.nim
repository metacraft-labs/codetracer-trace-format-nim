## What a cross-read test does when it cannot reach the Rust reader.
##
## The four cross-read tests (`test_nim_{step,value,io_event}_stream_crossread`
## and `test_nim_step_map_crossread`) hand a Nim-written bundle to the sibling
## Rust repo's `cargo test`. Without that repo or a Rust toolchain they cannot
## make the proof they exist for.
##
## A runner that declares them — the `repro.nim` execute edges and the nimble
## `test` task — sets `CT_CROSSREAD_REQUIRED=1`. There a missing prerequisite
## FAILS the test: the runner promised the proof, so a pass without it would
## be a false green. Only an ad-hoc run outside both runners, which sets
## nothing, skips with a message.

import std/os

const CrossReadRequiredEnv* = "CT_CROSSREAD_REQUIRED"
  ## Set to `1` by every runner that declares the cross-read tests.

proc crossReadRequired*(): bool =
  getEnv(CrossReadRequiredEnv) == "1"

proc crossReadUnavailable*(reason: string) =
  ## Report that the cross-read cannot run. Under a declaring runner this
  ## exits the process with status 1; otherwise it prints a `SKIP:` line and
  ## returns, and the caller returns too.
  if crossReadRequired():
    stderr.writeLine "FAIL: " & reason & " (" & CrossReadRequiredEnv &
      "=1: the runner declared this cross-read, so it may not skip)"
    quit 1
  echo "SKIP: " & reason
