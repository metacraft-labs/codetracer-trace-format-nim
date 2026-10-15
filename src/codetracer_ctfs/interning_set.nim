when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## The four interning tables are one unit: a trace carries `paths`, `funcs`,
## `types` and `varnames` — each as a `.dat` and a `.off` member — all
## together, or none of them (`internal-files.md` §"Interning Tables").
##
## A container holding a strict subset is not a trace with some empty
## tables: the readers that find `paths.dat` require the other seven members,
## and a reader that took an absent table as empty would answer for a trace
## the others refuse. So the writer refuses to finish such a container and
## every reader refuses to open one, all with the message built here. The
## Rust implementation (`codetracer_ctfs::interning_set`) states it in the
## same words; the cross-reader test in `codetracer-trace-format` compares
## them byte for byte.
##
## A table that is empty is still present: its `.off` member holds the single
## offset 0, and the member counts as carried.

import std/strutils

const InterningTableMembers* = [
  "paths.dat", "paths.off", "funcs.dat", "funcs.off",
  "types.dat", "types.off", "varnames.dat", "varnames.off"]
  ## Canonical order: the order the members are named in a refusal.

proc interningSetRefusal*(present: openArray[bool]): string =
  ## `present[i]` says whether `InterningTableMembers[i]` is in the
  ## container. "" when all eight or none are; otherwise the refusal, naming
  ## the members carried and the members missing.
  doAssert present.len == InterningTableMembers.len
  var have, lack: seq[string]
  for i, name in InterningTableMembers:
    if present[i]: have.add name else: lack.add name
  if have.len == 0 or lack.len == 0:
    return ""
  "the container carries " & have.join(", ") & " but not " & lack.join(", ") &
    ": the interning tables paths, funcs, types and varnames are carried " &
    "all four or none (internal-files.md \"Interning Tables\")"
