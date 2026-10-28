when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

## `ctfs-salvage <container>`: after every writer of a container died, copy
## into the file the root entries its abandoned live coordination page holds
## ahead of the file, where every byte they cover is in the file, then remove
## the page (`ctfs-container.md` §6, "Every writer dies"). Exit 0 with a line
## per member advanced; 1 when the container has a live writer or cannot be
## read; 2 on a usage error.

import std/os
import results
import codetracer_ctfs/salvage

when isMainModule:
  if paramCount() != 1:
    stderr.writeLine "usage: ctfs-salvage <container>"
    quit(2)
  let r = salvageContainer(paramStr(1))
  if r.isErr:
    stderr.writeLine r.error
    quit(1)
  let rep = r.get()
  if not rep.pageFound:
    echo "no live coordination page: nothing to salvage"
  for m in rep.copied: echo "advanced ", m
  for m in rep.skipped: echo "kept the file's entry of ", m, ": the page's Size is not covered by the file"
