## The ONE way a test gets a compiled `ct-print` to run.
##
## Two test files needed the shipped binary rather than the library, and each
## grew its own copy of "compile it if the cache is stale". The copies then
## disagreed about which sources count towards staleness and about how to hand
## pkg-config's answer to the compiler — the second of which only fails where
## libzstd is not already on the default search path, so one copy worked
## everywhere the other was exercised and nothing said the other was broken.
##
## It lives here for the reason `meta_flags_json` and `full_document_json` do:
## one job with two implementations is two jobs.

import std/[os, strutils, times, tempfiles]
import runtime_nim_compile

const
  repoRoot* = currentSourcePath().parentDir.parentDir
  ctPrintSrc* = repoRoot / "src" / "codetracer_ct_print.nim"

let
  ctPrintBuildDir = createTempDir("trace-nim-ctprint-", "")
  ctPrintBin* = ctPrintBuildDir / "ct-print"
    ## Only this process owns this exclusive namespace; no foreign warm binary
    ## or shared compiler cache can satisfy the original runtime assertions.

proc newestSourceTime(): Time =
  ## The modification time of the most recently changed file under `src/`.
  ##
  ## ct-print is a thin front end over the reader library: nearly everything it
  ## can get wrong lives in a module OTHER than `codetracer_ct_print.nim`.
  ## Dating the cached binary against that one file answers "fresh" for a
  ## binary built before a reader change, and the test then measures a ct-print
  ## that no longer exists in the tree — reporting a pass or a failure that
  ## belongs to the previous build.
  result = getLastModificationTime(ctPrintSrc)
  for path in walkDirRec(repoRoot / "src"):
    if path.endsWith(".nim"):
      let t = getLastModificationTime(path)
      if t > result:
        result = t

proc ensureCtPrint*(): string {.discardable.} =
  ## Compile ct-print into `ctPrintBin` when it is missing or stale, and
  ## return its path.
  if fileExists(ctPrintBin) and
     getLastModificationTime(ctPrintBin) >= newestSourceTime():
    return ctPrintBin
  createDir(ctPrintBin.parentDir)
  let (output, code) = compileWithTestToolchain(ctPrintSrc, ctPrintBin,
    ctPrintBuildDir / "nimcache", flags = "-d:release --mm:arc")
  doAssert code == 0, "failed to build ct-print:\n" & output
  ctPrintBin
