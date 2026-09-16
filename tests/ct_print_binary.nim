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

import std/[os, osproc, strutils, times]

const
  repoRoot* = currentSourcePath().parentDir.parentDir
  ctPrintSrc* = repoRoot / "src" / "codetracer_ct_print.nim"
  ctPrintBin* = "/tmp/ctprint_build/ct-print"
    ## A fixed path outside the repository, so the binary survives between
    ## runs and between branches. The staleness check below is what keeps that
    ## cache honest.

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
  var zstdFlags = ""
  when not defined(windows):
    let (cflags, c1) = execCmdEx("pkg-config --cflags libzstd")
    let (lflags, c2) = execCmdEx("pkg-config --libs libzstd")
    doAssert c1 == 0 and c2 == 0, "pkg-config libzstd failed"
    # One `--passC:` / `--passL:` per flag. pkg-config answers with a LIST
    # whenever libzstd is not already on the compiler's default search path,
    # and folding a list into a single quoted argument hands the C compiler
    # one token it cannot parse. Where libzstd is on the default path the
    # answer is a single flag and both spellings work, which is why the folded
    # form survived: it was only ever exercised where it could not fail.
    for f in cflags.strip().splitWhitespace():
      zstdFlags.add("--passC:" & quoteShell(f) & " ")
    for f in lflags.strip().splitWhitespace():
      zstdFlags.add("--passL:" & quoteShell(f) & " ")
  let cmd = "nim c -d:release --mm:arc -p:src " & zstdFlags &
    "--hints:off --warnings:off " &
    "-o:" & quoteShell(ctPrintBin) & " " & quoteShell(ctPrintSrc)
  let (output, code) = execCmdEx(cmd)
  doAssert code == 0, "failed to build ct-print:\n" & output
  ctPrintBin
