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

import std/[os, osproc, strutils, times, compilesettings, tempfiles]

const
  repoRoot* = currentSourcePath().parentDir.parentDir
  ctPrintSrc* = repoRoot / "src" / "codetracer_ct_print.nim"
  RuntimeNimCompiler = getCurrentCompilerExe()
  RuntimeNimLib = querySetting(SingleValueSetting.libPath)
  RuntimeSearchPaths = querySettingSeq(MultipleValueSetting.searchPaths)
  RuntimeLazyPaths = querySettingSeq(MultipleValueSetting.lazyPaths)
  RuntimeNimblePaths = querySettingSeq(MultipleValueSetting.nimblePaths)

when defined(windows):
  const
    ctZstdIncludeDir {.strdefine.} = getEnv("CT_ZSTD_INCLUDE_DIR")
    ctZstdLibraryFile {.strdefine.} = getEnv("CT_ZSTD_LIBRARY_FILE")

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
  var zstdFlags = ""
  when defined(windows):
    doAssert ctZstdIncludeDir.isAbsolute and dirExists(ctZstdIncludeDir) and
      fileExists(ctZstdIncludeDir / "zstd.h") and
      ctZstdLibraryFile.isAbsolute and fileExists(ctZstdLibraryFile),
      "runtime ct-print requires its original compiler declared Zstd inputs"
    zstdFlags = "-d:ctZstdIncludeDir=" & quoteShell(ctZstdIncludeDir) & " " &
      "-d:ctZstdLibraryFile=" & quoteShell(ctZstdLibraryFile) & " "
  else:
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
  # Preserve the actual compiler's public source-search roles for both
  # native Nimble/Just and typed callers, rather than requiring new ambient
  # environment variables. Typed callers declare those same locked trees.
  var dependencyFlags = ""
  let sourcePaths = RuntimeSearchPaths & RuntimeLazyPaths
  for i in countdown(sourcePaths.high, 0):
    dependencyFlags.add("--path:" & quoteShell(sourcePaths[i]) & " ")
  for i in countdown(RuntimeNimblePaths.high, 0):
    dependencyFlags.add("--nimblePath:" & quoteShell(RuntimeNimblePaths[i]) & " ")
  let cmd = quoteShell(RuntimeNimCompiler) &
    " c -d:release --mm:arc -p:src " & dependencyFlags & zstdFlags &
    "--lib:" & quoteShell(RuntimeNimLib) & " " &
    "--nimcache:" & quoteShell(ctPrintBuildDir / "nimcache") & " " &
    "--hints:off --warnings:off " &
    "-o:" & quoteShell(ctPrintBin) & " " & quoteShell(ctPrintSrc)
  let (output, code) = execCmdEx(cmd)
  doAssert code == 0, "failed to build ct-print:\n" & output
  ctPrintBin
