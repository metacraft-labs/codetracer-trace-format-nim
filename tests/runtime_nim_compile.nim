## The ONE way a test compiles a Nim program while it runs.
##
## Some tests compile a second program at runtime: `ct_print_binary` builds
## ct-print, and the stream cross-read tests build their fixture generators.
## Such a compile must use the toolchain the test itself was compiled with, not
## whatever `nim` and Nim packages the ambient environment offers. Under a
## `repro.nim` execute edge the ambient environment holds only the declared
## tools, and a bare `nim c -p:src` finds `results` only if a `~/.nimble`
## happens to supply it. So the compiler, its library, its source search paths
## and the zstd flags all come from the test's own compile and from the
## `PKG_CONFIG_PATH` (or, on Windows, the zstd defines) its runner declared.

import std/[os, osproc, strutils, compilesettings]

const
  RuntimeNimCompiler = getCurrentCompilerExe()
  RuntimeNimLib = querySetting(SingleValueSetting.libPath)
  RuntimeSearchPaths = querySettingSeq(MultipleValueSetting.searchPaths)
  RuntimeLazyPaths = querySettingSeq(MultipleValueSetting.lazyPaths)
  RuntimeNimblePaths = querySettingSeq(MultipleValueSetting.nimblePaths)

when defined(windows):
  const
    ctZstdIncludeDir {.strdefine.} = getEnv("CT_ZSTD_INCLUDE_DIR")
    ctZstdLibraryFile {.strdefine.} = getEnv("CT_ZSTD_LIBRARY_FILE")

proc zstdFlags(): string =
  when defined(windows):
    doAssert ctZstdIncludeDir.isAbsolute and dirExists(ctZstdIncludeDir) and
      fileExists(ctZstdIncludeDir / "zstd.h") and
      ctZstdLibraryFile.isAbsolute and fileExists(ctZstdLibraryFile),
      "a runtime Nim compile requires its original compiler declared Zstd inputs"
    result = "-d:ctZstdIncludeDir=" & quoteShell(ctZstdIncludeDir) & " " &
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
      result.add("--passC:" & quoteShell(f) & " ")
    for f in lflags.strip().splitWhitespace():
      result.add("--passL:" & quoteShell(f) & " ")

proc compileWithTestToolchain*(source, output, nimcache: string;
    flags = "-d:release"): tuple[output: string, exitCode: int] =
  ## Compile `source` into `output`, with `--nimcache:nimcache`, using
  ## the compiler, library and search paths this test was compiled with.
  ## `flags` are extra compiler switches, already shell-quoted.
  # Preserve the actual compiler's public source-search roles for both
  # native Nimble/Just and typed callers, rather than requiring new ambient
  # environment variables. Typed callers declare those same locked trees.
  var dependencyFlags = ""
  let sourcePaths = RuntimeSearchPaths & RuntimeLazyPaths
  for i in countdown(sourcePaths.high, 0):
    dependencyFlags.add("--path:" & quoteShell(sourcePaths[i]) & " ")
  for i in countdown(RuntimeNimblePaths.high, 0):
    dependencyFlags.add("--nimblePath:" & quoteShell(RuntimeNimblePaths[i]) & " ")
  let cmd = quoteShell(RuntimeNimCompiler) & " c " & flags & " -p:src " &
    dependencyFlags & zstdFlags() &
    "--lib:" & quoteShell(RuntimeNimLib) & " " &
    "--nimcache:" & quoteShell(nimcache) & " " &
    "--hints:off --warnings:off " &
    "-o:" & quoteShell(output) & " " & quoteShell(source)
  execCmdEx(cmd)
