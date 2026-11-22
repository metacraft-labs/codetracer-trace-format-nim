## Every module of the library compiles under `-d:nimPreviewSlimSystem`.
##
## Under that define `system` no longer exports `std/syncio` (`File`, `open`,
## `close`, `readFile`, ...) or `std/assertions` (`doAssert`, `assert`), so a
## module that uses them must import them itself. The library is linked into
## the CodeTracer Nim fork's compiler (`compiler/vm_trace.nim`), and the
## programs built from compiler modules with slim-system on — nimony's
## `nifler`, which `koch nimsuggest` builds first — then fail to compile at
## the first module that relies on the implicit export. `member_view.nim`
## (`File`, `close`, `doAssert`) was such a module, and every module importing
## it failed with it; `split_trace`, `managed_sender_ci`, `global_line_index`
## and `source_reload_json` had the same gap of their own. Nothing exercised
## the define before this test, so each one landed unnoticed.
##
## What is asserted, and why each part can fail:
##
##   1. **The define bites.** A probe that calls `readFile` without importing
##      `std/syncio` fails under the define and compiles without it. Without
##      this control a compiler that ignored the define (or a future Nim where
##      slim-system is the only mode and the probe's failure is the norm) would
##      make step 2 pass or fail for a reason other than the one it tests.
##   2. **No `src/` module has an error of its own under the define.** The
##      modules are found by walking `src/`, not listed, so a new module is
##      covered without an edit here. Each one is `nim check`ed, and every
##      `Error:` located in a file under `src/` fails the test, named with its
##      position: that is the import the file is missing. An error located in
##      a dependency is printed and does not fail the test. Upstream
##      `status-im/nim-stew` (the stew the nimble runner resolves) still uses
##      bare `doAssert` in `endians2` and other modules; the Nim fork builds
##      against the `metacraft-labs/nim-stew` `codetracer` branch, which
##      carries the guard, and upstreaming it is tracked on its own. `nim
##      check` goes on checking the importing modules past such an error, so
##      it does not hide this library's own.
##
## No mocks: this drives the real compiler over the real `src/` tree. The child
## compiles get this test's own search paths, so they resolve nim-stew and
## nim-results however the runner that built this test resolved them (nimble's
## package directory, or the `--path:` flags reprobuild passes).

import std/[os, osproc, strutils, algorithm, compilesettings, unittest]

const
  RepoRoot = currentSourcePath.parentDir.parentDir
  SrcDir = RepoRoot / "src"
  SearchPaths = querySettingSeq(searchPaths)

proc nimCheck(module, nimcache: string; slim: bool): tuple[output: string, exitCode: int] =
  # Full paths, so each diagnostic can be attributed to the file it is in.
  var args = @[getCurrentCompilerExe(), "check", "--hints:off",
               "--warnings:off", "--listFullPaths:on",
               "--nimcache:" & nimcache]
  if slim:
    args.add "-d:nimPreviewSlimSystem"
  for p in SearchPaths:
    args.add "--path:" & p
  args.add module
  execCmdEx(quoteShellCommand(args))

proc firstError(output: string): string =
  for line in output.splitLines():
    if "Error:" in line:
      return line.strip()
  output.strip()

proc errorFile(line: string): string =
  ## The file a compiler diagnostic `path(line, col) Error: ...` is located in.
  let paren = line.find('(')
  if paren > 0: line[0 ..< paren] else: ""

suite "library compiles under slim-system":

  let scratch = getTempDir() / "ctfnim-slim-system-" & $getCurrentProcessId()
  removeDir(scratch)
  createDir(scratch)

  test "the define removes the implicit syncio export (control)":
    let probe = scratch / "slim_probe.nim"
    writeFile(probe, "discard readFile(\"slim_probe.nim\")\n")
    let slim = nimCheck(probe, scratch / "probe_slim", slim = true)
    checkpoint(slim.output)
    check slim.exitCode != 0
    check "readFile" in slim.output.firstError
    let full = nimCheck(probe, scratch / "probe_full", slim = false)
    checkpoint(full.output)
    check full.exitCode == 0

  test "no src/ module has an error of its own under -d:nimPreviewSlimSystem":
    var modules: seq[string]
    for p in walkDirRec(SrcDir):
      if p.endsWith(".nim"):
        modules.add p
    modules.sort()
    # The walk really found the library, including the module that first
    # broke under the define.
    check modules.len > 30
    check SrcDir / "codetracer_ctfs" / "member_view.nim" in modules

    var own, foreign: seq[string]
    for i, m in modules:
      let r = nimCheck(m, scratch / ("m" & $i), slim = true)
      if r.exitCode == 0:
        continue
      for line in r.output.splitLines():
        if "Error:" notin line:
          continue
        let at = line.strip()
        if at.errorFile.isRelativeTo(SrcDir):
          let rel = at[RepoRoot.len + 1 .. ^1]
          if rel notin own:
            own.add rel
        elif at notin foreign:
          foreign.add at
    for f in foreign:
      echo "slim-system error in a dependency (not this library's): ", f
    echo "slim-system: ", own.len, " distinct errors in src/ over ",
      modules.len, " modules"
    for f in own:
      checkpoint(f)
    check own.len == 0

  removeDir(scratch)
