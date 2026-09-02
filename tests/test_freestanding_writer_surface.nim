## The writer half of the C ABI compiles for a target with no filesystem, and
## the filesystem gate removes exactly the two entry points that name a file.
##
## `new_trace_reader.nim` reached `fileExists` and `open(path, fmRead)` from
## `openNewTrace`, and the FFI reached `openNewTrace` from `ct_reader_open`.
## Nothing else in the module graph touches the filesystem, so those two entry
## points were the whole reason the 129-function C ABI could not be built for
## `--os:any` — the target a wasm32 embedding uses. `ctHasFilesystem` gates
## them on the TARGET (`posix`/`windows` versus the freestanding OSes) rather
## than on a define someone has to remember to pass.
##
## What is asserted here, and why each one can fail:
##
##   1. **The freestanding compile succeeds.** Before the gate it stopped at
##      `new_trace_reader.nim` with `undeclared identifier: 'fileExists'`. This
##      is a `--compileOnly` build, so it needs no cross C toolchain: what is
##      being checked is that the Nim module graph is freestanding-clean, not
##      that a particular linker is installed.
##   2. **The gate DISCRIMINATES, in both directions.** Every one of the C ABI's
##      `exportc` functions is looked for in the emitted C. Exactly
##      `ct_reader_open` and `ct_reader_refresh` must be missing — a gate that
##      removed nothing fails the first half of that, and a gate that removed
##      the reader wholesale (or the writer with it) fails the second. The
##      expected-missing set is spelled out, so widening the gate is a
##      deliberate edit to this test rather than a silent drift.
##   3. **The native arm keeps them.** `ctHasFilesystem` is true here and
##      `openNewTrace` resolves, which is the other half of the same claim:
##      the gate is a target difference, not a deletion.
##
## No mocks: this drives the real compiler over the real `src/` tree and reads
## the C it emits.

import std/[os, osproc, strutils, sequtils, algorithm, unittest]
import results
import codetracer_trace_writer/new_trace_reader

const
  RepoRoot = currentSourcePath.parentDir.parentDir
  FfiSource = RepoRoot / "src" / "codetracer_trace_writer_ffi.nim"
  # The two entry points that name a file. Everything else on the ABI works on
  # bytes it was handed, so everything else must survive.
  ExpectedMissing = ["ct_reader_open", "ct_reader_refresh"]

proc exportcNames(): seq[string] =
  ## Every `exportc` function the FFI declares, read out of the source: the
  ## pragma line, attributed to the `proc` header above it.
  var last = ""
  for line in lines(FfiSource):
    let stripped = line.strip()
    if stripped.startsWith("proc "):
      let rest = stripped[5 .. ^1]
      var name = ""
      for c in rest:
        if c.isAlphaNumeric or c == '_': name.add c
        else: break
      last = name
    if "exportc" in line and last.len > 0:
      result.add last
      last = ""

suite "freestanding writer surface":

  test "the native build keeps the filesystem entry points":
    check ctHasFilesystem
    check compiles(openNewTrace("/nonexistent.ct"))
    # A path that is not there is an error rather than a crash, which is also
    # the proof the proc is really linked in and not merely declared.
    let r = openNewTrace(RepoRoot / "no-such-container.ct")
    check r.isErr

  test "the C ABI compiles for --os:any and loses exactly the two path openers":
    let names = exportcNames()
    check names.len > 100          # the source really was parsed
    for m in ExpectedMissing:
      check m in names

    let cache = getTempDir() / "ctfnim-freestanding-surface"
    removeDir(cache)
    let cmd = quoteShellCommand([
      getCurrentCompilerExe(), "c", "--compileOnly",
      "--os:any", "--cpu:wasm32",
      "--mm:arc", "-d:useMalloc", "--threads:off", "--noMain",
      "-d:noSignalHandler", "-d:ctHostClock", "-d:ctLeanRecord",
      "--nimMainPrefix:codetracerTraceWriter",
      "--hints:off", "--warnings:off",
      "-p:" & (RepoRoot / "src"),
      "--nimcache:" & cache,
      FfiSource])
    let (output, code) = execCmdEx(cmd)
    if code != 0:
      # The one line that matters is the symbol the compile stopped on: that is
      # the next thing a freestanding build would need gated.
      for line in output.splitLines():
        if "Error:" in line:
          echo "freestanding compile stopped at: ", line.strip()
      checkpoint("the C ABI does not compile for --os:any; see the Error line above")
      fail()
    else:
      var emitted = ""
      for kind, p in walkDir(cache):
        if kind == pcFile and p.endsWith(".c"):
          emitted.add readFile(p)
      check emitted.len > 0

      let missing = names.filterIt(it notin emitted).sorted()
      echo "freestanding C ABI: ", names.len - missing.len, " of ", names.len,
        " exportc functions present; ", missing.len, " missing"
      if missing != @ExpectedMissing.sorted():
        checkpoint("expected exactly " & $(@ExpectedMissing.sorted()) &
          " to be gated out, got " & $missing.len & ": " &
          $missing[0 ..< min(12, missing.len)])
        fail()
      removeDir(cache)
