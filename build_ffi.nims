## The build of the C ABI library, `libcodetracer_trace_writer.a` (or its
## shared and MSVC forms): the ONE place its compiler flags are written.
##
## Every producer of that library runs this script — the `buildStaticLib`,
## `buildSharedLib`, `testFfi` and `testFfiThreads` nimble tasks, the flake's
## `trace-writer-ffi` package, and `codetracer_trace_writer_nim`'s build.rs in
## the sibling Rust repository, which builds the archive every Rust recorder
## links. A copied command line drifts: a flag added here for a reason (the
## `--threads:off` that gives the library one process-wide heap) silently goes
## missing from the copy, and the recorders that link the copy crash in the
## way the flag exists to prevent. The archive also says how it was built —
## `trace_writer_build_config()` — so a consumer can check rather than trust.
##
## It is plain NimScript, run by `nim` alone, because the consumers include
## builds with no `nimble` on PATH and Nix sandboxes with no network:
##
##   nim e build_ffi.nims [options] [-- <extra nim arguments>]
##
## Options:
##   --out:<file>        the library to write
##                       (default: `libcodetracer_trace_writer.a` here, or
##                       `codetracer_trace_writer.lib` for --target:msvc)
##   --app:staticlib|lib the library kind (default: staticlib)
##   --target:posix|msvc|mingw
##                       the ABI the library is linked into (default: the
##                       host's — msvc on Windows, posix elsewhere)
##   --nimcache:<dir>    where the C intermediates go
##
## Everything after `--` is handed to `nim c` AFTER the flags below: `--path:`
## for dependency sources, `--passC:-I...` for `zstd.h`, `--hints:off`, or —
## for a test that has to build the library WRONG to prove it can tell —
## an override such as `--threads:on`.

import std/[os, strutils]

import build_ffi_flags

let here = thisDir()
var
  app = "staticlib"
  target = when defined(windows): "msvc" else: "posix"
  output = ""
  nimcache = ""
  extra: seq[string]
  scriptSeen = false
  passthrough = false
for i in 1 .. paramCount():
  let a = paramStr(i)
  if not scriptSeen:
    scriptSeen = a.endsWith(".nims")
    continue
  if passthrough:
    extra.add a
  elif a == "--":
    passthrough = true
  elif a.startsWith("--out:"):
    output = a["--out:".len .. ^1]
  elif a.startsWith("--app:"):
    app = a["--app:".len .. ^1]
  elif a.startsWith("--target:"):
    target = a["--target:".len .. ^1]
  elif a.startsWith("--nimcache:"):
    nimcache = a["--nimcache:".len .. ^1]
  else:
    quit "build_ffi.nims: unknown option " & a &
      " (nim arguments go after `--`)", 2

if app notin ["staticlib", "lib"]:
  quit "build_ffi.nims: --app must be staticlib or lib, not " & app, 2
if target notin ["posix", "msvc", "mingw"]:
  quit "build_ffi.nims: --target must be posix, msvc or mingw, not " & target, 2
if output.len == 0:
  output = here / (if target == "msvc": "codetracer_trace_writer" &
    (if app == "lib": ".dll" else: ".lib")
  elif app == "lib": "libcodetracer_trace_writer.so"
  else: "libcodetracer_trace_writer.a")

var cmd = "nim c"
for f in ffiCompilerFlags(app, target):
  cmd.add " " & quoteShell(f)
cmd.add " " & quoteShell("--path:" & here / "src")
if nimcache.len > 0:
  cmd.add " " & quoteShell("--nimcache:" & nimcache)
for e in extra:
  cmd.add " " & quoteShell(e)
cmd.add " " & quoteShell("-o:" & output)
cmd.add " " & quoteShell(here / "src" / "codetracer_trace_writer_ffi.nim")
echo cmd
exec cmd
