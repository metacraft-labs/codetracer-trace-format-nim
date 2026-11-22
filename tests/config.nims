## The C-ABI tests' `--nimMainPrefix`, decided by the test's own source.
##
## WHY.  A test that `include`s `src/codetracer_trace_writer_ffi.nim` compiles
## the FFI module's
## `proc codetracerTraceWriterNimMain() {.importc.}` into the test binary. That
## symbol exists only when the compile passes
## `--nimMainPrefix:codetracerTraceWriter` (the same prefix `build_ffi.nims`
## gives the static library), and without it the link fails with an undefined
## reference. The nimble `test` task spells the flag on each such line.
## `repro.nim` cannot: the reprobuild recipe that compiles its test edges
## (`buildNimUnittest.build`) has parameters for `-d:`, `--path:`, `--mm:` and
## the C/linker pass-throughs, but none for an arbitrary compiler switch. So
## the flag is set here, where every compile of a `tests/` module picks it up
## whoever runs it.
##
## WHAT.  The switch is set when, and only when, the main module has a line
## that is exactly `include codetracer_trace_writer_ffi`. That is the reason
## the flag is needed, so it is also the test for it, and a new C-ABI test
## needs no list entry anywhere for its compile to be right. Both mistakes
## this can make are safe. A false positive renames the binary's own `NimMain`,
## which an executable's generated `main` calls under its new name. A false
## negative (a test that reaches the FFI module some other way) fails to link,
## loudly, naming `codetracerTraceWriterNimMain`.
##
## HOW IT IS PICKED UP.  Nim runs the `config.nims` of the main module's own
## directory after those of its parents (so after the repo-root `config.nims`,
## whose nimcache placement is unaffected), and then applies the command line
## again. A nimble line that also passes `--nimMainPrefix:codetracerTraceWriter`
## therefore sets the same value twice, which changes nothing.
import std/strutils

block ffiNimMainPrefix:
  let project = projectPath()
  if project.len == 0 or not fileExists(project):
    break ffiNimMainPrefix
  for line in readFile(project).splitLines():
    if line.strip() == "include codetracer_trace_writer_ffi":
      switch("nimMainPrefix", "codetracerTraceWriter")
      break ffiNimMainPrefix
