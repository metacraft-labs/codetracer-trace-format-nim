## Owning typed compiler producers shared by this recipe and pinned consumers.
## Call each once per graph and consume both its returned action and path helper.
## Paths are anchored to the declared project root; no cwd/environment discovery.
## Backend refs mirror the standard nim.c adapter and pinned nim.cfg: macOS
## defaults to clang, other ctPrint hosts to gcc. Canonical shared MSVC flags
## select vccexe.exe, whose compiler/linker subprocesses are cl.exe/link.exe.
## The owning recipe and each consumer must declare these tools in uses:;
## action refs both measure their identities and place them on the scoped PATH.

import std/os
import repro_project_dsl
import build_ffi_flags

proc sharedLibraryPath*(projectRoot: string): string =
  projectRoot / (when defined(windows): "codetracer_trace_writer.dll"
                 else: "libcodetracer_trace_writer.so")

proc ctPrintPath*(projectRoot: string): string =
  projectRoot / (when defined(windows): "ct-print.exe" else: "ct-print")

proc buildSharedLib*(projectRoot: string): BuildActionDef =
  doAssert projectRoot.len > 0 and not projectRoot.isAbsolute
  const target = (when defined(windows): "msvc" else: "posix")
  let output = sharedLibraryPath(projectRoot)
  let call = publicCliCall("nim", "nim", "c", "nim.nim.c", @[
    cliArgSeq("canonicalFlags", ffiCompilerFlags("lib", target),
              cpkPositional, 0),
    cliArg("parallelBuild", 2, alias = "--parallelBuild:", format = cafConcat),
    cliArg("paths", projectRoot / "src", alias = "--path:", format = cafConcat),
    cliArg("nimcache", projectRoot / ".repro/build/shared-writer/nimcache",
           alias = "--nimcache:", format = cafConcat),
    outputArg("output", output, alias = "--out:", format = cafConcat),
    inputArg("source", projectRoot / "src/codetracer_trace_writer_ffi.nim",
             cpkPositional, 1)
  ])
  result = recordToolInvocation(
    "codetracer-trace-format-nim.shared-writer.nim-c", call,
    extraInputs = @[projectRoot / "src", projectRoot / "include",
      projectRoot / "build_ffi_flags.nim", projectRoot / "build_ffi.nims",
      projectRoot / "build_writer_artifacts.nim", projectRoot / "nim.cfg",
      projectRoot / "config.nims", projectRoot / "codetracer_trace_format.nimble"],
    extraOutputs = @[output], dependencyPolicy = automaticMonitorPolicy())
  when defined(windows):
    appendRegisteredActionToolIdentityRefs(result.id, ["vccexe", "cl", "link"])
  elif defined(macosx):
    appendRegisteredActionToolIdentityRefs(result.id, ["clang"])
  else:
    appendRegisteredActionToolIdentityRefs(result.id, ["gcc"])

proc buildCtPrint*(projectRoot: string): BuildActionDef =
  doAssert projectRoot.len > 0 and not projectRoot.isAbsolute
  let output = ctPrintPath(projectRoot)
  let call = publicCliCall("nim", "nim", "c", "nim.nim.c", @[
    cliArg("mm", "arc", alias = "--mm:", format = cafConcat),
    cliArgSeq("defines", @["release"], alias = "-d:",
              format = cafConcat, repeated = true),
    cliArg("parallelBuild", 2, alias = "--parallelBuild:", format = cafConcat),
    cliArg("paths", projectRoot / "src", alias = "--path:", format = cafConcat),
    cliArg("nimcache", projectRoot / ".repro/build/ct-print/nimcache",
           alias = "--nimcache:", format = cafConcat),
    outputArg("output", output, alias = "--out:", format = cafConcat),
    inputArg("source", projectRoot / "src/codetracer_ct_print.nim",
             cpkPositional, 0)
  ])
  result = recordToolInvocation("codetracer-trace-format-nim.ct-print.nim-c", call,
    extraInputs = @[projectRoot / "src", projectRoot / "codetracer_trace_format.nimble",
      projectRoot / "nim.cfg", projectRoot / "config.nims",
      projectRoot / "build_writer_artifacts.nim"],
    extraOutputs = @[output], dependencyPolicy = automaticMonitorPolicy())
  when defined(macosx):
    appendRegisteredActionToolIdentityRefs(result.id, ["clang"])
  else:
    appendRegisteredActionToolIdentityRefs(result.id, ["gcc"])
