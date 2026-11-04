## Owning typed compiler producers shared by this recipe and pinned consumers.
##
## The CodeTracer recorders decode the ``.ct`` containers their tests record
## with ``ct-print``, at the fixed sibling path
## ``../codetracer-trace-format-nim/ct-print``, and link the C ABI writer
## library. A recorder's typed test graph must CONSTRUCT those prerequisites
## rather than assume they were built beforehand, so that ``repro test`` runs
## the same tests ``just test`` does from the same clean checkout. It does so
## by importing this module and calling ``buildCtPrint`` / ``buildSharedLib``
## with the relative path of this checkout, so the compiler flags live in one
## place: here.
##
## Call each once per graph and consume both its returned action and path
## helper: the action ID keeps this repository's identity, so a graph must
## construct it at most once and share the returned handle between the edges
## that consume it. Paths are anchored to the declared project root; no
## cwd/environment discovery.
##
## Backend refs mirror the standard nim.c adapter and pinned nim.cfg: macOS
## defaults to clang, other ctPrint hosts to gcc. Canonical shared MSVC flags
## select vccexe.exe, whose compiler/linker subprocesses are cl.exe/link.exe.
## The owning recipe and each consumer must declare these tools in uses:;
## action refs both measure their identities and place them on the scoped PATH.
## Optional consumer-declared compiler SDK refs precede the canonical backend
## refs, so its measured compiler wrapper supplies native headers/link inputs.
## Empty SDK refs preserve the owning recipe and existing consumers unchanged.

import std/os
import repro_project_dsl
import build_ffi_flags

proc sharedLibraryPath*(projectRoot: string): string =
  projectRoot / (when defined(windows): "codetracer_trace_writer.dll"
                 else: "libcodetracer_trace_writer.so")

proc ctPrintPath*(projectRoot: string): string =
  ## The ``ct-print`` binary ``buildCtPrint(projectRoot)`` produces: the
  ## repository-root ``ct-print`` (``ct-print.exe`` on Windows).
  projectRoot / (when defined(windows): "ct-print.exe" else: "ct-print")

proc buildSharedLib*(projectRoot: string;
                     compilerSdkRefs: seq[string] = @[];
                     nativeIncludeDir = ""; nativeLibDir = "";
                     nimDependencyDirs: seq[string] = @[];
                     nativeLibraryFile = ""): BuildActionDef =
  doAssert projectRoot.len > 0 and not projectRoot.isAbsolute
  const target = (when defined(windows): "msvc" else: "posix")
  let output = sharedLibraryPath(projectRoot)
  var dependencyFlags: seq[string] = @[]
  for dependencyDir in nimDependencyDirs:
    doAssert dependencyDir.isAbsolute and dirExists(dependencyDir),
      "declared Nim source dependency must be an existing absolute directory"
    dependencyFlags.add("--path:" & dependencyDir)
  var nativeFlags: seq[string] = @[]
  var nativeInputs: seq[string] = @[]
  when defined(windows):
    if nativeIncludeDir.len > 0 or nativeLibraryFile.len > 0:
      doAssert nativeIncludeDir.isAbsolute and
        fileExists(nativeIncludeDir / "zstd.h") and
        nativeLibraryFile.isAbsolute and fileExists(nativeLibraryFile),
        "declared Windows Zstd requires its actual header and library file"
      nativeFlags = @["-d:ctZstdIncludeDir=" & nativeIncludeDir,
        "-d:ctZstdLibraryFile=" & nativeLibraryFile]
      nativeInputs = @[nativeIncludeDir, nativeLibraryFile]
  else:
    if nativeIncludeDir.len > 0 or nativeLibDir.len > 0:
      doAssert nativeIncludeDir.isAbsolute and nativeLibDir.isAbsolute
      nativeFlags = @["--passC:-I" & nativeIncludeDir,
        "--passL:-L" & nativeLibDir, "--passL:-Wl,-rpath," & nativeLibDir]
      nativeInputs = @[nativeIncludeDir / "zstd.h", nativeLibDir]
  let call = publicCliCall("nim", "nim", "c", "nim.nim.c", @[
    cliArgSeq("canonicalFlags", ffiCompilerFlags("lib", target),
              cpkPositional, 0),
    cliArgSeq("nimSourceDependencyFlags", dependencyFlags, cpkPositional, 0),
    cliArgSeq("nativeDependencyFlags", nativeFlags, cpkPositional, 0),
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
      projectRoot / "config.nims", projectRoot / "codetracer_trace_format.nimble"] & nativeInputs & nimDependencyDirs,
    extraOutputs = @[output], dependencyPolicy = automaticMonitorPolicy())
  appendRegisteredActionToolIdentityRefs(result.id, compilerSdkRefs)
  when defined(windows):
    appendRegisteredActionToolIdentityRefs(result.id, ["vccexe", "cl", "link"])
  elif defined(macosx):
    appendRegisteredActionToolIdentityRefs(result.id, ["clang"])
  else:
    appendRegisteredActionToolIdentityRefs(result.id, ["gcc"])

proc buildCtPrint*(projectRoot: string;
                   compilerSdkRefs: seq[string] = @[];
                   nativeIncludeDir = ""; nativeLibDir = "";
                   nimDependencyDirs: seq[string] = @[];
                   nativeLibraryFile = "";
                   after: openArray[BuildActionDef] = []): BuildActionDef =
  ## Construct the typed ``nim c`` edge that compiles ``ct-print`` from the
  ## checkout at ``projectRoot``, a path relative to the calling recipe's
  ## root (``"."`` from this repository, ``"../codetracer-trace-format-nim"``
  ## from a sibling). An absolute path would bake one host's layout into the
  ## graph, so it is refused.
  ##
  ## ``after`` orders the compile behind edges it does not read the outputs
  ## of. Without ``nimDependencyDirs`` the compile resolves this package's
  ## nimble ``requires`` (``results``, ``stew``) from Nim's default nimble
  ## path; in a recorder's graph the edge that installs them there is the
  ## cargo build whose ``codetracer_trace_writer_nim`` ``build.rs`` runs
  ## ``nimble install --depsOnly`` in this checkout, so a recorder passes that
  ## edge here.
  doAssert projectRoot.len > 0 and not projectRoot.isAbsolute,
    "buildCtPrint: projectRoot must be a non-empty relative path, got '" &
    projectRoot & "'"
  let output = ctPrintPath(projectRoot)
  var dependencyFlags: seq[string] = @[]
  for dependencyDir in nimDependencyDirs:
    doAssert dependencyDir.isAbsolute and dirExists(dependencyDir),
      "declared Nim source dependency must be an existing absolute directory"
    dependencyFlags.add("--path:" & dependencyDir)
  var nativeFlags: seq[string] = @[]
  var nativeInputs: seq[string] = @[]
  when defined(windows):
    if nativeIncludeDir.len > 0 or nativeLibraryFile.len > 0:
      doAssert nativeIncludeDir.isAbsolute and
        fileExists(nativeIncludeDir / "zstd.h") and
        nativeLibraryFile.isAbsolute and fileExists(nativeLibraryFile),
        "declared Windows Zstd requires its actual header and library file"
      nativeFlags = @["-d:ctZstdIncludeDir=" & nativeIncludeDir,
        "-d:ctZstdLibraryFile=" & nativeLibraryFile]
      nativeInputs = @[nativeIncludeDir, nativeLibraryFile]
  else:
    if nativeIncludeDir.len > 0 or nativeLibDir.len > 0:
      doAssert nativeIncludeDir.isAbsolute and nativeLibDir.isAbsolute
      nativeFlags = @["--passC:-I" & nativeIncludeDir,
        "--passL:-L" & nativeLibDir, "--passL:-Wl,-rpath," & nativeLibDir]
      nativeInputs = @[nativeIncludeDir / "zstd.h", nativeLibDir]
  let call = publicCliCall("nim", "nim", "c", "nim.nim.c", @[
    cliArg("mm", "arc", alias = "--mm:", format = cafConcat),
    cliArgSeq("defines", @["release"], alias = "-d:",
              format = cafConcat, repeated = true),
    cliArgSeq("nimSourceDependencyFlags", dependencyFlags, cpkPositional, 0),
    cliArgSeq("nativeDependencyFlags", nativeFlags, cpkPositional, 0),
    cliArg("parallelBuild", 2, alias = "--parallelBuild:", format = cafConcat),
    cliArg("paths", projectRoot / "src", alias = "--path:", format = cafConcat),
    cliArg("nimcache", projectRoot / ".repro/build/ct-print/nimcache",
           alias = "--nimcache:", format = cafConcat),
    outputArg("output", output, alias = "--out:", format = cafConcat),
    inputArg("source", projectRoot / "src/codetracer_ct_print.nim",
             cpkPositional, 0)
  ])
  result = recordToolInvocation("codetracer-trace-format-nim.ct-print.nim-c", call,
    deps = combineActionDeps([], after),
    extraInputs = @[projectRoot / "src", projectRoot / "codetracer_trace_format.nimble",
      projectRoot / "nim.cfg", projectRoot / "config.nims",
      projectRoot / "build_writer_artifacts.nim"] & nativeInputs & nimDependencyDirs,
    extraOutputs = @[output], dependencyPolicy = automaticMonitorPolicy())
  appendRegisteredActionToolIdentityRefs(result.id, compilerSdkRefs)
  when defined(macosx):
    appendRegisteredActionToolIdentityRefs(result.id, ["clang"])
  else:
    appendRegisteredActionToolIdentityRefs(result.id, ["gcc"])
