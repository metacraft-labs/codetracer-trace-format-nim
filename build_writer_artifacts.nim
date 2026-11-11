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
## **Native tool contract.** Each constructor names, on the edge it returns,
## the native tools its compile runs and links, so the engine puts them on
## that action's scoped ``PATH`` (and, for a development package, its
## ``CPATH`` / ``LIBRARY_PATH`` / ``PKG_CONFIG_PATH``) and measures their
## identities:
##
## * the C backend ``nim c`` shells out to. Backend refs mirror the standard
##   nim.c adapter and pinned nim.cfg: macOS defaults to clang, other ctPrint
##   hosts to gcc. Canonical shared MSVC flags select vccexe.exe, whose
##   compiler/linker subprocesses are cl.exe/link.exe.
## * ``zstd-dev`` (the reprobuild stdlib's Zstandard development package:
##   ``zstd.h`` on ``CPATH`` and ``libzstd`` on ``LIBRARY_PATH``), because
##   ``src/codetracer_ctfs/zstd_bindings.nim`` includes ``zstd.h`` and, given
##   no explicit archive, links ``-lzstd``. ``buildCtPrint`` names it on every
##   host: on Windows the package realizes a MinGW static ``libzstd.a``,
##   which the gcc backend links. ``buildSharedLib`` names it on POSIX only:
##   its Windows backend is MSVC, which reads neither channel and cannot link
##   a MinGW archive, so a Windows shared library still takes zstd through
##   explicit paths. Either constructor registers it only when the caller does
##   not supply zstd itself: explicit ``nativeIncludeDir`` / ``nativeLibDir``
##   / ``nativeLibraryFile`` (the owning recipe passes its dev shell's
##   ``CT_ZSTD_*``), or ``compilerSdkRefs``, whose measured compiler wrapper
##   already carries zstd's headers and link inputs.
##
## ``CtPrintToolSelectors`` and ``SharedLibToolSelectors`` export the
## selectors for this host. **A consumer must write each of them in its own
## ``uses:`` block**, in addition to supplying the ``after =`` ordering
## ``buildCtPrint`` documents. A constructor runs inside the consumer's
## ``build:`` body, after the project interface has been extracted, so the
## edge-level ref alone cannot provision a tool: only a ``uses:`` literal
## does (reprobuild's ``declareProducerTool`` documents the same limit for
## its packaging producers, and exports their selectors as constants for the
## same reason). An edge ref the package never declared used to be dropped
## without a diagnostic, and the compile then failed far away, at
## ``zstd.h: No such file or directory``. The constructors now refuse such a
## consumer by name, listing the selectors to add: ``buildCtPrint`` on every
## host, ``buildSharedLib`` on POSIX. ``buildSharedLib``'s Windows refs, and
## what its Windows consumers declare, are unchanged.
##
## Optional consumer-declared compiler SDK refs precede the canonical backend
## refs, so its measured compiler wrapper supplies native headers/link inputs.
## Empty SDK refs preserve the owning recipe and existing consumers unchanged.

import std/[os, strutils]
import repro_project_dsl
import build_ffi_flags

const
  ZstdDevelopmentSelector* = "zstd-dev"
    ## The reprobuild stdlib package that supplies ``zstd.h`` and ``libzstd``
    ## (``repro_dsl_stdlib/packages/zstd_dev.nim``). The ``zstd`` selector is
    ## the CLI only and carries neither.
  CtPrintBackendSelectors = (when defined(macosx): @["clang"] else: @["gcc"])
  SharedLibBackendSelectors = (when defined(windows): @["vccexe", "cl", "link"]
                               elif defined(macosx): @["clang"]
                               else: @["gcc"])
  CtPrintZstdSelectors = @[ZstdDevelopmentSelector]
    ## On Windows too: ``zstd_bindings`` links ``-lzstd`` there when no
    ## archive is named, and ``zstd-dev``'s Windows realization supplies the
    ## header and a gcc-linkable ``libzstd.a`` through ``CPATH`` /
    ## ``LIBRARY_PATH``, as on POSIX.
  SharedLibZstdSelectors =
    (when defined(windows): newSeq[string]() else: @[ZstdDevelopmentSelector])
    ## Empty on Windows: the shared library is built by MSVC (``vccexe``),
    ## which neither reads ``CPATH`` / ``LIBRARY_PATH`` nor links the
    ## MinGW-built archive ``zstd-dev`` realizes there.
  CtPrintToolSelectors* = CtPrintBackendSelectors & CtPrintZstdSelectors
    ## Every selector ``buildCtPrint`` names on its edge on this host when the
    ## caller supplies no zstd of its own. A consumer declares each one in
    ## ``uses:`` (module doc, "Native tool contract").
  SharedLibToolSelectors* = SharedLibBackendSelectors & SharedLibZstdSelectors
    ## The same for ``buildSharedLib``.

proc requireDeclaredInUses(constructor, exportedName: string;
                           selectors: openArray[string]) =
  ## Refuse a consuming package whose ``uses:`` block lacks one of
  ## ``selectors``: the engine drops an edge ref to an undeclared tool
  ## without a diagnostic, and the compile then fails later on a missing
  ## compiler or header. Outside a package's ``build:`` body there is no
  ## owning package, and a package the registry does not hold gives nothing
  ## to check against; both are left alone.
  let owner = currentOwningPackage()
  if owner.len == 0:
    return
  for pkg in registeredPackages():
    if pkg.packageName != owner:
      continue
    var declared: seq[string] = @[]
    for use in pkg.allToolUses():
      declared.add(use.packageSelector)
    var missing: seq[string] = @[]
    for selector in selectors:
      if selector notin declared and selector notin missing:
        missing.add(selector)
    doAssert missing.len == 0,
      constructor & ": package '" & owner & "' does not declare \"" &
      missing.join("\", \"") & "\" in its uses: block. The edge " &
      constructor & " returns compiles and links with " &
      selectors.join(", ") & ", and a constructor called from build: " &
      "cannot provision a tool itself, so an undeclared one would be " &
      "dropped and the compile would fail on a missing compiler or header. " &
      "Add them to uses: (build_writer_artifacts." & exportedName &
      " exports the selectors for this host)."
    return

proc registerNativeTools(constructor, exportedName, actionId: string;
                         backendSelectors, zstdSelectors,
                         compilerSdkRefs: openArray[string];
                         callerSuppliesZstd, checkUses: bool) =
  ## Name the native tools the constructor's compile needs on ``actionId``:
  ## the consumer's SDK refs first, then the C backend, then (unless the
  ## caller supplies zstd through explicit paths or an SDK wrapper)
  ## ``zstdSelectors``. When ``checkUses`` holds, the consuming package must
  ## have declared the constructor's own selectors in ``uses:``.
  var selectors = @backendSelectors
  if not callerSuppliesZstd and compilerSdkRefs.len == 0:
    selectors.add(zstdSelectors)
  if checkUses:
    requireDeclaredInUses(constructor, exportedName, selectors)
  appendRegisteredActionToolIdentityRefs(actionId, compilerSdkRefs)
  appendRegisteredActionToolIdentityRefs(actionId, selectors)

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
  ## Construct the typed ``nim c`` edge that compiles the C ABI writer shared
  ## library from the checkout at ``projectRoot`` (relative, as for
  ## ``buildCtPrint``). The consuming package must declare
  ## ``SharedLibToolSelectors`` in its ``uses:`` block; see the module doc's
  ## "Native tool contract".
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
  registerNativeTools("buildSharedLib", "SharedLibToolSelectors", result.id,
    SharedLibBackendSelectors, SharedLibZstdSelectors, compilerSdkRefs,
    callerSuppliesZstd = nativeFlags.len > 0,
    # Its Windows contract names no zstd (``SharedLibZstdSelectors``), so its
    # Windows consumers are not checked, as before.
    checkUses = not defined(windows))

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
  ##
  ## The consuming package must declare ``CtPrintToolSelectors`` in its
  ## ``uses:`` block (the C compiler and ``zstd-dev``, on every host), and the
  ## call refuses a package that does not; the module doc's "Native tool
  ## contract" says why the constructor cannot provision them itself.
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
  registerNativeTools("buildCtPrint", "CtPrintToolSelectors", result.id,
    CtPrintBackendSelectors, CtPrintZstdSelectors, compilerSdkRefs,
    callerSuppliesZstd = nativeFlags.len > 0,
    checkUses = true)
