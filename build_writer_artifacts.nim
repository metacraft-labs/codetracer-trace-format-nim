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
## ``buildCtSpace`` / ``buildCtfsSalvage`` (the other two inspectors) share
## ``buildCtPrint``'s compile and its tool contract; ``buildStaticLib`` shares
## ``buildSharedLib``'s, with ``--app:staticlib``. ``buildFfiHost`` compiles a
## C host program against that archive; it needs the same tools as
## ``buildSharedLib`` plus the platform C driver, and is POSIX-only.
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

proc nimSourceDependencyFlags*(nimDependencyDirs: openArray[string]): seq[string] =
  ## The ``nim c`` flags that make ``nimDependencyDirs`` the compile's only
  ## source of Nim packages: ``--noNimblePath``, then one ``--path:`` per
  ## directory, in order. Empty when no directory is given, so the compile
  ## keeps Nim's default nimble path.
  ##
  ## ``--noNimblePath`` is what makes the directories the whole set. Without
  ## it Nim also lists the ``nimblepath`` entries of its own ``config/nim.cfg``
  ## (``$HOME/.nimble/pkgs2``, ``$HOME/.nimble/pkgs`` and, off Windows,
  ## ``/opt/nimble/pkgs2`` and ``/opt/nimble/pkgs``) at startup, and resolves
  ## any import the given directories do not provide from whatever packages
  ## the host has installed there. The listings are read by the edge, so they
  ## would make its recorded dependencies, and its cache key, depend on the
  ## host, and a dependency the caller forgot would build anyway.
  ## https://nim-lang.org/docs/nimc.html#compiler-usage-commandminusline-switches
  ## documents the switch; it clears the nimble paths and stops later
  ## ``nimblepath`` entries in the configuration files from adding any.
  ##
  ## Each directory must be an existing absolute path.
  if nimDependencyDirs.len == 0:
    return @[]
  result = @["--noNimblePath"]
  for dependencyDir in nimDependencyDirs:
    doAssert dependencyDir.isAbsolute and dirExists(dependencyDir),
      "declared Nim source dependency must be an existing absolute directory"
    result.add("--path:" & dependencyDir)

proc staticLibraryPath*(projectRoot: string): string =
  ## The archive ``buildStaticLib(projectRoot)`` produces, where
  ## ``build_ffi.nims`` writes it by default.
  projectRoot / (when defined(windows): "codetracer_trace_writer.lib"
                 else: "libcodetracer_trace_writer.a")

proc buildFfiLibrary(constructor, app, actionId, nimcacheName, output,
                     projectRoot: string;
                     compilerSdkRefs: seq[string];
                     nativeIncludeDir, nativeLibDir: string;
                     nimDependencyDirs: seq[string];
                     nativeLibraryFile: string): BuildActionDef =
  ## The one typed ``nim c`` compile of the C ABI writer library, in either
  ## form (``app`` is ``lib`` or ``staticlib``): the flags come from
  ## ``build_ffi_flags.ffiCompilerFlags``, the same list ``build_ffi.nims``
  ## (the nimble tasks, the flake, the Rust crate's build.rs) uses, so the
  ## two forms cannot drift from each other or from the script.
  doAssert projectRoot.len > 0 and not projectRoot.isAbsolute
  const target = (when defined(windows): "msvc" else: "posix")
  let dependencyFlags = nimSourceDependencyFlags(nimDependencyDirs)
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
    cliArgSeq("canonicalFlags", ffiCompilerFlags(app, target),
              cpkPositional, 0),
    cliArgSeq("nimSourceDependencyFlags", dependencyFlags, cpkPositional, 0),
    cliArgSeq("nativeDependencyFlags", nativeFlags, cpkPositional, 0),
    cliArg("parallelBuild", 2, alias = "--parallelBuild:", format = cafConcat),
    cliArg("paths", projectRoot / "src", alias = "--path:", format = cafConcat),
    cliArg("nimcache", projectRoot / ".repro/build" / nimcacheName / "nimcache",
           alias = "--nimcache:", format = cafConcat),
    outputArg("output", output, alias = "--out:", format = cafConcat),
    inputArg("source", projectRoot / "src/codetracer_trace_writer_ffi.nim",
             cpkPositional, 1)
  ])
  result = recordToolInvocation(actionId, call,
    extraInputs = @[projectRoot / "src", projectRoot / "include",
      projectRoot / "build_ffi_flags.nim", projectRoot / "build_ffi.nims",
      projectRoot / "build_writer_artifacts.nim", projectRoot / "nim.cfg",
      projectRoot / "config.nims", projectRoot / "codetracer_trace_format.nimble"] & nativeInputs & nimDependencyDirs,
    extraOutputs = @[output], dependencyPolicy = automaticMonitorPolicy())
  registerNativeTools(constructor, "SharedLibToolSelectors", result.id,
    SharedLibBackendSelectors, SharedLibZstdSelectors, compilerSdkRefs,
    callerSuppliesZstd = nativeFlags.len > 0,
    # Its Windows contract names no zstd (``SharedLibZstdSelectors``), so its
    # Windows consumers are not checked, as before.
    checkUses = not defined(windows))

proc buildSharedLib*(projectRoot: string;
                     compilerSdkRefs: seq[string] = @[];
                     nativeIncludeDir = ""; nativeLibDir = "";
                     nimDependencyDirs: seq[string] = @[];
                     nativeLibraryFile = ""): BuildActionDef =
  ## Construct the typed ``nim c`` edge that compiles the C ABI writer shared
  ## library from the checkout at ``projectRoot`` (relative, as for
  ## ``buildCtPrint``). ``nimDependencyDirs`` has the meaning
  ## ``buildCtPrint`` documents: when it is given, the compile resolves Nim
  ## packages from those directories only. The consuming package must declare
  ## ``SharedLibToolSelectors`` in its ``uses:`` block; see the module doc's
  ## "Native tool contract".
  buildFfiLibrary("buildSharedLib", "lib",
    "codetracer-trace-format-nim.shared-writer.nim-c", "shared-writer",
    sharedLibraryPath(projectRoot), projectRoot, compilerSdkRefs,
    nativeIncludeDir, nativeLibDir, nimDependencyDirs, nativeLibraryFile)

proc buildStaticLib*(projectRoot: string;
                     compilerSdkRefs: seq[string] = @[];
                     nativeIncludeDir = ""; nativeLibDir = "";
                     nimDependencyDirs: seq[string] = @[];
                     nativeLibraryFile = ""): BuildActionDef =
  ## Construct the typed ``nim c`` edge that compiles the C ABI writer
  ## STATIC library (``libcodetracer_trace_writer.a``) from the checkout at
  ## ``projectRoot``: what ``nimble buildStaticLib`` produces and what the
  ## Rust adapter crate links. Same contract as ``buildSharedLib``.
  buildFfiLibrary("buildStaticLib", "staticlib",
    "codetracer-trace-format-nim.static-writer.nim-c", "static-writer",
    staticLibraryPath(projectRoot), projectRoot, compilerSdkRefs,
    nativeIncludeDir, nativeLibDir, nimDependencyDirs, nativeLibraryFile)

when not defined(windows):
  proc buildFfiHost*(projectRoot, source, output: string;
                     archive: openArray[BuildActionDef] = [];
                     archivePath = "";
                     nativeIncludeDir = ""; nativeLibDir = "";
                     threads = false): BuildActionDef =
    ## Construct the typed C compiler edge that compiles the C host program
    ## ``source`` (a path under ``projectRoot``) to ``output``. It is the
    ## command the nimble ``testFfi`` / ``testFfiThreads`` tasks run:
    ## ``gcc -O1 -o <output> <source> <archive> -lzstd -lm [-lpthread]
    ## -I include``.
    ##
    ## The compiler is the C backend this edge registers
    ## (``SharedLibBackendSelectors``): ``clang`` on macOS, ``gcc`` elsewhere.
    ## The executable the edge invokes must be that selector, because the
    ## engine resolves the edge's executable against the tool profiles of the
    ## consumer's ``uses:``, which on macOS declare ``clang`` and no ``gcc``;
    ## an edge that invoked ``gcc`` there failed tool resolution with
    ## "references executable gcc but no tool profile was resolved for it".
    ##
    ## With ``archivePath`` (the static library that ``archive`` produces) the
    ## host links it. The archive and the libraries come AFTER the source: a
    ## static archive resolves only symbols already undefined when it is read,
    ## so they sit in later positional slots than the source. Then
    ## ``nativeIncludeDir`` / ``nativeLibDir`` (required) locate ``zstd.h`` /
    ## ``libzstd``, as for the Nim edges (the nimble tasks get them from the
    ## dev shell's ``CPATH`` / ``LIBRARY_PATH``). ``threads`` adds
    ## ``-lpthread``. Without ``archivePath`` the host is self-contained (the
    ## fixture guardian ``tests/ffi_host_guard.c``) and links nothing.
    doAssert projectRoot.len > 0 and not projectRoot.isAbsolute
    var compileFlags = @["-O1"]
    var libs: seq[string] = @[]
    var inputs = @[projectRoot / "include"]
    var arguments = @[
      outputArg("output", output, alias = "-o", format = cafSeparate),
      inputArg("source", projectRoot / source, cpkPositional, 1)]
    if archivePath.len > 0:
      doAssert nativeIncludeDir.isAbsolute and nativeLibDir.isAbsolute,
        "buildFfiHost: linking the archive needs the declared Zstd paths"
      compileFlags.add("-I" & (projectRoot / "include"))
      compileFlags.add("-I" & nativeIncludeDir)
      libs = @["-L" & nativeLibDir, "-Wl,-rpath," & nativeLibDir,
               "-lzstd", "-lm"]
      if threads:
        libs.add("-lpthread")
      when defined(macosx):
        libs.add(["-framework", "Security", "-framework", "CoreFoundation"])
      arguments.add(inputArg("archive", archivePath, cpkPositional, 2))
      arguments.add(cliArgSeq("linkFlags", libs, cpkPositional, 3))
      inputs.add(nativeIncludeDir / "zstd.h")
      inputs.add(nativeLibDir)
    arguments.add(cliArgSeq("compileFlags", compileFlags, cpkPositional, 0))
    const compiler = SharedLibBackendSelectors[0]
    let call = publicCliCall(compiler, compiler, "",
      compiler & "." & compiler & ".call", arguments)
    result = recordToolInvocation(
      "codetracer-trace-format-nim.ffi-host." & splitFile(output).name, call,
      deps = combineActionDeps([], archive),
      extraInputs = inputs,
      extraOutputs = @[output], dependencyPolicy = automaticMonitorPolicy())
    registerNativeTools("buildFfiHost", "SharedLibToolSelectors", result.id,
      SharedLibBackendSelectors, SharedLibZstdSelectors, @[],
      callerSuppliesZstd = true, checkUses = true)

proc buildInspector(constructor, tool, source, actionId, projectRoot: string;
                    compilerSdkRefs: seq[string];
                    nativeIncludeDir, nativeLibDir: string;
                    nimDependencyDirs: seq[string];
                    nativeLibraryFile: string;
                    after: openArray[BuildActionDef]): BuildActionDef =
  ## The one typed ``nim c`` compile of a repo-root inspector executable
  ## (``tool`` is its on-disk name, ``source`` the file under ``src/``):
  ## ``nim c -d:release --mm:arc -p:src -o:<tool> src/<source>``, as the
  ## nimble ``build*`` tasks spell it. ``buildCtPrint`` documents the rest.
  doAssert projectRoot.len > 0 and not projectRoot.isAbsolute,
    constructor & ": projectRoot must be a non-empty relative path, got '" &
    projectRoot & "'"
  let output = projectRoot / (when defined(windows): tool & ".exe" else: tool)
  let dependencyFlags = nimSourceDependencyFlags(nimDependencyDirs)
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
    cliArg("nimcache", projectRoot / ".repro/build" / tool / "nimcache",
           alias = "--nimcache:", format = cafConcat),
    outputArg("output", output, alias = "--out:", format = cafConcat),
    inputArg("source", projectRoot / "src" / source,
             cpkPositional, 0)
  ])
  result = recordToolInvocation(actionId, call,
    deps = combineActionDeps([], after),
    extraInputs = @[projectRoot / "src", projectRoot / "codetracer_trace_format.nimble",
      projectRoot / "nim.cfg", projectRoot / "config.nims",
      projectRoot / "build_writer_artifacts.nim"] & nativeInputs & nimDependencyDirs,
    extraOutputs = @[output], dependencyPolicy = automaticMonitorPolicy())
  registerNativeTools(constructor, "CtPrintToolSelectors", result.id,
    CtPrintBackendSelectors, CtPrintZstdSelectors, compilerSdkRefs,
    callerSuppliesZstd = nativeFlags.len > 0,
    checkUses = true)

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
  ## ``nimDependencyDirs`` names the directories this package's nimble
  ## ``requires`` (``results``, ``stew``) are compiled from, each an existing
  ## absolute path. When it is non-empty those directories are the ONLY place
  ## the compile looks for Nim packages: it passes ``--noNimblePath``, so it
  ## neither lists nor reads anything under Nim's default nimble path
  ## (``~/.nimble/pkgs2``, ``~/.nimble/pkgs``, ``/opt/nimble/...``), and an
  ## import none of the directories provides fails to compile instead of
  ## being resolved from the host (``nimSourceDependencyFlags``). Every
  ## directory is also an input of the edge, so the caller should name pinned
  ## checkouts.
  ##
  ## When it is empty the compile resolves the ``requires`` from Nim's
  ## default nimble path, as before. ``after`` orders the compile behind edges
  ## it does not read the outputs of; in a recorder's graph the edge that
  ## installs the ``requires`` on the nimble path is the cargo build whose
  ## ``codetracer_trace_writer_nim`` ``build.rs`` runs
  ## ``nimble install --depsOnly`` in this checkout, so a recorder that passes
  ## no ``nimDependencyDirs`` passes that edge here.
  ##
  ## The consuming package must declare ``CtPrintToolSelectors`` in its
  ## ``uses:`` block (the C compiler and ``zstd-dev``, on every host), and the
  ## call refuses a package that does not; the module doc's "Native tool
  ## contract" says why the constructor cannot provision them itself.
  buildInspector("buildCtPrint", "ct-print", "codetracer_ct_print.nim",
    "codetracer-trace-format-nim.ct-print.nim-c", projectRoot,
    compilerSdkRefs, nativeIncludeDir, nativeLibDir, nimDependencyDirs,
    nativeLibraryFile, after)

proc buildCtSpace*(projectRoot: string;
                   compilerSdkRefs: seq[string] = @[];
                   nativeIncludeDir = ""; nativeLibDir = "";
                   nimDependencyDirs: seq[string] = @[];
                   nativeLibraryFile = "";
                   after: openArray[BuildActionDef] = []): BuildActionDef =
  ## The ``ct-space`` inspector (``nimble buildCtSpace``), written to the
  ## repository root. Same arguments and tool contract as ``buildCtPrint``.
  buildInspector("buildCtSpace", "ct-space", "codetracer_ct_space.nim",
    "codetracer-trace-format-nim.ct-space.nim-c", projectRoot,
    compilerSdkRefs, nativeIncludeDir, nativeLibDir, nimDependencyDirs,
    nativeLibraryFile, after)

proc buildCtfsSalvage*(projectRoot: string;
                       compilerSdkRefs: seq[string] = @[];
                       nativeIncludeDir = ""; nativeLibDir = "";
                       nimDependencyDirs: seq[string] = @[];
                       nativeLibraryFile = "";
                       after: openArray[BuildActionDef] = []): BuildActionDef =
  ## The ``ctfs-salvage`` tool (``nimble buildCtfsSalvage``), written to the
  ## repository root. Same arguments and tool contract as ``buildCtPrint``.
  buildInspector("buildCtfsSalvage", "ctfs-salvage",
    "codetracer_ctfs_salvage.nim",
    "codetracer-trace-format-nim.ctfs-salvage.nim-c", projectRoot,
    compilerSdkRefs, nativeIncludeDir, nativeLibDir, nimDependencyDirs,
    nativeLibraryFile, after)
