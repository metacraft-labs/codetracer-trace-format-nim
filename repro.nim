## Reprobuild project file for codetracer-trace-format-nim.
##
## **Windows-Migration M2 — minimum viable recipe.** The sole goal of
## this milestone is to expose ``ct-print`` as a named build edge so
## downstream recorder repos can request it via the qualified-selector
## form ``codetracer-trace-format-nim:ctPrint``. The sibling-discovery
## logic in ``repro_cli_support.findSiblingProjectFile``
## (``reprobuild/libs/repro_cli_support/src/repro_cli_support.nim:537``)
## materialises this file when a recorder calls
## ``repro build codetracer-trace-format-nim:ctPrint --daemon=off``
## from its own working directory.
##
## Only the ``ct-print`` binary is wired up today — the nimble file's
## other ``task`` entries (``buildCtSpace``, ``buildStaticLib`` /
## ``buildSharedLib`` / ``testFfi``, the ``test`` task, the bench
## tasks, ``regenerateFixtures``) are intentionally NOT modelled here.
## They land in a follow-on milestone once their consumers (the trace
## reader/writer FFI tests, the ct-space utility) start being driven
## from reprobuild.
##
## **Tool provisioning.** ``defaultToolProvisioning "path"`` matches the
## pattern in ``reprobuild/repro.nim:101`` — on Windows, ``env.ps1``
## prepends ``D:\metacraft-dev-deps\nim\<ver>\...\bin`` and the MSYS2
## mingw64 ``bin/`` to ``PATH`` so the path-mode resolver finds
## ``nim.exe``, ``nimble.exe``, and ``gcc.exe`` without requiring the
## engine to download tarballs. On Linux/macOS the existing nix flake
## supplies the same versions; this file is unchanged across hosts.

import repro_project_dsl

package codetracer_trace_format_nim:
  defaultToolProvisioning "path"

  uses:
    # Toolchain floor — mirrors the nimble file's ``requires "nim >=
    # 2.2.0"`` declaration. ``nimble`` is listed because the package
    # ships a nimble file (downstream consumers that want to drive the
    # nimble ``test`` task expect it on PATH); ``gcc`` is the C back-end
    # ``nim c`` shells out to.
    "nim >=2.2 <3.0"
    "nimble"
    "gcc >=12"

  # The on-disk binary name is hyphenated (``ct-print``) but the Nim
  # identifier must be a valid ident, so the executable is declared as
  # ``ctPrint`` with an explicit ``name:`` override. This matches the
  # camelCase + ``name: "<hyphenated>"`` convention used throughout the
  # reprobuild apps block (``reprobuild/repro.nim:183-199``).
  executable ctPrint:
    name: "ct-print"

  build:
    # ---- Primary build edge for ct-print -----------------------------
    #
    # Mirrors the nimble task ``buildCtPrint`` at
    # ``codetracer_trace_format.nimble:73-74``:
    #
    #     nim c -d:release --mm:arc -p:src -o:ct-print src/codetracer_ct_print.nim
    #
    # The output path is normalised to ``build/bin/ct-print.exe`` (on
    # Windows; bare ``ct-print`` elsewhere) so the artifact ends up
    # under the conventional ``build/bin/`` tree the engine treats as
    # the standard executable sink (matches the pattern in
    # ``reprobuild/repro.nim:431``). ``--path:src`` is already in the
    # repo's ``nim.cfg``, so the ``paths`` flag here is redundant
    # belt-and-braces but kept explicit to mirror the nimble task.
    const binarySuffix = (when defined(windows): ".exe" else: "")
    const ctPrintBinary = "build/bin/ct-print" & binarySuffix

    let ctPrintBuild = nim.c(
      source = "src/codetracer_ct_print.nim",
      output = ctPrintBinary,
      mm = "arc",
      defines = @["release"],
      paths = @["src"],
      actionId = "codetracer-trace-format-nim.ct-print.nim-c",
      extraInputs = @[
        "src",
        "codetracer_trace_format.nimble",
        "nim.cfg",
      ],
      extraOutputs = @[ctPrintBinary])

    discard collect("default", @[ctPrintBuild])
