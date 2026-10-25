## Owning typed producer for ``ct-print``, shared by this repository's
## ``repro.nim`` and by every recipe whose tests decode traces with it.
##
## The CodeTracer recorders decode the ``.ct`` containers their tests record
## with ``ct-print``, at the fixed sibling path
## ``../codetracer-trace-format-nim/ct-print``. A recorder's typed test graph
## must CONSTRUCT that prerequisite rather than assume it was built
## beforehand, so that ``repro test`` runs the same tests ``just test`` does
## from the same clean checkout. It does so
## by importing this module and calling ``buildCtPrint`` with the relative
## path of this checkout, so the compiler flags live in one place: here.
##
## ``buildCtPrint`` returns an ordinary typed ``nim c`` action in the
## caller's graph, not a command that launches another ``repro``. Every path
## it names (source, inputs, output) is anchored to ``projectRoot``, so the
## edge reads and writes the same files whether this repository or a sibling
## constructs it. Its action ID keeps this repository's identity, so a graph
## must construct it at most once and share the returned handle between the
## edges that consume it.
##
## The flags mirror the nimble ``buildCtPrint`` task
## (``codetracer_trace_format.nimble``: ``nim c -d:release --mm:arc -p:src
## -o:ct-print src/codetracer_ct_print.nim``), which ``just build-ct-print``
## in the recorders runs. A caller's ``uses:`` block must declare the tools
## the action runs: ``nim`` and the C compiler ``nim c`` drives.

import std/os
import repro_project_dsl
# Aliased so that ``nim`` names the package value (``nim.c``), not the
# module, which would otherwise shadow it in this file.
import repro_dsl_stdlib/packages/nim as nim_package

proc ctPrintPath*(projectRoot: string): string =
  ## The ``ct-print`` binary ``buildCtPrint(projectRoot)`` produces: the
  ## repository-root ``ct-print`` (``ct-print.exe`` on Windows).
  projectRoot / (when defined(windows): "ct-print.exe" else: "ct-print")

proc buildCtPrint*(projectRoot: string;
                   after: openArray[BuildActionDef] = []): BuildActionDef =
  ## Construct the typed ``nim c`` edge that compiles ``ct-print`` from the
  ## checkout at ``projectRoot``, a path relative to the calling recipe's
  ## root (``"."`` from this repository, ``"../codetracer-trace-format-nim"``
  ## from a sibling). An absolute path would bake one host's layout into the
  ## graph, so it is refused.
  ##
  ## ``after`` orders the compile behind edges it does not read the outputs
  ## of. The compile resolves this package's nimble ``requires`` (``results``,
  ## ``stew``) from Nim's default nimble path; in a recorder's graph the
  ## edge that installs them there is the cargo build whose
  ## ``codetracer_trace_writer_nim`` ``build.rs`` runs ``nimble install
  ## --depsOnly`` in this checkout, so a recorder passes that edge here.
  doAssert projectRoot.len > 0 and not projectRoot.isAbsolute,
    "buildCtPrint: projectRoot must be a non-empty relative path, got '" &
    projectRoot & "'"
  let output = ctPrintPath(projectRoot)
  nim.c(
    source = projectRoot / "src/codetracer_ct_print.nim",
    output = output,
    mm = "arc",
    defines = @["release"],
    paths = @[projectRoot / "src"],
    actionId = "codetracer-trace-format-nim.ct-print.nim-c",
    after = after,
    extraInputs = @[
      projectRoot / "src",
      projectRoot / "codetracer_trace_format.nimble",
      projectRoot / "nim.cfg",
      projectRoot / "build_writer_artifacts.nim",
    ],
    extraOutputs = @[output])
