## Fixture consumer recipe for ``run.sh``: declares everything
## ``consumer_declared.repro.nim`` declares, but names only the ``stew``
## sources in ``nimDependencyDirs`` and leaves out ``results``, which
## ``ct-print`` also imports.
##
## ``run.sh`` plants a copy of ``results`` on the Nim default nimble path of
## the build (``$NIMBLE_DIR/pkgs2``, which Nim reads in place of
## ``$HOME/.nimble/pkgs2`` when ``NIMBLE_DIR`` is set). A consumer that supplies
## ``nimDependencyDirs`` gets a compile that reads nothing from that path, so
## this build must fail on the missing ``results`` instead of resolving it
## from the planted copy, which is what it did before ``buildCtPrint`` passed
## ``--noNimblePath``.
##
## ``run.sh`` copies it to ``<scratch>/consumer-missing-dependency/repro.nim``
## beside a copy of this checkout, the layout a recorder sees. It is not named
## ``repro.nim`` here so that nothing treats it as a project nested in this
## repository.

import std/os
import repro_project_dsl
import "../codetracer-trace-format-nim/build_writer_artifacts"

package ct_print_consumer_missing_dependency:
  defaultToolProvisioning "path"

  uses:
    "nim >=2.2 <3.0"
    "nimble"
    when defined(macosx):
      "clang"
    else:
      "gcc"
    "zstd-dev"

  build:
    let stewDir = getEnv("CT_NIM_STEW_SRC")
    doAssert stewDir.isAbsolute and dirExists(stewDir),
      "run.sh must forward CT_NIM_STEW_SRC from the owning dev shell"
    discard collect("default", @[buildCtPrint("../codetracer-trace-format-nim",
      nimDependencyDirs = @[stewDir])])
