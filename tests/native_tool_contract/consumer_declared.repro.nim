## Fixture consumer recipe for ``run.sh``: a sibling project that builds
## ``ct-print`` through ``build_writer_artifacts.buildCtPrint`` the way the
## CodeTracer recorders do, and declares in ``uses:`` exactly what a recorder
## declares for that edge: ``nim``, ``nimble``, the host C compiler and
## ``zstd-dev`` (``CtPrintToolSelectors``).
##
## ``run.sh`` copies it to ``<scratch>/consumer/repro.nim`` beside a copy of
## this checkout at ``<scratch>/codetracer-trace-format-nim``, the layout a
## recorder sees. It is not named ``repro.nim`` here so that nothing treats it
## as a project nested in this repository.

import std/os
import repro_project_dsl
import "../codetracer-trace-format-nim/build_writer_artifacts"

package ct_print_consumer:
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
    # A recorder orders this edge after the cargo build that installs the
    # nimble requires (``after =``). This fixture has no cargo build, so it
    # names the pinned stew/results sources ``run.sh`` forwards from the
    # owning dev shell instead; the native tools stay undeclared ambiently.
    var nimDependencyDirs: seq[string] = @[]
    for key in ["CT_NIM_STEW_SRC", "CT_NIM_RESULTS_SRC"]:
      let dependencyDir = getEnv(key)
      doAssert dependencyDir.isAbsolute and dirExists(dependencyDir),
        "run.sh must forward " & key & " from the owning dev shell"
      nimDependencyDirs.add(dependencyDir)
    discard collect("default", @[buildCtPrint("../codetracer-trace-format-nim",
      nimDependencyDirs = nimDependencyDirs)])
