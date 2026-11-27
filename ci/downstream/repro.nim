## repro.nim — the CodeTracer revision this repository's downstream lane tests
## against.
##
## This is a project of its own, nested in codetracer-trace-format-nim, with
## its own `repro.lock`. It declares exactly one repository, `codetracer`, and
## builds nothing. The repository's own `repro.nim` (at the root) names what
## this library is BUILT from; codetracer is a consumer, not one of those, so
## declaring it there would put it in this repository's develop set and in
## every refresh of its lock. Here it is a test pin, refreshed by hand.
##
## Consumer: `.github/workflows/downstream.yml`. Its `resolve` job reads the
## `codetracer` revision from `ci/downstream/repro.lock` (or takes a 40-hex
## `codetracer_revision` from a manual dispatch) through
## `ci/downstream/codetracer-revision.sh`. The `consumers` job clones
## codetracer at that commit beside this checkout, clones every other consumer
## at the revision codetracer's own locks pin for it, and runs
## `ci/downstream/run-consumers.sh`: codetracer's trace-reading suites and the
## Nim-writer recorders' trace-writing suites, against THIS commit, which sits
## where they all look for `codetracer-trace-format-nim`. A branch tip is
## never used. `tests/test_downstream_lane.sh` holds the lock, the resolver,
## the driver and the workflow to that.
##
## To move the pin: check codetracer out beside a standalone clone of this
## repository at the new revision (a PUBLISHED commit: CI clones it), run
## `repro lock refresh .` in this directory, and commit the lock. The revision
## must carry the codetracer files the lane runs (the workflow's "Refuse a
## codetracer revision that lacks what this lane runs" step lists them), and
## the consumers must pass against this repository's mainline at it: run
## `ci/downstream/run-consumers.sh --ct-dir ../codetracer all` from a layout
## like CI's first (AGENTS.md, "Downstream lane").
import repro_project_dsl

package codetracer_trace_format_nim_downstream:
  defaultToolProvisioning "path"
  uses:
    "codetracer"
  build:
    # Nothing is built here: codetracer and the recorders are consumed as
    # source by the downstream lane. A package with no `build:` block is not
    # solved, so the lock would record nothing.
    discard
