default:
    @just --list

alias t := test
alias fmt := format

# --- Build ------------------------------------------------------------

# Build every shipping artifact through the reprobuild graph (`repro.nim`,
# `default` collection): the `ct-print` / `ct-space` / `ctfs-salvage`
# inspectors and the C-FFI static library the Rust adapter crate
# (`codetracer_trace_writer_nim`) links against. CI runs the same command. The
# `build-*` recipes below run one nimble task each, for a quick edit loop.
build:
    repro build

# `ct-print` lands at the repo root on purpose — downstream recorder tests
# resolve it at the fixed sibling path `../codetracer-trace-format-nim/ct-print`.
build-ct-print:
    nimble buildCtPrint

build-ct-space:
    nimble buildCtSpace

# `ctfs-salvage`: copies an abandoned live coordination page's root entries into
# the file after every writer died (`ctfs-container.md` §6, "Every writer dies").
build-ctfs-salvage:
    nimble buildCtfsSalvage

# `libcodetracer_trace_writer.a` — what `codetracer_trace_writer_nim`'s
# build.rs compiles and links. Building it here proves this repo alone can
# produce the artifact the Rust FFI crate consumes.
build-static-lib:
    nimble buildStaticLib

build-shared-lib:
    nimble buildSharedLib

# --- Test -------------------------------------------------------------

# The full suite: the `test` collection of `repro.nim` (the nimble corpus and
# the C-FFI hosts, one build edge and one execute edge each), then the parts the
# graph does not model, `test-ffi-controls` and `test-downstream-lane`. CI
# runs the same commands.
test:
    repro test
    just test-ffi-controls
    just test-downstream-lane

# The downstream lane's own contract (no network, no consumer is run): the
# workflow `.github/workflows/downstream.yml` exists, runs on the mainline
# branches, pins codetracer only by `ci/downstream/repro.lock` or a 40-hex
# dispatch input, runs every consumer `ci/downstream/run-consumers.sh` defines
# and never lets provisioning reset this checkout; the resolver and the
# driver's swap guard refuse what they must. A shell suite, so the graph does
# not model it, like the controls below.
test-downstream-lane:
    bash tests/test_downstream_lane.sh

# Run this repository's consumers against this checkout (see AGENTS.md,
# "Downstream lane"). Needs a sibling layout like CI's: CT_DIR is the
# codetracer checkout beside this one.
# ci-test-audit: covered-by downstream.yml (runs ci/downstream/run-consumers.sh once per consumer, the script this recipe wraps)
test-downstream CT_DIR="../codetracer" *CONSUMERS="all":
    bash ci/downstream/run-consumers.sh --ct-dir {{CT_DIR}} {{CONSUMERS}}

# The must-fail controls the graph does not model. A graph node that has to fail
# cannot be told from a broken one, so `repro.nim` leaves them out:
#   * `nimble testFfiThreads`: the worker-thread host must CRASH against an
#     archive built `--threads:on`, and the concurrent-writers host must FAIL
#     against one built `-d:ffiNoProcessLock`. The task also rebuilds and
#     re-runs the shipped halves, which the graph covers.
#   * `nimble testFfiMutations`: the two mutation builds of
#     `test_ffi_failures_reach_the_caller` (`-d:ffiGuardNoCatch` /
#     `-d:ffiGuardNoLatch`) must turn it red, the check the nimble `test` task
#     makes too.
test-ffi-controls:
    nimble testFfiThreads
    nimble testFfiMutations

# The nimble corpus, run directly (the graph runs the same files in `test`). It
# is the `test` task in
# `codetracer_trace_format.nimble` (every `tests/test_*.nim` the package
# declares). The checks against the Rust implementation of the format live in
# codetracer-trace-format-conformance.
# ci-test-audit: covered-by test (repro test runs every file of the nimble test task, which tests/test_every_test_is_listed.nim enforces; test-ffi-controls runs its FFI mutation controls)
test-nim:
    nimble test

# The codetracer-ci sender writes each object by the route its upload session
# names, into a real MinIO with a stand-in of codetracer-ci's upload routes
# (`tests/test_managed_sender_ci_routes.nim`; the dev shell provides `minio`
# and `python3`). The nimble `test` task and the graph's `test` collection (so
# `just test`) run it on every host but Windows; this recipe runs it alone.
# ci-test-audit: covered-by test (repro test runs test_managed_sender_ci_routes on Linux and macOS)
test-managed-sender-ci-routes:
    nim c -r -d:release -p:src -o:build/test-bin/test_managed_sender_ci_routes tests/test_managed_sender_ci_routes.nim

# The native tool contract of `build_writer_artifacts` (POSIX): a sibling
# recipe that calls `buildCtPrint` with the documented `uses:` builds ct-print
# through `repro build` in an environment with no C compiler and no zstd, one
# that omits `zstd-dev` is refused by name, and one whose `nimDependencyDirs`
# omits a package does not get it from the nimble path. Not in `test`: it provisions
# its tools through Nix and takes minutes. CI's reprobuild lane runs it.
test-native-tool-contract:
    bash tests/native_tool_contract/run.sh

# Real provider compilation and warm graph-cache invalidation; no test corpus
# execution. Kept separate because the cold provider compile takes minutes.
test-repro-tool-paths:
    python3 tests/repro_tool_paths.py

test-repro-tool-path-transport:
    pwsh -NoProfile -File tests/repro_tool_paths_transport.ps1

# The C-FFI smoke tests run directly with nimble: build the static library,
# compile `tests/test_ffi.c` against it and run it, then `testFfiThreads`.
test-ffi:
    nimble testFfi
    nimble testFfiThreads

# Benchmarks are not part of `test` — they assert throughput and are
# machine-dependent.
# ci-test-audit: manual-only (machine-dependent throughput thresholds; see AGENTS.md)
bench:
    nimble bench

# --- Lint -------------------------------------------------------------

lint: lint-nim lint-nix

# Nim has no separate linter; `nim check` is the type/semantic pass. Run it
# over the library entry points that everything else imports.
#
# One command per line, so `just` stops at the first failing check, and no
# shebang: `nix flake check` runs this recipe in the build sandbox, which has
# no `/usr/bin/env` (`tests/test_flake_check_lint_hook.sh`).
lint-nim:
    nim check --hints:off --warnings:off -p:src src/codetracer_ctfs.nim
    nim check --hints:off --warnings:off -p:src src/codetracer_trace_types.nim
    nim check --hints:off --warnings:off -p:src src/codetracer_profile_writer.nim
    nim check --hints:off --warnings:off -p:src src/codetracer_trace_reader.nim
    nim check --hints:off --warnings:off -p:src src/codetracer_ct_print_lib.nim

lint-nix:
    nixfmt --check flake.nix

# --- Format -----------------------------------------------------------

# Nim ships `nimpretty`, but it is not safe to run unattended over this
# codebase (it reflows `when`/`case` bodies and has open correctness bugs),
# so — as in the sibling Nim repos (`io-mon`, `nim-stackable-hooks`,
# `codetracer-launcher`) — automated formatting covers the Nix sources only.
format: format-nix

format-nix:
    nixfmt flake.nix

# --- Nix --------------------------------------------------------------

# Verify the flake's default package (`ct-print`) builds.
nix-build:
    nix build .#default

# Entering the dev shell from another git repository must write nothing
# there (no git hooks, no .nimble). Runs `nix develop`, so it is not part of
# `test`, which runs inside the shell; CI runs it in its own step on linux-x64
# (`ci-reprobuild.yml`).
test-dev-shell:
    bash tests/test_dev_shell_writes_nothing_elsewhere.sh

# `nix flake check`'s pre-commit check runs the `lint` hook in the build
# sandbox and passes it. Builds the check, so it is not part of `test`; CI
# runs it beside `test-dev-shell`.
test-flake-check-lint-hook:
    bash tests/test_flake_check_lint_hook.sh

# The inherited Git authentication guard of ci/reconcile-checkout-templates.py
# admits exactly the job credential setup-nix exports and refuses every other
# numbered configuration. Runs the real script end to end with real Git; CI
# runs the same file in the "Reconcile authenticated inactive checkout
# templates" step on Linux (`ci-reprobuild.yml`).
# ci-test-audit: covered-by ci-reprobuild.yml (the reconcile step runs ci/test-reconcile-checkout-templates.py directly)
test-checkout-template-auth:
    python3 ci/test-reconcile-checkout-templates.py

# Check refs with hermetic scanner controls before the network observation.
# ci-test-audit: covered-by CI refs workflow (ci-refs.yml runs both commands as its own steps)
check-ci-refs:
    ci/check-ci-refs.sh --self-test
    ci/check-ci-refs.sh
