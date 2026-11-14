default:
    @just --list

alias t := test
alias fmt := format

# --- Build ------------------------------------------------------------

# Build every shipping artifact: the `ct-print` / `ct-space` inspectors and
# the C-FFI static library the Rust adapter crate
# (`codetracer_trace_writer_nim`) links against.
build: build-ct-print build-ct-space build-ctfs-salvage build-static-lib

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

# The full suite.
test: test-nim test-ffi

# The repo's canonical corpus — the `test` task in
# `codetracer_trace_format.nimble` (every `tests/test_*.nim` the package
# declares). The checks against the Rust implementation of the format live in
# codetracer-trace-format-conformance.
test-nim:
    nimble test

# The codetracer-ci sender writes each object by the route its upload session
# names, into a real MinIO with a stand-in of codetracer-ci's upload routes
# (`tests/test_managed_sender_ci_routes.nim`; the dev shell provides `minio`
# and `python3`). Not in the nimble task, like `tests/test_managed_sender.nim`.
test-managed-sender-ci-routes:
    nim c -r -d:release -p:src -o:build/test-bin/test_managed_sender_ci_routes tests/test_managed_sender_ci_routes.nim

# The native tool contract of `build_writer_artifacts` (POSIX): a sibling
# recipe that calls `buildCtPrint` with the documented `uses:` builds ct-print
# through `repro build` in an environment with no C compiler and no zstd, and
# one that omits `zstd-dev` is refused by name. Not in `test`: it provisions
# its tools through Nix and takes minutes. CI's reprobuild lane runs it.
test-native-tool-contract:
    bash tests/native_tool_contract/run.sh

# Real provider compilation and warm graph-cache invalidation; no test corpus
# execution. Kept separate because the cold provider compile takes minutes.
test-repro-tool-paths:
    python3 tests/repro_tool_paths.py

test-repro-tool-path-transport:
    pwsh -NoProfile -File tests/repro_tool_paths_transport.ps1

# The C-FFI smoke test: build the static library, compile `tests/test_ffi.c`
# against it and run it.
test-ffi:
    nimble testFfi
    nimble testFfiThreads

# Benchmarks are not part of `test` — they assert throughput and are
# machine-dependent.
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
# `test`, which runs inside the shell.
test-dev-shell:
    bash tests/test_dev_shell_writes_nothing_elsewhere.sh

# `nix flake check`'s pre-commit check runs the `lint` hook in the build
# sandbox and passes it. Builds the check, so it is not part of `test`.
test-flake-check-lint-hook:
    bash tests/test_flake_check_lint_hook.sh

# Check refs with hermetic scanner controls before the network observation.
check-ci-refs:
    ci/check-ci-refs.sh --self-test
    ci/check-ci-refs.sh
