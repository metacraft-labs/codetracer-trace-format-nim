default:
    @just --list

alias t := test
alias fmt := format

# --- Build ------------------------------------------------------------

# Build every shipping artifact: the `ct-print` / `ct-space` inspectors and
# the C-FFI static library the sibling Rust crate
# (`codetracer-trace-format/codetracer_trace_writer_nim`) links against.
build: build-ct-print build-ct-space build-static-lib

# `ct-print` lands at the repo root on purpose — downstream recorder tests
# resolve it at the fixed sibling path `../codetracer-trace-format-nim/ct-print`.
build-ct-print:
    nimble buildCtPrint

build-ct-space:
    nimble buildCtSpace

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
# declares, including the three `test_nim_*_crossread` proofs that drive the
# sibling Rust reader through `cargo test`).
test-nim:
    nimble test

# The C-FFI smoke test: build the static library, compile `tests/test_ffi.c`
# against it and run it.
test-ffi:
    nimble testFfi

# Benchmarks are not part of `test` — they assert throughput and are
# machine-dependent.
bench:
    nimble bench

# --- Lint -------------------------------------------------------------

lint: lint-nim lint-nix

# Nim has no separate linter; `nim check` is the type/semantic pass. Run it
# over the library entry points that everything else imports.
lint-nim:
    #!/usr/bin/env bash
    # `-e` so ANY failing check fails the recipe rather than only the last.
    set -euo pipefail
    for m in src/codetracer_ctfs.nim \
             src/codetracer_trace_types.nim \
             src/codetracer_trace_writer.nim \
             src/codetracer_trace_reader.nim \
             src/codetracer_ct_print_lib.nim; do
      nim check --hints:off --warnings:off -p:src "$m"
    done

lint-nix:
    nixfmt --check flake.nix

# `--self-test` is hermetic; the bare run resolves against the network, and
# skips rather than fails when the network is unreachable and $CI is unset.
# Kept out of `lint` so `just lint` stays usable offline.

# Check CI refs for branches that no longer exist
check-ci-refs:
    ci/check-ci-refs.sh --self-test
    ci/check-ci-refs.sh

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
