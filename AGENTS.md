# codetracer-trace-format-nim — agent instructions

Nim implementation of the CTFS (CodeTracer File System) container format and
of the CodeTracer trace writer/reader stack built on top of it. This repo is a
linchpin of the CodeTracer subtree:

- it ships **`ct-print`**, the `.ct` container inspector every recorder shells
  out to (downstream tests resolve it at the fixed sibling path
  `../codetracer-trace-format-nim/ct-print`, which is why the binary lands at
  the repo root and not under `build/bin/`);
- it ships **`ct-space`**, the container space inspector;
- it ships **`libcodetracer_trace_writer.a`**, the C-FFI static library that
  the sibling Rust crate `codetracer-trace-format/codetracer_trace_writer_nim`
  compiles (via its `build.rs`) and links into every Rust-side recorder.

The container format itself is specified in `codetracer-trace-format-spec`;
the canonical Rust implementation is the sibling `codetracer-trace-format`.

## Dev environment

The repo is self-contained: `flake.nix` supplies the whole toolchain and
`direnv` enters it automatically. It used to have neither, and borrowed the
sibling Rust repo's dev shell — do not go back to that.

```bash
direnv allow            # first time only
nix develop             # or just `cd` in with direnv active
```

`flake.nix` provides:

- `devShells.default` — Nim 2.2 + nimble (from `nix-codetracer-toolchains`,
  the same pin the sibling Rust repo uses), `gcc`, `pkg-config`, `zstd`,
  `pcre`, a Rust toolchain + `capnproto`, and `just` / `nixfmt` / `prek`.
- `packages.default` — `ct-print`, built with a bare `nim c` against the
  pinned dependency sources (no network inside the sandbox).
- `checks.pre-commit-check` — the `git-hooks.nix` hook set (`just lint`).

### Nim dependencies

`codetracer_trace_format.nimble` requires `stew` and `results` (and `stew`
in turn requires `unittest2`). The flake pins all three as `flake = false`
inputs and the dev shell seeds them, together with the nimble package index,
into a project-local writable `NIMBLE_DIR` at `./.nimble` (gitignored). A
first `nimble test` on a clean machine therefore resolves its dependencies
without hitting the network. Set `NIMBLE_DIR` yourself before entering the
shell if you want a shared cache instead.

### Why Rust is in a Nim repo's shell

`tests/test_nim_{step,value,io_event}_stream_crossread.nim` write a real
production `.ct` bundle and then drive the sibling Rust reader's integration
test (`cargo test -p codetracer_trace_reader …` in
`../codetracer-trace-format`) to prove the bundle is byte-compatible with the
canonical reader. Those tests contain a source-level SKIP arm for
environments that have neither `cargo` nor `direnv`. Shipping the Rust
toolchain in this shell is what keeps the cross-read proof actually running
instead of silently skipping. **Never** "fix" a cross-read failure by
removing the toolchain so the SKIP arm engages.

## Commands

| Command                     | What it does                                                         |
| --------------------------- | -------------------------------------------------------------------- |
| `just build`                | `ct-print` + `ct-space` + `libcodetracer_trace_writer.a`              |
| `just build-static-lib`     | Only the C-FFI static library the Rust crate links                    |
| `just test`                 | `nimble test` (the full declared corpus) **+** the C-FFI smoke test    |
| `just test-nim`             | Only `nimble test`                                                    |
| `just test-ffi`             | `nimble testFfi` — compiles `tests/test_ffi.c` against the static lib |
| `just lint`                 | `nim check` over the library entry points + `nixfmt --check`           |
| `just format` (alias `fmt`) | `nixfmt flake.nix`                                                    |
| `just bench`                | `nimble bench` — machine-dependent, deliberately not part of `test`    |
| `just nix-build`            | `nix build .#default`                                                 |

`repro.nim` expresses the same graph natively for `reprobuild` (per-test
build + execute edges); keep the two in sync — see
`codetracer-specs/Repo-Requirements.md` §2.8.1.

Nim has no formatter this repo trusts unattended (`nimpretty` reflows
`when`/`case` bodies), so `just format` covers the Nix sources only — the same
choice the sibling Nim repos `io-mon`, `nim-stackable-hooks` and
`codetracer-launcher` make.

## Layout

- `src/codetracer_ctfs.nim` — the CTFS container (Base40 filenames, multi-level
  block mapping, streaming writes, COW B-tree, sub-block pool).
- `src/codetracer_trace_types.nim` — the shared record/value types.
- `src/codetracer_trace_writer.nim` — the multi-stream production writer
  (`steps.dat`, `values.dat`, `calls.dat`, `events.dat`, line-hit and
  mem-write builders).
- `src/codetracer_trace_reader.nim` — the reader side.
- `src/codetracer_trace_writer_ffi.nim` — the C FFI surface; compiled to
  `libcodetracer_trace_writer.a`. Its entry points are declared with
  `--nimMainPrefix:codetracerTraceWriter`, which **must** match the
  `codetracerTraceWriterNimMain` `importc` — the prefix is what lets the lib be
  linked next to another Nim-compiled artifact without a duplicate `NimMain`.
- `src/codetracer_ct_print.nim` / `src/codetracer_ct_space.nim` — the CLIs.
- `include/codetracer_trace_writer.h` — the C header for the FFI.
- `tests/` — the corpus; the canonical list is the `test` task in
  `codetracer_trace_format.nimble`.

## Testing notes

- The corpus is declared in the `.nimble` `test` task, not discovered. Adding
  a test file means adding it there (and to `repro.nim`'s `testSpecs`).
- `tests/test_chunked_compressed_table.nim`'s
  `bench_chunked_table_write_throughput` gate is guarded by
  `when defined(release)`. The corpus compiles the file in **debug** on
  purpose, so that gate is compiled out; it only fires from `just bench`. Do
  not "promote" the file to a release build. (Its `bench_chunked_table_decompress`
  neighbour is *not* guarded — see the known-red note below.)
- `tests/test_path_filter.nim` uses `std/re`, which dlopens libpcre — the dev
  shell puts it on `LD_LIBRARY_PATH`.
- **Known red, pre-existing (3 tests).** Running every command of the nimble
  `test` task independently on x86_64-linux gives **48 pass / 3 fail**. All
  three failures are hard-coded *performance* gates embedded in correctness
  tests, and all three reproduce under the old borrowed
  `codetracer-trace-format` dev shell (in two of the three cases by a *wider*
  margin than in this repo's own shell) — they predate this repo's own flake
  and are not a provisioning problem. Numbers below are `this shell` /
  `old borrowed shell` on an idle x86_64-linux host:
  - `test_chunked_compressed_table.nim` `bench_chunked_table_decompress` —
    `perLookupNs < 50000`, measured 151 740 / 172 839 ns (in **both** debug
    and `-d:release`; each lookup zstd-decompresses a whole 64 KiB chunk with
    no chunk cache).
  - `test_sub_block_pool.nim` — `throughput > 500000.0` allocs/sec, measured
    303 751 / 210 322.
  - `test_new_trace_reader.nim` — `medianUs < 100.0`, measured 267.2 / 263.1 µs.

  Because `nimble test` stops at the first failing command, `just test`
  currently exits non-zero on this host. Do **not** relax or delete these
  assertions to get a green run. Fixing them is real work (a chunk cache in
  `ChunkedCompressedTableReader.read`; profiling the sub-block pool and the
  reader's navigate path) or moving the gates into the `bench` task, where
  their machine-dependent siblings already live. See the
  `## Known PRE-EXISTING red tests` list in `.agents/codebase-insights.txt`.
- `tests/test_reader_ffi.nim` and
  `tests/test_pending_value_after_delta_column.nim` are **not** in the corpus
  (not in the `.nimble` task, not in CI, not in `repro.nim`). They predate the
  SPEC-canonical step-stream migration and fail their own asserts. That is
  known product/test drift in an off-corpus FFI reader path, tracked
  separately — it is not a provisioning gap and must not be papered over.

## Specs

- `codetracer-trace-format-spec` — the format definition.
- `codetracer-specs/Repo-Requirements.md` — the repo conventions this layout
  implements (Part 1 §§1.1–1.3, 1.7; Part 3 support-repo matrix).
- `codetracer-specs/Planned-Features/Value-Origin-Tracking.milestones.org`
  § M34 — the milestone that added this flake, `.envrc` and Justfile.
