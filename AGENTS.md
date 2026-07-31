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
- **Benchmarks live in the `bench` task, correctness in `test`.** The two
  ChunkedCompressedTable microbenchmarks (`perLookupNs < 50000` per random
  lookup; `> 20M` records/sec write throughput) are in
  `tests/bench_chunked_table.nim`, which only `nimble bench` builds — they
  moved there from `tests/test_chunked_compressed_table.nim` in M34b,
  thresholds unchanged. Both measure the host: the decompress gate is
  ~240 compulsory 64 KiB Zstd inflations inside a 50 ms budget, i.e. "does
  this machine decompress faster than ~390 MB/s". Do not move them back into
  `test`, and do not "promote" `test_chunked_compressed_table.nim` to a
  release build — the corpus compiles it in **debug** on purpose.
  **Know the cost of that split:** `.github/workflows/ci-reprobuild.yml` runs
  `just test` and nothing else, and `repro.nim` deliberately does not model
  benchmarks, so **nothing in CI ever runs `just bench`**. A threshold in
  `bench` is documentation plus a manual command, not an enforced gate. Run
  `just bench` by hand when touching the reader or the writer's append path.
- The reader-side cache those benchmarks used to be the only witness for is
  asserted deterministically, with counters rather than a clock, by
  `test_chunked_table_chunk_cache_is_correct` in
  `tests/test_chunked_compressed_table.nim`: byte-identity of a cold vs. a
  cached read, correctness across a chunk change, correctness under forced
  eviction with a one-chunk budget, and — with a budget that is a fractional
  number of chunks — that `cacheSlotCount` stays bounded, which is what
  catches an evicted slot's buffer being stranded instead of recycled.
- `tests/test_path_filter.nim` uses `std/re`, which dlopens libpcre — the dev
  shell puts it on `LD_LIBRARY_PATH`.
- **The corpus is green.** Running every command of the nimble `test` task
  independently on x86_64-linux gives **51 pass / 0 fail**, and `just test`
  exits 0. It was 48/3 until M34b; the three red gates were hard-coded
  *performance* thresholds embedded in correctness tests. Two were closed by
  real optimisation and still assert in `test`:
  - `test_new_trace_reader.nim` `medianUs < 100.0` — was 263-267 µs, now
    **1.4 µs**. `ExecStreamReader.readEvent` re-decoded every preceding record
    in the chunk to reach record *k* (O(chunkSize) per random seek, and it
    re-counted the whole chunk on every inflation), and its one-slot chunk
    cache thrashed on a 3-chunk stream. It now keeps a per-chunk record-start
    table (built by the count pass it already made) and an LRU chunk cache.
  - `test_sub_block_pool.nim` `throughput > 500000.0` allocs/sec — was
    216-226 K, now **573-740 K**. `expandPool` zeroed each new 4 KiB block
    twice (`setLen` zero-fills, then a hand-written scalar loop did it again)
    and `allocate` cleared each slot with a scalar byte loop; both are single
    `zeroMem` calls now. This one has the **thinnest margin of the two**:
    re-sampled at load 127 it gives 569-646 K, i.e. as little as 14% over the
    500 K gate. Treat it, like `test_ram_cache`, as a gate that a slow or busy
    machine can tip red — and if it does, profile rather than relax it.

  The third could not be met on this host and its assertion moved to `bench`
  (see above) — but the underlying defect was fixed too:
  `ChunkedCompressedTableReader` had a *single* last-chunk slot, so a random
  read pattern re-inflated a 64 KiB frame on ~99.6% of lookups. With the LRU
  cache the same benchmark went from **151-161 µs** to a median of **54.6 µs**
  per lookup (12 samples, 42.5-77.1 µs) and a repeated read of a resident
  chunk costs **0.35 µs** instead of 155 µs. It still misses the 50 µs gate on
  the reference host because 240 of the 1000 lookups are compulsory misses
  and this host — a 2012-era Xeon E5-2650 @ 2.0 GHz — inflates a 64 KiB frame
  in 86 µs (quiet) to 148 µs (busy). `zstd -b3 -B65536` independently reports
  494 / 382 MB/s here, so 22-41 ms of the 50 ms budget goes into libzstd
  before a line of reader code runs. Re-sampled with the host at load 23 of
  32 cores: 37.7 / 48.5 / 53.5 / 56.0 / 72.6 µs — still 3 of 5 over the gate.
  See `Value-Origin-Tracking.milestones.org` § M34b.
- **One marginal timing gate remains, and it is NOT one of the three M34b
  fixed.** `tests/test_ram_cache.nim`'s `bench_ram_cache_hit_latency` asserts
  `perReadNs < 1000` and typically measures 790-890 ns on the reference host —
  roughly 15% of headroom. It has been seen to flake once at 1122 ns under a
  load spike. Nothing in M34b touches `ram_cache.nim`, and the gate passed in
  every completed `just test` run; but if it ever goes persistently red, the
  cause is structural and worth fixing rather than relaxing:
  `LruCache.get` returns `Option[V]`, i.e. it **copies the value out on every
  hit**, and the benchmark caches a 4096-byte block, so ~95% of that ~830 ns
  is one allocate + 4 KiB memcpy + free per read. A non-copying accessor
  (`peek`-style) is the fix; that is an API change to a type
  `cached_trace_reader.nim` also uses, so it is its own piece of work.
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
  § M34 — the milestone that added this flake, `.envrc` and Justfile;
  § M34b — the milestone that turned the resulting red `just test` green
  (chunk cache, exec-stream record index, sub-block pool zeroing).
