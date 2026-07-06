## Reprobuild project file for codetracer-trace-format-nim.
##
## **The linchpin recipe.** This repo ships ``ct-print`` (the ``.ct``
## container inspector every recorder in the CodeTracer subtree shells out
## to) plus the whole CTFS reader/writer/container library. Downstream
## recorder repos request the binary via the qualified selector
## ``codetracer-trace-format-nim:ctPrint`` — the sibling-discovery logic in
## ``repro_cli_support.findSiblingProjectFile``
## (``reprobuild/libs/repro_cli_support/src/repro_cli_support.nim:620``)
## materialises this file when a recorder runs
## ``repro build codetracer-trace-format-nim:ctPrint --daemon=off`` from its
## own working directory.
##
## **Scope.** The recipe wires up:
##
##   * ``executable ctPrint`` — ``src/codetracer_ct_print.nim`` compiled to
##     the repo-root ``ct-print`` (NOT ``build/bin/ct-print``): the
##     downstream recorder tests resolve the binary at the fixed sibling
##     path ``../codetracer-trace-format-nim/ct-print`` (see
##     ``codetracer-evm-recorder/tests/test_cli_convention.rs`` ``ct_print_path``),
##     so the artifact must land at the repo root.
##   * ``library codetracer_trace_format_nim`` — the ``src/`` tree that
##     consumers put on ``--path:src`` (the FFI static lib compiled by the
##     Rust sibling's ``codetracer_trace_writer_nim`` build.rs imports from
##     here).
##   * The repo's test corpus as per-file BUILD + EXECUTE edges (the
##     two-edge template from ``reprobuild-specs/Package-Model.md``
##     §"The test template"), collected into ``test-builds`` / ``test``.
##
## **Toolchain.** This repo has NO ``flake.nix`` of its own — its
## ``nim`` / ``nimble`` / ``libzstd`` / ``pkg-config`` toolchain is supplied
## by the sibling Rust ``codetracer-trace-format`` repo's dev shell (which a
## downstream recorder's flake also mirrors). ``defaultToolProvisioning
## "path"`` therefore selects the weak-local PATH resolver: the nix dev
## shell puts the toolchain floor on ``PATH`` and ``PKG_CONFIG_PATH``.
##
## **libzstd.** ``test_ct_print_events_log_fallback`` compiles ct-print at
## runtime and links libzstd via ``pkg-config --cflags/--libs libzstd``;
## the ct-print C output ``#include``s ``zstd.h``. The dev shell's
## ``PKG_CONFIG_PATH`` (zstd's ``pkgconfig`` dir) satisfies both, so no
## per-edge zstd plumbing is needed — the shared ``nim c`` back-end picks
## up ``-lzstd`` from the ambient link flags.
##
## **The three cross-read tests** (``test_nim_{step,value,io_event}_stream_crossread``)
## prove a Nim-written production ``.ct`` bundle is byte-compatible with the
## canonical RUST reader: each writes a bundle, then invokes the sibling
## ``../codetracer-trace-format`` Rust integration test via ``cargo test``.
## The dev shell puts ``cargo`` on ``PATH`` (from the same Rust flake), so
## the test's ``haveCargo`` branch runs ``cargo`` DIRECTLY — it never enters
## the ``direnv exec`` fallback. The real cross-check runs; nothing is
## skipped. Because all three drive ``cargo test`` against the SAME sibling
## Rust target directory, they are sequenced through a capacity-1 build pool
## so their concurrent ``cargo`` runs do not race the shared target lock.
##
## **``test_path_filter``** uses ``std/re`` (Nim's PCRE binding), which
## ``dlopen``s ``libpcre.so`` at runtime. The dev shell does not put pcre on
## ``LD_LIBRARY_PATH``, so instead of a fragile runtime search this edge
## links pcre directly (``--dynlibOverride:pcre --passL:-L<dir> -lpcre``);
## the nix ``ld`` wrapper bakes pcre's rpath into the binary, so it runs
## hermetically with no ``LD_LIBRARY_PATH``. The pcre lib dir is threaded in
## via the ``CT_PCRE_LIB_DIR`` env var the outer dev-shell nesting exports;
## when unset the edge falls back to a plain ``--passL:-lpcre`` (the nix C
## toolchain's ``NIX_LDFLAGS`` may still resolve it) so the recipe stays
## portable on hosts that put libpcre on the default search path.

import repro_project_dsl

# ``ct_test_nim_unittest`` supplies ``buildNimUnittest.build(...)`` (the
# per-test compile edge) and ``edge.testBinary.run(...)`` (the execute
# edge). It re-exports ``repro_project_dsl``.
import ct_test_nim_unittest

import std/os

type
  TestSpec = object
    ## One entry per test file.
    ##   * ``source`` — repo-relative ``.nim`` path.
    ##   * ``binary`` — ``build/test-bin/<stem>`` output.
    ##   * ``pooled`` — run the EXECUTE edge in the capacity-1 cargo pool
    ##     (the three cross-read tests share the sibling Rust target dir).
    ##   * ``debugOnly`` — compile WITHOUT ``-d:release``.
    ##     ``test_chunked_compressed_table`` embeds a hard 20M records/sec
    ##     write-throughput microbenchmark gate that is guarded
    ##     ``when defined(release)`` (line 327). The repo's OWN ``test``
    ##     corpus — both the nimble ``test`` task
    ##     (``codetracer_trace_format.nimble`` line 21) and CI
    ##     (``.github/workflows/test.yml`` line 40) — compiles this file
    ##     in DEBUG (no ``-d:release``), so the throughput gate is compiled
    ##     OUT there; the gate lives only in the ``bench`` task (nimble line
    ##     80, ``-d:release``). Compiling it ``-d:release`` here would
    ##     activate a machine-dependent throughput assertion the repo never
    ##     runs as a correctness test, so this edge matches the corpus:
    ##     debug build, functional assertions only, no perf gate.
    ##   * ``pcre`` — link libpcre into the compile (``test_path_filter``).
    source: string
    binary: string
    pooled: bool
    debugOnly: bool
    pcre: bool

const
  CrossReadPool = "codetracer_trace_format_nim.crossread-serial"

# The reproducible green corpus. It is the union of the nimble ``test``
# task (50 files) and the four CI-only reader/network tests
# (``test_cross_format`` / ``test_query_protocol`` / ``test_network_reader``
# / ``test_replication``), PLUS the additional in-tree unit tests that
# compile + run clean on a headless Linux host under ``-d:release -p:src``
# (``test_ct_print_full`` / ``test_ct_print_native`` / ``test_managed_sender``
# / ``test_reader_v4`` / ``test_source_views`` / ``test_column_aware_steps``).
# Every listed file compiles and runs to exit 0 with ``-d:release -p:src``;
# ``-d:release`` is a faithful superset of the per-file flags the nimble
# task uses (the non-release files run identically under it).
#
# NOT modelled — the two FFI-``include`` tests ``test_reader_ffi`` and
# ``test_pending_value_after_delta_column``. They are excluded from BOTH
# the nimble ``test`` task AND CI (``.github/workflows/test.yml``), require
# a special ``--nimMainPrefix:codetracerTraceWriter`` compile, and their
# read-back assertions predate the ``a99ae01`` SPEC-canonical step-stream
# migration: the reader-FFI value path now returns an empty value array for
# the fixture, so both fail their own ``doAssert``s in a pristine dev shell.
# That is a product/test drift in an off-corpus FFI reader path, not a
# provisioning gap, and fixing it is a separate reader-FFI milestone.
const testSpecs: seq[TestSpec] = @[
  TestSpec(source: "tests/test_base40.nim", binary: "build/test-bin/test_base40"),
  TestSpec(source: "tests/test_container.nim", binary: "build/test-bin/test_container"),
  TestSpec(source: "tests/test_streaming.nim", binary: "build/test-bin/test_streaming"),
  TestSpec(source: "tests/test_chunk_index.nim", binary: "build/test-bin/test_chunk_index"),
  TestSpec(source: "tests/test_fixed_record_table.nim", binary: "build/test-bin/test_fixed_record_table"),
  TestSpec(source: "tests/test_variable_record_table.nim", binary: "build/test-bin/test_variable_record_table"),
  TestSpec(source: "tests/test_seekable_zstd.nim", binary: "build/test-bin/test_seekable_zstd"),
  TestSpec(source: "tests/test_chunked_compressed_table.nim", binary: "build/test-bin/test_chunked_compressed_table", debugOnly: true),
  TestSpec(source: "tests/test_trace_types.nim", binary: "build/test-bin/test_trace_types"),
  TestSpec(source: "tests/test_varint.nim", binary: "build/test-bin/test_varint"),
  TestSpec(source: "tests/test_split_binary.nim", binary: "build/test-bin/test_split_binary"),
  TestSpec(source: "tests/test_trace_writer.nim", binary: "build/test-bin/test_trace_writer"),
  TestSpec(source: "tests/test_trace_reader.nim", binary: "build/test-bin/test_trace_reader"),
  TestSpec(source: "tests/test_golden_fixtures.nim", binary: "build/test-bin/test_golden_fixtures"),
  TestSpec(source: "tests/test_cross_compat.nim", binary: "build/test-bin/test_cross_compat"),
  TestSpec(source: "tests/test_meta_dat.nim", binary: "build/test-bin/test_meta_dat"),
  TestSpec(source: "tests/test_namespace_descriptor.nim", binary: "build/test-bin/test_namespace_descriptor"),
  TestSpec(source: "tests/test_sub_block_pool.nim", binary: "build/test-bin/test_sub_block_pool"),
  TestSpec(source: "tests/test_btree.nim", binary: "build/test-bin/test_btree"),
  TestSpec(source: "tests/test_cow_btree.nim", binary: "build/test-bin/test_cow_btree"),
  TestSpec(source: "tests/test_bulk_load_cow_btree.nim", binary: "build/test-bin/test_bulk_load_cow_btree"),
  TestSpec(source: "tests/test_namespace.nim", binary: "build/test-bin/test_namespace"),
  TestSpec(source: "tests/test_ct_space.nim", binary: "build/test-bin/test_ct_space"),
  TestSpec(source: "tests/test_shard_writer.nim", binary: "build/test-bin/test_shard_writer"),
  TestSpec(source: "tests/test_step_encoding.nim", binary: "build/test-bin/test_step_encoding"),
  TestSpec(source: "tests/test_interning_table.nim", binary: "build/test-bin/test_interning_table"),
  TestSpec(source: "tests/test_exec_stream.nim", binary: "build/test-bin/test_exec_stream"),
  TestSpec(source: "tests/test_value_stream.nim", binary: "build/test-bin/test_value_stream"),
  TestSpec(source: "tests/test_call_stream.nim", binary: "build/test-bin/test_call_stream"),
  TestSpec(source: "tests/test_io_event_stream.nim", binary: "build/test-bin/test_io_event_stream"),
  TestSpec(source: "tests/test_multi_stream_integration.nim", binary: "build/test-bin/test_multi_stream_integration"),
  TestSpec(source: "tests/test_new_trace_reader.nim", binary: "build/test-bin/test_new_trace_reader"),
  TestSpec(source: "tests/test_reader_calls_events.nim", binary: "build/test-bin/test_reader_calls_events"),
  TestSpec(source: "tests/test_reader_integration.nim", binary: "build/test-bin/test_reader_integration"),
  TestSpec(source: "tests/test_nim_step_stream_crossread.nim", binary: "build/test-bin/test_nim_step_stream_crossread", pooled: true),
  TestSpec(source: "tests/test_nim_value_stream_crossread.nim", binary: "build/test-bin/test_nim_value_stream_crossread", pooled: true),
  TestSpec(source: "tests/test_nim_io_event_stream_crossread.nim", binary: "build/test-bin/test_nim_io_event_stream_crossread", pooled: true),
  TestSpec(source: "tests/test_streaming_value_encoder.nim", binary: "build/test-bin/test_streaming_value_encoder"),
  TestSpec(source: "tests/test_value_ref.nim", binary: "build/test-bin/test_value_ref"),
  TestSpec(source: "tests/test_multi_stream_writer.nim", binary: "build/test-bin/test_multi_stream_writer"),
  TestSpec(source: "tests/test_linehits_builder.nim", binary: "build/test-bin/test_linehits_builder"),
  TestSpec(source: "tests/test_memwrites_builder.nim", binary: "build/test-bin/test_memwrites_builder"),
  TestSpec(source: "tests/test_step_map_builder.nim", binary: "build/test-bin/test_step_map_builder"),
  TestSpec(source: "tests/test_partial_trace_cache.nim", binary: "build/test-bin/test_partial_trace_cache"),
  TestSpec(source: "tests/test_ram_cache.nim", binary: "build/test-bin/test_ram_cache"),
  TestSpec(source: "tests/test_file_access.nim", binary: "build/test-bin/test_file_access"),
  TestSpec(source: "tests/test_split_trace.nim", binary: "build/test-bin/test_split_trace"),
  TestSpec(source: "tests/test_trace_storage_config.nim", binary: "build/test-bin/test_trace_storage_config"),
  TestSpec(source: "tests/test_path_filter.nim", binary: "build/test-bin/test_path_filter", pcre: true),
  TestSpec(source: "tests/test_ct_print_events_log_fallback.nim", binary: "build/test-bin/test_ct_print_events_log_fallback"),
  # CI-only (not in the nimble task) reader / network suites.
  TestSpec(source: "tests/test_cross_format.nim", binary: "build/test-bin/test_cross_format"),
  TestSpec(source: "tests/test_query_protocol.nim", binary: "build/test-bin/test_query_protocol"),
  TestSpec(source: "tests/test_network_reader.nim", binary: "build/test-bin/test_network_reader"),
  TestSpec(source: "tests/test_replication.nim", binary: "build/test-bin/test_replication"),
  # Additional in-tree unit tests that run clean headless.
  TestSpec(source: "tests/test_column_aware_steps.nim", binary: "build/test-bin/test_column_aware_steps"),
  TestSpec(source: "tests/test_ct_print_full.nim", binary: "build/test-bin/test_ct_print_full"),
  TestSpec(source: "tests/test_ct_print_native.nim", binary: "build/test-bin/test_ct_print_native"),
  TestSpec(source: "tests/test_managed_sender.nim", binary: "build/test-bin/test_managed_sender"),
  TestSpec(source: "tests/test_reader_v4.nim", binary: "build/test-bin/test_reader_v4"),
  TestSpec(source: "tests/test_source_views.nim", binary: "build/test-bin/test_source_views"),
]

package codetracer_trace_format_nim:
  defaultToolProvisioning "path"

  uses:
    # Toolchain floor — mirrors the nimble file's ``requires "nim >=
    # 2.2.0"``. ``nimble`` ships alongside so downstream consumers that
    # drive the nimble ``test`` task find it on PATH; ``gcc`` is the C
    # back-end ``nim c`` shells out to.
    "nim >=2.2 <3.0"
    "nimble"
    "gcc >=12"

  # The importable ``src/`` surface (``--path:src``): the CTFS container,
  # the trace reader/writer, the multi-stream writer, and the FFI entry
  # point the Rust sibling's build.rs compiles.
  library codetracer_trace_format_nim

  # ct-print — the on-disk name is hyphenated but the Nim ident must be
  # valid, so the executable is ``ctPrint`` with a ``name:`` override.
  executable ctPrint:
    name: "ct-print"

  build:
    # ---- ct-print (the `default` collection) -------------------------
    #
    # Mirrors the nimble ``buildCtPrint`` task
    # (``codetracer_trace_format.nimble`` ``nim c -d:release --mm:arc
    # -p:src -o:ct-print src/codetracer_ct_print.nim``). The output is the
    # repo-root ``ct-print`` so downstream recorder tests find it at the
    # fixed sibling path ``../codetracer-trace-format-nim/ct-print``.
    const binarySuffix = (when defined(windows): ".exe" else: "")
    const ctPrintBinary = "ct-print" & binarySuffix

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

    # ---- Test corpus (the `test` / `test-builds` collections) --------
    #
    # Two-edge template per file: a compile-only BUILD edge collected into
    # ``test-builds`` + an EXECUTE edge collected into ``test``. The three
    # cross-read EXECUTE edges share a capacity-1 pool so their concurrent
    # ``cargo test`` runs against the sibling Rust target dir do not race.
    let crossReadPool = buildPool(CrossReadPool, 1'u32)
    discard crossReadPool

    # pcre lib dir for ``test_path_filter`` (see module doc). Threaded in
    # by the outer dev-shell nesting via ``CT_PCRE_LIB_DIR``; when unset
    # the edge links a bare ``-lpcre`` and relies on the C toolchain's
    # default search path.
    let pcreLibDir = getEnv("CT_PCRE_LIB_DIR")

    var testBuildActions: seq[BuildActionDef] = @[]
    var testExecuteActions: seq[BuildActionDef] = @[]

    for spec in testSpecs:
      let stem = splitFile(spec.binary).name

      # ``test_path_filter`` links libpcre directly so Nim's ``re`` dynlib
      # ``dlopen`` resolves against the already-loaded library — no runtime
      # ``LD_LIBRARY_PATH`` needed (the nix ``ld`` wrapper bakes the rpath
      # in). ``-L<pcreLibDir>`` comes from ``CT_PCRE_LIB_DIR``; without it
      # a bare ``-lpcre`` relies on the C toolchain's default search path.
      var extraPassL: seq[string] = @[]
      if spec.pcre:
        if pcreLibDir.len > 0:
          extraPassL.add("-L" & pcreLibDir)
        extraPassL.add("-lpcre")

      # The whole corpus compiles ``-d:release`` (a faithful superset of the
      # nimble task's per-file flags), EXCEPT ``debugOnly`` files that the
      # repo's own ``test`` corpus builds in debug (see ``debugOnly`` doc).
      let buildDefines = if spec.debugOnly: newSeq[string]() else: @["release"]

      let edge = buildNimUnittest.build(
        source = spec.source,
        binary = spec.binary,
        defines = buildDefines,
        paths = @["src"],
        extraPassL = extraPassL,
        extraInputs = @["src", "codetracer_trace_format.nimble", "nim.cfg"],
        actionId = "codetracer-trace-format-nim.test_build." & stem)
      testBuildActions.add(edge.action)

      let executeEdge =
        if spec.pooled:
          edge.testBinary.run(
            actionId = "codetracer-trace-format-nim.test_execute." & stem,
            pool = CrossReadPool,
            registerImplicitName = false)
        else:
          edge.testBinary.run(
            actionId = "codetracer-trace-format-nim.test_execute." & stem,
            registerImplicitName = false)
      testExecuteActions.add(executeEdge)

    discard collect("test", testExecuteActions)
    discard collect("test-builds", testBuildActions)
