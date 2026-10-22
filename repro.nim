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
## **Toolchain.** The repository's own flake supplies ``nim``, ``nimble``,
## ``libzstd`` and ``pkg-config``. The repro dev environment activates that
## flake on Nix hosts; ``defaultToolProvisioning "path"`` consumes its tools
## without changing the existing native build graph.
##
## **libzstd.** ``test_ct_print_agreement``, ``test_logical_step_count`` and
## ``test_retired_streams_are_refused`` compile ct-print at runtime, through
## ``tests/ct_print_binary.nim``. On POSIX that supplies ``pkg-config
## --cflags/--libs libzstd``; the dev shell's ``PKG_CONFIG_PATH`` satisfies
## both. On Windows the binding names the zstd archive directly: adding MSYS2's
## whole library directory would let it replace the selected MinGW toolchain's
## implicit CRT and pthread archives. All three build into the SAME cached
## binary and nimcache (``.nimcache/test-ct-print/ct-print``), which the
## nimble task never races because it runs them one after another. Here their
## EXECUTE edges share a capacity-1 pool for the same reason.
##
## **The four cross-read tests** (``test_nim_{step,value,io_event}_stream_crossread``
## and ``test_nim_step_map_crossread``)
## prove a Nim-written production ``.ct`` bundle is byte-compatible with the
## canonical RUST reader: each writes a bundle, then invokes the sibling
## ``../codetracer-trace-format`` Rust integration test via ``cargo test``.
## The dev shell puts ``cargo`` on ``PATH`` (from the same Rust flake), so
## the test's ``haveCargo`` branch runs ``cargo`` DIRECTLY — it never enters
## the ``direnv exec`` fallback. The real cross-check runs; nothing is
## skipped. Because all four drive ``cargo test`` against the SAME sibling
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
import repro_dsl_stdlib/foreign_env

# ``ct_test_nim_unittest`` supplies ``buildNimUnittest.build(...)`` (the
# per-test compile edge) and ``edge.testBinary.run(...)`` (the execute
# edge). It re-exports ``repro_project_dsl``.
import ct_test_nim_unittest

import std/os

type
  TestSpec = object
    ## One entry per test build. Every field mirrors a flag on that file's
    ## line in the nimble ``test`` task, so the two runners compile the same
    ## thing.
    ##   * ``source`` — repo-relative ``.nim`` path.
    ##   * ``binary`` — ``build/test-bin/<stem>`` output. A file the nimble
    ##     task compiles twice (``test_reader_is_gcsafe``, once per memory
    ##     manager) has one entry per build, with distinct binaries.
    ##   * ``pool`` — run the EXECUTE edge in this capacity-1 pool: the
    ##     cross-read tests share the sibling Rust target dir
    ##     (``CrossReadPool``), and the tests that compile ct-print at runtime
    ##     share its cached binary (``CtPrintPool``). Empty runs unpooled.
    ##   * ``debugOnly`` — compile WITHOUT ``-d:release``. Matches the flags
    ##     the nimble ``test`` task uses for that file.
    ##     ``test_chunked_compressed_table`` is kept debug-only because that is
    ##     what the corpus does (the nimble ``test`` task, which is what
    ##     ``.github/workflows/ci-reprobuild.yml`` runs via ``just test``).
    ##     As of M34b the file holds
    ##     correctness assertions only: both of its microbenchmarks — the
    ##     50 µs per-lookup decompress gate and the 20M records/sec write
    ##     throughput gate — moved verbatim into ``tests/bench_chunked_table.nim``,
    ##     which only the ``bench`` task builds. Benchmarks are deliberately
    ##     NOT modelled here: they measure the host, not the build graph.
    ##     Entries added on 2026-10-18 set it wherever their nimble line has
    ##     no ``-d:release``; the older entries compile ``-d:release`` (see the
    ##     corpus note below).
    ##   * ``pcre`` — link libpcre into the compile (``test_path_filter``).
    ##   * ``defines`` — extra ``-d:`` symbols this file needs, matching the
    ##     nimble ``test`` task's per-file flags.
    ##     ``test_container_append_ordering`` needs
    ##     ``-d:ctfsAppendFaultInjection``: it abandons an append between its
    ##     two write phases, and that seam is compiled out of every ordinary
    ##     build (including the FFI static library the Rust crate links).
    ##     ``test_ffi_failures_reach_the_caller`` needs ``-d:ffiFaultInjection``
    ##     for the same kind of reason.
    ##   * ``mm`` — ``--mm:`` value, empty for Nim's default. The C-ABI tests
    ##     that ``include`` ``codetracer_trace_writer_ffi`` compile ``arc``, as
    ##     the static library does; ``test_reader_is_gcsafe`` is also built
    ##     under ``refc``, because consumers build with it.
    ##   * ``extraPaths`` — ``--path:`` entries beyond ``src``: ``tests`` for
    ##     the files that import a helper module from ``tests/``
    ##     (``-p:tests`` on their nimble line).
    ##   * ``linuxOnly`` — the nimble task runs the file under
    ##     ``when defined(linux)``; the edge is declared on Linux hosts only.
    ##
    ## The C-ABI tests also need ``--nimMainPrefix:codetracerTraceWriter``,
    ## and ``buildNimUnittest.build`` has no parameter for an arbitrary
    ## compiler switch. ``tests/config.nims`` sets it for every ``tests/``
    ## module whose source has a line ``include codetracer_trace_writer_ffi``,
    ## which is exactly the condition that makes the switch necessary, so no
    ## field here carries it. Nim reads that file for the nimble compile and
    ## for this recipe's compile alike, and the nimble line's own copy of the
    ## flag sets the same value again. A module that needs the prefix and does
    ## not get it fails to link, naming ``codetracerTraceWriterNimMain``.
    source: string
    binary: string
    pool: string
    debugOnly: bool
    pcre: bool
    defines: seq[string]
    mm: string
    extraPaths: seq[string]
    linuxOnly: bool

const
  CrossReadPool = "codetracer_trace_format_nim.crossread-serial"
  CtPrintPool = "codetracer_trace_format_nim.ct-print-runtime-serial"

# The reproducible green corpus. It is the union of the nimble ``test``
# task — every file it runs, each with that line's flags; that
# ``tests/test_every_test_is_listed.nim`` fails otherwise — and the three
# CI-only reader/network tests (``test_query_protocol`` /
# ``test_network_reader`` / ``test_replication``), PLUS the additional in-tree
# unit tests that compile + run clean on a headless Linux host under
# ``-d:release -p:src`` (``test_ct_print_full`` / ``test_managed_sender`` /
# ``test_reader_v4`` / ``test_source_views``).
# The entries that predate 2026-10-18 compile ``-d:release`` even where the
# nimble line does not; ``-d:release`` is a faithful superset of the per-file
# flags the nimble task uses (the non-release files run identically under it).
#
# NOT modelled:
#   * the two mutation builds of ``test_ffi_failures_reach_the_caller``
#     (``-d:ffiGuardNoCatch`` / ``-d:ffiGuardNoLatch``), which the nimble task
#     runs through ``gorgeEx`` and requires to FAIL. They are controls, not
#     tests, and a build graph whose nodes must fail cannot tell them from
#     broken ones — the same rule as the GDH falsifier arms below. The test
#     itself is modelled.
#   * ``tests/generate_golden_fixtures.nim``, which belongs to the nimble
#     ``regenerateFixtures`` task, not to ``test``. It is a generator: it
#     REWRITES the committed ``tests/fixtures/*.expected`` files and asserts
#     nothing, so as a test edge it would modify tracked sources and could
#     never fail. ``test_golden_fixtures`` is the test that reads them.
#   * benchmarks (``tests/bench_*.nim``), see ``debugOnly``.
const testSpecs: seq[TestSpec] = @[
  TestSpec(source: "tests/test_base40.nim", binary: "build/test-bin/test_base40"),
  TestSpec(source: "tests/test_container.nim", binary: "build/test-bin/test_container"),
  TestSpec(source: "tests/test_ctfs_v5_member_forms.nim", binary: "build/test-bin/test_ctfs_v5_member_forms"),
  # `ctfs-container.md` §1: `root_blocks`, the overflow of the entry array past
  # block 0 and its growth, with no free list root area at any `max_shards`
  # (removed 2026-10-08), in memory and streaming.
  TestSpec(source: "tests/test_root_directory_overflow.nim", binary: "build/test-bin/test_root_directory_overflow"),
  TestSpec(source: "tests/test_root_directory_grows.nim", binary: "build/test-bin/test_root_directory_grows"),
  # `ctfs-container.md` §1d, the compact profile's body: the reference
  # encoder/decoder's byte-exact round trip, the flipped-directory-bit control,
  # and the no-block-map assertion with its must-fail control against a full
  # container.
  TestSpec(source: "tests/test_compact_container_layout.nim", binary: "build/test-bin/test_compact_container_layout"),
  # `ctfs-container.md` §1a/§1f: NewTraceReader reads a recording in five
  # forms alike (version 5, version-6 full, compact, each of the last two
  # under whole-file zstd); the profile writer chooses on raw member bytes.
  TestSpec(source: "tests/test_version_6_containers_read.nim", binary: "build/test-bin/test_version_6_containers_read"),
  TestSpec(source: "tests/test_profile_threshold_choice.nim", binary: "build/test-bin/test_profile_threshold_choice"),
  TestSpec(source: "tests/test_step_encoding_rule.nim", binary: "build/test-bin/test_step_encoding_rule"),
  TestSpec(source: "tests/test_step_map_v2.nim", binary: "build/test-bin/test_step_map_v2"),
  TestSpec(source: "tests/test_durability_publishes_sealed_chunks.nim", binary: "build/test-bin/test_durability_publishes_sealed_chunks"),
  # M61/M61b integrity hardening: the write-side null-mapping guards and the
  # duplicate-name rejection ported from the native-recorder fork.
  TestSpec(source: "tests/test_ctfs_append_null_data_block.nim", binary: "build/test-bin/test_ctfs_append_null_data_block"),
  TestSpec(source: "tests/test_ctfs_duplicate_name.nim", binary: "build/test-bin/test_ctfs_duplicate_name"),
  TestSpec(source: "tests/test_container_append.nim", binary: "build/test-bin/test_container_append"),
  TestSpec(source: "tests/test_container_append_ordering.nim", binary: "build/test-bin/test_container_append_ordering", defines: @["ctfsAppendFaultInjection"]),
  TestSpec(source: "tests/test_partial_tail_bounds.nim", binary: "build/test-bin/test_partial_tail_bounds"),
  TestSpec(source: "tests/test_write_null_data_block.nim", binary: "build/test-bin/test_write_null_data_block"),
  TestSpec(source: "tests/test_trace_reader_null_mapping_root.nim", binary: "build/test-bin/test_trace_reader_null_mapping_root"),
  TestSpec(source: "tests/test_streaming.nim", binary: "build/test-bin/test_streaming"),
  TestSpec(source: "tests/test_chunk_index.nim", binary: "build/test-bin/test_chunk_index"),
  TestSpec(source: "tests/test_fixed_record_table.nim", binary: "build/test-bin/test_fixed_record_table"),
  TestSpec(source: "tests/test_variable_record_table.nim", binary: "build/test-bin/test_variable_record_table"),
  TestSpec(source: "tests/test_member_view.nim", binary: "build/test-bin/test_member_view"),
  TestSpec(source: "tests/test_member_names_are_encodable.nim", binary: "build/test-bin/test_member_names_are_encodable"),
  TestSpec(source: "tests/test_seekable_zstd.nim", binary: "build/test-bin/test_seekable_zstd"),
  TestSpec(source: "tests/test_chunked_compressed_table.nim", binary: "build/test-bin/test_chunked_compressed_table", debugOnly: true),
  TestSpec(source: "tests/test_trace_types.nim", binary: "build/test-bin/test_trace_types"),
  TestSpec(source: "tests/test_varint.nim", binary: "build/test-bin/test_varint"),
  TestSpec(source: "tests/test_split_binary.nim", binary: "build/test-bin/test_split_binary"),
  TestSpec(source: "tests/test_trace_reader.nim", binary: "build/test-bin/test_trace_reader"),
  # The reader's entry points stay callable from `{.gcsafe.}` code, under the
  # default memory manager and under refc (consumers build with it), as the
  # nimble `test` task compiles it.
  TestSpec(source: "tests/test_reader_is_gcsafe.nim", binary: "build/test-bin/test_reader_is_gcsafe"),
  TestSpec(source: "tests/test_reader_is_gcsafe.nim", binary: "build/test-bin/test_reader_is_gcsafe_refc", mm: "refc"),
  TestSpec(source: "tests/test_golden_fixtures.nim", binary: "build/test-bin/test_golden_fixtures"),
  TestSpec(source: "tests/test_cross_compat.nim", binary: "build/test-bin/test_cross_compat"),
  TestSpec(source: "tests/test_xxh64.nim", binary: "build/test-bin/test_xxh64"),
  TestSpec(source: "tests/test_corrmark_builder.nim", binary: "build/test-bin/test_corrmark_builder"),
  TestSpec(source: "tests/test_correlation_marker_api.nim", binary: "build/test-bin/test_correlation_marker_api"),
  TestSpec(source: "tests/test_close_publishes_entry_sizes.nim", binary: "build/test-bin/test_close_publishes_entry_sizes"),
  TestSpec(source: "tests/test_meta_dat.nim", binary: "build/test-bin/test_meta_dat"),
  TestSpec(source: "tests/test_writer_meta_blocks.nim", binary: "build/test-bin/test_writer_meta_blocks"),
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
  TestSpec(source: "tests/test_interning_table_set.nim", binary: "build/test-bin/test_interning_table_set"),
  TestSpec(source: "tests/test_qualified_interning.nim", binary: "build/test-bin/test_qualified_interning"),
  TestSpec(source: "tests/test_exec_stream.nim", binary: "build/test-bin/test_exec_stream"),
  TestSpec(source: "tests/test_value_stream.nim", binary: "build/test-bin/test_value_stream"),
  TestSpec(source: "tests/test_call_stream.nim", binary: "build/test-bin/test_call_stream"),
  TestSpec(source: "tests/test_io_event_stream.nim", binary: "build/test-bin/test_io_event_stream"),
  # RS-M1: spans.dat / spans.idx / spantype.ns writer + reader, and the
  # meta.dat bit 13 (FlagHasSpanStream) that gates them.
  TestSpec(source: "tests/test_span_stream.nim", binary: "build/test-bin/test_span_stream"),
  TestSpec(source: "tests/test_multi_stream_integration.nim", binary: "build/test-bin/test_multi_stream_integration"),
  TestSpec(source: "tests/test_new_trace_reader.nim", binary: "build/test-bin/test_new_trace_reader"),
  TestSpec(source: "tests/test_paths_dat_layout_authority.nim", binary: "build/test-bin/test_paths_dat_layout_authority"),
  TestSpec(source: "tests/test_column_table_decided_at_first_mention.nim", binary: "build/test-bin/test_column_table_decided_at_first_mention"),
  TestSpec(source: "tests/test_line_only_position_space.nim", binary: "build/test-bin/test_line_only_position_space"),
  TestSpec(source: "tests/test_path_registration_scales_linearly.nim", binary: "build/test-bin/test_path_registration_scales_linearly"),
  TestSpec(source: "tests/test_per_step_location_cost.nim", binary: "build/test-bin/test_per_step_location_cost"),
  TestSpec(source: "tests/test_mixed_column_aware_position_space.nim", binary: "build/test-bin/test_mixed_column_aware_position_space"),
  TestSpec(source: "tests/test_reader_calls_events.nim", binary: "build/test-bin/test_reader_calls_events"),
  TestSpec(source: "tests/test_reader_integration.nim", binary: "build/test-bin/test_reader_integration"),
  TestSpec(source: "tests/test_nim_step_stream_crossread.nim", binary: "build/test-bin/test_nim_step_stream_crossread", pool: CrossReadPool),
  TestSpec(source: "tests/test_nim_value_stream_crossread.nim", binary: "build/test-bin/test_nim_value_stream_crossread", pool: CrossReadPool),
  TestSpec(source: "tests/test_nim_io_event_stream_crossread.nim", binary: "build/test-bin/test_nim_io_event_stream_crossread", pool: CrossReadPool),
  TestSpec(source: "tests/test_streaming_value_encoder.nim", binary: "build/test-bin/test_streaming_value_encoder"),
  TestSpec(source: "tests/test_value_ref.nim", binary: "build/test-bin/test_value_ref"),
  TestSpec(source: "tests/test_multi_stream_writer.nim", binary: "build/test-bin/test_multi_stream_writer"),
  # MT7-5a: exported per-thread crossing block, read back via its C symbols.
  TestSpec(source: "tests/test_crossing_state.nim", binary: "build/test-bin/test_crossing_state"),
  TestSpec(source: "tests/test_multi_stream_attach.nim", binary: "build/test-bin/test_multi_stream_attach"),
  TestSpec(source: "tests/test_linehits_builder.nim", binary: "build/test-bin/test_linehits_builder"),
  TestSpec(source: "tests/test_memwrites_builder.nim", binary: "build/test-bin/test_memwrites_builder"),
  TestSpec(source: "tests/test_step_map_builder.nim", binary: "build/test-bin/test_step_map_builder"),
  TestSpec(source: "tests/test_partial_trace_cache.nim", binary: "build/test-bin/test_partial_trace_cache"),
  TestSpec(source: "tests/test_ram_cache.nim", binary: "build/test-bin/test_ram_cache"),
  TestSpec(source: "tests/test_file_access.nim", binary: "build/test-bin/test_file_access"),
  TestSpec(source: "tests/test_split_trace.nim", binary: "build/test-bin/test_split_trace"),
  TestSpec(source: "tests/test_trace_storage_config.nim", binary: "build/test-bin/test_trace_storage_config"),
  TestSpec(source: "tests/test_path_filter.nim", binary: "build/test-bin/test_path_filter", pcre: true),
  TestSpec(source: "tests/test_ct_print_unresolvable_position.nim", binary: "build/test-bin/test_ct_print_unresolvable_position"),
  # CI-only (not in the nimble task) reader / network suites.
  TestSpec(source: "tests/test_query_protocol.nim", binary: "build/test-bin/test_query_protocol"),
  TestSpec(source: "tests/test_network_reader.nim", binary: "build/test-bin/test_network_reader"),
  TestSpec(source: "tests/test_replication.nim", binary: "build/test-bin/test_replication"),
  # Additional in-tree unit tests that run clean headless.
  TestSpec(source: "tests/test_column_aware_steps.nim", binary: "build/test-bin/test_column_aware_steps"),
  TestSpec(source: "tests/test_function_in_a_file_with_no_steps.nim", binary: "build/test-bin/test_function_in_a_file_with_no_steps"),
  TestSpec(source: "tests/test_name_only_funcs_refusal.nim", binary: "build/test-bin/test_name_only_funcs_refusal"),
  TestSpec(source: "tests/test_ct_print_full.nim", binary: "build/test-bin/test_ct_print_full"),
  TestSpec(source: "tests/test_ct_print_native.nim", binary: "build/test-bin/test_ct_print_native"),
  TestSpec(source: "tests/test_managed_sender.nim", binary: "build/test-bin/test_managed_sender"),
  # Starts a MinIO and a Python stand-in of codetracer-ci's upload routes,
  # both from the dev shell (``flake.nix``).
  TestSpec(source: "tests/test_managed_sender_ci_routes.nim", binary: "build/test-bin/test_managed_sender_ci_routes"),
  TestSpec(source: "tests/test_reader_v4.nim", binary: "build/test-bin/test_reader_v4"),
  TestSpec(source: "tests/test_source_views.nim", binary: "build/test-bin/test_source_views"),
  # GDH-M1 — versioned paths: a path may be registered more than once and
  # each version gets its own correctly sized slot in the position space.
  # The FALSIFIER ARMS for these gates are not modelled here: they are
  # mutations that must FAIL, and a build graph whose nodes must fail is a
  # graph that cannot distinguish them from broken ones.
  # `tests/run_gdh1_gates.sh` runs them.
  TestSpec(source: "tests/test_gdh1_path_versions.nim", binary: "build/test-bin/test_gdh1_path_versions"),
  # GDH-M2 — the reload marker. Same rule as above: the falsifier arms are
  # mutations that must FAIL and are run by `tests/run_gdh2_gates.sh`, not
  # modelled here. The byte-identity gate is not modelled either — it needs a
  # build of the writer from a DIFFERENT revision, which is not a node this
  # graph can express.
  TestSpec(source: "tests/test_gdh2_reload_marker.nim", binary: "build/test-bin/test_gdh2_reload_marker"),
  # GDH-M6 review — the C header declares every `trace_writer_*` export, or
  # names it on a backlog. It only reads two files off disk:
  # `src/codetracer_trace_writer_ffi.nim` and `include/codetracer_trace_writer.h`.
  # Both are already `extraInputs` of the library edge, so its inputs are the
  # ones that should invalidate it.
  TestSpec(source: "tests/test_c_header_declares_the_writer_abi.nim", binary: "build/test-bin/test_c_header_declares_the_writer_abi"),
  # ---- Added 2026-10-18: the rest of the nimble `test` task ----------
  # Each entry carries its nimble line's flags. Debug-built (no
  # `-d:release` on the nimble line):
  TestSpec(source: "tests/test_nimcache_is_worktree_local.nim", binary: "build/test-bin/test_nimcache_is_worktree_local", debugOnly: true),
  TestSpec(source: "tests/test_root_auto_fill.nim", binary: "build/test-bin/test_root_auto_fill", debugOnly: true),
  TestSpec(source: "tests/test_namespace_images_refuse_malformed_input.nim", binary: "build/test-bin/test_namespace_images_refuse_malformed_input", debugOnly: true),
  TestSpec(source: "tests/test_cbor_top_level_type_id.nim", binary: "build/test-bin/test_cbor_top_level_type_id", debugOnly: true),
  TestSpec(source: "tests/test_follow_is_incremental.nim", binary: "build/test-bin/test_follow_is_incremental", debugOnly: true),
  TestSpec(source: "tests/test_spec_layout_always.nim", binary: "build/test-bin/test_spec_layout_always", debugOnly: true),
  # A failed final flush is reported (EFBIG under RLIMIT_FSIZE in a forked
  # child); relies on glibc stdio buffering, so Linux only, as in nimble.
  TestSpec(source: "tests/test_close_reports_final_flush_failure.nim", binary: "build/test-bin/test_close_reports_final_flush_failure", debugOnly: true, linuxOnly: true),
  # Release-built:
  TestSpec(source: "tests/test_paths_json_fallback.nim", binary: "build/test-bin/test_paths_json_fallback"),
  TestSpec(source: "tests/test_global_line_index_boundary.nim", binary: "build/test-bin/test_global_line_index_boundary"),
  TestSpec(source: "tests/test_line_count_table.nim", binary: "build/test-bin/test_line_count_table"),
  TestSpec(source: "tests/test_compact_profile_header_refusal.nim", binary: "build/test-bin/test_compact_profile_header_refusal"),
  TestSpec(source: "tests/test_function_declared_before_a_version.nim", binary: "build/test-bin/test_function_declared_before_a_version"),
  TestSpec(source: "tests/test_deep_call_nesting_flush.nim", binary: "build/test-bin/test_deep_call_nesting_flush"),
  TestSpec(source: "tests/test_linehits_reader.nim", binary: "build/test-bin/test_linehits_reader"),
  TestSpec(source: "tests/test_every_test_is_listed.nim", binary: "build/test-bin/test_every_test_is_listed"),
  # Re-runs the compiler over src/ for `--os:any` with `--compileOnly` and
  # its own temporary nimcache; it needs no cross toolchain.
  TestSpec(source: "tests/test_freestanding_writer_surface.nim", binary: "build/test-bin/test_freestanding_writer_surface"),
  # Drives `cargo test` in the sibling Rust repo, like the other cross-reads.
  TestSpec(source: "tests/test_nim_step_map_crossread.nim", binary: "build/test-bin/test_nim_step_map_crossread", pool: CrossReadPool),
  # Compile ct-print at runtime through `tests/ct_print_binary.nim` (see the
  # module doc) and import it from `tests/` (`-p:tests`).
  TestSpec(source: "tests/test_ct_print_agreement.nim", binary: "build/test-bin/test_ct_print_agreement", pool: CtPrintPool, extraPaths: @["tests"]),
  TestSpec(source: "tests/test_ct_print_full_large_unsigned.nim", binary: "build/test-bin/test_ct_print_full_large_unsigned", pool: CtPrintPool, extraPaths: @["tests"]),
  TestSpec(source: "tests/test_logical_step_count.nim", binary: "build/test-bin/test_logical_step_count", pool: CtPrintPool, mm: "arc", extraPaths: @["tests"]),
  TestSpec(source: "tests/test_retired_streams_are_refused.nim", binary: "build/test-bin/test_retired_streams_are_refused", pool: CtPrintPool, mm: "arc", extraPaths: @["tests"]),
  # The C-ABI tests: each `include`s `codetracer_trace_writer_ffi`, so each
  # compiles `--mm:arc` here and gets `--nimMainPrefix:codetracerTraceWriter`
  # from `tests/config.nims` (see `TestSpec`).
  TestSpec(source: "tests/test_older_versions_are_refused.nim", binary: "build/test-bin/test_older_versions_are_refused", mm: "arc"),
  TestSpec(source: "tests/test_line_only_orphan_carry_forward.nim", binary: "build/test-bin/test_line_only_orphan_carry_forward", mm: "arc"),
  TestSpec(source: "tests/test_orphan_call_args_step_location.nim", binary: "build/test-bin/test_orphan_call_args_step_location", mm: "arc"),
  TestSpec(source: "tests/test_pending_value_after_delta_column.nim", binary: "build/test-bin/test_pending_value_after_delta_column", mm: "arc"),
  TestSpec(source: "tests/test_reader_ffi.nim", binary: "build/test-bin/test_reader_ffi", mm: "arc"),
  TestSpec(source: "tests/test_reader_ffi_empty_names.nim", binary: "build/test-bin/test_reader_ffi_empty_names", mm: "arc"),
  TestSpec(source: "tests/test_ffi_fmt_2026_10.nim", binary: "build/test-bin/test_ffi_fmt_2026_10", mm: "arc"),
  TestSpec(source: "tests/test_io_event_pending_step_attribution.nim", binary: "build/test-bin/test_io_event_pending_step_attribution", mm: "arc"),
  TestSpec(source: "tests/test_ffi_linehits_and_correlation_index.nim", binary: "build/test-bin/test_ffi_linehits_and_correlation_index", mm: "arc"),
  TestSpec(source: "tests/test_reader_ffi_column_aware_paths.nim", binary: "build/test-bin/test_reader_ffi_column_aware_paths", mm: "arc"),
  TestSpec(source: "tests/test_reader_ffi_line_only_position_space.nim", binary: "build/test-bin/test_reader_ffi_line_only_position_space", mm: "arc"),
  TestSpec(source: "tests/test_ffi_line_count_table.nim", binary: "build/test-bin/test_ffi_line_count_table", mm: "arc"),
  TestSpec(source: "tests/test_ffi_column_table_decided_at_first_mention.nim", binary: "build/test-bin/test_ffi_column_table_decided_at_first_mention", mm: "arc"),
  TestSpec(source: "tests/test_ffi_register_path_and_varname.nim", binary: "build/test-bin/test_ffi_register_path_and_varname", mm: "arc"),
  TestSpec(source: "tests/test_ffi_trailing_values_attach_to_the_last_step.nim", binary: "build/test-bin/test_ffi_trailing_values_attach_to_the_last_step", mm: "arc"),
  TestSpec(source: "tests/test_ffi_exceptions_reach_the_container.nim", binary: "build/test-bin/test_ffi_exceptions_reach_the_container", mm: "arc"),
  TestSpec(source: "tests/test_ffi_source_reload_order.nim", binary: "build/test-bin/test_ffi_source_reload_order", mm: "arc"),
  TestSpec(source: "tests/test_ffi_call_entry_is_the_next_step.nim", binary: "build/test-bin/test_ffi_call_entry_is_the_next_step", mm: "arc"),
  TestSpec(source: "tests/test_ffi_refused_column_stays_with_its_step.nim", binary: "build/test-bin/test_ffi_refused_column_stays_with_its_step", mm: "arc"),
  TestSpec(source: "tests/test_block_size_is_one_of_three.nim", binary: "build/test-bin/test_block_size_is_one_of_three", mm: "arc"),
  TestSpec(source: "tests/test_meta_text_is_utf8.nim", binary: "build/test-bin/test_meta_text_is_utf8", mm: "arc"),
  TestSpec(source: "tests/test_ffi_assignment_refuses_unknown_kinds.nim", binary: "build/test-bin/test_ffi_assignment_refuses_unknown_kinds", mm: "arc"),
  # The fault-injection seam is compiled in for this test only. Its two
  # mutation controls are not modelled (see the corpus note above).
  TestSpec(source: "tests/test_ffi_failures_reach_the_caller.nim", binary: "build/test-bin/test_ffi_failures_reach_the_caller", mm: "arc", defines: @["ffiFaultInjection"]),
  TestSpec(source: "tests/test_ffi_in_memory.nim", binary: "build/test-bin/test_ffi_in_memory", mm: "arc"),
  TestSpec(source: "tests/test_ffi_recording_id.nim", binary: "build/test-bin/test_ffi_recording_id", mm: "arc"),
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

  devEnv:
    when not defined(windows):
      useFlakeDevShell()

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
    # ``test-builds`` + an EXECUTE edge collected into ``test``. Two
    # capacity-1 pools sequence the EXECUTE edges that share a mutable
    # resource: the cross-reads' ``cargo test`` runs against the sibling Rust
    # target dir, and the runtime ct-print build's cached binary and nimcache
    # (see module doc).
    let crossReadPool = buildPool(CrossReadPool, 1'u32)
    discard crossReadPool
    let ctPrintPool = buildPool(CtPrintPool, 1'u32)
    discard ctPrintPool

    # pcre lib dir for ``test_path_filter`` (see module doc). Threaded in
    # by the outer dev-shell nesting via ``CT_PCRE_LIB_DIR``; when unset
    # the edge links a bare ``-lpcre`` and relies on the C toolchain's
    # default search path.
    let pcreLibDir = getEnv("CT_PCRE_LIB_DIR")

    var testBuildActions: seq[BuildActionDef] = @[]
    var testExecuteActions: seq[BuildActionDef] = @[]

    for spec in testSpecs:
      # The nimble task runs a ``linuxOnly`` file under ``when
      # defined(linux)``; the edge follows the same host condition.
      if spec.linuxOnly and not defined(linux):
        continue
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

      # Compile ``-d:release`` EXCEPT for ``debugOnly`` files, which the
      # repo's own ``test`` corpus builds in debug (see ``debugOnly`` doc).
      var buildDefines = if spec.debugOnly: newSeq[string]() else: @["release"]
      # Per-file defines the nimble task also passes (see ``defines`` doc).
      for d in spec.defines:
        buildDefines.add(d)

      let edge = buildNimUnittest.build(
        source = spec.source,
        binary = spec.binary,
        defines = buildDefines,
        paths = @["src"] & spec.extraPaths,
        mm = spec.mm,
        extraPassL = extraPassL,
        # ``tests/config.nims`` decides the C-ABI tests' ``--nimMainPrefix``
        # (see ``TestSpec``), so it is an input of every test compile.
        extraInputs = @["src", "codetracer_trace_format.nimble", "nim.cfg",
                        "tests/config.nims"],
        actionId = "codetracer-trace-format-nim.test_build." & stem)
      testBuildActions.add(edge.action)

      let executeEdge = edge.testBinary.run(
        actionId = "codetracer-trace-format-nim.test_execute." & stem,
        pool = spec.pool,
        registerImplicitName = false)
      testExecuteActions.add(executeEdge)

    discard collect("test", testExecuteActions)
    discard collect("test-builds", testBuildActions)
