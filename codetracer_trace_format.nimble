# Package
version       = "0.1.0"
author        = "Metacraft Labs"
description   = "CTFS (CodeTracer File System) container format — Nim implementation"
license       = "MIT"
srcDir        = "src"

# Dependencies
requires "nim >= 2.2.0"
requires "stew >= 0.1.0"
requires "results"

task test, "Run all tests":
  exec "nim c -r tests/test_nimcache_is_worktree_local.nim"
  exec "nim c -r tests/test_base40.nim"
  exec "nim c -r tests/test_container.nim"
  # Container version 5: a member's MapBlock is 0 (empty), its only data
  # block tagged with bit 63, or a mapping block; readers refuse every
  # other container version, naming it.
  exec "nim c -r -p:src tests/test_ctfs_v5_member_forms.nim"
  # M61/M61b integrity hardening: the write-side null-mapping guards and the
  # duplicate-name rejection ported from the native-recorder fork.
  exec "nim c -r tests/test_ctfs_append_null_data_block.nim"
  exec "nim c -r tests/test_ctfs_duplicate_name.nim"
  # M38b: appending internal files to an already-closed container. Uses a
  # 4.5 MB internal file, so -d:release keeps it quick.
  exec "nim c -r -d:release tests/test_container_append.nim"
  # M57: the append's write ORDERING (tail first, block 0 last), which is why
  # an interrupted append leaves the old valid container. Needs the
  # fault-injection seam, which is compiled out of every other build.
  exec "nim c -r -d:release -d:ctfsAppendFaultInjection tests/test_container_append_ordering.nim"
  # M58: §5d's reader rule has a bound attached to it — accepting a partial
  # tail is only safe while every block number the reader resolves (mapping
  # root, mapping block, and DATA block) is checked against the container's
  # whole blocks. -d:release because one fixture is a 2.4 MB two-level file.
  exec "nim c -r -d:release tests/test_partial_tail_bounds.nim"
  # M61b: the WRITE side of the null-data-block defect. A block number of 0 is
  # block 0 — the header and root directory — so an unresolved mapping that
  # `writeToFile` does not refuse overwrites the container rather than one
  # stream. -d:release because one fixture is a 512-block two-level file.
  exec "nim c -r -d:release tests/test_write_null_data_block.nim"
  exec "nim c -r tests/test_streaming.nim"
  # Durability: a recording read while its writer is still open (what a
  # killed process leaves) has meta.dat, every sealed chunk and every interning
  # record registered before the seal.
  exec "nim c -r -d:release -p:src tests/test_durability_publishes_sealed_chunks.nim"
  exec "nim c -r tests/test_root_directory_overflow.nim"
  exec "nim c -r tests/test_root_directory_grows.nim"
  exec "nim c -r tests/test_chunk_index.nim"
  exec "nim c -r tests/test_fixed_record_table.nim"
  exec "nim c -r tests/test_variable_record_table.nim"
  exec "nim c -r -p:src tests/test_member_view.nim"
  exec "nim c -r -p:src tests/test_member_names_are_encodable.nim"
  exec "nim c -r tests/test_seekable_zstd.nim"
  exec "nim c -r tests/test_chunked_compressed_table.nim"
  exec "nim c -r tests/test_trace_types.nim"
  exec "nim c -r tests/test_varint.nim"
  exec "nim c -r tests/test_split_binary.nim"
  exec "nim c -r tests/test_trace_reader.nim"
  # The same §5d bound in the *other* Nim transcription of the §4 walk:
  # `codetracer_trace_reader.nim`'s own `readInternalFile`. It bounded byte
  # offsets only, and read a null mapping root as a walk through block 0 —
  # the header and root directory — so a damaged trace opened clean or
  # crashed on an overflowing block number.
  exec "nim c -r tests/test_trace_reader_null_mapping_root.nim"
  exec "nim c -r tests/test_golden_fixtures.nim"
  exec "nim c -r tests/test_cross_compat.nim"
  exec "nim c -r tests/test_xxh64.nim"
  # -d:release: the scale ladder builds a 100k-key index; a debug build
  # turns that measurement into a multi-minute wait for no extra coverage.
  exec "nim c -r -d:release tests/test_corrmark_builder.nim"
  exec "nim c -r tests/test_correlation_marker_api.nim"
  exec "nim c -r tests/test_close_publishes_entry_sizes.nim"
  exec "nim c -r tests/test_meta_dat.nim"
  # The writer writes the MCR, replay-launch and layout blocks it is given.
  exec "nim c -r tests/test_writer_meta_blocks.nim"
  exec "nim c -r tests/test_namespace_descriptor.nim"
  exec "nim c -d:release -r tests/test_sub_block_pool.nim"
  exec "nim c -d:release -r tests/test_btree.nim"
  exec "nim c -r -d:release -p:src tests/test_cow_btree.nim"
  exec "nim c -r -d:release -p:src tests/test_bulk_load_cow_btree.nim"
  exec "nim c -r -p:src tests/test_namespace_images_refuse_malformed_input.nim"
  exec "nim c -r -d:release -p:src tests/test_namespace.nim"
  exec "nim c -r -d:release -p:src tests/test_ct_space.nim"
  exec "nim c -r -d:release -p:src tests/test_shard_writer.nim"
  exec "nim c -r -p:src tests/test_step_encoding.nim"
  exec "nim c -r -p:src tests/test_interning_table.nim"
  exec "nim c -r -p:src tests/test_qualified_interning.nim"
  exec "nim c -r -p:src tests/test_exec_stream.nim"
  # The normative AbsoluteStep/DeltaStep rule and the reader's refusal of a
  # delta before a chunk's first AbsoluteStep.
  exec "nim c -r -d:release -p:src tests/test_step_encoding_rule.nim"
  exec "nim c -r -p:src tests/test_value_stream.nim"
  # The value reader's `typeId` is read off the CBOR without decoding it; it
  # must answer what a full decode does, for every kind.
  exec "nim c -r -p:src tests/test_cbor_top_level_type_id.nim"
  exec "nim c -r -p:src tests/test_call_stream.nim"
  exec "nim c -r -p:src tests/test_io_event_stream.nim"
  # RS-M1: spans.dat / spans.idx / spantype.ns writer + reader, and the
  # meta.dat bit 13 (FlagHasSpanStream) that gates them.
  exec "nim c -r -p:src tests/test_span_stream.nim"
  exec "nim c -r -d:release -p:src tests/test_multi_stream_integration.nim"
  exec "nim c -r -d:release -p:src tests/test_new_trace_reader.nim"
  # The reader's `paths.json` fallback and the `["a","b"]` decoder underneath
  # it, which replaced `std/json` there: `parseJson` reaches `parseFloat` and so
  # libc's `strtod`, which no freestanding target defines, and the reader could
  # not LINK for wasm32 over a float no container contains.
  exec "nim c -r -d:release -p:src tests/test_paths_json_fallback.nim"
  # meta.dat bit 4 is the sole authority on the paths.dat record layout: the
  # line-only and Layout A record spaces overlap (a 97-byte ASCII path decodes
  # as a complete Layout A record), so a reader that infers the layout from
  # the bytes answers a line-only trace with a truncated path, a fabricated
  # per-file line table and the wrong step line — with no error.
  exec "nim c -r -d:release -p:src tests/test_paths_dat_layout_authority.nim"
  # A column-aware file's table is decided by the writer at the file's first
  # mention: all-zero tables get one position, a missing table the
  # conventional one, whose columns are clamped and lines bounded.
  exec "nim c -r -d:release -p:src tests/test_column_table_decided_at_first_mention.nim"
  # A line-only global_position_index says nothing about how its integers were
  # apportioned between files, and the two writers of this container format
  # disagree — prefixSum[path_id] + (line - 1) here, (path_id shl 32) or line
  # in the Rust codetracer_trace_writer. Inverting one is an assumption, so
  # it has to be a falsifiable one: an address outside the space is refused
  # by name rather than clamped into a file that exists.
  exec "nim c -r -d:release -p:src tests/test_line_only_position_space.nim"
  # The file boundary in that space. An address is prefixSum[file_id] +
  # (line - 1), so a file's slot holds exactly the lines it has. Encoding
  # + line instead leaves each base unused and pushes a file's last line
  # into the next file's range — invisible behind an oversized stride,
  # a wrong answer at every boundary once slots are sized to real counts.
  exec "nim c -r -d:release -p:src tests/test_global_line_index_boundary.nim"
  # Registering a path extends that space by one base; it must not rebuild
  # every base. Asserted as growth (N vs 4N paths), not as a time.
  exec "nim c -r -d:release -p:src tests/test_path_registration_scales_linearly.nim"
  # A host resolving steps one call at a time (the C ABI's
  # `ct_reader_step_location`) pays about one sequential decode, not one chunk
  # decode and one position-space rebuild per step.
  exec "nim c -r -d:release -p:src tests/test_per_step_location_cost.nim"
  # The per-file line-count table (meta.dat bit 14): a line-only container
  # that STATES how large each of its files is instead of leaving a reader
  # to assume DefaultLinesPerFile. With the sizes recorded, a step past a
  # file's count addresses the next file and nothing downstream can tell
  # that apart from a real location — so the writer refuses it.
  exec "nim c -r -d:release -p:src tests/test_line_count_table.nim"
  # GDH-M1 — a path may be registered more than once (`registerPathVersion`),
  # each version gets its own correctly sized slot, and v1's addresses do not
  # move. The named falsifier arms for these four gates live behind
  # `-d:gdh1FalsifierArms` and are run by `tests/run_gdh1_gates.sh`, which
  # requires each one to turn its own gate red; they are deliberately not in
  # this corpus, whose members must all pass.
  exec "nim c -r -d:release -p:src tests/test_gdh1_path_versions.nim"
  # GDH-M2 — the reload boundary is discoverable in the container: step-stream
  # tag 0x08 (`TagSourceReload`) round-trips with its ordinal, its
  # (old_path_id, new_path_id, generation) triples and its in-flight count, and
  # a container carrying the tag WITHOUT declaring it is refused by name rather
  # than decoded into a shorter, plausible step stream. Like GDH-M1's, the
  # named falsifier arms live behind `-d:gdh2FalsifierArms` and are run by
  # `tests/run_gdh2_gates.sh`; that script also owns the byte-identity gate,
  # which needs a SECOND build of the writer (from the pinned pre-campaign
  # revision) and so cannot be a corpus member.
  exec "nim c -r -d:release -p:src tests/test_gdh2_reload_marker.nim"
  # Every reader door refuses meta.dat versions other than 6 and containers
  # other than version 5, naming both; the current versions open at the
  # recorded lines. `include`s codetracer_trace_writer_ffi to drive
  # ct_reader_open, so it needs --mm:arc and the FFI's --nimMainPrefix.
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_older_versions_are_refused.nim"
  # The same rule in the other direction, for the version AFTER 5: the doors
  # that read version 5 only (the legacy reader, the native-bundle detector)
  # refuse a container declaring the version-6 header by name, and the two
  # fields' parsers (a profile and a whole-file compression scheme, both closed
  # sets -- ctfs-container.md §1a, §1b) refuse an unknown or absent value
  # rather than reading it as the permissive default. Carries its control: the same reader
  # opens a FULL container of the same recording and succeeds.
  exec "nim c -r -d:release -p:src tests/test_compact_profile_header_refusal.nim"
  # The compact profile's BODY (ctfs-container.md §1d): the reference encoder
  # and decoder round-trip every member of a real recording BYTE-EXACTLY and
  # name the same members the full profile does; a single flipped bit in the
  # directory is detected rather than silently answered with a short or
  # shifted member; and the layout is asserted against the bytes to carry no
  # mapping block and no padding, with the same assertion run against a FULL
  # container of the same recording and required to FAIL. Also prints CCP-2's
  # deliverable-4 overhead figures against a VERSION-5 baseline.
  exec "nim c -r -d:release -p:src tests/test_compact_container_layout.nim"
  # The writer chooses the profile from a measured RAW-BYTE threshold
  # (ctfs-container.md §1e), over the split-stream writer: it records the full
  # profile and converts at close, every frame inflated (§1f). The quantity is
  # the compact container's member bytes; the boundary is asserted from both
  # sides against the same recording written full, with a one-step-shorter
  # recording shown to answer differently; a stored-size rule is shown to
  # choose differently; and every chunk of the compact container is the full
  # container's frame inflated, with the full container's frames as control.
  exec "nim c -r -d:release -p:src tests/test_profile_threshold_choice.nim"
  # NewTraceReader reads version 6 in both profiles and under whole-file zstd:
  # one recording in five forms answers every query alike, a compact container
  # carrying frames does not (the control), and the version-6 header's values
  # are refused by name.
  exec "nim c -r -d:release -p:src tests/test_version_6_containers_read.nim"
  # The column-aware step encoding end to end: the writer's opt-in, the
  # DeltaColumn round-trip, Layout A paths.dat, the position decoder, and the
  # meta.dat unknown-flag-bit rejection that keeps the extension clean.
  exec "nim c -r -d:release -p:src tests/test_column_aware_steps.nim"
  # A function declared in a file no step visited is written at close, with its
  # path in the space its address is computed in (and in meta.dat's list).
  exec "nim c -r -d:release -p:src tests/test_function_in_a_file_with_no_steps.nim"
  exec "nim c -r -d:release -p:src tests/test_function_declared_before_a_version.nim"
  # A funcs.dat/types.dat record that is a bare name (the pre-b891a0f writer's
  # shape) is refused with a message that says so.
  exec "nim c -r -d:release -p:src tests/test_name_only_funcs_refusal.nim"
  # A column-aware trace may table some of its files and not others, and the
  # two kinds of file take different amounts of the position space. Writer
  # and reader size them by one rule; sizing an untabled file 0 in the reader
  # put every later file's base too low and answered out of the file before.
  exec "nim c -r -d:release -p:src tests/test_mixed_column_aware_position_space.nim"
  exec "nim c -r -d:release -p:src tests/test_reader_calls_events.nim"
  exec "nim c -r -d:release -p:src tests/test_reader_integration.nim"
  # M24a-1: cross-read proof — a Nim-written production steps.dat is read by
  # the canonical Rust StepStreamReader (skips cleanly if the sibling Rust repo
  # or its toolchain is absent).
  exec "nim c -r -d:release -p:src tests/test_nim_step_stream_crossread.nim"
  # M24a-2: cross-read proof — a Nim-written production values.dat is read by
  # the canonical Rust ValueStreamReader (skips cleanly if the sibling Rust repo
  # or its toolchain is absent).
  exec "nim c -r -d:release -p:src tests/test_nim_value_stream_crossread.nim"
  # M24a-3: cross-read proof — a Nim-written production events.dat is read by
  # the canonical Rust IoEventStreamReader (skips cleanly if the sibling Rust
  # repo or its toolchain is absent).
  exec "nim c -r -d:release -p:src tests/test_nim_io_event_stream_crossread.nim"
  # The Nim and Rust step-map readers answer every lookup alike, a lookup of
  # line 0 (answered as line 1) included.
  exec "nim c -r -d:release -p:src tests/test_nim_step_map_crossread.nim"
  exec "nim c -r -p:src tests/test_streaming_value_encoder.nim"
  exec "nim c -r -p:src tests/test_value_ref.nim"
  exec "nim c -r -d:release -p:src tests/test_multi_stream_writer.nim"
  # A recursion's returns reach the call buffer innermost first; flushing them
  # in call_key order must not cost a pass over the buffer per record.
  exec "nim c -r -d:release -p:src tests/test_deep_call_nesting_flush.nim"
  # MT7-5a: the exported, ABI-stable, per-thread crossing block that mirrors the
  # writer's `pendingCrossings` seq (read back via the C symbols the reader uses).
  exec "nim c -r -d:release -p:src tests/test_crossing_state.nim"
  exec "nim c -r -d:release -p:src tests/test_multi_stream_attach.nim"
  exec "nim c -r -d:release -p:src tests/test_linehits_builder.nim"
  # The READ side of `linehits.tc`. The builder's own lookups answer from the
  # Table it filled while recording and never touch the serialised B-tree, so a
  # consumer that did not write the trace needs its own coverage.
  exec "nim c -r -d:release -p:src tests/test_linehits_reader.nim"
  exec "nim c -r -d:release -p:src tests/test_memwrites_builder.nim"
  exec "nim c -r -d:release -p:src tests/test_step_map_builder.nim"
  # step-map.ns version 2: the specified bytes, 64 KiB chunking, line 0 keyed
  # as line 1, and the reader's refusals.
  exec "nim c -r -d:release -p:src tests/test_step_map_v2.nim"
  exec "nim c -r -p:src tests/test_partial_trace_cache.nim"
  exec "nim c -r -d:release -p:src tests/test_ram_cache.nim"
  exec "nim c -r -d:release -p:src tests/test_file_access.nim"
  exec "nim c -r -p:src tests/test_split_trace.nim"
  exec "nim c -r -p:src tests/test_trace_storage_config.nim"
  exec "nim c -r -p:src tests/test_path_filter.nim"
  # `events.log` / `events.fmt` are not part of the trace format: every
  # reader (library, legacy TraceReader, C ABI, ct-print) refuses a container
  # carrying either, by name, and the C ABI refuses the formats that wrote one.
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src -p:tests tests/test_retired_streams_are_refused.nim"
  # A refresh follows a container being written incrementally (`ctfs-container.md` §6).
  exec "nim c -r -p:src tests/test_follow_is_incremental.nim"
  # Every container is read in the spec stream layout whatever its presence
  # bits say; one in the retired record-table layout is refused by name.
  exec "nim c -r -p:src tests/test_spec_layout_always.nim"
  # ct-print reports a step position it cannot resolve instead of emitting
  # the plausible wrong (path, line) the unchecked inverse produces.
  exec "nim c -r -d:release -p:src tests/test_ct_print_unresolvable_position.nim"
  # Line-only orphan pending-value carry-forward (92fce3a regression).
  # `include`s codetracer_trace_writer_ffi, so it needs --mm:arc and the
  # --nimMainPrefix the FFI's NimMain importc expects (see buildStaticLib).
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_line_only_orphan_carry_forward.nim"
  # A call's staged arguments must surface at the callee's definition line,
  # not at a position inherited from a different logical unit.
  # Same FFI-`include` compile requirements as the test above.
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_orphan_call_args_step_location.nim"
  # A variable registered after a column nudge must land on the step that
  # nudge belongs to, and a column offered for a file with no per-line table
  # must not move the step to another line. This file existed for months
  # without being listed here, so it went on describing a pipeline that had
  # moved on and nothing contradicted it.
  # Same FFI-`include` compile requirements as the tests above.
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_pending_value_after_delta_column.nim"
  # `ct-print --full` has ONE implementation, and its call_exit order obeys
  # the format's rule. The shipped binary and the in-process builder the test
  # corpus links were two near-copies that drifted for three months; this
  # compares them on a container whose calls share an exit step, which is the
  # only shape where the ordering is decided by the assembler rather than by
  # the step index.
  exec "nim c -r -d:release -p:src -p:tests tests/test_ct_print_agreement.nim"
  # The reader C FFI exports. Like the two tests above it `include`s the FFI
  # module, so it needs --mm:arc and the --nimMainPrefix. It had never been
  # listed here either: its fixture declared a container it had not written,
  # every value lookup was refused, and the refusal was turned into an empty
  # string that the assertions read as "no values".
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_reader_ffi.nim"
  # An empty type, variable or function name is a name: the reader's C ABI
  # returns it as a non-nil zero-length buffer, nil only on failure.
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_reader_ffi_empty_names.nim"
  # The 2026-10 revision through the C ABI: every value tag 0-9, every
  # EventLogKind exactly, record framing, meta.dat v6 written once at the
  # first record and the declared source-reload capability.
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_ffi_fmt_2026_10.nim"
  # Every test file is reachable from this task. Two were not, for months
  # each, and both described behaviour that had moved on without them.
  exec "nim c -r -d:release -p:src tests/test_every_test_is_listed.nim"
  # #601: an I/O event must be attributed to the step of the line that wrote
  # it. The FFI buffers one step, so `stepCount - 1` named the PREVIOUS step
  # and the flow view rendered program output one source line too high.
  # Same FFI-`include` compile requirements as the two tests above.
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_io_event_pending_step_attribution.nim"
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_ffi_linehits_and_correlation_index.nim"
  # The C ABI's view of the paths.dat layout question: meta.dat bit 4 decides,
  # `ct_reader_column_aware_paths_suspected` reports a record set that also
  # decodes as Layout A, and `ct_reader_open_assume_column_aware_paths` is the
  # caller's opt-in recovery — which refuses by name rather than falling back.
  # Same FFI-`include` compile requirements as the tests above.
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_reader_ffi_column_aware_paths.nim"
  # The C ABI's step-location accessors — what codetracer's db-backend turns
  # into DAP stackTrace frames — refuse a line-only position their address
  # space cannot address instead of clamping it into a file that exists, and
  # still answer every position this repository's writer produces.
  # Same FFI-`include` compile requirements as the tests above.
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_reader_ffi_line_only_position_space.nim"
  # The C ABI's door to the per-file line-count table. Every non-Nim recorder
  # drives this writer through the C entry points, so a mandatory-count
  # contract that only the Nim API enforces is not enforced at all: the
  # implicit path registration `trace_writer_register_step` performs has no
  # count, and must be refused by name rather than silently dropping the step.
  # Same FFI-`include` compile requirements as the tests above.
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_ffi_line_count_table.nim"
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_ffi_column_table_decided_at_first_mention.nim"
  # A path or variable name registered through the C ABI is interned when it
  # is registered, as the native API interns it, not when a record first
  # refers to it. Same FFI-`include` compile requirements as the tests above.
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_ffi_register_path_and_varname.nim"
  # Values staged when a recording ends go to the last STEP's value record,
  # not to a thread-switch record after it, even across a value-chunk boundary.
  # Same FFI-`include` compile requirements as the tests above.
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_ffi_trailing_values_attach_to_the_last_step.nim"
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_ffi_exceptions_reach_the_container.nim"
  # A reload marker written through the C ABI follows the step registered
  # before it (the pending step is flushed first). Same FFI-`include` compile
  # requirements as the tests above.
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_ffi_source_reload_order.nim"
  # A call registered through the C ABI begins at the next step, never at the
  # step pending when it arrived.
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_ffi_call_entry_is_the_next_step.nim"
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_ffi_refused_column_stays_with_its_step.nim"
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_block_size_is_one_of_three.nim"
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_meta_text_is_utf8.nim"
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_ffi_assignment_refuses_unknown_kinds.nim"
  # Every class of C ABI entry point reports its failure to the caller, and
  # the guard that does it is on every exported proc. Then the two mutation
  # builds, each removing one half of the guard: the test must FAIL under both,
  # or it is not measuring the guard.
  exec "nim c -r -d:release -d:ffiFaultInjection --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_ffi_failures_reach_the_caller.nim"
  for mutation in ["ffiGuardNoCatch", "ffiGuardNoLatch"]:
    let (mutOut, mutCode) = gorgeEx("nim c -r -d:release -d:ffiFaultInjection -d:" &
      mutation & " --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src " &
      "-o:tests/test_ffi_failures_mutant_" & mutation &
      " tests/test_ffi_failures_reach_the_caller.nim")
    if mutCode == 0 or "ALL PASS" in mutOut:
      echo mutOut
      raise newException(AssertionDefect, "mutation " & mutation &
        " left test_ffi_failures_reach_the_caller GREEN: the test does not " &
        "detect the loss of the guard it exists for")
    echo "PASS: mutation " & mutation & " turns test_ffi_failures_reach_the_caller red"
  # The C ABI's in-memory constructors: an embedder with no filesystem gets a
  # container's BYTES rather than a file. Carries its own positive control (the
  # file arm, in the same directory) and its own mutation control (one extra
  # step), which is what showed that a container's LENGTH cannot tell two
  # traces apart. Same FFI-`include` compile requirements as the tests above.
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_ffi_in_memory.nim"
  # The C ABI lets a caller PIN the recording identity instead of having one
  # minted. Carries its own control — a writer that does not call the setter
  # gets a different, valid id — without which the positive assertion cannot
  # tell a working setter from a no-op. Same FFI-`include` compile requirements
  # as the tests above.
  exec "nim c -r -d:release --mm:arc --nimMainPrefix:codetracerTraceWriter -p:src tests/test_ffi_recording_id.nim"
  # The C ABI builds for a target with no filesystem, and `ctHasFilesystem`
  # removes EXACTLY the two entry points that name a file. Re-runs the compiler
  # over src/ with `--os:any --cpu:wasm32 --compileOnly` and reads the emitted
  # C, so it needs no cross toolchain.
  exec "nim c -r -d:release -p:src tests/test_freestanding_writer_surface.nim"
  # Every `trace_writer_*` entry point the FFI exports is DECLARED in the C
  # header hosts vendor, or is on a named backlog. "Exported but undeclared"
  # breaks no build and fails no test — it just silently removes a capability
  # from every caller — and it has now landed three times (GDH-M3 twice,
  # GDH-M6's `trace_writer_set_recording_id` once, the last of which made
  # byte-identical GDScript recordings impossible for as long as nobody
  # looked). Reads the real FFI source and the real header; no toolchain.
  exec "nim c -r -d:release -p:src tests/test_c_header_declares_the_writer_abi.nim"

task regenerateFixtures, "Regenerate .expected golden fixture files":
  exec "nim c -r tests/generate_golden_fixtures.nim"

task bench, "Run benchmarks":
  exec "nim c -d:release -r tests/bench_seekable_zstd.nim"
  exec "nim c -d:release -r tests/bench_split_binary.nim"
  # M34b: the two ChunkedCompressedTable microbenchmarks moved here out of
  # tests/test_chunked_compressed_table.nim (thresholds carried over verbatim).
  # Both measure the host — see the header of tests/bench_chunked_table.nim.
  exec "nim c -d:release -r tests/bench_chunked_table.nim"
  exec "nim c -d:release -r tests/bench_varint.nim"
  exec "nim c -d:release -r -p:src tests/bench_streaming_writer.nim"
  exec "nim c -d:release -r -p:src tests/test_exec_stream.nim"

task benchSuite, "Run unified benchmark regression suite":
  exec "nim c -d:release -r -p:src tests/bench_regression_suite.nim"

task benchSplitBinary, "Run split-binary benchmarks":
  exec "nim c -d:release -r tests/bench_split_binary.nim"

task buildCtPrint, "Build ct-print utility":
  exec "nim c -d:release --mm:arc -p:src -o:ct-print src/codetracer_ct_print.nim"

task buildCtSpace, "Build ct-space utility":
  exec "nim c -d:release --mm:arc -p:src -o:ct-space src/codetracer_ct_space.nim"

task testReader, "Run trace reader tests":
  exec "nim c -r -p:src tests/test_trace_reader.nim"

task buildStaticLib, "Build static library (C FFI)":
  # The flags live in build_ffi.nims, the one build every producer of the
  # archive runs (this task, the flake's trace-writer-ffi package, and the
  # Rust `codetracer_trace_writer_nim` crate's build.rs).
  exec "nim e --hints:off build_ffi.nims"

task buildSharedLib, "Build shared library (C FFI)":
  exec "nim e --hints:off build_ffi.nims --app:lib"

task testFfiThreads, "C hosts: close after the recording thread exited; concurrent writers":
  # The host library is built --threads:off with a process lock around every
  # entry point (src/codetracer_trace_writer_ffi_runtime.c). Two hosts, each
  # with a control that proves it still reaches the defect it guards:
  #
  #  * test_ffi_worker_thread_exit.c must PASS against the shipped flags and
  #    CRASH against an archive built --threads:on (per-thread heaps);
  #  * test_ffi_concurrent_writers.c must PASS against the shipped flags and
  #    FAIL against one built -d:ffiNoProcessLock (one heap, no lock).
  when hostOS == "windows":
    echo "SKIP: testFfiThreads uses pthreads and mmap"
  else:
    let dir = "build/ffi-threads"
    mkDir(dir)
    # The shipped archive is build_ffi.nims's; the two controls are the same
    # build with one override each, passed after `--` so it wins.
    let build = "nim e --hints:off build_ffi.nims"
    exec build & " --nimcache:" & dir & "/nc-ship --out:" & dir & "/lib-ship.a -- --hints:off"
    exec build & " --nimcache:" & dir & "/nc-tls --out:" & dir & "/lib-tls.a -- --hints:off --threads:on --warnings:off"
    exec build & " --nimcache:" & dir & "/nc-nolock --out:" & dir & "/lib-nolock.a -- --hints:off --warnings:off -d:ffiNoProcessLock"
    let extra = when hostOS == "macosx": " -framework Security -framework CoreFoundation" else: ""
    for t in ["worker_thread_exit", "concurrent_writers"]:
      for v in ["ship", "tls", "nolock"]:
        exec "gcc -O1 -o " & dir & "/" & t & "-" & v & " tests/test_ffi_" & t & ".c " & dir & "/lib-" & v & ".a -lzstd -lm -lpthread -I include" & extra
    exec dir & "/worker_thread_exit-ship"
    exec dir & "/concurrent_writers-ship"
    let (_, tlsCode) = gorgeEx(dir & "/worker_thread_exit-tls")
    if tlsCode == 0:
      raise newException(AssertionDefect, "the worker-thread host PASSED against a --threads:on " &
        "archive: the test no longer reaches the cross-thread free it guards")
    var lockCaught = false
    for attempt in 0 ..< 5:
      let (_, code) = gorgeEx(dir & "/concurrent_writers-nolock")
      if code != 0:
        lockCaught = true
        break
    if not lockCaught:
      raise newException(AssertionDefect, "the concurrent-writers host PASSED 5 times against an " &
        "archive without the process lock: it no longer exercises concurrent entry")
    echo "PASS: --threads:on crashes the worker-exit host (exit " & $tlsCode &
      "); no lock fails the concurrent host; the shipped flags pass both"

task testFfi, "Build and run C FFI test":
  # The archive every producer builds (see build_ffi.nims); test_ffi.c checks
  # it reports that build's configuration.
  exec "nim e --hints:off build_ffi.nims"
  # MT1: the three replay-observation chokepoints must be exported symbols so a
  # replay-time observer (MCR) can interpose on them — guard it explicitly.
  exec "bash tests/check_chokepoint_symbols.sh libcodetracer_trace_writer.a"
  # macOS: the static lib pulls in SecRandomCopyBytes (Nim's std randomness),
  # which lives in the Security framework; CoreFoundation is its transitive dep.
  # On Linux these are not needed. Without them the link fails with an
  # "Undefined symbols: _SecRandomCopyBytes" error.
  when hostOS == "macosx":
    exec "gcc -o tests/test_ffi tests/test_ffi.c ./libcodetracer_trace_writer.a -lzstd -lm -framework Security -framework CoreFoundation -I include"
  else:
    exec "gcc -o tests/test_ffi tests/test_ffi.c ./libcodetracer_trace_writer.a -lzstd -lm -I include"
  exec "./tests/test_ffi"
