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
  exec "nim c -r tests/test_base40.nim"
  exec "nim c -r tests/test_container.nim"
  exec "nim c -r tests/test_streaming.nim"
  exec "nim c -r tests/test_chunk_index.nim"
  exec "nim c -r tests/test_fixed_record_table.nim"
  exec "nim c -r tests/test_variable_record_table.nim"
  exec "nim c -r tests/test_seekable_zstd.nim"
  exec "nim c -r tests/test_chunked_compressed_table.nim"
  exec "nim c -r tests/test_trace_types.nim"
  exec "nim c -r tests/test_varint.nim"
  exec "nim c -r tests/test_split_binary.nim"
  exec "nim c -r tests/test_trace_writer.nim"
  exec "nim c -r tests/test_trace_reader.nim"
  exec "nim c -r tests/test_golden_fixtures.nim"
  exec "nim c -r tests/test_cross_compat.nim"
  exec "nim c -r tests/test_meta_dat.nim"
  exec "nim c -r tests/test_namespace_descriptor.nim"
  exec "nim c -d:release -r tests/test_sub_block_pool.nim"
  exec "nim c -d:release -r tests/test_btree.nim"
  exec "nim c -r -d:release -p:src tests/test_namespace.nim"
  exec "nim c -r -d:release -p:src tests/test_ct_space.nim"
  exec "nim c -r -d:release -p:src tests/test_shard_writer.nim"
  exec "nim c -r -p:src tests/test_step_encoding.nim"
  exec "nim c -r -p:src tests/test_interning_table.nim"
  exec "nim c -r -p:src tests/test_exec_stream.nim"
  exec "nim c -r -p:src tests/test_value_stream.nim"
  exec "nim c -r -p:src tests/test_call_stream.nim"
  exec "nim c -r -p:src tests/test_io_event_stream.nim"
  exec "nim c -r -d:release -p:src tests/test_multi_stream_integration.nim"
  exec "nim c -r -d:release -p:src tests/test_new_trace_reader.nim"
  exec "nim c -r -d:release -p:src tests/test_reader_calls_events.nim"
  exec "nim c -r -d:release -p:src tests/test_reader_integration.nim"
  exec "nim c -r -p:src tests/test_streaming_value_encoder.nim"
  exec "nim c -r -p:src tests/test_value_ref.nim"
  exec "nim c -r -d:release -p:src tests/test_multi_stream_writer.nim"
  exec "nim c -r -d:release -p:src tests/test_linehits_builder.nim"
  exec "nim c -r -p:src tests/test_partial_trace_cache.nim"
  exec "nim c -r -d:release -p:src tests/test_ram_cache.nim"
  exec "nim c -r -d:release -p:src tests/test_file_access.nim"

task regenerateFixtures, "Regenerate .expected golden fixture files":
  exec "nim c -r tests/generate_golden_fixtures.nim"

task bench, "Run benchmarks":
  exec "nim c -d:release -r tests/bench_seekable_zstd.nim"
  exec "nim c -d:release -r tests/bench_split_binary.nim"
  exec "nim c -d:release -r tests/test_chunked_compressed_table.nim"
  exec "nim c -d:release -r tests/bench_varint.nim"
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
  exec "nim c --app:staticlib --mm:arc --noMain -d:release -p:src -o:libcodetracer_trace_writer.a src/codetracer_trace_writer_ffi.nim"

task buildSharedLib, "Build shared library (C FFI)":
  exec "nim c --app:lib --mm:arc --noMain -d:release -p:src -o:libcodetracer_trace_writer.so src/codetracer_trace_writer_ffi.nim"

task testFfi, "Build and run C FFI test":
  exec "nim c --app:staticlib --mm:arc --noMain -d:release -p:src -o:libcodetracer_trace_writer.a src/codetracer_trace_writer_ffi.nim"
  exec "gcc -o tests/test_ffi tests/test_ffi.c -L. -lcodetracer_trace_writer -lzstd -lm -I include"
  exec "./tests/test_ffi"
