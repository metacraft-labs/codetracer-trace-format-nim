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
  exec "nim c -r tests/test_seekable_zstd.nim"
  exec "nim c -r tests/test_trace_types.nim"
  exec "nim c -r tests/test_varint.nim"
  exec "nim c -r tests/test_split_binary.nim"

task bench, "Run benchmarks":
  exec "nim c -d:release -r tests/bench_seekable_zstd.nim"
  exec "nim c -d:release -r tests/bench_split_binary.nim"

task benchSplitBinary, "Run split-binary benchmarks":
  exec "nim c -d:release -r tests/bench_split_binary.nim"
