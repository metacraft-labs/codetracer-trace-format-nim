{.push raises: [].}

## CTFS (CodeTracer File System) container format — Nim implementation.
##
## This is the main entry point. Import this module to get access to the
## full CTFS API:
##
##   import codetracer_ctfs
##
## Submodules:
##   - base40: filename encoding/decoding
##   - types: type definitions, constants, low-level helpers
##   - block_mapping: multi-level chain mapping
##   - container: create/read/write/close operations
##   - streaming: streaming mode for concurrent readers
##   - chunk_index: inline chunk header encode/decode

import codetracer_ctfs/base40
import codetracer_ctfs/types
import codetracer_ctfs/block_mapping
import codetracer_ctfs/container
import codetracer_ctfs/streaming
import codetracer_ctfs/chunk_index
import codetracer_ctfs/fixed_record_table
import codetracer_ctfs/variable_record_table
import codetracer_ctfs/zstd_bindings
import codetracer_ctfs/seekable_zstd
import codetracer_ctfs/chunked_compressed_table
import codetracer_ctfs/namespace_descriptor
import codetracer_ctfs/sub_block_pool
import codetracer_ctfs/btree
import codetracer_ctfs/namespace
import codetracer_ctfs/shard_writer
import codetracer_ctfs/sharded_namespace
import codetracer_ctfs/space_analyzer

import codetracer_trace_types
import codetracer_trace_writer/varint
import codetracer_trace_writer/split_binary
import codetracer_trace_writer/interning_table
import codetracer_trace_writer/value_stream

export base40
export types
export block_mapping
export container
export streaming
export chunk_index
export fixed_record_table
export variable_record_table
export zstd_bindings
export seekable_zstd
export chunked_compressed_table
export namespace_descriptor
export sub_block_pool
export btree
export namespace
export shard_writer
export sharded_namespace
export space_analyzer

export codetracer_trace_types
export varint
export split_binary
export interning_table
export value_stream
