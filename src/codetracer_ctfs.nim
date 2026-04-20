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
import codetracer_ctfs/zstd_bindings
import codetracer_ctfs/seekable_zstd

export base40
export types
export block_mapping
export container
export streaming
export chunk_index
export zstd_bindings
export seekable_zstd
