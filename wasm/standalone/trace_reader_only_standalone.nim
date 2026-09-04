## The CTFS reader ALONE, as a freestanding `wasm32-unknown-unknown` module.
##
## `trace_reader_standalone.nim` links the writer too, because its selftest
## builds the container it reads. That is the round trip, but it is not the
## shape a read-heavy embedding ships: a browser that opens `.ct` containers
## and never records one should not carry a writer, a Zstd compressor or a
## container allocator it will not call.
##
## This module imports `new_trace_reader` and nothing that writes. Its size is
## therefore the honest answer to "what does reading cost", and its link is the
## honest answer to "does the READER surface stand on its own" — a module that
## also links a writer could be linking the writer's copy of a symbol.
##
## Build: see `wasm/build-trace-reader-standalone.sh`.

import results
import ../../src/codetracer_trace_writer/new_trace_reader
import ./trace_reader_corpus

include ./trace_reader_abi
