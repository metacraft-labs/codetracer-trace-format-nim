## codetracer_ct_print_lib: the `ct-print` JSON rendering, for tests.
##
## This module used to hold a SECOND implementation of that rendering, so the
## test corpus could call it in-process without compiling and shelling out to
## the binary. It drifted from the shipped CLI for three months — see
## `codetracer_trace_writer/full_document_json.nim`, which now holds the one
## implementation both entry points use.
##
## What remains here is the name the test corpus imports. It re-exports the
## shared module so the four test files that `import` or `include` it keep
## working unchanged, and so that a future change lands in one place rather
## than in whichever copy the author happened to be reading.
##
## Public surface (all from `full_document_json`):
##   - `valueRecordToJson(v)` — render a decoded ValueRecord as JsonNode.
##   - `decodeValueBytesToJson(bytes)` — decode CBOR + render to JsonNode.
##   - `FullOpts` and `buildFullDocument(reader, opts)` — the full
##     content-faithful dump used by `ct-print --full` and `--events`.
##   - `resolveGli` — the line-only global-position-index inverse. It REFUSES
##     an index the trace's address space cannot hold rather than answering
##     with a plausible `(path, line)`; the emitted event then carries
##     `position_error` instead of `path_id` / `line` / `path`.
##   - `addEventMetadata`, `isCorrelationMarker`, `precomputeStepGlis`,
##     `bytesToUtf8`, `bytesToHexLower`, `normalizePath`, `valuesForStep`,
##     `resolveStepLocation`.

import codetracer_trace_writer/full_document_json
export full_document_json
