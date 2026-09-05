## The CTFS reader as a freestanding `wasm32-unknown-unknown` module, together
## with the writer that feeds it.
##
## `trace_writer_standalone.nim` proves the writer links with no WASI and no
## imports. The browser is read-heavy — it opens containers constantly and
## emits them rarely — so the load-bearing question is the other direction:
## does `new_trace_reader` link freestanding, and does it decode a container it
## is merely handed?
##
## This module answers both, and a third thing neither answers alone.
## `ct_selftest` writes a container and reads it back without leaving linear
## memory, which is the round trip a browser performs. `ct_input_alloc` /
## `ct_verify_input` cover the case that round trip cannot: a container the
## module did NOT write, handed to it as bytes by the host — a module that only
## ever reads back its own bytes has proven a loop, not a reader.
##
## `trace_reader_only_standalone.nim` is this module without the writer; read
## its header for why both exist.
##
## The query exports below exist so the adjudication is not "the module said
## zero". The host reads step counts, decoded positions, interned strings,
## source views, IO events, spans and line hits back out and compares them
## against expectations computed from the corpus definition, so a reader that
## returned a plausible wrong answer is caught.
##
## Build: see `wasm/build-trace-reader-standalone.sh`.

import results
import ../../src/codetracer_trace_writer/new_trace_reader
import ../../src/codetracer_trace_writer/span_stream
import ../../src/codetracer_trace_writer/linehits_reader
import ./trace_reader_corpus
import ./trace_reader_corpus_build

include ./trace_reader_abi

# ---------------------------------------------------------------------------
# Writing (so the round trip can happen entirely inside the module)
# ---------------------------------------------------------------------------

var built: seq[byte]
  ## The container this module writes, for `ct_selftest` and for a host that
  ## wants the wasm-written bytes.

proc ctBuild(): int32 {.exportc: "ct_build", cdecl.} =
  let r = buildCorpus()
  if r.isErr:
    built = @[]
    return 1
  built = r.get()
  0

proc ctPtr(): pointer {.exportc: "ct_ptr", cdecl.} =
  if built.len == 0: nil else: addr built[0]

proc ctLen(): int32 {.exportc: "ct_len", cdecl.} =
  int32(built.len)

proc ctSelftest(): int32 {.exportc: "ct_selftest", cdecl.} =
  ## Write a container and read it back, entirely inside the module.
  ##
  ## Runs `NimMain` itself so a wasm engine with no host glue at all can
  ## adjudicate it: `wasmtime run --invoke ct_selftest` prints the result.
  ## 0 means both containers were built and every field decoded as expected;
  ## anything else is the failing check from `verifyCorpus` /
  ## `verifyLegacyCorpus`, or 1 if a write failed.
  ##
  ## The legacy container is built here rather than only host-side because the
  ## legacy DECODERS are the ones with no other coverage on this target: they
  ## are selected by three meta.dat bits that this repo's writer never clears,
  ## so nothing else in the build path executes them.
  nimMain()
  if ctBuild() != 0: return 1
  let mainRc = verifyCorpus(built)
  if mainRc != 0: return mainRc
  let legacy = buildLegacyCorpus()
  if legacy.isErr: return 1
  verifyLegacyCorpus(legacy.get())
