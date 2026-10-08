## The full TraceWriter as a freestanding `wasm32-unknown-unknown` module.
##
## `ctfs_standalone.nim` proves the CONTAINER layer builds and reads back with
## no WASI and no imports. This module does the same for the layer above it —
## the split-stream `MultiStreamTraceWriter`, i.e. the step, value, call and
## I/O event streams, `meta.dat` and `paths.dat` — which is the layer a recorder
## actually calls and the one a browser or a Rust `cdylib` host embeds.
##
## Two things in the writer had to change before this shape was reachable, and
## both are in the module graph below rather than in this file:
##
##   * `uuid_v7.nim` imported `std/times` unconditionally, for `epochTime()`
##     alone. `--os:any` has no POSIX `struct tm`, so `times.nim` does not
##     compile there. Under `-d:ctHostClock` the host supplies the millisecond
##     clock, the way `-d:ctLeanRecord` already lets it supply the entropy.
##   * A writer given a path opens its container with
##     `createCtfsStreaming(path)`, which needs a filesystem the module does not
##     have. `initMultiStreamWriter("")` builds in a `seq[byte]` and `toBytes`
##     hands it back after `close`.
##
## `trace_writer_host_stub.c` supplies `ct_host_unix_ms`, `getentropy`, `fclose`
## and `fflush` as definitions, so "zero imports" is a property of the module.
## Read its header before reusing it: the entropy source is deterministic.
##
## Build: see `wasm/build-trace-writer-standalone.sh`.

import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_ctfs/container
import codetracer_ctfs/types

const
  SelftestRecordingId = "0192f8a0-1234-7abc-8def-0123456789ab"
    ## A caller-supplied `recordingId`, which is also what keeps the selftest
    ## clear of the deterministic UUIDv7 the host stub would otherwise mint.
  SelftestSteps = 8192
    ## Above the step stream's 4096-record chunk, so the run seals at least
    ## one chunk and the compressed-chunk path is exercised rather than
    ## skipped.

var built: seq[byte]

proc buildContainer(): int32 =
  ## Build a trace container in linear memory. Returns 0, or the failing step.
  let wr = initMultiStreamWriter("", "trace_writer_standalone",
                                 recordingId = SelftestRecordingId)
  if wr.isErr: return 1
  var w = wr.get()

  let pathRes = w.registerPath("/ct/standalone.nim")
  if pathRes.isErr: return 2
  let pathId = pathRes.get()

  for i in 0 ..< SelftestSteps:
    if w.registerStep(pathId, uint64(i + 1), []).isErr: return 3

  if w.close().isErr: return 5
  built = w.toBytes()
  if w.closeCtfs().isErr: return 4
  0

proc ctBuild(): int32 {.exportc: "ct_build", cdecl.} =
  buildContainer()

proc ctPtr(): pointer {.exportc: "ct_ptr", cdecl.} =
  if built.len == 0: nil else: addr built[0]

proc ctLen(): int32 {.exportc: "ct_len", cdecl.} =
  int32(built.len)

proc nimMain() {.importc: "NimMain", cdecl.}

proc ctInit() {.exportc: "ct_init", cdecl.} =
  ## `--noMain` means the host must run module-level initialisation itself.
  nimMain()

proc ctSelftest(): int32 {.exportc: "ct_selftest", cdecl.} =
  ## Build and re-read the container, entirely inside the module.
  ##
  ## Exists so the freestanding build can be adjudicated by a wasm engine with
  ## no host glue at all — `wasmtime run --invoke ct_selftest` prints the return
  ## value. 0 means the container was built and every stream this writer is
  ## supposed to emit read back; anything else is the failing step.
  nimMain()
  let rc = buildContainer()
  if rc != 0: return 100 + rc
  if built.len == 0: return 6

  # CTFS magic, so the answer is about a real container and not an empty seq.
  if built[0] != CtfsMagic[0] or built[1] != CtfsMagic[1] or
     built[2] != CtfsMagic[2] or built[3] != CtfsMagic[3] or
     built[4] != CtfsMagic[4]: return 7

  # The members a split-stream writer leaves behind. `steps.dat` carries the
  # steps; an empty one would still be a well-formed container, which is why
  # its length is checked and not just its presence.
  let steps = readInternalFile(built, "steps.dat")
  if steps.isErr: return 8
  if steps.get().len == 0: return 9

  if readInternalFile(built, "steps.idx").isErr: return 10
  if readInternalFile(built, "meta.dat").isErr: return 11
  if readInternalFile(built, "paths.dat").isErr: return 12
  # No combined event stream: it is not part of the trace format.
  if readInternalFile(built, "events.log").isOk: return 13

  0
