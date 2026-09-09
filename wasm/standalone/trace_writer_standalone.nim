## The full TraceWriter as a freestanding `wasm32-unknown-unknown` module.
##
## `ctfs_standalone.nim` proves the CONTAINER layer builds and reads back with
## no WASI and no imports. This module does the same for the layer above it —
## `codetracer_trace_writer`, i.e. split-binary encoding, chunked seekable Zstd,
## `meta.dat` and `paths.dat` — which is the layer a recorder actually calls and
## the one a browser or a Rust `cdylib` host embeds.
##
## Two things in the writer had to change before this shape was reachable, and
## both are in the module graph below rather than in this file:
##
##   * `uuid_v7.nim` imported `std/times` unconditionally, for `epochTime()`
##     alone. `--os:any` has no POSIX `struct tm`, so `times.nim` does not
##     compile there. Under `-d:ctHostClock` the host supplies the millisecond
##     clock, the way `-d:ctLeanRecord` already lets it supply the entropy.
##   * `newTraceWriter` opened its container with `createCtfsStreaming(path)`,
##     so the module would have needed a filesystem it does not have.
##     `newTraceWriterInMemory` builds in a `seq[byte]` and `containerBytes`
##     hands it back after `close`.
##
## `trace_writer_host_stub.c` supplies `ct_host_unix_ms`, `getentropy`, `fclose`
## and `fflush` as definitions, so "zero imports" is a property of the module.
## Read its header before reusing it: the entropy source is deterministic.
##
## Build: see `wasm/build-trace-writer-standalone.sh`.

import results
import codetracer_trace_writer
import codetracer_trace_types
import codetracer_ctfs/container
import codetracer_ctfs/types

const
  SelftestRecordingId = "0192f8a0-1234-7abc-8def-0123456789ab"
    ## A caller-supplied `recordingId`, which is also what keeps the selftest
    ## clear of the deterministic UUIDv7 the host stub would otherwise mint.
  SelftestSteps = 8192
    ## Above `DefaultChunkThreshold` (4096), so the run seals at least one
    ## chunk and the seekable-Zstd path is exercised rather than skipped.

var built: seq[byte]

proc buildContainer(): int32 =
  ## Build a trace container in linear memory. Returns 0, or the failing step.
  let wr = newTraceWriterInMemory("trace_writer_standalone", @[],
                                  recordingId = SelftestRecordingId)
  if wr.isErr: return 1
  var w = wr.get()

  if w.writePath("/ct/standalone.nim").isErr: return 2

  for i in 0 ..< SelftestSteps:
    if w.writeStep(0'u64, int64(i)).isErr: return 3

  # The column-aware opcode, which is the whole reason this writer is of
  # interest to a caller that has column information to record.
  if w.writeStepWithColumn(0'u64, 1'i64, 7'i64).isErr: return 4

  if w.close().isErr: return 5
  built = w.containerBytes()
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

  # The four streams `newTraceWriterInMemory` + `close` are supposed to leave
  # behind. `events.log` carries the steps; an empty one would still be a
  # well-formed container, which is why the length is checked and not just the
  # presence.
  let ev = readInternalFile(built, "events.log")
  if ev.isErr: return 8
  if ev.get().len == 0: return 9

  if readInternalFile(built, "events.fmt").isErr: return 10
  if readInternalFile(built, "meta.json").isErr: return 11
  if readInternalFile(built, "paths.json").isErr: return 12

  0
