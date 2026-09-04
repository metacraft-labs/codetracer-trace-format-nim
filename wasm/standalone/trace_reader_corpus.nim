## One corpus, one verifier, compiled for both a freestanding target and the
## host — so "the reader decoded it" means the same thing on both.
##
## `trace_reader_standalone.nim` builds this for `wasm32-unknown-unknown` and
## `trace_reader_native.nim` builds it for the host.  The corpus is WRITTEN by
## `trace_reader_corpus_build.nim`, which is a separate module so this one
## pulls in no writer. The container the native
## build emits is fed byte-for-byte to the wasm module by
## `trace_reader_host.mjs`, which is why the corpus must be deterministic:
## `CorpusRecordingId` is supplied rather than minted, so no clock and no
## entropy source enters the bytes.
##
## The verifier asserts DECODED CONTENT, not that a handle was obtained.
## Every expectation below is recomputed from `CorpusPaths` / `CorpusLineLens`
## rather than read out of the container, so a reader that returned a
## plausible-looking wrong answer fails.

{.push raises: [].}

import std/options
import results
import ../../src/codetracer_trace_writer/new_trace_reader
import ../../src/codetracer_trace_writer/value_stream
import ../../src/codetracer_trace_writer/call_stream

const
  CorpusRecordingId* = "0192f8a0-1234-7abc-8def-0123456789ab"
  CorpusProgram* = "ct_browser_reader_probe"

  CorpusSteps* = 5000
    ## Above the writer's default 4096-event chunk, so the read path seeks
    ## into a sealed seekable-Zstd chunk instead of decoding a single one.

  CorpusPath0* = "/ct/browser/main.nim"
  CorpusPath1* = "/ct/browser/lib.nim"

  CorpusFunc0* = "browserMain"
  CorpusFunc1* = "decodeStep"
  CorpusFunc2* = "renderFlow"

  CorpusType0* = "int64"
  CorpusType1* = "string"
  CorpusType2* = "StepEvent"

  CorpusVar0* = "acc"
  CorpusVar1* = "idx"
  CorpusVar2* = "path"

  CorpusValueEvery* = 500
    ## One variable value every N steps, so the value stream is non-empty and
    ## its per-step association is checkable.
  CorpusValueByte* = 42'u8

let
  CorpusLineLens0* = @[10'u32, 20'u32, 30'u32, 40'u32, 50'u32]
  CorpusLineLens1* = @[5'u32, 15'u32, 25'u32]

proc corpusFile*(i: int): uint64 =
  uint64(i mod 2)

proc corpusLine*(i: int): uint64 =
  ## 1-based, and always within the file's registered line count.
  if corpusFile(i) == 0'u64: uint64(i mod 5) + 1 else: uint64(i mod 3) + 1

proc corpusColumnDelta*(i: int): int64 =
  ## Offset from column 1, kept strictly below the shortest line of the file
  ## it lands on so a decoded position cannot spill into the next line.
  if corpusFile(i) == 0'u64: int64(i mod 7) else: int64(i mod 4)

proc corpusPositionIndex*(i: int): uint64 =
  ## The `global_position_index` the column-aware writer must emit for step
  ## `i`: file base + the byte offset of column 1 on the line + the column
  ## delta. Recomputed here rather than read back, so it is an expectation.
  let f = corpusFile(i)
  let line = corpusLine(i)
  let lens = if f == 0'u64: CorpusLineLens0 else: CorpusLineLens1
  var base: uint64 = 0
  if f == 1'u64:
    for L in CorpusLineLens0:
      base += uint64(L)
  var off: uint64 = 0
  for k in 0 ..< int(line) - 1:
    off += uint64(lens[k])
  base + off + uint64(corpusColumnDelta(i))

# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------

const
  ProbeIndices* = [0, 1, 2, 7, 63, 64, 65, 4095, 4096, 4097, 4999]
    ## Deliberately straddles the 4096-event chunk boundary and the ±63
    ## DeltaStep encoding window, so the checked steps are not all reached by
    ## the same decode path.

proc verifyCorpus*(data: seq[byte]): int32 =
  ## Open `data` as a CTFS container and check what comes back out.
  ## Returns 0, or a code identifying the first check that failed.
  if data.len == 0: return 1

  let rr = openNewTraceFromBytes(data)
  if rr.isErr: return 2
  var r = rr.get()

  if not r.meta.hasColumnAwareSteps: return 3

  # --- interning tables ----------------------------------------------------
  if r.pathCount() != 2'u64: return 10
  if r.functionCount() != 3'u64: return 11
  if r.typeCount() != 3'u64: return 12
  if r.varnameCount() != 3'u64: return 13

  let path0 = r.path(0'u64)
  if path0.isErr or path0.get() != CorpusPath0: return 20
  let path1 = r.path(1'u64)
  if path1.isErr or path1.get() != CorpusPath1: return 21

  let f0 = r.function(0'u64)
  if f0.isErr or f0.get() != CorpusFunc0: return 22
  let f1 = r.function(1'u64)
  if f1.isErr or f1.get() != CorpusFunc1: return 23
  let f2 = r.function(2'u64)
  if f2.isErr or f2.get() != CorpusFunc2: return 24

  let t0 = r.typeName(0'u64)
  if t0.isErr or t0.get() != CorpusType0: return 25
  let t1 = r.typeName(1'u64)
  if t1.isErr or t1.get() != CorpusType1: return 26
  let t2 = r.typeName(2'u64)
  if t2.isErr or t2.get() != CorpusType2: return 27

  let v0 = r.varname(0'u64)
  if v0.isErr or v0.get() != CorpusVar0: return 28
  let v1 = r.varname(1'u64)
  if v1.isErr or v1.get() != CorpusVar1: return 29
  let v2 = r.varname(2'u64)
  if v2.isErr or v2.get() != CorpusVar2: return 30

  # --- per-line tables -----------------------------------------------------
  if r.lineCountRaw(0'u64) != uint64(CorpusLineLens0.len): return 35
  if r.lineCountRaw(1'u64) != uint64(CorpusLineLens1.len): return 36
  for k in 0 ..< CorpusLineLens0.len:
    let ll = r.lineLength(0'u64, uint32(k))
    if ll.isNone or ll.get() != CorpusLineLens0[k]: return 37
  for k in 0 ..< CorpusLineLens1.len:
    let ll = r.lineLength(1'u64, uint32(k))
    if ll.isNone or ll.get() != CorpusLineLens1[k]: return 38

  # --- steps ---------------------------------------------------------------
  let sc = r.stepCount()
  if sc.isErr or sc.get() != uint64(CorpusSteps): return 40

  for i in ProbeIndices:
    let gliRes = r.stepAbsoluteGlobalLineIndex(uint64(i))
    if gliRes.isErr: return 50
    if gliRes.get() != corpusPositionIndex(i): return int32(51)
    let posRes = r.decodeGlobalPositionIndex(gliRes.get())
    if posRes.isErr: return 52
    let pos = posRes.get()
    if pos.file != corpusFile(i): return 53
    if pos.line != uint32(corpusLine(i)): return 54
    if pos.column != uint32(corpusColumnDelta(i) + 1): return 55

  # The bulk accessor must agree with the per-step one across a chunk seam.
  var bulk = newSeq[uint64](8)
  let nb = r.stepAbsoluteGlobalLineIndices(4092'u64, 8'u64, bulk)
  if nb.isErr or nb.get() != 8'u64: return 60
  for k in 0 ..< 8:
    if bulk[k] != corpusPositionIndex(4092 + k): return 61

  # --- values --------------------------------------------------------------
  let vals = r.values(0'u64)
  if vals.isErr: return 70
  if vals.get().len != 1: return 71
  if vals.get()[0].data.len != 2: return 72
  if vals.get()[0].data[1] != CorpusValueByte: return 73

  # --- calls ---------------------------------------------------------------
  let cc = r.callCount()
  if cc.isErr or cc.get() != 1'u64: return 80
  let c0 = r.call(0'u64)
  if c0.isErr or c0.get().functionId != 1'u64: return 81

  0'i32

{.pop.}
