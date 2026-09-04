## The write half of the reader probe's corpus.
##
## Split out of `trace_reader_corpus.nim` so a module that only READS is not
## forced to link a writer it never calls: `trace_reader_only_standalone.nim`
## imports the verifier alone, and its size is what a read-only embedding
## actually pays.

{.push raises: [].}

import results
import ../../src/codetracer_trace_writer/multi_stream_writer
import ../../src/codetracer_trace_writer/value_stream
import ./trace_reader_corpus

# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

proc buildCorpus*(): Result[seq[byte], string] =
  ## Build the corpus container in linear memory. No path is opened: the
  ## `path` argument of `initMultiStreamWriter` is metadata only.
  var wr = initMultiStreamWriter("/ct/browser/probe.ct", CorpusProgram,
                                 recordingId = CorpusRecordingId)
  if wr.isErr: return err("initMultiStreamWriter: " & wr.error)
  var w = wr.get()

  w.enableColumnAwareSteps()

  let p0 = w.registerPath(CorpusPath0, CorpusLineLens0)
  if p0.isErr: return err("registerPath 0: " & p0.error)
  let p1 = w.registerPath(CorpusPath1, CorpusLineLens1)
  if p1.isErr: return err("registerPath 1: " & p1.error)

  for name in [CorpusFunc0, CorpusFunc1, CorpusFunc2]:
    let r = w.registerFunction(name)
    if r.isErr: return err("registerFunction: " & r.error)
  for name in [CorpusType0, CorpusType1, CorpusType2]:
    let r = w.registerType(name)
    if r.isErr: return err("registerType: " & r.error)
  for name in [CorpusVar0, CorpusVar1, CorpusVar2]:
    let r = w.registerVarname(name)
    if r.isErr: return err("registerVarname: " & r.error)

  let callRes = w.registerCall(1'u64, [])
  if callRes.isErr: return err("registerCall: " & callRes.error)

  for i in 0 ..< CorpusSteps:
    var values: seq[VariableValue] = @[]
    if i mod CorpusValueEvery == 0:
      values.add(VariableValue(varnameId: 0'u64, typeId: 0'u64,
                               data: @[0x18'u8, CorpusValueByte]))
    let r = w.registerStepWithColumn(corpusFile(i), corpusLine(i),
                                     corpusColumnDelta(i), values)
    if r.isErr: return err("registerStepWithColumn: " & r.error)

  let retRes = w.registerReturn()
  if retRes.isErr: return err("registerReturn: " & retRes.error)

  let cl = w.close()
  if cl.isErr: return err("close: " & cl.error)
  ok(w.toBytes())

{.pop.}
