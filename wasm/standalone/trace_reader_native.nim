## The host-side half of the freestanding-reader probe.
##
## Writes the corpus container to `argv[1]`, reads it back through the same
## verifier the wasm module runs, and prints the expectations as JSON on
## stdout so `trace_reader_host.mjs` asserts against values produced by the
## corpus definition rather than by the module under test.
##
## Its real job is to supply a container the wasm module did not write: a
## module that only ever reads back its own bytes proves a round trip, not
## that the reader can open a container it was handed.

import std/[os, strutils]
import results
import ./trace_reader_corpus
import ./trace_reader_corpus_build

proc jsonEscape(s: string): string =
  result = ""
  for c in s:
    if c == '"' or c == '\\': result.add('\\')
    result.add(c)

when isMainModule:
  if paramCount() < 1:
    quit("usage: trace_reader_native <out.ct>", 2)

  let built = buildCorpus()
  if built.isErr:
    quit("buildCorpus: " & built.error, 1)
  let bytes = built.get()

  var f: File
  if not f.open(paramStr(1), fmWrite):
    quit("cannot open " & paramStr(1), 1)
  if bytes.len > 0:
    discard f.writeBuffer(unsafeAddr bytes[0], bytes.len)
  f.close()

  let rc = verifyCorpus(bytes)
  if rc != 0:
    quit("native verifyCorpus failed with code " & $rc, 1)

  var probes: seq[string] = @[]
  for i in ProbeIndices:
    probes.add("{\"index\":" & $i &
      ",\"position\":" & $corpusPositionIndex(i) &
      ",\"file\":" & $corpusFile(i) &
      ",\"line\":" & $corpusLine(i) &
      ",\"column\":" & $(corpusColumnDelta(i) + 1) & "}")

  echo "{",
    "\"bytes\":", bytes.len,
    ",\"steps\":", CorpusSteps,
    ",\"paths\":[\"", jsonEscape(CorpusPath0), "\",\"", jsonEscape(CorpusPath1), "\"]",
    ",\"functions\":[\"", CorpusFunc0, "\",\"", CorpusFunc1, "\",\"", CorpusFunc2, "\"]",
    ",\"types\":[\"", CorpusType0, "\",\"", CorpusType1, "\",\"", CorpusType2, "\"]",
    ",\"varnames\":[\"", CorpusVar0, "\",\"", CorpusVar1, "\",\"", CorpusVar2, "\"]",
    ",\"probes\":[", probes.join(","), "]",
    "}"
