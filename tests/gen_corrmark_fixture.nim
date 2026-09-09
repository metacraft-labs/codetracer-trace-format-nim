## Generate the cross-language `corrmark.ns` fixture.
##
## HELPER BINARY, NOT A TEST — deliberately absent from the nimble `test` task
## and from `repro.nim`, like `check_nsb1_namespace.nim` beside it. It exists so
## a reader written in another language can be pinned against a container this
## implementation actually produced, rather than against a second
## implementation's idea of the format (which would agree with itself no matter
## what either wrote).
##
## The consumer is `codetracer-ci`'s C# correlation-index reader; the fixture
## and the regeneration command live in that repo's
## `tests/fixtures/corrmark/README.md`.
##
##   nim c -r -p:src tests/gen_corrmark_fixture.nim <out.ct> <out.json>
##
## The JSON sidecar states what the container is expected to answer, so the
## consumer's assertions are about the fixture's declared content rather than
## about numbers someone transcribed by hand.

import std/os
import ../src/codetracer_trace_writer/multi_stream_writer

type SpanSpec = object
  traceIdHex: string
  spanIdHex: string
  wallNs: uint64
  monotonicNs: uint64

const Spans = [
  # The M25 observability corpus's own ids, so the fixture exercises the exact
  # shape the cross-repo consumer queries with.
  SpanSpec(traceIdHex: "6f92f3577b34da6a3ce929d0e0e4ab14",
           spanIdHex: "51000000000025aa",
           wallNs: 1788897919650415375'u64,
           monotonicNs: 2052008038067771'u64),
  SpanSpec(traceIdHex: "6f92f3577b34da6a3ce929d0e0e4ab14",
           spanIdHex: "30000000000025cc",
           wallNs: 1788878366340223810'u64,
           monotonicNs: 2032454727205762'u64),
  # A second trace id, so a lookup cannot pass by ignoring the trace half of
  # the key.
  SpanSpec(traceIdHex: "0102030405060708090a0b0c0d0e0f10",
           spanIdHex: "1112131415161718",
           wallNs: 1700000000000000001'u64,
           monotonicNs: 4242424242'u64),
]

proc main() =
  if paramCount() < 2:
    echo "usage: gen_corrmark_fixture <out.ct> <out.json>"
    quit 1
  let ctPath = paramStr(1)
  let jsonPath = paramStr(2)
  removeFile(ctPath)

  var w = initMultiStreamWriter(ctPath, "corrmark_fixture",
    recordingId = "01949fcc-7d92-7e9c-aaaa-c04414a4c000").get()
  doAssert w.registerPath("/src/service.py").isOk
  for i, s in Spans:
    doAssert w.registerStep(0, uint64(i + 1), []).isOk
    let r = w.registerSpanCoverageHex(
      s.traceIdHex, s.spanIdHex, s.wallNs, s.monotonicNs)
    doAssert r.isOk, r.error
  # A boundary marker too, so a consumer that confuses the two kinds fails
  # here rather than in production.
  doAssert w.registerCorrelationMarker(
    "send", "order-processing", "order-42", "the order body").isOk
  doAssert w.close().isOk
  doAssert w.closeCtfs().isOk

  var json = "{\n  \"spans\": [\n"
  for i, s in Spans:
    json.add("    {\"traceId\": \"" & s.traceIdHex & "\", \"spanId\": \"" &
      s.spanIdHex & "\", \"wallTimeUnixNs\": " & $s.wallNs &
      ", \"monotonicTimeNs\": " & $s.monotonicNs & ", \"geid\": " & $(i + 1) & "}")
    if i < Spans.high:
      json.add(",")
    json.add("\n")
  json.add("  ]\n}\n")
  writeFile(jsonPath, json)
  echo "wrote ", ctPath, " (", getFileSize(ctPath), " bytes) and ", jsonPath

main()
