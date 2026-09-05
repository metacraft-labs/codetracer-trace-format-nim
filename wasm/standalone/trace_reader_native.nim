## The host-side half of the freestanding-reader probe.
##
## Writes the corpus containers to `argv[1]` / `argv[2]` / `argv[3]`, reads
## them back through the same verifiers the wasm module runs, and prints the
## expectations as JSON on stdout so `trace_reader_host.mjs` asserts against
## values produced by the corpus definition rather than by the module under
## test.
##
## Its real job is to supply containers the wasm module did not write: a module
## that only ever reads back its own bytes proves a round trip, not that the
## reader can open a container it was handed.
##
##   trace_reader_native <corpus.ct> <legacy.ct> <misframed.ct>
##
## `legacy.ct` is the pre-M24a Nim-v4 framing, which the writer in `src/`
## cannot emit (`close()` stamps the three stream bits unconditionally) and is
## therefore laid out by hand — see `trace_reader_corpus_build.nim`.
## `misframed.ct` is those same bytes with meta.dat claiming the SPEC framing:
## not a container anything produces, but the isolated form of the question
## "what does the reader do when the discriminator and the bytes disagree?"

import std/[os, strutils]
import results
import ./trace_reader_corpus
import ./trace_reader_corpus_build

proc jsonEscape(s: string): string =
  ## Escape for a JSON string literal, including the control characters the
  ## corpus deliberately carries (view content has newlines, span metadata is
  ## flattened with 0x1E / 0x1F separators).
  result = ""
  for c in s:
    case c
    of '"': result.add("\\\"")
    of '\\': result.add("\\\\")
    of '\n': result.add("\\n")
    of '\r': result.add("\\r")
    of '\t': result.add("\\t")
    else:
      if uint8(c) < 0x20'u8:
        const Hex = "0123456789abcdef"
        result.add("\\u00")
        result.add(Hex[int(uint8(c)) shr 4])
        result.add(Hex[int(uint8(c)) and 0xF])
      else:
        result.add(c)

proc hex(bs: seq[byte]): string =
  const Hex = "0123456789abcdef"
  result = ""
  for b in bs:
    result.add(Hex[int(b) shr 4])
    result.add(Hex[int(b) and 0xF])

proc hexOfString(s: string): string =
  var bs = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    bs[i] = byte(s[i])
  hex(bs)

proc jsonArrayU64(xs: seq[uint64]): string =
  var parts: seq[string] = @[]
  for x in xs:
    parts.add($x)
  "[" & parts.join(",") & "]"

proc writeContainer(path: string, bytes: seq[byte]) =
  var f: File
  if not f.open(path, fmWrite):
    quit("cannot open " & path, 1)
  if bytes.len > 0:
    discard f.writeBuffer(unsafeAddr bytes[0], bytes.len)
  f.close()

when isMainModule:
  if paramCount() < 3:
    quit("usage: trace_reader_native <corpus.ct> <legacy.ct> <misframed.ct>", 2)

  let built = buildCorpus()
  if built.isErr:
    quit("buildCorpus: " & built.error, 1)
  let bytes = built.get()
  writeContainer(paramStr(1), bytes)

  let rc = verifyCorpus(bytes)
  if rc != 0:
    quit("native verifyCorpus failed with code " & $rc, 1)

  let legacyBuilt = buildLegacyCorpus()
  if legacyBuilt.isErr:
    quit("buildLegacyCorpus: " & legacyBuilt.error, 1)
  let legacyBytes = legacyBuilt.get()
  writeContainer(paramStr(2), legacyBytes)

  let legacyRc = verifyLegacyCorpus(legacyBytes)
  if legacyRc != 0:
    quit("native verifyLegacyCorpus failed with code " & $legacyRc, 1)

  let misBuilt = buildMisframedLegacyCorpus()
  if misBuilt.isErr:
    quit("buildMisframedLegacyCorpus: " & misBuilt.error, 1)
  let misBytes = misBuilt.get()
  writeContainer(paramStr(3), misBytes)

  # Measured on the HOST, and asserted identical on wasm. The value is not
  # predicted here: what a mis-discriminated container does is a property of
  # the reader, and the claim being made is that the target agrees with the
  # host about it — not that either outcome is the desired one.
  let misframedBits = probeMisframedLegacy(misBytes)

  var probes: seq[string] = @[]
  for i in ProbeIndices:
    probes.add("{\"index\":" & $i &
      ",\"position\":" & $corpusPositionIndex(i) &
      ",\"file\":" & $corpusFile(i) &
      ",\"line\":" & $corpusLine(i) &
      ",\"column\":" & $(corpusColumnDelta(i) + 1) & "}")

  var views: seq[string] = @[]
  for i in 0 ..< CorpusViewCount:
    views.add("{\"path\":" & $corpusViewPath(i) &
      ",\"kind\":" & $corpusViewKind(i) &
      ",\"name\":\"" & jsonEscape(corpusViewName(i)) & "\"" &
      ",\"contentHex\":\"" & hexOfString(corpusViewContent(i)) & "\"" &
      ",\"mapHex\":\"" & hexOfString(corpusViewMap(i)) & "\"}")

  var viewsForPath: seq[string] = @[]
  for p in 0'u64 ..< 2'u64:
    viewsForPath.add(jsonArrayU64(corpusViewsForPath(p)))

  var ioEvents: seq[string] = @[]
  for i in 0 ..< CorpusIoCount:
    ioEvents.add("{\"kind\":" & $ord(corpusIoKind(i)) &
      ",\"step\":" & $corpusIoStep(i) &
      ",\"dataHex\":\"" & hex(corpusIoData(i)) & "\"" &
      ",\"metaHex\":\"" & hex(corpusIoMeta(i)) & "\"}")

  var spans: seq[string] = @[]
  for i in 0 ..< CorpusSettledSpans:
    let s = corpusSettledSpan(i)
    var structural = 0
    if s.contiguousOnOneThread: structural = structural or 1
    if s.sharesTimeline: structural = structural or 2
    if s.concurrentWithSiblings: structural = structural or 4
    var flatMeta = ""
    for k in 0 ..< s.metadata.len:
      if k > 0: flatMeta.add(char(0x1E))
      flatMeta.add(s.metadata[k][0])
      flatMeta.add(char(0x1F))
      flatMeta.add(s.metadata[k][1])
    spans.add("{\"id\":" & $s.spanId &
      ",\"parent\":" & $s.parentSpanId &
      ",\"isOpen\":" & (if s.isOpen: "1" else: "0") &
      ",\"isExternal\":" & (if s.isExternal: "1" else: "0") &
      ",\"status\":" & $ord(s.status) &
      ",\"startStep\":" & $s.startStep &
      ",\"endStep\":" & $s.endStep &
      ",\"structural\":" & $structural &
      ",\"label\":\"" & jsonEscape(s.label) & "\"" &
      ",\"spanType\":\"" & jsonEscape(s.spanType) & "\"" &
      ",\"metadata\":\"" & jsonEscape(flatMeta) & "\"" &
      ",\"externalRecording\":\"" & jsonEscape(s.externalRecording) & "\"" &
      ",\"externalPath\":\"" & jsonEscape(s.externalPath) & "\"}")

  var spanTypes: seq[string] = @[]
  for name in ["web-request", "process", "test"]:
    spanTypes.add("{\"name\":\"" & jsonEscape(name) & "\"" &
      ",\"ids\":" & jsonArrayU64(corpusSpanIdsOfType(name)) & "}")

  var linehitProbes: seq[string] = @[]
  for stepIdx in LinehitProbePositions:
    let pos = corpusPositionIndex(stepIdx)
    let steps = corpusLinehitSteps(pos)
    var total: uint64 = 0
    for s in steps:
      total += s
    linehitProbes.add("{\"position\":" & $pos &
      ",\"count\":" & $steps.len &
      ",\"first\":" & $steps[0] &
      ",\"last\":" & $steps[^1] &
      ",\"sum\":" & $total & "}")

  var legacyGli: seq[uint64] = @[]
  for i in 0 ..< LegacySteps:
    legacyGli.add(legacyStepGli(i))

  var legacyValues: seq[string] = @[]
  for i in 0 ..< LegacySteps:
    legacyValues.add("\"" & hex(legacyValueBytes(i)) & "\"")

  var legacyIo: seq[string] = @[]
  for i in 0 ..< LegacyIoCount:
    legacyIo.add("{\"kind\":" & $ord(legacyIoKind(i)) &
      ",\"step\":" & $legacyIoStep(i) &
      ",\"dataHex\":\"" & hex(legacyIoData(i)) & "\"}")

  echo "{",
    "\"bytes\":", bytes.len,
    ",\"steps\":", CorpusSteps,
    ",\"paths\":[\"", jsonEscape(CorpusPath0), "\",\"", jsonEscape(CorpusPath1), "\"]",
    ",\"functions\":[\"", CorpusFunc0, "\",\"", CorpusFunc1, "\",\"", CorpusFunc2, "\"]",
    ",\"types\":[\"", CorpusType0, "\",\"", CorpusType1, "\",\"", CorpusType2, "\"]",
    ",\"varnames\":[\"", CorpusVar0, "\",\"", CorpusVar1, "\",\"", CorpusVar2, "\"]",
    ",\"probes\":[", probes.join(","), "]",
    ",\"views\":[", views.join(","), "]",
    ",\"viewsForPath\":[", viewsForPath.join(","), "]",
    ",\"ioEvents\":[", ioEvents.join(","), "]",
    ",\"spanRecords\":", CorpusSpanRecords,
    ",\"spans\":[", spans.join(","), "]",
    ",\"spanTypes\":[", spanTypes.join(","), "]",
    ",\"linehitPositions\":", corpusLinehitPositionCount(),
    ",\"linehitProbes\":[", linehitProbes.join(","), "]",
    ",\"linehitAbsent\":", LinehitAbsentPosition,
    ",\"legacy\":{",
      "\"bytes\":", legacyBytes.len,
      ",\"steps\":", LegacySteps,
      ",\"paths\":[\"", jsonEscape(LegacyPath0), "\",\"", jsonEscape(LegacyPath1), "\"]",
      ",\"function\":\"", jsonEscape(LegacyFunc0), "\"",
      ",\"type\":\"", jsonEscape(LegacyType0), "\"",
      ",\"varname\":\"", jsonEscape(LegacyVar0), "\"",
      ",\"gli\":", jsonArrayU64(legacyGli),
      ",\"valuesHex\":[", legacyValues.join(","), "]",
      ",\"io\":[", legacyIo.join(","), "]",
    "}",
    ",\"misframedBits\":", misframedBits,
    "}"
