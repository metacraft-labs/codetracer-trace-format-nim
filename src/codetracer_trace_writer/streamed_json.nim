when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

## Writing a JSON document whose large arrays are produced one element at a
## time, byte-identical to ``std/json``'s rendering of the whole tree.
##
## ``ct-print``'s JSON modes (``--full``, ``--events``, ``--json``,
## ``--json-events``, for split-stream and native containers alike) used to
## build their whole document as one ``JsonNode`` tree and then render it into
## one more string the size of the output. A real Python trace's ``--full``
## output is 12 GB; the tree for it is larger still, and the process reached
## 32 GB (codetracer-pm
## ``issues/2026-10-31-ct-print-full-uses-32-gb-to-decode-an-11-mb-python-trace.md``,
## and ``codetracer-trace-format-spec/trace-events.md`` §"Assembling an event
## stream": an assembler that writes the stream out MUST stream it).
##
## Each document is described here as a list of ``PrettyMember``s: a key with
## either a value small enough to hold (an interning table, the counts) or a
## ``NodeProducer`` that hands the elements of an array to a sink one at a
## time. The writers render and write each piece as it arrives, so only the
## element being written is alive. There is ONE writer for every mode and
## both container kinds, so the layout below exists once.
##
## The layout is ``std/json``'s ``toPretty``
## (https://github.com/nim-lang/Nim/blob/version-2-2/lib/pure/json.nim): a
## non-empty object is ``{``, then each key at the current indent plus 2 as
## ``"key": `` followed by its value, joined by ``,\n``, then a newline and
## ``}`` at the current indent; a non-empty array is ``[``, each element on
## its own line at the indent plus 2, joined by ``,\n``, then a newline and
## ``]`` at the current indent; an empty one is ``{}`` / ``[]``. A value
## rendered by ``pretty`` at indent 0 differs from its rendering nested at
## indent ``n`` only in that every line after its first carries ``n`` more
## spaces — and a JSON text has no raw newline inside a string, since
## ``escapeJson`` writes ``\n`` — so nesting is a reindent of ``pretty``'s
## output.

import std/[json, strutils]

type
  EventSink* = proc(node: JsonNode) {.raises: [].}
    ## Receives the elements of a streamed array one at a time, in order. The
    ## producer does not use a node after handing it over. It cannot raise, so
    ## producers in ``{.push raises: [].}`` modules can call it; a writer that
    ## fails to write records the error and raises it once the producer
    ## returns.

  NodeProducer* = proc(emit: EventSink)
    ## Produces an array's elements by calling ``emit`` for each.

  PrettyMember* = object
    ## One key of a streamed object, in output order.
    key*: string
    case streamed*: bool
    of false:
      value*: JsonNode
    of true:
      produce*: NodeProducer

proc held*(key: string, value: JsonNode): PrettyMember =
  ## A member whose value is held whole.
  PrettyMember(key: key, streamed: false, value: value)

proc streamedArray*(key: string, produce: NodeProducer): PrettyMember =
  ## A member whose value is an array produced one element at a time.
  PrettyMember(key: key, streamed: true, produce: produce)

proc membersOf*(header: JsonNode): seq[PrettyMember] =
  ## Every key of ``header`` as a held member, in order.
  for key, value in header.pairs:
    result.add(held(key, value))

proc addReindented(dest: var string, text: string, indent: int) =
  ## Append ``text`` with ``indent`` spaces after each of its newlines (see
  ## the module header for why that nests ``pretty`` output exactly). One
  ## ``replace`` copies the runs between newlines in bulk; appending a
  ## character at a time was ~40% of ``--full``'s runtime on a 12 GB output.
  dest.add(text.replace("\n", "\n" & spaces(indent)))

proc prettyOrCompact(node: JsonNode): string =
  ## ``pretty(node, indent = 2)``, or the compact ``$node`` should ``pretty``
  ## raise ``ValueError``. In Nim 2.2 it cannot: ``toPretty`` only appends,
  ## and ``pretty`` checks under ``{.raises: [].}``. The arm keeps the
  ## fallback ``ct-print`` always had, for a toolchain whose ``pretty`` does
  ## raise. It is per value now, where the old printer fell back to ``$`` for
  ## the whole document, so on such a toolchain the two texts would differ;
  ## on this one the arm is unreachable and they cannot.
  try:
    pretty(node, indent = 2)
  except ValueError:
    $node

type ArrayWriter = object
  ## Writes one array element by element. Owned by the writing proc; the
  ## sink handed to the producer closes over it.
  output: File
  indent: int          ## the indent the array itself sits at
  count: int
  failure: ref IOError

proc writeElement(w: var ArrayWriter, node: JsonNode) =
  if w.failure != nil:
    return
  let inner = w.indent + 2
  var chunk = (if w.count == 0: "[\n" else: ",\n") & spaces(inner)
  chunk.addReindented(prettyOrCompact(node), inner)
  inc w.count
  try:
    w.output.write(chunk)
  except IOError as e:
    w.failure = e

proc writeArray(output: File, produce: NodeProducer, indent: int) =
  ## Write the array ``produce`` yields, laid out as ``toPretty`` lays out an
  ## array at ``indent`` (its ``[`` already positioned by the caller).
  var w = ArrayWriter(output: output, indent: indent)
  let wp = addr w
  produce(proc(node: JsonNode) {.raises: [].} = wp[].writeElement(node))
  if w.failure != nil:
    raise w.failure
  output.write(if w.count == 0: "[]" else: "\n" & spaces(indent) & "]")

proc writePrettyDocument*(output: File, members: openArray[PrettyMember]) =
  ## Write ``pretty(doc, indent = 2)`` and a newline (``echo``'s) for the
  ## object ``doc`` whose keys are ``members`` in order, rendering each held
  ## value and each streamed element as it comes. ``members`` must not be
  ## empty: ``pretty`` renders an empty object as ``{}``, which no caller
  ## produces.
  doAssert members.len > 0, "writePrettyDocument: a document has keys"
  output.write("{\n")
  for i, m in members:
    var buf = (if i > 0: ",\n  " else: "  ")
    escapeJson(m.key, buf)
    buf.add(": ")
    if m.streamed:
      output.write(buf)
      writeArray(output, m.produce, 2)
    else:
      buf.addReindented(prettyOrCompact(m.value), 2)
      output.write(buf)
  output.write("\n}\n")
  output.flushFile()

proc writePrettyArray*(output: File, produce: NodeProducer) =
  ## Write ``pretty(arr, indent = 2)`` and a newline for the array whose
  ## elements ``produce`` yields.
  writeArray(output, produce, 0)
  output.write("\n")
  output.flushFile()

proc writeJsonLines*(output: File, header: JsonNode, produce: NodeProducer) =
  ## Write ``$header`` on one line, then ``$node`` on its own line for each
  ## node ``produce`` yields — the ``--events`` text.
  output.write($header & "\n")
  var failure: ref IOError
  produce(proc(node: JsonNode) {.raises: [].} =
    if failure == nil:
      try:
        output.write($node & "\n")
      except IOError as e:
        failure = e)
  if failure != nil:
    raise failure
  output.flushFile()
