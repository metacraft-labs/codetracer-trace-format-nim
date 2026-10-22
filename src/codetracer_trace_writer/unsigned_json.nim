## Rendering an unsigned 64-bit trace field as a JSON number.
##
## Its own module because two JSON renderers need it and neither may import
## the other: ``full_document_json`` (the ``ct-print`` document, which
## re-exports this) and ``codetracer_trace_reader`` (``TraceReader.toJson`` /
## ``toJsonEvents``), which runs under ``{.push raises: [].}`` and should not
## pull the whole ``ct-print`` assembly in for one conversion.

{.push raises: [].}

import std/json

proc unsignedJson*(n: uint64): JsonNode =
  ## Render an unsigned 64-bit value as a JSON number.
  ##
  ## Addresses, type ids, exception type ids and thread ids are unsigned
  ## 64-bit fields whose values the recorder chooses: a pointer in the upper
  ## half of the address space, or a tag in bit 63, is 2^63 or more.
  ## ``newJInt(int64(n))`` raises ``RangeDefect`` for every such value, and
  ## one of them used to abort the whole ``ct-print --full`` dump.
  ##
  ## A value below 2^63 stays an ordinary ``JInt``, so the document is
  ## unchanged for every value that rendered before. A larger one becomes
  ## ``std/json``'s raw-number node, which serializes as the exact unsigned
  ## decimal digits, unquoted (``%`` for ``BiggestUInt``,
  ## https://nim-lang.org/docs/json.html). A consumer that reads JSON integers
  ## as signed 64-bit must read these fields as unsigned.
  ##
  ## Raises nothing: ``%`` builds the node from ``$n``, which cannot fail,
  ## and the ``{.push raises: [].}`` above makes the compiler hold it to that.
  %BiggestUInt(n)

{.pop.}
