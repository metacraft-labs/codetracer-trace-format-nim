when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

## ct-print: Convert .ct trace files to human-readable formats.
##
## Usage:
##   ct-print <file.ct>                  # Print as text (default)
##   ct-print --json <file.ct>           # Print as JSON
##   ct-print --json-events <file.ct>    # Print only events as JSON array
##   ct-print --summary <file.ct>        # Print metadata and event counts only

import std/[os, parseopt]
import codetracer_trace_reader

proc main() =
  var format = "text"
  var filePath = ""

  for kind, key, val in getopt():
    case kind
    of cmdArgument: filePath = key
    of cmdLongOption:
      case key
      of "json": format = "json"
      of "json-events": format = "json-events"
      of "summary": format = "summary"
      else: quit("Unknown option: " & key)
    of cmdShortOption:
      case key
      of "j": format = "json"
      of "s": format = "summary"
      else: quit("Unknown option: " & key)
    of cmdEnd: discard

  if filePath == "":
    quit("Usage: ct-print [--json|--json-events|--summary] <file.ct>")

  let readerRes = openTrace(filePath)
  if readerRes.isErr:
    quit("Error: " & readerRes.unsafeError)
  var reader = readerRes.get()

  let readRes = reader.readEvents()
  if readRes.isErr:
    quit("Error reading events: " & readRes.unsafeError)

  case format
  of "json": echo reader.toJson()
  of "json-events": echo reader.toJsonEvents()
  of "summary": echo reader.toSummary()
  else: echo reader.toPrettyText()

main()
