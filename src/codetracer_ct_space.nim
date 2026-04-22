when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

## ct-space: Analyze CTFS container space utilization.
##
## Usage:
##   ct-space <file.ct>           # Print text report
##   ct-space --json <file.ct>    # Print JSON report

import std/[os, parseopt]
import results
import codetracer_ctfs/container
import codetracer_ctfs/space_analyzer

proc main() =
  var format = "text"
  var filePath = ""

  for kind, key, val in getopt():
    case kind
    of cmdArgument: filePath = key
    of cmdLongOption:
      case key
      of "json": format = "json"
      else: quit("Unknown option: " & key)
    of cmdShortOption:
      case key
      of "j": format = "json"
      else: quit("Unknown option: " & key)
    of cmdEnd: discard

  if filePath == "":
    quit("Usage: ct-space [--json] <file.ct>")

  let readRes = readCtfsFromFile(filePath)
  if readRes.isErr:
    quit("Error: " & readRes.error)
  let data = readRes.get()

  let reportRes = analyzeCtfs(data)
  if reportRes.isErr:
    quit("Error: " & reportRes.error)
  let report = reportRes.get()

  case format
  of "json": echo report.toJson()
  else: echo report.toText()

main()
