## The ONE rendering of a trace's ``meta.dat`` flag bits as JSON.
##
## Why this module exists, stated plainly because the defect it closes was
## invisible and cost real time:
##
## ``ct-print`` had **two** implementations of the flags block —
## ``src/codetracer_ct_print.nim`` (the shipped CLI) and
## ``src/codetracer_ct_print_lib.nim`` (used only by the test suite, which
## the CLI does not import). They drifted. ``has_line_count_table`` was added
## to the library copy alone, so the test corpus asserted that ``ct-print``
## reports whether a trace states its file sizes while **no build of the
## binary reported it at all**. A gate that runs against the library and an
## operator who runs the binary saw different traces.
##
## Two copies of a "surface everything" block cannot be kept in step by
## review, because the failure is a MISSING key rather than a wrong one and
## nothing goes red. So there is one copy, here, and both call it.
##
## Adding a flag: add it here once. Both the CLI and the library pick it up.

import std/json
import ./meta_dat

export json

proc metaFlagsJson*(meta: MetaDatContents): JsonNode =
  ## Every known ``meta.dat`` flag bit as its own boolean, keyed by the
  ## bit's constant name.
  ##
  ## Each flag is a separate field rather than an array so the JSON shape
  ## is a stable golden-test anchor: a trace written before a flag existed
  ## reports it ``false`` instead of omitting it, and a consumer can tell
  ## "the bit is clear" from "this ct-print does not know the bit" — which
  ## is exactly the distinction the duplicated block destroyed.
  result = newJObject()
  result["has_column_aware_steps"] = newJBool(meta.hasColumnAwareSteps)
  result["has_alternate_source_views"] = newJBool(
    meta.hasAlternateSourceViews)
  result["supports_column_breakpoints"] = newJBool(
    meta.supportsColumnBreakpoints)
  result["supports_column_motions"] = newJBool(meta.supportsColumnMotions)
  result["has_call_stream"] = newJBool(meta.hasCallStream)
  result["has_step_stream"] = newJBool(meta.hasStepStream)
  result["has_value_stream"] = newJBool(meta.hasValueStream)
  result["has_io_event_stream"] = newJBool(meta.hasIoEventStream)
  result["has_interning_tables"] = newJBool(meta.hasInterningTables)
  result["has_correlation_index"] = newJBool(meta.hasCorrelationIndex)
  # Whether the container STATES how large each of its files is, or leaves a
  # reader to assume `DefaultLinesPerFile` for every one of them. False is the
  # answer for every trace written before bit 14 existed, and it is the one an
  # operator needs when a step's reported line looks wrong: under the
  # assumption a file with more lines than the ceiling has its lines addressed
  # inside the next file's range, and no reader can detect that.
  result["has_line_count_table"] = newJBool(meta.hasLineCountTable)
  # GDH-M2: whether the container declares the source-reload marker, i.e.
  # whether step-stream tag 0x08 may legally appear.  False for every
  # trace written before the extended flag word existed (schema version
  # 4), which is every trace any recorder produces today.
  result["has_source_reload"] = newJBool(meta.hasSourceReload)

proc metaFlagKeys*(): seq[string] =
  ## The key set ``metaFlagsJson`` produces, for a consumer that wants to
  ## assert the two ``ct-print`` entry points agree without diffing whole
  ## documents.
  var probe = MetaDatContents()
  result = @[]
  for k, _ in metaFlagsJson(probe):
    result.add(k)
