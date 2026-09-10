## The ONE rendering of a ``TagSourceReload`` step event as JSON.
##
## GDH-M2 / design §6.3 of
## ``codetracer-specs/Planned-Features/GDScript-Hot-Reload-Multi-Version-Sources.md``.
##
## It lives here, beside ``meta_flags_json``, for the reason that module's
## header records: ``ct-print`` has TWO ``--events`` implementations
## (``src/codetracer_ct_print.nim``, the shipped CLI, and
## ``src/codetracer_ct_print_lib.nim``, which the test corpus links and the
## CLI does not import), and a block added to one of them is a MISSING key
## in the other — a failure nothing goes red for. GDH-M1 left that drift
## class closed for the flags block and open for everything else; this is
## one more block on the closed side rather than one more on the open one.
##
## The marker is rendered in FULL — ordinal, every changed triple, and the
## in-flight count. A dump that printed only the kind would let a consumer
## see that a reload happened while leaving it unable to check the marker
## against the ids the steps on either side resolve to, and that check is
## the entire property GDH-G7 exists to defend (design §6.3.1: inference
## from the path indices is nearly sufficient and is refused anyway).

import std/json
import ./step_encoding

export json

proc sourceReloadEventJson*(ev: StepEvent, stepIndex: uint64): JsonNode =
  ## One ``kind: "source_reload"`` entry for an ``--events`` / ``--full``
  ## dump.  ``ev`` must be a ``sekSourceReload`` event.
  doAssert ev.kind == sekSourceReload,
    "sourceReloadEventJson: not a source-reload event"
  result = newJObject()
  result["kind"] = newJString("source_reload")
  # The exec-stream index the marker occupies.  It is NOT a step index a
  # consumer may navigate to (design §7.3 — the marker is a timeline
  # annotation with no source location); it is here so the marker can be
  # placed relative to the steps around it, which is what makes the
  # boundary checkable rather than merely present.
  result["step_index"] = newJInt(int64(stepIndex))
  result["reload_ordinal"] = newJInt(int64(ev.reloadOrdinal))
  var changed = newJArray()
  for ch in ev.changed:
    var o = newJObject()
    o["old_path_id"] = newJInt(int64(ch.oldPathId))
    o["new_path_id"] = newJInt(int64(ch.newPathId))
    o["generation"] = newJInt(int64(ch.generation))
    changed.add(o)
  result["changed"] = changed
  result["changed_count"] = newJInt(int64(ev.changed.len))
  # Design §5.4: steps from a still-unwinding frame legitimately appear
  # AFTER the marker carrying the OLD path id.  Emitted always, including
  # the zero, so a consumer cannot read its absence as "clean cut".
  result["in_flight_frames"] = newJInt(int64(ev.inFlightFrames))
