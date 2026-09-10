## GDH-M2 falsifier-arm switches.
##
## The verification entries of
## ``codetracer-specs/Planned-Features/GDScript-Hot-Reload-Multi-Version-Sources.milestones.org``
## § GDH-M2 each NAME a mutation that must turn the gate red.  Those
## mutations are compiled into the real encoder / decoder / writer under
## **two** defines — the master switch ``-d:gdh2FalsifierArms`` plus the
## arm's own — so that no single stray ``-d:`` can arm one, and so that a
## build carrying one is self-evident: the master switch emits a
## compile-time warning.
##
## This module exists rather than the GDH-M1 arrangement (the switches
## living inside ``multi_stream_writer``) because GDH-M2's mutations are
## spread across three modules — ``step_encoding`` (the encode and the
## decode), ``meta_dat`` (the flag word) and ``multi_stream_writer`` (the
## marker's own validation) — and a switch defined in one of them cannot
## be reached from the others without an import cycle.
##
## ``tests/test_gdh2_reload_marker.nim`` asserts its own inertness before
## it asserts anything about the subject: a green run compiled with an arm
## active is a measurement of a mutant, not of the writer.

const Gdh2ArmsEnabled* = defined(gdh2FalsifierArms)

when Gdh2ArmsEnabled:
  {.warning: "gdh2FalsifierArms: reload-marker fault injection is COMPILED IN. This build must never be shipped or measured as a green result.".}

template gdh2Arm*(name: untyped): bool =
  ## True iff the named falsifier arm is armed — the master switch AND
  ## the arm's own define.  Always a compile-time constant, so an unarmed
  ## build contains none of the mutated code.
  when defined(gdh2FalsifierArms): defined(name) else: false

proc activeGdh2FalsifierArm*(): string =
  ## The name of the armed mutation, or "" when none is.  Used by the
  ## gate to prove its own green run measured the real implementation.
  if gdh2Arm(gdh2FalsifyConstantOrdinal): "gdh2FalsifyConstantOrdinal"
  elif gdh2Arm(gdh2FalsifySkipUnknownTag): "gdh2FalsifySkipUnknownTag"
  elif gdh2Arm(gdh2FalsifyUngatedDecode): "gdh2FalsifyUngatedDecode"
  elif gdh2Arm(gdh2FalsifyAlwaysSetBit): "gdh2FalsifyAlwaysSetBit"
  elif gdh2Arm(gdh2FalsifyZeroedMarker): "gdh2FalsifyZeroedMarker"
  elif gdh2Arm(gdh2FalsifyUncountedMarker): "gdh2FalsifyUncountedMarker"
  elif gdh2Arm(gdh2FalsifySwappedIds): "gdh2FalsifySwappedIds"
  else: ""
