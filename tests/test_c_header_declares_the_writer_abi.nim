## Every `trace_writer_*` entry point the FFI EXPORTS is DECLARED in the C
## header that hosts vendor — or is named, with a reason, in the backlog below.
##
## WHY THIS TEST EXISTS
##
## "Exported but undeclared" has now bitten this project three times, and each
## time it was found by a human noticing that a caller could not reach a
## function that demonstrably existed in the archive:
##
##   * GDH-M3 — two versioned-path entry points. The Godot fork vendors
##     `include/codetracer_trace_writer.h`; the symbols were in
##     `libcodetracer_trace_writer.a` and absent from the header, so the fork
##     could not mint a path version.
##   * GDH-M6 — `trace_writer_set_recording_id`, exported by the FFI since
##     M-REC-1 and never declared. The consequence was not a compile error
##     anywhere: it was that NO TWO GDSCRIPT RECORDINGS OF THE SAME PROGRAM
##     WERE EVER BYTE-IDENTICAL, because no caller could pin an identity — so
##     every gate of the form "did this change alter a recording" was
##     unrunnable for that recorder, and stayed unrunnable for as long as
##     nobody thought to look.
##
## The shape is what makes it hard to see. An undeclared export breaks
## NOTHING. The library builds, the symbol is emitted, `nm` shows it, every
## existing caller keeps working, and the only observable is a capability that
## quietly is not there. Nothing goes red, so nothing tells anyone — which is
## precisely why the check has to be mechanical rather than remembered.
##
## SCOPE, stated because it bounds the claim. This gate covers the
## `trace_writer_*` surface only: that is the writer C ABI, it is what
## `include/codetracer_trace_writer.h` is the header FOR, and it is the
## surface all three incidents were on. The `ct_reader_*` / `ct_value_*` /
## `ct_meta_dat_*` families are exported by the same FFI module but are not
## declared in this header at all and never were — they are reached through
## other bindings — so folding them in would make this test a list of 50
## permanent exceptions, which is a list nobody reads.
##
## NO MOCKS. This reads the real `src/codetracer_trace_writer_ffi.nim` and the
## real `include/codetracer_trace_writer.h` off disk.

import std/[os, strutils, sequtils, algorithm, unittest, sets, re]

const
  RepoRoot = currentSourcePath.parentDir.parentDir
  FfiSource = RepoRoot / "src" / "codetracer_trace_writer_ffi.nim"
  Header = RepoRoot / "include" / "codetracer_trace_writer.h"

  ## Exported, NOT declared, and knowingly so as of 2026-09-11.
  ##
  ## This is a RATCHET, not a licence. The test asserts the undeclared set is
  ## EXACTLY this list, so:
  ##   * a NEW undeclared export fails immediately — the thing that took three
  ##     incidents to notice is now caught at the commit that introduces it;
  ##   * declaring one of these and forgetting to strike it off ALSO fails, so
  ##     the list cannot rot into a set of names that are fine now.
  ##
  ## None of these is claimed to be correctly absent. They are the backlog the
  ## GDH-M6 review surfaced by running this comparison for the first time: the
  ## campaign fixed the one instance that blocked it and this is what was left
  ## standing behind it. Each is reachable only by a caller that writes its own
  ## `extern` declaration, which is the same footgun in a different position.
  ##
  ## The list is NINE, and the ninth is why this is a test and not a one-off
  ## script. The GDH-M6 review first ran the comparison as an ad-hoc regex over
  ## the same two files and got EIGHT: it missed
  ## `trace_writer_record_empty_filter_provenance`, whose signature wraps onto
  ## a second line before its pragma. The parse below — the one the shipped
  ## `test_freestanding_writer_surface.nim` already uses, attributing a pragma
  ## line to the `proc` above it — found all nine. An audit that is retyped
  ## each time it is wanted is an audit with a different answer each time.
  UndeclaredBacklog = [
    "trace_writer_add_filter_provenance",
    "trace_writer_enable_column_aware_steps",
    "trace_writer_enable_column_breakpoints_support",
    "trace_writer_enable_column_motions_support",
    "trace_writer_record_empty_filter_provenance",
    "trace_writer_register_call_arg",
    "trace_writer_register_delta_column",
    "trace_writer_register_path_with_line_lengths",
    "trace_writer_set_args",
  ]

  ## POSITIVE CONTROLS: the three entry points a human had to add to the
  ## header by hand, after each had shipped exported-and-unreachable. If the
  ## matcher below ever stops finding these, it has stopped working, and an
  ## "everything is declared" verdict from a broken matcher is the exact
  ## silent self-pass this file is written against.
  MustBeDeclared = [
    "trace_writer_register_path_version",      # GDH-M3
    "trace_writer_register_path_with_line_count",  # GDH-M3
    "trace_writer_set_recording_id",           # GDH-M6
  ]

proc exportcNames(): seq[string] =
  ## Every `exportc` proc the FFI declares, read out of the source: the pragma
  ## line, attributed to the `proc` header above it. Same parse as
  ## `test_freestanding_writer_surface.nim`, deliberately — two readers of one
  ## file that disagreed would be worse than one.
  var last = ""
  for line in lines(FfiSource):
    let stripped = line.strip()
    if stripped.startsWith("proc "):
      let rest = stripped[5 .. ^1]
      var name = ""
      for c in rest:
        if c.isAlphaNumeric or c == '_': name.add c
        else: break
      last = name
    if "exportc" in line and last.len > 0:
      result.add last
      last = ""

proc stripComments(src: string): string =
  ## C comments removed before anything is looked for.
  ##
  ## A header MENTIONS the names it is about: this one's prose names
  ## `trace_writer_register_path_version` while explaining the ordering
  ## constraint on the source-reload marker. A matcher that searched the raw
  ## text would count a name discussed in a paragraph as a name DECLARED, and
  ## would therefore report the exact defect this file exists to catch as
  ## already fixed. That is a silent self-pass with a documentation comment as
  ## its cause, so the stripping is the load-bearing part and it has its own
  ## discrimination test below.
  result = src.replace(re(r"/\*(.|\n)*?\*/", {reStudy}), " ")
  result = result.replace(re(r"//[^\n]*"), " ")

proc declaredNames(headerText: string): HashSet[string] =
  ## Names that appear as `name(` outside any comment — a declaration or a
  ## definition, which for a header is the same claim: a caller that includes
  ## this file can call it.
  result = initHashSet[string]()
  for m in headerText.findAll(re(r"\btrace_writer_[a-z0-9_]+\s*\(")):
    result.incl m.strip().strip(chars = {'('}).strip()

suite "the C header declares the writer ABI":

  test "the two parsers actually parsed something":
    # Anti-vacuity first, and for the usual reason: every assertion in this
    # file is of the form "X is in Y", and an empty Y makes the interesting
    # one vacuously FALSE while an empty X makes it vacuously TRUE. Both are
    # measured before either is used.
    check fileExists(FfiSource)
    check fileExists(Header)
    let names = exportcNames()
    check names.len > 100                       # 148 on 2026-09-11
    let declared = declaredNames(stripComments(readFile(Header)))
    check declared.len >= 40                    # 53 on 2026-09-11
    for n in MustBeDeclared:
      check n in names                          # really is exported, and
      check n in declared                       # really is declared

  test "comment-only mentions do NOT count as declarations":
    # The discrimination test for `stripComments`, run against synthetic text
    # rather than the real header — the real header happens to declare every
    # name its prose mentions, so it cannot tell a working stripper from a
    # no-op one. This can, and it is the parser that is under test here, not
    # the ABI.
    let fake = """
      /* Explains that trace_writer_ghost_in_a_comment is coming one day. */
      // and trace_writer_ghost_in_a_line_comment(handle) is discussed here
      int trace_writer_really_declared(trace_writer_t handle);
      """
    let got = declaredNames(stripComments(fake))
    check "trace_writer_really_declared" in got
    check "trace_writer_ghost_in_a_comment" notin got
    # The line-comment case is the sharper one: it is written WITH parentheses,
    # so it would satisfy a `name(` search on the unstripped text.
    check "trace_writer_ghost_in_a_line_comment" notin got

  test "every exported trace_writer_* entry point is declared, or is on the backlog":
    let declared = declaredNames(stripComments(readFile(Header)))
    var undeclared: seq[string] = @[]
    for name in exportcNames():
      if not name.startsWith("trace_writer_"): continue
      if name notin declared:
        undeclared.add name
    undeclared = undeclared.deduplicate()
    sort(undeclared)

    var expected = @UndeclaredBacklog
    sort(expected)

    # ONE assertion, EQUALITY, in both directions. A containment check here
    # ("every undeclared name is on the list") would let the backlog grow
    # silently, which is the failure mode being fixed; the mirror check
    # ("every listed name is still undeclared") is what stops the list
    # outliving the gap it records.
    check undeclared == expected
