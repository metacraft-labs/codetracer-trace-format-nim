## GDH-M3 verifier — the container half of
## ``gdh3_fork_uses_the_writers_own_path_id``.
##
## ``tests/test_gdh3_fork_path_ids.c`` produces the container with the real
## C ABI and prints the ids the WRITER handed back.  This reads the
## container with the production reader and asserts that the source views
## actually landed on those ids — the entry's own wording: *"proven by
## reading the view back and checking its `path_id` against the writer's
## returned value, not against a count the fork maintained."*
##
## Usage:
##   gdh3_verify two   <container.ct> <v1id> <otherid> <v2id> <bundledv2>
##   gdh3_verify single <container.ct> <v1id> <otherid>
##
## The ids come from the producer's stdout, so a producer that printed
## nothing cannot be verified into a pass: the driver requires all of them.

import std/[os, strutils]
import results
import codetracer_trace_writer/new_trace_reader

const Gate = "gdh3_fork_uses_the_writers_own_path_id"

proc fail(msg: string) {.noreturn.} =
  stderr.writeLine("GDH3-FAIL[" & Gate & "]: " & msg)
  quit(1)

template check(cond: bool, msg: string) =
  if not cond: fail(msg)

proc main() =
  if paramCount() < 4:
    fail("usage: gdh3_verify <two|single> <container> <ids...>")
  let mode = paramStr(1)
  let path = paramStr(2)
  check(fileExists(path),
    "the container " & path & " does not exist. A verifier whose input is " &
    "missing must DIE, not report a pass on a comparison it never made")

  let raw = readFile(path)
  var bytes = newSeq[byte](raw.len)
  for i, c in raw: bytes[i] = byte(c)
  let rr = openNewTraceFromBytes(bytes)
  check(rr.isOk, "the container does not open: " & rr.error)
  var r = rr.get()

  # --- anti-vacuity, in the order the entry states it ------------------
  #
  # "Assert a source view was actually written — counts.source_views > 0
  # from the container header — before comparing its path_id. A
  # register_source_view that failed and returned -1 would leave nothing
  # to compare and every subsequent assertion would be over an empty set."
  let nViews = r.sourceViewCount()
  check(nViews > 0'u64,
    "the container carries ZERO source views. Every assertion below is " &
    "over that set, so an empty one satisfies them all for free (trap 4)")
  check(r.meta.hasLineCountTable,
    "the container does not declare meta.dat bit 14. Versioned paths are " &
    "defined only on the bit-14 record layout, so a container without it " &
    "cannot be carrying what this gate is about — and bit 14 being clear " &
    "is exactly what GDH-M0 measured, because the header the fork vendors " &
    "did not declare trace_writer_enable_line_count_table")

  let wantViews = if mode == "two": 3'u64 else: 2'u64
  check(nViews == wantViews,
    "expected " & $wantViews & " source views, the container has " &
    $nViews & ". The count is knowable from the fixture, so it is " &
    "asserted rather than merely being required to be non-zero: `at " &
    "least one` is satisfied by one member of three")

  proc viewPathIds(): seq[uint64] =
    result = @[]
    for i in 0'u64 ..< nViews:
      let v = r.sourceView(i)
      check(v.isOk, "source view " & $i & " does not decode: " & v.error)
      check(v.get().content.len > 0,
        "source view " & $i & " is EMPTY; a comparison over empty content " &
        "is true for free")
      result.add(v.get().pathId)

  let ids = viewPathIds()

  if mode == "single":
    let v1 = parseBiggestUInt(paramStr(3)).uint64
    let oid = parseBiggestUInt(paramStr(4)).uint64
    check(ids[0] == v1,
      "the first source view landed on path " & $ids[0] & ", the writer " &
      "returned " & $v1)
    check(ids[1] == oid,
      "the second source view landed on path " & $ids[1] & ", the writer " &
      "returned " & $oid)
    echo "PASS: " & Gate & " [CONTROL ARM, one version] — " &
      $nViews & " views on writer ids " & $v1 & ", " & $oid
    return

  check(paramCount() >= 6, "the `two` mode needs v1, other, v2 and bundled ids")
  let v1 = parseBiggestUInt(paramStr(3)).uint64
  let oid = parseBiggestUInt(paramStr(4)).uint64
  let v2 = parseBiggestUInt(paramStr(5)).uint64
  let bundledV2 = parseBiggestUInt(paramStr(6)).uint64

  # The producer must have used the writer's id for the post-reload
  # bundle.  This is the assertion the mirror counter fails.
  check(bundledV2 == v2,
    "the source view registered AFTER the version was minted was attached " &
    "to path id " & $bundledV2 & ", while the writer assigned the new " &
    "version id " & $v2 & ". This is the mirror-counter defect: the host " &
    "re-derived the id from its own count of first-seen path STRINGS, the " &
    "string was already known, and v2's text went onto v1's entry — " &
    "silently, and for every source view from the first reload onward")
  check(v2 != v1,
    "the writer returned the SAME id for both versions (" & $v2 & "). " &
    "With one id there is nothing for this gate to discriminate, and the " &
    "two-version arm has collapsed into the control arm")

  check(ids[0] == v1, "view 0 is on path " & $ids[0] & ", expected " & $v1)
  check(ids[1] == oid, "view 1 is on path " & $ids[1] & ", expected " & $oid)
  check(ids[2] == v2,
    "view 2 (v2's source text) is on path " & $ids[2] & ", expected the " &
    "writer's " & $v2)

  # And the two versions must still be ONE virtual path: the whole point
  # of an index-discriminated version is that the string does not change.
  let s1 = r.path(v1)
  let s2 = r.path(v2)
  check(s1.isOk and s2.isOk, "a version's path id does not resolve")
  check(s1.get().len > 0, "the path string is EMPTY")
  check(s1.get() == s2.get(),
    "the two versions carry different path strings (`" & s1.get() &
    "` vs `" & s2.get() & "`)")

  # The two source views must carry DIFFERENT content — otherwise "both
  # versions are present" is satisfied by the same bytes twice.
  let c1 = r.sourceView(0)
  let c3 = r.sourceView(2)
  check(c1.isOk and c3.isOk, "a source view does not decode")
  check(c1.get().content != c3.get().content,
    "v1's and v2's source views hold identical bytes, so the container " &
    "does not actually carry two versions of anything")

  echo "PASS: " & Gate & " — 3 views, v2's text on the writer's id " & $v2 &
    ", both versions at `" & s1.get() & "`"

when isMainModule:
  main()
