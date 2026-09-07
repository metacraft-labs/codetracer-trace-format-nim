## A line-only `global_position_index` is not self-describing, so inverting
## one is an assumption — and the assumption has to be falsifiable.
##
## The spec (`codetracer-trace-format-spec/trace-events.md`
## §"Back-Compatibility") says only that a pre-extension trace's
## `global_position_index` is a `global_line_index` in which "each integer
## addresses one line". It defines no apportionment of the integers between
## files, and a container carries none: no stride, no per-file line count
## (that field arrives with `paths.dat` Layout A, under `meta.dat` bit 4), no
## producer identifier.
##
## Two writers of this container format pack it differently:
##
##   * `codetracer_trace_format_nim` — `prefixSum[path_id] + line`, every file
##     allocated `DefaultLinesPerFile` (100_000) addresses.
##     `multi_stream_writer.toGlobalLineIndex` / `global_line_index`.
##   * the Rust `codetracer_trace_writer` — `(path_id shl 32) or line`.
##     `step_stream.rs` `pack_global_line_index`, which
##     `codetracer/src/db-backend` round-trips its own step streams through.
##
## The reader here inverts under the first. Under the second, every step of
## every file above id 0 lands at least 4294967296 above the base — far past
## the top of a `DefaultLinesPerFile` space — so the disagreement is
## detectable even though the packing is not recorded. `tryResolve` reports
## it; the unchecked `resolve` clamps to the last file and answers with a
## path id that exists and a line number that is arithmetic.
##
## What is asserted here, and why each one can fail:
##
##   1. `tryResolve` REFUSES an address above the space and names both
##      packings, while `resolve` on the same address returns the plausible
##      wrong pair. A `tryResolve` that forwarded to `resolve` fails this.
##   2. `readEvents` on a container holding such an address fails by name
##      rather than emitting a `StepRecord`. A reader that resolves it
##      unchecked emits `(path 1, line 4294867301)` and returns `ok`.
##   3. `tryResolve` DISCRIMINATES: every address a Nim-written trace really
##      produces resolves, to exactly the `(path, line)` registered. A check
##      that refused unconditionally fails this half, and so does a
##      `DefaultLinesPerFile` that has drifted from the writer's.
##
## No mocks: the containers are produced by this repository's own writer and
## read back through the real reader.

import std/[os, strutils, assertions]
import results
import codetracer_trace_types
import codetracer_trace_reader
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/global_line_index

const
  PathA = "/src/main.py"
  PathB = "/src/helper.py"

proc packGlobalLineIndexRust(pathId: uint64, line: uint64): uint64 =
  ## `codetracer-trace-format/codetracer_trace_writer/src/step_stream.rs`
  ## `pack_global_line_index`: `(path_id << 32) | (line & 0xFFFF_FFFF)`.
  ## Reproduced rather than linked because the point is that the two
  ## packings are independent — a shared helper would hide the divergence
  ## this test exists to pin.
  (pathId shl 32) or (line and ((1'u64 shl 32) - 1))

proc writeTrace(file: string, steps: openArray[(uint64, uint64)]) =
  ## A line-only container (no `enableColumnAwareSteps`) over two paths,
  ## carrying `steps` as `(pathId, line)` pairs. `registerStep` runs the
  ## Nim packing, so passing a Rust-packed integer as `line` on path 0 —
  ## whose base is 0 — puts that exact integer on the wire, which is what a
  ## Rust-written container's `steps.dat` would hold.
  var w = initMultiStreamWriter(file & ".build", "line_only_position_space").get()
  doAssert w.registerPath(PathA).isOk
  doAssert w.registerPath(PathB).isOk
  for (p, l) in steps:
    doAssert w.registerStep(p, l, @[]).isOk
  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()
  writeFile(file, cast[string](bytes))

proc lineOnlySpace(pathCount: int): GlobalLineIndex =
  var counts = newSeq[uint64](pathCount)
  for i in 0 ..< pathCount:
    counts[i] = DefaultLinesPerFile
  buildGlobalLineIndex(counts)

let dir = getTempDir() / "ctfnim-line-only-position-space"
removeDir(dir)
createDir(dir)

# ---------------------------------------------------------------------------

proc test_foreign_packing_is_refused_not_answered() =
  ## THE REPRODUCER, at the arithmetic. A two-path trace addresses 200000
  ## positions. The Rust writer's encoding of (path 1, line 5) is
  ## 4294967301, which is not one of them.
  let gli = lineOnlySpace(2)
  doAssert gli.totalLines == 200_000'u64,
    "two paths at " & $DefaultLinesPerFile & " addresses each; got " &
    $gli.totalLines

  let foreign = packGlobalLineIndexRust(1'u64, 5'u64)
  doAssert foreign == 4_294_967_301'u64,
    "(1 shl 32) or 5 is 4294967301; got " & $foreign

  # What the unchecked inverse says about it: a path id that exists in the
  # trace, and a line number nothing wrote.
  let (clampedPath, clampedLine) = gli.resolve(foreign)
  doAssert clampedPath == 1,
    "resolve clamps to the last file; got path " & $clampedPath
  doAssert clampedLine == 4_294_867_301'u64,
    "resolve reports 4294967301 - 100000; got line " & $clampedLine

  let refused = gli.tryResolve(foreign)
  doAssert refused.isErr,
    "4294967301 is outside a 200000-address space and must be refused, " &
    "not answered with " & $gli.resolve(foreign)
  doAssert "4294967301" in refused.error,
    "the refusal must name the index it could not resolve: " & refused.error
  doAssert "200000" in refused.error,
    "the refusal must name the space it is outside of: " & refused.error
  doAssert "pack_global_line_index" in refused.error,
    "the refusal must name the other packing a reader could be looking at: " &
    refused.error

  echo "PASS: test_foreign_packing_is_refused_not_answered"

proc test_reader_fails_by_name_on_a_foreign_packed_step() =
  ## END TO END. A container whose `steps.dat` holds the integer a Rust
  ## writer would have written for (path 1, line 5) must be reported, not
  ## reinterpreted.
  let file = dir / "foreign_packing.ct"
  writeTrace(file, [(0'u64, packGlobalLineIndexRust(1'u64, 5'u64))])

  var readerRes = openTrace(file)
  doAssert readerRes.isOk, "openTrace failed (see trace reader Result)"
  var reader = readerRes.get()

  let res = reader.readEvents()
  doAssert res.isErr,
    "readEvents must refuse a position it cannot resolve; it returned ok " &
    "with " & $reader.events.len & " event(s)"
  doAssert res.error.startsWith("step 0:"),
    "the refusal must name the step it failed on: " & res.error
  doAssert "4294967301" in res.error,
    "the refusal must name the unresolvable index: " & res.error
  doAssert "does not carry" in res.error,
    "the refusal must say what the container is missing: " & res.error

  # The wrong answer this replaces: the unchecked inverse emitted a step at
  # path 1, line 4294867301, in a two-file trace whose sources have a few
  # dozen lines between them.
  for ev in reader.events:
    doAssert ev.kind != tleStep,
      "no step may be emitted for an unresolvable position; got path " &
      $uint64(ev.step.pathId) & " line " & $int64(ev.step.line)

  echo "PASS: test_reader_fails_by_name_on_a_foreign_packed_step"

proc test_native_packing_still_resolves_exactly() =
  ## THE OTHER HALF. Every address this repository's writer produces must
  ## still invert to the `(path, line)` that was registered — a refusal that
  ## fires here would be a check that cannot be satisfied rather than one
  ## that cannot fail.
  let gli = lineOnlySpace(2)
  for (path, line) in [(0'u64, 1'u64), (0'u64, 99_999'u64),
                       (1'u64, 1'u64), (1'u64, 42'u64)]:
    let encoded = gli.globalIndex(int(path), line)
    let got = gli.tryResolve(encoded)
    doAssert got.isOk,
      "the writer's own address " & $encoded & " for (path " & $path &
      ", line " & $line & ") must resolve: " & got.error
    doAssert got.get() == (int(path), line),
      "(path " & $path & ", line " & $line & ") round-trip gave " & $got.get()

  let file = dir / "native_packing.ct"
  writeTrace(file, [(0'u64, 3'u64), (1'u64, 7'u64), (0'u64, 12'u64)])

  var readerRes = openTrace(file)
  doAssert readerRes.isOk, "openTrace failed (see trace reader Result)"
  var reader = readerRes.get()
  let res = reader.readEvents()
  doAssert res.isOk, "an ordinary line-only trace must read: " & res.error

  var seen: seq[(uint64, int64)]
  for ev in reader.events:
    if ev.kind == tleStep:
      seen.add((uint64(ev.step.pathId), int64(ev.step.line)))
  doAssert seen == @[(0'u64, 3'i64), (1'u64, 7'i64), (0'u64, 12'i64)],
    "steps must read back where they were written; got " & $seen

  echo "PASS: test_native_packing_still_resolves_exactly"

proc test_a_pathless_trace_is_refused_not_defaulted() =
  ## The other index a line-only space cannot address: any of them, when the
  ## trace registers no paths at all. `resolve` would index `prefixSum[0]`
  ## of a one-element array and report path 0 of a trace that has none.
  let empty = lineOnlySpace(0)
  doAssert empty.totalLines == 0'u64
  let refused = empty.tryResolve(0'u64)
  doAssert refused.isErr, "a trace with no paths cannot locate a step"
  doAssert "no paths" in refused.error,
    "the refusal must name the reason: " & refused.error
  echo "PASS: test_a_pathless_trace_is_refused_not_defaulted"

test_foreign_packing_is_refused_not_answered()
test_reader_fails_by_name_on_a_foreign_packed_step()
test_native_packing_still_resolves_exactly()
test_a_pathless_trace_is_refused_not_defaulted()

removeDir(dir)
echo "ALL PASS: test_line_only_position_space"
