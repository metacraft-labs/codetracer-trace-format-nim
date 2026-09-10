## The C ABI's view of the line-only position space.
##
## `ct_reader_step_location` / `ct_reader_step_locations` /
## `ct_reader_step_locations_with_columns` are how
## `codetracer/src/db-backend/src/ctfs_trace_reader` populates `Db.steps`,
## so whatever they answer becomes a DAP `stackTrace` frame. They invert a
## line-only `global_position_index` through the writer's own address space
## — `prefixSum[path_id] + (line - 1)`, `DefaultLinesPerFile` addresses per file —
## which is an assumption about the producer, not a property of the trace.
## A line-only container states no packing: no stride, no per-file line
## count, no producer identifier.
##
## The assumption is contradicted in practice. The Rust
## `codetracer_trace_writer` writes the same container format and packs
## `(path_id shl 32) or line` (`step_stream.rs pack_global_line_index`).
## Every step of every file above id 0 then sits at least 4294967296 above
## the base, far past the top of a `DefaultLinesPerFile` space.
##
## What is asserted here, and why each one can fail:
##
##   1. All three accessors REFUSE such a position — non-zero / `UINT64_MAX`
##      with the index and the rival packing named in
##      `trace_writer_last_error`. Unchecked, they report path 1, line
##      4294867302 and return success, which is what reaches the DAP wire.
##   2. They DISCRIMINATE: on an ordinary line-only container built by the
##      same helper, all three succeed and report exactly the `(path, line)`
##      pairs registered, with the column slot 0. A guard that refused
##      everything fails this half.
##
## No mocks: the containers are produced by this repository's own writer and
## read back through the real C entry points.

# Include the FFI module so the C entry points can be driven directly.
# Mirrors tests/test_reader_ffi_column_aware_paths.nim.
include codetracer_trace_writer_ffi

# Drop the `raises: []` push from the FFI module so the test body can use
# higher-level helpers.
{.pop.}

import std/strutils

const
  PathA = "/src/main.py"
  PathB = "/src/helper.py"

proc packGlobalLineIndexRust(pathId: uint64, line: uint64): uint64 =
  ## `codetracer-trace-format/codetracer_trace_writer/src/step_stream.rs`
  ## `pack_global_line_index`. Reproduced rather than linked: the point is
  ## that the two packings are independent.
  (pathId shl 32) or (line and ((1'u64 shl 32) - 1))

proc lineEncodingTo(address: uint64): uint64 =
  ## The `line` to register on path 0 so that `address` itself is what lands
  ## on the wire. Path 0's base is 0 and the encode is `base + (line - 1)`,
  ## so the line is one more than the address it produces. Without the
  ## `+ 1` the injected step carries `address - 1` and the test asserts
  ## about an integer no writer emits.
  address + 1

proc writeTrace(file: string, steps: openArray[(uint64, uint64)]) =
  ## A line-only container over two paths. `registerStep` applies the Nim
  ## packing, so a Rust-packed address routed through `lineEncodingTo` on
  ## path 0 lands on the wire unchanged, exactly as a Rust-written
  ## container's `steps.dat` would hold it.
  var w = initMultiStreamWriter(file & ".build", "ffi_line_only_space").get()
  doAssert w.registerPath(PathA).isOk
  doAssert w.registerPath(PathB).isOk
  for (p, l) in steps:
    doAssert w.registerStep(p, l, @[]).isOk
  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()
  writeFile(file, cast[string](bytes))

proc ffiLastError(): string =
  let e = trace_writer_last_error()
  if e.isNil: "" else: $e

let dir = getTempDir() / "ctfnim-reader-ffi-line-only-position-space"
removeDir(dir)
createDir(dir)
let foreign = dir / "foreign.ct"
let ordinary = dir / "ordinary.ct"
writeTrace(foreign,
  [(0'u64, lineEncodingTo(packGlobalLineIndexRust(1'u64, 5'u64)))])
writeTrace(ordinary, [(0'u64, 3'u64), (1'u64, 7'u64), (0'u64, 12'u64)])

block single_accessor_refuses_by_name:
  let h = ct_reader_open(cstring(foreign))
  doAssert not h.isNil, "ct_reader_open failed: " & ffiLastError()
  var pathId, line: uint64
  let rc = ct_reader_step_location(h, 0'u64, addr pathId, addr line)
  doAssert rc != 0.cint,
    "an unresolvable position must not be reported as a location; got " &
    "path " & $pathId & " line " & $line
  let e = ffiLastError()
  doAssert e.startsWith("step 0:"),
    "the refusal must name the step: " & e
  doAssert "4294967301" in e,
    "the refusal must name the index it could not resolve: " & e
  doAssert "pack_global_line_index" in e,
    "the refusal must name the rival packing: " & e
  ct_reader_close(h)
  echo "PASS: single_accessor_refuses_by_name"

block bulk_accessor_refuses_by_name:
  let h = ct_reader_open(cstring(foreign))
  doAssert not h.isNil, "ct_reader_open failed: " & ffiLastError()
  var pathIds = newSeq[uint64](4)
  var lines = newSeq[uint64](4)
  let written = ct_reader_step_locations(
    h, 0'u64, 4'u64, addr pathIds[0], addr lines[0])
  doAssert written == high(uint64),
    "the bulk drain must fail, not fill; it reported " & $written &
    " entries, first being path " & $pathIds[0] & " line " & $lines[0]
  doAssert "4294967301" in ffiLastError(),
    "the refusal must name the index: " & ffiLastError()
  ct_reader_close(h)
  echo "PASS: bulk_accessor_refuses_by_name"

block column_accessor_refuses_by_name:
  let h = ct_reader_open(cstring(foreign))
  doAssert not h.isNil, "ct_reader_open failed: " & ffiLastError()
  doAssert ct_reader_has_column_aware_steps(h) == 0.cint,
    "the container declares line-only steps"
  var pathIds = newSeq[uint64](4)
  var lines = newSeq[uint64](4)
  var columns = newSeq[uint64](4)
  let written = ct_reader_step_locations_with_columns(
    h, 0'u64, 4'u64, addr pathIds[0], addr lines[0], addr columns[0])
  doAssert written == high(uint64),
    "the column-aware bulk drain takes the line-only branch here and must " &
    "fail with it; it reported " & $written & " entries"
  doAssert "4294967301" in ffiLastError(),
    "the refusal must name the index: " & ffiLastError()
  ct_reader_close(h)
  echo "PASS: column_accessor_refuses_by_name"

block accessors_still_answer_an_ordinary_trace:
  let h = ct_reader_open(cstring(ordinary))
  doAssert not h.isNil, "ct_reader_open failed: " & ffiLastError()

  let expected = @[(0'u64, 3'u64), (1'u64, 7'u64), (0'u64, 12'u64)]

  for i, exp in expected:
    var pathId, line: uint64
    let rc = ct_reader_step_location(h, uint64(i), addr pathId, addr line)
    doAssert rc == 0.cint,
      "step " & $i & " of an ordinary trace must resolve: " & ffiLastError()
    doAssert (pathId, line) == exp,
      "step " & $i & " expected " & $exp & ", got " & $(pathId, line)

  var pathIds = newSeq[uint64](3)
  var lines = newSeq[uint64](3)
  let written = ct_reader_step_locations(
    h, 0'u64, 3'u64, addr pathIds[0], addr lines[0])
  doAssert written == 3'u64,
    "the bulk drain must return 3 entries, got " & $written & ": " &
    ffiLastError()
  for i, exp in expected:
    doAssert (pathIds[i], lines[i]) == exp,
      "bulk step " & $i & " expected " & $exp & ", got " &
      $(pathIds[i], lines[i])

  var colPathIds = newSeq[uint64](3)
  var colLines = newSeq[uint64](3)
  var columns = newSeq[uint64](3)
  let colWritten = ct_reader_step_locations_with_columns(
    h, 0'u64, 3'u64, addr colPathIds[0], addr colLines[0], addr columns[0])
  doAssert colWritten == 3'u64,
    "the column-aware bulk drain must return 3 entries, got " & $colWritten &
    ": " & ffiLastError()
  for i, exp in expected:
    doAssert (colPathIds[i], colLines[i]) == exp,
      "column-aware bulk step " & $i & " expected " & $exp & ", got " &
      $(colPathIds[i], colLines[i])
    doAssert columns[i] == 0'u64,
      "a line-only trace records no column; step " & $i & " reported " &
      $columns[i]

  ct_reader_close(h)
  echo "PASS: accessors_still_answer_an_ordinary_trace"

removeDir(dir)
echo "ALL PASS: test_reader_ffi_line_only_position_space"
