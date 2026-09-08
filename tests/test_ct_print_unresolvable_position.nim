## `ct-print`'s job is to say what a container holds. When a step's
## `global_position_index` is not one the container's address space can
## hold, "I cannot resolve this" is the true answer and a `(path, line)`
## pair is not.
##
## A line-only position is one integer that addresses one line, and the
## container records nothing about how the integers were apportioned
## between files — no stride, no per-file line count, no producer id (see
## `global_line_index`'s module header, and `test_line_only_position_space`
## for the same argument at the reader). Two writers of this format pack it
## differently, and the unchecked inverse answers an index from the other
## packing by clamping it into the last file: a path id that exists in the
## trace and a line number that is arithmetic.
##
## ct-print emitted that pair into `path_id` / `line` / `path` on six code
## paths. It now emits `position_error` instead, and omits the position keys
## entirely — a consumer that reads `line` gets a missing key rather than a
## number nothing executed at.
##
## What is asserted here, and why each one can fail:
##
##   1. The `--full` / `--events` document carries `position_error` for an
##      unresolvable step, and carries NO `path_id`, `line` or `path`. A
##      ct-print that resolves unchecked emits `line: 4294867302` and no
##      error key.
##   2. The message names the index, the space it is outside of, and the
##      rival packing — enough to act on without reading the source.
##   3. Every step of an ordinary Nim-written two-path trace still gets its
##      registered `(path_id, line)` and NO `position_error`. A ct-print
##      that refused unconditionally, or whose address space had drifted
##      from the writer's, fails this half.
##
## No mocks: the containers are produced by this repository's writer and
## read back through the real reader.

import std/[os, json, strutils, assertions]
import results
import codetracer_ct_print_lib
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader

const
  PathA = "/src/main.py"
  PathB = "/src/helper.py"

let dir = getTempDir() / "ctfnim-ct-print-unresolvable-position"

proc packGlobalLineIndexRust(pathId: uint64, line: uint64): uint64 =
  ## `codetracer-trace-format/codetracer_trace_writer/src/step_stream.rs`
  ## `pack_global_line_index`: `(path_id << 32) | (line & 0xFFFF_FFFF)`.
  ## Reproduced rather than linked because the point is that the two
  ## packings are independent.
  (pathId shl 32) or (line and ((1'u64 shl 32) - 1))

proc lineEncodingTo(address: uint64): uint64 =
  ## The `line` to register on path 0 so that `address` itself is what lands
  ## on the wire. Path 0's base is 0 and the encode is `base + (line - 1)`,
  ## so the line is one more than the address it produces. Without the
  ## `+ 1` the injected step carries `address - 1` and the test asserts
  ## about an integer no writer emits.
  address + 1

proc writeTrace(file: string, steps: openArray[(uint64, uint64)]) =
  ## A line-only container over two paths. `registerStep` runs the Nim
  ## packing, so an already-packed address routed through `lineEncodingTo`
  ## on path 0 puts that exact integer on the wire, which is what a
  ## foreign writer's `steps.dat` would hold.
  var w = initMultiStreamWriter(file & ".build", "ct_print_position").get()
  doAssert w.registerPath(PathA).isOk
  doAssert w.registerPath(PathB).isOk
  for (p, l) in steps:
    doAssert w.registerStep(p, l, @[]).isOk
  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()
  writeFile(file, cast[string](bytes))

proc stepEvents(root: JsonNode): seq[JsonNode] =
  doAssert root.hasKey("events"), "document has no events array"
  for ev in root["events"]:
    if ev.hasKey("kind") and ev["kind"].getStr() == "step":
      result.add(ev)

# ---------------------------------------------------------------------------

proc test_unresolvable_step_is_reported_not_located() =
  let file = dir / "foreign_packing.ct"
  let foreign = packGlobalLineIndexRust(1'u64, 5'u64)
  doAssert foreign == 4_294_967_301'u64, "packing changed: " & $foreign
  writeTrace(file, [(0'u64, lineEncodingTo(foreign))])

  var readerRes = openNewTrace(file)
  doAssert readerRes.isOk, "openNewTrace failed: " & readerRes.error
  var reader = readerRes.get()

  let root = buildFullDocument(reader, FullOpts(stripPaths: false))
  let steps = stepEvents(root)
  doAssert steps.len == 1, "expected 1 step event, got " & $steps.len

  let ev = steps[0]
  doAssert ev.hasKey("position_error"),
    "an unresolvable position must be reported; the event says " & $ev
  for key in ["path_id", "line", "path"]:
    doAssert not ev.hasKey(key),
      "an unresolvable position must carry no " & key & "; the event says " & $ev

  let msg = ev["position_error"].getStr()
  doAssert "4294967301" in msg,
    "the report must name the index: " & msg
  doAssert "200000" in msg,
    "the report must name the space the index is outside of: " & msg
  doAssert "pack_global_line_index" in msg,
    "the report must name the other packing a reader could be looking at: " &
    msg

  echo "PASS: test_unresolvable_step_is_reported_not_located"

proc test_ordinary_steps_still_carry_their_positions() =
  ## THE OTHER HALF. Refusing is only right when the position really is
  ## unresolvable — every address this repository's writer produces must
  ## still come out as the `(path_id, line)` that was registered.
  let file = dir / "native_packing.ct"
  writeTrace(file, [(0'u64, 3'u64), (1'u64, 7'u64), (1'u64, 8'u64)])

  var readerRes = openNewTrace(file)
  doAssert readerRes.isOk, "openNewTrace failed: " & readerRes.error
  var reader = readerRes.get()

  let root = buildFullDocument(reader, FullOpts(stripPaths: false))
  let steps = stepEvents(root)
  doAssert steps.len == 3, "expected 3 step events, got " & $steps.len

  let expected = [(0, 3, PathA), (1, 7, PathB), (1, 8, PathB)]
  for i, ev in steps:
    doAssert not ev.hasKey("position_error"),
      "step " & $i & " is an ordinary position and must resolve: " &
      ev["position_error"].getStr()
    let (wantPath, wantLine, wantStr) = expected[i]
    doAssert ev["path_id"].getInt() == wantPath,
      "step " & $i & " path_id: " & $ev["path_id"].getInt()
    doAssert ev["line"].getInt() == wantLine,
      "step " & $i & " line: " & $ev["line"].getInt()
    doAssert ev["path"].getStr() == wantStr,
      "step " & $i & " path: " & ev["path"].getStr()

  echo "PASS: test_ordinary_steps_still_carry_their_positions"

when isMainModule:
  removeDir(dir)
  createDir(dir)
  test_unresolvable_step_is_reported_not_located()
  test_ordinary_steps_still_carry_their_positions()
  removeDir(dir)
  echo "All ct-print unresolvable-position tests passed."
