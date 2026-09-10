## The C ABI's view of the `paths.dat` layout question.
##
## `meta.dat` bit 4 decides whether a `paths.dat` record is raw path bytes or
## column-aware Layout A, and the two record spaces overlap — a 97-byte ASCII
## path decodes as a complete Layout A record by coincidence. The C ABI is
## where that mattered: a reader that promoted on a successful decode handed
## `ct_reader_line_count_raw` a fabricated per-file line table and
## `ct_reader_step_locations_with_columns` a wrong line, with no error, to the
## DAP layer in `codetracer/src/db-backend/src/ctfs_trace_reader`.
##
## What is asserted here, and why each one can fail:
##
##   1. `ct_reader_open` on the coincidental container reports the trace as
##      its `meta.dat` declares — `ct_reader_has_column_aware_steps` 0 and
##      `ct_reader_line_count_raw` 0. A reader that promoted reports 1 and 48.
##   2. `ct_reader_column_aware_paths_suspected` DISCRIMINATES: 1 for that
##      container, 0 for an ordinary line-only one built by the same helper.
##      A constant would fail one half or the other.
##   3. `ct_reader_open_assume_column_aware_paths` recovers the Layout A
##      tables when the records really do decode, and REFUSES by name (nil
##      handle, `paths.dat[0]: …` in `trace_writer_last_error`) when they do
##      not. A silent fallback to the line-only reading — what the old
##      speculative parse did — fails the second half.
##
## No mocks: the containers are produced by this repository's own writer and
## read back through the real C entry points.

# Include the FFI module so the C entry points can be driven directly.
# Mirrors tests/test_ffi_in_memory.nim.
include codetracer_trace_writer_ffi

# Drop the `raises: []` push from the FFI module so the test body can use
# higher-level helpers.
{.pop.}

import std/strutils

const
  # '/' + 46 filler + '/' + '0' + 48 filler = 97 bytes: path_len 47 from the
  # leading '/', line_count 48 from the '0', 48 single-byte line-length
  # varints from the filler, consuming the record exactly.
  CoincidentPath = "/" & repeat('a', 46) & "/" & "0" & repeat('b', 48)
  OrdinaryPath = "/src/main.py"

proc writeLineOnlyTrace(file: string, path: string) =
  ## A line-only container (no `enableColumnAwareSteps`) with three steps on
  ## lines 1..3 of `path`.
  var w = initMultiStreamWriter(file & ".build", "ffi_paths_probe").get()
  doAssert w.registerPath(path).isOk
  doAssert w.registerStep(0, 1, @[]).isOk
  doAssert w.registerStep(0, 2, @[]).isOk
  doAssert w.registerStep(0, 3, @[]).isOk
  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()
  writeFile(file, cast[string](bytes))

proc ffiLastError(): string =
  let e = trace_writer_last_error()
  if e.isNil: "" else: $e

let dir = getTempDir() / "ctfnim-reader-ffi-column-aware-paths"
removeDir(dir)
createDir(dir)
let coincident = dir / "coincident.ct"
let ordinary = dir / "ordinary.ct"
writeLineOnlyTrace(coincident, CoincidentPath)
writeLineOnlyTrace(ordinary, OrdinaryPath)

block declared_layout_is_authoritative:
  let h = ct_reader_open(cstring(coincident))
  doAssert not h.isNil, "ct_reader_open failed: " & ffiLastError()
  doAssert ct_reader_has_column_aware_steps(h) == 0.cint,
    "meta.dat declares line-only steps; the C ABI must report 0"
  doAssert ct_reader_line_count_raw(h, 0'u64) == 0'u64,
    "no per-line table was recorded, so none may be reported; got " &
    $ct_reader_line_count_raw(h, 0'u64)
  doAssert ct_reader_column_aware_paths_suspected(h) == 1.cint,
    "records that decode as Layout A under a clear bit 4 must be reported"
  ct_reader_close(h)
  echo "PASS: declared_layout_is_authoritative"

block suspicion_discriminates:
  let h = ct_reader_open(cstring(ordinary))
  doAssert not h.isNil, "ct_reader_open failed: " & ffiLastError()
  doAssert ct_reader_has_column_aware_steps(h) == 0.cint
  doAssert ct_reader_column_aware_paths_suspected(h) == 0.cint,
    "an ordinary line-only path must not be reported as suspected Layout A"
  ct_reader_close(h)
  doAssert ct_reader_column_aware_paths_suspected(nil) == -1.cint,
    "a NULL handle is an error, not a false"
  echo "PASS: suspicion_discriminates"

block override_recovers_layout_a:
  let h = ct_reader_open_assume_column_aware_paths(cstring(coincident))
  doAssert not h.isNil, "override open failed: " & ffiLastError()
  doAssert ct_reader_has_column_aware_steps(h) == 1.cint,
    "the override makes the handle read the trace as column-aware"
  doAssert ct_reader_line_count_raw(h, 0'u64) == 48'u64,
    "the Layout A record declares 48 lines; got " &
    $ct_reader_line_count_raw(h, 0'u64)
  ct_reader_close(h)
  echo "PASS: override_recovers_layout_a"

block override_refuses_by_name:
  let h = ct_reader_open_assume_column_aware_paths(cstring(ordinary))
  doAssert h.isNil,
    "the override must refuse records that are not Layout A, not fall back"
  let e = ffiLastError()
  doAssert e.startsWith("paths.dat[0]"),
    "the refusal must name the offending record, got: " & e
  echo "PASS: override_refuses_by_name"

removeDir(dir)
echo "ALL PASS: test_reader_ffi_column_aware_paths"
