## The C ABI's door to the per-file line-count table.
##
## Every recorder that is not itself written in Nim drives this writer
## through the C entry points, so a feature the C ABI does not expose is a
## feature those recorders cannot have. `trace_writer_enable_line_count_table`
## and `trace_writer_register_path_with_line_count` are the two calls that
## let such a recorder state its files' sizes instead of leaving the reader
## to assume `DefaultLinesPerFile` for each of them.
##
## What is asserted here, and why each one can fail:
##
##   1. A container built entirely through the C ABI carries the counts, and
##      the space it lays out is their sum. If the enable call did not reach
##      the writer, or the count did not reach `paths.dat`, the space is the
##      stride's and the assertion names both numbers.
##   2. The mandatory-count contract survives the trip through C. The
##      implicit path registration that `trace_writer_register_step`
##      performs for an unseen path has no count to record, so once the
##      table is on it is REFUSED by name — the C caller cannot end up with
##      a half-stated table by forgetting a call.
##   3. `trace_writer_register_path_with_line_count` refuses a count of 0
##      through C exactly as the Nim API does.
##   4. The writer-side spill refusal is reachable from C: a step past a
##      file's recorded count fails, and the error names the file.
##   5. The enable call is refused on a column-aware writer, so a C caller
##      cannot request a header declaring both record layouts.
##
## No mocks: the container is produced by the real C entry points and read
## back through `ct_reader_open` and this repository's own reader.

# Include the FFI module so the C entry points can be driven directly.
# Mirrors tests/test_ffi_in_memory.nim.
include codetracer_trace_writer_ffi

# Drop the `raises: []` push from the FFI module so the test body can use
# higher-level helpers.
{.pop.}

import std/strutils
import codetracer_trace_writer/new_trace_reader as ntr

const
  Program = "ffi_line_count_table"
  PathA = "/srv/first.rb"
  PathB = "/srv/second.rb"
  CountA = 24'u64
  CountB = 9'u64

proc ffiLastError(): string =
  let e = trace_writer_last_error()
  if e.isNil: "" else: $e

let dir = getTempDir() / "ctfnim-ffi-line-count-table"

proc test_a_container_built_through_c_states_its_file_sizes() =
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert h != nil, "trace_writer_new failed: " & ffiLastError()
  doAssert trace_writer_begin_in_memory(h) == 0,
    "trace_writer_begin_in_memory: " & ffiLastError()
  doAssert trace_writer_enable_line_count_table(h) == 0,
    "trace_writer_enable_line_count_table: " & ffiLastError()
  doAssert trace_writer_register_path_with_line_count(
    h, cstring(PathA), CountA) == 0, ffiLastError()
  doAssert trace_writer_register_path_with_line_count(
    h, cstring(PathB), CountB) == 0, ffiLastError()
  trace_writer_start(h, cstring(PathA), 1)
  trace_writer_register_step(h, cstring(PathA), 3)
  trace_writer_register_step(h, cstring(PathB), 2)
  doAssert trace_writer_close(h) == 0, "trace_writer_close: " & ffiLastError()

  let n = int(trace_writer_container_len(h))
  doAssert n > 0, "the C ABI produced no container"
  var bytes = newSeq[byte](n)
  copyMem(addr bytes[0], trace_writer_container_ptr(h), n)
  trace_writer_free(h)

  var r = ntr.openNewTraceFromBytes(bytes).get()
  doAssert r.meta.hasLineCountTable,
    "a container built through the C enable call must declare bit 14"
  doAssert r.recordedLineCount(0) == CountA,
    "file 0's count must reach paths.dat as " & $CountA & "; got " &
    $r.recordedLineCount(0)
  doAssert r.recordedLineCount(1) == CountB,
    "file 1's count must reach paths.dat as " & $CountB & "; got " &
    $r.recordedLineCount(1)
  doAssert r.path(0).get() == PathA,
    "the record's framing must not leak into the path; got " & r.path(0).get()
  let space = r.globalPositionSpace()
  doAssert space.totalLines == CountA + CountB,
    "the space must be the sum of the recorded counts (" &
    $(CountA + CountB) & "), not the stride's " &
    $(2'u64 * DefaultLinesPerFile) & "; got " & $space.totalLines

  echo "PASS: test_a_container_built_through_c_states_its_file_sizes"

proc test_the_implicit_path_registration_is_refused() =
  ## `trace_writer_register_step` registers an unseen path itself, with no
  ## count to record. Under the table that record would be the one file the
  ## reader still has to assume a size for, so it is refused rather than
  ## written.
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert trace_writer_begin_in_memory(h) == 0
  doAssert trace_writer_enable_line_count_table(h) == 0
  doAssert trace_writer_register_path_with_line_count(
    h, cstring(PathA), CountA) == 0, ffiLastError()
  trace_writer_start(h, cstring(PathA), 1)

  # PathB was never registered with a count, so the implicit registration
  # this step would perform has nothing to record.
  trace_writer_register_step(h, cstring(PathB), 1)
  let err = ffiLastError()
  doAssert PathB in err,
    "registering a step on an uncounted path must fail by name once the " &
    "line-count table is on; last_error was: " & err
  trace_writer_free(h)

  echo "PASS: test_the_implicit_path_registration_is_refused"

proc test_c_refuses_a_zero_count_and_a_spilled_step() =
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert trace_writer_begin_in_memory(h) == 0
  doAssert trace_writer_enable_line_count_table(h) == 0

  doAssert trace_writer_register_path_with_line_count(
    h, cstring(PathA), 0'u64) != 0,
    "a recorded count of 0 must be refused through C: a file sized 0 " &
    "shares its base with the next one"
  doAssert "line_count" in ffiLastError(),
    "the refusal must name the field; got: " & ffiLastError()

  trace_writer_free(h)

  # A FRESH handle for the spill arm, and an explicitly cleared error
  # slot. `lastError` is a module global that nothing resets on success,
  # so the zero-count refusal above would otherwise still be there and
  # the assertions below would be reading a message from a call they are
  # not about.
  setError("")
  doAssert ffiLastError().len == 0
  let h2 = trace_writer_new(cstring(Program), ffiBinary)
  doAssert trace_writer_begin_in_memory(h2) == 0
  doAssert trace_writer_enable_line_count_table(h2) == 0
  doAssert trace_writer_register_path_with_line_count(
    h2, cstring(PathA), CountA) == 0, ffiLastError()
  doAssert trace_writer_register_path_with_line_count(
    h2, cstring(PathB), CountB) == 0, ffiLastError()
  trace_writer_start(h2, cstring(PathA), 1)

  # The file's own last line is inside its slot. Asserted against an
  # error slot that is still empty, which is what makes the spill
  # assertion below evidence rather than an echo.
  trace_writer_register_step(h2, cstring(PathA), int64(CountA))
  doAssert ffiLastError().len == 0,
    "the file's last line must be accepted; last_error: " & ffiLastError()

  trace_writer_register_step(h2, cstring(PathA), int64(CountA + 1))
  # The refusal fires when the buffered step is FLUSHED, which the next
  # step event does.
  trace_writer_register_step(h2, cstring(PathB), 1)
  let spillErr = ffiLastError()
  doAssert PathA in spillErr and $CountA in spillErr,
    "a step past the recorded count must be refused by name through C — " &
    "its address is the first line of the next file, and no reader can " &
    "tell that apart from a real location; last_error was: " & spillErr
  trace_writer_free(h2)

  echo "PASS: test_c_refuses_a_zero_count_and_a_spilled_step"

proc test_c_refuses_the_table_on_a_column_aware_writer() =
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert trace_writer_begin_in_memory(h) == 0
  trace_writer_enable_column_aware_steps(h)
  doAssert trace_writer_enable_line_count_table(h) != 0,
    "a column-aware writer already carries line_count in its Layout A " &
    "records and sizes files in columns; the table must be refused"
  doAssert "column-aware" in ffiLastError(),
    "the refusal must say why; got: " & ffiLastError()
  trace_writer_free(h)

  echo "PASS: test_c_refuses_the_table_on_a_column_aware_writer"

removeDir(dir)
createDir(dir)

test_a_container_built_through_c_states_its_file_sizes()
test_the_implicit_path_registration_is_refused()
test_c_refuses_a_zero_count_and_a_spilled_step()
test_c_refuses_the_table_on_a_column_aware_writer()

removeDir(dir)
echo "ALL PASS: test_ffi_line_count_table"
