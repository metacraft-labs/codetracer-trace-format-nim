## The C ABI's column-aware path registrations: the writer decides a file's
## table when the file is first mentioned.
##
## Spec: ~codetracer-trace-format-spec/internal-files.md~ §"`paths.dat`
## Layout A"; a refused call fails the recording per ~trace-events.md~
## §"Recorder Integration — A Failed Call Fails the Recording".
##
## What is asserted here, and why each one can fail:
##
##   1. `trace_writer_register_path_with_line_lengths` with no table, and a
##      step naming a path never registered, record the conventional table
##      (100000 lines of 1024 positions) — not a table with no positions.
##   2. A table whose lines hold nothing gives its first line one position.
##   3. A column past 1024 on a conventional file, delivered as a
##      `trace_writer_register_delta_column` folded into its step, is recorded
##      at column 1024.
##   4. A step past line 100000 of a conventional file is refused by name, and
##      `trace_writer_close` then fails naming it.
##   5. A table offered after the file was interned — here by a step — is
##      refused by name unless it is the recorded table, and the recording
##      fails.
##
## No mocks: the container is produced by the real C entry points and read
## back through this repository's own reader.

include codetracer_trace_writer_ffi

{.pop.}

import std/[os, strutils, options]
import codetracer_trace_writer/new_trace_reader as ntr

const
  Program = "ffi_column_table_decided_at_first_mention"
  PathA = "/srv/app.py"
  PathB = "/srv/<frozen importlib._bootstrap>"
  PathC = "/srv/pkg/__init__.py"

proc ffiLastError(): string =
  let e = trace_writer_last_error()
  if e.isNil: "" else: $e

proc columnAwareHandle(): TraceWriterHandle =
  setError("")
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert h != nil, "trace_writer_new failed: " & ffiLastError()
  doAssert trace_writer_begin_in_memory(h) == 0, ffiLastError()
  trace_writer_enable_column_aware_steps(h)
  h

proc register(h: TraceWriterHandle, path: string,
    table: openArray[uint32]): cint =
  if table.len == 0:
    trace_writer_register_path_with_line_lengths(h, cstring(path), 0, nil)
  else:
    trace_writer_register_path_with_line_lengths(h, cstring(path),
      cint(table.len), cast[ptr UncheckedArray[uint32]](unsafeAddr table[0]))

proc containerOf(h: TraceWriterHandle): NewTraceReader =
  let n = int(trace_writer_container_len(h))
  var bytes = newSeq[byte](n)
  copyMem(addr bytes[0], trace_writer_container_ptr(h), n)
  ntr.openNewTraceFromBytes(bytes).get()

proc test_c_tables_are_decided_at_first_mention() =
  let h = columnAwareHandle()
  doAssert h.register(PathA, [5'u32, 5]) == 0, ffiLastError()
  doAssert h.register(PathC, [0'u32, 0]) == 0, ffiLastError()
  doAssert h.register("/srv/no_table.py", []) == 0, ffiLastError()
  trace_writer_start(h, cstring(PathA), 1)
  # PathB is first mentioned by a step, with a column past 1024.
  trace_writer_register_step(h, cstring(PathB), 7)
  trace_writer_register_delta_column(h, 4999)
  trace_writer_register_step(h, cstring(PathC), 1)
  trace_writer_register_step(h, cstring(PathA), 2)
  doAssert ffiLastError().len == 0, ffiLastError()
  doAssert trace_writer_close(h) == 0, "trace_writer_close: " & ffiLastError()
  var r = h.containerOf()
  trace_writer_free(h)

  doAssert r.path(3).get() == PathB
  for f in [2'u64, 3]:
    doAssert r.lineCountRaw(f) == 100000 and
      r.lineLengthRaw(f, 0).get() == 1024 and
      r.lineLengthRaw(f, 99999).get() == 1024,
      "file " & $f & " was first mentioned without a table and must have " &
      "the conventional one; it has " & $r.lineCountRaw(f) & " lines"
  doAssert r.lineCountRaw(1) == 2 and r.lineLengthRaw(1, 0).get() == 1 and
    r.lineLengthRaw(1, 1).get() == 0, "[0, 0] must be recorded as [1, 0]"
  var found = false
  for i in 0'u64 ..< r.stepCount().get():
    let g = r.stepAbsoluteGlobalLineIndex(i)
    if g.isErr: continue
    let d = r.decodeGlobalPositionIndex(g.get())
    if d.isOk and d.get().file == 3:
      doAssert d.get() == (file: 3'u64, line: 7'u32, column: 1024'u32),
        "column 5000 must be recorded at column 1024 of line 7: " & $d.get()
      found = true
  doAssert found, "the step on the conventional file is in the container"
  echo "PASS: test_c_tables_are_decided_at_first_mention"

proc test_c_reader_names_the_conventional_table() =
  ## Through the reader C ABI: the kind is explicit, and the per-line
  ## accessors answer the conventional table by its rule rather than 0.
  let h = columnAwareHandle()
  doAssert h.register(PathA, [5'u32, 5]) == 0, ffiLastError()
  trace_writer_start(h, cstring(PathA), 1)
  trace_writer_register_step(h, cstring(PathB), 7)
  trace_writer_register_step(h, cstring(PathA), 2)
  doAssert trace_writer_close(h) == 0, ffiLastError()
  let n = int(trace_writer_container_len(h))
  var bytes = newSeq[byte](n)
  copyMem(addr bytes[0], trace_writer_container_ptr(h), n)
  trace_writer_free(h)
  let ct = getTempDir() / "ffi_conventional_kind.ct"
  writeFile(ct, cast[string](bytes))
  let r = ct_reader_open(cstring(ct))
  doAssert r != nil
  doAssert ct_reader_path_table_kind(r, 0) == CtPathTableLines
  doAssert ct_reader_path_table_kind(r, 1) == CtPathTableConventional
  doAssert ct_reader_path_table_kind(r, 2) == -1, "no such path"
  doAssert ct_reader_line_count_raw(r, 1) == 100000
  var width: uint32
  doAssert ct_reader_line_length_raw(r, 1, 99999, addr width) == 0 and width == 1024
  doAssert ct_reader_line_length_raw(r, 1, 100000, addr width) != 0
  ct_reader_close(r)
  echo "PASS: test_c_reader_names_the_conventional_table"

proc test_c_refuses_a_line_past_the_conventional_table() =
  let h = columnAwareHandle()
  trace_writer_start(h, cstring(PathB), 1)
  trace_writer_register_step(h, cstring(PathB), 100000)
  trace_writer_register_step(h, cstring(PathB), 2)
  doAssert ffiLastError().len == 0, "line 100000 is in range: " & ffiLastError()
  trace_writer_register_step(h, cstring(PathB), 100001)
  trace_writer_register_step(h, cstring(PathB), 2)
  doAssert PathB in ffiLastError() and "100001" in ffiLastError(),
    "a line past 100000 must be refused by name; last_error: " & ffiLastError()
  doAssert trace_writer_close(h) != 0, "the refusal must fail the recording"
  doAssert PathB in ffiLastError(), "close must name it: " & ffiLastError()
  trace_writer_free(h)
  echo "PASS: test_c_refuses_a_line_past_the_conventional_table"

proc test_c_refuses_a_table_after_the_file_was_interned() =
  let h = columnAwareHandle()
  doAssert h.register(PathA, [5'u32, 5]) == 0, ffiLastError()
  trace_writer_start(h, cstring(PathA), 1)
  trace_writer_register_step(h, cstring(PathC), 1)
  trace_writer_register_step(h, cstring(PathA), 2)
  doAssert ffiLastError().len == 0, ffiLastError()
  doAssert h.register(PathA, [5'u32, 5]) == 0,
    "the recorded table again is accepted: " & ffiLastError()
  doAssert h.register(PathC, [3'u32, 4]) != 0,
    "a table after a step interned the file must be refused"
  doAssert PathC in ffiLastError() and "first interned" in ffiLastError(),
    "the refusal must name the path; last_error: " & ffiLastError()
  doAssert trace_writer_close(h) != 0, "the refusal must fail the recording"
  doAssert PathC in ffiLastError(), "close must name it: " & ffiLastError()
  trace_writer_free(h)
  echo "PASS: test_c_refuses_a_table_after_the_file_was_interned"

test_c_tables_are_decided_at_first_mention()
test_c_reader_names_the_conventional_table()
test_c_refuses_a_table_after_the_file_was_interned()
test_c_refuses_a_line_past_the_conventional_table()
echo "ALL PASS: test_ffi_column_table_decided_at_first_mention"
