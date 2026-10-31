## A C caller can register a path and a variable name, and each is interned
## when it is registered.
##
## The native API interns a path at `registerPath` and a variable name at
## `registerVarname`: the id is assigned in registration order, whether or not
## a record ever refers to it. Until these two entry points existed, a caller
## of the C ABI had no way to do either — a path joined `paths.dat` only when a
## step or a function referred to it, and a name joined `varnames.dat` only
## when a value used it. A caller that registers first and refers later (the
## Rust `TraceWriter` trait's `register_path` / `register_variable_name`, and
## every recorder built on it) therefore got different ids, a different
## `paths.dat`, and a different `varnames.dat` from the native writer fed the
## same registrations.
##
## What is asserted here, and why each one can fail:
##
##   1. A path registered through C and never stepped in is in `paths.dat`,
##      at the id the call returned, in registration order; and a later step
##      in a path registered earlier resolves to that earlier id. If the
##      entry point were a lookup, or deferred the record, the unreferenced
##      path would be missing and the ids would follow first use instead.
##   2. Registering the same path twice returns the same id and writes one
##      record.
##   3. A variable name registered through C and never given a value is in
##      `varnames.dat`, and a value registered later under that name uses its
##      id rather than minting a new one.
##   4. Under the line-count table a path registered without a count is
##      refused by name, exactly like the implicit registration a step
##      performs, so this entry point cannot put an unsized file into a table
##      whose every record must state a size.
##
## No mocks: the container is produced by the real C entry points and read
## back through this repository's own reader.

include codetracer_trace_writer_ffi

{.pop.}

import std/strutils
import codetracer_trace_writer/new_trace_reader as ntr

const Program = "ffi_register_path_and_varname"

proc ffiLastError(): string =
  let e = trace_writer_last_error()
  if e.isNil: "" else: $e

proc containerOf(h: TraceWriterHandle): seq[byte] =
  doAssert trace_writer_close(h) == 0, "trace_writer_close: " & ffiLastError()
  let n = int(trace_writer_container_len(h))
  doAssert n > 0, "the C ABI produced no container"
  result = newSeq[byte](n)
  copyMem(addr result[0], trace_writer_container_ptr(h), n)
  trace_writer_free(h)

proc test_a_registered_path_is_interned_at_registration() =
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert h != nil, "trace_writer_new failed: " & ffiLastError()
  doAssert trace_writer_begin_in_memory(h) == 0, ffiLastError()

  let a = trace_writer_register_path(h, "/src/a.nr")
  let never = trace_writer_register_path(h, "/src/never_stepped.nr")
  let b = trace_writer_register_path(h, "/src/b.nr")
  doAssert (a, never, b) == (0'u64, 1'u64, 2'u64),
    "paths are interned in registration order; got " & $(a, never, b) &
    " (" & ffiLastError() & ")"
  doAssert trace_writer_register_path(h, "/src/a.nr") == a,
    "registering a path twice must return its existing id"

  # Step in `b` before `a`: first use would give `b` id 0.
  trace_writer_register_step(h, "/src/b.nr", 3)
  trace_writer_register_step(h, "/src/a.nr", 4)

  var r = ntr.openNewTraceFromBytes(containerOf(h)).get()
  doAssert r.pathCount() == 3,
    "all three registered paths must be in paths.dat, the one never " &
    "stepped in included; got " & $r.pathCount()
  doAssert r.path(0).get() == "/src/a.nr" and
    r.path(1).get() == "/src/never_stepped.nr" and
    r.path(2).get() == "/src/b.nr",
    "paths.dat must hold the paths in registration order; got " &
    r.path(0).get() & ", " & r.path(1).get() & ", " & r.path(2).get()

  echo "PASS: test_a_registered_path_is_interned_at_registration"

proc test_a_registered_variable_name_is_interned_at_registration() =
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert trace_writer_begin_in_memory(h) == 0, ffiLastError()
  let unused = trace_writer_register_variable_name(h, "never_valued")
  let x = trace_writer_register_variable_name(h, "x")
  doAssert (unused, x) == (0'u64, 1'u64),
    "variable names are interned in registration order; got " & $(unused, x) &
    " (" & ffiLastError() & ")"
  doAssert trace_writer_register_variable_name(h, "x") == x,
    "registering a name twice must return its existing id"

  let intType = trace_writer_ensure_type_id(h, FfiTypeKind(7), "i64")
  trace_writer_register_step(h, "/src/a.nr", 1)
  trace_writer_register_variable_int_by_type_id(h, "x", 5, intType)

  var r = ntr.openNewTraceFromBytes(containerOf(h)).get()
  doAssert r.varnameCount() == 2,
    "both registered names must be in varnames.dat, the one never given a " &
    "value included, and the value must reuse `x`'s id; got " & $r.varnameCount()
  doAssert r.varname(0).get() == "never_valued" and r.varname(1).get() == "x",
    "varnames.dat must hold the names in registration order"

  echo "PASS: test_a_registered_variable_name_is_interned_at_registration"

proc test_an_uncounted_registration_is_refused_under_the_table() =
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert trace_writer_begin_in_memory(h) == 0
  doAssert trace_writer_enable_line_count_table(h) == 0, ffiLastError()
  let id = trace_writer_register_path(h, "/src/uncounted.nr")
  doAssert id == CtTwInvalidPathId,
    "a path with no count must be refused under the line-count table; got id " & $id
  doAssert "/src/uncounted.nr" in ffiLastError(),
    "the refusal must name the path; last_error was: " & ffiLastError()
  trace_writer_free(h)

  echo "PASS: test_an_uncounted_registration_is_refused_under_the_table"

test_a_registered_path_is_interned_at_registration()
test_a_registered_variable_name_is_interned_at_registration()
test_an_uncounted_registration_is_refused_under_the_table()
