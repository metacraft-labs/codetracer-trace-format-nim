## An empty name is a name: it round-trips through the C ABI, and the trace
## that holds it opens.
##
## The spec gives interned names no minimum length: `varnames.dat` records are
## raw bytes, `types.dat` and `funcs.dat` carry a length prefix, and a
## zero-length record is well-formed (internal-files.md §"Interning Tables").
## Recorders do write them (the Solana recorder registers empty type names).
##
## The reader's C ABI returns a heap copy of a name and NIL ON FAILURE. It used
## to return nil for an empty name too, so a caller could not tell "this name is
## empty" from "this lookup failed": the Rust wrapper read the nil as an error
## with an empty message, and a recording with one empty type name failed to
## open with "type 14:". An empty name now comes back as a non-nil,
## zero-length buffer (freed with `ct_free_buffer` like any other); nil stays
## the failure signal, with `last_error` set.
##
## Asserted, for an empty type name, variable name and function name, and for
## a non-empty one of each next to it: the writer accepts them, the reader
## returns non-nil with the right length and bytes, and an out-of-range id
## still fails (nil, with an error).
##
## No mocks: the real C ABI writer and reader over a real file.

include codetracer_trace_writer_ffi

{.pop.}

import std/[os, strutils]

proc nameOf(p: ptr uint8, n: csize_t): string =
  result = newString(int(n))
  if n > 0:
    copyMem(addr result[0], p, int(n))

proc check(label: string, get: proc (id: uint64, n: ptr csize_t): ptr uint8,
    id: uint64, expected: string) =
  var n: csize_t = 99
  let p = get(id, addr n)
  doAssert not p.isNil,
    label & " " & $id & ": the reader returned nil (the failure signal) for " &
    "the name '" & expected & "'; last error: '" & $trace_writer_last_error() & "'"
  doAssert int(n) == expected.len and nameOf(p, n) == expected,
    label & " " & $id & ": expected '" & expected & "', got " & $n & " bytes '" &
    nameOf(p, n) & "'"
  ct_free_buffer(p)

proc test_empty_names_round_trip_through_the_c_abi() =
  let dir = getTempDir() / ("ct_empty_names_" & $getCurrentProcessId())
  createDir(dir)
  defer: removeDir(dir)

  let w = trace_writer_new("empty_names", ffiBinary)
  doAssert w != nil
  doAssert trace_writer_begin_events(w, cstring(dir / "trace.json")) == 0
  let emptyType = trace_writer_ensure_type_id(w, FfiTypeKind(7), "")
  let namedType = trace_writer_ensure_type_id(w, FfiTypeKind(7), "i64")
  let emptyVar = trace_writer_register_variable_name(w, "")
  let namedVar = trace_writer_register_variable_name(w, "x")
  let emptyFn = trace_writer_ensure_function_id(w, "", "/src/a.nr", 3)
  let namedFn = trace_writer_ensure_function_id(w, "main", "/src/a.nr", 1)
  for id in [uint64(emptyType), uint64(namedType), emptyVar, namedVar,
             uint64(emptyFn), uint64(namedFn)]:
    doAssert id != high(uint64) and id != uint64(high(csize_t)),
      "the writer refused a registration: " & $trace_writer_last_error()
  trace_writer_register_step(w, "/src/a.nr", 1)
  doAssert trace_writer_close(w) == 0, $trace_writer_last_error()
  trace_writer_free(w)

  let r = ct_reader_open(cstring(dir / "empty_names.ct"))
  doAssert r != nil, "the trace did not open: " & $trace_writer_last_error()
  check("type", proc (id: uint64, n: ptr csize_t): ptr uint8 = ct_reader_type_name(r, id, n),
    uint64(emptyType), "")
  check("type", proc (id: uint64, n: ptr csize_t): ptr uint8 = ct_reader_type_name(r, id, n),
    uint64(namedType), "i64")
  check("varname", proc (id: uint64, n: ptr csize_t): ptr uint8 = ct_reader_varname(r, id, n),
    emptyVar, "")
  check("varname", proc (id: uint64, n: ptr csize_t): ptr uint8 = ct_reader_varname(r, id, n),
    namedVar, "x")
  check("function", proc (id: uint64, n: ptr csize_t): ptr uint8 = ct_reader_function(r, id, n),
    uint64(emptyFn), "")
  check("function", proc (id: uint64, n: ptr csize_t): ptr uint8 = ct_reader_function(r, id, n),
    uint64(namedFn), "main")

  # The failure signal still fails: an id past the table is nil, with an error.
  trace_writer_clear_last_error()
  var n: csize_t = 0
  doAssert ct_reader_type_name(r, 1_000_000, addr n).isNil,
    "an out-of-range type id did not fail"
  doAssert ($trace_writer_last_error()).len > 0,
    "an out-of-range type id failed without an error message"
  ct_reader_close(r)

when isMainModule:
  test_empty_names_round_trip_through_the_c_abi()
  echo "PASS test_empty_names_round_trip_through_the_c_abi"
