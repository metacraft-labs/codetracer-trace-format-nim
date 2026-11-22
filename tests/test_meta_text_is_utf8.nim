## The text meta.dat carries is UTF-8 (`internal-files.md` §"Metadata
## (meta.dat)": program, arguments, working directory, recorder id, the MCR
## block's strings and the filter-provenance paths are "varint length +
## UTF-8 bytes"). Bytes that are not UTF-8 are refused where they enter, by
## every C ABI entry point that takes them and by the encoder, rather than
## written into a member the spec says holds text.
##
## No mocks: the real C entry points and the real encoder.

include codetracer_trace_writer_ffi

{.pop.}

import std/strutils

proc lastErr(): string =
  let e = trace_writer_last_error()
  if e.isNil: "" else: $e

const Bad = "b\xff\xfe" & "d"
const Rid = "01890a5d-ac96-774b-bcce-b302099a8057"

trace_writer_clear_last_error()
doAssert trace_writer_new(cstring("/src/" & Bad & ".py"), ffiBinary) == nil,
  "a program name that is not UTF-8 is refused"
doAssert "UTF-8" in lastErr(), lastErr()

proc writer(): TraceWriterHandle =
  result = trace_writer_new(cstring("/src/ok.py"), ffiBinary)
  doAssert result != nil, lastErr()
  doAssert trace_writer_set_recording_id(result, cstring(Rid)) == 0, lastErr()
  doAssert trace_writer_begin_in_memory(result) == 0, lastErr()

block workdir:
  let h = writer()
  trace_writer_clear_last_error()
  trace_writer_set_workdir(h, cstring("/w" & Bad))
  doAssert "UTF-8" in lastErr(), "set_workdir refuses: " & lastErr()
  trace_writer_free(h)

block args:
  let h = writer()
  let a = "a" & Bad
  var p = cast[ptr uint8](unsafeAddr a[0])
  var n = csize_t(a.len)
  trace_writer_clear_last_error()
  trace_writer_set_args(h, addr p, addr n, 1)
  doAssert "UTF-8" in lastErr(), "set_args refuses: " & lastErr()
  trace_writer_free(h)

block mcr:
  let h = writer()
  var strategies = [cstring("ok")]
  doAssert trace_writer_set_mcr_fields(h, 0, 1, 0, 1, 1, 1, cstring("p" & Bad),
    "g", "ts", "am", "st", "hp",
    cast[ptr UncheckedArray[cstring]](addr strategies[0]), 1) != 0,
    "an MCR string that is not UTF-8 is refused"
  doAssert "UTF-8" in lastErr(), lastErr()
  var bad = [cstring("s" & Bad)]
  doAssert trace_writer_set_mcr_fields(h, 0, 1, 0, 1, 1, 1, "p", "g", "ts",
    "am", "st", "hp", cast[ptr UncheckedArray[cstring]](addr bad[0]), 1) != 0,
    "a hook strategy that is not UTF-8 is refused"
  trace_writer_free(h)

block provenance:
  let h = writer()
  let path = "f" & Bad
  var sha: array[32, uint8]
  doAssert trace_writer_add_filter_provenance(h,
    cast[ptr uint8](unsafeAddr path[0]), csize_t(path.len), addr sha[0], 32) != 0,
    "a filter path that is not UTF-8 is refused"
  doAssert "UTF-8" in lastErr(), lastErr()
  trace_writer_free(h)

block toBuffer:
  let prog = "p" & Bad
  var outBuf: ptr uint8
  var outLen: csize_t
  doAssert ct_write_meta_dat_to_buffer(cast[ptr uint8](unsafeAddr prog[0]),
    csize_t(prog.len), nil, 0, nil, nil, 0, nil, 0, nil, 0,
    addr outBuf, addr outLen) != 0, "ct_write_meta_dat_to_buffer refuses"
  doAssert "UTF-8" in lastErr(), lastErr()

block encoder:
  let meta = TraceMetadata(recordingId: Rid, program: "p", args: @[],
    workdir: "/w" & Bad)
  let enc = encodeMetaDat(meta, MetaDatFlagsInput())
  doAssert enc.isErr and "UTF-8" in enc.error, "the encoder refuses too"

echo "test_meta_text_is_utf8: OK"
