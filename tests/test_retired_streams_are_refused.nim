## `events.log` and `events.fmt` are not part of the trace format: every
## reader in this repository refuses a container that carries either, with an
## error that names the member.
##
## Each container is a real recording written by the split-stream writer, with
## the retired member appended beside its streams by the container's own
## append path, so the refusal cannot be explained by anything else the
## container lacks. A reader that ignored the member would open the recording
## successfully and the test fails then. No mocks.

include codetracer_trace_writer_ffi

{.pop.}

import std/[os, osproc, strutils]
import ct_print_binary
import codetracer_ctfs/container_append
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_reader

proc recording(path: string) =
  removeFile(path)
  var w = initMultiStreamWriter(path, "retired").get()
  let p = w.registerPath("/src/r.py").get()
  doAssert w.registerStep(p, 2, []).isOk
  doAssert w.registerStep(p, 3, []).isOk
  doAssert w.close().isOk
  doAssert w.closeCtfs().isOk

proc bytesOf(path: string): seq[byte] =
  let s = readFile(path)
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len: result[i] = byte(s[i])

proc expectNamed[T](r: Result[T, string], member, entry: string) =
  doAssert r.isErr, entry & " read a container carrying `" & member & "`"
  doAssert member in r.unsafeError,
    entry & " refused, but without naming `" & member & "`: " & r.unsafeError

proc refusedEverywhere(path, member: string) =
  expectNamed(openNewTrace(path), member, "openNewTrace")
  expectNamed(openNewTraceFromBytes(bytesOf(path)), member,
    "openNewTraceFromBytes")
  expectNamed(openTrace(path), member, "openTrace")

  let h = ct_reader_open(cstring(path))
  doAssert h.isNil, "ct_reader_open read a container carrying `" & member & "`"
  doAssert member in $trace_writer_last_error(),
    "ct_reader_open refused without naming `" & member & "`: " &
    $trace_writer_last_error()
  var bytes = bytesOf(path)
  let hb = ct_reader_open_bytes(addr bytes[0], csize_t(bytes.len))
  doAssert hb.isNil, "ct_reader_open_bytes read a container carrying `" &
    member & "`"
  doAssert member in $trace_writer_last_error()

  for format in ["--json-events", "--summary"]:
    let (output, code) = execCmdEx(quoteShell(ensureCtPrint()) & " " & format &
      " " & quoteShell(path))
    doAssert code != 0, "ct-print " & format & " read a container carrying `" &
      member & "`:\n" & output
    doAssert member in output, "ct-print " & format &
      " refused without naming `" & member & "`:\n" & output

proc withMember(member: string, content: seq[byte]): string =
  result = getTempDir() / ("retired_" & member.replace(".", "_") & ".ct")
  recording(result)
  doAssert appendInternalFiles(result, [member], [content]).isOk

proc test_the_recording_itself_reads() =
  let path = getTempDir() / "retired_control.ct"
  recording(path)
  doAssert openNewTrace(path).isOk
  doAssert openTrace(path).isOk
  let h = ct_reader_open(cstring(path))
  doAssert not h.isNil, $trace_writer_last_error()
  ct_reader_close(h)
  removeFile(path)
  echo "PASS: test_the_recording_itself_reads"

proc test_events_log_is_refused_by_name() =
  let path = withMember("events.log", @[0'u8])
  refusedEverywhere(path, "events.log")
  removeFile(path)
  echo "PASS: test_events_log_is_refused_by_name"

proc test_events_fmt_is_refused_by_name() =
  let path = withMember("events.fmt", @[byte('s')])
  refusedEverywhere(path, "events.fmt")
  removeFile(path)
  echo "PASS: test_events_fmt_is_refused_by_name"

proc test_an_empty_events_log_is_refused_too() =
  ## The entry is what is refused, not its content.
  let path = withMember("events.log", @[])
  refusedEverywhere(path, "events.log")
  removeFile(path)
  echo "PASS: test_an_empty_events_log_is_refused_too"

proc test_the_writer_refuses_the_formats_that_selected_it() =
  for f in [ffiJson, ffiBinaryV0]:
    let h = trace_writer_new(cstring("legacy"), f)
    doAssert h.isNil, "trace_writer_new accepted format " & $ord(f)
    doAssert "events.log" in $trace_writer_last_error(),
      $trace_writer_last_error()
  let h = trace_writer_new(cstring("current"), ffiBinary)
  doAssert not h.isNil
  trace_writer_free(h)
  echo "PASS: test_the_writer_refuses_the_formats_that_selected_it"

test_the_recording_itself_reads()
test_events_log_is_refused_by_name()
test_events_fmt_is_refused_by_name()
test_an_empty_events_log_is_refused_too()
test_the_writer_refuses_the_formats_that_selected_it()
