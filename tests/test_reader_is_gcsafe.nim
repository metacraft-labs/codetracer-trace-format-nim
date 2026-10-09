## The reader can be called from GC-safe code, under every memory manager.
##
## A consumer that reads a trace on a worker, or hands a reading proc to a
## `{.gcsafe.}` proc type, needs every entry point it reaches to be inferred
## GC-safe. Nim infers that from the body of each callee, and a proc that is
## called before its body has been seen — through a forward declaration with
## no effects stated — is taken to be NOT GC-safe. One such declaration deep
## in the stream readers is enough to make `openNewTrace`, `call` and
## `stepAbsoluteGlobalLineIndex` unusable from GC-safe code, with an error
## that names the consumer's proc and not the declaration that caused it.
##
## Each proc below is annotated `{.gcsafe.}` and calls one entry point the way
## a consumer does, so a reader proc that stops being GC-safe fails this file
## to compile, naming the call chain. The `test` task compiles it under refc
## as well as the default memory manager, since consumers build with both; it
## then runs the procs over a container written by this repository's writer,
## so the calls are exercised and not only type-checked.

import std/[os, assertions]
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader

let dir = getTempDir() / "ctfnim-reader-is-gcsafe"
createDir(dir)

proc openTrace(path: string): Result[NewTraceReader, string] {.gcsafe.} =
  openNewTrace(path)

proc openBytes(bytes: seq[byte]): Result[NewTraceReader, string] {.gcsafe.} =
  openNewTraceFromBytes(bytes)

proc firstCall(r: var NewTraceReader):
    Result[(string, string, uint32), string] {.gcsafe.} =
  ## What a dependency scan reads: the call count, a call, its function's
  ## name, and the position of its entry step.
  let count = ? r.callCount()
  if count == 0:
    return err("no calls")
  let c = ? r.call(0)
  let name = ? r.function(c.functionId)
  let gli = ? r.stepAbsoluteGlobalLineIndex(c.entryStep)
  let pos = ? r.decodeGlobalPositionIndex(gli)
  let file = ? r.path(pos.file)
  ok((name, file, pos.line))

proc writeTrace(name: string): string =
  var w = initMultiStreamWriter(dir / name & ".build", name).get()
  doAssert w.enableColumnAwareSteps().isOk
  let p = w.registerPath("/src/app.py", [10'u32, 10, 10])
  doAssert p.isOk, p.error
  let f = w.registerFunctionAt("/src/app.py", 2, "main")
  doAssert f.isOk, f.error
  doAssert w.registerStep(p.get(), 1, @[]).isOk
  doAssert w.registerCall(f.get(), @[]).isOk
  doAssert w.registerStep(p.get(), 2, @[]).isOk
  doAssert w.registerReturn().isOk
  let closed = w.close()
  doAssert closed.isOk, "close: " & closed.error
  result = dir / name & ".ct"
  writeFile(result, cast[string](w.toBytes()))
  discard w.closeCtfs()

proc test_the_reader_entry_points_run_from_gcsafe_procs() =
  let file = writeTrace("gcsafe")
  var fromPath = openTrace(file)
  doAssert fromPath.isOk, "openNewTrace: " & fromPath.error
  let got = firstCall(fromPath.get())
  doAssert got.isOk, got.error
  doAssert got.get() == ("main", "/src/app.py", 2'u32), $got.get()

  var fromBytes = openBytes(cast[seq[byte]](readFile(file)))
  doAssert fromBytes.isOk, "openNewTraceFromBytes: " & fromBytes.error
  doAssert firstCall(fromBytes.get()) == got
  echo "PASS: test_the_reader_entry_points_run_from_gcsafe_procs"

test_the_reader_entry_points_run_from_gcsafe_procs()
