## The container writer can be called from GC-safe code, under every memory
## manager.
##
## The sibling of `test_reader_is_gcsafe`, for the writing side. A consumer
## that writes a trace from a `{.gcsafe.}` proc (the native recorder's stream
## receiver, which runs on its own thread) needs every writer entry point it
## reaches to be inferred GC-safe. A proc called before its body has been seen,
## through a forward declaration with no effects stated, is taken to be NOT
## GC-safe, and the consumer's error names its own proc and not the
## declaration that caused it. Two such declarations were on these paths:
## `readMemberBytes` (reached by `publish`, so by every seal and by
## `closeCtfs`) and `closeLivePage` (reached by `createCtfsStreaming`).
##
## Each proc below is annotated `{.gcsafe.}` and calls one entry point the way
## a consumer does, so a writer proc that stops being GC-safe fails this file
## to compile, naming the call chain. The `test` task compiles it under refc
## as well as the default memory manager, since consumers build with both; it
## then runs the procs over a real streaming container in a fresh temporary
## directory and reads the result back, so the calls are exercised and not
## only type-checked.

import std/[os, assertions, tempfiles]
import results
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_ctfs/streaming

proc createStreaming(path: string): Result[Ctfs, string] {.gcsafe.} =
  createCtfsStreaming(path)

proc addMember(c: var Ctfs, name: string): Result[CtfsInternalFile, string]
    {.gcsafe.} =
  c.addFile(name)

proc appendMember(c: var Ctfs, f: var CtfsInternalFile,
                  data: openArray[byte]): Result[void, string] {.gcsafe.} =
  c.writeToFile(f, data)

proc sealMember(c: var Ctfs, f: CtfsInternalFile) {.gcsafe.} =
  c.syncEntry(f)

proc sealAll(c: var Ctfs) {.gcsafe.} =
  c.syncAllEntries()

proc publishAll(c: var Ctfs) {.gcsafe.} =
  c.publish()

proc closeContainer(c: var Ctfs): Result[void, string] {.gcsafe.} =
  c.closeCtfs()

proc main() =
  let dir = createTempDir("ctfs-writer-gcsafe-", "")
  defer: removeDir(dir)
  let path = dir / "writer.ct"
  var c = createStreaming(path).get()
  var f = c.addMember("events.dat").get()
  let payload = @[1'u8, 2, 3, 4, 5, 6, 7, 8]
  doAssert c.appendMember(f, payload).isOk
  c.sealMember(f)
  doAssert c.appendMember(f, payload).isOk
  c.sealAll()
  c.publishAll()
  doAssert c.closeContainer().isOk

  let bytes = readFile(path)
  var image = newSeq[byte](bytes.len)
  for i, ch in bytes: image[i] = byte(ch)
  let member = readInternalFile(image, "events.dat").get()
  doAssert member == payload & payload,
    "the container written through the GC-safe procs reads back wrong"

main()
