## A function names the version of its declaration file that was current when
## it was registered.
##
## A function's `funcs.dat` record is written at close, when the position
## space is complete, but which path id its declaration site belongs to is
## decided when it is registered: a bare path resolves to its newest version
## AT THAT POINT (`internal-files.md` §"`paths.dat` path versions"). A version
## registered later must not move it, or a function declared in the code the
## process started with reads back inside the reloaded file's range.
##
## A function whose file is not yet registered when it is registered is laid
## out where that file is first registered, as before.
##
## No mocks: a real writer writes a real container, read back by this
## repository's reader.

import std/os
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader

const A = "/src/a.gd"
const B = "/src/b.gd"

proc write(): seq[byte] =
  var w = initMultiStreamWriter(getTempDir() / "fn_before_version.ct",
    "fn_before_version").get()
  doAssert w.enableLineCountTable().isOk
  doAssert w.registerPath(A, lineCount = 10).isOk
  doAssert w.registerPath(B, lineCount = 20).isOk
  doAssert w.registerFunctionAt(A, 3, "before").get() == 0
  doAssert w.registerPathVersion(A, 12).get() == 2
  doAssert w.registerFunctionAt(A, 4, "after").get() == 1
  doAssert w.registerFunctionAt("/src/late.gd", 1, "unregistered").get() == 2
  doAssert w.registerPath("/src/late.gd", lineCount = 5).isOk
  doAssert w.registerStep(0, 1, @[]).isOk
  let closed = w.close()
  doAssert closed.isOk, closed.error
  result = w.toBytes()
  w.closeCtfs()

var r = openNewTraceFromBytes(write()).get()
let before = r.functionRecord(0).get().globalLineIndex
doAssert before == 2,
  "a function registered while version 0 of " & A & " was current is in its " &
  "range (line 3 -> address 2); got " & $before
let after = r.functionRecord(1).get().globalLineIndex
doAssert after == 10 + 20 + 3,
  "a function registered after the version is in the version's range " &
  "(base 30, line 4 -> 33); got " & $after
let late = r.functionRecord(2).get().globalLineIndex
doAssert late == 10 + 20 + 12,
  "a function whose file was registered after it is in that file's range " &
  "(base 42, line 1 -> 42); got " & $late
echo "test_function_declared_before_a_version: OK"
