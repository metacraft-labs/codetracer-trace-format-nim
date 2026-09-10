## `close()` must publish the container's entry-size array.
##
## The root file-entry array lives in block 0 and holds every internal file's
## SIZE.  `writeToFile` keeps that size in the in-memory image and flushes only
## the data block it just filled; block 0 itself is rewritten by `addFile`,
## `truncateFileContent` and `closeCtfs`.  So a container read off disk between
## the last `addFile` and `closeCtfs` reports stale sizes — and for the file
## written LAST that stale size is **0**, because its entry was created empty
## and never republished.
##
## `meta.dat` is always the last file a trace writer writes, which is what made
## this visible: `close()` returned `ok`, the bytes were in the container, and
## every reader decoded an empty program with all capability flags clear and
## all four stream counts gated to "(unavailable)".  A recording that reports
## success and reads back empty is the exact silent-pass class this suite
## exists to catch, so the property is asserted here rather than left to the
## discipline of every caller remembering `closeCtfs`.
##
## No mocks: a real streaming container on a real filesystem, read back from
## the path with the canonical reader.

import std/os
import results
import ../src/codetracer_ctfs/container
import ../src/codetracer_trace_writer/multi_stream_writer

proc u32le(data: openArray[byte], off: int): uint32 =
  for i in 0 ..< 4:
    result = result or (uint32(data[off + i]) shl (i * 8))

proc member(data: seq[byte], name: string): Result[seq[byte], string] =
  readInternalFile(data, name, u32le(data, 8), u32le(data, 12))

proc test_close_alone_publishes_meta_dat() =
  ## The regression proper: `close()` WITHOUT `closeCtfs()`.
  let path = getTempDir() / "test_close_publishes_meta.ct"
  removeFile(path)

  block:
    var w = initMultiStreamWriter(path, "entry_sizes",
      recordingId = "01949fcc-7d92-7e9c-aaaa-eeeeeeeeeeee").get()
    doAssert w.registerPath("/src/app.py").isOk
    doAssert w.registerStep(0, 1, []).isOk
    doAssert w.registerStep(0, 2, []).isOk
    doAssert w.close().isOk
    # DELIBERATELY no closeCtfs(): this test is about what `close()` alone
    # guarantees.  Before the fix `meta.dat` came back at 0 bytes here.

  let data = readCtfsFromFile(path).get()
  let meta = member(data, "meta.dat")
  doAssert meta.isOk, "meta.dat entry missing after close(): " & meta.error
  doAssert meta.get().len > 0,
    "close() left meta.dat at size 0 on disk — the entry-size array in " &
    "block 0 was never published, so the whole recording reads as an empty " &
    "program"

  # Every stream the writer always emits must report its real size too, not
  # just the last one.
  for name in ["steps.dat", "steps.idx", "values.idx", "events.idx",
               "paths.dat"]:
    let m = member(data, name)
    doAssert m.isOk, name & " missing: " & m.error
    doAssert m.get().len > 0, name & " reads back empty after close()"

  removeFile(path)
  echo "PASS: test_close_alone_publishes_meta_dat"

proc test_close_then_close_ctfs_agrees() =
  ## The full sequence must be unchanged by the fix, and must agree with what
  ## `close()` alone already published.
  let path = getTempDir() / "test_close_publishes_meta_full.ct"
  removeFile(path)

  var afterClose: seq[byte]
  block:
    var w = initMultiStreamWriter(path, "entry_sizes",
      recordingId = "01949fcc-7d92-7e9c-aaaa-ffffffffffff").get()
    doAssert w.registerPath("/src/app.py").isOk
    doAssert w.registerStep(0, 1, []).isOk
    doAssert w.close().isOk
    afterClose = member(readCtfsFromFile(path).get(), "meta.dat").get()
    doAssert w.closeCtfs().isOk, "closeCtfs must report its own I/O failures"

  let afterCloseCtfs = member(readCtfsFromFile(path).get(), "meta.dat").get()
  doAssert afterClose == afterCloseCtfs,
    "meta.dat changed between close() and closeCtfs(): close() published " &
    $afterClose.len & " bytes, the finalized image has " &
    $afterCloseCtfs.len

  removeFile(path)
  echo "PASS: test_close_then_close_ctfs_agrees"

when isMainModule:
  test_close_alone_publishes_meta_dat()
  test_close_then_close_ctfs_agrees()
