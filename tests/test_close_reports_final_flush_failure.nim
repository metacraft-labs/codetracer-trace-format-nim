## A writer that hands a container back as finished reports a failure of the
## last write that reaches the operating system: the flush `fclose` and
## `fflush` perform.
##
## `fwrite` only buffers the tail of a write that is not a multiple of the
## stdio buffer; the bytes reach the file in the final `fflush` / `fclose`.
## When that write fails — a full disk (`ENOSPC`), a file-size limit
## (`EFBIG`), an I/O error — the C library says so only in those two return
## values. A writer that drops them returns success for a container whose
## tail, and with it the root region's view of the members, is missing.
##
## HOW THE FAILURE IS PRODUCED. Nothing is mocked or faked. Each case runs in
## a forked child that sets `RLIMIT_FSIZE` and ignores `SIGXFSZ`, so a
## `write(2)` past the limit fails with `EFBIG` on the real filesystem. The
## limit is chosen so that every write the writer makes directly succeeds and
## only the buffered tail crosses it: the 1024-byte block size makes the
## image a whole number of blocks that is NOT a whole number of 4096-byte
## stdio buffers, and the limit is the largest buffer multiple below the
## image's length. The parent then checks the file holds exactly the bytes
## below the limit, which shows the earlier writes did land and the failure
## was the flush alone.
##
## The child reports through its exit status: 0 when the writer returned the
## failure, 1 when it returned success, 2 when the case's own setup broke.

import std/[os, posix]
import results
import codetracer_ctfs

when not defined(linux):
  {.error: "this test relies on Linux RLIMIT_FSIZE / EFBIG semantics".}

var RLIMIT_FSIZE {.importc: "RLIMIT_FSIZE", header: "<sys/resource.h>".}: cint

const
  BlockSize = 1024'u32
  StdioBuffer = 4096
    ## glibc sizes a file's stdio buffer by the file system's `st_blksize`,
    ## 4096 on the file systems this runs on.

proc limitFileSize(limit: int) =
  signal(SIGXFSZ, SIG_IGN)
  var rl = RLimit(rlim_cur: limit, rlim_max: limit)
  if setrlimit(RLIMIT_FSIZE, rl) != 0:
    echo "  child: setrlimit failed"
    exitnow(2)

proc payload(n: int): seq[byte] =
  result = newSeq[byte](n)
  for i in 0 ..< n:
    result[i] = byte((i * 31 + 7) and 0xff)

proc report(what: string, r: Result[void, string]) =
  if r.isErr:
    echo "  child: ", what, " reported: ", r.error
    stdout.flushFile()
    exitnow(0)
  echo "  child: ", what, " reported success"
  stdout.flushFile()
  exitnow(1)

proc setup[T](r: Result[T, string]): T =
  ## The value of a setup step in the child, which exits 2 when it fails.
  if r.isErr:
    echo "  child: setup failed: ", r.error
    stdout.flushFile()
    exitnow(2)
  when T isnot void:
    r.get()

proc fileLen(path: string): int =
  int(getFileSize(path))

proc runChild(body: proc () {.closure.}): int =
  stdout.flushFile()
  let pid = fork()
  doAssert pid >= 0, "fork failed"
  if pid == 0:
    try:
      body()
    except CatchableError as e:
      echo "  child: raised ", e.msg
    stdout.flushFile()
    exitnow(2)
  var status: cint
  doAssert waitpid(pid, status, 0) == pid
  doAssert WIFEXITED(status), "child did not exit normally"
  int(WEXITSTATUS(status))

var failures: seq[string]
  ## Every case runs, so one run shows which writers drop the failure.

proc expectReported(name: string, rc: int) =
  doAssert rc != 2, name & ": the case's setup failed in the child"
  if rc != 0:
    failures.add(name & ": the writer returned success although the final " &
      "flush failed with EFBIG; the file on disk is truncated")

proc expectLen(name, path: string, len: int) =
  ## The bytes below the limit landed: the writes before the flush succeeded.
  if fileLen(path) != len:
    failures.add(name & ": expected the " & $len & " bytes below the " &
      "limit on disk, found " & $fileLen(path))

proc limitBelow(len: int): int =
  doAssert len mod StdioBuffer != 0,
    "image of " & $len & " bytes leaves no buffered tail"
  (len div StdioBuffer) * StdioBuffer

proc test_closeCtfs_reports_failed_final_flush() =
  let path = getTempDir() / "test_close_flush_failure_stream.ct"
  removeFile(path)
  # The same image built in memory gives the length the streaming one has.
  var probe = createCtfs(BlockSize)
  var pf = probe.addFile("a.dat").get()
  doAssert probe.writeToFile(pf, payload(4000)).isOk
  let limit = limitBelow(probe.data.len)
  let rc = runChild(proc () =
    var c = setup createCtfsStreaming(path, BlockSize)
    var f = setup c.addFile("a.dat")
    setup c.writeToFile(f, payload(4000))
    if c.data.len != probe.data.len or fileLen(path) > limit:
      echo "  child: image of ", c.data.len, " bytes, ", fileLen(path),
        " already on disk; limit ", limit
      exitnow(2)
    limitFileSize(limit)
    report("closeCtfs", c.closeCtfs()))
  expectReported("closeCtfs", rc)
  expectLen("closeCtfs", path, limit)
  removeFile(path)
  echo "DONE: test_closeCtfs_reports_failed_final_flush"

proc test_writeCtfsToFile_reports_failed_close() =
  let path = getTempDir() / "test_close_flush_failure_whole.ct"
  removeFile(path)
  var c = createCtfs(BlockSize)
  var f = c.addFile("a.dat").get()
  doAssert c.writeToFile(f, payload(4000)).isOk
  let limit = limitBelow(c.data.len)
  let rc = runChild(proc () =
    limitFileSize(limit)
    report("writeCtfsToFile", c.writeCtfsToFile(path)))
  expectReported("writeCtfsToFile", rc)
  expectLen("writeCtfsToFile", path, limit)
  removeFile(path)
  echo "DONE: test_writeCtfsToFile_reports_failed_close"

proc test_createCtfsStreaming_reports_failed_initial_flush() =
  ## The root region written at open is smaller than the stdio buffer, so it
  ## reaches the file only in the flush that follows.
  let path = getTempDir() / "test_close_flush_failure_open.ct"
  removeFile(path)
  let rc = runChild(proc () =
    limitFileSize(0)
    let r = createCtfsStreaming(path, BlockSize)
    report("createCtfsStreaming",
      if r.isErr: Result[void, string].err(r.error)
      else: Result[void, string].ok()))
  expectReported("createCtfsStreaming", rc)
  removeFile(path)
  echo "DONE: test_createCtfsStreaming_reports_failed_initial_flush"

proc test_append_reports_failed_tail_flush() =
  ## The appended tail is one block, so it sits in the stdio buffer until the
  ## barrier before the root rewrite flushes it. When that flush fails the
  ## root region must not be rewritten to point at blocks that are not there.
  let path = getTempDir() / "test_close_flush_failure_append.ct"
  removeFile(path)
  var c = createCtfs(BlockSize)
  var f = c.addFile("a.dat").get()
  doAssert c.writeToFile(f, payload(4000)).isOk
  doAssert c.writeCtfsToFile(path).isOk
  let before = readCtfsFromFile(path).get()
  let rc = runChild(proc () =
    limitFileSize(before.len)
    report("appendInternalFiles",
      appendInternalFiles(path, ["b.dat"], [payload(500)])))
  expectReported("appendInternalFiles", rc)
  let after = readCtfsFromFile(path).get()
  if after != before:
    failures.add("appendInternalFiles: the failed append changed the " &
      "container on disk (" & $before.len & " bytes before, " & $after.len &
      " after)")
  removeFile(path)
  echo "DONE: test_append_reports_failed_tail_flush"

test_closeCtfs_reports_failed_final_flush()
test_writeCtfsToFile_reports_failed_close()
test_createCtfsStreaming_reports_failed_initial_flush()
test_append_reports_failed_tail_flush()

for f in failures:
  echo "FAIL: ", f
doAssert failures.len == 0, $failures.len & " writer(s) dropped a failed flush"
echo "PASS: every writer reports a failed final flush"
