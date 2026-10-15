when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Flushing and closing a `File` with the C library's result checked.
##
## `std/syncio`'s `flushFile` discards what `fflush` returns, and its `close`
## discards what `fclose` returns (unless the whole program is built with
## `-d:nimPreviewCheckedClose`). A write that `fwrite` only buffered reaches
## the operating system in that flush, so a failure there — `ENOSPC`, `EFBIG`,
## `EIO` — is the only report of a lost tail the writer ever gets. Every
## writer in this library that hands a file to a caller as finished flushes
## and closes it through here.
##
## The C symbols are the ones `std/syncio` already references, so this adds
## nothing to the import surface of a freestanding build.

import results

proc c_fflush(f: File): cint {.importc: "fflush", header: "<stdio.h>".}
proc c_fclose(f: File): cint {.importc: "fclose", header: "<stdio.h>".}
proc c_strerror(errnum: cint): cstring {.importc: "strerror",
  header: "<string.h>".}
var c_errno {.importc: "errno", header: "<errno.h>".}: cint

proc lastCError(): string =
  let e = c_errno
  "errno " & $e & " (" & $c_strerror(e) & ")"

proc flushChecked*(f: File): Result[void, string] =
  ## `fflush(f)`, failing with the C library's error when it fails.
  if c_fflush(f) != 0:
    return err("flush failed: " & lastCError())
  ok()

proc closeChecked*(f: var File): Result[void, string] =
  ## `fclose(f)`, failing with the C library's error when it fails. `fclose`
  ## flushes what is still buffered first, so this is the last chance to see a
  ## write fail. The stream is released either way and `f` is set to nil.
  if f.isNil:
    return ok()
  let rc = c_fclose(f)
  f = nil
  if rc != 0:
    return err("close failed: " & lastCError())
  ok()

proc writeAllChecked*(f: File, data: pointer, len: int): Result[void, string] =
  ## Write `len` bytes at the current position, failing on a short write or an
  ## error `fwrite` reports.
  if len <= 0:
    return ok()
  try:
    let n = f.writeBuffer(data, len)
    if n != len:
      return err("short write: " & $n & " of " & $len & " bytes")
  except IOError as e:
    return err("write failed: " & e.msg)
  ok()

proc writeFileChecked*(path: string, data: openArray[byte]): Result[void, string] =
  ## Create or truncate `path` and write `data` to it, failing when any write,
  ## the final flush or the close fails. `std/syncio`'s `writeFile` reports
  ## none of the last two.
  var f: File
  try:
    if not open(f, path, fmWrite):
      return err("cannot open " & path & " for writing")
  except IOError, OSError:
    return err("cannot open " & path & " for writing")
  let wrote =
    if data.len > 0: f.writeAllChecked(unsafeAddr data[0], data.len)
    else: Result[void, string].ok()
  let closed = f.closeChecked()
  if wrote.isErr:
    return err(path & ": " & wrote.error)
  if closed.isErr:
    return err(path & ": " & closed.error)
  ok()
