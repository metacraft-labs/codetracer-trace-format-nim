## Writing a test fixture's bytes to a file, exactly.
##
## A helper module, not a test: it is imported by the tests that write a
## container image (or any other binary fixture) to disk, and is absent from
## the nimble `test` task and from `repro.nim`'s `testSpecs` on purpose.
##
## Why not `writeFile(path, cast[string](bytes))`: on Windows Nim's
## `write(File, string)` (which `writeFile` calls) goes through
## `fprintf("%s")`, so each write ends at a zero byte and the last one keeps
## reading memory until the next zero byte it finds. A `seq[byte]` cast to a
## string has no terminator after its last byte, so the file gets whatever
## heap bytes follow the buffer. Measured on Windows 11 with Nim 2.2.12: a
## 9052-byte container image written that way came out as 9306 bytes in one
## run and 9323 in another, with the extra bytes being leftovers from earlier
## allocations. The test that read it back (`test_member_view`) was comparing
## a file it never wrote. On POSIX `write` uses `fwrite`, so the defect never
## shows there.
##
## Why not the `openArray[byte]` overload of `writeFile`: it takes
## `content[0]`'s address unconditionally, so an empty fixture (which tests
## that refuse an empty file need) raises `IndexDefect` there.
##
## `writeBytesFile` writes exactly the given bytes with `writeBuffer`, writes
## an empty file for empty input, and raises `IOError` where `writeFile`
## would: when the file cannot be opened or the write comes up short.

when defined(nimPreviewSlimSystem):
  import std/syncio

proc writeBytesFile*(path: string, data: openArray[byte]) {.raises: [IOError].} =
  ## Create or truncate `path` and write exactly `data` to it.
  var f: File
  if not open(f, path, fmWrite):
    raise newException(IOError, "cannot open: " & path)
  try:
    if data.len > 0 and f.writeBuffer(unsafeAddr data[0], data.len) != data.len:
      raise newException(IOError, "short write to " & path)
  finally:
    close(f)
