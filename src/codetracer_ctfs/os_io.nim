when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Positioned writes and reads on a container file's operating-system handle.
##
## `ctfs-container.md` §6 orders a writer's writes ("Writing through the
## page"; the seal phases of `ctfs-keyed-families.md` §5.8.0): data before the
## entry that publishes it, a node's new slots before its count, each
## published entry or record word in one write call of its own. A stdio
## stream buffers and coalesces, so the order and the number of the calls that
## reach the operating system are not the order of the program's writes. The
## writer therefore hands every byte of a streaming container to the operating
## system here, one `pwrite` (POSIX) or one positioned `WriteFile` (Windows)
## per call, repeated only to finish a short write. Nothing is buffered in
## between, so "handed to the operating system" (§6, "Durability", rule 5)
## holds when the call returns.
##
## Reads are positioned too (`pread`, `ReadFile` with an offset), so a reader
## that follows a container never moves a shared file position.

import results

when defined(windows):
  type
    WinHandle = int
    Overlapped {.pure.} = object
      internal: uint
      internalHigh: uint
      offset: uint32
      offsetHigh: uint32
      hEvent: WinHandle

  proc writeFileWin(h: WinHandle, buf: pointer, n: int32, written: ptr int32,
                    ov: ptr Overlapped): int32 {.
    stdcall, dynlib: "kernel32", importc: "WriteFile".}
  proc readFileWin(h: WinHandle, buf: pointer, n: int32, got: ptr int32,
                   ov: ptr Overlapped): int32 {.
    stdcall, dynlib: "kernel32", importc: "ReadFile".}
  proc getLastErrorWin(): int32 {.stdcall, dynlib: "kernel32",
    importc: "GetLastError".}
  const ErrorHandleEof = 38'i32
else:
  import std/posix

proc osHandleOf(f: File): int =
  when defined(windows): int(getOsFileHandle(f))
  else: int(getFileHandle(f))

proc writeAtOs*(f: File, offset: int64, data: pointer, len: int): Result[void, string] =
  ## Hand `len` bytes at `data` to the operating system at file offset
  ## `offset`, in one call, repeated only to finish a short write.
  if len <= 0:
    return ok()
  let h = osHandleOf(f)
  var done = 0
  while done < len:
    let p = cast[pointer](cast[uint](data) + uint(done))
    let at = offset + int64(done)
    when defined(windows):
      var ov: Overlapped
      ov.offset = uint32(uint64(at) and 0xFFFF_FFFF'u64)
      ov.offsetHigh = uint32(uint64(at) shr 32)
      var wrote: int32 = 0
      let chunk = int32(min(len - done, 1 shl 30))
      if writeFileWin(WinHandle(h), p, chunk, addr wrote, addr ov) == 0:
        return err("WriteFile at offset " & $at & " failed: error " &
          $getLastErrorWin())
      if wrote <= 0:
        return err("WriteFile at offset " & $at & " wrote nothing")
      done += int(wrote)
    else:
      let n = pwrite(cint(h), p, len - done, Off(at))
      if n < 0:
        if errno == EINTR: continue
        return err("pwrite at offset " & $at & " failed: errno " & $errno &
          " (" & $strerror(errno) & ")")
      if n == 0:
        return err("pwrite at offset " & $at & " wrote nothing")
      done += n
  ok()

proc readAtOs*(f: File, offset: int64, data: pointer, len: int): int =
  ## Read up to `len` bytes at file offset `offset`; how many were read,
  ## short where the file ends first, -1 where it cannot be read.
  if len <= 0:
    return 0
  let h = osHandleOf(f)
  var done = 0
  while done < len:
    let p = cast[pointer](cast[uint](data) + uint(done))
    let at = offset + int64(done)
    when defined(windows):
      var ov: Overlapped
      ov.offset = uint32(uint64(at) and 0xFFFF_FFFF'u64)
      ov.offsetHigh = uint32(uint64(at) shr 32)
      var got: int32 = 0
      let chunk = int32(min(len - done, 1 shl 30))
      if readFileWin(WinHandle(h), p, chunk, addr got, addr ov) == 0:
        if getLastErrorWin() == ErrorHandleEof:
          break
        return -1
      if got <= 0:
        break
      done += int(got)
    else:
      let n = pread(cint(h), p, len - done, Off(at))
      if n < 0:
        if errno == EINTR: continue
        return -1
      if n == 0:
        break
      done += n
  done

proc fileLength*(f: File): int64 =
  ## The file's current length, or -1 when it cannot be read.
  try:
    int64(getFileSize(f))
  except IOError:
    -1
