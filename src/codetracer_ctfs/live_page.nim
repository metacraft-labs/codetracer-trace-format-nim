when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions, sysatomics]

{.push raises: [].}

## The live coordination page (`ctfs-container.md` §6, "In-place record
## publication: the live coordination page", byte layout version 1).
##
## A shared-memory region, one per container being written, that holds the
## container's live root state and the live copy of every word a writer
## updates in place, each under a sequence counter. Writers publish there
## first; a reader on the same machine attaches and reads consistent values
## with the memory atomics; the file receives the same words at each seal.
## Nothing in the container file depends on any of it, and no byte of the file
## changes because of it.
##
## Layout (every field a little-endian `u64` at an 8-byte-aligned offset):
##
## | Offset | Field |
## |---|---|
## | 0 | `CTLP`, then `version: u32` = 1, written last at creation, with release |
## | 8 | identity, 32 bytes |
## | 40 | capacity |
## | 48 | state: 0 creating, 1 live, 2 closed, 3 abandoned |
## | 56 | `seal_epoch` (every writer's seals) |
## | 64 | `overflowed` (mirror allocations refused) |
## | 128 + 64·w | writer slot w (0..15): pid, start time, heartbeat, `seal_epoch`, state |
## | 1152 | `NextFreeBlock` |
## | 1160 + 8·f | family counter f (0..15) |
## | 1288 | `mirror_bump` |
## | 1296 | `extents_used` |
## | 4096 + 32·e | root mirror slot e: seq, Size, MapBlock, Name |
## | extents_off + 48·x | record-mirror extent x (0..255) |
## | slots_off .. capacity | mirror slots |
##
## `extents_off = 4096 + 4096 * ceil(32 * MaxRootEntries / 4096)` and
## `slots_off = extents_off + 12288`.
##
## Every procedure that publishes a value here runs the same protocol (§6,
## "Writing through the page", step 2): `seq + 1` (odd) with release, the
## payload words in the order the record kind requires, `seq + 2` (even) with
## release. A reader (§6, "Reading") loads `seq`, the payload and `seq` again,
## and accepts only when the two loads agree and are even.

import std/widestrs
import results
import ./sha256

const
  PageMagicVersion* = 0x504C5443'u64 or (1'u64 shl 32)
    ## The bytes `CTLP`, then version 1, read as one little-endian `u64`.
  PageVersion* = 1'u32
  DefaultPageCapacity* = 16 * 1024 * 1024
    ## `ctfs-container.md` §6: 16 MiB virtual by default (`CKF-3`, DR-12).
  PageStateCreating* = 0'u64
  PageStateLive* = 1'u64
  PageStateClosed* = 2'u64
  PageStateAbandoned* = 3'u64
  WriterSlotFree* = 0'u64
  WriterSlotLive* = 1'u64
  WriterSlotFinished* = 2'u64
  WriterSlotDead* = 3'u64
  PageWriterSlots* = 16
  PageFamilyCounters* = 16
  PageExtents* = 256
  HeartbeatDeadNs* = 10_000_000_000'u64
    ## A writer whose heartbeat is older than this is dead (§6, "A writer that
    ## dies").

  OffIdentity* = 8
  OffCapacity* = 40
  OffState* = 48
  OffSealEpoch* = 56
  OffOverflowed* = 64
  OffWriters* = 128
  WriterSlotSize* = 64
  OffNextFreeBlock* = 1152
  OffFamilyCounters* = 1160
  OffMirrorBump* = 1288
  OffExtentsUsed* = 1296
  OffRootMirror* = 4096
  RootMirrorSlotSize* = 32
  ExtentSize* = 48
  ExtentTableBytes* = 12288

type
  PageIdentity* = array[32, byte]
    ## A container file's identity (§6, "Identity"), packed as 32 bytes.

  PageView* = object
    ## A mapped page, or a buffer laid out as one (a test's simulated page).
    base*: pointer
    len*: int

  PageTapKind* = enum
    ptWords      ## a seq-protected publication: `off` is the slot's `seq`
    ptStore      ## a plain store of one word (state, counters, extents)

  PageTap* = proc (kind: PageTapKind, off: int,
                   words: openArray[uint64]) {.closure, gcsafe, raises: [].}
    ## A test seam: called with every publication the writer makes in the
    ## page, in order, so a test can rebuild the page as of any point.

  HeartbeatCtl = object
    page: pointer
    slotOff: int
    stop: int64
    when defined(windows):
      thread: int
    else:
      thread: uint

  LivePageObj* = object
    view*: PageView
    name*: string
    identity*: PageIdentity
    rootEntries*: int
    extentsOff*: int
    slotsOff*: int
    writerSlot*: int
    tap*: PageTap
    closed*: bool
    hb: ptr HeartbeatCtl
    when defined(windows):
      handle: int
    else:
      owner: bool

  LivePage* = ref LivePageObj
    ## The writer's side of a page: it created the page or joined it.

  PageReader* = object
    ## The reader's side: a page attached read-only, or a test's buffer.
    view*: PageView
    name*: string
    rootEntries*: int
    extentsOff*: int
    slotsOff*: int
    when defined(windows):
      handle: int

  MirrorExtent* = object
    ## A record-mirror extent: `count` records of member entry `entry`, from
    ## record `first`, mirrored at `slotOff`, each `seq` then `words` words.
    index*: int
    entry*: uint64
    first*: uint64
    count*: uint64
    slotOff*: int
    words*: int

  SlotRead* = enum
    srNever     ## `seq` is 0: never published
    srOk        ## two equal, even `seq` loads around the payload
    srRetry     ## odd, or changed during the read

proc extentsOffset*(rootEntries: int): int =
  4096 + 4096 * ((32 * rootEntries + 4095) div 4096)

proc slotsOffset*(rootEntries: int): int =
  extentsOffset(rootEntries) + ExtentTableBytes

# ---------------------------------------------------------------------------
# Word access
# ---------------------------------------------------------------------------

proc wordPtr(v: PageView, off: int): ptr uint64 {.inline.} =
  cast[ptr uint64](cast[uint](v.base) + uint(off))

proc load*(v: PageView, off: int): uint64 {.inline.} =
  atomicLoadN(wordPtr(v, off), ATOMIC_ACQUIRE)

proc loadRelaxed*(v: PageView, off: int): uint64 {.inline.} =
  atomicLoadN(wordPtr(v, off), ATOMIC_RELAXED)

proc store*(v: PageView, off: int, x: uint64) {.inline.} =
  atomicStoreN(wordPtr(v, off), x, ATOMIC_RELEASE)

proc storeRelaxed*(v: PageView, off: int, x: uint64) {.inline.} =
  atomicStoreN(wordPtr(v, off), x, ATOMIC_RELAXED)

proc fetchAdd*(v: PageView, off: int, x: uint64): uint64 {.inline.} =
  atomicFetchAdd(wordPtr(v, off), x, ATOMIC_ACQ_REL)

proc casWord(v: PageView, off: int, expected, desired: uint64): bool =
  var e = expected
  atomicCompareExchangeN(wordPtr(v, off), addr e, desired, false,
    ATOMIC_ACQ_REL, ATOMIC_RELAXED)

proc fenceRelease() {.inline.} = atomicThreadFence(ATOMIC_RELEASE)
proc fenceAcquire() {.inline.} = atomicThreadFence(ATOMIC_ACQUIRE)

# ---------------------------------------------------------------------------
# The machine's monotonic clock, process start times
# ---------------------------------------------------------------------------

when defined(windows):
  proc queryPerformanceCounter(c: ptr int64): int32 {.stdcall,
    dynlib: "kernel32", importc: "QueryPerformanceCounter".}
  proc queryPerformanceFrequency(c: ptr int64): int32 {.stdcall,
    dynlib: "kernel32", importc: "QueryPerformanceFrequency".}
  proc openProcess(access: uint32, inherit: int32, pid: uint32): int {.
    stdcall, dynlib: "kernel32", importc: "OpenProcess".}
  proc closeHandleW(h: int): int32 {.stdcall, dynlib: "kernel32",
    importc: "CloseHandle".}
  proc getExitCodeProcess(h: int, code: ptr uint32): int32 {.stdcall,
    dynlib: "kernel32", importc: "GetExitCodeProcess".}
  proc getProcessTimes(h: int, c, e, k, u: ptr uint64): int32 {.stdcall,
    dynlib: "kernel32", importc: "GetProcessTimes".}
  proc getCurrentProcessId(): uint32 {.stdcall, dynlib: "kernel32",
    importc: "GetCurrentProcessId".}
  proc sleepMs(ms: uint32) {.stdcall, dynlib: "kernel32", importc: "Sleep".}
  const
    ProcessQueryLimitedInformation = 0x1000'u32
    StillActive = 259'u32
elif defined(macosx):
  type MachTimebase {.pure.} = object
    numer, denom: uint32
  proc mach_continuous_time(): uint64 {.importc, header: "<mach/mach_time.h>".}
  proc mach_timebase_info(info: ptr MachTimebase): cint {.importc,
    header: "<mach/mach_time.h>".}
  proc proc_pidinfo(pid: cint, flavor: cint, arg: uint64, buffer: pointer,
                    size: cint): cint {.importc, header: "<libproc.h>".}
  import std/posix
else:
  import std/posix

proc monotonicNs*(): uint64 {.stackTrace: off.} =
  ## Nanoseconds of the machine's monotonic clock, which every process on the
  ## machine shares (§6, "The heartbeat").
  when defined(windows):
    var c, f: int64
    discard queryPerformanceCounter(addr c)
    discard queryPerformanceFrequency(addr f)
    if f <= 0: return 0
    uint64(c div f) * 1_000_000_000'u64 +
      uint64(c mod f) * 1_000_000_000'u64 div uint64(f)
  elif defined(macosx):
    var tb: MachTimebase
    discard mach_timebase_info(addr tb)
    let t = mach_continuous_time()
    if tb.denom == 0: t else: t div uint64(tb.denom) * uint64(tb.numer)
  else:
    var ts: Timespec
    discard clock_gettime(CLOCK_MONOTONIC, ts)
    uint64(ts.tv_sec) * 1_000_000_000'u64 + uint64(ts.tv_nsec)

proc currentPid*(): uint64 =
  when defined(windows): uint64(getCurrentProcessId())
  else: uint64(getpid())

proc processStartTime*(pid: uint64): uint64 =
  ## The operating system's creation time of process `pid`, or 0 when no live
  ## process has that pid (§6: "The process start time is the operating
  ## system's creation time for the pid"). A zombie counts as gone.
  when defined(windows):
    let h = openProcess(ProcessQueryLimitedInformation, 0, uint32(pid))
    if h == 0: return 0
    var code: uint32
    var c, e, k, u: uint64
    let okCode = getExitCodeProcess(h, addr code)
    let okTimes = getProcessTimes(h, addr c, addr e, addr k, addr u)
    discard closeHandleW(h)
    if okCode == 0 or okTimes == 0 or code != StillActive: return 0
    c
  elif defined(macosx):
    # struct proc_bsdinfo: pbi_status at 4, pbi_start_tvsec at 120,
    # pbi_start_tvusec at 128; 136 bytes (PROC_PIDTBSDINFO = 3).
    var buf: array[136, byte]
    let n = proc_pidinfo(cint(pid), 3, 0, addr buf[0], cint(buf.len))
    if n != cint(buf.len): return 0
    var status: uint32
    copyMem(addr status, addr buf[4], 4)
    if status == 5: return 0  # SZOMB
    var sec, usec: uint64
    copyMem(addr sec, addr buf[120], 8)
    copyMem(addr usec, addr buf[128], 8)
    sec * 1_000_000'u64 + usec + 1
  else:
    var f: File
    try:
      if not open(f, "/proc/" & $pid & "/stat", fmRead): return 0
    except IOError, OSError:
      return 0
    var s = ""
    try:
      s = f.readAll()
    except IOError, OSError:
      discard
    try: close(f) except IOError: discard
    var close = -1
    for i in countdown(s.len - 1, 0):
      if s[i] == ')':
        close = i
        break
    if close < 0: return 0
    var fields: seq[string]
    var cur = ""
    for ch in s[close + 1 .. ^1]:
      if ch in {' ', '\n', '\t'}:
        if cur.len > 0:
          fields.add cur
          cur = ""
      else:
        cur.add ch
    if cur.len > 0: fields.add cur
    if fields.len < 20 or fields[0] == "Z": return 0
    var v = 0'u64
    for ch in fields[19]:
      if ch notin {'0'..'9'}: return 0
      v = v * 10 + uint64(ord(ch) - ord('0'))
    v + 1

# ---------------------------------------------------------------------------
# Identity and name
# ---------------------------------------------------------------------------

when defined(windows):
  proc getFileInformationByHandleEx(h: int, cls: int32, info: pointer,
                                    size: uint32): int32 {.stdcall,
    dynlib: "kernel32", importc: "GetFileInformationByHandleEx".}
  const FileIdInfoClass = 18'i32
elif defined(macosx):
  proc c_fstat(fd: cint, buf: pointer): cint {.importc: "fstat",
    header: "<sys/stat.h>".}
else:
  proc c_syscall(n: clong): clong {.importc: "syscall", varargs,
    header: "<unistd.h>".}
  when defined(amd64):
    const SysStatx = 332
  elif defined(arm64):
    const SysStatx = 291
  else:
    const SysStatx = -1

proc putU64(id: var PageIdentity, at: int, v: uint64) =
  for i in 0 ..< 8: id[at + i] = byte((v shr (8 * i)) and 0xff)

proc fileIdentity*(f: File): Result[PageIdentity, string] =
  ## The identity of an open container file (§6, "Identity"):
  ## Windows `VolumeSerialNumber` as `u64`, the 16-byte `FileId`, 8 zero bytes;
  ## Linux `st_dev`, `st_ino`, `stx_btime.tv_sec`, `stx_btime.tv_nsec` as `u32`
  ## and 4 zero bytes (the birth time is zero where `statx` does not report
  ## it); macOS `st_dev`, `st_ino` and `st_birthtimespec`, packed like Linux's.
  var id: PageIdentity
  when defined(windows):
    var info: array[24, byte]
    let h = int(getOsFileHandle(f))
    if getFileInformationByHandleEx(h, FileIdInfoClass, addr info[0],
        uint32(info.len)) == 0:
      return err("GetFileInformationByHandleEx(FileIdInfo) failed")
    for i in 0 ..< 24: id[i] = info[i]
  elif defined(macosx):
    var st: array[144, byte]
    if c_fstat(getFileHandle(f), addr st[0]) != 0:
      return err("fstat failed")
    var dev: int32
    var ino, bsec, bnsec: uint64
    copyMem(addr dev, addr st[0], 4)
    copyMem(addr ino, addr st[8], 8)
    copyMem(addr bsec, addr st[80], 8)
    copyMem(addr bnsec, addr st[88], 8)
    id.putU64(0, uint64(cast[uint32](dev)))
    id.putU64(8, ino)
    id.putU64(16, bsec)
    id.putU64(24, bnsec and 0xFFFF_FFFF'u64)
  else:
    var st: Stat
    if fstat(getFileHandle(f), st) != 0:
      return err("fstat failed")
    id.putU64(0, uint64(st.st_dev))
    id.putU64(8, uint64(st.st_ino))
    when SysStatx > 0:
      var sx: array[256, byte]
      let empty: cstring = ""
      let rc = c_syscall(clong(SysStatx), cint(getFileHandle(f)), empty,
        cint(0x1000), cuint(0x800), addr sx[0])
      if rc == 0:
        var mask: uint32
        copyMem(addr mask, addr sx[0], 4)
        if (mask and 0x800'u32) != 0:
          var sec: int64
          var nsec: uint32
          copyMem(addr sec, addr sx[80], 8)
          copyMem(addr nsec, addr sx[88], 4)
          id.putU64(16, cast[uint64](sec))
          id.putU64(24, uint64(nsec))
  ok(id)

proc pageName*(id: PageIdentity): string =
  ## `ctfs.` and the first 24 lowercase hexadecimal digits of SHA-256 over the
  ## identity's bytes.
  "ctfs." & toHexLower(sha256(id))[0 ..< 24]

# ---------------------------------------------------------------------------
# Shared memory
# ---------------------------------------------------------------------------

when defined(windows):
  proc createFileMappingW(h: int, attrs: pointer, protect: uint32,
                          sizeHigh, sizeLow: uint32, name: WideCString): int {.
    stdcall, dynlib: "kernel32", importc: "CreateFileMappingW".}
  proc openFileMappingW(access: uint32, inherit: int32, name: WideCString): int {.
    stdcall, dynlib: "kernel32", importc: "OpenFileMappingW".}
  proc mapViewOfFile(h: int, access: uint32, offHigh, offLow: uint32,
                     n: uint): pointer {.stdcall, dynlib: "kernel32",
    importc: "MapViewOfFile".}
  proc unmapViewOfFile(p: pointer): int32 {.stdcall, dynlib: "kernel32",
    importc: "UnmapViewOfFile".}
  proc getLastErrorW(): uint32 {.stdcall, dynlib: "kernel32",
    importc: "GetLastError".}
  type MemoryBasicInformation {.pure.} = object
    baseAddress: pointer
    allocationBase: pointer
    allocationProtect: uint32
    partitionId: uint16
    regionSize: uint
    state: uint32
    protect: uint32
    typ: uint32
  proc virtualQuery(p: pointer, info: ptr MemoryBasicInformation,
                    n: uint): uint {.stdcall, dynlib: "kernel32",
    importc: "VirtualQuery".}
  const
    PageReadWrite = 0x04'u32
    FileMapAllAccess = 0x000F001F'u32
    FileMapRead = 0x0004'u32
    ErrorAlreadyExists = 183'u32
    InvalidHandleValue = -1

  proc sectionName(name: string): string = "Local\\" & name
else:
  proc shm_open(name: cstring, oflag: cint, mode: Mode): cint {.importc,
    header: "<sys/mman.h>".}
  proc shm_unlink(name: cstring): cint {.importc, header: "<sys/mman.h>".}
  proc shmPath(name: string): string = "/" & name

type ShmOpen = enum soCreated, soExists, soFailed

proc shmCreate(name: string, capacity: int, view: var PageView,
               handle: var int, why: var string): ShmOpen =
  when defined(windows):
    let w = newWideCString(sectionName(name))
    let h = createFileMappingW(InvalidHandleValue, nil, PageReadWrite,
      uint32(uint64(capacity) shr 32), uint32(uint64(capacity) and 0xFFFF_FFFF'u64), w)
    if h == 0:
      why = "CreateFileMappingW failed: error " & $getLastErrorW()
      return soFailed
    if getLastErrorW() == ErrorAlreadyExists:
      discard closeHandleW(h)
      return soExists
    let p = mapViewOfFile(h, FileMapAllAccess, 0, 0, uint(capacity))
    if p == nil:
      why = "MapViewOfFile failed: error " & $getLastErrorW()
      discard closeHandleW(h)
      return soFailed
    view = PageView(base: p, len: capacity)
    handle = h
    soCreated
  else:
    let fd = shm_open(cstring(shmPath(name)), O_CREAT or O_EXCL or O_RDWR,
      Mode(0o600))
    if fd < 0:
      if errno == EEXIST: return soExists
      why = "shm_open failed: errno " & $errno
      return soFailed
    if ftruncate(fd, Off(capacity)) != 0:
      why = "ftruncate failed: errno " & $errno
      discard close(fd)
      discard shm_unlink(cstring(shmPath(name)))
      return soFailed
    let p = mmap(nil, capacity, PROT_READ or PROT_WRITE, MAP_SHARED, fd, 0)
    discard close(fd)
    if p == MAP_FAILED:
      why = "mmap failed: errno " & $errno
      discard shm_unlink(cstring(shmPath(name)))
      return soFailed
    view = PageView(base: p, len: capacity)
    handle = 0
    soCreated

proc shmOpen(name: string, write: bool, view: var PageView,
             handle: var int): bool =
  ## Open an existing page object; false when there is none (or no access).
  when defined(windows):
    let acc = if write: FileMapAllAccess else: FileMapRead
    let h = openFileMappingW(acc, 0, newWideCString(sectionName(name)))
    if h == 0: return false
    let p = mapViewOfFile(h, acc, 0, 0, 0)
    if p == nil:
      discard closeHandleW(h)
      return false
    var mbi: MemoryBasicInformation
    if virtualQuery(p, addr mbi, uint(sizeof(mbi))) == 0:
      discard unmapViewOfFile(p)
      discard closeHandleW(h)
      return false
    view = PageView(base: p, len: int(mbi.regionSize))
    handle = h
    true
  else:
    let fd = shm_open(cstring(shmPath(name)),
      if write: O_RDWR else: O_RDONLY, Mode(0))
    if fd < 0: return false
    var st: Stat
    if fstat(fd, st) != 0 or int(st.st_size) < 4096:
      discard close(fd)
      return false
    let len = int(st.st_size)
    let prot = if write: PROT_READ or PROT_WRITE else: PROT_READ
    let p = mmap(nil, len, prot, MAP_SHARED, fd, 0)
    discard close(fd)
    if p == MAP_FAILED: return false
    view = PageView(base: p, len: len)
    handle = 0
    true

proc shmUnmap(view: var PageView, handle: int) =
  if view.base == nil: return
  when defined(windows):
    discard unmapViewOfFile(view.base)
    if handle != 0: discard closeHandleW(handle)
  else:
    discard munmap(view.base, view.len)
  view.base = nil
  view.len = 0

proc shmUnlink(name: string) =
  ## Remove the name (POSIX); on Windows the section goes with its last handle.
  when not defined(windows):
    discard shm_unlink(cstring(shmPath(name)))

proc pageObjectExists*(name: string): bool =
  ## True when a page object of this name can be opened for reading.
  var v: PageView
  var h: int
  if shmOpen(name, false, v, h):
    shmUnmap(v, h)
    return true
  false

# ---------------------------------------------------------------------------
# Reading (shared by the writer's own checks and by readers)
# ---------------------------------------------------------------------------

proc readSlot*(v: PageView, seqOff: int, words: var openArray[uint64]): SlotRead =
  ## One attempt of §6's read: `seq` as `s1` (0: never published; odd:
  ## retry), the payload, `seq` as `s2`; the payload is accepted only when
  ## `s2 == s1`.
  let s1 = v.load(seqOff)
  if s1 == 0: return srNever
  if (s1 and 1) == 1: return srRetry
  for i in 0 ..< words.len:
    words[i] = v.loadRelaxed(seqOff + 8 + 8 * i)
  fenceAcquire()
  let s2 = v.loadRelaxed(seqOff)
  if s2 == s1: srOk else: srRetry

proc readSlotSkippingSecondSeq*(v: PageView, seqOff: int,
                                words: var openArray[uint64]): SlotRead =
  ## The unprotected reader of the `test_live_page_torn` mutation arm: it
  ## skips the second `seq` load, so it can accept a payload torn by a writer
  ## that is between its odd and even stores. Never used by a reader.
  let s1 = v.load(seqOff)
  if s1 == 0: return srNever
  if (s1 and 1) == 1: return srRetry
  for i in 0 ..< words.len:
    words[i] = v.loadRelaxed(seqOff + 8 + 8 * i)
  srOk

proc identityOf(v: PageView): PageIdentity =
  for i in 0 ..< 4:
    let w = v.loadRelaxed(OffIdentity + 8 * i)
    for j in 0 ..< 8: result[8 * i + j] = byte((w shr (8 * j)) and 0xff)

proc writerAlive*(v: PageView, slot: int, nowNs: uint64): bool =
  ## Writer slot `slot` is live: claimed, its pid names a process with its
  ## recorded start time, and its heartbeat is younger than 10 seconds.
  let b = OffWriters + slot * WriterSlotSize
  if v.load(b + 32) != WriterSlotLive: return false
  let pid = v.loadRelaxed(b)
  let start = v.loadRelaxed(b + 8)
  let hb = v.load(b + 16)
  if start == 0 or processStartTime(pid) != start: return false
  nowNs < hb or nowNs - hb < HeartbeatDeadNs

proc liveWriters*(v: PageView): seq[int] =
  let now = monotonicNs()
  for i in 0 ..< PageWriterSlots:
    if writerAlive(v, i, now): result.add i

# ---------------------------------------------------------------------------
# The writer's side
# ---------------------------------------------------------------------------

proc tapStore(p: LivePage, off: int, x: uint64) =
  p.view.store(off, x)
  if p.tap != nil: p.tap(ptStore, off, [x])

proc publishWords*(p: LivePage, seqOff: int, words: openArray[uint64]) =
  ## Publish one in-place value under the slot's sequence counter (§6,
  ## "Writing through the page", step 2): `seq` odd with release, the payload
  ## in the order given, `seq` even with release.
  let s = p.view.loadRelaxed(seqOff)
  p.view.storeRelaxed(seqOff, s + 1)
  fenceRelease()
  for i in 0 ..< words.len:
    p.view.storeRelaxed(seqOff + 8 + 8 * i, words[i])
  p.view.store(seqOff, s + 2)
  if p.tap != nil: p.tap(ptWords, seqOff, words)

proc publishWordsOrdered*(p: LivePage, seqOff: int, words: openArray[uint64],
                          order: openArray[int]) =
  ## As `publishWords`, the payload words stored in `order` (indices into
  ## `words`): the store order a record kind requires (for a stream
  ## reference, `MapBlock` before `Size`), which is what makes a prefix of a
  ## dead writer's stores a valid record (§6, "the crash rule").
  let s = p.view.loadRelaxed(seqOff)
  p.view.storeRelaxed(seqOff, s + 1)
  fenceRelease()
  for i in order:
    p.view.storeRelaxed(seqOff + 8 + 8 * i, words[i])
  p.view.store(seqOff, s + 2)
  if p.tap != nil: p.tap(ptWords, seqOff, words)

proc heartbeat*(p: LivePage) =
  if p == nil or p.closed: return
  p.view.store(OffWriters + p.writerSlot * WriterSlotSize + 16, monotonicNs())

proc publishRoot*(p: LivePage, entry: int, size, mapBlock, name: uint64) =
  ## The root mirror of entry `entry`: `seq` odd, then `Name`, `MapBlock` and
  ## `Size` in that order, then `seq` even (§6, "The root mirror").
  if p == nil or p.closed or entry >= p.rootEntries: return
  let off = OffRootMirror + RootMirrorSlotSize * entry
  let s = p.view.loadRelaxed(off)
  p.view.storeRelaxed(off, s + 1)
  fenceRelease()
  p.view.storeRelaxed(off + 24, name)
  p.view.storeRelaxed(off + 16, mapBlock)
  p.view.storeRelaxed(off + 8, size)
  p.view.store(off, s + 2)
  if p.tap != nil: p.tap(ptWords, off, [size, mapBlock, name])

proc allocExtent*(p: LivePage, entry: uint64, first, count: uint64,
                  words: int): Result[MirrorExtent, void] =
  ## Claim a record-mirror extent: a fetch-add on `extents_used` and on
  ## `mirror_bump`. Either one past its end is an overflow: `overflowed` is
  ## incremented, and the records are followed in the file only.
  let idx = int(p.view.fetchAdd(OffExtentsUsed, 1))
  let need = uint64(8 + 8 * words) * count
  let slotOff = int(p.view.fetchAdd(OffMirrorBump, need))
  if idx >= PageExtents or slotOff + int(need) > p.view.len:
    discard p.view.fetchAdd(OffOverflowed, 1)
    if p.tap != nil: p.tap(ptStore, OffOverflowed, [p.view.load(OffOverflowed)])
    return err()
  let e = p.extentsOff + ExtentSize * idx
  p.view.storeRelaxed(e, entry)
  p.view.storeRelaxed(e + 8, first)
  p.view.storeRelaxed(e + 16, count)
  p.view.storeRelaxed(e + 24, uint64(slotOff))
  p.view.store(e + 32, uint64(words))
  if p.tap != nil:
    p.tap(ptStore, e, [entry])
    p.tap(ptStore, e + 8, [first])
    p.tap(ptStore, e + 16, [count])
    p.tap(ptStore, e + 24, [uint64(slotOff)])
    p.tap(ptStore, e + 32, [uint64(words)])
  ok(MirrorExtent(index: idx, entry: entry, first: first, count: count,
                  slotOff: slotOff, words: words))

proc slotOffset*(x: MirrorExtent, rec: uint64): int =
  x.slotOff + int(rec - x.first) * (8 + 8 * x.words)

proc publishRecord*(p: LivePage, x: MirrorExtent, rec: uint64,
                    words: openArray[uint64]) =
  if p == nil or p.closed: return
  p.publishWords(x.slotOffset(rec), words)

proc sealDone*(p: LivePage) =
  ## §6 step 4: the writer's `seal_epoch`, then the page's.
  if p == nil or p.closed: return
  let w = OffWriters + p.writerSlot * WriterSlotSize
  discard p.view.fetchAdd(w + 24, 1)
  discard p.view.fetchAdd(OffSealEpoch, 1)
  p.view.store(w + 16, monotonicNs())
  if p.tap != nil:
    p.tap(ptStore, w + 24, [p.view.load(w + 24)])
    p.tap(ptStore, OffSealEpoch, [p.view.load(OffSealEpoch)])

proc writerSealEpoch*(p: LivePage): uint64 =
  if p == nil: return 0
  p.view.load(OffWriters + p.writerSlot * WriterSlotSize + 24)

proc nextFreeBlockWord*(p: LivePage): uint64 = p.view.load(OffNextFreeBlock)

proc setNextFreeBlock*(p: LivePage, n: uint64) =
  if p == nil or p.closed: return
  p.tapStore(OffNextFreeBlock, n)

# The heartbeat thread: a native thread that touches no Nim-managed memory,
# so it is safe under every memory manager a consumer builds with.

when defined(windows):
  proc createThreadW(attrs: pointer, stack: uint, start: pointer, arg: pointer,
                     flags: uint32, tid: ptr uint32): int {.stdcall,
    dynlib: "kernel32", importc: "CreateThread".}
  proc waitForSingleObject(h: int, ms: uint32): uint32 {.stdcall,
    dynlib: "kernel32", importc: "WaitForSingleObject".}

  proc heartbeatMain(arg: pointer): uint32 {.stdcall, stackTrace: off,
      gcsafe.} =
    let ctl = cast[ptr HeartbeatCtl](arg)
    while atomicLoadN(addr ctl.stop, ATOMIC_ACQUIRE) == 0:
      atomicStoreN(cast[ptr uint64](cast[uint](ctl.page) + uint(ctl.slotOff)),
        monotonicNs(), ATOMIC_RELEASE)
      sleepMs(200)
    0
else:
  proc heartbeatMain(arg: pointer): pointer {.noconv, stackTrace: off,
      gcsafe.} =
    let ctl = cast[ptr HeartbeatCtl](arg)
    while atomicLoadN(addr ctl.stop, ATOMIC_ACQUIRE) == 0:
      atomicStoreN(cast[ptr uint64](cast[uint](ctl.page) + uint(ctl.slotOff)),
        monotonicNs(), ATOMIC_RELEASE)
      var ts = Timespec(tv_sec: posix.Time(0), tv_nsec: 200_000_000)
      var rem: Timespec
      discard nanosleep(ts, rem)
    nil

proc startHeartbeat(p: LivePage) =
  let ctl = cast[ptr HeartbeatCtl](allocShared0(sizeof(HeartbeatCtl)))
  ctl.page = p.view.base
  ctl.slotOff = OffWriters + p.writerSlot * WriterSlotSize + 16
  when defined(windows):
    ctl.thread = createThreadW(nil, 0, cast[pointer](heartbeatMain),
      cast[pointer](ctl), 0, nil)
    if ctl.thread == 0:
      deallocShared(ctl)
      return
  else:
    var t: Pthread
    if pthread_create(addr t, nil, heartbeatMain, cast[pointer](ctl)) != 0:
      deallocShared(ctl)
      return
    ctl.thread = cast[uint](t)
  p.hb = ctl

proc stopHeartbeat(p: LivePage) =
  let ctl = p.hb
  if ctl == nil: return
  atomicStoreN(addr ctl.stop, 1'i64, ATOMIC_RELEASE)
  when defined(windows):
    discard waitForSingleObject(ctl.thread, 0xFFFF_FFFF'u32)
    discard closeHandleW(ctl.thread)
  else:
    discard pthread_join(cast[Pthread](ctl.thread), nil)
  deallocShared(ctl)
  p.hb = nil

proc claimWriterSlot(p: LivePage): Result[int, string] =
  ## A compare-exchange of a slot's state from 0 to 1, tried on slots 0 to 15
  ## in order (a bounded scan), then pid, start time and heartbeat, stored with
  ## release. A slot is never reused within a page's life.
  let pid = currentPid()
  for i in 0 ..< PageWriterSlots:
    let b = OffWriters + i * WriterSlotSize
    if p.view.casWord(b + 32, WriterSlotFree, WriterSlotLive):
      p.view.storeRelaxed(b, pid)
      p.view.storeRelaxed(b + 8, processStartTime(pid))
      p.view.store(b + 16, monotonicNs())
      return ok(i)
  err("live coordination page " & p.name & ": all " & $PageWriterSlots &
    " writer slots are taken (a page has 16, so at most 15 producer " &
    "processes beside the coordinator)")

proc closeLivePage*(p: LivePage)

var hostedPages {.threadvar.}: seq[LivePage]
  ## The pages this process hosts and has not closed.

proc initFresh(p: LivePage, capacity: int) =
  ## Lay out a page that has just been created (all zero).
  for i in 0 ..< 4:
    var w = 0'u64
    for j in 0 ..< 8: w = w or (uint64(p.identity[8 * i + j]) shl (8 * j))
    p.view.storeRelaxed(OffIdentity + 8 * i, w)
  p.view.storeRelaxed(OffCapacity, uint64(capacity))
  p.view.storeRelaxed(OffState, PageStateCreating)
  p.view.storeRelaxed(OffMirrorBump, uint64(p.slotsOff))
  p.view.store(0, PageMagicVersion)

proc clearUsed(v: PageView, slotsOff: int) =
  ## Zero what a previous page under the same name used: the header, the root
  ## mirror, the extents and the slots up to its bump.
  let bump = int(min(uint64(v.len), max(v.loadRelaxed(OffMirrorBump),
    uint64(slotsOff))))
  v.store(0, 0)
  var off = 8
  while off + 8 <= bump:
    v.storeRelaxed(off, 0)
    off += 8

proc createLivePage*(f: File, rootEntries: int,
                     capacity: int = DefaultPageCapacity,
                     containerName = ""): Result[LivePage, string] =
  ## Create the page of the container open as `f` (§6, "Lifetime",
  ## "Creation"): exclusively. An existing object of the same name is opened
  ## and inspected first; if every writer it names is dead it is marked
  ## abandoned and replaced, otherwise creation fails, naming the container.
  ## The creator registers as a writer and sets the state to `live`; the caller
  ## publishes the root region only after this returns.
  let what = if containerName.len > 0: containerName else: "the container"
  let id = ? fileIdentity(f).mapErr(proc (e: string): string =
    "live coordination page of " & what & ": " & e)
  let name = pageName(id)
  # A page this process hosts for the same file belongs to a container this
  # process created on the file and never closed; creating a container on the
  # file again (which truncates it) supersedes that one: its page is closed.
  var kept: seq[LivePage]
  for old in hostedPages:
    if old.closed: continue
    if old.name == name and old.identity == id:
      closeLivePage(old)
    else:
      kept.add old
  hostedPages = kept
  let ext = extentsOffset(rootEntries)
  let slots = slotsOffset(rootEntries)
  if slots + 4096 > capacity:
    return err("live coordination page of " & what & ": capacity " &
      $capacity & " cannot hold the root mirror of " & $rootEntries & " entries")
  var p = LivePage(name: name, identity: id, rootEntries: rootEntries,
                   extentsOff: ext, slotsOff: slots)
  var why = ""
  var handle = 0
  var view: PageView
  case shmCreate(name, capacity, view, handle, why)
  of soCreated:
    p.view = view
    when defined(windows): p.handle = handle
    else: p.owner = true
    p.initFresh(capacity)
  of soFailed:
    return err("live coordination page " & name & " of " & what & ": " & why)
  of soExists:
    var old: PageView
    var oh = 0
    if not shmOpen(name, true, old, oh):
      return err("live coordination page " & name & " of " & what &
        " exists and cannot be opened to inspect it")
    if liveWriters(old).len > 0:
      shmUnmap(old, oh)
      return err("live coordination page " & name & " of " & what &
        " has live writers: the container is being written by another process")
    old.store(OffState, PageStateAbandoned)
    when defined(windows):
      # The section stays while any handle names it; reuse it in place.
      if old.len < capacity:
        shmUnmap(old, oh)
        return err("live coordination page " & name & " of " & what &
          " was abandoned but is still mapped by another process, with " &
          $old.len & " bytes")
      clearUsed(old, slots)
      p.view = PageView(base: old.base, len: capacity)
      p.handle = oh
      p.initFresh(capacity)
    else:
      shmUnmap(old, oh)
      shmUnlink(name)
      case shmCreate(name, capacity, view, handle, why)
      of soCreated:
        p.view = view
        p.owner = true
        p.initFresh(capacity)
      else:
        return err("live coordination page " & name & " of " & what &
          ": recreating it after removing an abandoned one failed: " & why)
  p.writerSlot = ? p.claimWriterSlot()
  p.view.store(OffState, PageStateLive)
  p.startHeartbeat()
  hostedPages.add p
  ok(p)

proc closeLivePage*(p: LivePage) =
  ## §6, "Close": the final seal is the caller's (every in-place word written
  ## to the file); then the writer's slot is finished, the state `closed`, and
  ## the name removed. Readers still mapping the page see `closed`.
  if p == nil or p.closed: return
  p.stopHeartbeat()
  let w = OffWriters + p.writerSlot * WriterSlotSize
  p.tapStore(w + 32, WriterSlotFinished)
  p.tapStore(OffState, PageStateClosed)
  shmUnlink(p.name)
  when defined(windows): shmUnmap(p.view, p.handle)
  else: shmUnmap(p.view, 0)
  p.closed = true

proc abandonLivePage*(p: LivePage) =
  ## Release this process's mapping without closing the page: what a writer
  ## that dies leaves behind (for tests of the abandonment path).
  if p == nil or p.closed: return
  p.stopHeartbeat()
  when defined(windows): shmUnmap(p.view, p.handle)
  else: shmUnmap(p.view, 0)
  p.closed = true

proc joinLivePage*(f: File, rootEntries: int): Result[LivePage, string] =
  ## Join the page of a container another process created, as an additional
  ## writer (`ctfs-container.md` §6, W2 and the hybrid): the page must be
  ## `live` and carry this file's identity; a writer slot is claimed (16 per
  ## page, so at most 15 producers beside the coordinator, a 16th refused by
  ## name) and kept alive by this process's heartbeat. The joiner never
  ## creates, closes or removes the page.
  let id = ? fileIdentity(f)
  let name = pageName(id)
  var v: PageView
  var h = 0
  if not shmOpen(name, true, v, h):
    return err("live coordination page " & name & ": none to join")
  if v.load(0) != PageMagicVersion or identityOf(v) != id or
      v.load(OffState) != PageStateLive:
    shmUnmap(v, h)
    return err("live coordination page " & name & " is not a live page of this file")
  var p = LivePage(view: v, name: name, identity: id, rootEntries: rootEntries,
                   extentsOff: extentsOffset(rootEntries),
                   slotsOff: slotsOffset(rootEntries))
  when defined(windows): p.handle = h
  let slot = p.claimWriterSlot()
  if slot.isErr:
    shmUnmap(p.view, h)
    return err(slot.error)
  p.writerSlot = slot.get()
  p.startHeartbeat()
  ok(p)

proc settleDeadWriters*(p: LivePage): seq[int] =
  ## A surviving writer's duty under W2 (§6, "A writer that dies"): every
  ## writer slot whose writer is dead (its pid no longer names a process with
  ## its start time, or its heartbeat is older than 10 seconds) is marked dead,
  ## and every root mirror slot left odd is settled. A root entry is
  ## prefix-valid (`MapBlock` before `Size`), so the settled value is the
  ## page's payload: its `seq` is made even. A record slot left odd is left to
  ## its readers, which retry a bounded number of times and then read the
  ## record from the file: the crash rule's value for a record kind that is
  ## not prefix-valid. Returns the slots marked dead.
  let now = monotonicNs()
  for i in 0 ..< PageWriterSlots:
    let b = OffWriters + i * WriterSlotSize
    if p.view.load(b + 32) == WriterSlotLive and i != p.writerSlot and
        not writerAlive(p.view, i, now):
      p.view.store(b + 32, WriterSlotDead)
      result.add i
  if result.len > 0:
    for e in 0 ..< p.rootEntries:
      let off = OffRootMirror + RootMirrorSlotSize * e
      let s = p.view.load(off)
      if (s and 1'u64) == 1'u64:
        p.view.store(off, s + 1)

# ---------------------------------------------------------------------------
# The reader's side
# ---------------------------------------------------------------------------

proc pageReaderOfView*(v: PageView, rootEntries: int): PageReader =
  ## A reader over a buffer laid out as a page (a test's simulated page).
  PageReader(view: v, rootEntries: rootEntries,
             extentsOff: extentsOffset(rootEntries),
             slotsOff: slotsOffset(rootEntries))

proc attachLivePage*(f: File, rootEntries: int,
                     skipIdentityCheck = false): Result[PageReader, string] =
  ## Attach to the page of the container open as `f`, read-only (§6,
  ## "Discovery"): only if the object exists, its magic and version are known,
  ## the identity stored in it equals the opened file's, and its state is
  ## `live`. Anything else is "not attached", with the reason.
  ## `skipIdentityCheck` is the mutation arm of `test_live_page_lifetime`;
  ## no reader passes it.
  let id = ? fileIdentity(f)
  let name = pageName(id)
  var v: PageView
  var h = 0
  if not shmOpen(name, false, v, h):
    return err("no live coordination page " & name)
  var r = pageReaderOfView(v, rootEntries)
  r.name = name
  when defined(windows): r.handle = h
  template refuse(msg: string) =
    shmUnmap(r.view, h)
    return err(msg)
  if v.len < r.slotsOff:
    refuse("live coordination page " & name & " is too small")
  if v.load(0) != PageMagicVersion:
    refuse("live coordination page " & name & " has an unknown magic or version")
  if not skipIdentityCheck and identityOf(v) != id:
    refuse("live coordination page " & name & " belongs to another file " &
      "(its identity differs from the container's)")
  if v.load(OffState) != PageStateLive:
    refuse("live coordination page " & name & " is not live (state " &
      $v.load(OffState) & ")")
  ok(r)

proc detach*(r: var PageReader) =
  when defined(windows): shmUnmap(r.view, r.handle)
  else: shmUnmap(r.view, 0)

proc state*(r: PageReader): uint64 = r.view.load(OffState)
proc sealEpoch*(r: PageReader): uint64 = r.view.load(OffSealEpoch)
proc overflowed*(r: PageReader): uint64 = r.view.load(OffOverflowed)
proc writerEpoch*(r: PageReader, slot: int): uint64 =
  r.view.load(OffWriters + slot * WriterSlotSize + 24)

proc readRoot*(r: PageReader, entry: int, retries = 8):
    tuple[state: SlotRead, size, mapBlock, name: uint64] =
  ## Entry `entry`'s root mirror, retried a bounded number of times.
  var w: array[3, uint64]
  for _ in 0 ..< retries:
    case readSlot(r.view, OffRootMirror + RootMirrorSlotSize * entry, w)
    of srOk: return (srOk, w[0], w[1], w[2])
    of srNever: return (srNever, 0'u64, 0'u64, 0'u64)
    of srRetry: discard
  (srRetry, 0'u64, 0'u64, 0'u64)

proc extentAt*(r: PageReader, idx: int): Result[MirrorExtent, void] =
  let e = r.extentsOff + ExtentSize * idx
  let words = r.view.load(e + 32)
  if words == 0: return err()
  ok(MirrorExtent(index: idx, entry: r.view.loadRelaxed(e),
    first: r.view.loadRelaxed(e + 8), count: r.view.loadRelaxed(e + 16),
    slotOff: int(r.view.loadRelaxed(e + 24)), words: int(words)))

proc findExtent*(r: PageReader, entry: uint64, rec: uint64):
    Result[MirrorExtent, void] =
  ## The extent that mirrors record `rec` of member entry `entry`.
  let used = min(int(r.view.load(OffExtentsUsed)), PageExtents)
  for i in 0 ..< used:
    let x = r.extentAt(i)
    if x.isOk:
      let e = x.get()
      if e.entry == entry and rec >= e.first and rec < e.first + e.count and
          e.slotOffset(rec) + 8 + 8 * e.words <= r.view.len:
        return ok(e)
  err()

proc reapAbandonedPage*(f: File): tuple[found, reaped: bool] =
  ## The next opener's duty (§6, "Every writer dies"): if the container's page
  ## exists and every writer it names is dead, mark it `abandoned` and remove
  ## its name.
  let id = fileIdentity(f)
  if id.isErr: return (false, false)
  let name = pageName(id.get())
  var v: PageView
  var h = 0
  if not shmOpen(name, true, v, h):
    return (false, false)
  if v.len < OffWriters + PageWriterSlots * WriterSlotSize or
      v.load(0) != PageMagicVersion:
    shmUnmap(v, h)
    return (true, false)
  if identityOf(v) != id.get():
    shmUnmap(v, h)
    return (true, false)
  if v.load(OffState) == PageStateLive and liveWriters(v).len > 0:
    shmUnmap(v, h)
    return (true, false)
  v.store(OffState, PageStateAbandoned)
  shmUnmap(v, h)
  shmUnlink(name)
  (true, true)
