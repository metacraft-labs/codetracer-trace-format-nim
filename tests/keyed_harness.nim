## Shared instruments of the keyed-member tests (not a test itself).
##
## # Stopping the world between any two writes
##
## A writer's taps (`Ctfs.writeTap`, `LivePage.tap`) record, in order, every
## write it hands the operating system and every word it publishes in its live
## coordination page. `TapLog` keeps them, with a mark after each seal the
## test saw complete. `Replay` then rebuilds the file and the page event by
## event, from the state they had when the taps were installed, and a test
## opens a reader (`openImageReader`) on the rebuilt pair after any event:
## exactly what a reader in another process sees if it reads between those
## two writes, and exactly what a writer killed after that write leaves. So
## one pass over the log is the kill-at-every-write test and the
## concurrent-reader test at once, for a file-only and for an attached reader.

import std/[os, osproc, strutils, times, random]
import results
import codetracer_ctfs/[types, live_page]

type
  TapEventKind* = enum teFile, tePage, teSeal

  TapEvent* = object
    kind*: TapEventKind
    off*: int64
    bytes*: seq[byte]
    words*: seq[uint64]
    pageKind*: PageTapKind
    seal*: int

  TapLog* = ref object
    events*: seq[TapEvent]
    file0*: seq[byte]
    page0*: seq[byte]
    seals*: int

proc installTaps*(c: var Ctfs, pageBytes = 1 shl 20): TapLog =
  ## Start recording `c`'s writes and page publications. The state at this
  ## point is the replay's starting point: the root region the file holds and
  ## the first `pageBytes` of the page.
  let log = TapLog()
  log.file0 = c.data[0 ..< c.fileRoot.len]
  if c.page != nil:
    let n = min(pageBytes, c.page.view.len)
    log.page0 = newSeq[byte](n)
    copyMem(addr log.page0[0], c.page.view.base, n)
  c.writeTap = proc (offset: int64, bytes: openArray[byte]) {.closure, gcsafe,
      raises: [].} =
    log.events.add TapEvent(kind: teFile, off: offset, bytes: @bytes)
  if c.page != nil:
    c.page.tap = proc (kind: PageTapKind, off: int,
                       words: openArray[uint64]) {.closure, gcsafe, raises: [].} =
      log.events.add TapEvent(kind: tePage, off: int64(off), words: @words,
                              pageKind: kind)
  log

proc markSeal*(log: TapLog) =
  ## A seal the test saw return.
  inc log.seals
  log.events.add TapEvent(kind: teSeal, seal: log.seals)

proc removeTaps*(c: var Ctfs) =
  c.writeTap = nil
  if c.page != nil: c.page.tap = nil

type
  Replay* = object
    log*: TapLog
    file*: ref seq[byte]
    page*: ref seq[byte]
    next*: int           ## the next event to apply
    sealsDone*: int      ## seals whose mark has been applied

proc initReplay*(log: TapLog, withPage: bool): Replay =
  result.log = log
  new(result.file)
  result.file[] = log.file0
  if withPage and log.page0.len > 0:
    new(result.page)
    result.page[] = log.page0

proc applyNext*(r: var Replay) =
  ## Apply one event to the rebuilt file and page.
  let e = r.log.events[r.next]
  inc r.next
  case e.kind
  of teFile:
    let endAt = int(e.off) + e.bytes.len
    if r.file[].len < endAt: r.file[].setLen(endAt)
    if e.bytes.len > 0:
      copyMem(addr r.file[][int(e.off)], unsafeAddr e.bytes[0], e.bytes.len)
  of tePage:
    if r.page == nil: return
    let start = case e.pageKind
      of ptWords: int(e.off) + 8
      of ptStore: int(e.off)
    let endAt = start + 8 * e.words.len
    if r.page[].len < endAt: r.page[].setLen(endAt)
    for i, w in e.words:
      for b in 0 ..< 8:
        r.page[][start + 8 * i + b] = byte((w shr (8 * b)) and 0xff)
    if e.pageKind == ptWords:
      # The slot's seq as a reader between two publications sees it: even, and
      # advanced by the publication (its odd window is inside one event).
      var s = 0'u64
      for b in 0 ..< 8: s = s or (uint64(r.page[][int(e.off) + b]) shl (8 * b))
      s += 2
      for b in 0 ..< 8: r.page[][int(e.off) + b] = byte((s shr (8 * b)) and 0xff)
  of teSeal:
    r.sealsDone = e.seal

proc atEnd*(r: Replay): bool = r.next >= r.log.events.len

proc fileWrites*(log: TapLog): int =
  for e in log.events:
    if e.kind == teFile: inc result

# ---------------------------------------------------------------------------
# Child processes, for real kills
# ---------------------------------------------------------------------------

proc selfExe*(): string = getAppFilename()

proc killHard*(p: Process) =
  ## TerminateProcess on Windows, SIGKILL elsewhere: nothing in the child
  ## runs after it.
  when defined(windows):
    p.terminate()
  else:
    p.kill()

proc appendLine*(path, line: string) =
  ## One complete line, appended and handed to the operating system before
  ## this returns (a seal log the parent reads after a kill).
  var f: File
  if not open(f, path, fmAppend):
    quit("cannot append to " & path, 2)
  f.write(line & "\n")
  f.flushFile()
  close(f)

proc logLine*(f: File, line: string) =
  ## One complete line on a log the writer holds open, handed to the
  ## operating system before this returns.
  f.write(line & "\n")
  f.flushFile()

proc lastSealLogged*(path: string): int =
  ## The last `done <n>` line of a writer's seal log: the last seal it saw
  ## complete before it was killed. A torn last line is ignored.
  result = 0
  if not fileExists(path): return
  let s = readFile(path)
  for line in s.split('\n'):
    if line.startsWith("done "):
      try: result = parseInt(line[5 .. ^1])
      except ValueError: discard

proc inSealAtKill*(path: string): bool =
  ## The log ends with an `intent` that has no `done`: the kill landed inside
  ## a seal.
  if not fileExists(path): return false
  var last = ""
  for line in readFile(path).split('\n'):
    if line.len > 0: last = line
  last.startsWith("intent ")

proc rng64*(x: var uint64): uint64 =
  ## splitmix64: the same stream in the writer and in the checker.
  x += 0x9E3779B97F4A7C15'u64
  var z = x
  z = (z xor (z shr 30)) * 0xBF58476D1CE4E5B9'u64
  z = (z xor (z shr 27)) * 0x94D049BB133111EB'u64
  z xor (z shr 31)

# ---------------------------------------------------------------------------
# A kill-at-random loop over a writer child
# ---------------------------------------------------------------------------

type
  KillCheck* = proc (path, logPath: string, seed: uint64, sealsDone: int): string {.closure.}
    ## "" or what the container killed after seal `sealsDone` got wrong.

proc killLoop*(kills: int, tag: string, writerArgs: proc (path, logPath: string,
               seed: uint64): seq[string] {.closure.}, check: KillCheck,
               rngSeed = 4242): tuple[ok: bool, msg: string] =
  ## Run the writer child once to time it, then `kills` times killed with
  ## TerminateProcess / SIGKILL at a uniformly random point of that time,
  ## checking each container from the file alone.
  let path = getTempDir() / ("ckf4n_kill_" & tag & "_" & $getCurrentProcessId() & ".ct")
  let logPath = path & ".seallog"
  removeFile(path); removeFile(logPath)
  let t0 = epochTime()
  var p = startProcess(selfExe(), args = writerArgs(path, logPath, 1),
                       options = {poParentStreams})
  discard p.waitForExit()
  close(p)
  let full = epochTime() - t0
  let fullSeals = lastSealLogged(logPath)
  var inSeal = 0
  var r = initRand(rngSeed)
  for i in 0 ..< kills:
    let seed = uint64(100 + i)
    removeFile(path); removeFile(logPath)
    var q = startProcess(selfExe(), args = writerArgs(path, logPath, seed),
                         options = {poParentStreams})
    sleep(r.rand(int(full * 1000.0)))
    q.killHard()
    discard q.waitForExit()
    close(q)
    if inSealAtKill(logPath): inc inSeal
    let d = lastSealLogged(logPath)
    let m = check(path, logPath, seed, d)
    if m.len > 0:
      return (false, "kill " & $i & " (seed " & $seed & ", after seal " & $d & "): " & m)
  removeFile(path); removeFile(logPath)
  (true, $kills & " kills (a full run: " & $fullSeals & " seals in " &
    formatFloat(full, ffDecimal, 2) & " s), " & $inSeal & " inside a seal")

proc runArmChild*(arm: string): int =
  ## Re-run this test binary with a mutation arm armed; its exit code.
  putEnv("CTFS_KEYED_MUTATE", arm)
  let p = startProcess(selfExe(), args = ["--arm", arm], options = {poParentStreams})
  result = p.waitForExit()
  close(p)
  delEnv("CTFS_KEYED_MUTATE")
