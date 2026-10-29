## The in-guest writer (`ctfs-container.md` §6, "How a writer's liveness is
## judged"): a writer that runs inside a process another tool records starts
## no thread, stores its heartbeat only inside its seals, marks its writer
## slot `liveness = process`, and is judged alive by its pid and start time,
## so a reader does not reap the page of a guest that is quiet for longer
## than the heartbeat's 10 seconds. The file-only form hosts no page at all.
##
## 1. Threads: the threads of this process before `createCtfsStreaming(...,
##    livePage = lpmInGuest)` and after creation, writes, seals and close are
##    the same threads (no thread appeared, the counts are equal). Control: a
##    hosted writer's creation adds exactly one thread, so the count sees the
##    heartbeat thread when there is one.
## 2. The slot: state live, `liveness = process`; the heartbeat word does not
##    move while the writer makes no seal, and moves at a seal. Judged at a
##    clock 60 s later the in-guest slot is alive and a hosted slot is not.
## 3. Following an in-guest writer in another process: a reader attaches;
##    the writer stays quiet (no seal) for 12 s; a fresh reader then still
##    attaches, the held follower stays attached, and the writer's next seal
##    reaches the follower through the page; at close the follower detaches
##    and reads the final file.
## 4. A killed in-guest writer is found dead by its pid: a fresh reader does
##    not attach and reaps the page.
## 5. `lpmFileOnly`: no page object is created and no thread; a reader
##    follows the file alone, its refusal naming the missing page, and sees
##    each seal's sizes from the file.
##
## Null predictions (mutation arms, run as children with
## `-d:ctfsKeyedMutation`; each exits 3 when this test catches it):
## - `inguest-starts-heartbeat`: the in-guest writer starts the heartbeat
##   thread anyway. Part 1 must see a new thread.
## - `inguest-heartbeat-liveness`: the in-guest slot keeps the heartbeat
##   rule. Part 3 must see the quiet guest's page reaped (the fresh reader
##   does not attach).

import std/[os, osproc, strutils, sets]
import results
import codetracer_ctfs/[types, container, streaming, live_page, live_reader]

when defined(windows):
  type ThreadEntry32 {.pure.} = object
    dwSize, cntUsage, th32ThreadID, th32OwnerProcessID: uint32
    tpBasePri, tpDeltaPri: int32
    dwFlags: uint32
  proc createToolhelp32Snapshot(flags, pid: uint32): int {.stdcall,
    dynlib: "kernel32", importc: "CreateToolhelp32Snapshot".}
  proc thread32First(h: int, e: ptr ThreadEntry32): int32 {.stdcall,
    dynlib: "kernel32", importc: "Thread32First".}
  proc thread32Next(h: int, e: ptr ThreadEntry32): int32 {.stdcall,
    dynlib: "kernel32", importc: "Thread32Next".}
  proc closeHandle(h: int): int32 {.stdcall, dynlib: "kernel32",
    importc: "CloseHandle".}
elif defined(macosx):
  proc proc_pidinfo(pid: cint, flavor: cint, arg: uint64, buffer: pointer,
                    size: cint): cint {.importc, header: "<libproc.h>".}

proc threadsOfThisProcess(): HashSet[uint64] =
  ## The ids of this process's threads (on macOS, which lists no thread ids
  ## without the task port, one placeholder per thread: the count).
  when defined(windows):
    let pid = uint32(getCurrentProcessId())
    let h = createToolhelp32Snapshot(4, 0)  # TH32CS_SNAPTHREAD
    doAssert h != -1, "CreateToolhelp32Snapshot failed"
    var e = ThreadEntry32(dwSize: uint32(sizeof(ThreadEntry32)))
    var more = thread32First(h, addr e)
    while more != 0:
      if e.th32OwnerProcessID == pid: result.incl uint64(e.th32ThreadID)
      e.dwSize = uint32(sizeof(ThreadEntry32))
      more = thread32Next(h, addr e)
    discard closeHandle(h)
  elif defined(macosx):
    # struct proc_taskinfo (PROC_PIDTASKINFO = 4), 96 bytes; pti_threadnum
    # is the i32 at offset 84.
    var buf: array[96, byte]
    let n = proc_pidinfo(cint(getCurrentProcessId()), 4, 0, addr buf[0],
                         cint(buf.len))
    doAssert n == cint(buf.len), "proc_pidinfo(PROC_PIDTASKINFO) failed"
    var count: int32
    copyMem(addr count, addr buf[84], 4)
    for i in 0 ..< int(count): result.incl uint64(i)
  else:
    for kind, p in walkDir("/proc/self/task"):
      result.incl parseBiggestUInt(extractFilename(p))

proc tmp(name: string): string =
  getTempDir() / ("ckf4n_inguest_" & name & "_" & $getCurrentProcessId() & ".ct")

var failures: seq[string]
proc expect(cond: bool, what: string) =
  if not cond:
    failures.add what
    echo "FAIL: ", what

proc pageNameOf(path: string): string =
  var f: File
  doAssert open(f, path, fmRead)
  result = pageName(fileIdentity(f).get())
  close(f)

proc waitFor(path: string, tries = 1000) =
  for _ in 0 ..< tries:
    if fileExists(path): return
    sleep(20)
  doAssert false, "never appeared: " & path

proc writeSome(c: var Ctfs, f: var CtfsInternalFile, n: int, b: byte) =
  var bytes = newSeq[byte](n)
  for i in 0 ..< n: bytes[i] = b
  doAssert c.writeToFile(f, bytes).isOk

# ---------------------------------------------------------------------------
# 1. Threads
# ---------------------------------------------------------------------------

proc threadsPart(): tuple[newThreads: int, before, after: int] =
  ## The threads an in-guest writer's whole life adds: (new thread ids seen
  ## at any of the checkpoints, the count before, the count after).
  let path = tmp("threads")
  removeFile(path)
  let before = threadsOfThisProcess()
  var seen = initHashSet[uint64]()
  template look() =
    for t in threadsOfThisProcess():
      if t notin before: seen.incl t
  var c = createCtfsStreaming(path, livePage = lpmInGuest).get()
  look()
  var f = c.addFile("x.dat").get()
  for i in 0 ..< 20:
    c.writeSome(f, 3000, byte(i))
    c.publish()
    look()
  sleep(500)  # a heartbeat thread would have stored twice by now
  look()
  doAssert c.closeCtfs().isOk
  look()
  let after = threadsOfThisProcess()
  removeFile(path)
  (seen.len, before.len, after.len)

proc hostedControl(): int =
  ## A hosted writer's creation: the threads it adds while it lives.
  let path = tmp("hosted")
  removeFile(path)
  let before = threadsOfThisProcess()
  var c = createCtfsStreaming(path).get()
  var added = 0
  for t in threadsOfThisProcess():
    if t notin before: inc added
  doAssert c.closeCtfs().isOk
  removeFile(path)
  added

# ---------------------------------------------------------------------------
# 3. A follower in another process
# ---------------------------------------------------------------------------

const QuietMs = 12_000

proc followChild(path: string) =
  ## The in-guest writer: seal once, then make no seal for `QuietMs`, then
  ## seal again, then close when the parent says so.
  var c = createCtfsStreaming(path, livePage = lpmInGuest).get()
  var f = c.addFile("x.dat").get()
  c.writeSome(f, 5000, 1)
  c.publish()
  writeFile(path & ".sealed1", "1")
  sleep(QuietMs)
  c.writeSome(f, 7000, 2)
  c.publish()
  writeFile(path & ".sealed2", "1")
  waitFor(path & ".close", 3000)
  doAssert c.closeCtfs().isOk
  writeFile(path & ".closed", "1")
  quit(0)

proc followerPart(): string =
  ## "" when the follower behaves as §6 requires; otherwise what went wrong.
  let path = tmp("follow")
  removeFile(path)
  for s in [".sealed1", ".sealed2", ".close", ".closed"]: removeFile(path & s)
  var p = startProcess(getAppFilename(), args = ["--follow-child", path],
                       options = {poParentStreams})
  defer:
    if p.running: p.terminate()
    discard p.waitForExit()
    close(p)
    for s in [".sealed1", ".sealed2", ".close", ".closed"]: removeFile(path & s)
    removeFile(path)
  waitFor(path & ".sealed1")
  var follower = openLiveReader(path).get()
  defer: follower.close()
  if not follower.attached:
    return "the follower did not attach to the in-guest writer's page: " &
      follower.attachRefusal
  let slot = follower.findSlot("x.dat")
  if follower.memberSize(slot) != 5000:
    return "the follower's x.dat size is " & $follower.memberSize(slot) & ", not 5000"
  # Past the heartbeat's limit with no seal: the guest is quiet, not dead.
  sleep(QuietMs - 1000)
  if fileExists(path & ".sealed2"):
    return "the child sealed early; the quiet window was not tested"
  var fresh = openLiveReader(path).get()
  let freshAttached = fresh.attached
  let refusal = fresh.attachRefusal
  fresh.close()
  if not freshAttached:
    return "a reader opened after 11 s without a seal did not attach to the " &
      "live in-guest writer's page: " & refusal
  block:
    let rr = follower.refresh()
    if rr.isErr: return "refresh: " & rr.error
  if not follower.attached:
    return "the held follower lost the page during the quiet window"
  waitFor(path & ".sealed2")
  block:
    let rr = follower.refresh()
    if rr.isErr: return "refresh: " & rr.error
  if not follower.attached:
    return "the follower is not attached after the second seal"
  if follower.memberSize(slot) != 12000:
    return "after the second seal the follower's x.dat size is " &
      $follower.memberSize(slot) & ", not 12000"
  writeFile(path & ".close", "1")
  waitFor(path & ".closed")
  block:
    let rr = follower.refresh()
    if rr.isErr: return "refresh: " & rr.error
  if follower.attached:
    return "the follower stayed attached after the writer closed"
  if follower.memberSize(slot) != 12000:
    return "after close the follower's x.dat size is " & $follower.memberSize(slot)
  ""

# ---------------------------------------------------------------------------
# 4. A killed in-guest writer
# ---------------------------------------------------------------------------

proc killedChild(path: string) =
  var c = createCtfsStreaming(path, livePage = lpmInGuest).get()
  var f = c.addFile("x.dat").get()
  c.writeSome(f, 4000, 9)
  c.publish()
  writeFile(path & ".ready", "1")
  while true: sleep(1000)

when isMainModule:
  let args = commandLineParams()
  if args.len == 2 and args[0] == "--follow-child":
    followChild(args[1])
  if args.len == 2 and args[0] == "--killed-child":
    killedChild(args[1])
  if args.len == 2 and args[0] == "--arm":
    # A mutation arm's child: exit 3 when this test catches the arm.
    case args[1]
    of "inguest-starts-heartbeat":
      let t = threadsPart()
      echo "  arm inguest-starts-heartbeat: ", t.newThreads, " new thread(s)"
      quit(if t.newThreads > 0: 3 else: 0)
    of "inguest-heartbeat-liveness":
      let msg = followerPart()
      echo "  arm inguest-heartbeat-liveness: ", (if msg.len > 0: "caught: " & msg else: "not caught")
      quit(if msg.len > 0: 3 else: 0)
    else:
      quit(1)

  echo "threads"
  let t = threadsPart()
  echo "  in-guest writer: ", t.before, " threads before, ", t.after,
    " after, ", t.newThreads, " new during its life"
  expect(t.newThreads == 0, "the in-guest writer started " & $t.newThreads & " thread(s)")
  expect(t.before == t.after, "thread count " & $t.before & " before and " &
    $t.after & " after the in-guest writer")
  let added = hostedControl()
  echo "  control, hosted writer: ", added, " thread(s) added"
  expect(added == 1, "the hosted writer's creation added " & $added &
    " threads, not its one heartbeat thread: the count does not see it")

  echo "the in-guest slot"
  block:
    let path = tmp("slot")
    removeFile(path)
    var c = createCtfsStreaming(path, livePage = lpmInGuest).get()
    let b = OffWriters + c.page.writerSlot * WriterSlotSize
    expect(c.page.view.load(b + 32) == WriterSlotLive, "the in-guest slot is not live")
    expect(c.page.view.load(b + OffSlotLiveness) == WriterLivenessProcess,
      "the in-guest slot's liveness is " & $c.page.view.load(b + OffSlotLiveness))
    let hb0 = c.page.view.load(b + 16)
    sleep(600)
    expect(c.page.view.load(b + 16) == hb0,
      "the in-guest heartbeat moved without a seal")
    var f = c.addFile("x.dat").get()
    c.writeSome(f, 100, 1)
    c.publish()
    expect(c.page.view.load(b + 16) > hb0, "the in-guest heartbeat did not move at a seal")
    let later = monotonicNs() + 60_000_000_000'u64
    expect(c.page.writerSlot in liveWritersAt(c.page.view, later),
      "an in-guest writer was judged dead by its heartbeat's age")
    let hpath = tmp("slothosted")
    removeFile(hpath)
    var h = createCtfsStreaming(hpath).get()
    expect(h.page.view.load(OffWriters + h.page.writerSlot * WriterSlotSize +
      OffSlotLiveness) == WriterLivenessHeartbeat, "a hosted slot's liveness is not heartbeat")
    expect(h.page.writerSlot notin liveWritersAt(h.page.view, later),
      "a hosted writer whose heartbeat is 60 s old was judged alive")
    doAssert h.closeCtfs().isOk
    doAssert c.closeCtfs().isOk
    removeFile(path); removeFile(hpath)

  echo "a follower in another process (", QuietMs div 1000, " s without a seal)"
  let msg = followerPart()
  expect(msg.len == 0, "follower: " & msg)
  if msg.len == 0: echo "  attached throughout; the quiet guest was not reaped"

  echo "a killed in-guest writer"
  block:
    let path = tmp("killed")
    removeFile(path); removeFile(path & ".ready")
    var p = startProcess(getAppFilename(), args = ["--killed-child", path],
                         options = {poParentStreams})
    waitFor(path & ".ready")
    var held = openLiveReader(path).get()
    expect(held.attached, "a reader did not attach to the in-guest child's page")
    p.terminate()
    discard p.waitForExit()
    close(p)
    var r = openLiveReader(path).get()
    expect(not r.attached, "a reader attached to the page of a killed in-guest writer")
    expect("no live writer" in r.attachRefusal, "the refusal: " & r.attachRefusal)
    expect(held.page.state() == PageStateAbandoned,
      "the killed in-guest writer's page was not marked abandoned")
    expect(r.memberSize(r.findSlot("x.dat")) == 4000, "the file-only size after the kill")
    r.close()
    held.close()
    removeFile(path); removeFile(path & ".ready")

  echo "file-only"
  block:
    let path = tmp("fileonly")
    removeFile(path)
    let before = threadsOfThisProcess()
    var c = createCtfsStreaming(path, livePage = lpmFileOnly).get()
    expect(c.page == nil, "a file-only writer has a page")
    expect(not pageObjectExists(pageNameOf(path)), "a file-only writer created a page object")
    var newThreads = 0
    for x in threadsOfThisProcess():
      if x notin before: inc newThreads
    expect(newThreads == 0, "a file-only writer started " & $newThreads & " thread(s)")
    var f = c.addFile("x.dat").get()
    c.writeSome(f, 3000, 1)
    c.publish()
    var r = openLiveReader(path).get()
    expect(not r.attached, "a reader attached to a file-only writer's container")
    expect("no live coordination page" in r.attachRefusal, "the refusal: " & r.attachRefusal)
    let slot = r.findSlot("x.dat")
    expect(r.memberSize(slot) == 3000, "file-only follow: size " & $r.memberSize(slot))
    c.writeSome(f, 2000, 2)
    c.publish()
    doAssert r.refresh().isOk
    expect(r.memberSize(slot) == 5000, "file-only follow after a second seal: size " &
      $r.memberSize(slot))
    doAssert c.closeCtfs().isOk
    doAssert r.refresh().isOk
    expect(r.memberSize(slot) == 5000, "file-only follow after close: size " & $r.memberSize(slot))
    r.close()
    removeFile(path)

  echo "mutation arms"
  when defined(ctfsKeyedMutation):
    for arm in ["inguest-starts-heartbeat", "inguest-heartbeat-liveness"]:
      putEnv("CTFS_KEYED_MUTATE", arm)
      let p = startProcess(getAppFilename(), args = ["--arm", arm],
                           options = {poParentStreams})
      let code = p.waitForExit()
      close(p)
      delEnv("CTFS_KEYED_MUTATE")
      expect(code == 3, "arm " & arm & " was not caught (exit " & $code & ")")
  else:
    failures.add "built without -d:ctfsKeyedMutation: the arms did not run"

  if failures.len > 0:
    echo "FAIL test_live_page_in_guest"
    quit(1)
  echo "PASS test_live_page_in_guest"
