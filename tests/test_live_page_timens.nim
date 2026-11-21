## A writer and a reader in different time namespaces (`ctfs-container.md`
## §6, "How a writer's liveness is judged"; Linux only).
##
## Linux reports a process's start time (`/proc/<pid>/stat` field 22) shifted
## into the READING process's time namespace, and `CLOCK_MONOTONIC` shifted
## into the CALLING process's. A writer in another time namespace, sharing
## this process's pid namespace and /dev/shm, therefore records a start time
## and a heartbeat that this process reads on other clocks. This test forks
## real writers into real time namespaces (created unprivileged through a user
## namespace) and judges them from this process, which stays in its own:
##
## a. a live hosted writer whose boot clock is 1000 s ahead. Its recorded
##    start time is not what this process reads for its pid (the control), so
##    a reader that compared start times across time namespaces would call it
##    dead. A reader attaches (`attachLivePage`, which reaps a page with no
##    live writer); `reapAbandonedPage` does not reap; a co-writer joined in
##    this process does not settle it as dead; the page stays `live` and
##    named, and its slot is in `liveWriters`;
## b. the same for a live hosted writer whose monotonic clock is 100 s behind
##    (less when the host has been up for less). Its start time reads the same
##    here, but a heartbeat on its raw `CLOCK_MONOTONIC` is 100 s old to this
##    process the moment it is stored (the control), so it would be judged
##    dead by its heartbeat;
## c. a live in-guest writer whose boot clock is 1000 s ahead is alive when
##    judged 60 s from now: its start time cannot be compared here, and it has
##    no heartbeat rule;
## d. a hosted writer whose monotonic and boot clocks are 1000 s ahead is
##    killed with SIGKILL. It is then judged dead once its heartbeat is older
##    than 10 s (the test waits that long), and a reader that attaches reaps
##    its page. A heartbeat on its raw clock would be 1000 s in this process's
##    future (the control) and never age, so the page would never be reaped;
## e. the writer stays in the original namespace and the reader enters a
##    namespace whose clocks are shifted, in both monotonic directions. A
##    writer in the reader's own shifted namespace exercises legacy slot 0;
## f. a legacy slot-0 writer in another time namespace is kept alive: its raw
##    heartbeat cannot be assigned a machine-clock age by this reader.
##
## Null predictions. Against a reader that compares start times across time
## namespaces (the code before the fix, and the arm
## `timens-start-time-compared`), cases a and c fail: the live writer is
## judged dead, `attachLivePage` marks its page abandoned and removes its
## name. Against a heartbeat on the raw monotonic clock (the code before the
## fix, and the arm `heartbeat-raw-clock`), case b fails the same way and
## case d never reaps the killed writer's page. The arms run as children,
## which exit 3 when the case catches them.
##
## Every page is created by a real writer and judged by the production
## reader. The legacy writer fixture replaces only the slot's liveness tag
## and heartbeat stores with the pre-fix protocol (tag 0, raw monotonic
## clock); this is necessary to exercise existing pages without requiring a
## historical compiler checkout in the installed test suite. Where
## the host forbids unprivileged user or time namespaces, the Linux test
## fails with the missing prerequisite. Other platforms announce that time
## namespaces are Linux-only.

when not defined(linux):
  echo "SKIP: test_live_page_timens: time namespaces exist on Linux only " &
    "(ctfs-container.md §6, \"How a writer's liveness is judged\")"
else:

  import std/[os, osproc, posix, strutils]
  import results
  import codetracer_ctfs/[types, container, streaming, live_page]

  const
    CloneNewTime = 0x0000_0080'i32
    CloneNewUser = 0x1000_0000'i32
    RootEntries = int(DefaultMaxRootEntries)

  proc unshare(flags: cint): cint {.importc, header: "<sched.h>".}

  var failures: seq[string]
  var inArm = false
    ## A mutation arm's child: a failure there is the arm being caught, which
    ## the child reports once, so it does not print each as a FAIL.
  proc expect(cond: bool, what: string) =
    if not cond:
      failures.add what
      if not inArm: echo "FAIL: ", what

  proc tmp(name: string): string =
    getTempDir() / ("ctfs_timens_" & name & "_" & $getCurrentProcessId() & ".ct")

  proc rawMonotonicNs(): uint64 =
    ## `CLOCK_MONOTONIC` as this process reads it, in its own time namespace.
    var ts: Timespec
    discard clock_gettime(CLOCK_MONOTONIC, ts)
    uint64(ts.tv_sec) * 1_000_000_000'u64 + uint64(ts.tv_nsec)

  proc enterUserNs(extra: cint): bool =
    ## Unshare a user namespace (and `extra`), mapping this uid and gid to 0
    ## inside, so files created there are this user's outside.
    let uid = getuid()
    let gid = getgid()
    if unshare(CloneNewUser or extra) != 0:
      return false
    try:
      writeFile("/proc/self/setgroups", "deny")
      writeFile("/proc/self/uid_map", "0 " & $uid & " 1")
      writeFile("/proc/self/gid_map", "0 " & $gid & " 1")
    except IOError:
      return false
    true

  proc writer(path: string, mode: LivePageMode, legacy = false) =
    ## A writer: create a container (and so its page), publish once, say so
    ## with its raw monotonic clock, and wait to be killed.
    var c = createCtfsStreaming(path,
      livePage = (if legacy: lpmInGuest else: mode)).get()
    var f = c.addFile("x.dat").get()
    doAssert c.writeToFile(f, [1'u8, 2, 3]).isOk
    c.publish()
    let b = OffWriters + c.page.writerSlot * WriterSlotSize
    if legacy:
      c.page.view.store(b + OffSlotLiveness, WriterLivenessHeartbeat)
      c.page.view.store(b + 16, rawMonotonicNs())
    writeFile(path & ".clock", $rawMonotonicNs())
    writeFile(path & ".ready", "1")
    while true:
      if legacy: c.page.view.store(b + 16, rawMonotonicNs())
      sleep(200)

  proc timeNsWriter(path, offsets, mode: string) =
    ## Fork a writer into a new time namespace with `offsets` (lines of
    ## `/proc/self/timens_offsets`, `;`-separated), in this pid namespace.
    if not enterUserNs(CloneNewTime):
      writeFile(path & ".skip", "unshare(CLONE_NEWUSER|CLONE_NEWTIME): errno " & $errno)
      quit(0)
    try:
      writeFile("/proc/self/timens_offsets", offsets.replace(";", "\n"))
    except IOError:
      writeFile(path & ".skip", "cannot write timens_offsets " & offsets)
      quit(0)
    let pid = fork()
    if pid == 0:
      writer(path, if mode == "inguest": lpmInGuest else: lpmHosted,
        legacy = mode == "legacy")
    var status: cint
    discard waitpid(pid, status, 0)
    quit(0)

  proc pageNameOf(path: string): string =
    var f: File
    doAssert open(f, path, fmRead)
    result = pageName(fileIdentity(f).get())
    close(f)

  type Started = object
    path, name: string
    process: Process
    writerPid: uint64
    recordedStart: uint64
    writerRaw: uint64   ## the writer's raw monotonic clock, at ready
    ourRaw: uint64      ## this process's, read just after
    unavailable: string

  proc startWriter(tag, offsets, mode: string): Started =
    result.path = tmp(tag)
    let path = result.path
    for ext in ["", ".ready", ".skip", ".clock"]: removeFile(path & ext)
    result.process = startProcess(getAppFilename(),
      args = (if offsets == "host": @["--writer", path, mode]
              else: @["--timens", path, offsets, mode]),
      options = {poParentStreams})
    while true:
      if fileExists(path & ".ready") or fileExists(path & ".skip"): break
      if not result.process.running(): break
      sleep(20)
    if not fileExists(path & ".ready"):
      result.unavailable = try: readFile(path & ".skip")
        except IOError: "writer did not become ready (exit " &
          $result.process.peekExitCode() & ")"
      return
    result.writerRaw = parseBiggestUInt(readFile(path & ".clock"))
    result.ourRaw = rawMonotonicNs()
    result.name = pageNameOf(path)
    var f: File
    doAssert open(f, path, fmRead)
    var r = attachLivePage(f, RootEntries, reapIfAbandoned = false).get()
    close(f)
    result.writerPid = r.view.load(OffWriters)
    result.recordedStart = r.view.load(OffWriters + 8)
    r.detach()

  proc finish(s: var Started) =
    if s.writerPid != 0:
      discard posix.kill(Pid(s.writerPid), SIGKILL)
    if s.name.len > 0:
      discard shm_unlink(cstring("/" & s.name))
    discard s.process.waitForExit()
    close(s.process)
    for ext in ["", ".ready", ".skip", ".clock"]: removeFile(s.path & ext)

  proc checkKeptAlive(s: Started, what: string) =
    ## A reader, a reaper and a co-writer in this process all leave the live
    ## writer's page alone.
    var f: File
    doAssert open(f, s.path, fmRead)
    let attached = attachLivePage(f, RootEntries)
    expect(attached.isOk, what & ": a reader did not attach: " &
      (if attached.isErr: attached.error else: ""))
    if attached.isOk:
      var r = attached.get()
      expect(r.state() == PageStateLive, what & ": the page's state is " & $r.state())
      expect(0 in liveWriters(r.view), what & ": the writer's slot is not in liveWriters")
      r.detach()
    expect(pageObjectExists(s.name), what & ": the reader removed the page's name")
    let reaped = reapAbandonedPage(f)
    expect(not reaped.reaped, what & ": reapAbandonedPage reaped the page")
    let replaced = createLivePage(f, RootEntries)
    expect(replaced.isErr and "live writers" in replaced.error,
      what & ": createLivePage did not refuse to replace the live page")
    if replaced.isOk: abandonLivePage(replaced.get())
    let joined = joinLivePage(f, RootEntries)
    expect(joined.isOk, what & ": a co-writer could not join: " &
      (if joined.isErr: joined.error else: ""))
    if joined.isOk:
      let p = joined.get()
      let settled = settleDeadWriters(p)
      expect(0 notin settled, what & ": a co-writer settled the live writer as dead")
      expect(p.view.load(OffWriters + 32) == WriterSlotLive,
        what & ": the writer's slot is no longer live")
      expect(p.view.load(OffState) == PageStateLive,
        what & ": the page's state is " & $p.view.load(OffState) & " after the co-writer")
      abandonLivePage(p)   # unmap only: the co-writer's slot stays, its process lives
    close(f)
    expect(pageObjectExists(s.name), what & ": the page's name is gone")

  proc caseA(): string =
    ## Returns "" when the case passed, else what failed (for the arms).
    let before = failures.len
    var s = startWriter("boot", "boottime 1000 0", "hosted")
    if s.unavailable.len > 0:
      expect(false, "case a prerequisite: " & s.unavailable)
      finish(s)
      return failures[before]
    let here = processStartTime(s.writerPid)
    expect(here != 0 and here != s.recordedStart,
      "a (control): the writer (pid " & $s.writerPid & ") has start time " &
      $s.recordedStart & " here too: the case cannot tell the outcomes apart")
    checkKeptAlive(s, "a, a live hosted writer whose boot clock is 1000 s ahead")
    finish(s)
    if failures.len > before: failures[before] else: ""

  proc monotonicBehindSecs(): int =
    ## How far behind to set a writer's monotonic clock: 100 s, less on a host
    ## up for less than about 200 s (the clock may not go below zero).
    let up = int(rawMonotonicNs() div 1_000_000_000'u64)
    min(100, up div 2)

  proc caseB(): string =
    let before = failures.len
    let behind = monotonicBehindSecs()
    if behind <= 12:
      expect(false, "case b prerequisite: host uptime must exceed 26 s")
      return failures[before]
    var s = startWriter("mono", "monotonic -" & $behind & " 0", "hosted")
    if s.unavailable.len > 0:
      expect(false, "case b prerequisite: " & s.unavailable)
      finish(s)
      return failures[before]
    expect(s.ourRaw > s.writerRaw and s.ourRaw - s.writerRaw > HeartbeatDeadNs,
      "b (control): the writer's raw monotonic clock " & $s.writerRaw &
      " is not more than 10 s behind this process's " & $s.ourRaw)
    checkKeptAlive(s, "b, a live hosted writer whose monotonic clock is " &
      $behind & " s behind")
    finish(s)
    if failures.len > before: failures[before] else: ""

  proc caseC(): string =
    let before = failures.len
    var s = startWriter("guest", "boottime 1000 0", "inguest")
    if s.unavailable.len > 0:
      expect(false, "case c prerequisite: " & s.unavailable)
      finish(s)
      return failures[before]
    let here = processStartTime(s.writerPid)
    expect(here != 0 and here != s.recordedStart,
      "c (control): the in-guest writer has start time " & $s.recordedStart &
      " here too: the case cannot tell the outcomes apart")
    var f: File
    doAssert open(f, s.path, fmRead)
    var r = attachLivePage(f, RootEntries, reapIfAbandoned = false).get()
    expect(r.view.load(OffWriters + OffSlotLiveness) == WriterLivenessProcess,
      "c: the in-guest writer's slot liveness is " & $r.view.load(OffWriters + OffSlotLiveness))
    let later = machineMonotonicNs().ns + 60_000_000_000'u64
    expect(0 in liveWritersAt(r.view, later),
      "c: a live in-guest writer whose boot clock is 1000 s ahead was judged dead")
    r.detach()
    close(f)
    checkKeptAlive(s, "c, a live in-guest writer whose boot clock is 1000 s ahead")
    finish(s)
    if failures.len > before: failures[before] else: ""

  proc caseD(): string =
    let before = failures.len
    var s = startWriter("killed", "monotonic 1000 0;boottime 1000 0", "hosted")
    if s.unavailable.len > 0:
      expect(false, "case d prerequisite: " & s.unavailable)
      finish(s)
      return failures[before]
    expect(s.writerRaw > s.ourRaw and s.writerRaw - s.ourRaw > 900_000_000_000'u64,
      "d (control): the writer's raw monotonic clock " & $s.writerRaw &
      " is not 1000 s ahead of this process's " & $s.ourRaw)
    var f: File
    doAssert open(f, s.path, fmRead)
    var r = attachLivePage(f, RootEntries, reapIfAbandoned = false).get()
    expect(0 in liveWriters(r.view), "d: the writer is not alive before the kill")
    discard posix.kill(Pid(s.writerPid), SIGKILL)
    s.writerPid = 0
    let later = machineMonotonicNs().ns + 11_000_000_000'u64
    expect(0 notin liveWritersAt(r.view, later),
      "d: a killed writer whose heartbeat is 11 s old was judged alive")
    # The heartbeat really ages: wait past the limit, then a reader reaps.
    sleep(11_000)
    let again = attachLivePage(f, RootEntries)
    expect(again.isErr and "no live writer" in again.error,
      "d: a reader attached to a killed writer's page 11 s after the kill")
    expect(r.state() == PageStateAbandoned,
      "d: the killed writer's page is in state " & $r.state() & ", not abandoned")
    expect(not pageObjectExists(s.name), "d: the killed writer's page is still named")
    r.detach()
    close(f)
    finish(s)
    if failures.len > before: failures[before] else: ""

  proc timeNsReader(path, offsets: string) =
    ## The writer stays in the original namespace; this reader's clocks are
    ## shifted instead. The fork enters the namespace configured by unshare.
    doAssert enterUserNs(CloneNewTime), "reader: cannot unshare user/time namespaces"
    writeFile("/proc/self/timens_offsets", offsets.replace(";", "\n"))
    expect(not currentMachineClock().known,
      "reader before fork: child namespace offsets mistaken for own offsets")
    var original: File
    doAssert open(original, path, fmRead)
    var held = attachLivePage(original, RootEntries, reapIfAbandoned = false).get()
    expect(held.view.load(OffWriters + OffSlotLiveness) == WriterLivenessMachineHeartbeat,
      "unknown-clock reader control: original writer is not machine-heartbeat liveness")
    expect(0 in liveWritersAt(held.view, high(uint64)),
      "reader with unknown clock offset judged a machine heartbeat expired")
    held.detach()
    close(original)
    let unknownPath = path & ".unknown"
    var unknown = createCtfsStreaming(unknownPath).get()
    let unknownSlot = OffWriters + unknown.page.writerSlot * WriterSlotSize
    expect(unknown.page.view.load(unknownSlot + OffSlotLiveness) == WriterLivenessProcess,
      "writer with unknown clock offset did not select process liveness")
    expect(unknown.page.writerSlot in liveWritersAt(unknown.page.view, high(uint64)),
      "writer with unknown clock offset judged dead by heartbeat age")
    doAssert unknown.closeCtfs().isOk
    removeFile(unknownPath)
    let pid = fork()
    doAssert pid >= 0
    if pid == 0:
      expect(currentMachineClock().known, "reader: machine clock unknown after fork")
      var f: File
      doAssert open(f, path, fmRead)
      var r = attachLivePage(f, RootEntries, reapIfAbandoned = false).get()
      let writerPid = r.view.load(OffWriters)
      expect(processStartTime(writerPid) != r.view.load(OffWriters + 8),
        "reader control: writer start time did not shift into reader namespace")
      r.detach()
      close(f)
      checkKeptAlive(Started(path: path, name: pageNameOf(path)),
        "a reader whose own clocks are shifted")
      # Legacy slot 0 in the reader's own shifted namespace: compare its
      # heartbeat on the correct clock, keeping the live writer but still
      # detecting a heartbeat older than the timeout.
      var legacy = startWriter("legacy_same", "host", "legacy")
      if legacy.unavailable.len > 0:
        expect(false, "same-namespace legacy prerequisite: " & legacy.unavailable)
      else:
        checkKeptAlive(legacy, "legacy writer in the same shifted namespace")
        doAssert open(f, legacy.path, fmRead)
        var old = attachLivePage(f, RootEntries, reapIfAbandoned = false).get()
        expect(0 notin liveWritersAt(old.view,
          machineMonotonicNs().ns + 11_000_000_000'u64),
          "legacy heartbeat did not age on the machine clock")
        old.detach()
        close(f)
      finish(legacy)
      quit(if failures.len == 0: 0 else: 1)
    var status: cint
    doAssert waitpid(pid, status, 0) == pid
    quit(if failures.len == 0 and WIFEXITED(status) and WEXITSTATUS(status) == 0: 0 else: 1)

  proc caseE() =
    var s = startWriter("reader", "host", "hosted")
    if s.unavailable.len > 0:
      expect(false, "case e prerequisite: " & s.unavailable)
      finish(s)
      return
    for offset in ["monotonic -" & $monotonicBehindSecs() & " 0;boottime 1000 0",
                   "monotonic 1000 0;boottime 1000 0"]:
      let p = startProcess(getAppFilename(), args = ["--reader", s.path, offset],
        options = {poParentStreams})
      expect(p.waitForExit() == 0, "case e: reader in shifted namespace failed")
      close(p)
    finish(s)

  proc caseF() =
    var s = startWriter("legacy_other", "monotonic -" &
      $monotonicBehindSecs() & " 0;boottime 1000 0", "legacy")
    if s.unavailable.len > 0:
      expect(false, "case f prerequisite: " & s.unavailable)
    else:
      expect(s.ourRaw > s.writerRaw and s.ourRaw - s.writerRaw > HeartbeatDeadNs,
        "legacy control: raw clock did not shift more than the heartbeat timeout")
      checkKeptAlive(s, "legacy writer in another time namespace")
    finish(s)

  when isMainModule:
    let args = commandLineParams()
    if args.len == 3 and args[0] == "--writer":
      writer(args[1], if args[2] == "inguest": lpmInGuest else: lpmHosted,
        legacy = args[2] == "legacy")
    if args.len == 3 and args[0] == "--reader":
      timeNsReader(args[1], args[2])
    if args.len == 4 and args[0] == "--timens":
      timeNsWriter(args[1], args[2], args[3])
    if args.len == 2 and args[0] == "--arm":
      # A mutation arm's child: exit 3 when this test catches the arm.
      inArm = true
      let msg =
        case args[1]
        of "timens-start-time-compared": caseA() & caseC()
        of "heartbeat-raw-clock": caseB() & caseD()
        else: quit(1)
      echo "  arm ", args[1], ": ", (if msg.len > 0: "caught: " & msg else: "not caught")
      quit(if msg.len > 0: 3 else: 0)

    echo "a. a live hosted writer, boot clock 1000 s ahead"
    discard caseA()
    echo "b. a live hosted writer, monotonic clock behind"
    discard caseB()
    echo "c. a live in-guest writer, boot clock 1000 s ahead"
    discard caseC()
    echo "d. a killed hosted writer, monotonic and boot clocks 1000 s ahead"
    discard caseD()
    echo "e. a reader in a time namespace, writer in the original namespace"
    caseE()
    echo "f. a legacy raw-clock writer in another time namespace"
    caseF()

    echo "mutation arms"
    when defined(ctfsKeyedMutation):
      for arm in ["timens-start-time-compared", "heartbeat-raw-clock"]:
        putEnv("CTFS_KEYED_MUTATE", arm)
        let p = startProcess(getAppFilename(), args = ["--arm", arm],
                             options = {poParentStreams})
        let code = p.waitForExit()
        close(p)
        delEnv("CTFS_KEYED_MUTATE")
        expect(code == 3, "arm " & arm & " was not caught (exit " & $code & ")")
    else:
      failures.add "built without -d:ctfsKeyedMutation: the arms did not run"
      echo "FAIL: built without -d:ctfsKeyedMutation: the arms did not run"

    if failures.len > 0:
      echo "FAIL test_live_page_timens"
      quit(1)
    echo "PASS test_live_page_timens"
