## A live writer owned by another user (`ctfs-container.md` §6, "How a
## writer's liveness is judged"; macOS only).
##
## A page can be shared across users: a recorder under `sudo` joins the page
## of an unprivileged coordinator. §6 calls a writer dead only "when its pid no
## longer names a process with its start time", so the unprivileged side must
## be able to read the start time of a process it does not own, and a pid that
## was reused must still read as dead. Pid 1 (launchd) is that process here: it
## is real, alive and root-owned, and the test needs no `sudo`.
##
## 1. `processStartTime(1)` is not 0, reads the same twice, and agrees to the
##    second with `ps -o lstart=`, a witness that shares no code with it;
## 2. the same for this process, which this user owns, and to the microsecond
##    the value `proc_pidinfo(PROC_PIDTBSDINFO)` gives, which can read a
##    process of this user: a writer built before the `sysctl` read recorded
##    that value, and a reader must keep recognising it;
## 3. on a real page, read through a reader's read-only attachment, a slot that
##    names pid 1 with its start time is alive under both liveness rules, is
##    listed by `liveWriters`, and is not settled as dead;
## 4. the same slot with a start time one microsecond off is dead under both
##    rules: the check that a reused pid is not the writer that recorded it;
## 5. a real slot whose writer was killed and reaped is dead, its pid naming no
##    process, however fresh its heartbeat, and a survivor settles it, which is
##    the control that the "not settled" of case 3 can be told apart;
## 6. a zombie counts as gone: a child that has exited and is not yet reaped
##    reads 0, while the same child read its start time when it was alive.
##
## Null prediction: on a build that reads the start time with
## `proc_pidinfo(PROC_PIDTBSDINFO)`, the call answers EPERM for another user's
## process, so `processStartTime(1)` is 0 and cases 1 and 3 fail; on a real page
## a coordinator would settle a live root writer as dead. A build that repaired
## that by reading EPERM as "alive, start time unknown" still reads 0 in case 1,
## and fails case 4 whatever start word the slot holds. A build whose encoding
## drifts from `sec * 1e6 + usec + 1` fails case 2, where `ps` alone, at a
## resolution of one second, could not see it. A build that ignored the empty
## answer for a missing pid reads a start time in case 5, and one without the
## zombie test on the `kinfo_proc` it reads fails case 6.
##
## Simulated state, and why. The page, the reader and the killed writer are
## real: a child joins the creator's page, claims a slot through
## `joinLivePage`, and is killed with SIGKILL and reaped. That child's slot is
## then rewritten through the creator's mapping, and not the creator's own slot,
## because the creator's heartbeat thread stores into its own slot throughout
## and a dead writer has no thread left to race the rewrite. A real root writer
## on this page would need `sudo`, which a test cannot demand, so cases 3 and 4
## leave in the slot the words that writer would leave:
##
## - `+0` pid: 1, the other user's process. Case 5 restores the killed child's;
## - `+8` start time: `processStartTime(1)`, what a root writer records for
##   itself, off by one in case 4 as a reused pid's would be. Case 5 restores
##   the killed child's;
## - `+16` heartbeat: the clock now, what a live hosted writer's thread keeps
##   there, so a heartbeat older than 10 s cannot be what judged the slot dead;
## - `+32` state: `live`, as the child's claim left it. It is stored again only
##   so a build that wrongly settled the slot in case 3 does not leave the later
##   cases passing on a slot that is already dead;
## - `+40` liveness: each of `heartbeat` (a hosted writer) and `process` (an
##   in-guest writer), the two values a real writer stores.
##
## `ps` is /bin/ps, not whatever the dev shell puts first on PATH, and it runs
## with `LC_ALL=C TZ=UTC` so its `lstart` reads the same on any host and is
## parsed in UTC: a local time is ambiguous for the hour a clock goes back.

when not defined(macosx):
  echo "SKIP: test_live_page_other_user: the sysctl arm of processStartTime " &
    "is macOS-only (Linux reads a world-readable /proc/<pid>/stat, and " &
    "Windows has no equivalent of this arm)"
else:

  import std/[os, osproc, posix, strutils, times]
  import results
  import codetracer_ctfs/[types, container, streaming, live_page]

  const
    SlotPid = 0
    SlotStart = 8
    SlotHeartbeat = 16
    SlotState = 32
    LivenessRules = [WriterLivenessHeartbeat, WriterLivenessProcess]

  var failures: seq[string]
  proc expect(cond: bool, what: string) =
    if not cond:
      failures.add what
      echo "FAIL: ", what

  proc ps(field: string, pid: uint64): string =
    ## One `ps` field of `pid`, stripped; "" when `ps` knows no such process.
    let (output, rc) = execCmdEx("LC_ALL=C TZ=UTC /bin/ps -o " & field & "= -p " & $pid)
    if rc == 0: output.strip() else: ""

  proc lstartSeconds(pid: uint64): int64 =
    ## `pid`'s start time in seconds since the epoch, as `ps` tells it, from a
    ## line like `Thu Jan  1 02:00:15 1970`; -1 when it cannot be read.
    let line = ps("lstart", pid)
    try:
      parse(line.splitWhitespace().join(" "), "ddd MMM d HH:mm:ss yyyy", utc()).
        toTime().toUnix()
    except TimeParseError:
      echo "ps -o lstart= -p ", pid, " said: ", line
      -1

  proc startAgrees(what: string, pid: uint64) =
    ## Cases 1 and 2: `pid` has a start time, it is stable, and `ps` says the
    ## same second. The start time is `sec * 1e6 + usec + 1`.
    let first = processStartTime(pid)
    let second = processStartTime(pid)
    expect(first != 0, what & ": processStartTime(" & $pid & ") is 0, which " &
      "means no live process has that pid, and ps says it started at " &
      ps("lstart", pid))
    expect(first == second, what & ": two reads of the start time differ: " &
      $first & ", " & $second)
    if first != 0:
      let witness = lstartSeconds(pid)
      expect(int64((first - 1) div 1_000_000'u64) == witness,
        what & ": the start time " & $first & " is not the second ps reports, " &
        $witness)

  proc proc_pidinfo(pid: cint, flavor: cint, arg: uint64, buffer: pointer,
                    size: cint): cint {.importc, header: "<libproc.h>".}

  proc pidinfoStart(pid: uint64): uint64 =
    ## `pid`'s start time in the slot's encoding, read through `proc_pidinfo`.
    ## `struct proc_bsdinfo` (PROC_PIDTBSDINFO = 3) is 136 bytes, with
    ## `pbi_start_tvsec` at 120 and `pbi_start_tvusec` at 128. 0 when it cannot
    ## be read, which for this process is a failed case and not an answer.
    var buf: array[136, byte]
    if proc_pidinfo(cint(pid), 3, 0, addr buf[0], cint(buf.len)) != cint(buf.len):
      return 0
    var sec, usec: uint64
    copyMem(addr sec, addr buf[120], 8)
    copyMem(addr usec, addr buf[128], 8)
    sec * 1_000_000'u64 + usec + 1

  proc nameProcess(v: PageView, slot: int, pid, start, liveness: uint64) =
    ## Leave in `slot` the words a writer with this pid and start time leaves
    ## (the header says why each one).
    let b = OffWriters + slot * WriterSlotSize
    v.store(b + SlotPid, pid)
    v.store(b + SlotStart, start)
    v.store(b + SlotHeartbeat, monotonicNs())
    v.store(b + OffSlotLiveness, liveness)
    v.store(b + SlotState, WriterSlotLive)

  proc childWriter(path: string, rootEntries: int) =
    ## A hosted writer: join the page, say so, and wait to be killed.
    var f: File
    doAssert open(f, path, fmReadWriteExisting)
    doAssert joinLivePage(f, rootEntries).isOk, "the writer could not join"
    writeFile(path & ".ready", "1")
    while true: sleep(1000)

  proc waitReady(path: string): bool =
    for _ in 0 ..< 500:
      if fileExists(path & ".ready"): return true
      sleep(20)
    false

  proc slotOfPid(v: PageView, pid: uint64): int =
    for i in 0 ..< PageWriterSlots:
      if v.load(OffWriters + i * WriterSlotSize + SlotPid) == pid:
        return i
    -1

  proc pageCases(path: string) =
    ## Cases 3, 4 and 5, on one real page.
    removeFile(path); removeFile(path & ".ready")
    var c = createCtfsStreaming(path).get()
    var child: Process = nil
    var f: File = nil
    var reader: PageReader
    try:
      let rootEntries = c.page.rootEntries
      child = startProcess(getAppFilename(),
        args = ["--child", path, $rootEntries], options = {poParentStreams})
      doAssert waitReady(path), "the joining writer never got ready"
      let childPid = uint64(child.processID)
      doAssert open(f, path, fmRead)
      reader = attachLivePage(f, rootEntries).get()
      let page = c.page.view
      let slot = slotOfPid(reader.view, childPid)
      doAssert slot >= 0 and slot != c.page.writerSlot,
        "the joining writer claimed no slot of its own (slot " & $slot & ")"
      let childStart = reader.view.load(OffWriters + slot * WriterSlotSize + SlotStart)
      expect(childStart != 0, "the joining writer recorded no start time")
      expect(writerAlive(reader.view, slot, monotonicNs()),
        "control: the joined writer is alive and is judged dead")

      child.kill()                  # SIGKILL: no close; its heartbeat stops
      discard child.waitForExit()
      close(child)
      child = nil

      # 3. A live process of another user, named by a slot.
      let rootStart = processStartTime(1)
      for liveness in LivenessRules:
        let rule = " (slot liveness " & $liveness & ", start word " & $rootStart & ")"
        nameProcess(page, slot, 1, rootStart, liveness)
        expect(writerAlive(reader.view, slot, monotonicNs()),
          "case 3: a live writer of another user is judged dead" & rule)
        expect(slot in liveWriters(reader.view),
          "case 3: liveWriters leaves out a live writer of another user" & rule)
        expect(c.page.settleDeadWriters().len == 0,
          "case 3: a survivor settled a live writer of another user" & rule)

      # 4. The same pid with another start time is another process.
      for liveness in LivenessRules:
        nameProcess(page, slot, 1, rootStart + 1, liveness)
        expect(not writerAlive(reader.view, slot, monotonicNs()),
          "case 4: a slot that names pid 1 with start time " & $(rootStart + 1) &
          ", when pid 1 started at " & $rootStart & ", is judged alive (slot " &
          "liveness " & $liveness & "): the pid-reuse check is weakened")

      # 5. A pid that names no process. A pid reused by the system in this
      # window would fail the control, not pass the case.
      let gone = posix.kill(Pid(childPid), 0) != 0 and errno == ESRCH
      expect(gone, "control: the reaped writer's pid " & $childPid & " names a process")
      expect(processStartTime(childPid) == 0,
        "case 5: processStartTime of the reaped pid " & $childPid & " is " &
        $processStartTime(childPid))
      for liveness in LivenessRules:
        nameProcess(page, slot, childPid, childStart, liveness)
        expect(not writerAlive(reader.view, slot, monotonicNs()),
          "case 5: a killed and reaped writer is judged alive (slot liveness " &
          $liveness & ")")
      expect(c.page.settleDeadWriters() == @[slot],
        "case 5: a survivor did not settle the killed writer's slot " & $slot)
    finally:
      if child != nil:
        child.kill()
        discard child.waitForExit()
        close(child)
      reader.detach()
      if f != nil: close(f)
      expect(c.closeCtfs().isOk, "the container did not close")
      removeFile(path); removeFile(path & ".ready")

  proc zombieCase() =
    ## Case 6. The child waits on a pipe until its start time has been read,
    ## then exits and stays a zombie until `waitpid` below.
    var fds: array[2, cint]
    doAssert pipe(fds) == 0
    let pid = fork()
    doAssert pid >= 0, "fork failed"
    if pid == 0:
      discard close(fds[1])
      var b: char
      discard read(fds[0], addr b, 1)
      exitnow(0)
    discard close(fds[0])
    let child = uint64(pid)
    let alive = processStartTime(child)
    expect(alive != 0, "case 6: the running child reads no start time, so a 0 " &
      "for its zombie would say nothing")
    discard close(fds[1])           # the child's read returns 0, and it exits

    var zombie = false
    for _ in 0 ..< 500:
      if ps("stat", child).startsWith("Z"):
        zombie = true
        break
      sleep(20)
    expect(zombie, "case 6: the exited child never showed as a zombie (ps stat '" &
      ps("stat", child) & "'), so the zombie arm was not exercised")
    if zombie:
      expect(processStartTime(child) == 0,
        "case 6: the zombie " & $child & " reads start time " &
        $processStartTime(child) & ", not 0 (gone)")
    var status: cint
    doAssert waitpid(pid, status, 0) == pid, "the zombie could not be reaped"

  when isMainModule:
    let args = commandLineParams()
    if args.len == 3 and args[0] == "--child":
      childWriter(args[1], parseInt(args[2]))
    if getuid() == 0:
      echo "SKIP: test_live_page_other_user: running as root, which can read " &
        "every process, so nothing here would be another user's"
    else:
      # The premise: pid 1 is another user's process.
      expect(ps("uid", 1) == "0", "pid 1 is not root's (ps says uid '" &
        ps("uid", 1) & "'), so this test is not about another user")

      startAgrees("case 1", 1)
      startAgrees("case 2", currentPid())
      let own = pidinfoStart(currentPid())
      expect(own != 0, "case 2: proc_pidinfo could not read this process")
      expect(processStartTime(currentPid()) == own,
        "case 2: the start time " & $processStartTime(currentPid()) &
        " is not the value proc_pidinfo gives, " & $own)
      pageCases(getTempDir() / ("ctfs_other_user_" & $getCurrentProcessId() & ".ct"))
      zombieCase()

      if failures.len > 0: quit(1)
      echo "PASS test_live_page_other_user"
