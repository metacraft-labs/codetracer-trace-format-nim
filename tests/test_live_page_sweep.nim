## The sweep of pages no process opens (`ctfs-container.md` §6, "A page no
## process opens"; Linux only).
##
## A writer killed with SIGKILL leaves its live coordination page in
## /dev/shm. A reader reaps it when it opens THAT container, but the next
## recording writes another container at another path, so nobody would.
## `sweepAbandonedPages` walks `/dev/shm/ctfs.*` and removes every page whose
## writers are all proven dead, without opening any container:
##
## 1. claiming a writer slot records the writer's pid namespace at `+48` and
##    its time namespace at `+56`, the inodes behind /proc/self/ns/pid and
##    /proc/self/ns/time (read here independently with `stat`);
## 2. a hosted writer killed with SIGKILL leaves its page; the sweep removes it;
## 3. the sweep keeps: this process's own page (a live writer); a dead
##    writer's page whose slot states another pid namespace, or another time
##    namespace; one whose slot states pid namespace 0 (a page written before
##    the words, or by a writer that could not place itself); one still in
##    state `creating`; and one with no claimed slot. Each of those is then
##    restored to what the killed writer left and swept, which shows that the
##    word the case changed is what kept it, not some other condition;
## 4. a LIVE writer in another time namespace keeps its page. Its recorded
##    start time is not what this process reads for its pid (/proc reports a
##    start time in the reader's time namespace), so without the time
##    namespace word the sweep would judge it dead and remove a page in use.
##    The arm checks that difference first: it is the control that the arm
##    can tell the two outcomes apart;
## 5. a writer whose /proc is not its own pid namespace's (it is in a child
##    pid namespace but sees its parent's /proc, where its pid names another
##    process or none) states namespace 0, and so does a process forked into
##    a new pid namespace after its parent already asked: the answer is not
##    inherited. With a /proc of its own namespace mounted, it states that
##    namespace.
##
## Null prediction: without a sweep (or with one that removes nothing), case 2
## leaves `/dev/shm/<page name>` behind, exactly as a recorder killed with
## SIGKILL left its page before the sweep existed. A sweep that ignored the
## namespaces would remove the case-3 pages and case 4's live page.
##
## Simulated state, and why. The pages are real: each is created by a real
## writer in a child process that is really killed. For the namespace and
## state cases of 3 the test then rewrites ONE word of that dead writer's page
## through its own mapping (the writer is dead, so nothing races it): a writer
## in another pid namespace would need the namespace's /proc as well, a page
## from before the namespace words cannot be produced by the current writer,
## and a writer killed inside `createLivePage` between the creation and the
## `live` store cannot be stopped there reliably. Every word written is one a
## real writer of that kind leaves in exactly that field. Cases 4 and 5 use
## real namespaces, created unprivileged through a user namespace; where the
## host forbids that, those arms say SKIP by name and the rest still runs.
##
## The sweep walks the real /dev/shm, which other processes on the host share,
## so every assertion is about a named page, never about the count it returns.

when not defined(linux):
  echo "SKIP: test_live_page_sweep: sweeping is defined on Linux only " &
    "(ctfs-container.md §6, \"A page no process opens\")"
else:

  import std/[os, osproc, posix, strutils]
  import results
  import codetracer_ctfs/[types, container, streaming, live_page]

  const
    CloneNewNs = 0x0002_0000'i32
    CloneNewTime = 0x0000_0080'i32
    CloneNewUser = 0x1000_0000'i32
    CloneNewPid = 0x2000_0000'i32
    MsRec = 16384'u
    MsPrivate = 262144'u

  proc unshare(flags: cint): cint {.importc, header: "<sched.h>".}
  proc mount(src, target, fstype: cstring, flags: culong, data: pointer): cint
    {.importc, header: "<sys/mount.h>".}

  var failures: seq[string]
  proc expect(cond: bool, what: string) =
    if not cond:
      failures.add what
      echo "FAIL: ", what

  proc tmp(name: string): string =
    getTempDir() / ("ctfs_sweep_" & name & "_" & $getCurrentProcessId() & ".ct")

  proc nsInode(path: string): uint64 =
    var st: Stat
    doAssert stat(cstring(path), st) == 0, "cannot stat " & path
    uint64(st.st_ino)

  proc childWriter(path: string) =
    ## A hosted writer: create a container (and so its page), publish once, say
    ## so, and wait to be killed.
    var c = createCtfsStreaming(path).get()
    var f = c.addFile("x.dat").get()
    doAssert c.writeToFile(f, [1'u8, 2, 3]).isOk
    c.publish()
    writeFile(path & ".ready", "1")
    while true: sleep(1000)

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

  proc timeNsWriter(path: string) =
    ## Case 4: a writer forked into a new time namespace whose boot clock is
    ## 1000 s ahead, in this process's pid namespace.
    if not enterUserNs(CloneNewTime):
      writeFile(path & ".skip", "unshare(CLONE_NEWUSER|CLONE_NEWTIME): errno " & $errno)
      quit(0)
    try:
      writeFile("/proc/self/timens_offsets", "boottime 1000 0")
    except IOError:
      writeFile(path & ".skip", "cannot write timens_offsets")
      quit(0)
    let pid = fork()
    if pid == 0:
      childWriter(path)
    var status: cint
    discard waitpid(pid, status, 0)
    quit(0)

  proc pidNsProbe(path: string, remountProc: bool) =
    ## Case 5: ask for the namespaces in this process (as a writer that created
    ## a page earlier would have), then fork a child into a new pid namespace
    ## and report what the child states and what its namespace really is.
    discard currentNamespaces()
    let extra = if remountProc: CloneNewPid or CloneNewNs else: CloneNewPid
    if not enterUserNs(extra):
      writeFile(path & ".skip", "unshare(CLONE_NEWUSER|CLONE_NEWPID): errno " & $errno)
      quit(0)
    let pid = fork()
    if pid == 0:
      if remountProc:
        if mount("none", "/", nil, MsRec or MsPrivate, nil) != 0 or
            mount("proc", "/proc", "proc", 0, nil) != 0:
          writeFile(path & ".skip", "cannot mount a /proc for the new pid namespace: errno " & $errno)
          quit(0)
      let stated = currentNamespaces().pid
      writeFile(path, $stated & " " & $nsInode("/proc/self/ns/pid") & " " & $getpid())
      quit(0)
    var status: cint
    discard waitpid(pid, status, 0)
    quit(0)

  proc pageNameOf(path: string): string =
    var f: File
    doAssert open(f, path, fmRead)
    result = pageName(fileIdentity(f).get())
    close(f)

  proc waitReady(path: string): bool =
    for _ in 0 ..< 1500:
      if fileExists(path & ".ready") or fileExists(path & ".skip"): break
      sleep(20)
    fileExists(path & ".ready")

  proc killedWriterPage(tag: string): tuple[path, name: string] =
    ## A page whose only writer was killed with SIGKILL, as a recorder killed
    ## mid-recording leaves it.
    let path = tmp(tag)
    removeFile(path); removeFile(path & ".ready")
    var p = startProcess(getAppFilename(), args = ["--child", path],
                         options = {poParentStreams})
    doAssert waitReady(path), "the writer never got ready: " & path
    let name = pageNameOf(path)
    p.kill()                      # SIGKILL: no close, no unlink
    discard p.waitForExit()
    close(p)
    doAssert pageObjectExists(name),
      "the killed writer's page is not there to sweep: " & name
    (path, name)

  proc mapPage(name: string): PageView =
    ## This test's own read-write mapping of a page's first 4096 bytes (the
    ## header and the writer slots), to rewrite one word of a dead writer's page.
    let fd = shm_open(cstring("/" & name), O_RDWR, Mode(0))
    doAssert fd >= 0, "cannot open " & name
    let p = mmap(nil, 4096, PROT_READ or PROT_WRITE, MAP_SHARED, fd, 0)
    discard close(fd)
    doAssert p != MAP_FAILED
    PageView(base: p, len: 4096)

  proc unmapPage(v: PageView) = discard munmap(v.base, v.len)

  proc slot0(field: int): int = OffWriters + field

  proc runChild(mode, path: string) =
    var p = startProcess(getAppFilename(), args = [mode, path],
                         options = {poParentStreams})
    discard p.waitForExit()
    close(p)

  when isMainModule:
    let args = commandLineParams()
    if args.len == 2:
      case args[0]
      of "--child": childWriter(args[1])
      of "--timens": timeNsWriter(args[1])
      of "--pidns": pidNsProbe(args[1], remountProc = false)
      of "--pidns-proc": pidNsProbe(args[1], remountProc = true)
      else: discard

    let own = SlotNamespaces(pid: nsInode("/proc/self/ns/pid"),
                             time: nsInode("/proc/self/ns/time"))
    doAssert own.pid != 0 and own.time != 0

    # 1. The slot's namespace words, and this process's own page is kept.
    let ownPath = tmp("own")
    removeFile(ownPath)
    var own0 = createCtfsStreaming(ownPath).get()
    let ownName = own0.page.name
    expect(currentNamespaces() == own,
      "currentNamespaces is " & $currentNamespaces() & ", /proc/self/ns says " & $own)
    expect(own0.page.view.load(slot0(OffSlotPidNs)) == own.pid,
      "claiming a writer slot recorded pid namespace " &
      $own0.page.view.load(slot0(OffSlotPidNs)) & ", not " & $own.pid)
    expect(own0.page.view.load(slot0(OffSlotTimeNs)) == own.time,
      "claiming a writer slot recorded time namespace " &
      $own0.page.view.load(slot0(OffSlotTimeNs)) & ", not " & $own.time)
    expect(pageSweepVerdict(own0.page.view, own) == psAlive,
      "this process's own page is judged " & $pageSweepVerdict(own0.page.view, own))

    # 2. A killed writer's page is swept.
    block:
      let (path, name) = killedWriterPage("killed")
      let v = mapPage(name)
      expect(pageSweepVerdict(v, own) == psDead,
        "a killed writer's page is judged " & $pageSweepVerdict(v, own))
      sweepAbandonedPages()
      expect(not pageObjectExists(name),
        "the sweep left the killed writer's page " & name & " in /dev/shm")
      expect(v.load(OffState) == PageStateAbandoned,
        "the swept page was not marked abandoned (state " & $v.load(OffState) & ")")
      unmapPage(v)
      expect(pageObjectExists(ownName), "the sweep removed this process's own live page")
      removeFile(path); removeFile(path & ".ready")

    # 3. Pages the sweep must keep, each then restored and swept.
    template keptThenSwept(tag, why: string, expected: PageSweepVerdict,
                           off: int, value: uint64) =
      block:
        let (path, name) = killedWriterPage(tag)
        let v = mapPage(name)
        let original = v.load(off)
        v.store(off, value)
        expect(pageSweepVerdict(v, own) == expected,
          why & ": judged " & $pageSweepVerdict(v, own) & ", not " & $expected)
        sweepAbandonedPages()
        expect(pageObjectExists(name), "the sweep removed " & why)
        v.store(off, original)
        expect(pageSweepVerdict(v, own) == psDead,
          why & ", restored: judged " & $pageSweepVerdict(v, own))
        sweepAbandonedPages()
        expect(not pageObjectExists(name),
          "the sweep kept " & why & " after it was restored")
        unmapPage(v)
        removeFile(path); removeFile(path & ".ready")

    keptThenSwept("otherns", "a dead writer's page whose slot states another pid namespace",
                  psOtherNamespace, slot0(OffSlotPidNs), own.pid + 1)
    keptThenSwept("othertime", "a dead writer's page whose slot states another time namespace",
                  psOtherNamespace, slot0(OffSlotTimeNs), own.time + 1)
    keptThenSwept("ns0", "a dead writer's page whose slot states pid namespace 0",
                  psUnstated, slot0(OffSlotPidNs), 0'u64)
    keptThenSwept("creating", "a page in state creating",
                  psCreating, OffState, PageStateCreating)
    keptThenSwept("noslot", "a page with no claimed writer slot",
                  psNoWriter, slot0(32), WriterSlotFree)

    # A sweeper that cannot place itself (namespace 0) proves nothing.
    block:
      let (path, name) = killedWriterPage("ownns0")
      let v = mapPage(name)
      expect(pageSweepVerdict(v, SlotNamespaces()) == psUnstated,
        "a sweeper with no namespace judged a page " & $pageSweepVerdict(v, SlotNamespaces()))
      unmapPage(v)
      sweepAbandonedPages()
      expect(not pageObjectExists(name), "the sweep kept a killed writer's page")
      removeFile(path); removeFile(path & ".ready")

    # 4. A live writer in another time namespace keeps its page.
    block:
      let path = tmp("timens")
      removeFile(path); removeFile(path & ".ready"); removeFile(path & ".skip")
      var p = startProcess(getAppFilename(), args = ["--timens", path],
                           options = {poParentStreams})
      if not waitReady(path):
        let why = try: readFile(path & ".skip") except IOError: "no reason given"
        echo "SKIP: test_live_page_sweep case 4 (a live writer in another time " &
          "namespace): ", why
        p.kill()
      else:
        let name = pageNameOf(path)
        let v = mapPage(name)
        let writer = v.load(slot0(0))
        let recorded = v.load(slot0(8))
        # The control: here the writer's pid reads with another start time, so
        # the pid and start time alone would call this live writer dead.
        expect(processStartTime(writer) != 0 and processStartTime(writer) != recorded,
          "the writer in another time namespace (pid " & $writer & ") has start " &
          "time " & $recorded & " here too: this arm cannot tell the outcomes apart")
        expect(v.load(slot0(OffSlotTimeNs)) != own.time,
          "the writer in another time namespace stated this process's time namespace")
        expect(pageSweepVerdict(v, own) == psOtherNamespace,
          "a live writer in another time namespace: judged " & $pageSweepVerdict(v, own))
        sweepAbandonedPages()
        expect(pageObjectExists(name),
          "the sweep removed the page of a live writer in another time namespace")
        discard posix.kill(Pid(writer), SIGKILL)
        unmapPage(v)
        discard shm_unlink(cstring("/" & name))
      discard p.waitForExit()
      close(p)
      removeFile(path); removeFile(path & ".ready"); removeFile(path & ".skip")

    # 5. What a process in a new pid namespace states.
    for (mode, remount) in [("--pidns", false), ("--pidns-proc", true)]:
      let path = tmp(mode[2 .. ^1])
      removeFile(path); removeFile(path & ".skip")
      runChild(mode, path)
      if not fileExists(path):
        let why = try: readFile(path & ".skip") except IOError: "no reason given"
        echo "SKIP: test_live_page_sweep case 5 (", mode, "): ", why
      else:
        let parts = readFile(path).split(' ')
        let stated = parseBiggestUInt(parts[0])
        let real = parseBiggestUInt(parts[1])
        expect(real != own.pid, mode & ": the child is not in a new pid namespace")
        if remount:
          expect(stated == real,
            mode & ": a writer with its own namespace's /proc stated " & $stated &
            ", not its pid namespace " & $real)
        else:
          expect(stated == 0,
            mode & ": a writer that sees another pid namespace's /proc stated " &
            $stated & " (its own is " & $real & ", its parent's " & $own.pid &
            "), not 0")
      removeFile(path); removeFile(path & ".skip")

    expect(pageObjectExists(ownName), "the sweeps removed this process's own live page")
    doAssert own0.closeCtfs().isOk
    removeFile(ownPath)

    if failures.len > 0: quit(1)
    echo "PASS test_live_page_sweep"
