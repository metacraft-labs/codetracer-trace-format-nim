## The hosted writer's heartbeat thread blocks every signal
## (`live_page.nim` `startHeartbeat`).
##
## The writer runs inside a host process (php-fpm through the PHP recorder, a
## Ruby or Python interpreter) whose signal handling is the host's. A
## process-directed signal goes to any one thread that does not block it
## (https://man7.org/linux/man-pages/man7/signal.7.html, "Signal mask and
## pending signals"), so a heartbeat thread that blocks nothing can take the
## host's SIGTERM and run the host's handler while another thread is closing
## the container. That was observed: php-fpm workers whose heartbeat thread had
## `SigBlk: 0000000000000000` died inside `trace_writer_close` (codetracer-pm
## `issues/2026-11-04-trace-writer-heartbeat-thread-receives-host-stop-signals.md`).
##
## 1. Control: creating a hosted writer adds exactly one thread, so the new
##    thread is the heartbeat thread.
## 2. Its mask (Linux, `/proc/self/task/<tid>/status` `SigBlk`): every
##    standard signal but SIGKILL and SIGSTOP is blocked; SIGTERM, SIGINT and
##    SIGQUIT are named in the failure.
## 3. The creating thread's mask is what it was before the writer was created
##    (Linux, the same `SigBlk` word).
## 4. Delivery (every POSIX platform): a writer is created while the main
##    thread blocks nothing; the main thread then blocks SIGTERM (as the PHP
##    recorder does while it closes) and a SIGTERM is sent to the process. It
##    is not handled while the heartbeat thread runs (it stays pending), and is
##    handled on the main thread once that thread unblocks it. A heartbeat
##    thread that blocks nothing takes it at once.
##
## Parts 2 and 4 first wait for the heartbeat thread to be past its start-up:
## glibc blocks every signal in a thread it is creating until the thread sets
## the mask it inherited, so an early read sees a full mask either way.
##
## No mocks: a real container with its live page, real threads and real
## signals. On Windows (no POSIX signals) the test skips.

when defined(windows):
  echo "SKIP: Windows delivers no POSIX signals to arbitrary threads"
else:
  import std/[os, posix, strutils, sets]
  import results
  import codetracer_ctfs/[container, streaming]

  var failures: seq[string]
  proc expect(cond: bool, what: string) =
    if not cond:
      failures.add what
      echo "FAIL: ", what

  proc threadsOfThisProcess(): HashSet[uint64] =
    ## The ids of this process's threads (Linux only; elsewhere empty).
    when defined(linux):
      for kind, p in walkDir("/proc/self/task"):
        result.incl parseBiggestUInt(extractFilename(p))

  proc sigBlk(tid: uint64): uint64 =
    ## A thread's blocked mask from `/proc/self/task/<tid>/status`, where bit
    ## `n - 1` is signal `n` (https://man7.org/linux/man-pages/man5/proc_pid_status.5.html).
    for line in lines("/proc/self/task/" & $tid & "/status"):
      if line.startsWith("SigBlk:"):
        return fromHex[uint64](line["SigBlk:".len .. ^1].strip())
    doAssert false, "no SigBlk line for thread " & $tid

  proc waitUntilRunning(tid: uint64) =
    ## Wait until the thread has blocked voluntarily at least once (its first
    ## `nanosleep`), so it is past its start-up. glibc's `pthread_create`
    ## blocks every signal, the internal ones too, until the new thread's
    ## start routine sets the mask it inherited, so a mask read before that
    ## is the transient one, `fffffffffffbfeff`, whatever the creator held.
    for _ in 0 ..< 250:
      for line in lines("/proc/self/task/" & $tid & "/status"):
        if line.startsWith("voluntary_ctxt_switches:") and
            parseInt(line.split(':')[1].strip()) > 0:
          return
      sleep(20)
    doAssert false, "thread " & $tid & " never ran"

  proc tmp(name: string): string =
    getTempDir() / ("hb_sigmask_" & name & "_" & $getCurrentProcessId() & ".ct")

  # The thread a SIGTERM was handled on, and how many were handled.
  var handledOn: Pthread
  var handledCount: int

  proc onTerm(sig: cint) {.noconv.} =
    handledOn = pthread_self()
    atomicInc(handledCount)

  proc maskPart() =
    when defined(linux):
      let path = tmp("mask")
      removeFile(path)
      let callerTid = uint64(getThreadId())
      let callerBefore = sigBlk(callerTid)
      let before = threadsOfThisProcess()
      var c = createCtfsStreaming(path).get()
      var added: seq[uint64]
      for t in threadsOfThisProcess():
        if t notin before: added.add t
      expect added.len == 1,
        "a hosted writer's creation added " & $added.len &
        " threads, not 1 (the heartbeat thread)"
      if added.len == 1:
        waitUntilRunning(added[0])
        let blk = sigBlk(added[0])
        for (sig, name) in [(SIGTERM, "SIGTERM"), (SIGINT, "SIGINT"),
                            (SIGQUIT, "SIGQUIT"), (SIGHUP, "SIGHUP")]:
          expect (blk and (1'u64 shl (sig - 1))) != 0,
            "the heartbeat thread does not block " & name &
            " (SigBlk " & toHex(blk) & ")"
        for sig in 1 .. 31:
          if sig == SIGKILL or sig == SIGSTOP: continue
          expect (blk and (1'u64 shl (sig - 1))) != 0,
            "the heartbeat thread does not block signal " & $sig &
            " (SigBlk " & toHex(blk) & ")"
      expect sigBlk(callerTid) == callerBefore,
        "the creating thread's mask changed from " & toHex(callerBefore) &
        " to " & toHex(sigBlk(callerTid))
      doAssert c.closeCtfs().isOk
      removeFile(path)
    else:
      echo "skip: no /proc task status on this platform (part 4 still runs)"

  proc deliveryPart() =
    var sa: Sigaction
    sa.sa_handler = onTerm
    doAssert sigemptyset(sa.sa_mask) == 0
    var oldSa: Sigaction
    doAssert sigaction(SIGTERM, sa, oldSa) == 0

    # The writer is created while this thread blocks nothing, as a host's
    # request thread does, so the heartbeat thread's mask is its own doing.
    let path = tmp("deliver")
    removeFile(path)
    let before = threadsOfThisProcess()
    var c = createCtfsStreaming(path).get()
    for t in threadsOfThisProcess():
      if t notin before: waitUntilRunning(t)
    sleep(300)  # past start-up where there is no /proc to ask

    # Then this thread blocks SIGTERM, as the PHP recorder does while it
    # closes: the only other thread is the heartbeat thread.
    var term, oldMask, scratch: Sigset
    doAssert sigemptyset(term) == 0
    doAssert sigaddset(term, SIGTERM) == 0
    doAssert pthread_sigmask(SIG_BLOCK, term, oldMask) == 0
    handledCount = 0
    doAssert kill(getpid(), SIGTERM) == 0
    sleep(300)  # more than a heartbeat period: the thread has woken since
    expect handledCount == 0,
      "a process-directed SIGTERM was handled while the main thread blocked " &
      "it, so another thread (the heartbeat) took it"
    var pending: Sigset
    doAssert sigpending(pending) == 0
    expect sigismember(pending, SIGTERM) == 1 or handledCount > 0,
      "the SIGTERM is neither pending nor handled"
    let me = pthread_self()
    # Unblocking delivers the pending SIGTERM to this thread before
    # `pthread_sigmask` returns (pthread_sigmask(3), sigprocmask(2)).
    doAssert pthread_sigmask(SIG_SETMASK, oldMask, scratch) == 0
    expect handledCount == 1,
      "the SIGTERM was handled " & $handledCount & " times, not once"
    if handledCount == 1:
      expect pthread_equal(handledOn, me) != 0,
        "the SIGTERM was handled on another thread than the main thread"
    doAssert c.closeCtfs().isOk
    removeFile(path)
    doAssert sigaction(SIGTERM, oldSa, sa) == 0

  maskPart()
  deliveryPart()
  if failures.len > 0:
    echo failures.len, " failure(s)"
    quit(1)
  echo "OK: the heartbeat thread blocks every signal; the creator's mask is kept"
