## Root publication dependencies are visible in both the file-only and attached
## live readers after EVERY individual write, including prefixes a process kill
## can leave. Real CTFS payload/mapping/root writes are replayed by writeTap into
## a byte-exact image; the attached reader gets a snapshot of the actual live
## page. Tap snapshots model successful writes because writeTap precedes the
## OS write; the separate SIGKILL case checks the actual file. No mocks: the
## tap is the library's supported per-write observation seam.
## Events are created before their bootstrap prerequisites, as a premain
## constructor does. Creation order must not become publication order.
## Linux failure arms redirect the real output File to /dev/full exactly at
## the requested payload or root write, then restore it; this makes an actual
## OS write fail without mocking or bypassing the publication implementation.
import std/[os, osproc]
when defined(posix): import std/posix
import results
import codetracer_ctfs/[types, container, streaming, live_page, live_reader, base40]

const Prerequisites = ["meta.dat", "cp0.mzd", "cp0.mzi", "cp0.maps", "cp0.regs"]

proc payload(name: string): seq[byte] =
  result = newSeq[byte](if name == "cp0.mzd": 9000 else: 37)
  for i in 0 ..< result.len: result[i] = byte((i * 7 + name.len) and 255)

proc visiblePrefix(image: ref seq[byte], page: ref seq[byte]): string =
  let opened = openImageReader(image, page)
  if opened.isErr: return "cannot open observed write prefix: " & opened.error
  var reader = opened.get()
  defer: reader.close()
  if reader.findSlot("events.dat") < 0: return ""
  for name in Prerequisites:
    let slot = reader.findSlot(name)
    if slot < 0:
      return "events published before prerequisite " & name
    let got = reader.readMember(slot, 0, payload(name).len)
    if got.isErr or got.get() != payload(name):
      return "events published before readable prerequisite " & name
  ""

proc check(route: int): string =
  let path = getTempDir() / ("root_order_" & $getCurrentProcessId() & ".ct")
  var c = createCtfsStreaming(path).get()
  let image = new(seq[byte])
  image[] = cast[seq[byte]](readFile(path))
  var failed = ""
  var writes = 0
  var rootWrites: seq[string]
  var intentNames: seq[string]
  c.registerSealObserver(SealObserver(beforeSeal:
    proc (ordinal: uint64, items: seq[SealItem]) {.closure, gcsafe, raises: [].} =
      for item in items:
        if item.kind == sikEntry: intentNames.add item.name))
  c.writeTap = proc (offset: int64, bytes: openArray[byte]) {.closure, gcsafe,
      raises: [].} =
    {.cast(gcsafe).}:
      let off = int(offset)
      if image[].len < off + bytes.len: image[].setLen(off + bytes.len)
      copyMem(addr image[][off], unsafeAddr bytes[0], bytes.len)
      inc writes
      if off < c.rootRegionBytes():
        if bytes.len != FileEntrySize:
          failed = "a publication wrote more than one root entry"
        else:
          rootWrites.add base40Decode(readU64LE(bytes, 16))
      if failed.len == 0:
        failed = visiblePrefix(image, nil)
      if failed.len == 0:
        let page = new(seq[byte])
        page[] = newSeq[byte](c.page.view.len)
        copyMem(addr page[][0], c.page.view.base, page[].len)
        failed = visiblePrefix(image, page)
  var meta = c.addFile("meta.dat").get()
  var events = c.addFile("events.dat").get()
  doAssert c.writeToFile(events, @[1'u8, 2, 3]).isOk
  doAssert c.writeToFile(meta, payload("meta.dat")).isOk
  for i in 1 ..< Prerequisites.len:
    var member = c.addFile(Prerequisites[i]).get()
    doAssert c.writeToFile(member, payload(Prerequisites[i])).isOk
  doAssert c.setRootPublicationOrder(Prerequisites).isOk
  case route
  of 0: c.publish()
  of 1: c.syncEntry(events)
  of 2: c.syncAllEntries()
  else:
    # A caller's preparation hook may itself publish before the outer call.
    let prepare = proc () = c.syncEntry(events)
    prepare()
    c.publish()
  c.writeTap = nil
  doAssert c.closeCtfs().isOk
  removeFile(path)
  doAssert writes > 0 and rootWrites.len == Prerequisites.len + 1,
    "publication observer did not exercise every root entry"
  doAssert rootWrites == intentNames, "observer order differs from actual root writes"
  if failed.len > 0:
    return failed & "; observed root order=" & $rootWrites
  ""

when defined(posix):
  proc killedWriter(path: string) =
    # File-only avoids leaving an abandoned shared-memory page in this
    # deliberately SIGKILLed child; the per-write case separately checks the
    # actual live page with attached readers.
    var c = createCtfsStreaming(path, livePage = lpmFileOnly).get()
    var meta = c.addFile("meta.dat").get()
    var events = c.addFile("events.dat").get()
    doAssert c.writeToFile(meta, payload("meta.dat")).isOk
    doAssert c.writeToFile(events, @[1'u8, 2, 3]).isOk
    for i in 1 ..< Prerequisites.len:
      var f = c.addFile(Prerequisites[i]).get()
      doAssert c.writeToFile(f, payload(Prerequisites[i])).isOk
    doAssert c.setRootPublicationOrder(Prerequisites).isOk
    c.writeTap = proc (offset: int64, bytes: openArray[byte]) {.closure,
        gcsafe, raises: [].} =
      if offset < int64(c.rootRegionBytes()) and bytes.len == FileEntrySize and
          base40Decode(readU64LE(bytes, 16)) == "cp0.mzd":
        discard posix.kill(posix.getpid(), SIGKILL)
    c.syncEntry(events)
    quit(7) # The kill point was not reached: that is a test failure.

  proc checkKilledPrefix() =
    let path = getTempDir() / ("root_order_kill_" & $getCurrentProcessId() & ".ct")
    let child = startProcess(getAppFilename(), args = ["--killed", path],
                             options = {poParentStreams})
    let rc = child.waitForExit()
    child.close()
    doAssert rc == 128 + int(SIGKILL), "writer did not reach the kill point: " & $rc
    let image = new(seq[byte])
    image[] = cast[seq[byte]](readFile(path))
    let failure = visiblePrefix(image, nil)
    removeFile(path)
    doAssert failure.len == 0, "SIGKILL left an invalid publication prefix: " & failure
    echo "PASS: actual SIGKILL preserves the bootstrap-before-event boundary"

proc checkPolicyRefusals() =
  var c = createCtfs(maxRootEntries = 1)
  discard c.addFile("meta.dat").get()
  doAssert c.setRootPublicationOrder(["meta.dat"]).isOk
  doAssert c.setRootPublicationOrder(["meta.dat", "meta.dat"]).isErr
  doAssert c.setRootPublicationOrder(["INVALID!"]).isErr
  doAssert c.addFile("events.dat").isErr,
    "growing the root cannot preserve entry-by-entry publication order"
  doAssert c.setRootPublicationOrder([]).isOk
  doAssert c.addFile("events.dat").isOk,
    "clearing the policy must restore ordinary root growth"
  echo "PASS: root publication policy validates names and guards growth"

when defined(linux):
  proc checkFailedPrerequisite(dataFailure: bool, observeIntent = false) =
    let path = getTempDir() / ("root_order_fail_" & $getCurrentProcessId() & ".ct")
    var c = createCtfsStreaming(path).get()
    var meta = c.addFile("meta.dat").get()
    var events = c.addFile("events.dat").get()
    doAssert c.writeToFile(meta, payload("meta.dat")).isOk
    doAssert c.writeToFile(events, @[1'u8]).isOk
    for i in 1 ..< Prerequisites.len:
      var f = c.addFile(Prerequisites[i]).get()
      doAssert c.writeToFile(f, payload(Prerequisites[i])).isOk
    doAssert c.setRootPublicationOrder(Prerequisites).isOk
    let original = c.streamFile
    var full: File
    doAssert open(full, "/dev/full", fmReadWriteExisting)
    var injected = false
    var attempted: seq[string]
    var done = 0
    var intents = 0
    let after = proc (ordinal, epoch: uint64) {.closure, gcsafe, raises: [].} = inc done
    let before = proc (ordinal: uint64, items: seq[SealItem]) {.closure, gcsafe, raises: [].} = inc intents
    c.registerSealObserver(SealObserver(afterSeal: after,
      beforeSeal: (if observeIntent: before else: nil)))
    c.writeTap = proc (offset: int64, bytes: openArray[byte]) {.closure,
        gcsafe, raises: [].} =
      if offset < int64(c.rootRegionBytes()):
        let name = base40Decode(readU64LE(bytes, 16))
        attempted.add name
        if not dataFailure and name == "cp0.maps":
          injected = true
          c.streamFile = full
      elif dataFailure:
        injected = true
        c.streamFile = full
    c.publish()
    c.streamFile = original
    c.writeTap = nil
    full.close()
    let failedWrite = c.ioFailure.len > 0
    let failedOrdinal = c.sealOrdinal
    let failedDone = done
    let failedIntents = intents
    let reader = pageReaderOfView(c.page.view, c.page.rootEntries)
    let eventRoot = reader.readRoot(events.entryIndex)
    # Restore the real descriptor and release the live page BEFORE asserting,
    # so a failed test cannot strand the page or a poisoned descriptor.
    c.publish()
    let repairedDone = done
    let repairedOrdinal = c.sealOrdinal
    let closed = c.closeCtfs()
    let finalImage = new(seq[byte])
    finalImage[] = cast[seq[byte]](readFile(path))
    let recovered = visiblePrefix(finalImage, nil)
    removeFile(path)
    doAssert closed.isOk
    if observeIntent:
      doAssert dataFailure and failedIntents == 0 and intents == 1,
        "a payload failure before intent can retry without duplicate intent"
    doAssert repairedDone == 1 and repairedOrdinal == 1,
      "normal publication retry must complete exactly one seal"
    doAssert done == 1 and c.sealOrdinal == 1, "close must not invent another seal"
    doAssert recovered.len == 0, "retry left unreadable dependencies: " & recovered
    doAssert injected and failedWrite,
      "the real /dev/full write refusal was not exercised"
    doAssert "events.dat" notin attempted,
      "publication passed a failed prerequisite"
    doAssert failedDone == 0 and failedOrdinal == 0,
      "a failed prerequisite was reported as a completed seal"
    if dataFailure:
      doAssert attempted.len == 0, "failed payload writes must publish no root"
    doAssert eventRoot.name == 0,
      "the live page exposed events after a failed prerequisite write"
    echo "PASS: failed ", (if dataFailure: "payload" else: "prerequisite root"),
      " write blocks dependent root entries",
      (if observeIntent: " (full observer payload-before-intent retry)" else: "")


proc checkAbortDoesNotPublish() =
  let path = getTempDir() / ("root_order_abort_" & $getCurrentProcessId() & ".ct")
  var c = createCtfsStreaming(path).get()
  var f = c.addFile("events.dat").get()
  doAssert c.writeToFile(f, newSeq[byte](9000)).isOk
  c.registerSealObserver(SealObserver())
  let pageName = c.page.name
  var writes = 0
  c.writeTap = proc (offset: int64, bytes: openArray[byte]) {.closure, gcsafe, raises: [].} = inc writes
  doAssert hasSealObserver(path)
  let aborted = c.abortCtfs()
  let image = new(seq[byte])
  image[] = cast[seq[byte]](readFile(path))
  removeFile(path)
  doAssert aborted.isOk and c.abortCtfs().isOk
  doAssert writes == 0 and c.sealOrdinal == 0
  doAssert c.streamFile == nil and c.page == nil
  doAssert not pageObjectExists(pageName) and not hasSealObserver(path)
  var r = openImageReader(image).get()
  doAssert r.findSlot("events.dat") < 0, "abort published a staged member"
  r.close()
  echo "PASS: nonpublishing abort releases every owned resource and registration"

proc checkRepeatedAbortPreservesSuccessor() =
  ## Two real, sequential writers reuse a path. Teardown of the abandoned
  ## object must not remove the successor's independent observer registration.
  let path = getTempDir() / ("root_order_reuse_" & $getCurrentProcessId() & ".ct")
  var first = createCtfsStreaming(path, livePage = lpmFileOnly).get()
  first.registerSealObserver(SealObserver())
  doAssert hasSealObserver(path)
  doAssert first.abortCtfs().isOk
  doAssert not hasSealObserver(path)
  var second = createCtfsStreaming(path, livePage = lpmFileOnly).get()
  second.registerSealObserver(SealObserver())
  doAssert hasSealObserver(path), "the successor observer was not registered"
  let repeated = first.abortCtfs()
  let successorStillRegistered = hasSealObserver(path)
  let released = second.abortCtfs()
  removeFile(path)
  doAssert repeated.isOk and released.isOk
  doAssert successorStillRegistered,
    "repeated abort removed a later writer's observer registration"
  doAssert not hasSealObserver(path)
  echo "PASS: repeated abort preserves the successor writer's registration"

when defined(linux):
  proc checkObservedFailureIsTerminal() =
    let path = getTempDir() / ("root_order_terminal_" & $getCurrentProcessId() & ".ct")
    var c = createCtfsStreaming(path).get()
    var events = c.addFile("events.dat").get()
    doAssert c.writeToFile(events, @[1'u8]).isOk
    for name in Prerequisites:
      var f = c.addFile(name).get()
      doAssert c.writeToFile(f, payload(name)).isOk
    doAssert c.setRootPublicationOrder(Prerequisites).isOk
    var intents = 0
    var completed = 0
    let before = proc (ordinal: uint64, items: seq[SealItem]) {.closure, gcsafe, raises: [].} =
      inc intents
    let after = proc (ordinal, epoch: uint64) {.closure, gcsafe, raises: [].} =
      inc completed
    c.registerSealObserver(SealObserver(beforeSeal: before, afterSeal: after))
    let pageName = c.page.name
    let original = c.streamFile
    var full: File
    doAssert open(full, "/dev/full", fmReadWriteExisting)
    var injected = false
    var writes = 0
    c.writeTap = proc (offset: int64, bytes: openArray[byte]) {.closure, gcsafe, raises: [].} =
      inc writes
      if not injected and offset < int64(c.rootRegionBytes()) and bytes.len == FileEntrySize and
          base40Decode(readU64LE(bytes, 16)) == "cp0.maps":
        injected = true
        c.streamFile = full
    c.publish()
    c.streamFile = original
    full.close()
    let writesAtFailure = writes
    c.publish() # Cannot emit a second intent for the incomplete seal.
    let refusedReset = c.setRootPublicationOrder([])
    let closed = c.closeCtfs() # Cannot retry implicitly at close either.
    let closedAgain = c.closeCtfs()
    let image = new(seq[byte])
    image[] = cast[seq[byte]](readFile(path))
    removeFile(path)
    doAssert intents == 1, "failed publication emitted a duplicate observer intent"
    doAssert completed == 0 and c.sealOrdinal == 0, "failed seal claimed completion"
    doAssert injected and refusedReset.isErr and closed.isErr and closedAgain.isErr
    doAssert writes == writesAtFailure, "terminal seal was republished"
    doAssert c.streamFile == nil and c.page == nil
    doAssert not pageObjectExists(pageName) and not hasSealObserver(path)
    doAssert visiblePrefix(image, nil).len == 0
    echo "PASS: failed observed seal stays terminal without duplicate intent or completion"

when defined(posix):
  let args = commandLineParams()
  if args.len == 2 and args[0] == "--killed":
    killedWriter(args[1])
    quit(7)
  if args.len == 1 and args[0] == "--check-kill":
    checkKilledPrefix()
    quit(0)
  when defined(linux):
    if args.len == 1 and args[0] == "--check-observed-failure":
      checkObservedFailureIsTerminal()
      quit(0)
for route in 0 .. 3:
  let failure = check(route)
  doAssert failure.len == 0, failure
echo "PASS: every root publication prefix is readable"
when defined(posix): checkKilledPrefix()

checkPolicyRefusals()
when defined(linux):
  checkFailedPrerequisite(false)
  checkFailedPrerequisite(true)

checkAbortDoesNotPublish()
checkRepeatedAbortPreservesSuccessor()
when defined(linux): checkObservedFailureIsTerminal()

when defined(linux): checkFailedPrerequisite(true, observeIntent = true)
