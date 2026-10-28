## The live coordination page's lifetime (`ctfs-container.md` §6, "The page:
## naming and discovery" and "Lifetime, and writers that die"): created with
## the container, attached by a reader of the same file, closed and unlinked
## at close; a page whose writer was killed is found dead and reaped by the
## next opener (an explicit reap, or a reader's open, which does not attach
## to it), and a new writer of the same file then creates its page; a
## live writer's page refuses a second creator; a page that carries another
## file's identity is refused; a reader that cannot attach follows the file.
##
## Null prediction (the mutation arm): a reader that skips the identity check
## (`attachLivePage(..., skipIdentityCheck = true)`) attaches to the page that
## carries another file's identity. The test asserts that it does, so the
## refusal the protocol's reader gives is the identity check's and not some
## other condition's.

import std/[os, osproc, strutils]
import results
import codetracer_ctfs/[types, container, streaming, live_page, live_reader]

proc tmp(name: string): string =
  getTempDir() / ("ckf4n_lifetime_" & name & "_" & $getCurrentProcessId() & ".ct")

var failures: seq[string]
proc expect(cond: bool, what: string) =
  if not cond:
    failures.add what
    echo "FAIL: ", what

proc childWriter(path: string) =
  ## Create a container, publish one member, say so, and wait to be killed.
  var c = createCtfsStreaming(path).get()
  var f = c.addFile("x.dat").get()
  doAssert c.writeToFile(f, [1'u8, 2, 3]).isOk
  c.publish()
  writeFile(path & ".ready", "1")
  while true: sleep(1000)

proc waitFor(path: string) =
  for _ in 0 ..< 500:
    if fileExists(path): return
    sleep(20)
  doAssert false, "the child never got ready: " & path

when isMainModule:
  let args = commandLineParams()
  if args.len == 2 and args[0] == "--child":
    childWriter(args[1])

  # 1. Create, attach, close, unlink.
  block:
    let path = tmp("basic")
    removeFile(path)
    var c = createCtfsStreaming(path).get()
    var f = c.addFile("x.dat").get()
    doAssert c.writeToFile(f, [7'u8, 8, 9]).isOk
    c.publish()
    let name = c.page.name
    expect(name.startsWith("ctfs.") and name.len == 29, "the page name is " & name)
    var r = openLiveReader(path).get()
    expect(r.attached, "a reader of a live container did not attach: " & r.attachRefusal)
    expect(r.page.state() == PageStateLive, "the page is not live")
    expect(r.page.sealEpoch() == 1, "seal_epoch is " & $r.page.sealEpoch() & " after one seal")
    expect(r.memberSize(r.findSlot("x.dat")) == 3, "the attached reader's x.dat size")
    doAssert c.closeCtfs().isOk
    expect(r.page.state() == PageStateClosed,
      "a reader still mapping the page does not see it closed")
    discard r.refresh()
    expect(not r.attached, "the reader stayed attached to a closed page")
    expect(r.memberSize(r.findSlot("x.dat")) == 3, "the closed container's x.dat size")
    r.close()
    expect(not pageObjectExists(name), "the page outlived its close and its readers")
    var r2 = openLiveReader(path).get()
    expect(not r2.attached, "a reader attached to a closed container's page")
    expect(r2.memberSize(r2.findSlot("x.dat")) == 3, "a file-only reader's x.dat size")
    r2.close()
    removeFile(path)

  # 2. A writer killed: its page is dead, reaped by the next opener, and a new
  #    writer of the same file creates its page. A live writer's page refuses
  #    a second creator.
  block:
    let path = tmp("killed")
    removeFile(path); removeFile(path & ".ready")
    var p = startProcess(getAppFilename(), args = ["--child", path],
                         options = {poParentStreams})
    waitFor(path & ".ready")
    var f: File
    doAssert open(f, path, fmReadWriteExisting)
    let livePage = createLivePage(f, 31)
    expect(livePage.isErr and "live writers" in livePage.error,
      "a second creator of a live writer's page was not refused: " &
      (if livePage.isErr: livePage.error else: "created"))
    let (found0, reaped0) = reapAbandonedPage(f)
    expect(found0 and not reaped0, "a live writer's page was reaped")
    # Hold a handle so the page outlives the writer on Windows too.
    var held = openLiveReader(path).get()
    expect(held.attached, "the reader did not attach to the child's page")
    p.terminate()
    discard p.waitForExit()
    close(p)
    let (found, reaped) = reapAbandonedPage(f)
    expect(found and reaped, "the dead writer's page was not reaped (found " &
      $found & ", reaped " & $reaped & ")")
    expect(held.page.state() == PageStateAbandoned,
      "a reader mapping the reaped page does not see it abandoned")
    held.close()
    let again = createLivePage(f, 31)
    expect(again.isOk, "a new writer of the killed writer's file could not create its page: " &
      (if again.isErr: again.error else: ""))
    if again.isOk:
      expect(again.get().view.load(OffState) == PageStateLive, "the new page is not live")
      closeLivePage(again.get())
    close(f)
    removeFile(path); removeFile(path & ".ready")

  # 2b. A writer killed, then a reader opens the container: the reader is
  #     "the next process to open the container" (§6, "Every writer dies").
  #     It does not attach to the dead writer's page; it marks the page
  #     abandoned and removes its name. On Linux and macOS the page outlives
  #     the writer by itself; on Windows a held handle keeps it.
  block:
    let path = tmp("killedread")
    removeFile(path); removeFile(path & ".ready")
    var p = startProcess(getAppFilename(), args = ["--child", path],
                         options = {poParentStreams})
    waitFor(path & ".ready")
    var held = openLiveReader(path).get()
    expect(held.attached, "the reader did not attach to the child's page: " &
      held.attachRefusal)
    let name = held.page.name
    p.terminate()
    discard p.waitForExit()
    close(p)
    var r = openLiveReader(path).get()
    expect(not r.attached, "a reader attached to the page of a killed writer")
    expect("no live writer" in r.attachRefusal,
      "the refusal does not name the dead writers: " & r.attachRefusal)
    expect(held.page.state() == PageStateAbandoned,
      "the reader that found every writer dead did not mark the page abandoned")
    expect(r.memberSize(r.findSlot("x.dat")) == 3,
      "the file-only reader's x.dat size after the writer died")
    r.close()
    held.close()
    when not defined(windows):
      expect(not pageObjectExists(name),
        "the reader that found every writer dead did not remove the page's name")
    removeFile(path); removeFile(path & ".ready")

  # 3. A page that carries another file's identity is refused; the arm that
  #    skips the identity check attaches to it.
  block:
    let a = tmp("ida")
    let b = tmp("idb")
    removeFile(a); removeFile(b)
    var ca = createCtfsStreaming(a).get()
    var cb = createCtfsStreaming(b).get()
    # b's page is made to carry a's identity: what a name collision, or a
    # page left by a deleted file whose inode was reused, looks like.
    for i in 0 ..< 4:
      cb.page.view.store(OffIdentity + 8 * i, ca.page.view.load(OffIdentity + 8 * i))
    var fb: File
    doAssert open(fb, b, fmRead)
    let refused = attachLivePage(fb, 31)
    expect(refused.isErr and "another file" in refused.error,
      "a page with another file's identity was not refused by its identity: " &
      (if refused.isErr: refused.error else: "attached"))
    var armed = attachLivePage(fb, 31, skipIdentityCheck = true)
    expect(armed.isOk, "the mutation arm did not attach: the refusal above was not the identity check's")
    if armed.isOk: armed.get().detach()
    close(fb)
    var rb = openLiveReader(b).get()
    expect(not rb.attached, "openLiveReader attached to a page of another identity")
    rb.close()
    discard ca.closeCtfs()
    discard cb.closeCtfs()
    removeFile(a); removeFile(b)

  if failures.len > 0: quit(1)
  echo "PASS test_live_page_lifetime"
