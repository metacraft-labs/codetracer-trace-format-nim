{.push raises: [].}

## test_changed_entries_scan -- `changedEntries` names exactly the root slots
## the slot-by-slot definition names.
##
## `changedEntries` compares the slots the file holds in runs of 64 with one
## `equalMem` each, and looks slot by slot only inside a run that differs
## (container.nim, its doc comment, for why: it runs at every publication over
## the whole root directory).  The definition it must keep is the one it
## replaced, kept here verbatim as `reference`: every slot the image holds
## whole, in slot order, that the file does not hold whole or holds with other
## bytes.
##
## The cases are the ones a run-based scan can get wrong: a difference in the
## first and the last slot of a run, in the last slot the file holds, in a
## slot the file holds only part of, with the file holding nothing, every
## slot, or a length that is not a whole number of slots, and with
## `maxRootEntries` below the slots the image holds.  Then 2,000 random
## states.  Each compares the two lists whole.
##
## NO MOCKS: a real container (`createCtfs`); only the two fields the scan
## reads, `data` and `fileRoot`, are set by hand, as `publishEntry` sets the
## second.

import std/random
import codetracer_ctfs

proc reference(c: Ctfs): seq[int] =
  ## The scan as it was (container.nim before 2026-11-16).
  for i in 0 ..< int(c.maxRootEntries):
    let off = c.fileEntryOffset(i)
    if off + FileEntrySize > c.data.len: break
    var changed = off + FileEntrySize > c.fileRoot.len
    if not changed:
      for k in 0 ..< FileEntrySize:
        if c.data[off + k] != c.fileRoot[off + k]:
          changed = true
          break
    if changed: result.add i

proc check(c: Ctfs, what: string) =
  let want = reference(c)
  let got = c.changedEntries()
  if got != want:
    try: stderr.writeLine("FAIL: " & what & ": changedEntries " & $got &
                          ", the definition " & $want)
    except IOError, Exception: discard
    quit(1)

proc root(c: Ctfs): int = c.fileEntryOffset(int(c.maxRootEntries))

proc main() =
  # 64 root blocks of 4 KiB: 10,922 slots, the recorder's checkpointed size.
  var c = createCtfs(maxRootEntries = 10_922)
  let slots = (c.data.len - c.fileEntryOffset(0)) div FileEntrySize
  doAssert slots >= 10_922, "the image holds " & $slots & " slots"
  # Distinct bytes in every slot, so a misplaced comparison cannot agree.
  for i in c.fileEntryOffset(0) ..< c.data.len:
    c.data[i] = byte((i * 131 + 7) and 0xFF)
  let full = c.data[0 ..< c.root()]

  c.fileRoot = @[]
  check(c, "the file holds nothing")
  c.fileRoot = full
  check(c, "the file holds every slot")
  doAssert c.changedEntries().len == 0
  for slot in [0, 1, 62, 63, 64, 65, 127, 128, 10_880, 10_921]:
    c.fileRoot = full
    c.fileRoot[c.fileEntryOffset(slot) + 23] += 1
    check(c, "slot " & $slot & " differs")
    doAssert c.changedEntries() == @[slot]
  for n in [1, 23, 24, 25, 64 * 24, 64 * 24 + 1, 64 * 24 - 1, 5_000]:
    c.fileRoot = full[0 ..< c.fileEntryOffset(0) + n]
    check(c, "the file holds " & $n & " root byte(s)")
  c.fileRoot = full[0 ..< c.fileEntryOffset(0) - 1]
  check(c, "the file holds less than the header")
  block fewerEntries:
    c.fileRoot = full
    c.fileRoot[c.fileEntryOffset(100)] += 1
    c.fileRoot[c.fileEntryOffset(200)] += 1
    let keep = c.maxRootEntries
    c.maxRootEntries = 150
    check(c, "maxRootEntries 150 below a difference at 200")
    c.maxRootEntries = keep

  var r = initRand(20261116)
  for round in 1 .. 2_000:
    c.fileRoot = full
    let cut = r.rand(c.root())
    if r.rand(3) == 0: c.fileRoot.setLen(cut)
    for _ in 0 .. r.rand(6):
      if c.fileRoot.len > c.fileEntryOffset(0):
        let at = r.rand(c.fileEntryOffset(0) ..< c.fileRoot.len)
        c.fileRoot[at] = byte(r.rand(255))
    check(c, "random state " & $round)
  echo "PASS: test_changed_entries_scan (" & $slots & " slots, 2,000 random states)"

main()
