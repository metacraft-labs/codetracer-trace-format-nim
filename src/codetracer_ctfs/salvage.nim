when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Salvage of a container whose writers all died (`ctfs-container.md` §6,
## "Every writer dies"): before the abandoned live coordination page is
## removed, copy into the file each root entry whose page mirror shows a later
## `Size` than the file holds, when every byte that `Size` covers is in the
## file (the writer put them there before the page published them, §6 writer
## step 1). Salvage is an improvement, never a requirement of durability: the
## file is valid through each writer's last seal without it.
##
## A page with a live writer is not touched. An entry is copied only when its
## name is the file's, its `Size` is larger, and its `MapBlock` resolves every
## block of that `Size` inside the file's whole blocks and outside the root
## region; it is written whole, in one write. The page is then marked
## `abandoned` and its name removed.

import results
import ./types
import ./base40
import ./os_io
import ./live_page
import ./live_reader

type
  SalvageReport* = object
    pageFound*: bool
    liveWriters*: int
    copied*: seq[string]      ## members whose entry was advanced
    skipped*: seq[string]     ## members whose page value could not be proven

proc salvageContainer*(path: string): Result[SalvageReport, string] =
  var rep: SalvageReport
  var f: File
  if not open(f, path, fmReadWriteExisting):
    return err("ctfs-salvage: cannot open " & path & " for writing")
  defer: close(f)
  var r = ? openLiveReader(path, attach = false)
  defer: r.close()
  let id = ? fileIdentity(f)
  let name = pageName(id)
  var pr = attachLivePage(f, r.slots, skipIdentityCheck = false)
  if pr.isErr:
    # Not live: a closed or abandoned page, or none. Nothing to salvage from
    # a page that is gone.
    rep.pageFound = pageObjectExists(name)
    discard reapAbandonedPage(f)
    return ok(rep)
  var page = pr.get()
  rep.pageFound = true
  let alive = liveWriters(page.view)
  rep.liveWriters = alive.len
  if alive.len > 0:
    page.detach()
    return err("ctfs-salvage: " & path & " has " & $alive.len &
      " live writer(s); salvage runs only after every writer has died")
  let base = r.headerSize
  for i in 0 ..< r.slots:
    let m = page.readRoot(i)
    if m.state != srOk: continue
    let e = r.entries[i]
    if m.name != e.name or m.size <= e.size or m.name == 0: continue
    let member = base40Decode(m.name)
    # Every block the page's Size covers must be in the file.
    var proven = true
    let bs = uint64(r.blockSize)
    for lb in 0'u64 ..< (m.size + bs - 1) div bs:
      if r.resolveBlock(member, m.mapBlock, lb).isErr:
        proven = false
        break
    if not proven:
      rep.skipped.add member
      continue
    var b: array[24, byte]
    writeU64LE(b, 0, m.size)
    writeU64LE(b, 8, m.mapBlock)
    writeU64LE(b, 16, m.name)
    ? writeAtOs(f, int64(base + 24 * i), addr b[0], 24)
    rep.copied.add member
  page.detach()
  discard reapAbandonedPage(f)
  ok(rep)
