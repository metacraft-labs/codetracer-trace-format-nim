## Block caches keep only what `ctfs-container.md` §6, "What Is Mutated In
## Place, And What A Cache May Keep", lets them keep: the root region is
## re-read at every revalidation, and a member's last partial data block and
## its mapping blocks, and every block of a keyed member's in-place records
## and index nodes, are dropped when the member changes. Everything else is
## kept for good.
##
## A live container is written seal by seal; a `CachedBlockReader` over the
## file (its fetcher reads the file and counts) is revalidated after each seal
## with `blocksToDrop`, and every block it serves for the members' published
## bytes must equal the file's.
##
## Null prediction: a cache revalidated with nothing to drop (the control)
## serves a stale block (a page store node or a partial data block written in
## place after it was cached); the test asserts that the control does.

import std/[os, tables, sets]
import results
import codetracer_ctfs/[types, container, streaming, live_reader, page_store,
                        cached_trace_reader, partial_trace_cache, os_io]

proc fileBlock(f: File, b: uint64): seq[byte] =
  result = newSeq[byte](4096)
  discard readAtOs(f, int64(b) * 4096, addr result[0], 4096)

proc run(control: bool): tuple[stale, served, fetches: int] =
  let path = getTempDir() / ("ckf4n_cache_" & $getCurrentProcessId() & ".ct")
  removeFile(path)
  var c = createCtfsStreaming(path, maxRootEntries = 0).get()
  var ps = c.createPageStore().get()
  var ev = c.addFile("events.dat").get()
  var f: File
  doAssert open(f, path, fmRead)
  var fetches = 0
  var cache = initCachedBlockReader(proc (b: uint64): Result[seq[byte], string] =
    inc fetches
    ok(fileBlock(f, b)))
  cache.setRootBlocks(1)
  var rd = openLiveReader(path, attach = false).get()
  var page = newSeq[byte](4096)
  var n = 0
  for s in 0 ..< 30:
    for k in 0 ..< 200:
      var d: PageDigest
      for i in 0 ..< 16: d[i] = byte((n * 7 + i * 13 + (n shr 8)) and 0xff)
      page[0] = byte(n and 0xff)
      discard ps.storePage(d, page).get()
      inc n
    doAssert ps.seal(c).isOk
    doAssert c.writeToFile(ev, page[0 ..< 1500]).isOk
    c.publish()
    let before = rd.entries
    doAssert rd.refresh().isOk
    if not control:
      cache.invalidate(rd.blocksToDrop(before))
    # Serve every block of every published member through the cache.
    for i in 0 ..< rd.slots:
      let e = rd.entries[i]
      if e.name == 0 or e.size == 0: continue
      for lb in 0'u64 ..< (e.size + 4095) div 4096:
        let b = rd.resolveBlock(rd.slotName(i), e.mapBlock, lb).get()
        let got = cache.readBlock(b).get()
        let want = fileBlock(f, b)
        let used = int(min(4096'u64, e.size - lb * 4096))
        inc result.served
        if got[0 ..< used] != want[0 ..< used]: inc result.stale
  result.fetches = fetches
  rd.close()
  close(f)
  discard c.closeCtfs()
  removeFile(path)

when isMainModule:
  let ok = run(false)
  echo "  revalidated: ", ok.served, " block reads served, ", ok.fetches,
    " fetched, ", ok.stale, " stale"
  if ok.stale != 0:
    echo "FAIL: a revalidated cache served stale bytes"
    quit(1)
  if ok.fetches >= ok.served:
    echo "FAIL: the cache kept nothing"
    quit(1)
  let ctl = run(true)
  echo "  control (nothing dropped): ", ctl.stale, " stale of ", ctl.served
  if ctl.stale == 0:
    echo "FAIL: the control served nothing stale: the test cannot see a cache that keeps too much"
    quit(1)
  echo "PASS test_block_caches"
