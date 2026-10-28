when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Opening and following a container while it is written
## (`ctfs-container.md` §6, "Reader Protocol: Opening And Following A
## Container"), attached to its live coordination page or following the file
## alone.
##
## This is the one reader protocol, with the attached mode beside the
## file-only follower: whether the reader is attached changes only where it
## reads the words that change in place. Attached, it reads them from the page
## under their sequence counters (`live_page.readSlot`); file-only, it reads
## each twice and accepts a value only when both reads agree and a `Size` has
## not decreased (§6, "A reader that cannot attach"). Data, mapping and record
## bytes are read from the file in both modes, with every refusal of §4: a
## block number of `0`, one in `[1, root_blocks)`, or one past the
## container's whole blocks.
##
## A reader reads either a file (`openLiveReader`) or a test's image of one
## (`openImageReader`): an image a test rebuilt from a writer's taps as of a
## given write, with the page as of the same point, so a test can stop the
## world between any two writes.

import std/[tables]
import results
import ./types
import ./base40
import ./os_io
import ./live_page

export live_page

type
  RootSlot* = object
    name*: uint64
    size*: uint64
    mapBlock*: uint64

  LiveReader* = object
    path*: string
    file: File
    image: ref seq[byte]
    pageBuf: ref seq[byte]
    attached*: bool
    page*: PageReader
    blockSize*: uint32
    headerSize*: int
    slots*: int
    rootBlocks*: uint64
    entries*: seq[RootSlot]
    accepted: seq[bool]
    wholeBlocks*: uint64
    mapCache: Table[uint64, uint64]
    extentCache: seq[MirrorExtent]
    refreshes*: int
    attachRefusal*: string
      ## Why the reader is not attached, when it tried.

const
  RootReadRetries* = 8

proc readRaw*(r: var LiveReader, off: uint64, n: int): Result[seq[byte], string] =
  ## `n` bytes at file offset `off`; refused when the file is shorter.
  var b = newSeq[byte](n)
  if n == 0: return ok(b)
  if r.image != nil:
    if off + uint64(n) > uint64(r.image[].len):
      return err("container " & r.path & ": " & $n & " bytes at offset " &
        $off & " are past its end")
    copyMem(addr b[0], addr r.image[][int(off)], n)
    return ok(b)
  let got = readAtOs(r.file, int64(off), addr b[0], n)
  if got != n:
    return err("container " & r.path & ": " & $n & " bytes at offset " &
      $off & " could not be read")
  ok(b)

proc fileBlocks(r: var LiveReader): uint64 =
  let len =
    if r.image != nil: int64(r.image[].len)
    else: fileLength(r.file)
  if len < 0: 0'u64 else: uint64(len) div uint64(r.blockSize)

proc parseHeader(r: var LiveReader): Result[void, string] =
  let h = ? r.readRaw(0, 24)
  for i in 0 ..< 5:
    if h[i] != CtfsMagic[i]:
      return err("container " & r.path & ": not a CTFS container")
  let ver = h[5]
  if ver != CtfsVersion and ver != CtfsVersionV6:
    return err("container " & r.path & ": version " & $ver & " is not read")
  if ver == CtfsVersionV6 and h[V6ProfileOffset] != 0'u8:
    return err("container " & r.path & ": a compact container is not followed")
  r.headerSize = if ver == CtfsVersionV6: V6HeaderSize else: 16
  r.blockSize = readU32LE(h, 8)
  let refusal = blockSizeRefusal(r.blockSize)
  if refusal.len > 0:
    return err("container " & r.path & ": " & refusal)
  let declared = readU32LE(h, 12)
  r.slots = int(rootEntryCount(r.blockSize, declared, r.headerSize))
  r.rootBlocks = rootBlockCount(r.blockSize, uint32(r.slots), 0, r.headerSize)
  r.entries = newSeq[RootSlot](r.slots)
  r.accepted = newSeq[bool](r.slots)
  ok()

proc slotName*(r: LiveReader, i: int): string = base40Decode(r.entries[i].name)

proc readRootFromFile(r: var LiveReader): Result[seq[tuple[ok: bool, e: RootSlot]], string] =
  ## Every root slot read twice from the file; a slot whose two reads differ
  ## is not accepted this time.
  let n = r.slots * FileEntrySize
  let a = ? r.readRaw(uint64(r.headerSize), n)
  let b = ? r.readRaw(uint64(r.headerSize), n)
  var res = newSeq[tuple[ok: bool, e: RootSlot]](r.slots)
  for i in 0 ..< r.slots:
    let o = i * FileEntrySize
    var same = true
    for k in 0 ..< FileEntrySize:
      if a[o + k] != b[o + k]:
        same = false
        break
    res[i] = (ok: same, e: RootSlot(size: readU64LE(a, o),
      mapBlock: readU64LE(a, o + 8), name: readU64LE(a, o + 16)))
  ok(res)

proc refresh*(r: var LiveReader): Result[void, string] =
  ## Read every root slot again (§6, Reader Protocol, step 2, and "A
  ## refresh"): from the page's root mirror when attached, from the file
  ## under the torn-read rule otherwise. A slot whose `Size` decreased, whose
  ## name changed, or whose member disappeared is refused, naming the member,
  ## and the reader keeps answering from its previous state.
  if r.attached and r.page.state() != PageStateLive:
    # Closed or abandoned: the file is static from here; read it once more.
    if r.pageBuf == nil: r.page.detach()
    r.attached = false
  var fresh = newSeq[tuple[ok: bool, e: RootSlot]](r.slots)
  let fromFile = ? r.readRootFromFile()
  for i in 0 ..< r.slots:
    fresh[i] = fromFile[i]
    if r.attached:
      let m = r.page.readRoot(i, RootReadRetries)
      case m.state
      of srOk:
        fresh[i] = (ok: true, e: RootSlot(size: m.size, mapBlock: m.mapBlock,
                                          name: m.name))
      of srNever: discard
      of srRetry: fresh[i].ok = false
  for i in 0 ..< r.slots:
    if not fresh[i].ok or not r.accepted[i]: continue
    let old = r.entries[i]
    let e = fresh[i].e
    if old.name != 0 and e.name != old.name:
      return err("container " & r.path & ": member " & base40Decode(old.name) &
        (if e.name == 0: " disappeared" else: " was renamed " & base40Decode(e.name)) &
        " between two refreshes")
    if e.size < old.size:
      return err("container " & r.path & ": member " & base40Decode(old.name) &
        "'s Size decreased from " & $old.size & " to " & $e.size &
        " between two refreshes")
  for i in 0 ..< r.slots:
    if fresh[i].ok:
      r.entries[i] = fresh[i].e
      r.accepted[i] = true
  r.wholeBlocks = r.fileBlocks()
  inc r.refreshes
  ok()

proc initReader(r: var LiveReader, attach: bool, attachPage: bool,
                skipIdentityCheck = false): Result[void, string] =
  ? r.parseHeader()
  if attach and attachPage:
    let p = attachLivePage(r.file, r.slots, skipIdentityCheck)
    if p.isOk:
      r.page = p.get()
      r.attached = true
    else:
      r.attachRefusal = p.error
  ? r.refresh()
  ok()

proc openLiveReader*(path: string, attach = true,
                     skipIdentityCheck = false): Result[LiveReader, string] =
  ## Open a container to read or follow it: read the header once, try to
  ## attach to its live coordination page (when `attach`), read every root
  ## slot.
  var r = LiveReader(path: path)
  try:
    if not open(r.file, path, fmRead):
      return err("cannot open container " & path)
  except IOError, OSError:
    return err("cannot open container " & path)
  let res = r.initReader(attach, true, skipIdentityCheck)
  if res.isErr:
    try: close(r.file) except IOError: discard
    return err(res.error)
  ok(r)

proc openImageReader*(image: ref seq[byte], page: ref seq[byte] = nil,
                      name = "<image>"): Result[LiveReader, string] =
  ## A reader of a test's image of a container file, attached to `page` (a
  ## buffer laid out as the page) when given.
  var r = LiveReader(path: name, image: image, pageBuf: page)
  ? r.parseHeader()
  if page != nil and page[].len >= slotsOffset(r.slots):
    r.page = pageReaderOfView(PageView(base: addr page[][0], len: page[].len),
                              r.slots)
    r.attached = r.page.view.load(0) == PageMagicVersion and
      r.page.state() == PageStateLive
  ? r.refresh()
  ok(r)

proc close*(r: var LiveReader) =
  if r.attached and r.pageBuf == nil:
    r.page.detach()
  r.attached = false
  if r.file != nil:
    try: close(r.file) except IOError: discard
    r.file = nil

proc findSlot*(r: LiveReader, name: string): int =
  ## The slot of member `name`, or -1.
  if not base40Encodable(name): return -1
  let k = base40Encode(name)
  for i in 0 ..< r.slots:
    if r.entries[i].name == k: return i
  -1

proc memberSize*(r: LiveReader, slot: int): uint64 =
  if slot < 0: 0'u64 else: r.entries[slot].size

# ---------------------------------------------------------------------------
# Resolving and reading member bytes (§2, §4)
# ---------------------------------------------------------------------------

proc checkBlock(r: var LiveReader, b: uint64, what: string): Result[void, string] =
  if b == 0'u64:
    return err(what & " is block 0, the container's root directory")
  if b < r.rootBlocks:
    return err(what & " is block " & $b & ", inside the " & $r.rootBlocks &
      "-block root region")
  if b >= r.wholeBlocks:
    r.wholeBlocks = r.fileBlocks()
    if b >= r.wholeBlocks:
      return err(what & " is block " & $b & ", past the container's " &
        $r.wholeBlocks & " whole blocks")
  ok()

proc readPtr(r: var LiveReader, blk: uint64, slot: uint64): Result[uint64, string] =
  ## A mapping pointer; a non-zero one is kept for good (§6, "What a cache
  ## may keep": only a mapping block's non-zero slots).
  let key = blk * 1024'u64 + slot
  if key in r.mapCache:
    return ok(r.mapCache.getOrDefault(key))
  let b = ? r.readRaw(blk * uint64(r.blockSize) + slot * 8, 8)
  let v = readU64LE(b, 0)
  if v != 0'u64:
    r.mapCache[key] = v
  ok(v)

proc resolveBlock*(r: var LiveReader, name: string, mapBlock: uint64,
                   idx: uint64): Result[uint64, string] =
  ## The data block of logical block `idx` of a member whose `MapBlock` is
  ## `mapBlock` (§2's three forms; §4's cumulative chain).
  let what = "member " & name & " block " & $idx
  if mapBlock == 0'u64:
    return err(what & ": the member has no block")
  if isDirectMapBlock(mapBlock):
    if idx != 0'u64:
      return err(what & ": a direct member has one block")
    let b = directDataBlock(mapBlock)
    ? r.checkBlock(b, what)
    return ok(b)
  ? r.checkBlock(mapBlock, what & "'s mapping root")
  let usable = uint64(r.blockSize) div 8 - 1
  var i = idx
  var level = 1
  var cap = usable
  var cur = mapBlock
  while i >= cap:
    i -= cap
    inc level
    if level > MaxChainLevels:
      return err(what & ": past the mapping's levels")
    let nxt = ? r.readPtr(cur, usable)
    ? r.checkBlock(nxt, what & "'s level-" & $level & " chain pointer")
    cur = nxt
    cap = cap * usable
  var sub = cap div usable
  var l = level
  while l > 1:
    let child = ? r.readPtr(cur, i div sub)
    ? r.checkBlock(child, what & "'s level-" & $l & " child pointer")
    cur = child
    i = i mod sub
    sub = sub div usable
    dec l
  let d = ? r.readPtr(cur, i)
  ? r.checkBlock(d, what)
  ok(d)

proc readMember*(r: var LiveReader, slot: int, off: uint64, n: int):
    Result[seq[byte], string] =
  ## `n` bytes of the member in `slot` at `off`, below its accepted `Size`.
  if slot < 0 or slot >= r.slots:
    return err("container " & r.path & ": no such member slot")
  let e = r.entries[slot]
  let name = base40Decode(e.name)
  if off + uint64(n) > e.size:
    return err("member " & name & ": " & $n & " bytes at " & $off &
      " are past its Size " & $e.size)
  var outp = newSeq[byte](n)
  var done = 0
  let bs = uint64(r.blockSize)
  while done < n:
    let at = off + uint64(done)
    let b = ? r.resolveBlock(name, e.mapBlock, at div bs)
    let k = min(n - done, int(bs - at mod bs))
    let bytes = ? r.readRaw(b * bs + at mod bs, k)
    copyMem(addr outp[done], unsafeAddr bytes[0], k)
    done += k
  ok(outp)

proc readStream*(r: var LiveReader, label: string, size, mapBlock: uint64,
                 off: uint64, n: int): Result[seq[byte], string] =
  ## `n` bytes at `off` of a keyed stream whose record names `(size,
  ## mapBlock)` (`ctfs-container.md` §7a), with every refusal a member's read
  ## has.
  if off + uint64(n) > size:
    return err(label & ": " & $n & " bytes at " & $off & " are past its size " & $size)
  var outp = newSeq[byte](n)
  var done = 0
  let bs = uint64(r.blockSize)
  while done < n:
    let at = off + uint64(done)
    let b = ? r.resolveBlock(label, mapBlock, at div bs)
    let k = min(n - done, int(bs - at mod bs))
    let bytes = ? r.readRaw(b * bs + at mod bs, k)
    copyMem(addr outp[done], unsafeAddr bytes[0], k)
    done += k
  ok(outp)

proc readMemberU64*(r: var LiveReader, slot: int, off: uint64): Result[uint64, string] =
  let b = ? r.readMember(slot, off, 8)
  ok(readU64LE(b, 0))

const InPlaceSuffixes = [".hdr", ".nod", ".bkt", ".ovf", ".top"]
const InPlaceMembers = ["threads.rec", "corrmark.rec", "corrmark.ext"]

proc updatesInPlace*(name: string): bool =
  ## A keyed member whose realization updates blocks in place: a header, B1
  ## nodes, H1 buckets and overflow blocks, C1's top table, and the records
  ## and extents MCR's catalogue updates in place.
  if name in InPlaceMembers: return true
  for s in InPlaceSuffixes:
    if name.len > s.len and name[name.len - s.len .. ^1] == s: return true
  false

proc blocksToDrop*(r: var LiveReader, before: seq[RootSlot]): seq[uint64] =
  ## What a block cache must drop after a refresh that moved the root from
  ## `before` to the reader's current entries (`ctfs-container.md` §6, "What
  ## Is Mutated In Place, And What A Cache May Keep"): for a member whose
  ## entry changed, its last partial data block as of `before` and its level-1
  ## mapping block (a slot held as 0 must be re-read); for a keyed member that
  ## updates blocks in place, every one of its blocks. The root region is
  ## never kept, so it is not listed.
  let bs = uint64(r.blockSize)
  for i in 0 ..< r.slots:
    let e = r.entries[i]
    if e.name == 0: continue
    let old = if i < before.len: before[i] else: RootSlot()
    let name = r.slotName(i)
    if updatesInPlace(name):
      for lb in 0'u64 ..< (e.size + bs - 1) div bs:
        let b = r.resolveBlock(name, e.mapBlock, lb)
        if b.isOk: result.add b.get()
      continue
    if old == e: continue
    if old.size mod bs != 0'u64 and old.mapBlock != 0'u64:
      let b = r.resolveBlock(name, e.mapBlock, old.size div bs)
      if b.isOk: result.add b.get()
    if e.mapBlock != 0'u64 and not isDirectMapBlock(e.mapBlock):
      result.add e.mapBlock

# ---------------------------------------------------------------------------
# Record mirrors
# ---------------------------------------------------------------------------

proc mirrorExtent*(r: var LiveReader, entry: int, rec: uint64):
    Result[MirrorExtent, void] =
  ## The page's mirror extent of record `rec` of the member in slot `entry`,
  ## if the writer claimed one. A found extent is kept: it never changes.
  if not r.attached: return err()
  for x in r.extentCache:
    if x.entry == uint64(entry) and rec >= x.first and rec < x.first + x.count:
      return ok(x)
  let x = r.page.findExtent(uint64(entry), rec)
  if x.isOk:
    r.extentCache.add x.get()
  x
