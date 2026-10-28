when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## The machinery every keyed member shares (`ctfs-container.md` §7a,
## `ctfs-keyed-families.md` §4 and §5.8.0): its root members, the records it
## appends, the words it stores in place, and the seal that publishes them in
## phases.
##
## # A seal is planned, then executed
##
## A keyed family buffers everything between seals in its own memory, so the
## container's general publication (`publish`) never publishes half of a
## family. At a seal the family builds a `SealPlan`: it appends the bytes
## nothing yet publishes (new records, new index nodes, value-heap frames) to
## its members, which marks their blocks dirty, and it lists, in write order,
## the operations that publish them:
##
## - a **barrier**: every dirty block is handed to the operating system, in
##   block order (`flushDirtyData`), and the next operation starts only after
##   those writes have returned;
## - **entries**: the root entries of named members as they stand at that
##   point of the plan, each through the live coordination page first and then
##   in one 24-byte write;
## - an **in-place write**: bytes stored over bytes the file already holds (a
##   node's new slots, a count, a hole fill, a header), each in one write call
##   of its own;
## - a **mirror**: a value published in the page under its slot's `seq`.
##
## `executeSeal` then calls the seal observer with every publication item
## (`CTFS-Binary-Format.md` §2.15.3: before the seal's first publication
## write), runs the operations in order, and increments the writer's
## `seal_epoch` (`ctfs-container.md` §6 step 4). Appending at plan time hands
## nothing to the operating system: a streaming container defers its writes to
## the barriers.

import std/[tables]
import results
import ./types
import ./base40
import ./block_mapping
import ./container
import ./live_page
import ./mutation_arms

export live_page, mutation_arms

const
  KeyedHeaderSize* = 256
    ## `ctfs-keyed-families.md` §5.8.0: a header member is exactly 256 bytes,
    ## 32 `u64` words.
  NodeSize* = 4096
    ## B1's nodes and H1's buckets are whole 4096-byte blocks of their member.
  MirrorExtentRecords* = 4096'u64
    ## The per-thread and node mirrors claim ranges of 4096 records
    ## (`ctfs-container.md` §6, "Claiming a mirror extent").

type
  KMember* = object
    ## A root member a keyed family owns.
    name*: string
    f*: CtfsInternalFile

  SealOpKind* = enum
    sokBarrier, sokEntries, sokInPlace, sokMirror

  SealOp* = object
    case kind*: SealOpKind
    of sokBarrier:
      discard
    of sokEntries:
      entries*: seq[tuple[slot: int, bytes: array[24, byte]]]
    of sokInPlace:
      at*: int               ## image offset
      bytes*: seq[byte]
      hasItem*: bool
      item*: SealItem
    of sokMirror:
      seqOff*: int
      words*: seq[uint64]

  SealPlan* = object
    ops*: seq[SealOp]
    projRoot: seq[byte]
      ## The root region as the file will hold it after the operations listed
      ## so far: what a later `entries` operation is compared against.
    withItems: bool

proc registerKeyedMember*(c: var Ctfs, name: string) =
  if name notin c.keyedMembers:
    c.keyedMembers.add name

proc createKMember*(c: var Ctfs, name: string): Result[KMember, string] =
  ## Create a root member that a keyed family owns, and register it as keyed
  ## (root growth refuses a container that holds one, naming it).
  let f = ? c.addFile(name)
  c.registerKeyedMember(name)
  ok(KMember(name: name, f: f))

proc size*(m: KMember): uint64 {.inline.} = m.f.writePos

proc entryIndex*(m: KMember): int {.inline.} = m.f.entryIndex

proc imageOffset*(c: Ctfs, m: KMember, memberOff: uint64): Result[int, string] =
  ## The image offset of byte `memberOff` of member `m`.
  if memberOff >= m.f.writePos:
    return err("offset " & $memberOff & " is past member " & m.name &
      "'s " & $m.f.writePos & " bytes")
  let bs = uint64(c.blockSize)
  let mapBlock = readU64LE(c.data, c.fileEntryOffset(m.f.entryIndex) + 8)
  let b = c.resolveFileBlock(mapBlock, memberOff div bs)
  if b == 0'u64 or b < c.rootBlockCount() or b >= c.nextFreeBlock:
    return err("member " & m.name & " does not resolve block " &
      $(memberOff div bs))
  ok(c.blockOffset(b) + int(memberOff mod bs))

proc appendTo*(c: var Ctfs, m: var KMember, bytes: openArray[byte]): Result[uint64, string] =
  ## Append bytes nothing publishes yet; returns the member offset they start
  ## at.
  let at = m.f.writePos
  ? c.writeToFile(m.f, bytes)
  ok(at)

proc padTo*(c: var Ctfs, m: var KMember, align: uint64): Result[void, string] =
  let r = m.f.writePos mod align
  if r != 0:
    var z = newSeq[byte](int(align - r))
    discard ? c.appendTo(m, z)
  ok()

proc readMember*(c: Ctfs, m: KMember, memberOff: uint64, n: int): Result[seq[byte], string] =
  ## `n` bytes of member `m` at `memberOff`, as the image holds them.
  var outp = newSeq[byte](n)
  var done = 0
  let bs = int(c.blockSize)
  while done < n:
    let off = memberOff + uint64(done)
    let at = ? c.imageOffset(m, off)
    let k = min(n - done, bs - int(off mod uint64(bs)))
    copyMem(addr outp[done], unsafeAddr c.data[at], k)
    done += k
  ok(outp)

# ---------------------------------------------------------------------------
# Records (`ctfs-keyed-families.md` §2.2, §5.8.0)
# ---------------------------------------------------------------------------

proc perBlock*(recSize: int): uint64 = uint64(4096 div recSize)

proc recordOffset*(recSize: int, r: uint64): uint64 =
  ## Record `r` at `(r div per_block) * 4096 + (r mod per_block) * S`.
  let pb = perBlock(recSize)
  (r div pb) * 4096'u64 + (r mod pb) * uint64(recSize)

proc recordCount*(recSize: int, size: uint64): uint64 =
  ## The whole records below a member `Size`.
  let pb = perBlock(recSize)
  (size div 4096'u64) * pb + min((size mod 4096'u64) div uint64(recSize), pb)

proc appendRecord*(c: var Ctfs, m: var KMember, recSize: int,
                   rec: openArray[byte]): Result[uint64, string] =
  ## Append the next record: the zero tail of the block before it first when
  ## the record does not fit there. Returns its record number.
  doAssert rec.len == recSize
  let n = recordCount(recSize, m.f.writePos)
  let off = recordOffset(recSize, n)
  if off > m.f.writePos:
    var z = newSeq[byte](int(off - m.f.writePos))
    discard ? c.appendTo(m, z)
  discard ? c.appendTo(m, rec)
  ok(n)

# ---------------------------------------------------------------------------
# Building a plan
# ---------------------------------------------------------------------------

proc initSealPlan*(c: Ctfs): SealPlan =
  SealPlan(projRoot: c.fileRoot, withItems: c.sealObserver != nil)

proc barrier*(p: var SealPlan) =
  if p.ops.len > 0 and p.ops[^1].kind == sokBarrier:
    return
  p.ops.add SealOp(kind: sokBarrier)

proc publishEntries*(p: var SealPlan, c: Ctfs, members: openArray[KMember]) =
  ## The entries of `members` as they stand at this point of the plan, those
  ## that differ from what the file will hold.
  var op = SealOp(kind: sokEntries)
  for m in members:
    let off = c.fileEntryOffset(m.f.entryIndex)
    var b: array[24, byte]
    for k in 0 ..< 24: b[k] = c.data[off + k]
    var same = off + 24 <= p.projRoot.len
    if same:
      for k in 0 ..< 24:
        if p.projRoot[off + k] != b[k]:
          same = false
          break
    if same: continue
    if p.projRoot.len < off + 24: p.projRoot.setLen(off + 24)
    for k in 0 ..< 24: p.projRoot[off + k] = b[k]
    op.entries.add (slot: m.f.entryIndex, bytes: b)
  if op.entries.len > 0:
    p.ops.add op

proc inPlace*(p: var SealPlan, c: Ctfs, m: KMember, memberOff: uint64,
              bytes: openArray[byte]): Result[void, string] =
  ## Store `bytes` over member bytes the file holds, in one write, publishing
  ## nothing by itself (a node's new slots past its count).
  let at = ? c.imageOffset(m, memberOff)
  if int(memberOff mod uint64(c.blockSize)) + bytes.len > int(c.blockSize):
    return err("an in-place write of member " & m.name & " at " & $memberOff &
      " would straddle a block")
  p.ops.add SealOp(kind: sokInPlace, at: at, bytes: @bytes)
  ok()

proc inPlaceItem*(p: var SealPlan, c: Ctfs, m: KMember, memberOff: uint64,
                  bytes: openArray[byte], item: SealItem): Result[void, string] =
  ## As `inPlace`, for a write that publishes: `item` names it.
  ? p.inPlace(c, m, memberOff, bytes)
  p.ops[^1].hasItem = true
  p.ops[^1].item = item
  ok()

proc mirror*(p: var SealPlan, seqOff: int, words: openArray[uint64]) =
  p.ops.add SealOp(kind: sokMirror, seqOff: seqOff, words: @words)

proc wordsOf*(bytes: openArray[byte], at: int): seq[SealWord] =
  ## The 8-byte words of `bytes`, which start at byte `at` of a record or node.
  var i = 0
  while i + 8 <= bytes.len:
    result.add (at: at + i, v: readU64LE(bytes, i))
    i += 8

# ---------------------------------------------------------------------------
# Executing a plan
# ---------------------------------------------------------------------------

proc planItems*(c: Ctfs, p: SealPlan): seq[SealItem] =
  ## Every publication item of the plan, in write order.
  var proj = c.fileRoot
  for op in p.ops:
    case op.kind
    of sokEntries:
      for e in op.entries:
        let off = c.fileEntryOffset(e.slot)
        let size = readU64LE(e.bytes, 0)
        let mapBlock = readU64LE(e.bytes, 8)
        let encoded = readU64LE(e.bytes, 16)
        var fromSize =
          if off + 24 <= proj.len and readU64LE(proj, off + 16) == encoded:
            readU64LE(proj, off)
          else: 0'u64
        var replace = false
        if size < fromSize:
          replace = true
          fromSize = 0
        if proj.len < off + 24: proj.setLen(off + 24)
        for k in 0 ..< 24: proj[off + k] = e.bytes[k]
        let name = base40Decode(encoded)
        var item = SealItem(kind: sikEntry, entry: e.slot, name: name,
          size: size, mapBlock: mapBlock, fromSize: fromSize, replace: replace)
        if size > fromSize:
          let all = readMemberBytes(c.data, name, size, mapBlock, c.blockSize)
          if all.isOk:
            item.bytes = all.get()[int(fromSize) ..< int(size)]
        result.add item
    of sokInPlace:
      if op.hasItem: result.add op.item
    else: discard

proc executeSeal*(c: var Ctfs, p: SealPlan): Result[void, string] =
  ## Run a plan: the observer's `beforeSeal` with every item, then the
  ## operations in order, then the seal's epoch (`endSeal`). A plan that
  ## publishes nothing is not a seal and increments nothing.
  var publishes = false
  for op in p.ops:
    if op.kind == sokEntries or (op.kind == sokInPlace and op.hasItem):
      publishes = true
  if p.withItems and c.sealObserver != nil and publishes:
    c.beginSeal(c.planItems(p))
  var ops = p.ops
  if mutated("size-before-record"):
    # Mutation arm: each member's Size published before the bytes it covers.
    var i = 0
    while i + 1 < ops.len:
      if ops[i].kind == sokBarrier and ops[i + 1].kind == sokEntries:
        swap(ops[i], ops[i + 1])
        i += 2
      else:
        inc i
  for op in ops:
    case op.kind
    of sokBarrier:
      c.flushDirtyData()
    of sokEntries:
      for e in op.entries:
        let off = c.fileEntryOffset(e.slot)
        if c.page != nil:
          c.page.publishRoot(e.slot, readU64LE(e.bytes, 0),
            readU64LE(e.bytes, 8), readU64LE(e.bytes, 16))
        if c.streaming:
          if not c.handBytesToOs(off, e.bytes):
            continue
          if c.fileRoot.len < off + 24: c.fileRoot.setLen(off + 24)
          for k in 0 ..< 24: c.fileRoot[off + k] = e.bytes[k]
    of sokInPlace:
      copyMem(addr c.data[op.at], unsafeAddr op.bytes[0], op.bytes.len)
      if c.streaming and not c.handBytesToOs(op.at, op.bytes):
        c.markDirty(op.at, op.bytes.len)
    of sokMirror:
      if c.page != nil:
        c.page.publishWords(op.seqOff, op.words)
  if publishes:
    c.endSeal()
  if c.ioFailure.len > 0:
    return err("seal of a keyed member: " & c.ioFailure)
  ok()

# ---------------------------------------------------------------------------
# Header members (`ctfs-keyed-families.md` §5.8.0)
# ---------------------------------------------------------------------------

type
  KHeader* = object
    ## A 256-byte header member of in-place root words.
    m*: KMember
    w*: array[32, uint64]
    written: array[32, uint64]
    everWritten*: bool
    extent: MirrorExtent
    hasExtent: bool
    extentTried: bool
    mirrorWords*: int
      ## How many leading words the page mirrors (B1: 2; H1: 4).

proc createKHeader*(c: var Ctfs, name: string, mirrorWords: int): Result[KHeader, string] =
  let m = ? c.createKMember(name)
  ok(KHeader(m: m, mirrorWords: mirrorWords))

proc headerBytes*(h: KHeader): array[KeyedHeaderSize, byte] =
  for i in 0 ..< 32:
    writeU64LE(result, 8 * i, h.w[i])

proc headerDirty*(h: KHeader): bool =
  not h.everWritten or h.w != h.written

proc headerMirror(h: var KHeader, c: Ctfs): Result[MirrorExtent, void] =
  if not h.extentTried and c.page != nil:
    h.extentTried = true
    let x = c.page.allocExtent(uint64(h.m.f.entryIndex), 0, 1, h.mirrorWords)
    if x.isOk:
      h.extent = x.get()
      h.hasExtent = true
  if h.hasExtent: ok(h.extent) else: err()

proc planHeader*(h: var KHeader, c: var Ctfs, p: var SealPlan): Result[void, string] =
  ## Publish the header's words when one changed: the page's mirror slot
  ## first, then the 256 bytes in one write; the first time, as an append
  ## published by the member's entry.
  if not h.headerDirty():
    return ok()
  let bytes = h.headerBytes()
  let x = h.headerMirror(c)
  var mw = newSeq[uint64](h.mirrorWords)
  for i in 0 ..< h.mirrorWords: mw[i] = h.w[i]
  if not h.everWritten:
    discard ? c.appendTo(h.m, bytes)
    p.barrier()
    if x.isOk: p.mirror(x.get().slotOffset(0), mw)
    p.publishEntries(c, [h.m])
  else:
    if x.isOk: p.mirror(x.get().slotOffset(0), mw)
    var item = SealItem(kind: sikIndex, name: h.m.name, record: 0,
      words: wordsOf(bytes, 0))
    ? p.inPlaceItem(c, h.m, 0, bytes, item)
  h.everWritten = true
  h.written = h.w
  ok()
