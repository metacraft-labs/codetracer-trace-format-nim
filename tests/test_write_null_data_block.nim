{.push raises: [].}

## The **write** side of the null-data-block defect
## (`CTFS-Binary-Format.md` §4, and the "same gap exists on the write side"
## note under §5d).
##
## NO MOCKS. Every case writes a real container with the production writer
## (`createCtfs` / `addFile` / `writeToFile` / `writeCtfsToFile`) to a real
## temporary file, damages it the way a crash actually damages it — by zeroing
## a single u64 in a real mapping block — and then drives the production
## `openClosedCtfs` / `writeToFile` over the result. Nothing is stubbed.
##
## # What this is about
##
## M61a closed the read-side half: a `level == 1` mapping slot holding `0`
## resolved to block **0**, so seven readers served the container's own header
## and root directory as a stream's content. The write-side twin was left
## open, and it is destructive rather than merely wrong:
##
##   - `lookupDataBlock` answers "unresolved" with `0` from four separate
##     places — level overflow, a null chain pointer, a null child, and the
##     level-1 slot itself. **`navigateAndLookup` is not one of the defects.**
##     At `level == 1` the slot *is* the answer, so a null slot already comes
##     back as `0`; adding `if ptr == 0: return 0` there is a literal no-op.
##     (`test_a_null_chain_pointer_...` below is the demonstration: it nulls a
##     pointer the multi-level branch *already* checks, and the payload still
##     lands on block 0.)
##   - `container.nim`'s `writeToFile` took that `0` as a data block number and
##     computed `c.blockOffset(0) == 0`, so the caller's payload was written
##     **over block 0 — the container header and the entire root directory**.
##
## One zeroed u64 therefore turned "one stream is damaged" into "the container
## is destroyed and every stream in it is unreachable", with `writeToFile`
## still returning `ok()`.
##
## # Reachability
##
## `writeToFile` only consults `lookupDataBlock` on its **mid-block** path,
## i.e. when the handle's `writePos` is not a block multiple. Inside this
## repository every handle comes from `addFile` in the same session, so the
## mapping it walks is the one it just built and no in-repo caller can drive
## the lookup to zero. The library nevertheless *exports* the primitives that
## make it reachable — `Ctfs`, `CtfsInternalFile`, `openClosedCtfs` and
## `writeToFile` are all public — and the documented consumer pattern for
## "append to a stream that already exists in a sealed container" is exactly
## the one below: reopen, locate the entry, position a handle at
## `FileEntry.Size`, write. The sibling hand-maintained copy of this module,
## `codetracer-native-recorder`'s `ct_recorder/ctfs_nim.nim`, ships that
## pattern as `openCtfsStreamingAppend` + `findFile` and reaches the defect
## through a production entry point
## (`ct_server_record/span_emit.writeDiscoveredSpans`).
##
## `locateEntryForAppend` below is that consumer, written out in six lines so
## the sequence under test is the real one and not a poke at internals.
##
## # The second null-pointer hole: an append that allocates over a damaged slot
##
## The four cases above all reach the mapping through `lookupDataBlock`, which
## `writeToFile` consults only on its **mid-block** path. When the handle's
## `writePos` *is* a block multiple, `writeToFile` takes the other branch and
## calls `insertDataBlock` — and both of that proc's null branches
## (`insertDataBlock`'s chain pointer, `navigateAndInsert`'s child pointer) read
## a `0` as **"not allocated yet"** and allocate a replacement.
##
## Neither can tell "unallocated" from "corrupted" by looking at the pointer,
## and until the last three cases here existed neither tried: appending to a
## crash-damaged container overwrote the only pointer to the existing level-2+
## subtree, orphaning every data block under it, and returned `ok()`. Block 0 is
## never touched, so it is not the header destruction above — but it is the same
## zero, on the same write path, and it loses data. The canonical Rust writer
## had the identical hole at the identical two sites; it is fixed there in the
## same change, and pinned by `codetracer_ctfs/tests/writer_null_data_block.rs`.
##
## They *can* be told apart from the index. A mapping is filled in strictly
## increasing block-index order, so a pointer is legitimately null exactly when
## the index being placed is the **first index that pointer covers**: `idx == 0`
## after rebasing at a level, `subIdx == 0` within a child. Anything else means
## an earlier index already went through that pointer. `CTFS-Binary-Format.md`
## §4, "Null pointers during allocation", now states that normatively, because
## both implementations of the walk had the same hole and the spec did not say
## which reading was right.

import std/[os, strutils]
import results
import codetracer_ctfs
import codetracer_ctfs/container_append

const
  BS = int(DefaultBlockSize)

proc pattern(n: int, salt: int): seq[byte] =
  result = newSeq[byte](n)
  for i in 0 ..< n:
    result[i] = byte((i * 7 + i div BS + salt) mod 251)

proc tmpDir(name: string): string {.raises: [].} =
  result = getTempDir() / ("ctfs_wnull_" & name & "_" & $getCurrentProcessId())
  try:
    removeDir(result)
    createDir(result)
  except OSError, IOError:
    doAssert false, "cannot prepare " & result

proc dropDir(path: string) {.raises: [].} =
  try:
    removeDir(path)
  except OSError:
    discard

proc rawBytes(path: string): seq[byte] {.raises: [].} =
  let raw = readCtfsFromFile(path)
  doAssert raw.isOk, "readCtfsFromFile: " & raw.error
  raw.get()

proc putBytes(path: string, data: seq[byte]) {.raises: [].} =
  try:
    writeFile(path, data)
  except IOError, OSError:
    doAssert false, "cannot write " & path

proc sealedContainer(path: string, files: seq[(string, seq[byte])]) {.raises: [].} =
  var c = createCtfs()
  for (name, content) in files:
    let h = c.addFile(name)
    doAssert h.isOk, "addFile " & name & ": " & h.error
    var f = h.get()
    doAssert c.writeToFile(f, content).isOk, "writeToFile " & name
  doAssert writeCtfsToFile(c, path).isOk, "writeCtfsToFile"

proc entrySlot(data: openArray[byte], name: string): int {.raises: [].} =
  ## Index of the root entry carrying `name`, or -1.
  let encoded = base40Encode(name)
  for i in 0 ..< int(DefaultMaxRootEntries):
    let off = HeaderSize + ExtHeaderSize + i * FileEntrySize
    if off + FileEntrySize > data.len:
      break
    if readU64LE(data, off + 16) == encoded:
      return i
  -1

proc locateEntryForAppend(c: Ctfs, name: string): CtfsInternalFile {.raises: [].} =
  ## The consumer pattern this defect is reached through: locate an existing
  ## entry and position a handle at its end so `writeToFile` appends.
  ## Character-for-character what `ct_recorder/ctfs_nim.nim`'s `findFile`
  ## does, and the only thing `container_append` does not offer itself.
  let i = entrySlot(c.data, name)
  doAssert i >= 0, "no root entry named " & name
  let off = HeaderSize + ExtHeaderSize + i * FileEntrySize
  CtfsInternalFile(entryIndex: i, writePos: readU64LE(c.data, off),
                   dataBlockCount: 0)

proc mapRootOf(data: openArray[byte], name: string): uint64 {.raises: [].} =
  let i = entrySlot(data, name)
  doAssert i >= 0, "no root entry named " & name
  readU64LE(data, HeaderSize + ExtHeaderSize + i * FileEntrySize + 8)

proc containerIsIntact(data: openArray[byte], files: seq[(string, seq[byte])]):
    bool {.raises: [].} =
  ## Block 0 is still a CTFS header and the root directory still resolves every
  ## member to its recorded bytes. Deliberately checked from the *bytes*, not
  ## from a writer's own bookkeeping.
  if not hasCtfsMagic(data):
    return false
  for (name, content) in files:
    let r = readInternalFile(data, name)
    if r.isErr or r.get() != content:
      return false
  true

proc hexAt(data: openArray[byte], start, n: int): string {.raises: [].} =
  var parts: seq[string] = @[]
  for i in start ..< min(start + n, data.len):
    parts.add(toHex(data[i], 2).toLowerAscii)
  parts.join(" ")

proc block0Damage(before, after: openArray[byte]): string {.raises: [].} =
  ## What a failing case has to say out loud: which bytes of block 0 — the
  ## header and the root directory — the append rewrote, and to what.
  var first = -1
  var last = -1
  for i in 0 ..< min(BS, min(before.len, after.len)):
    if before[i] != after[i]:
      if first < 0: first = i
      last = i
  if first < 0:
    return "block 0 is unchanged"
  "block 0 bytes " & $first & ".." & $last & " were rewritten: was [" &
    hexAt(before, first, 16) & "] now [" & hexAt(after, first, 16) & "]"

# ---------------------------------------------------------------------------

proc test_a_null_level_1_slot_is_refused_rather_than_written_over_block_0()
    {.raises: [].} =
  ## THE case. A complete, sealed, block-multiple container — no truncation
  ## anywhere — with one level-1 mapping slot zeroed: the state a crash between
  ## the mapping-block flush and the data-block flush leaves. Appending to that
  ## stream must fail and must leave the file untouched, not write the payload
  ## over the header and root directory.
  let dir = tmpDir("l1")
  let path = dir / "t.ct"
  # `steps.dat` ends 4 bytes into its second block, which is what puts the
  # append on `writeToFile`'s mid-block path — and, since a null data block
  # resolves to block 0, puts the payload at byte 4 of the container: the
  # version byte, the encryption and shard bytes, the extended header's
  # BlockSize and MaxRootEntries, and then the root directory itself.
  let files = @[
    ("meta.dat", pattern(BS * 2, 1)),
    ("steps.dat", pattern(BS + 4, 2)),
  ]
  sealedContainer(path, files)

  var damaged = rawBytes(path)
  doAssert damaged.len mod BS == 0, "the fixture is not block-aligned"
  doAssert containerIsIntact(damaged, files), "the fixture is not readable to begin with"

  # Null the level-1 slot for steps.dat's data block 1 — the block the append
  # will land in. Exactly one u64 changes; nothing else about the container
  # differs from what the production writer produced.
  let root = mapRootOf(damaged, "steps.dat")
  doAssert root != 0'u64
  writeU64LE(damaged, int(root) * BS + 1 * 8, 0'u64)
  putBytes(path, damaged)
  let beforeAppend = rawBytes(path)

  var c = openClosedCtfs(path)
  doAssert c.isOk, "openClosedCtfs refused a block-multiple container: " & c.error
  var ctfs = c.get()
  var handle = locateEntryForAppend(ctfs, "steps.dat")
  doAssert handle.writePos mod uint64(BS) != 0'u64,
    "the fixture must leave the append on the mid-block path"

  let payload = pattern(64, 9)
  let res = ctfs.writeToFile(handle, payload)

  # The destructive claim first: block 0 is the header and the root directory,
  # and it is what a zero data block number addresses.
  doAssert ctfs.data == beforeAppend,
    "the append modified the container image — " &
    block0Damage(beforeAppend, ctfs.data)
  doAssert hasCtfsMagic(ctfs.data),
    "block 0 lost the CTFS magic — the payload was written over the header"
  doAssert containerIsIntact(ctfs.data, files[0 .. 0]),
    "the undamaged member stopped resolving after the append"

  doAssert res.isErr,
    "writeToFile reported success through a null level-1 mapping slot"
  doAssert "null data block" in res.error or "no data block" in res.error,
    "the refusal does not name the null data block: " & res.error

  # And the file on disk, which no phase of this test rewrote, still reads.
  let after = rawBytes(path)
  doAssert after == beforeAppend, "the file on disk changed"
  doAssert containerIsIntact(after, @[files[0]]),
    "the undamaged member stopped being readable"

  dropDir(dir)
  echo "PASS: test_a_null_level_1_slot_is_refused_rather_than_written_over_block_0"

proc test_a_null_chain_pointer_is_refused_rather_than_written_over_block_0()
    {.raises: [].} =
  ## The same destination reached through a *different* `return 0` in
  ## `lookupDataBlock` — the multi-level chain pointer, which M61a's read-side
  ## census already guarded. That it still lands on block 0 here is what says
  ## the defect is `writeToFile`'s unchecked use of the result, not only the
  ## missing level-1 check: guarding `navigateAndLookup` alone would leave this
  ## case writing over the header.
  let dir = tmpDir("chain")
  let path = dir / "t.ct"
  # 512 full blocks + 100 bytes: data block 512 is the first that needs the
  # level-2 chain, and the stream ends mid-block inside it.
  let files = @[
    ("meta.dat", pattern(BS, 1)),
    ("big.dat", pattern(BS * 512 + 100, 3)),
  ]
  sealedContainer(path, files)

  var damaged = rawBytes(path)
  doAssert containerIsIntact(damaged, files), "the fixture is not readable to begin with"
  let root = mapRootOf(damaged, "big.dat")
  let usable = BS div 8 - 1
  doAssert readU64LE(damaged, int(root) * BS + usable * 8) != 0'u64,
    "the fixture does not actually use a level-2 chain"
  writeU64LE(damaged, int(root) * BS + usable * 8, 0'u64)
  putBytes(path, damaged)
  let beforeAppend = rawBytes(path)

  var c = openClosedCtfs(path)
  doAssert c.isOk, "openClosedCtfs: " & c.error
  var ctfs = c.get()
  var handle = locateEntryForAppend(ctfs, "big.dat")
  let res = ctfs.writeToFile(handle, pattern(32, 4))

  doAssert ctfs.data == beforeAppend,
    "the append modified the container image — " &
    block0Damage(beforeAppend, ctfs.data)
  doAssert hasCtfsMagic(ctfs.data), "block 0 lost the CTFS magic"
  doAssert res.isErr, "writeToFile reported success through a null chain pointer"

  dropDir(dir)
  echo "PASS: test_a_null_chain_pointer_is_refused_rather_than_written_over_block_0"

proc test_a_null_mapping_root_is_refused_rather_than_written_over_block_0()
    {.raises: [].} =
  ## The entry-level twin: `FileEntry.MapBlock` itself is zero while `Size`
  ## still says the stream has bytes — a crash between publishing an entry's
  ## size and publishing its mapping root. `writeToFile` used to feed that zero
  ## straight into `lookupDataBlock` as the root, resolving pointers out of the
  ## header. It must be refused before any block arithmetic happens.
  let dir = tmpDir("root")
  let path = dir / "t.ct"
  let files = @[("meta.dat", pattern(BS, 1)), ("steps.dat", pattern(BS + 40, 5))]
  sealedContainer(path, files)

  var damaged = rawBytes(path)
  let i = entrySlot(damaged, "steps.dat")
  doAssert i >= 0
  writeU64LE(damaged, HeaderSize + ExtHeaderSize + i * FileEntrySize + 8, 0'u64)
  putBytes(path, damaged)
  let beforeAppend = rawBytes(path)

  var c = openClosedCtfs(path)
  doAssert c.isOk, "openClosedCtfs: " & c.error
  var ctfs = c.get()
  var handle = locateEntryForAppend(ctfs, "steps.dat")
  let res = ctfs.writeToFile(handle, pattern(16, 6))

  doAssert ctfs.data == beforeAppend,
    "the append modified the container image — " &
    block0Damage(beforeAppend, ctfs.data)
  doAssert hasCtfsMagic(ctfs.data), "block 0 lost the CTFS magic"
  doAssert res.isErr, "writeToFile accepted a null mapping root"
  doAssert "mapping root" in res.error, "the refusal does not name the root: " & res.error

  dropDir(dir)
  echo "PASS: test_a_null_mapping_root_is_refused_rather_than_written_over_block_0"

proc test_an_undamaged_append_still_works() {.raises: [].} =
  ## The control, and the thing that says the guard did not merely stop the
  ## writer from writing. Same shape as the first case with nothing zeroed:
  ## the mid-block append must succeed, the stream must read back as the
  ## concatenation, every other member must be untouched, and — the off-by-one
  ## that a `>=`/`>` slip would produce — a stream that ends exactly on a block
  ## boundary must also still be appendable.
  let dir = tmpDir("ok")
  let path = dir / "t.ct"
  let head = pattern(BS + 100, 2)
  let aligned = pattern(BS * 2, 7)
  let files = @[
    ("meta.dat", pattern(BS * 2, 1)),
    ("steps.dat", head),
    ("edge.dat", aligned),
  ]
  sealedContainer(path, files)

  var c = openClosedCtfs(path)
  doAssert c.isOk, "openClosedCtfs: " & c.error
  var ctfs = c.get()

  let midTail = pattern(300, 11)   # mid-block append: crosses into a new block
  var midHandle = locateEntryForAppend(ctfs, "steps.dat")
  doAssert midHandle.writePos mod uint64(BS) != 0'u64
  let midRes = ctfs.writeToFile(midHandle, midTail)
  doAssert midRes.isOk, "the guard refused a healthy mid-block append: " & midRes.error

  let edgeTail = pattern(50, 12)   # block-aligned append: the `>= vs >` edge
  var edgeHandle = locateEntryForAppend(ctfs, "edge.dat")
  doAssert edgeHandle.writePos mod uint64(BS) == 0'u64
  let edgeRes = ctfs.writeToFile(edgeHandle, edgeTail)
  doAssert edgeRes.isOk, "the guard refused a healthy aligned append: " & edgeRes.error

  doAssert hasCtfsMagic(ctfs.data), "a healthy append damaged block 0"
  doAssert containerIsIntact(ctfs.data, @[
    ("meta.dat", files[0][1]),
    ("steps.dat", head & midTail),
    ("edge.dat", aligned & edgeTail),
  ]), "a healthy append did not produce the expected container"

  dropDir(dir)
  echo "PASS: test_an_undamaged_append_still_works"

# ---------------------------------------------------------------------------
# The allocation half: a null pointer `insertDataBlock` used to read as
# "not allocated yet".
# ---------------------------------------------------------------------------

const Usable = BS div 8 - 1
  ## Data pointers per mapping block; the last of the `BS div 8` entries is the
  ## chain pointer. 511 for the default 4096-byte block.

proc ptrAt(data: openArray[byte], blockNum: uint64, index: int): uint64
    {.raises: [].} =
  readU64LE(data, int(blockNum) * BS + index * 8)

proc reopenAndAppend(path: string, name: string, tail: seq[byte]):
    (Result[void, string], seq[byte]) {.raises: [].} =
  ## Reopen a sealed container, append to an existing stream, and hand back both
  ## the writer's answer and the image it left. This is the documented consumer
  ## pattern of `CTFS-Binary-Format.md` §5d, not a poke at internals.
  let c = openClosedCtfs(path)
  doAssert c.isOk, "openClosedCtfs: " & c.error
  var ctfs = c.get()
  var handle = locateEntryForAppend(ctfs, name)
  let res = ctfs.writeToFile(handle, tail)
  (res, ctfs.data)

proc readback(data: openArray[byte], name: string): string {.raises: [].} =
  ## What a stream reads back as, in one line, for a failure message.
  let r = readInternalFile(data, name)
  if r.isErr: "refused by the reader: " & r.error
  else: $r.get().len & " bytes"

proc test_a_null_chain_pointer_is_refused_rather_than_orphaning_the_subtree()
    {.raises: [].} =
  ## A sealed container whose stream is an **exact block multiple**, so the
  ## append takes `writeToFile`'s allocating branch rather than its mid-block
  ## one and `lookupDataBlock` — where the first four cases catch the zero — is
  ## never consulted.
  let dir = tmpDir("orphan_chain")
  let path = dir / "t.ct"
  # 512 data blocks: indices 0..510 live in the level-1 root and index 511 is
  # the first that needs the level-2 chain, so the chain pointer exists and
  # covers 511 real data blocks.
  let files = @[("meta.dat", pattern(BS, 1)), ("big.dat", pattern(BS * 512, 3))]
  sealedContainer(path, files)

  var damaged = rawBytes(path)
  doAssert containerIsIntact(damaged, files), "the fixture is not readable to begin with"
  let root = mapRootOf(damaged, "big.dat")
  let oldL2 = ptrAt(damaged, root, Usable)
  doAssert oldL2 != 0'u64, "the fixture does not actually use a level-2 chain"
  writeU64LE(damaged, int(root) * BS + Usable * 8, 0'u64)
  putBytes(path, damaged)
  let beforeAppend = rawBytes(path)

  let (res, image) = reopenAndAppend(path, "big.dat", pattern(BS, 9))
  let newL2 = ptrAt(image, root, Usable)

  doAssert res.isErr,
    "writeToFile reported success on a container whose level-2 chain pointer " &
    "was null. It wrote a fresh mapping block into root[" & $Usable & "] (was " &
    $oldL2 & " before the damage, 0 after it, now " & $newL2 & "), so the " &
    $Usable & " data blocks the old level-2 subtree at block " & $oldL2 &
    " mapped are no longer referenced by anything in the container and cannot " &
    "be recovered from it. big.dat now " & readback(image, "big.dat")
  # Checked before the wording, deliberately: refusing is only worth anything if
  # it also leaves the evidence in place, and the pointer a repair tool would use
  # to find block `oldL2` again must not have been replaced by a fresh, empty
  # mapping block. Dropping the chain-pointer rule but keeping the child one
  # still produces *a* refusal — from one level further down, after the damage
  # has been done — so an assertion on the message alone would call that a pass
  # of the wrong kind.
  doAssert newL2 == 0'u64,
    "the append was refused but still rewrote root[" & $Usable & "] to " &
    $newL2 & ", destroying the only thing that says the level-2 subtree at " &
    "block " & $oldL2 & " ever existed"
  doAssert "null chain pointer" in res.error,
    "the refusal does not name the null chain pointer: " & res.error
  doAssert image == beforeAppend,
    "the refused append modified the container image — " &
    block0Damage(beforeAppend, image)
  doAssert containerIsIntact(image, @[files[0]]),
    "the undamaged member stopped resolving"

  dropDir(dir)
  echo "PASS: test_a_null_chain_pointer_is_refused_rather_than_orphaning_the_subtree"

proc test_a_null_level_2_child_is_refused_rather_than_orphaning_the_subtree()
    {.raises: [].} =
  ## The same zero one step further down the walk: a level-2 block's child
  ## pointer, reached by `navigateAndInsert` rather than by `insertDataBlock`'s
  ## chain loop. Two separate sites, so two cases.
  let dir = tmpDir("orphan_child")
  let path = dir / "t.ct"
  # 1023 data blocks. Rebased at level 2, index i maps to r = i - 511 and the
  # level-2 block's entry is r div 511. Index 1022 gives r = 511, i.e. entry 1 —
  # so the level-2 block has two children, and the next append (index 1023,
  # r = 512) descends through entry 1 at a non-zero remainder.
  let files = @[("meta.dat", pattern(BS, 1)), ("big.dat", pattern(BS * 1023, 3))]
  sealedContainer(path, files)

  var damaged = rawBytes(path)
  doAssert containerIsIntact(damaged, files), "the fixture is not readable to begin with"
  let root = mapRootOf(damaged, "big.dat")
  let l2 = ptrAt(damaged, root, Usable)
  doAssert l2 != 0'u64, "the fixture does not use a level-2 chain"
  let oldChild = ptrAt(damaged, l2, 1)
  doAssert oldChild != 0'u64, "the fixture does not use a second level-2 child"
  writeU64LE(damaged, int(l2) * BS + 8, 0'u64)
  putBytes(path, damaged)
  let beforeAppend = rawBytes(path)

  let (res, image) = reopenAndAppend(path, "big.dat", pattern(BS, 9))
  let newChild = ptrAt(image, l2, 1)

  doAssert res.isErr,
    "writeToFile reported success on a container whose level-2 child pointer " &
    "was null. It wrote a fresh mapping block into block " & $l2 & " entry 1 " &
    "(was " & $oldChild & " before the damage, 0 after it, now " & $newChild &
    "), orphaning the level-1 subtree at block " & $oldChild & ". big.dat now " &
    readback(image, "big.dat")
  doAssert "null mapping pointer" in res.error,
    "the refusal does not name the null mapping pointer: " & res.error

  doAssert newChild == 0'u64,
    "the append was refused but still rewrote block " & $l2 & " entry 1 to " &
    $newChild & ", orphaning the level-1 subtree at block " & $oldChild
  doAssert image == beforeAppend,
    "the refused append modified the container image — " &
    block0Damage(beforeAppend, image)

  dropDir(dir)
  echo "PASS: test_a_null_level_2_child_is_refused_rather_than_orphaning_the_subtree"

proc test_a_reopened_append_may_still_create_the_level_2_chain_it_needs()
    {.raises: [].} =
  ## The control for the two cases above, and the one that says the new refusals
  ## do not simply stop the writer from ever extending a mapping.
  ##
  ## It deliberately crosses the level-1/level-2 boundary **inside the reopened
  ## session**: the sealed container has 510 data blocks (all in the level-1
  ## root, no chain pointer at all) and the append takes it to 513, so index 511
  ## is the first to need level 2 and the chain pointer is legitimately null when
  ## the reopened writer reaches it. That is the exact state the rule has to keep
  ## allowing, and it is where a rule stated one index too strictly shows up.
  let dir = tmpDir("orphan_ok")
  let path = dir / "t.ct"
  let head = pattern(BS * 510, 4)
  let files = @[("meta.dat", pattern(BS * 2, 1)), ("big.dat", head)]
  sealedContainer(path, files)

  var image0 = rawBytes(path)
  let root = mapRootOf(image0, "big.dat")
  doAssert ptrAt(image0, root, Usable) == 0'u64,
    "the fixture must start with no level-2 chain, or it does not test creating one"

  let tail = pattern(BS * 3, 5)
  let (res, image) = reopenAndAppend(path, "big.dat", tail)
  doAssert res.isOk, "a healthy reopened append was refused: " & res.error
  doAssert ptrAt(image, root, Usable) != 0'u64,
    "the reopened append did not create the level-2 chain it needed"
  doAssert containerIsIntact(image, @[
    ("meta.dat", files[0][1]),
    ("big.dat", head & tail),
  ]), "the reopened append did not produce the expected container"

  dropDir(dir)
  echo "PASS: test_a_reopened_append_may_still_create_the_level_2_chain_it_needs"

when isMainModule:
  test_a_null_level_1_slot_is_refused_rather_than_written_over_block_0()
  test_a_null_chain_pointer_is_refused_rather_than_written_over_block_0()
  test_a_null_mapping_root_is_refused_rather_than_written_over_block_0()
  test_an_undamaged_append_still_works()
  test_a_null_chain_pointer_is_refused_rather_than_orphaning_the_subtree()
  test_a_null_level_2_child_is_refused_rather_than_orphaning_the_subtree()
  test_a_reopened_append_may_still_create_the_level_2_chain_it_needs()
  echo "All CTFS write-side null-data-block tests passed!"
