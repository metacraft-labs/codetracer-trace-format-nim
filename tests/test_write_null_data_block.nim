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

when isMainModule:
  test_a_null_level_1_slot_is_refused_rather_than_written_over_block_0()
  test_a_null_chain_pointer_is_refused_rather_than_written_over_block_0()
  test_a_null_mapping_root_is_refused_rather_than_written_over_block_0()
  test_an_undamaged_append_still_works()
  echo "All CTFS write-side null-data-block tests passed!"
