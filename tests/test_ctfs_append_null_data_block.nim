{.push raises: [].}

## The **write** path over a mapping that does not resolve
## (`CTFS-Binary-Format.md` §4, and the write-side note under §5d).
##
## Ported from the drifted native-recorder fork
## (`codetracer-native-recorder/ct_recorder/tests/test_ctfs_append_null_data_block.nim`,
## M61/M61b) onto the canonical `codetracer_ctfs` library. The fork exercised
## these checks through its reopen/append surface (`openCtfsStreamingAppend` +
## `findFile`), which the canonical library does not carry; the canonical
## library exposes every `Ctfs`/`CtfsInternalFile` field and every low-level
## helper, so each case here builds a real in-memory container with the
## library's own writer, damages a single u64 in a real mapping block the way a
## crash actually damages it, constructs the handle the reopen path would have
## produced (positioned mid-block or on a block boundary), and drives the
## public `writeToFile` over the result. NO MOCKS.
##
## # What is pinned
##
## `lookupDataBlock` answers "I cannot resolve this" with `0` — from a null
## level-1 slot, a null chain pointer, a null child, or a level overflow — and
## block 0 is the container header and the whole root directory. Fed straight
## into `blockOffset`, a `0` addresses byte 0, so a caller's payload lands on
## the header and every file entry: the `.ct` stops being a container. The
## twin defect on the allocation side reads a null chain/child pointer as "not
## allocated yet" and allocates a replacement over the only pointer to an
## existing subtree, orphaning every data block under it while the append
## reports success. Both are refused; the valid cases are untouched.

import std/strutils
import results
import codetracer_ctfs

const BS = 4096
const Usable = BS div 8 - 1  # 511 entries usable per mapping block

proc pattern(n: int, salt: int): seq[byte] {.raises: [].} =
  result = newSeq[byte](n)
  for i in 0 ..< n:
    result[i] = byte((i * 7 + i div BS + salt) mod 251)

proc mapRootOf(c: Ctfs, idx: int): uint64 {.raises: [].} =
  ## The file entry's mapping-root block number.
  readU64LE(c.data, c.fileEntryOffset(idx) + 8)

proc block0(c: Ctfs): seq[byte] {.raises: [].} =
  c.data[0 ..< BS]

proc readBack(c: Ctfs, name: string): Result[seq[byte], string] {.raises: [].} =
  ## Read one named stream through the independent in-memory reader, which
  ## never consults the writer's bookkeeping.
  readInternalFile(c.toBytes(), name, uint32(BS))

# ---------------------------------------------------------------------------

proc test_a_null_level_1_slot_does_not_overwrite_block_zero() {.raises: [].} =
  ## A container with one level-1 mapping slot zeroed: the state a crash between
  ## the mapping-block flush and the data-block flush leaves. A mid-block append
  ## to that stream must fail and must leave the file readable, not write the
  ## payload over the header and root directory.
  var c = createCtfs(uint32(BS))
  let mh = c.addFile("meta.dat")
  doAssert mh.isOk, mh.error
  var mf = mh.get()
  doAssert c.writeToFile(mf, pattern(BS * 2, 1)).isOk

  let sh = c.addFile("spans.dat")
  doAssert sh.isOk, sh.error
  var sf = sh.get()
  # Ends 4 bytes into its second data block, so the handle is mid-block.
  doAssert c.writeToFile(sf, pattern(BS + 4, 2)).isOk

  let root = c.mapRootOf(sf.entryIndex)
  doAssert root != 0'u64
  # Exactly one u64 changes: the level-1 slot for spans.dat's data block 1.
  writeU64LE(c.data, c.blockOffset(root) + 1 * 8, 0'u64)
  let before = c.block0()

  let res = c.writeToFile(sf, pattern(64, 9))
  doAssert res.isErr, "writeToFile reported success through a null level-1 slot"
  doAssert "null data block" in res.error,
    "the refusal does not name the null data block: " & res.error
  doAssert c.block0() == before,
    "the append rewrote the container header and root directory"
  doAssert hasCtfsMagic(c.toBytes()), "block 0 lost the CTFS magic"
  doAssert c.readBack("meta.dat").isOk,
    "the undamaged member stopped resolving after the refused append"
  echo "PASS: test_a_null_level_1_slot_does_not_overwrite_block_zero"

proc test_a_null_mapping_root_does_not_overwrite_block_zero() {.raises: [].} =
  ## `FileEntry.MapBlock` is zero while `Size` still says the stream has bytes —
  ## a crash between publishing an entry's size and publishing its mapping root.
  ## `writeToFile` used to feed that zero into `lookupDataBlock` as the *root*,
  ## reading its pointers out of the container header.
  var c = createCtfs(uint32(BS))
  let mh = c.addFile("meta.dat")
  doAssert mh.isOk, mh.error
  var mf = mh.get()
  doAssert c.writeToFile(mf, pattern(BS, 1)).isOk

  let sh = c.addFile("spans.dat")
  doAssert sh.isOk, sh.error
  var sf = sh.get()
  doAssert c.writeToFile(sf, pattern(BS + 40, 5)).isOk

  writeU64LE(c.data, c.fileEntryOffset(sf.entryIndex) + 8, 0'u64)
  let before = c.block0()

  let res = c.writeToFile(sf, pattern(16, 6))
  doAssert res.isErr, "writeToFile accepted a null mapping root"
  doAssert "mapping root" in res.error,
    "the refusal does not name the root: " & res.error
  doAssert c.block0() == before,
    "the append rewrote the container header and root directory"
  doAssert hasCtfsMagic(c.toBytes()), "block 0 lost the CTFS magic"
  echo "PASS: test_a_null_mapping_root_does_not_overwrite_block_zero"

proc test_a_mapping_root_past_the_high_water_mark_is_refused() {.raises: [].} =
  ## The other direction of the mapping-root bound: a garbage pointer past the
  ## allocator's high-water mark, which would otherwise index past `c.data`.
  var c = createCtfs(uint32(BS))
  let sh = c.addFile("spans.dat")
  doAssert sh.isOk, sh.error
  var sf = sh.get()
  doAssert c.writeToFile(sf, pattern(BS + 8, 2)).isOk

  writeU64LE(c.data, c.fileEntryOffset(sf.entryIndex) + 8, c.nextFreeBlock + 99)
  let before = c.block0()
  let res = c.writeToFile(sf, pattern(16, 3))
  doAssert res.isErr, "writeToFile accepted a mapping root past the high-water mark"
  doAssert "mapping root" in res.error, "unhelpful refusal: " & res.error
  doAssert c.block0() == before, "the refused append rewrote block 0"
  echo "PASS: test_a_mapping_root_past_the_high_water_mark_is_refused"

proc test_a_null_chain_pointer_does_not_orphan_the_subtree() {.raises: [].} =
  ## A stream that is an exact block multiple, so the next append takes
  ## `writeToFile`'s allocating branch and calls `insertDataBlock`. Its level-2
  ## chain pointer is zeroed; the append must refuse rather than allocate a
  ## replacement over the only pointer to the existing 511-block subtree.
  var c = createCtfs(uint32(BS))
  let mh = c.addFile("meta.dat")
  doAssert mh.isOk, mh.error
  var mf = mh.get()
  doAssert c.writeToFile(mf, pattern(BS, 1)).isOk

  let sh = c.addFile("spans.dat")
  doAssert sh.isOk, sh.error
  var sf = sh.get()
  # 512 data blocks: index 511 is the first that needs the level-2 chain.
  doAssert c.writeToFile(sf, pattern(BS * 512, 3)).isOk

  let root = c.mapRootOf(sf.entryIndex)
  let oldL2 = readU64LE(c.data, c.blockOffset(root) + Usable * 8)
  doAssert oldL2 != 0'u64, "the fixture does not actually use a level-2 chain"
  writeU64LE(c.data, c.blockOffset(root) + Usable * 8, 0'u64)
  let before = c.block0()
  let blocksBefore = c.nextFreeBlock
  let bytesBefore = c.data.len

  let res = c.writeToFile(sf, pattern(BS, 9))
  doAssert res.isErr,
    "writeToFile reported success through a null level-2 chain pointer"
  doAssert "null chain pointer" in res.error,
    "the refusal does not name the null chain pointer: " & res.error
  doAssert readU64LE(c.data, c.blockOffset(root) + Usable * 8) == 0'u64,
    "the refused append rewrote the chain pointer, orphaning the old subtree"
  doAssert c.nextFreeBlock == blocksBefore,
    "the refused append left the block count ahead of the mapping"
  doAssert c.data.len == bytesBefore, "the refused append grew the buffer"
  doAssert c.block0() == before, "the refused append rewrote block 0"
  doAssert c.readBack("meta.dat").isOk,
    "the undamaged member stopped resolving after the refused append"
  echo "PASS: test_a_null_chain_pointer_does_not_orphan_the_subtree"

proc test_a_null_level_2_child_does_not_orphan_the_subtree() {.raises: [].} =
  ## The same zero one step further down: a level-2 block's child pointer,
  ## reached by `navigateAndInsert` rather than by `insertDataBlock`'s chain
  ## loop. Two separate sites, so two cases.
  var c = createCtfs(uint32(BS))
  let mh = c.addFile("meta.dat")
  doAssert mh.isOk, mh.error
  var mf = mh.get()
  doAssert c.writeToFile(mf, pattern(BS, 1)).isOk

  let sh = c.addFile("spans.dat")
  doAssert sh.isOk, sh.error
  var sf = sh.get()
  # 1023 data blocks: index 1022 (r = 511) populates the level-2 block's
  # entry 1, so the next append (index 1023, r = 512) descends through entry 1
  # at a non-zero remainder.
  doAssert c.writeToFile(sf, pattern(BS * 1023, 3)).isOk

  let root = c.mapRootOf(sf.entryIndex)
  let l2 = readU64LE(c.data, c.blockOffset(root) + Usable * 8)
  doAssert l2 != 0'u64, "the fixture does not use a level-2 chain"
  let oldChild = readU64LE(c.data, c.blockOffset(l2) + 1 * 8)
  doAssert oldChild != 0'u64, "the fixture does not use a second level-2 child"
  writeU64LE(c.data, c.blockOffset(l2) + 1 * 8, 0'u64)
  let before = c.block0()

  let res = c.writeToFile(sf, pattern(BS, 9))
  doAssert res.isErr,
    "writeToFile reported success through a null level-2 child pointer"
  doAssert "null mapping pointer" in res.error,
    "the refusal does not name the null mapping pointer: " & res.error
  doAssert readU64LE(c.data, c.blockOffset(l2) + 1 * 8) == 0'u64,
    "the refused append rewrote the child pointer, orphaning the subtree"
  doAssert c.block0() == before, "the refused append rewrote block 0"
  echo "PASS: test_a_null_level_2_child_does_not_orphan_the_subtree"

proc test_a_healthy_append_across_the_level_boundary_still_works()
    {.raises: [].} =
  ## The control that says the new refusals do not simply stop the writer from
  ## ever extending a mapping. Nothing is damaged: an in-memory stream is grown
  ## across the level-1/level-2 boundary (index 511 is legitimately the first
  ## index its chain pointer covers), and it must read back byte-exact.
  var c = createCtfs(uint32(BS))
  let sh = c.addFile("spans.dat")
  doAssert sh.isOk, sh.error
  var sf = sh.get()
  let head = pattern(BS * 510, 4)  # 510 blocks: all level-1, no chain yet
  doAssert c.writeToFile(sf, head).isOk

  let root = c.mapRootOf(sf.entryIndex)
  doAssert readU64LE(c.data, c.blockOffset(root) + Usable * 8) == 0'u64,
    "the fixture must start with no level-2 chain"

  let tail = pattern(BS * 3, 5)    # crosses into level 2 at index 511
  let res = c.writeToFile(sf, tail)
  doAssert res.isOk, "a healthy append across the level boundary was refused: " &
    res.error
  doAssert readU64LE(c.data, c.blockOffset(root) + Usable * 8) != 0'u64,
    "the append did not create the level-2 chain it needed"

  let rb = c.readBack("spans.dat")
  doAssert rb.isOk, rb.error
  doAssert rb.get() == head & tail, "the stream did not read back as written"
  echo "PASS: test_a_healthy_append_across_the_level_boundary_still_works"

proc test_rewrite_file_content_is_length_preserving_in_place() {.raises: [].} =
  ## `rewriteFileContent`: a same-length overwrite touches only the changed
  ## bytes and keeps every data block where it was; a length mismatch and an
  ## empty file are both refused.
  var c = createCtfs(uint32(BS))
  let sh = c.addFile("meta.dat")
  doAssert sh.isOk, sh.error
  var sf = sh.get()
  let original = pattern(BS + 1234, 7)
  doAssert c.writeToFile(sf, original).isOk
  let mapBefore = c.mapRootOf(sf.entryIndex)

  let replacement = pattern(BS + 1234, 21)
  doAssert c.rewriteFileContent(sf, replacement).isOk,
    "a same-length rewrite was refused"
  doAssert c.mapRootOf(sf.entryIndex) == mapBefore,
    "the rewrite moved the mapping root"
  let rb = c.readBack("meta.dat")
  doAssert rb.isOk, rb.error
  doAssert rb.get() == replacement, "the rewrite did not take effect"

  doAssert c.rewriteFileContent(sf, pattern(BS, 3)).isErr,
    "a length-mismatched rewrite was accepted"

  var empty = c.addFile("empty.dat")
  doAssert empty.isOk
  doAssert c.rewriteFileContent(empty.get(), @[]).isErr,
    "rewriting an empty file was accepted"
  echo "PASS: test_rewrite_file_content_is_length_preserving_in_place"

proc test_truncate_file_content_points_at_a_fresh_mapping() {.raises: [].} =
  ## `truncateFileContent`: the entry is re-pointed at a fresh, empty mapping
  ## block and a pos-0 handle is returned, so the caller can rewrite at a
  ## different length. The new, shorter content must read back exactly.
  var c = createCtfs(uint32(BS))
  let sh = c.addFile("spantype.ns")
  doAssert sh.isOk, sh.error
  var sf = sh.get()
  doAssert c.writeToFile(sf, pattern(BS * 2 + 10, 8)).isOk
  let oldRoot = c.mapRootOf(sf.entryIndex)

  let th = c.truncateFileContent(sf)
  doAssert th.isOk, th.error
  var nf = th.get()
  doAssert nf.writePos == 0, "the truncated handle is not at position 0"
  doAssert c.mapRootOf(sf.entryIndex) != oldRoot,
    "truncate did not move the mapping root off the abandoned blocks"

  let shorter = pattern(37, 9)
  doAssert c.writeToFile(nf, shorter).isOk
  let rb = c.readBack("spantype.ns")
  doAssert rb.isOk, rb.error
  doAssert rb.get() == shorter, "the truncated stream did not read back as rewritten"
  echo "PASS: test_truncate_file_content_points_at_a_fresh_mapping"

when isMainModule:
  test_a_null_level_1_slot_does_not_overwrite_block_zero()
  test_a_null_mapping_root_does_not_overwrite_block_zero()
  test_a_mapping_root_past_the_high_water_mark_is_refused()
  test_a_null_chain_pointer_does_not_orphan_the_subtree()
  test_a_null_level_2_child_does_not_orphan_the_subtree()
  test_a_healthy_append_across_the_level_boundary_still_works()
  test_rewrite_file_content_is_length_preserving_in_place()
  test_truncate_file_content_points_at_a_fresh_mapping()
  echo "All ctfs append null-data-block guard tests passed!"
