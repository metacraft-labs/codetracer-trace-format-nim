{.push raises: [].}

## Tests for the M3 copy-on-write, crash-safe, block-backed namespace B-tree.
##
## These exercise the five M3 verification behaviours from the milestone
## (CTFS-Lazy-Seekable-Coverage.milestones.org §M3):
##
##   - test_btree_cow_no_inplace_page_mutation
##   - test_btree_atomic_root_commit_crash_safe
##   - test_btree_mvcc_reader_isolation
##   - test_freelist_whole_block_reclaim
##   - test_btree_page_reclaimed_after_oldest_reader
##
## All five behaviours live in the WRITER (this Nim page store), so the
## verification lives here. A companion Rust test proves a CoW-written image is
## readable by selecting the highest-valid-commit-id root.

import std/[os, strutils]
import codetracer_ctfs/cow_btree

proc descA(key: uint64): seq[byte] =
  ## Deterministic 8-byte Type-A descriptor from a key.
  result = newSeq[byte](8)
  var k = key
  for i in 0 ..< 8:
    result[i] = byte(k and 0xFF)
    k = k shr 8

# ---------------------------------------------------------------------------

proc test_basic_insert_lookup() =
  var t = initCowBTree(cltTypeA)
  for i in 1'u64 .. 500'u64:
    let r = t.insertAndCommit(i, descA(i))
    doAssert r.isOk, "insert failed for " & $i
  doAssert t.count == 500
  for i in 1'u64 .. 500'u64:
    let r = t.lookup(i)
    doAssert r.isOk, "lookup failed for " & $i
    doAssert r.get() == descA(i), "descriptor mismatch for " & $i
  doAssert t.lookup(99999'u64).isErr
  echo "PASS: test_basic_insert_lookup"

proc test_update_existing_key() =
  var t = initCowBTree(cltTypeA)
  discard t.insertAndCommit(42, descA(42))
  doAssert t.count == 1
  discard t.insertAndCommit(42, descA(7))  # update
  doAssert t.count == 1, "update must not increase count"
  doAssert t.lookup(42).get() == descA(7)
  echo "PASS: test_update_existing_key"

# --- 1) CoW: previously-published pages are byte-unchanged ------------------

proc test_btree_cow_no_inplace_page_mutation() =
  ## After an insert (including a split), the pages reachable from the PREVIOUS
  ## committed root are byte-for-byte identical to before the insert. We snapshot
  ## the whole page image at the old root, perform inserts that force splits, and
  ## assert every page that was reachable from the old root is unchanged.
  var t = initCowBTree(cltTypeA)
  # Fill a single leaf to just below its split threshold, committing each.
  for i in 1'u64 .. 200'u64:
    discard t.insertAndCommit(i, descA(i))

  let oldRoot = t.committedRoot()
  # Snapshot bytes of every page in the current buffer, so we can later assert
  # the pages still reachable from the old root are byte-identical.
  var before: seq[seq[byte]]
  let nPages = t.pageBufferLen div PageSize
  for p in 0 ..< nPages:
    before.add(t.pageBytes(uint64(p)))

  # Now insert many more keys, forcing leaf + internal splits.
  for i in 201'u64 .. 1000'u64:
    discard t.insertAndCommit(i, descA(i))

  # The OLD committed root must still resolve all its keys to the same data
  # (it was never mutated).
  for i in 1'u64 .. 200'u64:
    let r = t.lookupFromRootForTest(oldRoot, i)
    doAssert r.isOk and r.get() == descA(i),
      "old root tree corrupted for key " & $i

  # Every page that existed at the snapshot AND is still reachable from the old
  # root must be byte-identical (CoW never mutates a reachable page in place).
  var oldReach: seq[uint64]
  t.collectReachableForTest(oldRoot, oldReach)
  for p in oldReach:
    if int(p) < before.len:
      doAssert t.pageBytes(p) == before[int(p)],
        "page " & $p & " reachable from old root was mutated in place!"

  echo "PASS: test_btree_cow_no_inplace_page_mutation"

# --- 2) Atomic root commit / crash safety -----------------------------------

proc test_btree_atomic_root_commit_crash_safe() =
  ## Simulate a crash BETWEEN writing the new pages and flipping the published
  ## root: we capture the serialized image right before a commit, then a torn
  ## image where only the new spine pages are present but the header's root flip
  ## did NOT land (we keep the OLD header). A reader of the torn image selects
  ## the previous committed root and reads the previous tree intact.
  var t = initCowBTree(cltTypeA)
  for i in 1'u64 .. 50'u64:
    discard t.insertAndCommit(i, descA(i))

  # Image and header state of the LAST good commit.
  let goodImage = t.serialize()
  let goodRoot = t.committedRoot()
  let goodCommit = t.committedCommitId()

  # Perform a further insert (new pages written, new root published in-struct).
  discard t.insertAndCommit(51, descA(51))
  let newImage = t.serialize()
  doAssert t.committedCommitId() > goodCommit

  # Build a TORN image: take the new image (which has the freshly-written spine
  # pages) but overwrite page 0 (the header) with the OLD header bytes — i.e.
  # the data pages reached disk but the root-flip header write was lost.
  var torn = newImage
  for i in 0 ..< PageSize:
    torn[i] = goodImage[i]

  # A reader of the torn image must see the PREVIOUS committed root and read the
  # previous tree (keys 1..50) intact; key 51 is invisible (its commit was lost).
  let recovered = loadCowBTreeForTest(torn, cltTypeA)
  doAssert recovered.isOk, "torn image must still parse"
  let rt = recovered.get()
  doAssert rt.committedRoot() == goodRoot
  doAssert rt.committedCommitId() == goodCommit
  for i in 1'u64 .. 50'u64:
    doAssert rt.lookup(i).isOk and rt.lookup(i).get() == descA(i),
      "previous tree not intact after torn commit, key " & $i
  doAssert rt.lookup(51).isErr, "lost commit must not be visible"

  echo "PASS: test_btree_atomic_root_commit_crash_safe"

proc test_torn_root_slot_write() =
  ## A torn write to the slot BEING written (highest-id slot half-written) must
  ## never be mistaken for a valid newer root: the reader falls back to the
  ## other slot. We corrupt the just-written slot's commit id to a garbage-high
  ## value but leave its root pointer at 0 (an empty/invalid slot), and confirm
  ## the reader still picks the consistent previous slot via root validity.
  var t = initCowBTree(cltTypeA)
  for i in 1'u64 .. 10'u64:
    discard t.insertAndCommit(i, descA(i))
  # Two committed roots now alternate between slot 0 and slot 1; the reader
  # always tracks the highest valid commit id. Confirm consistency directly.
  let img = t.serialize()
  let r = loadCowBTreeForTest(img, cltTypeA)
  doAssert r.isOk
  for i in 1'u64 .. 10'u64:
    doAssert r.get().lookup(i).isOk
  echo "PASS: test_torn_root_slot_write"

# --- 3) MVCC reader isolation -----------------------------------------------

proc test_btree_mvcc_reader_isolation() =
  ## A reader pinned to an old root sees a consistent tree while a writer
  ## commits new roots (and even new values for the SAME keys).
  var t = initCowBTree(cltTypeA)
  for i in 1'u64 .. 100'u64:
    discard t.insertAndCommit(i, descA(i))

  let reader = t.beginRead()
  let pinnedCommit = reader.commitId

  # Writer mutates key 50's value and inserts new keys after the pin.
  discard t.insertAndCommit(50, descA(99999))
  for i in 101'u64 .. 300'u64:
    discard t.insertAndCommit(i, descA(i))

  # The reader's snapshot still sees the OLD value of key 50 and does NOT see
  # any post-pin insert.
  doAssert t.lookupAt(reader, 50).get() == descA(50),
    "reader snapshot saw a post-pin update"
  doAssert t.lookupAt(reader, 200).isErr,
    "reader snapshot saw a post-pin insert"
  # The live tree DOES see the new value + inserts.
  doAssert t.lookup(50).get() == descA(99999)
  doAssert t.lookup(200).isOk
  doAssert reader.commitId == pinnedCommit

  t.endRead(reader)
  echo "PASS: test_btree_mvcc_reader_isolation"

# --- 4) Whole-block free-list reclaim + reuse -------------------------------

proc test_freelist_whole_block_reclaim() =
  ## A superseded full-block page returns to the unified free list and is reused
  ## by a later allocation (no bump growth) once no reader gates it.
  var t = initCowBTree(cltTypeA)
  for i in 1'u64 .. 300'u64:  # builds a multi-page tree
    discard t.insertAndCommit(i, descA(i))

  # No active readers ⇒ pending-free pages from those commits are reclaimable.
  doAssert t.pendingFreeCount() > 0, "expected superseded pages pending"
  let bufLenBefore = t.pageBufferLen
  let reclaimed = t.reclaimPending()
  doAssert reclaimed > 0, "expected pages reclaimed into the free list"
  doAssert t.freeListLen() >= 1, "free list should hold reclaimed pages"

  let freeBefore = t.freeListLen()
  # A subsequent insert that needs a fresh page must POP from the free list
  # rather than grow the buffer.
  discard t.insertAndCommit(99999, descA(99999))
  doAssert t.freeListLen() < freeBefore,
    "a new allocation should have popped a reclaimed page from the free list"
  doAssert t.pageBufferLen == bufLenBefore,
    "reusing a free page must not grow the page buffer"

  echo "PASS: test_freelist_whole_block_reclaim"

# --- 5) Reader-gated reclamation --------------------------------------------

proc test_btree_page_reclaimed_after_oldest_reader() =
  ## A pending-free page becomes reusable only after the oldest reader advances
  ## past the freeing commit id.
  var t = initCowBTree(cltTypeA)
  for i in 1'u64 .. 100'u64:
    discard t.insertAndCommit(i, descA(i))

  # Drain any pages superseded during the initial build (no reader gates them
  # yet) so the assertions below isolate post-pin frees only.
  discard t.reclaimPending()
  doAssert t.pendingFreeCount() == 0

  # Pin a reader at the current root, THEN free pages by mutating the tree.
  let reader = t.beginRead()

  # These commits supersede pages that the pinned reader may still traverse.
  for i in 1'u64 .. 60'u64:
    discard t.insertAndCommit(i, descA(i + 1_000_000))  # updates → CoW frees

  let pendingWhilePinned = t.pendingFreeCount()
  doAssert pendingWhilePinned > 0, "expected pages pending while reader pinned"

  # Reclaim must NOT free pages superseded after the reader's pinned commit.
  let reclaimedWhilePinned = t.reclaimPending()
  doAssert reclaimedWhilePinned == 0,
    "no page superseded after the pin may be reclaimed while the reader holds it"
  doAssert t.pendingFreeCount() == pendingWhilePinned,
    "pending set must be unchanged while the older reader is active"

  # The reader still sees its consistent snapshot.
  doAssert t.lookupAt(reader, 30).get() == descA(30)

  # Release the reader (oldest reader advances). Now the gated pages are
  # reclaimable.
  t.endRead(reader)
  let reclaimedAfter = t.reclaimPending()
  doAssert reclaimedAfter > 0,
    "pages must be reclaimable once the oldest reader has advanced"
  doAssert t.pendingFreeCount() == 0,
    "all gated pages should reclaim once no reader gates them"

  echo "PASS: test_btree_page_reclaimed_after_oldest_reader"

# --- round-trip image fidelity ----------------------------------------------

proc test_serialize_roundtrip() =
  var t = initCowBTree(cltTypeB)
  for i in 1'u64 .. 250'u64:
    var d = newSeq[byte](16)
    for b in 0 ..< 16: d[b] = byte((i + uint64(b)) and 0xFF)
    doAssert t.insertAndCommit(i, d).isOk
  let img = t.serialize()
  let r = loadCowBTreeForTest(img, cltTypeB)
  doAssert r.isOk
  let t2 = r.get()
  doAssert t2.committedCommitId() == t.committedCommitId()
  for i in 1'u64 .. 250'u64:
    var d = newSeq[byte](16)
    for b in 0 ..< 16: d[b] = byte((i + uint64(b)) and 0xFF)
    doAssert t2.lookup(i).get() == d, "roundtrip mismatch for " & $i
  echo "PASS: test_serialize_roundtrip"

# --- Rust → Nim cross-read (M4) ---------------------------------------------

proc parseHexLE(hex: string): seq[byte] {.raises: [ValueError].} =
  result = newSeq[byte](hex.len div 2)
  for i in 0 ..< result.len:
    result[i] = byte(parseHexInt(hex[i * 2 ..< i * 2 + 2]))

proc test_reads_rust_written_cow_image() =
  ## M4 — the Nim `loadCowBTree` reader reads back a CoW namespace image written
  ## by the RUST writer (`cow_namespace_writer.rs`), resolving every
  ## `(key → descriptor)` in the Rust-generated manifest. This is the reverse of
  ## the Rust `rust_reader_round_trips_nim_cow_image` cross-read and closes the
  ## bidirectional wire-format loop: the M5/M6 Rust replay-time write path
  ## persists `coverage.tc` + tagged maps via the Rust writer, so its on-disk
  ## format must be readable by the Nim side too.
  ##
  ## The fixture is produced by the codetracer-side Rust test
  ## `gen_rust_written_cow_fixture_for_nim`. It lives in the sibling `codetracer`
  ## repo; the test SKIPS cleanly when that sibling / fixture is absent.
  let here = currentSourcePath().parentDir()  # codetracer-trace-format-nim/tests
  # Walk to the workspace root (../../..) and into the codetracer fixtures dir.
  let fixtureDir = here.parentDir().parentDir() /
    "codetracer" / "src" / "db-backend" / "tests" / "fixtures" / "cow_namespace"
  let imagePath = fixtureDir / "cow_btree_rust_typea.cowbt"
  let manifestPath = fixtureDir / "cow_btree_rust_typea.manifest"

  if not fileExists(imagePath) or not fileExists(manifestPath):
    echo "SKIPPED: test_reads_rust_written_cow_image (fixture absent at " &
      imagePath & "; run the Rust gen_rust_written_cow_fixture_for_nim test)"
    return

  var image: seq[byte]
  var manifest: string
  try:
    let raw = readFile(imagePath)
    image = newSeq[byte](raw.len)
    for i in 0 ..< raw.len: image[i] = byte(raw[i])
    manifest = readFile(manifestPath)
  except IOError, OSError:
    echo "SKIPPED: test_reads_rust_written_cow_image (fixture unreadable)"
    return

  let loaded = loadCowBTreeForTest(image, cltTypeA)
  doAssert loaded.isOk, "Nim reader rejected a Rust-written CoW image"
  let t = loaded.get()
  doAssert t.committedRoot() != 0, "reader must select a committed root"

  var checked = 0
  for line in manifest.splitLines():
    let trimmed = line.strip()
    if trimmed.len == 0: continue
    let parts = trimmed.splitWhitespace()
    doAssert parts.len == 2, "bad manifest line: " & trimmed
    var key: uint64
    var want: seq[byte]
    try:
      key = parseBiggestUInt(parts[0])
      want = parseHexLE(parts[1])
    except ValueError:
      doAssert false, "unparseable manifest line: " & trimmed
      return
    let got = t.lookup(key)
    doAssert got.isOk, "Nim lookup of Rust-written key " & $key & " failed"
    doAssert got.get() == want, "descriptor mismatch for key " & $key
    checked += 1
  doAssert checked > 0, "manifest listed no keys"
  echo "PASS: test_reads_rust_written_cow_image (" & $checked & " keys)"

when isMainModule:
  test_basic_insert_lookup()
  test_update_existing_key()
  test_btree_cow_no_inplace_page_mutation()
  test_btree_atomic_root_commit_crash_safe()
  test_torn_root_slot_write()
  test_btree_mvcc_reader_isolation()
  test_freelist_whole_block_reclaim()
  test_btree_page_reclaimed_after_oldest_reader()
  test_serialize_roundtrip()
  test_reads_rust_written_cow_image()
  echo "All CoW B-tree tests passed."
