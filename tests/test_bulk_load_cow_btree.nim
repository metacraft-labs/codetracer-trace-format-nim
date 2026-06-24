{.push raises: [].}

## Tests for the M4-perf bulk-load CoW B-tree constructor (`cow_btree.bulkLoad`).
##
## `bulkLoad` builds a committed tree from a pre-sorted batch in ONE bottom-up
## pass (pack leaves, build internal levels, one root commit) — O(N) page writes
## and a single commit — replacing the per-key `insertAndCommit` build (one CoW
## spine-copy + atomic commit PER key) used by `ctfs_store.buildPayloadNamespace`.
##
## These tests prove:
##   - VALUE EQUIVALENCE + READER COMPAT: a bulk-built tree resolves the SAME
##     `(key → descriptor)` set and enumerates the SAME keys as a per-key build,
##     and reloads cleanly through `loadCowBTree` (the reader path the Rust
##     `CowNamespaceReader` mirrors). The double-buffered root + payload (Type-B)
##     descriptors are correct.
##   - SINGLE COMMIT: the bulk build publishes exactly ONE commit (id 1, slot 0).
##   - PURITY / BYTE-STABILITY: the same sorted batch bulk-builds to the SAME
##     bytes every time, and a different insertion order of the same logical set
##     bulk-builds identically (the property `ctfs_store.rebuildNamespace` and
##     the daemon≡file invariant rely on).
##   - NOT byte-identical to the per-key build (documented: approach (b)) — the
##     per-key image carries abandoned CoW pages + a higher commit id, so its
##     bytes differ, but the LOGICAL tree is the same. We assert this explicitly
##     so the test records WHY byte-identity (approach a) was infeasible.
##   - EDGE CASES + VALIDATION: empty batch, single key, exactly `order` keys,
##     `order + 1` (forces a second level), large N; a non-ascending batch, a
##     duplicate key, a wrong descriptor width, and a non-fresh tree are all
##     rejected with an `Err` (never a silent mis-build).
##   - A SPEEDUP MEASUREMENT (bulk vs per-key build at a representative scale).

import std/[algorithm, times, monotimes]
import codetracer_ctfs/cow_btree

proc descB(key: uint64): seq[byte] =
  ## Deterministic 16-byte Type-B descriptor from a key.
  result = newSeq[byte](16)
  var k = key
  for i in 0 ..< 16:
    result[i] = byte((k + uint64(i * 7)) and 0xFF)
    k = k shr 8

proc orderB(): int =
  ## The Type-B leaf order = (4096 - 8) div (8 + 16). Kept local so the tests
  ## reference the same split threshold the writer uses.
  (PageSize - 8) div (8 + 16)

proc buildSeq(keys: seq[uint64]): seq[byte] =
  ## The per-key `insertAndCommit` build (what `buildPayloadNamespace` used to
  ## do) over a sorted key set.
  var t = initCowBTree(cltTypeB, skipSubBlocks = true)
  for k in keys:
    doAssert t.insertAndCommit(k, descB(k)).isOk, "seq insert " & $k
  t.serialize()

proc buildBulk(keys: seq[uint64]): seq[byte] =
  ## The bulk build over a sorted key set.
  var t = initCowBTree(cltTypeB, skipSubBlocks = true)
  var entries: seq[(uint64, seq[byte])]
  for k in keys: entries.add (k, descB(k))
  let r = t.bulkLoad(entries)
  doAssert r.isOk, "bulkLoad: " & (if r.isErr: r.error else: "")
  t.serialize()

proc assertResolves(keys: seq[uint64], img: seq[byte], label: string) =
  ## Every key resolves to its descriptor; key enumeration matches the sorted
  ## input; an absent key is reported absent.
  let loaded = loadCowBTree(img, cltTypeB)
  doAssert loaded.isOk, label & ": load failed " &
    (if loaded.isErr: loaded.error else: "")
  let t = loaded.value
  for k in keys:
    let lk = t.lookup(k)
    doAssert lk.isOk, label & ": missing key " & $k
    doAssert lk.value == descB(k), label & ": descriptor mismatch key " & $k
  doAssert t.lookup(0xFFFF_FFFF_FFFF_FFFF'u64).isErr, label & ": phantom key resolved"
  let ks = t.keys()
  doAssert ks.isOk, label & ": keys() failed"
  var want = keys
  want.sort()
  doAssert ks.value == want, label & ": key set mismatch"

proc keysOf(n: int): seq[uint64] =
  ## A deterministic ascending key set (gapped so the keys are not 1..N).
  for j in 1 .. n: result.add uint64(j) * 3 + 7
  result.sort()

# ---------------------------------------------------------------------------

proc test_bulk_load_matches_incremental() =
  ## A bulk-built tree is VALUE-equivalent and READER-compatible with the per-key
  ## build across leaf/internal-split scales, and both reload correctly.
  let ord = orderB()
  for n in [1, 2, ord - 1, ord, ord + 1, 2 * ord, 1000, 5000]:
    let keys = keysOf(n)
    let seqImg = buildSeq(keys)
    let bulkImg = buildBulk(keys)

    # Both images resolve the identical logical tree.
    assertResolves(keys, seqImg, "seq n=" & $n)
    assertResolves(keys, bulkImg, "bulk n=" & $n)

    # The bulk image publishes exactly ONE commit (id 1, slot 0).
    let bl = loadCowBTree(bulkImg, cltTypeB)
    doAssert bl.isOk
    doAssert bl.value.committedCommitId() == 1'u64,
      "bulk build must publish a single commit (n=" & $n & ")"
    doAssert bl.value.committedRoot() != 0, "bulk root must be published"

    # The two builds reach the SAME logical contents but are NOT byte-identical
    # (approach (b)): the per-key image carries abandoned CoW pages + a higher
    # commit id. For tiny inputs (n <= 1) the per-key build coincidentally has no
    # abandoned pages and matches; for n >= 2 the bytes diverge. We assert the
    # bulk image is never LARGER than the per-key one (the perf win is real).
    doAssert bulkImg.len <= seqImg.len,
      "bulk image must not be larger than the per-key image (n=" & $n & ")"
    if n >= 2:
      doAssert bulkImg != seqImg,
        "n>=2 per-key image carries abandoned pages; bytes must differ (n=" & $n & ")"
  echo "PASS: test_bulk_load_matches_incremental"

proc test_bulk_load_is_pure_and_byte_stable() =
  ## The bulk build is a pure function of the sorted batch: building the same
  ## logical key set twice (and from a shuffled-then-sorted order) yields
  ## byte-identical images. This is the property `ctfs_store.rebuildNamespace`
  ## and the daemon≡file byte-equality invariant rely on.
  let keys = keysOf(1234)
  let a = buildBulk(keys)
  let b = buildBulk(keys)
  doAssert a == b, "bulk build is not deterministic"

  # Same logical set, supplied in a different ORDER (then sorted by the caller,
  # as `buildPayloadNamespace` does) — must produce the SAME bytes.
  var shuffled = keys
  # Reverse, then sort back to ascending (the canonical order bulkLoad requires).
  shuffled.reverse()
  shuffled.sort()
  let c = buildBulk(shuffled)
  doAssert a == c, "bulk build is not order-independent after canonical sort"
  echo "PASS: test_bulk_load_is_pure_and_byte_stable"

proc test_bulk_load_empty_and_validation() =
  ## Edge cases + validation: empty batch leaves an empty (never-committed) tree;
  ## a non-ascending batch, a duplicate key, a wrong descriptor width, and a
  ## non-fresh tree are all rejected with an `Err`.
  block: # empty
    var t = initCowBTree(cltTypeB, skipSubBlocks = true)
    let r = t.bulkLoad(newSeq[(uint64, seq[byte])](0))
    doAssert r.isOk, "empty bulk load should succeed"
    doAssert t.committedRoot() == 0, "empty bulk load leaves no root"
    doAssert t.count == 0
    # An empty image still round-trips through the reader to zero keys.
    let loaded = loadCowBTree(t.serialize(), cltTypeB)
    doAssert loaded.isOk and loaded.value.keys().isOk
    doAssert loaded.value.keys().value.len == 0

  block: # not strictly ascending (out of order)
    var t = initCowBTree(cltTypeB, skipSubBlocks = true)
    let r = t.bulkLoad(@[(5'u64, descB(5)), (3'u64, descB(3))])
    doAssert r.isErr, "out-of-order batch must be rejected"

  block: # duplicate key
    var t = initCowBTree(cltTypeB, skipSubBlocks = true)
    let r = t.bulkLoad(@[(3'u64, descB(3)), (3'u64, descB(4))])
    doAssert r.isErr, "duplicate key must be rejected"

  block: # wrong descriptor width
    var t = initCowBTree(cltTypeB, skipSubBlocks = true)
    let r = t.bulkLoad(@[(3'u64, newSeq[byte](8))])
    doAssert r.isErr, "wrong descriptor width must be rejected"

  block: # non-fresh tree (already has a commit)
    var t = initCowBTree(cltTypeB, skipSubBlocks = true)
    doAssert t.insertAndCommit(1'u64, descB(1)).isOk
    let r = t.bulkLoad(@[(2'u64, descB(2))])
    doAssert r.isErr, "bulkLoad onto a committed tree must be rejected"
  echo "PASS: test_bulk_load_empty_and_validation"

proc test_bulk_load_speedup() =
  ## A representative-scale timing comparison: bulk vs per-key build. Asserts the
  ## bulk path is meaningfully faster (a generous lower bound so the test is not
  ## flaky on a loaded CI host) and reports the actual numbers.
  const N = 50_000
  let keys = keysOf(N)
  var entries: seq[(uint64, seq[byte])]
  for k in keys: entries.add (k, descB(k))

  let t0 = getMonoTime()
  var seqTree = initCowBTree(cltTypeB, skipSubBlocks = true)
  for k in keys:
    doAssert seqTree.insertAndCommit(k, descB(k)).isOk
  let seqMs = (getMonoTime() - t0).inMilliseconds

  let t1 = getMonoTime()
  var bulkTree = initCowBTree(cltTypeB, skipSubBlocks = true)
  doAssert bulkTree.bulkLoad(entries).isOk
  let bulkMs = (getMonoTime() - t1).inMilliseconds

  echo "  bulk-load speedup @ ", N, " keys: per-key=", seqMs, " ms, bulk=",
    bulkMs, " ms (", (if bulkMs > 0: seqMs.float / bulkMs.float else: 0.0),
    "x faster)"
  # Sanity: both produce the same logical tree.
  assertResolves(keys, seqTree.serialize(), "speedup seq")
  assertResolves(keys, bulkTree.serialize(), "speedup bulk")
  # The bulk build must be at least 2x faster at this scale (it is dramatically
  # more in practice; 2x is a robust, non-flaky lower bound).
  doAssert bulkMs * 2 <= seqMs,
    "expected bulk build >= 2x faster (per-key=" & $seqMs & "ms bulk=" & $bulkMs & "ms)"
  echo "PASS: test_bulk_load_speedup"

when isMainModule:
  test_bulk_load_matches_incremental()
  test_bulk_load_is_pure_and_byte_stable()
  test_bulk_load_empty_and_validation()
  test_bulk_load_speedup()
  echo "All bulk-load CoW B-tree tests passed."
