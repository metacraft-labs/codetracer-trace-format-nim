{.push raises: [].}

## M3b cross-read fixture generator.
##
## Writes a real CoW namespace B-tree page image (produced by the production
## `cow_btree` writer: path-copying CoW inserts + double-buffered atomic root
## commits, including node splits and an in-place key UPDATE) to a binary file,
## plus a sidecar text manifest of the `(key, descriptor-hex)` pairs the image
## should resolve to via the highest-valid-commit-id root.
##
## The companion Rust reader test
## (`codetracer/src/db-backend/tests/cow_namespace_crossread_test.rs` and the
## reader `ctfs_trace_reader::cow_namespace_reader`) parses the NamespaceHeader,
## selects the published root, traverses the immutable page graph, and asserts
## every `(key → descriptor)` matches the sidecar — the load-bearing proof that
## the Nim CoW writer's on-disk page format is byte-compatible with the Rust
## reader, AND that the Rust reader selects the right double-buffered root.
##
## Usage: `gen_cow_btree_crossread_fixture <out.cowbt> <out.manifest>`.

import std/[os, strutils]
import results
import codetracer_ctfs/cow_btree

proc fail(msg: string) {.raises: [].} =
  try:
    stderr.writeLine("gen_cow_btree_crossread_fixture: " & msg)
  except IOError, ValueError:
    discard
  quit(1)

proc descA(key: uint64): seq[byte] =
  ## Deterministic 8-byte descriptor: key in LE.
  result = newSeq[byte](8)
  var k = key
  for i in 0 ..< 8:
    result[i] = byte(k and 0xFF)
    k = k shr 8

proc main() {.raises: [].} =
  let args = commandLineParams()
  if args.len < 2:
    fail("usage: gen_cow_btree_crossread_fixture <out.cowbt> <out.manifest>")
  let outImage = args[0]
  let outManifest = args[1]

  var t = initCowBTree(cltTypeA)

  # Insert enough keys to force a leaf split and a multi-level tree (Type A leaf
  # order is (4096-8)/16 = 255), so the Rust reader must traverse a real
  # internal→leaf page graph. 300 keeps the committed image small while still
  # exercising splits.
  const N = 300'u64
  for i in 1'u64 .. N:
    let r = t.insertAndCommit(i * 3, descA(i * 3))
    if r.isErr: fail("insert: " & r.error)
    # Reclaim superseded pages between commits (no reader is pinned, so they are
    # immediately reusable) to keep the published page image compact: fresh
    # allocations pop reclaimed pages from the free list instead of growing the
    # buffer. This exercises the whole-block reclaim path on the WRITE side too.
    discard t.reclaimPending()

  # An in-place UPDATE (same key, new descriptor) exercises the CoW update path
  # and ensures the Rust reader resolves the LATEST committed value.
  let updated = descA(0xDEADBEEF'u64)
  let ur = t.insertAndCommit(3'u64, updated)  # key 3 was inserted at i=1
  if ur.isErr: fail("update: " & ur.error)

  # Reclaim superseded pages into the free list (so the published image carries
  # a non-trivial free list — the reader must ignore free pages entirely).
  discard t.reclaimPending()

  let image = t.serialize()

  # Write the binary image.
  try:
    writeFile(outImage, image)
  except IOError, OSError:
    fail("write image failed: " & outImage)

  # Build the manifest of expected (key, descriptor-hex) for every live key.
  var lines: seq[string] = @[]
  for i in 1'u64 .. N:
    let key = i * 3
    let expected = if key == 3'u64: updated else: descA(key)
    var hexd = ""
    for b in expected:
      hexd.add(toHex(int(b), 2))
    lines.add($key & " " & hexd.toLowerAscii())

  try:
    writeFile(outManifest, lines.join("\n") & "\n")
  except IOError, OSError:
    fail("write manifest failed: " & outManifest)

  echo "wrote ", outImage, " (", image.len, " bytes, ",
    image.len div PageSize, " pages) and ", outManifest

main()
