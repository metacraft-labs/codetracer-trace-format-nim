## The `NSB1` namespace image and the two members built on it, `linehits.tc`
## and `corrmark.ns`, refuse malformed input by name instead of reading out of
## bounds, looping, or answering a lookup the bytes cannot support.
##
## Each case starts from a well-formed image the production builders wrote
## and damages one field, so a case fails only on the check it names. No
## mocks: the images are the builders' own output and the readers are the
## production readers.

import std/strutils
import ../src/codetracer_ctfs/cow_btree
import ../src/codetracer_trace_writer/varint
import ../src/codetracer_trace_writer/linehits_builder
import ../src/codetracer_trace_writer/linehits_reader
import ../src/codetracer_trace_writer/corrmark_builder

proc putU64(img: var seq[byte], off: int, v: uint64) =
  for i in 0 ..< 8: img[off + i] = byte((v shr (8 * i)) and 0xFF)

proc putU16(img: var seq[byte], off: int, v: uint16) =
  img[off] = byte(v and 0xFF)
  img[off + 1] = byte(v shr 8)

proc getU64(img: openArray[byte], off: int): uint64 =
  for i in 0 ..< 8: result = result or (uint64(img[off + i]) shl (8 * i))

proc desc(off, len: uint64): seq[byte] =
  result = newSeq[byte](16)
  result.putU64(0, off)
  result.putU64(8, len)

proc twoLevelTree(): seq[byte] =
  ## 400 Type-B keys: three leaves under one internal root.
  var t = initCowBTree(cltTypeB, skipSubBlocks = true)
  var entries: seq[(uint64, seq[byte])]
  for k in 0'u64 ..< 400'u64:
    entries.add((k * 2, desc(k, 0)))
  doAssert t.bulkLoad(entries).isOk
  t.serialize()

proc refuses(img: openArray[byte], fragment: string, what: string) =
  let r = loadCowBTree(img, cltTypeB)
  doAssert r.isErr, what & ": accepted"
  doAssert fragment in r.error, what & ": refused as '" & r.error &
    "', expected it to mention '" & fragment & "'"

const
  Root0 = 4
  Flags = 36
  PageCountOff = 53
  Page = 4096

proc test_a_well_formed_image_loads() =
  let r = loadCowBTree(twoLevelTree(), cltTypeB)
  doAssert r.isOk, r.error
  doAssert r.get().count() == 400
  echo "PASS: test_a_well_formed_image_loads"

proc test_header_refusals() =
  let good = twoLevelTree()
  refuses(good[0 ..< 100], "shorter than its header page", "short image")
  var bad = good
  bad[3] = byte('2')
  refuses(bad, "magic", "bad magic")
  refuses(good & @[0'u8], "page-aligned", "ragged image")
  bad = good
  bad[Flags] = 0b110
  refuses(bad, "unknown flag bits", "unknown flags")
  bad = good
  bad[Flags] = 0b10
  refuses(bad, "leaf type", "leaf type A where B is expected")
  bad = good
  bad.putU64(PageCountOff, 99)
  refuses(bad, "page_count", "page_count past the image")
  echo "PASS: test_header_refusals"

proc test_root_and_child_page_refusals() =
  let good = twoLevelTree()
  let root = int(getU64(good, Root0))
  var bad = good
  bad.putU64(Root0, 50)
  refuses(bad, "outside the tree", "root past page_count")
  # The root is the internal node; its first child pointer follows its keys.
  let count = int(good[root * Page + 2]) or (int(good[root * Page + 3]) shl 8)
  let child0 = root * Page + 8 + count * 8
  bad = good
  bad.putU64(child0, 0)
  refuses(bad, "outside the tree", "child page 0")
  bad = good
  bad.putU64(child0, uint64(root))
  refuses(bad, "reached twice", "child that is its own parent")
  bad = good
  bad.putU64(child0 + 8, getU64(good, child0))
  refuses(bad, "reached twice", "two children sharing a page")
  echo "PASS: test_root_and_child_page_refusals"

proc test_node_refusals() =
  let good = twoLevelTree()
  let leaf = 1 * Page
  var bad = good
  bad[leaf] = 7
  refuses(bad, "node kind", "unknown node kind")
  bad = good
  bad[leaf + 5] = 1
  refuses(bad, "reserved header bytes", "reserved byte set")
  bad = good
  bad.putU16(leaf + 2, 0)
  refuses(bad, "holds no keys", "empty node")
  bad = good
  bad.putU16(leaf + 2, 1000)
  refuses(bad, "more than a page holds", "count past the page")
  bad = good
  bad.putU64(leaf + 8 + 8, 0)
  refuses(bad, "do not ascend", "keys out of order")
  bad = good
  # The first leaf's last key raised past the separator that bounds it.
  let lastKey = leaf + 8 + 169 * 8
  bad.putU64(lastKey, 100_000)
  refuses(bad, "outside the range", "key past its parent's separator")
  echo "PASS: test_node_refusals"

proc test_uneven_leaf_depth_is_refused() =
  ## Root -> [leaf, leaf, E], E -> [leaf, N]: the third leaf is moved one
  ## level down under a new internal node E beside a new leaf N whose key is
  ## above every other, so every key stays in its range and only the depth is
  ## wrong.
  var img = twoLevelTree()
  let root = int(getU64(img, Root0))
  let count = 2
  let pageCount = int(getU64(img, PageCountOff))
  let e = pageCount
  let n = pageCount + 1
  img.setLen((pageCount + 2) * Page)
  img.putU64(PageCountOff, uint64(pageCount + 2))
  let lastChildSlot = root * Page + 8 + count * 8 + count * 8
  let third = getU64(img, lastChildSlot)
  img[n * Page] = 1
  img.putU16(n * Page + 2, 1)
  img.putU64(n * Page + 8, 100_000)
  img[e * Page] = 0
  img.putU16(e * Page + 2, 1)
  img.putU64(e * Page + 8, 100_000)
  img.putU64(e * Page + 16, third)
  img.putU64(e * Page + 24, uint64(n))
  img.putU64(lastChildSlot, uint64(e))
  refuses(img, "depths", "leaves at two depths")
  echo "PASS: test_uneven_leaf_depth_is_refused"

proc lineHitsImage(): seq[byte] =
  var b = initLinehitsBuilder()
  b.recordHit(5, 0)
  b.recordHit(5, 300)
  b.recordHit(9, 1)
  doAssert b.finalize().isOk
  b.serializeCowNamespace().get()

proc descOffsetOfFirstKey(img: openArray[byte]): int =
  ## The descriptor of key 0 of the single leaf (page 1) of a one-leaf tree.
  let count = int(img[Page + 2]) or (int(img[Page + 3]) shl 8)
  Page + 8 + count * 8

proc test_line_hit_payload_refusals() =
  let good = lineHitsImage()
  let r = openLinehitsImage(good)
  doAssert r.isOk, r.error
  doAssert r.get().hits(5).get() == @[0'u64, 300]
  let d = descOffsetOfFirstKey(good)
  var bad = good
  bad.putU64(d, uint64(good.len))
  var reader = openLinehitsImage(bad).get()
  doAssert reader.hits(5).isErr and "out of bounds" in reader.hits(5).error
  bad = good
  bad.putU64(d, high(uint64) - 2)
  reader = openLinehitsImage(bad).get()
  doAssert reader.hits(5).isErr and "out of bounds" in reader.hits(5).error
  bad = good
  # Shorten key 5's list by one byte: the varint for 300 is two bytes, so
  # its last byte falls outside the descriptor.
  bad.putU64(d + 8, getU64(good, d + 8) - 1)
  reader = openLinehitsImage(bad).get()
  let h = reader.hits(5)
  doAssert h.isErr, "a varint running past its descriptor was accepted: " &
    $h.get()
  echo "PASS: test_line_hit_payload_refusals"

proc test_correlation_bucket_refusals() =
  var m: CorrelationMarker
  m.traceId[15] = 1
  m.spanId[7] = 2
  let good = serializeCorrmarkNamespace([m]).get()
  var idx = openCorrmarkIndex(good).get()
  doAssert idx.lookup(m.traceId, m.spanId).get().len == 1
  let d = descOffsetOfFirstKey(good)
  var bad = good
  bad.putU64(d, high(uint64) - 2)
  idx = openCorrmarkIndex(bad).get()
  doAssert idx.lookup(m.traceId, m.spanId).isErr
  doAssert idx.allEntries().isErr
  bad = good
  bad.putU64(d + 8, getU64(good, d + 8) - 1)
  idx = openCorrmarkIndex(bad).get()
  let r = idx.lookup(m.traceId, m.spanId)
  doAssert r.isErr and "bucket" in r.error, "truncated bucket accepted"
  bad = good
  bad.putU64(d + 8, getU64(good, d + 8) + 1)
  idx = openCorrmarkIndex(bad).get()
  doAssert idx.lookup(m.traceId, m.spanId).isErr,
    "a descriptor longer than its bucket was accepted"
  bad = good
  let bucket = int(getU64(good, d))
  bad[bucket] = 2
  idx = openCorrmarkIndex(bad).get()
  doAssert idx.lookup(m.traceId, m.spanId).isErr, "count past the bucket"
  echo "PASS: test_correlation_bucket_refusals"

when isMainModule:
  test_a_well_formed_image_loads()
  test_header_refusals()
  test_root_and_child_page_refusals()
  test_node_refusals()
  test_uneven_leaf_depth_is_refused()
  test_line_hit_payload_refusals()
  test_correlation_bucket_refusals()
  echo "=== namespace image refusal tests passed ==="
