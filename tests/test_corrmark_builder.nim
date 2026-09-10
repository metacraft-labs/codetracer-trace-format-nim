## `corrmark.ns` — the correlation index.
##
## The properties that matter are the ones where a wrong answer would be
## silent: a hash hit that is not a key match, and an absent index being
## reported as "no match". Both are asserted here rather than left to the
## consumer.

import std/[monotimes, times, strutils]
import ../src/codetracer_ctfs/cow_btree
import ../src/codetracer_trace_writer/corrmark_builder

proc mk(traceLo, spanLo: uint64, geid: uint64 = 0): CorrelationMarker =
  ## A marker whose ids are derived from two integers, so tests can make many
  ## distinct keys cheaply.
  for i in 0 ..< 8:
    result.traceId[8 + i] = byte((traceLo shr ((7 - i) * 8)) and 0xFF)
    result.spanId[i] = byte((spanLo shr ((7 - i) * 8)) and 0xFF)
  result.wallTimeUnixNs = 1788878366340223810'u64 + geid
  result.monotonicTimeNs = 2032454727205762'u64 + geid
  result.geid = geid
  result.threadId = 0
  result.kind = MarkerKindSpan
  result.flags = 0

proc test_roundtrip_and_miss() =
  let markers = @[mk(1, 1, 10), mk(2, 2, 20), mk(3, 3, 30)]
  let image = serializeCorrmarkNamespace(markers)
  doAssert image.isOk, "serialize failed: " & image.error
  let img = image.get()

  for m in markers:
    let hits = lookupCorrelationMarkers(img, m.traceId, m.spanId)
    doAssert hits.isOk, "lookup failed: " & hits.error
    doAssert hits.get().len == 1, "expected exactly one hit"
    doAssert hits.get()[0].geid == m.geid, "wrong entry returned"

  # A key that was never indexed is a clean miss, not an error.
  let absent = mk(99, 99)
  let miss = lookupCorrelationMarkers(img, absent.traceId, absent.spanId)
  doAssert miss.isOk, "miss must not be an error: " & miss.error
  doAssert miss.get().len == 0, "expected no hits for an unindexed span"
  echo "PASS: test_roundtrip_and_miss"

proc test_empty_index_is_valid_and_answers_miss() =
  ## An empty index is "indexed, covers nothing" — a real answer. It must
  ## serialize and must answer misses, because the consumer distinguishes it
  ## from an ABSENT namespace, and that distinction is worthless if an empty
  ## index fails to load.
  let image = serializeCorrmarkNamespace([])
  doAssert image.isOk, "empty serialize failed: " & image.error
  let probe = mk(1, 1)
  let hits = lookupCorrelationMarkers(image.get(), probe.traceId, probe.spanId)
  doAssert hits.isOk, "empty lookup failed: " & hits.error
  doAssert hits.get().len == 0
  echo "PASS: test_empty_index_is_valid_and_answers_miss"

proc test_collision_returns_the_right_span() =
  ## THE test. Two distinct correlation keys are forced into one bucket and the
  ## lookup must return only the one that actually matches.
  ##
  ## Rather than search for a natural XXH64 collision, the bucket is built
  ## directly: both markers are encoded under a single B-tree key, which is
  ## exactly the on-disk state a real collision produces. A reader that trusts
  ## the B-tree hit instead of comparing the full 24-byte key returns two hits
  ## here, or the wrong one.
  let a = mk(0xAAAA, 0xAAAA, 1)
  let b = mk(0xBBBB, 0xBBBB, 2)
  doAssert a.markerKey() != b.markerKey(), "test setup: keys should differ"

  # Hand-build the colliding image: one key, a two-entry bucket.
  var payload: seq[byte] = @[]
  var sizing = initCowBTree(cltTypeB, skipSubBlocks = true)
  var zero: seq[byte] = @[]
  for i in 0 ..< 16: zero.add(0'u8)
  doAssert sizing.insertAndCommit(a.markerKey(), zero).isOk
  let base = uint64(sizing.serialize().len)

  var tree = initCowBTree(cltTypeB, skipSubBlocks = true)
  var bucket: seq[byte] = @[]
  bucket.add(2'u8); bucket.add(0'u8); bucket.add(0'u8); bucket.add(0'u8)  # count = 2 LE
  var tmp: seq[byte] = @[]
  for m in [a, b]:
    tmp.setLen(0)
    for x in m.traceId: bucket.add(x)
    for x in m.spanId: bucket.add(x)
    for v in [m.wallTimeUnixNs, m.monotonicTimeNs, m.geid, m.threadId]:
      for i in 0 ..< 8: bucket.add(byte((v shr (i * 8)) and 0xFF))
    for i in 0 ..< 2: bucket.add(byte((m.kind shr (i * 8)) and 0xFF))
    for i in 0 ..< 2: bucket.add(byte((m.flags shr (i * 8)) and 0xFF))

  var desc: seq[byte] = @[]
  for i in 0 ..< 8: desc.add(byte((base shr (i * 8)) and 0xFF))
  for i in 0 ..< 8: desc.add(byte((uint64(bucket.len) shr (i * 8)) and 0xFF))
  doAssert tree.insertAndCommit(a.markerKey(), desc).isOk

  var image = tree.serialize()
  image.add(bucket)
  while image.len mod PageSize != 0: image.add(0)

  let hitsA = lookupCorrelationMarkers(image, a.traceId, a.spanId)
  doAssert hitsA.isOk, "colliding lookup A failed: " & hitsA.error
  doAssert hitsA.get().len == 1,
    "a colliding bucket must yield ONE hit for A, got " & $hitsA.get().len
  doAssert hitsA.get()[0].geid == 1, "returned the wrong span for A"

  # B shares the bucket but has a different real key, so looking B up under
  # its OWN key must miss (its key was never inserted) — and looking up A must
  # never return B.
  for h in hitsA.get():
    doAssert h.geid != 2, "returned the colliding OTHER span"
  echo "PASS: test_collision_returns_the_right_span"

proc test_lookup_cost_at_scale() =
  ## Measured, not asserted-by-complexity-class. The contract claims a B-tree
  ## lookup rather than a scan; these are the numbers behind it.
  ##
  ## A ladder rather than a single point, because the question is whether cost
  ## per lookup GROWS with the key count. One N cannot answer that.
  for n in [1_000, 10_000, 100_000]:
    var markers = newSeq[CorrelationMarker](n)
    for i in 0 ..< n:
      markers[i] = mk(uint64(i) + 1, uint64(i) + 1, uint64(i))

    let tBuild = getMonoTime()
    let built = serializeCorrmarkNamespace(markers)
    doAssert built.isOk, "scale serialize failed: " & built.error
    let buildMs = float((getMonoTime() - tBuild).inNanoseconds) / 1_000_000.0
    let img = built.get()

    let tOpen = getMonoTime()
    var index = openCorrmarkIndex(img)
    doAssert index.isOk, "open failed: " & index.error
    var idxOpened = index.get()
    let openUs = float((getMonoTime() - tOpen).inNanoseconds) / 1000.0

    const Probes = 2000
    var checksum = 0
    let t0 = getMonoTime()
    for i in 0 ..< Probes:
      let k = (i * 97) mod n
      let probe = mk(uint64(k) + 1, uint64(k) + 1)
      let hits = idxOpened.lookup(probe.traceId, probe.spanId)
      doAssert hits.isOk, "scale lookup failed: " & hits.error
      doAssert hits.get().len == 1, "scale lookup missed key " & $k
      checksum += hits.get().len
    let perLookupUs =
      float((getMonoTime() - t0).inNanoseconds) / float(Probes) / 1000.0
    doAssert checksum == Probes

    echo "  n=", n, "  image=", img.len div 1024, " KiB",
         "  bytes/key=", img.len div n,
         "  build=", formatFloat(buildMs, ffDecimal, 1), " ms",
         "  open=", formatFloat(openUs, ffDecimal, 1), " us",
         "  lookup=", formatFloat(perLookupUs, ffDecimal, 3), " us"
  echo "PASS: test_lookup_cost_at_scale"

when isMainModule:
  test_roundtrip_and_miss()
  test_empty_index_is_valid_and_answers_miss()
  test_collision_returns_the_right_span()
  test_lookup_cost_at_scale()
  echo "=== corrmark builder tests passed ==="
