{.push raises: [].}

## Builder for `corrmark.ns`, the correlation index.
##
## Records which distributed-trace spans a recording covers, so a consumer can
## answer that with a B-tree lookup instead of decoding the event stream. Built
## during recording, like `memwrites.tc` and `linehits.tc`, whose builder this
## one deliberately mirrors: `NSB1` CoW B-tree, Type-B `[offset][len]`
## descriptors into an appended payload region.
##
## Spec: `codetracer-trace-format-spec/internal-files.md`
## §"Correlation Index (`corrmark.ns`)".
## Contract: `codetracer-specs/Testing/CTFS-Correlation-Marker-Contract.md`.
##
## THE KEY IS A HASH, SO A HIT IS NOT A MATCH. The 64-bit B-tree key is
## `XXH64(trace_id_be || span_id_be)` over a 192-bit correlation key, so
## distinct spans can land on one key. Every entry therefore carries its full
## 24-byte key and the leaf value is a BUCKET; a reader must confirm the full
## key rather than trust the B-tree. `lookupCorrelationMarkers` below does
## that, and is the reference for the Rust and C# readers.

import std/[algorithm, tables]
import results
import ../codetracer_ctfs/cow_btree
import ../codetracer_ctfs/xxh64

export results

const
  CorrmarkNamespaceName* = "corrmark.ns"
    ## 11 characters — within base40's 12-character limit.

  MarkerKindSpan* = 0'u16
    ## A distributed-trace span; key is XXH64 over `trace_id_be || span_id_be`.
  MarkerKindBoundary* = 1'u16
    ## A `MarkerPayload` cross-process boundary crossing. Reserved: the
    ## discriminator is written from the start so adding this later is not a
    ## format break (contract §10).

  MarkerFlagExit* = 0x0001'u16
    ## bit 0 of `flags`: direction — enter = 0, exit = 1.

  CorrmarkEntrySize* = 16 + 8 + 8 + 8 + 8 + 8 + 2 + 2  ## 60 bytes

type
  CorrelationMarker* = object
    ## One marker, as the recorder observed it.
    traceId*: array[16, byte]   ## big-endian, wire order
    spanId*: array[8, byte]     ## big-endian, wire order
    wallTimeUnixNs*: uint64
    monotonicTimeNs*: uint64
    geid*: uint64               ## coordinate into the event stream
    threadId*: uint64
    kind*: uint16
    flags*: uint16

proc putU16LE(dst: var seq[byte], v: uint16) =
  for i in 0 ..< 2:
    dst.add(byte((v shr (i * 8)) and 0xFF))

proc putU32LE(dst: var seq[byte], v: uint32) =
  for i in 0 ..< 4:
    dst.add(byte((v shr (i * 8)) and 0xFF))

proc putU64LE(dst: var seq[byte], v: uint64) =
  for i in 0 ..< 8:
    dst.add(byte((v shr (i * 8)) and 0xFF))

proc readU16LE(data: openArray[byte], off: int): uint16 =
  for i in 0 ..< 2:
    result = result or (uint16(data[off + i]) shl (i * 8))

proc readU32LE(data: openArray[byte], off: int): uint32 =
  for i in 0 ..< 4:
    result = result or (uint32(data[off + i]) shl (i * 8))

proc readU64LE(data: openArray[byte], off: int): uint64 =
  for i in 0 ..< 8:
    result = result or (uint64(data[off + i]) shl (i * 8))

proc descriptor(offset, size: uint64): seq[byte] =
  result = @[]
  result.putU64LE(offset)
  result.putU64LE(size)

proc markerKey*(m: CorrelationMarker): uint64 =
  ## The `corrmark.ns` B-tree key for `m`.
  correlationKey(m.traceId, m.spanId)

proc encodeEntry(m: CorrelationMarker, dst: var seq[byte]) =
  for b in m.traceId: dst.add(b)
  for b in m.spanId: dst.add(b)
  dst.putU64LE(m.wallTimeUnixNs)
  dst.putU64LE(m.monotonicTimeNs)
  dst.putU64LE(m.geid)
  dst.putU64LE(m.threadId)
  dst.putU16LE(m.kind)
  dst.putU16LE(m.flags)

proc decodeEntry(data: openArray[byte], off: int): CorrelationMarker =
  for i in 0 ..< 16: result.traceId[i] = data[off + i]
  for i in 0 ..< 8: result.spanId[i] = data[off + 16 + i]
  result.wallTimeUnixNs = readU64LE(data, off + 24)
  result.monotonicTimeNs = readU64LE(data, off + 32)
  result.geid = readU64LE(data, off + 40)
  result.threadId = readU64LE(data, off + 48)
  result.kind = readU16LE(data, off + 56)
  result.flags = readU16LE(data, off + 58)

proc sameKey(a, b: CorrelationMarker): bool =
  for i in 0 ..< 16:
    if a.traceId[i] != b.traceId[i]: return false
  for i in 0 ..< 8:
    if a.spanId[i] != b.spanId[i]: return false
  true

proc cmpMarkers(a, b: CorrelationMarker): int =
  for i in 0 ..< 16:
    if a.traceId[i] != b.traceId[i]: return cmp(a.traceId[i], b.traceId[i])
  for i in 0 ..< 8:
    if a.spanId[i] != b.spanId[i]: return cmp(a.spanId[i], b.spanId[i])
  cmp(a.geid, b.geid)

proc serializeCorrmarkNamespace*(markers: openArray[CorrelationMarker]):
    Result[seq[byte], string] =
  ## Build the `corrmark.ns` namespace image.
  ##
  ## Empty input yields an empty `NSB1` namespace, so a recorder that always
  ## creates the namespace still emits a valid image — and, per the contract,
  ## an EMPTY index is a meaningful answer ("indexed, covers nothing"), quite
  ## different from the namespace being absent ("never indexed").
  var byKey = initTable[uint64, seq[CorrelationMarker]]()
  var keys: seq[uint64] = @[]
  for m in markers:
    let k = m.markerKey()
    if not byKey.hasKey(k):
      keys.add(k)
    byKey.mgetOrPut(k, @[]).add(m)
  keys.sort()

  # Two passes: the first sizes the tree so the payload offsets written by the
  # second are correct.
  #
  # BULK LOAD, not per-key insert. `keys` is already sorted and de-duplicated
  # (duplicates became bucket entries above), which is exactly `bulkLoad`'s
  # contract, and it builds the tree bottom-up in one pass with a single
  # commit. The per-key `insertAndCommit` this started as is copy-on-write:
  # it publishes a commit per key and copies the path down on each one, which
  # measured 7.6 MB of image for 1000 keys — about two pages per key of
  # superseded versions — against 32 KB for the same tree bulk-loaded. A
  # correlation index is written once at close over a known set, so the
  # constructor form is the right one.
  var sizingTree = initCowBTree(cltTypeB, skipSubBlocks = true)
  let zeroDesc = descriptor(0, 0)
  var sizingEntries: seq[(uint64, seq[byte])] = @[]
  for key in keys:
    sizingEntries.add((key, zeroDesc))
  discard ?sizingTree.bulkLoad(sizingEntries)
  let payloadBase = uint64(sizingTree.serialize().len)

  var payload: seq[byte] = @[]
  var finalEntries: seq[(uint64, seq[byte])] = @[]
  for key in keys:
    var bucket = byKey.getOrDefault(key)
    bucket.sort(cmpMarkers)
    let off = payloadBase + uint64(payload.len)
    let before = payload.len
    payload.putU32LE(uint32(bucket.len))
    for m in bucket:
      m.encodeEntry(payload)
    finalEntries.add((key, descriptor(off, uint64(payload.len - before))))

  var finalTree = initCowBTree(cltTypeB, skipSubBlocks = true)
  discard ?finalTree.bulkLoad(finalEntries)
  var image = finalTree.serialize()
  image.add(payload)
  while image.len mod PageSize != 0:
    image.add(0)
  ok(image)

type
  CorrmarkIndex* = object
    ## An opened `corrmark.ns` image.
    ##
    ## OPEN ONCE, LOOK UP MANY TIMES. Parsing the namespace image costs
    ## O(image); doing it per lookup makes every query O(image) and throws away
    ## the entire reason this index exists. The first draft of this module
    ## folded the two together, and 2000 probes against a 1000-key index did not
    ## finish — the B-tree was doing its job and the API around it was not.
    tree: CowBTree
    image: seq[byte]

proc openCorrmarkIndex*(image: openArray[byte]): Result[CorrmarkIndex, string] =
  ## Parse a `corrmark.ns` image once, ready for repeated lookups.
  var idx = CorrmarkIndex(image: @image)
  idx.tree = ?loadCowBTree(image, cltTypeB)
  ok(idx)

proc lookup*(idx: var CorrmarkIndex,
             traceIdBe: openArray[byte],
             spanIdBe: openArray[byte]):
    Result[seq[CorrelationMarker], string] =
  ## Resolve a correlation key.
  ##
  ## Returns an empty seq for "indexed, and this recording does not cover that
  ## span" — which callers MUST NOT conflate with the namespace being absent
  ## from the container. The full 24-byte key is compared for every bucket
  ## entry, so a hash collision costs one comparison rather than returning
  ## another span's recording.
  let key = correlationKey(traceIdBe, spanIdBe)
  let descRes = idx.tree.lookup(key)
  if descRes.isErr:
    return ok(@[])  # no such key — a legitimate miss
  let desc = descRes.get()
  if desc.len < 16:
    return err("corrmark.ns: short descriptor")
  let off = int(readU64LE(desc, 0))
  let size = int(readU64LE(desc, 8))
  if size < 4 or off + size > idx.image.len:
    return err("corrmark.ns: descriptor out of range")

  var probe: CorrelationMarker
  let tn = min(traceIdBe.len, 16)
  for i in 0 ..< tn: probe.traceId[i] = traceIdBe[i]
  let sn = min(spanIdBe.len, 8)
  for i in 0 ..< sn: probe.spanId[i] = spanIdBe[i]

  let count = int(readU32LE(idx.image, off))
  if 4 + count * CorrmarkEntrySize > size:
    return err("corrmark.ns: bucket overruns its descriptor")
  var hits: seq[CorrelationMarker] = @[]
  for i in 0 ..< count:
    let m = decodeEntry(idx.image, off + 4 + i * CorrmarkEntrySize)
    if sameKey(m, probe):
      hits.add(m)
  ok(hits)

proc lookupCorrelationMarkers*(image: openArray[byte],
                               traceIdBe: openArray[byte],
                               spanIdBe: openArray[byte]):
    Result[seq[CorrelationMarker], string] =
  ## One-shot convenience: open and query. For a SINGLE lookup only — use
  ## `openCorrmarkIndex` + `lookup` for anything repeated.
  var idx = ?openCorrmarkIndex(image)
  idx.lookup(traceIdBe, spanIdBe)
