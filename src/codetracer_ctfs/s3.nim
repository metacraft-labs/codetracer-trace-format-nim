when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## S3, the static bucketized hash (`ctfs-keyed-families.md` §5.8.5; F5).
##
## One member written once, after its last record exists, and published by
## one root-entry publication. The member is exactly `nb` buckets of 4096
## bytes (the production form: no header, `nb` is its `Size div 4096`), with
## `nb = max(1, ceil(n / (511 * 0.9)))` for `n` entries.
##
## Bucket: `count: u16` at 0 (at most 511), `overflow: u8` at 2 (1 when a
## probe passed this bucket full), entries at `8 + 8·i`: `fp: u32 = h(key) >>
## 32`, `ref: u32`, a record number.
##
## Built in ascending `(key, ref)` order, so the image is a function of its
## entries (G12): the production form. (The benchmark inserted in hash-map
## iteration order, which is not deterministic.) Each entry goes into bucket
## `h(key) mod nb`, probing to `(b + 1) mod nb` past full buckets and setting
## `overflow` on each one it passes. A family that can hold `2^32` records or
## more must not use S3: the build refuses, naming the member.

import std/[algorithm, math]
import results
import ./types
import ./container
import ./keyed_store
import ./live_reader
import ./h1

const
  S3BucketCapacity* = 511
  S3Load* = 0.9

proc s3BucketCount*(n: int): int =
  max(1, int(ceil(float(n) / (float(S3BucketCapacity) * S3Load))))

proc buildS3Image*(entries: openArray[tuple[key, refv: uint64]], member: string):
    Result[seq[byte], string] =
  ## The image of the entries, built in ascending `(key, ref)` order.
  var es = @entries
  for e in es:
    if e.refv >= (1'u64 shl 32):
      return err("S3 " & member & ": record " & $e.refv & " does not fit a u32 " &
        "reference; a family that can hold 2^32 records must not use S3")
  if not mutated("s3-unsorted"):
    es.sort(proc (a, b: tuple[key, refv: uint64]): int =
      if a.key != b.key: cmp(a.key, b.key) else: cmp(a.refv, b.refv))
  let nb = s3BucketCount(es.len)
  var img = newSeq[byte](nb * 4096)
  var counts = newSeq[int](nb)
  for e in es:
    let h = splitmix64(e.key)
    var b = int(h mod uint64(nb))
    var probes = 0
    while counts[b] >= S3BucketCapacity:
      img[b * 4096 + 2] = 1
      b = (b + 1) mod nb
      inc probes
      if probes > nb:
        return err("S3 " & member & ": every bucket is full")
    let o = b * 4096 + 8 + 8 * counts[b]
    writeU32LE(img, o, uint32(h shr 32))
    writeU32LE(img, o + 4, uint32(e.refv))
    inc counts[b]
  for b in 0 ..< nb:
    img[b * 4096] = byte(counts[b] and 0xff)
    img[b * 4096 + 1] = byte(counts[b] shr 8)
  ok(img)

proc planS3*(c: var Ctfs, m: var KMember, entries: openArray[tuple[key, refv: uint64]],
             p: var SealPlan): Result[void, string] =
  ## Write the image into the (empty) member, then publish its entry once.
  if m.size != 0'u64:
    return err("S3 " & m.name & " is written once; it holds " & $m.size & " bytes")
  let img = ? buildS3Image(entries, m.name)
  discard ? c.appendTo(m, img)
  p.barrier()
  p.publishEntries(c, [m])
  ok()

proc s3Candidates*(r: var LiveReader, slot: int, key: uint64): Result[seq[uint64], string] =
  ## §5.8.5, "Lookup": from `h(key) mod nb`, every `ref` whose `fp` matches,
  ## continuing while `overflow` is set, for at most `nb` buckets.
  var res: seq[uint64]
  let size = r.memberSize(slot)
  if slot < 0 or size == 0'u64: return ok(res)
  if size mod 4096 != 0:
    return err("S3 member " & r.slotName(slot) & ": " & $size & " bytes, not whole buckets")
  let nb = int(size div 4096)
  let h = splitmix64(key)
  let fp = uint32(h shr 32)
  var b = int(h mod uint64(nb))
  for _ in 0 ..< nb:
    let blk = ? r.readMember(slot, uint64(b) * 4096, 4096)
    let count = int(uint16(blk[0]) or (uint16(blk[1]) shl 8))
    if count > S3BucketCapacity:
      return err("S3 member " & r.slotName(slot) & ": bucket " & $b & " count " & $count)
    for i in 0 ..< count:
      if readU32LE(blk, 8 + 8 * i) == fp:
        res.add uint64(readU32LE(blk, 12 + 8 * i))
    if blk[2] == 0: break
    b = (b + 1) mod nb
  ok(res)
