{.push raises: [].}

## The CTFS **compact profile** body: the reference encoder and decoder.
##
## Spec: `codetracer-trace-format-spec/ctfs-container.md` §1d (normative for
## every offset here), with §1 / §1a / §1b / §1c for the shared version-6
## header and §3 for the name packing.
##
## ```
## Compact container (version 6, profile = 1):
##   [0 .. 23]                     ContainerHeaderV6 (24 bytes)
##   [24 .. 27]                    MemberCount, N (u32 LE)
##   [28 .. 28 + 24*N - 1]         Directory: N 24-byte (name, offset, length)
##   [28 + 24*N .. Size - 1]       The members' bytes, concatenated
##
##   Size = 28 + 24*N + sum(length)
## ```
##
## **The directory is not a block map.** A block map answers "which block holds
## byte N of this member" — what random access into a large member needs. A
## directory answers "where does this member start and how long is it" — what a
## one-shot load needs. The second is one `(u64, u64)` per member against a
## 4 KB mapping block per member, and it suffices precisely because the whole
## file is resident before the first query. So this module has no block
## arithmetic in it at all: no `blockSize`, no `blockOffset`, no mapping walk.
##
## **No alignment, deliberately.** Nothing here is padded to a block, a page or
## a word. Alignment serves ranged reads (`ctfs-container.md` design goal 5,
## "one 4 KB fetch reveals full structure") and a one-shot load issues none. A
## writer that aligned anyway would reintroduce the cost the profile exists to
## remove, and `readCompactDirectory`'s total check would refuse the result.
##
## **Why the decoder checks contiguity and the total rather than bounds.** A
## decoder that checked only `offset + length <= size` accepts a directory with
## one perturbed `Offset` and then serves a SHIFTED member, or one perturbed
## `Length` and serves a SHORT member — successfully, with nothing to indicate
## it. Checks 2–4 of §1d make a single perturbed `Offset` or `Length` field
## unrepresentable: any value breaks either contiguity with its neighbour or the
## total. That is not a checksum and does not claim to be — a flipped bit in a
## `Name` yields a different well-formed name — but it is the guarantee that
## matters: a wrong member's bytes can never be served under a right name.

import results
import ./types
import ./base40
import ./container
import ./zstd_bindings

export results
export container.CompactDirEntry, container.CompactDirectory,
  container.compactDirEntryOffset, container.nameIsWellFormed,
  container.readCompactDirectory, container.findCompactMember,
  container.CompactMemberCountOffset, container.CompactDirectoryOffset,
  container.CompactDirEntrySize, container.CompactDirNameOffset,
  container.CompactDirOffsetOffset, container.CompactDirLengthOffset,
  container.CompactEmptySize

type
  CompactMember* = object
    ## One member of a compact container. `name` is the decoded §3 name and
    ## `encodedName` the `u64` the directory carries; they are kept together so
    ## a round-trip can assert on the packing as well as on the string.
    name*: string
    encodedName*: uint64
    payload*: seq[byte]

proc compactContainerSize*(members: openArray[CompactMember]): uint64 =
  ## `ctfs-container.md` §1d: `Size = 28 + 24*N + sum(length)`. This is the
  ## layout's whole claim, so the encoder and the measurement tooling compute it
  ## from one place.
  var total = uint64(CompactDirectoryOffset) +
    uint64(members.len) * uint64(CompactDirEntrySize)
  for m in members:
    total += uint64(m.payload.len)
  total

# ---------------------------------------------------------------------------
# The encoder
# ---------------------------------------------------------------------------

proc encodeCompactContainer*(members: openArray[CompactMember],
    compression: CtfsWholeFileCompression = wfcNone,
    encryption: CtfsEncryptionMethod = emNone): Result[seq[byte], string] =
  ## Lay `members` out as a version-6 compact container (`ctfs-container.md`
  ## §1d). Member payloads are copied VERBATIM — the profile stores a member as
  ## written, and byte-exactness on the payloads is what makes the round-trip a
  ## proof rather than an equivalence.
  ##
  ## `compression` is recorded in the header only. §1b's scheme covers the image
  ## from offset 24 to the end of the STORED object, which is the transport's or
  ## the publisher's business and not this function's; what this returns is the
  ## reconstructed image a reader holds. Declaring `wfcZstd` here without the
  ## caller compressing would be a header that lies, so the caller owns both
  ## halves.
  var seen: seq[uint64]
  for m in members:
    if not base40Encodable(m.name):
      return err("member name '" & m.name & "' is not representable in the " &
        "base40 alphabet of ctfs-container.md §3 (1..12 characters from " &
        "0-9 a-z . / -): encoding it would silently collide with a shorter name")
    let encoded = base40Encode(m.name)
    if m.encodedName != 0'u64 and m.encodedName != encoded:
      return err("member '" & m.name & "' carries encodedName " &
        $m.encodedName & " but its name packs to " & $encoded &
        ": the compact directory carries the same packing as FileEntry.Name " &
        "(§1d) and the two must not disagree")
    for prev in seen:
      if prev == encoded:
        return err("duplicate member name '" & m.name &
          "' in a compact container: §1d check 6 requires the N names to be " &
          "distinct, and the full profile refuses duplicates too")
    seen.add(encoded)

  let total = compactContainerSize(members)
  if total > uint64(high(int)):
    return err("compact container would be " & $total &
      " bytes, past what this platform can address")
  var image = newSeq[byte](int(total))

  for i in 0 ..< 5:
    image[i] = CtfsMagic[i]
  image[5] = CtfsVersionV6
  image[6] = uint8(ord(encryption))
  image[7] = 0'u8
    # §1a: a compact container MUST write max_shards = 0 — it has no block
    # number space to partition, so "one shard" would be a second spelling of
    # "no sharding".
  writeU32LE(image, 8, 0'u32)
    # §1d: BlockSize = 0. There are no blocks, and writing 4096 "because it is
    # the default" would spell "there are no blocks" as a block size.
  writeU32LE(image, 12, 0'u32)
    # §1d: MaxRootEntries = 0. There is no FileEntry array to bound.
  image[V6ProfileOffset] = uint8(ord(cpCompact))
  image[V6CompressionOffset] = uint8(ord(compression))
  # bytes 18..23 stay zero: §1 makes a non-zero value there a refusal.

  writeU32LE(image, CompactMemberCountOffset, uint32(members.len))
  var payloadOff = CompactDirectoryOffset +
    members.len * CompactDirEntrySize
  for i, m in members.pairs:
    let e = compactDirEntryOffset(i)
    writeU64LE(image, e + CompactDirNameOffset, base40Encode(m.name))
    writeU64LE(image, e + CompactDirOffsetOffset, uint64(payloadOff))
    writeU64LE(image, e + CompactDirLengthOffset, uint64(m.payload.len))
    if m.payload.len > 0:
      copyMem(addr image[payloadOff], unsafeAddr m.payload[0], m.payload.len)
    payloadOff += m.payload.len

  if payloadOff != image.len:
    return err("encoder produced " & $image.len & " bytes but filled " &
      $payloadOff & ": §1d's Size identity does not hold, which would mean " &
      "the container has padding in it")
  ok(image)

# ---------------------------------------------------------------------------
# The decoder
# ---------------------------------------------------------------------------

proc compactMemberBytes*(data: openArray[byte], dir: CompactDirectory,
    name: string): Result[seq[byte], string] =
  ## A member's bytes, sliced out of a validated image.
  let idx = findCompactMember(dir, name)
  if idx < 0:
    return err("internal file not found: " & name)
  let e = dir.entries[idx]
  var out0 = newSeq[byte](int(e.length))
  if e.length > 0:
    copyMem(addr out0[0], unsafeAddr data[int(e.offset)], int(e.length))
  ok(out0)

proc decodeCompactContainer*(data: openArray[byte],
    bodyReconstructed = false): Result[seq[CompactMember], string] =
  ## Every member of a compact container, in directory order.
  let dir = ?readCompactDirectory(data, bodyReconstructed)
  var members: seq[CompactMember]
  for e in dir.entries:
    var payload = newSeq[byte](int(e.length))
    if e.length > 0:
      copyMem(addr payload[0], unsafeAddr data[int(e.offset)], int(e.length))
    members.add(CompactMember(name: e.name, encodedName: e.encodedName,
                              payload: payload))
  ok(members)

proc compactMemberNames*(data: openArray[byte],
    bodyReconstructed = false): Result[seq[string], string] =
  ## The member names a compact container declares, in directory order.
  let dir = ?readCompactDirectory(data, bodyReconstructed)
  var names: seq[string]
  for e in dir.entries:
    names.add(e.name)
  ok(names)

# ---------------------------------------------------------------------------
# The bridge from a full container
# ---------------------------------------------------------------------------

proc collectFullProfileMembers*(full: openArray[byte]):
    Result[seq[CompactMember], string] =
  ## Every member of a FULL-profile container (version 5 or a version-6 full
  ## container), read through the real reader, in root-directory order.
  ##
  ## This is what makes the encoder usable on a container that exists: the
  ## compact profile's members are a full container's members under the same
  ## names, so re-laying one out is the whole of the conversion. The member's
  ## `MapBlock` form — empty, direct, or mapped (`ctfs-container.md` §2) — is
  ## `readMemberBytes`'s business; what this walk reads out of the entry is the
  ## NAME, which §1d carries identically.
  if not hasCtfsMagic(full):
    return err("not a CTFS container: the first five bytes are not the magic")
  let profile = ?readCtfsProfile(full)
  if profile != cpFull:
    return err("container declares profile " & $profile &
      ": collectFullProfileMembers reads the full body")
  let headerSize =
    if full.len >= 6 and full[5] == CtfsVersionV6: V6HeaderSize
    else: HeaderSize + ExtHeaderSize
  if full.len < headerSize:
    return err("container is " & $full.len & " bytes, short of its " &
      $headerSize & "-byte header")

  let blockSize = readU32LE(full, 8)
  if blockSize == 0'u32:
    return err("full container declares a zero block size")
  let maxShards = readMaxShards(full)
  let rootArea = 7 * int(maxShards) * 6
  var maxEntries = readU32LE(full, 12)
  if maxEntries == 0'u32:
    maxEntries = uint32((int(blockSize) - headerSize - rootArea) div
      FileEntrySize)

  var members: seq[CompactMember]
  for i in 0 ..< int(maxEntries):
    let off = headerSize + rootArea + i * FileEntrySize
    if off + FileEntrySize > full.len:
      break
    let entrySize = readU64LE(full, off)
    let entryMap = readU64LE(full, off + 8)
    let encoded = readU64LE(full, off + 16)
    if entrySize == 0'u64 and entryMap == 0'u64 and encoded == 0'u64:
      continue  # an all-zero entry is an empty slot (§2)
    if not nameIsWellFormed(encoded):
      return err("full container's entry " & $i & " carries name word " &
        $encoded & ", which does not round-trip through the base40 packing")
    let name = base40Decode(encoded)
    let bytes = readMemberBytes(full, name, entrySize, entryMap, blockSize)
    if bytes.isErr:
      return err("reading member '" & name & "' out of the full container: " &
        bytes.error)
    members.add(CompactMember(name: name, encodedName: encoded,
                              payload: bytes.get()))
  ok(members)

# ---------------------------------------------------------------------------
# A zstd frame's content
# ---------------------------------------------------------------------------

proc appendFrameContent*(frame: openArray[byte], what: string,
    output: var seq[byte]): Result[void, string] =
  ## Append the content of the one zstd frame `frame` to `output`. The frame
  ## must declare its content size, as every frame this format writes does.
  if frame.len == 0:
    return err(what & ": an empty frame")
  let size = ZSTD_getFrameContentSize(unsafeAddr frame[0], csize_t(frame.len))
  if size == ZSTD_CONTENTSIZE_UNKNOWN or size == ZSTD_CONTENTSIZE_ERROR:
    return err(what & ": the frame does not declare its content size")
  let start = output.len
  output.setLenUninit(start + int(size))
  if size > 0:
    let got = zstdDecompressShared(addr output[start], csize_t(size),
      unsafeAddr frame[0], csize_t(frame.len))
    if ZSTD_isError(got) != 0 or int(got) != int(size):
      return err(what & ": the frame does not decode to its declared " &
        $size & " bytes")
  ok()

# ---------------------------------------------------------------------------
# Whole-file compression (§1a, §1b)
# ---------------------------------------------------------------------------

proc reconstructImage*(stored: openArray[byte]): Result[seq[byte], string] =
  ## The container image a stored object holds: `header || decompress(rest)`
  ## for a version-6 container under the zstd whole-file scheme
  ## (`ctfs-container.md` §1a), the bytes as they are otherwise. The image
  ## keeps the header, which still declares the scheme it was stored under.
  let scheme = ? readWholeFileCompression(stored)
  if scheme == wfcNone:
    return ok(@stored)
  if stored.len <= V6HeaderSize:
    return err("a container under whole-file zstd carries no body after its " &
      "header")
  var image = @(stored.toOpenArray(0, V6HeaderSize - 1))
  ? appendFrameContent(stored.toOpenArray(V6HeaderSize, stored.len - 1),
    "the whole-file zstd body", image)
  ok(image)

proc compressImage*(image: openArray[byte], level = 3):
    Result[seq[byte], string] =
  ## A version-6 container image stored under the zstd whole-file scheme: its
  ## header with the scheme declared, and the rest as one zstd frame that
  ## declares its content size. `reconstructImage` undoes it.
  if image.len < V6HeaderSize or image[5] != CtfsVersionV6:
    return err("whole-file compression is declared in a version-6 header, " &
      "and this image has none")
  let bodyLen = image.len - V6HeaderSize
  var stored = newSeqUninit[byte](V6HeaderSize +
    int(ZSTD_compressBound(csize_t(bodyLen))))
  copyMem(addr stored[0], unsafeAddr image[0], V6HeaderSize)
  stored[V6CompressionOffset] = uint8(ord(wfcZstd))
  let n = ZSTD_compress(addr stored[V6HeaderSize],
    csize_t(stored.len - V6HeaderSize),
    if bodyLen > 0: unsafeAddr image[V6HeaderSize] else: nil, csize_t(bodyLen),
    cint(level))
  if ZSTD_isError(n) != 0:
    return err("whole-file zstd compression failed: " & $ZSTD_getErrorName(n))
  stored.setLen(V6HeaderSize + int(n))
  ok(stored)

