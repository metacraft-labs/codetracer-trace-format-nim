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

export results

const
  CompactMemberCountOffset* = V6HeaderSize
    ## Offset of the u32 LE member count: 24, immediately after the header.
  CompactDirectoryOffset* = V6HeaderSize + 4
    ## Offset of the first directory entry: 28.
  CompactDirEntrySize* = 24
    ## `(name: u64, offset: u64, length: u64)` — the same 24 bytes a
    ## `FileEntry` occupies, carrying a different three fields.
  CompactDirNameOffset* = 0
  CompactDirOffsetOffset* = 8
  CompactDirLengthOffset* = 16
  CompactEmptySize* = CompactDirectoryOffset
    ## The size of a compact container with no members: 28 bytes.

type
  CompactMember* = object
    ## One member of a compact container. `name` is the decoded §3 name and
    ## `encodedName` the `u64` the directory carries; they are kept together so
    ## a round-trip can assert on the packing as well as on the string.
    name*: string
    encodedName*: uint64
    payload*: seq[byte]

  CompactDirEntry* = object
    name*: string
    encodedName*: uint64
    offset*: uint64
    length*: uint64

  CompactDirectory* = object
    ## A validated directory. Constructing one of these is the only way to get
    ## at a member, so every read goes through §1d's six checks.
    entries*: seq[CompactDirEntry]
    size*: uint64  ## the container image's length, as checked

proc compactDirEntryOffset*(index: int): int =
  ## Byte offset of directory entry `index` from the start of the image.
  CompactDirectoryOffset + index * CompactDirEntrySize

proc compactContainerSize*(members: openArray[CompactMember]): uint64 =
  ## `ctfs-container.md` §1d: `Size = 28 + 24*N + sum(length)`. This is the
  ## layout's whole claim, so the encoder and the measurement tooling compute it
  ## from one place.
  var total = uint64(CompactDirectoryOffset) +
    uint64(members.len) * uint64(CompactDirEntrySize)
  for m in members:
    total += uint64(m.payload.len)
  total

proc nameIsWellFormed*(encoded: uint64): bool =
  ## §1d check 5: a name is non-zero and round-trips through §3's packing.
  ##
  ## Two things this refuses that a bare `base40Decode` does not. A `u64` at or
  ## above `40^12` names nothing — the 12 base-40 digits cannot represent it —
  ## and re-encoding the 12 digits it does carry does not reproduce it. And a
  ## packing with a padding digit before a non-padding one decodes to a string
  ## with an embedded NUL, which §3's encoder cannot produce and which DOES
  ## re-encode to itself (the encoder maps anything outside the alphabet to
  ## index 0), so the round-trip alone misses it and `base40Encodable` is what
  ## catches it.
  if encoded == 0'u64:
    return false
  let decoded = base40Decode(encoded)
  base40Encodable(decoded) and base40Encode(decoded) == encoded

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

proc readCompactDirectory*(data: openArray[byte],
    bodyReconstructed = false): Result[CompactDirectory, string] =
  ## Parse and VALIDATE the directory of a compact container, applying all six
  ## of `ctfs-container.md` §1d's checks and naming the offending value.
  ##
  ## The header gate comes first and comes through the §1c parsers, so a
  ## container that is not a version-6 compact one is refused here rather than
  ## read as a directory that is not one.
  ##
  ## `bodyReconstructed` exists because of a subtlety in §1a that is easy to get
  ## backwards, and this function did get it backwards once. A reader of a
  ## container under a whole-file scheme reconstructs the image as
  ## `header || decompress(rest)`, and the reconstructed image KEEPS the
  ## original 24-byte header — so it still declares its scheme. A decoder that
  ## refused any container declaring `wfcZstd` would therefore refuse the
  ## legitimately reconstructed image as well as the stored one, which is the
  ## opposite of the intended safety. So the field is not the gate: the caller
  ## states whether it has done the reconstruction, and the DEFAULT is that it
  ## has not, so handing this function stored compressed bytes is still a
  ## refusal that names the reason rather than a directory read out of a
  ## compressed body.
  if not hasCtfsMagic(data):
    return err("not a CTFS container: the first five bytes are not the magic")
  let profile = ?readCtfsProfile(data)
  if profile != cpCompact:
    return err("container declares profile " & $profile &
      ", not compact: ctfs-container.md §1d describes the compact body only")
  ?checkV6Reserved(data)
  # Parsed unconditionally, because an UNKNOWN scheme is §1c's refusal whether
  # or not the caller claims to have reconstructed anything.
  let scheme = ?readWholeFileCompression(data)
  if scheme != wfcNone and not bodyReconstructed:
    return err("compact container declares whole-file compression scheme " &
      $scheme & ": its body must be reconstructed as header || " &
      "decompress(rest) before the directory is read (ctfs-container.md §1a), " &
      "and the caller says it has not been")

  let blockSize = readU32LE(data, 8)
  if blockSize != 0'u32:
    return err("compact container declares BlockSize " & $blockSize &
      ", not 0: §1d requires 0 because the profile has no blocks, and a " &
      "block size in a layout with no blocks is a second spelling of one state")
  let maxRootEntries = readU32LE(data, 12)
  if maxRootEntries != 0'u32:
    return err("compact container declares MaxRootEntries " &
      $maxRootEntries & ", not 0: §1d requires 0 because there is no " &
      "FileEntry array for a maximum to bound")
  let maxShards = readMaxShards(data)
  if maxShards != 0'u8:
    return err("compact container declares MaxShards " & $maxShards &
      ", not 0: §1a requires 0 because the profile has no block-number space " &
      "to partition")

  if data.len < CompactDirectoryOffset:
    return err("compact container is " & $data.len & " bytes, short of the " &
      $CompactDirectoryOffset & " a header and member count occupy")

  let count = readU32LE(data, CompactMemberCountOffset)
  # §1d check 1: the directory itself fits.
  let dirEnd = uint64(CompactDirectoryOffset) +
    uint64(count) * uint64(CompactDirEntrySize)
  if dirEnd > uint64(data.len):
    return err("compact container declares " & $count & " members, whose " &
      "directory would end at byte " & $dirEnd & " of a " & $data.len &
      "-byte container (§1d check 1)")

  var dir = CompactDirectory(size: uint64(data.len))
  var expected = dirEnd  # §1d check 2: the first member starts right here.
  for i in 0 ..< int(count):
    let e = compactDirEntryOffset(i)
    let encoded = readU64LE(data, e + CompactDirNameOffset)
    let offset = readU64LE(data, e + CompactDirOffsetOffset)
    let length = readU64LE(data, e + CompactDirLengthOffset)

    # §1d check 5.
    if not nameIsWellFormed(encoded):
      return err("compact directory entry " & $i & " carries name word " &
        $encoded & ", which does not round-trip through the base40 packing " &
        "of ctfs-container.md §3 (§1d check 5)")
    let name = base40Decode(encoded)

    # §1d checks 2 and 3, as one: the member begins where its predecessor
    # ended, and the first begins where the directory ended.
    if offset != expected:
      return err("compact directory entry " & $i & " ('" & name &
        "') declares offset " & $offset & " but the members are contiguous " &
        "and the previous one ended at " & $expected &
        " (§1d check " & (if i == 0: "2" else: "3") &
        "): a gap would be padding and an overlap or a jump would serve a " &
        "shifted member")
    if length > uint64(data.len) or offset + length > uint64(data.len):
      return err("compact directory entry " & $i & " ('" & name &
        "') declares " & $length & " bytes at offset " & $offset &
        ", past the end of a " & $data.len & "-byte container")

    # §1d check 6.
    for prev in dir.entries:
      if prev.encodedName == encoded:
        return err("compact directory names '" & name &
          "' twice, at entries " & $i & " and earlier (§1d check 6)")

    dir.entries.add(CompactDirEntry(name: name, encodedName: encoded,
                                    offset: offset, length: length))
    expected = offset + length

  # §1d check 4: nothing follows the last member. This is the check that makes
  # "no padding" an assertion against the bytes rather than a restatement of
  # the encoder's intent, and it is also what refuses a truncated container
  # whose directory happens to be intact.
  if expected != uint64(data.len):
    return err("compact container is " & $data.len &
      " bytes but its " & $count & " members end at " & $expected &
      ": §1d requires Size = 28 + 24*N + sum(length), so the " &
      $(int64(data.len) - int64(expected)) &
      "-byte difference is padding or truncation (§1d check 4)")

  ok(dir)

proc findCompactMember*(dir: CompactDirectory, name: string): int =
  ## Index of `name` in a validated directory, or -1. A linear search over one
  ## `u64` per member: §1d states the directory is NOT sorted, so this is the
  ## only correct lookup.
  if not base40Encodable(name):
    return -1
  let encoded = base40Encode(name)
  for i, e in dir.entries.pairs:
    if e.encodedName == encoded:
      return i
  -1

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
