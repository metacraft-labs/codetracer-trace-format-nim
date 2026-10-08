{.push raises: [].}

## CCP-2: the compact layout — concatenated raw streams behind a directory.
##
## Spec: `codetracer-trace-format-spec/ctfs-container.md` §1d (normative for
## every offset asserted here), with §1/§1a/§1c for the shared version-6
## header and §3 for the name packing.
##
## **What this file is for, in one sentence per arm.**
##
## 1. `test_a_compact_container_round_trips_byte_exactly` — every member of a
##    real recording survives encode-then-decode BYTE-IDENTICALLY, and the
##    member NAME set equals the full profile's for the same recording. The
##    control is a single flipped bit in the directory: it must be DETECTED,
##    not silently answered with a short or shifted member.
## 2. `test_the_compact_container_carries_no_block_map` — asserted against the
##    BYTES: no mapping block, and `Size = 28 + 24*N + sum(length)` with no
##    padding. The control is the same assertion run against a FULL container,
##    which must FAIL — otherwise the arm is restating the encoder's intent
##    rather than measuring the layout.
## 3. `measure_the_overhead_reduction` — CCP-2's deliverable 4. The reduction is
##    re-taken against a VERSION-5 baseline, because the direct-block tag
##    (container version 5) already removed the mapping block of every member
##    that fits in one block, and the Introduction's 86,016-byte mapping figure
##    is a VERSION-3 one that is largely already recovered. Measuring against
##    v3 would bank a saving that is already banked.
##
## NO MOCKS. The full container is produced by this repository's own writer
## through the real stream writers; its members are read back through the real
## `readMemberBytes`; and the compact container is produced by the reference
## encoder in `codetracer_ctfs/compact.nim` and read back by the reference
## decoder in the same module. Nothing here stands in for anything.
##
## **The base40 trap, pinned on purpose.** §3's alphabet is `0 = \0 (padding)`,
## `1..10 = '0'..'9'`, `11..36 = 'a'..'z'`, `37 = '.'`, `38 = '/'`,
## `39 = '-'`. There is NO space character and no capital letter, and index 1 is
## `'0'` rather than `'\0'`. A decoder whose table is off by one decodes
## `meta.dat` into something else and reports success. That mistake was made
## while this milestone was being written, so `test_the_known_member_names_pack`
## checks the packing against the twenty-one names of the measured container by
## name rather than trusting it.

import std/[os, strutils, algorithm]
import results
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_ctfs/compact
import codetracer_ctfs/base40
import codetracer_trace_types
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/interning_table
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/exec_stream
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/call_stream
import codetracer_trace_writer/io_event_stream

const
  TmpDir = "tmp_compact_container_layout"
  StepCount = 20_000
    ## Large enough that at least one member outgrows a block and is therefore
    ## MAPPED (`ctfs-container.md` §2), which is the read path a container of
    ## twelve 12-byte members never exercises, and large enough that the
    ## payload-to-container ratio is in the neighbourhood of the measured
    ## blockchain container's (18,148 stored bytes) rather than three orders of
    ## magnitude below it. Both are checked rather than hoped for: see
    ## `test_the_compact_container_carries_no_block_map`.
  SpecFixture = "../codetracer-trace-format-spec/fixtures/minimal_trace.ct"
    ## A real, committed version-5 container from a documented producer
    ## (`codetracer-trace-format-spec/fixtures/README.md`). The published
    ## BlockTracer container the campaign's Introduction measures
    ## (`/t/vl/3h/vl3h7u4w62wz3p4c44gpikxtbt/trace.ct`) is NOT in this
    ## workspace, so it cannot be re-measured here; this is the real
    ## version-5 container that is.

  ## The twenty-one member names of the measured Aztec container. Listed
  ## literally so the packing is checked against names rather than against
  ## itself.
  MeasuredContainerNames = [
    "events.log", "events.fmt", "meta.json", "paths.json",
    "calls.dat", "calls.idx", "steps.dat", "steps.idx",
    "values.dat", "values.idx", "events.dat", "events.idx",
    "paths.dat", "paths.off", "funcs.dat", "funcs.off",
    "types.dat", "types.off", "varnames.dat", "varnames.off",
    "meta.dat"]

proc toBytes(s: string): seq[byte] {.raises: [].} =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

# ---------------------------------------------------------------------------
# A real full container, written by this repository's own writer.
# ---------------------------------------------------------------------------

proc writeFullContainer(): seq[byte] {.raises: [].} =
  ## A version-5 full-profile container carrying a real recording: a step
  ## stream, a value stream, a call stream, an I/O event stream, the interning
  ## tables and `meta.dat`. See `StepCount` for why it is as long as it is.
  var ctfs = createCtfs()

  let metaFileRes = ctfs.addFile("meta.dat")
  doAssert metaFileRes.isOk, "addFile meta.dat: " & metaFileRes.error
  var metaFile = metaFileRes.get()
  let meta = TraceMetadata(
    recordingId: "01949fcc-7d92-7e9c-8ccc-eeeeeeeeeeee",
    program: "ccp2_compact_layout",
    args: @["--steps=" & $StepCount],
    workdir: "/home/test")
  let metaWr = ctfs.writeMetaDat(metaFile, meta,
    recorderId = "ccp2-fixture", hasStepStream = true,
    hasValueStream = true, hasIoEventStream = true)
  doAssert metaWr.isOk, "writeMetaDat: " & metaWr.error

  let tabRes = initTraceInterningTables(ctfs)
  doAssert tabRes.isOk, "initTraceInterningTables: " & tabRes.error
  var tab = tabRes.get()
  for i in 0 ..< 8:
    discard ctfs.ensurePathId(tab, "/src/module_" & $i & ".py")
  for i in 0 ..< 12:
    discard ctfs.ensureFunctionId(tab, "function_" & $i)
  for t in ["int", "str", "float", "list", "dict"]:
    discard ctfs.ensureTypeId(tab, t)
  for v in ["i", "j", "acc", "total", "name", "buffer"]:
    discard ctfs.ensureVarnameId(tab, v)

  let execRes = initExecStreamWriter(ctfs, chunkSize = 64)
  doAssert execRes.isOk, "initExecStreamWriter: " & execRes.error
  var execW = execRes.get()
  let valRes = initValueStreamWriter(ctfs)
  doAssert valRes.isOk, "initValueStreamWriter: " & valRes.error
  var valW = valRes.get()
  let callRes = initCallStreamWriter(ctfs)
  doAssert callRes.isOk, "initCallStreamWriter: " & callRes.error
  var callW = callRes.get()
  let ioRes = initIOEventStreamWriter(ctfs)
  doAssert ioRes.isOk, "initIOEventStreamWriter: " & ioRes.error
  var ioW = ioRes.get()

  for i in 0 ..< StepCount:
    var ev: StepEvent
    if i mod 64 == 0:
      ev = StepEvent(kind: sekAbsoluteStep, globalLineIndex: uint64(i))
    else:
      ev = StepEvent(kind: sekDeltaStep, lineDelta: 1)
    doAssert ctfs.writeEvent(execW, ev).isOk, "writeEvent step " & $i
    doAssert ctfs.writeStepValues(valW, @[
      VariableValue(varnameId: uint32(i mod 6),
                    data: toBytes("value-" & $i))]).isOk,
      "writeStepValues " & $i

  for c in 0 ..< 12:
    doAssert ctfs.writeCall(callW, call_stream.CallRecord(
      functionId: uint32(c), parentCallKey: -1,
      entryStep: uint64(c * 40), exitStep: uint64(c * 40 + 39),
      depth: 0, args: @[], returnValue: @[VoidReturnMarker],
      exception: @[], children: @[])).isOk, "writeCall " & $c
  doAssert finalizeCallStream(ctfs, callW).isOk, "finalizeCallStream"

  for e in 0 ..< 20:
    doAssert ctfs.writeEvent(ioW, IOEvent(
      kind: elkWrite, stepId: uint64(e * 25),
      data: ("line " & $e & "\n").toBytes)).isOk, "writeEvent io " & $e

  doAssert ctfs.flush(execW).isOk, "flush exec"
  doAssert value_stream.flush(ctfs, valW).isOk, "flush values"
  doAssert io_event_stream.flush(ctfs, ioW).isOk, "flush io"

  result = ctfs.toBytes()
  ctfs.closeCtfs()

proc writeFixture(path: string, bytes: seq[byte]) {.raises: [].} =
  try:
    let f = open(path, fmWrite)
    if bytes.len > 0:
      discard f.writeBytes(bytes, 0, bytes.len)
    f.close()
  except CatchableError as e:
    doAssert false, "could not write " & path & ": " & e.msg
  except Defect as e:
    doAssert false, "could not write " & path & ": " & e.msg

# ---------------------------------------------------------------------------
# The full profile's own name set, walked WITHOUT the compact code path.
# ---------------------------------------------------------------------------

proc fullProfileNames(full: openArray[byte]): seq[string] {.raises: [].} =
  ## The member names of a full container, read out of its `FileEntry` array
  ## directly. Deliberately NOT `collectFullProfileMembers`: the arm below
  ## compares the compact directory's name set against the full profile's, and
  ## deriving both from one walk would compare a list with itself.
  let blockSize = readU32LE(full, 8)
  doAssert blockSize != 0'u32, "the full container declares a zero block size"
  var maxEntries = readU32LE(full, 12)
  if maxEntries == 0'u32:
    maxEntries = uint32(
      (int(blockSize) - HeaderSize - ExtHeaderSize) div FileEntrySize)
  for i in 0 ..< int(maxEntries):
    let off = HeaderSize + ExtHeaderSize + i * FileEntrySize
    if off + FileEntrySize > full.len:
      break
    let entrySize = readU64LE(full, off)
    let entryMap = readU64LE(full, off + 8)
    let encoded = readU64LE(full, off + 16)
    if entrySize == 0'u64 and entryMap == 0'u64 and encoded == 0'u64:
      continue
    result.add(base40Decode(encoded))

# ---------------------------------------------------------------------------
# The layout assertion, written so it can be run against EITHER profile.
# ---------------------------------------------------------------------------

type LayoutVerdict = object
  failures: seq[string]
  memberCount: int
  payloadBytes: uint64
  accountedBytes: uint64
  unalignedMemberOffsets: int

proc checkLayoutHasNoBlockMapAndNoPadding(
    data: openArray[byte]): LayoutVerdict {.raises: [].} =
  ## Four independent checks on RAW BYTES, each reported by name so the control
  ## below can say WHICH of them a full container fails. Nothing here consults
  ## the `Profile` byte: a check that asked the header what layout it was
  ## looking at would be reading the encoder's intent back out, which is exactly
  ## what the milestone's control forbids.
  ##
  ##   block-space      — a block map needs a block-number space, and a
  ##                      container that declares a `BlockSize` has one. §1d
  ##                      requires 0.
  ##   directory-fits   — the member count at offset 24 and the 24-byte entries
  ##                      from offset 28 must lie inside the container.
  ##   byte-coverage    — EVERY byte of the image is covered exactly once by
  ##                      (header, count, directory, members). This is the
  ##                      check that measures "no mapping block": a mapping
  ##                      block is 4,096 bytes belonging to no member, and if
  ##                      every byte belongs to a member there is nowhere for
  ##                      one to be. It subsumes "no padding", which is the
  ##                      same statement about the bytes between members.
  ##   no-4k-alignment  — at least one member begins at an offset that is not a
  ##                      multiple of 4,096. The full profile cannot satisfy
  ##                      this: every one of its members begins at a block.
  if readU32LE(data, 8) != 0'u32:
    result.failures.add("block-space: the header declares BlockSize " &
      $readU32LE(data, 8) & ", so the container has a block-number space a " &
      "mapping block could live in")

  if data.len < CompactDirectoryOffset:
    result.failures.add("directory-fits: the container is " & $data.len &
      " bytes, short of the " & $CompactDirectoryOffset &
      " a header and a member count occupy")
    return

  let count = readU32LE(data, CompactMemberCountOffset)
  result.memberCount = int(count)
  let dirEnd = uint64(CompactDirectoryOffset) +
    uint64(count) * uint64(CompactDirEntrySize)
  if dirEnd > uint64(data.len):
    result.failures.add("directory-fits: offset " &
      $CompactMemberCountOffset & " reads a member count of " & $count &
      ", whose directory would end at byte " & $dirEnd & " of a " &
      $data.len & "-byte container")
    return

  var covered = newSeq[bool](data.len)
  for i in 0 ..< int(dirEnd):
    covered[i] = true

  var doubleCovered = 0
  for i in 0 ..< int(count):
    let e = compactDirEntryOffset(i)
    let offset = readU64LE(data, e + CompactDirOffsetOffset)
    let length = readU64LE(data, e + CompactDirLengthOffset)
    result.payloadBytes += length
    if offset > uint64(data.len) or length > uint64(data.len) or
       offset + length > uint64(data.len):
      result.failures.add("byte-coverage: entry " & $i & " claims " &
        $length & " bytes at offset " & $offset & ", outside a " & $data.len &
        "-byte container")
      return
    if offset mod 4096'u64 != 0'u64:
      result.unalignedMemberOffsets += 1
    for b in int(offset) ..< int(offset + length):
      if covered[b]:
        doubleCovered += 1
      covered[b] = true

  result.accountedBytes = 0
  for b in 0 ..< data.len:
    if covered[b]:
      result.accountedBytes += 1

  if doubleCovered > 0:
    result.failures.add("byte-coverage: " & $doubleCovered &
      " bytes are claimed by more than one member or by a member and the " &
      "directory")

  var uncovered = 0
  for b in 0 ..< data.len:
    if not covered[b]:
      uncovered += 1
  if uncovered > 0:
    result.failures.add("byte-coverage: " & $uncovered & " of " & $data.len &
      " bytes (" & $((uncovered * 1000 div data.len).float / 10.0) &
      "%) belong to no member and to no directory entry — a mapping block, a " &
      "partially filled data block or padding")

  if count > 0'u32 and result.unalignedMemberOffsets == 0:
    result.failures.add("no-4k-alignment: every one of the " & $count &
      " members begins at a multiple of 4096, which is what a block-aligned " &
      "layout looks like")

type FullProfileCensus = object
  ## What a FULL container's root table says it spends. Taken from the
  ## `MapBlock` field of every entry, which `ctfs-container.md` §2 makes the
  ## authority on a member's layout ("Decide the layout from `MapBlock`, never
  ## from `Size`"). The milestone's own census argument is that the set of
  ## blocks a census identifies as mapping-like is EXACTLY the set of `MapBlock`
  ## values the root table declares, so this reads the declaration.
  members: int
  empty: int
  direct: int
  mapped: int
  mappingBlocks: int
  contentBytes: uint64

proc censusFullProfile(full: openArray[byte]): FullProfileCensus
    {.raises: [].} =
  let blockSize = readU32LE(full, 8)
  doAssert blockSize != 0'u32
  let usable = int(blockSize) div 8 - 1
  var maxEntries = readU32LE(full, 12)
  if maxEntries == 0'u32:
    maxEntries = uint32(
      (int(blockSize) - HeaderSize - ExtHeaderSize) div FileEntrySize)
  for i in 0 ..< int(maxEntries):
    let off = HeaderSize + ExtHeaderSize + i * FileEntrySize
    if off + FileEntrySize > full.len:
      break
    let entrySize = readU64LE(full, off)
    let entryMap = readU64LE(full, off + 8)
    let encoded = readU64LE(full, off + 16)
    if entrySize == 0'u64 and entryMap == 0'u64 and encoded == 0'u64:
      continue
    result.members += 1
    result.contentBytes += entrySize
    if entryMap == 0'u64:
      result.empty += 1
    elif isDirectMapBlock(entryMap):
      result.direct += 1
    else:
      result.mapped += 1
      # The mapping blocks this member owns, from the same arithmetic §4
      # uses: one level-1 block per `usable` data blocks, then a level above
      # it per `usable` of those, and so on.
      let dataBlocks = int((entrySize + uint64(blockSize) - 1) div
        uint64(blockSize))
      var level = (dataBlocks + usable - 1) div usable
      while level >= 1:
        result.mappingBlocks += level
        if level == 1:
          break
        level = (level + usable - 1) div usable

# ---------------------------------------------------------------------------
# test_the_known_member_names_pack  (deliverable 2, and its trap)
# ---------------------------------------------------------------------------

proc test_the_known_member_names_pack() {.raises: [].} =
  ## §1d carries the name as §3's base40 `u64`, bit for bit what
  ## `FileEntry.Name` carries. The alphabet has NO space character, and index 1
  ## is `'0'` and not `'\0'`; an off-by-one table decodes `meta.dat` as garbage
  ## and says nothing. So the packing is checked against the twenty-one names of
  ## the container the campaign measured, by name.
  for name in MeasuredContainerNames:
    doAssert base40Encodable(name),
      "'" & name & "' is a real member name of the measured container and " &
      "base40Encodable says it cannot be packed"
    let encoded = base40Encode(name)
    doAssert encoded != 0'u64, "'" & name & "' packed to the null name word"
    let decoded = base40Decode(encoded)
    doAssert decoded == name,
      "base40 round-trip broke on '" & name & "': packed to " & $encoded &
      " and unpacked to '" & decoded & "'. An off-by-one alphabet gives " &
      "exactly this, and `meta.dat` is the name it is noticed on"
    doAssert nameIsWellFormed(encoded),
      "'" & name & "' packs to a word the §1d check-5 validator rejects"

  # The alphabet itself, pinned at its two ends and at the characters it does
  # NOT have. `' '` and capitals are outside it, so the encoder maps them to
  # the padding index — which is why §1d routes every name through
  # `base40Encodable` first rather than through the encoder.
  doAssert base40Decode(base40Encode("0")) == "0",
    "index 1 of the alphabet is '0'; a table that put '\\0' there shifts " &
    "every digit"
  doAssert not base40Encodable("meta dat"),
    "a space is not in the base40 alphabet and must not be accepted"
  doAssert not base40Encodable("Meta.dat"),
    "the alphabet has no capital letters"

  # The two distinct hazards an out-of-alphabet character creates, and they are
  # different, which a single example would hide.
  #
  # A TRAILING one collides outright: `' '` maps to the padding index, so
  # `"meta "` is bit for bit `"meta"`. This is the collision `base40Encodable`
  # was written for, and `"snap!pages" == "snap"` in its own doc comment is an
  # overstatement of it — an INTERIOR out-of-alphabet character does not
  # collide, because the characters after it keep their positions.
  doAssert base40Encode("meta ") == base40Encode("meta"),
    "a trailing out-of-alphabet character encodes as padding, so it must " &
    "collide with the truncated name — the collision base40Encodable exists " &
    "to refuse"
  doAssert base40Encode("meta dat") != base40Encode("meta"),
    "an INTERIOR out-of-alphabet character does NOT collide with the " &
    "truncation: 'd', 'a' and 't' keep positions 5, 6 and 7"
  # An interior one instead yields a word whose name has an embedded NUL. §3's
  # encoder cannot produce that, and it re-encodes to ITSELF (the encoder maps
  # the NUL to index 0 as well), so the round-trip alone does not catch it and
  # §1d check 5's `base40Encodable` arm is what does.
  let interior = base40Encode("meta dat")
  doAssert base40Encode(base40Decode(interior)) == interior,
    "the round-trip does NOT catch an embedded-NUL name, which is why " &
    "check 5 is a round-trip AND an alphabet test"
  doAssert not nameIsWellFormed(interior),
    "§1d check 5 must refuse a name word that decodes to a string with an " &
    "embedded NUL; the round-trip alone accepts it"
  doAssert not nameIsWellFormed(0'u64),
    "the null name word names nothing and is refused (§1d check 5)"
  doAssert not nameIsWellFormed(high(uint64)),
    "a name word at or above 40^12 names nothing: the twelve base-40 digits " &
    "cannot represent it, so re-encoding them cannot reproduce it"

  echo "PASS: test_the_known_member_names_pack"

# ---------------------------------------------------------------------------
# test_a_compact_container_round_trips_byte_exactly
# ---------------------------------------------------------------------------

proc test_a_compact_container_round_trips_byte_exactly() {.raises: [].} =
  let fullBytes = writeFullContainer()
  let membersRes = collectFullProfileMembers(fullBytes)
  doAssert membersRes.isOk,
    "could not read the full container's members: " & membersRes.error
  let members = membersRes.get()
  doAssert members.len > 0, "the full container carried no members"

  let encoded = encodeCompactContainer(members)
  doAssert encoded.isOk, "encodeCompactContainer: " & encoded.error
  let compactBytes = encoded.get()

  # --- the normative offsets of §1d, asserted rather than assumed ---
  doAssert CompactMemberCountOffset == 24 and CompactDirectoryOffset == 28 and
           CompactDirEntrySize == 24,
    "§1d puts the member count at 24, the first directory entry at 28 and " &
    "makes an entry 24 bytes; these constants are the layout"
  doAssert readU32LE(compactBytes, CompactMemberCountOffset) ==
           uint32(members.len),
    "the member count at offset 24 does not match the members encoded"
  let firstPayload = CompactDirectoryOffset + members.len * CompactDirEntrySize
  doAssert readU64LE(compactBytes,
             compactDirEntryOffset(0) + CompactDirOffsetOffset) ==
           uint64(firstPayload),
    "§1d: the first member begins at 28 + 24*N = " & $firstPayload
  doAssert compactBytes.len.uint64 == compactContainerSize(members),
    "§1d: Size = 28 + 24*N + sum(length)"
  doAssert compactBytes[5] == CtfsVersionV6 and
           compactBytes[V6ProfileOffset] == uint8(ord(cpCompact)),
    "the encoder did not stamp version 6 / profile 1"

  # --- the round trip, BYTE-EXACT on the payloads ---
  let decoded = decodeCompactContainer(compactBytes)
  doAssert decoded.isOk, "decodeCompactContainer: " & decoded.error
  let back = decoded.get()
  doAssert back.len == members.len,
    "the decoder returned " & $back.len & " members for " & $members.len
  for i in 0 ..< members.len:
    doAssert back[i].name == members[i].name,
      "member " & $i & " came back as '" & back[i].name & "', not '" &
      members[i].name & "': §1d says the members are in directory order"
    doAssert back[i].encodedName == base40Encode(members[i].name),
      "member '" & members[i].name & "' came back under a different name word"
    doAssert back[i].payload.len == members[i].payload.len,
      "member '" & members[i].name & "' came back " & $back[i].payload.len &
      " bytes long, not " & $members[i].payload.len &
      " — a SHORT member, which is the failure the length field exists to " &
      "make impossible"
    for j in 0 ..< members[i].payload.len:
      doAssert back[i].payload[j] == members[i].payload[j],
        "member '" & members[i].name & "' differs at byte " & $j & ": " &
        $back[i].payload[j] & " came back for " & $members[i].payload[j] &
        ". The round trip is BYTE-EXACT, not equivalent"

  # --- and byte-exact against the FULL profile's own read path ---
  # The stronger form of the same claim: a compact decode and a full-profile
  # `readInternalFile` of the same recording return the same bytes, so the
  # conversion is lossless rather than merely self-consistent.
  let blockSize = readU32LE(fullBytes, 8)
  var maxEntries = readU32LE(fullBytes, 12)
  if maxEntries == 0'u32:
    maxEntries = uint32(
      (int(blockSize) - HeaderSize - ExtHeaderSize) div FileEntrySize)
  let dirRes = readCompactDirectory(compactBytes)
  doAssert dirRes.isOk, "readCompactDirectory: " & dirRes.error
  let dir = dirRes.get()
  for m in members:
    let viaFull = readInternalFile(fullBytes, m.name, blockSize, maxEntries)
    doAssert viaFull.isOk,
      "the full profile could not read back '" & m.name & "': " & viaFull.error
    let viaCompact = compactMemberBytes(compactBytes, dir, m.name)
    doAssert viaCompact.isOk,
      "the compact profile could not read back '" & m.name & "': " &
      viaCompact.error
    doAssert viaCompact.get() == viaFull.get(),
      "'" & m.name & "' differs between the two profiles of one recording"

  # --- the NAME SET equals the full profile's, for the same recording ---
  var compactNames = compactMemberNames(compactBytes).valueOr:
    doAssert false, "compactMemberNames: " & error
    @[]
  var fullNames = fullProfileNames(fullBytes)
  doAssert compactNames == fullNames,
    "the compact container does not name the same members in the same " &
    "order as the full container of the same recording:\n  full   = " &
    $fullNames & "\n  compact= " & $compactNames
  var sortedCompact = compactNames
  var sortedFull = fullNames
  sort(sortedCompact)
  sort(sortedFull)
  doAssert sortedCompact == sortedFull, "the name SETS differ"
  doAssert "meta.dat" in compactNames,
    "the recording has no meta.dat, so the name most likely to expose an " &
    "off-by-one base40 table is not in this comparison at all"
  echo "  members: " & $compactNames.len & " — " & $compactNames

  # -----------------------------------------------------------------------
  # THE CONTROL: a single flipped bit in the directory is DETECTED.
  #
  # "Detected" is stated precisely, because a directory is not a checksum and
  # cannot be: a flipped bit in a NAME yields a different, well-formed name,
  # which is a change a reader can see rather than a corruption it cannot. What
  # must never happen is the failure the milestone names — a decode that
  # SUCCEEDS, reports the same names, and hands back a member that is SHORT or
  # SHIFTED. So:
  #
  #   * every flip in an `Offset` or a `Length` field must be REFUSED, and
  #   * no flip anywhere in the directory may produce a successful decode whose
  #     name set is unchanged and whose payloads are not.
  # -----------------------------------------------------------------------
  var refused = 0
  var acceptedWithDifferentNames = 0
  var acceptedUnchanged = 0
  var offsetOrLengthFlips = 0
  for pos in CompactMemberCountOffset ..< firstPayload:
    # Which field of which entry is this byte in? The count is treated as part
    # of the directory: it is what says how long the directory is.
    var isOffsetOrLength = false
    if pos >= CompactDirectoryOffset:
      let within = (pos - CompactDirectoryOffset) mod CompactDirEntrySize
      isOffsetOrLength = within >= CompactDirOffsetOffset
    for bit in 0 ..< 8:
      var flipped = compactBytes
      flipped[pos] = flipped[pos] xor (1'u8 shl bit)
      if isOffsetOrLength:
        offsetOrLengthFlips += 1
      let verdict = readCompactDirectory(flipped)
      if verdict.isErr:
        refused += 1
        continue
      doAssert not isOffsetOrLength,
        "flipping bit " & $bit & " of byte " & $pos & ", which is in an " &
        "Offset or a Length field, was ACCEPTED. §1d's checks 2-4 exist so " &
        "that no value of those fields can be both accepted and wrong: a " &
        "perturbed offset serves a SHIFTED member and a perturbed length a " &
        "SHORT one, and either is a successful read of the wrong bytes"
      # The flip was accepted. Then it must have changed what the container
      # SAYS, not what it silently hands back.
      let flippedNames = compactMemberNames(flipped).valueOr:
        doAssert false, "a directory that validated would not list its names"
        @[]
      if flippedNames != compactNames:
        acceptedWithDifferentNames += 1
        continue
      acceptedUnchanged += 1
      let flippedDir = verdict.get()
      for m in members:
        let got = compactMemberBytes(flipped, flippedDir, m.name)
        doAssert got.isOk and got.get() == m.payload,
          "flipping bit " & $bit & " of byte " & $pos & " produced a decode " &
          "that SUCCEEDED, named the same members, and handed back a " &
          "different '" & m.name & "'. That is the short-or-shifted member " &
          "§1d's checks exist to make unrepresentable"

  doAssert offsetOrLengthFlips == members.len * 16 * 8,
    "the control did not reach every bit of every Offset and Length field: " &
    "visited " & $offsetOrLengthFlips & ", expected " &
    $(members.len * 16 * 8)
  doAssert refused >= offsetOrLengthFlips,
    "fewer flips were refused (" & $refused & ") than there are Offset and " &
    "Length bits (" & $offsetOrLengthFlips & "), which cannot happen if " &
    "every one of those was refused"
  doAssert acceptedUnchanged == 0,
    "no flip may leave the name set unchanged AND validate; " &
    $acceptedUnchanged & " did"
  doAssert acceptedWithDifferentNames > 0,
    "every single flip was refused, including the ones in NAME fields. That " &
    "would mean this control cannot distinguish a decoder that detects " &
    "corruption from one that refuses everything, so the counter is asserted " &
    "non-zero in both directions"
  echo "  flipped-byte control: " & $((firstPayload -
    CompactMemberCountOffset) * 8) & " single-bit flips over the " &
    $(firstPayload - CompactMemberCountOffset) & "-byte directory — " &
    $refused & " refused, " & $acceptedWithDifferentNames &
    " accepted under a CHANGED name set, 0 accepted with the name set " &
    "unchanged and a payload altered"

  # And the un-flipped container still validates, so the control above is not
  # passing because the decoder refuses everything.
  doAssert readCompactDirectory(compactBytes).isOk,
    "CONTROL FAILED: the unmodified container is refused by its own decoder"

  # -----------------------------------------------------------------------
  # A whole-file scheme is a statement about how the object was STORED, and a
  # reconstructed image still declares it.
  #
  # This pins a defect this file's first version had. §1a says a reader of a
  # container under a whole-file scheme reconstructs the image as
  # `header || decompress(rest)` — and the reconstructed image KEEPS the
  # original 24-byte header, so it still declares `wfcZstd`. The decoder's
  # first version refused any container declaring a scheme, which refuses the
  # reconstructed image along with the stored one: the two are the same byte in
  # a stored object and DIFFERENT FACTS in a reconstructed one. So the caller
  # states which it is holding, the default is "stored", and an UNKNOWN scheme
  # is §1c's refusal either way.
  # -----------------------------------------------------------------------
  let declaredZstd = encodeCompactContainer(members, wfcZstd).valueOr:
    doAssert false, "encodeCompactContainer(wfcZstd): " & error
    @[]
  doAssert declaredZstd[V6CompressionOffset] == uint8(ord(wfcZstd)),
    "the encoder did not record the scheme it was asked to declare"
  let asStored = readCompactDirectory(declaredZstd)
  doAssert asStored.isErr,
    "a container declaring a whole-file scheme was read as though its body " &
    "were in hand. The default must be that it is not: reading a directory " &
    "out of a compressed body would report a layout defect that does not exist"
  doAssert asStored.error.contains("reconstructed"),
    "the refusal does not say what the caller has to do. Got: " &
    asStored.error
  let asReconstructed = readCompactDirectory(declaredZstd,
                                             bodyReconstructed = true)
  doAssert asReconstructed.isOk,
    "a RECONSTRUCTED image was refused because its header still declares the " &
    "scheme it was stored under, which is exactly the misreading §1a warns " &
    "against: " & (if asReconstructed.isErr: asReconstructed.error else: "")
  doAssert asReconstructed.get().entries.len == members.len,
    "the reconstructed image's directory does not list every member"
  # ... and an UNKNOWN scheme is refused in BOTH, because §1c's rule is about
  # the value and not about who is holding the bytes.
  var unknownScheme = declaredZstd
  unknownScheme[V6CompressionOffset] = 9'u8
  doAssert readCompactDirectory(unknownScheme).isErr,
    "scheme byte 9 is outside §1b's closed set and was accepted"
  let unknownReconstructed = readCompactDirectory(unknownScheme,
                                                  bodyReconstructed = true)
  doAssert unknownReconstructed.isErr,
    "scheme byte 9 was accepted once the caller claimed the body was " &
    "reconstructed. `bodyReconstructed` says which bytes are in hand; it is " &
    "not permission to skip the closed-set check"
  doAssert unknownReconstructed.error.contains("9"),
    "the refusal of scheme 9 does not name it. Got: " &
    unknownReconstructed.error

  if not dirExists(TmpDir):
    try:
      createDir(TmpDir)
    except CatchableError, Defect:
      discard
  writeFixture(TmpDir / "full.ct", fullBytes)
  writeFixture(TmpDir / "compact.ct", compactBytes)

  echo "PASS: test_a_compact_container_round_trips_byte_exactly"

# ---------------------------------------------------------------------------
# test_the_compact_container_carries_no_block_map
# ---------------------------------------------------------------------------

proc test_the_compact_container_carries_no_block_map() {.raises: [].} =
  let fullBytes = writeFullContainer()
  let members = collectFullProfileMembers(fullBytes).valueOr:
    doAssert false, "collectFullProfileMembers: " & error
    @[]
  let compactBytes = encodeCompactContainer(members).valueOr:
    doAssert false, "encodeCompactContainer: " & error
    @[]

  let compact = checkLayoutHasNoBlockMapAndNoPadding(compactBytes)
  doAssert compact.failures.len == 0,
    "the compact container failed its own layout assertion:\n  " &
    compact.failures.join("\n  ")
  doAssert compact.memberCount == members.len,
    "the layout check read " & $compact.memberCount & " members, not " &
    $members.len
  doAssert compact.accountedBytes == uint64(compactBytes.len),
    "the layout check accounted for " & $compact.accountedBytes & " of " &
    $compactBytes.len & " bytes"

  # The §1d identity, recomputed from the BYTES rather than from the encoder.
  var sumLengths = 0'u64
  let count = readU32LE(compactBytes, CompactMemberCountOffset)
  for i in 0 ..< int(count):
    sumLengths += readU64LE(compactBytes,
      compactDirEntryOffset(i) + CompactDirLengthOffset)
  let identity = uint64(CompactDirectoryOffset) +
    uint64(count) * uint64(CompactDirEntrySize) + sumLengths
  doAssert identity == uint64(compactBytes.len),
    "§1d: Size = 28 + 24*N + sum(length) = " & $identity & ", but the " &
    "container is " & $compactBytes.len & " bytes. The difference is padding"
  doAssert compactBytes.len mod 4096 != 0,
    "the compact container's size is a whole number of 4096-byte blocks, " &
    "which a layout with no blocks and no padding would reach only by " &
    "coincidence — check that nothing has started rounding"

  # --- THE CONTROL: the same assertion against a FULL container must FAIL ---
  let full = checkLayoutHasNoBlockMapAndNoPadding(fullBytes)
  doAssert full.failures.len > 0,
    "CONTROL FAILED: a FULL container — which has a mapping block or a " &
    "partially filled data block for every member — passed an assertion " &
    "whose whole claim is that neither is present. The assertion would then " &
    "be restating the encoder's intent rather than measuring the layout"
  var sawBlockSpace = false
  var sawAccounting = false
  for f in full.failures:
    if f.startsWith("block-space"): sawBlockSpace = true
    if f.startsWith("byte-coverage") or f.startsWith("directory-fits"):
      sawAccounting = true
  doAssert sawBlockSpace,
    "the full container did not fail the block-space check, though it " &
    "declares a 4096-byte block size. Failures were:\n  " &
    full.failures.join("\n  ")
  doAssert sawAccounting,
    "the full container failed ONLY the block-space check, which is a single " &
    "header field. The arm has to fail on the LAYOUT too — a full " &
    "container's mapping and partly filled blocks belong to no member — or " &
    "it is measuring a byte and not a structure. Failures were:\n  " &
    full.failures.join("\n  ")
  echo "  CONTROL, the same assertion against the FULL container of the " &
    "same recording — " & $full.failures.len & " failures:"
  for f in full.failures:
    echo "    FAIL " & f

  # --- and the same claim stated as a census, so it is a quantity ---
  # A byte-accounting proof ("every byte of the compact container belongs to a
  # member or to the directory, so there is nowhere for a mapping block to be")
  # is only as interesting as the thing it excludes. So the full container of
  # the same recording is censused through its own `MapBlock` fields, and the
  # arm requires it to actually HAVE the structure the compact profile lacks.
  let census = censusFullProfile(fullBytes)
  doAssert census.members == members.len,
    "the census found " & $census.members & " members, not " & $members.len
  doAssert census.mapped > 0,
    "no member of this recording outgrew a block, so every member is DIRECT " &
    "and the full container has no mapping block at all. At container " &
    "version 5 that is normal and expected (the direct-block tag removed " &
    "them), but it also means this arm's 'no block map' claim would be " &
    "compared against a container that has none either — raise StepCount " &
    "until at least one member is mapped, so the comparison has content"
  doAssert census.mappingBlocks > 0,
    "a mapped member owns at least one mapping block (§4)"
  let blockSize = int(readU32LE(fullBytes, 8))
  echo "    census of the FULL container's own root table: " &
    $census.members & " members — " & $census.empty & " empty, " &
    $census.direct & " direct (no mapping block, version 5's tag), " &
    $census.mapped & " mapped"
  echo "    mapping blocks it declares: " & $census.mappingBlocks & " x " &
    $blockSize & " = " & $(census.mappingBlocks * blockSize) & " bytes"
  echo "    the compact container of the same recording declares 0, and has " &
    "no field that could name one: a §1d directory entry is " &
    "(name, offset, length) and every byte of the container belongs to a " &
    "member or to the directory"

  echo "PASS: test_the_compact_container_carries_no_block_map"

# ---------------------------------------------------------------------------
# measure_the_overhead_reduction  (deliverable 4)
# ---------------------------------------------------------------------------

proc predictedFullV5Size(members: openArray[CompactMember],
                         blockSize: int): int {.raises: [].} =
  ## The structural model of a version-5 full container: one root block, then
  ## per member — nothing if it is empty (`MapBlock = 0`), one data block if it
  ## fits one block (the direct-block tag, §2), and otherwise
  ## `ceil(size/blockSize)` data blocks plus the mapping blocks to address them.
  ##
  ## This is the PREDICTION. It is stated as a formula so that a disagreement
  ## with the measured container is a finding about the layout rather than a
  ## rounding error.
  let usable = blockSize div 8 - 1
  var blocks = 1  # block 0: header, free-list roots, FileEntry array
  for m in members:
    let size = m.payload.len
    if size == 0:
      continue
    if size <= blockSize:
      blocks += 1
      continue
    let dataBlocks = (size + blockSize - 1) div blockSize
    blocks += dataBlocks
    var mapping = 0
    var level = (dataBlocks + usable - 1) div usable
    while level >= 1:
      mapping += level
      if level == 1:
        break
      level = (level + usable - 1) div usable
    blocks += mapping
  blocks * blockSize

proc reportOne(label: string, fullBytes: seq[byte]) {.raises: [].} =
  ## A reader error here is a FAILURE, not a "NOT MEASURED".
  ##
  ## Both arms below used to `echo "NOT MEASURED"` and `return`, and the driver
  ## treats a returning proc as a pass — so a container this repository's own
  ## reader stopped being able to read would have turned this measurement off
  ## and left the suite green. The two states that reach here are not
  ## symmetrical: `fullBytes` is already in hand, so "absent" is impossible and
  ## every remaining outcome is "present and unreadable", which is exactly the
  ## state a skip must not be spent on.
  let members = collectFullProfileMembers(fullBytes).valueOr:
    doAssert false, label & ": the container is IN HAND and this repository's " &
      "own reader cannot read it: " & error & ". That is a finding about the " &
      "reader or the container, and reporting it as NOT MEASURED would hide " &
      "it behind a passing suite."
    return
  let compactBytes = encodeCompactContainer(members).valueOr:
    doAssert false, label & ": the members decoded and the compact encoder " &
      "then refused them: " & error
    return
  var payload = 0
  var empties = 0
  var mapped = 0
  let blockSize = int(readU32LE(fullBytes, 8))
  for m in members:
    payload += m.payload.len
    if m.payload.len == 0: empties += 1
    elif m.payload.len > blockSize: mapped += 1
  let predictedCompact = int(compactContainerSize(members))
  let predictedFull = predictedFullV5Size(members, blockSize)

  doAssert predictedCompact == compactBytes.len,
    "§1d's size identity is the encoder's own contract and it does not hold " &
    "for " & label & ": predicted " & $predictedCompact & ", encoded " &
    $compactBytes.len

  var zeros = 0
  for b in fullBytes:
    if b == 0'u8: zeros += 1

  echo "  " & label & ":"
  echo "    members                              " & $members.len &
    " (" & $empties & " empty, " & $mapped & " larger than one block)"
  echo "    sum of stored member sizes           " & $payload
  echo "    FULL container, version " & $fullBytes[5] & ", measured    " &
    $fullBytes.len & "  (" & $zeros & " zero bytes, " &
    $((zeros * 1000 div fullBytes.len).float / 10.0) & "%)"
  echo "    FULL container, version 5, predicted " & $predictedFull &
    "   delta " & $(fullBytes.len - predictedFull)
  echo "    COMPACT container, predicted         " & $predictedCompact &
    "  = 28 + 24*" & $members.len & " + " & $payload
  echo "    COMPACT container, measured          " & $compactBytes.len
  echo "    structural overhead, FULL            " &
    $(fullBytes.len - payload) & "  (" &
    $(((fullBytes.len - payload) * 1000 div fullBytes.len).float / 10.0) & "%)"
  echo "    structural overhead, COMPACT         " &
    $(compactBytes.len - payload) & "  (" &
    $(((compactBytes.len - payload) * 1000 div compactBytes.len).float / 10.0) &
    "%)"
  echo "    reduction, full -> compact           " &
    $(fullBytes.len - compactBytes.len) & " bytes, " &
    $(((fullBytes.len - compactBytes.len) * 1000 div fullBytes.len).float /
      10.0) & "%  (" &
    $((fullBytes.len * 10 div compactBytes.len).float / 10.0) & "x)"
  if fullBytes.len != predictedFull:
    echo "    FINDING: the version-5 structural model and the measured " &
      "container disagree by " & $(fullBytes.len - predictedFull) &
      " bytes. A prediction and a measurement that disagree is a finding, " &
      "not a rounding error — see the milestone's record of this one."

proc measure_the_overhead_reduction() {.raises: [].} =
  ## CCP-2 deliverable 4. Taken against a VERSION-5 baseline: the direct-block
  ## tag already removed the mapping block of every member that fits in one
  ## block, so the Introduction's 86,016-byte version-3 mapping figure is
  ## largely already recovered and measuring against it would bank a saving
  ## twice.
  echo "CCP-2 deliverable 4 — overhead reduction, version-5 baseline:"
  reportOne("this repository's writer, " & $StepCount &
    "-step recording (version 5)", writeFullContainer())

  # THREE states, and only the first is a legitimate skip.
  #
  #   1. the sibling spec repo is not checked out  -> skip, and say so
  #   2. it is checked out and the fixture does not read -> FAIL
  #   3. it reads -> measure
  #
  # State 2 used to be state 1's message with a different suffix, and both
  # returned without failing. The distinction is the whole point: state 1 is
  # fixed by checking the repo out and state 2 is not fixed by anything the
  # person running the suite can do at the command line -- it means this
  # reader and a committed container from a documented producer disagree. A
  # version floor that moves under this fixture lands in state 2, so if the
  # two are reported the same way the floor moves silently.
  if not fileExists(SpecFixture):
    echo "  " & SpecFixture & ": NOT MEASURED (reader-subject absent) — the " &
      "sibling spec repo is not checked out next to this one; `repro ws " &
      "enable codetracer` provides it"
  else:
    let bytes = readCtfsFromFile(SpecFixture).valueOr:
      doAssert false, SpecFixture & ": the fixture is PRESENT and this " &
        "reader refuses it: " & error & ". A committed container from a " &
        "documented producer that this repository cannot read is a finding " &
        "about the two of them, and it must not be reported as an absent " &
        "subject -- checking the repo out again will not fix it."
      return
    reportOne(SpecFixture & " (committed spec fixture, version " &
      $bytes[5] & ")", bytes)

  echo "  The published BlockTracer container the campaign's Introduction " &
    "measures (/t/vl/3h/vl3h7u4w62wz3p4c44gpikxtbt/trace.ct, Aztec, 21 " &
    "streams) is NOT in this workspace: its 188,416 / 18,851 / 17,544 / " &
    "14,839 figures cannot be re-taken from the same bytes here and are " &
    "NOT MEASURED by this arm."
  echo "PASS: measure_the_overhead_reduction"

# ---------------------------------------------------------------------------

when isMainModule:
  try:
    removeDir(TmpDir)
    createDir(TmpDir)
  except CatchableError as e:
    doAssert false, "could not prepare " & TmpDir & ": " & e.msg
  except Defect as e:
    doAssert false, "could not prepare " & TmpDir & ": " & e.msg

  test_the_known_member_names_pack()
  test_a_compact_container_round_trips_byte_exactly()
  test_the_compact_container_carries_no_block_map()
  measure_the_overhead_reduction()

  # `CCP2_KEEP_FIXTURES=1` leaves `full.ct` and `compact.ct` behind, so the
  # one-shot-compression figures can be taken over the same bytes from outside
  # this process without editing this file.
  var keep = false
  try:
    keep = getEnv("CCP2_KEEP_FIXTURES").len > 0
  except CatchableError, Defect:
    discard
  if keep:
    echo "CCP2_KEEP_FIXTURES: left " & (TmpDir / "full.ct") & " and " &
      (TmpDir / "compact.ct") & " in place"
  else:
    try:
      removeDir(TmpDir)
    except CatchableError, Defect:
      discard
  echo "All CCP-2 compact container layout tests passed!"
