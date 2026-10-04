{.push raises: [].}

## The compact profile of a finished container, and whole-file compression:
## what a writer that writes the full profile and converts at close does, and
## what a reader does to a stored object before it reads a member.
##
## Spec: `codetracer-trace-format-spec/ctfs-container.md` §1e (the threshold
## and the converting writer), §1f (how a framed member is stored in a
## compact container), §1d (the compact body), §1a/§1b (whole-file
## compression).
##
## Filesystem-free: the C ABI's in-memory writer and the freestanding reader
## use it. `codetracer_profile_writer` is the writer built on it.

import results
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ../codetracer_ctfs/compact
import ./step_map_builder

export results, compact

const
  DefaultRawByteThreshold*: uint64 = 1'u64 shl 20
    ## 1 MiB of raw member bytes: §1e's RECOMMENDED default, and a judgement
    ## recorded as one. The container that opened the compact-profile campaign
    ## has 85,118 bytes of logical content, an order of magnitude under it, and
    ## blockchain traces are bounded by gas rather than by taste.

# ---------------------------------------------------------------------------
# Full → compact: every frame replaced by its content (§1f)
# ---------------------------------------------------------------------------

proc inflateChunkedTable(dat, idx: openArray[byte], name: string,
    headerSize, entrySize: int): Result[(seq[byte], seq[byte]), string] =
  ## A chunked compressed table (`ctfs-container.md` §7) with its chunks
  ## replaced by their content: `foo.dat` the contents back to back, `foo.idx`
  ## the same header and entries with each entry's leading `u64` offset moved
  ## to its chunk's content. `headerSize`/`entrySize` are 4/8 for the §7
  ## layout and 8/16 for `spans.idx`, whose entries also carry a cumulative
  ## record count, kept as it is.
  if idx.len < headerSize or (idx.len - headerSize) mod entrySize != 0:
    return err(name & ".idx is " & $idx.len & " bytes, not a " & $headerSize &
      "-byte header and " & $entrySize & "-byte entries")
  let chunks = (idx.len - headerSize) div entrySize
  var newDat: seq[byte]
  var newIdx = @idx
  for c in 0 ..< chunks:
    let e = headerSize + c * entrySize
    let start = readU64LE(idx, e)
    let stop =
      if c + 1 < chunks: readU64LE(idx, e + entrySize)
      else: uint64(dat.len)
    if start > stop or stop > uint64(dat.len):
      return err(name & ".idx entry " & $c & " spans " & $start & ".." & $stop &
        " of a " & $dat.len & "-byte " & name & ".dat")
    writeU64LE(newIdx, e, uint64(newDat.len))
    ? appendFrameContent(dat.toOpenArray(int(start), int(stop) - 1),
      name & ".dat chunk " & $c, newDat)
  ok((newDat, newIdx))

proc inflateStepMap(m: openArray[byte]): Result[seq[byte], string] =
  ## `step-map.ns` with every chunk's frame replaced by its content and the
  ## chunk table's `frame_offset`s moved to it (`internal-files.md`
  ## §"`step-map.ns`"; `ctfs-container.md` §1f).
  if m.len < StepMapHeaderSize:
    return err("step-map.ns is " & $m.len & " bytes, shorter than its header")
  let n = int(readU32LE(m, 6))
  let tableEnd = StepMapHeaderSize + n * StepMapChunkEntrySize
  if n < 0 or tableEnd > m.len:
    return err("step-map.ns: its chunk table runs past the member")
  var output = @(m.toOpenArray(0, tableEnd - 1))
  for c in 0 ..< n:
    let e = StepMapHeaderSize + c * StepMapChunkEntrySize
    let start = tableEnd + int(readU64LE(m, e))
    let stop =
      if c + 1 < n: tableEnd + int(readU64LE(m, e + StepMapChunkEntrySize))
      else: m.len
    if start > stop or stop > m.len:
      return err("step-map.ns: chunk " & $c & "'s frame is out of range")
    writeU64LE(output, e, uint64(output.len - tableEnd))
    ? appendFrameContent(m.toOpenArray(start, stop - 1), "step-map.ns chunk " & $c,
      output)
  ok(output)

proc compactMembersOf*(full: openArray[byte]):
    Result[seq[CompactMember], string] =
  ## The members a compact container of the full container `full` carries:
  ## the full container's members in its own order, under the same names, with
  ## every framed member's frames replaced by their content
  ## (`ctfs-container.md` §1f).
  ##
  ## Framed members are recognised by the formats this repository's split-
  ## stream writer emits: a `.dat` with an `.idx` companion is a chunked
  ## compressed table (`spans` in its own index layout), and `step-map.ns`.
  ## A seekable-zstd `events.log` is refused rather than copied, since copying
  ## its frames would break §1e's raw-member property.
  var members = ? collectFullProfileMembers(full)
  proc find(members: seq[CompactMember], name: string): int =
    for i, m in members.pairs:
      if m.name == name: return i
    -1
  for i in 0 ..< members.len:
    let name = members[i].name
    if name == "events.log":
      return err("events.log is a seekable-zstd stream this conversion does " &
        "not inflate; a compact container must not carry its frames " &
        "(ctfs-container.md §1e)")
    if name == StepMapFileName:
      members[i].payload = ? inflateStepMap(members[i].payload)
    elif name.len > 4 and name[^4 .. ^1] == ".dat":
      let stem = name[0 ..< name.len - 4]
      let j = members.find(stem & ".idx")
      if j >= 0:
        let (headerSize, entrySize) = if stem == "spans": (8, 16) else: (4, 8)
        let (dat, idx) = ? inflateChunkedTable(members[i].payload,
          members[j].payload, stem, headerSize, entrySize)
        members[i].payload = dat
        members[j].payload = idx
  ok(members)

proc rawMemberBytes*(members: openArray[CompactMember]): uint64 =
  ## §1e's measured quantity: the total length of the compact members.
  for m in members:
    result += uint64(m.payload.len)

proc selectProfile*(full: openArray[byte], threshold: uint64):
    Result[(CtfsProfile, seq[byte], uint64), string] =
  ## The container a converting writer emits for the full container `full`:
  ## the compact container when the compact members total less than
  ## `threshold` raw bytes, the full container otherwise (`ctfs-container.md`
  ## §1e). Returns the profile, the container's bytes and the raw total.
  let members = ? compactMembersOf(full)
  let raw = rawMemberBytes(members)
  if raw < threshold:
    ok((cpCompact, ? encodeCompactContainer(members), raw))
  else:
    ok((cpFull, @full, raw))

