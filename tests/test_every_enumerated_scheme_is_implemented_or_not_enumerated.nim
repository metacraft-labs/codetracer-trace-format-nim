## The closed set of whole-file compression schemes, asserted in BOTH
## directions: what `ctfs-container.md` §1b enumerates and what this library
## can actually store and recover must be the SAME SET.
##
## Spec: `codetracer-trace-format-spec/ctfs-container.md` §1b, which states
## the standard this arm enforces — "A member of this set is a promise that
## every reader of the format implements the scheme" and "An enumerated member
## with no implementation is worse than an absent one: it is a capability a
## consumer can read in this document and cannot rely on." §1b also names the
## shape it refuses to repeat: the per-member method enum's `2 = LZ4`,
## "reserved, not yet implemented".
##
## Campaign: `codetracer-specs/milestones/CTFS-Compact-Profile.milestones.org`
## CCP-8, verification arm
## `test_every_enumerated_scheme_is_implemented_or_not_enumerated`.
##
## **Three sets, and the arm is the three equalities.**
##
## * `S` — the values §1b ENUMERATES, parsed out of the spec markdown. Not
##   mirrored into a constant here: a mirror of the spec is a second copy that
##   drifts, and the drift is exactly what this arm exists to catch.
## * `A` — the values the reader ACCEPTS, probed over all 256 byte values
##   through both doors (`parseWholeFileCompression` on the byte and
##   `readWholeFileCompression` on a real container's header), which must
##   agree with each other value by value.
## * `R` — the values this library ROUND-TRIPS, probed by storing a real
##   container under each and recovering it. Membership requires byte-exact
##   recovery of the image AND of every member's payload, in both profiles,
##   and — for a scheme that is not `none` — that the stored bytes DIFFER from
##   the image, so a no-op codec cannot buy membership.
##
## `S == A` is the two sides of the closed set. `A == R` is §1b's promise:
## a value the reader accepts must be one the library can actually undo.
##
## **CONTROL, both directions, and both were run (see the milestone's
## evidence).** Adding a row to §1b's table without an implementation puts a
## value in `S` that is in neither `A` nor `R`, and this arm goes red naming
## it. Teaching `parseWholeFileCompression` and `storeImage` a scheme that
## §1b does not enumerate puts a value in `A` and `R` that is not in `S`, and
## this arm goes red naming that. Neither side can move alone.
##
## **No mocks.** The spec set comes from the committed spec file; the
## containers come from this library's own compact encoder; the schemes are
## applied by the library's own `storeImage` and undone by its own
## `reconstructImage`. Nothing here is stubbed, and there is no table of
## codecs in this file — `storeImage`'s `case` is the only one, which is why
## probing it reports the implementation rather than a copy of it.

import std/[os, strutils, algorithm, sequtils, tables]
import results
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_ctfs/compact

const
  SpecPath = "../codetracer-trace-format-spec/ctfs-container.md"
    ## The sibling spec repo, by the same workspace convention
    ## `test_compact_container_layout.nim` uses for its fixture.
  FixturePath = "../codetracer-trace-format-spec/fixtures/minimal_trace.ct"
    ## A real, committed version-5 container from a documented producer
    ## (`codetracer-trace-format-spec/fixtures/README.md`), the same one
    ## `test_compact_container_layout.nim` measures against.
  SectionHeading = "### 1b."
  NextHeading = "### 1c."

proc toBytes(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

# ---------------------------------------------------------------------------
# S: the set §1b enumerates
# ---------------------------------------------------------------------------

proc specEnumeratedSchemes(): Table[int, string] =
  ## The `(value, name)` rows of §1b's table.
  ##
  ## Every step is checked rather than assumed, because a parse that silently
  ## found nothing would make this whole arm pass vacuously — which is the
  ## failure mode of a spec-conformance test and worth more care than the
  ## comparison it feeds.
  let path = currentSourcePath().parentDir.parentDir / SpecPath
  doAssert fileExists(path),
    "the spec is not at " & path & ". This arm reads §1b's table out of the " &
    "spec rather than mirroring it into a constant; without the file it " &
    "would compare the implementation against an empty set and pass. Check " &
    "out codetracer-trace-format-spec as a sibling of this repository."
  var text = ""
  try:
    text = readFile(path)
  except IOError, OSError:
    doAssert false, "could not read " & path
  let head = text.find(SectionHeading)
  doAssert head >= 0,
    SectionHeading & " is not in " & path & ": §1b has been renumbered or " &
    "retitled, and this arm is looking at the wrong section"
  var tail = text.find(NextHeading, head)
  if tail < 0:
    tail = text.len
  let section = text[head ..< tail]

  for line in section.splitLines():
    let row = line.strip()
    if not row.startsWith("|"):
      continue
    let cells = row.split('|')
    if cells.len < 4:
      continue
    let value = cells[1].strip().strip(chars = {'`'})
    let name = cells[2].strip().strip(chars = {'`'})
    # The header row (`| Value | Name | ...`) and the `|---|` separator both
    # fail this, which is how they are skipped without naming them.
    var n = 0
    try:
      n = parseInt(value)
    except ValueError:
      continue
    doAssert name.len > 0,
      "§1b enumerates value " & $n & " with no name: a member of the closed " &
      "set is a promise, and an unnamed one cannot be checked against an " &
      "implementation"
    doAssert n notin result,
      "§1b enumerates value " & $n & " twice: a field that admits two " &
      "spellings of one state is not a specification of that state"
    result[n] = name

  doAssert result.len > 0,
    "§1b's table parsed to no rows. The table's shape has changed and this " &
    "arm is reading nothing; it must not pass on an empty spec set."
  doAssert 0 in result and result[0] == "none",
    "§1b does not enumerate `0 = none`, which every version-5-and-below " &
    "container is inferred to carry (§1a). The parse is wrong, or the spec " &
    "has moved in a way this arm must be rewritten for. Parsed: " & $result

# ---------------------------------------------------------------------------
# The container the probes run over
# ---------------------------------------------------------------------------

proc members(): seq[CompactMember] =
  ## A member set with the properties the probes need: an empty member, a
  ## member whose payload is highly compressible (so a real codec visibly
  ## shrinks the object), one that is not, and the four interning tables
  ## together, since a partial set is refused by every writer here.
  @[CompactMember(name: "meta.dat", payload: toBytes("CTMD" & repeat('\7', 60))),
    CompactMember(name: "empty.dat", payload: @[]),
    CompactMember(name: "steps.dat", payload: toBytes(repeat("step-", 4000))),
    CompactMember(name: "values.dat",
      payload: toBytes("\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99")),
    CompactMember(name: "paths.dat", payload: toBytes("/src/a.nim\n")),
    CompactMember(name: "paths.off", payload: toBytes("\0\0\0\0\0\0\0\0")),
    CompactMember(name: "funcs.dat", payload: toBytes("work")),
    CompactMember(name: "funcs.off", payload: toBytes("\0\0\0\0\0\0\0\0")),
    CompactMember(name: "types.dat", payload: toBytes("int")),
    CompactMember(name: "types.off", payload: toBytes("\0\0\0\0\0\0\0\0")),
    CompactMember(name: "varnames.dat", payload: toBytes("i")),
    CompactMember(name: "varnames.off", payload: toBytes("\0\0\0\0\0\0\0\0"))]

proc compactImage(): seq[byte] =
  let encoded = encodeCompactContainer(members())
  doAssert encoded.isOk, "encodeCompactContainer: " & encoded.error
  result = encoded.get()
  doAssert result[5] == CtfsVersionV6 and
           result[V6ProfileOffset] == uint8(ord(cpCompact)),
    "the probe's container is not a version-6 compact one"

proc fullImage(): seq[byte] =
  ## A version-6 FULL container, so the probes cover the profile whose body a
  ## whole-file scheme has to leave addressable after reconstruction (§1a:
  ## "every offset in this document holds unchanged, including block
  ## numbering"). Built by hand from one block, because what matters here is
  ## that the bytes survive the round trip, not what they decode to.
  result = newSeq[byte](int(DefaultBlockSize) * 3)
  for i in 0 ..< CtfsMagic.len:
    result[i] = CtfsMagic[i]
  result[5] = CtfsVersionV6
  writeU32LE(result, 8, DefaultBlockSize)
  writeU32LE(result, 12, 0'u32)        # auto-filled root entries
  result[V6ProfileOffset] = uint8(ord(cpFull))
  result[V6CompressionOffset] = uint8(ord(wfcNone))
  # Body bytes that are neither all-zero (a codec would hide a truncation in
  # them) nor incompressible.
  for i in V6HeaderSize ..< result.len:
    result[i] = byte((i * 7) mod 251)

proc fixtureCompactImage(): seq[byte] =
  ## The committed spec-repo fixture, converted to a version-6 compact
  ## container at TEST TIME.
  ##
  ## CCP-8 deliverable 3 asks for the corpus to exercise a whole-file scheme
  ## end to end. The corpus cannot be the stored object: a whole-file scheme
  ## is a VERSION-6 header field (byte 17 of the 24-byte header) and every
  ## committed `.ct` in this workspace is container version 5, which has no
  ## such byte — §1a is explicit that a container at version 5 or below is
  ## full-profile with no whole-file compression. So the v6 container is
  ## produced here, from the real committed members of a real recording, and
  ## nothing new is committed. `minimal_trace.ct` is a split-stream container
  ## written by this repository's own writer, with a fixed recording id, so it
  ## reproduces byte for byte (`fixtures/README.md`).
  let path = currentSourcePath().parentDir.parentDir / FixturePath
  doAssert fileExists(path),
    "the committed fixture is not at " & path & ". The corpus half of this " &
    "arm reads a REAL container's members rather than only the synthetic " &
    "ones above, and without the file it would silently check half of what " &
    "it claims to."
  var raw = ""
  try:
    raw = readFile(path)
  except IOError, OSError:
    doAssert false, "could not read " & path
  let bytes = toBytes(raw)
  doAssert bytes[5] == CtfsVersion,
    "the fixture is container version " & $bytes[5] & ", not " & $CtfsVersion &
    "; this probe assumes the committed corpus is version 5 and that a " &
    "version-6 container has to be produced from it"
  let got = collectFullProfileMembers(bytes)
  doAssert got.isOk, "could not read the fixture's members: " & got.error
  doAssert got.get().len >= 10,
    "the fixture yielded only " & $got.get().len & " members; the conversion " &
    "is reading the wrong thing"
  let encoded = encodeCompactContainer(got.get())
  doAssert encoded.isOk, "encodeCompactContainer over the fixture: " & encoded.error
  encoded.get()

# ---------------------------------------------------------------------------
# A: the set the reader accepts
# ---------------------------------------------------------------------------

proc acceptedSchemes(image: seq[byte]): seq[int] =
  ## Every byte value the reader accepts in the whole-file `Compression`
  ## field, probed over ALL 256 of them through both doors.
  ##
  ## Probing the whole byte space rather than the enum is what makes the
  ## second control direction real: a parser taught an unenumerated scheme
  ## shows up here, where iterating the enum could not see it.
  for b in 0 .. 255:
    let fromByte = parseWholeFileCompression(uint8(b))
    var head = image
    head[V6CompressionOffset] = uint8(b)
    let fromHeader = readWholeFileCompression(head)
    doAssert fromByte.isOk == fromHeader.isOk,
      "scheme byte " & $b & ": the raw parser and the header reader " &
      "disagree about whether it is accepted (" &
      (if fromByte.isOk: "parser ok" else: "parser: " & fromByte.error) &
      " / " & (if fromHeader.isOk: "header ok" else: "header: " &
      fromHeader.error) & "). Two doors onto one closed set must not have " &
      "two answers."
    if fromByte.isOk:
      doAssert fromHeader.get() == fromByte.get(),
        "scheme byte " & $b & " parses to " & $fromByte.get() &
        " from the byte and " & $fromHeader.get() & " from the header"
      doAssert ord(fromByte.get()) == b,
        "scheme byte " & $b & " parses to " & $fromByte.get() &
        ", whose ordinal is " & $ord(fromByte.get()) &
        ": the enum's ordinal IS the wire value (§1b)"
      result.add(b)
    else:
      doAssert fromByte.error.contains($b),
        "scheme byte " & $b & " is refused without being named: " &
        fromByte.error & " (§1c MUSTs that the offending value be named)"

# ---------------------------------------------------------------------------
# R: the set the library round-trips
# ---------------------------------------------------------------------------

proc roundTrips(scheme: CtfsWholeFileCompression, image: seq[byte],
    what: string): bool =
  ## Whether `scheme` genuinely round-trips `image`: stored by `storeImage`,
  ## recovered BYTE-EXACTLY by `reconstructImage`, with §1a's header
  ## plaintext, and — unless the scheme is `none` — with stored bytes that
  ## actually differ from the image.
  let stored = storeImage(image, scheme)
  if stored.isErr:
    echo "  ", what, " ", scheme, ": storeImage refused: ", stored.error
    return false
  let obj = stored.get()

  # §1a: the 24-byte header is always stored as plaintext, since it is what
  # declares the scheme.
  doAssert obj.len >= V6HeaderSize, what & " " & $scheme &
    ": the stored object is shorter than the header that declares its scheme"
  var expectHead = image[0 ..< V6HeaderSize]
  expectHead[V6CompressionOffset] = uint8(ord(scheme))
  doAssert obj[0 ..< V6HeaderSize] == expectHead,
    what & " " & $scheme & ": the stored object's 24-byte header is not the " &
    "image's with the scheme declared. §1a: \"the 24-byte header is always " &
    "stored as plaintext, since it is what declares the scheme\"."
  doAssert ord(readWholeFileCompression(obj).get()) == ord(scheme),
    what & " " & $scheme & ": the stored object does not declare the scheme " &
    "it was stored under"

  if scheme != wfcNone:
    doAssert obj != expectHead & image[V6HeaderSize .. ^1],
      what & " " & $scheme & ": the stored object is the image with nothing " &
      "but the scheme stamped, so the encoder did nothing. A no-op codec " &
      "must not count as an implementation — that is §1b's " &
      "declared-but-unimplemented defect wearing a pass."

  let back = reconstructImage(obj)
  if back.isErr:
    echo "  ", what, " ", scheme, ": reconstructImage refused: ", back.error
    return false
  # §1a: the reconstructed image KEEPS the header, which still declares the
  # scheme it was stored under — so the image to compare against is the
  # original with the scheme stamped, not the original.
  doAssert back.get() == expectHead & image[V6HeaderSize .. ^1],
    what & " " & $scheme & ": the reconstructed image is not the original. " &
    "§1a: a reader reconstructs `header || decompress(rest)` and then every " &
    "offset in the document holds unchanged."
  true

proc membersSurvive(scheme: CtfsWholeFileCompression) =
  ## The members read back byte-exactly out of a compact container stored
  ## under `scheme` and reconstructed: the end-to-end half, so the field is
  ## not write-only.
  let image = compactImage()
  let obj = storeImage(image, scheme).valueOr:
    doAssert false, "storeImage: " & error
    return
  # As STORED, a decoder not told it has reconstructed anything must refuse —
  # the refusal arm of §1a, kept here because it is what makes the success
  # below attributable to the reconstruction.
  if scheme != wfcNone:
    let asStored = decodeCompactContainer(obj)
    doAssert asStored.isErr,
      "a compact container stored under " & $scheme & " was read as a " &
      "directory without being reconstructed"
    doAssert asStored.error.contains("reconstructed"),
      "the refusal does not say what is missing: " & asStored.error
  let back = reconstructImage(obj).valueOr:
    doAssert false, "reconstructImage: " & error
    return
  let got = decodeCompactContainer(back, bodyReconstructed = true).valueOr:
    doAssert false, "decodeCompactContainer: " & error
    return
  let want = members()
  doAssert got.len == want.len,
    $scheme & ": " & $got.len & " members read back, not " & $want.len
  for i in 0 ..< want.len:
    doAssert got[i].name == want[i].name,
      $scheme & ": member " & $i & " is '" & got[i].name & "', not '" &
      want[i].name & "'"
    doAssert got[i].payload == want[i].payload,
      $scheme & ": member '" & got[i].name & "' did not survive the round " &
      "trip byte-exactly (" & $got[i].payload.len & " bytes back, " &
      $want[i].payload.len & " written)"

# ---------------------------------------------------------------------------
# The arm
# ---------------------------------------------------------------------------

proc schemeName(scheme: CtfsWholeFileCompression): string =
  ## The spec's name for an enum member, DERIVED rather than tabulated, so a
  ## member added to the enum gets its spec name without an edit here (and a
  ## member whose name does not match §1b's fails below).
  ($scheme)[3 .. ^1].toLowerAscii()

proc test_every_enumerated_scheme_is_implemented_or_not_enumerated() =
  let spec = specEnumeratedSchemes()
  var specValues = toSeq(spec.keys)
  specValues.sort()

  let compact = compactImage()
  let full = fullImage()
  let fixture = fixtureCompactImage()

  let accepted = acceptedSchemes(compact)
  let acceptedFull = acceptedSchemes(full)
  doAssert accepted == acceptedFull,
    "the accepted scheme set differs between a compact container (" &
    $accepted & ") and a full one (" & $acceptedFull & "): the field is a " &
    "property of the version-6 header, not of the body behind it"

  var rounded: seq[int]
  for scheme in CtfsWholeFileCompression:
    let compactOk = roundTrips(scheme, compact, "compact")
    let fullOk = roundTrips(scheme, full, "version-6 full")
    # The CORPUS half: the same probe over a real committed recording's
    # members, so the implemented set is not a property of the synthetic
    # member list above.
    let fixtureOk = roundTrips(scheme, fixture,
      "the committed fixture, compact")
    doAssert fixtureOk == compactOk,
      $scheme & " round-trips the synthetic compact container and not the " &
      "committed fixture's (synthetic " & $compactOk & ", fixture " &
      $fixtureOk & ")"
    doAssert compactOk == fullOk,
      $scheme & " round-trips one profile and not the other (compact " &
      $compactOk & ", full " & $fullOk & "). §1a's scheme covers the image " &
      "from offset 24 and knows nothing about which body is behind it."
    if compactOk:
      membersSurvive(scheme)
      rounded.add(ord(scheme))
  rounded.sort()

  # The scheme measured on a real recording, printed so the arm records what
  # the capability is worth rather than only that it exists.
  let storedFixture = storeImage(fixture, wfcZstd)
  if storedFixture.isOk:
    echo "  the committed fixture, compact: ", fixture.len,
      " bytes; stored under zstd: ", storedFixture.get().len
  echo "  §1b enumerates:        ", specValues.mapIt($it & " = " & spec[it])
  echo "  the reader accepts:    ", accepted
  echo "  the library round-trips: ", rounded

  # --- S vs A, both directions ---
  let enumeratedNotAccepted = specValues.filterIt(it notin accepted)
  doAssert enumeratedNotAccepted.len == 0,
    "ctfs-container.md §1b enumerates " &
    enumeratedNotAccepted.mapIt($it & " = " & spec[it]).join(", ") &
    ", which this reader does not accept. §1b: \"An enumerated member with " &
    "no implementation is worse than an absent one: it is a capability a " &
    "consumer can read in this document and cannot rely on.\" Either " &
    "implement it (parseWholeFileCompression, storeImage, reconstructImage) " &
    "or remove the row from §1b's table."
  let acceptedNotEnumerated = accepted.filterIt(it notin specValues)
  doAssert acceptedNotEnumerated.len == 0,
    "this reader accepts whole-file scheme(s) " &
    acceptedNotEnumerated.join(", ") & ", which §1b does not enumerate. A " &
    "scheme no consumer can read about in the spec is a private extension, " &
    "and the next writer to emit it produces containers conforming readers " &
    "refuse. Enumerate it in §1b or stop accepting it."

  # --- A vs R: §1b's promise ---
  let acceptedNotRoundTripped = accepted.filterIt(it notin rounded)
  doAssert acceptedNotRoundTripped.len == 0,
    "this reader accepts scheme(s) " & acceptedNotRoundTripped.join(", ") &
    " that it cannot store and recover. §1b: \"A member of this set is a " &
    "promise that every reader of the format implements the scheme.\" An " &
    "accepted value with no working codec is the `2 = LZ4` shape §1b names " &
    "as the one it refuses to repeat."
  let roundTrippedNotAccepted = rounded.filterIt(it notin accepted)
  doAssert roundTrippedNotAccepted.len == 0,
    "scheme(s) " & roundTrippedNotAccepted.join(", ") & " round-trip but " &
    "are refused at the header door, so no container could ever carry them"

  # --- and the names, so the two sides are the same SCHEMES and not merely
  # the same numbers ---
  for scheme in CtfsWholeFileCompression:
    doAssert spec[ord(scheme)] == schemeName(scheme),
      "§1b calls value " & $ord(scheme) & " `" & spec[ord(scheme)] &
      "` and this library calls it `" & schemeName(scheme) & "` (" &
      $scheme & "). The sets agree on the numbers and disagree on what " &
      "they mean, which is worse than disagreeing outright."

  echo "PASS: test_every_enumerated_scheme_is_implemented_or_not_enumerated"

test_every_enumerated_scheme_is_implemented_or_not_enumerated()
