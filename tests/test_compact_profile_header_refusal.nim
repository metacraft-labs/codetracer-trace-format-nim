{.push raises: [].}

## CCP-1: the header declares the profile and the whole-file scheme, and a
## reader that does not know a value REFUSES the container.
##
## Spec: `codetracer-trace-format-spec/ctfs-container.md` §1, §1a, §1b, §1c.
##
## **Why this file exists at all.** This format has already shipped a layout
## change under an unchanged version stamp: containers were written with the
## corrected global line index packing while the version byte still said 3,
## so a reader that trusted the stamp placed every step one line high AND
## returned success. The repair is three artefacts that exist only because of
## it — `SupportedMetaDatVersions = [4, 5]`,
## `LastShiftedGlobalIndexVersion = 3`, and the `acceptShiftedGlobalIndex`
## opt-in whose own documentation says it exists so that behaviour does not
## depend on how a recording happened to be written. The compact profile is
## a second layout change, and the whole point of CCP-1 is that this one
## arrives behind a gate.
##
## **What this adds over `test_older_versions_are_refused.nim`, which landed
## alongside it.** That test stamps version 6 onto a CURRENT container and
## asserts every reader door names 6 and 5 — so the version refusal itself is
## already pinned there, and this file does not claim it. What is here and not
## there is the rest of the gate: a container that is genuinely COMPACT rather
## than a version-5 body with its stamp changed; the CONTROL that the same
## reader opens the FULL container of the SAME recording and succeeds; the
## proof that the two name the same members; the discrimination that the
## refusal is keyed on the version byte and not on the compact body's zero
## block size; and the whole of the second arm, the profile and scheme fields'
## closed-set parsing, which has no counterpart there.
##
## NO MOCKS. The full container is produced by this repository's own writer,
## through the real stream writers, and read back through the real
## `openTrace` and the real `detectNativeBundle`. The compact container is
## built by `buildCompactFixture` below out of the full container's own
## bytes, and the stream NAME set of the two is asserted equal, so "a compact
## container of the same recording" is a checked claim rather than a label.
##
## **`buildCompactFixture`'s body layout is a FIXTURE, not the format.** CCP-2
## owns the normative compact body; what CCP-1 needs is a container carrying
## the new profile value, and every assertion here is satisfied before the
## body is reached. The one thing the fixture does take from CCP-2 on purpose
## is the stream-name packing: base40 in a `u64`, exactly as `FileEntry`
## carries it today, so the name sets can be compared at all.

import std/[os, strutils]
import results
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_ctfs/base40
import codetracer_trace_types
import codetracer_trace_reader
import native_decoder
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/interning_table
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/exec_stream
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/call_stream
import codetracer_trace_writer/io_event_stream

const TmpDir = "tmp_compact_profile_refusal"

proc toBytes(s: string): seq[byte] {.raises: [].} =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

# ---------------------------------------------------------------------------
# A real v4 full container, written by this repository's own writer.
# ---------------------------------------------------------------------------

proc writeFullContainer(): seq[byte] {.raises: [].} =
  var ctfs = createCtfs()

  let metaFileRes = ctfs.addFile("meta.dat")
  doAssert metaFileRes.isOk, "addFile meta.dat: " & metaFileRes.error
  var metaFile = metaFileRes.get()
  let meta = TraceMetadata(
    recordingId: "01949fcc-7d92-7e9c-8ccc-dddddddddddd",
    program: "ccp1_profile_gate",
    args: @["--steps=20"],
    workdir: "/home/test")
  let metaWr = ctfs.writeMetaDat(metaFile, meta,
    recorderId = "ccp1-fixture", hasStepStream = true,
    hasValueStream = true, hasIoEventStream = true)
  doAssert metaWr.isOk, "writeMetaDat: " & metaWr.error

  let tabRes = initTraceInterningTables(ctfs)
  doAssert tabRes.isOk, "initTraceInterningTables: " & tabRes.error
  var tab = tabRes.get()
  discard ctfs.ensurePathId(tab, "/src/main.py")
  discard ctfs.ensurePathId(tab, "/src/utils.py")
  discard ctfs.ensureFunctionId(tab, "main")
  discard ctfs.ensureTypeId(tab, "int")
  discard ctfs.ensureVarnameId(tab, "i")

  let execRes = initExecStreamWriter(ctfs, chunkSize = 8)
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

  for i in 0 ..< 20:
    var ev: StepEvent
    if i == 0:
      ev = StepEvent(kind: sekAbsoluteStep, globalLineIndex: 0)
    else:
      ev = StepEvent(kind: sekDeltaStep, lineDelta: 1)
    doAssert ctfs.writeEvent(execW, ev).isOk, "writeEvent step"
    doAssert ctfs.writeStepValues(valW, @[
      VariableValue(varnameId: 0, data: toBytes($i))]).isOk,
      "writeStepValues"

  doAssert ctfs.writeCall(callW, call_stream.CallRecord(
    functionId: 0, parentCallKey: -1, entryStep: 0, exitStep: 19,
    depth: 0, args: @[], returnValue: @[VoidReturnMarker],
    exception: @[], children: @[])).isOk, "writeCall"
  doAssert finalizeCallStream(ctfs, callW).isOk, "finalizeCallStream"

  doAssert ctfs.writeEvent(ioW, IOEvent(
    kind: elkWrite, stepId: 0, data: "hello\n".toBytes)).isOk, "writeEvent io"

  doAssert ctfs.flush(execW).isOk, "flush exec"
  doAssert value_stream.flush(ctfs, valW).isOk, "flush values"
  doAssert io_event_stream.flush(ctfs, ioW).isOk, "flush io"

  result = ctfs.toBytes()
  ctfs.closeCtfs()

# ---------------------------------------------------------------------------
# The compact fixture.
# ---------------------------------------------------------------------------

type StreamEntry = object
  encodedName: uint64
  name: string
  payload: seq[byte]

proc collectStreams(full: openArray[byte]): seq[StreamEntry] {.raises: [].} =
  ## Walk the version-5 root directory and pull every member out through the
  ## real reader, so the fixture carries the recording's actual bytes. The
  ## member's `MapBlock` form — empty, direct, or mapped (`ctfs-container.md`
  ## §2) — is `readInternalFile`'s business, not this walk's; what is read here
  ## is the NAME, which is the one thing the compact directory has to carry
  ## identically.
  let blockSize = readU32LE(full, 8)
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
    let name = base40Decode(encoded)
    let bytes = readInternalFile(full, name, blockSize, maxEntries)
    doAssert bytes.isOk, "reading '" & name & "' out of the full container: " &
      bytes.error
    result.add(StreamEntry(encodedName: encoded, name: name,
                           payload: bytes.get()))

proc buildCompactFixture(streams: seq[StreamEntry],
                         profile: uint8 = uint8(ord(cpCompact)),
                         compression: uint8 = uint8(ord(wfcNone)),
                         version: uint8 = CtfsVersionV6): seq[byte]
                        {.raises: [].} =
  ## 24-byte version-6 header, a `(name, offset, length)` directory, and the
  ## members concatenated raw. Fixture layout — the compact-layout milestone is
  ## normative for the body.
  let dirBytes = 4 + streams.len * 24
  var payloadBytes = 0
  for s in streams:
    payloadBytes += s.payload.len
  result = newSeq[byte](V6HeaderSize + dirBytes + payloadBytes)

  for i in 0 ..< 5:
    result[i] = CtfsMagic[i]
  result[5] = version
  result[6] = uint8(ord(emNone))
  result[7] = 0'u8   # §1a: a compact container MUST write max_shards = 0
  writeU32LE(result, 8, 0'u32)   # no blocks, so no block size
  writeU32LE(result, 12, 0'u32)  # no root entry array
  result[V6ProfileOffset] = profile
  result[V6CompressionOffset] = compression
  # bytes 18..23 stay zero — §1 makes a non-zero value there a refusal

  writeU32LE(result, V6HeaderSize, uint32(streams.len))
  var dirOff = V6HeaderSize + 4
  var payloadOff = V6HeaderSize + dirBytes
  for s in streams:
    writeU64LE(result, dirOff, s.encodedName)
    writeU64LE(result, dirOff + 8, uint64(payloadOff))
    writeU64LE(result, dirOff + 16, uint64(s.payload.len))
    for j in 0 ..< s.payload.len:
      result[payloadOff + j] = s.payload[j]
    dirOff += 24
    payloadOff += s.payload.len

  doAssert payloadOff == result.len,
    "the fixture has padding in it, which would make it a bad stand-in for " &
    "a layout whose whole claim is that it has none"

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
# test_an_unknowing_reader_refuses_a_compact_container
# ---------------------------------------------------------------------------

proc test_an_unknowing_reader_refuses_a_compact_container() {.raises: [].} =
  ## This build does not implement the compact body — CCP-2 does — so it is
  ## itself an unknowing reader, and it must refuse a compact container by
  ## name rather than parse its header as a v4 one.
  ##
  ## THE CONTROL, and it is the control the v3/v4 incident shows is
  ## necessary: the SAME reader opens a FULL container of the SAME recording
  ## and succeeds. Without it, a reader that refused every container on this
  ## path would pass the refusal assertion.
  let fullBytes = writeFullContainer()
  let streams = collectStreams(fullBytes)
  doAssert streams.len > 0, "the full container carried no streams"

  let compactBytes = buildCompactFixture(streams)

  # "Of the same recording" is checked, not asserted: the two containers name
  # the same streams, and they name them with the same base40 packing.
  var fullNames: seq[string]
  for s in streams:
    fullNames.add(s.name)
  var compactNames: seq[string]
  let count = readU32LE(compactBytes, V6HeaderSize)
  for i in 0 ..< int(count):
    compactNames.add(base40Decode(
      readU64LE(compactBytes, V6HeaderSize + 4 + i * 24)))
  doAssert fullNames == compactNames,
    "the fixture does not carry the same stream set as the recording it was " &
    "built from: full=" & $fullNames & " compact=" & $compactNames

  let fullPath = TmpDir / "full.ct"
  let compactPath = TmpDir / "compact.ct"
  writeFixture(fullPath, fullBytes)
  writeFixture(compactPath, compactBytes)

  # --- the refusal ---
  let compactOpen = openTrace(compactPath)
  doAssert compactOpen.isErr,
    "openTrace ACCEPTED a compact container. A reader that cannot read the " &
    "body must not get as far as the body: this build reads version " &
    $CtfsVersion & " and the container declares " & $CtfsVersionV6
  doAssert compactOpen.unsafeError.contains("version " & $CtfsVersionV6),
    "the refusal does not NAME the version it refused, which is what " &
    "ctfs-container.md §1c and §2 both require. Got: " &
    compactOpen.unsafeError
  doAssert compactOpen.unsafeError.contains("version " & $CtfsVersion),
    "the refusal does not say which version this reader DOES read. Got: " &
    compactOpen.unsafeError

  # The native-bundle door refuses it too, with the same account of the same
  # byte — `ct-print` comes through there, not through `openTrace`.
  let detect = detectNativeBundle(compactBytes)
  doAssert detect.isErr, "detectNativeBundle accepted a compact container"
  doAssert detect.unsafeError.contains("version " & $CtfsVersionV6),
    "detectNativeBundle's refusal does not name the version. Got: " &
    detect.unsafeError

  # --- THE CONTROL ---
  let fullOpen = openTrace(fullPath)
  doAssert fullOpen.isOk,
    "CONTROL FAILED: the same reader could not open a FULL container of the " &
    "same recording, so the refusal above is not attributable to the " &
    "profile. Got: " & fullOpen.unsafeError
  doAssert fullOpen.get().metadata.program == "ccp1_profile_gate",
    "CONTROL FAILED: the full container opened but did not read back as the " &
    "recording that was written"
  let fullDetect = detectNativeBundle(fullBytes)
  doAssert fullDetect.isOk,
    "CONTROL FAILED: detectNativeBundle could not read the full container: " &
    (if fullDetect.isErr: fullDetect.unsafeError else: "")

  # And the refusal is attributable to the VERSION STAMP specifically: the
  # same fixture bytes, stamped version 5, get past the version gate and fail
  # on something else (their body is not a version-5 body). This is what pins
  # the mechanism — a reader that refused on, say, the zero block size would
  # refuse this one too.
  let misstamped = buildCompactFixture(streams, version = CtfsVersion)
  let misstampedPath = TmpDir / "compact_stamped_v5.ct"
  writeFixture(misstampedPath, misstamped)
  let misstampedOpen = openTrace(misstampedPath)
  doAssert misstampedOpen.isErr,
    "a compact body stamped version 5 was ACCEPTED, which would mean the " &
    "version gate is the only thing standing between this reader and a body " &
    "it cannot parse"
  doAssert not misstampedOpen.unsafeError.contains("version " & $CtfsVersionV6),
    "stamping the fixture version 5 still produced the version-6 refusal, " &
    "so the refusal is not keyed on the version byte at all. Got: " &
    misstampedOpen.unsafeError

  echo "PASS: test_an_unknowing_reader_refuses_a_compact_container"

# ---------------------------------------------------------------------------
# test_an_unknown_compression_scheme_is_refused_not_defaulted
# ---------------------------------------------------------------------------

proc test_an_unknown_compression_scheme_is_refused_not_defaulted()
    {.raises: [].} =
  ## The failure mode being excluded is a parser that treats unrecognised
  ## input as the permissive default. This campaign found
  ## `deploy-gate-decide.mjs` mapping BOTH an unset repository variable and a
  ## typo'd one onto the permissive value, so both the unknown and the empty
  ## case are exercised here, and `none` is what neither may produce.
  ##
  ## CONTROL: the whole enumerated set is exercised first, and each member
  ## must parse to itself. A parser that refused everything would satisfy the
  ## refusal assertions and fail this one.

  # --- CONTROL: every enumerated scheme parses, and parses to itself ---
  var seenSchemes = 0
  for scheme in CtfsWholeFileCompression:
    let parsed = parseWholeFileCompression(uint8(ord(scheme)))
    doAssert parsed.isOk,
      "enumerated scheme " & $scheme & " was refused by its own parser: " &
      parsed.error
    doAssert parsed.get() == scheme,
      "scheme " & $scheme & " parsed as " & $parsed.get()
    seenSchemes += 1
  doAssert seenSchemes == 2,
    "the closed set has " & $seenSchemes & " members; ctfs-container.md §1b " &
    "enumerates exactly two (none, zstd). A member added to the enum without " &
    "an implementation is the defect CCP-8 exists to prevent, so this count " &
    "is pinned rather than derived"

  var seenProfiles = 0
  for profile in CtfsProfile:
    let parsed = parseCtfsProfile(uint8(ord(profile)))
    doAssert parsed.isOk,
      "enumerated profile " & $profile & " was refused: " & parsed.error
    doAssert parsed.get() == profile,
      "profile " & $profile & " parsed as " & $parsed.get()
    seenProfiles += 1
  doAssert seenProfiles == 2, "the profile set has " & $seenProfiles & " members"

  # --- the UNKNOWN case: refused by name, and NOT mapped to none/full ---
  for bad in [2'u8, 3'u8, 7'u8, 42'u8, 255'u8]:
    let scheme = parseWholeFileCompression(bad)
    doAssert scheme.isErr,
      "scheme byte " & $bad & " is outside the closed set and was ACCEPTED"
    doAssert scheme.error.contains($bad),
      "the refusal of scheme " & $bad & " does not name the value. Got: " &
      scheme.error
    let prof = parseCtfsProfile(bad)
    doAssert prof.isErr,
      "profile byte " & $bad & " is outside the closed set and was ACCEPTED"
    doAssert prof.error.contains($bad),
      "the refusal of profile " & $bad & " does not name the value. Got: " &
      prof.error

  # The same thing through the header reader, which is the door a container
  # comes in by.
  let streams = collectStreams(writeFullContainer())
  let unknownScheme = buildCompactFixture(streams, compression = 9'u8)
  let schemeRead = readWholeFileCompression(unknownScheme)
  doAssert schemeRead.isErr,
    "a container declaring whole-file scheme 9 was accepted"
  doAssert schemeRead.error.contains("9"),
    "the refusal does not name scheme 9. Got: " & schemeRead.error

  let unknownProfile = buildCompactFixture(streams, profile = 4'u8)
  let profileRead = readCtfsProfile(unknownProfile)
  doAssert profileRead.isErr,
    "a container declaring profile 4 was accepted"
  doAssert profileRead.error.contains("4"),
    "the refusal does not name profile 4. Got: " & profileRead.error

  # --- the EMPTY case: a header too short to carry the field ---
  # `none` is byte 0 and "there is no byte" is not byte 0. A parser that
  # returns the permissive value for both has no way to report the second,
  # which is precisely what the deploy gate did with an unset variable.
  # The boundary is PER FIELD and is asserted as such. A 17-byte head carries
  # the profile byte and not the compression byte, so it must be answered for
  # one and refused for the other; demanding a refusal for both would be
  # testing a parser that cannot tell its own fields apart.
  let v6 = buildCompactFixture(streams)
  for truncateTo in [0, 5, 6, 7, 16, V6ProfileOffset, V6CompressionOffset,
                     V6HeaderSize - 1, V6HeaderSize]:
    var head = newSeq[byte](truncateTo)
    for i in 0 ..< truncateTo:
      head[i] = v6[i]

    let scheme = readWholeFileCompression(head)
    if truncateTo > V6CompressionOffset:
      doAssert scheme.isOk and scheme.get() == wfcNone,
        "a " & $truncateTo & "-byte head DOES carry the compression byte and " &
        "it says 0, so it must read as wfcNone: " &
        (if scheme.isErr: scheme.error else: "got " & $scheme.get())
    else:
      doAssert scheme.isErr,
        "a " & $truncateTo & "-byte head does not reach offset " &
        $V6CompressionOffset & " and was still accepted as a whole-file " &
        "scheme declaration"
      doAssert not (scheme.isOk and scheme.get() == wfcNone),
        "a " & $truncateTo & "-byte head mapped to wfcNone — an absent field " &
        "read as the permissive value"

    let prof = readCtfsProfile(head)
    if truncateTo > V6ProfileOffset:
      doAssert prof.isOk and prof.get() == cpCompact,
        "a " & $truncateTo & "-byte head DOES carry the profile byte and it " &
        "says 1, so it must read as cpCompact: " &
        (if prof.isErr: prof.error else: "got " & $prof.get())
    else:
      doAssert prof.isErr,
        "a " & $truncateTo & "-byte head does not reach offset " &
        $V6ProfileOffset & " and was still accepted as a profile declaration"
      doAssert not (prof.isOk and prof.get() == cpFull),
        "a " & $truncateTo & "-byte head mapped to cpFull"

    let reserved = checkV6Reserved(head)
    if truncateTo < 6:
      # Below the version byte this check has no way to know it is looking at
      # a v6 header, and the version gate — not this one — owns that case. It
      # is the single place where a short buffer gets a permissive answer, and
      # it is stated here so it is a decision rather than an oversight.
      doAssert reserved.isOk,
        "a " & $truncateTo & "-byte head cannot be classified by this check " &
        "at all, so it must defer rather than invent a verdict"
    elif truncateTo < V6HeaderSize:
      doAssert reserved.isErr,
        "a " & $truncateTo & "-byte head declares version " &
        $CtfsVersionV6 & " and passed the reserved-bytes check, which it " &
        "cannot have read in full"
    else:
      doAssert reserved.isOk,
        "the whole 24-byte header was refused by the reserved-bytes check: " &
        (if reserved.isErr: reserved.error else: "")

  # --- non-zero reserved bytes are refused, by offset and by value ---
  for i in 0 ..< V6ReservedLen:
    var poisoned = v6
    poisoned[V6ReservedOffset + i] = 0x5A'u8
    let res = checkV6Reserved(poisoned)
    doAssert res.isErr,
      "reserved byte at offset " & $(V6ReservedOffset + i) & " was 0x5A and " &
      "the container was accepted. 'Ignored' and 'unknown' are the same byte"
    doAssert res.error.contains($(V6ReservedOffset + i)),
      "the refusal does not name the offending offset. Got: " & res.error
  let cleanReserved = checkV6Reserved(v6)
  doAssert cleanReserved.isOk,
    "the unpoisoned fixture was refused by the reserved-bytes check: " &
    (if cleanReserved.isErr: cleanReserved.error else: "")

  # --- a KNOWN version is an inference, not a default ---
  # Version 5 answers full/none because its body is specified and admits no
  # whole-file scheme. That is the one case where a byte-less answer is
  # legitimate, and it is legitimate BECAUSE the version is the one this
  # library reads — not because the parser fell back.
  let fullBytes = writeFullContainer()
  let p = readCtfsProfile(fullBytes)
  doAssert p.isOk and p.get() == cpFull,
    "a version-5 container did not read as the full profile"
  let c = readWholeFileCompression(fullBytes)
  doAssert c.isOk and c.get() == wfcNone,
    "a version-5 container did not read as whole-file scheme none"

  # ... and an UNREAD version does not get that inference. This is the line
  # between the two: same missing byte, different answer, decided by whether
  # the version is the one this library reads. Version 4 is included because
  # it is the version this library wrote until recently, so "an old container"
  # and "a container with no profile byte" must not answer the same.
  for alien in [4'u8, 9'u8]:
    var alienVersion = fullBytes
    alienVersion[5] = alien
    doAssert readCtfsProfile(alienVersion).isErr,
      "version " & $alien & " was given the full-profile inference that " &
      "version 5 gets, so the inference is a fallback after all"
    doAssert readWholeFileCompression(alienVersion).isErr,
      "version " & $alien & " was given the scheme-none inference"


  echo "PASS: test_an_unknown_compression_scheme_is_refused_not_defaulted"

# ---------------------------------------------------------------------------

when isMainModule:
  try:
    removeDir(TmpDir)
    createDir(TmpDir)
  except CatchableError as e:
    doAssert false, "could not prepare " & TmpDir & ": " & e.msg
  except Defect as e:
    doAssert false, "could not prepare " & TmpDir & ": " & e.msg

  test_an_unknowing_reader_refuses_a_compact_container()
  test_an_unknown_compression_scheme_is_refused_not_defaulted()

  # `CCP1_KEEP_FIXTURES=1` leaves `full.ct` and `compact.ct` behind. CCP-1's
  # fifth deliverable is a reader built BEFORE this milestone shown to refuse a
  # compact container BY RUNNING IT, and that measurement needs the two
  # containers as files to feed an older binary. Reproducing it should not
  # require editing this file, so the hook is here rather than in a note.
  var keep = false
  try:
    keep = getEnv("CCP1_KEEP_FIXTURES").len > 0
  except CatchableError:
    discard
  except Defect:
    discard
  if keep:
    echo "CCP1_KEEP_FIXTURES: left " & (TmpDir / "full.ct") & " and " &
      (TmpDir / "compact.ct") & " in place"
  else:
    try:
      removeDir(TmpDir)
    except CatchableError:
      discard
    except Defect:
      discard
  echo "All CCP-1 compact-profile header refusal tests passed!"
