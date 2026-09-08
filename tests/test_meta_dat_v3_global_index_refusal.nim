## A container written before the global line index correction must be
## refused, not read one line high.
##
## The line-only `global_position_index` encode changed from
## `prefixSum[path_id] + line` to `prefixSum[path_id] + (line - 1)` — the
## exact inverse of the decode the spec states
## (`codetracer-trace-format-spec` branch `zk/gli-off-by-one`, commit
## `becd9e1`). The two encodes differ by one at every step, and nothing in
## the bytes distinguishes them: both put every step at an address the
## trace's own space can address, so `tryResolve` has nothing to refuse.
## A container written under the old encode and read under the new decode
## therefore answers a plausible `(path, line)` pair for every step, each
## one line too high, and returns success.
##
## That is unlike the rival Rust packing `(path_id shl 32) or line`, which
## `test_line_only_position_space.nim` covers: that one lands OUTSIDE the
## space, which is the only reason a reader can catch it. Being inside the
## space is what makes this one need a discriminator rather than a check.
##
## `meta.dat`'s schema version is that discriminator — the only field that
## can be one. `recorder_id` names the producer, not its address packing,
## and there is no stride field, no per-file line count and no packing id.
## So the encode change is a schema break: `MetaDatVersion` is 4, and a
## container at `LastShiftedGlobalIndexVersion` (3) or below is refused.
##
## **How the v3 container here is built, and why it is the real thing.**
## For a recorded line `L`, the old encode emitted `prefixSum + L`; the
## current encode emits `prefixSum + (L - 1)`. So today's writer, handed
## `L + 1`, puts `prefixSum + L` on the wire — the very integer the old
## writer wrote for `L`. `writeShiftedContainer` uses that identity and
## `test_v3_container_carries_the_superseded_addresses` PROVES it, by
## checking the on-wire addresses against `prefixSum[path] + line`
## recomputed from the recorded lines. Without that proof the fixture
## would just be a container with different numbers in it, and every
## assertion below would be about an artefact rather than about the
## defect. The version field is then stamped back to 3 in the one place
## `meta.dat` stores it.
##
## What is asserted here, and why each one can fail:
##
##   1. `readMetaDat` refuses a v3 buffer, and the refusal NAMES the cause
##      and the remedy — it is what a user sees, so an unreadable message
##      is a failure. A plain "unsupported version 3" fails this.
##   2. `openNewTraceFromBytes` and `openNewTrace` propagate that refusal.
##      They are the gate that mattered: before this change the reader
##      parsed `meta.dat` and DISCARDED the error, so a bumped version
##      alone would have opened a v3 container with a zeroed `meta` and
##      read it one line high anyway.
##   3. The C ABI's `ct_reader_open` — the door `codetracer/src/db-backend`
##      comes through — returns nil and leaves the reason in
##      `trace_writer_last_error`.
##   4. It DISCRIMINATES: the same container at version 4 opens, and its
##      steps read back at exactly the lines they were recorded at. A gate
##      that refused everything passes 1-3 and fails this.
##   5. The mutation control, `test_without_the_gate_the_v3_container_reads_one_line_high`:
##      resolving the v3 container's own on-wire addresses through the
##      trace's address space — which is precisely what the reader does
##      once the version gate is removed — yields every step one line
##      high, and succeeds while doing it. This pins the CONSEQUENCE of
##      dropping the gate, without pinning the buggy lines as correct:
##      the assertion is `decoded == recorded + 1`, stated as the defect.
##
## No mocks: the containers are produced by this repository's own writer
## and read back through the real reader and the real C entry points.

# Include the FFI module so the C entry points can be driven directly.
# Mirrors tests/test_reader_ffi_line_only_position_space.nim.
include codetracer_trace_writer_ffi

# Drop the `raises: []` push from the FFI module so the test body can use
# higher-level helpers.
{.pop.}

import std/strutils

const
  PathA = "/src/app.rb"
  PathB = "/src/helper.rb"
  TmpDir = "tmp_meta_dat_v3_refusal"

  RecordedSteps: array[3, (uint64, uint64)] = [
    (0'u64, 3'u64), (1'u64, 7'u64), (0'u64, 12'u64)
  ]
    ## The `(path_id, line)` pairs every case below records. Two paths, and
    ## a step on each side of the file boundary, so an off-by-one that only
    ## showed up in file 0 could not hide.

proc lineOnlySpace(pathCount: int): GlobalLineIndex =
  ## The address space a line-only container of `pathCount` paths has.
  ## Sized through the writer's own constant, so a drift in
  ## `DefaultLinesPerFile` cannot make this test agree with a reader that
  ## has moved.
  var counts = newSeq[uint64](pathCount)
  for i in 0 ..< pathCount:
    counts[i] = DefaultLinesPerFile
  buildGlobalLineIndex(counts)

proc writeContainer(file: string, steps: openArray[(uint64, uint64)]) =
  ## A line-only container (no `enableColumnAwareSteps`) over two paths.
  var w = initMultiStreamWriter(file & ".build", "meta_dat_v3_refusal").get()
  doAssert w.registerPath(PathA).isOk
  doAssert w.registerPath(PathB).isOk
  for (p, l) in steps:
    doAssert w.registerStep(p, l, @[]).isOk
  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()
  writeFile(file, cast[string](bytes))

proc writeCurrentContainer(file: string) =
  ## `RecordedSteps` written by the current writer: a v4 container whose
  ## addresses are `prefixSum + (line - 1)`.
  writeContainer(file, RecordedSteps)

proc writeShiftedContainer(file: string) =
  ## `RecordedSteps` as the PRE-CORRECTION writer would have written them.
  ##
  ## Each line is recorded one higher, which makes the current encode emit
  ## the address the old encode emitted for the real line — see the header,
  ## and `test_v3_container_carries_the_superseded_addresses`, which checks
  ## the resulting addresses against the old formula rather than trusting
  ## this comment.
  var shifted: seq[(uint64, uint64)]
  for (p, l) in RecordedSteps:
    shifted.add((p, l + 1))
  writeContainer(file, shifted)

proc stampVersion(file: string, version: uint16) =
  ## Rewrite the `meta.dat` schema version in an existing container, and
  ## touch nothing else.
  ##
  ## `meta.dat` is stored uncompressed, and its `CTMD` magic occurs exactly
  ## once in a container this small; both are asserted rather than assumed,
  ## because a silent miss would leave the file at the current version and
  ## every refusal below would be testing nothing.
  ##
  ## That only the version field moves is also asserted, and it is load
  ## bearing: the step addresses are read off the container BEFORE it is
  ## stamped (a stamped one cannot be opened, which is the point), so those
  ## addresses are the stamped container's addresses only if stamping left
  ## the streams alone.
  let before = readFile(file)
  var raw = before
  let i = raw.find("CTMD")
  doAssert i >= 0, "no CTMD magic in " & file
  doAssert raw.count("CTMD") == 1,
    "CTMD is not unique in " & file & "; cannot stamp the version"
  raw[i + 4] = char(version and 0xFF)
  raw[i + 5] = char((version shr 8) and 0xFF)

  doAssert raw.len == before.len, "stamping changed the container's length"
  var changed: seq[int]
  for k in 0 ..< raw.len:
    if raw[k] != before[k]:
      changed.add(k)
  doAssert changed.len > 0, "stamping version " & $version & " changed nothing"
  for k in changed:
    doAssert k == i + 4 or k == i + 5,
      "stamping changed byte " & $k & ", outside the meta.dat version field"

  writeFile(file, raw)

proc onWireAddresses(file: string): seq[uint64] =
  ## The `global_position_index` of every step in an OPENABLE container.
  ## Callers that want a v3 container's addresses read them here before
  ## stamping the version down — see `stampVersion`.
  var reader = openNewTrace(file).get()
  let n = reader.stepCount().get()
  result = newSeq[uint64](n)
  doAssert reader.stepAbsoluteGlobalLineIndices(0'u64, n, result).get() == n

proc writeV3Container(file: string) =
  ## A faithful pre-correction container: the superseded addresses, under
  ## the superseded schema version.
  writeShiftedContainer(file)
  stampVersion(file, LastShiftedGlobalIndexVersion)

# ---------------------------------------------------------------------------

proc test_v3_container_carries_the_superseded_addresses() =
  ## The fixture is the real thing: its on-wire addresses are exactly
  ## `prefixSum[path_id] + line` for the RECORDED lines — the formula this
  ## repository's writer used before the correction — and they differ by
  ## one from what the current writer emits for the same lines.
  ##
  ## Everything below rests on this. If the shift-by-one construction ever
  ## stopped reproducing the old encode, the other cases would go on
  ## passing while testing a container no writer ever produced.
  let v3 = TmpDir / "carries.ct"
  writeShiftedContainer(v3)   # version left at 4: this case is about addresses
  let current = TmpDir / "carries_current.ct"
  writeCurrentContainer(current)

  let space = lineOnlySpace(2)
  let onWire = onWireAddresses(v3)
  let currentOnWire = onWireAddresses(current)
  doAssert onWire.len == RecordedSteps.len
  doAssert currentOnWire.len == RecordedSteps.len

  for i, (p, l) in RecordedSteps:
    let supersededAddress = space.prefixSum[p] + l          # + line
    let currentAddress = space.globalIndex(int(p), l)       # + (line - 1)
    doAssert onWire[i] == supersededAddress,
      "step " & $i & ": the v3 fixture carries " & $onWire[i] &
      ", but the superseded encode of (path " & $p & ", line " & $l &
      ") is " & $supersededAddress
    doAssert currentOnWire[i] == currentAddress,
      "step " & $i & ": the current writer emitted " & $currentOnWire[i] &
      " for (path " & $p & ", line " & $l & "), expected " & $currentAddress
    doAssert onWire[i] == currentOnWire[i] + 1,
      "step " & $i & ": the two encodes must differ by exactly one, got " &
      $onWire[i] & " and " & $currentOnWire[i]

  echo "PASS: test_v3_container_carries_the_superseded_addresses"

proc test_read_meta_dat_refuses_v3_by_name() =
  ## `readMetaDat` refuses, and the message says what changed, what the
  ## consequence of reading it anyway would be, and what to do about it.
  let v3 = TmpDir / "meta.ct"
  writeV3Container(v3)

  let data = readCtfsFromFile(v3)
  doAssert data.isOk
  let meta = readInternalFile(data.get(), "meta.dat", DefaultBlockSize,
    DefaultMaxRootEntries)
  doAssert meta.isOk, "the fixture must still HAVE a meta.dat"

  let parsed = readMetaDat(meta.get())
  doAssert parsed.isErr,
    "readMetaDat accepted a version-" & $LastShiftedGlobalIndexVersion &
    " meta.dat"
  let msg = parsed.error
  # The cause, in terms the reader can act on. Asserted piece by piece
  # because "it returned an error" is not the contract — a user meeting an
  # unreadable trace has to learn WHY and WHAT TO DO, and a bare
  # "unsupported version 3" tells them neither.
  doAssert "schema version 3" in msg, "the refusal must name the version: " & msg
  doAssert "global line index" in msg,
    "the refusal must name what changed: " & msg
  doAssert "prefixSum[path_id] + line" in msg and
      "prefixSum[path_id] + (line - 1)" in msg,
    "the refusal must show both encodes: " & msg
  doAssert "one line high" in msg,
    "the refusal must say what reading it anyway would do: " & msg
  doAssert "Re-record" in msg, "the refusal must name the remedy: " & msg

  echo "PASS: test_read_meta_dat_refuses_v3_by_name"

proc test_open_refuses_v3_and_accepts_v4() =
  ## Both directions, at both container entry points. The v3 arm is the
  ## regression; the v4 arm is what stops a gate that refuses everything
  ## from passing.
  let v3 = TmpDir / "open_v3.ct"
  writeV3Container(v3)
  let v4 = TmpDir / "open_v4.ct"
  writeCurrentContainer(v4)

  block fromFile:
    let opened = openNewTrace(v3)
    doAssert opened.isErr, "openNewTrace opened a v3 container"
    doAssert "global line index" in opened.error,
      "openNewTrace must propagate the named refusal, got: " & opened.error

  block fromBytes:
    let raw = readFile(v3)
    var bytes = newSeq[byte](raw.len)
    for i, ch in raw:
      bytes[i] = byte(ch)
    let opened = openNewTraceFromBytes(bytes)
    doAssert opened.isErr, "openNewTraceFromBytes opened a v3 container"
    doAssert "global line index" in opened.error,
      "openNewTraceFromBytes must propagate the named refusal, got: " &
      opened.error

  block currentStillOpens:
    let opened = openNewTrace(v4)
    doAssert opened.isOk,
      "a current container must still open: " & opened.error

  echo "PASS: test_open_refuses_v3_and_accepts_v4"

proc test_ffi_reader_open_refuses_v3_and_accepts_v4() =
  ## The C ABI is how `codetracer/src/db-backend` opens a trace, so the
  ## refusal has to survive the boundary rather than stop at the Nim API.
  let v3 = TmpDir / "ffi_v3.ct"
  writeV3Container(v3)
  let v4 = TmpDir / "ffi_v4.ct"
  writeCurrentContainer(v4)

  block refused:
    let h = ct_reader_open(v3.cstring)
    doAssert h.isNil, "ct_reader_open returned a handle for a v3 container"
    let err = $trace_writer_last_error()
    doAssert "global line index" in err,
      "the C ABI must leave the named refusal in trace_writer_last_error, " &
      "got: " & err
    doAssert "Re-record" in err, "the remedy must survive the boundary: " & err

  block accepted:
    let h = ct_reader_open(v4.cstring)
    doAssert not h.isNil,
      "ct_reader_open refused a current container: " & $trace_writer_last_error()
    doAssert ct_reader_step_count(h) == uint64(RecordedSteps.len)
    ct_reader_close(h)

  echo "PASS: test_ffi_reader_open_refuses_v3_and_accepts_v4"

proc test_current_container_reads_back_at_the_recorded_lines() =
  ## The positive control, and the property the old encode violated: a step
  ## recorded at line L reads back at line L. Multi-path, so a step in file
  ## 1 has to come back out of file 1.
  let v4 = TmpDir / "roundtrip.ct"
  writeCurrentContainer(v4)

  var reader = openNewTrace(v4).get()
  let n = reader.stepCount().get()
  doAssert n == uint64(RecordedSteps.len)
  var addrs = newSeq[uint64](n)
  doAssert reader.stepAbsoluteGlobalLineIndices(0'u64, n, addrs).get() == n

  let space = lineOnlySpace(2)
  for i, (p, l) in RecordedSteps:
    let resolved = space.tryResolve(addrs[i])
    doAssert resolved.isOk,
      "step " & $i & " did not resolve: " & resolved.error
    let (gotPath, gotLine) = resolved.get()
    doAssert uint64(gotPath) == p,
      "step " & $i & ": path " & $gotPath & ", recorded on path " & $p
    doAssert gotLine == l,
      "step " & $i & ": read back at line " & $gotLine &
      ", recorded at line " & $l

  echo "PASS: test_current_container_reads_back_at_the_recorded_lines"

proc test_without_the_gate_the_v3_container_reads_one_line_high() =
  ## MUTATION CONTROL. Resolving the v3 container's own addresses through
  ## the trace's address space is exactly what the reader does once the
  ## version gate is removed — the gate is the only thing standing between
  ## a v3 container and this arithmetic.
  ##
  ## Every step then resolves, to the right FILE and to a line one too
  ## high, and nothing reports an error. That is the defect: not a failure
  ## a user could notice, an answer they could not tell from a right one.
  ##
  ## The assertion is `decoded == recorded + 1`, phrased as the defect, so
  ## it cannot be mistaken for a specification of correct behaviour. If the
  ## version gate is deleted, `test_open_refuses_v3_and_accepts_v4` goes
  ## red and this case stays green, naming what the deletion costs.
  let v3 = TmpDir / "mutation.ct"
  writeShiftedContainer(v3)

  let space = lineOnlySpace(2)
  # Read before stamping: a stamped container cannot be opened, which is
  # exactly the property under test. `stampVersion` proves the addresses
  # survive the stamp untouched.
  let onWire = onWireAddresses(v3)
  stampVersion(v3, LastShiftedGlobalIndexVersion)
  doAssert openNewTrace(v3).isErr,
    "the mutation control must run against a container the gate refuses"
  doAssert onWire.len == RecordedSteps.len

  for i, (p, l) in RecordedSteps:
    let resolved = space.tryResolve(onWire[i])
    doAssert resolved.isOk,
      "step " & $i & ": the whole point is that this does NOT fail — an " &
      "address from the superseded encode is inside the space, so there " &
      "is nothing for tryResolve to refuse. Got: " & resolved.error
    let (gotPath, gotLine) = resolved.get()
    doAssert uint64(gotPath) == p,
      "step " & $i & ": the file is right; only the line is wrong. Got " &
      "path " & $gotPath & ", recorded on path " & $p
    doAssert gotLine == l + 1,
      "step " & $i & ": without the version gate a v3 container reads ONE " &
      "LINE HIGH. Recorded at line " & $l & ", expected to decode as " &
      $(l + 1) & ", got " & $gotLine

  echo "PASS: test_without_the_gate_the_v3_container_reads_one_line_high"

# ---------------------------------------------------------------------------

when isMainModule:
  removeDir(TmpDir)
  createDir(TmpDir)

  test_v3_container_carries_the_superseded_addresses()
  test_read_meta_dat_refuses_v3_by_name()
  test_open_refuses_v3_and_accepts_v4()
  test_ffi_reader_open_refuses_v3_and_accepts_v4()
  test_current_container_reads_back_at_the_recorded_lines()
  test_without_the_gate_the_v3_container_reads_one_line_high()

  removeDir(TmpDir)
  echo "All meta.dat v3 global-index refusal tests passed!"
