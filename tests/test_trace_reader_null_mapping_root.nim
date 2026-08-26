{.push raises: [].}

## The §5d block-number bound in the **second** Nim transcription of the §4
## walk: `src/codetracer_trace_reader.nim`'s `readInternalFile`.
##
## `codetracer_ctfs/container.nim` has the same walk and has been bounded since
## M58 (`tests/test_partial_tail_bounds.nim`). This one had not been, and the
## two disagreed about the same bytes — the campaign's recurring shape, one
## repo's two copies of a rule instead of two repos'.
##
## What it was missing, precisely:
##
## - **The mapping root's null branch.** `findInternalFileEntry` returns
##   `(0, 0)` for "no such name", and the guard was the *conjunction*
##   `fileSize == 0 and mapBlock == 0`. An entry that exists, declares a
##   `Size > 0` and carries a null mapping root — the state a crash between
##   publishing an entry's size and publishing its mapping root leaves — is
##   neither "absent" nor caught, so the walk ran with `currentLevelBlock = 0`
##   and read **block 0, the header and root directory, as the stream's
##   mapping table**. The entry fields then decode as data-block pointers.
## - **Any block-number bound at all.** Only byte offsets were checked, against
##   `data.len`. §5d is explicit that this is not the bound: the final data
##   block's copy is clamped to the entry's `Size`, so a short read out of the
##   partial region succeeds.
##
## Both were measured through the public `openTrace` before being fixed, and
## the second one was worse than "wrong bytes":
##
## - nulling `meta.dat`'s mapping root left `openTrace` returning **ok** — the
##   damaged trace opened, and the loss was silent;
## - nulling another entry's crashed the process outright with an unhandled
##   `OverflowDefect` from `int(dataBlock) * int(blockSize)`, because
##   `dataBlock` had been read out of block 0 and was ~4.6e15. A `Result`-typed
##   API killing its caller on a damaged input is not an error path.
##
## NO MOCKS. Every container here is produced by the production
## `codetracer_trace_writer`, written to a real file, damaged by editing the
## real bytes of its root directory the way a torn write damages them, and read
## back through the public `openTrace`. Nothing stubs a reader, a writer or an
## I/O error, and no private proc is reached into.

import std/[os, strutils]
import results
import codetracer_trace_reader
import codetracer_trace_writer

const
  EntryArrayOffset = 16
  FileEntrySize = 24

proc tmpDir(name: string): string {.raises: [].} =
  result = getTempDir() / ("ctfs_nullroot_" & name & "_" & $getCurrentProcessId())
  try:
    removeDir(result)
    createDir(result)
  except OSError, IOError:
    doAssert false, "cannot prepare " & result

proc dropDir(path: string) {.raises: [].} =
  try:
    removeDir(path)
  except OSError:
    discard

proc readBytes(path: string): seq[byte] {.raises: [].} =
  try:
    let s = readFile(path)
    result = newSeq[byte](s.len)
    for i in 0 ..< s.len:
      result[i] = byte(s[i])
  except IOError, OSError:
    doAssert false, "cannot read " & path

proc writeBytes(path: string, data: seq[byte]) {.raises: [].} =
  try:
    var s = newString(data.len)
    for i in 0 ..< data.len:
      s[i] = char(data[i])
    writeFile(path, s)
  except IOError, OSError:
    doAssert false, "cannot write " & path

proc u64le(d: openArray[byte], off: int): uint64 {.raises: [].} =
  for i in countdown(7, 0):
    result = (result shl 8) or uint64(d[off + i])

## A real trace, large enough that its streams span several blocks and the
## container needs more than a root block plus one mapping block.
proc writeRealTrace(path: string) {.raises: [].} =
  let wr = newTraceWriter(path, "prog", @["a"], workdir = "/tmp/w")
  doAssert wr.isOk, "newTraceWriter failed"
  var w = wr.get()
  doAssert w.writePath("/src/main.nim").isOk, "writePath"
  for i in 0 ..< 400:
    doAssert w.writeStep(0, i).isOk, "writeStep " & $i
  doAssert w.close().isOk, "close"

## Every populated root-directory slot, as (index, size, mapBlock).
iterator populatedEntries(raw: seq[byte]): (int, uint64, uint64) {.raises: [].} =
  for i in 0 ..< 64:
    let off = EntryArrayOffset + i * FileEntrySize
    if off + FileEntrySize > raw.len:
      break
    if u64le(raw, off + 16) != 0'u64:
      yield (i, u64le(raw, off), u64le(raw, off + 8))

# ---------------------------------------------------------------------------

## Everything the public API will say about a trace, as one comparable string.
## Used as the undamaged control: a reader that accepts a damaged container
## must at least return *the same answers*, or it has degraded silently.
proc observe(path: string): tuple[opened: bool, err: string,
                                  meta: string, events: string] {.raises: [].} =
  let r = openTrace(path)
  if r.isErr:
    return (false, r.unsafeError, "", "")
  var reader = r.get()
  let ev = reader.readEvents()
  if ev.isErr:
    return (false, ev.unsafeError, "", "")
  (true, "", reader.toJson(), reader.toJsonEvents())

proc test_a_null_mapping_root_is_reported_and_never_silently_degrades() {.raises: [].} =
  ## THE test. For **every** stream in a real trace, in turn: null that one
  ## entry's mapping root and drive the container through the public API.
  ##
  ## Sweeping every entry rather than picking one is deliberate. Which failure
  ## a null root produces depends on what block 0's bytes happen to decode to
  ## at that stream's block indices — a refusal for some, wrong content for
  ## others, an arithmetic overflow for others again — so a single-entry
  ## fixture pins whichever one it happened to land on and lets the rest
  ## regress silently.
  ##
  ## The assertion is deliberately **not** "every nulled entry must produce an
  ## error". Some streams (`events.fmt`) this reader never reads, so nulling
  ## them is legitimately unobservable, and demanding an error would either be
  ## false or need an allowlist that rots. What is asserted instead is that
  ## each damaged container is either *refused, naming the damage*, or answers
  ## **exactly what the undamaged one answers**. That is what a silent
  ## degradation fails: before the fix, nulling `meta.json` left `openTrace`
  ## returning ok with an empty program name and no source paths — accepted,
  ## and different.
  let dir = tmpDir("sweep")
  let path = dir / "trace.ct"
  writeRealTrace(path)

  let clean = readBytes(path)
  doAssert clean.len mod 4096 == 0,
    "the sealed container is not block-aligned; the fixture proves nothing"

  let control = observe(path)
  doAssert control.opened,
    "the undamaged control does not read, so nothing below is about the damage: " &
    control.err
  doAssert control.meta.len > 0 and control.events.len > 0,
    "the control observed nothing, so an equality against it would pass vacuously"

  var swept = 0
  var refused = 0
  for (idx, size, mapBlock) in populatedEntries(clean):
    if size == 0'u64 or mapBlock == 0'u64:
      continue
    swept += 1

    var damaged = clean
    let off = EntryArrayOffset + idx * FileEntrySize
    for i in 0 ..< 8:
      damaged[off + 8 + i] = 0'u8
    let victim = dir / ("nulled_" & $idx & ".ct")
    writeBytes(victim, damaged)

    # Structural first: the call has to *return*. An unhandled `OverflowDefect`
    # out of a `Result`-typed entry point kills the caller, and a test that
    # only inspected messages would never run far enough to see it.
    let got = observe(victim)

    if not got.opened:
      refused += 1
      doAssert "mapping root" in got.err,
        "entry " & $idx & "'s refusal does not say it was the mapping root: " & got.err
      doAssert "null" in got.err,
        "entry " & $idx & "'s refusal does not say the pointer is null: " & got.err
      continue

    doAssert got.meta == control.meta,
      "the reader accepted a container whose entry " & $idx & " declares " &
      $size & " bytes and carries a null mapping root, and answered with " &
      "different metadata than the undamaged trace — block 0 is the root " &
      "directory, so the loss was filled in from the container's own header " &
      "rather than reported\n  undamaged: " & control.meta & "\n  damaged:   " & got.meta
    doAssert got.events == control.events,
      "the reader accepted a container whose entry " & $idx &
      " carries a null mapping root and answered with different events than " &
      "the undamaged trace"

  doAssert swept >= 3,
    "the fixture only produced " & $swept & " damageable streams; it is too " &
    "small to exercise the walk"
  doAssert refused >= 1,
    "no nulled entry was refused at all, so the sweep is only asserting " &
    "equality and would pass against a reader with no bound whatsoever"

  dropDir(dir)
  echo "PASS: test_a_null_mapping_root_is_reported_and_never_silently_degrades (" &
    $swept & " streams swept, " & $refused & " refused)"

proc test_a_block_number_past_the_container_is_refused_not_multiplied() {.raises: [].} =
  ## The bound, separately from the null. A mapping root that is non-zero but
  ## names a block the container does not have must be refused *before* it
  ## reaches `int(dataBlock) * int(blockSize)` — the multiplication that
  ## overflowed and crashed the process when the number came out of block 0.
  let dir = tmpDir("bound")
  let path = dir / "trace.ct"
  writeRealTrace(path)

  let clean = readBytes(path)
  let wholeBlocks = uint64(clean.len div 4096)

  let control = observe(path)
  doAssert control.opened, "the undamaged control does not read: " & control.err

  var swept = 0
  var refused = 0
  for (idx, size, mapBlock) in populatedEntries(clean):
    if size == 0'u64 or mapBlock == 0'u64:
      continue
    swept += 1

    var damaged = clean
    let off = EntryArrayOffset + idx * FileEntrySize
    # A number far past the container, and far enough that `int * blockSize`
    # would overflow a 64-bit signed multiply if it were ever performed.
    let huge = 0x0010_0000_0000_0000'u64
    var v = huge
    for i in 0 ..< 8:
      damaged[off + 8 + i] = byte(v and 0xff'u64)
      v = v shr 8
    let victim = dir / ("huge_" & $idx & ".ct")
    writeBytes(victim, damaged)

    # Same discipline as the sweep above: refused naming the damage, or
    # answering exactly what the undamaged trace answers. The one thing that
    # must never happen is the multiply — `int(4503599627370496) * 4096`
    # overflows and killed the process before the bound existed, and a
    # returning call is what the equality below is even able to check.
    let got = observe(victim)
    if not got.opened:
      refused += 1
      doAssert "out of bounds" in got.err,
        "entry " & $idx & "'s refusal does not name the bound: " & got.err
      doAssert "mapping root" in got.err,
        "entry " & $idx & "'s refusal does not say it was the mapping root: " & got.err
      continue

    doAssert got.meta == control.meta and got.events == control.events,
      "the reader accepted a trace whose entry " & $idx &
      " names mapping root block " & $huge & " against " & $wholeBlocks &
      " whole blocks, and answered differently than the undamaged trace"

  doAssert swept >= 3, "too few streams swept: " & $swept
  doAssert refused >= 1,
    "no entry was refused, so this test is only asserting equality and would " &
    "pass against a reader with no bound at all"

  dropDir(dir)
  echo "PASS: test_a_block_number_past_the_container_is_refused_not_multiplied (" &
    $swept & " swept, " & $refused & " refused)"

proc test_a_data_block_past_the_container_is_refused_not_served() {.raises: [].} =
  ## The data-block bound, which the two tests above do **not** reach: their
  ## damage is at the mapping root, so the root's own guards fire first and the
  ## walk never gets as far as a data block. Added after mutation testing found
  ## that removing the data-block bound left both of them green.
  ##
  ## §5d's shape: cut the container inside its final block, so the last data
  ## block of some stream lies outside the whole blocks while every byte the
  ## reader would copy is still physically present — the clamped copy that a
  ## byte-offset check satisfies and a block-number bound does not.
  let dir = tmpDir("cut")
  let path = dir / "trace.ct"
  writeRealTrace(path)

  let clean = readBytes(path)
  doAssert clean.len mod 4096 == 0, "the sealed container is not block-aligned"

  let control = observe(path)
  doAssert control.opened, "the undamaged control does not read: " & control.err

  # Keep the final block's first 100 bytes: present on disk, but in the
  # partial region, so `floor(len / blockSize)` no longer covers its block.
  let cut = (clean.len - 4096) + 100
  let victim = dir / "cut.ct"
  writeBytes(victim, clean[0 ..< cut])

  let got = readBytes(victim)
  doAssert got.len mod 4096 != 0,
    "the truncated fixture is still block-aligned, so it does not exercise " &
    "the partial region"
  doAssert (got.len div 4096) == (clean.len div 4096) - 1,
    "the cut did not move the last block outside the whole blocks"

  let after = observe(victim)
  doAssert not after.opened,
    "the reader served a truncated container whose last data block lies " &
    "outside its " & $(got.len div 4096) & " whole blocks, and reported success"
  doAssert "out of bounds" in after.err,
    "the refusal does not name the bound: " & after.err
  doAssert "truncated" in after.err,
    "the refusal does not say the container is truncated: " & after.err

  dropDir(dir)
  echo "PASS: test_a_data_block_past_the_container_is_refused_not_served"

proc test_an_empty_stream_is_not_an_absent_one() {.raises: [].} =
  ## The other half of splitting the conjunction, and the reason the split is
  ## not merely cosmetic. `findInternalFileEntry` used to answer `(0, 0)` both
  ## for "no such name" and for a stream that exists and is empty, and the
  ## caller's `fileSize == 0 and mapBlock == 0` turned the second into
  ## "internal file not found".
  ##
  ## Added after mutation testing: restoring the conjunction left every other
  ## case in this file green, because a nulled entry has `Size > 0` and is
  ## caught by the null-mapping-root guard instead.
  let dir = tmpDir("empty")
  let path = dir / "trace.ct"
  writeRealTrace(path)

  var raw = readBytes(path)
  # Make one existing entry an empty stream: size 0, mapping root 0 — exactly
  # what a legitimately empty member looks like in the root directory.
  var idx = -1
  for (i, size, mapBlock) in populatedEntries(raw):
    if size > 0'u64 and mapBlock > 0'u64:
      idx = i
      break
  doAssert idx >= 0, "no populated entry to empty"
  let off = EntryArrayOffset + idx * FileEntrySize
  for i in 0 ..< 16:
    raw[off + i] = 0'u8

  let victim = dir / "empty.ct"
  writeBytes(victim, raw)

  # The reader must not crash, and must not claim the *container* is broken:
  # an empty member is a valid container, and the entry is still named in the
  # directory, so "internal file not found" would be a different fact.
  let got = observe(victim)
  if not got.opened:
    doAssert "not found" notin got.err,
      "an entry that exists and is empty was reported as an absent one: " & got.err

  dropDir(dir)
  echo "PASS: test_an_empty_stream_is_not_an_absent_one"

proc test_an_undamaged_trace_is_unaffected_by_the_bounds() {.raises: [].} =
  ## The negative control. The two rules above must be invisible to every
  ## ordinary trace, or they degenerate into "refuse anything unusual" — the
  ## answer §5d removed from the Go reader.
  let dir = tmpDir("control")
  let path = dir / "trace.ct"
  writeRealTrace(path)

  var r = openTrace(path)
  doAssert r.isOk, "the bound refused an undamaged trace: " & r.unsafeError
  var reader = r.get()
  doAssert reader.readEvents().isOk, "readEvents refused an undamaged trace"

  # A genuinely absent name must still say "not found" rather than borrowing
  # the null-mapping-root wording: the two are different answers and the whole
  # point of splitting the conjunction is that they stay different.
  let missing = openTrace(dir / "does_not_exist.ct")
  doAssert missing.isErr, "opening a nonexistent path succeeded"

  dropDir(dir)
  echo "PASS: test_an_undamaged_trace_is_unaffected_by_the_bounds"

when isMainModule:
  test_an_undamaged_trace_is_unaffected_by_the_bounds()
  test_a_null_mapping_root_is_reported_and_never_silently_degrades()
  test_a_block_number_past_the_container_is_refused_not_multiplied()
  test_a_data_block_past_the_container_is_refused_not_served()
  test_an_empty_stream_is_not_an_absent_one()
  echo "All trace-reader null-mapping-root tests passed!"
