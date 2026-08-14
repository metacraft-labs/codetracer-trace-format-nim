{.push raises: [].}

## The bound that makes accepting a partial tail safe, in the Nim reader
## (`codetracer_ctfs/container.nim`'s `readInternalFile`,
## `CTFS-Binary-Format.md` §5d).
##
## NO MOCKS. Every case writes a real container with the production writer to a
## real temporary file, damages it the way a crash actually damages it (by
## extending or cutting the real bytes), and reads it back with the production
## reader. Nothing is stubbed, and no test-only traversal is used.
##
## # Why this exists (M58)
##
## §5d requires a reader to **accept** a container whose length is not a whole
## number of blocks and ignore the trailing fragment. `readInternalFile` always
## did. What it did not do was bound the *block numbers* it resolves: it
## checked byte offsets against `data.len` only. Those are not the same check.
## The final data block's copy is clamped to what is left of the entry, so a
## stream whose last, short data block landed in the partial region was read
## **successfully**, out of bytes the container does not own, and returned as
## content.
##
## That is precisely the defect M57's review found in the Go reader after it
## started accepting: relaxing the container-level check without bounding the
## data-block resolution turns a truncated file into wrong bytes with no error,
## which is strictly worse than the refusal it replaced. M58 relaxed
## `check_ctfs_container.nim` — the adjudicating reader — so the same
## obligation lands here.
##
## The partial-tail state does not itself reach that path: the fragment left by
## an interrupted append is unreferenced (block 0 is the previous one and every
## pointer in it is below the previous end of file). It takes a **truncated**
## container, which has the same shape on disk and cannot be told apart from
## the bytes, to provoke it. Both must land on an error rather than on content
## — and, just as important, a truncated container must still surrender only
## the streams it actually lost.

import std/[os, strutils]
import results
import codetracer_ctfs
import codetracer_ctfs/container_append

const
  BS = int(DefaultBlockSize)

proc pattern(n: int, salt: int): seq[byte] =
  result = newSeq[byte](n)
  for i in 0 ..< n:
    result[i] = byte((i * 7 + i div BS + salt) mod 251)

proc tmpDir(name: string): string {.raises: [].} =
  result = getTempDir() / ("ctfs_bounds_" & name & "_" & $getCurrentProcessId())
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

proc rawBytes(path: string): seq[byte] {.raises: [].} =
  let raw = readCtfsFromFile(path)
  doAssert raw.isOk, "readCtfsFromFile: " & raw.error
  raw.get()

proc putBytes(path: string, data: seq[byte]) {.raises: [].} =
  try:
    writeFile(path, data)
  except IOError, OSError:
    doAssert false, "cannot write " & path

proc sealedContainer(path: string, files: seq[(string, seq[byte])]) {.raises: [].} =
  var c = createCtfs()
  for (name, content) in files:
    let h = c.addFile(name)
    doAssert h.isOk, "addFile " & name & ": " & h.error
    var f = h.get()
    doAssert c.writeToFile(f, content).isOk, "writeToFile " & name
  doAssert writeCtfsToFile(c, path).isOk, "writeCtfsToFile"

# ---------------------------------------------------------------------------

proc test_a_truncated_stream_is_refused_rather_than_served_short() {.raises: [].} =
  ## THE bound test. A container is cut so that one stream's last, short data
  ## block becomes the first *partial* block, with exactly its own bytes
  ## present — so every byte the reader would copy is physically there and the
  ## old `blockOff + toCopy > data.len` check passes.
  ##
  ## Remove the `dataBlock >= wholeBlocks` check in `readInternalFile` and this
  ## test goes red: `readInternalFile` returns the full 12,388 bytes with no
  ## error, out of a block the container does not own.
  let dir = tmpDir("shortserve")
  let path = dir / "cut.ct"

  # A size whose last data block carries only 100 bytes, so the clamped copy is
  # short enough to be satisfiable out of a partial block.
  const TailBytes = 100
  let content = pattern(3 * BS + TailBytes, 5)
  # `z.dat` is written last so its blocks sit at the end of the container and
  # the cut below does not also destroy the stream we want to survive.
  sealedContainer(path, @[("meta.dat", pattern(5000, 3)), ("z.dat", content)])

  let whole = rawBytes(path)
  doAssert whole.len mod BS == 0, "the sealed container is not block-aligned"

  # Find z.dat's last data block by cutting at successively larger block
  # boundaries is fragile; instead cut at "everything but the final block's
  # unused bytes". The writer allocates z.dat's data blocks contiguously at the
  # end, so the container's last block IS z.dat's last data block.
  let lastBlock = (whole.len div BS) - 1
  let cut = lastBlock * BS + TailBytes
  doAssert cut < whole.len,
    "the cut is not inside the final block; the fixture proves nothing"
  putBytes(path, whole[0 ..< cut])

  let got = rawBytes(path)
  doAssert got.len mod BS != 0,
    "the truncated fixture is still block-aligned, so it does not exercise " &
    "the partial region"
  doAssert (got.len div BS) == lastBlock,
    "the fixture did not put z.dat's last data block outside the whole blocks"

  let bad = readInternalFile(got, "z.dat")
  doAssert bad.isErr,
    "readInternalFile returned " & $bad.get().len & " bytes with no error for " &
    "a stream whose last data block lies outside the container's " &
    $(got.len div BS) & " whole blocks — the partial region was served as content"
  doAssert "out of bounds" in bad.error, "unhelpful refusal: " & bad.error
  doAssert "truncated" in bad.error,
    "the refusal does not say the container is truncated: " & bad.error

  # The other half of "bound, don't refuse the file": the stream that was NOT
  # cut must still read back byte-exact.
  let survivor = readInternalFile(got, "meta.dat")
  doAssert survivor.isOk,
    "a truncation that lost z.dat also cost meta.dat: " & survivor.error
  doAssert survivor.get() == pattern(5000, 3),
    "meta.dat came back changed from a container truncated elsewhere"

  dropDir(dir)
  echo "PASS: test_a_truncated_stream_is_refused_rather_than_served_short"

proc test_a_partial_tail_costs_nothing() {.raises: [].} =
  ## The positive half, over more shapes than the ordering test's single
  ## stream: a sealed container extended by an unreferenced whole block plus a
  ## fragment reads back byte-for-byte identically to the sealed one, for a
  ## small file, a file of exactly one block, and a multi-level-mapping file.
  let dir = tmpDir("partialtail")
  let path = dir / "torn.ct"

  let files = @[
    ("meta.dat", pattern(7, 1)),           # smaller than a block
    ("steps.dat", pattern(BS, 2)),         # exactly one block
    ("values.dat", pattern(123457, 3)),    # many blocks, level 1
    ("snapshot.mem", pattern(2457613, 4)), # 600 data blocks -> two-level mapping
  ]
  sealedContainer(path, files)
  let whole = rawBytes(path)

  var torn = whole
  torn.add(pattern(BS + 777, 42))
  doAssert torn.len mod BS == 777,
    "the fixture does not leave a 777-byte partial tail"
  putBytes(path, torn)

  let got = rawBytes(path)
  for (name, content) in files:
    doAssert hasInternalFile(got, name), name & " vanished behind a partial tail"
    let r = readInternalFile(got, name)
    doAssert r.isOk, "the reader refused " & name & " over a partial tail: " & r.error
    doAssert r.get() == content, name & " came back changed over a partial tail"

  # And the unreferenced fragment must not have invented a stream.
  doAssert not hasInternalFile(got, "a.dat"),
    "an unreferenced partial tail surfaced as an internal file"

  dropDir(dir)
  echo "PASS: test_a_partial_tail_costs_nothing"

proc test_the_append_still_refuses_a_partial_tail() {.raises: [].} =
  ## The deliberate asymmetry §5d turns on, restated here so a later change
  ## that "makes the two agree" has to argue with a test. The reader accepts;
  ## the append does not, because it recovers `NextFreeBlock` from the length
  ## and a wrong one overwrites live data.
  let dir = tmpDir("appendrefuses")
  let path = dir / "torn.ct"
  sealedContainer(path, @[("meta.dat", pattern(5000, 3))])
  var torn = rawBytes(path)
  torn.add(pattern(777, 42))
  putBytes(path, torn)

  let res = appendInternalFiles(path, ["a.dat"], [pattern(4, 0)])
  doAssert res.isErr, "the append accepted a container whose length it cannot trust"
  doAssert "whole number" in res.error, "unhelpful message: " & res.error

  # The refusal must be clean: the container is still readable afterwards.
  let after = rawBytes(path)
  let r = readInternalFile(after, "meta.dat")
  doAssert r.isOk, "the refused append cost the pre-existing stream: " & r.error
  doAssert r.get() == pattern(5000, 3), "the refused append changed the stream"

  dropDir(dir)
  echo "PASS: test_the_append_still_refuses_a_partial_tail"

proc test_a_clean_container_is_unaffected_by_the_bound() {.raises: [].} =
  ## The negative control. The bound must be invisible to every ordinary
  ## container — including one whose last stream ends exactly on a block
  ## boundary, where an off-by-one in `>=` vs `>` would bite.
  let dir = tmpDir("clean")
  let path = dir / "clean.ct"
  let files = @[
    ("meta.dat", pattern(4 * BS, 1)),        # exact block multiple
    ("steps.dat", pattern(BS * 511, 2)),     # exactly `usable` blocks: last level-1 fit
    ("values.dat", pattern(BS * 512, 3)),    # one past it: first that needs the chain
  ]
  sealedContainer(path, files)
  let got = rawBytes(path)
  doAssert got.len mod BS == 0, "the control fixture is not block-aligned"
  for (name, content) in files:
    let r = readInternalFile(got, name)
    doAssert r.isOk, "the bound refused a clean container's " & name & ": " & r.error
    doAssert r.get() == content, name & " came back wrong from a clean container"

  dropDir(dir)
  echo "PASS: test_a_clean_container_is_unaffected_by_the_bound"

when isMainModule:
  test_a_partial_tail_costs_nothing()
  test_a_truncated_stream_is_refused_rather_than_served_short()
  test_the_append_still_refuses_a_partial_tail()
  test_a_clean_container_is_unaffected_by_the_bound()
  echo "All CTFS partial-tail bound tests passed!"
