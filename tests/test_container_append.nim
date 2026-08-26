{.push raises: [].}

## Tests for appending internal files to an already-closed CTFS container
## (`codetracer_ctfs/container_append.nim`).
##
## NO MOCKS. Every case writes a real container to a real temporary
## directory, reopens it through the real filesystem and reads it back with
## the production reader.
##
## The load-bearing check here is `test_append_crosses_the_multi_level_boundary`.
## `appendInternalFiles` deliberately owns no mapping arithmetic — it drives
## `addFile` / `writeToFile` / `insertDataBlock` — but "deliberately" is a
## claim about the code, not about the bytes, and the block-mapping hierarchy
## of `CTFS-Binary-Format.md` §4 is precisely where a second container writer
## has silently gone wrong before (see the module header). So the appended
## streams are read back by `readInternalFile`, which walks the levels with
## its **own** inline transcription of the algorithm rather than by calling
## `lookupDataBlock`: writer and reader here are two implementations, not one.
##
## The Go-side cross-read in `codetracer-wasm-recorder`
## (`internal/ctfs/ffi_crossread_test.go`) is the other half — a different
## language reading the same bytes.

import std/[os, strutils]
import results
import codetracer_ctfs

const
  # A 4096-byte block gives `usable = 511`, so data block 511 is the first
  # that needs a level-2 mapping block: exactly the ~2 MB threshold past
  # which a wrong reading of §4 starts returning the wrong bytes. 1100
  # blocks reach into the level-2 block's *second* slot as well (511 direct,
  # then 511 through L2 slot 0, then 78 through L2 slot 1), so the descent is
  # exercised and not just the chain hop.
  BigBlockCount = 1100
  MultiLevelSize = BigBlockCount * int(DefaultBlockSize)

proc pattern(n: int, salt: int): seq[byte] =
  ## A byte pattern whose value depends on the absolute offset, so a block
  ## delivered from the wrong place is visible rather than plausible.
  result = newSeq[byte](n)
  for i in 0 ..< n:
    result[i] = byte((i * 7 + i div int(DefaultBlockSize) + salt) mod 251)

proc tmpDir(name: string): string {.raises: [].} =
  result = getTempDir() / ("ctfs_append_" & name & "_" & $getCurrentProcessId())
  try:
    removeDir(result)
    createDir(result)
  except OSError, IOError:
    doAssert false, "cannot prepare " & result

proc fileSize(path: string): int64 {.raises: [].} =
  try:
    result = getFileSize(path)
  except OSError, IOError:
    doAssert false, "cannot stat " & path

proc dropDir(path: string) {.raises: [].} =
  try:
    removeDir(path)
  except OSError:
    discard

proc sealedContainer(path: string, files: seq[(string, seq[byte])]) {.raises: [].} =
  ## Produce a closed container the ordinary way: the live writer, then
  ## `writeCtfsToFile`. This is what a finished trace looks like on disk.
  var c = createCtfs()
  for (name, content) in files:
    let h = c.addFile(name)
    doAssert h.isOk, "addFile " & name & ": " & h.error
    var f = h.get()
    doAssert c.writeToFile(f, content).isOk, "writeToFile " & name
  doAssert writeCtfsToFile(c, path).isOk, "writeCtfsToFile"

proc readBack(path: string, name: string): seq[byte] {.raises: [].} =
  let raw = readCtfsFromFile(path)
  doAssert raw.isOk, "readCtfsFromFile: " & raw.error
  let got = readInternalFile(raw.get(), name)
  doAssert got.isOk, "readInternalFile " & name & ": " & got.error
  got.get()

proc test_append_adds_streams_without_disturbing_the_old_ones() {.raises: [].} =
  let dir = tmpDir("basic")
  let path = dir / "t.ct"
  let meta = pattern(37, 1)
  let steps = pattern(9000, 2)
  sealedContainer(path, @[("meta.dat", meta), ("steps.dat", steps)])
  let before = fileSize(path)

  let extra = pattern(70000, 3)
  let tiny = pattern(3, 4)
  let res = appendInternalFiles(path, ["snapshot.idx", "snapglob.dat"],
                                [extra, tiny])
  doAssert res.isOk, "appendInternalFiles: " & res.error

  doAssert readBack(path, "meta.dat") == meta, "meta.dat changed"
  doAssert readBack(path, "steps.dat") == steps, "steps.dat changed"
  doAssert readBack(path, "snapshot.idx") == extra, "snapshot.idx wrong"
  doAssert readBack(path, "snapglob.dat") == tiny, "snapglob.dat wrong"
  doAssert fileSize(path) > before, "the container did not grow"
  doAssert fileSize(path) mod int64(DefaultBlockSize) == 0,
    "an appended container must stay a whole number of blocks"
  dropDir(dir)
  echo "PASS: test_append_adds_streams_without_disturbing_the_old_ones"

proc test_append_crosses_the_multi_level_boundary() {.raises: [].} =
  ## The case a self-consistent-but-wrong writer passes and a cross-read
  ## fails: a stream far past `usable` data blocks.
  let dir = tmpDir("multilevel")
  let path = dir / "t.ct"
  sealedContainer(path, @[("meta.dat", pattern(11, 5))])

  let big = pattern(MultiLevelSize, 6)
  let res = appendInternalFiles(path, ["snapshot.mem"], [big])
  doAssert res.isOk, "appendInternalFiles: " & res.error

  let got = readBack(path, "snapshot.mem")
  doAssert got.len == big.len, "size " & $got.len & " != " & $big.len
  for i in 0 ..< big.len:
    if got[i] != big[i]:
      doAssert false, "byte " & $i & " (block " &
        $(i div int(DefaultBlockSize)) & ") is " & $got[i] & ", want " & $big[i]
  doAssert readBack(path, "meta.dat") == pattern(11, 5), "meta.dat changed"
  dropDir(dir)
  echo "PASS: test_append_crosses_the_multi_level_boundary"

proc test_repeated_appends_accumulate() {.raises: [].} =
  ## Each append reopens a container the previous append sealed, so the
  ## allocator state recovered from the file length has to be right every
  ## time, not just the first.
  let dir = tmpDir("repeat")
  let path = dir / "t.ct"
  sealedContainer(path, @[("meta.dat", pattern(5, 7))])
  var expected: seq[(string, seq[byte])] = @[("meta.dat", pattern(5, 7))]
  for round in 0 ..< 4:
    let name = "round" & $round & ".dat"
    let content = pattern(3000 * (round + 1) + 17, 20 + round)
    doAssert appendInternalFiles(path, [name], [content]).isOk,
      "append round " & $round
    expected.add((name, content))
    for (n, want) in expected:
      doAssert readBack(path, n) == want, n & " wrong after round " & $round
  dropDir(dir)
  echo "PASS: test_repeated_appends_accumulate"

proc test_empty_stream_is_still_a_stream() {.raises: [].} =
  let dir = tmpDir("empty")
  let path = dir / "t.ct"
  sealedContainer(path, @[("meta.dat", pattern(5, 8))])
  doAssert appendInternalFiles(path, ["snaptab.dat"], [newSeq[byte](0)]).isOk,
    "a zero-length stream must still be attachable"
  let raw = readCtfsFromFile(path)
  doAssert raw.isOk
  # A zero-length internal file gets a mapping block like every other, so it
  # is *present* rather than indistinguishable from an unused entry slot.
  doAssert hasInternalFile(raw.get(), "snaptab.dat"),
    "the zero-length stream is not visible in the root directory"
  doAssert readBack(path, "snaptab.dat").len == 0
  dropDir(dir)
  echo "PASS: test_empty_stream_is_still_a_stream"

proc test_append_refuses_an_existing_name() {.raises: [].} =
  let dir = tmpDir("dup")
  let path = dir / "t.ct"
  sealedContainer(path, @[("meta.dat", pattern(64, 9))])
  let before = readCtfsFromFile(path)
  doAssert before.isOk
  let res = appendInternalFiles(path, ["meta.dat"], [pattern(10, 10)])
  doAssert res.isErr, "overwriting an existing stream was allowed"
  doAssert "append-only" in res.error, "unhelpful message: " & res.error
  let after = readCtfsFromFile(path)
  doAssert after.isOk
  doAssert after.get() == before.get(),
    "a refused append must not have touched the file"
  dropDir(dir)
  echo "PASS: test_append_refuses_an_existing_name"

proc test_append_refuses_a_duplicate_within_one_batch() {.raises: [].} =
  let dir = tmpDir("dupbatch")
  let path = dir / "t.ct"
  sealedContainer(path, @[("meta.dat", pattern(64, 11))])
  let res = appendInternalFiles(path, ["a.dat", "a.dat"],
                                [pattern(4, 1), pattern(4, 2)])
  doAssert res.isErr, "the same name twice in one batch was allowed"
  doAssert "twice" in res.error, "unhelpful message: " & res.error
  dropDir(dir)
  echo "PASS: test_append_refuses_a_duplicate_within_one_batch"

proc test_append_refuses_an_unencodable_name() {.raises: [].} =
  ## `base40Encode` maps anything outside the alphabet to padding, so
  ## "snap!pages" would otherwise be stored — silently — as "snap".
  let dir = tmpDir("badname")
  let path = dir / "t.ct"
  sealedContainer(path, @[("meta.dat", pattern(64, 12))])
  for bad in ["snap!pages", "SNAPSHOT.IDX", "thirteenchars", ""]:
    let res = appendInternalFiles(path, [bad], [pattern(4, 0)])
    doAssert res.isErr, "the unencodable name " & bad & " was accepted"
  dropDir(dir)
  echo "PASS: test_append_refuses_an_unencodable_name"

proc test_append_refuses_a_non_container() {.raises: [].} =
  let dir = tmpDir("notct")
  let path = dir / "t.ct"
  try:
    writeFile(path, "this is not a CTFS container at all, not even close")
  except IOError, OSError:
    doAssert false, "cannot write the fixture"
  let res = appendInternalFiles(path, ["a.dat"], [pattern(4, 0)])
  doAssert res.isErr, "a non-container was appended to"
  doAssert "magic" in res.error, "unhelpful message: " & res.error
  dropDir(dir)
  echo "PASS: test_append_refuses_a_non_container"

proc test_append_refuses_a_truncated_container() {.raises: [].} =
  ## The allocator state is recovered from the file length, so a length that
  ## is not a whole number of blocks means the recovery would be wrong — and
  ## a wrong `nextFreeBlock` overwrites live data. Refused, not rounded.
  let dir = tmpDir("truncated")
  let path = dir / "t.ct"
  sealedContainer(path, @[("meta.dat", pattern(9000, 13))])
  let raw = readCtfsFromFile(path)
  doAssert raw.isOk
  var cut = raw.get()
  cut.setLen(cut.len - 7)
  try:
    writeFile(path, cut)
  except IOError, OSError:
    doAssert false, "cannot write the truncated fixture"
  let res = appendInternalFiles(path, ["a.dat"], [pattern(4, 0)])
  doAssert res.isErr, "a truncated container was appended to"
  doAssert "whole number" in res.error, "unhelpful message: " & res.error
  dropDir(dir)
  echo "PASS: test_append_refuses_a_truncated_container"

proc test_append_refuses_an_encrypted_container() {.raises: [].} =
  let dir = tmpDir("encrypted")
  let path = dir / "t.ct"
  var c = createCtfs(encryption = emAes256Gcm)
  doAssert writeCtfsToFile(c, path).isOk
  let res = appendInternalFiles(path, ["a.dat"], [pattern(4, 0)])
  doAssert res.isErr, "an encrypted container was appended to"
  doAssert "encrypted" in res.error, "unhelpful message: " & res.error
  dropDir(dir)
  echo "PASS: test_append_refuses_an_encrypted_container"

proc test_append_refuses_when_the_entry_array_is_full() {.raises: [].} =
  let dir = tmpDir("full")
  let path = dir / "t.ct"
  var c = createCtfs()
  for i in 0 ..< int(DefaultMaxRootEntries):
    let h = c.addFile("f" & $i & ".dat")
    doAssert h.isOk
    var f = h.get()
    doAssert c.writeToFile(f, pattern(8, i)).isOk
  doAssert writeCtfsToFile(c, path).isOk
  let res = appendInternalFiles(path, ["one.more"], [pattern(4, 0)])
  doAssert res.isErr, "a full entry array accepted another file"
  doAssert "free file entry" in res.error, "unhelpful message: " & res.error
  dropDir(dir)
  echo "PASS: test_append_refuses_when_the_entry_array_is_full"

proc test_mismatched_batch_is_refused() {.raises: [].} =
  let dir = tmpDir("mismatch")
  let path = dir / "t.ct"
  sealedContainer(path, @[("meta.dat", pattern(5, 14))])
  let res = appendInternalFiles(path, ["a.dat", "b.dat"], [pattern(4, 0)])
  doAssert res.isErr, "a name/content length mismatch was accepted"
  doAssert appendInternalFiles(path, [], []).isOk,
    "an empty batch should be a no-op, not an error"
  dropDir(dir)
  echo "PASS: test_mismatched_batch_is_refused"

when isMainModule:
  test_append_adds_streams_without_disturbing_the_old_ones()
  test_append_crosses_the_multi_level_boundary()
  test_repeated_appends_accumulate()
  test_empty_stream_is_still_a_stream()
  test_append_refuses_an_existing_name()
  test_append_refuses_a_duplicate_within_one_batch()
  test_append_refuses_an_unencodable_name()
  test_append_refuses_a_non_container()
  test_append_refuses_a_truncated_container()
  test_append_refuses_an_encrypted_container()
  test_append_refuses_when_the_entry_array_is_full()
  test_mismatched_batch_is_refused()
  echo "All container-append tests passed!"
