{.push raises: [].}

## Tests that an *interrupted* append leaves the old valid container
## (`codetracer_ctfs/container_append.nim`, `CTFS-Binary-Format.md` §5d).
##
## NO MOCKS. Every case writes a real container to a real temporary
## directory, abandons a real append part-way through a real write, and reads
## the resulting real file back with the production reader. The only thing
## that is not "production" is *when* the append stops, and that is the whole
## subject of the file.
##
## # Why this exists (M57)
##
## `container_append.nim` writes the new blocks and fsyncs them **first**, and
## rewrites block 0 **last**. That ordering is the entire reason an
## interrupted append leaves a readable container rather than a file entry
## pointing at absent data — and until M57 **nothing tested it**.
##
## The existing corpus could not: every append test in both repos — twelve in
## `test_container_append.nim`, five in the recorder's `container_write_test.go`,
## six in its `ffi_crossread_test.go`, and the C smoke test — compares two
## *quiescent* snapshots, one before the call and one after it returns. The
## closest, `TestAppendExtendsRatherThanRewrites`, checks **which** bytes
## changed, not **when**: it asserts block 0 changed, that nothing between
## block 0 and the old EOF changed, and that the file grew. Reverse the two
## phases and every one of those claims still holds, because the final bytes
## on disk are identical either way. The ordering was load-bearing, documented
## in three places, and unpinned.
##
## # How the reversal is caught
##
## `-d:ctfsAppendFaultInjection` compiles in a single flag that abandons the
## append at the **midpoint** between its two write phases. That point is
## defined relative to the two phases, not to either one of them, so swapping
## them swaps what has happened when the append stops:
##
##   correct order   → the tail is on disk, block 0 is still the old one
##   reversed order  → block 0 already names the new stream, its blocks are absent
##
## The two assertions below split exactly on that. `test_..._does_not_publish`
## fails under a reversal because block 0 *has* been republished and the new
## name is reachable but unbacked; `test_..._writes_the_tail_first` fails
## because the file never grew. Verified by actually applying the reversal.

import std/[os, strutils]
import results
import codetracer_ctfs
import codetracer_ctfs/container_append

const
  # Two blocks' worth of payload, so the abandoned tail is unmistakably
  # present (several new blocks) rather than a rounding artefact.
  TailPayload = 9000

proc pattern(n: int, salt: int): seq[byte] =
  result = newSeq[byte](n)
  for i in 0 ..< n:
    result[i] = byte((i * 7 + i div int(DefaultBlockSize) + salt) mod 251)

proc tmpDir(name: string): string {.raises: [].} =
  result = getTempDir() / ("ctfs_order_" & name & "_" & $getCurrentProcessId())
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

proc sealedContainer(path: string, files: seq[(string, seq[byte])]) {.raises: [].} =
  var c = createCtfs()
  for (name, content) in files:
    let h = c.addFile(name)
    doAssert h.isOk, "addFile " & name & ": " & h.error
    var f = h.get()
    doAssert c.writeToFile(f, content).isOk, "writeToFile " & name
  doAssert writeCtfsToFile(c, path).isOk, "writeCtfsToFile"

## An append abandoned at the midpoint, shared by both tests below. Returns
## the container bytes as they were before it, and as they are after.
proc interruptedAppend(path: string): (seq[byte], seq[byte]) {.raises: [].} =
  sealedContainer(path, @[("meta.dat", pattern(5000, 3))])
  let before = rawBytes(path)

  ctfsAppendStopAtMidpoint = true
  let res = appendInternalFiles(path, ["snapshot.mem"], [pattern(TailPayload, 9)])
  ctfsAppendStopAtMidpoint = false

  doAssert res.isErr, "the fault injection did not fire; this test proves nothing"
  doAssert "fault injection" in res.error, "unexpected failure: " & res.error
  (before, rawBytes(path))

proc test_an_interrupted_append_does_not_publish_the_new_stream() {.raises: [].} =
  ## The durability property itself: block 0 is rewritten LAST, so an append
  ## that dies before it leaves a container that never heard of the new
  ## stream — rather than one whose entry array points at blocks that are not
  ## there.
  ##
  ## Under a reversal (block 0 first) this fails on the very first assertion:
  ## block 0 would already carry the new `FileEntry`.
  let dir = tmpDir("unpublished")
  let path = dir / "c.ct"
  let (before, after) = interruptedAppend(path)

  let bs = int(DefaultBlockSize)
  doAssert before.len >= bs and after.len >= bs, "container smaller than a block"
  doAssert before[0 ..< bs] == after[0 ..< bs],
    "block 0 was rewritten before the tail was on disk — an append " &
    "interrupted at the midpoint published a stream whose blocks may not exist"

  # And read through the production reader, not only by comparing bytes: the
  # new name must be absent, and the pre-existing stream must be intact.
  doAssert not hasInternalFile(after, "snapshot.mem"),
    "the abandoned stream is reachable in a container whose tail never landed"
  let old = readInternalFile(after, "meta.dat")
  doAssert old.isOk, "the pre-existing stream stopped being readable: " & old.error
  doAssert old.get() == pattern(5000, 3), "the pre-existing stream changed"

  dropDir(dir)
  echo "PASS: test_an_interrupted_append_does_not_publish_the_new_stream"

proc test_an_interrupted_append_has_already_written_the_tail() {.raises: [].} =
  ## The other half of the ordering, asserted from the opposite side: at the
  ## midpoint the new blocks are ALREADY on disk. Without this, a writer that
  ## simply never wrote the tail would satisfy the test above.
  ##
  ## Under a reversal this fails: block 0 is rewritten in place and does not
  ## extend the file, so the container would still be its original length.
  let dir = tmpDir("tailfirst")
  let path = dir / "c.ct"
  let (before, after) = interruptedAppend(path)

  doAssert after.len > before.len,
    "the container did not grow at the midpoint (" & $before.len & " -> " &
    $after.len & "), so the tail had not been written when block 0's turn came"

  # Everything between block 0 and the old end of file is untouched: the
  # append only ever extends and republishes, never rewrites in the middle.
  let bs = int(DefaultBlockSize)
  doAssert before[bs ..< before.len] == after[bs ..< before.len],
    "the interrupted append rewrote an existing block"

  dropDir(dir)
  echo "PASS: test_an_interrupted_append_has_already_written_the_tail"

proc test_the_container_survives_a_second_real_append() {.raises: [].} =
  ## The recovery story, end to end: after an interrupted append the file is
  ## the old valid container, and a subsequent *complete* append succeeds and
  ## produces a container carrying both streams.
  ##
  ## This is what "wasteful but perfectly readable" has to mean to be worth
  ## anything, and it is asserted with the fault injection switched back off,
  ## through the ordinary production path.
  let dir = tmpDir("recovers")
  let path = dir / "c.ct"
  discard interruptedAppend(path)

  # The abandoned tail leaves a length that is still a whole number of blocks
  # here (the write completed; only block 0 was skipped), so the append's
  # own precondition holds and it can proceed.
  let res = appendInternalFiles(path, ["snapshot.mem"], [pattern(TailPayload, 9)])
  doAssert res.isOk, "a second append onto the abandoned container failed: " & res.error

  let final = rawBytes(path)
  let a = readInternalFile(final, "meta.dat")
  doAssert a.isOk, "meta.dat unreadable after recovery: " & a.error
  doAssert a.get() == pattern(5000, 3), "meta.dat changed across the recovery"
  let b = readInternalFile(final, "snapshot.mem")
  doAssert b.isOk, "snapshot.mem unreadable after recovery: " & b.error
  doAssert b.get() == pattern(TailPayload, 9), "snapshot.mem came back wrong"

  dropDir(dir)
  echo "PASS: test_the_container_survives_a_second_real_append"

proc test_a_partial_tail_is_still_read_by_the_production_reader() {.raises: [].} =
  ## M57 / piece 3. A crash *inside* the tail write — rather than between the
  ## phases — leaves a file whose length is not a block multiple. The reader
  ## must still read it: block 0 is the old one, every referenced block sits
  ## below the old end of file, and the partial tail is unreferenced.
  ##
  ## This pins the Nim reader's tolerance so it cannot regress into the Go
  ## reader's old strictness. `CTFS-Binary-Format.md` §5d now states this as
  ## a requirement on readers rather than leaving it an accident of the
  ## implementation, and the Go reader was changed to match
  ## (`internal/ctfs/container.go`).
  let dir = tmpDir("partialtail")
  let path = dir / "c.ct"
  sealedContainer(path, @[("meta.dat", pattern(5000, 3))])
  let whole = rawBytes(path)

  # A tail write that died part-way: some whole new blocks plus a partial one.
  var torn = whole
  torn.add(pattern(int(DefaultBlockSize) + 777, 42))
  doAssert torn.len mod int(DefaultBlockSize) != 0,
    "the fixture is a block multiple, so it does not exercise the partial tail"
  try:
    writeFile(path, torn)
  except IOError, OSError:
    doAssert false, "cannot write the partial-tail fixture"

  let got = rawBytes(path)
  let old = readInternalFile(got, "meta.dat")
  doAssert old.isOk,
    "the reader refused a container with a partial tail: " & old.error
  doAssert old.get() == pattern(5000, 3),
    "a partial tail changed what a pre-existing stream reads back as"
  doAssert not hasInternalFile(got, "snapshot.mem"),
    "an unreferenced partial tail made a stream appear"

  # The append, by contrast, must keep refusing it: it recovers the allocator
  # state from the file length, and §5d says refused rather than rounded.
  let res = appendInternalFiles(path, ["a.dat"], [pattern(4, 0)])
  doAssert res.isErr, "the append accepted a container it cannot size"
  doAssert "whole number" in res.error, "unhelpful message: " & res.error

  dropDir(dir)
  echo "PASS: test_a_partial_tail_is_still_read_by_the_production_reader"

when isMainModule:
  test_an_interrupted_append_does_not_publish_the_new_stream()
  test_an_interrupted_append_has_already_written_the_tail()
  test_the_container_survives_a_second_real_append()
  test_a_partial_tail_is_still_read_by_the_production_reader()
  echo "All container-append ordering tests passed!"
