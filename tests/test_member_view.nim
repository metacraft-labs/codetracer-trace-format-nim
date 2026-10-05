## A member read in place (`member_view.nim`) answers every range as the
## member copied out (`readInternalFile`) does.
##
## Two members are written alternately, a block at a time and a little more,
## so each one's blocks interleave with the other's and its bytes lie in many
## runs. Every range that starts or ends within a few bytes of a block
## boundary — inside one run, straddling two, and the whole member — is read
## through `span`, `copyOut` and `readU64LE` and compared with the copy. A
## range past the member is refused. No mocks: a real container, written by
## this repository's CTFS writer.
##
## An image read from its file as it is used (`openFileImage`) answers every
## range alike, reads the blocks a read reaches and no others, refuses to read
## in place a range not yet read, refuses a file cut short after it was opened,
## and reads a container whose members do not lie in blocks whole.

import std/[os, strutils]
import results
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_ctfs/compact
import codetracer_ctfs/member_view

proc patterned(n: int, seed: int): seq[byte] =
  result = newSeq[byte](n)
  for i in 0 ..< n:
    result[i] = byte((i * 31 + seed * 7 + (i shr 8)) and 0xff)

block every_range_reads_as_the_copy:
  var c = createCtfs()
  var a = c.addFile("a.dat").get()
  var b = c.addFile("b.dat").get()
  let pieceA = patterned(5000, 1)
  let pieceB = patterned(4100, 2)
  for _ in 0 ..< 6:
    doAssert c.writeToFile(a, pieceA).isOk
    doAssert c.writeToFile(b, pieceB).isOk
  let image = newContainerImage(c.toBytes())
  for name in ["a.dat", "b.dat"]:
    let copy = readInternalFile(image.bytes, name).get()
    let v = viewMember(image, name).get()
    doAssert v.len == copy.len
    doAssert locateMember(image.bytes, name).get().len >= 6,
      name & " lies in one run, so no range straddles two"
    var cuts: seq[int]
    var k = 0
    while k <= copy.len:
      for d in [-9, -1, 0, 1, 8]:
        if k + d >= 0 and k + d <= copy.len: cuts.add(k + d)
      k += int(DefaultBlockSize)
    cuts.add(copy.len)
    var scratch: seq[byte]
    for first in cuts:
      for last in cuts:
        if last < first: continue
        let n = last - first
        let p = v.span(first, n, scratch)
        for i in 0 ..< n:
          doAssert p[i] == copy[first + i], name & " [" & $first & ", " &
            $last & ") byte " & $i
        doAssert v.copyOut(first, n) == copy[first ..< last]
        if n >= 8:
          var want = 0'u64
          for i in 0 ..< 8: want = want or (uint64(copy[first + i]) shl (8 * i))
          doAssert v.readU64LE(first) == want
    doAssertRaises(AssertionDefect):
      discard v.span(copy.len - 3, 4, scratch)
  # A member already held on its own reads the same.
  let own = viewBytes(patterned(70, 3))
  doAssert own.copyOut(5, 60) == patterned(70, 3)[5 ..< 65]
  echo "PASS every_range_reads_as_the_copy"

proc readRange(v: MemberView, first, n: int): Result[seq[byte], string] =
  v.ensureLoaded(first, n)
  ok(v.copyOut(first, n))

proc tempPath(name: string): string =
  getTempDir() / ("test_member_view_" & $getCurrentProcessId() & "_" & name)

block a_file_image_reads_what_is_asked_and_reads_it_as_the_copy:
  var c = createCtfs()
  var a = c.addFile("a.dat").get()
  var b = c.addFile("b.dat").get()
  var big = c.addFile("big.dat").get()
  for _ in 0 ..< 6:
    doAssert c.writeToFile(a, patterned(5000, 1)).isOk
    doAssert c.writeToFile(b, patterned(4100, 2)).isOk
  # More blocks than one mapping block holds: read through a chain pointer
  # and a second-level mapping block.
  doAssert c.writeToFile(big, patterned(520 * int(DefaultBlockSize), 4)).isOk
  let whole = c.toBytes()
  let path = tempPath("lazy.ct")
  writeFile(path, cast[string](whole))
  defer: removeFile(path)
  let image = openFileImage(path).get()
  doAssert image.readsFromFile
  doAssert image.blocksRead == 1, "the open reads the root directory alone"
  var read = image.blocksRead
  for name in ["a.dat", "b.dat", "big.dat"]:
    let copy = readInternalFile(whole, name).get()
    let v = viewMember(image, name).get()
    let mapping = image.blocksRead - read
    doAssert mapping in 1 .. 3, name & " read " & $mapping & " mapping blocks"
    var scratch: seq[byte]
    doAssertRaises(AssertionDefect):
      discard v.span(0, 8, scratch)
    # A range in the middle reads the blocks it covers, and no others.
    let mid = copy.len div 2
    doAssert v.readRange(mid, 10).get() == copy[mid ..< mid + 10]
    doAssert image.blocksRead - read - mapping in 1 .. 2, name
    for first in [0, 1, 4095, 4096, copy.len div 3]:
      for n in [0, 1, 9, 4097, copy.len - first]:
        if first + n <= copy.len:
          doAssert v.readRange(first, n).get() == copy[first ..< first + n],
            name & " [" & $first & ", +" & $n & ")"
    doAssert v.contents().get() == copy
    read = image.blocksRead
  doAssert image.blocksRead <= whole.len div int(DefaultBlockSize),
    "no block is read twice"
  echo "PASS a_file_image_reads_what_is_asked_and_reads_it_as_the_copy"

block a_file_cut_short_after_it_was_opened_is_refused:
  var c = createCtfs()
  var a = c.addFile("a.dat").get()
  for _ in 0 ..< 6:
    doAssert c.writeToFile(a, patterned(5000, 1)).isOk
  let whole = c.toBytes()
  let path = tempPath("cut.ct")
  writeFile(path, cast[string](whole))
  defer: removeFile(path)
  let image = openFileImage(path).get()
  let v = viewMember(image, "a.dat").get()
  writeFile(path, cast[string](whole[0 ..< 2 * int(DefaultBlockSize)]))
  let r = v.contents()
  doAssert r.isErr and "could not be read" in r.error, $r
  # Control: an image of the uncut file reads the member.
  writeFile(path, cast[string](whole))
  doAssert viewMember(openFileImage(path).get(), "a.dat").get().contents().get() ==
    readInternalFile(whole, "a.dat").get()
  echo "PASS a_file_cut_short_after_it_was_opened_is_refused"

block a_container_without_blocks_is_read_whole:
  var c = createCtfs()
  var a = c.addFile("a.dat").get()
  doAssert c.writeToFile(a, patterned(9000, 1)).isOk
  let full = c.toBytes()
  let compactBytes =
    encodeCompactContainer(collectFullProfileMembers(full).get()).get()
  for (what, bytes) in [("compact", compactBytes),
      ("compressed", compressImage(compactBytes).get())]:
    let path = tempPath(what & ".ct")
    writeFile(path, cast[string](bytes))
    let image = openFileImage(path).get()
    removeFile(path)
    doAssert not image.readsFromFile, what
    doAssert image.bytes == bytes, what
  echo "PASS a_container_without_blocks_is_read_whole"
