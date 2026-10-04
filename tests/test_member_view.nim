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

import results
import codetracer_ctfs/types
import codetracer_ctfs/container
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
