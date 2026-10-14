## A header declaring `MaxRootEntries = 0` is read with the auto-filled root
## directory (`ctfs-container.md` §1, "Auto-fill"; §1a for version 6): every
## reader in this library scans `(BlockSize - header) / 24` slots -- 170 at
## version 5 and 169 at version 6 for a 4096-byte block -- and never reads the
## `0` as "no entries".
##
## The readers here take the count from their caller, and the ones that read
## it from the header (`codetracer_trace_reader.openTrace`,
## `native_decoder.detectNativeBundle`, ct-print, and every caller that passes
## the header's bytes 12..15 through, as the CTFS benchmark's conformance
## harness does) hand a declared `0` straight to `findFileEntry`.
##
## The null prediction: a reader that takes the `0` literally scans no slot.
## `readInternalFile` answers "internal file not found" for every member,
## `hasInternalFile` false, `rootMembers` nothing, `openTrace` "meta.dat
## missing", `detectNativeBundle` refuses the header as nonsensical, and
## `analyzeCtfs` reports no file. Every zero-declaration case below fails
## under that reading; the explicit-declaration controls read the same
## members through the same procs with the count spelled out, so a failure in
## a zero case cannot be a defect of the fixture.
##
## The fixtures: the library's own writer creates a root that fills block 0
## exactly (170 entries: `16 + 170 * 24 = 4096`) with a member in every slot,
## the last one included, and only header bytes 12..15 are then rewritten to
## `0`. That is the same container: auto-fill names the same 170 slots and no
## other byte changes. A member in slot 169 is what tells 170 from any smaller
## count a reader might substitute (31, `DefaultMaxRootEntries`, among them).
## The version-6 fixture moves the same entry array 8 bytes later behind a
## 24-byte header, at 169 entries, the version-6 auto-fill count; every data
## block keeps its number.
##
## Every case runs, and the binary exits 1 if any failed, naming each; it also
## checks that every case it lists ran.

import std/[os, sets, strutils]
import results
import codetracer_ctfs
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_ctfs/member_view
import codetracer_ctfs/space_analyzer
import codetracer_trace_reader
import codetracer_trace_writer/multi_stream_writer
import native_decoder

const
  BS = 4096'u32
  V5Auto = (int(BS) - 16) div 24
  V6Auto = (int(BS) - 24) div 24

var failures: seq[string]
var casesRun: seq[string]

proc check(cond: bool, what: string) =
  if not cond:
    failures.add(what)

proc memberName(i: int): string = "m" & $i

proc memberContent(i: int): seq[byte] =
  ## Every layout: empty, a partial direct block, a whole block, and mapped
  ## members of two and four blocks.
  let n = case i mod 5
    of 0: 0
    of 1: 1 + i * 7
    of 2: int(BS)
    of 3: int(BS) + 1 + i
    else: 3 * int(BS) + i * 13
  result = newSeq[byte](n)
  for k in 0 ..< n:
    result[k] = byte((i * 131 + k * 7 + (k shr 8)) and 0xFF)

proc written(declared: int, names: openArray[string],
    content: proc (i: int): seq[byte]): seq[byte] =
  ## A version-5 container written by `createCtfs` declaring `declared` root
  ## entries, holding `names` in slot order.
  var c = createCtfs(blockSize = BS, maxRootEntries = uint32(declared))
  for i, name in names:
    var f = c.addFile(name).get()
    doAssert c.writeToFile(f, content(i)).isOk
  result = c.toBytes()
  doAssert result[5] == CtfsVersion
  doAssert readU32LE(result, 12) == uint32(declared)

proc declare(image: var seq[byte], count: uint32) =
  writeU32LE(image, 12, count)

proc asV6(v5: seq[byte]): seq[byte] =
  ## The version-5 image `v5`, whose root holds `V6Auto` entries, as a
  ## version-6 full-profile, uncompressed container declaring `0`.
  result = newSeq[byte](v5.len)
  for k in 0 ..< 16: result[k] = v5[k]
  result[5] = CtfsVersionV6
  writeU32LE(result, 12, 0'u32)
  # Bytes 16..23 stay zero: profile 0 (full), compression 0 (none), reserved.
  for k in 0 ..< V6Auto * 24: result[24 + k] = v5[16 + k]
  for k in int(BS) ..< v5.len: result[k] = v5[k]

proc names(n: int): seq[string] =
  for i in 0 ..< n: result.add(memberName(i))

proc readsAll(data: seq[byte], count: int, maxEntries: uint32, what: string) =
  ## Every member `m0 .. m<count-1>` reads back byte-exact, is present, and
  ## sits in its own slot.
  var bad = 0
  for i in 0 ..< count:
    let name = memberName(i)
    let got = readInternalFile(data, name, BS, maxEntries)
    if got.isErr or got.get() != memberContent(i): inc bad
    if not hasInternalFile(data, name, maxEntries): inc bad
    let e = findFileEntry(data, name, maxEntries)
    if not e.found or e.index != i: inc bad
  check(bad == 0, what & ": " & $bad & " of " & $(3 * count) &
    " member checks failed (first: " & (block:
      let r = readInternalFile(data, memberName(count - 1), BS, maxEntries)
      if r.isErr: r.unsafeError else: "read ok") & ")")

template testCase(name: string, body: untyped) =
  casesRun.add(name)
  let before = failures.len
  block:
    body
  echo (if failures.len == before: "PASS: " else: "FAIL: "), name

let v5Explicit = written(V5Auto, names(V5Auto), memberContent)
var v5Zero = v5Explicit
v5Zero.declare(0)
doAssert v5Zero[16 .. ^1] == v5Explicit[16 .. ^1], "only the declaration differs"

testCase "v5 explicit 170, readInternalFile/hasInternalFile/findFileEntry (control)":
  readsAll(v5Explicit, V5Auto, uint32(V5Auto), "explicit 170")

testCase "v5 declared 0, the header's value passed through":
  readsAll(v5Zero, V5Auto, 0'u32, "declared 0")

testCase "v6 explicit 169 (control)":
  var v6 = asV6(written(V6Auto, names(V6Auto), memberContent))
  v6.declare(uint32(V6Auto))
  readsAll(v6, V6Auto, uint32(V6Auto), "v6 explicit 169")

testCase "v6 declared 0, the header's value passed through":
  let v6 = asV6(written(V6Auto, names(V6Auto), memberContent))
  readsAll(v6, V6Auto, 0'u32, "v6 declared 0")

proc ghostContent(at: int): proc (i: int): seq[byte] =
  ## `memberContent`, except that member 1 -- the first member with data, so
  ## its bytes open block 1 -- carries the name word of `ghost` at byte `at`.
  ## That is where the name word of the slot just past the auto-filled root
  ## would lie: block 1's bytes 0..7 at version 6 (slot 169 spans 4080..4103)
  ## and 16..23 at version 5 (slot 170 spans 4096..4119).
  result = proc (i: int): seq[byte] =
    result = memberContent(i)
    if i == 1:
      result.setLen(64)
      let w = base40Encode("ghost")
      for k in 0 ..< 8: result[at + k] = byte((w shr (8 * k)) and 0xFF)

testCase "the auto-filled root ends at block 0 (one slot more finds a ghost)":
  var v5 = written(V5Auto, names(V5Auto), ghostContent(16))
  v5.declare(0)
  check(findFileEntry(v5, memberName(V5Auto - 1), 0'u32).found,
    "v5 declared 0: the member in slot 169 is not found")
  check(not hasInternalFile(v5, "ghost", 0'u32),
    "v5 declared 0: a slot past the auto-filled root was scanned")
  let v6 = asV6(written(V6Auto, names(V6Auto), ghostContent(0)))
  check(findFileEntry(v6, memberName(V6Auto - 1), 0'u32).found,
    "v6 declared 0: the member in slot 168 is not found")
  check(not hasInternalFile(v6, "ghost", 0'u32),
    "v6 declared 0: a slot past the auto-filled root was scanned")
  # The ghost word is where the probe says it is: an explicit count one past
  # the auto-filled root finds it.
  check(hasInternalFile(v5, "ghost", uint32(V5Auto + 1)) and
    hasInternalFile(v6, "ghost", uint32(V6Auto + 1)),
    "the ghost word is not at the slot past the auto-filled root")

testCase "rootMembers enumerates the auto-filled root":
  for requested in [0'u32, DefaultMaxRootEntries]:
    var seen = initHashSet[string]()
    for (name, size) in rootMembers(v5Zero, requested):
      seen.incl(base40Decode(name))
    check(seen == toHashSet(names(V5Auto)),
      "rootMembers(declared 0, " & $requested & ") lists " & $seen.len &
      " of " & $V5Auto & " members")

proc viewAll(path: string) =
  ## Every member through an image read from its file; the image (and its
  ## file handle) is released when this returns.
  let image = openFileImage(path, BS, 0'u32)
  check(image.isOk, "openFileImage: " & (if image.isErr: image.unsafeError else: ""))
  if image.isOk:
    var bad = 0
    for i in 0 ..< V5Auto:
      let v = viewMember(image.get(), memberName(i), BS, 0'u32)
      if v.isErr or v.get().contents().get() != memberContent(i): inc bad
    check(bad == 0, "viewMember: " & $bad & " of " & $V5Auto & " members")

testCase "openFileImage + viewMember with the header's value":
  let path = getTempDir() / "ctfs_root_auto_fill_view.ct"
  writeFile(path, cast[string](v5Zero))
  viewAll(path)
  try: removeFile(path) except OSError: discard

testCase "analyzeCtfs reports every member of the auto-filled root":
  let r = analyzeCtfs(v5Zero, int(BS))
  check(r.isOk, "analyzeCtfs: " & (if r.isErr: r.unsafeError else: ""))
  if r.isOk:
    var seen = initHashSet[string]()
    for f in r.get().files: seen.incl(f.name)
    check(seen == toHashSet(names(V5Auto)),
      "analyzeCtfs lists " & $seen.len & " of " & $V5Auto & " members")

proc threadFileName(tid: int): string =
  result = "t"
  for _ in 0 ..< (11 - ($tid).len): result.add('0')
  result.add($tid)

testCase "detectNativeBundle on a declared 0":
  # meta.dat and two thread streams, the last of them in slot 169.
  var bundleNames: seq[string]
  for i in 0 ..< V5Auto - 3: bundleNames.add(memberName(i))
  bundleNames.add("meta.dat")
  bundleNames.add(threadFileName(3))
  bundleNames.add(threadFileName(7))
  var bundle = written(V5Auto, bundleNames, memberContent)
  bundle.declare(0)
  let info = detectNativeBundle(bundle)
  check(info.isOk, "detectNativeBundle: " & (if info.isErr: info.unsafeError else: ""))
  if info.isOk:
    check(info.get().maxRoot == uint32(V5Auto),
      "detectNativeBundle: maxRoot " & $info.get().maxRoot)
    check(info.get().threadStreams.len == 2,
      "detectNativeBundle: " & $info.get().threadStreams.len & " thread streams")
    check(info.get().metaDat == memberContent(V5Auto - 3),
      "detectNativeBundle: meta.dat differs")

testCase "openTrace on a trace whose header declares 0":
  let path = getTempDir() / "ctfs_root_auto_fill_trace.ct"
  var w = initMultiStreamWriter(path, "/auto/fill/program").get()
  w.metadata.args = @["--one", "two"]
  w.metadata.workdir = "/auto/fill"
  let p = w.registerPath("/auto/fill/src.nim").get()
  for line in [1'u64, 2, 3]:
    doAssert w.registerStep(p, line, []).isOk
  doAssert w.close().isOk
  doAssert w.closeCtfs().isOk
  let control = openTrace(path)
  check(control.isOk, "openTrace (as written): " &
    (if control.isErr: control.unsafeError else: ""))
  var bytes = readCtfsFromFile(path).get()
  # The writer declared 31; relabelling it 0 is the same container only if the
  # slots auto-fill adds (31 .. 169) are empty.
  let declaredByWriter = int(readU32LE(bytes, 12))
  for k in 16 + declaredByWriter * 24 ..< int(BS):
    doAssert bytes[k] == 0, "slot bytes past the writer's declaration are not empty"
  bytes.declare(0)
  writeFile(path, cast[string](bytes))
  let zero = openTrace(path)
  check(zero.isOk, "openTrace (declared 0): " &
    (if zero.isErr: zero.unsafeError else: ""))
  if control.isOk and zero.isOk:
    check(zero.get().metadata.program == control.get().metadata.program and
      zero.get().metadata.args == control.get().metadata.args and
      zero.get().metadata.workdir == control.get().metadata.workdir and
      zero.get().paths == control.get().paths and
      zero.get().paths.len == 1,
      "openTrace (declared 0) reads different metadata or paths")
  try: removeFile(path) except OSError: discard

const Expected = 10
for f in failures:
  echo "  failure: ", f
echo "cases run: ", casesRun.len, " of ", Expected, "; failures: ", failures.len
if casesRun.len != Expected:
  echo "FAIL: ", casesRun.len, " cases ran, expected ", Expected
  quit(2)
if failures.len > 0:
  quit(1)
echo "PASS: test_root_auto_fill"
