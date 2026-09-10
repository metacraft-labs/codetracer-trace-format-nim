## The file boundary in the global position space.
##
## An address is `prefixSum[file_id] + (line - 1)`, and the inverse is
## `line = p - prefixSum[f] + 1` (spec:
## `codetracer-trace-format-spec/internal-files.md` §"Global Line Index"
## and `trace-events.md` §"Decoding `global_position_index`"). The `- 1` is
## what makes a file's slot hold exactly the lines it has: the first line
## at the file's own base, the last at `base + count - 1`, none left over
## and none spilling into the next file.
##
## Encoding `prefixSum[file_id] + line` instead leaves the base unused and
## pushes a file's last line one address past the end of its own range.
## An oversized stride hides that — the unused address at each base
## absorbs it — and it becomes a wrong answer at every file boundary the
## moment a slot is sized to a real line count: with counts `[10, 10]`,
## `(file 0, line 10)` encodes to `10`, which decodes as `(file 1, line 0)`.
##
## Three boundaries carry the reproducer:
##
##   1. the last line of a non-final file, which is what spills;
##   2. the first line of a non-initial file, which is the address the
##      spill collides with;
##   3. the last line of the *final* file, which under the `+ line` encode
##      lands on `totalLines` — one past the top of the space — and is
##      refused by `tryResolve` rather than answered.
##
## Each is asserted twice: against the arithmetic at real per-file line
## counts, which is what a slot sized to `line_count` will use, and end to
## end through this repository's writer and reader at the
## `DefaultLinesPerFile` stride they use today. The stride case must hold
## either way — a file's last addressable line is `DefaultLinesPerFile`,
## and under the `+ line` encode that step reads back as line 0 of the
## next file.
##
## What is asserted here, and why each one can fail:
##
##   1. At counts `[10, 10]` the twenty lines occupy exactly the twenty
##      addresses `0 .. 19`. A `+ line` encode puts line 1 at address 1
##      and line 10 at address 10, and fails on the named address.
##   2. Line 0 — not a source line — does not address below the file's
##      base. Unclamped, `line - 1` wraps to `2^64 - 1` on file 0.
##   3. Every `(file, line)` a space can hold round-trips through its
##      address, no two lines share an address, and no address belongs to
##      no line. A `+ line` encode collides the last line of one file with
##      the first of the next, and fails all three halves.
##   4. The same three boundaries at the stride, arithmetically and end to
##      end. The end-to-end half is what a `+ line` encode turns into a
##      trace that reads back at coordinates nothing was registered at.
##
## No mocks: the container is produced by `MultiStreamTraceWriter` and read
## back through `openTrace` / `readEvents`, the same path a debugger takes.

import std/[os, assertions]
import results
import codetracer_trace_types
import codetracer_trace_reader
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/global_line_index

const
  PathA = "/src/first.py"
  PathB = "/src/second.py"

let dir = getTempDir() / "ctfnim-gli-boundary"

proc lineOnlySpace(pathCount: int): GlobalLineIndex =
  ## The space the writer encodes line-only steps in today: every file gets
  ## `DefaultLinesPerFile` addresses.
  var counts = newSeq[uint64](pathCount)
  for i in 0 ..< pathCount:
    counts[i] = DefaultLinesPerFile
  buildGlobalLineIndex(counts)

# ---------------------------------------------------------------------------

proc test_real_line_counts_boundary_is_exact() =
  ## THE REPRODUCER, at the arithmetic. Two files of ten lines each. The
  ## twenty lines must occupy the twenty addresses `0 .. 19` — no address
  ## unused at either base, and nothing above the top.
  let gli = buildGlobalLineIndex(@[10'u64, 10'u64])
  doAssert gli.totalLines == 20'u64,
    "two files of ten lines address twenty positions; got " & $gli.totalLines

  doAssert gli.globalIndex(0, 1) == 0'u64,
    "file 0's first line must sit at file 0's own base 0; got " &
    $gli.globalIndex(0, 1)
  doAssert gli.globalIndex(0, 10) == 9'u64,
    "file 0's LAST line must be the last address of file 0's range (9), " &
    "not the first address of file 1's (10); got " & $gli.globalIndex(0, 10)
  doAssert gli.globalIndex(1, 1) == 10'u64,
    "file 1's first line must sit at file 1's own base 10; got " &
    $gli.globalIndex(1, 1)
  doAssert gli.globalIndex(1, 10) == 19'u64,
    "file 1's last line must be the top of the space (19), not one past " &
    "it (20); got " & $gli.globalIndex(1, 10)

  # Boundary 1: the last line of the non-final file.
  let lastOfFirst = gli.tryResolve(gli.globalIndex(0, 10))
  doAssert lastOfFirst.isOk,
    "(file 0, line 10) must resolve: " & lastOfFirst.error
  doAssert lastOfFirst.get() == (0, 10'u64),
    "(file 0, line 10) must resolve to itself, not spill into file 1; got " &
    $lastOfFirst.get()

  # Boundary 2: the first line of the non-initial file — the address the
  # spill collides with.
  let firstOfSecond = gli.tryResolve(gli.globalIndex(1, 1))
  doAssert firstOfSecond.isOk,
    "(file 1, line 1) must resolve: " & firstOfSecond.error
  doAssert firstOfSecond.get() == (1, 1'u64),
    "(file 1, line 1) must resolve to itself; got " & $firstOfSecond.get()

  doAssert gli.globalIndex(0, 10) != gli.globalIndex(1, 1),
    "the last line of file 0 and the first line of file 1 must not share " &
    "an address; both encode to " & $gli.globalIndex(0, 10)

  # Boundary 3: the last line of the final file. Encoded as `base + line`
  # it is `totalLines`, which is outside the space and refused.
  let topOfSpace = gli.tryResolve(gli.globalIndex(1, 10))
  doAssert topOfSpace.isOk,
    "the last line of the last file must be addressable, not one past " &
    "the top of the space: " & topOfSpace.error
  doAssert topOfSpace.get() == (1, 10'u64),
    "(file 1, line 10) must resolve to itself; got " & $topOfSpace.get()

  doAssert gli.tryResolve(20'u64).isErr,
    "20 is one past a twenty-address space and must be refused"

  echo "PASS: test_real_line_counts_boundary_is_exact"

proc test_line_zero_does_not_wrap_below_the_file_base() =
  ## Line 0 is not a source line, and `line - 1` on it is where a 0-based
  ## offset can go wrong: unclamped it is `prefixSum[file] - 1`, which
  ## wraps to 2^64-1 for file 0 and lands in the previous file's LAST line
  ## for every other file. Neither is a location, and the first is a
  ## ten-byte varint on the wire.
  let gli = buildGlobalLineIndex(@[10'u64, 10'u64])
  for fileId in 0 .. 1:
    let p = gli.globalIndex(fileId, 0)
    doAssert p == gli.prefixSum[fileId],
      "line 0 of file " & $fileId & " must not address below the file's " &
      "own base " & $gli.prefixSum[fileId] & "; got " & $p
    doAssert p < gli.totalLines,
      "line 0 of file " & $fileId & " encoded to " & $p &
      ", outside a space of " & $gli.totalLines & " — the subtraction wrapped"

  echo "PASS: test_line_zero_does_not_wrap_below_the_file_base"

proc test_round_trip_over_every_registered_line() =
  ## THE PROPERTY, over multi-file spaces. For every `(file, line)` a space
  ## can hold, `resolve(globalIndex(file, line))` is that same pair, and no
  ## two pairs share an address.
  for counts in [@[10'u64, 10'u64], @[1'u64, 7'u64, 3'u64, 100'u64],
                 @[64'u64, 1'u64, 1'u64, 9'u64]]:
    let gli = buildGlobalLineIndex(counts)
    var seen = newSeq[bool](int(gli.totalLines))
    for fileId in 0 ..< counts.len:
      for line in 1'u64 .. counts[fileId]:
        let p = gli.globalIndex(fileId, line)
        doAssert p < gli.totalLines,
          "(file " & $fileId & ", line " & $line & ") in " & $counts &
          " encodes to " & $p & ", outside a space of " & $gli.totalLines
        doAssert not seen[int(p)],
          "(file " & $fileId & ", line " & $line & ") in " & $counts &
          " reuses address " & $p & ", which another line already holds"
        seen[int(p)] = true
        let got = gli.tryResolve(p)
        doAssert got.isOk,
          "(file " & $fileId & ", line " & $line & ") in " & $counts &
          " encodes to " & $p & ", which does not resolve: " & got.error
        doAssert got.get() == (fileId, line),
          "(file " & $fileId & ", line " & $line & ") in " & $counts &
          " round-tripped through " & $p & " to " & $got.get()
    for p in 0 ..< seen.len:
      doAssert seen[p],
        "address " & $p & " of a " & $gli.totalLines & "-address space " &
        "for " & $counts & " belongs to no line; the space is not exactly " &
        "the lines it was built from"

  echo "PASS: test_round_trip_over_every_registered_line"

proc test_stride_boundary_arithmetic() =
  ## The same three boundaries in the space the writer uses TODAY, where a
  ## file's slot is the `DefaultLinesPerFile` stride rather than its real
  ## line count. The oversized slot is what hides the off-by-one at every
  ## line but the last one a file can address — so the last one is asserted.
  let gli = lineOnlySpace(2)
  let stride = DefaultLinesPerFile
  doAssert gli.totalLines == 2'u64 * stride,
    "two files at " & $stride & " addresses each; got " & $gli.totalLines

  doAssert gli.globalIndex(0, stride) == stride - 1,
    "line " & $stride & " is the last line file 0's slot can address, so " &
    "it must be the slot's last address " & $(stride - 1) & "; got " &
    $gli.globalIndex(0, stride)
  let lastOfFirst = gli.tryResolve(gli.globalIndex(0, stride))
  doAssert lastOfFirst.isOk and lastOfFirst.get() == (0, stride),
    "(path 0, line " & $stride & ") must resolve to itself, not to line 0 " &
    "of path 1; got " & $lastOfFirst

  let firstOfSecond = gli.tryResolve(gli.globalIndex(1, 1))
  doAssert firstOfSecond.isOk and firstOfSecond.get() == (1, 1'u64),
    "(path 1, line 1) must resolve to itself; got " & $firstOfSecond

  let topOfSpace = gli.tryResolve(gli.globalIndex(1, stride))
  doAssert topOfSpace.isOk and topOfSpace.get() == (1, stride),
    "(path 1, line " & $stride & ") is the top of the space and must be " &
    "addressable, not refused as one past it; got " & $topOfSpace

  echo "PASS: test_stride_boundary_arithmetic"

proc test_stride_boundary_end_to_end() =
  ## END TO END, through the real writer and the real reader. A two-file
  ## trace whose steps sit on the last line of file 0 and the first line of
  ## file 1 must read back at the coordinates they were registered at.
  let stride = DefaultLinesPerFile
  let registered = [(0'u64, 1'u64), (0'u64, stride),
                    (1'u64, 1'u64), (1'u64, stride)]

  let file = dir / "gli_boundary.ct"
  var w = initMultiStreamWriter(file & ".build", "gli_boundary").get()
  doAssert w.registerPath(PathA).isOk
  doAssert w.registerPath(PathB).isOk
  for (p, l) in registered:
    doAssert w.registerStep(p, l, @[]).isOk,
      "registerStep(" & $p & ", " & $l & ") failed"
  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()
  writeFile(file, cast[string](bytes))

  var readerRes = openTrace(file)
  doAssert readerRes.isOk, "openTrace failed (see trace reader Result)"
  var reader = readerRes.get()
  let res = reader.readEvents()
  doAssert res.isOk,
    "a step on the last line of the last file is inside this trace's " &
    "address space and must not be refused: " & res.error

  var seen: seq[(uint64, uint64)]
  for ev in reader.events:
    if ev.kind == tleStep:
      seen.add((uint64(ev.step.pathId), uint64(int64(ev.step.line))))
  var want: seq[(uint64, uint64)]
  for pl in registered:
    want.add(pl)
  doAssert seen == want,
    "steps must read back at the (path, line) they were registered at.\n" &
    "  registered: " & $want & "\n  read back:  " & $seen

  echo "PASS: test_stride_boundary_end_to_end"

removeDir(dir)
createDir(dir)

test_real_line_counts_boundary_is_exact()
test_line_zero_does_not_wrap_below_the_file_base()
test_round_trip_over_every_registered_line()
test_stride_boundary_arithmetic()
test_stride_boundary_end_to_end()

removeDir(dir)
echo "ALL PASS: test_global_line_index_boundary"
