## GDH-OQ-7 — what does a session with HUNDREDS of edits to one file cost?
##
## Not a test: a MEASUREMENT, run by hand to close the open question, and kept
## so the number can be re-measured rather than remembered.  It is deliberately
## absent from the nimble `test` task — it measures the host, and a threshold
## over it would be a flake (see this repo's CLAUDE.md on `bench` vs `test`).
##
## Build & run:
##   nim c -d:release --mm:arc -p:src -o:/tmp/gdh6_scale \
##       tests/gdh6_version_scale.nim && /tmp/gdh6_scale
##
## THE QUESTION, from the design doc's open-question list:
##
##   "Multi-reload of the same file: does anything cap the number of versions,
##    and does the position space's growth (one slot per version) matter for a
##    session with hundreds of edits?  The slot cost is the file's line count
##    per version, which is small, but `paths.dat` and the prefix-sum array
##    grow linearly and `rebuildGli` is O(files) on every registration."
##
## WHAT IS ACTUALLY MEASURED HERE
##
##   * wall time to register N versions of ONE file, with a step emitted
##     against each version — because `rebuildGli` is LAZY (`gliDirty`), so a
##     registration with no following step costs nothing and measuring
##     registrations alone would flatter the result;
##   * the container's size on disk, so "paths.dat grows linearly" is a number
##     of bytes rather than an adjective;
##   * the top of the position space after N versions, against the uint64
##     ceiling, so "does anything cap the number of versions" has an answer.

import std/[os, times, strformat]
import results
import codetracer_trace_writer/multi_stream_writer

const Lines = 400'u64  ## a realistically-sized source file

proc measure(versions: int, dir: string): tuple[seconds: float, bytes: int64,
    top: uint64] =
  createDir(dir)
  let init = initMultiStreamWriter(dir / "trace.build", "gdh6_scale")
  doAssert init.isOk, init.error
  var w = init.get()
  doAssert w.enableLineCountTable().isOk
  let firstId = w.registerPath("res://probe.gd", lineCount = Lines)
  doAssert firstId.isOk
  doAssert w.registerStep(firstId.get(), 1'u64, @[]).isOk

  let start = epochTime()
  var lastId = firstId.get()
  for i in 1 .. versions:
    let id = w.registerPathVersion("res://probe.gd", Lines)
    doAssert id.isOk, id.error
    lastId = id.get()
    # A step against the NEW version is what forces the lazy `rebuildGli`.
    # Without it the prefix-sum array is rebuilt once at close and the
    # measurement says nothing about the per-registration cost.
    doAssert w.registerStep(lastId, uint64(1 + (i mod int(Lines))), @[]).isOk
  let elapsed = epochTime() - start

  doAssert w.close().isOk
  let bytes = w.toBytes()
  discard w.closeCtfs()
  result.seconds = elapsed
  result.bytes = int64(bytes.len)
  # The top of the position space: every file's slot, summed.
  result.top = uint64(versions + 1) * Lines

proc main() =
  let root = getTempDir() / "gdh6-scale-" & $getCurrentProcessId()
  createDir(root)
  defer: removeDir(root)
  echo "GDH-OQ-7 — cost of N versions of one ", Lines, "-line file"
  echo ""
  # The byte columns, spelled out because a mislabelled one was quoted into the
  # spec (fixed at GDH-M6's review, 2026-09-11).
  #
  # `d bytes` is the growth since the PREVIOUS ROW and `B/ver` divides it by the
  # versions added in that step — i.e. the MARGINAL cost of one more version at
  # this size.  A single column headed `bytes/ver` used to print the raw row
  # delta, so the number a reader took as "bytes per version" was actually
  # "bytes added since the last row", too big by the step width.  The spec's
  # "~98 bytes per version" happens to be right because it was derived by
  # dividing afterwards; the printed column was not.
  #
  # `avg B/ver` is the total growth over the BASELINE row divided by all N
  # versions, which is the honest figure for "what did this whole session
  # cost".  The two differ (90 vs 98 at N=1000) and both are worth having: the
  # marginal rate is what the next reload costs, the average is what the
  # session cost.
  #
  # CAVEAT, and it bounds every byte figure here: the container grows in 4 KiB
  # BLOCKS, so `bytes` is quantised and the rates are only meaningful across a
  # span of many versions.  Row-to-row at small N they are mostly rounding.
  echo &"""{"versions":>9} {"seconds":>10} {"us/version":>12} {"bytes":>10} """ &
       &"""{"d bytes":>9} {"B/ver":>8} {"avg B/ver":>10} {"space top":>12}"""
  var prevBytes = 0'i64
  var prevVersions = 0
  var baseBytes = 0'i64
  for versions in [1, 10, 50, 100, 250, 500, 1000]:
    let m = measure(versions, root / $versions)
    let perVersion = m.seconds * 1_000_000.0 / float(versions)
    if baseBytes == 0: baseBytes = m.bytes
    let deltaBytes = if prevBytes == 0: 0'i64 else: m.bytes - prevBytes
    let marginal =
      if prevVersions == 0 or versions == prevVersions: 0.0
      else: float(deltaBytes) / float(versions - prevVersions)
    let average = float(m.bytes - baseBytes) / float(versions)
    prevBytes = m.bytes
    prevVersions = versions
    echo &"{versions:>9} {m.seconds:>10.4f} {perVersion:>12.1f} {m.bytes:>10} " &
         &"{deltaBytes:>9} {marginal:>8.1f} {average:>10.1f} {m.top:>12}"
  echo ""
  echo "uint64 position-space ceiling: ", high(uint64)
  echo "versions of a ", Lines, "-line file before it is reached: ",
    high(uint64) div Lines

when isMainModule:
  main()
