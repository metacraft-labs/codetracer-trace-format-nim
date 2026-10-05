## The split-stream writer writes the flag-gated `meta.dat` blocks it is given
## (`internal-files.md` §"Extended Fields": MCR fields, replay-launch fields, a
## layout snapshot, the filter provenance chain), and refuses to be given one
## once `meta.dat` is written, at the first record. Read back through
## `readMetaDat` and through `NewTraceReader` out of a real container. No
## mocks.

import std/[options, strutils]
import results
import codetracer_ctfs
import codetracer_trace_types
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader

proc fixtureMcr(): McrMetaFields =
  McrMetaFields(tickSource: tsPerfCounter, totalThreads: 7,
    atomicMode: amSeqCst, totalEvents: 1'u64 shl 40, totalCheckpoints: 300,
    startTimeUnixUs: 1_760_000_000_000_000'u64, platform: "linux-x86_64",
    tickGranularity: "ns", tickSourceStr: "perf_counter",
    atomicModeStr: "seq_cst", startTimeStr: "2026-10-05T22:00:00Z",
    hookProfile: "default", hookStrategies: @["ldpreload", "seccomp_unotify"])

proc fixtureLayout(): LayoutSnapshotFields =
  var fp: seq[byte]
  for i in 0 ..< 200: fp.add(byte(i))
  LayoutSnapshotFields(layoutHash: 0x0123_4567_89ab_cdef'u64,
    layoutFingerprint: fp)

proc metaOf(w: var MultiStreamTraceWriter): MetaDatContents =
  readMetaDat(readInternalFile(w.toBytes(), "meta.dat").get()).get()

block the_writer_writes_the_blocks_it_is_given:
  var w = initMultiStreamWriter("", "prog").get()
  doAssert w.setMcrFields(fixtureMcr()).isOk
  doAssert w.setReplayLaunchFields(ReplayLaunchFields(aslrDisabled: true)).isOk
  doAssert w.setLayoutSnapshot(fixtureLayout()).isOk
  doAssert w.registerPath("/src/a.py").isOk
  doAssert w.registerStep(0, 1, []).isOk
  doAssert w.close().isOk
  let m = w.metaOf()
  doAssert m.mcrFields == some(fixtureMcr()), $m.mcrFields
  doAssert m.replayLaunchFields == some(ReplayLaunchFields(aslrDisabled: true))
  doAssert m.layoutSnapshotFields == some(fixtureLayout())
  doAssert not m.hasFilterProvenance
  # The reader of the whole container answers the same.
  let r = openNewTraceFromBytes(w.toBytes()).get()
  doAssert r.meta.mcrFields == some(fixtureMcr())
  doAssert r.meta.layoutSnapshotFields == some(fixtureLayout())
  echo "PASS the_writer_writes_the_blocks_it_is_given"

block a_writer_given_none_writes_none:
  # Control: the same recording without the setters carries no block.
  var w = initMultiStreamWriter("", "prog").get()
  doAssert w.registerPath("/src/a.py").isOk
  doAssert w.registerStep(0, 1, []).isOk
  doAssert w.close().isOk
  let m = w.metaOf()
  doAssert m.mcrFields.isNone and m.replayLaunchFields.isNone and
    m.layoutSnapshotFields.isNone
  echo "PASS a_writer_given_none_writes_none"

block a_block_given_after_the_first_record_is_refused:
  var w = initMultiStreamWriter("", "prog").get()
  doAssert w.registerPath("/src/a.py").isOk
  doAssert w.registerStep(0, 1, []).isOk
  for (what, r) in [("setMcrFields", w.setMcrFields(fixtureMcr())),
      ("setReplayLaunchFields",
        w.setReplayLaunchFields(ReplayLaunchFields(aslrDisabled: true))),
      ("setLayoutSnapshot", w.setLayoutSnapshot(fixtureLayout()))]:
    doAssert r.isErr and what in r.error and "first record" in r.error,
      what & ": " & $r
  doAssert w.close().isOk
  let m = w.metaOf()
  doAssert m.mcrFields.isNone and m.replayLaunchFields.isNone and
    m.layoutSnapshotFields.isNone, "a refused block is not written"
  echo "PASS a_block_given_after_the_first_record_is_refused"

echo "ALL PASS test_writer_meta_blocks"
