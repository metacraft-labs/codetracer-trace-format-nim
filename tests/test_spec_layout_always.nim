## Every container is read in the spec stream layout, whatever its `meta.dat`
## stream-presence bits say (`internal-files.md` §"Stream-presence flags are a
## hint, not a gate"), and a container in the retired record-table layout --
## values, I/O events and calls as uncompressed `.off` tables -- is refused,
## naming the member.
##
## 1. A real recording, its `meta.dat` rewritten with every presence bit
##    clear, reads exactly as the recording does. A reader that chose a
##    layout from those bits misreads or refuses it.
## 2. A container in the retired layout, laid out by hand as no writer
##    produces it (`wasm/standalone/trace_reader_corpus_build.nim`), is
##    refused by name, with its bits clear and with them claiming the spec
##    layout. A reader that still read that layout opens it.
## No mocks.

import std/[os, strutils]
import results
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_ctfs/member_view
import codetracer_ctfs/base40
import codetracer_trace_types
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/new_trace_reader
import ../wasm/standalone/trace_reader_corpus_build

proc recording(): seq[byte] =
  let path = getTempDir() / "spec_layout_always.ct"
  removeFile(path)
  var w = initMultiStreamWriter(path, "bits", chunkSize = 16).get()
  let p = w.registerPath("/src/a.py").get()
  let f = w.registerFunctionAt("/src/a.py", 1, "f").get()
  doAssert w.registerCall(f, []).isOk
  for i in 0 ..< 50:
    doAssert w.registerStep(p, uint64(1 + i mod 5), []).isOk
    if i mod 10 == 0:
      doAssert w.registerIOEvent(elkWrite, @[byte('x')]).isOk
  doAssert w.registerReturn().isOk
  doAssert w.close().isOk
  doAssert w.closeCtfs().isOk
  let s = readFile(path)
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len: result[i] = byte(s[i])
  removeFile(path)

proc withPresenceBitsClear(bytes: seq[byte]): seq[byte] =
  ## `bytes` with `meta.dat` rewritten carrying the same metadata and no
  ## stream-presence bit, every other member copied as it is.
  let meta = readMetaDat(readInternalFile(bytes, "meta.dat").get()).get()
  var c = createCtfs()
  for (name, _) in rootMembers(bytes, DefaultMaxRootEntries):
    let n = base40Decode(name)
    var f = c.addFile(n).get()
    if n == "meta.dat":
      doAssert c.writeMetaDat(f, TraceMetadata(recordingId: meta.recordingId,
        program: meta.program, args: meta.args, workdir: meta.workdir),
        hasStepStream = false, hasValueStream = false,
        hasIoEventStream = false).isOk
    else:
      doAssert c.writeToFile(f, readInternalFile(bytes, n).get()).isOk
  c.toBytes()

proc everything(bytes: seq[byte]): string =
  var r = openNewTraceFromBytes(bytes).get()
  result.add($r.stepCount().get() & "|" & $r.callCount().get() & "|" &
    $r.ioEventCount().get() & "|")
  for i in 0'u64 ..< r.stepCount().get():
    result.add($r.stepAbsoluteGlobalLineIndex(i).get() & ",")
  for i in 0'u64 ..< r.ioEventCount().get():
    result.add($r.ioEvent(i).get().stepId & ";")
  for k in 0'u64 ..< r.callCount().get():
    let c = r.call(k).get()
    result.add($c.entryStep & "-" & $c.exitStep & ";")

proc test_a_container_with_every_presence_bit_clear_reads_alike() =
  let bytes = recording()
  let cleared = withPresenceBitsClear(bytes)
  let meta = readMetaDat(readInternalFile(cleared, "meta.dat").get()).get()
  doAssert not meta.hasStepStream and not meta.hasValueStream and
    not meta.hasIoEventStream, "the bits were not cleared"
  doAssert everything(cleared) == everything(bytes),
    "the container with its bits clear reads differently"
  echo "PASS: test_a_container_with_every_presence_bit_clear_reads_alike"

proc test_a_container_in_the_retired_layout_is_refused_by_name() =
  for (what, built) in [("bits clear", buildLegacyCorpus()),
                        ("bits claiming the spec layout",
                         buildMisframedLegacyCorpus())]:
    let r = openNewTraceFromBytes(built.get())
    doAssert r.isErr, "a container in the retired layout (" & what &
      ") was read"
    doAssert "values.off" in r.unsafeError, what & ": " & r.unsafeError
  echo "PASS: test_a_container_in_the_retired_layout_is_refused_by_name"

test_a_container_with_every_presence_bit_clear_reads_alike()
test_a_container_in_the_retired_layout_is_refused_by_name()
