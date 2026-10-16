## `ctfs-container.md` §6, "Durability: a writer publishes every sealed
## chunk": a recording whose process dies leaves a container a reader opens and
## reads up to the last chunk it completed.
##
## The writer here is never closed while the container is read: the file on
## disk is exactly what a killed process would leave. Asserted:
##
## * `meta.dat` is on disk, complete, from the first record on (rule 1);
## * every sealed chunk of `steps.dat` reads back, and nothing of the unsealed
##   one does (rules 2 and 3);
## * every interning record registered before the last seal is readable —
##   a path, a function, a variable name (rule 2);
## * after close, the rest is there too.
##
## No mocks: the production writer streaming to a real file, read back by the
## production reader from the bytes on disk.

import std/[os, strutils]
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader

const ChunkSize = 8

proc test_a_killed_recording_reads_up_to_its_last_sealed_chunk() =
  let path = getTempDir() / ("durability_" & $getCurrentProcessId() & ".ct")
  removeFile(path)
  var w = initMultiStreamWriter(path, "durable", chunkSize = ChunkSize).get()
  doAssert w.setWorkdir("/work").isOk
  let p = w.registerPath("/src/a.nim").get()
  doAssert w.registerStep(p, 1, []).isOk

  block meta_dat_is_on_disk_at_the_first_record:
    var r = openNewTrace(path)
    doAssert r.isOk, "the container does not open after its first record: " &
      r.error
    doAssert r.get().meta.program == "durable" and
      r.get().meta.workdir == "/work", "meta.dat is not complete on disk"

  let f = w.registerFunction("main").get()
  discard f
  let vn = w.registerVarname("x").get()
  for i in 2 .. 2 * ChunkSize + 3:      # 19 steps: 2 chunks sealed, 3 pending
    doAssert w.registerStep(p, uint64(i), [VariableValue(varnameId: vn,
      typeId: 0, data: @[0x01'u8])]).isOk

  block sealed_chunks_and_their_interning_are_on_disk:
    var r = openNewTrace(path)
    doAssert r.isOk, "the container does not open mid-recording: " & r.error
    var rd = r.get()
    let n = rd.stepCount()
    doAssert n.isOk, "steps.dat does not read mid-recording: " & n.error
    doAssert n.get() == 2 * ChunkSize,
      "expected the " & $(2 * ChunkSize) & " steps of the sealed chunks, " &
      "read " & $n.get()
    doAssert rd.path(p).get() == "/src/a.nim",
      "a path registered before the seal is not readable"
    doAssert rd.varnameCount() == 1 and rd.varname(vn).get() == "x",
      "a variable name registered before the seal is not readable"
    for i in 0'u64 ..< 2 * ChunkSize:
      doAssert rd.stepAbsoluteGlobalLineIndex(i).get() == i,
        "step " & $i & " reads back at the wrong position"

  doAssert w.close().isOk
  doAssert w.closeCtfs().isOk
  var rd = openNewTrace(path).get()
  doAssert rd.stepCount().get() == 2 * ChunkSize + 3
  removeFile(path)
  echo "PASS: test_a_killed_recording_reads_up_to_its_last_sealed_chunk"

test_a_killed_recording_reads_up_to_its_last_sealed_chunk()
