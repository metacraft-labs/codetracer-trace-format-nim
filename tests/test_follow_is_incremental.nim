## Following a container while it is written (`ctfs-container.md` §6): a
## refresh of a `NewTraceReader` extends the stream readers it has opened
## rather than opening the container again.
##
## The recording is written by the split-stream writer into a real file and
## read back while the writer is open. What is asserted, and how each fails:
##
## 1. After a refresh the reader answers what a fresh open answers.
## 2. A refresh that finds nothing new decodes no chunk, and one that finds
##    new chunks decodes exactly one (the new last chunk, to count it): a
##    refresh that opened the container again would drop the stream reader
##    and its decode count.
## 3. A container whose member shrank is refused, naming the member.
## No mocks.

import std/[os, strutils]
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader

const ChunkSize = 64

proc stage(w: var MultiStreamTraceWriter, p: uint64, n: int) =
  for i in 0 ..< n:
    doAssert w.registerStep(p, uint64(1 + i mod 7), []).isOk

proc gli(r: var NewTraceReader): seq[uint64] =
  let n = r.stepCount().get()
  for i in 0'u64 ..< n:
    result.add(r.stepAbsoluteGlobalLineIndex(i).get())

proc test_refresh_is_incremental() =
  let path = getTempDir() / "follow_incremental.ct"
  removeFile(path)
  var w = initMultiStreamWriter(path, "follow", chunkSize = ChunkSize).get()
  let p = w.registerPath("/src/f.py").get()
  w.stage(p, 3 * ChunkSize + 5)
  var r = openNewTrace(path).get()
  let first = r.stepCount().get()
  doAssert first == 3 * ChunkSize, "sealed steps readable: " & $first
  discard r.step(first - 1).get()
  let decoded = r.execChunkDecompressions()

  doAssert r.refresh(path).isOk
  doAssert r.execStreamLoaded(), "a refresh dropped the step reader"
  doAssert r.execChunkDecompressions() == decoded,
    "a refresh that found nothing new decoded a chunk"

  w.stage(p, 2 * ChunkSize)
  doAssert r.refresh(path).isOk
  doAssert r.execChunkDecompressions() == decoded + 1,
    "a refresh that found new chunks decoded " &
    $(r.execChunkDecompressions() - decoded) & " chunks, not one"
  var fresh = openNewTrace(path).get()
  doAssert r.gli() == fresh.gli(), "the refreshed reader differs from a fresh one"
  doAssert r.stepCount().get() == 5 * ChunkSize

  doAssert w.close().isOk
  doAssert w.closeCtfs().isOk
  doAssert r.refresh(path).isOk
  var closed = openNewTrace(path).get()
  doAssert r.gli() == closed.gli()
  doAssert r.stepCount().get() == closed.stepCount().get()
  removeFile(path)
  echo "PASS: test_refresh_is_incremental"

proc test_a_member_that_shrank_is_refused() =
  let path = getTempDir() / "follow_shrank.ct"
  removeFile(path)
  var w = initMultiStreamWriter(path, "follow", chunkSize = ChunkSize).get()
  let p = w.registerPath("/src/f.py").get()
  w.stage(p, 3 * ChunkSize)
  doAssert w.close().isOk
  doAssert w.closeCtfs().isOk
  var r = openNewTrace(path).get()
  discard r.stepCount().get()
  # Shrink the first member larger than a word by one byte.
  var d = readFile(path)
  var found = false
  for slot in 0 ..< 31:
    let at = 16 + slot * 24
    var size = 0'u64
    for i in 0 ..< 8: size = size or (uint64(byte(d[at + i])) shl (8 * i))
    var name = 0'u64
    for i in 0 ..< 8: name = name or (uint64(byte(d[at + 16 + i])) shl (8 * i))
    if name != 0 and size > 8 and not found:
      let s = size - 1
      for i in 0 ..< 8: d[at + i] = char((s shr (8 * i)) and 0xFF)
      found = true
  doAssert found
  writeFile(path, d)
  let res = r.refresh(path)
  doAssert res.isErr, "a container whose member shrank was followed"
  doAssert "shrank" in res.unsafeError, res.unsafeError
  removeFile(path)
  echo "PASS: test_a_member_that_shrank_is_refused"

test_refresh_is_incremental()
test_a_member_that_shrank_is_refused()
