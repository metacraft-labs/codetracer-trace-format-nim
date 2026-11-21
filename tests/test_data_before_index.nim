## Data before index in the chunked writers (`ctfs-container.md` §7, "Writer
## Protocol", and §6 "Durability" rule 2): a chunk's bytes, and its data
## member's entry, reach the file before the index entry that names the
## chunk. So at every write a reader could stop at, every offset a published
## `.idx` holds lies below its `.dat`'s published `Size`.
##
## The production multi-stream writer writes a trace (steps, values, calls,
## events) into a streaming container the test owns, with the write tap on.
## The file is rebuilt write by write, and at each prefix every `X.idx` the root
## publishes is checked against `X.dat`.
##
## Null prediction: a writer that published an index entry before the chunk's
## data member (the order this library's §7 text listed until it was
## corrected) is reported at the prefix between the two publications, naming
## the stream. The control `controlIndexFirst` below rebuilds the same writes
## with each `.idx` entry write moved before the last `.dat` entry write ahead
## of it, and must be reported.

import std/[os, strutils, tables]
import results
import codetracer_ctfs/[types, container, streaming, live_reader]
import codetracer_trace_writer/multi_stream_writer
import ./keyed_harness

proc indexOffsets(rd: var LiveReader, slot: int): seq[uint64] =
  ## A §7 index: `chunk_size: u32`, then `u64` offsets.
  let size = rd.memberSize(slot)
  if size < 4: return
  let b = rd.readMember(slot, 0, int(size)).get()
  var o = 4
  while o + 8 <= b.len:
    result.add readU64LE(b, o)
    o += 8

proc check(file: ref seq[byte], label: string): string =
  var rd = openImageReader(file).get()
  for i in 0 ..< rd.slots:
    let n = rd.slotName(i)
    if not n.endsWith(".idx") or rd.entries[i].size == 0: continue
    let dat = n[0 ..< n.len - 4] & ".dat"
    let ds = rd.findSlot(dat)
    let offs = indexOffsets(rd, i)
    if offs.len == 0: continue
    if ds < 0: return label & ": " & n & " names chunks and " & dat & " is absent"
    let dsize = rd.memberSize(ds)
    for o in offs:
      if o >= dsize:
        return label & ": " & n & " names a chunk at " & $o & ", and " & dat &
          "'s published Size is " & $dsize
  ""

proc replayCheck(log: TapLog): string =
  var rep = initReplay(log, false)
  while true:
    let m = check(rep.file, "prefix " & $rep.next & "/" & $log.events.len)
    if m.len > 0: return m
    if rep.atEnd: break
    rep.applyNext()
  ""

proc controlIndexFirst(log: TapLog, rootEnd: int64, slotOf: Table[int64, string]): TapLog =
  ## The same writes, with each `.idx` entry write moved to just before the
  ## last earlier write of its `.dat` entry: the index published before the
  ## chunk's data member.
  result = TapLog(file0: log.file0, page0: log.page0)
  for e in log.events:
    let n = if e.kind == teFile and e.off < rootEnd: slotOf.getOrDefault(e.off, "") else: ""
    if n.endsWith(".idx"):
      let dat = n[0 ..< n.len - 4] & ".dat"
      var j = result.events.len - 1
      while j >= 0 and not (result.events[j].kind == teFile and result.events[j].off < rootEnd and
          slotOf.getOrDefault(result.events[j].off, "") == dat):
        dec j
      if j >= 0:
        result.events.insert(e, j)
        continue
    result.events.add e

when isMainModule:
  let path = getTempDir() / ("ckf4n_dbi_" & $getCurrentProcessId() & ".ct")
  removeFile(path)
  var c = createCtfsStreaming(path).get()
  let log = installTaps(c)
  var w = initMultiStreamWriterAttached(addr c, "dbi", chunkSize = 8).get()
  doAssert w.setWorkdir("/w").isOk
  let p = w.registerPath("/src/a.nim").get()
  let vn = w.registerVarname("x").get()
  for i in 1 .. 300:
    doAssert w.registerStep(p, uint64(i mod 40 + 1), [VariableValue(varnameId: vn,
      data: @[byte(i and 0xff)])]).isOk
    if i mod 50 == 0: c.publish()
  doAssert w.close().isOk
  c.removeTaps()
  doAssert c.closeCtfs().isOk
  var slotOf: Table[int64, string]
  block:
    var r = openLiveReader(path, attach = false).get()
    for i in 0 ..< r.slots:
      if r.entries[i].name != 0: slotOf[int64(16 + 24 * i)] = r.slotName(i)
    var n = 0
    for i in 0 ..< r.slots:
      if r.slotName(i).endsWith(".idx") and r.entries[i].size > 4: inc n
    doAssert n >= 2, "the trace has " & $n & " chunked streams with sealed chunks"
    r.close()
  removeFile(path)
  let m = replayCheck(log)
  if m.len > 0:
    echo "FAIL: ", m
    quit(1)
  let ctl = replayCheck(controlIndexFirst(log, 4096, slotOf))
  if ctl.len == 0:
    echo "FAIL: the control (index entries before data entries) was not reported"
    quit(1)
  echo "  control reported: ", ctl
  echo "PASS test_data_before_index (", log.events.len, " writes checked)"
