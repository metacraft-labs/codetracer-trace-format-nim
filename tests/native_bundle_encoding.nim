## The native recorder's on-disk encodings, written by hand for tests.
##
## The native MCR recorder (`codetracer-native-recorder/ct_recorder`) is not a
## dependency of this repo, so the tests that need a native bundle write its
## per-thread event streams and its OS event log themselves. Two tests do
## (`test_ct_print_native.nim`, `test_ct_print_full_streams.nim`); this is
## their one copy of the layouts, so a layout change lands once.
##
## Not a test: no `test_` prefix, absent from the nimble `test` task and
## `repro.nim`.

import stew/endians2

proc threadFileName*(tid: uint32): string =
  ## Same convention as ct_recorder/trace_writer.threadFileName: 't' +
  ## 11 zero-padded decimal digits.
  let n = $tid
  result = "t"
  for _ in 0 ..< (11 - n.len):
    result.add('0')
  result.add(n)

proc encodeEventHeader*(
    eventType: uint16, size: uint32, ctTid: uint32,
    geid: uint64, tick: uint64): seq[byte] =
  ## Little-endian 26-byte EventHeader (see ct_events/header.nim).
  result.add toBytesLE(eventType)
  result.add toBytesLE(size)
  result.add toBytesLE(ctTid)
  result.add toBytesLE(geid)
  result.add toBytesLE(tick)
  doAssert result.len == 26

proc encodeOsEntry*(
    geid, tick: uint64, tid: uint32, kind: uint8,
    fd: int32, returnValue: int64, metadata: string,
    content: openArray[byte]): seq[byte] =
  ## Mirror of ct_recorder/event_log_writer.serializeEntry.
  result.add toBytesLE(geid)
  result.add toBytesLE(tick)
  result.add toBytesLE(tid)
  result.add kind
  result.add toBytesLE(cast[uint32](fd))
  result.add toBytesLE(cast[uint64](returnValue))
  result.add toBytesLE(uint16(metadata.len))
  for c in metadata: result.add byte(c)
  result.add toBytesLE(uint32(content.len))
  for b in content: result.add b

proc encodeOsEventLogChunk*(entries: seq[seq[byte]]): seq[byte] =
  ## One chunk inside `eventlog.dat`: u32 size_of_payload | u32 entry_count
  ## | <serialized entries>.
  var body: seq[byte] = @[]
  body.add toBytesLE(uint32(entries.len))
  for e in entries: body.add e
  result.add toBytesLE(uint32(body.len))
  result.add body

proc encodeOsEventLogIndex*(
    entryCount: uint32,
    chunkOffsets, chunkGeids: openArray[uint64]): seq[byte] =
  ## `eventlog.idx`, as event_log_writer.finalize writes it:
  ##   "EIDX" | version u16 | entry_count u32 | chunk_size u32
  ##   | chunk_count u32 | chunk_offsets[u64] | chunk_geids[u64]
  doAssert chunkOffsets.len == chunkGeids.len
  for c in "EIDX": result.add byte(c)
  result.add toBytesLE(1'u16)
  result.add toBytesLE(entryCount)
  result.add toBytesLE(256'u32)  # chunk_size, fixed by the writer
  result.add toBytesLE(uint32(chunkOffsets.len))
  for off in chunkOffsets: result.add toBytesLE(off)
  for g in chunkGeids: result.add toBytesLE(g)
