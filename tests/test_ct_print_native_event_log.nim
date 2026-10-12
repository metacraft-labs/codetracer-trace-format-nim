## ct-print's native decoder reads a native recording's OS event log only
## under its current names, `eventlog.dat` / `eventlog.idx`
## (`MCR-Event-Log-Stream.md` §2; CTFS names hold at most 12 characters).
##
## * A recording that still carries the members under the words of the
##   retired `event_log.dat` / `event_log.idx` (names base40 cannot pack,
##   which recorders before the rename stored lossily) is refused naming the
##   member; it is not read and the bundle is not decoded without it.
## * A recording with only one of the two members is refused.
## * An entry whose kind byte is outside the stream's table (§2.3, 0-14) is
##   refused by value, not printed as an unknown kind.
##
## The bundles are built in-process with the container writer and a real
## `meta.dat`; the event-log bytes are the recorder's layout, written by
## hand because the recorder package is not a dependency. No mocks.

import std/[json, strutils]
import results
import stew/endians2
import codetracer_trace_types
import codetracer_ctfs
import codetracer_ctfs/container
import codetracer_trace_writer/meta_dat
import native_decoder

proc entryBytes(kind: uint8): seq[byte] =
  result.add toBytesLE(3'u64)    # geid
  result.add toBytesLE(200'u64)  # tick
  result.add toBytesLE(1'u32)    # tid
  result.add kind
  result.add toBytesLE(1'u32)    # fd
  result.add toBytesLE(3'u64)    # return value
  result.add toBytesLE(6'u16)
  for c in "stdout": result.add byte(c)
  result.add toBytesLE(3'u32)
  for c in "hi\n": result.add byte(c)

proc chunkBytes(kind: uint8): seq[byte] =
  let entry = entryBytes(kind)
  result.add toBytesLE(uint32(4 + entry.len))
  result.add toBytesLE(1'u32)
  result.add entry

proc indexBytes(): seq[byte] =
  for c in "EIDX": result.add byte(c)
  result.add toBytesLE(1'u16)    # version
  result.add toBytesLE(1'u32)    # entries
  result.add toBytesLE(256'u32)  # chunk size
  result.add toBytesLE(1'u32)    # chunks
  result.add toBytesLE(0'u64)    # chunk offset
  result.add toBytesLE(3'u64)    # chunk geid

proc bundle(kind = 0'u8, withDat = true, withIdx = true): seq[byte] =
  var c = createCtfs()
  let meta = writeMetaDatToBuffer(TraceMetadata(
    recordingId: "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
    workdir: "/w", program: "/w/p", args: @["/w/p"]))
  var m = c.addFile("meta.dat").get()
  doAssert c.writeToFile(m, meta).isOk
  # One thread stream: a single 26-byte event header (type 0).
  var t = c.addFile("t00000000001").get()
  var ev: seq[byte] = @[]
  ev.add toBytesLE(0'u16); ev.add toBytesLE(26'u32); ev.add toBytesLE(1'u32)
  ev.add toBytesLE(1'u64); ev.add toBytesLE(100'u64)
  doAssert c.writeToFile(t, ev).isOk
  if withDat:
    var d = c.addFile(EventLogDat).get()
    doAssert c.writeToFile(d, chunkBytes(kind)).isOk
  if withIdx:
    var i = c.addFile(EventLogIdx).get()
    doAssert c.writeToFile(i, indexBytes()).isOk
  result = c.toBytes()

proc plantWord(image: var seq[byte], name: string, word: uint64) =
  ## Store member `name`'s directory entry under `word` instead.
  let current = base40Encode(name)
  var planted = false
  for off in countup(16 + 16, 4096 - 8, 24):
    if fromBytesLE(uint64, image.toOpenArray(off, off + 7)) == current:
      let w = toBytesLE(word)
      for k in 0 ..< 8: image[off + k] = w[k]
      planted = true
  doAssert planted, "no directory entry for " & name

proc decode(image: seq[byte]): Result[string, string] =
  let doc = ?buildNativeFullDocument(image, NativeOpts(stripPaths: false))
  ok($doc)

block the_current_names_are_read:
  let r = decode(bundle())
  doAssert r.isOk, r.error
  doAssert "hi\\n" in r.get(), r.get()
  echo "PASS the_current_names_are_read"

block the_retired_names_are_refused_by_name:
  for (current, retired) in [(EventLogDat, "event_log.dat"),
                             (EventLogIdx, "event_log.idx")]:
    var image = bundle()
    plantWord(image, current, base40Encode(retired))
    let r = decode(image)
    doAssert r.isErr, "a bundle carrying " & retired & " decoded: " & r.get()
    doAssert ("`" & retired & "`") in r.error,
      "the refusal must name " & retired & ": " & r.error
  echo "PASS the_retired_names_are_refused_by_name"

block one_member_without_the_other_is_refused:
  let noIdx = decode(bundle(withIdx = false))
  doAssert noIdx.isErr and "eventlog.idx" in noIdx.error, "no idx: " & (if noIdx.isOk: noIdx.get() else: noIdx.error)
  let noDat = decode(bundle(withDat = false))
  doAssert noDat.isErr and "eventlog.dat" in noDat.error, "no dat: " & (if noDat.isOk: noDat.get() else: noDat.error)
  echo "PASS one_member_without_the_other_is_refused"

block an_unassigned_kind_is_refused_by_value:
  for kind in [15'u8, 24, 255]:
    let r = decode(bundle(kind))
    doAssert r.isErr, "event log kind " & $kind & " decoded: " & r.get()
    doAssert ("unassigned event log kind " & $kind) in r.error, r.error
  echo "PASS an_unassigned_kind_is_refused_by_value"
