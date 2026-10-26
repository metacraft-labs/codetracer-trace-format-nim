## The 2026-10 format revision through the C ABI, end to end:
##
## * every value-stream tag 0-9 is written by its entry point and decoded by
##   the reader (`trace-events.md` §"Value Stream": "a writer MUST be able to
##   write every tag its API exposes, and a reader MUST decode tags 0-9"); the
##   place-model record bytes are pinned against the Rust encoding;
## * an event's kind is its exact `EventLogKind`, 0-13, round-tripped through
##   the writer, the reader and `ct_reader_event_fields`; 14 is refused
##   (§"EventLogKind (u8 enum)");
## * `meta.dat` is written once, at the first record, and is complete then: a
##   later `set_workdir` / `set_args` / capability is refused and fails the
##   close; a source reload needs the up-front declaration
##   (`internal-files.md` §"Extended flags");
## * `meta.dat` is version 6, its `flags_ext` word is always present, and it
##   carries no path list (§"Metadata (meta.dat)").
##
## No mocks: the real C ABI writer and reader over real files.

include codetracer_trace_writer_ffi

{.pop.}

import std/[os, strutils]

proc cbor(i: int64): seq[byte] =
  let e = ct_value_encoder_new()
  doAssert ct_value_write_int(e, i, 7) == 0
  var n: csize_t
  let p = ct_value_get_bytes(e, addr n)
  result = newSeq[byte](int(n))
  copyMem(addr result[0], p, int(n))
  ct_value_encoder_free(e)

proc tmpDir(tag: string): string =
  result = getTempDir() / ("ct_fmt2026_" & tag & "_" & $getCurrentProcessId())
  removeDir(result)
  createDir(result)

proc ctPath(dir, prog: string): string = dir / (prog & ".ct")

block every_value_tag_is_written_and_read:
  let dir = tmpDir("tags")
  let w = trace_writer_new("tags", ffiBinary)
  doAssert trace_writer_begin_events(w, cstring(dir / "t.json")) == 0
  trace_writer_register_step(w, "/src/a.nr", 1)
  let c41 = cbor(41)
  let c42 = cbor(42)
  let c43 = cbor(43)
  trace_writer_register_variable_cbor(w, "x", unsafeAddr c41[0], csize_t(c41.len))
  doAssert trace_writer_bind_variable(w, "x", -3) == 0                    # 1
  doAssert trace_writer_register_drop_variable(w, "y") == 0              # 2
  doAssert trace_writer_register_cell_value(w, 7, unsafeAddr c41[0],
    csize_t(c41.len)) == 0                                               # 4
  doAssert trace_writer_register_compound_value(w, -8, unsafeAddr c42[0],
    csize_t(c42.len)) == 0                                               # 5
  doAssert trace_writer_assign_cell(w, 9, unsafeAddr c43[0],
    csize_t(c43.len)) == 0                                               # 6
  doAssert trace_writer_assign_compound_item(w, 10, 2, -11) == 0         # 7
  doAssert trace_writer_register_variable_cell(w, "x", 12) == 0          # 8
  ct_bind_variable(w, "z", 13)                                           # 1
  trace_writer_register_step(w, "/src/a.nr", 2)
  doAssert trace_writer_close(w) == 0, $trace_writer_last_error()
  trace_writer_free(w)

  var r = openNewTrace(ctPath(dir, "tags")).get()
  let evs = r.valueEvents(0).get()
  var tags: seq[ValueEventKind]
  for e in evs: tags.add(e.kind)
  doAssert tags == @[veStepValues, veBindVariable, veDropVariable,
    veCellValue, veCompoundValue, veAssignCell, veAssignCompoundItem,
    veVariableCell, veBindVariable], $tags
  let x = r.varname(evs[1].variableId).get()
  doAssert x == "x" and evs[1].variablePlace == -3
  doAssert evs[3].place == 7 and evs[3].valueCbor == c41
  doAssert evs[4].place == -8 and evs[4].valueCbor == c42
  doAssert evs[5].place == 9 and evs[5].valueCbor == c43
  doAssert evs[6].compoundPlace == 10 and evs[6].itemIndex == 2 and
    evs[6].itemPlace == -11
  doAssert r.varname(evs[7].variableId).get() == "x" and evs[7].variablePlace == 12
  doAssert r.varname(evs[8].variableId).get() == "z" and evs[8].variablePlace == 13
  # The place model's bytes, as the Rust `ValueStreamEvent::encode` writes
  # them: places zigzag-signed, values length-prefixed.
  var expect: seq[byte]
  encodeBindVariableEvent(evs[1].variableId, -3, expect)
  doAssert expect == @[1'u8, byte(evs[1].variableId), 5]
  expect.setLen(0)
  encodeAssignCompoundItemEvent(10, 2, -11, expect)
  doAssert expect == @[7'u8, 20, 2, 21]
  expect.setLen(0)
  encodeCellValueEvent(7, c41, expect)
  doAssert expect[0 .. 2] == @[4'u8, 14, byte(c41.len)]
  removeDir(dir)
  echo "PASS every_value_tag_is_written_and_read"

block every_event_kind_round_trips_exactly:
  let dir = tmpDir("kinds")
  let w = trace_writer_new("kinds", ffiBinary)
  doAssert trace_writer_begin_events(w, cstring(dir / "t.json")) == 0
  trace_writer_register_step(w, "/src/a.nr", 1)
  for k in 0 .. 13:
    trace_writer_register_special_event(w, FfiEventLogKind(k),
      cstring("m" & $k), cstring("c" & $k))
  doAssert trace_writer_close(w) == 0, $trace_writer_last_error()
  trace_writer_free(w)
  var r = openNewTrace(ctPath(dir, "kinds")).get()
  doAssert r.ioEventCount().get() == 14
  for k in 0 .. 13:
    let ev = r.ioEvent(uint64(k)).get()
    doAssert ord(ev.kind) == k, "event " & $k & " read back as " & $ev.kind
  let rh = ct_reader_open(cstring(ctPath(dir, "kinds")))
  doAssert rh != nil
  for k in 0 .. 13:
    var kind: uint8
    var step: uint64
    var data: ptr uint8
    var n: csize_t
    doAssert ct_reader_event_fields(rh, uint64(k), addr kind, addr step,
      addr data, addr n) == 0
    doAssert int(kind) == k, "C ABI reported kind " & $kind & " for " & $k
    ct_free_buffer(data)
  ct_reader_close(rh)

  # An unassigned kind is refused, by value, and the recording fails.
  let w2 = trace_writer_new("kinds2", ffiBinary)
  doAssert trace_writer_begin_events(w2, cstring(dir / "t.json")) == 0
  trace_writer_register_step(w2, "/src/a.nr", 1)
  trace_writer_register_special_event(w2, cast[FfiEventLogKind](14'i32), "m", "c")
  doAssert "14" in $trace_writer_last_error(), $trace_writer_last_error()
  doAssert trace_writer_close(w2) != 0, "a refused event must fail the close"
  trace_writer_free(w2)

  # And a reader refuses an on-disk 14.
  var rec = encodeIOEvent(IOEvent(kind: elkEvmEvent, stepId: 0))
  rec[0] = 14
  let bad = decodeIOEvent(rec)
  doAssert bad.isErr and "14" in bad.error, $bad
  removeDir(dir)
  echo "PASS every_event_kind_round_trips_exactly"

block records_must_fill_their_frame:
  var call = encodeCallRecord(call_stream.CallRecord(functionId: 1,
    parentCallKey: -1, returnValue: @[0xFF'u8]))
  doAssert decodeCallRecord(call).isOk
  call.add(0)
  let c = decodeCallRecord(call)
  doAssert c.isErr and "frame" in c.error, $c
  var io = encodeIOEvent(IOEvent(kind: elkWrite, stepId: 3, data: @[1'u8]))
  io.add(0)
  let e = decodeIOEvent(io)
  doAssert e.isErr and "frame" in e.error, $e
  # A value record whose last event runs past the frame.
  var v: seq[byte]
  encodeCellValueEvent(1, @[1'u8, 2, 3], v)
  let vr = decodeRecordEvents(v.toOpenArray(0, v.high - 1))
  doAssert vr.isErr, "a value event overrunning its record decoded"
  echo "PASS records_must_fill_their_frame"

block meta_dat_is_written_at_the_first_record_and_is_final:
  let dir = tmpDir("meta")
  let w = trace_writer_new("meta", ffiBinary)
  doAssert trace_writer_begin_events(w, cstring(dir / "t.json")) == 0
  trace_writer_set_workdir(w, "/work/before")
  doAssert trace_writer_declare_source_reload(w) == 0
  trace_writer_register_step(w, "/src/a.nr", 1)
  trace_writer_register_step(w, "/src/a.nr", 2)   # flushes record 0
  # meta.dat is on disk now, before close, with everything set so far.
  let img = readCtfsFromFile(ctPath(dir, "meta")).get()
  let md = readMetaDat(readInternalFile(img, "meta.dat").get()).get()
  doAssert md.version == 6 and md.workdir == "/work/before" and md.hasSourceReload
  doAssert md.hasStepStream and md.hasCallStream and md.hasValueStream and
    md.hasIoEventStream and md.hasInterningTables
  doAssert not md.hasSpanStream and not md.hasCorrelationIndex and
    not md.hasAlternateSourceViews
  doAssert trace_writer_declare_source_reload(w) != 0,
    "a capability declared after the first record must be refused"
  trace_writer_clear_last_error()
  trace_writer_set_workdir(w, "/work/after")
  doAssert "first record" in $trace_writer_last_error(), $trace_writer_last_error()
  doAssert trace_writer_close(w) != 0, "a refused metadata change fails the close"
  trace_writer_free(w)
  let md2 = readMetaDat(readInternalFile(readCtfsFromFile(
    ctPath(dir, "meta")).get(), "meta.dat").get()).get()
  doAssert md2.workdir == "/work/before", "meta.dat was rewritten"

  # A reload in a trace that did not declare one is refused.
  let w3 = trace_writer_new("noreload", ffiBinary)
  doAssert trace_writer_begin_events(w3, cstring(dir / "t.json")) == 0
  doAssert trace_writer_register_path(w3, "/src/a.nr") != high(uint64)
  doAssert trace_writer_register_path(w3, "/src/b.nr") != high(uint64)
  trace_writer_register_step(w3, "/src/a.nr", 1)
  var ch = CtTwSourceReloadChange(old_path_id: 0, new_path_id: 1, generation: 2)
  let ord = trace_writer_register_source_reload(w3,
    cast[ptr UncheckedArray[CtTwSourceReloadChange]](addr ch), 1, 0)
  doAssert ord == 0 and "declare" in $trace_writer_last_error(),
    $trace_writer_last_error()
  discard trace_writer_close(w3)
  trace_writer_free(w3)
  removeDir(dir)
  echo "PASS meta_dat_is_written_at_the_first_record_and_is_final"

block meta_dat_v6_header_and_no_path_list:
  let buf = writeMetaDatToBuffer(TraceMetadata(
    recordingId: "0192f8a0-1234-7abc-8def-0123456789ab", program: "p",
    workdir: "w"), recorderId = "r")
  doAssert buf[4] == 6 and buf[5] == 0
  doAssert buf[8 .. 11] == @[0'u8, 0, 0, 0], "flags_ext is always present"
  # recording id (36) + program + args count + workdir + recorder id; nothing after.
  doAssert buf.len == 12 + 1 + 36 + 2 + 1 + 2 + 2
  var v5 = buf
  v5[4] = 5
  let r5 = readMetaDat(v5)
  doAssert r5.isErr and "version 5" in r5.error and "version 6" in r5.error, $r5
  var unknownExt = buf
  unknownExt[9] = 1
  let rx = readMetaDat(unknownExt)
  doAssert rx.isErr and "flags_ext" in rx.error, $rx
  echo "PASS meta_dat_v6_header_and_no_path_list"

echo "ALL PASS test_ffi_fmt_2026_10"
