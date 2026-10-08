## The C ABI's line-hit opt-in and its read side for `linehits.tc`,
## `corrmark.ns` and `markers.dat`.
##
## Asserted, and why each can fail:
##
##   1. `trace_writer_enable_linehits` makes the writer record every step
##      after it: the positions and exec-record indices `ct_linehits_json`
##      reports are the ones the recording made, including a revisited line
##      that collects two step ids in order. A writer that ignored the opt-in
##      writes no `linehits.tc` and the read returns 1.
##   2. A container recorded without the opt-in or without markers has no
##      such member, and every read says so with 1 — "not indexed" — rather
##      than an empty document.
##   3. The correlation index reports both kinds; a span lookup confirms the
##      full 24-byte key and a boundary lookup the marker id and key value,
##      and a near miss on either returns `[]`.
##   4. Marker labels come back in id order.
##   5. A container whose `linehits.tc` or `corrmark.ns` is damaged makes the
##      read fail with -1 and a reason, never an empty answer.
##
## No mocks: the containers are written by the C ABI and read back through it.

include codetracer_trace_writer_ffi

{.pop.}

import std/strutils

const
  AppPath = "/srv/app.py"

proc document(rc: cint, buf: ptr uint8, n: csize_t): string =
  doAssert rc == 0, "read failed with " & $rc & ": " & $trace_writer_last_error()
  result = newString(int(n))
  if n > 0:
    copyMem(addr result[0], buf, int(n))
  ct_free_buffer(buf)

proc record(dir, name: string, linehits, markers: bool): string =
  createDir(dir)
  let ctPath = dir / (name & ".ct")
  if fileExists(ctPath): removeFile(ctPath)
  let h = trace_writer_new(cstring(name), ffiBinary)
  doAssert h != nil
  doAssert trace_writer_begin_events(h, cstring(dir / "events.bin")) == 0
  if linehits:
    doAssert trace_writer_enable_linehits(h) == 0, $trace_writer_last_error()
  let p = cstring(AppPath)
  trace_writer_start(h, p, 1)
  trace_writer_register_step(h, p, 2)
  trace_writer_register_step(h, p, 3)
  trace_writer_register_step(h, p, 2)
  if markers:
    var id: uint64
    let label = "api-call"
    doAssert trace_writer_ensure_marker_id(h,
      cast[ptr UncheckedArray[byte]](unsafeAddr label[0]),
      csize_t(label.len), addr id) == 0
    doAssert id == 0
    let dir = "send"
    let key = "order-42"
    doAssert trace_writer_mark_correlation_by_id(h, id,
      cast[ptr UncheckedArray[byte]](unsafeAddr label[0]), csize_t(label.len),
      cast[ptr UncheckedArray[byte]](unsafeAddr dir[0]), csize_t(dir.len),
      cast[ptr UncheckedArray[byte]](unsafeAddr key[0]), csize_t(key.len),
      nil, 0, nil, 0, nil, 0, nil, 0) == 0
    var traceId: array[16, byte]
    var spanId: array[8, byte]
    for i in 0 ..< 16: traceId[i] = byte(i + 1)
    for i in 0 ..< 8: spanId[i] = byte(0xA0 + i)
    doAssert trace_writer_mark_span_coverage(h,
      cast[ptr UncheckedArray[byte]](addr traceId[0]), 16,
      cast[ptr UncheckedArray[byte]](addr spanId[0]), 8, 111, 222) == 0
  trace_writer_register_step(h, p, 4)
  doAssert trace_writer_close(h) == 0, $trace_writer_last_error()
  trace_writer_free(h)
  ctPath

proc test_line_hits_are_read_back() =
  let ct = record(getTempDir() / "ct_ffi_linehits", "lh", true, false)
  var buf: ptr uint8
  var n: csize_t
  let doc = document(ct_linehits_json(cstring(ct), addr buf, addr n), buf, n)
  # Lines 1..4 of a file sized at the conventional 100000 lines are
  # positions 0..3; steps 0..4 are start, 2, 3, 2, 4.
  doAssert doc == """[{"position":0,"steps":[0]},{"position":1,"steps":[1,3]},""" &
    """{"position":2,"steps":[2]},{"position":3,"steps":[4]}]""", doc
  echo "PASS: test_line_hits_are_read_back"

proc test_absent_members_answer_one() =
  let ct = record(getTempDir() / "ct_ffi_linehits_absent", "none", false, false)
  var buf: ptr uint8
  var n: csize_t
  doAssert ct_linehits_json(cstring(ct), addr buf, addr n) == 1
  doAssert buf.isNil and n == 0
  doAssert ct_correlation_index_json(cstring(ct), addr buf, addr n) == 1
  doAssert ct_marker_labels_json(cstring(ct), addr buf, addr n) == 1
  var t: array[16, byte]
  var s: array[8, byte]
  doAssert ct_correlation_lookup_span(cstring(ct),
    cast[ptr UncheckedArray[byte]](addr t[0]), 16,
    cast[ptr UncheckedArray[byte]](addr s[0]), 8, addr buf, addr n) == 1
  doAssert ct_correlation_lookup_boundary(cstring(ct), 0, nil, 0,
    addr buf, addr n) == 1
  echo "PASS: test_absent_members_answer_one"

proc test_correlation_index_is_read_back() =
  let ct = record(getTempDir() / "ct_ffi_corrmark", "cm", false, true)
  var buf: ptr uint8
  var n: csize_t
  let all = document(ct_correlation_index_json(cstring(ct), addr buf, addr n),
    buf, n)
  doAssert all.count("\"kind\":0") == 1 and all.count("\"kind\":1") == 1, all
  # Both markers were declared while the step for line 2 (exec record 3) was
  # pending, so both carry geid 3.
  doAssert all.count("\"geid\":3") == 2, all
  let labels = document(ct_marker_labels_json(cstring(ct), addr buf, addr n),
    buf, n)
  doAssert labels == "[\"" & "api-call".toHex.toLowerAscii & "\"]", labels

  var t: array[16, byte]
  var s: array[8, byte]
  for i in 0 ..< 16: t[i] = byte(i + 1)
  for i in 0 ..< 8: s[i] = byte(0xA0 + i)
  let hit = document(ct_correlation_lookup_span(cstring(ct),
    cast[ptr UncheckedArray[byte]](addr t[0]), 16,
    cast[ptr UncheckedArray[byte]](addr s[0]), 8, addr buf, addr n), buf, n)
  doAssert hit.count("\"kind\":0") == 1 and "\"wall_time_unix_ns\":111" in hit,
    hit
  s[7] = 0
  let miss = document(ct_correlation_lookup_span(cstring(ct),
    cast[ptr UncheckedArray[byte]](addr t[0]), 16,
    cast[ptr UncheckedArray[byte]](addr s[0]), 8, addr buf, addr n), buf, n)
  doAssert miss == "[]", miss
  let key = "order-42"
  let b = document(ct_correlation_lookup_boundary(cstring(ct), 0,
    cast[ptr UncheckedArray[byte]](unsafeAddr key[0]), csize_t(key.len),
    addr buf, addr n), buf, n)
  doAssert b.count("\"kind\":1") == 1, b
  let other = "order-43"
  let bm = document(ct_correlation_lookup_boundary(cstring(ct), 0,
    cast[ptr UncheckedArray[byte]](unsafeAddr other[0]), csize_t(other.len),
    addr buf, addr n), buf, n)
  doAssert bm == "[]", bm
  let bm2 = document(ct_correlation_lookup_boundary(cstring(ct), 1,
    cast[ptr UncheckedArray[byte]](unsafeAddr key[0]), csize_t(key.len),
    addr buf, addr n), buf, n)
  doAssert bm2 == "[]", bm2
  echo "PASS: test_correlation_index_is_read_back"

proc damageFirstNamespace(ct: string) =
  ## Overwrite the first `NSB1` magic in the file — the namespace the
  ## container holds — so the member is still present but malformed.
  var data = readFile(ct)
  let at = data.find("NSB1")
  doAssert at >= 0
  data[at + 3] = 'X'
  writeFile(ct, data)

proc test_damaged_members_fail() =
  let lh = record(getTempDir() / "ct_ffi_linehits_bad", "lhbad", true, false)
  damageFirstNamespace(lh)
  var buf: ptr uint8
  var n: csize_t
  doAssert ct_linehits_json(cstring(lh), addr buf, addr n) == -1
  doAssert "magic" in $trace_writer_last_error(), $trace_writer_last_error()
  let cm = record(getTempDir() / "ct_ffi_corrmark_bad", "cmbad", false, true)
  damageFirstNamespace(cm)
  doAssert ct_correlation_index_json(cstring(cm), addr buf, addr n) == -1
  doAssert "magic" in $trace_writer_last_error(), $trace_writer_last_error()
  echo "PASS: test_damaged_members_fail"

test_line_hits_are_read_back()
test_absent_members_answer_one()
test_correlation_index_is_read_back()
test_damaged_members_fail()
echo "=== FFI line hits and correlation index tests passed ==="
