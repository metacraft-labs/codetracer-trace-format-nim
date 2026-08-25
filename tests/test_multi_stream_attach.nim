{.push raises: [].}

## Tests for `initMultiStreamWriterAttached` (Stage C step 1).
##
## The attach constructor lets a MultiStreamTraceWriter write its streams into
## a CTFS container that ANOTHER writer created and OWNS, so two producers (e.g.
## MCR's native recorder and the GDScript materialized recorder) can share ONE
## `.ct`.  The two write DISTINCT stream names; only the container's creator
## owns `meta.dat` and the container's lifetime.
##
## What this file proves:
##   1. attach correctness — an attached writer's own streams
##      (steps.dat/values.dat/calls.dat + step-map.ns) land in the shared
##      container and decode correctly;
##   2. non-interference — the owner's pre-existing files (a stand-in meta.dat
##      and a `t00000000001`) are intact and byte-unchanged after the attached
##      writer's close(): close() did NOT overwrite meta.dat and did NOT close
##      the container out from under the owner (the owner's own closeCtfs still
##      succeeds and the file re-reads cleanly);
##   3. owned-path guard — writing identical content through the OWNED path and
##      the ATTACH path yields byte-for-byte identical stream files, i.e. the
##      attach path reuses the owned encoding unchanged, and the owned path's
##      produced bytes are unaffected by the new field/accessor.  (The
##      cross-read + golden-fixture corpus is the authoritative "vs before this
##      change" guard for the owned bytes; this test adds a local equality.)
##
## No mocks: every assertion runs against a real streaming CTFS container on
## the filesystem and the real stream decoders.

import std/os
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/exec_stream
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/call_stream
import codetracer_trace_writer/step_encoding
import codetracer_ctfs/container
import codetracer_ctfs/streaming

# A pinned recording id so both writers produce comparable output (the id only
# affects meta.dat, which we do not compare, but pinning removes all doubt).
const PinnedId = "0192f0c0-0000-7000-8000-000000000001"

proc toBytes(s: string): seq[byte] {.raises: [].} =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

proc tmpPath(name: string): string {.raises: [].} =
  try:
    getTempDir() / name
  except OSError, Exception:
    name

proc rm(path: string) {.raises: [].} =
  try:
    removeFile(path)
  except OSError, Exception:
    discard

# ---------------------------------------------------------------------------
# Common content producer.  Drives the SAME sequence of register calls against
# any writer (owned or attached) so their stream bytes are directly comparable.
# ---------------------------------------------------------------------------

proc populate(w: var MultiStreamTraceWriter) {.raises: [].} =
  let p0 = w.registerPath("/src/game.gd")
  doAssert p0.isOk and p0.get() == 0, "registerPath game.gd"
  let p1 = w.registerPath("/src/enemy.gd")
  doAssert p1.isOk and p1.get() == 1, "registerPath enemy.gd"

  let f0 = w.registerFunction("_ready")
  doAssert f0.isOk, "registerFunction _ready"
  let f1 = w.registerFunction("_process")
  doAssert f1.isOk, "registerFunction _process"

  # 12 steps alternating between the two files, sequential lines.
  for i in 0 ..< 12:
    let pathId = if i < 6: 0'u64 else: 1'u64
    let line = uint64((i mod 6) + 1)
    let vals = @[VariableValue(varnameId: 0, typeId: 0, data: ($i).toBytes)]
    let res = w.registerStep(pathId, line, vals)
    doAssert res.isOk, "registerStep " & $i & ": " & res.error

  # A small call tree: _ready() { _process(); }.
  let c0 = w.registerCall(0, @[])
  doAssert c0.isOk, "registerCall 0: " & c0.error
  let c1 = w.registerCall(1, @[CallArg(varnameId: 0, value: "1".toBytes)])
  doAssert c1.isOk, "registerCall 1: " & c1.error
  let r1 = w.registerReturn("7".toBytes)
  doAssert r1.isOk, "registerReturn 1: " & r1.error
  let r0 = w.registerReturn()
  doAssert r0.isOk, "registerReturn 0: " & r0.error

proc readWholeFile(path: string): seq[byte] {.raises: [].} =
  let res = readCtfsFromFile(path)
  doAssert res.isOk, "readCtfsFromFile " & path & ": " & res.error
  res.get()

# ---------------------------------------------------------------------------
# Test 1 — attach correctness + owner non-interference.
# ---------------------------------------------------------------------------

proc test_attach_shares_container() {.raises: [].} =
  let pathA = tmpPath("attach_owner.ct")
  rm(pathA)

  # --- Owner creates and owns container A, and writes its OWN files. ---
  let ownerRes = createCtfsStreaming(pathA)
  doAssert ownerRes.isOk, "createCtfsStreaming: " & ownerRes.error
  var ownerCtfs = ownerRes.get()

  # Stand-in "owner-owned" files: a fake meta.dat and a first native
  # per-thread stream `t00000000001`.  Distinctive bytes so we can prove they
  # survive the attached writer's close() unchanged.
  let fakeMeta = "OWNER-META-DAT-v1".toBytes
  let fakeThread = "native-thread-stream-t1".toBytes

  block:
    let mf = ownerCtfs.addFile("meta.dat")
    doAssert mf.isOk, "owner addFile meta.dat: " & mf.error
    var metaFile = mf.get()
    let w = ownerCtfs.writeToFile(metaFile, fakeMeta)
    doAssert w.isOk, "owner writeToFile meta.dat: " & w.error
  block:
    let tf = ownerCtfs.addFile("t00000000001")
    doAssert tf.isOk, "owner addFile t00000000001: " & tf.error
    var threadFile = tf.get()
    let w = ownerCtfs.writeToFile(threadFile, fakeThread)
    doAssert w.isOk, "owner writeToFile t00000000001: " & w.error

  # --- A second producer ATTACHES to the owner's container. ---
  let attRes = initMultiStreamWriterAttached(addr ownerCtfs, "materialized_prog",
    recordingId = PinnedId)
  doAssert attRes.isOk, "initMultiStreamWriterAttached: " & attRes.error
  var att = attRes.get()
  populate(att)
  doAssert att.stepCount == 12, "attached stepCount: " & $att.stepCount

  # Attached close(): finalizes ITS streams, but must NOT write meta.dat and
  # must NOT close the shared container.
  let ac = att.close()
  doAssert ac.isOk, "attached close: " & ac.error
  # closeCtfs on an attached writer is a no-op (owner owns the lifetime); if it
  # were not, the owner's finalize below would write to a closed file.
  att.closeCtfs()

  # --- Owner finalizes and closes the container it owns. ---
  ownerCtfs.closeCtfs()

  # --- Re-open A from disk and verify. ---
  let bytes = readWholeFile(pathA)

  # (b) owner files intact + byte-unchanged (meta.dat NOT overwritten).
  let metaBack = readInternalFile(bytes, "meta.dat")
  doAssert metaBack.isOk, "meta.dat missing after attach: " & metaBack.error
  doAssert metaBack.get() == fakeMeta,
    "meta.dat was overwritten by the attached writer"
  let threadBack = readInternalFile(bytes, "t00000000001")
  doAssert threadBack.isOk, "t00000000001 missing: " & threadBack.error
  doAssert threadBack.get() == fakeThread, "t00000000001 corrupted"

  # (a) attached writer's streams present + decode correctly.
  let execRes = initExecStreamReader(bytes)
  doAssert execRes.isOk, "initExecStreamReader: " & execRes.error
  var execR = execRes.get()
  let ev0 = execR.readEvent(0)
  doAssert ev0.isOk, "readEvent 0: " & ev0.error
  doAssert ev0.get().kind == sekAbsoluteStep, "step 0 should be absolute"
  let ev1 = execR.readEvent(1)
  doAssert ev1.isOk, "readEvent 1: " & ev1.error
  doAssert ev1.get().kind == sekDeltaStep and ev1.get().lineDelta == 1,
    "step 1 should be delta +1"
  let ev11 = execR.readEvent(11)
  doAssert ev11.isOk, "readEvent 11: " & ev11.error

  let valRes = initValueStreamReader(bytes)
  doAssert valRes.isOk, "initValueStreamReader: " & valRes.error
  var valR = valRes.get()
  let v0 = valR.readStepValues(0)
  doAssert v0.isOk, "readStepValues 0: " & v0.error
  doAssert v0.get().len == 1 and v0.get()[0].data == "0".toBytes,
    "value 0 mismatch"
  let v5 = valR.readStepValues(5)
  doAssert v5.isOk and v5.get()[0].data == "5".toBytes, "value 5 mismatch"

  let callRes = initCallStreamReader(bytes)
  doAssert callRes.isOk, "initCallStreamReader: " & callRes.error
  var callR = callRes.get()
  let call0 = callR.readCall(0)
  doAssert call0.isOk, "readCall 0: " & call0.error
  doAssert call0.get().functionId == 0, "call0 functionId"
  doAssert call0.get().returnValue == @[VoidReturnMarker],
    "call0 returnValue should be void"
  let call1 = callR.readCall(1)
  doAssert call1.isOk, "readCall 1: " & call1.error
  doAssert call1.get().functionId == 1, "call1 functionId"
  doAssert call1.get().returnValue == "7".toBytes, "call1 returnValue"

  # (c) the attached close() did NOT close the container out from under the
  # owner: the owner's closeCtfs above wrote the final image, so the file is
  # complete and every stream decodes — which the reads above already proved.
  # step-map.ns (an additive file the attached writer emits) is present too.
  let smBack = readInternalFile(bytes, "step-map.ns")
  doAssert smBack.isOk and smBack.get().len > 0,
    "attached writer did not emit step-map.ns"

  rm(pathA)
  echo "PASS: test_attach_shares_container"

# ---------------------------------------------------------------------------
# Test 2 — owned path guard: owned vs attach produce identical stream bytes.
# ---------------------------------------------------------------------------

proc test_owned_and_attach_stream_bytes_identical() {.raises: [].} =
  # Owned writer to a fresh path.
  let ownedPath = tmpPath("owned_guard.ct")
  rm(ownedPath)
  let ownedRes = initMultiStreamWriter(ownedPath, "materialized_prog",
    recordingId = PinnedId)
  doAssert ownedRes.isOk, "initMultiStreamWriter: " & ownedRes.error
  var owned = ownedRes.get()
  populate(owned)
  doAssert owned.close().isOk, "owned close"
  owned.closeCtfs()
  let ownedBytes = readWholeFile(ownedPath)

  # Attach writer into a bare owner container (no owner-named files, so the
  # only difference from the owned container is the absence of meta.dat).
  let attPath = tmpPath("attach_guard.ct")
  rm(attPath)
  let ownerRes = createCtfsStreaming(attPath)
  doAssert ownerRes.isOk, "createCtfsStreaming: " & ownerRes.error
  var ownerCtfs = ownerRes.get()
  let attRes = initMultiStreamWriterAttached(addr ownerCtfs, "materialized_prog",
    recordingId = PinnedId)
  doAssert attRes.isOk, "initMultiStreamWriterAttached: " & attRes.error
  var att = attRes.get()
  populate(att)
  doAssert att.close().isOk, "attached close"
  att.closeCtfs()
  ownerCtfs.closeCtfs()
  let attBytes = readWholeFile(attPath)

  # Every stream file the two writers share must be byte-for-byte identical.
  # (meta.dat is intentionally excluded — the owner owns it; the attach path
  # does not write one.)
  const sharedFiles = [
    "steps.dat", "steps.idx",
    "values.dat", "values.idx",
    "calls.dat", "calls.idx",
    "paths.dat", "paths.off",
    "funcs.dat", "funcs.off",
    "types.dat", "types.off",
    "varnames.dat", "varnames.off",
    "step-map.ns",
  ]
  for name in sharedFiles:
    let a = readInternalFile(ownedBytes, name)
    let b = readInternalFile(attBytes, name)
    doAssert a.isOk, "owned missing " & name & ": " & a.error
    doAssert b.isOk, "attach missing " & name & ": " & b.error
    doAssert a.get() == b.get(),
      "stream file '" & name & "' differs between owned and attach paths " &
      "(owned " & $a.get().len & "B vs attach " & $b.get().len & "B)"

  # And the owned container must carry a meta.dat that the attach one lacks —
  # proving the ownership split is real, not that both simply skipped it.
  doAssert readInternalFile(ownedBytes, "meta.dat").isOk,
    "owned path unexpectedly has no meta.dat"
  doAssert readInternalFile(attBytes, "meta.dat").isErr,
    "attach path unexpectedly wrote meta.dat"

  rm(ownedPath)
  rm(attPath)
  echo "PASS: test_owned_and_attach_stream_bytes_identical"

# ---------------------------------------------------------------------------
# Test 3 — nil pointer rejected.
# ---------------------------------------------------------------------------

proc test_attach_rejects_nil() {.raises: [].} =
  let res = initMultiStreamWriterAttached(nil, "prog")
  doAssert res.isErr, "attach to nil ctfs must fail"
  echo "PASS: test_attach_rejects_nil"

when isMainModule:
  test_attach_shares_container()
  test_owned_and_attach_stream_bytes_identical()
  test_attach_rejects_nil()
  echo "ALL PASS: test_multi_stream_attach"
