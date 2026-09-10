## GDH-G9 probe — one recording, produced identically by two builds.
##
## ``gdh2_no_reload_container_is_byte_identical`` asserts that a recording
## with the reload machinery COMPILED IN but never triggered produces a
## container byte-identical to one produced before the campaign.  The only
## way to make that a measurement rather than a claim is to compile the
## SAME probe against two trees and diff the bytes, which is what
## ``tests/run_gdh2_gates.sh`` does with this file.
##
## Three constraints shape it, and each is load-bearing:
##
## 1. **It must compile against the PRE-CAMPAIGN tree**, so it may use
##    only writer API that existed at the pinned baseline revision.
##    Nothing here mentions ``registerPathVersion``, ``registerSourceReload``
##    or any GDH symbol — a probe that referenced one could not be built
##    on the baseline side at all, and "it did not compile" is not a
##    property of the container.
## 2. **The recording identity must be PINNED.**  The writer mints a fresh
##    UUIDv7 ``recordingId`` per recording when the caller passes none, so
##    two recordings of one program differ for a reason that has nothing
##    to do with this campaign.  Excluding byte ranges from the comparison
##    instead is forbidden by the milestone entry, and rightly: an
##    exclusion list grows one entry at a time and each entry is
##    invisible.
## 3. **The container must be NON-TRIVIAL.**  Byte-identity over an empty
##    container is free.  This one carries three paths, an interned
##    function / type / varname set, 24 steps across the three files,
##    nested calls with arguments, and per-step variable values — so every
##    stream the writer maintains has content in it.
##
## Usage: ``gdh2_identity_probe <output.ct>``.  It prints the container's
## size and its recording id so the driver can assert the pin took effect
## on BOTH sides rather than assume it.

import std/[os, strutils]
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/call_stream
import codetracer_trace_writer/new_trace_reader
import codetracer_ctfs/container

const PinnedRecordingId = "01890000-0000-7000-8000-0000000091d9"

proc die(msg: string) {.noreturn.} =
  stderr.writeLine("GDH2-PROBE-FAIL: " & msg)
  quit(1)

proc main() =
  if paramCount() < 1:
    die("usage: gdh2_identity_probe <output.ct>")
  let outPath = paramStr(1)
  let build = outPath & ".build"

  let wr = initMultiStreamWriter(build, "gdh2_identity_probe",
    recordingId = PinnedRecordingId)
  if wr.isErr: die("initMultiStreamWriter: " & wr.error)
  var w = wr.get()

  var pathIds: seq[uint64] = @[]
  for name in ["res://gdh2/probe.gd", "res://gdh2/autoload.gd",
               "res://gdh2/util.gd"]:
    let p = w.registerPath(name)
    if p.isErr: die("registerPath " & name & ": " & p.error)
    pathIds.add(p.get())

  let fnMain = w.registerFunction("_ready")
  if fnMain.isErr: die("registerFunction: " & fnMain.error)
  let fnInner = w.registerFunction("probe")
  if fnInner.isErr: die("registerFunction: " & fnInner.error)
  let tyInt = w.registerType("Int")
  if tyInt.isErr: die("registerType: " & tyInt.error)
  let vnN = w.registerVarname("n")
  if vnN.isErr: die("registerVarname: " & vnN.error)
  let vnAcc = w.registerVarname("acc")
  if vnAcc.isErr: die("registerVarname: " & vnAcc.error)

  let callTop = w.registerCall(fnMain.get(),
    [CallArg(varnameId: vnN.get(), value: @[0x01'u8])])
  if callTop.isErr: die("registerCall: " & callTop.error)

  # 24 steps spread over the three files, with values on most of them, so
  # the step / value / call / interning streams all carry content.
  for i in 0 ..< 24:
    let pid = pathIds[i mod pathIds.len]
    let line = uint64(3 + (i mod 11))
    var vals: seq[VariableValue] = @[]
    if i mod 3 != 0:
      vals.add(VariableValue(varnameId: vnAcc.get(), typeId: tyInt.get(),
        data: @[byte(i and 0x7F)]))
    let s = w.registerStep(pid, line, vals)
    if s.isErr: die("registerStep " & $i & ": " & s.error)
    if i == 8:
      let c = w.registerCall(fnInner.get(),
        [CallArg(varnameId: vnN.get(), value: @[0x08'u8])])
      if c.isErr: die("registerCall inner: " & c.error)
    if i == 17:
      let r = w.registerReturn(@[0x11'u8])
      if r.isErr: die("registerReturn inner: " & r.error)

  let retTop = w.registerReturn(@[0x00'u8])
  if retTop.isErr: die("registerReturn: " & retTop.error)

  let c = w.close()
  if c.isErr: die("close: " & c.error)
  let bytes = w.toBytes()
  discard w.closeCtfs()
  if bytes.len == 0: die("the writer produced an empty container")
  writeFile(outPath, cast[string](bytes))

  # Read the recording id back OUT of the container rather than echoing
  # the constant: the pin has to be shown to have reached the bytes.  A
  # probe that printed its own input would agree with itself on a build
  # where the pin was ignored.
  let rr = openNewTraceFromBytes(bytes)
  if rr.isErr: die("the probe's own container does not open: " & rr.error)
  var r = rr.get()
  echo "bytes=" & $bytes.len
  echo "recording_id=" & r.meta.recordingId
  echo "meta_version=" & $r.meta.version
  echo "paths=" & $r.pathCount()
  let sc = r.stepCount()
  echo "steps=" & (if sc.isOk: $sc.get() else: "ERR")
  let cc = r.callCount()
  echo "calls=" & (if cc.isOk: $cc.get() else: "ERR")
  let vc = r.valueCount()
  echo "values=" & (if vc.isOk: $vc.get() else: "ERR")

  # Per-member extracts, so the driver can digest the two members where a
  # GDH-M2 change would have to show up — `meta.dat` (the schema version
  # and the flag words) and `steps.dat` (where tag 0x08 would live).  A
  # whole-container digest already covers them, but a per-member digest
  # says WHICH member moved when one does, and the milestone entry asks
  # for per-member digests by name.
  for member in ["meta.dat", "steps.dat"]:
    let m = readInternalFile(bytes, member, 4096'u32, 170'u32)
    if m.isErr: die("cannot extract member " & member & ": " & m.error)
    let mb = m.get()
    if mb.len == 0: die("member " & member & " is empty")
    writeFile(outPath & ".member-" & member, cast[string](mb))
    echo "member_" & member.replace(".", "_") & "_bytes=" & $mb.len

when isMainModule:
  main()
