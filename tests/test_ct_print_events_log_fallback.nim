## M23e-1 — ct-print `--json-events` split-stream preference + events.log
## fallback safety.
##
## Background (the actual on-disk reality, verified by this test):
##   * ct-print's v4 path (`NewTraceReader` → `printJsonEventsV4`) sources
##     every event from the SPLIT per-kind streams (`steps.dat` / `calls.dat`
##     / `values.dat` / `events.dat` + the four interning tables) and never
##     consults the legacy combined `events.log`.
##   * A bundle that carries ONLY `events.log` (no split streams) used to be
##     opened by the v4 reader anyway (it only needs `meta.dat` + interning
##     tables) — the lazy stream readers then found nothing and ct-print
##     silently emitted an (almost) empty event array.  M23e-1 routes such a
##     bundle to the legacy `events.log` reader so its events surface.
##   * A bundle that carries `events.log` (with OR without split streams) is
##     read via the LEGACY `events.log` reader.  M23e-4 boundary: the
##     production Nim split writer is `events.log`-FREE, so the v4 path is
##     reserved for it; the SECONDARY Rust `CtfsTraceWriter` emits
##     `events.log` + split streams additively, but its step/value/io-event
##     split wire formats are NOT v4-readable — so any `events.log`-bearing
##     bundle is read via `events.log`.
##
## What this test pins (the achievable, faithful guarantees):
##   1. SPLIT bundle → lowercase `type`-tagged array (`path`/`function`/
##      `step`/`call`/…) — the canonical v4 `--json-events` shape the
##      reprobuild engine consumes.
##   2. events.log ROUTING (M23e-4): a bundle carrying BOTH a grafted
##      `events.log` and the split streams is read via the LEGACY
##      `events.log` reader (NOT the split path), because an `events.log`-
##      bearing bundle is treated as a secondary-writer combined bundle whose
##      splits may not be v4-readable.  Its `--json-events` therefore matches
##      the events.log-only bundle's legacy output, not the split output.
##   3. events.log FALLBACK safety: an `events.log`-only bundle produces
##      a NON-EMPTY event array (it is routed to the legacy reader) rather
##      than the near-empty array the v4 path produced before M23e-1.
##
## NOTE on byte-identity between the two READERS: the legacy `events.log`
## reader emits a DIFFERENT format generation (capitalized `Step`/`Call`/…,
## flat, no step indices / entry-exit steps / interning-table declarations,
## values keyed by `variable_id` not `varname_id`) than the split reader's
## lowercase shape.  They are NOT byte-identical and cannot be made so by a
## reader-only change: the flat `events.log` model lacks the `varname_id` /
## `type_id` interning and call→step linkage the split `--json-events`
## surfaces.  Byte-identity is therefore asserted where it IS achievable and
## meaningful — between two reads that BOTH go through the split path
## (guarantee #2).

import std/[os, osproc, strutils, json, times]
import results
import codetracer_trace_writer        # legacy single-stream TraceWriter
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/value_stream
import codetracer_trace_types
import codetracer_ctfs/container

const
  repoRoot = currentSourcePath().parentDir.parentDir
  ctPrintSrc = repoRoot / "src" / "codetracer_ct_print.nim"
  ctPrintBin = "/tmp/ctprint_build/ct-print"

# ---------------------------------------------------------------------------
# Bundle builders — the SAME logical recording in each on-disk layout.
# ---------------------------------------------------------------------------

proc buildEventsLogBundle(path: string) =
  ## Legacy combined-stream bundle: ONLY `events.log` (+ meta.dat / meta.json
  ## / paths.json), no split streams.
  removeFile(path)
  var w = newTraceWriter(path, "demo", @["--x"], "/wd").get()
  doAssert w.writePath("/wd/main.py").isOk
  doAssert w.writeFunction(0, 1, "main").isOk
  doAssert w.writeStep(0, 1).isOk
  doAssert w.writeCall(0).isOk
  doAssert w.writeStep(0, 2).isOk
  doAssert w.writeValue(0,
    ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(0))).isOk
  doAssert w.writeReturn().isOk
  doAssert w.writeMetaDat().isOk
  doAssert w.close().isOk

proc buildSplitBundle(path: string): seq[byte] =
  ## Split multi-stream bundle modeling the same logical recording. Returns
  ## the serialized bytes (also written to `path`).
  removeFile(path)
  var w = initMultiStreamWriter(path, "demo").get()
  w.metadata.args = @["--x"]
  w.metadata.workdir = "/wd"
  let p0 = w.registerPath("/wd/main.py").get()
  let fnMain = w.registerFunction("main").get()
  let vnX = w.registerVarname("x").get()
  let tInt = w.registerType("int").get()
  doAssert w.registerStep(p0, 1'u64, @[]).isOk
  doAssert w.registerCall(fnMain, @[]).isOk
  # CBOR uint 42 == 0x18 0x2a.
  let vals = @[VariableValue(varnameId: vnX, typeId: tInt,
    data: @[byte(0x18), byte(0x2a)])]
  doAssert w.registerStep(p0, 2'u64, vals).isOk
  doAssert w.registerReturn().isOk
  doAssert w.close().isOk
  let bytes = w.toBytes()
  writeFile(path, cast[string](bytes))
  bytes

proc base40(name: string): uint64 =
  ## Encode a CTFS internal-file name into its base40 root-entry key (see
  ## ``codetracer_ctfs/base40.nim``).  Inlined here so the test does not
  ## depend on that module's private export surface.
  var val: uint64 = 0
  var mult: uint64 = 1
  for i in 0 ..< 12:
    var ci: uint64 = 0
    if i < name.len:
      let c = name[i]
      if c >= '0' and c <= '9': ci = uint64(ord(c) - ord('0') + 1)
      elif c >= 'a' and c <= 'z': ci = uint64(ord(c) - ord('a') + 11)
      elif c == '.': ci = 37
      elif c == '/': ci = 38
      elif c == '-': ci = 39
    val += ci * mult
    mult *= 40
  val

proc putU64LE(buf: var seq[byte], off: int, v: uint64) =
  for i in 0 ..< 8:
    buf[off + i] = byte((v shr (8 * i)) and 0xFF)

proc readU64LEloc(buf: seq[byte], off: int): uint64 =
  for i in 0 ..< 8:
    result = result or (uint64(buf[off + i]) shl (8 * i))

proc graftEntryInto(srcPath, outPath, entryName: string) =
  ## Produce a COMBINED bundle that carries BOTH layouts by ADDING a
  ## root-directory entry named ``entryName`` to ``srcPath``.
  ##
  ## We reuse the existing ``meta.dat`` block/size for the new entry's
  ## payload pointer: the routing decision keys only on the PRESENCE of the
  ## entry name (``hasInternalFile``), never on its byte content, so a filler
  ## pointer is sufficient and keeps the graft a pure root-directory edit
  ## with no block reallocation.  We graft a SPLIT stream (``steps.dat``)
  ## into the events.log bundle so the combined bundle has BOTH an
  ## (authentic, decodable) ``events.log`` AND a ``steps.dat`` marker —
  ## exercising the M23e-4 "events.log present ⇒ read via legacy even when a
  ## split stream is also present" routing branch with a bundle whose
  ## ``events.log`` is genuinely readable.
  var d = readCtfsFromFile(srcPath).get()
  const headerSize = 8
  const extHeaderSize = 8
  const feSize = 24
  const maxEntries = 31
  # Locate meta.dat's (size, mapBlock) to reuse as harmless filler payload.
  let metaKey = base40("meta.dat")
  var metaSize: uint64 = 0
  var metaMap: uint64 = 0
  for i in 0 ..< maxEntries:
    let off = headerSize + extHeaderSize + i * feSize
    if readU64LEloc(d, off + 16) == metaKey:
      metaSize = readU64LEloc(d, off)
      metaMap = readU64LEloc(d, off + 8)
      break
  doAssert metaMap != 0, "source bundle has no meta.dat to reuse for graft"
  # Find the first empty root slot and write the new entry into it.
  let newKey = base40(entryName)
  var grafted = false
  for i in 0 ..< maxEntries:
    let off = headerSize + extHeaderSize + i * feSize
    if off + feSize > d.len: break
    if readU64LEloc(d, off) == 0'u64 and readU64LEloc(d, off + 8) == 0'u64 and
       readU64LEloc(d, off + 16) == 0'u64:
      putU64LE(d, off, metaSize)        # size  (filler)
      putU64LE(d, off + 8, metaMap)     # mapBlock (filler)
      putU64LE(d, off + 16, newKey)     # name = entryName
      grafted = true
      break
  doAssert grafted, "no empty root slot to graft " & entryName & " into"
  writeFile(outPath, cast[string](d))

# ---------------------------------------------------------------------------
# ct-print binary
# ---------------------------------------------------------------------------

proc ensureCtPrint() =
  ## Compile ct-print into `ctPrintBin` when missing or stale. The libzstd
  ## flags mirror the documented build recipe; pkg-config resolves them.
  if fileExists(ctPrintBin) and
     getLastModificationTime(ctPrintBin) >= getLastModificationTime(ctPrintSrc):
    return
  createDir(ctPrintBin.parentDir)
  let (cflags, c1) = execCmdEx("pkg-config --cflags libzstd")
  let (lflags, c2) = execCmdEx("pkg-config --libs libzstd")
  doAssert c1 == 0 and c2 == 0, "pkg-config libzstd failed"
  let cmd = "nim c -d:release --mm:arc -p:src " &
    "--passC:" & quoteShell(cflags.strip()) & " " &
    "--passL:" & quoteShell(lflags.strip()) & " " &
    "--hints:off --warnings:off " &
    "-o:" & quoteShell(ctPrintBin) & " " & quoteShell(ctPrintSrc)
  let (output, code) = execCmdEx(cmd)
  doAssert code == 0, "failed to build ct-print:\n" & output

proc jsonEvents(bundle: string): string =
  let (output, code) = execCmdEx(
    quoteShell(ctPrintBin) & " --json-events " & quoteShell(bundle))
  doAssert code == 0, "ct-print --json-events failed (" & $code & "):\n" & output
  output

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

proc lowercaseTypes(arr: JsonNode): seq[string] =
  doAssert arr.kind == JArray
  for ev in arr.elems:
    if ev.kind == JObject and ev.hasKey("type"):
      result.add ev["type"].getStr()

ensureCtPrint()

let tmp = getTempDir() / ("ct_print_m23e1_" & $epochTime())
createDir(tmp)
let eventsLogBundle = tmp / "events_log_only.ct"
let splitBundle = tmp / "split_only.ct"
let combinedBundle = tmp / "combined.ct"

buildEventsLogBundle(eventsLogBundle)
discard buildSplitBundle(splitBundle)
# Combined bundle: the events.log bundle (authentic, decodable events.log) with
# a grafted ``steps.dat`` marker, so it carries BOTH layouts. M23e-4 routes it
# via the legacy events.log reader (events.log present ⇒ legacy).
graftEntryInto(eventsLogBundle, combinedBundle, "steps.dat")

block test_split_is_canonical_lowercase:
  let splitOut = jsonEvents(splitBundle)
  let arr = parseJson(splitOut)
  let types = lowercaseTypes(arr)
  doAssert "path" in types, "split bundle missing lowercase 'path': " & $types
  doAssert "function" in types, "split bundle missing 'function': " & $types
  doAssert "step" in types, "split bundle missing 'step': " & $types
  doAssert "call" in types, "split bundle missing 'call': " & $types
  echo "[ok] split bundle emits canonical lowercase type-tagged events"

block test_events_log_routes_to_legacy_when_both_present:
  ## M23e-4: a bundle carrying BOTH events.log and a split-stream marker must
  ## read via the LEGACY events.log reader — its output is BYTE-IDENTICAL to
  ## the events.log-only bundle's output (both reads go through the legacy
  ## path), and DISTINCT from the split-only bundle's lowercase output.
  let eventsLogOut = jsonEvents(eventsLogBundle)
  let combinedOut = jsonEvents(combinedBundle)
  doAssert eventsLogOut == combinedOut,
    "events.log routing broken: combined (events.log+steps.dat) output differs " &
    "from events.log-only output.\n--- events.log ---\n" & eventsLogOut &
    "\n--- combined ---\n" & combinedOut
  let splitOut = jsonEvents(splitBundle)
  doAssert combinedOut != splitOut,
    "combined bundle must NOT read via the split path (its events.log is the " &
    "authoritative source under M23e-4)"
  echo "[ok] events.log present ⇒ legacy reader even when a split marker is present"

block test_events_log_only_fallback_is_populated:
  ## The events.log-only bundle must now produce a NON-EMPTY event array
  ## (routed to the legacy reader). Before M23e-1 the v4 path emitted an
  ## (almost) empty array because the split streams were absent.
  let elOut = jsonEvents(eventsLogBundle)
  let arr = parseJson(elOut)
  doAssert arr.kind == JArray
  # The legacy reader surfaces Path/Function/Step/Call/Value/Return — at
  # minimum the two Step events and the Call must appear. A regressed
  # (empty-fallback) build would yield <= 1 element (just the path).
  doAssert arr.len >= 6,
    "events.log fallback produced too few events (regressed to empty?): " &
    $arr.len & " events:\n" & elOut
  var sawStep = false
  var sawCall = false
  for ev in arr.elems:
    if ev.kind == JObject and ev.hasKey("type"):
      case ev["type"].getStr()
      of "Step": sawStep = true
      of "Call": sawCall = true
      else: discard
  doAssert sawStep, "events.log fallback missing Step events:\n" & elOut
  doAssert sawCall, "events.log fallback missing Call event:\n" & elOut
  echo "[ok] events.log-only bundle falls back to a populated legacy event array"

removeDir(tmp)
echo "All ct-print events.log fallback (M23e-1) tests passed."
