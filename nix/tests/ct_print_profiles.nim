## Exercise the installed inspector against its own production writer and
## profile encoder. A malformed-profile and metadata mutation prove that
## accepting a new container layout does not disable either refusal.
import std/[os, osproc, json, strutils]
import results
import codetracer_trace_writer/[multi_stream_writer, compact_profile, meta_dat]
import codetracer_ctfs/[compact, types]

static:
  doAssert MetaDatVersion == 6, "the historical inspector test must use its paired writer"

let inspector = paramStr(1)
doAssert inspector.len > 0, "pass the installed ct-print binary"
let work = getTempDir() / ("ct-print-profiles-" & $getCurrentProcessId())
createDir(work)

proc bytes(path: string): seq[byte] =
  let text = readFile(path)
  for ch in text: result.add(byte(ch))

proc inspect(path, view: string): (string, int) =
  execCmdEx(quoteShell(inspector) & " --" & view & " " & quoteShell(path))

try:
  let fullPath = work / "full.ct"
  var writer = initMultiStreamWriter(fullPath, "profile-fixture").get()
  let path = writer.registerPath("main.nr").get()
  doAssert writer.registerStep(path, 1'u64, @[]).isOk
  doAssert writer.registerStep(path, 2'u64, @[]).isOk
  doAssert writer.close().isOk
  writer.closeCtfs()
  let compact = selectProfile(bytes(fullPath), high(uint64)).get()[1]
  let compactPath = work / "compact.ct"
  writeFile(compactPath, compact)
  let storedPath = work / "compressed.ct"
  writeFile(storedPath, compressImage(compact).get())
  for view in ["full", "summary", "meta-json", "events"]:
    let expected = inspect(fullPath, view)
    doAssert expected[1] == 0, expected[0]
    for candidate in [compactPath, storedPath]:
      let actual = inspect(candidate, view)
      doAssert actual[1] == 0, actual[0]
      doAssert actual[0] == expected[0], view & ": profile changed decoded content"
  let doc = parseJson(inspect(compactPath, "full")[0])
  doAssert doc["counts"]["steps"].getInt() == 2

  var badProfile = compact
  badProfile[V6ProfileOffset] = 2
  let badPath = work / "bad.ct"
  writeFile(badPath, badProfile)
  let profileRefusal = inspect(badPath, "full")
  doAssert profileRefusal[1] != 0 and "profile" in profileRefusal[0]

  var members = compactMembersOf(bytes(fullPath)).get()
  for member in members.mitems:
    if member.name == "meta.dat":
      member.payload[4] = 0
      member.payload[5] = 0
  writeFile(badPath, encodeCompactContainer(members).get())
  let metaRefusal = inspect(badPath, "full")
  doAssert metaRefusal[1] != 0 and "schema version 0" in metaRefusal[0], metaRefusal[0]
  echo "PASS: installed ct-print preserves decoded content across profiles and refuses invalid declarations"
finally:
  removeDir(work)
