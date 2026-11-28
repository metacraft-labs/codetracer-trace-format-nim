## A trace is opened with the block size and the root directory its own
## header declares, never with the library's defaults.
##
## Spec: `codetracer-trace-format-spec/ctfs-container.md` §1 ("Container
## Header": `BlockSize` is 1024, 2048 or 4096, and `MaxRootEntries` is fixed
## when the container is created), "Block 0 Layout" (a declared `0` is the
## auto-filled root, 170 slots at 4096 bytes: "A reader that reads `0` as "no
## entries" reads an empty container and reports success") and "The Root
## Directory Is Fixed At Creation" (a reader reads both once, when it opens
## the container).
##
## One recording, written by this repository's own split-stream writer (4096
## bytes, 31 slots), is rebuilt member for member, every member's bytes
## unchanged, into five containers:
##
## * `same`    -- 4096 bytes, 31 slots: the CONTROL. The rebuild with the
##   writer's own parameters must read as the original before and after any
##   change to the reader; it is what shows the rebuild itself is faithful.
## * `shift31` -- 4096 bytes, `MaxRootEntries = 0` (the auto-filled root),
##   31 one-byte filler members in slots 0 to 30, the trace after them.
## * `root64`  -- 4096 bytes, 64 slots, the same 31 fillers first.
## * `bs1024`, `bs2048` -- 1024- and 2048-byte blocks, 31 slots.
##
## Each is opened by every entry point a caller has -- `openNewTrace(path)`,
## `openNewTraceFromBytes(bytes)`, `refresh` on a reader that reads its file
## as it is used, the C ABI's `ct_reader_open`, `ct_reader_open_bytes` and
## `ct_reader_refresh`, and `ct-print --summary` -- and each must report the
## base recording's path, function and step counts exactly (ct-print: its
## whole summary). `openStepMapIn` must read the base's breakpoint index. A reader that
## scans the 31 slots of its default reads `shift31` and `root64` as empty
## containers (no paths, no functions, no `steps.dat`) and reports success; a
## reader that resolves members with its default 4096-byte block refuses
## `bs1024` and `bs2048` ("member block size 4096 differs from the CTFS
## header's 1024").
##
## The recording is large enough that its streams span several blocks at
## every block size, so a member is resolved through a mapping block, which an
## image read from its file loads on demand: a reader that loaded the file in
## units of its own default block rather than the container's would read the
## wrong bytes there.
##
## No mocks: the containers come from the real writer and the real container
## writer, and are read through the real readers and the real `ct-print`.

include codetracer_trace_writer_ffi

{.pop.}

import std/[os, osproc, strutils]
import ./exact_bytes_file
import codetracer_ctfs/base40
import codetracer_trace_writer/new_trace_reader as ntr
import codetracer_trace_writer/step_map_builder
import ct_print_binary

type Counts = tuple[paths, functions, steps: uint64]

proc recording(): seq[byte] =
  ## 3,000 steps over three files, a call every 7 steps, a value on every
  ## step: `steps.dat` and `values.dat` each span several 1024-byte blocks.
  var w = initMultiStreamWriter("", "declared_root",
    recordingId = "01949fcc-7d92-7e9c-8ccc-eeeeeeeeeeee").get()
  for p in ["/src/a.nim", "/src/b.nim", "/src/c.nim"]:
    doAssert w.registerPath(p).isOk
  let work = w.registerFunction("work").get()
  discard w.registerFunction("rest").get()
  for i in 0 ..< 3000:
    let vals = @[VariableValue(varnameId: uint64(i mod 5),
      data: @[byte(i and 0xff), byte(i shr 8)])]
    doAssert w.registerStep(uint64(i mod 3), uint64(1 + (i * 7) mod 300),
      vals).isOk
    if i mod 7 == 0:
      doAssert w.registerCall(work, @[]).isOk
    if i mod 7 == 3:
      doAssert w.registerReturn(@[byte(9)]).isOk
  doAssert w.close().isOk
  result = w.toBytes()
  doAssert w.closeCtfs().isOk

proc members(data: seq[byte]): seq[(string, seq[byte])] =
  ## Every member of the full-profile container `data`, in slot order, read
  ## with the geometry its header declares.
  let bs = readU32LE(data, 8)
  let slots = headerRootEntryCount(data)
  let base = containerHeaderSize(data)
  for i in 0 ..< int(slots):
    let nameWord = readU64LE(data, base + i * FileEntrySize + 16)
    if nameWord == 0: continue
    let name = base40Decode(nameWord)
    let r = readInternalFile(data, name, bs, slots)
    doAssert r.isOk, name & ": " & r.error
    result.add((name, r.get()))

proc rebuild(src: seq[(string, seq[byte])], path: string, blockSize: uint32,
             maxRootEntries: uint32, fillers: int): seq[byte] =
  ## `src` written into a new container of the given geometry, after
  ## `fillers` one-byte members, and the container's bytes.
  var c = createCtfs(blockSize = blockSize, maxRootEntries = maxRootEntries)
  for i in 0 ..< fillers:
    var f = c.addFile("f" & align($i, 2, '0') & ".dat").get()
    doAssert c.writeToFile(f, [byte(i)]).isOk
  for (name, bytes) in src:
    var f = c.addFile(name).get()
    if bytes.len > 0:
      doAssert c.writeToFile(f, bytes).isOk
  doAssert writeCtfsToFile(c, path).isOk
  cast[seq[byte]](readFile(path))

var failures: seq[string]
template check(cond: bool, msg: string) =
  if not cond: failures.add(msg)

proc countsOf(r: var NewTraceReader, what: string): Counts =
  ## The three counts; a step count that cannot be read is a failure, and
  ## reads as `high(uint64)` so it equals no recording's.
  let steps = r.stepCount()
  check(steps.isOk, what & ": stepCount: " &
    (if steps.isOk: "" else: steps.error))
  (r.pathCount(), r.functionCount(),
   if steps.isOk: steps.get() else: high(uint64))

proc ffiCountsOf(h: pointer): Counts =
  (ct_reader_path_count(h), ct_reader_function_count(h),
   ct_reader_step_count(h))

proc stepMapOf(bytes: seq[byte]): string =
  ## The container's whole breakpoint index (`step-map.ns`), as text, or the
  ## error that kept it from being read.
  let m = openStepMapIn(bytes)
  if m.isErr: return "ERR " & m.error
  let all = m.get().loadAll()
  if all.isErr: "ERR " & all.error else: $all.get()

proc lastErr(): string =
  let e = trace_writer_last_error()
  if e.isNil: "" else: $e

let dir = getTempDir() / "ct_open_reads_the_declared_root"
removeDir(dir)
createDir(dir)

let base = recording()
doAssert readU32LE(base, 8) == 4096'u32 and readU32LE(base, 12) == 31'u32,
  "the writer's own geometry is 4096 bytes and 31 slots"
var baseReader = ntr.openNewTraceFromBytes(base).get()
let want = countsOf(baseReader, "base")
# Counts that a reader of an empty container could also report would make
# every equality below vacuous.
doAssert want.paths >= 2 and want.functions >= 1 and want.steps >= 4,
  "the base recording is too small to tell an empty read apart: " & $want

let wantStepMap = stepMapOf(base)
doAssert not wantStepMap.startsWith("ERR") and wantStepMap.len > 100,
  "the base recording carries a breakpoint index: " & wantStepMap

let src = members(base)
doAssert src.len >= 10, "the recording has " & $src.len & " members"

let forms = [
  ("same", 4096'u32, 31'u32, 0),
  ("shift31", 4096'u32, 0'u32, 31),
  ("root64", 4096'u32, 64'u32, 31),
  ("bs1024", 1024'u32, 31'u32, 0),
  ("bs2048", 2048'u32, 31'u32, 0),
]

ensureCtPrint()
let basePath = dir / "base.ct"
writeBytesFile(basePath, base)
let (wantSummary, baseStatus) = execCmdEx(
  quoteShell(ctPrintBin) & " --summary " & quoteShell(basePath))
doAssert baseStatus == 0, "ct-print --summary of the base: " & wantSummary
doAssert ("steps: ") in wantSummary, wantSummary

doAssert failures.len == 0, "the base recording reads: " & $failures

for (name, bs, declared, fillers) in forms:
  let path = dir / (name & ".ct")
  let bytes = rebuild(src, path, bs, declared, fillers)
  doAssert readU32LE(bytes, 8) == bs and readU32LE(bytes, 12) == declared,
    name & ": the rebuild carries the geometry asked for"
  if fillers > 0:
    # The trace's first member lies past slot 30, out of reach of a reader
    # that scans the default 31 slots.
    let first = base40Decode(readU64LE(bytes,
      containerHeaderSize(bytes) + fillers * FileEntrySize + 16))
    doAssert first == src[0][0], name & ": slot " & $fillers & " is " & first

  # By path: the image is read from its file as it is used.
  block byPath:
    var opened = ntr.openNewTrace(path)
    if opened.isErr:
      check(false, name & ": openNewTrace: " & opened.error)
      break byPath
    var r = opened.get()
    let got = countsOf(r, name & " openNewTrace")
    check(got == want, name & ": openNewTrace read " & $got & ", not " & $want)
    # `refresh` reads the root directory again and reopens over the same
    # image, keeping the stream reader `stepCount` loaded.
    let refreshed = r.refresh(path)
    if refreshed.isErr:
      check(false, name & ": refresh: " & refreshed.error)
      break byPath
    let again = countsOf(r, name & " refresh")
    check(again == want, name & ": after refresh " & $again & ", not " & $want)

  block fromBytes:
    var opened = ntr.openNewTraceFromBytes(bytes)
    if opened.isErr:
      check(false, name & ": openNewTraceFromBytes: " & opened.error)
      break fromBytes
    let got = countsOf(opened.get(), name & " openNewTraceFromBytes")
    check(got == want,
      name & ": openNewTraceFromBytes read " & $got & ", not " & $want)

  # `openStepMapIn`, the other entry point that opens a container's member.
  let gotStepMap = stepMapOf(bytes)
  check(gotStepMap == wantStepMap, name & ": openStepMapIn read " &
    (if gotStepMap.len > 200: gotStepMap[0 ..< 200] & "..." else: gotStepMap))

  # The C ABI: by path, with a refresh of that handle, and from bytes.
  block cAbi:
    trace_writer_clear_last_error()
    let h = ct_reader_open(cstring(path))
    if h.isNil:
      check(false, name & ": ct_reader_open: " & lastErr())
      break cAbi
    check(ffiCountsOf(h) == want,
      name & ": ct_reader_open read " & $ffiCountsOf(h) & ", not " & $want)
    check(ct_reader_refresh(h, cstring(path)) == 0,
      name & ": ct_reader_refresh: " & lastErr())
    check(ffiCountsOf(h) == want, name & ": after ct_reader_refresh " &
      $ffiCountsOf(h) & ", not " & $want)
    ct_reader_close(h)
  block cAbiBytes:
    trace_writer_clear_last_error()
    let hb = ct_reader_open_bytes(unsafeAddr bytes[0], csize_t(bytes.len))
    if hb.isNil:
      check(false, name & ": ct_reader_open_bytes: " & lastErr())
      break cAbiBytes
    check(ffiCountsOf(hb) == want, name & ": ct_reader_open_bytes read " &
      $ffiCountsOf(hb) & ", not " & $want)
    ct_reader_close(hb)

  let (summary, status) = execCmdEx(
    quoteShell(ctPrintBin) & " --summary " & quoteShell(path))
  check(status == 0 and summary.replace(path, basePath) == wantSummary,
    name & ": ct-print --summary (exit " & $status & "):\n" & summary)

for f in failures:
  echo "FAIL: " & f
doAssert failures.len == 0, $failures.len & " failure(s)"
removeDir(dir)
echo "test_open_new_trace_reads_the_declared_root: OK"
