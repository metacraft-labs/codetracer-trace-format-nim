## `ct-print`'s JSON modes stream their output, in memory proportional to the
## trace, without changing a byte of it: `--full`, `--events`, `--markers`,
## `--json` and `--json-events` on split-stream containers, and `--full` /
## `--events` on native MCR bundles.
##
## codetracer-pm
## `issues/2026-10-31-ct-print-full-uses-32-gb-to-decode-an-11-mb-python-trace.md`:
## `ct-print --full` on an 11 MB Python trace (a recording of `import black`,
## ~310 K steps and ~37 K calls) reached 32 GB RSS and ran for ~20 minutes.
## Two defects, both in the one assembly of the document
## (`full_document_json.buildFullDocument`) and its printer:
##
## * **Memory.** The WHOLE document was built as a `JsonNode` tree — every
##   decoded variable of every step, at hundreds of bytes per node — and
##   `--full` then rendered it into ONE more string the size of the entire
##   output before writing a byte. `--events` and `--markers` built the whole
##   tree too, as did the legacy `--json` / `--json-events` (with their own
##   per-step scans) and the native decoder (`native_decoder`).
## * **Time.** For every step the assembler scanned ALL call records twice
##   (entries and exits) and all IO events, copying each `CallRecord` with its
##   argument and children sequences on every iteration: steps x calls work.
##   Separately, attributing each step to its enclosing call went through
##   `NewTraceReader.callForStep`, a binary search plus a walk back over every
##   already-returned sibling, on a call stream that holds one inflated chunk:
##   on the Python shape below (a module body that calls thousands of
##   functions) that is again steps x calls.
##
## The contract checked here:
##
## 1. **Streamed.** On a container shaped like that trace, the child's peak
##    RSS is well below the size of the output it wrote, and below a bound
##    proportional to the container (see `rssBound`), in every split-stream
##    JSON mode; on a large native bundle, likewise for `--full`, and within
##    the bound for `--events` (see `test_a_large_native_bundle_streams`). An
##    implementation that holds the rendered output, or the tree it is
##    rendered from, cannot meet the first: the rendered string alone is the
##    output's size.
## 2. **Terminates.** The run is killed after `runLimit`, so a quadratic
##    assembly fails this test instead of hanging the corpus. The limit is a
##    safety net, not the detector — the detector is (1) plus the counts — and
##    it sits more than 10x above the fixed binary's runtime on a busy host.
## 3. **Byte-identical.** On smaller containers, where the in-process tree is
##    affordable, the binary's `--full` output is exactly
##    `pretty(buildFullDocument(...), indent = 2)` plus `echo`'s newline,
##    `--events` is exactly the header's `$` then one `$ev` per line, and
##    `--markers --json-out` is the `pretty` of the marker events. The
##    fixtures cover calls whose entry and exit share a step, call entries
##    past the last step (the `synthesized_at_step` drain), IO events at steps
##    and past the last step, raise/catch records, a correlation marker, and a
##    container with no steps at all. A native bundle's `--full` / `--events`
##    are compared with `buildNativeFullDocument` the same way. `--json` and
##    `--json-events` have no in-process builder (their documents only exist
##    in the CLI), so their text is held to being `pretty`'s own layout of the
##    document it spells, with the records the fixture holds; their
##    byte-identity with the pre-streaming binary was checked by hand on
##    eleven containers, native ones included, for every mode.
## 4. **Same attribution.** Every `kind="step"` event's `function_id` and
##    `depth` equal what `NewTraceReader.callForStep` answers for that step:
##    the document takes the enclosing call's key from the reader
##    (`enclosingCallKey`) and its record from the table it already holds,
##    and this checks the two halves still meet. `callForStep` itself is held
##    to its definition by `test_call_for_step_matches_reference.nim`.
##
## Peak RSS is read with `wait4` on the one child, not `getrusage(
## RUSAGE_CHILDREN)`: the latter is the maximum over every child this process
## has reaped, and `ensureCtPrint` reaps a Nim compiler that is far larger
## than the ct-print under test. `ru_maxrss` is bytes on macOS and KiB on
## Linux. On macOS the peak and the cap also take the physical footprint into
## account (`residentBytes`): page compression keeps a runaway process's
## resident set far below what it holds.
##
## Falsifiability: at `bbe8b15` (before the fix), on the shared macOS arm64
## host, the large `--full` run passed twice the memory bound (237 MiB) after
## 7.2 s without having written a byte, and was killed: (1) fails. The fixed
## binary wrote the same container's 377 MiB in 5.2 s at a 21 MiB peak. On
## the large native bundle the unstreamed decoder peaked at 852 MiB
## (`--full`) and 576 MiB (`--events`) against a 190 MiB bound; streamed, 42
## MiB.
## Restoring `echo pretty(root, indent = 2)` over `buildFullDocument` in
## `printFullV4` fails (1): RSS exceeds the output size. Restoring the
## per-step linear scans fails (2) at `runLimit`. Getting the streamed
## indentation wrong by one space fails (3). Changing the event order does
## NOT fail (3), since the binary and the in-process tree share
## `forEachFullEvent`; it fails `checkOrder`, which holds the document to a
## brute-force reading of the order `trace-events.md` specifies (checked by
## breaking the `call_exit` tie-break to `call_key` ascending).
##
## The memory cap (`runCtPrintMeasured`) kills a child that passes twice the
## bound: it protects a shared host from the defect this test guards against,
## and it is why the unfixed binary fails here in seconds rather than at
## `runLimit`.
##
## No mocks: every container is written by the production writer, read by the
## production reader, and printed by the shipped `ct-print` binary built from
## this tree (`tests/ct_print_binary.nim`).

import std/[os, posix, strutils, sequtils, json, options, monotimes, times,
  tempfiles, algorithm]
import results
import codetracer_trace_types
import codetracer_trace_writer/cbor
import codetracer_trace_writer/call_stream
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/io_event_stream
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/new_trace_reader
import codetracer_trace_writer/full_document_json
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/interning_table
import codetracer_ctfs
import codetracer_ctfs/container
import native_decoder
import ct_print_binary
import native_bundle_encoding

const
  bigCalls = 30_000
    ## Python-shaped: the `import black` trace had 37 485 calls over 309 587
    ## exec records; this gives ~30 K calls over ~280 K exec records.
  bigStepsPerCall = 8
  nativeEventsPerThread = 150_000
    ## 300 K thread events: a ~10 MiB native bundle, ~100 MiB of `--full`.
  smallCalls = 40
  smallStepsPerCall = 3
  bufferedStepId = 1_000_000_000
    ## An IO event's explicit step coordinate past every step (the drain
    ## after the step loop prints it).
  runLimit = initDuration(minutes = 5)
    ## The fixed binary prints the large container in a few seconds on the
    ## shared macOS arm64 host (see the PASS line this test echoes); five
    ## minutes is far over 10x that even under heavy contention, and far
    ## under the time the quadratic assembly needs.

# ---------------------------------------------------------------------------
# Running the binary, measured
# ---------------------------------------------------------------------------

proc c_exit(code: cint) {.importc: "_exit", header: "<unistd.h>", noreturn.}

when defined(macosx):
  type RusageInfoV0 {.importc: "struct rusage_info_v0",
      header: "<libproc.h>".} = object
    ri_resident_size: uint64
    ri_phys_footprint: uint64
  proc proc_pid_rusage(pid: cint, flavor: cint, buffer: pointer): cint
    {.importc, header: "<libproc.h>".}

proc residentBytes(pid: Pid): int64 =
  ## The child's memory right now, or 0 when it cannot be read. On macOS this
  ## is the larger of the resident set and the physical footprint: the kernel
  ## compresses and swaps a growing process's pages, so its resident set
  ## stalls at a few GB while its footprint keeps climbing. Measured on the
  ## shared macOS arm64 host on the trace of the issue below: the unfixed
  ## binary's resident set read 4.7 GB while its footprint was 60 GB, so a
  ## cap on the resident set alone never fires.
  when defined(macosx):
    var info: RusageInfoV0
    if proc_pid_rusage(cint(pid), 0, addr info) == 0:  # RUSAGE_INFO_V0
      return int64(max(info.ri_resident_size, info.ri_phys_footprint))
  elif defined(linux):
    try:
      let fields = readFile("/proc/" & $pid & "/statm").splitWhitespace()
      return parseBiggestInt(fields[1]) * int64(sysconf(SC_PAGESIZE))
    except CatchableError:
      discard
  0

type Measured = object
  exitCode: int        ## -1 when the child was killed by a signal
  signal: int
  timedOut: bool
  overMemoryCap: bool
  maxRssBytes: int64
    ## The larger of `wait4`'s peak resident set and the largest
    ## `residentBytes` sampled while the child ran.
  seconds: float
  stdoutBytes: int64

proc runCtPrintMeasured(args: openArray[string],
    stdoutPath, stderrPath: string, memoryCap = high(int64)): Measured =
  ## Run the shipped `ct-print` with `args`, stdout and stderr redirected to
  ## files, killing it after `runLimit` or once its resident set passes
  ## `memoryCap`; return its exit, its own peak RSS and how much it wrote.
  ##
  ## The cap exists for the host, not for the verdict: this test runs on
  ## shared machines, and the defect it guards against grew to 32 GB.
  var argvSeq = @[ctPrintBin]
  for a in args: argvSeq.add(a)
  let argv = allocCStringArray(argvSeq)
  defer: deallocCStringArray(argv)
  # Converted before the fork: the child only makes async-signal-safe calls.
  let outC = stdoutPath.cstring
  let errC = stderrPath.cstring
  let binC = ctPrintBin.cstring
  let started = getMonoTime()
  let pid = fork()
  if pid == 0:
    let fo = posix.open(outC, O_WRONLY or O_CREAT or O_TRUNC, Mode(0o600))
    let fe = posix.open(errC, O_WRONLY or O_CREAT or O_TRUNC, Mode(0o600))
    if fo < 0 or fe < 0 or dup2(fo, 1) < 0 or dup2(fe, 2) < 0:
      c_exit(126)
    discard execv(binC, argv)
    c_exit(127)
  doAssert pid > 0, "fork failed: " & osErrorMsg(osLastError())
  var status: cint
  var ru: Rusage
  var sampledPeak = 0'i64
  while true:
    let r = wait4(pid, addr status, WNOHANG, addr ru)
    if r == pid:
      break
    if r < 0 and errno != EINTR:
      doAssert false, "wait4 failed: " & osErrorMsg(osLastError())
    let now = residentBytes(pid)
    sampledPeak = max(sampledPeak, now)
    let overTime = getMonoTime() - started > runLimit
    let overMemory = not overTime and now > memoryCap
    if overTime or overMemory:
      discard kill(pid, SIGKILL)
      result.timedOut = overTime
      result.overMemoryCap = overMemory
      while wait4(pid, addr status, 0, addr ru) < 0 and errno == EINTR:
        discard
      break
    sleep(20)
  result.seconds = float((getMonoTime() - started).inMilliseconds) / 1000.0
  when defined(macosx):
    result.maxRssBytes = int64(ru.ru_maxrss)
  else:
    result.maxRssBytes = int64(ru.ru_maxrss) * 1024
  result.maxRssBytes = max(result.maxRssBytes, sampledPeak)
  if WIFEXITED(status):
    result.exitCode = int(WEXITSTATUS(status))
  else:
    result.exitCode = -1
    result.signal = int(WTERMSIG(status))
  result.stdoutBytes = int64(getFileSize(stdoutPath))

proc mib(n: int64): string = formatFloat(float(n) / 1048576.0, ffDecimal, 1) & " MiB"

proc describe(m: Measured): string =
  "exit " & $m.exitCode & (if m.signal != 0: " (signal " & $m.signal & ")" else: "") &
    (if m.timedOut: ", KILLED after " & $runLimit.inSeconds & " s" else: "") &
    (if m.overMemoryCap: ", KILLED at the memory cap" else: "") &
    ", " & formatFloat(m.seconds, ffDecimal, 1) & " s, peak RSS " &
    mib(m.maxRssBytes) & ", wrote " & mib(m.stdoutBytes)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

proc encodeValue(v: ValueRecord): seq[byte] =
  var enc = CborEncoder.init()
  enc.encodeCborValueRecord(v)
  enc.getBytes()

proc toBytes(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i, c in s: result[i] = byte(c)

proc writePythonShaped(path: string, calls, stepsPerCall, chunkSize: int):
    tuple[steps, entries: int] =
  ## A module body that calls `calls` functions one after another, with a
  ## module-level step between calls — the shape of a recorded `import`.
  ## Every fourth call makes a nested one-step call (its entry and exit share
  ## a step), some raise and catch, and every step carries three variables.
  ## The tail leaves two frames open at close, so both exit on the last step
  ## and the innermost frame's entry lies past it (the writer's close-time
  ## drain), and puts one IO event past the last step.
  ##
  ## Returns the number of `kind="step"` events and of call entries the
  ## document must contain.
  var w = initMultiStreamWriter(path, "python", chunkSize = chunkSize).get()
  w.metadata.workdir = "/wd"
  w.metadata.args = @["-c", "import black"]
  let pMod = w.registerPath("/wd/main.py").get()
  let pLib = w.registerPath("/wd/lib/black/__init__.py").get()
  let fnModule = w.registerFunction("<module>").get()
  let fnVisit = w.registerFunction("visit").get()
  let fnLeaf = w.registerFunction("leaf").get()
  let vnI = w.registerVarname("i").get()
  let vnName = w.registerVarname("name").get()
  let vnItems = w.registerVarname("items").get()
  let vnN = w.registerVarname("n").get()
  let tInt = TypeId(w.registerType("int").get())
  let tStr = TypeId(w.registerType("str").get())
  let tList = TypeId(w.registerType("list").get())
  let tExc = w.registerType("ValueError").get()

  proc vars(i, s: int): seq[VariableValue] =
    @[
      VariableValue(varnameId: vnI, data: encodeValue(
        ValueRecord(kind: vrkInt, intVal: int64(i * 31 + s), intTypeId: tInt))),
      VariableValue(varnameId: vnName, data: encodeValue(
        ValueRecord(kind: vrkString, text: "node-" & $i & "-" & $s,
          strTypeId: tStr))),
      VariableValue(varnameId: vnItems, data: encodeValue(
        ValueRecord(kind: vrkSequence, isSlice: false, seqTypeId: tList,
          seqElements: @[
            ValueRecord(kind: vrkInt, intVal: int64(i), intTypeId: tInt),
            ValueRecord(kind: vrkInt, intVal: int64(s), intTypeId: tInt)])))]

  proc arg(i: int): seq[CallArg] =
    @[CallArg(varnameId: vnN, value: encodeValue(
      ValueRecord(kind: vrkInt, intVal: int64(i), intTypeId: tInt)))]

  proc ret(i: int): seq[byte] =
    encodeValue(ValueRecord(kind: vrkInt, intVal: int64(i * 2), intTypeId: tInt))

  doAssert w.registerStep(pMod, 1, vars(0, 0)).isOk
  inc result.steps
  doAssert w.registerCall(fnModule, @[]).isOk
  inc result.entries
  for i in 0 ..< calls:
    doAssert w.registerStep(pMod, uint64(2 + i mod 50), vars(i, 0)).isOk
    inc result.steps
    if i == 3:
      doAssert w.registerIOEvent(elkWrite, toBytes("hello\n")).isOk
    doAssert w.registerCall(fnVisit, arg(i)).isOk
    inc result.entries
    for s in 0 ..< stepsPerCall:
      doAssert w.registerStep(pLib, uint64(10 + s), vars(i, s + 1)).isOk
      inc result.steps
      if s == 1 and i mod 4 == 1:
        doAssert w.registerCall(fnLeaf, arg(-i)).isOk
        inc result.entries
        doAssert w.registerStep(pLib, 40, vars(i, 99)).isOk
        inc result.steps
        doAssert w.registerReturn(ret(-i)).isOk
      if s == 0 and i mod 97 == 5:
        doAssert w.registerRaise(tExc, toBytes("bad node " & $i)).isOk
        doAssert w.registerCatch(tExc).isOk
    doAssert w.registerReturn(ret(i)).isOk

  # Tail: two IO events, one of them a correlation marker, then frames left
  # open at close.
  doAssert w.registerStep(pMod, 60, vars(calls, 0)).isOk
  inc result.steps
  doAssert w.registerIOEvent(elkWriteOther, toBytes("warning\n")).isOk
  doAssert w.registerCorrelationMarker("send", "edge", "k-1",
    showValue = "42").isOk
  doAssert w.registerIOEvent(elkWrite, toBytes("buffered\n"),
    stepId = some(uint64(bufferedStepId))).isOk
  doAssert w.registerCall(fnVisit, arg(calls)).isOk
  inc result.entries
  doAssert w.registerStep(pLib, 10, vars(calls, 1)).isOk
  inc result.steps
  doAssert w.registerCall(fnLeaf, arg(calls + 1)).isOk  # entry past last step
  inc result.entries
  doAssert w.close().isOk
  doAssert w.closeCtfs().isOk

proc writeNoSteps(path: string) =
  ## A container whose exec stream is empty but which carries IO events: the
  ## assembler then has no step loop, and must still print every IO event.
  var w = initMultiStreamWriter(path, "empty", chunkSize = 4).get()
  w.metadata.workdir = "/wd"
  discard w.registerPath("/wd/main.py").get()
  discard w.registerFunction("main").get()
  doAssert w.registerIOEvent(elkWrite, toBytes("before any step\n")).isOk
  doAssert w.registerIOEvent(elkWrite, toBytes("later\n"),
    stepId = some(7'u64)).isOk
  doAssert w.close().isOk
  doAssert w.closeCtfs().isOk

# ---------------------------------------------------------------------------
# Expected text, computed in-process from the tree
# ---------------------------------------------------------------------------

proc inProcessDocument(path: string, opts: FullOpts): JsonNode =
  var reader = openNewTrace(path).get()
  buildFullDocument(reader, opts)

proc expectedFull(doc: JsonNode): string =
  pretty(doc, indent = 2) & "\n"

proc expectedEvents(doc: JsonNode): string =
  var header = newJObject()
  for k, v in doc.pairs:
    if k != "events":
      header[k] = v
  result = $header & "\n"
  for ev in doc["events"].elems:
    result.add($ev & "\n")

proc expectedMarkers(doc: JsonNode): string =
  var arr = newJArray()
  for ev in doc["events"].elems:
    if isCorrelationMarker(ev):
      arr.add(ev)
  pretty(arr, indent = 2) & "\n"

proc firstDifference(a, b: string): string =
  ## Where two texts first differ, with a little context, for the failure
  ## message (the texts themselves can be megabytes).
  var i = 0
  while i < a.len and i < b.len and a[i] == b[i]: inc i
  let lo = max(0, i - 200)
  "first difference at byte " & $i & " (lengths " & $a.len & " / " & $b.len &
    ")\n--- binary:\n" & a[lo ..< min(a.len, i + 200)] &
    "\n--- expected:\n" & b[lo ..< min(b.len, i + 200)]

proc checkBinaryMatches(dir, bundle: string, args: openArray[string],
    expected, what: string) =
  let outPath = dir / "out.txt"
  let errPath = dir / "err.txt"
  var full = @args
  full.add(bundle)
  let m = runCtPrintMeasured(full, outPath, errPath)
  doAssert not m.timedOut and m.exitCode == 0,
    "ct-print " & args.join(" ") & " on " & what & ": " & describe(m) &
    "\nstderr:\n" & readFile(errPath)
  let got = readFile(outPath)
  doAssert got == expected,
    "ct-print " & args.join(" ") & " on " & what &
    " is not byte-identical to the in-process rendering: " &
    firstDifference(got, expected)

proc checkAttribution(path: string, doc: JsonNode) =
  ## Contract (4): each step's enclosing call is the reader's `callForStep`.
  var reader = openNewTrace(path).get()
  var checked = 0
  for ev in doc["events"].elems:
    if ev["kind"].getStr() != "step": continue
    let idx = uint64(ev["step_index"].getBiggestInt())
    let c = reader.callForStep(idx)
    if c.isOk:
      doAssert ev.hasKey("function_id") and
          uint64(ev["function_id"].getBiggestInt()) == c.get().functionId and
          ev["depth"].getInt() == int(c.get().depth),
        "step " & $idx & " is attributed to " & $ev.getOrDefault("function_id") &
        " at depth " & $ev.getOrDefault("depth") & ", but callForStep answers " &
        "function " & $c.get().functionId & " at depth " & $c.get().depth
      inc checked
    else:
      doAssert not ev.hasKey("function_id"),
        "step " & $idx & " has no enclosing call per callForStep, but the " &
        "document attributes it to function " & $ev["function_id"]
  doAssert checked > 0, "no step was attributed to a call; the check is vacuous"

proc referenceOrder(path: string): seq[string] =
  ## The event order `trace-events.md` §"Assembling an event stream" sets,
  ## computed by brute force from the records, independently of
  ## `forEachFullEvent`: per exec record, its call entries (call_key
  ## ascending), its own event (none for a column nudge), its IO events
  ## (index ascending), its call exits (call_key DESCENDING); then the call
  ## entries past the last record by (entry step, call_key); then the IO
  ## events past it, in index order. Each event is named `kind:key`, with an
  ## exec record's event as `rec:<index>`. The byte-identity checks compare
  ## the binary with the in-process tree, and both come from
  ## `forEachFullEvent`, so they cannot see an order change; this can.
  var reader = openNewTrace(path).get()
  var calls: seq[call_stream.CallRecord]
  for i in 0'u64 ..< reader.callCount().get():
    calls.add(reader.call(i).get())
  var ios: seq[IOEvent]
  for i in 0'u64 ..< reader.ioEventCount().get():
    ios.add(reader.ioEvent(i).get())
  let n = reader.stepCount().get()
  for s in 0'u64 ..< n:
    for k, c in calls:
      if c.entryStep == s: result.add("call_entry:" & $k)
    let ev = reader.step(s)
    if not (ev.isOk and ev.get().kind == sekDeltaColumn):
      result.add("rec:" & $s)
    for i, e in ios:
      if e.stepId == s: result.add("io:" & $i)
    for k in countdown(calls.len - 1, 0):
      if calls[k].exitStep == s: result.add("call_exit:" & $k)
  if n > 0:
    var past: seq[(uint64, int)]
    for k, c in calls:
      if c.entryStep >= n: past.add((c.entryStep, k))
    past.sort()
    for (_, k) in past: result.add("call_entry:" & $k)
  for i, e in ios:
    if e.stepId >= n: result.add("io:" & $i)

proc documentOrder(doc: JsonNode): seq[string] =
  ## The events of `doc` named as `referenceOrder` names them.
  for ev in doc["events"].elems:
    case ev["kind"].getStr()
    of "call_entry", "call_exit":
      result.add(ev["kind"].getStr() & ":" & $ev["call_key"].getBiggestInt())
    of "io":
      result.add("io:" & $ev["io_index"].getBiggestInt())
    else:
      doAssert ev.hasKey("step_index"), "an exec record's event without " &
        "step_index: " & $ev
      result.add("rec:" & $ev["step_index"].getBiggestInt())

proc checkOrder(path: string, doc: JsonNode, what: string) =
  let want = referenceOrder(path)
  let got = documentOrder(doc)
  var i = 0
  while i < want.len and i < got.len and want[i] == got[i]: inc i
  doAssert want.len > 0 and got == want,
    what & ": the event order is not the specified one; first difference at " &
    "event " & $i & " (" & $got.len & " events, " & $want.len & " expected): " &
    "got " & $got[i ..< min(got.len, i + 6)] & ", expected " &
    $want[i ..< min(want.len, i + 6)]

proc kinds(doc: JsonNode): seq[string] =
  for ev in doc["events"].elems:
    result.add(ev["kind"].getStr())

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

proc test_output_is_byte_identical_to_the_in_process_rendering(dir: string) =
  let bundle = dir / "small.ct"
  discard writePythonShaped(bundle, smallCalls, smallStepsPerCall, chunkSize = 16)

  for strip in [false, true]:
    let opts = FullOpts(stripPaths: strip)
    let doc = inProcessDocument(bundle, opts)
    # Non-degeneracy: the shapes the assembler orders specially are present,
    # or the comparison says nothing about them.
    let ks = kinds(doc)
    doAssert "raise" in ks and "catch" in ks and "io" in ks, $ks
    var synthesized, ioPast, sharedExitSteps = 0
    var exitSteps: seq[int64]
    for ev in doc["events"].elems:
      case ev["kind"].getStr()
      of "call_entry":
        if ev.hasKey("synthesized_at_step"): inc synthesized
        if ev["entry_step"].getBiggestInt() == ev["exit_step"].getBiggestInt():
          inc sharedExitSteps
      of "call_exit":
        if ev["exit_step"].getBiggestInt() in exitSteps: inc sharedExitSteps
        exitSteps.add(ev["exit_step"].getBiggestInt())
      of "io":
        if ev["step_id"].getBiggestInt() == bufferedStepId: inc ioPast
      else: discard
    doAssert synthesized == 1 and ioPast == 1 and sharedExitSteps > 0,
      "fixture lost a shape: synthesized=" & $synthesized & " ioPast=" &
      $ioPast & " shared=" & $sharedExitSteps

    var args = @["--full"]
    if strip: args.add("--strip-paths")
    checkBinaryMatches(dir, bundle, args, expectedFull(doc), "the small container")
    args[0] = "--events"
    checkBinaryMatches(dir, bundle, args, expectedEvents(doc), "the small container")
    if not strip:
      doAssert expectedMarkers(doc).contains("boundary_id"),
        "the fixture's correlation marker is missing"
      checkBinaryMatches(dir, bundle, ["--markers", "--json-out"],
        expectedMarkers(doc), "the small container")
      checkAttribution(bundle, doc)
    checkOrder(bundle, doc, "the small container")
  echo "PASS: --full, --events and --markers are byte-identical to the " &
    "in-process rendering"

proc test_a_container_without_steps_prints_its_io(dir: string) =
  let bundle = dir / "nosteps.ct"
  writeNoSteps(bundle)
  let doc = inProcessDocument(bundle, FullOpts())
  doAssert kinds(doc) == @["io", "io"],
    "every IO event of a container without steps must be printed: " &
    $kinds(doc)
  checkOrder(bundle, doc, "a container without steps")
  checkBinaryMatches(dir, bundle, ["--full"], expectedFull(doc),
    "a container without steps")
  checkBinaryMatches(dir, bundle, ["--events"], expectedEvents(doc),
    "a container without steps")
  echo "PASS: a container without steps prints its IO events, byte-identically"

proc countKinds(path: string): tuple[steps, entries, exits: int] =
  ## Count the events of a `--full` output by its pretty-printed `kind` lines,
  ## reading the file line by line rather than parsing it whole.
  for line in lines(path):
    case line
    of "      \"kind\": \"step\",": inc result.steps
    of "      \"kind\": \"call_entry\",": inc result.entries
    of "      \"kind\": \"call_exit\",": inc result.exits
    else: discard

proc rssBound(containerBytes: int64): int64 =
  ## What a streaming ct-print may hold, as a function of the container:
  ##
  ## * the container image, read whole — twice at most (the layout probe in
  ##   `main` and the reader), so 2x;
  ## * one 8-byte position per exec record (`precomputeStepGlis`), and the
  ##   call records and IO events kept for ordering, all bounded by the
  ##   decompressed streams, which compress ~4-8x here: allow 8x more;
  ## * the reader's bounded chunk caches (8 MiB each) and the runtime: 96 MiB.
  ##
  ## 96 MiB + 10x the container. The unfixed binary exceeds it by an order of
  ## magnitude long before it finishes.
  96 * 1024 * 1024 + 10 * containerBytes

proc checkStreamed(dir, bundle: string, args: openArray[string],
    bound: int64, outputPerRss: int, what: string): Measured =
  ## Run ct-print ``args`` on ``bundle`` and require that it finished, within
  ## the memory ``bound``, with a peak RSS under ``1/outputPerRss`` of what it
  ## wrote — the property no implementation that holds its output, or the
  ## tree it renders it from, can have. Leaves the output at
  ## ``dir / "streamed.out"`` for the caller to inspect.
  let outPath = dir / "streamed.out"
  let errPath = dir / "streamed.err"
  var full = @args
  full.add(bundle)
  let m = runCtPrintMeasured(full, outPath, errPath, memoryCap = 2 * bound)
  let label = "ct-print " & args.join(" ")
  echo "  " & label & " on " & what & ": " & describe(m)
  doAssert not m.overMemoryCap,
    label & " passed twice the memory bound (" & mib(2 * bound) & ") on " &
    what & " and was killed: " & describe(m)
  doAssert not m.timedOut,
    label & " did not finish within " & $runLimit.inSeconds & " s on " &
    what & ": " & describe(m)
  doAssert m.exitCode == 0,
    label & " failed: " & describe(m) & "\nstderr:\n" & readFile(errPath)
  doAssert m.maxRssBytes * outputPerRss < m.stdoutBytes,
    label & " held its output instead of streaming it: peak RSS " &
    mib(m.maxRssBytes) & " is not under 1/" & $outputPerRss & " of the " &
    mib(m.stdoutBytes) & " it wrote"
  doAssert m.maxRssBytes < bound,
    label & " used " & mib(m.maxRssBytes) & " on " & what &
    "; the bound is " & mib(bound)
  m

proc test_a_large_trace_streams_in_bounded_memory(dir: string) =
  let bundle = dir / "large.ct"
  let expected = writePythonShaped(bundle, bigCalls, bigStepsPerCall,
    chunkSize = 4096)
  let containerBytes = int64(getFileSize(bundle))
  let bound = rssBound(containerBytes)
  let what = "a " & mib(containerBytes) & " container (" & $expected.steps &
    " steps, " & $expected.entries & " calls)"

  discard checkStreamed(dir, bundle, ["--full"], bound, 4, what)
  let got = countKinds(dir / "streamed.out")
  doAssert got.steps == expected.steps and got.entries == expected.entries and
      got.exits == expected.entries,
    "the streamed document is incomplete: " & $got & ", expected " & $expected
  # The legacy modes hold no values' content but one entry per value, and
  # `--json-events` decodes every value: each is as large as `--events`.
  for args in [@["--events"], @["--json"], @["--json-events"]]:
    discard checkStreamed(dir, bundle, args, bound, 4, what)
  removeFile(dir / "streamed.out")
  echo "PASS: a Python-shaped trace streams in memory proportional to the trace"

proc test_legacy_json_modes_keep_their_layout(dir: string) =
  ## `--json` and `--json-events` have no in-process builder to compare
  ## with: their documents only ever existed inside the CLI. What the
  ## streamed text can be held to is that it IS `pretty`'s layout of the
  ## document it spells (`pretty(parseJson(text))` gives it back, so no
  ## indentation, separator or bracket is off), and that the document is
  ## the right one: the declarations, steps, calls, values and IO the
  ## fixture holds. Byte-identity with the pre-streaming binary was checked
  ## by hand on eleven containers; see this file's header.
  let bundle = dir / "small.ct"
  discard writePythonShaped(bundle, smallCalls, smallStepsPerCall, chunkSize = 16)
  let steps = inProcessDocument(bundle, FullOpts())["counts"]["steps"].getInt()
  var reader = openNewTrace(bundle).get()
  let execRecords = int(reader.stepCount().get())  # steps plus raise/catch
  for args in [@["--json"], @["--json-events"]]:
    let outPath = dir / "legacy.out"
    let errPath = dir / "legacy.err"
    var full = args
    full.add(bundle)
    let m = runCtPrintMeasured(full, outPath, errPath)
    doAssert m.exitCode == 0, "ct-print " & args[0] & ": " & describe(m) &
      "\n" & readFile(errPath)
    let text = readFile(outPath)
    let doc = parseJson(text)
    doAssert pretty(doc) & "\n" == text,
      "ct-print " & args[0] & " is not pretty's layout of its own document: " &
      firstDifference(text, pretty(doc) & "\n")
    if args[0] == "--json":
      doAssert doc["calls"].len == smallCalls * 5 div 4 + 3 and
          doc["values"].len == 3 * steps and doc["ioEvents"].len == 4,
        "--json is missing records: " & $doc["calls"].len & " calls, " &
        $doc["values"].len & " values, " & $doc["ioEvents"].len & " IO"
    else:
      var kinds: seq[string]
      for ev in doc.elems:
        kinds.add(ev["type"].getStr())
      # `--json-events` writes `type: "step"` for every exec record.
      doAssert kinds.count("step") == execRecords and "call" in kinds and
          "value" in kinds and "io" in kinds and "path" in kinds,
        "--json-events is missing records: " & $kinds.count("step") &
        " step entries for " & $execRecords & " exec records, in " &
        $kinds.len & " events"
  echo "PASS: --json and --json-events keep pretty's layout"

# ---------------------------------------------------------------------------
# Native MCR bundles
# ---------------------------------------------------------------------------

proc writeNativeBundle(path: string, eventsPerThread: int) =
  ## A native MCR bundle (see `test_ct_print_native.nim`): two threads with
  ## interleaved GEIDs and a mix of printable and binary payloads, stored
  ## uncompressed (which the decoder accepts), and an OS event log of two
  ## writes. Built in memory with the CTFS writer, then written out.
  var ctfs = createCtfs()
  let meta = writeMetaDatToBuffer(
    TraceMetadata(recordingId: "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
      workdir: "/synthetic", program: "/synthetic/program",
      args: @["--demo", "/synthetic/input"]),
    mcrFields = some(McrMetaFields(tickSource: tsMonotonic, totalThreads: 2,
      atomicMode: amRelaxed, totalEvents: uint64(2 * eventsPerThread),
      platform: "linux", tickGranularity: "edge", tickSourceStr: "none",
      atomicModeStr: "relaxed", hookProfile: "test",
      hookStrategies: @["ldpreload"])))
  var metaFile = ctfs.addFile("meta.dat").get()
  doAssert ctfs.writeToFile(metaFile, meta).isOk
  var tables = initTraceInterningTables(ctfs).get()
  discard ctfs.ensurePathId(tables, "/synthetic/src/main.c").get()
  for tid in 0'u32 .. 1'u32:
    var stream: seq[byte]
    for i in 0 ..< eventsPerThread:
      var payload: seq[byte]
      if i mod 3 == 0:
        payload = toBytes("v=" & $i & "\n")
      else:
        for k in 0 ..< 6: payload.add(byte((i * 7 + k * 31) and 0xFF))
      let geid = uint64(2 * i) + uint64(tid)
      stream.add encodeEventHeader(uint16(i mod 21), uint32(26 + payload.len),
        tid, geid, geid * 10)
      stream.add payload
    var f = ctfs.addFile(threadFileName(tid)).get()
    doAssert ctfs.writeToFile(f, stream).isOk
  let e1 = encodeOsEntry(geid = 3, tick = 30, tid = 1, kind = 0, fd = 1,
    returnValue = 3, metadata = "stdout", content = toBytes("hi\n"))
  let e2 = encodeOsEntry(geid = 5, tick = 50, tid = 0, kind = 0, fd = 2,
    returnValue = 2, metadata = "stderr", content = @[byte 0, 0xFF])
  var dat = ctfs.addFile(EventLogDat).get()
  doAssert ctfs.writeToFile(dat, encodeOsEventLogChunk(@[e1, e2])).isOk
  var idx = ctfs.addFile(EventLogIdx).get()
  doAssert ctfs.writeToFile(idx, encodeOsEventLogIndex(entryCount = 2,
    chunkOffsets = [0'u64], chunkGeids = [3'u64])).isOk
  let bytes = ctfs.toBytes()
  discard ctfs.closeCtfs()
  var s = newString(bytes.len)
  if bytes.len > 0:
    copyMem(addr s[0], unsafeAddr bytes[0], bytes.len)
  writeFile(path, s)

proc test_native_output_is_byte_identical(dir: string) =
  let bundle = dir / "native.ct"
  writeNativeBundle(bundle, 50)
  let data = readFile(bundle)
  var bytes = newSeq[byte](data.len)
  for i, c in data: bytes[i] = byte(c)
  for strip in [false, true]:
    let doc = buildNativeFullDocument(bytes, NativeOpts(stripPaths: strip)).get()
    doAssert doc["events"].len == 102 and
        doc["metadata"]["recorder"].getStr() == "native-mcr",
      "the native bundle did not decode as the fixture: " &
      $doc["events"].len & " events"
    var args = @["--full"]
    if strip: args.add("--strip-paths")
    checkBinaryMatches(dir, bundle, args, expectedFull(doc), "a native bundle")
    args[0] = "--events"
    checkBinaryMatches(dir, bundle, args, expectedEvents(doc), "a native bundle")
  echo "PASS: native --full and --events are byte-identical to the in-process rendering"

proc test_a_large_native_bundle_streams(dir: string) =
  ## The native decoder holds the decoded records (bounded by the container)
  ## but must not hold the tree or its rendering. Per event the container
  ## carries ~33 bytes, the held record ~100, and the `--full` output ~295,
  ## so the output-to-RSS ratio asserted for `--full` is 1 rather than the
  ## split-stream modes' 4: the unstreamed decoder's rendered string ALONE
  ## is the output's size, and its tree several times more. `--events`
  ## writes ~180 bytes per event, too close to what the records take for a
  ## ratio to be a portable assertion, so it is held to the absolute bound
  ## only. Measured on the shared macOS arm64 host: streamed, 42 MiB for
  ## both modes (84.5 and 52.4 MiB written); the unstreamed decoder at
  ## `bbe8b15`, 852 MiB for `--full` and 576 MiB for `--events`, against a
  ## bound of 190 MiB.
  let bundle = dir / "native-large.ct"
  writeNativeBundle(bundle, nativeEventsPerThread)
  let containerBytes = int64(getFileSize(bundle))
  let what = "a " & mib(containerBytes) & " native bundle (" &
    $(2 * nativeEventsPerThread) & " events)"
  discard checkStreamed(dir, bundle, ["--full"], rssBound(containerBytes), 1,
    what)
  discard checkStreamed(dir, bundle, ["--events"], rssBound(containerBytes), 0,
    what)
  removeFile(dir / "streamed.out")
  echo "PASS: a large native bundle streams in memory proportional to it"

let dir = createTempDir("ct_print_full_streams_", "")
try:
  ensureCtPrint()
  test_output_is_byte_identical_to_the_in_process_rendering(dir)
  test_a_container_without_steps_prints_its_io(dir)
  test_legacy_json_modes_keep_their_layout(dir)
  test_native_output_is_byte_identical(dir)
  test_a_large_trace_streams_in_bounded_memory(dir)
  test_a_large_native_bundle_streams(dir)
finally:
  removeDir(dir)

echo "ALL PASS: test_ct_print_full_streams"
