## The C ABI's in-memory constructors: a writer an embedder with no filesystem
## can reach.
##
## Every constructor on `codetracer_trace_writer_ffi`'s C ABI was file-based.
## `trace_writer_begin_events` takes an events PATH, derives a `.ct` path from
## its parent directory, and `trace_writer_close` opens that path and writes to
## it — so a wasm module, or any host that wants the container's bytes rather
## than a file, could not get in at all. The layer below was already in memory
## (`initMultiStreamWriter` builds on `createCtfs()`, and
## `newTraceWriterInMemory` is its single-stream sibling); what was missing was
## the way to reach it from C and the way to get the bytes back.
##
## What is asserted here, and why each one can fail:
##
##   1. `trace_writer_container_ready` DISCRIMINATES: 0 before close, 1 after.
##      An empty container is a legitimate result — a recording with no events
##      still has a `meta.dat` — so a zero LENGTH cannot stand in for "not
##      finished". A flag that were constant would fail the first half.
##   2. The bytes carry the CTFS magic. A constructor that returned a plausible
##      length over an empty buffer passes a length check and fails this.
##   3. **The in-memory path creates NO FILE**, asserted over a directory this
##      test owns and enumerates. Paired with a POSITIVE CONTROL in the same
##      directory: the file path, driven with the same events, DOES create one.
##      Without the control, "no file appeared" is equally true of a directory
##      nothing ever wrote to.
##   4. The two paths carry the SAME TRACE, compared over the DECODED step
##      stream read back through this repository's own reader.
##      **Not over the container's length.** The first draft of this test
##      asserted length equality and the mutation control below refuted it:
##      CTFS is block-structured, so one extra step is absorbed into the blocks
##      already allocated and the length does not move. A length comparison
##      would therefore have passed for two demonstrably different traces,
##      which is this campaign's "an assertion that cannot fail" wearing an
##      equality. The lengths are REPORTED, and the fact that they agree is a
##      note rather than evidence.
##   5. Its own MUTATION CONTROL: an in-memory writer given one extra step
##      produces a step stream the comparison REJECTS. Without it, assertion 4
##      is an equality nobody has seen fail — and it is what showed the length
##      comparison was one.
##   6. Choosing one mode and then the other is REFUSED by name, in both
##      orders. Both `begin`s are idempotent no-ops on an already-open writer,
##      so a caller that made both would otherwise silently get whichever it
##      made first.

# Include the FFI module so the C entry points can be driven directly.
# Mirrors tests/test_io_event_pending_step_attribution.nim.
include codetracer_trace_writer_ffi

# Drop the `raises: []` push from the FFI module so the test body can use
# higher-level helpers.
{.pop.}

import std/[algorithm, strutils]

const
  Program = "in_memory_probe"
  AppPath = "/srv/probe.js"
  Lines = @[4'i64, 5'i64, 6'i64]
  ExtraLine = 7'i64

proc drive(h: TraceWriterHandle, lines: openArray[int64]) =
  ## The same event sequence for every arm, so the arms differ in one thing.
  var lineLengths = newSeq[uint32](32)
  for i in 0 ..< lineLengths.len:
    lineLengths[i] = 40'u32
  discard trace_writer_register_path_with_line_lengths(
    h, cstring(AppPath), cint(lineLengths.len),
    cast[ptr UncheckedArray[uint32]](addr lineLengths[0]))
  trace_writer_start(h, cstring(AppPath), lines[0])
  for ln in lines:
    trace_writer_register_step(h, cstring(AppPath), ln)

proc containerOf(h: TraceWriterHandle): string =
  ## The finished bytes, copied out of the handle.
  let n = int(trace_writer_container_len(h))
  if n == 0:
    return ""
  result = newString(n)
  copyMem(addr result[0], trace_writer_container_ptr(h), n)

proc filesIn(dir: string): seq[string] =
  for kind, p in walkDir(dir):
    if kind == pcFile:
      result.add extractFilename(p)
  result.sort()

proc stepLines(path: string): seq[uint64] =
  let r = ct_reader_open(cstring(path))
  doAssert r != nil, "ct_reader_open(" & path & ") failed: " &
    $trace_writer_last_error()
  for s in 0'u64 ..< ct_reader_step_count(r):
    var pathIds = [0'u64]
    var lines = [0'u64]
    var cols = [0'u64]
    let written = ct_reader_step_locations_with_columns(
      r, s, 1'u64, addr pathIds[0], addr lines[0], addr cols[0])
    doAssert written == 1'u64,
      "step_locations(" & $s & ") failed: " & $trace_writer_last_error()
    result.add lines[0]
  ct_reader_close(r)

proc inMemoryContainer(lines: openArray[int64], dir: string): string =
  ## Build a container without touching the filesystem. `dir` is passed only so
  ## the caller can assert nothing appeared in it; nothing here writes to it.
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert h != nil, "trace_writer_new failed: " & $trace_writer_last_error()
  doAssert trace_writer_begin_in_memory(h) == 0,
    "begin_in_memory failed: " & $trace_writer_last_error()
  doAssert trace_writer_container_ready(h) == 0,
    "the container reported itself ready BEFORE close, so the flag is constant"
  drive(h, lines)
  doAssert trace_writer_close(h) == 0,
    "close failed: " & $trace_writer_last_error()
  doAssert trace_writer_container_ready(h) == 1,
    "the container did not report itself ready after close"
  result = containerOf(h)
  trace_writer_free(h)

proc fileContainer(lines: openArray[int64], dir: string): string =
  createDir(dir)
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert h != nil, "trace_writer_new failed: " & $trace_writer_last_error()
  doAssert trace_writer_begin_events(h, cstring(dir / "events.bin")) == 0,
    "begin_events failed: " & $trace_writer_last_error()
  drive(h, lines)
  doAssert trace_writer_close(h) == 0,
    "close failed: " & $trace_writer_last_error()
  trace_writer_free(h)
  result = readFile(dir / (Program & ".ct"))

# ---------------------------------------------------------------------------

let root = getTempDir() / "ct_ffi_in_memory"
removeDir(root)
createDir(root)

let memDir = root / "memory-arm"
createDir(memDir)
let inMem = inMemoryContainer(Lines, memDir)

# 3. THE IN-MEMORY PATH TOUCHED NOTHING, and the control says the directory is
#    one a container WOULD have appeared in.
doAssert filesIn(memDir).len == 0,
  "the in-memory arm wrote " & $filesIn(memDir) & " into a directory it was " &
  "given only so this assertion could be made"

let fileDir = root / "file-arm"
let onDisk = fileContainer(Lines, fileDir)
doAssert (Program & ".ct") in filesIn(fileDir),
  "the POSITIVE CONTROL failed: the file arm produced no .ct in " & fileDir &
  ", so 'no file appeared' above is not evidence about the in-memory arm"

# 2. The bytes are a container and not a plausible length over nothing.
doAssert inMem.len > 0, "the in-memory container is empty"
doAssert inMem.len >= CtfsMagic.len, "the in-memory container is shorter than the magic"
for i, b in CtfsMagic:
  doAssert byte(inMem[i]) == b,
    "byte " & $i & " of the in-memory container is " & $byte(inMem[i]) &
    ", not the CTFS magic's " & $b

# 4. The two arms carry the same trace. Compared over the DECODED steps; see
#    the header for why the container's LENGTH is not the comparison to make.
let memPath = root / "in-memory-readback.ct"
writeFile(memPath, inMem)
let memLines = stepLines(memPath)
let diskLines = stepLines(fileDir / (Program & ".ct"))
# `trace_writer_start` emits a step of its own at the entry line, so a drive of
# N `register_step` calls records N + 1 steps. The expectation says so rather
# than being a bare number nobody can check against the driver.
doAssert memLines.len == Lines.len + 1,
  "the in-memory container has " & $memLines.len & " steps; the driver makes " &
  $Lines.len & " register_step calls after a start, so " & $(Lines.len + 1) &
  " was expected"
doAssert memLines == diskLines,
  "the two arms disagree on the step lines: in-memory " & $memLines &
  ", on disk " & $diskLines

# 5. MUTATION CONTROL. One extra step must make both comparisons fail, so the
#    two above are measurements rather than equalities nobody has seen move.
let mutantDir = root / "mutant-arm"
createDir(mutantDir)
let mutant = inMemoryContainer(Lines & ExtraLine, mutantDir)
let mutantPath = root / "mutant-readback.ct"
writeFile(mutantPath, mutant)
let mutantLines = stepLines(mutantPath)
doAssert mutantLines.len == memLines.len + 1,
  "the mutant arm recorded " & $mutantLines.len & " steps against the subject's " &
  $memLines.len & " — one extra step did not arrive, so the arm did not apply"
doAssert mutantLines != memLines,
  "one extra step did not change the step lines, so the comparison above " &
  "cannot tell two different traces apart"
# THE LENGTH IS RECORDED AS A MEASUREMENT, NOT ASSERTED. One extra step leaves
# it unchanged — CTFS allocates in blocks and the record fits in the ones
# already there — which is exactly why the comparison above is over the decoded
# steps. If a future change makes the length move, this note is what says the
# old reasoning has to be re-taken rather than the assertion silently becoming
# strong.
echo "test_ffi_in_memory: NOTE container lengths — in-memory ", inMem.len,
     ", on disk ", onDisk.len, ", mutant (one extra step) ", mutant.len,
     "; the mutant's equality with the subject is why length is not asserted"

# 6. The two modes are mutually exclusive, and saying so is a refusal rather
#    than a silent no-op, in BOTH orders.
block:
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert trace_writer_begin_in_memory(h) == 0
  doAssert trace_writer_begin_events(h, cstring(root / "events.bin")) != 0,
    "begin_events on an in-memory writer was accepted"
  doAssert "already open in memory" in $trace_writer_last_error(),
    "the refusal did not name the mode: " & $trace_writer_last_error()
  trace_writer_free(h)

block:
  let d = root / "order-arm"
  createDir(d)
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert trace_writer_begin_events(h, cstring(d / "events.bin")) == 0
  doAssert trace_writer_begin_in_memory(h) != 0,
    "begin_in_memory on a file-backed writer was accepted"
  doAssert "already open on a file" in $trace_writer_last_error(),
    "the refusal did not name the cause: " & $trace_writer_last_error()
  trace_writer_free(h)

# 1b. A writer that was never begun reports no container rather than a stale one.
block:
  let h = trace_writer_new(cstring(Program), ffiBinary)
  doAssert trace_writer_container_ready(h) == 0
  doAssert trace_writer_container_len(h) == 0.csize_t
  doAssert trace_writer_container_ptr(h) == nil
  doAssert trace_writer_close(h) == 0
  doAssert trace_writer_container_ready(h) == 0,
    "a writer that was never begun reported a container after close"
  trace_writer_free(h)

removeDir(root)
echo "test_ffi_in_memory: OK — in-memory container ", inMem.len,
     " bytes, ", memLines.len, " steps, no file written; ",
     "file arm ", onDisk.len, " bytes, ", diskLines.len, " steps; ",
     "mutant ", mutantLines.len, " steps"
