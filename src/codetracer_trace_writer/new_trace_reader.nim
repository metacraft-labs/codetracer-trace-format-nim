{.push raises: [].}

## Seek-based trace reader (M18 + M19).
##
## Opens a multi-stream CTFS trace and provides random access to all data.
## Interning tables are loaded eagerly at startup; execution, value, call,
## and IO-event streams are initialized lazily on first access.

import results
import std/os
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ./meta_dat
import ./interning_table
import ./exec_stream
import ./value_stream
import ./call_stream
import ./io_event_stream
import ./step_encoding

type
  NewTraceReader* = object
    data: seq[byte]            ## raw .ct file bytes (mmap later)
    blockSize: uint32
    maxEntries: uint32

    # Metadata
    meta*: MetaDatContents

    # Interning tables (loaded at startup)
    pathReader: InterningTableReader
    funcReader: InterningTableReader
    typeReader: InterningTableReader
    varnameReader: InterningTableReader

    # Stream readers (lazy, loaded on first access)
    execReader: ExecStreamReader
    valueReader: ValueStreamReader
    callReader: CallStreamReader
    ioEventReader: IOEventStreamReader

    # Flags for lazy initialization
    execLoaded: bool
    valueLoaded: bool
    callLoaded: bool
    ioEventLoaded: bool

# ---------------------------------------------------------------------------
# Opening
# ---------------------------------------------------------------------------

proc openNewTraceFromBytes*(data: seq[byte],
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): Result[NewTraceReader, string] =
  ## Open a trace from in-memory bytes. Used for testing.

  var reader: NewTraceReader
  reader.data = data
  reader.blockSize = blockSize
  reader.maxEntries = maxEntries

  # Read meta.dat
  let metaDataRes = readInternalFile(data, "meta.dat", blockSize, maxEntries)
  if metaDataRes.isOk:
    let metaRes = readMetaDat(metaDataRes.get())
    if metaRes.isOk:
      reader.meta = metaRes.get()

  # Load interning tables (these are small, load at startup)
  let pathRes = initInterningTableReader(data, "paths", blockSize, maxEntries)
  if pathRes.isOk: reader.pathReader = pathRes.get()

  let funcRes = initInterningTableReader(data, "funcs", blockSize, maxEntries)
  if funcRes.isOk: reader.funcReader = funcRes.get()

  let typeRes = initInterningTableReader(data, "types", blockSize, maxEntries)
  if typeRes.isOk: reader.typeReader = typeRes.get()

  let vnRes = initInterningTableReader(data, "varnames", blockSize, maxEntries)
  if vnRes.isOk: reader.varnameReader = vnRes.get()

  ok(reader)

proc openNewTrace*(path: string): Result[NewTraceReader, string] =
  ## Open a multi-stream trace file from disk.
  ## Loads meta.dat and interning tables at startup.
  ## All other streams are loaded lazily on first access.

  if not fileExists(path):
    return err("file not found: " & path)

  var data: seq[byte]
  try:
    let f = open(path, fmRead)
    let size = f.getFileSize()
    data = newSeq[byte](size)
    discard f.readBytes(data, 0, size)
    f.close()
  except:
    return err("failed to read file: " & path)

  openNewTraceFromBytes(data)

# ---------------------------------------------------------------------------
# Interning table accessors
# ---------------------------------------------------------------------------

proc path*(r: NewTraceReader, id: uint64): Result[string, string] =
  r.pathReader.readById(id)

proc function*(r: NewTraceReader, id: uint64): Result[string, string] =
  r.funcReader.readById(id)

proc typeName*(r: NewTraceReader, id: uint64): Result[string, string] =
  r.typeReader.readById(id)

proc varname*(r: NewTraceReader, id: uint64): Result[string, string] =
  r.varnameReader.readById(id)

proc pathCount*(r: NewTraceReader): uint64 = r.pathReader.count()
proc functionCount*(r: NewTraceReader): uint64 = r.funcReader.count()
proc typeCount*(r: NewTraceReader): uint64 = r.typeReader.count()
proc varnameCount*(r: NewTraceReader): uint64 = r.varnameReader.count()

# ---------------------------------------------------------------------------
# Step access (lazy init exec reader)
# ---------------------------------------------------------------------------

proc ensureExecReader(r: var NewTraceReader): Result[void, string] =
  if not r.execLoaded:
    let res = initExecStreamReader(r.data, int(r.blockSize), int(r.maxEntries))
    if res.isErr: return err(res.error)
    r.execReader = res.get()
    r.execLoaded = true
  ok()

proc step*(r: var NewTraceReader, n: uint64): Result[StepEvent, string] =
  ?r.ensureExecReader()
  r.execReader.readEvent(n)

proc stepAbsoluteGlobalLineIndex*(r: var NewTraceReader,
    n: uint64): Result[uint64, string] =
  ## Return the absolute global line index for step N.
  ##
  ## The exec stream stores steps as a mix of AbsoluteStep and DeltaStep
  ## events. Each chunk starts with an AbsoluteStep, and subsequent events
  ## may be DeltaStep (relative to the previous). This method scans from
  ## the start of the chunk containing step N, accumulating deltas, to
  ## produce the absolute global line index.
  ?r.ensureExecReader()

  let chunkSize = uint64(r.execReader.chunkSize)
  let chunkStart = (n div chunkSize) * chunkSize
  var currentGli: uint64 = 0

  for i in chunkStart .. n:
    let ev = ?r.execReader.readEvent(i)
    case ev.kind
    of sekAbsoluteStep:
      currentGli = ev.globalLineIndex
    of sekDeltaStep:
      currentGli = uint64(int64(currentGli) + ev.lineDelta)
    else:
      # Non-step events (raise, catch, thread_switch) don't change GLI
      discard

  ok(currentGli)

proc stepCount*(r: var NewTraceReader): Result[uint64, string] =
  ?r.ensureExecReader()
  ok(r.execReader.totalEvents)

# ---------------------------------------------------------------------------
# Value access (lazy init)
# ---------------------------------------------------------------------------

proc ensureValueReader(r: var NewTraceReader): Result[void, string] =
  if not r.valueLoaded:
    let res = initValueStreamReader(r.data, r.blockSize, r.maxEntries)
    if res.isErr: return err(res.error)
    r.valueReader = res.get()
    r.valueLoaded = true
  ok()

proc values*(r: var NewTraceReader, n: uint64): Result[seq[VariableValue], string] =
  ?r.ensureValueReader()
  r.valueReader.readStepValues(n)

iterator valuesIter*(r: var NewTraceReader, n: uint64): VariableValue =
  ## Yields variable values one at a time for a given step.
  let vals = r.values(n)
  if vals.isOk:
    for v in vals.get():
      yield v

proc values*(r: var NewTraceReader, n: uint64, output: var openArray[VariableValue]): int =
  ## Fill output buffer with values for step n. Returns the number of values written.
  let vals = r.values(n)
  if vals.isErr: return 0
  let vs = vals.get()
  let count = min(vs.len, output.len)
  for i in 0 ..< count:
    output[i] = vs[i]
  count

proc valueCount*(r: var NewTraceReader): Result[uint64, string] =
  ?r.ensureValueReader()
  ok(r.valueReader.count())

# ---------------------------------------------------------------------------
# Call access (lazy init)
# ---------------------------------------------------------------------------

proc ensureCallReader(r: var NewTraceReader): Result[void, string] =
  if not r.callLoaded:
    let res = initCallStreamReader(r.data, r.blockSize, r.maxEntries)
    if res.isErr: return err(res.error)
    r.callReader = res.get()
    r.callLoaded = true
  ok()

proc call*(r: var NewTraceReader, callKey: uint64): Result[CallRecord, string] =
  ?r.ensureCallReader()
  r.callReader.readCall(callKey)

proc callCount*(r: var NewTraceReader): Result[uint64, string] =
  ?r.ensureCallReader()
  ok(r.callReader.count())

proc callForStep*(r: var NewTraceReader, stepId: uint64): Result[CallRecord, string] =
  ## Find the innermost enclosing call record for the given step using
  ## proportional (interpolation) search on call records sorted by entryStep.
  ## Call records store [entryStep, exitStep] ranges. The search exploits
  ## the approximate uniform distribution of step IDs across calls,
  ## giving O(log log C) convergence -- typically 2-3 iterations.
  ?r.ensureCallReader()
  let totalCalls = r.callReader.count()
  if totalCalls == 0:
    return err("no call records")

  var lo: uint64 = 0
  var hi: uint64 = totalCalls - 1

  # Best match: deepest (most nested) call containing stepId
  var bestCall: CallRecord
  var bestFound = false

  for iteration in 0 ..< 20:  # safety bound
    if lo > hi:
      break

    # Read boundary calls
    let loCall = ?r.callReader.readCall(lo)
    let hiCall = ?r.callReader.readCall(hi)

    if stepId < loCall.entryStep:
      break
    if stepId > hiCall.exitStep:
      break

    # Check if lo contains our step
    if stepId >= loCall.entryStep and stepId <= loCall.exitStep:
      if not bestFound or loCall.depth > bestCall.depth:
        bestCall = loCall
        bestFound = true
      # Narrow from the left to find deeper calls
      lo = lo + 1
      if lo > hi: break
      continue

    # Check if hi contains our step
    if lo != hi and stepId >= hiCall.entryStep and stepId <= hiCall.exitStep:
      if not bestFound or hiCall.depth > bestCall.depth:
        bestCall = hiCall
        bestFound = true
      hi = hi - 1
      if lo > hi: break
      continue

    if lo == hi:
      break

    # Interpolate position based on entryStep distribution
    let rangeSteps = hiCall.entryStep - loCall.entryStep
    if rangeSteps == 0:
      break
    let offset = stepId - loCall.entryStep
    let estimate = lo + (hi - lo) * offset div rangeSteps
    let mid = max(lo + 1, min(estimate, hi - 1))

    let midCall = ?r.callReader.readCall(mid)
    if stepId >= midCall.entryStep and stepId <= midCall.exitStep:
      if not bestFound or midCall.depth > bestCall.depth:
        bestCall = midCall
        bestFound = true
      # Continue searching for deeper calls
      lo = mid + 1
      continue
    elif stepId < midCall.entryStep:
      hi = mid - 1
    else:
      lo = mid + 1

  if bestFound:
    ok(bestCall)
  else:
    err("step " & $stepId & " not found in any call")

iterator callRange*(r: var NewTraceReader, start, count: uint64): CallRecord =
  ## Yields call records in [start, start+count).
  let _ = r.ensureCallReader()
  for i in start ..< start + count:
    let res = r.callReader.readCall(i)
    if res.isOk:
      yield res.get()

proc callRange*(r: var NewTraceReader, start, count: uint64,
                output: var openArray[CallRecord]): int =
  ## Fill output buffer with call records starting at `start`.
  ## Returns the number of records written.
  let _ = r.ensureCallReader()
  var written = 0
  for i in start ..< start + count:
    if written >= output.len: break
    let res = r.callReader.readCall(i)
    if res.isOk:
      output[written] = res.get()
      written += 1
  written

# ---------------------------------------------------------------------------
# IO event access (lazy init)
# ---------------------------------------------------------------------------

proc ensureIOEventReader(r: var NewTraceReader): Result[void, string] =
  if not r.ioEventLoaded:
    let res = initIOEventStreamReader(r.data, r.blockSize, r.maxEntries)
    if res.isErr: return err(res.error)
    r.ioEventReader = res.get()
    r.ioEventLoaded = true
  ok()

proc ioEvent*(r: var NewTraceReader, index: uint64): Result[IOEvent, string] =
  ?r.ensureIOEventReader()
  r.ioEventReader.readEvent(index)

proc ioEventCount*(r: var NewTraceReader): Result[uint64, string] =
  ?r.ensureIOEventReader()
  ok(r.ioEventReader.count())

iterator events*(r: var NewTraceReader, start, count: uint64): IOEvent =
  ## Yields IO events in [start, start+count).
  let _ = r.ensureIOEventReader()
  for i in start ..< start + count:
    let res = r.ioEventReader.readEvent(i)
    if res.isOk:
      yield res.get()

proc events*(
    r: var NewTraceReader, start, count: uint64,
    output: var openArray[IOEvent]): int =
  ## Fill output buffer with IO events starting at `start`.
  ## Returns the number of events written.
  let _ = r.ensureIOEventReader()
  var written = 0
  for i in start ..< start + count:
    if written >= output.len: break
    let res = r.ioEventReader.readEvent(i)
    if res.isOk:
      output[written] = res.get()
      written += 1
  written
