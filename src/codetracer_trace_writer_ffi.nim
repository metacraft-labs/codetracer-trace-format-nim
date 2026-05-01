{.push raises: [].}

## C FFI interface for the CodeTracer trace writer.
## Drop-in replacement for the Rust codetracer_trace_writer_ffi.
##
## Compile with: nim c --app:staticlib --mm:arc --noMain -d:release src/codetracer_trace_writer_ffi.nim
## Or for shared: nim c --app:lib --mm:arc --noMain -d:release src/codetracer_trace_writer_ffi.nim
##
## Differences from the Rust FFI (codetracer_trace_writer_ffi/src/lib.rs):
##
##   1. The Rust API uses a trait-object TraceWriter that writes separate JSON/binary
##      files (trace.json, trace_metadata.json, trace_paths.json). The Nim implementation
##      writes a single .ct CTFS container file with embedded split-binary events.
##
##   2. The Rust `trace_writer_new` takes (program, format). The Nim version takes
##      (program, trace_dir) where trace_dir is the directory in which to create
##      the .ct file. The format is always CTFS+split-binary (the modern format).
##
##   3. The Rust API has separate begin_metadata/finish_metadata/begin_events/
##      finish_events/begin_paths/finish_paths. The Nim API wraps these as no-ops
##      (metadata/paths are written on close) and provides the same function symbols
##      for link-compatibility. They always return 0 (success).
##
##   4. trace_writer_start and trace_writer_set_workdir are supported.
##
##   5. trace_writer_ensure_function_id and trace_writer_ensure_type_id maintain
##      internal registries (like Rust) and return sequential IDs. Functions and
##      types are emitted as events in the trace stream.
##
##   6. trace_writer_register_variable_int / _raw, trace_writer_register_call,
##      trace_writer_register_return / _int / _raw, trace_writer_register_special_event
##      are all supported with the same signatures as Rust.

import codetracer_trace_writer
import codetracer_trace_types
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/call_stream
import codetracer_trace_writer/io_event_stream
import codetracer_trace_writer/step_encoding
import codetracer_trace_writer/streaming_value_encoder
import codetracer_trace_writer/new_trace_reader
import std/tables
import std/os
import std/options

# ---------------------------------------------------------------------------
# Thread-local error buffer
# ---------------------------------------------------------------------------

var lastError {.threadvar.}: string
var emptyStr {.threadvar.}: string

proc setError(msg: string) =
  lastError = msg

proc trace_writer_last_error(): cstring {.exportc, cdecl.} =
  ## Retrieve the last error message for the current thread.
  ## Returns a pointer valid until the next FFI call on the same thread.
  ## Returns an empty string when no error has occurred.
  if lastError.len == 0:
    emptyStr = ""
    return cstring(emptyStr)
  return cstring(lastError)

# ---------------------------------------------------------------------------
# Internal state: wraps TraceWriter + registries
# ---------------------------------------------------------------------------

type
  FunctionEntry = object
    name: string
    path: string
    line: int64

  TypeEntry = object
    kind: TypeKind
    langType: string

  TraceWriterState = object
    # Old writer (single-stream CTFS)
    writer: TraceWriter
    writerReady: bool  # true once .ct file has been created

    # New writer (multi-stream CTFS)
    msWriter: MultiStreamTraceWriter
    msWriterReady: bool
    useMultiStream: bool

    # Pending step buffering for multi-stream mode:
    # The old FFI registers step FIRST, then variables one-at-a-time.
    # The multi-stream API writes step + all values together.
    # So we buffer the step info and accumulate values, then flush
    # on the next step or on close.
    hasPendingStep: bool
    pendingStepPathId: uint64
    pendingStepLine: uint64
    pendingValues: seq[VariableValue]

    # Pending call arguments for multi-stream mode:
    # The recorder calls trace_writer_register_call_arg once per arg
    # *immediately* before trace_writer_register_call. We accumulate
    # them here and consume them on register_call so the arguments are
    # carried on the call record (used by the frontend to render the
    # call's argument names + values in the calltrace pane).
    pendingCallArgs: seq[CallArg]

    ctFilePath: string  # path to the .ct output file (set in begin_events)
    programName: string  # stored from trace_writer_new, used when creating .ct
    workdir: string
    started: bool
    # Function registry: name+path+line -> id
    functions: seq[FunctionEntry]
    functionIndex: Table[string, csize_t]  # "name\0path\0line" -> index
    # Type registry: kind+langType -> id
    types: seq[TypeEntry]
    typeIndex: Table[string, csize_t]  # "kind\0langType" -> index

  TraceWriterHandle = ptr TraceWriterState

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc functionKey(name, path: string, line: int64): string =
  name & "\x00" & path & "\x00" & $line

proc typeKey(kind: TypeKind, langType: string): string =
  $ord(kind) & "\x00" & langType

proc toNimStr(s: cstring): string =
  if s.isNil:
    return ""
  $s

# ---------------------------------------------------------------------------
# FFI enum mirrors (C-compatible)
# ---------------------------------------------------------------------------

type
  FfiTraceFormat {.size: sizeof(cint).} = enum
    ffiJson = 0
    ffiBinaryV0 = 1
    ffiBinary = 2

  FfiTypeKind {.size: sizeof(cint).} = enum
    ffiTkSeq = 0
    ffiTkSet = 1
    ffiTkHashSet = 2
    ffiTkOrderedSet = 3
    ffiTkArray = 4
    ffiTkVarargs = 5
    ffiTkStruct = 6
    ffiTkInt = 7
    ffiTkFloat = 8
    ffiTkString = 9
    ffiTkCString = 10
    ffiTkChar = 11
    ffiTkBool = 12
    ffiTkLiteral = 13
    ffiTkRef = 14
    ffiTkRecursion = 15
    ffiTkRaw = 16
    ffiTkEnum = 17
    ffiTkEnum16 = 18
    ffiTkEnum32 = 19
    ffiTkC = 20
    ffiTkTableKind = 21
    ffiTkUnion = 22
    ffiTkPointer = 23
    ffiTkError = 24
    ffiTkFunctionKind = 25
    ffiTkTypeValue = 26
    ffiTkTuple = 27
    ffiTkVariant = 28
    ffiTkHtml = 29
    ffiTkNone = 30
    ffiTkNonExpanded = 31
    ffiTkAny = 32
    ffiTkSlice = 33

  FfiEventLogKind {.size: sizeof(cint).} = enum
    ffiElkWrite = 0
    ffiElkWriteFile = 1
    ffiElkWriteOther = 2
    ffiElkRead = 3
    ffiElkReadFile = 4
    ffiElkReadOther = 5
    ffiElkReadDir = 6
    ffiElkOpenDir = 7
    ffiElkCloseDir = 8
    ffiElkSocket = 9
    ffiElkOpen = 10
    ffiElkError = 11
    ffiElkTraceLogEvent = 12
    ffiElkEvmEvent = 13

proc toTypeKind(k: FfiTypeKind): TypeKind =
  TypeKind(ord(k))

proc toEventLogKind(k: FfiEventLogKind): EventLogKind =
  EventLogKind(ord(k))

# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

proc trace_writer_new(
    program: cstring,
    format: FfiTraceFormat,
): TraceWriterHandle {.exportc, cdecl.} =
  ## Create a new trace writer handle.
  ## The .ct file is NOT created here — it is deferred to trace_writer_begin_events
  ## which receives the output path from the Python recorder. This ensures the .ct
  ## file ends up in the correct output directory rather than the current working dir.
  ## Returns NULL on allocation failure (check trace_writer_last_error).
  let prog = toNimStr(program)

  let state = cast[TraceWriterHandle](alloc0(sizeof(TraceWriterState)))
  state[] = TraceWriterState(
    writerReady: false,
    msWriterReady: false,
    useMultiStream: format == ffiBinary,
    hasPendingStep: false,
    programName: prog,
    workdir: "",
    started: false,
    functions: @[],
    functionIndex: initTable[string, csize_t](),
    types: @[],
    typeIndex: initTable[string, csize_t](),
  )
  return state

proc trace_writer_free(handle: TraceWriterHandle) {.exportc, cdecl.} =
  ## Free a trace writer handle. Passing NULL is a no-op.
  if handle.isNil:
    return
  # Close if writer was actually created and not already closed
  if handle.useMultiStream:
    if handle.msWriterReady:
      # close() is idempotent — safe to call even if already closed
      discard handle.msWriter.close()
      handle.msWriter.closeCtfs()
  else:
    if handle.writerReady and not handle.writer.closed:
      discard handle.writer.close()
  try:
    `=destroy`(handle[])
  except:
    discard
  dealloc(handle)

# ---------------------------------------------------------------------------
# File I/O — begin / finish (compatibility stubs)
# ---------------------------------------------------------------------------

proc trace_writer_begin_metadata(
    handle: TraceWriterHandle,
    path: cstring,
): cint {.exportc, cdecl.} =
  ## Begin writing metadata. In the Nim CTFS implementation, metadata is
  ## written on close, so this is a no-op that records the path.
  if handle.isNil:
    setError("NULL handle")
    return 1.cint
  0.cint

proc trace_writer_finish_metadata(handle: TraceWriterHandle): cint {.exportc, cdecl.} =
  if handle.isNil:
    setError("NULL handle")
    return 1.cint
  0.cint

proc trace_writer_begin_events(
    handle: TraceWriterHandle,
    path: cstring,
): cint {.exportc, cdecl.} =
  ## Creates the .ct file in the same directory as the given events path.
  ## The .ct filename is derived from the program name stored at construction time.
  ## This is the point where the actual CTFS container file is opened on disk.
  if handle.isNil:
    setError("NULL handle")
    return 1.cint

  if handle.useMultiStream:
    if handle.msWriterReady:
      return 0.cint

    let eventsPath = toNimStr(path)
    let outDir = parentDir(eventsPath)
    let (_, progBase, _) = splitFile(handle.programName)
    let ctPath = outDir / (progBase & ".ct")

    let res = initMultiStreamWriter(ctPath, handle.programName)
    if res.isErr:
      setError(res.error)
      return 1.cint

    handle.msWriter = res.get()
    handle.msWriter.metadata.workdir = handle.workdir
    handle.msWriterReady = true
    handle.ctFilePath = ctPath
    return 0.cint

  # Old single-stream path
  if handle.writerReady:
    # Already initialized — nothing to do (idempotent)
    return 0.cint

  let eventsPath = toNimStr(path)
  let outDir = parentDir(eventsPath)
  # Place the .ct file in the same directory as the events path.
  # Use only the base filename of the program (strip directory and extension),
  # since programName may be a full path like "/tmp/test_recorder.py".
  let (_, progBase, _) = splitFile(handle.programName)
  let ctPath = outDir / (progBase & ".ct")

  let res = newTraceWriter(ctPath, handle.programName, @[], handle.workdir)
  if res.isErr:
    setError(res.error)
    return 1.cint

  handle.writer = res.get()
  handle.writerReady = true
  0.cint

proc trace_writer_finish_events(handle: TraceWriterHandle): cint {.exportc, cdecl.} =
  if handle.isNil:
    setError("NULL handle")
    return 1.cint
  if handle.useMultiStream:
    # Multi-stream writer flushes on close; nothing to do here
    return 0.cint
  if not handle.writerReady:
    return 0.cint
  # Flush (sync) current events
  let res = handle.writer.sync()
  if res.isErr:
    setError(res.error)
    return 1.cint
  0.cint

proc trace_writer_begin_paths(
    handle: TraceWriterHandle,
    path: cstring,
): cint {.exportc, cdecl.} =
  if handle.isNil:
    setError("NULL handle")
    return 1.cint
  0.cint

proc trace_writer_finish_paths(handle: TraceWriterHandle): cint {.exportc, cdecl.} =
  if handle.isNil:
    setError("NULL handle")
    return 1.cint
  0.cint

# ---------------------------------------------------------------------------
# Multi-stream helpers
# ---------------------------------------------------------------------------

proc flushPendingStep(handle: TraceWriterHandle): cint =
  ## Flush the buffered pending step and its accumulated variable values
  ## to the multi-stream writer. Returns 0 on success, 1 on error.
  if not handle.hasPendingStep:
    return 0.cint
  let res = handle.msWriter.registerStep(
    handle.pendingStepPathId,
    handle.pendingStepLine,
    handle.pendingValues)
  if res.isErr:
    setError(res.error)
    return 1.cint
  handle.pendingValues.setLen(0)
  handle.hasPendingStep = false
  0.cint

# ---------------------------------------------------------------------------
# Tracing primitives
# ---------------------------------------------------------------------------

proc trace_writer_start(
    handle: TraceWriterHandle,
    path: cstring,
    line: int64,
) {.exportc, cdecl.} =
  ## Record the initial step (entry point).
  if handle.isNil:
    return
  let p = toNimStr(path)

  if handle.useMultiStream:
    if not handle.msWriterReady:
      return
    let pathIdRes = handle.msWriter.registerPath(p)
    if pathIdRes.isErr:
      return
    let pathId = pathIdRes.get()
    # Buffer this as the first pending step
    handle.pendingStepPathId = pathId
    handle.pendingStepLine = uint64(line)
    handle.hasPendingStep = true
    handle.started = true
    return

  # Register path
  discard handle.writer.writePath(p)
  # Write the first step at pathId 0 (first registered path)
  discard handle.writer.writeStep(0'u64, line)
  handle.started = true

proc trace_writer_set_workdir(
    handle: TraceWriterHandle,
    workdir: cstring,
) {.exportc, cdecl.} =
  ## Override the working directory recorded in the trace metadata.
  ## Can be called before or after begin_events — the value is stored and
  ## propagated to the writer when/if it becomes ready.
  if handle.isNil:
    return
  handle.workdir = toNimStr(workdir)
  # Update the metadata in the writer if already created
  if handle.useMultiStream:
    if handle.msWriterReady:
      handle.msWriter.metadata.workdir = handle.workdir
  elif handle.writerReady:
    handle.writer.metadata.workdir = handle.workdir

proc trace_writer_register_step(
    handle: TraceWriterHandle,
    path: cstring,
    line: int64,
) {.exportc, cdecl.} =
  ## Register a step at the given source path and line.
  if handle.isNil:
    return
  let p = toNimStr(path)

  if handle.useMultiStream:
    if not handle.msWriterReady:
      return
    # Flush the previous pending step (with its accumulated values)
    discard flushPendingStep(handle)

    let pathIdRes = handle.msWriter.registerPath(p)
    if pathIdRes.isErr:
      return
    let pathId = pathIdRes.get()
    # Buffer this as the new pending step
    handle.pendingStepPathId = pathId
    handle.pendingStepLine = uint64(line)
    handle.hasPendingStep = true
    return

  # Register path (dedup handled by paths list — emit every time like Rust)
  discard handle.writer.writePath(p)
  # pathId is the count of paths registered so far minus 1
  let pathId = uint64(handle.writer.paths.len - 1)
  discard handle.writer.writeStep(pathId, line)

proc trace_writer_ensure_function_id(
    handle: TraceWriterHandle,
    name: cstring,
    path: cstring,
    line: int64,
): csize_t {.exportc, cdecl.} =
  ## Register a function and return its ID. Returns SIZE_MAX on error.
  if handle.isNil:
    return high(csize_t)
  let n = toNimStr(name)
  let p = toNimStr(path)
  let key = functionKey(n, p, line)

  let existing = handle.functionIndex.getOrDefault(key, high(csize_t))
  if existing != high(csize_t):
    return existing

  let id = csize_t(handle.functions.len)
  handle.functions.add(FunctionEntry(name: n, path: p, line: line))
  handle.functionIndex[key] = id

  if handle.useMultiStream:
    # Intern the function name in the multi-stream interning table
    if handle.msWriterReady:
      discard handle.msWriter.registerFunction(n)
  else:
    # Emit function event: use pathId 0 for now (callers should register paths first)
    # In practice recorders call ensure_function_id with the path they already registered
    discard handle.writer.writeFunction(0'u64, line, n)

  return id

proc trace_writer_ensure_type_id(
    handle: TraceWriterHandle,
    kind: FfiTypeKind,
    lang_type: cstring,
): csize_t {.exportc, cdecl.} =
  ## Register a type and return its ID. Returns SIZE_MAX on error.
  if handle.isNil:
    return high(csize_t)
  let lt = toNimStr(lang_type)
  let tk = toTypeKind(kind)
  let key = typeKey(tk, lt)

  let existing = handle.typeIndex.getOrDefault(key, high(csize_t))
  if existing != high(csize_t):
    return existing

  let id = csize_t(handle.types.len)
  handle.types.add(TypeEntry(kind: tk, langType: lt))
  handle.typeIndex[key] = id

  if handle.useMultiStream:
    # Intern the type name in the multi-stream interning table
    if handle.msWriterReady:
      discard handle.msWriter.registerType(lt)
  else:
    # Emit type event
    discard handle.writer.writeEvent(TraceLowLevelEvent(
      kind: tleType,
      typeRecord: TypeRecord(
        kind: tk,
        langType: lt,
        specificInfo: TypeSpecificInfo(kind: tsikNone),
      ),
    ))

  return id

proc trace_writer_register_call_arg(
    handle: TraceWriterHandle,
    name: cstring,
    cbor_data: ptr uint8,
    cbor_len: csize_t,
) {.exportc, cdecl.} =
  ## Stage a single argument (name + CBOR-encoded value) for the next
  ## ``trace_writer_register_call`` invocation. Recorders should call this
  ## once per parameter immediately before ``trace_writer_register_call``.
  ##
  ## The arguments are consumed (and the buffer cleared) by the next
  ## register_call. Calling ``trace_writer_register_call`` without staging
  ## any args is still valid and yields a call record with empty ``args``.
  ##
  ## Only meaningful in multi-stream (CTFS) mode. In legacy single-stream
  ## mode this is a no-op.
  if handle.isNil:
    return
  if not handle.useMultiStream:
    return
  if not handle.msWriterReady:
    return
  let nameStr = toNimStr(name)
  let varnameRes = handle.msWriter.registerVarname(nameStr)
  if varnameRes.isErr:
    return
  let vnId = varnameRes.get()
  var data = newSeq[byte](int(cbor_len))
  if not cbor_data.isNil and cbor_len > 0.csize_t:
    copyMem(addr data[0], cbor_data, int(cbor_len))
  handle.pendingCallArgs.add(CallArg(varnameId: vnId, value: data))

proc trace_writer_register_call(
    handle: TraceWriterHandle,
    function_id: csize_t,
) {.exportc, cdecl.} =
  ## Register a call to the function identified by function_id.
  ##
  ## Any arguments staged via ``trace_writer_register_call_arg`` since the
  ## previous call/return are attached to the new call record and the
  ## staging buffer is cleared.
  if handle.isNil:
    return
  if handle.useMultiStream:
    discard handle.msWriter.registerCall(uint64(function_id),
        handle.pendingCallArgs)
    handle.pendingCallArgs.setLen(0)
    return
  # Legacy single-stream path doesn't support call args yet.
  handle.pendingCallArgs.setLen(0)
  discard handle.writer.writeCall(uint64(function_id))

proc trace_writer_register_return(handle: TraceWriterHandle) {.exportc, cdecl.} =
  ## Register a function return with no explicit return value.
  if handle.isNil:
    return
  if handle.useMultiStream:
    discard handle.msWriter.registerReturn()
    return
  discard handle.writer.writeReturn()

proc trace_writer_register_return_int(
    handle: TraceWriterHandle,
    value: int64,
    type_kind: FfiTypeKind,
    type_name: cstring,
) {.exportc, cdecl.} =
  ## Register a function return with an integer return value.
  if handle.isNil:
    return
  let typeId = trace_writer_ensure_type_id(handle, type_kind, type_name)

  if handle.useMultiStream:
    # Encode the return value as CBOR bytes using the streaming encoder
    var sve = StreamingValueEncoder.init()
    discard sve.writeInt(value, uint64(typeId))
    let retBytes = sve.getBytes()
    discard handle.msWriter.registerReturn(retBytes)
    return

  discard handle.writer.writeEvent(TraceLowLevelEvent(
    kind: tleReturn,
    returnRecord: ReturnRecord(
      returnValue: ValueRecord(
        kind: vrkInt,
        intVal: value,
        intTypeId: TypeId(typeId),
      ),
    ),
  ))

proc trace_writer_register_return_raw(
    handle: TraceWriterHandle,
    value_repr: cstring,
    type_kind: FfiTypeKind,
    type_name: cstring,
) {.exportc, cdecl.} =
  ## Register a function return with a string (raw) return value.
  if handle.isNil:
    return
  let typeId = trace_writer_ensure_type_id(handle, type_kind, type_name)

  if handle.useMultiStream:
    var sve = StreamingValueEncoder.init()
    discard sve.writeRaw(toNimStr(value_repr), uint64(typeId))
    let retBytes = sve.getBytes()
    discard handle.msWriter.registerReturn(retBytes)
    return

  discard handle.writer.writeEvent(TraceLowLevelEvent(
    kind: tleReturn,
    returnRecord: ReturnRecord(
      returnValue: ValueRecord(
        kind: vrkRaw,
        rawStr: toNimStr(value_repr),
        rawTypeId: TypeId(typeId),
      ),
    ),
  ))

proc trace_writer_register_variable_int(
    handle: TraceWriterHandle,
    name: cstring,
    value: int64,
    type_kind: FfiTypeKind,
    type_name: cstring,
) {.exportc, cdecl.} =
  ## Register a variable with an integer value.
  if handle.isNil:
    return
  let typeId = trace_writer_ensure_type_id(handle, type_kind, type_name)

  if handle.useMultiStream:
    # Intern the variable name
    if handle.msWriterReady:
      let vnIdRes = handle.msWriter.registerVarname(toNimStr(name))
      if vnIdRes.isErr:
        return
      let vnId = vnIdRes.get()
      # Encode the value as CBOR
      var sve = StreamingValueEncoder.init()
      discard sve.writeInt(value, uint64(typeId))
      let data = sve.getBytes()
      handle.pendingValues.add(VariableValue(
        varnameId: vnId, typeId: uint64(typeId), data: data))
    return

  # Emit variable name event
  discard handle.writer.writeEvent(TraceLowLevelEvent(
    kind: tleVariableName,
    varName: toNimStr(name),
  ))
  # Emit value event with variableId = 0 (simplified — real recorders track IDs)
  discard handle.writer.writeValue(0'u64, ValueRecord(
    kind: vrkInt,
    intVal: value,
    intTypeId: TypeId(typeId),
  ))

proc trace_writer_register_variable_raw(
    handle: TraceWriterHandle,
    name: cstring,
    value_repr: cstring,
    type_kind: FfiTypeKind,
    type_name: cstring,
) {.exportc, cdecl.} =
  ## Register a variable with a string (raw) value representation.
  if handle.isNil:
    return
  let typeId = trace_writer_ensure_type_id(handle, type_kind, type_name)

  if handle.useMultiStream:
    # Intern the variable name
    if handle.msWriterReady:
      let vnIdRes = handle.msWriter.registerVarname(toNimStr(name))
      if vnIdRes.isErr:
        return
      let vnId = vnIdRes.get()
      # Encode the value as CBOR
      var sve = StreamingValueEncoder.init()
      discard sve.writeRaw(toNimStr(value_repr), uint64(typeId))
      let data = sve.getBytes()
      handle.pendingValues.add(VariableValue(
        varnameId: vnId, typeId: uint64(typeId), data: data))
    return

  # Emit variable name event
  discard handle.writer.writeEvent(TraceLowLevelEvent(
    kind: tleVariableName,
    varName: toNimStr(name),
  ))
  # Emit value event
  discard handle.writer.writeValue(0'u64, ValueRecord(
    kind: vrkRaw,
    rawStr: toNimStr(value_repr),
    rawTypeId: TypeId(typeId),
  ))

proc trace_writer_register_variable_cbor(
    handle: TraceWriterHandle,
    name: cstring,
    cbor_data: ptr uint8,
    cbor_len: csize_t,
) {.exportc, cdecl.} =
  ## Register a variable with pre-encoded CBOR value bytes (from the streaming
  ## encoder). This avoids the intermediate ValueRecord tree for complex values
  ## like sequences, tuples, and dicts — the caller encodes directly to CBOR
  ## via the ct_value_* C API, then passes the result here.
  if handle.isNil:
    return

  if handle.useMultiStream:
    if handle.msWriterReady:
      let vnIdRes = handle.msWriter.registerVarname(toNimStr(name))
      if vnIdRes.isErr:
        return
      let vnId = vnIdRes.get()
      var data = newSeq[byte](int(cbor_len))
      if not cbor_data.isNil and cbor_len > 0.csize_t:
        copyMem(addr data[0], cbor_data, int(cbor_len))
      # Use type_id 0 — the actual type is already encoded in the CBOR bytes
      handle.pendingValues.add(VariableValue(
        varnameId: vnId, typeId: 0'u64, data: data))
    return

  # Legacy path: fall back to raw representation (extract is not feasible from
  # opaque CBOR, so emit the CBOR length as a placeholder).
  discard handle.writer.writeEvent(TraceLowLevelEvent(
    kind: tleVariableName,
    varName: toNimStr(name),
  ))
  discard handle.writer.writeValue(0'u64, ValueRecord(
    kind: vrkRaw,
    rawStr: "<cbor:" & $cbor_len & ">",
    rawTypeId: TypeId(0),
  ))

proc trace_writer_register_return_cbor(
    handle: TraceWriterHandle,
    cbor_data: ptr uint8,
    cbor_len: csize_t,
) {.exportc, cdecl.} =
  ## Register a function return with pre-encoded CBOR value bytes.
  ## Same rationale as trace_writer_register_variable_cbor.
  if handle.isNil:
    return

  if handle.useMultiStream:
    var data = newSeq[byte](int(cbor_len))
    if not cbor_data.isNil and cbor_len > 0.csize_t:
      copyMem(addr data[0], cbor_data, int(cbor_len))
    discard handle.msWriter.registerReturn(data)
    return

  # Legacy path: emit a raw return placeholder
  discard handle.writer.writeEvent(TraceLowLevelEvent(
    kind: tleReturn,
    returnRecord: ReturnRecord(
      returnValue: ValueRecord(
        kind: vrkRaw,
        rawStr: "<cbor:" & $cbor_len & ">",
        rawTypeId: TypeId(0),
      ),
    ),
  ))

proc toIOEventKind(k: FfiEventLogKind): IOEventKind =
  ## Map FFI event log kinds to multi-stream IOEventKind.
  ## The multi-stream IO event stream has a simpler set of kinds.
  case k
  of ffiElkWrite, ffiElkWriteFile, ffiElkWriteOther:
    ioStdout
  of ffiElkRead, ffiElkReadFile, ffiElkReadOther, ffiElkReadDir,
      ffiElkOpenDir, ffiElkCloseDir, ffiElkSocket, ffiElkOpen:
    ioFileOp
  of ffiElkError:
    ioError
  of ffiElkTraceLogEvent, ffiElkEvmEvent:
    ioStderr

proc trace_writer_register_special_event(
    handle: TraceWriterHandle,
    kind: FfiEventLogKind,
    metadata: cstring,
    content: cstring,
) {.exportc, cdecl.} =
  ## Register an I/O or special event with optional metadata.
  if handle.isNil:
    return

  if handle.useMultiStream:
    # Combine metadata + content into IO event data
    let contentStr = toNimStr(content)
    var data = newSeq[byte](contentStr.len)
    for i in 0 ..< contentStr.len:
      data[i] = byte(contentStr[i])
    discard handle.msWriter.registerIOEvent(toIOEventKind(kind), data)
    return

  discard handle.writer.writeEvent(TraceLowLevelEvent(
    kind: tleEvent,
    recordEvent: RecordEvent(
      kind: toEventLogKind(kind),
      metadata: toNimStr(metadata),
      content: toNimStr(content),
    ),
  ))

# ---------------------------------------------------------------------------
# Close
# ---------------------------------------------------------------------------

proc trace_writer_close(handle: TraceWriterHandle): cint {.exportc, cdecl.} =
  ## Close the trace writer and flush all remaining data.
  ## Returns 0 on success, non-zero on failure.
  ## If the writer was never initialized (begin_events never called), this is a no-op.
  if handle.isNil:
    setError("NULL handle")
    return 1.cint

  if handle.useMultiStream:
    if not handle.msWriterReady:
      return 0.cint
    # Flush the last pending step
    let flushRc = flushPendingStep(handle)
    if flushRc != 0:
      return flushRc
    let closeRes = handle.msWriter.close()
    if closeRes.isErr:
      setError(closeRes.error)
      return 1.cint
    # Write the in-memory CTFS bytes to disk
    let ctfsBytes = handle.msWriter.toBytes()
    try:
      var f = open(handle.ctFilePath, fmWrite)
      if ctfsBytes.len > 0:
        discard f.writeBuffer(unsafeAddr ctfsBytes[0], ctfsBytes.len)
      f.close()
    except IOError:
      setError("failed to write .ct file: " & handle.ctFilePath)
      return 1.cint
    handle.msWriter.closeCtfs()
    return 0.cint

  if not handle.writerReady:
    # Writer was never opened — nothing to close
    return 0.cint
  let res = handle.writer.close()
  if res.isErr:
    setError(res.error)
    return 1.cint
  0.cint

# ---------------------------------------------------------------------------
# meta.dat — write via TraceWriter handle
# ---------------------------------------------------------------------------

proc ct_write_meta_dat(
    handle: TraceWriterHandle,
    recorder_id: ptr uint8,
    recorder_id_len: csize_t
): cint {.exportc, cdecl.} =
  ## Write meta.dat to the trace's CTFS container using the metadata
  ## and paths already registered via trace_writer_set_workdir,
  ## trace_writer_start, and trace_writer_register_path.
  ## recorder_id is optional (pass NULL/0 for empty).
  ## Returns 0 on success.
  if handle.isNil:
    setError("NULL handle")
    return 1.cint

  if handle.useMultiStream:
    # Multi-stream writer writes meta.dat automatically during close().
    # Nothing to do here — the metadata and paths are already tracked.
    if not handle.msWriterReady:
      setError("writer not ready (call begin_events first)")
      return 1.cint
    return 0.cint

  if not handle.writerReady:
    setError("writer not ready (call begin_events first)")
    return 1.cint

  var recId = ""
  if not recorder_id.isNil and recorder_id_len > 0.csize_t:
    recId = newString(int(recorder_id_len))
    copyMem(addr recId[0], recorder_id, int(recorder_id_len))

  let wRes = handle.writer.writeMetaDat(recorderId = recId)
  if wRes.isErr:
    setError(wRes.error)
    return 1.cint

  0.cint

# ---------------------------------------------------------------------------
# meta.dat — standalone buffer write
# ---------------------------------------------------------------------------

proc ct_write_meta_dat_to_buffer(
    program: ptr uint8, program_len: csize_t,
    workdir: ptr uint8, workdir_len: csize_t,
    args: ptr ptr uint8, arg_lens: ptr csize_t, args_count: csize_t,
    paths: ptr ptr uint8, path_lens: ptr csize_t, paths_count: csize_t,
    recorder_id: ptr uint8, recorder_id_len: csize_t,
    out_buf: ptr ptr uint8, out_len: ptr csize_t
): cint {.exportc, cdecl.} =
  ## Write meta.dat to a newly allocated buffer from explicit fields.
  ## The caller must free the buffer with ct_free_buffer.
  ## Returns 0 on success.
  if out_buf.isNil or out_len.isNil:
    setError("NULL output pointers")
    return 1.cint

  var progStr = ""
  if not program.isNil and program_len > 0.csize_t:
    progStr = newString(int(program_len))
    copyMem(addr progStr[0], program, int(program_len))

  var wdStr = ""
  if not workdir.isNil and workdir_len > 0.csize_t:
    wdStr = newString(int(workdir_len))
    copyMem(addr wdStr[0], workdir, int(workdir_len))

  var argSeq = newSeq[string](int(args_count))
  for i in 0 ..< int(args_count):
    let aPtr = cast[ptr UncheckedArray[ptr uint8]](args)[i]
    let aLen = cast[ptr UncheckedArray[csize_t]](arg_lens)[i]
    if not aPtr.isNil and aLen > 0.csize_t:
      argSeq[i] = newString(int(aLen))
      copyMem(addr argSeq[i][0], aPtr, int(aLen))

  var pathSeq = newSeq[string](int(paths_count))
  for i in 0 ..< int(paths_count):
    let pPtr = cast[ptr UncheckedArray[ptr uint8]](paths)[i]
    let pLen = cast[ptr UncheckedArray[csize_t]](path_lens)[i]
    if not pPtr.isNil and pLen > 0.csize_t:
      pathSeq[i] = newString(int(pLen))
      copyMem(addr pathSeq[i][0], pPtr, int(pLen))

  var recId = ""
  if not recorder_id.isNil and recorder_id_len > 0.csize_t:
    recId = newString(int(recorder_id_len))
    copyMem(addr recId[0], recorder_id, int(recorder_id_len))

  let meta = TraceMetadata(program: progStr, args: argSeq, workdir: wdStr)
  let buf = writeMetaDatToBuffer(meta, pathSeq, recorderId = recId)

  let outPtr = cast[ptr uint8](alloc(buf.len))
  if outPtr.isNil:
    setError("allocation failed")
    return 1.cint
  copyMem(outPtr, unsafeAddr buf[0], buf.len)
  out_buf[] = outPtr
  out_len[] = csize_t(buf.len)
  0.cint

proc ct_free_buffer(buf: ptr uint8) {.exportc, cdecl.} =
  ## Free a buffer allocated by ct_write_meta_dat_to_buffer.
  if not buf.isNil:
    dealloc(buf)

# ---------------------------------------------------------------------------
# meta.dat — reader handle
# ---------------------------------------------------------------------------

type MetaDatReaderHandle = ptr MetaDatContents

proc ct_read_meta_dat(
    data: ptr uint8,
    data_len: csize_t
): MetaDatReaderHandle {.exportc, cdecl.} =
  ## Parse meta.dat from raw bytes. Returns handle on success, nil on failure.
  if data.isNil or data_len == 0.csize_t:
    setError("NULL or empty data")
    return nil

  var buf = newSeq[byte](int(data_len))
  copyMem(addr buf[0], data, int(data_len))

  let res = readMetaDat(buf)
  if res.isErr:
    setError(res.error)
    return nil

  let h = cast[MetaDatReaderHandle](alloc0(sizeof(MetaDatContents)))
  h[] = res.get()
  return h

proc ct_meta_dat_program(h: MetaDatReaderHandle, out_len: ptr csize_t): ptr uint8 {.exportc, cdecl.} =
  ## Get the program string. Returns pointer valid until ct_meta_dat_free.
  if h.isNil or out_len.isNil:
    return nil
  out_len[] = csize_t(h.program.len)
  if h.program.len == 0:
    return nil
  return cast[ptr uint8](unsafeAddr h.program[0])

proc ct_meta_dat_workdir(h: MetaDatReaderHandle, out_len: ptr csize_t): ptr uint8 {.exportc, cdecl.} =
  if h.isNil or out_len.isNil:
    return nil
  out_len[] = csize_t(h.workdir.len)
  if h.workdir.len == 0:
    return nil
  return cast[ptr uint8](unsafeAddr h.workdir[0])

proc ct_meta_dat_args_count(h: MetaDatReaderHandle): csize_t {.exportc, cdecl.} =
  if h.isNil:
    return 0.csize_t
  return csize_t(h.args.len)

proc ct_meta_dat_arg(h: MetaDatReaderHandle, idx: csize_t, out_len: ptr csize_t): ptr uint8 {.exportc, cdecl.} =
  if h.isNil or out_len.isNil or int(idx) >= h.args.len:
    return nil
  out_len[] = csize_t(h.args[int(idx)].len)
  if h.args[int(idx)].len == 0:
    return nil
  return cast[ptr uint8](unsafeAddr h.args[int(idx)][0])

proc ct_meta_dat_paths_count(h: MetaDatReaderHandle): csize_t {.exportc, cdecl.} =
  if h.isNil:
    return 0.csize_t
  return csize_t(h.paths.len)

proc ct_meta_dat_path(h: MetaDatReaderHandle, idx: csize_t, out_len: ptr csize_t): ptr uint8 {.exportc, cdecl.} =
  if h.isNil or out_len.isNil or int(idx) >= h.paths.len:
    return nil
  out_len[] = csize_t(h.paths[int(idx)].len)
  if h.paths[int(idx)].len == 0:
    return nil
  return cast[ptr uint8](unsafeAddr h.paths[int(idx)][0])

proc ct_meta_dat_recorder_id(h: MetaDatReaderHandle, out_len: ptr csize_t): ptr uint8 {.exportc, cdecl.} =
  if h.isNil or out_len.isNil:
    return nil
  out_len[] = csize_t(h.recorderId.len)
  if h.recorderId.len == 0:
    return nil
  return cast[ptr uint8](unsafeAddr h.recorderId[0])

proc ct_meta_dat_free(h: MetaDatReaderHandle) {.exportc, cdecl.} =
  ## Free a MetaDatContents handle.
  if h.isNil:
    return
  `=destroy`(h[])
  dealloc(h)

# ---------------------------------------------------------------------------
# Streaming Value Encoder — C FFI
# ---------------------------------------------------------------------------

type ValueEncoderHandle = ptr StreamingValueEncoder

proc ct_value_encoder_new(): ValueEncoderHandle {.exportc, cdecl.} =
  ## Create a new streaming value encoder. Returns NULL on allocation failure.
  let h = cast[ValueEncoderHandle](alloc0(sizeof(StreamingValueEncoder)))
  h[] = StreamingValueEncoder.init()
  return h

proc ct_value_encoder_free(h: ValueEncoderHandle) {.exportc, cdecl.} =
  ## Free a value encoder handle. Passing NULL is a no-op.
  if h.isNil:
    return
  `=destroy`(h[])
  dealloc(h)

proc ct_value_encoder_reset(h: ValueEncoderHandle) {.exportc, cdecl.} =
  ## Reset the encoder for reuse (clears buffer, resets nesting stack).
  if h.isNil:
    return
  h[].reset()

proc ct_value_write_int(h: ValueEncoderHandle, value: int64, type_id: uint64): cint {.exportc, cdecl.} =
  if h.isNil:
    setError("NULL handle")
    return 1.cint
  let r = h[].writeInt(value, type_id)
  if r.isErr:
    setError(r.error)
    return 1.cint
  0.cint

proc ct_value_write_float(h: ValueEncoderHandle, value: float64, type_id: uint64): cint {.exportc, cdecl.} =
  if h.isNil:
    setError("NULL handle")
    return 1.cint
  let r = h[].writeFloat(value, type_id)
  if r.isErr:
    setError(r.error)
    return 1.cint
  0.cint

proc ct_value_write_bool(h: ValueEncoderHandle, value: cint): cint {.exportc, cdecl.} =
  if h.isNil:
    setError("NULL handle")
    return 1.cint
  # Bool type_id is needed for byte-identical output; use 0 as default from C
  let r = h[].writeBool(value != 0, typeId = 0'u64)
  if r.isErr:
    setError(r.error)
    return 1.cint
  0.cint

proc ct_value_write_bool_typed(h: ValueEncoderHandle, value: cint, type_id: uint64): cint {.exportc, cdecl.} =
  if h.isNil:
    setError("NULL handle")
    return 1.cint
  let r = h[].writeBool(value != 0, typeId = type_id)
  if r.isErr:
    setError(r.error)
    return 1.cint
  0.cint

proc ct_value_write_string(h: ValueEncoderHandle, data: ptr uint8, len: csize_t, type_id: uint64): cint {.exportc, cdecl.} =
  if h.isNil:
    setError("NULL handle")
    return 1.cint
  var s = ""
  if not data.isNil and len > 0.csize_t:
    s = newString(int(len))
    copyMem(addr s[0], data, int(len))
  let r = h[].writeString(s, type_id)
  if r.isErr:
    setError(r.error)
    return 1.cint
  0.cint

proc ct_value_write_none(h: ValueEncoderHandle): cint {.exportc, cdecl.} =
  if h.isNil:
    setError("NULL handle")
    return 1.cint
  let r = h[].writeNone(typeId = 0'u64)
  if r.isErr:
    setError(r.error)
    return 1.cint
  0.cint

proc ct_value_write_none_typed(h: ValueEncoderHandle, type_id: uint64): cint {.exportc, cdecl.} =
  if h.isNil:
    setError("NULL handle")
    return 1.cint
  let r = h[].writeNone(typeId = type_id)
  if r.isErr:
    setError(r.error)
    return 1.cint
  0.cint

proc ct_value_write_raw(h: ValueEncoderHandle, data: ptr uint8, len: csize_t, type_id: uint64): cint {.exportc, cdecl.} =
  if h.isNil:
    setError("NULL handle")
    return 1.cint
  var s = ""
  if not data.isNil and len > 0.csize_t:
    s = newString(int(len))
    copyMem(addr s[0], data, int(len))
  let r = h[].writeRaw(s, type_id)
  if r.isErr:
    setError(r.error)
    return 1.cint
  0.cint

proc ct_value_write_error(h: ValueEncoderHandle, data: ptr uint8, len: csize_t, type_id: uint64): cint {.exportc, cdecl.} =
  ## Write an error value with the given message and type ID.
  if h.isNil:
    setError("NULL handle")
    return 1.cint
  var s = ""
  if not data.isNil and len > 0.csize_t:
    s = newString(int(len))
    copyMem(addr s[0], data, int(len))
  let r = h[].writeError(s, type_id)
  if r.isErr:
    setError(r.error)
    return 1.cint
  0.cint

proc ct_value_begin_struct(h: ValueEncoderHandle, type_id: uint64, field_count: cint): cint {.exportc, cdecl.} =
  if h.isNil:
    setError("NULL handle")
    return 1.cint
  let r = h[].beginStruct(type_id, int(field_count))
  if r.isErr:
    setError(r.error)
    return 1.cint
  0.cint

proc ct_value_begin_sequence(h: ValueEncoderHandle, type_id: uint64, element_count: cint): cint {.exportc, cdecl.} =
  if h.isNil:
    setError("NULL handle")
    return 1.cint
  let r = h[].beginSequence(type_id, int(element_count))
  if r.isErr:
    setError(r.error)
    return 1.cint
  0.cint

proc ct_value_begin_tuple(h: ValueEncoderHandle, type_id: uint64, element_count: cint): cint {.exportc, cdecl.} =
  if h.isNil:
    setError("NULL handle")
    return 1.cint
  let r = h[].beginTuple(type_id, int(element_count))
  if r.isErr:
    setError(r.error)
    return 1.cint
  0.cint

proc ct_value_end_compound(h: ValueEncoderHandle): cint {.exportc, cdecl.} =
  if h.isNil:
    setError("NULL handle")
    return 1.cint
  let r = h[].endCompound()
  if r.isErr:
    setError(r.error)
    return 1.cint
  0.cint

proc ct_value_get_bytes(h: ValueEncoderHandle, out_len: ptr csize_t): ptr uint8 {.exportc, cdecl.} =
  ## Get pointer to the encoded CBOR bytes. Valid until next reset/write/free.
  ## Sets *out_len to the byte count. Returns NULL on error.
  if h.isNil or out_len.isNil:
    return nil
  let buf = h[].getBytesView()
  out_len[] = csize_t(buf.len)
  if buf.len == 0:
    return nil
  return cast[ptr uint8](unsafeAddr buf[0])

# ===========================================================================
# Reader FFI — C interface to NewTraceReader
# ===========================================================================

# ---------------------------------------------------------------------------
# JSON helpers (no-exception, manual string building)
# ---------------------------------------------------------------------------

proc bytesToJsonArray(data: seq[byte]): string =
  ## Render byte sequence as JSON array of integers.
  result = "["
  for i, b in data:
    if i > 0: result.add(",")
    result.add($int(b))
  result.add("]")

proc stepEventToJson(ev: StepEvent): string =
  ## Serialize a StepEvent to JSON.
  case ev.kind
  of sekAbsoluteStep:
    "{\"kind\":\"absolute_step\",\"global_line_index\":" & $ev.globalLineIndex & "}"
  of sekDeltaStep:
    "{\"kind\":\"delta_step\",\"line_delta\":" & $ev.lineDelta & "}"
  of sekRaise:
    "{\"kind\":\"raise\",\"exception_type_id\":" & $ev.exceptionTypeId &
      ",\"message\":" & bytesToJsonArray(ev.message) & "}"
  of sekCatch:
    "{\"kind\":\"catch\",\"exception_type_id\":" & $ev.catchExceptionTypeId & "}"
  of sekThreadSwitch:
    "{\"kind\":\"thread_switch\",\"thread_id\":" & $ev.threadId & "}"

proc variableValueToJson(v: VariableValue): string =
  "{\"varname_id\":" & $v.varnameId &
    ",\"type_id\":" & $v.typeId &
    ",\"data\":" & bytesToJsonArray(v.data) & "}"

proc variableValuesToJson(vs: seq[VariableValue]): string =
  result = "["
  for i, v in vs:
    if i > 0: result.add(",")
    result.add(variableValueToJson(v))
  result.add("]")

proc callRecordToJson(rec: call_stream.CallRecord): string =
  result = "{\"function_id\":" & $rec.functionId &
    ",\"parent_call_key\":" & $rec.parentCallKey &
    ",\"entry_step\":" & $rec.entryStep &
    ",\"exit_step\":" & $rec.exitStep &
    ",\"depth\":" & $rec.depth
  # args
  result.add(",\"args\":[")
  for i, arg in rec.args:
    if i > 0: result.add(",")
    result.add("{\"varname_id\":" & $arg.varnameId &
      ",\"value\":" & bytesToJsonArray(arg.value) & "}")
  result.add("]")
  # return value
  result.add(",\"return_value\":" & bytesToJsonArray(rec.returnValue))
  # exception
  result.add(",\"exception\":" & bytesToJsonArray(rec.exception))
  # children
  result.add(",\"children\":[")
  for i, c in rec.children:
    if i > 0: result.add(",")
    result.add($c)
  result.add("]}")

proc ioEventToJson(ev: IOEvent): string =
  let kindStr = case ev.kind
    of ioStdout: "stdout"
    of ioStderr: "stderr"
    of ioFileOp: "file_op"
    of ioError: "error"
  "{\"kind\":\"" & kindStr & "\",\"step_id\":" & $ev.stepId &
    ",\"data\":" & bytesToJsonArray(ev.data) & "}"

# ---------------------------------------------------------------------------
# Reader handle
# ---------------------------------------------------------------------------

type
  TraceReaderHandle = ptr NewTraceReader

proc allocJsonResult(s: string, outLen: ptr csize_t): ptr uint8 =
  ## Allocate a copy of the JSON string and set outLen. Caller frees
  ## with ct_free_buffer.
  if outLen.isNil:
    return nil
  let n = s.len
  outLen[] = csize_t(n)
  if n == 0:
    return nil
  let buf = cast[ptr uint8](alloc(n))
  if buf.isNil:
    return nil
  copyMem(buf, unsafeAddr s[0], n)
  return buf

proc allocStringResult(s: string, outLen: ptr csize_t): ptr uint8 =
  ## Return a pointer into a heap-copied string. Caller frees with ct_free_buffer.
  if outLen.isNil:
    return nil
  let n = s.len
  outLen[] = csize_t(n)
  if n == 0:
    return nil
  let buf = cast[ptr uint8](alloc(n))
  if buf.isNil:
    return nil
  copyMem(buf, unsafeAddr s[0], n)
  return buf

# ---------------------------------------------------------------------------
# Reader lifecycle
# ---------------------------------------------------------------------------

proc ct_reader_open(path: cstring): pointer {.exportc, cdecl.} =
  ## Open a .ct trace file. Returns opaque reader handle or nil on failure.
  ## Check trace_writer_last_error() for error message on failure.
  if path.isNil:
    setError("NULL path")
    return nil
  let p = $path
  let res = openNewTrace(p)
  if res.isErr:
    setError(res.error)
    return nil
  let h = cast[TraceReaderHandle](alloc0(sizeof(NewTraceReader)))
  h[] = res.get()
  return cast[pointer](h)

proc ct_reader_close(h: pointer) {.exportc, cdecl.} =
  ## Close and free a reader handle. Passing NULL is a no-op.
  if h.isNil:
    return
  let rh = cast[TraceReaderHandle](h)
  try:
    `=destroy`(rh[])
  except:
    discard
  dealloc(rh)

# ---------------------------------------------------------------------------
# Counts
# ---------------------------------------------------------------------------

proc ct_reader_step_count(h: pointer): uint64 {.exportc, cdecl.} =
  if h.isNil: return 0
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].stepCount()
  if res.isErr:
    setError(res.error)
    return 0
  res.get()

proc ct_reader_call_count(h: pointer): uint64 {.exportc, cdecl.} =
  if h.isNil: return 0
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].callCount()
  if res.isErr:
    setError(res.error)
    return 0
  res.get()

proc ct_reader_event_count(h: pointer): uint64 {.exportc, cdecl.} =
  if h.isNil: return 0
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].ioEventCount()
  if res.isErr:
    setError(res.error)
    return 0
  res.get()

# ---------------------------------------------------------------------------
# Interning
# ---------------------------------------------------------------------------

proc ct_reader_path(h: pointer, id: uint64, outLen: ptr csize_t): ptr uint8 {.exportc, cdecl.} =
  ## Get path string by id. Caller must free result with ct_free_buffer.
  if h.isNil or outLen.isNil: return nil
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].path(id)
  if res.isErr:
    setError(res.error)
    return nil
  allocStringResult(res.get(), outLen)

proc ct_reader_function(h: pointer, id: uint64, outLen: ptr csize_t): ptr uint8 {.exportc, cdecl.} =
  ## Get function name by id. Caller must free result with ct_free_buffer.
  if h.isNil or outLen.isNil: return nil
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].function(id)
  if res.isErr:
    setError(res.error)
    return nil
  allocStringResult(res.get(), outLen)

proc ct_reader_type_name(h: pointer, id: uint64, outLen: ptr csize_t): ptr uint8 {.exportc, cdecl.} =
  ## Get type name by id. Caller must free result with ct_free_buffer.
  if h.isNil or outLen.isNil: return nil
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].typeName(id)
  if res.isErr:
    setError(res.error)
    return nil
  allocStringResult(res.get(), outLen)

proc ct_reader_varname(h: pointer, id: uint64, outLen: ptr csize_t): ptr uint8 {.exportc, cdecl.} =
  ## Get variable name by id. Caller must free result with ct_free_buffer.
  if h.isNil or outLen.isNil: return nil
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].varname(id)
  if res.isErr:
    setError(res.error)
    return nil
  allocStringResult(res.get(), outLen)

proc ct_reader_path_count(h: pointer): uint64 {.exportc, cdecl.} =
  if h.isNil: return 0
  let rh = cast[TraceReaderHandle](h)
  rh[].pathCount()

proc ct_reader_function_count(h: pointer): uint64 {.exportc, cdecl.} =
  if h.isNil: return 0
  let rh = cast[TraceReaderHandle](h)
  rh[].functionCount()

proc ct_reader_type_count(h: pointer): uint64 {.exportc, cdecl.} =
  if h.isNil: return 0
  let rh = cast[TraceReaderHandle](h)
  rh[].typeCount()

proc ct_reader_varname_count(h: pointer): uint64 {.exportc, cdecl.} =
  if h.isNil: return 0
  let rh = cast[TraceReaderHandle](h)
  rh[].varnameCount()

# ---------------------------------------------------------------------------
# Step access — returns JSON-encoded step event
# ---------------------------------------------------------------------------

proc ct_reader_step(h: pointer, n: uint64, outLen: ptr csize_t): ptr uint8 {.exportc, cdecl.} =
  ## Returns step event N as JSON bytes. Caller must free with ct_free_buffer.
  if h.isNil or outLen.isNil: return nil
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].step(n)
  if res.isErr:
    setError(res.error)
    return nil
  allocJsonResult(stepEventToJson(res.get()), outLen)

# ---------------------------------------------------------------------------
# Value access — returns JSON-encoded values array
# ---------------------------------------------------------------------------

proc ct_reader_values(h: pointer, n: uint64, outLen: ptr csize_t): ptr uint8 {.exportc, cdecl.} =
  ## Returns variable values for step N as JSON array. Caller must free with ct_free_buffer.
  if h.isNil or outLen.isNil: return nil
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].values(n)
  if res.isErr:
    setError(res.error)
    return nil
  allocJsonResult(variableValuesToJson(res.get()), outLen)

# ---------------------------------------------------------------------------
# Call access — returns JSON-encoded call record
# ---------------------------------------------------------------------------

proc ct_reader_call(h: pointer, key: uint64, outLen: ptr csize_t): ptr uint8 {.exportc, cdecl.} =
  ## Returns call record by key as JSON. Caller must free with ct_free_buffer.
  if h.isNil or outLen.isNil: return nil
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].call(key)
  if res.isErr:
    setError(res.error)
    return nil
  allocJsonResult(callRecordToJson(res.get()), outLen)

proc ct_reader_call_for_step(h: pointer, stepId: uint64, outLen: ptr csize_t): ptr uint8 {.exportc, cdecl.} =
  ## Returns the innermost call record enclosing the given step as JSON.
  ## Caller must free with ct_free_buffer.
  if h.isNil or outLen.isNil: return nil
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].callForStep(stepId)
  if res.isErr:
    setError(res.error)
    return nil
  allocJsonResult(callRecordToJson(res.get()), outLen)

# ---------------------------------------------------------------------------
# IO Event access — returns JSON-encoded IO event
# ---------------------------------------------------------------------------

proc ct_reader_event(h: pointer, index: uint64, outLen: ptr csize_t): ptr uint8 {.exportc, cdecl.} =
  ## Returns IO event by index as JSON. Caller must free with ct_free_buffer.
  if h.isNil or outLen.isNil: return nil
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].ioEvent(index)
  if res.isErr:
    setError(res.error)
    return nil
  allocJsonResult(ioEventToJson(res.get()), outLen)

# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

proc ct_reader_program(h: pointer, outLen: ptr csize_t): ptr uint8 {.exportc, cdecl.} =
  ## Get program name from trace metadata. Caller must free with ct_free_buffer.
  if h.isNil or outLen.isNil: return nil
  let rh = cast[TraceReaderHandle](h)
  allocStringResult(rh[].meta.program, outLen)

proc ct_reader_workdir(h: pointer, outLen: ptr csize_t): ptr uint8 {.exportc, cdecl.} =
  ## Get workdir from trace metadata. Caller must free with ct_free_buffer.
  if h.isNil or outLen.isNil: return nil
  let rh = cast[TraceReaderHandle](h)
  allocStringResult(rh[].meta.workdir, outLen)

# ===========================================================================
# Structured reader FFI — binary accessors that avoid JSON parsing
# ===========================================================================
#
# These functions return data through output pointer parameters rather than
# JSON strings, eliminating allocation and parsing overhead on the hot path.
# The Rust caller populates Db arrays directly from these.

# ---------------------------------------------------------------------------
# Global line index resolution helper
# ---------------------------------------------------------------------------
#
# The multi-stream writer encodes steps as globalLineIndex values using a
# prefix-sum over per-file line counts (DefaultLinesPerFile = 100_000 per
# file). To convert back to (path_id, line), the reader must reconstruct
# the same prefix-sum. We build it lazily from the path count.

import codetracer_trace_writer/global_line_index

proc getOrBuildGli(rh: TraceReaderHandle): GlobalLineIndex =
  ## Build a GlobalLineIndex from the reader's path count, using the same
  ## DefaultLinesPerFile constant the writer uses.
  let pathCount = rh[].pathCount()
  var counts = newSeq[uint64](int(pathCount))
  for i in 0 ..< int(pathCount):
    counts[i] = DefaultLinesPerFile
  buildGlobalLineIndex(counts)

# ---------------------------------------------------------------------------
# ct_reader_step_location — resolve step N to (path_id, line)
# ---------------------------------------------------------------------------

proc ct_reader_step_location(
    h: pointer, n: uint64,
    outPathId: ptr uint64, outLine: ptr uint64
): cint {.exportc, cdecl.} =
  ## Resolve step N to its source location (path_id, line).
  ## Returns 0 on success, non-zero on failure.
  ##
  ## Internally, steps are stored as globalLineIndex values (a prefix-sum
  ## encoding of path_id + line). This function resolves deltas within
  ## the exec stream chunk and then maps the absolute GLI back to
  ## (path_id, line) using the same DefaultLinesPerFile the writer used.
  if h.isNil or outPathId.isNil or outLine.isNil:
    setError("NULL parameter")
    return 1.cint
  let rh = cast[TraceReaderHandle](h)

  # Get the absolute global line index (handles delta resolution)
  let gliRes = rh[].stepAbsoluteGlobalLineIndex(n)
  if gliRes.isErr:
    setError(gliRes.error)
    return 1.cint
  let globalIdx = gliRes.get()

  # Resolve GLI to (path_id, line) using the same prefix-sum the writer used
  let gli = getOrBuildGli(rh)
  let (pathId, line) = gli.resolve(globalIdx)
  outPathId[] = uint64(pathId)
  outLine[] = line
  0.cint

# ---------------------------------------------------------------------------
# ct_reader_step_value_count / ct_reader_step_value — structured value access
# ---------------------------------------------------------------------------

proc ct_reader_step_value_count(
    h: pointer, n: uint64
): uint64 {.exportc, cdecl.} =
  ## Returns the number of variable values at step N.
  ## Returns 0 on failure.
  if h.isNil: return 0
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].values(n)
  if res.isErr:
    setError(res.error)
    return 0
  uint64(res.get().len)

proc ct_reader_step_value(
    h: pointer, n: uint64, valueIdx: uint64,
    outVarnameId: ptr uint64, outTypeId: ptr uint64,
    outData: ptr ptr uint8, outDataLen: ptr csize_t
): cint {.exportc, cdecl.} =
  ## Returns the variable value at (step N, value index valueIdx).
  ## The data pointer points to a heap-allocated copy that the caller
  ## must free with ct_free_buffer.
  ## Returns 0 on success, non-zero on failure.
  if h.isNil or outVarnameId.isNil or outTypeId.isNil or
      outData.isNil or outDataLen.isNil:
    setError("NULL parameter")
    return 1.cint
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].values(n)
  if res.isErr:
    setError(res.error)
    return 1.cint
  let vals = res.get()
  if int(valueIdx) >= vals.len:
    setError("value index " & $valueIdx & " out of range (count=" & $vals.len & ")")
    return 1.cint
  let v = vals[int(valueIdx)]
  outVarnameId[] = v.varnameId
  outTypeId[] = v.typeId
  if v.data.len == 0:
    outData[] = nil
    outDataLen[] = 0.csize_t
  else:
    let buf = cast[ptr uint8](alloc(v.data.len))
    if buf.isNil:
      setError("allocation failed")
      return 1.cint
    copyMem(buf, unsafeAddr v.data[0], v.data.len)
    outData[] = buf
    outDataLen[] = csize_t(v.data.len)
  0.cint

# ---------------------------------------------------------------------------
# ct_reader_call_fields / ct_reader_call_child — structured call access
# ---------------------------------------------------------------------------

proc ct_reader_call_fields(
    h: pointer, key: uint64,
    outFunctionId: ptr uint64, outParentKey: ptr int64,
    outEntryStep: ptr uint64, outExitStep: ptr uint64,
    outDepth: ptr uint32, outChildrenCount: ptr uint64
): cint {.exportc, cdecl.} =
  ## Returns the scalar fields of call record `key`.
  ## Returns 0 on success, non-zero on failure.
  if h.isNil or outFunctionId.isNil or outParentKey.isNil or
      outEntryStep.isNil or outExitStep.isNil or
      outDepth.isNil or outChildrenCount.isNil:
    setError("NULL parameter")
    return 1.cint
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].call(key)
  if res.isErr:
    setError(res.error)
    return 1.cint
  let rec = res.get()
  outFunctionId[] = rec.functionId
  outParentKey[] = rec.parentCallKey
  outEntryStep[] = rec.entryStep
  outExitStep[] = rec.exitStep
  outDepth[] = uint32(rec.depth)
  outChildrenCount[] = uint64(rec.children.len)
  0.cint

proc ct_reader_call_child(
    h: pointer, key: uint64, childIdx: uint64
): uint64 {.exportc, cdecl.} =
  ## Returns the call_key of child at index `childIdx` within call `key`.
  ## Returns uint64.high on failure.
  if h.isNil: return high(uint64)
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].call(key)
  if res.isErr:
    setError(res.error)
    return high(uint64)
  let rec = res.get()
  if int(childIdx) >= rec.children.len:
    setError("child index " & $childIdx & " out of range")
    return high(uint64)
  rec.children[int(childIdx)]

proc ct_reader_call_arg_count(
    h: pointer, key: uint64
): uint64 {.exportc, cdecl.} =
  ## Returns the number of arguments captured for call ``key``.
  ## Returns 0 on failure (and sets the last error).
  if h.isNil:
    setError("NULL handle")
    return 0
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].call(key)
  if res.isErr:
    setError(res.error)
    return 0
  uint64(res.get().args.len)

proc ct_reader_call_arg(
    h: pointer, key: uint64, argIdx: uint64,
    outVarnameId: ptr uint64,
    outData: ptr ptr uint8, outDataLen: ptr csize_t
): cint {.exportc, cdecl.} =
  ## Returns the (varname_id, CBOR-encoded value) pair of argument ``argIdx``
  ## within call ``key``. The data pointer is heap-allocated; caller must free
  ## with ``ct_free_buffer``. Returns 0 on success, non-zero on failure.
  if h.isNil or outVarnameId.isNil or outData.isNil or outDataLen.isNil:
    setError("NULL parameter")
    return 1.cint
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].call(key)
  if res.isErr:
    setError(res.error)
    return 1.cint
  let rec = res.get()
  if int(argIdx) >= rec.args.len:
    setError("arg index " & $argIdx & " out of range")
    return 1.cint
  let arg = rec.args[int(argIdx)]
  outVarnameId[] = arg.varnameId
  if arg.value.len == 0:
    outData[] = nil
    outDataLen[] = 0.csize_t
  else:
    let buf = cast[ptr uint8](alloc(arg.value.len))
    if buf.isNil:
      setError("allocation failed")
      return 1.cint
    copyMem(buf, unsafeAddr arg.value[0], arg.value.len)
    outData[] = buf
    outDataLen[] = csize_t(arg.value.len)
  0.cint

# ---------------------------------------------------------------------------
# ct_reader_event_fields — structured IO event access
# ---------------------------------------------------------------------------

proc ct_reader_event_fields(
    h: pointer, index: uint64,
    outKind: ptr uint8, outStepId: ptr uint64,
    outData: ptr ptr uint8, outDataLen: ptr csize_t
): cint {.exportc, cdecl.} =
  ## Returns the fields of IO event at `index`.
  ## The data pointer is heap-allocated; caller must free with ct_free_buffer.
  ## kind values: 0=stdout, 1=stderr, 2=file_op, 3=error.
  ## Returns 0 on success, non-zero on failure.
  if h.isNil or outKind.isNil or outStepId.isNil or
      outData.isNil or outDataLen.isNil:
    setError("NULL parameter")
    return 1.cint
  let rh = cast[TraceReaderHandle](h)
  let res = rh[].ioEvent(index)
  if res.isErr:
    setError(res.error)
    return 1.cint
  let ev = res.get()
  outKind[] = uint8(ev.kind)
  outStepId[] = ev.stepId
  if ev.data.len == 0:
    outData[] = nil
    outDataLen[] = 0.csize_t
  else:
    let buf = cast[ptr uint8](alloc(ev.data.len))
    if buf.isNil:
      setError("allocation failed")
      return 1.cint
    copyMem(buf, unsafeAddr ev.data[0], ev.data.len)
    outData[] = buf
    outDataLen[] = csize_t(ev.data.len)
  0.cint

# ---------------------------------------------------------------------------
# NimMain — required for static/shared lib initialization
# ---------------------------------------------------------------------------

proc NimMain() {.importc.}

proc codetracer_trace_writer_init() {.exportc, cdecl.} =
  ## Call this once before using any other function if linking as a static lib.
  ## For shared libs (.so/.dylib), this is called automatically via a constructor.
  NimMain()
