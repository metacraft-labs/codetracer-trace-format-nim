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
import std/tables
import std/os

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
    writer: TraceWriter
    writerReady: bool  # true once .ct file has been created
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
  if handle.writerReady and not handle.writer.closed:
    discard handle.writer.close()
  `=destroy`(handle[])
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
  if handle.writerReady:
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

proc trace_writer_register_call(
    handle: TraceWriterHandle,
    function_id: csize_t,
) {.exportc, cdecl.} =
  ## Register a call to the function identified by function_id.
  if handle.isNil:
    return
  discard handle.writer.writeCall(uint64(function_id))

proc trace_writer_register_return(handle: TraceWriterHandle) {.exportc, cdecl.} =
  ## Register a function return with no explicit return value.
  if handle.isNil:
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

proc trace_writer_register_special_event(
    handle: TraceWriterHandle,
    kind: FfiEventLogKind,
    metadata: cstring,
    content: cstring,
) {.exportc, cdecl.} =
  ## Register an I/O or special event with optional metadata.
  if handle.isNil:
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
  if not handle.writerReady:
    # Writer was never opened — nothing to close
    return 0.cint
  let res = handle.writer.close()
  if res.isErr:
    setError(res.error)
    return 1.cint
  0.cint

# ---------------------------------------------------------------------------
# NimMain — required for static/shared lib initialization
# ---------------------------------------------------------------------------

proc NimMain() {.importc.}

proc codetracer_trace_writer_init() {.exportc, cdecl.} =
  ## Call this once before using any other function if linking as a static lib.
  ## For shared libs (.so/.dylib), this is called automatically via a constructor.
  NimMain()
