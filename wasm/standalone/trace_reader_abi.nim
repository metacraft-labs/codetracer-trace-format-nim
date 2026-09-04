## The read-side C ABI of the freestanding reader probe, `include`d by both
## `trace_reader_only_standalone.nim` (reader alone) and
## `trace_reader_standalone.nim` (reader plus the writer that feeds the
## in-module round trip), so the two modules answer host queries with one
## implementation and their sizes differ only by what they actually link.

var
  input: seq[byte]
    ## The container the HOST hands in, through `ct_input_alloc`.
  reader: NewTraceReader
  readerOpen: bool
  strBuf: seq[byte]
    ## Landing pad for a string result, so the host can read it out of memory.

proc nimMain() {.importc: "NimMain", cdecl.}

proc ctInit() {.exportc: "ct_init", cdecl.} =
  ## `--noMain` means the host runs module-level initialisation itself.
  nimMain()

# ---------------------------------------------------------------------------
# Reading bytes the host supplies
# ---------------------------------------------------------------------------

proc ctInputAlloc(n: int32): pointer {.exportc: "ct_input_alloc", cdecl.} =
  ## Reserve `n` writable bytes for the host to copy a container into, and
  ## return their address in linear memory.
  if n <= 0:
    input = @[]
    return nil
  input = newSeq[byte](int(n))
  addr input[0]

proc ctInputLen(): int32 {.exportc: "ct_input_len", cdecl.} =
  int32(input.len)

proc ctVerifyInput(): int32 {.exportc: "ct_verify_input", cdecl.} =
  ## Run the corpus verifier over the host-supplied bytes. 0 means every
  ## decoded field matched; anything else names the first check that failed.
  verifyCorpus(input)

# ---------------------------------------------------------------------------
# Queries, so the host can check the decode rather than trust a return code
# ---------------------------------------------------------------------------

proc ctOpenInput(): int32 {.exportc: "ct_open_input", cdecl.} =
  ## Open the host-supplied bytes as a trace. 0 on success.
  readerOpen = false
  let rr = openNewTraceFromBytes(input)
  if rr.isErr: return 1
  reader = rr.get()
  readerOpen = true
  0

proc ctStepCount(): int64 {.exportc: "ct_step_count", cdecl.} =
  if not readerOpen: return -1
  let r = reader.stepCount()
  if r.isErr: -2 else: int64(r.get())

proc ctPathCount(): int64 {.exportc: "ct_path_count", cdecl.} =
  if not readerOpen: return -1
  int64(reader.pathCount())

proc ctFunctionCount(): int64 {.exportc: "ct_function_count", cdecl.} =
  if not readerOpen: return -1
  int64(reader.functionCount())

proc ctTypeCount(): int64 {.exportc: "ct_type_count", cdecl.} =
  if not readerOpen: return -1
  int64(reader.typeCount())

proc ctVarnameCount(): int64 {.exportc: "ct_varname_count", cdecl.} =
  if not readerOpen: return -1
  int64(reader.varnameCount())

proc ctCallCount(): int64 {.exportc: "ct_call_count", cdecl.} =
  if not readerOpen: return -1
  let r = reader.callCount()
  if r.isErr: -2 else: int64(r.get())

proc ctColumnAware(): int32 {.exportc: "ct_column_aware", cdecl.} =
  if not readerOpen: return -1
  if reader.meta.hasColumnAwareSteps: 1 else: 0

proc ctStepPosition(n: int64): int64 {.exportc: "ct_step_position", cdecl.} =
  ## The absolute `global_position_index` of step `n`.
  if not readerOpen: return -1
  let r = reader.stepAbsoluteGlobalLineIndex(uint64(n))
  if r.isErr: -2 else: int64(r.get())

proc ctPosFile(p: int64): int64 {.exportc: "ct_pos_file", cdecl.} =
  if not readerOpen: return -1
  let r = reader.decodeGlobalPositionIndex(uint64(p))
  if r.isErr: -2 else: int64(r.get().file)

proc ctPosLine(p: int64): int64 {.exportc: "ct_pos_line", cdecl.} =
  if not readerOpen: return -1
  let r = reader.decodeGlobalPositionIndex(uint64(p))
  if r.isErr: -2 else: int64(r.get().line)

proc ctPosColumn(p: int64): int64 {.exportc: "ct_pos_column", cdecl.} =
  if not readerOpen: return -1
  let r = reader.decodeGlobalPositionIndex(uint64(p))
  if r.isErr: -2 else: int64(r.get().column)

const
  StrKindPath = 0'i32
  StrKindFunction = 1'i32
  StrKindType = 2'i32
  StrKindVarname = 3'i32

proc ctStr(kind: int32, id: int64): int32 {.exportc: "ct_str", cdecl.} =
  ## Decode one interned string into `strBuf` and return its length, or -1.
  ## The host then reads `ct_str_len` bytes from `ct_str_ptr`.
  strBuf = @[]
  if not readerOpen: return -1
  let res =
    case kind
    of StrKindPath: reader.path(uint64(id))
    of StrKindFunction: reader.function(uint64(id))
    of StrKindType: reader.typeName(uint64(id))
    of StrKindVarname: reader.varname(uint64(id))
    else: results.err(Result[string, string], "unknown kind")
  if res.isErr: return -1
  let s = res.get()
  strBuf = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    strBuf[i] = byte(s[i])
  int32(strBuf.len)

proc ctStrPtr(): pointer {.exportc: "ct_str_ptr", cdecl.} =
  if strBuf.len == 0: nil else: addr strBuf[0]

proc ctStrLen(): int32 {.exportc: "ct_str_len", cdecl.} =
  int32(strBuf.len)
