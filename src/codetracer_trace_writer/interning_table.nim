when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## InterningTable: maps strings to sequential IDs backed by VariableRecordTable.
##
## The writer maintains an in-memory hash table for deduplication. Each unique
## string is appended to the underlying VariableRecordTable and assigned a
## monotonically increasing ID.
##
## The reader needs no hash table — it reads by index from VariableRecordTable.
##
## Used for: paths.dat/off, funcs.dat/off, types.dat/off, varnames.dat/off.

import std/tables
import std/strutils
import results
import ../codetracer_ctfs/types
import ../codetracer_ctfs/variable_record_table
import ./varint

type
  InterningTableWriter* = object
    table: VariableRecordTableWriter
    lookup: Table[string, uint64]  ## string -> ID mapping
    nextId: uint64

  InterningTableReader* = object
    table: VariableRecordTableReader

proc initInterningTableWriter*(ctfs: var Ctfs, baseName: string): Result[InterningTableWriter, string] =
  ## Create a new interning table with the given base name.
  ## Creates baseName.dat and baseName.off files.
  let tableRes = initVariableRecordTableWriter(ctfs, baseName)
  if tableRes.isErr:
    return err(tableRes.error)
  ok(InterningTableWriter(
    table: tableRes.get(),
    lookup: initTable[string, uint64](),
    nextId: 0
  ))

const InterningUnitSeparator* = '\x1f'
  ## ASCII unit separator (0x1f). Never a valid byte in a path or identifier,
  ## so it is used to join the qualifier and the bare name inside a single
  ## interning payload: ``qualifier & US & name``. See spec
  ## ``Interning-Table-Coexistence.md`` §3.

proc qualifiedPayload*(qualifier, name: string): string =
  ## Build the stored payload for a fully-qualified interning key.
  ##
  ## When ``qualifier == ""`` the BARE ``name`` is returned (no separator) —
  ## this is what guarantees byte-identical output to the pre-qualifier format
  ## for every existing/standalone trace. Otherwise the payload is
  ## ``qualifier & US & name``.
  if qualifier.len == 0:
    name
  else:
    qualifier & InterningUnitSeparator & name

proc ensureQualifiedId*(ctfs: var Ctfs, it: var InterningTableWriter,
    qualifier, name: string): Result[uint64, string] =
  ## Return the ID for the fully-qualified key ``(qualifier, name)``.
  ##
  ## The dedup lookup and the on-disk payload are keyed on the *composite*
  ## payload (``qualifiedPayload``), so the same bare ``name`` under two
  ## different qualifiers gets two distinct ids, while a genuine repeat of the
  ## same ``(qualifier, name)`` dedups to one id. An empty qualifier stores the
  ## bare name and is byte-identical to the old ``ensureId`` output.
  let payload = qualifiedPayload(qualifier, name)
  let existing = it.lookup.getOrDefault(payload, high(uint64))
  if existing != high(uint64):
    return ok(existing)

  let id = it.nextId
  it.nextId += 1

  # Convert payload to bytes and append to the variable record table
  var payloadBytes = newSeq[byte](payload.len)
  for i in 0 ..< payload.len:
    payloadBytes[i] = byte(payload[i])

  let appendRes = ctfs.append(it.table, payloadBytes)
  if appendRes.isErr:
    return err(appendRes.error)

  it.lookup[payload] = id
  ok(id)

proc ensureId*(ctfs: var Ctfs, it: var InterningTableWriter, name: string): Result[uint64, string] =
  ## Return the ID for name. If name hasn't been seen, append it to the table
  ## and return a new ID. If already interned, return the existing ID.
  ##
  ## This is the unqualified entry point: it interns through the SAME
  ## nextId/lookup/table mechanism as ``ensureQualifiedId`` with an empty
  ## qualifier, so there is a single allocation path and the bytes it produces
  ## are provably the old bytes.
  ensureQualifiedId(ctfs, it, "", name)

proc ensurePathIdColumnAware*(ctfs: var Ctfs, it: var InterningTableWriter,
    path: string, lineLengths: openArray[uint32]): Result[uint64, string] =
  ## P6.5 / Layout A: column-aware paths.dat record.
  ##
  ## The on-disk record encoding switches to a self-describing form
  ## when the trace's ``FLAG_HAS_COLUMN_AWARE_STEPS`` is set:
  ##
  ## ```
  ## path_len: varint
  ## path_bytes: [u8] × path_len
  ## line_count: varint
  ## line_lengths: [varint] × line_count
  ##     (line_lengths[0] is an absolute zigzag varint;
  ##      subsequent entries are zigzag-encoded deltas from the
  ##      previous line length)
  ## ```
  ##
  ## See ``codetracer-trace-format-spec/trace-events.md`` §"paths.dat
  ## per-line offset table" / "Layout A".
  ##
  ## ``lineLengths`` may be empty (the recorder has not surfaced
  ## per-line column counts yet) — in that case ``line_count = 0`` and
  ## no per-line varints are emitted, but the ``path_len`` prefix is
  ## still written so the reader can demarcate the path bytes from the
  ## (empty) trailing block.
  let existing = it.lookup.getOrDefault(path, high(uint64))
  if existing != high(uint64):
    return ok(existing)

  let id = it.nextId
  it.nextId += 1

  var record: seq[byte] = @[]
  encodeVarint(uint64(path.len), record)
  for i in 0 ..< path.len:
    record.add(byte(path[i]))
  encodeVarint(uint64(lineLengths.len), record)
  if lineLengths.len > 0:
    encodeSignedVarint(int64(lineLengths[0]), record)
    for i in 1 ..< lineLengths.len:
      let delta = int64(lineLengths[i]) - int64(lineLengths[i - 1])
      encodeSignedVarint(delta, record)

  let appendRes = ctfs.append(it.table, record)
  if appendRes.isErr:
    return err(appendRes.error)

  it.lookup[path] = id
  ok(id)

proc count*(it: InterningTableWriter): uint64 = it.nextId

# Reader

proc initInterningTableReader*(ctfsBytes: openArray[byte], baseName: string,
                                blockSize: uint32 = DefaultBlockSize,
                                maxEntries: uint32 = DefaultMaxRootEntries): Result[InterningTableReader, string] =
  ## Initialize a reader from raw CTFS container bytes.
  let tableRes = initVariableRecordTableReader(ctfsBytes, baseName, blockSize, maxEntries)
  if tableRes.isErr:
    return err(tableRes.error)
  ok(InterningTableReader(table: tableRes.get()))

proc readById*(r: InterningTableReader, id: uint64): Result[string, string] =
  ## Read the interned string by its ID.
  let dataRes = r.table.read(id)
  if dataRes.isErr:
    return err(dataRes.error)
  let data = dataRes.get()
  var s = newString(data.len)
  for i in 0 ..< data.len:
    s[i] = char(data[i])
  ok(s)

proc readRawById*(r: InterningTableReader,
    id: uint64): Result[seq[byte], string] =
  ## P6.5: read the raw record bytes for the given id.  Used by the
  ## column-aware paths.dat reader path: when the trace's column
  ## extension is on, paths.dat records carry a self-describing
  ## ``path_len + path_bytes + line_count + line_lengths`` layout that
  ## the caller needs to decode itself (``readById`` would surface the
  ## raw bytes as a Latin-1 string, mangling the trailing varints).
  r.table.read(id)

proc splitInterningPayload*(payload: string): tuple[qualifier, name: string] =
  ## Split a stored interning payload back into ``(qualifier, name)``.
  ##
  ## Splits on the FIRST unit separator (0x1f): present ⇒ ``(qualifier, name)``;
  ## absent ⇒ ``("", payload)``, the unqualified/single-producer case (every
  ## existing trace). Only the first separator is significant — any further
  ## 0x1f bytes are impossible in a well-formed payload but, if present, stay in
  ## ``name`` rather than being lost.
  let sep = payload.find(InterningUnitSeparator)
  if sep < 0:
    ("", payload)
  else:
    (payload[0 ..< sep], payload[sep + 1 .. ^1])

proc count*(r: InterningTableReader): uint64 = r.table.count()

# ---------------------------------------------------------------------------
# Convenience API: standard trace interning tables
# ---------------------------------------------------------------------------

type
  TraceInterningTables* = object
    paths*: InterningTableWriter
    funcs*: InterningTableWriter
    types*: InterningTableWriter
    varnames*: InterningTableWriter

proc initTraceInterningTables*(ctfs: var Ctfs): Result[TraceInterningTables, string] =
  ## Create the four standard interning tables for a trace.
  var t: TraceInterningTables
  t.paths = ?(initInterningTableWriter(ctfs, "paths"))
  t.funcs = ?(initInterningTableWriter(ctfs, "funcs"))
  t.types = ?(initInterningTableWriter(ctfs, "types"))
  t.varnames = ?(initInterningTableWriter(ctfs, "varnames"))
  ok(t)

proc ensurePathId*(ctfs: var Ctfs, t: var TraceInterningTables, path: string): Result[uint64, string] =
  ctfs.ensureId(t.paths, path)

proc ensurePathIdColumnAware*(ctfs: var Ctfs, t: var TraceInterningTables,
    path: string, lineLengths: openArray[uint32]): Result[uint64, string] =
  ## Wrapper for the column-aware paths.dat record encoding (Layout A).
  ## See ``ensurePathIdColumnAware`` on ``InterningTableWriter`` for the
  ## on-disk layout.
  ctfs.ensurePathIdColumnAware(t.paths, path, lineLengths)

proc ensureFunctionId*(ctfs: var Ctfs, t: var TraceInterningTables, name: string): Result[uint64, string] =
  ctfs.ensureId(t.funcs, name)

proc ensureTypeId*(ctfs: var Ctfs, t: var TraceInterningTables, name: string): Result[uint64, string] =
  ctfs.ensureId(t.types, name)

proc ensureVarnameId*(ctfs: var Ctfs, t: var TraceInterningTables, name: string): Result[uint64, string] =
  ctfs.ensureId(t.varnames, name)

# Qualifier-aware wrappers. These do not replace the bare forms above (existing
# callers — multi_stream_writer.nim, MCR — keep compiling unchanged); they let a
# producer that opts into fully-qualified keys intern through the same owner
# tables. An empty qualifier is byte-identical to the bare form.

proc ensureQualifiedPathId*(ctfs: var Ctfs, t: var TraceInterningTables,
    qualifier, path: string): Result[uint64, string] =
  ctfs.ensureQualifiedId(t.paths, qualifier, path)

proc ensureQualifiedFunctionId*(ctfs: var Ctfs, t: var TraceInterningTables,
    qualifier, name: string): Result[uint64, string] =
  ctfs.ensureQualifiedId(t.funcs, qualifier, name)

proc ensureQualifiedTypeId*(ctfs: var Ctfs, t: var TraceInterningTables,
    qualifier, name: string): Result[uint64, string] =
  ctfs.ensureQualifiedId(t.types, qualifier, name)

proc ensureQualifiedVarnameId*(ctfs: var Ctfs, t: var TraceInterningTables,
    qualifier, name: string): Result[uint64, string] =
  ctfs.ensureQualifiedId(t.varnames, qualifier, name)
