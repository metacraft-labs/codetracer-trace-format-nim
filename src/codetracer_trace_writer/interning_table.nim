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
import results
import ../codetracer_ctfs/types
import ../codetracer_ctfs/variable_record_table

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

proc ensureId*(ctfs: var Ctfs, it: var InterningTableWriter, name: string): Result[uint64, string] =
  ## Return the ID for name. If name hasn't been seen, append it to the table
  ## and return a new ID. If already interned, return the existing ID.
  let existing = it.lookup.getOrDefault(name, high(uint64))
  if existing != high(uint64):
    return ok(existing)

  let id = it.nextId
  it.nextId += 1

  # Convert string to bytes and append to the variable record table
  var nameBytes = newSeq[byte](name.len)
  for i in 0 ..< name.len:
    nameBytes[i] = byte(name[i])

  let appendRes = ctfs.append(it.table, nameBytes)
  if appendRes.isErr:
    return err(appendRes.error)

  it.lookup[name] = id
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

proc ensureFunctionId*(ctfs: var Ctfs, t: var TraceInterningTables, name: string): Result[uint64, string] =
  ctfs.ensureId(t.funcs, name)

proc ensureTypeId*(ctfs: var Ctfs, t: var TraceInterningTables, name: string): Result[uint64, string] =
  ctfs.ensureId(t.types, name)

proc ensureVarnameId*(ctfs: var Ctfs, t: var TraceInterningTables, name: string): Result[uint64, string] =
  ctfs.ensureId(t.varnames, name)
