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

proc zeroLineCountDiagnostic*(path: string): string =
  ## THE named diagnostic for a line-count-table record offered without a
  ## count. Extracted into one proc rather than duplicated because two
  ## call sites now raise it — ``ensureQualifiedPathIdWithLineCount``
  ## (the deduping registration) and ``appendQualifiedPathWithLineCount``
  ## (the versioned append) — and a second version of a file is exactly
  ## the case where an implementer is tempted to let the count slide.
  ## Two spellings of "the same" refusal is how a gate that asserts the
  ## refusal by name stops discriminating.
  "paths.dat: a line-count-table record needs a non-zero " &
    "line_count for " & path & " — a file sized 0 shares its base with " &
    "the next file, and the two are indistinguishable at decode. A " &
    "writer that cannot count the file's lines records the ceiling it " &
    "uses instead"

proc appendQualifiedPathWithLineCount*(ctfs: var Ctfs,
    it: var InterningTableWriter, qualifier, path: string,
    lineCount: uint64): Result[uint64, string] =
  ## Append a line-count-table ``paths.dat`` record for ``path`` WITHOUT
  ## consulting the dedup lookup — the versioned-path registration
  ## (design §6.1, ``registerPathVersion``).
  ##
  ## The record layout is exactly ``ensureQualifiedPathIdWithLineCount``'s
  ## — ``payload_len + payload + line_count`` — and the payload is
  ## byte-identical to the one an earlier version of the same path
  ## already stored. That is the whole point: **a version is a path
  ## index, never a path string.** Nothing is appended to the payload, no
  ## generation is interposed into it, and no suffix is added, because
  ## every consumer that resolves a user-supplied path resolves it by
  ## string.
  ##
  ## The lookup is UPDATED to the new id rather than left alone. A path
  ## string has exactly one *current* version, and the interning table is
  ## the only place a later bare ``registerPath(path)`` can learn it; a
  ## lookup left pointing at the superseded record would make the writer
  ## and its own interning table disagree about which file a step-by-name
  ## belongs to — the same class of mirrored-state defect that design
  ## §6.4 requires deleting from the Godot fork, reintroduced one layer
  ## down.
  let payload = qualifiedPayload(qualifier, path)
  if lineCount == 0:
    return err(zeroLineCountDiagnostic(path))

  let id = it.nextId
  it.nextId += 1

  var record: seq[byte] = @[]
  encodeVarint(uint64(payload.len), record)
  for i in 0 ..< payload.len:
    record.add(byte(payload[i]))
  encodeVarint(lineCount, record)

  let appendRes = ctfs.append(it.table, record)
  if appendRes.isErr:
    return err(appendRes.error)

  it.lookup[payload] = id
  ok(id)

proc ensureQualifiedPathIdWithLineCount*(ctfs: var Ctfs,
    it: var InterningTableWriter, qualifier, path: string,
    lineCount: uint64): Result[uint64, string] =
  ## Line-count-table `paths.dat` record, written when the trace sets
  ## ``meta.dat`` ``FlagHasLineCountTable`` (bit 14):
  ##
  ## ```
  ## payload_len: varint
  ## payload:     [u8] × payload_len
  ## line_count:  varint
  ## ```
  ##
  ## The framing is the first three fields of the column-aware Layout A
  ## record (``ensurePathIdColumnAware``) and stops there: a line-only
  ## trace addresses lines, so the file's size is its line count and
  ## there is no per-line table to follow. Which of the two a reader
  ## decodes is decided by the ``meta.dat`` bits, never by inspecting the
  ## bytes — the record spaces overlap.
  ##
  ## ``payload`` is the interning payload, so a qualified producer's
  ## ``qualifier & US & path`` round-trips exactly as it does in the bare
  ## layout (``qualifiedPayload`` / ``splitInterningPayload``); dedup is
  ## keyed on the same payload for the same reason.
  ##
  ## ``lineCount`` is the number of lines the file has, and it is what
  ## the file's slot in the global position space is sized to. It is
  ## required: a record without one would put the reader back to
  ## assuming a size, which is what this layout exists to remove. A
  ## caller that cannot determine a file's real line count passes the
  ## ``DefaultLinesPerFile`` ceiling it intends to use, so that the
  ## number the space was laid out with is the number on the wire.
  let payload = qualifiedPayload(qualifier, path)
  let existing = it.lookup.getOrDefault(payload, high(uint64))
  if existing != high(uint64):
    # An already-interned path has already had its count written; this
    # call writes no record, so it has nothing to require. The dedup
    # lookup therefore comes BEFORE the count check rather than after
    # it: recorders re-name a path on every step, and the count belongs
    # to the record, not to the call.
    return ok(existing)
  if lineCount == 0:
    return err(zeroLineCountDiagnostic(path))

  let id = it.nextId
  it.nextId += 1

  var record: seq[byte] = @[]
  encodeVarint(uint64(payload.len), record)
  for i in 0 ..< payload.len:
    record.add(byte(payload[i]))
  encodeVarint(lineCount, record)

  let appendRes = ctfs.append(it.table, record)
  if appendRes.isErr:
    return err(appendRes.error)

  it.lookup[payload] = id
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

# ---------------------------------------------------------------------------
# Spec record shapes for `funcs.dat` and `types.dat`
# ---------------------------------------------------------------------------
#
# TWO OF THE FOUR TABLES ARE NOT BARE BYTES, and this module used to treat all
# four as if they were. `internal-files.md` gives the four record shapes:
#
#   | paths.dat    | raw bytes (path) (+ line-count / Layout A table)  |
#   | varnames.dat | raw bytes (name)                                  |
#   | types.dat    | kind: u8, lang_type_len: varint, lang_type, specific_info |
#   | funcs.dat    | global_line_index: varint, name_len: varint, name |
#
# Paths and varnames are raw bytes, so the generic table above is right for
# them. Functions and types are structured, and writing them through the same
# generic path produced records a conforming reader misreads: the Rust writer
# emits the spec shape, and reading it as bare bytes yields the varint bytes as
# part of the name — measured, as a `functions` array entry beginning `\xa6\x8d`
# before `token::transfer`, which is not even valid UTF-8.
#
# `specific_info` is the CBOR of `TypeSpecificInfo`, matching the Rust writer;
# `None` is the four-character text string, five bytes.

const TypeSpecificInfoNoneCbor* = [0x64'u8, 0x4e, 0x6f, 0x6e, 0x65]
  ## CBOR of `TypeSpecificInfo::None` — text(4) "None". Serde encodes a unit
  ## enum variant as its name, and the Rust writer stores exactly this.

proc encodeFuncRecord*(globalLineIndex: uint64, name: string): seq[byte] =
  ## `funcs.dat` record: `global_line_index: varint, name_len: varint, name`.
  result = @[]
  encodeVarint(globalLineIndex, result)
  encodeVarint(uint64(name.len), result)
  for ch in name:
    result.add(byte(ch))

proc decodeFuncRecord*(data: openArray[byte]):
    Result[tuple[globalLineIndex: uint64, name: string], string] =
  ## Inverse of `encodeFuncRecord`. Refuses a truncated record by name rather
  ## than returning a short string, because a silently short function name is
  ## indistinguishable from a real one.
  var pos = 0
  let gli = ?decodeVarint(data, pos)
  let nameLen = ?decodeVarint(data, pos)
  if uint64(data.len - pos) < nameLen:
    return err("funcs.dat record is truncated: declares a " & $nameLen &
      "-byte name with only " & $(data.len - pos) & " bytes left")
  var name = newString(int(nameLen))
  for i in 0 ..< int(nameLen):
    name[i] = char(data[pos + i])
  ok((globalLineIndex: gli, name: name))

proc encodeTypeRecord*(kind: uint8, langType: string,
    specificInfo: openArray[byte] = TypeSpecificInfoNoneCbor): seq[byte] =
  ## `types.dat` record: `kind: u8, lang_type_len: varint, lang_type,
  ## specific_info`.
  result = @[kind]
  encodeVarint(uint64(langType.len), result)
  for ch in langType:
    result.add(byte(ch))
  for b in specificInfo:
    result.add(b)

proc decodeTypeRecord*(data: openArray[byte]):
    Result[tuple[kind: uint8, langType: string], string] =
  ## Inverse of `encodeTypeRecord`, for the two leading fields. The trailing
  ## `specific_info` blob is left to the caller: nothing in this library needs
  ## it decoded, and decoding CBOR here would pull a dependency into a module
  ## that is compiled for freestanding targets.
  if data.len < 1:
    return err("types.dat record is empty: it must carry at least the kind byte")
  var pos = 1
  let langLen = ?decodeVarint(data, pos)
  if uint64(data.len - pos) < langLen:
    return err("types.dat record is truncated: declares a " & $langLen &
      "-byte lang_type with only " & $(data.len - pos) & " bytes left")
  var lang = newString(int(langLen))
  for i in 0 ..< int(langLen):
    lang[i] = char(data[pos + i])
  ok((kind: data[0], langType: lang))

proc appendRecord*(ctfs: var Ctfs, it: var InterningTableWriter,
    record: openArray[byte]): Result[uint64, string] =
  ## Append a pre-encoded record and return its id, WITHOUT the string-keyed
  ## dedup the bare tables use.
  ##
  ## Dedup for the structured tables is the caller's, because their identity is
  ## not the payload: two functions with the same name at different declaration
  ## sites are two records, and the FFI keys its id space on the name alone. A
  ## payload-keyed dedup here would silently merge or split those.
  let id = it.nextId
  it.nextId += 1
  var bytes = newSeq[byte](record.len)
  for i in 0 ..< record.len:
    bytes[i] = record[i]
  ?ctfs.append(it.table, bytes)
  ok(id)

proc readFuncById*(r: InterningTableReader, id: uint64):
    Result[tuple[globalLineIndex: uint64, name: string], string] =
  ## Read a `funcs.dat` record in its spec shape.
  decodeFuncRecord(?r.table.read(id))

proc readTypeById*(r: InterningTableReader, id: uint64):
    Result[tuple[kind: uint8, langType: string], string] =
  ## Read a `types.dat` record in its spec shape.
  decodeTypeRecord(?r.table.read(id))

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

proc ensureMarkerLabelId*(ctfs: var Ctfs, it: var InterningTableWriter,
                          label: string): Result[uint64, string] =
  ## Intern a correlation-marker label, returning its numeric id.
  ##
  ## Deliberately NOT part of `TraceInterningTables`: those four tables are
  ## created eagerly for every trace, and adding a fifth there would put
  ## `markers.dat` / `markers.off` into every container ever written, whether
  ## or not it declares a marker.  The writer creates this table lazily on the
  ## first `ensureMarkerId`, so a recording with no markers is byte-identical
  ## to one written before markers existed.
  ctfs.ensureId(it, label)

proc ensurePathId*(ctfs: var Ctfs, t: var TraceInterningTables, path: string): Result[uint64, string] =
  ctfs.ensureId(t.paths, path)

proc ensurePathIdColumnAware*(ctfs: var Ctfs, t: var TraceInterningTables,
    path: string, lineLengths: openArray[uint32]): Result[uint64, string] =
  ## Wrapper for the column-aware paths.dat record encoding (Layout A).
  ## See ``ensurePathIdColumnAware`` on ``InterningTableWriter`` for the
  ## on-disk layout.
  ctfs.ensurePathIdColumnAware(t.paths, path, lineLengths)

proc ensureStructuredId*(ctfs: var Ctfs, it: var InterningTableWriter,
    key: string, record: openArray[byte]): Result[uint64, string] =
  ## Intern a STRUCTURED record under a string key.
  ##
  ## The bare tables key their dedup on the payload because for them the payload
  ## IS the key. `funcs.dat` and `types.dat` records are not: the same name can
  ## encode to different bytes depending on the declaration site or the kind, so
  ## the key is passed separately and the record is appended as given.
  let existing = it.lookup.getOrDefault(key, high(uint64))
  if existing != high(uint64):
    return ok(existing)
  let id = ?ctfs.appendRecord(it, record)
  it.lookup[key] = id
  ok(id)

proc ensureFunctionId*(ctfs: var Ctfs, t: var TraceInterningTables, name: string): Result[uint64, string] =
  ## Intern a function with no declaration site.
  ##
  ## Writes the SPEC record shape (`internal-files.md:46`) with a
  ## `global_line_index` of 0 — the address of line 1 of the first file, which
  ## is what an unspecified site resolves to. A caller that knows the site uses
  ## `MultiStreamTraceWriter.registerFunctionAt`, which computes the real
  ## address at close.
  ctfs.ensureStructuredId(t.funcs, name, encodeFuncRecord(0, name))

proc ensureTypeId*(ctfs: var Ctfs, t: var TraceInterningTables, name: string,
    kind: uint8 = 0): Result[uint64, string] =
  ## Intern a type in the SPEC record shape (`internal-files.md:45`).
  ctfs.ensureStructuredId(t.types, name, encodeTypeRecord(kind, name))

proc ensureVarnameId*(ctfs: var Ctfs, t: var TraceInterningTables, name: string): Result[uint64, string] =
  ctfs.ensureId(t.varnames, name)

# Qualifier-aware wrappers. These do not replace the bare forms above (existing
# callers — multi_stream_writer.nim, MCR — keep compiling unchanged); they let a
# producer that opts into fully-qualified keys intern through the same owner
# tables. An empty qualifier is byte-identical to the bare form.

proc ensureQualifiedPathId*(ctfs: var Ctfs, t: var TraceInterningTables,
    qualifier, path: string): Result[uint64, string] =
  ctfs.ensureQualifiedId(t.paths, qualifier, path)

proc ensureQualifiedPathIdWithLineCount*(ctfs: var Ctfs,
    t: var TraceInterningTables, qualifier, path: string,
    lineCount: uint64): Result[uint64, string] =
  ## Wrapper for the line-count-table `paths.dat` record encoding.  See
  ## ``ensureQualifiedPathIdWithLineCount`` on ``InterningTableWriter``
  ## for the on-disk layout.
  ctfs.ensureQualifiedPathIdWithLineCount(t.paths, qualifier, path, lineCount)

proc appendQualifiedPathWithLineCount*(ctfs: var Ctfs,
    t: var TraceInterningTables, qualifier, path: string,
    lineCount: uint64): Result[uint64, string] =
  ## Wrapper for the NON-deduping versioned append (GDH-M1, design §6.1).
  ## See ``appendQualifiedPathWithLineCount`` on ``InterningTableWriter``.
  ctfs.appendQualifiedPathWithLineCount(t.paths, qualifier, path, lineCount)

proc ensureQualifiedFunctionId*(ctfs: var Ctfs, t: var TraceInterningTables,
    qualifier, name: string): Result[uint64, string] =
  let key = qualifiedPayload(qualifier, name)
  ctfs.ensureStructuredId(t.funcs, key, encodeFuncRecord(0, key))

proc ensureQualifiedTypeId*(ctfs: var Ctfs, t: var TraceInterningTables,
    qualifier, name: string): Result[uint64, string] =
  let key = qualifiedPayload(qualifier, name)
  ctfs.ensureStructuredId(t.types, key, encodeTypeRecord(0, key))

proc ensureQualifiedVarnameId*(ctfs: var Ctfs, t: var TraceInterningTables,
    qualifier, name: string): Result[uint64, string] =
  ctfs.ensureQualifiedId(t.varnames, qualifier, name)
