{.push raises: [].}

## Seek-based trace reader (M18 + M19).
##
## Opens a multi-stream CTFS trace and provides random access to all data.
## Interning tables are loaded eagerly at startup; execution, value, call,
## and IO-event streams are initialized lazily on first access.

import results
import std/options
import std/tables
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ../codetracer_ctfs/variable_record_table
import ../codetracer_ctfs/member_view
import ./meta_dat
import ./interning_table
import ./exec_stream
import ./value_stream
import ./call_stream
import ./io_event_stream
import ./step_encoding
import ./varint
import ./global_line_index
import ../codetracer_ctfs/compact

# What `values` returns, and its type id, read from the value's CBOR.
export value_stream.VariableValue, value_stream.typeId

const ctHasFilesystem* = defined(posix) or defined(windows)
  ## Whether the target this reader is being compiled for has an
  ## operating-system filesystem.
  ##
  ## Derived from the target rather than from a define a caller must remember:
  ## `--os:linux` / `--os:macosx` define `posix`, `--os:windows` defines
  ## `windows`, and the freestanding targets (`--os:any`, `--os:standalone`)
  ## define neither. `container_append.nim` gates `fsync` the same way.
  ##
  ## Everything this reader can do is reachable through
  ## `openNewTraceFromBytes`, which is unconditional. Only the two entry points
  ## that take a **path** — `openNewTrace` and `refresh` — sit behind this
  ## constant, so a freestanding build loses the door, not the room.

when ctHasFilesystem:
  import std/os

type
  SourceView* = object
    ## Decoded shape of one ``source_views.dat`` record.  See
    ## ``codetracer-trace-format-spec/internal-files.md`` §
    ## "Alternate Source Views (Deminification Support)".
    pathId*: uint64
    viewKind*: uint8
    viewName*: string
    content*: seq[byte]
    sourcemapV3*: seq[byte]

  NewTraceReader* = object
    image: ContainerImage
      ## The container's bytes, which the interning tables and stream readers
      ## share and read their members out of in place.
    blockSize: uint32
    maxEntries: uint32

    # The address space step positions are encoded in, built once at open:
    # it is a prefix sum over every path, and `globalPositionSpace` is called
    # per step by a host that resolves steps one call at a time.
    posSpace: GlobalLineIndex

    # Metadata
    meta*: MetaDatContents

    # Interning tables (loaded at startup)
    pathReader: InterningTableReader
    funcReader: InterningTableReader
    typeReader: InterningTableReader
    varnameReader: InterningTableReader


    # P6.5 / Layout A — per-file line-length tables, parsed from the
    # column-aware paths.dat records when `meta.hasColumnAwareSteps`
    # is set.  ``lineLengths[fileId][line]`` is the addressable column
    # count of line (0-indexed) in file ``fileId``.  When the trace is
    # not column-aware, this stays empty and column queries return
    # ``none``.  An EMPTY entry is a ``line_count = 0`` record: the
    # conventional table (100000 lines of 1024), held as that rule.
    lineLengths: seq[seq[uint32]]
    # Per-file line counts, parsed from the line-count-table paths.dat
    # records when `meta.hasLineCountTable` is set.  ``lineCounts[fileId]``
    # is the number of lines in file ``fileId``, and it is what that
    # file's slot in the line-only global position space is sized to.
    # Empty on a trace that records no counts, in which case
    # ``positionSpaceCounts`` falls back to the pre-table convention.
    lineCounts: seq[uint64]
    # The interning payloads decoded out of those same records, kept so
    # `path` answers from the parse the open already did rather than
    # re-deriving the framing per call.  Parallel to `lineCounts`.
    lineCountPayloads: seq[string]
    # GDH-M1 / design §7.0 — the VERSION ORDINAL of every path entry,
    # 0-based and in path-id order: 0 for the first entry carrying a
    # given interning payload, 1 for the second, and so on.  Parallel to
    # the paths.dat records, one entry per id, computed on the first
    # version query (`ensurePathVersions`); empty until then.
    #
    # `paths.dat` needs no new layout for this: a second version of a
    # file is a second record with the SAME payload and its own line
    # count, so the ordinal is a property the container already states
    # and this is where it is recovered.  A legacy trace, whose payloads
    # are all distinct, yields 0 everywhere — which is exactly what
    # `Location.source_generation` already promises for such traces, so
    # no existing trace changes behaviour.
    #
    # Keyed on the interning PAYLOAD rather than on the split path
    # (`splitInterningPayload`): the qualifier is a producer namespace
    # (Interning-Table-Coexistence §2), so "gdscript\x1ffoo.gd" and
    # "mcr\x1ffoo.gd" are two producers' files and not two versions of
    # one.  Collapsing them would attribute a native recorder's file to
    # a VM reload.
    pathVersionOrdinals: seq[uint64]
    # Total number of entries sharing each id's payload.  `1` for every
    # id in a trace with no versioned paths.  Parallel to
    # `pathVersionOrdinals`.
    pathVersionTotals: seq[uint64]
    pathVersionsBuilt: bool
    pathVersionsError: string
      ## Why the ordinals could not be computed, when they could not.
    # Advisory: the trace declares line-only paths.dat records, yet every
    # record also decodes as a complete Layout A record.  Surfaced by
    # `columnAwarePathsSuspected`; never used to reinterpret data.
    layoutASuspected: bool
    # The `assumeColumnAwarePaths` override this handle was opened with,
    # kept so `refresh` re-opens the container under the same reading.
    assumedColumnAwarePaths: bool
    # Per-file cumulative line-base table (prefix sum of lineLengths).
    # Built lazily on first column resolution.  ``lineBase[fileId][l]``
    # is the in-file offset where line ``l`` starts in the file's
    # contiguous position range.
    lineBase: seq[seq[uint64]]
    # Per-file base in the global position space (prefix sum of each
    # file's ``sum(lineLengths)`` in column-aware mode, or
    # ``line_count`` in line-only mode).  ``fileBase[fileId]`` is the
    # ``global_position_index`` of the first position in that file.
    fileBase: seq[uint64]
    # Per-file size (sum of lineLengths in column-aware mode) used by
    # ``decodeGlobalPositionIndex``'s binary search.
    fileSize: seq[uint64]
    posTablesBuilt: bool

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

    # Alternate source views (spec §"Alternate Source Views
    # (Deminification Support)").  Parsed eagerly at open time when
    # ``meta.hasAlternateSourceViews`` is set so the random-access
    # accessors below (``sourceView``, ``sourceViewsForPath``) are
    # zero-cost on the hot path.  Empty on pre-extension traces — the
    # back-compat default is "no views".
    sourceViews: seq[SourceView]
    # Reverse index: ``sourceViewsByPath[pathId]`` is the list of
    # ``sourceViews`` indices whose ``pathId`` matches.  Built alongside
    # ``sourceViews`` so ``sourceViewsForPath`` is O(1) regardless of
    # how many views the trace carries.
    sourceViewsByPath: seq[seq[uint64]]

# ---------------------------------------------------------------------------
# paths.json
# ---------------------------------------------------------------------------
#
# ``paths.json`` was the only JSON document this reader ever parsed, and its
# schema was fixed by the spec: an array of source-path strings. Reading it
# with ``std/json`` cost far more than the schema did — ``parsejson`` reaches
# ``parseFloat``, which reaches libc's ``strtod``, which a freestanding target
# has no definition of. The module then failed to LINK for
# ``wasm32-unknown-unknown`` over a float parser no ``.ct`` container ever
# needed. The decoder below is the whole grammar the document could contain.
#
# The sidecar itself is retired and no reader path calls this any more. The
# decoder is kept because it is the only string-array parser here that a
# freestanding target can link, and because dropping it would take its
# escape/surrogate test corpus with it.

proc appendUtf8(dest: var string, cp: uint32) =
  ## Append one code point to ``dest`` in UTF-8, the encoding a Nim string
  ## holding a path is already assumed to be in.
  if cp < 0x80'u32:
    dest.add(char(uint8(cp)))
  elif cp < 0x800'u32:
    dest.add(char(uint8(0xC0'u32 or (cp shr 6))))
    dest.add(char(uint8(0x80'u32 or (cp and 0x3F'u32))))
  elif cp < 0x10000'u32:
    dest.add(char(uint8(0xE0'u32 or (cp shr 12))))
    dest.add(char(uint8(0x80'u32 or ((cp shr 6) and 0x3F'u32))))
    dest.add(char(uint8(0x80'u32 or (cp and 0x3F'u32))))
  else:
    dest.add(char(uint8(0xF0'u32 or (cp shr 18))))
    dest.add(char(uint8(0x80'u32 or ((cp shr 12) and 0x3F'u32))))
    dest.add(char(uint8(0x80'u32 or ((cp shr 6) and 0x3F'u32))))
    dest.add(char(uint8(0x80'u32 or (cp and 0x3F'u32))))

proc jsonSkipWs(text: string, i: var int) =
  while i < text.len and (text[i] == ' ' or text[i] == '\t' or
                          text[i] == '\n' or text[i] == '\r'):
    i += 1

proc jsonHex4(text: string, i: int, value: var uint32): bool =
  ## Read the four hex digits of a ``\uXXXX`` escape starting at ``i``.
  if i + 4 > text.len:
    return false
  value = 0
  for k in 0 ..< 4:
    let c = text[i + k]
    var d: uint32
    if c >= '0' and c <= '9': d = uint32(ord(c) - ord('0'))
    elif c >= 'a' and c <= 'f': d = uint32(ord(c) - ord('a') + 10)
    elif c >= 'A' and c <= 'F': d = uint32(ord(c) - ord('A') + 10)
    else: return false
    value = value * 16'u32 + d
  true

proc jsonParseString(text: string, i: var int, dest: var string): bool =
  ## Decode one JSON string starting at the opening quote ``text[i]``, leaving
  ## ``i`` just past the closing quote. Returns false on anything that is not
  ## a well-formed string, which the caller treats as a malformed document.
  if i >= text.len or text[i] != '"':
    return false
  i += 1
  dest = ""
  while i < text.len:
    let c = text[i]
    if c == '"':
      i += 1
      return true
    if c == '\\':
      i += 1
      if i >= text.len:
        return false
      let e = text[i]
      case e
      of '"', '\\', '/':
        dest.add(e)
        i += 1
      of 'b':
        dest.add('\b')
        i += 1
      of 'f':
        dest.add('\f')
        i += 1
      of 'n':
        dest.add('\n')
        i += 1
      of 'r':
        dest.add('\r')
        i += 1
      of 't':
        dest.add('\t')
        i += 1
      of 'u':
        i += 1
        var cp: uint32 = 0
        if not jsonHex4(text, i, cp):
          return false
        i += 4
        if cp >= 0xD800'u32 and cp <= 0xDBFF'u32:
          # A high surrogate carries only half a code point; the low half must
          # follow as a second escape or the document is malformed.
          if i + 1 >= text.len or text[i] != '\\' or text[i + 1] != 'u':
            return false
          i += 2
          var lo: uint32 = 0
          if not jsonHex4(text, i, lo):
            return false
          i += 4
          if lo < 0xDC00'u32 or lo > 0xDFFF'u32:
            return false
          cp = 0x10000'u32 + ((cp - 0xD800'u32) shl 10) + (lo - 0xDC00'u32)
        elif cp >= 0xDC00'u32 and cp <= 0xDFFF'u32:
          return false  # a low surrogate with no high half before it
        appendUtf8(dest, cp)
      else:
        return false
    else:
      if uint8(c) < 0x20'u8:
        return false  # an unescaped control character
      dest.add(c)
      i += 1
  false  # ran off the end before the closing quote

proc decodeJsonStringArray*(text: string): Option[seq[string]] =
  ## Decode ``["a", "b"]`` into its elements.
  ##
  ## Returns ``none`` for every document that is not an array of strings —
  ## including an array holding a number or an object, which this reader has
  ## no meaning for. ``none`` is the same outcome a malformed ``paths.json``
  ## has always had at the call site: the fallback list stays empty and path
  ## lookups fall through to the binary interning table's own error.
  var i = 0
  jsonSkipWs(text, i)
  if i >= text.len or text[i] != '[':
    return none(seq[string])
  i += 1
  var items: seq[string] = @[]
  jsonSkipWs(text, i)
  if i < text.len and text[i] == ']':
    i += 1
  else:
    while true:
      jsonSkipWs(text, i)
      var s = ""
      if not jsonParseString(text, i, s):
        return none(seq[string])
      items.add(s)
      jsonSkipWs(text, i)
      if i < text.len and text[i] == ',':
        i += 1
        continue
      if i < text.len and text[i] == ']':
        i += 1
        break
      return none(seq[string])
  jsonSkipWs(text, i)
  if i != text.len:
    return none(seq[string])
  some(items)

# ---------------------------------------------------------------------------
# paths.dat Layout A
# ---------------------------------------------------------------------------

const MaxProbeLineCount = 1_000_000
  ## Upper bound on the ``line_count`` a *probe* will accept before
  ## declaring a record not-Layout-A.  Sized to the largest source file
  ## we'd plausibly see (a few hundred thousand lines covers the Linux
  ## kernel's biggest translation unit).  The authoritative decode does
  ## not apply it: a trace that declares Layout A is trusted about its
  ## own line counts, and a corrupt count there surfaces as a decode
  ## error on the line-length varints that follow.

template refuse(message: string) {.dirty.} =
  ## `return err(message)`, for the record decoders below, which answer
  ## `bool` and name their refusal in `why`.
  why = message
  return false

template varintField(raw: openArray[byte], pos: var int,
    what: string): uint64 =
  ## The varint at `pos`, or a refusal naming `what` and why it does not
  ## decode.
  var v {.gensym.}: uint64
  if not readVarint(raw, pos, v):
    refuse(what & ": " & decodeVarint(raw, pos).unsafeError)
  v

proc decodeLayoutARecord(raw: openArray[byte], probe: bool,
    lls: var seq[uint32], why: var string): bool =
  ## One ``paths.dat`` record as Layout A, its line lengths into `lls`; see
  ## `parseLayoutAPathRecords` for `probe`.
  var pos = 0
  let pathLen = varintField(raw, pos, "column-aware path_len varint")
  if pathLen > uint64(raw.len - pos):
    refuse("path_bytes truncated")
  if probe:
    if pathLen == uint64(raw.len - pos):
      # No room left for the line_count varint after the path bytes.
      # A legacy record ends right after the raw path string.
      refuse("not Layout A (no line_count)")
    for k in pos ..< pos + int(pathLen):
      let b = raw[k]
      # Reject control characters except tab — paths are filesystem
      # names which the spec keeps within printable UTF-8 / ASCII.
      if b < 0x09'u8 or (b > 0x0D'u8 and b < 0x20'u8):
        refuse("not Layout A (control byte in path)")
  pos += int(pathLen)
  let lineCount = varintField(raw, pos, "column-aware line_count varint")
  if probe and lineCount > uint64(MaxProbeLineCount):
    refuse("not Layout A (line_count " & $lineCount & " exceeds probe bound)")
  lls = newSeq[uint32](int(lineCount))
  var prev: int64 = 0
  for l in 0 ..< int(lineCount):
    let z = varintField(raw, pos, "line_length[" & $l & "]")
    let d = if (z and 1) == 0: int64(z shr 1) else: not int64(z shr 1)
    let current = if l == 0: d else: prev + d
    if current < 0:
      refuse("line_length[" & $l & "] negative: " & $current)
    lls[l] = uint32(current)
    prev = current
  if probe and pos != raw.len:
    refuse("not Layout A (" & $(raw.len - pos) & " trailing byte(s))")
  true

proc decodeLineCountRecord(raw: openArray[byte], payload: var string,
    count: var uint64, why: var string): bool =
  ## One ``paths.dat`` record as the line-count-table layout; see
  ## `parseLineCountPathRecords`.
  var pos = 0
  let payloadLen = varintField(raw, pos, "line-count payload_len varint")
  if payloadLen > uint64(raw.len - pos):
    refuse("payload truncated (payload_len " & $payloadLen & ", " &
      $(raw.len - pos) & " byte(s) left)")
  payload = newString(int(payloadLen))
  if payloadLen > 0:
    copyMem(addr payload[0], unsafeAddr raw[pos], int(payloadLen))
  pos += int(payloadLen)
  count = varintField(raw, pos, "line_count varint")
  if count == 0:
    refuse("line_count is 0 for " & payload &
      ". A trace that sets FLAG_HAS_LINE_COUNT_TABLE states every " &
      "file's size, and a file sized 0 shares its base with the next " &
      "one — the two would be indistinguishable at decode")
  if pos != raw.len:
    refuse($(raw.len - pos) & " trailing byte(s) after line_count")
  true

proc parseLayoutAPathRecords(pathReader: InterningTableReader,
    probe: bool): Result[seq[seq[uint32]], string] =
  ## Decode every ``paths.dat`` record as Layout A —
  ## ``path_len + path_bytes + line_count + line_lengths`` (spec
  ## §"paths.dat per-line offset table") — and return the per-file
  ## line-length tables.
  ##
  ## ``probe`` selects between the two callers:
  ##
  ##   * ``probe = false`` (authoritative): the trace has *declared*
  ##     Layout A, so a record that does not decode is a corrupt trace
  ##     and the error names the record and the field that failed.
  ##   * ``probe = true`` (advisory): the caller is asking whether a
  ##     record set *could* be Layout A.  Extra structural conditions
  ##     apply — printable path bytes, a bounded line count, and a parse
  ##     that consumes the record exactly — and any failure is reported
  ##     as a plain "not Layout A" rather than as corruption.
  ##
  ## A ``probe`` that returns ``ok`` is NOT proof of Layout A.  The
  ## record space of the two layouts overlaps: a legitimate line-only
  ## record holding the 97-byte ASCII path
  ## ``'/' & 'a'.repeat(46) & "/0" & 'b'.repeat(48)`` decodes cleanly as
  ## Layout A (path_len 47 from the leading ``'/'``, line_count 48 from
  ## the ``'0'``, 48 line lengths from the ``'b'``s) and consumes the
  ## record exactly.  Treat the result as a hint about a suspected
  ## recorder bug, never as a licence to reinterpret the record.
  let pathTotal = pathReader.count()
  # Grown record by record: a probe usually stops at the first record, and
  # then never needed a table per path.
  var llsAll: seq[seq[uint32]]
  for i in 0'u64 ..< pathTotal:
    let rawRes = pathReader.readRawById(i)
    var lls: seq[uint32]
    var why: string
    if rawRes.isErr or not decodeLayoutARecord(rawRes.unsafeGet(), probe, lls,
        why):
      return err("paths.dat[" & $i & "]: " &
        (if rawRes.isErr: rawRes.unsafeError else: why))
    llsAll.add(move lls)
  ok(llsAll)

proc parseLineCountPathRecords(pathReader: InterningTableReader):
    Result[tuple[payloads: seq[string], counts: seq[uint64]], string] =
  ## Decode every ``paths.dat`` record as the line-count-table layout —
  ## ``payload_len + payload + line_count`` — and return the interning
  ## payloads alongside the per-file line counts.
  ##
  ## Authoritative only: this runs when the trace has DECLARED the layout
  ## through ``meta.dat`` bit 14, so a record that does not decode is a
  ## corrupt trace and the error names the record and the field. There is
  ## no probing counterpart, and there must not be: the record space
  ## overlaps the bare layout's (a path whose first byte happens to be
  ## its own remaining length decodes cleanly), so inferring the layout
  ## from the bytes would answer with a truncated path and a fabricated
  ## line count — the same defect the Layout A probe exists to avoid
  ## acting on.
  ##
  ## A zero ``line_count`` is refused rather than defaulted. Under this
  ## layout every file's size is a number the container states, and a
  ## file sized zero would share its base with the next one; silently
  ## substituting a stride there is exactly the assumption the table was
  ## added to remove.
  let pathTotal = pathReader.count()
  var payloads = newSeq[string](int(pathTotal))
  var counts = newSeq[uint64](int(pathTotal))
  for i in 0'u64 ..< pathTotal:
    let rawRes = pathReader.readRawById(i)
    var why: string
    if rawRes.isErr or not decodeLineCountRecord(rawRes.unsafeGet(),
        payloads[i], counts[i], why):
      return err("paths.dat[" & $i & "]: " &
        (if rawRes.isErr: rawRes.unsafeError else: why))
  ok((payloads, counts))

proc path*(r: NewTraceReader, id: uint64): Result[string, string] {.gcsafe.}
  ## Forward declaration — the ordinal pass below reads every record's
  ## payload through the SAME accessor a consumer does, so the two can
  ## never disagree about what a record's string is.
  ##
  ## `{.gcsafe.}` is declared here rather than inferred because Nim infers
  ## GC-safety from a proc's BODY, and a forward declaration has none at the
  ## point its callers are analysed: everything that reaches `path` through
  ## this declaration — `computePathVersionOrdinals`, and therefore the
  ## version accessors that call it — was silently inferred
  ## GC-UNSAFE, which made the whole reader unusable from a `{.gcsafe.}` proc
  ## type. `path` touches no globals (only the `NewTraceReader` it is given),
  ## so the annotation states a fact rather than waiving a check: the compiler
  ## still verifies the definition below against it and rejects the body if it
  ## ever does reach mutable global state.

proc computePathVersionOrdinals(r: var NewTraceReader): Result[void, string] =
  ## GDH-M1 — assign every ``paths.dat`` entry its 0-based version
  ## ordinal, in path-id order, and count how many entries share each
  ## payload.
  ##
  ## Linear in the number of paths. It runs on the first version query
  ## rather than at open, because it reads and hashes every path string,
  ## which an open that only wants a step's location does not need. It
  ## cannot go stale: ``refresh`` replaces the whole reader, and with it
  ## the not-yet-built state.
  let total = int(r.pathReader.count())
  r.pathVersionOrdinals = newSeq[uint64](total)
  r.pathVersionTotals = newSeq[uint64](total)
  # One count per distinct payload; each id's ordinal is the count before it.
  var payloads = newSeq[string](total)
  var seen = initTable[string, uint64](total)
  for id in 0 ..< total:
    var payloadRes = r.path(uint64(id))
    if payloadRes.isErr:
      return err("paths.dat[" & $id & "]: " & payloadRes.unsafeError)
    payloads[id] = move payloadRes.get()
    let n = addr seen.mgetOrPut(payloads[id], 0'u64)
    r.pathVersionOrdinals[id] = n[]
    inc n[]
  for id in 0 ..< total:
    r.pathVersionTotals[id] = seen.getOrDefault(payloads[id])
  ok()

# ---------------------------------------------------------------------------
# Opening
# ---------------------------------------------------------------------------

proc decodeSourceView(raw: openArray[byte], v: var SourceView,
    why: var string): bool =
  ## One `source_views.dat` record (`internal-files.md` §"Alternate Source
  ## Views"): `path_id`, `view_kind`, then the name, content and source map,
  ## each length-prefixed. False, with `why` set, where it does not decode.
  var pos = 0
  template field(dest: var seq[byte] | var string, what: string) =
    var n: uint64
    if not readVarint(raw, pos, n):
      why = what & "_len: " & decodeVarint(raw, pos).unsafeError
      return false
    if n > uint64(raw.len - pos):
      why = what & " truncated"
      return false
    dest.setLen(int(n))
    for k in 0 ..< int(n):
      when dest is string: dest[k] = char(raw[pos + k])
      else: dest[k] = raw[pos + k]
    pos += int(n)
  if not readVarint(raw, pos, v.pathId):
    why = "path_id varint: " & decodeVarint(raw, pos).unsafeError
    return false
  if pos >= raw.len:
    why = "view_kind byte missing"
    return false
  v.viewKind = raw[pos]
  pos += 1
  field(v.viewName, "view_name")
  field(v.content, "content")
  field(v.sourcemapV3, "map")
  true

proc openNewTraceFromImage(image: ContainerImage, blockSize: uint32,
    maxEntries: uint32, assumeColumnAwarePaths: bool):
    Result[NewTraceReader, string]

proc openNewTraceFromBytes*(data: sink seq[byte],
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries,
    assumeColumnAwarePaths: bool = false): Result[NewTraceReader, string] =
  ## Open a trace from in-memory bytes. The reader keeps ``data``: pass the
  ## buffer by its last use (or ``move`` it) and it is taken without a copy.
  ##
  ## ``assumeColumnAwarePaths`` overrides the ``meta.dat`` bit 4
  ## declaration for the ``paths.dat`` record layout only.  Pass it when
  ## you know out of band that the trace was produced by a recorder that
  ## emitted Layout A path records without setting the flag (see the
  ## note at the Layout A block below).  It is an assertion by the
  ## caller, not a guess by the reader: on a trace whose records are not
  ## Layout A the open fails with a named ``paths.dat[N]: …`` error
  ## instead of returning misdecoded positions.
  var bytes = data
  # Versions 5 and 6 in both profiles are read (`ctfs-container.md` §1a); a
  # container stored under a whole-file scheme is reconstructed first, as
  # `header || decompress(rest)`. The reconstructed image keeps its header,
  # which still declares the scheme it was stored under; in the reader's own
  # copy that byte is set to `none`, the true statement about the bytes it
  # holds, so every member read below refuses a stored compressed body and
  # needs no word that this one has been undone.
  if bytes.len > V6CompressionOffset and bytes[5] == CtfsVersionV6 and
      bytes[V6CompressionOffset] != uint8(ord(wfcNone)):
    var image = ? reconstructImage(bytes)
    image[V6CompressionOffset] = uint8(ord(wfcNone))
    bytes = move image
  ? checkReadableContainer(bytes)
  openNewTraceFromImage(newContainerImage(move bytes), blockSize, maxEntries,
    assumeColumnAwarePaths)

proc openNewTraceFromImage(image: ContainerImage, blockSize: uint32,
    maxEntries: uint32, assumeColumnAwarePaths: bool):
    Result[NewTraceReader, string] =
  ## The reader over a container image whose header has been checked
  ## (`checkReadableContainer`): held whole, or read from its file as it is
  ## used (`openFileImage`).
  var reader: NewTraceReader
  reader.image = image
  reader.blockSize = blockSize
  reader.maxEntries = maxEntries
  reader.assumedColumnAwarePaths = assumeColumnAwarePaths

  # Read meta.dat.  A container that HAS one and cannot parse it is refused,
  # rather than opened with a zeroed `meta`.  Every flag this reader consults
  # lives in meta.dat and every one of them defaults to false, so carrying on
  # without it does not degrade to a partial answer — it silently picks the
  # other reading: line-only paths.dat records for a Layout A trace, the
  # line-count decode for a column-aware one, and the current global line
  # index decode for a container written under the superseded one. Those are
  # the misdecodes `readMetaDat`'s version and unknown-flag-bit checks exist
  # to prevent, and discarding its error here is what let them through.
  #
  # A container with NO meta.dat opens with the flags at their defaults.
  # It used to be the case that such a container was read through the
  # legacy `paths.json` sidecar; that sidecar is retired, so a container
  # without meta.dat is now read entirely from the binary tables.
  let metaView = viewMember(reader.image, "meta.dat", blockSize, maxEntries)
  if metaView.isOk:
    let metaRes = readMetaDat(? metaView.get().contents())
    if metaRes.isErr:
      return err("meta.dat present but not readable: " & metaRes.unsafeError)
    reader.meta = metaRes.get()

  # Load interning tables (these are small, load at startup). A table that
  # is absent is empty; a table that is present but does not read is refused,
  # not answered as empty (`ctfs-container.md` §4, "A null is not an absence").
  template loadTable(name: string, dest: untyped) =
    if hasInternalFile(reader.image.bytes, name & ".dat", maxEntries) or
        hasInternalFile(reader.image.bytes, name & ".off", maxEntries):
      var tr = initInterningTableReader(reader.image, name, blockSize,
        maxEntries)
      if tr.isErr:
        return err(name & ".dat: " & tr.unsafeError)
      dest = move tr.get()
  loadTable("paths", reader.pathReader)
  loadTable("funcs", reader.funcReader)
  loadTable("types", reader.typeReader)
  loadTable("varnames", reader.varnameReader)

  # P6.5 / Layout A — the shape of a ``paths.dat`` record is decided by
  # ``meta.dat`` bit 4 (``FlagHasColumnAwareSteps``).  When it is set
  # each record is ``path_len + path_bytes + line_count + line_lengths``
  # and the per-file line-length table is cached here; when it is clear
  # each record is the raw path bytes and there is no table to cache.
  #
  # The reader does not infer the layout from the record bytes, because
  # the two layouts are not distinguishable by inspection: the 97-byte
  # ASCII path ``'/' & 'a'.repeat(46) & "/0" & 'b'.repeat(48)`` is an
  # ordinary line-only record that also decodes as a complete Layout A
  # record (see ``parseLayoutAPathRecords``).  A reader that promoted on
  # a successful decode returned a 47-character prefix of that path, a
  # fabricated 48-line table for the file, and a plausible but wrong
  # ``(file, line, column)`` for every step — wrong answers with no
  # error, which is worse than any refusal.
  #
  # The recorder-side bug that motivated the promotion was real: before
  # ``708ee44`` ("P6.4: implement DeltaColumn") the writer's ``close()``
  # did not forward ``columnAwareSteps`` to ``writeMetaDat``, so a
  # recorder that called ``enableColumnAwareSteps()`` produced Layout A
  # records under a CLEAR bit 4.  Blockchain recorders that adopted
  # column-aware mode in mid-June 2026 recorded fixtures in that window.
  # Recovering such a trace is now the CALLER's decision, taken by
  # passing ``assumeColumnAwarePaths = true``: the parse then runs in
  # authoritative mode, so a trace that is genuinely line-only fails the
  # open with a named ``paths.dat[N]: …`` error rather than yielding
  # misdecoded positions.
  #
  # A trace opened without the override never has its meta flag
  # promoted.  When its records nonetheless look like Layout A the
  # reader names the condition through ``columnAwarePathsSuspected``
  # (and ct-print's ``column_aware_paths_suspected`` flag) so an
  # affected trace is reported rather than silently reinterpreted.
  if reader.pathReader.count() > 0:
    if reader.meta.hasColumnAwareSteps or assumeColumnAwarePaths:
      let parsed = parseLayoutAPathRecords(reader.pathReader, probe = false)
      if parsed.isErr:
        return err(parsed.unsafeError)
      reader.lineLengths = parsed.get()
      # Only reachable via the caller's explicit override; a trace that
      # declared bit 4 already has the flag set.
      reader.meta.hasColumnAwareSteps = true
    elif reader.meta.hasLineCountTable:
      # Bit 14 — every record carries the file's line count after the
      # path bytes.  Like bit 4 this is a DECLARATION, so the parse is
      # authoritative and a record that does not decode fails the open
      # rather than falling back to the bare layout: falling back would
      # hand the caller a path with a length prefix glued to its front
      # and put every file back on the assumed stride.
      let parsed = parseLineCountPathRecords(reader.pathReader)
      if parsed.isErr:
        return err(parsed.unsafeError)
      reader.lineCountPayloads = parsed.get().payloads
      reader.lineCounts = parsed.get().counts
    else:
      reader.layoutASuspected =
        parseLayoutAPathRecords(reader.pathReader, probe = true).isOk

  # Alternate source views (spec §"Alternate Source Views
  # (Deminification Support)").  Found by the member's presence, not by
  # meta.dat bit 5: `meta.dat` is written at open, before a view is
  # registered, so the bit cannot say (`internal-files.md` §"Stream-presence
  # flags are a hint, not a gate").  Every record is decoded eagerly so the
  # per-view accessors below run in O(1).
  if hasInternalFile(reader.image.bytes, "srcviews.dat", maxEntries):
    # See the writer's note on the abbreviated 12-char base name:
    # ``source_views.dat`` (spec name, 16 chars) collides with
    # ``source_views.off`` in the base40 filename encoding, so the
    # on-disk files are ``srcviews.dat`` / ``srcviews.off``.
    let svRes = initVariableRecordTableReader(
      reader.image, "srcviews", blockSize, maxEntries)
    if svRes.isErr:
      return err("source_views.dat: " & svRes.unsafeError)
    let svReader = svRes.get()
    let total = svReader.count()
    reader.sourceViews = newSeq[SourceView](int(total))
    let pathCount = reader.pathReader.count()
    reader.sourceViewsByPath = newSeq[seq[uint64]](int(pathCount))
    for i in 0'u64 ..< total:
      let rawRes = svReader.read(i)
      var why: string
      if rawRes.isErr or not decodeSourceView(rawRes.unsafeGet(),
          reader.sourceViews[int(i)], why):
        return err("source_views.dat[" & $i & "]: " &
          (if rawRes.isErr: rawRes.unsafeError else: why))
      let pathId = reader.sourceViews[int(i)].pathId
      if pathId < pathCount:
        reader.sourceViewsByPath[int(pathId)].add(i)

  reader.posSpace = positionSpace(reader.lineLengths, reader.lineCounts,
    int(reader.pathReader.count()), reader.meta.hasColumnAwareSteps)
  ok(reader)

when ctHasFilesystem:
  # The only two entry points in this module that name a file. Everything
  # below them reads bytes that are already in memory, so this is the whole
  # filesystem surface of the reader.

  proc openNewTrace*(path: string,
      assumeColumnAwarePaths: bool = false): Result[NewTraceReader, string] =
    ## Open a multi-stream trace file from disk.
    ## Loads meta.dat and interning tables at startup.
    ## All other streams are loaded lazily on first access.
    ##
    ## See `openNewTraceFromBytes` for ``assumeColumnAwarePaths``.

    if not fileExists(path):
      return err("file not found: " & path)
    # Read as it is used: the members the first answers need are read, and
    # the rest of the file when it is asked for.
    var image = ? openFileImage(path)
    if not image.readsFromFile:
      return openNewTraceFromBytes(move image.bytes,
        assumeColumnAwarePaths = assumeColumnAwarePaths)
    ? checkReadableContainer(image.bytes)
    openNewTraceFromImage(image, DefaultBlockSize, DefaultMaxRootEntries,
      assumeColumnAwarePaths)

  proc refresh*(r: var NewTraceReader, path: string): Result[void, string] =
    ## Re-read a growing CTFS container into this handle and invalidate every
    ## lazily-opened stream reader whose chunk table may have grown.
    ## Re-opens under the same ``assumeColumnAwarePaths`` reading this
    ## handle was created with.
    var reopened = openNewTrace(path,
      assumeColumnAwarePaths = r.assumedColumnAwarePaths)
    if reopened.isErr:
      return err(reopened.unsafeError)
    r = move reopened.get()
    ok()

# ---------------------------------------------------------------------------
# Interning table accessors
# ---------------------------------------------------------------------------

proc path*(r: NewTraceReader, id: uint64): Result[string, string] =
  if r.pathReader.count() > 0:
    if r.meta.hasColumnAwareSteps:
      # P6.5 / Layout A: paths.dat record is
      # ``path_len + path_bytes + line_count + line_lengths``.  Decode
      # only the path prefix to surface the legacy string-shaped API.
      let rawRes = r.pathReader.readRawById(id)
      if rawRes.isErr:
        return err(rawRes.unsafeError)
      let raw = rawRes.get()
      var pos = 0
      let pathLenRes = decodeVarint(raw, pos)
      if pathLenRes.isErr:
        return err("paths.dat[" & $id & "]: " & pathLenRes.unsafeError)
      let pathLen = int(pathLenRes.get())
      if pos + pathLen > raw.len:
        return err("paths.dat[" & $id & "]: path_bytes truncated")
      var s = newString(pathLen)
      for k in 0 ..< pathLen:
        s[k] = char(raw[pos + k])
      ok(s)
    elif r.meta.hasLineCountTable:
      # Bit 14: the record is ``payload_len + payload + line_count``, and
      # the payload was decoded at open.  Reading it back raw here would
      # surface the length prefix and the trailing count as part of the
      # path string.
      if id >= uint64(r.lineCountPayloads.len):
        return err("paths.dat[" & $id & "]: out of range (" &
          $r.lineCountPayloads.len & " record(s))")
      ok(r.lineCountPayloads[int(id)])
    else:
      r.pathReader.readById(id)
  else:
    r.pathReader.readById(id)  # error path — preserve the original error

proc bareRecordDiagnosis(r: NewTraceReader, table: string, id: uint64,
    decodeError: string): string =
  ## The refusal for a `funcs.dat` / `types.dat` record that does not decode as
  ## the spec's structured record. Under `meta.dat` bit 12 clear the likely
  ## cause is known: the Nim writer wrote BARE NAMES into both tables until
  ## b891a0f (2026-09-15), at schema versions 4 and 5 — so the container is not
  ## an old version, and a version check cannot say what is wrong with it. The
  ## structured decode's own message ("truncated: declares an N-byte name")
  ## describes neither the record nor the remedy.
  if r.meta.hasInterningTables:
    return decodeError
  table & " record " & $id & " is not the spec's structured record " &
    "(internal-files.md \"Interning Tables\"); in a container with meta.dat " &
    "bit 12 clear it is a bare name, the shape the Nim writer wrote before " &
    "b891a0f (2026-09-15). Such a container does not conform to the spec " &
    "at any schema version and is not read; re-record it with a current " &
    "recorder. (Decoding it as a structured record reported: " &
    decodeError & ")"

proc function*(r: NewTraceReader, id: uint64): Result[string, string] =
  ## THE RECORD IS STRUCTURED, NOT BARE BYTES. `internal-files.md:46` gives a
  ## `funcs.dat` record as `global_line_index: varint, name_len: varint, name`.
  ## Reading it with `readById` returns the varints as leading characters of the
  ## name — which is what this did, and what made a Rust-written container print
  ## a function called `\xa6\x8dtoken::transfer`.
  let rec = r.funcReader.readFuncById(id)
  if rec.isErr:
    return err(r.bareRecordDiagnosis("funcs.dat", id, rec.unsafeError))
  ok(rec.get().name)

proc functionRecord*(r: NewTraceReader, id: uint64):
    Result[tuple[globalLineIndex: uint64, name: string], string] =
  ## The whole record, for a caller that wants the declaration site as well as
  ## the name.
  let rec = r.funcReader.readFuncById(id)
  if rec.isErr:
    return err(r.bareRecordDiagnosis("funcs.dat", id, rec.unsafeError))
  rec

proc typeName*(r: NewTraceReader, id: uint64): Result[string, string] =
  ## Structured for the same reason as `function`: `internal-files.md:45` gives
  ## a `types.dat` record as `kind: u8, lang_type_len: varint, lang_type,
  ## specific_info`.
  let rec = r.typeReader.readTypeById(id)
  if rec.isErr:
    return err(r.bareRecordDiagnosis("types.dat", id, rec.unsafeError))
  ok(rec.get().langType)

proc typeRecord*(r: NewTraceReader, id: uint64):
    Result[tuple[kind: uint8, langType: string], string] =
  ## The kind alongside the name.
  let rec = r.typeReader.readTypeById(id)
  if rec.isErr:
    return err(r.bareRecordDiagnosis("types.dat", id, rec.unsafeError))
  rec

proc varname*(r: NewTraceReader, id: uint64): Result[string, string] =
  r.varnameReader.readById(id)

proc columnAwarePathsSuspected*(r: NewTraceReader): bool =
  ## True when this trace's ``meta.dat`` declares line-only steps, yet
  ## every ``paths.dat`` record also decodes as a complete Layout A
  ## record.  It names the one condition a reader cannot resolve on its
  ## own: either the trace really is line-only and the coincidence is
  ## harmless, or its recorder emitted Layout A records without setting
  ## ``FlagHasColumnAwareSteps`` (a writer bug fixed in ``708ee44``,
  ## after several mid-June-2026 recorder fixtures were captured).
  ##
  ## This is a report, not a decision.  The reader keeps treating the
  ## trace exactly as its ``meta.dat`` declares — path strings and step
  ## positions are the line-only ones.  A caller that has independent
  ## grounds to believe the recorder was affected reopens the trace with
  ## ``assumeColumnAwarePaths = true``.
  ##
  ## Always false when the trace declares column-aware steps: there is
  ## nothing to suspect, the layout is stated.
  r.layoutASuspected

proc pathCount*(r: NewTraceReader): uint64 =
  r.pathReader.count()

# ---------------------------------------------------------------------------
# Versioned paths (GDH-M1 — design §6.1 / §7.0)
# ---------------------------------------------------------------------------

proc ensurePathVersions(r: var NewTraceReader): Result[void, string] =
  ## Compute the version ordinals once. The layout decisions they key on
  ## (`path()`'s answer) were all made at open.
  if not r.pathVersionsBuilt:
    let res = r.computePathVersionOrdinals()
    r.pathVersionsBuilt = true
    if res.isErr:
      r.pathVersionOrdinals.setLen(0)
      r.pathVersionTotals.setLen(0)
      r.pathVersionsError = res.unsafeError
  if r.pathVersionsError.len > 0:
    return err(r.pathVersionsError)
  ok()

proc pathVersionOrdinal*(r: var NewTraceReader,
    id: uint64): Result[uint64, string] =
  ## The 0-based VERSION ORDINAL of path ``id``: 0 for the first entry in
  ## ``paths.dat`` carrying this entry's string, 1 for the second, and so
  ## on in path-id order.
  ##
  ## This is what design §7.0 requires ``Location.source_generation`` to
  ## be populated from. It is 0 for every id of a trace whose paths are
  ## all distinct — every trace written before versioned paths existed —
  ## which is what that field's own documentation already promises.
  ##
  ## It is deliberately NOT "how many reloads happened": a reload that
  ## touched a file never executed again adds no entry, and the wire
  ## generation the observer sent starts at 1 rather than 0. The two are
  ## off by one by construction and the reload marker records the mapping
  ## rather than leaving it to be inferred.
  ? r.ensurePathVersions()
  if id >= uint64(r.pathVersionOrdinals.len):
    return err("pathVersionOrdinal: path id " & $id & " is out of range (" &
      $r.pathVersionOrdinals.len & " path(s) in paths.dat)")
  ok(r.pathVersionOrdinals[int(id)])

proc pathVersionCount*(r: var NewTraceReader,
    id: uint64): Result[uint64, string] =
  ## How many ``paths.dat`` entries — including ``id`` itself — carry
  ## ``id``'s string. ``1`` for an unversioned path.
  ? r.ensurePathVersions()
  if id >= uint64(r.pathVersionTotals.len):
    return err("pathVersionCount: path id " & $id & " is out of range (" &
      $r.pathVersionTotals.len & " path(s) in paths.dat)")
  ok(r.pathVersionTotals[int(id)])

proc pathIdsForString*(r: NewTraceReader, payload: string): seq[uint64] =
  ## Every path id whose ``paths.dat`` string equals ``payload``, in
  ## path-id order — so index 0 is the earliest version.
  ##
  ## This is the shape design §7.1 requires of a reader's path map: a
  ## string maps to an ORDERED LIST of ids, never to one id. A last-wins
  ## map answers a pre-reload lookup with the post-reload file, which is
  ## a wrong answer that looks like a right one.
  ##
  ## An empty result means the string is not in this trace; it never
  ## means "ambiguous". A caller that must pick one version picks it by
  ## version, not by the size of the candidate set.
  result = @[]
  for id in 0'u64 ..< r.pathCount():
    let p = r.path(id)
    if p.isOk and p.get() == payload:
      result.add(id)

# ---------------------------------------------------------------------------
# Alternate source views (Deminification Support).  See spec §
# "Alternate Source Views (Deminification Support)" in
# ``codetracer-trace-format-spec/internal-files.md``.
# ---------------------------------------------------------------------------

proc sourceViewCount*(r: NewTraceReader): uint64 =
  ## Number of formatted-view records carried by this trace.  Always
  ## zero on pre-extension traces (``meta.hasAlternateSourceViews ==
  ## false``).
  uint64(r.sourceViews.len)

proc sourceView*(r: NewTraceReader, idx: uint64): Result[SourceView, string] =
  ## Random-access read of view ``idx``.  Returns ``err`` when ``idx``
  ## is out of range — the reader's per-record decode happens at open
  ## time so this accessor is O(1).
  if idx >= uint64(r.sourceViews.len):
    return err("sourceView: index " & $idx & " out of range (" &
      $r.sourceViews.len & " view(s))")
  ok(r.sourceViews[int(idx)])

proc sourceViewsForPath*(r: NewTraceReader, pathId: uint64): seq[uint64] =
  ## Indices into the source-views table that target ``pathId``.
  ## Returns an empty seq when the trace carries no views for that
  ## path (or when ``pathId`` is out of range — back-compat-safe
  ## default to mirror the spec's "no views" pre-extension behaviour).
  if pathId >= uint64(r.sourceViewsByPath.len):
    return @[]
  r.sourceViewsByPath[int(pathId)]
proc functionCount*(r: NewTraceReader): uint64 = r.funcReader.count()

# ---------------------------------------------------------------------------
# Lazy-stream load probes (instrumentation)
# ---------------------------------------------------------------------------
#
# The exec (steps), value, call and IO-event stream readers are all
# initialized LAZILY — only on the first access that needs them (see the
# ``ensure*Reader`` helpers below).  These read-only probes surface that
# laziness so a consumer can PROVE which streams a given read path actually
# touched.  The incremental test runner's seekable executed-function read
# (codetracer ``ct_test/incremental/ctfs_seekable.nim``) asserts, after
# building the executed-function set, that ``valueStreamLoaded`` is still
# false — i.e. it never opened/decoded the (far larger) value stream — and
# that ``execChunkDecompressions`` stays bounded (it only seeks the chunks
# holding call-entry steps for best-effort def-line resolution, never
# scanning the whole step stream).

proc execStreamLoaded*(r: NewTraceReader): bool = r.execLoaded
  ## True once the steps (exec) stream reader has been initialized.

proc valueStreamLoaded*(r: NewTraceReader): bool = r.valueLoaded
  ## True once the value stream reader has been initialized.

proc callStreamLoaded*(r: NewTraceReader): bool = r.callLoaded
  ## True once the call stream reader has been initialized.

proc ioEventStreamLoaded*(r: NewTraceReader): bool = r.ioEventLoaded
  ## True once the IO-event stream reader has been initialized.

proc execChunkDecompressions*(r: NewTraceReader): uint64 =
  ## Distinct Zstd chunk inflations the exec (steps) stream reader has
  ## performed so far.  Zero while the exec stream is unopened.  A targeted
  ## per-step seek inflates at most one new chunk; a whole-stream scan
  ## inflates every chunk.  Lets callers prove a step read stayed bounded.
  if r.execLoaded: r.execReader.chunkDecompressions() else: 0'u64
proc typeCount*(r: NewTraceReader): uint64 = r.typeReader.count()
proc varnameCount*(r: NewTraceReader): uint64 = r.varnameReader.count()

# ---------------------------------------------------------------------------
# P6.5 — column-aware position decoding (spec §"Source Location
#                                            Addressing")
# ---------------------------------------------------------------------------

type
  PathTableKind* = enum
    ## What ``paths.dat`` records about a file's size, by the layout
    ## ``meta.dat`` declares (``internal-files.md`` §"Interning Tables").
    ptkBare = 0          ## no size: a bare record (neither bit 4 nor 14)
    ptkLineCount = 1     ## a line count (bit 14)
    ptkLines = 2         ## a per-line table (bit 4, ``line_count > 0``)
    ptkConventional = 3  ## the conventional table, 100000 lines of 1024
                         ## (bit 4, ``line_count = 0``)

proc pathTableKind*(r: NewTraceReader, fileId: uint64): Option[PathTableKind] =
  ## Which kind of size ``paths.dat`` records for ``fileId``; ``none`` when
  ## there is no such path. A column-aware record of ``line_count = 0`` is
  ## ``ptkConventional`` — never "no table" — so a caller can tell it from
  ## a file with no Layout A data without reading its line lengths.
  if fileId >= r.pathCount():
    return none(PathTableKind)
  if fileId < uint64(r.lineLengths.len):
    if r.lineLengths[fileId].len == 0:
      return some(ptkConventional)
    return some(ptkLines)
  if r.meta.hasLineCountTable:
    return some(ptkLineCount)
  some(ptkBare)

proc lineLengthRaw*(r: NewTraceReader, fileId: uint64,
    lineIndex0: uint32): Option[uint32]

proc lineLength*(r: NewTraceReader, fileId: uint64,
    lineIndex0: uint32): Option[uint32] =
  ## Return the addressable column count of ``lineIndex0`` (0-indexed,
  ## so line 1 of the file is ``lineIndex0 = 0``) in the file with id
  ## ``fileId``.  Returns ``none`` when the trace is not column-aware,
  ## when ``fileId`` is out of range, when the line index is past the
  ## file's known line table, or when the recorder did not surface a
  ## per-line table.  A ``line_count = 0`` record is the conventional
  ## table: every line up to ``DefaultLinesPerFile`` has
  ## ``ConventionalLineLength`` columns.
  ##
  ## Note: callers that have a 1-indexed line number (per the spec
  ## convention used by AbsoluteStep / DeltaStep cursor tracking) must
  ## subtract 1 before calling.
  if not r.meta.hasColumnAwareSteps:
    return none(uint32)
  r.lineLengthRaw(fileId, lineIndex0)

proc lineLengthRaw*(r: NewTraceReader, fileId: uint64,
    lineIndex0: uint32): Option[uint32] =
  ## Ungated sibling of [lineLength] — surfaces the addressable column
  ## count for ``(fileId, lineIndex0)`` without consulting
  ## ``meta.hasColumnAwareSteps``.  It reads the per-file table the open
  ## call parsed, so it answers for a trace that declares column-aware
  ## steps and for one opened with ``assumeColumnAwarePaths = true``;
  ## on a trace opened normally that declares line-only steps there is
  ## no table and every query is ``none``.  Also ``none`` when ``fileId``
  ## is out of range or ``lineIndex0`` is past the file's line table.  A
  ## ``line_count = 0`` record answers by the conventional table's rule.
  if fileId >= uint64(r.lineLengths.len):
    return none(uint32)
  let lls = r.lineLengths[fileId]
  if lls.len == 0:
    if uint64(lineIndex0) < DefaultLinesPerFile:
      return some(ConventionalLineLength)
    return none(uint32)
  if int(lineIndex0) >= lls.len:
    return none(uint32)
  some(lls[int(lineIndex0)])

proc lineCountRaw*(r: NewTraceReader, fileId: uint64): uint64 =
  ## Ungated companion to [lineLengthRaw]: number of lines registered in
  ## paths.dat Layout A for ``fileId``.  Returns ``0`` when this handle
  ## parsed no Layout A table for the file — which includes every trace
  ## that declares line-only steps and was opened without
  ## ``assumeColumnAwarePaths``. A ``line_count = 0`` record is the
  ## conventional table, so its count is ``DefaultLinesPerFile``.
  if fileId >= uint64(r.lineLengths.len):
    return 0'u64
  let lls = r.lineLengths[fileId]
  if lls.len == 0: DefaultLinesPerFile else: uint64(lls.len)

proc globalPositionSpace*(r: NewTraceReader): lent GlobalLineIndex =
  ## The address space this trace's ``global_position_index`` values were
  ## encoded in, laid out by the rule the writer used
  ## (``global_line_index.positionSpaceCounts``).
  ##
  ## This is what a caller inverts a position through when
  ## ``decodeGlobalPositionIndex`` has nothing to say about it — a
  ## line-only trace, or a column-aware one whose file has no per-line
  ## table. Rebuilding the space from the path count alone gives every
  ## file ``DefaultLinesPerFile``, which is wrong for any trace that
  ## states its own per-file sizes: such a file is smaller than the
  ## default, so every file after it sits too high, and a position
  ## resolves into the wrong file with a line number that is in range.
  ## The sizes come from the trace itself — the per-line tables of a
  ## column-aware container, the line counts of one that sets bit 14 —
  ## and only a container that states neither falls back to the default.
  ##
  ## Inverting through it is still an assumption about the producer's
  ## packing — see the ``global_line_index`` module header — so callers
  ## must go through ``tryResolve``, not ``resolve``.
  r.posSpace

proc recordedLineCount*(r: NewTraceReader, fileId: uint64): uint64 =
  ## The line count this trace RECORDS for ``fileId``, or 0 when it
  ## records none.
  ##
  ## Zero is not "the file has no lines" — the writer refuses to record
  ## that and the reader refuses to parse it. It is "this container does
  ## not state the file's size", which is every trace without
  ## ``meta.dat`` bit 14, and it is why the answer is deliberately not a
  ## fallback stride: a caller asking what the trace records must be able
  ## to tell a recorded size from an assumed one.
  if fileId >= uint64(r.lineCounts.len):
    return 0'u64
  r.lineCounts[int(fileId)]

proc ensurePositionTables(r: var NewTraceReader) =
  ## Build per-file cumulative tables used by ``decodeGlobalPositionIndex``.
  ## Idempotent: callable from every per-step resolution.
  ##
  ## A file's slot is sized by ``global_line_index.fileAddressCount``, the
  ## same rule the writer's ``rebuildGli`` lays the space out with
  ## (``positionSpaceCount``). That matters for a ``line_count = 0``
  ## record: it is the conventional table and occupies
  ## ``ConventionalFileSize`` addresses, so sizing it ``0`` here would put
  ## every later file's base that much too low and land the file search in
  ## the file before the right one.
  if r.posTablesBuilt:
    return
  let fileCount = r.lineLengths.len
  r.lineBase = newSeq[seq[uint64]](fileCount)
  r.fileBase = newSeq[uint64](fileCount)
  r.fileSize = newSeq[uint64](fileCount)
  var runningGlobal: uint64 = 0
  for fid in 0 ..< fileCount:
    # A file with the conventional table (an empty entry) keeps an empty
    # line base: its lines are resolved by the rule, not by a table.
    let lls = r.lineLengths[fid]
    var lb = newSeq[uint64](lls.len)
    var sum: uint64 = 0
    for i in 0 ..< lls.len:
      lb[i] = sum
      sum += uint64(lls[i])
    r.lineBase[fid] = lb
    r.fileBase[fid] = runningGlobal
    r.fileSize[fid] = positionSpaceCount(r.lineLengths, [], fid, true)
    runningGlobal += r.fileSize[fid]
  r.posTablesBuilt = true

proc decodeGlobalPositionIndex*(r: var NewTraceReader,
    p: uint64): Result[tuple[file: uint64, line: uint32, column: uint32],
    string] =
  ## P6.5 — resolve a ``global_position_index`` to ``(file, line,
  ## column)`` using the per-file / per-line cumulative tables built
  ## from the column-aware paths.dat records.  Implements the spec
  ## algorithm at ``codetracer-trace-format-spec/trace-events.md``
  ## §"Decoding ``global_position_index``": ``O(log F)`` file-table
  ## binary search + ``O(log L)`` line-table binary search.
  ##
  ## Only valid on column-aware traces.  ``line`` and ``column`` are
  ## 1-based to match the spec.
  if not r.meta.hasColumnAwareSteps:
    return err("decodeGlobalPositionIndex requires a column-aware trace")
  r.ensurePositionTables()
  if r.fileBase.len == 0:
    return err("trace has no paths registered")

  # Binary search for the file: largest fid with fileBase[fid] <= p.
  var lo = 0
  var hi = r.fileBase.len - 1
  var fid = -1
  while lo <= hi:
    let mid = (lo + hi) div 2
    if r.fileBase[mid] <= p:
      fid = mid
      lo = mid + 1
    else:
      hi = mid - 1
  if fid < 0:
    return err("global_position_index " & $p &
      " precedes the first file's base")
  if p >= r.fileBase[fid] + r.fileSize[fid]:
    return err("global_position_index " & $p &
      " out of range for file " & $fid)

  let q = p - r.fileBase[fid]
  let lb = r.lineBase[fid]
  if lb.len == 0:
    # The conventional table: every line has ConventionalLineLength columns.
    let width = uint64(ConventionalLineLength)
    return ok((file: uint64(fid), line: uint32(q div width + 1),
      column: uint32(q mod width + 1)))

  # Binary search for the line: largest l with lb[l] <= q.
  lo = 0
  hi = lb.len - 1
  var l = -1
  while lo <= hi:
    let mid = (lo + hi) div 2
    if lb[mid] <= q:
      l = mid
      lo = mid + 1
    else:
      hi = mid - 1
  if l < 0:
    return err("in-file offset " & $q & " precedes the first line")

  let column = uint32(q - lb[l] + 1)
  ok((file: uint64(fid), line: uint32(l + 1), column: column))

# ---------------------------------------------------------------------------
# Step access (lazy init exec reader)
# ---------------------------------------------------------------------------

proc loadExecReader(r: var NewTraceReader): Result[void, string] =
  if not r.execLoaded:
    # M24a-1: select the steps.dat/steps.idx framing by the meta.dat
    # ``has_step_stream`` flag.  Bundles written by the current Nim writer
    # (and by the Rust writer) set the flag and use the SPEC-canonical layout
    # (header-less chunks, no total_events trailer) that the Rust
    # ``StepStreamReader`` reads byte-for-byte.  Pre-M24a-1 Nim-v4 bundles
    # never set the flag and use the legacy framing (per-chunk u32 count +
    # total_events trailer); ``legacy = not hasStepStream`` keeps them readable.
    var res = initExecStreamReader(r.image, int(r.blockSize), int(r.maxEntries),
      legacy = not r.meta.hasStepStream,
      # GDH-M2: tag 0x08 is decodable only where the container declares
      # it.  A container that carries the tag with the flag clear is
      # refused BY NAME here rather than decoded — see
      # `decodeStepEvent`'s `allowSourceReload` note for why skipping is
      # strictly worse than refusing.
      allowSourceReload = r.meta.hasSourceReload)
    if res.isErr: return err(res.unsafeError)
    r.execReader = move res.get()
    r.execLoaded = true
  ok()

template ensureExecReader(r: var NewTraceReader) =
  ## `loadExecReader` on first use, its refusal returned from the enclosing
  ## proc; a check of one flag afterwards.
  if not r.execLoaded:
    ? r.loadExecReader()

proc step*(r: var NewTraceReader, n: uint64): Result[StepEvent, string] =
  r.ensureExecReader()
  r.execReader.readEvent(n)

proc stepAbsoluteGlobalLineIndex*(r: var NewTraceReader,
    n: uint64): Result[uint64, string] =
  ## Return the absolute global line index for step N.
  ##
  ## The exec stream stores positions as AbsoluteStep and DeltaStep records;
  ## each chunk's first position record is an AbsoluteStep and a reader
  ## decodes a chunk by itself (`resolveChunkPositions`), refusing a delta
  ## before the chunk's anchor.
  r.ensureExecReader()
  # The exec reader keeps each decoded chunk's positions, as far as reads
  # have reached: a caller that resolves steps one call at a time
  # (ct-print, the C ABI's `ct_reader_step_location`) decodes each record
  # once.
  r.execReader.eventPosition(n)

proc stepCount*(r: var NewTraceReader): Result[uint64, string] =
  r.ensureExecReader()
  ok(r.execReader.totalEvents)

proc logicalStepCount*(r: var NewTraceReader): Result[uint64, string] =
  ## Return the user-facing "step count" — every exec event except
  ## column-only nudges (``sekDeltaColumn``).  Matches the pre-P6
  ## semantics of ``stepCount`` (totalEvents) on line-only traces
  ## byte-for-byte, and continues to match it for column-aware
  ## traces because DeltaColumn events are subtracted out.  Used by
  ## ct-print for ``counts.steps`` so golden anchors stay stable
  ## across the writer's column-aware-mode opt-in.
  ##
  ## Why this isn't named "stepCount": ``stepCount`` still returns
  ## the raw exec event count because the FFI and chunked-storage
  ## internals correlate calls / IO events back to events-stream
  ## position via that index — including DeltaColumn nudges.
  ##
  ## Fast path: when the trace is not column-aware, the count is
  ## exactly ``stepCount`` and we skip the event walk entirely.
  ## Column-aware traces walk the event stream via ``readChunkEvents``
  ## (O(N) total decode cost — chunks are decompressed once each,
  ## events read in bulk).  Looping ``readEvent`` is O(N²) because
  ## each call re-scans from the chunk start.
  r.ensureExecReader()
  if not r.meta.hasColumnAwareSteps and not r.meta.hasSourceReload:
    return ok(r.execReader.totalEvents)
  var n: uint64 = 0
  var chunkBuf: seq[StepEvent]
  let total = int(r.execReader.totalEvents)
  let chunkSize = int(r.execReader.chunkSize)
  let chunkCount = (total + chunkSize - 1) div chunkSize
  for chunkIdx in 0 ..< chunkCount:
    discard ?r.execReader.readChunkEvents(chunkIdx, chunkBuf)
    for ev in chunkBuf:
      case ev.kind
      of sekDeltaColumn:
        discard
      of sekSourceReload:
        # GDH-M2 / design §7.3: the reload marker is a timeline
        # ANNOTATION, not a step.  It has no source location, so a
        # consumer must not be able to step to it, and counting it as a
        # step would put a position-less entry in the middle of a
        # position-indexed sequence.
        discard
      else:
        n += 1
  ok(n)

proc sourceReloadCount*(r: var NewTraceReader): Result[uint64, string] =
  ## GDH-M2 — how many ``TagSourceReload`` markers the execution stream
  ## carries.  Zero on a container that does not declare the extended
  ## flag, WITHOUT walking the stream: the tag cannot legally be present
  ## there and a reader that walked anyway would be asserting on bytes it
  ## has already refused.
  ##
  ## The exec reader is nonetheless OPENED before that early return, and
  ## the ordering is the whole point.  Opening is not walking — it costs
  ## the stream's header, not its chunks, so the fast path stays fast —
  ## but without it this proc answers ``0`` for a container whose
  ## execution stream is truncated, absent or refused, and "there are no
  ## markers" becomes indistinguishable from "I never looked".  That
  ## conflation is the defect this whole campaign exists to remove
  ## (HLX-M1's resolver answered "not found" when it could not read the
  ## table), and it would have been reachable through ``ct-print`` on
  ## EVERY container written to date, since they are all v4.
  r.ensureExecReader()
  if not r.meta.hasSourceReload:
    return ok(0'u64)
  var n: uint64 = 0
  var chunkBuf: seq[StepEvent]
  let total = int(r.execReader.totalEvents)
  let chunkSize = int(r.execReader.chunkSize)
  let chunkCount = (total + chunkSize - 1) div chunkSize
  for chunkIdx in 0 ..< chunkCount:
    discard ?r.execReader.readChunkEvents(chunkIdx, chunkBuf)
    for ev in chunkBuf:
      if ev.kind == sekSourceReload:
        n += 1
  ok(n)

type SourceReloadMarker* = object
  ## One decoded ``TagSourceReload`` event, together with the exec-stream
  ## index it occupies.  The index is what ties the marker to the steps on
  ## either side of it — which is the whole point of recording the
  ## boundary rather than inferring it from the path indices (§6.3.1).
  stepIndex*: uint64
  reloadOrdinal*: uint64
  changed*: seq[SourceReloadChange]
  inFlightFrames*: uint64

proc sourceReloads*(r: var NewTraceReader): Result[seq[SourceReloadMarker], string] =
  ## Every reload marker in the trace, in stream order.
  ##
  ## Like ``sourceReloadCount``, the exec reader is OPENED before the
  ## undeclared-container early return: an empty seq must mean "this
  ## stream carries no markers", never "this stream could not be read".
  var markers: seq[SourceReloadMarker] = @[]
  r.ensureExecReader()
  if not r.meta.hasSourceReload:
    return ok(markers)
  var chunkBuf: seq[StepEvent]
  let total = int(r.execReader.totalEvents)
  let chunkSize = int(r.execReader.chunkSize)
  let chunkCount = (total + chunkSize - 1) div chunkSize
  for chunkIdx in 0 ..< chunkCount:
    let firstIdx = ?r.execReader.readChunkEvents(chunkIdx, chunkBuf)
    for offset, ev in chunkBuf:
      if ev.kind == sekSourceReload:
        markers.add(SourceReloadMarker(
          stepIndex: firstIdx + uint64(offset),
          reloadOrdinal: ev.reloadOrdinal,
          changed: ev.changed,
          inFlightFrames: ev.inFlightFrames))
  ok(markers)

proc stepAbsoluteGlobalLineIndices*(r: var NewTraceReader,
    startN: uint64, count: uint64,
    output: var openArray[uint64]): Result[uint64, string] =
  ## Bulk variant of [stepAbsoluteGlobalLineIndex].
  ##
  ## Resolves the absolute global line index for steps in
  ## ``[startN, startN + count)`` and writes them into ``output``.  Returns
  ## the number of step entries actually written (always equal to
  ## ``min(count, total_events - startN, output.len)``).
  ##
  ## Why this helper exists: the per-step accessor re-scans from the start
  ## of the chunk containing each requested step, which gives the loop
  ## ``for n in 0 ..< N: stepAbsoluteGlobalLineIndex(n)`` an O(N²/chunk)
  ## decode cost — every step inside a chunk re-decodes every prior step
  ## of that chunk.  This bulk routine streams events through each chunk
  ## exactly once, accumulating the running ``currentGli`` across delta
  ## events, which is O(N) and removes the per-step Rust→Nim FFI overhead
  ## entirely.  Non-step events (Raise, Catch, ThreadStart/Exit/Switch)
  ## carry no GLI delta so the running GLI is left untouched, mirroring
  ## [stepAbsoluteGlobalLineIndex].
  r.ensureExecReader()

  if count == 0'u64 or output.len == 0:
    return ok(0'u64)

  let totalEvents = r.execReader.totalEvents
  if startN >= totalEvents:
    return ok(0'u64)

  let endN = min(startN + count, totalEvents)
  let want = endN - startN
  let writable = min(uint64(output.len), want)
  if writable == 0'u64:
    return ok(0'u64)
  let stopN = startN + writable

  let chunkSize = uint64(r.execReader.chunkSize)
  if chunkSize == 0'u64:
    return err("execReader has zero chunkSize")

  var events: seq[StepEvent] = @[]
  var positions: seq[uint64] = @[]
  var n = startN
  while n < stopN:
    let chunkIdx = int(n div chunkSize)
    # Stream all events of the chunk through the cache exactly once.
    # ``readChunkEvents`` returns the chunk's first global event index
    # so we can map seq positions back to absolute step indices.
    let firstIdxRes = r.execReader.readChunkEvents(chunkIdx, events)
    if firstIdxRes.isErr:
      return err(firstIdxRes.unsafeError)
    let firstIdx = firstIdxRes.get()

    ? resolveChunkPositions(events, chunkIdx, positions)
    for offset in 0 ..< events.len:
      let absIdx = firstIdx + uint64(offset)
      if absIdx >= n and absIdx < stopN:
        output[int(absIdx - startN)] = positions[offset]

    # Advance ``n`` to the next chunk boundary so the outer loop picks
    # the correct chunk on the next iteration.
    n = firstIdx + uint64(events.len)
    if events.len == 0:
      # Defensive: should never happen on a well-formed trace, but break
      # rather than spin if a chunk decodes to zero events.
      break

  ok(writable)

# ---------------------------------------------------------------------------
# Value access (lazy init)
# ---------------------------------------------------------------------------

proc loadValueReader(r: var NewTraceReader): Result[void, string] =
  if not r.valueLoaded:
    # M24a-2: select the values.dat/values.idx framing by the meta.dat
    # ``has_value_stream`` flag.  Bundles written by the current Nim writer
    # (and by the Rust writer) set the flag and use the SPEC-canonical chunked
    # layout that the Rust ``ValueStreamReader`` reads byte-for-byte.  Pre-M24a-2
    # Nim-v4 bundles never set the flag and use the legacy ``.off`` VRT framing;
    # ``legacy = not hasValueStream`` keeps them readable.
    var res = initValueStreamReader(r.image, r.blockSize, r.maxEntries,
      legacy = not r.meta.hasValueStream)
    if res.isErr: return err(res.unsafeError)
    r.valueReader = move res.get()
    r.valueLoaded = true
  ok()

template ensureValueReader(r: var NewTraceReader) =
  ## `loadValueReader` on first use, its refusal returned from the enclosing
  ## proc; a check of one flag afterwards.
  if not r.valueLoaded:
    ? r.loadValueReader()

proc values*(r: var NewTraceReader, n: uint64): Result[seq[VariableValue], string] =
  r.ensureValueReader()
  r.valueReader.readStepValues(n)

proc valueEvents*(r: var NewTraceReader,
    n: uint64): Result[seq[DecodedValueEvent], string] =
  ## Every value-stream event of exec record ``n`` — tags 0-9, in wire order.
  r.ensureValueReader()
  r.valueReader.readStepEvents(n)

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
  r.ensureValueReader()
  ok(r.valueReader.count())

proc lastSkippedValueTags*(r: NewTraceReader): seq[uint8] =
  if r.valueLoaded:
    r.valueReader.lastSkippedTags
  else:
    @[]

proc skippedValueTags*(r: NewTraceReader): seq[uint8] =
  if r.valueLoaded:
    r.valueReader.skippedTags
  else:
    @[]

proc skippedValueTagCounts*(r: NewTraceReader): seq[(uint8, int)] =
  if r.valueLoaded:
    r.valueReader.skippedTagCounts
  else:
    @[]


# ---------------------------------------------------------------------------
# Call access (lazy init)
# ---------------------------------------------------------------------------

proc loadCallReader(r: var NewTraceReader): Result[void, string] =
  if not r.callLoaded:
    var res = initCallStreamReader(r.image, r.blockSize, r.maxEntries)
    if res.isErr: return err(res.unsafeError)
    r.callReader = move res.get()
    r.callLoaded = true
  ok()

template ensureCallReader(r: var NewTraceReader) =
  ## `loadCallReader` on first use, its refusal returned from the enclosing
  ## proc; a check of one flag afterwards.
  if not r.callLoaded:
    ? r.loadCallReader()

proc call*(r: var NewTraceReader, callKey: uint64): Result[CallRecord, string] =
  r.ensureCallReader()
  r.callReader.readCallInto(callKey)

proc callCount*(r: var NewTraceReader): Result[uint64, string] =
  r.ensureCallReader()
  ok(r.callReader.count())

proc callForStep*(r: var NewTraceReader, stepId: uint64): Result[CallRecord, string] =
  ## Find the innermost enclosing call record for the given step.
  ##
  ## Call records are sorted by `entryStep` ascending (= entry order, which
  ## also matches call_key allocation order after CTFS-M-CallKeyOrder).
  ## Because calls NEST, child entries follow their parent's entry but a
  ## parent's `exitStep` is far larger than any of its children's: child
  ## ranges `[entryStep, exitStep]` are strictly contained in the parent's.
  ##
  ## Strategy (correct for arbitrary nesting):
  ##   1. Binary-search for the largest index `k` with
  ##      `calls[k].entryStep <= stepId`. All calls at index > k were
  ##      entered after `stepId` and cannot contain it.
  ##   2. Walk back from `k` and return the FIRST call whose
  ##      `exitStep >= stepId`. That call is, by construction, the
  ##      most-recently-entered frame still open at `stepId`, hence the
  ##      deepest enclosing call. Earlier indices in the walk are either
  ##      siblings that already returned (their parent eventually has
  ##      `exitStep >= stepId`) or the matching parent itself.
  ##
  ## CTFS-M-FunctionAttrTemplate: the previous implementation interleaved
  ## an interpolation search with an early-exit on `stepId > hi.exitStep`
  ## and used the lo/hi-contains shortcut to record matches. Both pieces
  ## broke for steps that sit in a caller's body AFTER a nested call
  ## returned: the interpolation could jump past the parent (whose
  ## `exitStep` extends far beyond a sibling child's `exitStep`), and the
  ## hi-side early-exit truncated the search even when `lo` itself still
  ## covered `stepId`. The net effect was that every post-return step of
  ## any caller -- and every step in code emitted via template inlining
  ## that physically sits after the inlined call returns -- was reported
  ## as "not found in any call", emitting a step with no `function`,
  ## `function_id`, or `depth` attribution.
  r.ensureCallReader()
  let totalCalls = r.callReader.count()
  if totalCalls == 0:
    return err("no call records")

  # Step 1: binary search for largest index k with entryStep <= stepId.
  # If no such index exists (stepId precedes the first call), bail out.
  var lo: uint64 = 0
  var hi: uint64 = totalCalls - 1
  var k: int64 = -1
  while lo <= hi:
    let mid = lo + (hi - lo) div 2
    let midCall = ?r.callReader.readCall(mid)
    if midCall.entryStep <= stepId:
      k = int64(mid)
      if mid == high(uint64):  # defensive, can't happen with reasonable trace
        break
      lo = mid + 1
    else:
      if mid == 0:
        break
      hi = mid - 1
  if k < 0:
    return err("step " & $stepId & " not found in any call")

  # Step 2: walk backwards looking for the first enclosing call. The
  # walk traverses sibling subtrees that already returned; their parent
  # (or grandparent, etc.) is the answer. Worst case is O(N) but the
  # expected cost on well-formed traces is O(call-depth-at-stepId)
  # because each backward hop skips at most one returned subtree before
  # landing on the enclosing frame.
  var i = uint64(k)
  while true:
    let c = ?r.callReader.readCall(i)
    if c.exitStep >= stepId:
      return ok(c)
    if i == 0:
      break
    i -= 1
  err("step " & $stepId & " not found in any call")

iterator callRange*(r: var NewTraceReader, start, count: uint64): CallRecord =
  ## Yields call records in [start, start+count).
  discard r.loadCallReader()
  for i in start ..< start + count:
    let res = r.callReader.readCall(i)
    if res.isOk:
      yield res.get()

proc callRange*(r: var NewTraceReader, start, count: uint64,
                output: var openArray[CallRecord]): int =
  ## Fill output buffer with call records starting at `start`.
  ## Returns the number of records written.
  discard r.loadCallReader()
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

proc loadIOEventReader(r: var NewTraceReader): Result[void, string] =
  if not r.ioEventLoaded:
    # M24a-3: select the events.dat/events.idx framing by the meta.dat
    # ``has_io_event_stream`` flag.  Bundles written by the current Nim writer
    # (and by the Rust writer) set the flag and use the SPEC-canonical chunked
    # layout that the Rust ``IoEventStreamReader`` reads byte-for-byte.
    # Pre-M24a-3 Nim-v4 bundles never set the flag and use the legacy ``.off``
    # VRT framing; ``legacy = not hasIoEventStream`` keeps them readable.
    var res = initIOEventStreamReader(r.image, r.blockSize, r.maxEntries,
      legacy = not r.meta.hasIoEventStream)
    if res.isErr: return err(res.unsafeError)
    r.ioEventReader = move res.get()
    r.ioEventLoaded = true
  ok()

template ensureIOEventReader(r: var NewTraceReader) =
  ## `loadIOEventReader` on first use, its refusal returned from the enclosing
  ## proc; a check of one flag afterwards.
  if not r.ioEventLoaded:
    ? r.loadIOEventReader()

proc ioEvent*(r: var NewTraceReader, index: uint64): Result[IOEvent, string] =
  r.ensureIOEventReader()
  r.ioEventReader.readEvent(index)

proc ioEventCount*(r: var NewTraceReader): Result[uint64, string] =
  r.ensureIOEventReader()
  ok(r.ioEventReader.count())

iterator events*(r: var NewTraceReader, start, count: uint64): IOEvent =
  ## Yields IO events in [start, start+count).
  discard r.loadIOEventReader()
  for i in start ..< start + count:
    let res = r.ioEventReader.readEvent(i)
    if res.isOk:
      yield res.get()

proc events*(
    r: var NewTraceReader, start, count: uint64,
    output: var openArray[IOEvent]): int =
  ## Fill output buffer with IO events starting at `start`.
  ## Returns the number of events written.
  discard r.loadIOEventReader()
  var written = 0
  for i in start ..< start + count:
    if written >= output.len: break
    let res = r.ioEventReader.readEvent(i)
    if res.isOk:
      output[written] = res.get()
      written += 1
  written
