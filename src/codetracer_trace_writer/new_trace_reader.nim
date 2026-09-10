{.push raises: [].}

## Seek-based trace reader (M18 + M19).
##
## Opens a multi-stream CTFS trace and provides random access to all data.
## Interning tables are loaded eagerly at startup; execution, value, call,
## and IO-event streams are initialized lazily on first access.

import results
import std/options
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ../codetracer_ctfs/variable_record_table
import ./meta_dat
import ./interning_table
import ./exec_stream
import ./value_stream
import ./call_stream
import ./io_event_stream
import ./step_encoding
import ./varint
import ./global_line_index

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

    # paths.json fallback for traces that don't carry a binary paths
    # interning table yet (the M13 ct_recorder writer populates
    # paths.json but the binary paths.dat / paths.off table is still
    # an open TODO per the meta-json-retirement work tracked in
    # codetracer-specs/Planned-Work/Legacy-CTFS-Format-Cleanup.md).
    # When pathReader is empty we fall back to this list so callers
    # get the source paths they actually recorded.
    pathsJson: seq[string]

    # P6.5 / Layout A — per-file line-length tables, parsed from the
    # column-aware paths.dat records when `meta.hasColumnAwareSteps`
    # is set.  ``lineLengths[fileId][line]`` is the addressable column
    # count of line (0-indexed) in file ``fileId``.  When the trace is
    # not column-aware, this stays empty and column queries return
    # ``none``.
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
# ``paths.json`` is the only JSON document this reader parses, and its schema
# is fixed by the spec: an array of source-path strings. Reading it with
# ``std/json`` costs far more than the schema does — ``parsejson`` reaches
# ``parseFloat``, which reaches libc's ``strtod``, which a freestanding target
# has no definition of. The module then fails to LINK for
# ``wasm32-unknown-unknown`` over a float parser no ``.ct`` container ever
# needs. The decoder below is the whole grammar the document can contain.

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
  var llsAll = newSeq[seq[uint32]](int(pathTotal))
  for i in 0'u64 ..< pathTotal:
    let rawRes = pathReader.readRawById(i)
    if rawRes.isErr:
      return err("paths.dat[" & $i & "]: " & rawRes.error)
    let raw = rawRes.get()
    var pos = 0
    let pathLenRes = decodeVarint(raw, pos)
    if pathLenRes.isErr:
      return err("paths.dat[" & $i & "]: column-aware path_len varint: " &
        pathLenRes.error)
    let pathLen = int(pathLenRes.get())
    if pos + pathLen > raw.len:
      return err("paths.dat[" & $i & "]: path_bytes truncated")
    if probe:
      if pos + pathLen >= raw.len:
        # No room left for the line_count varint after the path bytes.
        # A legacy record ends right after the raw path string.
        return err("paths.dat[" & $i & "]: not Layout A (no line_count)")
      for k in pos ..< pos + pathLen:
        let b = raw[k]
        # Reject control characters except tab — paths are filesystem
        # names which the spec keeps within printable UTF-8 / ASCII.
        if b < 0x09'u8 or (b > 0x0D'u8 and b < 0x20'u8):
          return err("paths.dat[" & $i & "]: not Layout A (control byte in path)")
    pos += pathLen
    let lineCountRes = decodeVarint(raw, pos)
    if lineCountRes.isErr:
      return err("paths.dat[" & $i & "]: column-aware line_count varint: " &
        lineCountRes.error)
    let lineCount = int(lineCountRes.get())
    if probe and lineCount > MaxProbeLineCount:
      return err("paths.dat[" & $i & "]: not Layout A (line_count " &
        $lineCount & " exceeds probe bound)")
    var lls = newSeq[uint32](lineCount)
    var prev: int64 = 0
    for l in 0 ..< lineCount:
      let dRes = decodeSignedVarint(raw, pos)
      if dRes.isErr:
        return err("paths.dat[" & $i & "]: line_length[" & $l & "]: " &
          dRes.error)
      let d = dRes.get()
      let current = if l == 0: d else: prev + d
      if current < 0:
        return err("paths.dat[" & $i & "]: line_length[" & $l &
          "] negative: " & $current)
      lls[l] = uint32(current)
      prev = current
    if probe and pos != raw.len:
      return err("paths.dat[" & $i & "]: not Layout A (" &
        $(raw.len - pos) & " trailing byte(s))")
    llsAll[i] = lls
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
    if rawRes.isErr:
      return err("paths.dat[" & $i & "]: " & rawRes.error)
    let raw = rawRes.get()
    var pos = 0
    let payloadLenRes = decodeVarint(raw, pos)
    if payloadLenRes.isErr:
      return err("paths.dat[" & $i & "]: line-count payload_len varint: " &
        payloadLenRes.error)
    let payloadLen = int(payloadLenRes.get())
    if pos + payloadLen > raw.len:
      return err("paths.dat[" & $i & "]: payload truncated (payload_len " &
        $payloadLen & ", " & $(raw.len - pos) & " byte(s) left)")
    var s = newString(payloadLen)
    for k in 0 ..< payloadLen:
      s[k] = char(raw[pos + k])
    pos += payloadLen
    let countRes = decodeVarint(raw, pos)
    if countRes.isErr:
      return err("paths.dat[" & $i & "]: line_count varint: " & countRes.error)
    let count = countRes.get()
    if count == 0:
      return err("paths.dat[" & $i & "]: line_count is 0 for " & s &
        ". A trace that sets FLAG_HAS_LINE_COUNT_TABLE states every " &
        "file's size, and a file sized 0 shares its base with the next " &
        "one — the two would be indistinguishable at decode")
    if pos != raw.len:
      return err("paths.dat[" & $i & "]: " & $(raw.len - pos) &
        " trailing byte(s) after line_count")
    payloads[i] = s
    counts[i] = count
  ok((payloads, counts))

# ---------------------------------------------------------------------------
# Opening
# ---------------------------------------------------------------------------

proc openNewTraceFromBytes*(data: seq[byte],
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries,
    assumeColumnAwarePaths: bool = false): Result[NewTraceReader, string] =
  ## Open a trace from in-memory bytes. Used for testing.
  ##
  ## ``assumeColumnAwarePaths`` overrides the ``meta.dat`` bit 4
  ## declaration for the ``paths.dat`` record layout only.  Pass it when
  ## you know out of band that the trace was produced by a recorder that
  ## emitted Layout A path records without setting the flag (see the
  ## note at the Layout A block below).  It is an assertion by the
  ## caller, not a guess by the reader: on a trace whose records are not
  ## Layout A the open fails with a named ``paths.dat[N]: …`` error
  ## instead of returning misdecoded positions.

  var reader: NewTraceReader
  reader.data = data
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
  # A container with NO meta.dat is a different case and still opens: the
  # legacy `paths.json` fallback below is the reading for those.
  let metaDataRes = readInternalFile(data, "meta.dat", blockSize, maxEntries)
  if metaDataRes.isOk:
    let metaRes = readMetaDat(metaDataRes.get())
    if metaRes.isErr:
      return err("meta.dat present but not readable: " & metaRes.error)
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

  # paths.json fallback: when no binary paths interning table is
  # present, try the JSON form ct_recorder writes (M13).  The
  # binary table is preferred when both exist — see pathCount / path.
  if reader.pathReader.count() == 0:
    let pathsJsonRes = readInternalFile(data, "paths.json", blockSize, maxEntries)
    if pathsJsonRes.isOk:
      let pathsBytes = pathsJsonRes.get()
      if pathsBytes.len > 0:
        var pathsTxt = newString(pathsBytes.len)
        for i, b in pathsBytes:
          pathsTxt[i] = char(b)
        let parsed = decodeJsonStringArray(pathsTxt)
        if parsed.isSome:
          reader.pathsJson = parsed.get()

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
        return err(parsed.error)
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
        return err(parsed.error)
      reader.lineCountPayloads = parsed.get().payloads
      reader.lineCounts = parsed.get().counts
    else:
      reader.layoutASuspected =
        parseLayoutAPathRecords(reader.pathReader, probe = true).isOk

  # Alternate source views (spec §"Alternate Source Views
  # (Deminification Support)").  When the writer set bit 5 we eagerly
  # decode every record so the per-view accessors below run in O(1).
  # When the bit is clear we don't touch the container — pre-extension
  # traces have no such files.
  if reader.meta.hasAlternateSourceViews:
    # See the writer's note on the abbreviated 12-char base name:
    # ``source_views.dat`` (spec name, 16 chars) collides with
    # ``source_views.off`` in the base40 filename encoding, so the
    # on-disk files are ``srcviews.dat`` / ``srcviews.off``.
    let svRes = initVariableRecordTableReader(
      data, "srcviews", blockSize, maxEntries)
    if svRes.isErr:
      return err("source_views.dat: " & svRes.error)
    let svReader = svRes.get()
    let total = svReader.count()
    reader.sourceViews = newSeq[SourceView](int(total))
    let pathCount = reader.pathReader.count()
    reader.sourceViewsByPath = newSeq[seq[uint64]](int(pathCount))
    for i in 0'u64 ..< total:
      let rawRes = svReader.read(i)
      if rawRes.isErr:
        return err("source_views.dat[" & $i & "]: " & rawRes.error)
      let raw = rawRes.get()
      var pos = 0
      let pathIdRes = decodeVarint(raw, pos)
      if pathIdRes.isErr:
        return err("source_views.dat[" & $i & "]: path_id varint: " &
          pathIdRes.error)
      let pathId = pathIdRes.get()
      if pos >= raw.len:
        return err("source_views.dat[" & $i & "]: view_kind byte missing")
      let viewKind = raw[pos]
      pos += 1
      let viewNameLenRes = decodeVarint(raw, pos)
      if viewNameLenRes.isErr:
        return err("source_views.dat[" & $i & "]: view_name_len: " &
          viewNameLenRes.error)
      let viewNameLen = int(viewNameLenRes.get())
      if pos + viewNameLen > raw.len:
        return err("source_views.dat[" & $i & "]: view_name truncated")
      var viewName = newString(viewNameLen)
      for k in 0 ..< viewNameLen:
        viewName[k] = char(raw[pos + k])
      pos += viewNameLen
      let contentLenRes = decodeVarint(raw, pos)
      if contentLenRes.isErr:
        return err("source_views.dat[" & $i & "]: content_len: " &
          contentLenRes.error)
      let contentLen = int(contentLenRes.get())
      if pos + contentLen > raw.len:
        return err("source_views.dat[" & $i & "]: content truncated")
      var content = newSeq[byte](contentLen)
      for k in 0 ..< contentLen:
        content[k] = raw[pos + k]
      pos += contentLen
      let mapLenRes = decodeVarint(raw, pos)
      if mapLenRes.isErr:
        return err("source_views.dat[" & $i & "]: map_len: " &
          mapLenRes.error)
      let mapLen = int(mapLenRes.get())
      if pos + mapLen > raw.len:
        return err("source_views.dat[" & $i & "]: map truncated")
      var smap = newSeq[byte](mapLen)
      for k in 0 ..< mapLen:
        smap[k] = raw[pos + k]
      pos += mapLen
      reader.sourceViews[int(i)] = SourceView(
        pathId: pathId,
        viewKind: viewKind,
        viewName: viewName,
        content: content,
        sourcemapV3: smap,
      )
      if pathId < pathCount:
        reader.sourceViewsByPath[int(pathId)].add(i)

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

    var data: seq[byte]
    try:
      let f = open(path, fmRead)
      let size = f.getFileSize()
      data = newSeq[byte](size)
      discard f.readBytes(data, 0, size)
      f.close()
    except:
      return err("failed to read file: " & path)

    openNewTraceFromBytes(data, assumeColumnAwarePaths = assumeColumnAwarePaths)

  proc refresh*(r: var NewTraceReader, path: string): Result[void, string] =
    ## Re-read a growing CTFS container into this handle and invalidate every
    ## lazily-opened stream reader whose chunk table may have grown.
    ## Re-opens under the same ``assumeColumnAwarePaths`` reading this
    ## handle was created with.
    let reopened = openNewTrace(path,
      assumeColumnAwarePaths = r.assumedColumnAwarePaths)
    if reopened.isErr:
      return err(reopened.error)
    r = reopened.get()
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
        return err(rawRes.error)
      let raw = rawRes.get()
      var pos = 0
      let pathLenRes = decodeVarint(raw, pos)
      if pathLenRes.isErr:
        return err("paths.dat[" & $id & "]: " & pathLenRes.error)
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
  elif r.pathsJson.len > 0 and id < uint64(r.pathsJson.len):
    ok(r.pathsJson[int(id)])
  else:
    r.pathReader.readById(id)  # error path — preserve the original error

proc function*(r: NewTraceReader, id: uint64): Result[string, string] =
  r.funcReader.readById(id)

proc typeName*(r: NewTraceReader, id: uint64): Result[string, string] =
  r.typeReader.readById(id)

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
  let binary = r.pathReader.count()
  if binary > 0: binary
  else: uint64(r.pathsJson.len)

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

proc lineLength*(r: NewTraceReader, fileId: uint64,
    lineIndex0: uint32): Option[uint32] =
  ## Return the addressable column count of ``lineIndex0`` (0-indexed,
  ## so line 1 of the file is ``lineIndex0 = 0``) in the file with id
  ## ``fileId``.  Returns ``none`` when the trace is not column-aware,
  ## when ``fileId`` is out of range, when the line index is past the
  ## file's known line table, or when the recorder did not surface a
  ## per-line table (``line_count = 0`` in paths.dat).  The back-compat
  ## default is "no per-line data" → ``none``, matching the spec
  ## contract for pre-extension traces.
  ##
  ## Note: callers that have a 1-indexed line number (per the spec
  ## convention used by AbsoluteStep / DeltaStep cursor tracking) must
  ## subtract 1 before calling.
  if not r.meta.hasColumnAwareSteps:
    return none(uint32)
  if fileId >= uint64(r.lineLengths.len):
    return none(uint32)
  let lls = r.lineLengths[fileId]
  if int(lineIndex0) >= lls.len:
    return none(uint32)
  some(lls[int(lineIndex0)])

proc lineLengthRaw*(r: NewTraceReader, fileId: uint64,
    lineIndex0: uint32): Option[uint32] =
  ## Ungated sibling of [lineLength] — surfaces the addressable column
  ## count for ``(fileId, lineIndex0)`` without consulting
  ## ``meta.hasColumnAwareSteps``.  It reads the per-file table the open
  ## call parsed, so it answers for a trace that declares column-aware
  ## steps and for one opened with ``assumeColumnAwarePaths = true``;
  ## on a trace opened normally that declares line-only steps there is
  ## no table and every query is ``none``.  Also ``none`` when ``fileId``
  ## is out of range or ``lineIndex0`` is past the file's line table.
  if fileId >= uint64(r.lineLengths.len):
    return none(uint32)
  let lls = r.lineLengths[fileId]
  if int(lineIndex0) >= lls.len:
    return none(uint32)
  some(lls[int(lineIndex0)])

proc lineCountRaw*(r: NewTraceReader, fileId: uint64): uint64 =
  ## Ungated companion to [lineLengthRaw]: number of lines registered in
  ## paths.dat Layout A for ``fileId``.  Returns ``0`` when this handle
  ## parsed no Layout A table for the file — which includes every trace
  ## that declares line-only steps and was opened without
  ## ``assumeColumnAwarePaths`` (the legitimate "no per-line data"
  ## sentinel — see spec §"paths.dat per-line offset table").
  if fileId >= uint64(r.lineLengths.len):
    return 0'u64
  uint64(r.lineLengths[fileId].len)

proc globalPositionSpace*(r: NewTraceReader): GlobalLineIndex =
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
  buildGlobalLineIndex(positionSpaceCounts(
    r.lineLengths, r.lineCounts, int(r.pathCount()),
    r.meta.hasColumnAwareSteps))

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
  ## same rule the writer's ``rebuildGli`` lays the space out with. That
  ## matters for the files with no line-length table: they occupy
  ## ``DefaultLinesPerFile`` addresses in the space the positions were
  ## encoded in, so sizing them ``0`` here would put every later file's
  ## base that much too low and land the file search in the file before
  ## the right one — which then answers with a line number that is the
  ## next file's base, in range and indistinguishable from a real one.
  if r.posTablesBuilt:
    return
  let fileCount = r.lineLengths.len
  r.lineBase = newSeq[seq[uint64]](fileCount)
  r.fileBase = newSeq[uint64](fileCount)
  r.fileSize = newSeq[uint64](fileCount)
  var runningGlobal: uint64 = 0
  for fid in 0 ..< fileCount:
    let lls = r.lineLengths[fid]
    var lb = newSeq[uint64](lls.len)
    var sum: uint64 = 0
    for i in 0 ..< lls.len:
      lb[i] = sum
      sum += uint64(lls[i])
    r.lineBase[fid] = lb
    r.fileBase[fid] = runningGlobal
    r.fileSize[fid] = fileAddressCount(lls)
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
    return err("file " & $fid & " has no line-length table")

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

proc ensureExecReader(r: var NewTraceReader): Result[void, string] =
  if not r.execLoaded:
    # M24a-1: select the steps.dat/steps.idx framing by the meta.dat
    # ``has_step_stream`` flag.  Bundles written by the current Nim writer
    # (and by the Rust writer) set the flag and use the SPEC-canonical layout
    # (header-less chunks, no total_events trailer) that the Rust
    # ``StepStreamReader`` reads byte-for-byte.  Pre-M24a-1 Nim-v4 bundles
    # never set the flag and use the legacy framing (per-chunk u32 count +
    # total_events trailer); ``legacy = not hasStepStream`` keeps them readable.
    let res = initExecStreamReader(r.data, int(r.blockSize), int(r.maxEntries),
      legacy = not r.meta.hasStepStream)
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
  let chunkIdx = int(n div chunkSize)
  let eventInChunk = int(n mod chunkSize)
  var currentGli: uint64 = 0

  # Decode the containing chunk in one pass via readChunkEvents (O(N)
  # per chunk) rather than looping ``readEvent(i)`` (O(N²) per chunk
  # because each readEvent re-scans from chunk start).  When ct-print
  # calls this proc once per step in a for-loop the difference is
  # cubic vs quadratic — a 1000-event trace went from ~45s to <1s.
  var chunkBuf: seq[StepEvent]
  discard ?r.execReader.readChunkEvents(chunkIdx, chunkBuf)
  for i in 0 .. eventInChunk:
    let ev = chunkBuf[i]
    case ev.kind
    of sekAbsoluteStep:
      currentGli = ev.globalLineIndex
    of sekDeltaStep:
      currentGli = uint64(int64(currentGli) + ev.lineDelta)
    of sekDeltaColumn:
      # P6.5: in column-aware traces ``global_position_index`` is
      # one-dimensional, so a column-only delta is also a position
      # delta.  Apply it to the running GLI so callers that decode the
      # absolute position see the post-column-delta cursor.  In
      # line-only traces this branch never fires because writers
      # cannot emit tag 0x07 without the column flag and the meta-dat
      # strict-rejection check guards against mismatches.
      currentGli = uint64(int64(currentGli) + ev.columnDelta)
    else:
      # Non-step events (raise, catch, thread_switch) don't change GLI
      discard

  ok(currentGli)

proc stepCount*(r: var NewTraceReader): Result[uint64, string] =
  ?r.ensureExecReader()
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
  ?r.ensureExecReader()
  if not r.meta.hasColumnAwareSteps:
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
      else:
        n += 1
  ok(n)

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
  ?r.ensureExecReader()

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

  var currentGli: uint64 = 0
  var events: seq[StepEvent] = @[]
  var n = startN
  while n < stopN:
    let chunkIdx = int(n div chunkSize)
    # Stream all events of the chunk through the cache exactly once.
    # ``readChunkEvents`` returns the chunk's first global event index
    # so we can map seq positions back to absolute step indices.
    let firstIdxRes = r.execReader.readChunkEvents(chunkIdx, events)
    if firstIdxRes.isErr:
      return err(firstIdxRes.error)
    let firstIdx = firstIdxRes.get()

    for offset, ev in events:
      let absIdx = firstIdx + uint64(offset)
      case ev.kind
      of sekAbsoluteStep:
        currentGli = ev.globalLineIndex
      of sekDeltaStep:
        currentGli = uint64(int64(currentGli) + ev.lineDelta)
      of sekDeltaColumn:
        # P6.5: see ``stepAbsoluteGlobalLineIndex`` for rationale —
        # column deltas advance ``global_position_index`` in the
        # one-dimensional column-aware position space.
        currentGli = uint64(int64(currentGli) + ev.columnDelta)
      else:
        discard
      if absIdx >= n and absIdx < stopN:
        output[int(absIdx - startN)] = currentGli

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

proc ensureValueReader(r: var NewTraceReader): Result[void, string] =
  if not r.valueLoaded:
    # M24a-2: select the values.dat/values.idx framing by the meta.dat
    # ``has_value_stream`` flag.  Bundles written by the current Nim writer
    # (and by the Rust writer) set the flag and use the SPEC-canonical chunked
    # layout that the Rust ``ValueStreamReader`` reads byte-for-byte.  Pre-M24a-2
    # Nim-v4 bundles never set the flag and use the legacy ``.off`` VRT framing;
    # ``legacy = not hasValueStream`` keeps them readable.
    let res = initValueStreamReader(r.data, r.blockSize, r.maxEntries,
      legacy = not r.meta.hasValueStream)
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
  ?r.ensureCallReader()
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
    # M24a-3: select the events.dat/events.idx framing by the meta.dat
    # ``has_io_event_stream`` flag.  Bundles written by the current Nim writer
    # (and by the Rust writer) set the flag and use the SPEC-canonical chunked
    # layout that the Rust ``IoEventStreamReader`` reads byte-for-byte.
    # Pre-M24a-3 Nim-v4 bundles never set the flag and use the legacy ``.off``
    # VRT framing; ``legacy = not hasIoEventStream`` keeps them readable.
    let res = initIOEventStreamReader(r.data, r.blockSize, r.maxEntries,
      legacy = not r.meta.hasIoEventStream)
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
