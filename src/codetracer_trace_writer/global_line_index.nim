{.push raises: [].}

## Global line index: collapses (file_id, line) into a single varint.
##
## Files are concatenated in file-id order into one per-trace address
## space. A prefix-sum array gives O(1) encode and O(log N) decode via
## binary search:
##
## .. code-block::
##
##   globalIndex(file_id, line) = prefixSum[file_id] + (line - 1)
##   resolve(p)                 = (f, p - prefixSum[f] + 1)
##
## ``line`` is 1-based, so the ``- 1`` puts a file's first line at its own
## ``prefixSum[file_id]`` and its last at ``prefixSum[file_id] + count -
## 1``: the range is exactly the addresses the file's lines occupy, none
## left over and none spilling into the next file. That is what makes a
## file's slot size equal to its line count, and it is the same 0-based
## in-file offset the column-aware mode uses, where offset 0 is line 1
## column 1. See ``codetracer-trace-format-spec/internal-files.md``
## §"Global Line Index" for the encode and ``trace-events.md``
## §"Decoding ``global_position_index``" for the decode.
##
## **How many addresses each file gets is recorded, when the container
## says it is.** The spec (``trace-events.md`` §"Per-File Contiguous
## Integer Ranges") sizes a line-only file at ``file_size = line_count``.
## A container that sets ``meta.dat`` ``FlagHasLineCountTable`` (bit 14)
## carries that count per file in its ``paths.dat`` records, so a reader
## rebuilds the exact space the writer laid out rather than assuming one.
## The counts are mandatory under that bit: every record carries one, and
## a writer that cannot determine a file's real line count records the
## ``DefaultLinesPerFile`` ceiling it used instead of leaving the reader
## to guess it.
##
## A container WITHOUT that bit records nothing about the apportionment —
## no stride field, no per-file line count, no producer identifier — and
## every file is ``DefaultLinesPerFile`` addresses by convention alone.
## That is the pre-table format, still written by every recorder that has
## not opted in, and still read here on exactly those terms.
##
## Two writers of the same container format disagree about it today:
##
## * this one — the prefix sum above, with every file allocated
##   `DefaultLinesPerFile` addresses, and
## * the Rust ``codetracer_trace_writer`` —
##   ``(path_id shl 32) or line``, its ``step_stream.rs``
##   ``pack_global_line_index`` / ``unpack_global_line_index``.
##
## The spec excludes the second (``trace-events.md`` §"The address is a
## prefix sum, and nothing else"): a shift of 32 bounds the trace, costs
## five varint bytes per address after the first against a 2-3 byte
## budget, and — because a container carries no discriminator and both
## inverses always return a plausible pair — leaves a reader handed the
## wrong scheme answering a location that was never in the trace.
##
## So the inverse direction is only defined relative to an assumption about
## the producer. `tryResolve` states that assumption and refuses the
## addresses that contradict it, rather than answering with a plausible
## `(file, line)` pair for a position that was never in the trace.
##
## **A third packing is already on disk.** This writer encoded
## ``prefixSum[file_id] + line`` — the same address space, shifted one up —
## until 2026-09. Such an address is INSIDE the space, so `tryResolve` has
## nothing to refuse and the trace reads back one line high everywhere; the
## refusal above catches the Rust writer's packing only because that one
## lands outside the space.
##
## Nothing in the address distinguishes the two, so the discriminator is
## the container's schema version rather than its arithmetic: `meta.dat` is
## at `MetaDatVersion` 4 for the encode above, and `readMetaDat` refuses
## version 3 and below by name (see its own version history for why a
## shim is not available). `recorder_id` does not serve — it names the
## producer, not the producer's address packing, and the same recorder
## spans the change. Re-recording is the only remedy for a bundle already
## written.

import results
export results

const DefaultLinesPerFile*: uint64 = 100_000
  ## The line count a writer records for a file whose real line count it
  ## cannot determine.
  ##
  ## It is a count of addressable lines, so a file of exactly this many
  ## lines fits: lines `1 .. DefaultLinesPerFile` occupy the whole slot
  ## and line `DefaultLinesPerFile + 1` is the first that would spill.
  ##
  ## Whether that ceiling is safe is not a property a reader can check,
  ## so it is not left to be inferred. A writer that emits the per-file
  ## line-count table (`meta.dat` `FlagHasLineCountTable`) writes this
  ## number into `paths.dat` like any other count, and the reader lays
  ## the file's slot out from what it read. A writer that does not emit
  ## the table produces a container in which every file's size is this
  ## constant by convention and by nothing else — the pre-table format,
  ## kept readable, and the reason the constant still lives here next to
  ## the prefix-sum arithmetic it parameterises.

const ConventionalLineLength*: uint32 = 1024
  ## The per-line position count of the conventional column-aware table
  ## (``internal-files.md`` §"`paths.dat` Layout A"): a column-aware file
  ## first mentioned with no table is recorded as ``DefaultLinesPerFile``
  ## lines of this many positions. A column above it is recorded at this
  ## column of its line; a line above ``DefaultLinesPerFile`` is refused.

proc conventionalLineLengths*(): seq[uint32] =
  ## The conventional column-aware table.
  result = newSeq[uint32](int(DefaultLinesPerFile))
  for L in result.mitems:
    L = ConventionalLineLength

proc isConventionalTable*(lineLengths: openArray[uint32]): bool =
  ## Whether ``lineLengths`` is the conventional table. Decided by content,
  ## so a file is treated alike whether the writer chose the table or the
  ## recorder built it: the two are the same record on the wire.
  if uint64(lineLengths.len) != DefaultLinesPerFile:
    return false
  for L in lineLengths:
    if L != ConventionalLineLength:
      return false
  true

proc columnTableAtFirstMention*(lineLengths: openArray[uint32]): seq[uint32] =
  ## The table a column-aware writer records for a file first mentioned with
  ## ``lineLengths`` (``internal-files.md`` §"`paths.dat` Layout A"):
  ##
  ## * empty — no table given — is the conventional table;
  ## * a table whose lines hold nothing gives its first line one position,
  ##   keeping its line count (``[0]`` → ``[1]``, ``[0, 0]`` → ``[1, 0]``),
  ##   so the file's size is not 0;
  ## * anything else is recorded as given.
  if lineLengths.len == 0:
    return conventionalLineLengths()
  result = @lineLengths
  for L in lineLengths:
    if L != 0:
      return
  result[0] = 1

proc fileAddressCount*(lineLengths: openArray[uint32],
    lineCount: uint64 = 0): uint64 =
  ## Addresses the global position space allocates to one file: **the
  ## number of positions the file has.**
  ##
  ## That is the whole rule. The two addressing modes differ only in what
  ## a *position* is, and each supplies the corresponding count:
  ##
  ## * column-aware — a position is an addressable column, so the file
  ##   has `sum(lineLengths)` of them and `lineLengths` is its per-line
  ##   table;
  ## * line-only — a position is a line, so the file has `lineCount` of
  ##   them and `lineLengths` is empty.
  ##
  ## The units differ (bytes on one axis, lines on the other) because the
  ## address means a different thing in each mode; the sizing does not.
  ## Keeping it in one proc is what stops the two from drifting: a file
  ## sized one way here and another way there shifts the base of every
  ## file after it, and a position then resolves into the wrong file at a
  ## line number that is in range.
  ##
  ## The count is the number of positions, not one more: the in-file
  ## offset is 0-based, so the slot `[base, base + count)` holds every
  ## position and no address is left unused at the base. It is never
  ## zero — a file with no positions would share its base with the next
  ## file, and the two would be indistinguishable at decode.
  ##
  ## Neither input given means the trace records no size for the file:
  ## it occupies `DefaultLinesPerFile` addresses, the pre-table
  ## convention. A trace may mix that with sized files — `registerPath`
  ## takes both the line lengths and the line count as optional
  ## arguments — so every party that lays the space out must call this
  ## rather than re-derive it.
  if lineLengths.len > 0:
    var total: uint64 = 0
    for L in lineLengths:
      total += uint64(L)
    return max(total, 1'u64)
  if lineCount > 0:
    return lineCount
  DefaultLinesPerFile

proc positionSpaceCount*(lineLengths: openArray[seq[uint32]],
    lineCounts: openArray[uint64], fileId: int, columnAware: bool): uint64 =
  ## The address count of the one file `fileId`, by the rule
  ## `positionSpaceCounts` applies to every file. A writer that extends its
  ## space one registration at a time sizes the new file through this, so the
  ## incremental and the one-pass layouts cannot disagree.
  let count =
    if fileId < lineCounts.len: lineCounts[fileId]
    else: 0'u64
  if columnAware and fileId < lineLengths.len:
    fileAddressCount(lineLengths[fileId], count)
  else:
    fileAddressCount([], count)

proc positionSpaceCounts*(lineLengths: openArray[seq[uint32]],
    lineCounts: openArray[uint64],
    fileCount: int, columnAware: bool): seq[uint64] =
  ## Per-file address counts for a trace with `fileCount` registered
  ## paths, in the order the paths were registered.
  ##
  ## `lineLengths` carries the column-aware per-line tables and
  ## `lineCounts` the line-only per-file line counts; a trace supplies
  ## whichever its mode addresses in, and either may be short of
  ## `fileCount` (or empty) for the files it says nothing about. Those
  ## fall back to `DefaultLinesPerFile`, which is what a container
  ## without a size table has always meant.
  result = newSeq[uint64](fileCount)
  for i in 0 ..< fileCount:
    result[i] = positionSpaceCount(lineLengths, lineCounts, i, columnAware)

type
  GlobalLineIndex* = object
    prefixSum*: seq[uint64]  # prefixSum[file_id] = cumulative line count before this file
    totalLines*: uint64

proc buildGlobalLineIndex*(lineCounts: openArray[uint64]): GlobalLineIndex =
  ## Build prefix sum from per-file line counts.
  var prefix = newSeq[uint64](lineCounts.len + 1)
  prefix[0] = 0
  for i in 0 ..< lineCounts.len:
    prefix[i + 1] = prefix[i] + lineCounts[i]
  GlobalLineIndex(
    prefixSum: prefix,
    totalLines: prefix[^1]
  )

proc appendFile*(gli: var GlobalLineIndex, count: uint64) =
  ## Extend the space by one file of `count` addresses, placed after every
  ## file already in it. No existing base moves, so this is O(1) and leaves
  ## the index equal to `buildGlobalLineIndex` over the extended counts.
  ##
  ## `gli` must already be an index (at least `buildGlobalLineIndex([])`):
  ## the default-initialised object has no base for file 0.
  doAssert gli.prefixSum.len > 0,
    "appendFile on an index that was never built"
  gli.totalLines += count
  gli.prefixSum.add(gli.totalLines)

proc globalIndex*(gli: GlobalLineIndex, fileId: int, line: uint64): uint64 =
  ## Convert a 1-based `(file_id, line)` to a global line index:
  ## `prefixSum[fileId] + (line - 1)`. Inverted by `resolve`.
  ##
  ## Line 0 is not a source line, and the offset is clamped at 0 for it.
  ## Unclamped it would be `prefixSum[fileId] - 1`, which wraps to
  ## `2^64 - 1` for file 0 — a ten-byte varint on the wire — and lands in
  ## the previous file's last line for every other file. The clamp costs
  ## injectivity for an input that is not a location anyway: line 0 gets
  ## line 1's address, the file's own base. The column-aware encoder in
  ## `multi_stream_writer.toGlobalLineIndex` clamps to the same address,
  ## so the two modes agree on what a caller's 0 means.
  let inFileOffset = if line == 0: 0'u64 else: line - 1
  gli.prefixSum[fileId] + inFileOffset

proc resolve*(gli: GlobalLineIndex, globalIdx: uint64): (int, uint64) =
  ## Convert global line index back to (file_id, line): binary-search the
  ## prefix sums for the file, then `line = globalIdx - prefixSum[f] + 1`
  ## because the in-file offset is 0-based and lines are 1-based. Inverse
  ## of `globalIndex` over every line a file has.
  ##
  ## Unchecked: `globalIdx` is assumed to be an address of this index, i.e.
  ## below `totalLines`. An index above the top of the space is answered by
  ## clamping to the last file, which yields a file id that exists and a
  ## line number that is arithmetic rather than evidence. Callers that
  ## handle positions from a container — where the producer's packing is
  ## not known — must use `tryResolve` instead.
  # Find the largest fileId where prefixSum[fileId] <= globalIdx
  var lo = 0
  var hi = gli.prefixSum.len - 2  # last valid fileId
  while lo < hi:
    let mid = (lo + hi + 1) div 2
    if gli.prefixSum[mid] <= globalIdx:
      lo = mid
    else:
      hi = mid - 1
  (lo, globalIdx - gli.prefixSum[lo] + 1)

proc tryResolve*(gli: GlobalLineIndex,
    globalIdx: uint64): Result[(int, uint64), string] =
  ## Convert a line-only `global_position_index` back to `(file_id, line)`,
  ## refusing the indices this index cannot address.
  ##
  ## The refusal is what separates a decode from a guess. A line-only
  ## container states neither which packing its producer used nor how many
  ## addresses each file was given (see the module header), so inverting
  ## the integer is an assumption. The assumption is at least falsifiable:
  ## an address at or above `totalLines` is not one this space can have
  ## produced, and the commonest way to reach one is to read a trace whose
  ## producer packed differently — the Rust writer's `(path_id shl 32) or
  ## line` puts every step of every file above id 0 far beyond the top of a
  ## `DefaultLinesPerFile` space.
  ##
  ## `totalLines` is the exact top: the last file's last line sits at
  ## `totalLines - 1`, so nothing this space can encode is refused and
  ## everything above it is.
  ##
  ## What it cannot catch: a spill *within* the space. A file whose lines
  ## run past its slot addresses the next file's range, and the arithmetic
  ## there is indistinguishable from a legitimate address. That is a
  ## property of a packing with no delimiters, not something a reader can
  ## check, so it is caught where the file's size is known — at the
  ## writer, which refuses a step past a file's recorded line count
  ## (`multi_stream_writer.registerStep`) rather than emitting an address
  ## that lands in the next file.
  if gli.prefixSum.len < 2:
    return err("line-only global_position_index " & $globalIdx &
      " cannot be resolved to (file, line): the trace registers no paths")
  if globalIdx >= gli.totalLines:
    return err("line-only global_position_index " & $globalIdx &
      " is outside this trace's address space of " & $gli.totalLines &
      " (" & $(gli.prefixSum.len - 1) & " path(s)). A line-only container " &
      "records no packing discriminator, and the writers disagree: " &
      "codetracer_trace_format_nim packs prefixSum[path_id] + (line - 1), " &
      "the Rust codetracer_trace_writer packs (path_id shl 32) or line " &
      "(step_stream.rs pack_global_line_index). Resolving this index would " &
      "require an assumption the container does not carry")
  ok(gli.resolve(globalIdx))
