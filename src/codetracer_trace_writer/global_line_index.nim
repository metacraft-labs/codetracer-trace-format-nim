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
## **The apportionment between files is a writer convention, not a
## container format.** The spec (``trace-events.md`` §"Source Location
## Addressing", §"Back-Compatibility") says only that a line-only trace's
## ``global_position_index`` is a ``global_line_index`` in which "each
## integer addresses one line". How many integers each file gets is left
## to the writer, and a container records nothing about the choice — there
## is no stride field, no per-file line count, and no producer identifier
## that would let a reader recover it.
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
## until 2026-09, and no field in the container distinguishes a bundle
## written then from one written now: `meta.dat` carries a schema version
## and a `recorder_id`, neither of which names the writer's address
## packing. A line-only trace recorded before the change therefore reads
## back one line high, everywhere and silently: the address is inside the
## space, so `tryResolve` has nothing to refuse. The refusal above catches
## the Rust writer's packing only because that one lands outside the space.
## Re-recording is the only remedy a reader has.

import results
export results

const DefaultLinesPerFile*: uint64 = 100_000
  ## Addresses allocated to a file with no per-line length table.
  ##
  ## Real line counts would come from the source files, which the writer
  ## does not have; the constant is a ceiling generous enough that a
  ## file's lines never spill into the next file's range. It is a count of
  ## addressable lines, so a file of exactly this many lines fits: lines
  ## `1 .. DefaultLinesPerFile` occupy the whole slot and line
  ## `DefaultLinesPerFile + 1` is the first that spills.
  ##
  ## It lives here, next to the prefix-sum arithmetic it parameterises, so
  ## that the writer that encodes with it and any reader that inverts it
  ## cannot drift apart — the drift being undetectable in the container,
  ## which carries no record of the value used.

proc fileAddressCount*(lineLengths: openArray[uint32]): uint64 =
  ## Addresses the global position space allocates to one file.
  ##
  ## A file with a per-line length table occupies exactly its byte
  ## capacity, so a `global_position_index` inside its range resolves to a
  ## `(line, column)`. A file without one occupies `DefaultLinesPerFile`
  ## addresses and its positions resolve to a line only.
  ##
  ## Either way the count is the number of positions the file has, not one
  ## more: the in-file offset is 0-based, so the slot `[base, base +
  ## count)` holds every position and no address is left unused at the
  ## base.
  ##
  ## A trace may mix the two: `registerPath` takes the line lengths as an
  ## optional argument, so a column-aware recorder that has them for its
  ## own sources and not for a dependency's produces exactly that. The two
  ## sizings are not interchangeable — a file sized 20 here and 100000
  ## there shifts every later file's base — and the container records
  ## neither the sizes nor the rule that produced them. So every party
  ## that lays out the space must call this, not re-derive it: writer,
  ## reader position tables, and the line-only fallback all size a file
  ## the same way or they do not agree on which file a position is in.
  if lineLengths.len == 0:
    return DefaultLinesPerFile
  var total: uint64 = 0
  for L in lineLengths:
    total += uint64(L)
  max(total, 1'u64)

proc positionSpaceCounts*(lineLengths: openArray[seq[uint32]],
    fileCount: int, columnAware: bool): seq[uint64] =
  ## Per-file address counts for a trace with `fileCount` registered
  ## paths, in the order the paths were registered.
  ##
  ## Line-only traces give every file `DefaultLinesPerFile` regardless of
  ## what line-length tables happen to be around, which is what keeps a
  ## pre-column-aware trace byte-for-byte what it always was.
  result = newSeq[uint64](fileCount)
  for i in 0 ..< fileCount:
    if columnAware and i < lineLengths.len:
      result[i] = fileAddressCount(lineLengths[i])
    else:
      result[i] = DefaultLinesPerFile

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
  ## check; `DefaultLinesPerFile` is sized so it does not arise in practice.
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
