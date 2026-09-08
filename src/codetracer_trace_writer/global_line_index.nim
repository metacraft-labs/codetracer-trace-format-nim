{.push raises: [].}

## Global line index: collapses (file_id, line) into a single varint.
##
## Each file contributes a range of global line numbers. A prefix-sum array
## allows O(1) encode and O(log N) decode via binary search.
##
## **This packing is a writer convention, not a container format.** The
## spec (``codetracer-trace-format-spec/trace-events.md`` §"Source Location
## Addressing", §"Back-Compatibility") says only that a line-only trace's
## ``global_position_index`` is a ``global_line_index`` in which "each
## integer addresses one line". How the integers are apportioned between
## files is left to the writer, and a container records nothing about the
## choice — there is no stride field, no per-file line count, and no
## producer identifier that would let a reader recover it.
##
## Two writers of the same container format disagree about it today:
##
## * this one — ``prefixSum[file_id] + line``, with every file allocated
##   `DefaultLinesPerFile` addresses, and
## * the Rust ``codetracer_trace_writer`` —
##   ``(path_id shl 32) or line``, its ``step_stream.rs``
##   ``pack_global_line_index`` / ``unpack_global_line_index``.
##
## So the inverse direction is only defined relative to an assumption about
## the producer. `tryResolve` states that assumption and refuses the
## addresses that contradict it, rather than answering with a plausible
## `(file, line)` pair for a position that was never in the trace.

import results
export results

const DefaultLinesPerFile*: uint64 = 100_000
  ## Addresses allocated to a file with no per-line length table.
  ##
  ## Real line counts would come from the source files, which the writer
  ## does not have; the constant is a ceiling generous enough that a
  ## file's lines never spill into the next file's range.
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
  ## Convert (file_id, line) to global line index.
  gli.prefixSum[fileId] + line

proc resolve*(gli: GlobalLineIndex, globalIdx: uint64): (int, uint64) =
  ## Convert global line index back to (file_id, line).
  ## Uses binary search on the prefix sum array.
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
  (lo, globalIdx - gli.prefixSum[lo])

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
      "codetracer_trace_format_nim packs prefixSum[path_id] + line, the " &
      "Rust codetracer_trace_writer packs (path_id shl 32) or line " &
      "(step_stream.rs pack_global_line_index). Resolving this index would " &
      "require an assumption the container does not carry")
  ok(gli.resolve(globalIdx))
