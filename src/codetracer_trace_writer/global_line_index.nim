{.push raises: [].}

## Global line index: collapses (file_id, line) into a single varint.
##
## Each file contributes a range of global line numbers. A prefix-sum array
## allows O(1) encode and O(log N) decode via binary search.

import results
export results

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
