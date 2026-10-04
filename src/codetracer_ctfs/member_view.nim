{.push raises: [].}

## A member of a container read in place.
##
## A reader that holds a container's bytes and reads its members out of them
## needs no copy of a member: a `MemberView` is the container image, shared
## (`ContainerImage`, a reference), and where the member's bytes lie in it
## (`locateMember`'s runs). A range of the member that lies in one run is read
## where it is; one that straddles two runs — a chunk that crosses a block
## boundary — is copied into a scratch buffer the caller keeps. Bounds are
## the member's: every accessor checks its range against the member's length.
##
## A member that is already a buffer of its own (read from a file, or copied
## out by `readInternalFile`) is viewed as one run over itself (`viewBytes`).

import results
import ./types
import ./container

export results

type
  ContainerImage* = ref object
    ## A container's bytes, shared by the views of its members.
    bytes*: seq[byte]

  MemberView* = object
    image: ContainerImage
    runs: seq[MemberRun]   ## in member order
    starts: seq[int]       ## the member offset at which each run begins
    len: int

proc newContainerImage*(bytes: sink seq[byte]): ContainerImage =
  ContainerImage(bytes: bytes)

proc init(image: ContainerImage, runs: sink seq[MemberRun]): MemberView =
  result = MemberView(image: image, runs: runs)
  result.starts = newSeqUninit[int](result.runs.len)
  for i, r in result.runs.pairs:
    result.starts[i] = result.len
    result.len += r.len

proc viewMember*(image: ContainerImage, name: string,
    blockSize: uint32 = DefaultBlockSize,
    maxEntries: uint32 = DefaultMaxRootEntries): Result[MemberView, string] =
  ## The internal file `name` of the container `image`, in place. Refuses
  ## what `readInternalFile` refuses.
  ok(init(image, ? locateMember(image.bytes, name, blockSize, maxEntries)))

proc viewBytes*(bytes: sink seq[byte]): MemberView =
  ## A member already held as a buffer of its own.
  let n = bytes.len
  init(newContainerImage(bytes),
    if n == 0: newSeq[MemberRun](0) else: @[(at: 0, len: n)])

proc len*(v: MemberView): int {.inline.} = v.len

proc runOf(v: MemberView, offset: int): int =
  ## The run that holds member byte `offset`, which is below `len`.
  var lo = 0
  var hi = v.runs.len - 1
  while lo < hi:
    let mid = (lo + hi + 1) div 2
    if v.starts[mid] <= offset: lo = mid
    else: hi = mid - 1
  lo

proc span*(v: MemberView, first, n: int,
    scratch: var seq[byte]): ptr UncheckedArray[byte] =
  ## Member bytes `[first, first + n)`, which the caller has checked lie
  ## within `len`: in place when they lie in one run, otherwise copied into
  ## `scratch`. Valid until `scratch` changes or the image is released. Nil
  ## for `n == 0`.
  doAssert first >= 0 and n >= 0 and first + n <= v.len
  if n == 0:
    return nil
  var k = v.runOf(first)
  let into = first - v.starts[k]
  if into + n <= v.runs[k].len:
    return cast[ptr UncheckedArray[byte]](addr v.image.bytes[v.runs[k].at + into])
  scratch.setLenUninit(n)  # every byte copied below
  var done = 0
  var off = into
  while done < n:
    let take = min(n - done, v.runs[k].len - off)
    copyMem(addr scratch[done], addr v.image.bytes[v.runs[k].at + off], take)
    done += take
    inc k
    off = 0
  cast[ptr UncheckedArray[byte]](addr scratch[0])

template withSpan*(v: MemberView, first, n: int, scratch: var seq[byte],
    bytes, body: untyped) =
  ## Run `body` with `bytes`, an `openArray[byte]` over member bytes
  ## `[first, first + n)` (see `span`).
  let p = v.span(first, n, scratch)
  template bytes: untyped = p.toOpenArray(0, n - 1)
  body

proc copyOut*(v: MemberView, first, n: int): seq[byte] =
  ## Member bytes `[first, first + n)` as a buffer of their own.
  var scratch: seq[byte]
  let p = v.span(first, n, scratch)
  result = newSeqUninit[byte](n)
  if n > 0:
    copyMem(addr result[0], p, n)

proc readU64LE*(v: MemberView, offset: int): uint64 =
  ## The little-endian `u64` at member byte `offset`.
  var scratch: seq[byte]
  let p = v.span(offset, 8, scratch)
  for i in 0 ..< 8:
    result = result or (uint64(p[i]) shl (8 * i))

proc readU32LE*(v: MemberView, offset: int): uint32 =
  ## The little-endian `u32` at member byte `offset`.
  var scratch: seq[byte]
  let p = v.span(offset, 4, scratch)
  for i in 0 ..< 4:
    result = result or (uint32(p[i]) shl (8 * i))
