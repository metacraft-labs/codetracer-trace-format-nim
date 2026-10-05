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
##
## # An image read from its file as it is used
##
## `openFileImage` opens a container by path without reading it whole: the
## image is the file's length, and holds its header and root directory at
## first, each mapping block when `viewMember` resolves a member through it,
## and each member's bytes when a reader first asks for them (`load`). A
## reader that answers its first question from a few members reads those
## members and not the file. Every byte read is read once, and the file stays
## open until the image is released. A range must be loaded before it is read
## in place: `span` refuses, as a defect, to read a range of such an image
## that has not been. An image held whole (`newContainerImage`) needs no load.

import results
import ./types
import ./container

export results

const fileImages = defined(posix) or defined(windows)
  ## Whether this target has files to read an image out of.

type
  ContainerImageObj = object
    bytes*: seq[byte]
      ## The container's bytes. In an image read from its file, a block's
      ## bytes are its own only once `loaded` says so.
    loaded: seq[bool]
      ## Per block, for an image read from its file: its bytes have been read.
      ## Empty for an image held whole.
    blockSize: int
    when fileImages:
      file: File
      path: string

  ContainerImage* = ref ContainerImageObj
    ## A container's bytes, shared by the views of its members.

  MemberView* = object
    image: ContainerImage
    runs: seq[MemberRun]   ## in member order
    starts: seq[int]       ## the member offset at which each run begins
    len: int

proc `=destroy`(x: ContainerImageObj) =
  when fileImages:
    if x.file != nil:
      close(x.file)
    `=destroy`(x.path)
  `=destroy`(x.bytes)
  `=destroy`(x.loaded)

proc `=copy`(a: var ContainerImageObj, b: ContainerImageObj) {.error.}

proc newContainerImage*(bytes: sink seq[byte]): ContainerImage =
  ContainerImage(bytes: bytes)

proc readsFromFile*(image: ContainerImage): bool {.inline.} =
  ## True for an image read from its file as it is used (`openFileImage`).
  ## Never, on a target with no files: none of the loading below is linked.
  when fileImages: image.loaded.len > 0
  else: false

proc loadBlocks(image: ContainerImage, first, last: int): Result[void, string] =
  ## Read blocks `first .. last` of a file-backed image, those not read yet,
  ## each run of them with one read.
  when fileImages:
    var b = first
    while b <= last:
      if image.loaded[b]:
        inc b
        continue
      var e = b
      while e + 1 <= last and not image.loaded[e + 1]:
        inc e
      let at = b * image.blockSize
      let n = min((e + 1) * image.blockSize, image.bytes.len) - at
      var got = 0
      try:
        image.file.setFilePos(at)
        got = image.file.readBuffer(addr image.bytes[at], n)
      except IOError, OSError:
        got = -1
      if got != n:
        return err("container file " & image.path & ": blocks " & $b &
          " to " & $e & " could not be read; the file is shorter than when " &
          "it was opened, or unreadable")
      for k in b .. e:
        image.loaded[k] = true
      b = e + 1
    ok()
  else:
    err("this target reads no files")

proc loadRange(image: ContainerImage, at, n: int): Result[void, string] =
  ## Make image bytes `[at, at + n)` its own.
  if n <= 0:
    return ok()
  image.loadBlocks(at div image.blockSize, (at + n - 1) div image.blockSize)

proc blocksRead*(image: ContainerImage): int =
  ## How many blocks of an image read as it is used have been read from its
  ## file so far; 0 for an image held whole.
  for b in image.loaded:
    if b: inc result

proc isLoaded(image: ContainerImage, at: int): bool {.inline.} =
  not image.readsFromFile or image.loaded[at div image.blockSize]

when fileImages:
  proc openFileImage*(path: string,
      blockSize: uint32 = DefaultBlockSize,
      maxEntries: uint32 = DefaultMaxRootEntries): Result[ContainerImage, string] =
    ## The container at `path`, read as it is used: its first block and its
    ## root directory now, everything else on first use. A container this
    ## could not read in place — a compact one, or one stored under a
    ## whole-file scheme, whose members do not lie in blocks — is read whole,
    ## and so is one shorter than a block. `readsFromFile` tells the two apart.
    var f: File
    if not open(f, path, fmRead):
      return err("failed to read file: " & path)
    var image = ContainerImage(blockSize: int(blockSize), file: f, path: path)
    try:
      image.bytes = newSeqUninit[byte](f.getFileSize())
    except IOError, OSError:
      return err("failed to read file: " & path)
    let whole = image.bytes.len < int(blockSize) or blockSize == 0
    if not whole:
      image.loaded = newSeq[bool](image.bytes.len div int(blockSize) +
        ord(image.bytes.len mod int(blockSize) != 0))
      ? image.loadBlocks(0, 0)
    template d: untyped = image.bytes
    let blockBody = not whole and hasCtfsMagic(d) and d.len > V6CompressionOffset and
      (d[5] != CtfsVersionV6 or (d[V6ProfileOffset] == uint8(ord(cpFull)) and
        d[V6CompressionOffset] == uint8(ord(wfcNone))))
    if not blockBody:
      image.loaded = @[]
      try:
        image.file.setFilePos(0)
        if image.file.readBuffer(addr image.bytes[0], image.bytes.len) !=
            image.bytes.len:
          return err("failed to read file: " & path)
      except IOError, OSError:
        return err("failed to read file: " & path)
      close(image.file)
      image.file = nil
      return ok(image)
    # The root directory: the entry array after the header, as many entries as
    # the header or the reader allows, whichever is more.
    let base = if d[5] == CtfsVersionV6: V6HeaderSize else: HeaderSize + ExtHeaderSize
    let declared = uint32(d[12]) or (uint32(d[13]) shl 8) or
      (uint32(d[14]) shl 16) or (uint32(d[15]) shl 24)
    let rootEnd = min(uint64(base) + uint64(max(declared, maxEntries)) *
      uint64(FileEntrySize), uint64(image.bytes.len))
    ? image.loadRange(0, int(rootEnd))
    ok(image)

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
  ## what `readInternalFile` refuses. A mapping block of an image read from
  ## its file is read as the member is resolved through it.
  if image.readsFromFile:
    var failed = ""
    let loader: BlockLoader = proc (b: uint64): bool =
      let r = image.loadBlocks(int(b), int(b))
      if r.isErr: failed = r.unsafeError
      r.isOk
    let runs = locateMember(image.bytes, name, blockSize, maxEntries, loader)
    if runs.isErr:
      return err(if failed.len > 0: failed else: runs.unsafeError)
    return ok(init(image, runs.unsafeGet()))
  ok(init(image, ? locateMember(image.bytes, name, blockSize, maxEntries)))

proc viewBytes*(bytes: sink seq[byte]): MemberView =
  ## A member already held as a buffer of its own.
  let n = bytes.len
  init(newContainerImage(bytes),
    if n == 0: newSeq[MemberRun](0) else: @[(at: 0, len: n)])

proc len*(v: MemberView): int {.inline.} = v.len

proc loadSlow(v: MemberView, first, n: int): Result[void, string] =
  var k = 0
  while k < v.runs.len and v.starts[k] + v.runs[k].len <= first:
    inc k
  var at = first
  let stop = first + n
  while at < stop and k < v.runs.len:
    let into = at - v.starts[k]
    let take = min(stop - at, v.runs[k].len - into)
    ? v.image.loadRange(v.runs[k].at + into, take)
    at += take
    inc k
  ok()

template ensureLoaded*(v: MemberView, first, n: int) =
  ## Make member bytes `[first, first + n)` readable in place, reading them
  ## from the file of an image read as it is used; a refusal is returned from
  ## the enclosing proc. Free for an image held whole.
  if v.image.readsFromFile:
    ? v.loadSlow(first, n)

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
  template mustBeLoaded(at, n: int) =
    doAssert v.image.isLoaded(at) and v.image.isLoaded(at + n - 1),
      "a member range read before it was loaded from its file"
  var k = v.runOf(first)
  let into = first - v.starts[k]
  if into + n <= v.runs[k].len:
    mustBeLoaded(v.runs[k].at + into, n)
    return cast[ptr UncheckedArray[byte]](addr v.image.bytes[v.runs[k].at + into])
  scratch.setLenUninit(n)  # every byte copied below
  var done = 0
  var off = into
  while done < n:
    let take = min(n - done, v.runs[k].len - off)
    mustBeLoaded(v.runs[k].at + off, take)
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

proc contents*(v: MemberView): Result[seq[byte], string] =
  ## The whole member as a buffer of its own, read from the file of an image
  ## read as it is used.
  v.ensureLoaded(0, v.len)
  ok(v.copyOut(0, v.len))

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
