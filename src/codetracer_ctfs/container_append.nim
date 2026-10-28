when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Appending internal files to an **already-closed** CTFS container.
##
## `container.nim`'s `addFile` / `writeToFile` run *mid-write*: they operate on
## a live `Ctfs` the producer is still filling, and the container is sealed
## afterwards by `closeCtfs` / `writeCtfsToFile`. There was no way to add a
## stream to a container that had already been sealed, which is a real gap
## rather than a theoretical one — a consumer that derives data *from* a
## finished trace (the WASM replay snapshotter of
## `codetracer-specs/Recording-Backends/WASM-Replay-Snapshots-And-Slices.md`
## §6, whose `snap*` namespaces must live **inside** the `.ct` and not beside
## it) only learns what it wants to store after the trace writer has closed the
## container.
##
## Filling that gap here, in the canonical writer, is the point. The
## alternative — a second implementation of the container layout in the
## consumer — was tried and drifted: two writers disagreed about whether the
## multi-level block mapping of `CTFS-Binary-Format.md` §4 is cumulative, and
## containers written the wrong way were silently mis-read past ~511 data
## blocks (~2 MB at the default 4096-byte block). This module therefore adds
## **no** mapping arithmetic of its own: it reconstructs a `Ctfs` over the
## sealed bytes and then calls exactly the same `addFile` / `writeToFile` /
## `insertDataBlock` the live writer uses, so there is only ever one
## implementation of §4 to be right or wrong.
##
## # What "reopen" costs
##
## The whole container is read into memory, because `Ctfs.data` is a flat
## byte-indexed image and `blockOffset` addresses it absolutely — the same
## design the live writer already has (it holds the whole trace in memory
## until it is closed). Only the *tail* and block 0 are written back, so the
## bytes of the existing streams are never rewritten and cannot be damaged by
## a partial write.
##
## # Write ordering
##
## `CTFS-Binary-Format.md` §6's writer protocol, applied to the append: every
## new data and mapping block is written and flushed **first**, and block 0 —
## the only thing that makes them reachable — is rewritten and flushed
## **last**. A crash in between leaves a container with unreferenced trailing
## blocks, which is wasteful but perfectly readable, rather than a file entry
## pointing at absent data.
##
## # Deliberate limits
##
##   - **The container must be quiescent.** No other process may be writing
##     it. The trace writer has closed it by the time a derived stream is
##     attached.
##   - **Version 5 only.** Older headers are refused by name
##     (`ctfs-container.md` §2), as every reader of this library refuses them;
##     appending into a container no reader here would open
##     is refused rather than attempted.
##   - **Never overwrites.** CTFS is append-only; a name that already exists
##     is an error, because a stale-but-present stream is exactly the
##     "returns the wrong bytes" failure the format's consumers cannot see.
##   - **The entry array may span several blocks** (`ctfs-container.md` §1,
##     `root_blocks`), and it does not grow: a batch the directory cannot hold
##     whole is refused before anything is written, naming the first member
##     that does not fit and the occupancy (§5). The append writes, in order:
##     the tail, then the changed entries in one write. The header and every
##     byte in `[root_blocks, previous end of file)` are never rewritten.
##   - **Not while a seal observer watches the container**
##     (`CTFS-Binary-Format.md` §2.15.2): the per-seal log has no item for an
##     append, so it is refused, naming the container.

from std/posix import fsync
import std/oserrors
import results
import ./types
import ./base40
import ./container
import ./checked_io

export container

proc appendError(path, msg: string): string =
  "ctfs append: " & path & ": " & msg

proc openClosedCtfs*(path: string): Result[Ctfs, string] =
  ## Reconstruct a writable `Ctfs` over a sealed container on disk.
  ##
  ## Everything the live writer keeps in RAM is recovered from the file: the
  ## header fields directly, and `nextFreeBlock` from the file length. The
  ## latter is sound precisely because the writer materialises every block it
  ## allocates — `allocBlock` extends `data` to `nextFreeBlock * blockSize`
  ## and the streaming path flushes the new block immediately — so a sealed
  ## container is always a whole number of blocks and its length *is* the
  ## allocator's state. `CTFS-Binary-Format.md` §6 keeps `NextFreeBlock` as
  ## live shared state and does not persist it, so this is the only way to
  ## recover it, and a file length that is not a block multiple means the
  ## container is truncated or still being written — refused, not rounded.
  let raw = readCtfsFromFile(path)
  if raw.isErr:
    return err(appendError(path, raw.error))
  let data = raw.get()

  if data.len < HeaderSize + ExtHeaderSize:
    return err(appendError(path,
      "is " & $data.len & " bytes, too small to carry a CTFS header"))
  if not hasCtfsMagic(data):
    return err(appendError(path,
      "does not start with the CTFS magic C0DE72ACE2; it is not a CTFS container"))
  if data[5] != CtfsVersion:
    return err(appendError(path,
      "is CTFS v" & $data[5] & "; appending is supported only for v" &
      $CtfsVersion & " containers, whose header byte layout this writer produced"))
  if readEncryptionMethod(data) != emNone:
    return err(appendError(path,
      "is encrypted; its block mapping is opaque without the key, so no " &
      "internal file can be added"))

  let blockSize = readU32LE(data, 8)
  let refusal = blockSizeRefusal(blockSize)
  if refusal.len > 0:
    return err(appendError(path, refusal))
  if data.len mod int(blockSize) != 0:
    return err(appendError(path,
      "is " & $data.len & " bytes, not a whole number of " & $blockSize &
      "-byte blocks; it is truncated or still being written"))

  var maxRootEntries = readU32LE(data, 12)
  if maxRootEntries == 0'u32:
    # A zero means "fill block 0" — `ctfs-container.md` §1's normative reading
    # of `MaxRootEntries`, which yields 170 entries at the default 4096-byte
    # block ((4096 - 16) / 24). Every reader in this library applies the same
    # rule (`types.rootEntryCount`): `findFileEntry`, and through it
    # `readInternalFile` and `hasInternalFile`, reads a declared `0` passed
    # through from the header as the auto-filled root, and
    # `codetracer_trace_reader`, `native_decoder`, ct-print and the space
    # analyzer read the header through `headerRootEntryCount`. Until
    # 2026-10-14 they did not: the first group scanned no slot and the
    # `native_decoder` / ct-print pair refused the header, so a container
    # from a producer that declares the spec's recommended `0` read as empty
    # (`tests/test_root_auto_fill.nim`). No writer in this library declares
    # `0` (`createCtfs` defaults to `DefaultMaxRootEntries`, 31), so no
    # container written here was ever read differently.
    maxRootEntries = uint32(
      (int(blockSize) - HeaderSize - ExtHeaderSize) div FileEntrySize)
  if uint64(data.len) < rootBlockCount(blockSize, maxRootEntries, readMaxShards(data)) *
      uint64(blockSize):
    return err(appendError(path,
      "declares " & $maxRootEntries & " root entries, whose root region is " &
      "longer than the file"))

  var c: Ctfs
  c.data = data
  c.blockSize = blockSize
  c.maxRootEntries = maxRootEntries
  c.encryption = emNone
  c.maxShards = readMaxShards(data)
  c.nextFreeBlock = uint64(data.len div int(blockSize))
  c.streaming = false
  ok(c)

proc findEntrySlot(c: Ctfs, encodedName: uint64): int =
  ## Index of the root entry carrying `encodedName`, or -1.
  for i in 0 ..< int(c.maxRootEntries):
    let off = c.fileEntryOffset(i)
    if off + FileEntrySize > c.data.len:
      break
    if readU64LE(c.data, off + 16) == encodedName:
      return i
  -1

proc syncToDisk(f: File): Result[void, string] =
  ## Durability barrier between the two write phases.
  ##
  ## The stdio flush hands the buffered bytes to the operating system, and is
  ## where a write `fwrite` only buffered fails (a full disk, a file-size
  ## limit). The ordering guarantee the append rests on ("the new blocks are
  ## on disk before block 0 points at them") needs the page cache pushed out
  ## too, hence the `fsync`. On platforms without `fsync` the stdio flush is
  ## what there is, and the ordering still holds against a process crash —
  ## which is the failure this protects against in practice, since the
  ## container is written once and closed.
  ##
  ## A failure of either is returned, so that the root region is never
  ## rewritten to point at blocks that did not reach the file.
  ? f.flushChecked()
  when defined(posix):
    if fsync(f.getFileHandle()) != 0:
      return err("fsync failed: " & osErrorMsg(osLastError()))
  ok()

when defined(ctfsAppendFaultInjection):
  var ctfsAppendStopAtMidpoint*: bool = false
    ## Test-only fault injection: abandon an append **between** its two write
    ## phases, which is the one instant the durability argument is about.
    ##
    ## This is compiled in only under `-d:ctfsAppendFaultInjection`; a normal
    ## build of this module contains no trace of it, and the release library
    ## the Rust crate links cannot reach it.
    ##
    ## It exists because the ordering in `writeAppendedBlocks` is the whole
    ## reason an interrupted append leaves the *old valid container* rather
    ## than a corrupt one, and until M57 nothing tested it. Every append test
    ## in both repos compares two quiescent snapshots — before the call and
    ## after it returns — so reversing the two phases changed no observable
    ## byte and the entire corpus stayed green.
    ##
    ## There is no seam in the standard library to observe write ordering from
    ## the outside (no injectable `File`, no fsync hook), and the container is
    ## written through concrete `std` calls by design. Stopping at the
    ## midpoint is therefore the honest way to ask the question: it produces a
    ## *real* half-finished container on a *real* filesystem, and the test
    ## then reads it back with the production reader. Nothing is mocked.
    ##
    ## See `tests/test_container_append_ordering.nim`.

proc closeAfterFailure(f: var File) {.raises: [].} =
  ## Close `f` on the way out of a failed append. The failure already
  ## reported is the one the caller needs; a close that fails as well adds
  ## nothing to it.
  discard f.closeChecked()

when defined(ctfsAppendFaultInjection):
  var ctfsAppendFailEntryWrite*: bool = false
    ## Test-only: the entry write fails (as a full disk or an I/O error would
    ## make it), after the tail is on disk. The append reports it, and the
    ## container keeps its old entries.

proc writeAppendedBlocks(c: Ctfs, path: string, firstNewBlock: uint64,
                         oldRoot: seq[byte]): Result[void, string] =
  ## Write the appended tail, then the changed entries in one write
  ## (`ctfs-container.md` §5, "Write ordering" and "Batching"): the header is
  ## not rewritten, and nothing in `[root_blocks, previous end of file)` is.
  let bs = int(c.blockSize)
  let tailStart = int(firstNewBlock) * bs
  if tailStart > c.data.len:
    return err(appendError(path, "internal error: the appended image shrank"))
  # The changed entries: the one range of the root region the append writes.
  var lo = -1
  var hi = -1
  for k in 0 ..< min(oldRoot.len, c.data.len):
    if oldRoot[k] != c.data[k]:
      if k < containerHeaderSize(c.data):
        return err(appendError(path, "internal error: the append changed the header"))
      if lo < 0: lo = k
      hi = k
  if lo >= 0:
    lo = containerHeaderSize(c.data) + ((lo - containerHeaderSize(c.data)) div FileEntrySize) * FileEntrySize
    hi = containerHeaderSize(c.data) + ((hi - containerHeaderSize(c.data)) div FileEntrySize + 1) * FileEntrySize

  var f: File
  if not open(f, path, fmReadWriteExisting):
    return err(appendError(path, "cannot reopen the container for append"))
  try:
    if c.data.len > tailStart:
      let n = c.data.len - tailStart
      f.setFilePos(int64(tailStart))
      if f.writeBuffer(unsafeAddr c.data[tailStart], n) != n:
        closeAfterFailure(f)
        return err(appendError(path, "short write appending " & $n & " bytes"))
      let synced = syncToDisk(f)
      if synced.isErr:
        closeAfterFailure(f)
        return err(appendError(path, "writing the appended blocks: " &
          synced.error))

    # ---- the midpoint the durability argument turns on ----------------
    # Everything above is on disk; nothing below has run. A crash here is
    # the case `CTFS-Binary-Format.md` §5d describes: unreferenced trailing
    # blocks, and the old entries. This statement sits *between* the two
    # phases deliberately, so that swapping them moves the two writes around
    # it rather than moving it along with either.
    when defined(ctfsAppendFaultInjection):
      if ctfsAppendStopAtMidpoint:
        closeAfterFailure(f)
        return err(appendError(path,
          "fault injection: abandoned between the tail write and the entries"))

    # Only now does anything point at the blocks just written: the changed
    # entries, in one write.
    if lo >= 0:
      when defined(ctfsAppendFaultInjection):
        if ctfsAppendFailEntryWrite:
          closeAfterFailure(f)
          return err(appendError(path, "fault injection: the entry write failed"))
      f.setFilePos(int64(lo))
      if f.writeBuffer(unsafeAddr c.data[lo], hi - lo) != hi - lo:
        closeAfterFailure(f)
        return err(appendError(path, "short write of the appended entries"))
      let synced = syncToDisk(f)
      if synced.isErr:
        closeAfterFailure(f)
        return err(appendError(path, "writing the appended entries: " &
          synced.error))
  except IOError, OSError:
    closeAfterFailure(f)
    return err(appendError(path, "I/O error while appending: " &
      getCurrentExceptionMsg()))
  let closed = f.closeChecked()
  if closed.isErr:
    return err(appendError(path, "closing the container after the append: " &
      closed.error))
  ok()

proc appendInternalFiles*(path: string, names: openArray[string],
                          contents: openArray[seq[byte]]): Result[void, string] =
  ## Append internal files to the sealed container at `path`.
  ##
  ## The batch is attached as a unit: every stream's blocks are written first,
  ## and the single rewrite of block 0 at the end publishes all of them at
  ## once. There is deliberately no singular version of this call — attaching
  ## a related set of streams one at a time would make a half-attached
  ## container reachable, and every consumer would have to cope with it.
  ##
  ## A batch that would leave the container carrying some but not all of the
  ## interning tables is refused, and the file is not touched.
  if names.len != contents.len:
    return err("ctfs append: " & $names.len & " name(s) for " &
      $contents.len & " content buffer(s)")
  if names.len == 0:
    return ok()

  var encoded = newSeq[uint64](names.len)
  for i in 0 ..< names.len:
    if not base40Encodable(names[i]):
      return err("ctfs append: \"" & names[i] &
        "\" is not a CTFS internal filename: base40 packs at most 12 characters " &
        "from [0-9a-z./-] into the u64 name field")
    encoded[i] = base40Encode(names[i])
    for j in 0 ..< i:
      if encoded[j] == encoded[i]:
        return err("ctfs append: " & names[i] & " appears twice in one batch")

  if hasSealObserver(path):
    return err(appendError(path, "a seal observer is registered on this " &
      "container; the per-seal log cannot observe an append " &
      "(CTFS-Binary-Format.md §2.15.2)"))
  var c = ?openClosedCtfs(path)
  let firstNewBlock = c.nextFreeBlock
  let oldRoot = c.data[0 ..< min(c.data.len, int(c.rootBlockCount()) * int(c.blockSize))]

  for i in 0 ..< names.len:
    if c.findEntrySlot(encoded[i]) >= 0:
      return err(appendError(path, "already contains an internal file named " &
        names[i] & "; CTFS is append-only and this writer will not overwrite it"))

  # A batch the directory cannot hold whole is refused before anything is
  # written, naming the first member that does not fit and the occupancy
  # (`ctfs-container.md` §5): the root does not grow (§1, rule 2), and no part
  # of a batch is attached.
  var free = 0
  var used = 0
  for i in 0 ..< int(c.maxRootEntries):
    let off = c.fileEntryOffset(i)
    if off + FileEntrySize > c.data.len: break
    if readU64LE(c.data, off) == 0 and readU64LE(c.data, off + 8) == 0 and
        readU64LE(c.data, off + 16) == 0: inc free
    else: inc used
  if free < names.len:
    return err(appendError(path, "the root directory cannot hold the batch " &
      "whole: " & names[free] & " does not fit (" & $used & " of " &
      $c.maxRootEntries & " entries used, " & $free & " free, " & $names.len &
      " to add); a closed container's root does not grow"))

  for i in 0 ..< names.len:
    let handle = c.addFile(names[i])
    if handle.isErr:
      return err(appendError(path, "cannot add " & names[i] & ": " & handle.error))
    var f = handle.get()
    let written = c.writeToFile(f, contents[i])
    if written.isErr:
      return err(appendError(path, "cannot write " & names[i] & ": " & written.error))

  # The appended container is a finished one, so it carries the interning
  # tables all four or none (`interningSetRefusal`). Refused before anything
  # reaches the file, which keeps the container on disk as it was.
  let partialSet = interningSetRefusal(c)
  if partialSet.len > 0:
    return err(appendError(path, partialSet))

  writeAppendedBlocks(c, path, firstNewBlock, oldRoot)
