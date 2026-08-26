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
##   - **v4 only.** v2/v3 headers spell bytes 6 and 7 differently
##     (`container.nim`'s `readEncryptionMethod`), and this writer only ever
##     produced v4. Appending into a header whose fields mean something else
##     is refused rather than guessed at.
##   - **Never overwrites.** CTFS is append-only; a name that already exists
##     is an error, because a stale-but-present stream is exactly the
##     "returns the wrong bytes" failure the format's consumers cannot see.
##   - **The entry array must live in block 0.** Entry arrays may in principle
##     spill past block 0; nothing this writer produces does, and growing one
##     is not what an append is for.

from std/posix import fsync
import results
import ./types
import ./base40
import ./container

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
  if blockSize == 0'u32 or blockSize mod 8 != 0 or
     int(blockSize) < HeaderSize + ExtHeaderSize + FileEntrySize:
    return err(appendError(path, "declares an unusable block size of " & $blockSize))
  if data.len mod int(blockSize) != 0:
    return err(appendError(path,
      "is " & $data.len & " bytes, not a whole number of " & $blockSize &
      "-byte blocks; it is truncated or still being written"))

  var maxRootEntries = readU32LE(data, 12)
  if maxRootEntries == 0'u32:
    # A zero means "fill block 0" — `ctfs-container.md` §1's normative reading
    # of `MaxRootEntries`, which yields 170 entries at the default 4096-byte
    # block ((4096 - 16) / 24).
    #
    # **The readers in this tree do not agree on that, so do not read the
    # fallback below as the house convention.** Three readings coexist:
    #
    #   - auto-fill to 170, as here: the wasm recorder's independent Go reader
    #     (`internal/ctfs/container.go`'s `locateEntryArray`) and the
    #     cross-read checker `tests/check_ctfs_container.nim`;
    #   - **no fallback at all** — `container.nim`'s `readInternalFile` and
    #     `hasInternalFile` default `maxEntries` to `DefaultMaxRootEntries`
    #     (31) and scan exactly that many slots, and
    #     `codetracer_trace_reader.nim` passes the header's value straight
    #     through, so a declared 0 makes it scan *nothing* and report every
    #     internal file absent;
    #   - outright refusal: `native_decoder.nim` errors on `maxRoot == 0`.
    #
    # The divergence is harmless *today* only because nothing ever writes a
    # zero: `createCtfs`'s `maxRootEntries` parameter defaults to
    # `DefaultMaxRootEntries` = 31 and every caller takes that default
    # (`multi_stream_writer.nim`, `split_trace.nim`, `createCtfsStreaming`,
    # and the FFI's `ct_container_create`), while the Rust writer's one
    # production call site passes a literal 31. Every `.ct` in the workspace
    # carries 31 in header bytes 12-15, so the branch below is unreachable for
    # any container these writers produce — and 31 entries are read the same
    # way by all three readings.
    #
    # It is reachable from a *foreign* producer, and the spec's own defaults
    # table recommends exactly the 0 that half of this tree cannot read. That
    # makes the split a latent interop hazard rather than a style difference,
    # which is why it is written down here instead of quietly relied upon.
    maxRootEntries = uint32(
      (int(blockSize) - HeaderSize - ExtHeaderSize) div FileEntrySize)
  if HeaderSize + ExtHeaderSize + int(maxRootEntries) * FileEntrySize > int(blockSize):
    return err(appendError(path,
      "declares " & $maxRootEntries & " root entries, which do not fit in its " &
      $blockSize & "-byte block 0; an entry array that spills past block 0 " &
      "cannot be extended by this writer"))

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

proc syncToDisk(f: File) =
  ## Best-effort durability barrier between the two write phases.
  ##
  ## `flushFile` only drains the stdio buffer; the ordering guarantee the
  ## append rests on ("the new blocks are on disk before block 0 points at
  ## them") needs the page cache pushed out too. On platforms without
  ## `fsync` the stdio flush is what there is, and the ordering still holds
  ## against a process crash — which is the failure this protects against in
  ## practice, since the container is written once and closed.
  f.flushFile()
  when defined(posix):
    discard fsync(f.getFileHandle())

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

proc writeAppendedBlocks(c: Ctfs, path: string,
                         firstNewBlock: uint64): Result[void, string] =
  ## Write the appended tail, then block 0. Never touches anything between.
  let bs = int(c.blockSize)
  let tailStart = int(firstNewBlock) * bs
  if tailStart > c.data.len:
    return err(appendError(path, "internal error: the appended image shrank"))

  var f: File
  if not open(f, path, fmReadWriteExisting):
    return err(appendError(path, "cannot reopen the container for append"))
  try:
    if c.data.len > tailStart:
      let n = c.data.len - tailStart
      f.setFilePos(int64(tailStart))
      if f.writeBuffer(unsafeAddr c.data[tailStart], n) != n:
        close(f)
        return err(appendError(path, "short write appending " & $n & " bytes"))
      syncToDisk(f)

    # ---- the midpoint the durability argument turns on ----------------
    # Everything above is on disk; nothing below has run. A crash here is
    # the case `CTFS-Binary-Format.md` §5d describes: unreferenced trailing
    # blocks, and block 0 still the old one. This statement sits *between*
    # the two phases deliberately, so that swapping them moves the two
    # writes around it rather than moving it along with either.
    when defined(ctfsAppendFaultInjection):
      if ctfsAppendStopAtMidpoint:
        close(f)
        return err(appendError(path,
          "fault injection: abandoned between the tail write and block 0"))

    # Only now does anything point at the blocks just written.
    f.setFilePos(0)
    if f.writeBuffer(unsafeAddr c.data[0], bs) != bs:
      close(f)
      return err(appendError(path, "short write rewriting block 0"))
    syncToDisk(f)
  except IOError, OSError:
    close(f)
    return err(appendError(path, "I/O error while appending"))
  close(f)
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

  var c = ?openClosedCtfs(path)
  let firstNewBlock = c.nextFreeBlock

  for i in 0 ..< names.len:
    if c.findEntrySlot(encoded[i]) >= 0:
      return err(appendError(path, "already contains an internal file named " &
        names[i] & "; CTFS is append-only and this writer will not overwrite it"))

  for i in 0 ..< names.len:
    let handle = c.addFile(names[i])
    if handle.isErr:
      return err(appendError(path, "cannot add " & names[i] & ": " & handle.error))
    var f = handle.get()
    let written = c.writeToFile(f, contents[i])
    if written.isErr:
      return err(appendError(path, "cannot write " & names[i] & ": " & written.error))

  writeAppendedBlocks(c, path, firstNewBlock)
