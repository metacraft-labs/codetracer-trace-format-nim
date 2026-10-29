when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## CTFS streaming mode — creates containers that stream writes to disk
## so concurrent readers can see data as it is written.

import results
import ./types
import ./container
import ./checked_io
import ./live_page
import ./block_mapping

proc createCtfsStreaming*(path: string, blockSize: uint32 = DefaultBlockSize,
                          maxRootEntries: uint32 = DefaultMaxRootEntries,
                          encryption: CtfsEncryptionMethod = emNone,
                          maxShards: uint8 = DefaultMaxShards,
                          pageCapacity: int = DefaultPageCapacity,
                          livePage: LivePageMode = lpmHosted):
                          Result[Ctfs, string] =
  ## Create a new CTFS container (version 5) that streams writes to disk.
  ##
  ## In the order `ctfs-container.md` §6 ("Lifetime", "Creation") gives: the
  ## file is created; the container's live coordination page is created
  ## (exclusively, with the stale-page check) and this process registers in
  ## it as a writer and sets it `live`; only then is the root region (the
  ## header and the empty entry array) written, in one write. That write is
  ## not a seal (§6, "What counts as a seal"): it publishes no member.
  ##
  ## `livePage` says how this process hosts the page (§6, "How a writer's
  ## liveness is judged"). `lpmHosted`, the default: a heartbeat thread keeps
  ## it alive. `lpmInGuest`, for a writer that runs inside a process another
  ## tool records: the page is created here and no thread is started; after
  ## this returns the writer touches the page only with memory stores and
  ## clock reads inside its seals (and at close), and readers judge it alive
  ## by its pid and start time. `lpmFileOnly`: no page; readers follow the
  ## file alone until close, with the guarantees of a reader that cannot
  ## attach.
  let refusal = blockSizeRefusal(blockSize)
  if refusal.len > 0:
    return err(refusal)
  var c = createCtfs(blockSize, maxRootEntries, encryption, maxShards)
  try:
    c.streamFile = open(path, fmReadWrite)
  except IOError, OSError:
    return err("failed to open streaming file: " & path)
  let rootEntries = int(headerRootEntryCount(c.data))
  if livePage != lpmFileOnly:
    let page = createLivePage(c.streamFile, rootEntries, pageCapacity, path,
                              livePage)
    if page.isErr:
      discard c.streamFile.closeChecked()
      return err(page.error)
    c.page = page.get()
  c.streamPath = path
  c.streaming = true
  c.deferWrites = true
  let n = c.rootRegionBytes()
  if not c.handToOs(0, n):
    if c.page != nil: closeLivePage(c.page)
    c.page = nil
    discard c.streamFile.closeChecked()
    return err("failed to write the root region of streaming file " & path &
      ": " & c.ioFailure)
  c.fileRoot = c.data[0 ..< n]
  ok(c)

proc syncEntry*(c: var Ctfs, f: CtfsInternalFile) =
  ## Publish the container (see `publish`) so concurrent readers see every
  ## member's current size, `f`'s included.
  c.publish()

proc syncAllEntries*(c: var Ctfs) =
  ## Publish the container (see `publish`): every member's data, then every
  ## entry that changed since the last publication.
  c.publish()
