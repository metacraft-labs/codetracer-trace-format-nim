when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## The writer chooses the container profile from a measured RAW-BYTE
## threshold, over the split-stream writer.
##
## Spec: `codetracer-trace-format-spec/ctfs-container.md` §1e (the threshold
## and the converting writer), §1f (how a framed member is stored in a compact
## container), §1d (the compact body) and §1a/§1b/§1c (the version-6 header).
##
## **What this writer is.** A `MultiStreamTraceWriter` writing the FULL
## profile throughout — streamed to `path` as it records, exactly as the
## writer every recorder uses — and, at `close`, a conversion: the full
## container's members, every frame inflated (§1f), are what a compact
## container would carry, and when their total is below the threshold the
## compact container replaces the full one. There is no mid-recording
## switchover, so there is no buffered prefix to lose: until `close` the file
## on disk is the full container, durable from its first seal
## (`ctfs-container.md` §6), and a recording killed before `close` leaves it.
## The cost is writing a small recording twice.
##
## **The unit is RAW member bytes.** §1e: the decision is whether the whole
## trace can be resident at load, a property of logical size. The quantity is
## the sum of the compact members' lengths — `Size - 28 - 24*N` of the compact
## container — measured on the very members the compact container would be
## laid out from, so the number the decision is made on and the bytes that
## are written are the same bytes.
##
## **Byte-identical to the Rust writer.** The compact container is a function
## of the full container (§1f, "Two writers produce the same bytes"), and the
## two writers' full containers are byte-identical; so are their compact ones,
## which `codetracer-trace-format`'s cross-writer test checks.

import std/os
import results
import codetracer_ctfs/types
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/compact_profile

export results, compact_profile

# ---------------------------------------------------------------------------
# The writer
# ---------------------------------------------------------------------------

type
  ProfileWriter* = object
    ## A split-stream trace writer that decides its container profile at
    ## `close`. Record through `writer`, which is the full-profile writer.
    w: MultiStreamTraceWriter
    path: string
    threshold: uint64
    profile: CtfsProfile
    raw: uint64
    image: seq[byte]
    closed: bool

proc initProfileWriter*(path: string, program: string,
    threshold: uint64 = DefaultRawByteThreshold,
    recordingId: string = ""): Result[ProfileWriter, string] =
  ## A writer of `path`, or in memory when `path` is empty (`toBytes` then
  ## holds the container). The full container streams to `path` as it is
  ## recorded; `close` replaces it with the compact container when the raw
  ## member bytes come to less than `threshold`. A `threshold` of 0 writes the
  ## full profile always.
  var w = ? initMultiStreamWriter(path, program, recordingId = recordingId)
  ok(ProfileWriter(w: move w, path: path, threshold: threshold))

proc writer*(pw: var ProfileWriter): var MultiStreamTraceWriter =
  ## The full-profile writer the recording goes through.
  pw.w

proc rawByteThreshold*(pw: ProfileWriter): uint64 = pw.threshold

proc chosenProfile*(pw: ProfileWriter): CtfsProfile =
  ## The profile `close` wrote. `cpFull` before `close`.
  pw.profile

proc rawMemberBytes*(pw: ProfileWriter): uint64 =
  ## The raw member bytes the choice was made on; 0 before `close`.
  pw.raw

proc replaceFile(path: string, image: openArray[byte]): Result[void, string] =
  ## Write `image` to `path` through a sibling temporary, so `path` holds the
  ## full container or the compact one and never a partial write.
  let tmp = path & ".compact.tmp"
  try:
    var f = open(tmp, fmWrite)
    defer: f.close()
    if image.len > 0 and f.writeBytes(image, 0, image.len) != image.len:
      return err("short write to " & tmp)
  except IOError, OSError:
    return err("cannot write " & tmp & ": " & getCurrentExceptionMsg())
  try:
    moveFile(tmp, path)
  except CatchableError:
    try: removeFile(tmp) except OSError: discard
    return err("cannot replace " & path & ": " & getCurrentExceptionMsg())
  except Exception:
    return err("cannot replace " & path)
  ok()

proc close*(pw: var ProfileWriter): Result[void, string] =
  ## Finish the recording, choose the profile on its raw member bytes, and
  ## leave the chosen container at `path` (or in `toBytes`).
  if pw.closed:
    return ok()
  ? pw.w.close()
  var full = pw.w.toBytes()
  ? pw.w.closeCtfs()
  pw.closed = true
  if pw.threshold == 0:
    pw.profile = cpFull
    pw.raw = rawMemberBytes(? compactMembersOf(full))
    pw.image = move full
    return ok()
  let (profile, image, raw) = ? selectProfile(full, pw.threshold)
  pw.profile = profile
  pw.raw = raw
  pw.image = image
  if profile == cpCompact and pw.path.len > 0:
    ? replaceFile(pw.path, pw.image)
  ok()

proc toBytes*(pw: ProfileWriter): seq[byte] =
  ## The container `close` chose, full or compact.
  pw.image
