{.push raises: [].}

## Binary meta.dat writer for CTFS trace metadata.
##
## Layout:
##   [4] magic "CTMD"
##   [2] version u16 LE
##   [2] flags u16 LE (bit 0: has_mcr_fields)
##   varint-prefixed program string
##   varint args_count, then varint-prefixed arg strings
##   varint-prefixed workdir string
##   varint-prefixed recorder_id string
##   varint paths_count, then varint-prefixed path strings
##   if has_mcr_fields:
##     varint tick_source
##     varint total_threads
##     varint atomic_mode

import std/options
import results
import ../codetracer_trace_types
import ../codetracer_ctfs/types
import ../codetracer_ctfs/container
import ./varint

const
  MetaDatMagic*: array[4, byte] = [0x43'u8, 0x54, 0x4D, 0x44]  # "CTMD"
  MetaDatVersion*: uint16 = 1
  FlagHasMcrFields*: uint16 = 1  # bit 0

proc writeRawBytes(
    c: var Ctfs, f: var CtfsInternalFile,
    data: openArray[byte]): Result[void, string] =
  c.writeToFile(f, data)

proc writeU16LE(
    c: var Ctfs, f: var CtfsInternalFile,
    val: uint16): Result[void, string] =
  let bytes = [byte(val and 0xFF), byte((val shr 8) and 0xFF)]
  c.writeToFile(f, bytes)

proc writeVarint(
    c: var Ctfs, f: var CtfsInternalFile,
    val: uint64): Result[void, string] =
  var buf: seq[byte]
  encodeVarint(val, buf)
  c.writeToFile(f, buf)

proc writeVarintString(
    c: var Ctfs, f: var CtfsInternalFile,
    s: string): Result[void, string] =
  ? c.writeVarint(f, uint64(s.len))
  if s.len > 0:
    let bytes = cast[seq[byte]](s)
    ? c.writeToFile(f, bytes)
  ok()

proc writeMetaDat*(
    c: var Ctfs, f: var CtfsInternalFile,
    meta: TraceMetadata,
    paths: openArray[string],
    recorderId: string = "",
    mcrFields: Option[McrMetaFields] = none(McrMetaFields)
): Result[void, string] =
  ## Write binary meta.dat to a CTFS internal file.

  # Magic
  ? c.writeRawBytes(f, MetaDatMagic)

  # Version
  ? c.writeU16LE(f, MetaDatVersion)

  # Flags
  var flags: uint16 = 0
  if mcrFields.isSome:
    flags = flags or FlagHasMcrFields
  ? c.writeU16LE(f, flags)

  # Program
  ? c.writeVarintString(f, meta.program)

  # Args
  ? c.writeVarint(f, uint64(meta.args.len))
  for arg in meta.args:
    ? c.writeVarintString(f, arg)

  # Workdir
  ? c.writeVarintString(f, meta.workdir)

  # Recorder ID
  ? c.writeVarintString(f, recorderId)

  # Paths
  ? c.writeVarint(f, uint64(paths.len))
  for p in paths:
    ? c.writeVarintString(f, p)

  # MCR fields
  if mcrFields.isSome:
    let mcr = mcrFields.get()
    ? c.writeVarint(f, uint64(ord(mcr.tickSource)))
    ? c.writeVarint(f, uint64(mcr.totalThreads))
    ? c.writeVarint(f, uint64(ord(mcr.atomicMode)))

  ok()
