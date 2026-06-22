{.push raises: [].}

## Step-map namespace builder (M26b).
##
## Accumulates the `(path_id, line) -> [step_id]` BREAKPOINT index during
## recording and serialises it, at finalize time, into the spec's flat `STMP`
## namespace (`step-map.ns`).  This is the on-disk, computed-at-recording-time
## equivalent of the db-backend's in-memory `path -> line -> [step]` map: when a
## `.ct` carries `step-map.ns`, BREAKPOINT line->step resolution is an
## O(unique-lines) index lookup that never materialises the whole step table.
##
## ## Why this keys off `global_line_index`, not the raw `(pathId, line)` args
##
## The db-backend's seekable reader recovers a step's `(path_id, line)` by
## decoding the exec stream's `global_line_index` through
## `unpack_global_line_index` — `(gli >> 32, gli & 0xFFFF_FFFF)` — and the M26
## whole-table breakpoint build keys on exactly those decoded coordinates (see
## `codetracer/src/db-backend/src/ctfs_trace_reader/step_value_stream_source.rs`
## `reconstruct_db_step` / `step_line`).  To make a Nim-written `step-map.ns`
## BYTE-FOR-BYTE agree with that whole-table derivation, this builder keys off
## the SAME `global_line_index` the writer encoded into the exec stream and
## unpacks it identically.  Keying off the raw `registerStep(pathId, line)`
## arguments would silently disagree whenever the writer's
## `toGlobalLineIndex` packing differs from the reader's `unpack` (e.g. the
## column-aware byte-offset path), so we deliberately mirror the reader's
## decode here.
##
## ## On-disk format — spec §4.1 `STMP` (matches `serialize_step_map`)
##
## ```text
## Header (18 bytes):
##   [magic:u32 = 0x53544D50 "STMP"][version:u16 = 1]
##   [path_count:u32][path_table_offset:u64]
## Path table (path_table_offset), sorted by path_id, 20 bytes each:
##   [path_id:u64][line_count:u32][lines_offset:u64]
## Line entries (lines_offset, per path), sorted by line, 32 bytes each:
##   [line:u32][step_count:u32][first_step_id:i64][last_step_id:i64][steps_offset:u64]
## Step-id lists (steps_offset), step_count x i64, ascending.
## ```
##
## All integers little-endian.  This is the inverse of the db-backend's
## `StepMapNamespace::parse` and must remain byte-identical to the canonical
## Rust `serialize_step_map`; the round-trip (Nim write -> Rust read) is the
## load-bearing test.

import std/[tables, algorithm]
import results

export results

const
  StepMapMagic*: uint32 = 0x5354_4D50'u32
    ## ASCII "STMP" read as a little-endian u32 — the namespace magic.
  StepMapVersion*: uint16 = 1
    ## The only format version the M26 reader understands.
  StepMapFileName*: string = "step-map.ns"
    ## The CTFS container-internal file name for the prepopulated index.
  GliLineBits: uint32 = 32
    ## Bits reserved for the `line` component of a packed `global_line_index`;
    ## `path_id` occupies the bits above it.  Mirrors the db-backend's
    ## `GLI_LINE_BITS` so the unpack here matches the reader exactly.
  GliLineMask: uint64 = (1'u64 shl GliLineBits) - 1

type
  StepMapBuilder* = object
    ## Accumulates `(path_id, line) -> [step_id]` during recording.  Step ids
    ## arrive in ascending order (the writer's monotonic `stepCount`), so each
    ## per-line list is already sorted; the serializer re-sorts defensively to
    ## stay byte-identical to the canonical Rust path regardless of insertion
    ## order.
    byPath: Table[uint64, Table[uint32, seq[int64]]]

proc initStepMapBuilder*(): StepMapBuilder =
  ## A fresh, empty step-map builder.
  StepMapBuilder(byPath: initTable[uint64, Table[uint32, seq[int64]]]())

proc unpackGlobalLineIndex*(gli: uint64): (uint64, uint32) =
  ## Recover `(path_id, line)` from a packed `global_line_index`, identically to
  ## the db-backend's `unpack_global_line_index`.  Returned `line` is a u32
  ## (the wire width of the step-map line field).
  let pathId = gli shr GliLineBits
  let line = uint32(gli and GliLineMask)
  (pathId, line)

proc recordStep*(b: var StepMapBuilder, globalLineIndex: uint64,
    stepId: uint64) =
  ## Record that step `stepId` executed at `globalLineIndex`.  The
  ## `(path_id, line)` key is derived by unpacking the gli exactly as the
  ## reader does (see module docs), so the resulting table mirrors the
  ## db-backend whole-table build byte-for-byte.
  let (pathId, line) = unpackGlobalLineIndex(globalLineIndex)
  var byLine = addr b.byPath.mgetOrPut(pathId, initTable[uint32, seq[int64]]())
  var ids = addr byLine[].mgetOrPut(line, newSeq[int64]())
  ids[].add(int64(stepId))

proc entryCount*(b: StepMapBuilder): int =
  ## Total number of distinct `(path_id, line)` keys recorded.
  for byLine in b.byPath.values:
    result += byLine.len

proc serialize*(b: StepMapBuilder): seq[byte] =
  ## Serialise the accumulated map into the spec §4.1 `STMP` wire format.
  ##
  ## Paths are emitted sorted by `path_id` and lines sorted ascending within a
  ## path, with each per-line step-id list sorted ascending — the binary-
  ## searchable ordering the spec (and `serialize_step_map`) require.  The
  ## layout is planned in full before any byte is written so every internal
  ## offset is exact.
  const
    HeaderSize = 18
    PathEntrySize = 20
    LineEntrySize = 32

  # Stable, sorted view of the data captured ONCE so the layout-planning and
  # byte-writing passes iterate in identical order.  `paths` is ascending by
  # path_id; `linesPerPath[p]` is the ascending `(line, sorted_step_ids)` list
  # for path `paths[p]`.  Re-indexing the source `Table` is avoided entirely
  # (it would raise `KeyError`, which this module forbids).
  var paths: seq[uint64]
  for pathId in b.byPath.keys:
    paths.add(pathId)
  paths.sort()

  let pathCount = paths.len
  let pathTableOffset = HeaderSize

  var linesPerPath: seq[seq[(uint32, seq[int64])]]
  for pathId in paths:
    var lineKeys: seq[uint32]
    let byLine = b.byPath.getOrDefault(pathId)
    for line in byLine.keys:
      lineKeys.add(line)
    lineKeys.sort()
    var entries: seq[(uint32, seq[int64])]
    for line in lineKeys:
      var ids = byLine.getOrDefault(line)
      ids.sort()
      entries.add((line, ids))
    linesPerPath.add(entries)

  # Plan offsets: header | path table | per-path line blocks | step-id lists.
  var cursor = pathTableOffset + pathCount * PathEntrySize

  var lineBlockOffsets: seq[int]
  for p in 0 ..< pathCount:
    lineBlockOffsets.add(cursor)
    cursor += linesPerPath[p].len * LineEntrySize

  # Step-id list offsets, flattened in (path, line) iteration order.
  var stepListOffsets: seq[int]
  for p in 0 ..< pathCount:
    for entry in linesPerPath[p]:
      stepListOffsets.add(cursor)
      cursor += entry[1].len * 8

  let total = cursor
  var buf = newSeq[byte](total)

  proc putU16(off: int, v: uint16) =
    buf[off] = byte(v and 0xFF)
    buf[off + 1] = byte((v shr 8) and 0xFF)

  proc putU32(off: int, v: uint32) =
    buf[off] = byte(v and 0xFF)
    buf[off + 1] = byte((v shr 8) and 0xFF)
    buf[off + 2] = byte((v shr 16) and 0xFF)
    buf[off + 3] = byte((v shr 24) and 0xFF)

  proc putU64(off: int, v: uint64) =
    var x = v
    for i in 0 ..< 8:
      buf[off + i] = byte(x and 0xFF)
      x = x shr 8

  proc putI64(off: int, v: int64) =
    putU64(off, cast[uint64](v))

  # Header.
  putU32(0, StepMapMagic)
  putU16(4, StepMapVersion)
  putU32(6, uint32(pathCount))
  putU64(10, uint64(pathTableOffset))

  # Path table + line entries + step lists.
  var stepListIdx = 0
  for p in 0 ..< pathCount:
    let pathId = paths[p]
    let entries = linesPerPath[p]
    let linesOffset = lineBlockOffsets[p]

    let pbase = pathTableOffset + p * PathEntrySize
    putU64(pbase, pathId)
    putU32(pbase + 8, uint32(entries.len))
    putU64(pbase + 12, uint64(linesOffset))

    for l in 0 ..< entries.len:
      let line = entries[l][0]
      let ids = entries[l][1]
      let lbase = linesOffset + l * LineEntrySize
      let stepsOffset = stepListOffsets[stepListIdx]
      inc stepListIdx
      let first = if ids.len > 0: ids[0] else: 0'i64
      let last = if ids.len > 0: ids[^1] else: 0'i64
      putU32(lbase, line)
      putU32(lbase + 4, uint32(ids.len))
      putI64(lbase + 8, first)
      putI64(lbase + 16, last)
      putU64(lbase + 24, uint64(stepsOffset))

      for s in 0 ..< ids.len:
        putI64(stepsOffset + s * 8, ids[s])

  buf
