{.push raises: [].}

## Replay-facing CTFS block resolver over shared trace-storage manifests.
##
## Split files and CTFS block shards are modeled as independent axes. Replay
## code asks for a logical `(segment, block)` and this module resolves it to
## the concrete placed object replicas described by the manifest.

import results
import ./trace_storage_config

type
  LogicalCtfsBlockLocation* = object
    segmentIndex*: int
    blockId*: uint64
    replicas*: seq[PlacedObject]

  ObjectBlockReader* = proc(obj: PlacedObject, blockId: uint64): Result[seq[byte], string] {.closure, raises: [].}

proc segmentCount*(source: TraceSource): int =
  case source.kind
  of tskSingleCtfs:
    1
  of tskSplitCtfs:
    source.segments.len
  of tskShardedSplitCtfs:
    source.shardedSegments.len
  of tskMaterializedArtifact:
    0

proc segmentCount*(manifest: TraceStorageManifest): int =
  manifest.source.segmentCount()

proc resolveLogicalCtfsBlock*(source: TraceSource, segmentIndex: int,
    blockId: uint64): Result[LogicalCtfsBlockLocation, string] =
  case source.kind
  of tskSingleCtfs:
    if segmentIndex != 0:
      return err("single CTFS trace has no segment " & $segmentIndex)
    ok(LogicalCtfsBlockLocation(
      segmentIndex: segmentIndex,
      blockId: blockId,
      replicas: @[source.file]))
  of tskSplitCtfs:
    for segment in source.segments:
      if segment.index == segmentIndex:
        return ok(LogicalCtfsBlockLocation(
          segmentIndex: segmentIndex,
          blockId: blockId,
          replicas: @[segment.file]))
    err("split CTFS trace has no segment " & $segmentIndex)
  of tskShardedSplitCtfs:
    for segment in source.shardedSegments:
      if segment.index != segmentIndex:
        continue
      for shard in segment.shards:
        if blockId >= shard.blockStart and blockId <= shard.blockEnd:
          if shard.replicas.len == 0:
            return err("sharded CTFS segment " & $segmentIndex & " block " & $blockId & " has no replicas")
          return ok(LogicalCtfsBlockLocation(
            segmentIndex: segmentIndex,
            blockId: blockId,
            replicas: shard.replicas))
      return err("sharded CTFS segment " & $segmentIndex & " has no shard for block " & $blockId)
    err("sharded CTFS trace has no segment " & $segmentIndex)
  of tskMaterializedArtifact:
    err("materialized trace artifacts do not expose CTFS block locations")

proc resolveLogicalCtfsBlock*(manifest: TraceStorageManifest,
    segmentIndex: int, blockId: uint64): Result[LogicalCtfsBlockLocation, string] =
  manifest.source.resolveLogicalCtfsBlock(segmentIndex, blockId)

proc readLogicalCtfsBlock*(manifest: TraceStorageManifest, segmentIndex: int,
    blockId: uint64, reader: ObjectBlockReader): Result[seq[byte], string] =
  let location = ?manifest.resolveLogicalCtfsBlock(segmentIndex, blockId)
  var lastError = "no CTFS block replicas were readable"
  for replica in location.replicas:
    let blockResult = reader(replica, blockId)
    if blockResult.isOk:
      return blockResult
    lastError = blockResult.error
  err(lastError)

proc readLogicalCtfsRange*(manifest: TraceStorageManifest, segmentIndex: int,
    firstBlock: uint64, count: uint64, reader: ObjectBlockReader): Result[seq[seq[byte]], string] =
  var blocks: seq[seq[byte]]
  for offset in 0'u64 ..< count:
    blocks.add(?manifest.readLogicalCtfsBlock(segmentIndex, firstBlock + offset, reader))
  ok(blocks)
