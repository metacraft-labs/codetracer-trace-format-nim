import std/[json, options]

const traceStorageSchema* = "codetracer.trace-storage.v1"

type
  StorageModeKind* = enum
    smManagedUpload, smDirectStorage

  StoragePoolPurpose* = enum
    sppCtfs, sppMaterializedArtifact, sppManifest

  ReplicationPlacement* = enum
    rpSamePool, rpDistinctServers

  UploadState* = enum
    usPending, usUploading, usUploaded, usRetryableFailure, usFatalFailure

  LifecycleState* = enum
    lsPending, lsUploading, lsUploaded, lsFinalized, lsRetryableFailure, lsFatalFailure

  DataState* = enum
    dsRetained, dsExpired, dsDeleted

  TraceSourceKind* = enum
    tskSingleCtfs, tskSplitCtfs, tskShardedSplitCtfs, tskMaterializedArtifact

  MaterializedLanguage* = enum
    mlPython, mlRuby, mlJavascript

  ServiceIdentity* = object
    serviceName*: string
    environment*: string
    instanceId*: string
    tenantId*: string

  StorageMode* = object
    kind*: StorageModeKind
    controlPlaneUrl*: string

  StorageEndpoint* = object
    scheme*: string
    baseUrl*: string

  CredentialRef* = object
    provider*: string
    key*: string

  StorageServer* = object
    id*: string
    pool*: string
    endpoint*: StorageEndpoint
    credentialRef*: CredentialRef

  StoragePool* = object
    id*: string
    purpose*: StoragePoolPurpose
    serverIds*: seq[string]

  SplitPolicy* = object
    enabled*: bool
    maxSegmentBytes*: uint64
    checkpointAligned*: bool

  CtfsShardPolicy* = object
    enabled*: bool
    shardCount*: int
    blockRangeBytes*: uint64

  MaterializedArtifactPolicy* = object
    pool*: string
    maxArtifactBytes*: uint64

  ReplicationPolicy* = object
    minReplicas*: int
    targetReplicas*: int
    placement*: ReplicationPlacement

  RetentionPolicy* = object
    retainedForDays*: int
    deleteAfterDays*: int

  TraceStorageConfig* = object
    schema*: string
    service*: ServiceIdentity
    mode*: StorageMode
    storageServers*: seq[StorageServer]
    pools*: seq[StoragePool]
    splitPolicy*: SplitPolicy
    shardPolicy*: CtfsShardPolicy
    materializedArtifactPolicy*: MaterializedArtifactPolicy
    replication*: ReplicationPolicy
    retention*: RetentionPolicy

  Placement* = object
    pool*: string
    serverId*: string

  PlacedObject* = object
    objectId*: string
    uri*: string
    sizeBytes*: uint64
    sha256*: string
    placement*: Placement
    upload*: UploadState
    dataState*: DataState

  CtfsSegment* = object
    index*: int
    geidStart*: uint64
    geidEnd*: uint64
    file*: PlacedObject

  CtfsShard* = object
    shardIndex*: int
    blockStart*: uint64
    blockEnd*: uint64
    replicas*: seq[PlacedObject]

  ShardedCtfsSegment* = object
    index*: int
    geidStart*: uint64
    geidEnd*: uint64
    shards*: seq[CtfsShard]

  ReplayStart* = object
    traceId*: string
    spanId*: string
    geid*: Option[uint64]
    timestampUnixNanos*: Option[uint64]

  TraceSource* = object
    kind*: TraceSourceKind
    file*: PlacedObject
    segments*: seq[CtfsSegment]
    shardedSegments*: seq[ShardedCtfsSegment]
    language*: MaterializedLanguage
    artifact*: PlacedObject
    artifacts*: seq[PlacedObject]
    replayStart*: ReplayStart

  RetryState* = object
    attempt*: int
    nextRetryAt*: Option[string]
    lastError*: Option[string]

  FinalizeState* = object
    finalized*: bool
    finalizedAt*: Option[string]
    idempotencyKey*: string

  ReplicationState* = object
    targetReplicas*: int
    completedReplicas*: int

  TraceStorageManifest* = object
    schema*: string
    recordingId*: string
    service*: ServiceIdentity
    source*: TraceSource
    lifecycle*: LifecycleState
    retry*: RetryState
    finalize*: FinalizeState
    retention*: DataState
    replication*: ReplicationState

proc requiredObject(parent: JsonNode, key: string): JsonNode =
  if parent.kind != JObject or not parent.hasKey(key) or parent[key].kind != JObject:
    raise newException(ValueError, "missing object field: " & key)
  parent[key]

proc requiredArray(parent: JsonNode, key: string): JsonNode =
  if parent.kind != JObject or not parent.hasKey(key) or parent[key].kind != JArray:
    raise newException(ValueError, "missing array field: " & key)
  parent[key]

proc requiredString(parent: JsonNode, key: string): string =
  if parent.kind != JObject or not parent.hasKey(key) or parent[key].kind != JString:
    raise newException(ValueError, "missing string field: " & key)
  parent[key].getStr()

proc requiredInt(parent: JsonNode, key: string): int =
  if parent.kind != JObject or not parent.hasKey(key) or parent[key].kind != JInt:
    raise newException(ValueError, "missing integer field: " & key)
  parent[key].getInt()

proc requiredUint64(parent: JsonNode, key: string): uint64 =
  let value = parent.requiredInt(key)
  if value < 0:
    raise newException(ValueError, "negative integer field: " & key)
  uint64(value)

proc requiredBool(parent: JsonNode, key: string): bool =
  if parent.kind != JObject or not parent.hasKey(key) or parent[key].kind != JBool:
    raise newException(ValueError, "missing boolean field: " & key)
  parent[key].getBool()

proc optionalString(parent: JsonNode, key: string): Option[string] =
  if parent.kind != JObject or not parent.hasKey(key) or parent[key].kind == JNull:
    return none(string)
  if parent[key].kind != JString:
    raise newException(ValueError, "invalid optional string field: " & key)
  some(parent[key].getStr())

proc optionalUint64(parent: JsonNode, key: string): Option[uint64] =
  if parent.kind != JObject or not parent.hasKey(key) or parent[key].kind == JNull:
    return none(uint64)
  if parent[key].kind != JInt:
    raise newException(ValueError, "invalid optional integer field: " & key)
  let value = parent[key].getInt()
  if value < 0:
    raise newException(ValueError, "negative optional integer field: " & key)
  some(uint64(value))

proc parseStorageModeKind(value: string): StorageModeKind =
  case value
  of "managed_upload": smManagedUpload
  of "direct_storage": smDirectStorage
  else: raise newException(ValueError, "unknown storage mode: " & value)

proc storageModeKindName(value: StorageModeKind): string =
  case value
  of smManagedUpload: "managed_upload"
  of smDirectStorage: "direct_storage"

proc parseStoragePoolPurpose(value: string): StoragePoolPurpose =
  case value
  of "ctfs": sppCtfs
  of "materialized_artifact": sppMaterializedArtifact
  of "manifest": sppManifest
  else: raise newException(ValueError, "unknown storage pool purpose: " & value)

proc storagePoolPurposeName(value: StoragePoolPurpose): string =
  case value
  of sppCtfs: "ctfs"
  of sppMaterializedArtifact: "materialized_artifact"
  of sppManifest: "manifest"

proc parseReplicationPlacement(value: string): ReplicationPlacement =
  case value
  of "same_pool": rpSamePool
  of "distinct_servers": rpDistinctServers
  else: raise newException(ValueError, "unknown replication placement: " & value)

proc replicationPlacementName(value: ReplicationPlacement): string =
  case value
  of rpSamePool: "same_pool"
  of rpDistinctServers: "distinct_servers"

proc parseUploadState*(value: string): UploadState =
  case value
  of "pending": usPending
  of "uploading": usUploading
  of "uploaded": usUploaded
  of "retryable_failure": usRetryableFailure
  of "fatal_failure": usFatalFailure
  else: raise newException(ValueError, "unknown upload state: " & value)

proc uploadStateName*(value: UploadState): string =
  case value
  of usPending: "pending"
  of usUploading: "uploading"
  of usUploaded: "uploaded"
  of usRetryableFailure: "retryable_failure"
  of usFatalFailure: "fatal_failure"

proc parseLifecycleState*(value: string): LifecycleState =
  case value
  of "pending": lsPending
  of "uploading": lsUploading
  of "uploaded": lsUploaded
  of "finalized": lsFinalized
  of "retryable_failure": lsRetryableFailure
  of "fatal_failure": lsFatalFailure
  else: raise newException(ValueError, "unknown lifecycle state: " & value)

proc lifecycleStateName*(value: LifecycleState): string =
  case value
  of lsPending: "pending"
  of lsUploading: "uploading"
  of lsUploaded: "uploaded"
  of lsFinalized: "finalized"
  of lsRetryableFailure: "retryable_failure"
  of lsFatalFailure: "fatal_failure"

proc parseDataState*(value: string): DataState =
  case value
  of "retained": dsRetained
  of "expired": dsExpired
  of "deleted": dsDeleted
  else: raise newException(ValueError, "unknown data state: " & value)

proc dataStateName*(value: DataState): string =
  case value
  of dsRetained: "retained"
  of dsExpired: "expired"
  of dsDeleted: "deleted"

proc parseTraceSourceKind(value: string): TraceSourceKind =
  case value
  of "single_ctfs": tskSingleCtfs
  of "split_ctfs": tskSplitCtfs
  of "sharded_split_ctfs": tskShardedSplitCtfs
  of "materialized_artifact": tskMaterializedArtifact
  else: raise newException(ValueError, "unknown trace source kind: " & value)

proc traceSourceKindName(value: TraceSourceKind): string =
  case value
  of tskSingleCtfs: "single_ctfs"
  of tskSplitCtfs: "split_ctfs"
  of tskShardedSplitCtfs: "sharded_split_ctfs"
  of tskMaterializedArtifact: "materialized_artifact"

proc parseMaterializedLanguage(value: string): MaterializedLanguage =
  case value
  of "python": mlPython
  of "ruby": mlRuby
  of "javascript": mlJavascript
  else: raise newException(ValueError, "unknown materialized language: " & value)

proc materializedLanguageName(value: MaterializedLanguage): string =
  case value
  of mlPython: "python"
  of mlRuby: "ruby"
  of mlJavascript: "javascript"

proc parseServiceIdentity(node: JsonNode): ServiceIdentity =
  ServiceIdentity(
    serviceName: node.requiredString("service_name"),
    environment: node.requiredString("environment"),
    instanceId: node.requiredString("instance_id"),
    tenantId: node.requiredString("tenant_id"))

proc parseStorageMode(node: JsonNode): StorageMode =
  if node.kind != JObject:
    raise newException(ValueError, "storage mode must be an object")
  var modeName = ""
  var modeValue: JsonNode
  for key, value in node:
    if modeName.len != 0:
      raise newException(ValueError, "storage mode must contain one variant")
    modeName = key
    modeValue = value
  if modeName.len == 0 or modeValue.kind != JObject:
    raise newException(ValueError, "storage mode must contain one object variant")
  StorageMode(
    kind: parseStorageModeKind(modeName),
    controlPlaneUrl: modeValue.requiredString("control_plane_url"))

proc parseStorageEndpoint(node: JsonNode): StorageEndpoint =
  StorageEndpoint(
    scheme: node.requiredString("scheme"),
    baseUrl: node.requiredString("base_url"))

proc parseCredentialRef(node: JsonNode): CredentialRef =
  CredentialRef(
    provider: node.requiredString("provider"),
    key: node.requiredString("key"))

proc parseStorageServer(node: JsonNode): StorageServer =
  StorageServer(
    id: node.requiredString("id"),
    pool: node.requiredString("pool"),
    endpoint: parseStorageEndpoint(node.requiredObject("endpoint")),
    credentialRef: parseCredentialRef(node.requiredObject("credential_ref")))

proc parseStoragePool(node: JsonNode): StoragePool =
  result = StoragePool(
    id: node.requiredString("id"),
    purpose: parseStoragePoolPurpose(node.requiredString("purpose")))
  for server in node.requiredArray("server_ids"):
    if server.kind != JString:
      raise newException(ValueError, "server_ids must contain strings")
    result.serverIds.add(server.getStr())

proc parsePlacedObject(node: JsonNode): PlacedObject
proc parseTraceSource(node: JsonNode): TraceSource

proc parseSplitPolicy(node: JsonNode): SplitPolicy =
  SplitPolicy(
    enabled: node.requiredBool("enabled"),
    maxSegmentBytes: node.requiredUint64("max_segment_bytes"),
    checkpointAligned: node.requiredBool("checkpoint_aligned"))

proc parseCtfsShardPolicy(node: JsonNode): CtfsShardPolicy =
  CtfsShardPolicy(
    enabled: node.requiredBool("enabled"),
    shardCount: node.requiredInt("shard_count"),
    blockRangeBytes: node.requiredUint64("block_range_bytes"))

proc parseMaterializedArtifactPolicy(node: JsonNode): MaterializedArtifactPolicy =
  MaterializedArtifactPolicy(
    pool: node.requiredString("pool"),
    maxArtifactBytes: node.requiredUint64("max_artifact_bytes"))

proc parseReplicationPolicy(node: JsonNode): ReplicationPolicy =
  ReplicationPolicy(
    minReplicas: node.requiredInt("min_replicas"),
    targetReplicas: node.requiredInt("target_replicas"),
    placement: parseReplicationPlacement(node.requiredString("placement")))

proc parseRetentionPolicy(node: JsonNode): RetentionPolicy =
  RetentionPolicy(
    retainedForDays: node.requiredInt("retained_for_days"),
    deleteAfterDays: node.requiredInt("delete_after_days"))

proc parsePlacement(node: JsonNode): Placement =
  Placement(
    pool: node.requiredString("pool"),
    serverId: node.requiredString("server_id"))

proc parsePlacedObject(node: JsonNode): PlacedObject =
  PlacedObject(
    objectId: node.requiredString("object_id"),
    uri: node.requiredString("uri"),
    sizeBytes: node.requiredUint64("size_bytes"),
    sha256: node.requiredString("sha256"),
    placement: parsePlacement(node.requiredObject("placement")),
    upload: parseUploadState(node.requiredString("upload")),
    dataState: parseDataState(node.requiredString("data_state")))

proc parseCtfsSegment(node: JsonNode): CtfsSegment =
  CtfsSegment(
    index: node.requiredInt("index"),
    geidStart: node.requiredUint64("geid_start"),
    geidEnd: node.requiredUint64("geid_end"),
    file: parsePlacedObject(node.requiredObject("file")))

proc parseCtfsShard(node: JsonNode): CtfsShard =
  result = CtfsShard(
    shardIndex: node.requiredInt("shard_index"),
    blockStart: node.requiredUint64("block_start"),
    blockEnd: node.requiredUint64("block_end"))
  for replica in node.requiredArray("replicas"):
    result.replicas.add(parsePlacedObject(replica))

proc parseShardedCtfsSegment(node: JsonNode): ShardedCtfsSegment =
  result = ShardedCtfsSegment(
    index: node.requiredInt("index"),
    geidStart: node.requiredUint64("geid_start"),
    geidEnd: node.requiredUint64("geid_end"))
  for shard in node.requiredArray("shards"):
    result.shards.add(parseCtfsShard(shard))

proc parseReplayStart(node: JsonNode): ReplayStart =
  ReplayStart(
    traceId: node.requiredString("trace_id"),
    spanId: node.requiredString("span_id"),
    geid: node.optionalUint64("geid"),
    timestampUnixNanos: node.optionalUint64("timestamp_unix_nanos"))

proc parseTraceSource(node: JsonNode): TraceSource =
  result.kind = parseTraceSourceKind(node.requiredString("kind"))
  case result.kind
  of tskSingleCtfs:
    result.file = parsePlacedObject(node.requiredObject("file"))
  of tskSplitCtfs:
    for segment in node.requiredArray("segments"):
      result.segments.add(parseCtfsSegment(segment))
  of tskShardedSplitCtfs:
    for segment in node.requiredArray("segments"):
      result.shardedSegments.add(parseShardedCtfsSegment(segment))
  of tskMaterializedArtifact:
    result.language = parseMaterializedLanguage(node.requiredString("language"))
    result.artifact = parsePlacedObject(node.requiredObject("artifact"))
    if node.kind == JObject and node.hasKey("artifacts"):
      if node["artifacts"].kind != JArray:
        raise newException(ValueError, "artifacts must be an array")
      for artifact in node["artifacts"]:
        result.artifacts.add(parsePlacedObject(artifact))
    result.replayStart = parseReplayStart(node.requiredObject("replay_start"))

proc parseRetryState(node: JsonNode): RetryState =
  RetryState(
    attempt: node.requiredInt("attempt"),
    nextRetryAt: node.optionalString("next_retry_at"),
    lastError: node.optionalString("last_error"))

proc parseFinalizeState(node: JsonNode): FinalizeState =
  FinalizeState(
    finalized: node.requiredBool("finalized"),
    finalizedAt: node.optionalString("finalized_at"),
    idempotencyKey: node.requiredString("idempotency_key"))

proc parseReplicationState(node: JsonNode): ReplicationState =
  ReplicationState(
    targetReplicas: node.requiredInt("target_replicas"),
    completedReplicas: node.requiredInt("completed_replicas"))

proc parseTraceStorageConfig*(input: string): TraceStorageConfig =
  let root = parseJson(input)
  result = TraceStorageConfig(
    schema: root.requiredString("schema"),
    service: parseServiceIdentity(root.requiredObject("service")),
    mode: parseStorageMode(root.requiredObject("mode")),
    splitPolicy: parseSplitPolicy(root.requiredObject("split_policy")),
    shardPolicy: parseCtfsShardPolicy(root.requiredObject("shard_policy")),
    materializedArtifactPolicy: parseMaterializedArtifactPolicy(root.requiredObject("materialized_artifact_policy")),
    replication: parseReplicationPolicy(root.requiredObject("replication")),
    retention: parseRetentionPolicy(root.requiredObject("retention")))
  for server in root.requiredArray("storage_servers"):
    result.storageServers.add(parseStorageServer(server))
  for pool in root.requiredArray("pools"):
    result.pools.add(parseStoragePool(pool))

proc parseTraceStorageManifest*(input: string): TraceStorageManifest =
  let root = parseJson(input)
  TraceStorageManifest(
    schema: root.requiredString("schema"),
    recordingId: root.requiredString("recording_id"),
    service: parseServiceIdentity(root.requiredObject("service")),
    source: parseTraceSource(root.requiredObject("source")),
    lifecycle: parseLifecycleState(root.requiredString("lifecycle")),
    retry: parseRetryState(root.requiredObject("retry")),
    finalize: parseFinalizeState(root.requiredObject("finalize")),
    retention: parseDataState(root.requiredString("retention")),
    replication: parseReplicationState(root.requiredObject("replication")))

proc jsonUint(value: uint64): JsonNode =
  %BiggestInt(value)

proc jsonOptString(value: Option[string]): JsonNode =
  if value.isSome: %value.get() else: newJNull()

proc jsonOptUint(value: Option[uint64]): JsonNode =
  if value.isSome: jsonUint(value.get()) else: newJNull()

proc toJson*(service: ServiceIdentity): JsonNode =
  %*{
    "service_name": service.serviceName,
    "environment": service.environment,
    "instance_id": service.instanceId,
    "tenant_id": service.tenantId
  }

proc toJson*(mode: StorageMode): JsonNode =
  %*{mode.kind.storageModeKindName: {"control_plane_url": mode.controlPlaneUrl}}

proc toJson*(endpoint: StorageEndpoint): JsonNode =
  %*{"scheme": endpoint.scheme, "base_url": endpoint.baseUrl}

proc toJson*(credentialRef: CredentialRef): JsonNode =
  %*{"provider": credentialRef.provider, "key": credentialRef.key}

proc toJson*(server: StorageServer): JsonNode =
  %*{
    "id": server.id,
    "pool": server.pool,
    "endpoint": server.endpoint.toJson(),
    "credential_ref": server.credentialRef.toJson()
  }

proc toJson*(pool: StoragePool): JsonNode =
  %*{
    "id": pool.id,
    "purpose": pool.purpose.storagePoolPurposeName,
    "server_ids": pool.serverIds
  }

proc toJson*(policy: SplitPolicy): JsonNode =
  %*{
    "enabled": policy.enabled,
    "max_segment_bytes": policy.maxSegmentBytes.jsonUint(),
    "checkpoint_aligned": policy.checkpointAligned
  }

proc toJson*(policy: CtfsShardPolicy): JsonNode =
  %*{
    "enabled": policy.enabled,
    "shard_count": policy.shardCount,
    "block_range_bytes": policy.blockRangeBytes.jsonUint()
  }

proc toJson*(policy: MaterializedArtifactPolicy): JsonNode =
  %*{
    "pool": policy.pool,
    "max_artifact_bytes": policy.maxArtifactBytes.jsonUint()
  }

proc toJson*(policy: ReplicationPolicy): JsonNode =
  %*{
    "min_replicas": policy.minReplicas,
    "target_replicas": policy.targetReplicas,
    "placement": policy.placement.replicationPlacementName
  }

proc toJson*(policy: RetentionPolicy): JsonNode =
  %*{
    "retained_for_days": policy.retainedForDays,
    "delete_after_days": policy.deleteAfterDays
  }

proc toJson*(placement: Placement): JsonNode =
  %*{"pool": placement.pool, "server_id": placement.serverId}

proc toJson*(file: PlacedObject): JsonNode =
  %*{
    "object_id": file.objectId,
    "uri": file.uri,
    "size_bytes": file.sizeBytes.jsonUint(),
    "sha256": file.sha256,
    "placement": file.placement.toJson(),
    "upload": file.upload.uploadStateName,
    "data_state": file.dataState.dataStateName
  }

proc toJson*(segment: CtfsSegment): JsonNode =
  %*{
    "index": segment.index,
    "geid_start": segment.geidStart.jsonUint(),
    "geid_end": segment.geidEnd.jsonUint(),
    "file": segment.file.toJson()
  }

proc toJson*(shard: CtfsShard): JsonNode =
  let replicas = newJArray()
  for replica in shard.replicas:
    replicas.add(replica.toJson())
  %*{
    "shard_index": shard.shardIndex,
    "block_start": shard.blockStart.jsonUint(),
    "block_end": shard.blockEnd.jsonUint(),
    "replicas": replicas
  }

proc toJson*(segment: ShardedCtfsSegment): JsonNode =
  let shards = newJArray()
  for shard in segment.shards:
    shards.add(shard.toJson())
  %*{
    "index": segment.index,
    "geid_start": segment.geidStart.jsonUint(),
    "geid_end": segment.geidEnd.jsonUint(),
    "shards": shards
  }

proc toJson*(replayStart: ReplayStart): JsonNode =
  %*{
    "trace_id": replayStart.traceId,
    "span_id": replayStart.spanId,
    "geid": replayStart.geid.jsonOptUint(),
    "timestamp_unix_nanos": replayStart.timestampUnixNanos.jsonOptUint()
  }

proc toJson*(source: TraceSource): JsonNode =
  result = %*{"kind": source.kind.traceSourceKindName}
  case source.kind
  of tskSingleCtfs:
    result["file"] = source.file.toJson()
  of tskSplitCtfs:
    let segments = newJArray()
    for segment in source.segments:
      segments.add(segment.toJson())
    result["segments"] = segments
  of tskShardedSplitCtfs:
    let segments = newJArray()
    for segment in source.shardedSegments:
      segments.add(segment.toJson())
    result["segments"] = segments
  of tskMaterializedArtifact:
    result["language"] = %source.language.materializedLanguageName
    result["artifact"] = source.artifact.toJson()
    if source.artifacts.len > 0:
      let artifacts = newJArray()
      for artifact in source.artifacts:
        artifacts.add(artifact.toJson())
      result["artifacts"] = artifacts
    result["replay_start"] = source.replayStart.toJson()

proc toJson*(retry: RetryState): JsonNode =
  %*{
    "attempt": retry.attempt,
    "next_retry_at": retry.nextRetryAt.jsonOptString(),
    "last_error": retry.lastError.jsonOptString()
  }

proc toJson*(finalize: FinalizeState): JsonNode =
  %*{
    "finalized": finalize.finalized,
    "finalized_at": finalize.finalizedAt.jsonOptString(),
    "idempotency_key": finalize.idempotencyKey
  }

proc toJson*(replication: ReplicationState): JsonNode =
  %*{
    "target_replicas": replication.targetReplicas,
    "completed_replicas": replication.completedReplicas
  }

proc toJson*(config: TraceStorageConfig): JsonNode =
  let servers = newJArray()
  for server in config.storageServers:
    servers.add(server.toJson())
  let pools = newJArray()
  for pool in config.pools:
    pools.add(pool.toJson())
  %*{
    "schema": config.schema,
    "service": config.service.toJson(),
    "mode": config.mode.toJson(),
    "storage_servers": servers,
    "pools": pools,
    "split_policy": config.splitPolicy.toJson(),
    "shard_policy": config.shardPolicy.toJson(),
    "materialized_artifact_policy": config.materializedArtifactPolicy.toJson(),
    "replication": config.replication.toJson(),
    "retention": config.retention.toJson()
  }

proc toJson*(manifest: TraceStorageManifest): JsonNode =
  %*{
    "schema": manifest.schema,
    "recording_id": manifest.recordingId,
    "service": manifest.service.toJson(),
    "source": manifest.source.toJson(),
    "lifecycle": manifest.lifecycle.lifecycleStateName,
    "retry": manifest.retry.toJson(),
    "finalize": manifest.finalize.toJson(),
    "retention": manifest.retention.dataStateName,
    "replication": manifest.replication.toJson()
  }

proc toJsonString*(config: TraceStorageConfig): string =
  $config.toJson()

proc toJsonString*(manifest: TraceStorageManifest): string =
  $manifest.toJson()
