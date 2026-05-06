import std/[httpclient, json, os, strutils, times, uri]
import ./managed_sender
import ./trace_storage_config

type
  CodetracerCiSenderConfig* = object
    baseUrl*: string
    tenantId*: string
    bearerToken*: string
    platform*: string
    serviceName*: string
    instanceId*: string

  CodetracerCiSenderBackend* = ref object of ManagedSenderBackend
    config*: CodetracerCiSenderConfig
    sessionId: string
    s3KeyPrefix: string
    storagePoolId: string
    storageServerId: string
    storageEndpointUri: string

proc codetracerCiSenderConfigFromEnv*(): CodetracerCiSenderConfig =
  result = CodetracerCiSenderConfig(
    baseUrl: getEnv("CODETRACER_MANAGED_UPLOAD_URL"),
    tenantId: getEnv("CODETRACER_MANAGED_UPLOAD_TENANT"),
    bearerToken: getEnv("CODETRACER_MANAGED_UPLOAD_TOKEN"),
    platform: getEnv("CODETRACER_MANAGED_UPLOAD_PLATFORM", "native"),
    serviceName: getEnv("CODETRACER_MANAGED_UPLOAD_SERVICE", "native-service"),
    instanceId: getEnv("CODETRACER_MANAGED_UPLOAD_INSTANCE", "native-recorder"))

proc newCodetracerCiSenderBackend*(config: CodetracerCiSenderConfig): CodetracerCiSenderBackend =
  CodetracerCiSenderBackend(config: config)

proc baseUrl(config: CodetracerCiSenderConfig): string =
  config.baseUrl.strip(leading = false, trailing = true, chars = {'/'})

proc authedClient(config: CodetracerCiSenderConfig): HttpClient =
  result = newHttpClient(timeout = 10_000)
  result.headers = newHttpHeaders({
    "Authorization": "Bearer " & config.bearerToken,
  })

proc ensureSession(backend: CodetracerCiSenderBackend): bool =
  if backend.sessionId.len > 0:
    return true
  if backend.config.baseUrl.len == 0 or backend.config.tenantId.len == 0 or backend.config.bearerToken.len == 0:
    return false
  let url = baseUrl(backend.config) & "/api/v1/tenants/" & backend.config.tenantId & "/traces/upload-session"
  let body = %*{
    "platform": backend.config.platform,
    "serviceIdentity": {
      "serviceName": backend.config.serviceName,
    },
    "instanceIdentity": {
      "instanceId": backend.config.instanceId,
    }
  }
  let client = authedClient(backend.config)
  try:
    let response = client.request(url, httpMethod = HttpPost, body = $body,
      headers = newHttpHeaders({
        "Authorization": "Bearer " & backend.config.bearerToken,
        "Content-Type": "application/json",
      }))
    if response.code notin {Http200, Http201}:
      return false
    let parsed = parseJson(response.body)
    backend.sessionId = parsed{"sessionId"}.getStr()
    backend.s3KeyPrefix = parsed{"s3KeyPrefix"}.getStr()
    backend.storagePoolId = parsed{"storagePoolId"}.getStr()
    backend.storageServerId = parsed{"storageServerId"}.getStr()
    backend.storageEndpointUri = parsed{"storageEndpointUri"}.getStr()
    backend.sessionId.len > 0 and backend.s3KeyPrefix.len > 0
  except CatchableError:
    false
  finally:
    client.close()

proc sessionObjectKey(backend: CodetracerCiSenderBackend, item: ManagedUploadObject): string =
  if item.objectKey.startsWith(backend.s3KeyPrefix):
    return item.objectKey
  let name =
    if item.objectKey.len > 0: splitFile(item.objectKey).name & splitFile(item.objectKey).ext
    else: splitFile(item.localPath).name & splitFile(item.localPath).ext
  backend.s3KeyPrefix.strip(leading = false, trailing = true, chars = {'/'}) & "/" & name

proc uploadObject(backend: CodetracerCiSenderBackend, item: ManagedUploadObject,
    contentType: string): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] =
  if not backend.ensureSession():
    return (false, ManagedUploadReceipt(), ManagedSenderError(retryable: true, message: "failed to create codetracer-ci upload session"))
  var data: string
  try:
    data = readFile(item.localPath)
  except CatchableError as e:
    return (false, ManagedUploadReceipt(), ManagedSenderError(retryable: true, message: "failed to read upload object: " & e.msg))
  if uint64(data.len) != item.contentLength:
    return (false, ManagedUploadReceipt(), ManagedSenderError(retryable: false, message: "upload object content length changed"))
  let objectKey = backend.sessionObjectKey(item)
  let url = baseUrl(backend.config) & "/api/v1/observability/storage-policy/tenants/" &
    backend.config.tenantId & "/local-storage/objects/" & encodeUrl(objectKey)
  let client = authedClient(backend.config)
  try:
    let response = client.request(url, httpMethod = HttpPut, body = data,
      headers = newHttpHeaders({
        "Authorization": "Bearer " & backend.config.bearerToken,
        "Content-Type": contentType,
        "Content-Length": $data.len,
      }))
    if response.code.int >= 200 and response.code.int < 300:
      return (true, ManagedUploadReceipt(
        objectKey: objectKey,
        storagePoolId: backend.storagePoolId,
        storageServerId: backend.storageServerId,
        storageEndpointUri: backend.storageEndpointUri), ManagedSenderError())
    let retryable = response.code.int >= 500 or response.code in {Http408, Http429}
    (false, ManagedUploadReceipt(), ManagedSenderError(retryable: retryable,
      message: "codetracer-ci upload failed: HTTP " & $response.code.int))
  except CatchableError as e:
    (false, ManagedUploadReceipt(), ManagedSenderError(retryable: true, message: e.msg))
  finally:
    client.close()

method uploadSlice*(backend: CodetracerCiSenderBackend,
    item: ManagedUploadObject): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] =
  backend.uploadObject(item, "application/vnd.codetracer.ctfs")

method uploadMaterializedArtifact*(backend: CodetracerCiSenderBackend,
    item: ManagedUploadObject): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] =
  backend.uploadObject(item, "application/vnd.codetracer.materialized-trace+json")

method uploadManifest*(backend: CodetracerCiSenderBackend,
    item: ManagedUploadObject): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] =
  backend.uploadObject(item, "application/vnd.codetracer.recording-manifest+json")

proc dataStateName(value: DataState): string =
  case value
  of dsRetained: "retained"
  of dsExpired: "expired"
  of dsDeleted: "deleted"

proc traceSourceKindName(value: TraceSourceKind): string =
  case value
  of tskSingleCtfs: "single_ctfs"
  of tskSplitCtfs: "split_ctfs"
  of tskShardedSplitCtfs: "sharded_split_ctfs"
  of tskMaterializedArtifact: "materialized_artifact"

proc segmentSliceJson(segment: CtfsSegment, sourceKind: TraceSourceKind): JsonNode =
  %*{
    "key": segment.file.objectId,
    "objectKey": segment.file.objectId,
    "index": segment.index,
    "order": segment.index,
    "sizeBytes": segment.file.sizeBytes,
    "sha256": segment.file.sha256,
    "retentionStatus": segment.file.dataState.dataStateName,
    "uploadState": segment.file.upload.uploadStateName,
    "sourceKind": sourceKind.traceSourceKindName,
    "timeRange": {
      "geidStart": segment.geidStart,
      "geidEnd": segment.geidEnd,
    },
    "geidStart": segment.geidStart,
    "geidEnd": segment.geidEnd,
    "storagePoolId": segment.file.placement.pool,
    "storageServerId": segment.file.placement.serverId,
    "storageEndpointUri": segment.file.uri,
  }

proc shardedSegmentJson(segment: ShardedCtfsSegment): JsonNode =
  let shards = newJArray()
  for shard in segment.shards:
    let replicas = newJArray()
    for replicaIndex, replica in shard.replicas:
      replicas.add(%*{
        "replicaIndex": replicaIndex,
        "objectKey": replica.uri.replace("local://", ""),
        "storagePoolId": replica.placement.pool,
        "storageServerId": replica.placement.serverId,
        "storageEndpointUri": replica.uri,
        "contentLength": replica.sizeBytes,
        "contentHash": replica.sha256,
        "uploadCompletionState": "complete",
        "retentionStatus": "available",
      })
    shards.add(%*{
      "shardIndex": shard.shardIndex,
      "blockStart": shard.blockStart,
      "blockEnd": shard.blockEnd,
      "replicas": replicas,
    })
  %*{
    "segmentIndex": segment.index,
    "order": segment.index,
    "geidStart": segment.geidStart,
    "geidEnd": segment.geidEnd,
    "shards": shards,
  }

proc mcrSlicesJson(manifest: TraceStorageManifest): JsonNode =
  result = newJArray()
  if manifest.source.kind != tskSplitCtfs:
    return
  for segment in manifest.source.segments:
    result.add(segment.segmentSliceJson(manifest.source.kind))

proc shardedMcrSegmentsJson(manifest: TraceStorageManifest): JsonNode =
  result = newJArray()
  if manifest.source.kind != tskShardedSplitCtfs:
    return
  for segment in manifest.source.shardedSegments:
    result.add(segment.shardedSegmentJson())

proc mcrTimeRangeJson(manifest: TraceStorageManifest): JsonNode =
  result = newJObject()
  if manifest.source.kind == tskSplitCtfs and manifest.source.segments.len > 0:
    var geidStart = manifest.source.segments[0].geidStart
    var geidEnd = manifest.source.segments[0].geidEnd
    for segment in manifest.source.segments:
      if segment.geidStart < geidStart:
        geidStart = segment.geidStart
      if segment.geidEnd > geidEnd:
        geidEnd = segment.geidEnd
    result["geidStart"] = %geidStart
    result["geidEnd"] = %geidEnd
    return
  if manifest.source.kind != tskShardedSplitCtfs or manifest.source.shardedSegments.len == 0:
    return
  var geidStart = manifest.source.shardedSegments[0].geidStart
  var geidEnd = manifest.source.shardedSegments[0].geidEnd
  for segment in manifest.source.shardedSegments:
    if segment.geidStart < geidStart:
      geidStart = segment.geidStart
    if segment.geidEnd > geidEnd:
      geidEnd = segment.geidEnd
  result["geidStart"] = %geidStart
  result["geidEnd"] = %geidEnd

proc mcrManifestJson(backend: CodetracerCiSenderBackend, request: ManagedFinalizeRequest): JsonNode =
  let manifestKey = backend.s3KeyPrefix.strip(leading = false, trailing = true, chars = {'/'}) & "/manifest.json"
  let finalizedAt = now().utc.format("yyyy-MM-dd'T'HH:mm:ss'Z'")
  let mcrSlices = request.manifest.mcrSlicesJson()
  let shardedMcrSegments = request.manifest.shardedMcrSegmentsJson()
  let timeRange = request.manifest.mcrTimeRangeJson()
  %*{
    "kind": "mcr_slices",
    "uploadCompletionState": "complete",
    "serviceIdentity": {"serviceName": backend.config.serviceName},
    "instanceIdentity": {"instanceId": backend.config.instanceId},
    "timeRange": timeRange,
    "retentionStatus": "available",
    "missingSliceKeys": [],
    "mcrSlices": mcrSlices,
    "shardedMcrSegments": shardedMcrSegments,
    "materializedTraceArtifacts": [],
    "totalSlices": request.totalSlices,
    "totalEvents": request.totalEvents,
    "createdAt": finalizedAt,
    "finalizedAt": finalizedAt,
    "manifestS3Key": manifestKey,
  }

proc finalizePayloadJson*(backend: CodetracerCiSenderBackend,
    request: ManagedFinalizeRequest): JsonNode =
  %*{
    "totalSlices": request.totalSlices,
    "totalEvents": request.totalEvents,
    "manifestS3Key": backend.s3KeyPrefix.strip(leading = false, trailing = true, chars = {'/'}) & "/manifest.json",
    "recordingManifest": backend.mcrManifestJson(request),
  }

proc canFinalizeComplete(request: ManagedFinalizeRequest): bool =
  if request.totalSlices <= 0:
    return false
  if request.manifest.source.kind == tskSplitCtfs:
    if request.manifest.source.segments.len != request.totalSlices:
      return false
    for i, segment in request.manifest.source.segments:
      if segment.file.objectId.len == 0 or segment.file.sizeBytes == 0 or
          segment.file.sha256.len == 0 or segment.file.upload != usUploaded:
        return false
      if segment.index != i:
        return false
    return true
  if request.manifest.source.kind != tskShardedSplitCtfs:
    return false
  if request.manifest.source.shardedSegments.len != request.totalSlices:
    return false
  for i, segment in request.manifest.source.shardedSegments:
    if segment.index != i or segment.shards.len == 0:
      return false
    for shard in segment.shards:
      if shard.blockEnd < shard.blockStart or shard.replicas.len == 0:
        return false
      for replica in shard.replicas:
        if replica.objectId.len == 0 or replica.sizeBytes == 0 or
            replica.sha256.len == 0 or replica.upload != usUploaded:
          return false
  true

method finalize*(backend: CodetracerCiSenderBackend,
    request: ManagedFinalizeRequest): tuple[ok: bool, err: ManagedSenderError] =
  if not request.canFinalizeComplete():
    return (false, ManagedSenderError(retryable: false,
      message: "refusing complete finalize without uploaded MCR slice metadata"))
  if not backend.ensureSession():
    return (false, ManagedSenderError(retryable: true, message: "failed to create codetracer-ci upload session"))
  let body = backend.finalizePayloadJson(request)
  let url = baseUrl(backend.config) & "/api/v1/traces/" & backend.sessionId & "/finalize"
  let client = authedClient(backend.config)
  try:
    let response = client.request(url, httpMethod = HttpPost, body = $body,
      headers = newHttpHeaders({
        "Authorization": "Bearer " & backend.config.bearerToken,
        "Content-Type": "application/json",
      }))
    if response.code.int >= 200 and response.code.int < 300:
      return (true, ManagedSenderError())
    let retryable = response.code.int >= 500 or response.code in {Http408, Http429}
    (false, ManagedSenderError(retryable: retryable,
      message: "codetracer-ci finalize failed: HTTP " & $response.code.int))
  except CatchableError as e:
    (false, ManagedSenderError(retryable: true, message: e.msg))
  finally:
    client.close()

method health*(backend: CodetracerCiSenderBackend): ManagedSenderHealth =
  ManagedSenderHealth(healthy: backend.config.baseUrl.len > 0, message: backend.config.baseUrl)
