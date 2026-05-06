import std/[options, os, strformat, strutils, unittest]

import codetracer_ctfs/trace_storage_config
import codetracer_ctfs/trace_storage_reader
import results

proc fixturePath(name: string): string =
  var dir = getCurrentDir()
  while dir.len > 1:
    let candidate = dir / "codetracer-trace-format" / "codetracer_ctfs" /
      "tests" / "fixtures" / "trace_storage" / name
    if fileExists(candidate):
      return candidate
    dir = dir.parentDir()
  raise newException(IOError, "cannot find shared trace-storage fixture: " & name)

proc manifestJson(
    lifecycle: string,
    upload: string,
    dataState: string,
    retryAttempt: int,
    nextRetryAt: string,
    lastError: string,
    finalized: bool,
    finalizedAt: string,
    idempotencyKey: string
): string =
  let nextRetryJson = if nextRetryAt.len == 0: "null" else: "\"" & nextRetryAt & "\""
  let lastErrorJson = if lastError.len == 0: "null" else: "\"" & lastError & "\""
  let finalizedAtJson = if finalizedAt.len == 0: "null" else: "\"" & finalizedAt & "\""
  fmt"""
{{
  "schema": "{traceStorageSchema}",
  "recording_id": "rec-{lifecycle}",
  "service": {{
    "service_name": "checkout-api",
    "environment": "staging",
    "instance_id": "checkout-api-7f8d",
    "tenant_id": "tenant-a"
  }},
  "source": {{
    "kind": "single_ctfs",
    "file": {{
      "object_id": "ctfs-{lifecycle}",
      "uri": "ctfs://store-a/rec-{lifecycle}/trace.ct",
      "size_bytes": 4096,
      "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "placement": {{ "pool": "ctfs-hot", "server_id": "store-a" }},
      "upload": "{upload}",
      "data_state": "{dataState}"
    }}
  }},
  "lifecycle": "{lifecycle}",
  "retry": {{
    "attempt": {retryAttempt},
    "next_retry_at": {nextRetryJson},
    "last_error": {lastErrorJson}
  }},
  "finalize": {{
    "finalized": {finalized},
    "finalized_at": {finalizedAtJson},
    "idempotency_key": "{idempotencyKey}"
  }},
  "retention": "{dataState}",
  "replication": {{ "target_replicas": 2, "completed_replicas": 1 }}
}}
"""

proc placedObject(objectId, serverId: string): PlacedObject =
  PlacedObject(
    objectId: objectId,
    uri: "ctfs://" & serverId & "/" & objectId & ".cts",
    sizeBytes: 4096'u64,
    sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    placement: Placement(pool: "ctfs-hot", serverId: serverId),
    upload: usUploaded,
    dataState: dsRetained)

proc shardedManifest(recordingId: string, segmentCount: int): TraceStorageManifest =
  var source = TraceSource(kind: tskShardedSplitCtfs)
  for segmentIndex in 0 ..< segmentCount:
    source.shardedSegments.add(ShardedCtfsSegment(
      index: segmentIndex,
      geidStart: uint64(1 + segmentIndex * 100),
      geidEnd: uint64(100 + segmentIndex * 100),
      shards: @[
        CtfsShard(
          shardIndex: 0,
          blockStart: 0'u64,
          blockEnd: 63'u64,
          replicas: @[
            placedObject(recordingId & "-segment-" & $segmentIndex & "-shard-low-a", "store-a"),
            placedObject(recordingId & "-segment-" & $segmentIndex & "-shard-low-b", "store-b")]),
        CtfsShard(
          shardIndex: 1,
          blockStart: 64'u64,
          blockEnd: 127'u64,
          replicas: @[
            placedObject(recordingId & "-segment-" & $segmentIndex & "-shard-high-a", "store-a"),
            placedObject(recordingId & "-segment-" & $segmentIndex & "-shard-high-b", "store-b")])
      ]))
  TraceStorageManifest(
    schema: traceStorageSchema,
    recordingId: recordingId,
    service: ServiceIdentity(serviceName: "checkout", environment: "test",
      instanceId: "checkout-1", tenantId: "tenant-a"),
    source: source,
    lifecycle: lsFinalized,
    retry: RetryState(attempt: 0),
    finalize: FinalizeState(finalized: true, finalizedAt: some("2026-05-06T00:00:00Z"),
      idempotencyKey: "finalize-" & recordingId),
    retention: dsRetained,
    replication: ReplicationState(targetReplicas: 2, completedReplicas: 2))

proc blockBytes(objectId: string, blockId: uint64): seq[byte] =
  for ch in objectId & ":" & $blockId:
    result.add(byte(ord(ch)))

suite "shared trace storage config":
  test "test_shared_trace_storage_config_roundtrip_nim":
    let config = parseTraceStorageConfig(readFile(fixturePath("storage_config.full.json")))
    check config.schema == traceStorageSchema
    check config.mode.kind == smDirectStorage
    check config.mode.controlPlaneUrl == "https://ci.example.test/trace-storage"
    check config.storageServers.len == 2
    check config.storageServers[0].endpoint.baseUrl == "https://store-a.example.test"
    check config.storageServers[0].credentialRef.key == "CODETRACER_STORE_A_TOKEN"
    check config.pools.len == 3
    check config.pools[0].serverIds == @["store-a", "store-b"]
    check config.splitPolicy.enabled
    check config.splitPolicy.maxSegmentBytes == 67_108_864'u64
    check config.shardPolicy.enabled
    check config.shardPolicy.shardCount == 4
    check config.shardPolicy.blockRangeBytes == 8_388_608'u64
    check config.materializedArtifactPolicy.pool == "artifacts-hot"
    check config.replication.targetReplicas == 2
    check config.replication.placement == rpDistinctServers
    check config.retention.retainedForDays == 30
    check config.retention.deleteAfterDays == 90

    let reparsed = parseTraceStorageConfig(config.toJsonString())
    check reparsed.mode.kind == config.mode.kind
    check reparsed.storageServers[1].endpoint.baseUrl == config.storageServers[1].endpoint.baseUrl
    check reparsed.shardPolicy.blockRangeBytes == config.shardPolicy.blockRangeBytes

  test "test_shared_trace_storage_config_roundtrip_managed_upload_nim":
    let config = parseTraceStorageConfig(readFile(fixturePath("storage_config.managed_upload.json")))
    check config.schema == traceStorageSchema
    check config.mode.kind == smManagedUpload
    check config.mode.controlPlaneUrl == "https://ci.example.test/managed-upload"
    check config.service.environment == "production"
    check config.storageServers[1].endpoint.baseUrl == "https://managed-store-b.example.test"
    check config.storageServers[1].credentialRef.provider == "vault"
    check config.pools[0].serverIds == @["managed-store-a", "managed-store-b"]
    check config.splitPolicy.maxSegmentBytes == 33_554_432'u64
    check config.shardPolicy.shardCount == 8
    check config.shardPolicy.blockRangeBytes == 4_194_304'u64
    check config.replication.minReplicas == 2
    check config.replication.targetReplicas == 3
    check config.replication.placement == rpSamePool
    check config.retention.deleteAfterDays == 45

    let reparsed = parseTraceStorageConfig(config.toJsonString())
    check reparsed.mode.kind == smManagedUpload
    check reparsed.replication.placement == rpSamePool

  test "test_shared_manifest_models_all_trace_source_variants_nim":
    let single = parseTraceStorageManifest(readFile(fixturePath("manifest.single_ctfs.json")))
    check single.schema == traceStorageSchema
    check single.lifecycle == lsFinalized
    check single.source.kind == tskSingleCtfs
    check single.source.file.placement.pool == "ctfs-hot"
    check single.source.file.upload == usUploaded

    let split = parseTraceStorageManifest(readFile(fixturePath("manifest.split_ctfs.json")))
    check split.source.kind == tskSplitCtfs
    check split.source.segments.len == 2
    check split.source.segments[1].file.dataState == dsRetained

    let sharded = parseTraceStorageManifest(readFile(fixturePath("manifest.sharded_split_ctfs.json")))
    check sharded.source.kind == tskShardedSplitCtfs
    check sharded.source.shardedSegments[0].shards[0].replicas.len == 2
    check sharded.source.shardedSegments[0].shards[0].blockEnd == 63'u64

    let python = parseTraceStorageManifest(readFile(fixturePath("manifest.python_materialized.json")))
    check python.source.kind == tskMaterializedArtifact
    check python.source.language == mlPython
    check python.source.artifact.placement.pool == "artifacts-hot"

    let ruby = parseTraceStorageManifest(readFile(fixturePath("manifest.ruby_materialized.json")))
    check ruby.source.language == mlRuby

    let javascript = parseTraceStorageManifest(readFile(fixturePath("manifest.javascript_materialized.json")))
    check javascript.source.language == mlJavascript

    let reparsed = parseTraceStorageManifest(sharded.toJsonString())
    check reparsed.source.kind == tskShardedSplitCtfs
    check reparsed.source.shardedSegments[0].shards[0].replicas[1].placement.serverId == "store-b"

  test "test_manifest_models_split_files_and_block_shards_orthogonally":
    let single = parseTraceStorageManifest(readFile(fixturePath("manifest.single_ctfs.json")))
    let split = parseTraceStorageManifest(readFile(fixturePath("manifest.split_ctfs.json")))
    let shardedSingle = shardedManifest("single-sharded", 1)
    let shardedSplit = shardedManifest("split-sharded", 2)

    check single.segmentCount() == 1
    check split.segmentCount() == 2
    check shardedSingle.segmentCount() == 1
    check shardedSplit.segmentCount() == 2

    check single.resolveLogicalCtfsBlock(0, 7'u64).get().replicas[0].objectId == "ctfs-single"
    check split.resolveLogicalCtfsBlock(1, 7'u64).get().replicas[0].objectId == "ctfs-split-1"
    check shardedSingle.resolveLogicalCtfsBlock(0, 7'u64).get().replicas[0].objectId == "single-sharded-segment-0-shard-low-a"
    check shardedSplit.resolveLogicalCtfsBlock(1, 70'u64).get().replicas[0].objectId == "split-sharded-segment-1-shard-high-a"

    var failedOnce = false
    var calls: seq[string]
    let reader: ObjectBlockReader = proc(obj: PlacedObject, blockId: uint64): Result[seq[byte], string] {.raises: [].} =
      calls.add(obj.objectId & ":" & $blockId)
      if obj.objectId == "split-sharded-segment-1-shard-high-a" and not failedOnce:
        failedOnce = true
        return err("replica unavailable")
      ok(blockBytes(obj.objectId, blockId))

    let bytes = shardedSplit.readLogicalCtfsBlock(1, 70'u64, reader).get()
    check bytes == blockBytes("split-sharded-segment-1-shard-high-b", 70'u64)
    check calls == @[
      "split-sharded-segment-1-shard-high-a:70",
      "split-sharded-segment-1-shard-high-b:70"]

    let materialized = parseTraceStorageManifest(readFile(fixturePath("manifest.python_materialized.json")))
    check materialized.resolveLogicalCtfsBlock(0, 0'u64).isErr

  test "test_shard_replica_failure_during_replay":
    let manifest = shardedManifest("replicated", 1)
    var calls: seq[string]
    let reader: ObjectBlockReader = proc(obj: PlacedObject, blockId: uint64): Result[seq[byte], string] {.raises: [].} =
      calls.add(obj.placement.serverId)
      if obj.placement.serverId == "store-a":
        return err("storage node stopped")
      ok(blockBytes(obj.objectId, blockId))

    let blockData = manifest.readLogicalCtfsBlock(0, 70'u64, reader).get()
    check blockData == blockBytes("replicated-segment-0-shard-high-b", 70'u64)
    check calls == @["store-a", "store-b"]

  test "test_shared_manifest_models_materialized_artifacts_without_ctfs_shards":
    for fixture in ["manifest.python_materialized.json", "manifest.ruby_materialized.json", "manifest.javascript_materialized.json"]:
      let manifest = parseTraceStorageManifest(readFile(fixturePath(fixture)))
      check manifest.source.kind == tskMaterializedArtifact
      check manifest.segmentCount() == 0
      check manifest.source.artifact.placement.pool == "artifacts-hot"
      check manifest.source.artifact.dataState == dsRetained
      check manifest.replication.targetReplicas == 2
      check manifest.retention == dsRetained
      check manifest.source.artifacts.len == 0
      let json = manifest.toJsonString()
      check not json.contains("shards")
      check not json.contains("block_start")
      check not json.contains("geid_start")
      let reparsed = parseTraceStorageManifest(json)
      check reparsed.source.kind == tskMaterializedArtifact
      check reparsed.replication.targetReplicas == manifest.replication.targetReplicas

  test "test_shared_manifest_roundtrips_upload_lifecycle_data_retry_and_finalize_states_nim":
    type Case = object
      lifecycle: string
      expectedLifecycle: LifecycleState
      upload: string
      expectedUpload: UploadState
      dataState: string
      expectedDataState: DataState
      retryAttempt: int
      nextRetryAt: string
      lastError: string
      finalized: bool
      finalizedAt: string
      idempotencyKey: string

    let cases = [
      Case(lifecycle: "pending", expectedLifecycle: lsPending, upload: "pending",
        expectedUpload: usPending, dataState: "retained", expectedDataState: dsRetained,
        retryAttempt: 0, idempotencyKey: "finalize-pending"),
      Case(lifecycle: "uploading", expectedLifecycle: lsUploading, upload: "uploading",
        expectedUpload: usUploading, dataState: "retained", expectedDataState: dsRetained,
        retryAttempt: 1, nextRetryAt: "2026-05-06T10:10:00Z",
        lastError: "in-flight retry lease", idempotencyKey: "finalize-uploading"),
      Case(lifecycle: "uploaded", expectedLifecycle: lsUploaded, upload: "uploaded",
        expectedUpload: usUploaded, dataState: "retained", expectedDataState: dsRetained,
        retryAttempt: 0, idempotencyKey: "finalize-uploaded"),
      Case(lifecycle: "finalized", expectedLifecycle: lsFinalized, upload: "uploaded",
        expectedUpload: usUploaded, dataState: "retained", expectedDataState: dsRetained,
        retryAttempt: 0, finalized: true, finalizedAt: "2026-05-06T10:11:00Z",
        idempotencyKey: "finalize-finalized"),
      Case(lifecycle: "retryable_failure", expectedLifecycle: lsRetryableFailure,
        upload: "retryable_failure", expectedUpload: usRetryableFailure,
        dataState: "expired", expectedDataState: dsExpired, retryAttempt: 3,
        nextRetryAt: "2026-05-06T10:12:00Z", lastError: "temporary storage server outage",
        idempotencyKey: "finalize-retryable"),
      Case(lifecycle: "fatal_failure", expectedLifecycle: lsFatalFailure,
        upload: "fatal_failure", expectedUpload: usFatalFailure,
        dataState: "deleted", expectedDataState: dsDeleted, retryAttempt: 4,
        lastError: "sha256 mismatch after upload", idempotencyKey: "finalize-fatal")
    ]

    for testCase in cases:
      let manifest = parseTraceStorageManifest(manifestJson(
        testCase.lifecycle,
        testCase.upload,
        testCase.dataState,
        testCase.retryAttempt,
        testCase.nextRetryAt,
        testCase.lastError,
        testCase.finalized,
        testCase.finalizedAt,
        testCase.idempotencyKey))

      check manifest.lifecycle == testCase.expectedLifecycle
      check manifest.source.kind == tskSingleCtfs
      check manifest.source.file.upload == testCase.expectedUpload
      check manifest.source.file.dataState == testCase.expectedDataState
      check manifest.retention == testCase.expectedDataState
      check manifest.retry.attempt == testCase.retryAttempt
      if testCase.nextRetryAt.len == 0:
        check manifest.retry.nextRetryAt.isNone
      else:
        check manifest.retry.nextRetryAt.get() == testCase.nextRetryAt
      if testCase.lastError.len == 0:
        check manifest.retry.lastError.isNone
      else:
        check manifest.retry.lastError.get() == testCase.lastError
      check manifest.finalize.finalized == testCase.finalized
      if testCase.finalizedAt.len == 0:
        check manifest.finalize.finalizedAt.isNone
      else:
        check manifest.finalize.finalizedAt.get() == testCase.finalizedAt
      check manifest.finalize.idempotencyKey == testCase.idempotencyKey

      let reparsed = parseTraceStorageManifest(manifest.toJsonString())
      check reparsed.lifecycle == testCase.expectedLifecycle
      check reparsed.source.file.upload == testCase.expectedUpload
      check reparsed.finalize.idempotencyKey == testCase.idempotencyKey
