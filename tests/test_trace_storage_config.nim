## The Nim trace-storage model on its own: the sharded-replica read path and
## the manifest's lifecycle, upload, data, retry and finalize states, each
## round-tripped through JSON. No mocks: the production parser and reader, with
## an in-memory block reader standing in for a storage server.
##
## The checks that the Nim parser reads the trace-storage fixtures shared with
## the Rust implementation, and reads them as that implementation does, are
## cross-implementation checks; they live in codetracer-trace-format-conformance.

import std/[options, strformat, unittest]

import codetracer_ctfs/trace_storage_config
import codetracer_ctfs/trace_storage_reader
import results

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
