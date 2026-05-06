import std/[json, options, tables, unittest]
import codetracer_ctfs/[managed_sender, managed_sender_ci, trace_storage_config]

type
  TestBackend = ref object of ManagedSenderBackend
    failUploads: int
    failFinalizes: int
    uploaded: seq[string]
    finalized: seq[string]
    finalizedSliceCounts: seq[int]

method uploadSlice(backend: TestBackend,
    item: ManagedUploadObject): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] =
  if backend.failUploads > 0:
    dec backend.failUploads
    return (false, ManagedUploadReceipt(), ManagedSenderError(retryable: true, message: "transient slice failure"))
  backend.uploaded.add(item.objectKey)
  (true, ManagedUploadReceipt(objectKey: item.objectKey, storagePoolId: "shared-local",
    storageServerId: "local-storage-1", storageEndpointUri: "local://codetracer-ci/storage-service"),
    ManagedSenderError())

method uploadMaterializedArtifact(backend: TestBackend,
    item: ManagedUploadObject): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] =
  if backend.failUploads > 0:
    dec backend.failUploads
    return (false, ManagedUploadReceipt(), ManagedSenderError(retryable: true, message: "transient artifact failure"))
  backend.uploaded.add(item.objectKey)
  (true, ManagedUploadReceipt(objectKey: item.objectKey, storagePoolId: "shared-local",
    storageServerId: "local-storage-1", storageEndpointUri: "local://codetracer-ci/storage-service"),
    ManagedSenderError())

method uploadManifest(backend: TestBackend,
    item: ManagedUploadObject): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] =
  backend.uploaded.add(item.objectKey)
  (true, ManagedUploadReceipt(objectKey: item.objectKey, storagePoolId: "shared-local",
    storageServerId: "local-storage-1", storageEndpointUri: "local://codetracer-ci/storage-service"),
    ManagedSenderError())

method finalize(backend: TestBackend,
    request: ManagedFinalizeRequest): tuple[ok: bool, err: ManagedSenderError] =
  if backend.failFinalizes > 0:
    dec backend.failFinalizes
    return (false, ManagedSenderError(retryable: true, message: "transient finalize failure"))
  backend.finalized.add(request.idempotencyKey)
  backend.finalizedSliceCounts.add(request.manifest.source.segments.len)
  (true, ManagedSenderError())

suite "managed shared sender":
  test "test_shared_sender_retries_and_finalize_is_idempotent_nim":
    var backend = TestBackend(failUploads: 2, failFinalizes: 1)
    var state = initManagedSenderState("finalize-m32")

    let slice = ManagedUploadObject(
      objectKey: "traces/tenant/recording/slice_0000.ct",
      localPath: "/tmp/slice_0000.ct",
      contentLength: 128,
      sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      kind: mukMcrSlice,
      sliceIndex: 0)
    let artifact = ManagedUploadObject(
      objectKey: "traces/tenant/recording/python-materialized-trace-v1.json",
      localPath: "/tmp/python/materialized-trace-v1.json",
      contentLength: 256,
      sha256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      kind: mukMaterializedArtifact,
      artifactKind: "materialized_trace_v1")

    check not state.uploadWithBackend(backend, slice).ok
    check state.objects[slice.objectKey].upload == usRetryableFailure
    check not state.uploadWithBackend(backend, artifact).ok
    check state.objects[artifact.objectKey].upload == usRetryableFailure

    let receipts = state.retryPending(backend)
    check receipts.len == 2
    check state.objects[slice.objectKey].upload == usUploaded
    check state.objects[artifact.objectKey].upload == usUploaded

    var manifest = TraceStorageManifest(
      schema: traceStorageSchema,
      recordingId: "recording",
      service: ServiceIdentity(serviceName: "checkout", environment: "test", instanceId: "checkout-1", tenantId: "tenant"),
      lifecycle: lsUploaded,
      retry: RetryState(attempt: 0, nextRetryAt: none(string), lastError: none(string)),
      finalize: FinalizeState(finalized: false, finalizedAt: none(string), idempotencyKey: "finalize-m32"),
      retention: dsRetained,
      replication: ReplicationState(targetReplicas: 1, completedReplicas: 1))
    manifest.source.kind = tskSplitCtfs
    manifest.source.segments = @[CtfsSegment(
      index: 0,
      geidStart: 1,
      geidEnd: 11,
      file: PlacedObject(
        objectId: "traces/tenant/recording/slice_0000.ct",
        uri: "local://codetracer-ci/storage-service/traces/tenant/recording/slice_0000.ct",
        sizeBytes: 128,
        sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        placement: Placement(pool: "shared-local", serverId: "local-storage-1"),
        upload: usUploaded,
        dataState: dsRetained))]

    let request = ManagedFinalizeRequest(totalSlices: 1, totalEvents: 10, manifest: manifest, idempotencyKey: "finalize-m32")
    check not state.finalizeManagedUpload(backend, request).ok
    check state.finalizeManagedUpload(backend, request).ok
    check state.finalizeManagedUpload(backend, request).ok
    check backend.uploaded.len == 2
    check backend.finalized == @["finalize-m32"]
    check backend.finalizedSliceCounts == @[1]

  test "codetracer_ci_finalize_payload_includes_mcr_slice_metadata_nim":
    let backend = newCodetracerCiSenderBackend(CodetracerCiSenderConfig(
      baseUrl: "http://127.0.0.1:8080",
      tenantId: "tenant-a",
      bearerToken: "token",
      platform: "native",
      serviceName: "checkout",
      instanceId: "ct-mcr"))
    var manifest = TraceStorageManifest(
      schema: traceStorageSchema,
      recordingId: "recording",
      service: ServiceIdentity(serviceName: "checkout", environment: "test", instanceId: "ct-mcr", tenantId: "tenant-a"),
      lifecycle: lsUploaded,
      retry: RetryState(attempt: 0, nextRetryAt: none(string), lastError: none(string)),
      finalize: FinalizeState(finalized: false, finalizedAt: none(string), idempotencyKey: "finalize-m32"),
      retention: dsRetained,
      replication: ReplicationState(targetReplicas: 1, completedReplicas: 1))
    manifest.source.kind = tskSplitCtfs
    manifest.source.segments = @[
      CtfsSegment(
        index: 0,
        geidStart: 10,
        geidEnd: 20,
        file: PlacedObject(
          objectId: "traces/tenant-a/session/slice_0000.ct",
          uri: "local://codetracer-ci/storage-service/traces/tenant-a/session/slice_0000.ct",
          sizeBytes: 4096,
          sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
          placement: Placement(pool: "shared-local", serverId: "local-storage-1"),
          upload: usUploaded,
          dataState: dsRetained))]

    let request = ManagedFinalizeRequest(
      totalSlices: 1,
      totalEvents: 55,
      manifest: manifest,
      idempotencyKey: "finalize-m32")
    let payload = backend.finalizePayloadJson(request)
    let slices = payload["recordingManifest"]["mcrSlices"]
    check slices.kind == JArray
    check slices.len == 1
    check slices[0]["key"].getStr() == "traces/tenant-a/session/slice_0000.ct"
    check slices[0]["index"].getInt() == 0
    check slices[0]["order"].getInt() == 0
    check slices[0]["sizeBytes"].getInt() == 4096
    check slices[0]["sha256"].getStr() == "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    check slices[0]["retentionStatus"].getStr() == "retained"
    check slices[0]["uploadState"].getStr() == "uploaded"
    check slices[0]["sourceKind"].getStr() == "split_ctfs"
    check slices[0]["timeRange"]["geidStart"].getInt() == 10
    check slices[0]["timeRange"]["geidEnd"].getInt() == 20
    check payload["recordingManifest"]["timeRange"]["geidStart"].getInt() == 10
    check payload["recordingManifest"]["timeRange"]["geidEnd"].getInt() == 20
