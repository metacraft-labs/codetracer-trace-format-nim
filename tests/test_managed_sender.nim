import std/[json, options, tables, unittest]
import codetracer_ctfs/[managed_sender, managed_sender_ci, trace_storage_config]
import codetracer_trace_types
import codetracer_trace_writer/[meta_dat, uuid_v7]

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
    # codetracer-ci's McrSliceManifest: it refuses an entry without
    # sliceIndex, sliceKey, uploadCompletionState and retentionStatus, and
    # resolves dive-in links only through `complete` + `available` slices.
    check slices[0]["sliceIndex"].getInt() == 0
    check slices[0]["sliceKey"].getStr() == "traces/tenant-a/session/slice_0000.ct"
    check slices[0]["uploadCompletionState"].getStr() == "complete"
    check slices[0]["retentionStatus"].getStr() == "available"
    check slices[0]["contentLength"].getInt() == 4096
    check slices[0]["contentHash"].getStr() == "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    check slices[0]["geidStart"].getInt() == 10
    check slices[0]["geidEnd"].getInt() == 20
    check payload["recordingManifest"]["timeRange"]["geidStart"].getInt() == 10
    check payload["recordingManifest"]["timeRange"]["geidEnd"].getInt() == 20
    # The manifest's recordingId above ("recording") is not a recorder id the
    # writer would produce; the sender forwards whatever the recorder put
    # there and codetracer-ci validates it (400 invalid_recording_id).
    check payload["recordingManifest"]["recordingId"].getStr() == "recording"

  test "codetracer_ci_finalize_payload_declares_the_meta_dat_recording_id_nim":
    # HS-M2 U3b: the recording id codetracer-ci resolves a dive-in link by is
    # the one in the recording's meta.dat. Mint it as the writer does, write
    # a real v6 meta.dat, read the id back through the reader, and hand the
    # sender the manifest the recorder builds from it.
    let minted = newUuidV7()
    check minted.isOk
    let metaBytes = writeMetaDatToBuffer(TraceMetadata(
      recordingId: $minted.get(), workdir: "/srv", program: "inventory",
      args: @["inventory"]))
    let meta = readMetaDat(metaBytes)
    check meta.isOk
    let metaRecordingId = meta.get().recordingId
    check metaRecordingId.len == 36

    let backend = newCodetracerCiSenderBackend(CodetracerCiSenderConfig(
      baseUrl: "http://127.0.0.1:8080", tenantId: "tenant-a", bearerToken: "ct_ci_token",
      platform: "native", serviceName: "inventory", instanceId: "ct-mcr"))
    proc requestFor(recordingId: string): ManagedFinalizeRequest =
      var manifest = TraceStorageManifest(
        schema: traceStorageSchema,
        recordingId: recordingId,
        service: ServiceIdentity(serviceName: "inventory", environment: "test", instanceId: "ct-mcr", tenantId: "tenant-a"),
        lifecycle: lsUploaded,
        retry: RetryState(attempt: 0, nextRetryAt: none(string), lastError: none(string)),
        finalize: FinalizeState(finalized: false, finalizedAt: none(string), idempotencyKey: "u3b"),
        retention: dsRetained,
        replication: ReplicationState(targetReplicas: 1, completedReplicas: 1))
      manifest.source.kind = tskSplitCtfs
      manifest.source.segments = @[CtfsSegment(index: 0, geidStart: 0, geidEnd: 9,
        file: PlacedObject(objectId: "traces/t/s/slices/slice_0000.ct", uri: "local://s/slice_0000.ct",
          sizeBytes: 1, sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
          placement: Placement(pool: "p", serverId: "s"), upload: usUploaded, dataState: dsRetained))]
      ManagedFinalizeRequest(totalSlices: 1, totalEvents: 10, manifest: manifest, idempotencyKey: "u3b")

    let declared = backend.finalizePayloadJson(requestFor(metaRecordingId))
    check declared["recordingManifest"].hasKey("recordingId")
    check declared["recordingManifest"]["recordingId"].getStr() == metaRecordingId
    # Negative control: a recorder that named no recording declares none,
    # rather than an empty string codetracer-ci would refuse.
    let undeclared = backend.finalizePayloadJson(requestFor(""))
    check not undeclared["recordingManifest"].hasKey("recordingId")
