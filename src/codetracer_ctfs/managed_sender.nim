import std/[options, tables]
import ./trace_storage_config

type
  ManagedUploadKind* = enum
    mukMcrSlice, mukMaterializedArtifact, mukManifest

  ManagedUploadObject* = object
    objectKey*: string
    localPath*: string
    contentLength*: uint64
    sha256*: string
    kind*: ManagedUploadKind
    sliceIndex*: int
    artifactKind*: string

  ManagedUploadReceipt* = object
    objectKey*: string
    storagePoolId*: string
    storageServerId*: string
    storageEndpointUri*: string

  ManagedSenderHealth* = object
    healthy*: bool
    message*: string

  ManagedSenderError* = object
    retryable*: bool
    message*: string

  ManagedFinalizeRequest* = object
    totalSlices*: int
    totalEvents*: uint64
    manifest*: TraceStorageManifest
    idempotencyKey*: string

  ManagedObjectState* = object
    uploadObject*: ManagedUploadObject
    receipt*: Option[ManagedUploadReceipt]
    upload*: UploadState
    retry*: RetryState

  ManagedSenderState* = object
    objects*: Table[string, ManagedObjectState]
    finalized*: bool
    finalizeAttempts*: int
    idempotencyKey*: string

  ManagedSenderBackend* = ref object of RootObj

method uploadSlice*(backend: ManagedSenderBackend,
    item: ManagedUploadObject): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] {.base.} =
  (false, ManagedUploadReceipt(), ManagedSenderError(retryable: false, message: "uploadSlice not implemented"))

method uploadMaterializedArtifact*(backend: ManagedSenderBackend,
    item: ManagedUploadObject): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] {.base.} =
  (false, ManagedUploadReceipt(), ManagedSenderError(retryable: false, message: "uploadMaterializedArtifact not implemented"))

method uploadManifest*(backend: ManagedSenderBackend,
    item: ManagedUploadObject): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] {.base.} =
  (false, ManagedUploadReceipt(), ManagedSenderError(retryable: false, message: "uploadManifest not implemented"))

method finalize*(backend: ManagedSenderBackend,
    request: ManagedFinalizeRequest): tuple[ok: bool, err: ManagedSenderError] {.base.} =
  (false, ManagedSenderError(retryable: false, message: "finalize not implemented"))

method health*(backend: ManagedSenderBackend): ManagedSenderHealth {.base.} =
  ManagedSenderHealth(healthy: false, message: "health not implemented")

proc initManagedSenderState*(idempotencyKey: string): ManagedSenderState =
  ManagedSenderState(
    objects: initTable[string, ManagedObjectState](),
    finalized: false,
    finalizeAttempts: 0,
    idempotencyKey: idempotencyKey)

proc retryState(attempt: int, message: string): RetryState =
  RetryState(attempt: attempt, nextRetryAt: none(string), lastError: some(message))

proc uploadWithBackend*(
    state: var ManagedSenderState,
    backend: ManagedSenderBackend,
    item: ManagedUploadObject): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] =
  if state.objects.hasKey(item.objectKey) and state.objects[item.objectKey].upload == usUploaded:
    let existing = state.objects[item.objectKey]
    return (true, existing.receipt.get(), ManagedSenderError())

  if not state.objects.hasKey(item.objectKey):
    state.objects[item.objectKey] = ManagedObjectState(
      uploadObject: item,
      receipt: none(ManagedUploadReceipt),
      upload: usPending,
      retry: retryState(0, ""))
  state.objects[item.objectKey].upload = usUploading

  let response =
    case item.kind
    of mukMcrSlice: backend.uploadSlice(item)
    of mukMaterializedArtifact: backend.uploadMaterializedArtifact(item)
    of mukManifest: backend.uploadManifest(item)

  if response.ok:
    state.objects[item.objectKey].receipt = some(response.receipt)
    state.objects[item.objectKey].upload = usUploaded
  else:
    let attempt = state.objects[item.objectKey].retry.attempt + 1
    state.objects[item.objectKey].retry = retryState(attempt, response.err.message)
    state.objects[item.objectKey].upload =
      if response.err.retryable: usRetryableFailure else: usFatalFailure
  response

proc tryUploadWithBackend*(state: var ManagedSenderState,
    backend: ManagedSenderBackend,
    item: ManagedUploadObject): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] {.raises: [].} =
  try:
    {.cast(raises: []).}:
      result = state.uploadWithBackend(backend, item)
  except CatchableError as e:
    result = (false, ManagedUploadReceipt(), ManagedSenderError(retryable: true, message: e.msg))

proc retryPending*(state: var ManagedSenderState,
    backend: ManagedSenderBackend): seq[ManagedUploadReceipt] =
  for _, entry in state.objects:
    if entry.upload == usRetryableFailure:
      let response = state.uploadWithBackend(backend, entry.uploadObject)
      if response.ok:
        result.add(response.receipt)

proc finalizeManagedUpload*(state: var ManagedSenderState,
    backend: ManagedSenderBackend,
    request: ManagedFinalizeRequest): tuple[ok: bool, err: ManagedSenderError] =
  inc state.finalizeAttempts
  if state.finalized and request.idempotencyKey == state.idempotencyKey:
    return (true, ManagedSenderError())
  let response = backend.finalize(request)
  if response.ok:
    state.finalized = true
    state.idempotencyKey = request.idempotencyKey
  response

proc tryFinalizeManagedUpload*(state: var ManagedSenderState,
    backend: ManagedSenderBackend,
    request: ManagedFinalizeRequest): tuple[ok: bool, err: ManagedSenderError] {.raises: [].} =
  try:
    {.cast(raises: []).}:
      result = state.finalizeManagedUpload(backend, request)
  except CatchableError as e:
    result = (false, ManagedSenderError(retryable: true, message: e.msg))
