import std/[monotimes, options, os, tables, times]
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
    retryAfterMs*: int
      ## The server's own hint (`Retry-After`) for when to try again, in
      ## milliseconds; 0 when it gave none.

  ManagedRetryPolicy* = object
    ## How `tryUploadWithBackend` and `tryFinalizeManagedUpload` repeat a step
    ## that failed with a retryable error. Every step they repeat is
    ## idempotent: a slice is written to the same key with the same bytes,
    ## and codetracer-ci's finalize of a session is safe to repeat (a retry
    ## takes over a finalize still checking the stored slices, and a retry of
    ## a finalized upload answers 204; codetracer-ci
    ## rewrite-docs/04-apis-events/http-api.md, "POST
    ## /api/v1/traces/{sessionId}/finalize").
    maxAttempts*: int
      ## Attempts in all, the first included; 1 disables retrying.
    initialDelayMs*: int
      ## The pause before the first retry; it doubles per retry.
    maxDelayMs*: int
      ## The longest pause between two attempts.
    budgetMs*: int
      ## No attempt starts later than this many milliseconds after the
      ## first one did, so a step gives up within the budget plus the
      ## duration of one attempt (which the sender's own timeouts bound).

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

proc defaultManagedRetryPolicy*(): ManagedRetryPolicy =
  ## Five attempts within two minutes: a transient failure (a timeout, a
  ## 5xx, codetracer-ci's `409 upload_finalizing` or `503
  ## upload_session_busy`) costs a recording at most that long, and a
  ## permanent one still ends the upload in bounded time.
  ManagedRetryPolicy(maxAttempts: 5, initialDelayMs: 500, maxDelayMs: 8_000, budgetMs: 120_000)

proc noManagedRetry*(): ManagedRetryPolicy =
  ## A single attempt.
  ManagedRetryPolicy(maxAttempts: 1)

proc retryDelayMs(policy: ManagedRetryPolicy, retry: int, err: ManagedSenderError): int =
  ## The pause before retry number `retry` (1 for the first): exponential
  ## from `initialDelayMs`, capped at `maxDelayMs`, and never shorter than
  ## what the server asked for with `Retry-After`.
  result = max(policy.initialDelayMs, 0)
  for _ in 1 ..< retry:
    if result >= policy.maxDelayMs:
      break
    result = result * 2
  result = min(result, max(policy.maxDelayMs, 0))
  result = max(result, err.retryAfterMs)

proc shouldRetry(policy: ManagedRetryPolicy, attempt: int, started: MonoTime,
    err: ManagedSenderError, delayMs: var int): bool =
  ## Whether attempt `attempt + 1` may follow a failed attempt `attempt`, and
  ## after what pause: only a retryable error, within `maxAttempts`, and only
  ## when the next attempt would start within the budget.
  if not err.retryable or attempt >= policy.maxAttempts:
    return false
  delayMs = policy.retryDelayMs(attempt, err)
  let elapsedMs = int((getMonoTime() - started).inMilliseconds)
  elapsedMs + delayMs <= policy.budgetMs

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
    item: ManagedUploadObject,
    policy = defaultManagedRetryPolicy()): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] {.raises: [].} =
  ## `uploadWithBackend`, repeated by `policy` while it fails retryably.
  ## Never raises. The error returned is the last attempt's, prefixed with
  ## the number of attempts when there were several.
  let started = getMonoTime()
  var attempt = 0
  while true:
    inc attempt
    try:
      {.cast(raises: []).}:
        result = state.uploadWithBackend(backend, item)
    except CatchableError as e:
      result = (false, ManagedUploadReceipt(), ManagedSenderError(retryable: true, message: e.msg))
    var delayMs = 0
    if result.ok or not policy.shouldRetry(attempt, started, result.err, delayMs):
      if not result.ok and attempt > 1:
        result.err.message = "after " & $attempt & " attempts: " & result.err.message
      return
    sleep(delayMs)

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
    request: ManagedFinalizeRequest,
    policy = defaultManagedRetryPolicy()): tuple[ok: bool, err: ManagedSenderError] {.raises: [].} =
  ## `finalizeManagedUpload`, repeated by `policy` while it fails retryably:
  ## a finalize that timed out (codetracer-ci reads and hashes every stored
  ## slice before it answers, so a slow store makes it slow) is retried
  ## rather than given up. Never raises. The error returned is the last
  ## attempt's, prefixed with the number of attempts when there were several.
  let started = getMonoTime()
  var attempt = 0
  while true:
    inc attempt
    try:
      {.cast(raises: []).}:
        result = state.finalizeManagedUpload(backend, request)
    except CatchableError as e:
      result = (false, ManagedSenderError(retryable: true, message: e.msg))
    var delayMs = 0
    if result.ok or not policy.shouldRetry(attempt, started, result.err, delayMs):
      if not result.ok and attempt > 1:
        result.err.message = "after " & $attempt & " attempts: " & result.err.message
      return
    sleep(delayMs)
