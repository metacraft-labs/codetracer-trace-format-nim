when defined(nimPreviewSlimSystem):
  import std/[syncio]

import std/[httpclient, json, net, os, strutils, times, uri]
when defined(posix):
  from std/posix import EINTR, ECONNREFUSED
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
    finalizeTimeoutMs*: int
      ## How long the finalize may take to answer, in milliseconds; 0 (the
      ## default) derives it from the upload's size (`finalizeTimeoutMs`).

  CodetracerCiSenderBackend* = ref object of ManagedSenderBackend
    config*: CodetracerCiSenderConfig
    sessionId: string
    s3KeyPrefix: string
    storagePoolId: string
    storageServerId: string
    storageEndpointUri: string
    sliceUploadMethod: string

const
  ## The timeout of every request but the finalize: each answers after a
  ## bounded amount of work on codetracer-ci's side.
  RequestTimeoutMs* = 10_000
  ## The finalize's timeout when it is derived from the upload's size:
  ## codetracer-ci reads every stored slice back from the object store and
  ## hashes it before it answers (http-api.md, "POST
  ## /api/v1/traces/{sessionId}/finalize"), so its latency grows with the
  ## recording. A base for the request itself, plus one second per
  ## `FinalizeReadBytesPerSecond` bytes uploaded, a floor for a slow or
  ## nearly full store (on 2026-10-17 a finalize of one recording outlasted
  ## the former fixed 10 s on runners whose volume was 97 to 98 percent
  ## full), capped so a stalled server is still given up on.
  FinalizeBaseTimeoutMs* = 30_000
  FinalizeReadBytesPerSecond* = 4 * 1024 * 1024
  FinalizeMaxTimeoutMs* = 600_000

const
  ## The upload session's `sliceUploadMethod` (codetracer-ci
  ## rewrite-docs/04-apis-events/http-api.md §4.4.1, "POST
  ## /api/v1/tenants/{tenantId}/traces/upload-session"): how codetracer-ci
  ## wants this session's objects written.  Its deployment's object store
  ## decides it.  An S3 or MinIO store issues a presigned URL per object
  ## (`POST /api/v1/traces/{sessionId}/slice-upload-url`), which needs no
  ## stored recorder credential; the local file system store takes the bytes
  ## through its local-storage `PUT` route.
  SliceUploadPresignedUrl* = "presigned-url"
  SliceUploadLocalStorage* = "local-storage"

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

const InterruptedRequestRepeats* = 3
  ## How many times `sendRequest` repeats one request that a signal
  ## interrupted, beyond its first try.

proc interruptedBySignal(e: ref OSError): bool =
  ## Whether a request failed because a signal interrupted the system call it
  ## was waiting in (EINTR), not because of anything the server did.
  when defined(posix):
    e.errorCode == EINTR
  else:
    false

proc sendRequest(url: string, httpMethod: HttpMethod, body: string,
    headers: HttpHeaders, timeoutMs = RequestTimeoutMs): Response =
  ## One request on a fresh connection, closed before this returns (a
  ## synchronous `HttpClient` has read the whole body by then).
  ##
  ## A request whose wait a signal interrupted is repeated, up to
  ## `InterruptedRequestRepeats` times. std `httpclient` waits for every
  ## answer with a timeout, in `poll`/`select` (std/net `waitFor`), and those
  ## calls fail with EINTR when a handled signal arrives, even when the
  ## handler was installed with `SA_RESTART` (signal(7), "Interruption of
  ## system calls and library functions by signal handlers":
  ## https://man7.org/linux/man-pages/man7/signal.7.html). The recorder
  ## handles SIGTERM and SIGINT as a graceful stop, so its own stop request
  ## interrupted the upload it was waiting on. That is not an answer from
  ## the server, and every request this sender makes may be repeated: an
  ## object is written to the same key with the same bytes, a presigned URL
  ## is issued again, and codetracer-ci's finalize is safe to repeat
  ## (`ManagedRetryPolicy`). An upload session asked for twice leaves the
  ## first one unused, as a retried session request already could.
  ##
  ## No redirect is followed: a 3xx answer (RFC 9110 §15.4,
  ## https://www.rfc-editor.org/rfc/rfc9110#section-15.4) is returned to the
  ## caller as it came, and each caller reports it as a failure
  ## (`redirectRefusal`). std `httpclient` follows up to five by default
  ## (`newHttpClient`'s `maxRedirects`), and in Nim 2.2.8 its `request`
  ## replays a PUT or POST answered 301, 302 or 303 as a body-less GET, whose
  ## 2xx the caller would take for a stored object or a committed finalize,
  ## resends the body on a 307 or 308 to wherever the answer points, and
  ## drops `Authorization` only when the redirect names another host name,
  ## so another port of the same host, or a subdomain of it, would receive
  ## the CI token. The sender writes where codetracer-ci told it to and
  ## nowhere else (http-api.md §4.4.1: the presigned URL is the whole
  ## authorization of the write), and the token stays at codetracer-ci's
  ## origin.
  var repeats = 0
  while true:
    let client = newHttpClient(timeout = timeoutMs, maxRedirects = 0)
    try:
      return client.request(url, httpMethod = httpMethod, body = body, headers = headers)
    except OSError as e:
      if not e.interruptedBySignal() or repeats >= InterruptedRequestRepeats:
        raise
      inc repeats
    finally:
      client.close()

proc retryableStatus(code: HttpCode): bool
proc retryAfterMs(response: Response): int

proc redirectRefusal(what: string, response: Response): ManagedSenderError =
  ## The failure for a 3xx answer, which `sendRequest` does not follow. It
  ## is not retryable: codetracer-ci redirecting one of its own routes, or
  ## the object store redirecting a URL the deployment's own store
  ## presigned (an S3 region redirect cannot arise for it), is a
  ## misconfiguration a retry meets again. The target is named without its
  ## query, which may carry another signature, and without its user
  ## information (RFC 3986 §3.2.1), which may carry a password.
  var target = "no Location"
  let location = response.headers.getOrDefault("location").strip()
  if location.len > 0:
    var uri = parseUri(location)
    uri.username = ""
    uri.password = ""
    uri.query = ""
    uri.anchor = ""
    target = $uri
  ManagedSenderError(retryable: false,
    message: what & " answered a redirect (HTTP " & $response.code.int & " to " & target &
      "), which the sender does not follow")

const SessionFailure = "failed to create codetracer-ci upload session"

proc ensureSession(backend: CodetracerCiSenderBackend,
    err: var ManagedSenderError): bool =
  ## Opens the upload session once. On failure, `err` says why and whether a
  ## retry can help: a refused connection (nothing listens at the configured
  ## address) or a refusal the server will repeat is reported as not
  ## retryable, so the retry policy does not spend its pauses on it; a
  ## timeout, a 5xx, 408 or 429 stays retryable.
  if backend.sessionId.len > 0:
    return true
  if backend.config.baseUrl.len == 0 or backend.config.tenantId.len == 0 or backend.config.bearerToken.len == 0:
    err = ManagedSenderError(retryable: false,
      message: SessionFailure & ": no base URL, tenant or token is configured")
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
  try:
    let response = sendRequest(url, HttpPost, $body,
      newHttpHeaders({
        "Authorization": "Bearer " & backend.config.bearerToken,
        "Content-Type": "application/json",
      }))
    if response.code.is3xx:
      err = redirectRefusal(SessionFailure & ": codetracer-ci", response)
      return false
    if response.code notin {Http200, Http201}:
      err = ManagedSenderError(retryable: response.code.retryableStatus,
        retryAfterMs: response.retryAfterMs,
        message: SessionFailure & ": HTTP " & $response.code.int)
      return false
    let parsed = parseJson(response.body)
    backend.sessionId = parsed{"sessionId"}.getStr()
    backend.s3KeyPrefix = parsed{"s3KeyPrefix"}.getStr()
    backend.storagePoolId = parsed{"storagePoolId"}.getStr()
    backend.storageServerId = parsed{"storageServerId"}.getStr()
    backend.storageEndpointUri = parsed{"storageEndpointUri"}.getStr()
    # A codetracer-ci from before the field names no method; it took every
    # object through the local-storage route, so that is what an absent
    # field means.
    backend.sliceUploadMethod = parsed{"sliceUploadMethod"}.getStr(SliceUploadLocalStorage)
    result = backend.sessionId.len > 0 and backend.s3KeyPrefix.len > 0
    if not result:
      err = ManagedSenderError(retryable: true,
        message: SessionFailure & ": the answer names no session or key prefix")
  except OSError as e:
    # A refused connection is not repeated (as `sendRequest` does not repeat
    # it): nothing listens there, and retrying only delays the failure the
    # caller must report.
    let refused =
      when defined(posix): e.errorCode == ECONNREFUSED
      else: false
    err = ManagedSenderError(retryable: not refused,
      message: SessionFailure & ": " & e.msg)
    result = false
  except CatchableError as e:
    err = ManagedSenderError(retryable: true, message: SessionFailure & ": " & e.msg)
    result = false

proc sessionObjectKey(backend: CodetracerCiSenderBackend, item: ManagedUploadObject): string =
  if item.objectKey.startsWith(backend.s3KeyPrefix):
    return item.objectKey
  let name =
    if item.objectKey.len > 0: splitFile(item.objectKey).name & splitFile(item.objectKey).ext
    else: splitFile(item.localPath).name & splitFile(item.localPath).ext
  backend.s3KeyPrefix.strip(leading = false, trailing = true, chars = {'/'}) & "/" & name

proc retryableStatus(code: HttpCode): bool =
  code.int >= 500 or code in {Http408, Http429}

proc retryAfterMs(response: Response): int =
  ## `Retry-After` in its delay-seconds form (RFC 9110 §10.2.3,
  ## https://www.rfc-editor.org/rfc/rfc9110#section-10.2.3), in milliseconds;
  ## 0 when absent or given as a date (the retry policy's own pause applies).
  let raw = response.headers.getOrDefault("retry-after").strip()
  if raw.len == 0 or raw.len > 6:
    return 0
  for c in raw:
    if c notin Digits:
      return 0
  parseInt(raw) * 1000

proc problemCode(response: Response): string =
  ## The `code` of a codetracer-ci problem answer (`ApiProblems`), or "".
  try:
    parseJson(response.body){"code"}.getStr()
  except CatchableError:
    ""

proc receiptFor(backend: CodetracerCiSenderBackend, objectKey: string): ManagedUploadReceipt =
  ManagedUploadReceipt(
    objectKey: objectKey,
    storagePoolId: backend.storagePoolId,
    storageServerId: backend.storageServerId,
    storageEndpointUri: backend.storageEndpointUri)

proc putLocalStorage(backend: CodetracerCiSenderBackend, objectKey, data,
    contentType: string): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] =
  ## The local file system store: the bytes go to codetracer-ci itself,
  ## `PUT /api/v1/observability/storage-policy/tenants/{tenantId}/local-storage/objects/{objectKey}`
  ## (http-api.md §4.4.1), which writes them into its store.
  let url = baseUrl(backend.config) & "/api/v1/observability/storage-policy/tenants/" &
    backend.config.tenantId & "/local-storage/objects/" & encodeUrl(objectKey)
  try:
    let response = sendRequest(url, HttpPut, data,
      newHttpHeaders({
        "Authorization": "Bearer " & backend.config.bearerToken,
        "Content-Type": contentType,
        "Content-Length": $data.len,
      }))
    if response.code.int >= 200 and response.code.int < 300:
      return (true, backend.receiptFor(objectKey), ManagedSenderError())
    if response.code.is3xx:
      return (false, ManagedUploadReceipt(),
        redirectRefusal("codetracer-ci's local-storage write of " & objectKey, response))
    (false, ManagedUploadReceipt(), ManagedSenderError(retryable: response.code.retryableStatus,
      message: "codetracer-ci upload failed: HTTP " & $response.code.int))
  except CatchableError as e:
    (false, ManagedUploadReceipt(), ManagedSenderError(retryable: true, message: e.msg))

proc putPresigned(backend: CodetracerCiSenderBackend, item: ManagedUploadObject,
    objectKey, data, contentType: string): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] =
  ## An S3 or MinIO store: codetracer-ci presigns a PUT of the object
  ## (`POST /api/v1/traces/{sessionId}/slice-upload-url`, http-api.md
  ## §4.4.1), and the bytes go straight to the store.  The route takes the
  ## file name and builds the key `{s3KeyPrefix}{fileName}` itself, so the
  ## key it answers must be the one this sender derived: the finalize's
  ## manifest names the objects by that key.
  let fileName = objectKey.rsplit('/', maxsplit = 1)[^1]
  let url = baseUrl(backend.config) & "/api/v1/traces/" & backend.sessionId & "/slice-upload-url"
  let body = %*{
    "sliceIndex": max(item.sliceIndex, 0),
    "fileName": fileName,
    "contentLength": data.len,
  }
  var uploadUrl, issuedKey: string
  try:
    let response = sendRequest(url, HttpPost, $body,
      newHttpHeaders({
        "Authorization": "Bearer " & backend.config.bearerToken,
        "Content-Type": "application/json",
      }))
    if response.code.is3xx:
      return (false, ManagedUploadReceipt(),
        redirectRefusal("codetracer-ci's slice upload URL request for " & objectKey, response))
    if response.code.int < 200 or response.code.int >= 300:
      return (false, ManagedUploadReceipt(), ManagedSenderError(retryable: response.code.retryableStatus,
        message: "codetracer-ci slice upload URL refused: HTTP " & $response.code.int))
    let parsed = parseJson(response.body)
    uploadUrl = parsed{"uploadUrl"}.getStr()
    issuedKey = parsed{"s3Key"}.getStr()
  except CatchableError as e:
    return (false, ManagedUploadReceipt(), ManagedSenderError(retryable: true, message: e.msg))
  if issuedKey != objectKey:
    return (false, ManagedUploadReceipt(), ManagedSenderError(retryable: false,
      message: "codetracer-ci issued an upload URL for " & issuedKey & ", not " & objectKey))
  if not (uploadUrl.startsWith("http://") or uploadUrl.startsWith("https://")):
    return (false, ManagedUploadReceipt(), ManagedSenderError(retryable: false,
      message: "codetracer-ci issued no HTTP upload URL for " & objectKey))
  # The presigned URL is the whole authorization: the store refuses a second
  # one, so the request carries no Authorization header (and the CI token is
  # not sent to the object store).
  try:
    let response = sendRequest(uploadUrl, HttpPut, data,
      newHttpHeaders({
        "Content-Type": contentType,
        "Content-Length": $data.len,
      }))
    if response.code.int >= 200 and response.code.int < 300:
      return (true, backend.receiptFor(objectKey), ManagedSenderError())
    if response.code.is3xx:
      return (false, ManagedUploadReceipt(),
        redirectRefusal("the object store's presigned upload of " & objectKey, response))
    (false, ManagedUploadReceipt(), ManagedSenderError(retryable: response.code.retryableStatus,
      message: "object store refused the presigned upload of " & objectKey & ": HTTP " & $response.code.int))
  except CatchableError as e:
    (false, ManagedUploadReceipt(), ManagedSenderError(retryable: true, message: e.msg))

proc uploadObject(backend: CodetracerCiSenderBackend, item: ManagedUploadObject,
    contentType: string): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] =
  ## Writes one object of the session by the route the session named
  ## (`sliceUploadMethod`).
  var sessionErr: ManagedSenderError
  if not backend.ensureSession(sessionErr):
    return (false, ManagedUploadReceipt(), sessionErr)
  var data: string
  try:
    data = readFile(item.localPath)
  except CatchableError as e:
    return (false, ManagedUploadReceipt(), ManagedSenderError(retryable: true, message: "failed to read upload object: " & e.msg))
  if uint64(data.len) != item.contentLength:
    return (false, ManagedUploadReceipt(), ManagedSenderError(retryable: false, message: "upload object content length changed"))
  let objectKey = backend.sessionObjectKey(item)
  case backend.sliceUploadMethod
  of SliceUploadPresignedUrl:
    backend.putPresigned(item, objectKey, data, contentType)
  of SliceUploadLocalStorage:
    backend.putLocalStorage(objectKey, data, contentType)
  else:
    (false, ManagedUploadReceipt(), ManagedSenderError(retryable: false,
      message: "codetracer-ci named an unknown sliceUploadMethod: " & backend.sliceUploadMethod))

method uploadSlice*(backend: CodetracerCiSenderBackend,
    item: ManagedUploadObject): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] =
  backend.uploadObject(item, "application/vnd.codetracer.ctfs")

method uploadMaterializedArtifact*(backend: CodetracerCiSenderBackend,
    item: ManagedUploadObject): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] =
  backend.uploadObject(item, "application/vnd.codetracer.materialized-trace+json")

method uploadManifest*(backend: CodetracerCiSenderBackend,
    item: ManagedUploadObject): tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError] =
  backend.uploadObject(item, "application/vnd.codetracer.recording-manifest+json")

# The finalize body is codetracer-ci's `FinalizeUploadSessionApiRequest`; its
# `recordingManifest` is codetracer-ci's `RecordingManifest`, and each
# `mcrSlices` entry its `McrSliceManifest` (codetracer-ci
# apps/Monolith/Monolith.TraceStorage/RecordingManifest.cs; documented in
# codetracer-ci rewrite-docs/04-apis-events/http-api.md, "POST
# /api/v1/traces/{sessionId}/finalize"). codetracer-ci refuses a slice entry
# without `sliceIndex`, `sliceKey`, `uploadCompletionState` and
# `retentionStatus`, and answers only slices that are `complete` and
# `available` when it resolves a dive-in link.

proc ciRetentionStatus(value: DataState): string =
  ## codetracer-ci's `RetentionStatuses`: a slice it may still serve is
  ## `available`; one whose bytes are gone is `expired`.
  case value
  of dsRetained: "available"
  of dsExpired, dsDeleted: "expired"

proc ciUploadCompletionState(value: UploadState): string =
  ## codetracer-ci's `UploadCompletionStates` for one slice.
  if value == usUploaded: "complete" else: "uploading"

proc segmentSliceJson(segment: CtfsSegment): JsonNode =
  %*{
    "sliceIndex": segment.index,
    "sliceKey": segment.file.objectId,
    "uploadCompletionState": segment.file.upload.ciUploadCompletionState,
    "retentionStatus": segment.file.dataState.ciRetentionStatus,
    "contentLength": segment.file.sizeBytes,
    "contentHash": "sha256:" & segment.file.sha256,
    "geidStart": segment.geidStart,
    "geidEnd": segment.geidEnd,
    "storagePoolId": segment.file.placement.pool,
    "storageServerId": segment.file.placement.serverId,
    "storageEndpointUri": segment.file.uri,
  }

proc sessionPlacementJson(sessionValue: string): JsonNode =
  ## The session's own placement value, or JSON null when `upload-session`
  ## named none.
  if sessionValue.len > 0: %sessionValue else: newJNull()

proc shardedSegmentJson(backend: CodetracerCiSenderBackend, segment: ShardedCtfsSegment): JsonNode =
  ## A sharded segment as codetracer-ci's `ShardedMcrSegmentManifest`. Every
  ## replica was written through this upload session, so its placement is the
  ## one `upload-session` answered. codetracer-ci refuses a managed finalize
  ## whose replica names a `storageEndpointUri` that is not exactly that
  ## placement's endpoint, or that names any endpoint at all when the session
  ## went to the deployment's object store (http-api.md §4.4.1, "POST
  ## /api/v1/traces/{sessionId}/finalize"). So the pool, server and endpoint
  ## are the session's, and never the replica's own object URI or recorded
  ## placement. A session in the deployment's object store has no pool, server
  ## or endpoint, so the replica names none of the three (JSON null), which
  ## the finalize accepts as an entry without placement, read from that
  ## store. The object key is the key the object was stored under
  ## (`objectId`, the upload receipt's key), as it is for a slice's
  ## `sliceKey`.
  let shards = newJArray()
  for shard in segment.shards:
    let replicas = newJArray()
    for replicaIndex, replica in shard.replicas:
      replicas.add(%*{
        "replicaIndex": replicaIndex,
        "objectKey": replica.objectId,
        "storagePoolId": sessionPlacementJson(backend.storagePoolId),
        "storageServerId": sessionPlacementJson(backend.storageServerId),
        "storageEndpointUri": sessionPlacementJson(backend.storageEndpointUri),
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
    result.add(segment.segmentSliceJson())

proc shardedMcrSegmentsJson(backend: CodetracerCiSenderBackend, manifest: TraceStorageManifest): JsonNode =
  result = newJArray()
  if manifest.source.kind != tskShardedSplitCtfs:
    return
  for segment in manifest.source.shardedSegments:
    result.add(backend.shardedSegmentJson(segment))

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
  let shardedMcrSegments = backend.shardedMcrSegmentsJson(request.manifest)
  let timeRange = request.manifest.mcrTimeRangeJson()
  result = %*{
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
  # The recording's own id (the UUIDv7 in its `meta.dat`, which the recorder
  # put into the manifest): codetracer-ci resolves a dive-in link's
  # `recording_id` by it (codetracer-specs
  # spec/Observability-Platform/docs/attribute-url-contract-v0.md, "Dive-In
  # URL"). Left out when the recorder named none; codetracer-ci then knows the
  # upload by its own id only.
  if request.manifest.recordingId.len > 0:
    result["recordingId"] = %request.manifest.recordingId

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

proc uploadedBytes(request: ManagedFinalizeRequest): uint64 =
  ## The bytes the finalize will read back: every slice, or every shard
  ## replica of a sharded recording.
  for segment in request.manifest.source.segments:
    result += segment.file.sizeBytes
  for segment in request.manifest.source.shardedSegments:
    for shard in segment.shards:
      for replica in shard.replicas:
        result += replica.sizeBytes

proc finalizeTimeoutMs*(config: CodetracerCiSenderConfig, request: ManagedFinalizeRequest): int =
  ## The finalize's timeout: `config.finalizeTimeoutMs` when set, else
  ## `FinalizeBaseTimeoutMs` plus one second per `FinalizeReadBytesPerSecond`
  ## uploaded bytes, at most `FinalizeMaxTimeoutMs`.
  if config.finalizeTimeoutMs > 0:
    return config.finalizeTimeoutMs
  let readMs = request.uploadedBytes() div uint64(FinalizeReadBytesPerSecond) * 1000
  int(min(uint64(FinalizeBaseTimeoutMs) + readMs, uint64(FinalizeMaxTimeoutMs)))

proc finalizeErrorRetryable(response: Response): bool =
  ## Which finalize refusals a retry can get past (http-api.md, "POST
  ## /api/v1/traces/{sessionId}/finalize"): `409 upload_finalizing` (a later
  ## finalize took this one's claim over; repeating it commits or answers
  ## 204), `503 upload_session_busy` and every other 5xx, 408 and 429. Every
  ## other 409 (`slice_content_mismatch`, `recording_id_conflict`) and every
  ## 4xx is the same on a retry. A 3xx never reaches it: `finalize` reports
  ## it by `redirectRefusal`, not retryable.
  if response.code == Http409:
    return response.problemCode() == "upload_finalizing"
  response.code.retryableStatus

method finalize*(backend: CodetracerCiSenderBackend,
    request: ManagedFinalizeRequest): tuple[ok: bool, err: ManagedSenderError] =
  if not request.canFinalizeComplete():
    return (false, ManagedSenderError(retryable: false,
      message: "refusing complete finalize without uploaded MCR slice metadata"))
  var sessionErr: ManagedSenderError
  if not backend.ensureSession(sessionErr):
    return (false, sessionErr)
  let body = backend.finalizePayloadJson(request)
  let url = baseUrl(backend.config) & "/api/v1/traces/" & backend.sessionId & "/finalize"
  let timeoutMs = backend.config.finalizeTimeoutMs(request)
  try:
    let response = sendRequest(url, HttpPost, $body,
      newHttpHeaders({
        "Authorization": "Bearer " & backend.config.bearerToken,
        "Content-Type": "application/json",
      }), timeoutMs = timeoutMs)
    if response.code.int >= 200 and response.code.int < 300:
      return (true, ManagedSenderError())
    if response.code.is3xx:
      return (false, redirectRefusal("codetracer-ci finalize", response))
    let code = response.problemCode()
    (false, ManagedSenderError(retryable: response.finalizeErrorRetryable(),
      retryAfterMs: response.retryAfterMs(),
      message: "codetracer-ci finalize failed: HTTP " & $response.code.int &
        (if code.len > 0: " " & code else: "")))
  except TimeoutError as e:
    (false, ManagedSenderError(retryable: true,
      message: "codetracer-ci finalize did not answer within " & $timeoutMs & " ms: " & e.msg))
  except CatchableError as e:
    (false, ManagedSenderError(retryable: true, message: e.msg))

method health*(backend: CodetracerCiSenderBackend): ManagedSenderHealth =
  ManagedSenderHealth(healthy: backend.config.baseUrl.len > 0, message: backend.config.baseUrl)
