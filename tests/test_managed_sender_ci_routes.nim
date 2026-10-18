## The codetracer-ci sender writes each object by the route its upload
## session names (`sliceUploadMethod`, codetracer-ci
## rewrite-docs/04-apis-events/http-api.md §4.4.1), and a finalized upload's
## objects are the recording's bytes.
##
## - presigned-url (an S3 or MinIO store): one `slice-upload-url` per object,
##   the bytes PUT to the presigned URL with no Authorization header; no
##   object goes through the local-storage route, which codetracer-ci
##   refuses to a tenant without a recorder storage credential (no API
##   issues one; http-api.md §4.4.1).
## - local-storage (the local file system store), and a session that names no
##   method (a codetracer-ci from before the field): the local-storage `PUT`.
## - an unknown method, and an issued key that is not the one the sender
##   derived: refused, not retried.
## - a finalize that does not answer within its timeout, `409
##   upload_finalizing` and `503 upload_session_busy` (with `Retry-After`)
##   are retried by the sender's retry policy, within its budget, and the
##   retry finalizes; `409 slice_content_mismatch` is not retried; the
##   finalize's own timeout outlasts the former fixed 10 s; a transient
##   refusal of a slice's upload URL is retried (managed_sender.nim,
##   `tryFinalizeManagedUpload`, `tryUploadWithBackend`).
## - a handled signal that arrives while a request waits for its answer
##   interrupts that wait (EINTR), and the request is repeated rather than
##   reported as failed, even with no retry policy: a slice write and a
##   finalize each complete on one policy attempt (managed_sender_ci.nim,
##   `sendRequest`). The signal is a handler installed with `signal()`,
##   which glibc installs with `SA_RESTART`, as codetracer-native-recorder
##   installs its graceful-stop handler for SIGTERM; it is SIGUSR1 here so
##   the test process is not stopped by it.
## - a session request that the stand-in refuses with 503 is retried by the
##   retry policy, while a refused connection at session creation is not:
##   one connection attempt under a retry policy too, reported as not
##   retryable (managed_sender_ci.nim, `ensureSession`).
## - a request whose connection is refused (ECONNREFUSED: a socket error
##   that is not EINTR) is not repeated: one connection attempt, then the
##   failure (`sendRequest` repeats only an interrupted wait). The attempts
##   are counted by `connect` below, which this test defines: it counts the
##   calls that name the refused port and passes every call to the C
##   library's `connect` unchanged. Justification: a refused connection
##   reaches no listener that could count it, and the other socket errors a
##   peer can cause (a reset) reach the sender as a closed connection, not
##   as an OSError (std/net `isDisconnectionError`).
##
## What runs, and what is a double:
## - The object store is a real MinIO (`MINIO_BIN`, else `minio` on PATH;
##   the dev shell provides it), on a private bucket: an unsigned PUT to it is
##   refused (checked), so an accepted upload came through a valid signature.
## - codetracer-ci's three upload routes are a stand-in
##   (tests/support/ci_upload_standin.py, run with `python3`). Justification:
##   codetracer-ci is a .NET service this repository cannot build or run, and
##   what is under test here is the sender's side of the routes as
##   http-api.md defines them. The stand-in presigns with AWS Signature
##   Version 4 against the MinIO, answers the local-storage route 403 on that
##   store as codetracer-ci does, and finalizes only when every slice it
##   reads back from the store has the length and SHA-256 the manifest
##   declares. For the retry tests it can also answer the first finalize
##   calls slowly or with the refusals http-api.md defines for them
##   (`--finalize-script`), and refuse the first slice-upload-url calls
##   (`--fail-upload-urls`): the real Monolith produces those answers only
##   under load or contention a test cannot schedule. The real codetracer-ci on a real MinIO, with a real recording
##   through this sender, is codetracer-observability-e2e's
##   `e2e_homeostate_recording_uploads_and_dive_in_resolves`; codetracer-ci's
##   own `ManagedUploadCiTokenTests` check the method each store names.
##
## A missing MinIO or python3 fails the test; it never passes without them.

import std/[json, net, os, osproc, httpclient, options, random, streams, strtabs, strutils, times, unittest]
from std/posix import nil
import codetracer_ctfs/[managed_sender, managed_sender_ci, trace_storage_config]

const
  Token = "ct_ci_sender_routes_test_token"
  standinScript = currentSourcePath().parentDir / "support" / "ci_upload_standin.py"

type
  Running = object
    process: Process
    port: int

proc requireExe(name, envVar: string): string =
  result = getEnv(envVar)
  if result.len == 0:
    result = findExe(name)
  if result.len == 0 or not fileExists(result):
    raise newException(IOError, "this test needs " & name & " (set " & envVar &
      " or enter the dev shell, which provides it)")

proc freePort(): int =
  ## A port the kernel picks for a listener, then closes: MinIO cannot bind
  ## port 0 and report it.
  let p = startProcess(requireExe("python3", "PYTHON3_BIN"), args = ["-c",
    "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1])"],
    options = {poUsePath})
  let output = p.outputStream.readAll()
  discard p.waitForExit()
  p.close()
  parseInt(output.strip())

proc waitFor(predicate: proc(): bool, what: string, seconds = 30.0) =
  let deadline = epochTime() + seconds
  while not predicate():
    if epochTime() > deadline:
      raise newException(IOError, "timed out waiting for " & what)
    sleep(100)

proc answers(url: string): bool =
  let client = newHttpClient(timeout = 1000)
  try:
    discard client.request(url, httpMethod = HttpGet)
    true
  except CatchableError:
    false
  finally:
    client.close()

proc randomSecret(): string =
  for _ in 0 ..< 24:
    result.add char(ord('a') + rand(25))

proc sha256Hex(path: string): string =
  ## The same hash the recorder sends (its `contentHash`), computed here with
  ## Python's hashlib so the test does not use the code under test.
  let p = startProcess(requireExe("python3", "PYTHON3_BIN"), args = ["-c",
    "import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())", path],
    options = {poUsePath})
  result = p.outputStream.readAll().strip()
  discard p.waitForExit()
  p.close()

let work = getTempDir() / ("managed-sender-routes-" & $getCurrentProcessId())
createDir(work)
randomize()

let minioBin = requireExe("minio", "MINIO_BIN")
let python = requireExe("python3", "PYTHON3_BIN")
let accessKey = "sender" & randomSecret()
let secretKey = randomSecret()
let minioPort = freePort()
let minioUrl = "http://127.0.0.1:" & $minioPort
let minio = startProcess(minioBin, args = ["server", "--quiet", "--address", "127.0.0.1:" & $minioPort,
  "--console-address", "127.0.0.1:" & $freePort(), work / "minio"],
  env = block:
    var e = newStringTable()
    for k, v in envPairs(): e[k] = v
    e["MINIO_ROOT_USER"] = accessKey
    e["MINIO_ROOT_PASSWORD"] = secretKey
    e["MINIO_BROWSER"] = "off"
    e,
  options = {poParentStreams})
waitFor(proc(): bool = answers(minioUrl & "/minio/health/ready"), "MinIO to be ready")

var standins: seq[Process]

proc startStandin(name, methodName: string, wrongKey = false, badSignature = false,
    finalizeScript = "", failUploadUrls = 0, slowLocalWrites = 0.0,
    failSessions = 0): Running =
  let portFile = work / (name & ".port")
  var args = @[standinScript, "--port-file", portFile,
    "--token", Token, "--method", methodName, "--minio-url", minioUrl,
    "--access-key", accessKey, "--secret-key", secretKey, "--bucket", "sender-" & name,
    "--local-root", work / ("local-" & name)]
  if wrongKey:
    args.add "--wrong-key"
  if badSignature:
    args.add "--bad-signature"
  if finalizeScript.len > 0:
    args.add ["--finalize-script", finalizeScript]
  if failUploadUrls > 0:
    args.add ["--fail-upload-urls", $failUploadUrls]
  if slowLocalWrites > 0:
    args.add ["--slow-local-writes", $slowLocalWrites]
  if failSessions > 0:
    args.add ["--fail-sessions", $failSessions]
  let p = startProcess(python, args = args, options = {poStdErrToStdOut, poParentStreams})
  standins.add p
  waitFor(proc(): bool = fileExists(portFile), "the codetracer-ci stand-in " & name)
  Running(process: p, port: parseInt(readFile(portFile).strip()))

proc stopAll() =
  for p in standins:
    p.terminate()
    discard p.waitForExit()
    p.close()
  minio.terminate()
  discard minio.waitForExit()
  minio.close()
  removeDir(work)

proc sliceFile(name: string, size: int): string =
  result = work / name
  var data = newString(size)
  for i in 0 ..< size:
    data[i] = char(rand(255))
  writeFile(result, data)

proc sliceObject(path: string, index: int): ManagedUploadObject =
  ManagedUploadObject(objectKey: extractFilename(path), localPath: path,
    contentLength: uint64(getFileSize(path)), sha256: sha256Hex(path),
    kind: mukMcrSlice, sliceIndex: index)

proc backendFor(standin: Running, finalizeTimeoutMs = 0): CodetracerCiSenderBackend =
  newCodetracerCiSenderBackend(CodetracerCiSenderConfig(
    baseUrl: "http://127.0.0.1:" & $standin.port, tenantId: "tenant-routes",
    bearerToken: Token, platform: "native", serviceName: "inventory", instanceId: "ct-mcr",
    finalizeTimeoutMs: finalizeTimeoutMs))

proc getJson(standin: Running, path: string): JsonNode =
  let client = newHttpClient(timeout = 10_000)
  try:
    parseJson(client.getContent("http://127.0.0.1:" & $standin.port & path))
  finally:
    client.close()

proc requestsTo(standin: Running, suffix: string): seq[JsonNode] =
  for r in standin.getJson("/_test/requests"):
    if r["path"].getStr().contains(suffix):
      result.add r

proc finalizeRequest(receipts: seq[(ManagedUploadObject, ManagedUploadReceipt)]): ManagedFinalizeRequest =
  ## The manifest the recorder builds from the receipts (codetracer-native-recorder
  ## ct_cli record_cmd.nim, managedUploadFile).
  var manifest = TraceStorageManifest(
    schema: traceStorageSchema, recordingId: "0192f0c4-a1b2-7c3d-8e9f-0a1b2c3d4e5f",
    service: ServiceIdentity(serviceName: "inventory", environment: "test", instanceId: "ct-mcr", tenantId: "tenant-routes"),
    lifecycle: lsUploaded,
    retry: RetryState(attempt: 0, nextRetryAt: none(string), lastError: none(string)),
    finalize: FinalizeState(finalized: false, finalizedAt: none(string), idempotencyKey: "routes"),
    retention: dsRetained, replication: ReplicationState(targetReplicas: 1, completedReplicas: 1))
  manifest.source.kind = tskSplitCtfs
  for i, (item, receipt) in receipts:
    manifest.source.segments.add CtfsSegment(index: i, geidStart: uint64(i * 10), geidEnd: uint64(i * 10 + 10),
      file: PlacedObject(objectId: receipt.objectKey, uri: receipt.storageEndpointUri & "/" & receipt.objectKey,
        sizeBytes: item.contentLength, sha256: item.sha256,
        placement: Placement(pool: receipt.storagePoolId, serverId: receipt.storageServerId),
        upload: usUploaded, dataState: dsRetained))
  ManagedFinalizeRequest(totalSlices: receipts.len, totalEvents: 20, manifest: manifest, idempotencyKey: "routes")

proc uploadTwoSlices(standin: Running, tag: string, finalizeTimeoutMs = 0):
    tuple[state: ManagedSenderState, backend: CodetracerCiSenderBackend,
          items: seq[ManagedUploadObject], results: seq[tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError]]] =
  result.backend = backendFor(standin, finalizeTimeoutMs)
  result.state = initManagedSenderState("routes-" & tag)
  # Sizes on both sides of MinIO's inline-data threshold (128 KiB).
  for i, size in [4093, 300_001]:
    let item = sliceObject(sliceFile(tag & "-slice_000" & $i & ".ct", size), i)
    result.items.add item
    result.results.add result.state.uploadWithBackend(result.backend, item)

proc receiptsOf(up: tuple[state: ManagedSenderState, backend: CodetracerCiSenderBackend,
    items: seq[ManagedUploadObject], results: seq[tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError]]]):
    seq[(ManagedUploadObject, ManagedUploadReceipt)] =
  for i, r in up.results:
    doAssert r.ok, "upload " & $i & " failed: " & r.err.message
    result.add (up.items[i], r.receipt)

proc finalizeTimes(standin: Running): seq[float] =
  for r in standin.requestsTo("/finalize"):
    result.add r["time"].getFloat()

proc quickRetries(budgetMs = 30_000): ManagedRetryPolicy =
  ManagedRetryPolicy(maxAttempts: 5, initialDelayMs: 100, maxDelayMs: 400, budgetMs: budgetMs)

# The connection counter for "a refused connection is not repeated": this
# program's `connect`, which its own calls (std/net's among them) bind to
# ahead of the C library's. It counts the calls whose IPv4 destination port is
# `countedPort` and returns what the C library's `connect` returns.
{.emit: """
#include <dlfcn.h>
#include <netinet/in.h>
#include <sys/socket.h>
int ctTestCountedPort = 0;
int ctTestCountedConnects = 0;
int connect(int fd, const struct sockaddr *address, socklen_t length) {
  static int (*real)(int, const struct sockaddr *, socklen_t) = 0;
  if (real == 0) real = (int (*)(int, const struct sockaddr *, socklen_t))dlsym(RTLD_NEXT, "connect");
  if (address != 0 && address->sa_family == AF_INET && ctTestCountedPort != 0 &&
      ntohs(((const struct sockaddr_in *)address)->sin_port) == ctTestCountedPort)
    ctTestCountedConnects++;
  return real(fd, address, length);
}
""".}
var countedPort {.importc: "ctTestCountedPort", nodecl.}: cint
var countedConnects {.importc: "ctTestCountedConnects", nodecl.}: cint

var handledSignals: int
  ## SIGUSR1s this process handled; the handler only counts them.

proc countSignal(sig: cint) {.noconv.} =
  inc handledSignals

proc signalThisProcessAfter(ms: int): Process =
  ## A separate process sends this one SIGUSR1 `ms` milliseconds from now: a
  ## process-directed signal, as `kill` from a stop script is, delivered
  ## while the test's only thread waits in the request under test.
  startProcess("/bin/sh", args = ["-c", "sleep " & formatFloat(ms / 1000, ffDecimal, 3) &
    "; kill -USR1 " & $getCurrentProcessId()])

proc storedSha256(standin: Running, name, key: string): string =
  ## The SHA-256 of the object the local-storage route wrote.
  let path = work / ("local-" & name) / key
  if fileExists(path): sha256Hex(path) else: ""

try:
  suite "codetracer-ci sender routes":
    test "the bucket is private: an unsigned PUT to MinIO is refused":
      # Vacuity guard for the presigned arm: an object it stores got there
      # through the URL's signature, not through an open bucket.
      discard startStandin("guard", SliceUploadPresignedUrl)
      let client = newHttpClient(timeout = 10_000)
      let response = client.request(minioUrl & "/sender-guard/traces/x/unsigned.ct", httpMethod = HttpPut, body = "x")
      client.close()
      check response.code == Http403

    test "presigned-url: every object through slice-upload-url to MinIO, none through the local route":
      let standin = startStandin("presigned", SliceUploadPresignedUrl)
      let up = uploadTwoSlices(standin, "presigned")
      for r in up.results:
        check r.ok
        check r.err.message == ""
      var receipts: seq[(ManagedUploadObject, ManagedUploadReceipt)]
      for i, r in up.results:
        receipts.add (up.items[i], r.receipt)
        let parts = r.receipt.objectKey.split('/')
        check parts.len == 5 and parts[0] == "traces" and parts[1] == "tenant-routes" and parts[3] == "slices"
        check parts[4] == extractFilename(up.items[i].localPath)
      let issued = standin.requestsTo("/slice-upload-url")
      check issued.len == 2
      for r in issued:
        check r["authorization"].getBool()
      check standin.requestsTo("/local-storage/").len == 0
      var state = up.state
      let finalized = state.finalizeManagedUpload(up.backend, finalizeRequest(receipts))
      check finalized.ok
      # What the finalize read back from MinIO is each slice's bytes.
      let objects = standin.getJson("/_test/objects")
      for i, r in up.results:
        check objects[r.receipt.objectKey]["sha256"].getStr() == up.items[i].sha256
        check objects[r.receipt.objectKey]["length"].getInt() == int(up.items[i].contentLength)

    test "presigned-url: the object store's refusal is reported, not taken for an upload":
      # The stand-in issues URLs whose signature MinIO does not accept: the
      # upload fails naming the store's answer, and nothing is finalized.
      let standin = startStandin("badsig", SliceUploadPresignedUrl, badSignature = true)
      let up = uploadTwoSlices(standin, "badsig")
      for r in up.results:
        check not r.ok
        check "object store refused the presigned upload" in r.err.message
        check "HTTP 403" in r.err.message
      check standin.requestsTo("/local-storage/").len == 0

    test "local-storage: every object through the local-storage route, no slice-upload-url":
      let standin = startStandin("local", SliceUploadLocalStorage)
      let up = uploadTwoSlices(standin, "local")
      var receipts: seq[(ManagedUploadObject, ManagedUploadReceipt)]
      for i, r in up.results:
        check r.ok
        receipts.add (up.items[i], r.receipt)
      check standin.requestsTo("/local-storage/objects/").len == 2
      check standin.requestsTo("/slice-upload-url").len == 0
      var state = up.state
      check state.finalizeManagedUpload(up.backend, finalizeRequest(receipts)).ok

    test "a session that names no method takes the local-storage route":
      let standin = startStandin("absent", "absent")
      let up = uploadTwoSlices(standin, "absent")
      for r in up.results:
        check r.ok
      check standin.requestsTo("/local-storage/objects/").len == 2
      check standin.requestsTo("/slice-upload-url").len == 0

    test "an unknown method is refused, not retried, and nothing is written":
      let standin = startStandin("unknown", "carrier-pigeon")
      let up = uploadTwoSlices(standin, "unknown")
      for r in up.results:
        check not r.ok
        check not r.err.retryable
        check "carrier-pigeon" in r.err.message
      check standin.requestsTo("/local-storage/").len == 0
      check standin.requestsTo("/slice-upload-url").len == 0

    test "an issued key other than the sender's is refused, not retried":
      let standin = startStandin("wrongkey", SliceUploadPresignedUrl, wrongKey = true)
      let up = uploadTwoSlices(standin, "wrongkey")
      for r in up.results:
        check not r.ok
        check not r.err.retryable
        check "other-" in r.err.message
      check standin.requestsTo("/slice-upload-url").len == 2

  suite "codetracer-ci sender retries":
    test "a finalize slower than its timeout fails on one attempt (negative control)":
      # The `slow:3` step really outlasts a 1 s finalize timeout: without
      # retrying, the finalize fails, retryably, naming the timeout.
      let standin = startStandin("slow-once", SliceUploadPresignedUrl, finalizeScript = "slow:3")
      let up = uploadTwoSlices(standin, "slow-once", finalizeTimeoutMs = 1000)
      var state = up.state
      let finalized = state.tryFinalizeManagedUpload(up.backend, finalizeRequest(receiptsOf(up)), noManagedRetry())
      check not finalized.ok
      check finalized.err.retryable
      check "did not answer within 1000 ms" in finalized.err.message
      check standin.finalizeTimes().len == 1

    test "a finalize slower than its timeout is retried, and the retry finalizes":
      let standin = startStandin("slow-retry", SliceUploadPresignedUrl, finalizeScript = "slow:3")
      let up = uploadTwoSlices(standin, "slow-retry", finalizeTimeoutMs = 1000)
      var state = up.state
      let finalized = state.tryFinalizeManagedUpload(up.backend, finalizeRequest(receiptsOf(up)), quickRetries())
      check finalized.ok
      check finalized.err.message == ""
      check standin.finalizeTimes().len == 2
      check state.finalized
      # The retry's check read every slice back with the uploaded bytes.
      let objects = standin.getJson("/_test/objects")
      for i, r in up.results:
        check objects[r.receipt.objectKey]["sha256"].getStr() == up.items[i].sha256

    test "503 upload_session_busy and 409 upload_finalizing are retried, Retry-After honored":
      let standin = startStandin("busy", SliceUploadPresignedUrl, finalizeScript = "busy:2,finalizing")
      let up = uploadTwoSlices(standin, "busy")
      var state = up.state
      let finalized = state.tryFinalizeManagedUpload(up.backend, finalizeRequest(receiptsOf(up)), quickRetries())
      check finalized.ok
      let times = standin.finalizeTimes()
      check times.len == 3
      if times.len == 3:
        # The server's Retry-After (2 s) outweighs the policy's 100 ms; the
        # next pause is the policy's own (200 ms).
        check times[1] - times[0] >= 1.9
        check times[2] - times[1] < 1.5

    test "409 slice_content_mismatch is not retried":
      let standin = startStandin("mismatch", SliceUploadPresignedUrl, finalizeScript = "mismatch")
      let up = uploadTwoSlices(standin, "mismatch")
      var state = up.state
      let finalized = state.tryFinalizeManagedUpload(up.backend, finalizeRequest(receiptsOf(up)), quickRetries())
      check not finalized.ok
      check not finalized.err.retryable
      check "409 slice_content_mismatch" in finalized.err.message
      check standin.finalizeTimes().len == 1
      check not state.finalized

    test "the retries end within the budget":
      # Every finalize outlasts the 1 s timeout; with a 2.5 s budget no
      # attempt starts after 2.5 s, so the call returns within the budget
      # plus one attempt's timeout (and scheduling slack).
      let standin = startStandin("budget", SliceUploadPresignedUrl,
        finalizeScript = "slow:3,slow:3,slow:3,slow:3,slow:3,slow:3")
      let up = uploadTwoSlices(standin, "budget", finalizeTimeoutMs = 1000)
      var state = up.state
      let started = epochTime()
      let finalized = state.tryFinalizeManagedUpload(up.backend, finalizeRequest(receiptsOf(up)),
        ManagedRetryPolicy(maxAttempts: 10, initialDelayMs: 200, maxDelayMs: 200, budgetMs: 2500))
      let elapsed = epochTime() - started
      check not finalized.ok
      check finalized.err.retryable
      let attempts = standin.finalizeTimes().len
      check attempts >= 2 and attempts < 10
      check ("after " & $attempts & " attempts: ") in finalized.err.message
      check elapsed < 2.5 + 1.0 + 1.0
      check standin.finalizeTimes()[^1] - standin.finalizeTimes()[0] <= 2.5

    test "the finalize's derived timeout outlasts the former fixed 10 s":
      # No configured timeout and no retry: a finalize that answers after
      # 11 s still finalizes on its one attempt.
      let standin = startStandin("derived", SliceUploadPresignedUrl, finalizeScript = "slow:11")
      let up = uploadTwoSlices(standin, "derived")
      var state = up.state
      let request = finalizeRequest(receiptsOf(up))
      check up.backend.config.finalizeTimeoutMs(request) >= FinalizeBaseTimeoutMs
      let finalized = state.tryFinalizeManagedUpload(up.backend, request, noManagedRetry())
      check finalized.ok
      check standin.finalizeTimes().len == 1

    test "a transient refusal of a slice's upload URL is retried":
      let standin = startStandin("urlbusy", SliceUploadPresignedUrl, failUploadUrls = 2)
      var state = initManagedSenderState("routes-urlbusy")
      let backend = backendFor(standin)
      let item = sliceObject(sliceFile("urlbusy-slice_0000.ct", 5000), 0)
      let uploaded = state.tryUploadWithBackend(backend, item, quickRetries())
      check uploaded.ok
      check standin.requestsTo("/slice-upload-url").len == 3
      # Without retrying, the refusal is what the caller sees.
      let standin2 = startStandin("urlbusy-once", SliceUploadPresignedUrl, failUploadUrls = 1)
      var state2 = initManagedSenderState("routes-urlbusy-once")
      let once = state2.tryUploadWithBackend(backendFor(standin2), item, noManagedRetry())
      check not once.ok
      check once.err.retryable
      check "HTTP 503" in once.err.message
  suite "codetracer-ci sender under a handled signal":
    discard posix.signal(posix.SIGUSR1, countSignal)

    test "a slice write whose wait a signal interrupts is repeated, and the object is stored":
      # The stand-in answers the first write 2 s after it arrives; SIGUSR1
      # arrives 0.7 s into the upload, while the sender waits for that answer.
      let standin = startStandin("eintr-write", SliceUploadLocalStorage, slowLocalWrites = 2.0)
      var state = initManagedSenderState("routes-eintr-write")
      let item = sliceObject(sliceFile("eintr-write-slice_0000.ct", 5000), 0)
      handledSignals = 0
      let killer = signalThisProcessAfter(700)
      let uploaded = state.tryUploadWithBackend(backendFor(standin), item, noManagedRetry())
      discard killer.waitForExit()
      killer.close()
      # Vacuity guards: the signal was delivered and handled, and the write
      # it interrupted was the one sent first (a second write follows only
      # an interrupted one: there is no retry policy here).
      check handledSignals == 1
      check standin.requestsTo("/local-storage/objects/").len == 2
      check uploaded.ok
      check uploaded.err.message == ""
      check standin.storedSha256("eintr-write", uploaded.receipt.objectKey) == item.sha256

    test "a finalize whose wait a signal interrupts is repeated, and the repeat finalizes":
      let standin = startStandin("eintr-finalize", SliceUploadPresignedUrl, finalizeScript = "slow:2")
      let up = uploadTwoSlices(standin, "eintr-finalize")
      var state = up.state
      let request = finalizeRequest(receiptsOf(up))
      handledSignals = 0
      let killer = signalThisProcessAfter(700)
      let finalized = state.tryFinalizeManagedUpload(up.backend, request, noManagedRetry())
      discard killer.waitForExit()
      killer.close()
      check handledSignals == 1
      check standin.finalizeTimes().len == 2
      check finalized.ok
      check finalized.err.message == ""
      check state.finalized

  suite "codetracer-ci sender under a socket error":
    test "a refused connection is not repeated":
      # A port nothing listens on: bound, then closed.
      let probe = newSocket()
      probe.bindAddr(Port(0), "127.0.0.1")
      let refusedPort = probe.getLocalAddr()[1].int
      probe.close()
      var state = initManagedSenderState("routes-refused")
      let item = sliceObject(sliceFile("refused-slice_0000.ct", 5000), 0)
      let backend = newCodetracerCiSenderBackend(CodetracerCiSenderConfig(
        baseUrl: "http://127.0.0.1:" & $refusedPort, tenantId: "tenant-routes",
        bearerToken: Token, platform: "native", serviceName: "inventory", instanceId: "ct-mcr"))
      countedPort = cint(refusedPort)
      countedConnects = 0
      let uploaded = state.tryUploadWithBackend(backend, item, noManagedRetry())
      countedPort = 0
      check not uploaded.ok
      check "failed to create codetracer-ci upload session" in uploaded.err.message
      # One attempt; the counter is not vacuous: it saw that attempt.
      check countedConnects == 1

    test "a refused connection at session creation is not retried by the retry policy":
      # The same refused port under a retry policy: the session failure is
      # reported as not retryable, so the policy makes no second attempt (it
      # used to make five, pausing between them, for every object).
      let probe = newSocket()
      probe.bindAddr(Port(0), "127.0.0.1")
      let refusedPort = probe.getLocalAddr()[1].int
      probe.close()
      var state = initManagedSenderState("routes-refused-policy")
      let item = sliceObject(sliceFile("refused-policy-slice_0000.ct", 5000), 0)
      let backend = newCodetracerCiSenderBackend(CodetracerCiSenderConfig(
        baseUrl: "http://127.0.0.1:" & $refusedPort, tenantId: "tenant-routes",
        bearerToken: Token, platform: "native", serviceName: "inventory", instanceId: "ct-mcr"))
      countedPort = cint(refusedPort)
      countedConnects = 0
      let uploaded = state.tryUploadWithBackend(backend, item, quickRetries())
      countedPort = 0
      check not uploaded.ok
      check not uploaded.err.retryable
      check "failed to create codetracer-ci upload session" in uploaded.err.message
      check "attempts" notin uploaded.err.message
      check countedConnects == 1

  suite "codetracer-ci sender session retries":
    test "a 503 at session creation is retried, and the retry uploads":
      let standin = startStandin("sessbusy", SliceUploadPresignedUrl, failSessions = 2)
      var state = initManagedSenderState("routes-sessbusy")
      let item = sliceObject(sliceFile("sessbusy-slice_0000.ct", 5000), 0)
      let uploaded = state.tryUploadWithBackend(backendFor(standin), item, quickRetries())
      check uploaded.ok
      check standin.requestsTo("/upload-session").len == 3
      # Without retrying, the refusal is what the caller sees, as retryable.
      let standin2 = startStandin("sessbusy-once", SliceUploadPresignedUrl, failSessions = 1)
      var state2 = initManagedSenderState("routes-sessbusy-once")
      let once = state2.tryUploadWithBackend(backendFor(standin2), item, noManagedRetry())
      check not once.ok
      check once.err.retryable
      check "HTTP 503" in once.err.message
finally:
  stopAll()
