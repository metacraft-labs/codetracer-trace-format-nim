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
##   declares. The real codetracer-ci on a real MinIO, with a real recording
##   through this sender, is codetracer-observability-e2e's
##   `e2e_homeostate_recording_uploads_and_dive_in_resolves`; codetracer-ci's
##   own `ManagedUploadCiTokenTests` check the method each store names.
##
## A missing MinIO or python3 fails the test; it never passes without them.

import std/[json, os, osproc, httpclient, options, random, streams, strtabs, strutils, times, unittest]
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

proc startStandin(name, methodName: string, wrongKey = false, badSignature = false): Running =
  let portFile = work / (name & ".port")
  var args = @[standinScript, "--port-file", portFile,
    "--token", Token, "--method", methodName, "--minio-url", minioUrl,
    "--access-key", accessKey, "--secret-key", secretKey, "--bucket", "sender-" & name,
    "--local-root", work / ("local-" & name)]
  if wrongKey:
    args.add "--wrong-key"
  if badSignature:
    args.add "--bad-signature"
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

proc backendFor(standin: Running): CodetracerCiSenderBackend =
  newCodetracerCiSenderBackend(CodetracerCiSenderConfig(
    baseUrl: "http://127.0.0.1:" & $standin.port, tenantId: "tenant-routes",
    bearerToken: Token, platform: "native", serviceName: "inventory", instanceId: "ct-mcr"))

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

proc uploadTwoSlices(standin: Running, tag: string):
    tuple[state: ManagedSenderState, backend: CodetracerCiSenderBackend,
          items: seq[ManagedUploadObject], results: seq[tuple[ok: bool, receipt: ManagedUploadReceipt, err: ManagedSenderError]]] =
  result.backend = backendFor(standin)
  result.state = initManagedSenderState("routes-" & tag)
  # Sizes on both sides of MinIO's inline-data threshold (128 KiB).
  for i, size in [4093, 300_001]:
    let item = sliceObject(sliceFile(tag & "-slice_000" & $i & ".ct", size), i)
    result.items.add item
    result.results.add result.state.uploadWithBackend(result.backend, item)

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
finally:
  stopAll()
