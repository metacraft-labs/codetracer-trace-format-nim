#!/usr/bin/env python3
"""A stand-in for codetracer-ci's managed upload routes, writing to a real MinIO.

Used by tests/test_managed_sender_ci_routes.nim to drive the real sender
(src/codetracer_ctfs/managed_sender_ci.nim) through every route an upload
session can name.  It serves the routes as codetracer-ci documents them
(codetracer-ci rewrite-docs/04-apis-events/http-api.md §4.4.1):

  POST /api/v1/tenants/{t}/traces/upload-session
       answers sessionId, s3KeyPrefix (traces/{t}/{s}/slices/), no storage
       pool, and `sliceUploadMethod` (--method; `absent` leaves the field out,
       as a codetracer-ci from before the field did);
  POST /api/v1/traces/{s}/slice-upload-url
       builds the key {s3KeyPrefix}{fileName} and answers a presigned PUT URL
       for it on the MinIO (AWS Signature Version 4, query form:
       https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html),
       or on the local store a file:// path, as codetracer-ci's local file
       system provider does;
  PUT  /api/v1/observability/storage-policy/tenants/{t}/local-storage/objects/{key}
       writes the object into the local store; on the S3 store it answers
       403 storage_policy_access_denied, as codetracer-ci does for a tenant
       with no recorder storage credential (http-api.md §4.4.1);
  POST /api/v1/traces/{s}/finalize
       reads every slice the manifest names back from the store (MinIO
       through a presigned GET) and answers 204 only when each object's
       length and SHA-256 are the manifest's, 409 slice_content_mismatch
       otherwise, as codetracer-ci's finalize checks them.

--fail-upload-urls N answers the first N slice-upload-url calls
503 upload_session_busy (a transient refusal the sender retries).

--fail-sessions N answers the first N upload-session calls 503
upload_session_busy (a transient refusal the sender retries).

--slow-local-writes SECONDS answers the first local-storage write that long
after it arrives (the object is written either way): a request the sender
is still waiting on, for the tests that interrupt that wait with a signal.

--finalize-script scripts the first finalize calls, one comma-separated step
per call, consumed in order; once it is used up, every finalize checks the
store as above.  Steps: `slow:<seconds>` checks the store and answers after
that long (a finalize slower than the sender's timeout: the sender has
disconnected by then, and codetracer-ci then cancels the check, so this one
answers nothing it would act on); `busy:<seconds>` answers
503 upload_session_busy with `Retry-After: <seconds>`; `finalizing` answers
409 upload_finalizing; `mismatch` answers 409 slice_content_mismatch without
checking (http-api.md, "POST /api/v1/traces/{sessionId}/finalize").

--redirect ROUTE=STATUS answers every call of one route with that 3xx status
and `Location: <--redirect-to><request path>` instead of serving it, for the
tests that check the sender follows no redirect (RFC 9110 §15.4,
https://www.rfc-editor.org/rfc/rfc9110#section-15.4).  ROUTE is `session`,
`upload-url`, `local` (the local-storage write) or `finalize`, which are
codetracer-ci answering, or `store`, the object store answering the
presigned write: slice-upload-url then answers a URL on this stand-in,
`/_store/{key}`, whose PUT is answered with the redirect (MinIO cannot be
made to redirect a presigned write on its own endpoint).  Repeatable.

--sink runs a redirect target instead of the routes: it answers every
request 200 with `{}`, which a followed redirect would take for success,
and records each one (method, path, whether an Authorization header came,
whether it carried --token, the body's length).

GET /_test/requests answers the requests seen (method, path, whether an
Authorization header came), GET /_test/objects what the finalize read back.

Every request but the test routes and the stand-in object store's
`/_store/` writes (a presigned write carries no Authorization header) needs
`Authorization: Bearer <--token>`; the --sink asks for none.
--wrong-key makes slice-upload-url answer a key other than the one the
route derives (a server the sender must not trust blindly); --bad-signature
makes it answer a URL whose signature the store refuses.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import hmac
import json
import os
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote, unquote, urlparse
from urllib.request import Request, urlopen

REGION = "us-east-1"


def _sign(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode(), hashlib.sha256).digest()


def presign(method: str, endpoint: str, path: str, access: str, secret: str, expires: int = 300) -> str:
    """A SigV4 query-string presigned URL for `method` on `path` at `endpoint`."""
    host = urlparse(endpoint).netloc
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    scope = f"{day}/{REGION}/s3/aws4_request"
    query = {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{access}/{scope}",
        "X-Amz-Date": amz_date,
        "X-Amz-Expires": str(expires),
        "X-Amz-SignedHeaders": "host",
    }
    canonical_query = "&".join(
        f"{quote(k, safe='-_.~')}={quote(v, safe='-_.~')}" for k, v in sorted(query.items()))
    canonical_uri = quote(path, safe="/-_.~")
    canonical_request = "\n".join(
        [method, canonical_uri, canonical_query, f"host:{host}", "", "host", "UNSIGNED-PAYLOAD"])
    string_to_sign = "\n".join(
        ["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical_request.encode()).hexdigest()])
    key = _sign(_sign(_sign(_sign(("AWS4" + secret).encode(), day), REGION), "s3"), "aws4_request")
    signature = hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    return f"{endpoint}{canonical_uri}?{canonical_query}&X-Amz-Signature={signature}"


class Store:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.lock = threading.Lock()
        self.requests: list[dict] = []
        self.sessions: dict[str, dict] = {}
        self.read_back: dict[str, dict] = {}
        self.finalize_script: list[str] = [
            step for step in (args.finalize_script or "").split(",") if step]
        self.redirects: dict[str, int] = {}
        for spec in args.redirect:
            route, _, status = spec.partition("=")
            self.redirects[route] = int(status)

    def s3_url(self, method: str, key: str) -> str:
        a = self.args
        return presign(method, a.minio_url, f"/{a.bucket}/{key}", a.access_key, a.secret_key)

    def read_object(self, key: str) -> bytes | None:
        if self.args.method == "presigned-url":
            try:
                with urlopen(self.s3_url("GET", key), timeout=30) as response:
                    return response.read()
            except HTTPError:
                return None
        path = Path(self.args.local_root) / key
        return path.read_bytes() if path.is_file() else None


def make_handler(store: Store):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args) -> None:  # quiet
            return

        def answer(self, status: int, body: object | None = None) -> None:
            raw = b"" if body is None else json.dumps(body).encode()
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def problem(self, status: int, code: str, headers: dict[str, str] | None = None) -> None:
            raw = json.dumps({"status": status, "code": code}).encode()
            self.send_response(status)
            self.send_header("content-type", "application/problem+json")
            self.send_header("content-length", str(len(raw)))
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(raw)

        def body(self) -> bytes:
            return self.rfile.read(int(self.headers.get("content-length") or 0))

        def note(self) -> None:
            with store.lock:
                store.requests.append({
                    "method": self.command,
                    "path": urlparse(self.path).path,
                    "authorization": "authorization" in {k.lower() for k in self.headers.keys()},
                    "time": time.monotonic(),
                })

        def redirected(self, route: str) -> bool:
            """Answers the request with the redirect --redirect names for
            `route`, if any; True when it did."""
            status = store.redirects.get(route)
            if status is None:
                return False
            self.send_response(status)
            self.send_header("location", store.args.redirect_to + self.path)
            self.send_header("content-length", "0")
            self.end_headers()
            return True

        def authorized(self) -> bool:
            if self.headers.get("authorization") == f"Bearer {store.args.token}":
                return True
            self.problem(401, "unauthorized")
            return False

        def do_GET(self) -> None:
            path = urlparse(self.path).path
            if path == "/_test/requests":
                with store.lock:
                    self.answer(200, store.requests)
            elif path == "/_test/objects":
                with store.lock:
                    self.answer(200, store.read_back)
            else:
                self.problem(404, "not_found")

        def do_POST(self) -> None:
            self.note()
            path = urlparse(self.path).path
            parts = path.strip("/").split("/")
            data = self.body()
            if not self.authorized():
                return
            # api/v1/tenants/{t}/traces/upload-session
            if parts[:3] == ["api", "v1", "tenants"] and parts[4:] == ["traces", "upload-session"]:
                if self.redirected("session"):
                    return
                with store.lock:
                    refuse = store.args.fail_sessions > 0
                    if refuse:
                        store.args.fail_sessions -= 1
                if refuse:
                    self.problem(503, "upload_session_busy")
                    return
                tenant, session = parts[3], str(uuid.uuid4())
                prefix = f"traces/{tenant}/{session}/slices/"
                with store.lock:
                    store.sessions[session] = {"tenant": tenant, "prefix": prefix}
                answer = {"sessionId": session, "recordingId": session, "artifactId": session,
                          "s3KeyPrefix": prefix, "storagePoolId": None, "storageServerId": None,
                          "storageEndpointUri": None, "storageProviderKind": "managed-storage"}
                if store.args.method != "absent":
                    answer["sliceUploadMethod"] = store.args.method
                self.answer(200, answer)
            # api/v1/traces/{s}/slice-upload-url
            elif parts[:3] == ["api", "v1", "traces"] and parts[4:] == ["slice-upload-url"]:
                if self.redirected("upload-url"):
                    return
                with store.lock:
                    refuse = store.args.fail_upload_urls > 0
                    if refuse:
                        store.args.fail_upload_urls -= 1
                if refuse:
                    self.problem(503, "upload_session_busy")
                    return
                session = store.sessions.get(parts[3])
                request = json.loads(data or b"{}")
                name = str(request.get("fileName", ""))
                if session is None:
                    self.problem(404, "session_not_found")
                    return
                if not name or "/" in name or name in (".", "..") or "%" in name:
                    self.problem(400, "invalid_file_name")
                    return
                key = session["prefix"] + name
                if store.args.wrong_key:
                    key = session["prefix"] + "other-" + name
                url = (store.s3_url("PUT", key) if store.args.method == "presigned-url"
                       else Path(store.args.local_root, key).as_uri())
                if "store" in store.redirects:
                    url = f"http://127.0.0.1:{self.server.server_address[1]}/_store/{key}"
                if store.args.bad_signature:
                    url = url[:-4] + ("0000" if not url.endswith("0000") else "1111")
                self.answer(200, {"uploadUrl": url, "sliceIndex": request.get("sliceIndex"), "s3Key": key})
            # api/v1/traces/{s}/finalize
            elif parts[:3] == ["api", "v1", "traces"] and parts[4:] == ["finalize"]:
                if self.redirected("finalize"):
                    return
                session = store.sessions.get(parts[3])
                if session is None:
                    self.problem(404, "session_not_found")
                    return
                with store.lock:
                    step = store.finalize_script.pop(0) if store.finalize_script else ""
                kind, _, arg = step.partition(":")
                if kind == "busy":
                    self.problem(503, "upload_session_busy", {"retry-after": arg})
                    return
                if kind == "finalizing":
                    self.problem(409, "upload_finalizing")
                    return
                if kind == "mismatch":
                    self.problem(409, "slice_content_mismatch")
                    return
                if kind == "slow":
                    time.sleep(float(arg))
                manifest = json.loads(data)["recordingManifest"]
                ok = bool(manifest.get("mcrSlices"))
                for entry in manifest.get("mcrSlices", []):
                    key = entry["sliceKey"]
                    stored = store.read_object(key)
                    seen = None if stored is None else {
                        "length": len(stored), "sha256": hashlib.sha256(stored).hexdigest()}
                    with store.lock:
                        store.read_back[key] = seen
                    if (not key.startswith(session["prefix"]) or seen is None
                            or seen["length"] != entry.get("contentLength")
                            or f"sha256:{seen['sha256']}" != entry.get("contentHash")):
                        ok = False
                try:
                    if ok:
                        self.answer(204)
                    else:
                        self.problem(409, "slice_content_mismatch")
                except (BrokenPipeError, ConnectionResetError):
                    pass  # the sender gave up on this one (a `slow` step)
            else:
                self.problem(404, "not_found")

        def do_PUT(self) -> None:
            self.note()
            path = urlparse(self.path).path
            marker = "/local-storage/objects/"
            data = self.body()
            if path.startswith("/_store/"):
                # The object store's side: a presigned write carries no
                # Authorization header, so none is asked for.
                if not self.redirected("store"):
                    self.problem(404, "not_found")
                return
            if not self.authorized():
                return
            if marker not in path:
                self.problem(404, "not_found")
                return
            if self.redirected("local"):
                return
            if store.args.method == "presigned-url":
                self.problem(403, "storage_policy_access_denied")
                return
            key = unquote(path.split(marker, 1)[1])
            target = Path(store.args.local_root) / key
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            with store.lock:
                delay = store.args.slow_local_writes
                store.args.slow_local_writes = 0.0
            if delay > 0:
                time.sleep(delay)
            try:
                self.answer(204)
            except (BrokenPipeError, ConnectionResetError):
                pass  # the sender gave up waiting on this one

    return Handler


def make_sink_handler(store: Store):
    """The --sink: where a followed redirect would land."""
    class Sink(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args) -> None:  # quiet
            return

        def answer(self, raw: bytes) -> None:
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def any_method(self) -> None:
            path = urlparse(self.path).path
            if self.command == "GET" and path == "/_test/requests":
                with store.lock:
                    self.answer(json.dumps(store.requests).encode())
                return
            data = self.rfile.read(int(self.headers.get("content-length") or 0))
            with store.lock:
                store.requests.append({
                    "method": self.command,
                    "path": path,
                    "authorization": "authorization" in {k.lower() for k in self.headers.keys()},
                    "token": store.args.token in (self.headers.get("authorization") or ""),
                    "length": len(data),
                    "time": time.monotonic(),
                })
            self.answer(b"{}")

        do_GET = do_POST = do_PUT = any_method

    return Sink


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port-file", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--method", default="")
    parser.add_argument("--minio-url", default="")
    parser.add_argument("--access-key", default="")
    parser.add_argument("--secret-key", default="")
    parser.add_argument("--bucket", default="")
    parser.add_argument("--local-root", default="")
    parser.add_argument("--wrong-key", action="store_true")
    parser.add_argument("--bad-signature", action="store_true")
    parser.add_argument("--finalize-script", default="")
    parser.add_argument("--fail-upload-urls", type=int, default=0)
    parser.add_argument("--fail-sessions", type=int, default=0)
    parser.add_argument("--slow-local-writes", type=float, default=0.0)
    parser.add_argument("--redirect", action="append", default=[])
    parser.add_argument("--redirect-to", default="")
    parser.add_argument("--sink", action="store_true")
    args = parser.parse_args()
    if not args.sink and not args.method:
        parser.error("--method is required unless --sink is given")
    store = Store(args)
    if args.method == "presigned-url" and not args.sink:
        # The bucket, created through a presigned PUT like every other call.
        request = Request(presign("PUT", args.minio_url, f"/{args.bucket}", args.access_key, args.secret_key),
                          method="PUT", data=b"")
        try:
            urlopen(request, timeout=30).close()
        except HTTPError as error:
            if error.code != 409:  # BucketAlreadyOwnedByYou
                raise
    handler = make_sink_handler(store) if args.sink else make_handler(store)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    tmp = args.port_file + ".tmp"
    Path(tmp).write_text(str(server.server_address[1]))
    os.replace(tmp, args.port_file)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
