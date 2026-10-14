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
       length and SHA-256 are the manifest's, 409 otherwise, as codetracer-ci's
       finalize checks them.

GET /_test/requests answers the requests seen (method, path, whether an
Authorization header came), GET /_test/objects what the finalize read back.

Every request but the test routes needs `Authorization: Bearer <--token>`.
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

        def problem(self, status: int, code: str) -> None:
            self.answer(status, {"status": status, "code": code})

        def body(self) -> bytes:
            return self.rfile.read(int(self.headers.get("content-length") or 0))

        def note(self) -> None:
            with store.lock:
                store.requests.append({
                    "method": self.command,
                    "path": urlparse(self.path).path,
                    "authorization": "authorization" in {k.lower() for k in self.headers.keys()},
                })

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
                if store.args.bad_signature:
                    url = url[:-4] + ("0000" if not url.endswith("0000") else "1111")
                self.answer(200, {"uploadUrl": url, "sliceIndex": request.get("sliceIndex"), "s3Key": key})
            # api/v1/traces/{s}/finalize
            elif parts[:3] == ["api", "v1", "traces"] and parts[4:] == ["finalize"]:
                session = store.sessions.get(parts[3])
                if session is None:
                    self.problem(404, "session_not_found")
                    return
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
                if ok:
                    self.answer(204)
                else:
                    self.problem(409, "finalize_failed")
            else:
                self.problem(404, "not_found")

        def do_PUT(self) -> None:
            self.note()
            path = urlparse(self.path).path
            marker = "/local-storage/objects/"
            data = self.body()
            if not self.authorized():
                return
            if marker not in path:
                self.problem(404, "not_found")
                return
            if store.args.method == "presigned-url":
                self.problem(403, "storage_policy_access_denied")
                return
            key = unquote(path.split(marker, 1)[1])
            target = Path(store.args.local_root) / key
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            self.answer(204)

    return Handler


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port-file", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--method", required=True)
    parser.add_argument("--minio-url", default="")
    parser.add_argument("--access-key", default="")
    parser.add_argument("--secret-key", default="")
    parser.add_argument("--bucket", default="")
    parser.add_argument("--local-root", default="")
    parser.add_argument("--wrong-key", action="store_true")
    parser.add_argument("--bad-signature", action="store_true")
    args = parser.parse_args()
    store = Store(args)
    if args.method == "presigned-url":
        # The bucket, created through a presigned PUT like every other call.
        request = Request(presign("PUT", args.minio_url, f"/{args.bucket}", args.access_key, args.secret_key),
                          method="PUT", data=b"")
        try:
            urlopen(request, timeout=30).close()
        except HTTPError as error:
            if error.code != 409:  # BucketAlreadyOwnedByYou
                raise
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(store))
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
