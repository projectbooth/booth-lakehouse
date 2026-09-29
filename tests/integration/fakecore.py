"""A stand-in for the parts of booth-core (and booth-storage's future ``s3`` provider) this module's
integration tests need — test tooling only, never deployed.

What it stands in for, and how faithfully:

- **An OIDC issuer** (discovery + JWKS, real RS256 signatures). ``POST /test/token`` mints a token for
  any subject/groups: the test-only substitute for a person logging in to Keycloak.
- **The gateway** (``/modules/lakehouse/*`` → the API container, prefix stripped, ``X-Workspace``
  validated against the token and forwarded as ``X-Booth-Workspace``/``X-Booth-Role`` — ADR 0025).
- **Workload-token minting** (``POST /api/internal/workload-tokens``, ADR 0058's request/response
  shape, role = min(ceiling, the owner's last-seen role)).
- **The ADR 0080 credential broker + booth-storage's ``s3`` provider**, at the PROVISIONAL shape
  ``booth_lakehouse.broker.HttpBroker`` speaks (docs/decisions/0001) — booth-core hasn't built its real
  one yet. Everything the contract requires is honoured here so the tests exercise it: the caller is
  authorized first (role in the workspace; ``readwrite`` needs editor), the TTL is mandatory and capped,
  every issuance goes to an append-only audit list without the credential value, and the credential is
  **real and really scoped**: a MinIO service account (static key, expiring) or an STS session
  (session token) whose policy covers exactly the requested prefix with exactly the requested access.
  So "a grant for table A can't read table B" is enforced by MinIO, not by this file.

Backends are per workspace, like booth-storage's registry: ``FAKECORE_BACKENDS`` maps
``workspace → backendId → {bucket, prefix}``.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import os
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3
import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from minio import MinioAdmin
from minio.credentials import StaticProvider

ISSUER = os.environ.get("FAKECORE_ISSUER", "http://fakecore:9090")
API = os.environ.get("FAKECORE_API_URL", "http://api:8080")
MINIO = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "booth-test")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "booth-test-secret")
MINT_CREDENTIAL = os.environ.get("FAKECORE_MINT_CREDENTIAL", "test-mint-credential")
BACKENDS = json.loads(os.environ.get("FAKECORE_BACKENDS", "{}"))
TTL_CEILING = int(os.environ.get("FAKECORE_TTL_CEILING", "3600"))

KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
KID = "fakecore-1"
RANK = {"owner": 3, "editor": 2, "viewer": 1}

AUDIT: list[dict] = []  # append-only issuance record: never a credential value
DIRECTORY: dict[str, dict[str, str]] = {}  # sub -> {workspace: role}, like core's user directory
_lock = threading.Lock()

admin = MinioAdmin(endpoint=MINIO, credentials=StaticProvider(MINIO_USER, MINIO_PASSWORD), secure=False)
sts = boto3.client("sts", endpoint_url=f"http://{MINIO}", aws_access_key_id=MINIO_USER, aws_secret_access_key=MINIO_PASSWORD, region_name="us-east-1")


def _b64(n: int) -> str:
    return base64.urlsafe_b64encode(n.to_bytes((n.bit_length() + 7) // 8, "big")).rstrip(b"=").decode()


def sign(sub: str, groups: list[str], ttl: int = 600) -> str:
    now = int(time.time())
    return jwt.encode({"iss": ISSUER, "sub": sub, "groups": groups, "iat": now, "exp": now + ttl}, KEY, algorithm="RS256", headers={"kid": KID})


def verify(token: str) -> dict:
    return jwt.decode(token, KEY.public_key(), algorithms=["RS256"], issuer=ISSUER, options={"verify_aud": False})


def role_of(claims: dict, workspace: str) -> str:
    best = ""
    for g in claims.get("groups", []):
        parts = g.split("/")
        if len(parts) == 4 and parts[1] == "workspaces" and parts[2] == workspace and RANK.get(parts[3], 0) > RANK.get(best, 0):
            best = parts[3]
    return best


def policy(bucket: str, prefix: str, access: str) -> dict:
    actions = ["s3:GetObject"] + (["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"] if access == "readwrite" else [])
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": actions, "Resource": [f"arn:aws:s3:::{bucket}/{prefix}/*"]},
            {"Effect": "Allow", "Action": ["s3:GetBucketLocation"], "Resource": [f"arn:aws:s3:::{bucket}"]},
            {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": [f"arn:aws:s3:::{bucket}"], "Condition": {"StringLike": {"s3:prefix": [prefix, f"{prefix}/*"]}}},
        ],
    }


def issue(claims: dict, workspace: str, body: dict) -> tuple[int, dict]:
    if body.get("kind") != "s3":
        return 400, {"error": "unknown_kind", "message": "only s3 here"}
    scope = body.get("scope") or {}
    backend_id, path, access = scope.get("backendId", ""), str(scope.get("path", "")).strip("/"), scope.get("access")
    role = role_of(claims, workspace)
    need = "editor" if access == "readwrite" else "viewer"
    if access not in ("read", "readwrite") or not path or ".." in path.split("/"):
        return 400, {"error": "bad_scope", "message": "scope needs backendId, a path and access read|readwrite"}
    if RANK.get(role, 0) < RANK[need]:
        return 403, {"error": "forbidden", "message": f"{access} needs {need}; caller has {role or 'no role'} in {workspace}"}
    backend = BACKENDS.get(workspace, {}).get(backend_id)
    if backend is None:
        return 404, {"error": "no_such_backend", "message": f"no backend {backend_id!r} in workspace {workspace!r}"}
    if backend.get("kind", "s3") != "s3":
        # The contract's "refuse rather than widen": e.g. a filesystem backend can't mint a
        # prefix-scoped s3 credential at all.
        return 422, {"error": "scope_not_supported", "message": f"backend {backend_id} can't issue an s3 credential scoped to a path"}
    ttl = int(body.get("ttlSeconds") or 0)
    if ttl <= 0:
        return 400, {"error": "ttl_required", "message": "ttlSeconds is mandatory"}
    ttl = min(ttl, TTL_CEILING)
    key_prefix = f"{backend['prefix'].strip('/')}/{path}".strip("/")
    pol = policy(backend["bucket"], key_prefix, access)
    static = (body.get("options") or {}).get("sessionToken") == "forbidden"
    if static:
        # MinIO refuses an expiring service account under 15 minutes (found, not assumed): the
        # provider's real floor, reported back through expiresAt rather than hidden.
        ttl = max(ttl, 905)
        exp = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=ttl)
        raw = json.loads(admin.add_service_account(policy=pol, expiration=exp.strftime("%Y-%m-%dT%H:%M:%SZ")))
        c = raw.get("credentials", raw)
        cred = {"accessKeyId": c["accessKey"], "secretAccessKey": c["secretKey"]}
        expires = exp.timestamp()
    else:
        ttl = max(ttl, 900)  # STS minimum
        r = sts.assume_role(RoleArn="arn:minio:iam:::role/booth", RoleSessionName="booth", Policy=json.dumps(pol), DurationSeconds=ttl)["Credentials"]
        cred = {"accessKeyId": r["AccessKeyId"], "secretAccessKey": r["SecretAccessKey"], "sessionToken": r["SessionToken"]}
        expires = r["Expiration"].timestamp()
    lease = str(uuid.uuid4())
    with _lock:
        AUDIT.append({"leaseId": lease, "subject": claims["sub"], "workspace": workspace, "kind": "s3", "scope": {"backendId": backend_id, "path": path, "access": access}, "expiresAt": expires, "static": static})
    cred.update({"endpoint": f"http://{MINIO}", "region": "us-east-1", "bucket": backend["bucket"], "keyPrefix": key_prefix, "pathStyle": True})
    return 201, {"leaseId": lease, "kind": "s3", "expiresAt": expires, "scope": {"backendId": backend_id, "path": path, "access": access}, "credential": cred}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # quiet
        pass

    def _send(self, status: int, body: dict | bytes, content_type: str = "application/json", headers: dict | None = None):
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    def _body(self) -> bytes:
        n = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(n) if n else b""

    def _bearer(self) -> str:
        auth = self.headers.get("Authorization", "")
        return auth[7:] if auth.lower().startswith("bearer ") else ""

    def do_GET(self):
        self._route()

    def do_POST(self):
        self._route()

    def do_PUT(self):
        self._route()

    def do_DELETE(self):
        self._route()

    def do_HEAD(self):
        self._route()

    def _route(self):
        p = self.path.split("?", 1)[0]
        if p == "/.well-known/openid-configuration":
            return self._send(200, {"issuer": ISSUER, "jwks_uri": f"{ISSUER}/jwks.json"})
        if p == "/jwks.json":
            n = KEY.public_key().public_numbers()
            return self._send(200, {"keys": [{"kty": "RSA", "kid": KID, "use": "sig", "alg": "RS256", "n": _b64(n.n), "e": _b64(n.e)}]})
        if p == "/healthz":
            return self._send(200, {"ok": True})
        if p == "/test/token" and self.command == "POST":
            b = json.loads(self._body())
            groups = b.get("groups", [])
            with _lock:
                for g in groups:
                    parts = g.split("/")
                    if len(parts) == 4:
                        DIRECTORY.setdefault(b["sub"], {})[parts[2]] = parts[3]
            return self._send(200, {"token": sign(b["sub"], groups, b.get("ttl", 600))})
        if p == "/test/audit":
            with _lock:
                return self._send(200, {"items": list(AUDIT)})
        if p == "/api/internal/workload-tokens" and self.command == "POST":
            if self._bearer() != MINT_CREDENTIAL:
                return self._send(401, {"error": "bad credential"})
            b = json.loads(self._body())
            owner_role = DIRECTORY.get(b.get("owner", ""), {}).get(b.get("workspace", ""), "")
            if not owner_role:
                return self._send(403, {"error": "not entitled"})
            role = min(owner_role, b.get("roleCeiling", "viewer"), key=lambda r: RANK[r])
            tok = sign(b["subject"], [f"/workspaces/{b['workspace']}/{role}"], 600)
            return self._send(200, {"token": tok, "tokenType": "Bearer", "expiresAt": int(time.time()) + 600, "role": role})
        if p == "/api/credentials" and self.command == "POST":
            try:
                claims = verify(self._bearer())
            except jwt.PyJWTError:
                return self._send(401, {"error": "unauthorized"})
            try:
                status, doc = issue(claims, self.headers.get("X-Workspace", ""), json.loads(self._body() or b"{}"))
            except Exception as e:  # noqa: BLE001 - surface provider failures as 502, like a real broker would
                return self._send(502, {"error": "provider_failed", "message": str(e)})
            return self._send(status, doc)
        if p.startswith("/modules/lakehouse/"):
            return self._gateway()
        return self._send(404, {"error": "not found"})

    def _gateway(self):
        token = self._bearer()
        ws = self.headers.get("X-Workspace", "")
        try:
            claims = verify(token)
        except jwt.PyJWTError:
            return self._send(401, {"error": "unauthorized"})
        role = role_of(claims, ws)
        if not role:
            return self._send(403, {"error": "no role in workspace"})
        target = API + self.path[len("/modules/lakehouse"):]
        req = urllib.request.Request(target, data=self._body() or None, method=self.command)
        for k in ("Authorization", "Content-Type", "Accept", "Idempotency-Key", "X-Iceberg-Access-Delegation"):
            if self.headers.get(k) is not None:
                req.add_header(k, self.headers[k])
        req.add_header("X-Booth-Workspace", ws)
        req.add_header("X-Booth-Role", role)
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                status, data, ctype, etag = resp.status, resp.read(), resp.headers.get("Content-Type", "application/json"), resp.headers.get("ETag")
        except urllib.error.HTTPError as e:
            status, data, ctype, etag = e.code, e.read(), e.headers.get("Content-Type", "application/json"), None
        return self._send(status, data, ctype, {"ETag": etag} if etag else None)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 9090), Handler).serve_forever()
