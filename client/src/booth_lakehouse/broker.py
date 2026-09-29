"""The consumer side of the ADR 0080 credential broker, for the ``s3`` kind — the only place in this
module that knows the broker's wire shape.

**The wire shape here is provisional.** ``contracts/credential-broker.md`` fixes the broker's hard
requirements (authorize first, mandatory short TTL, never log the value, refuse rather than widen)
but leaves the request/response shape to booth-core, which hadn't built it when this was written
(docs/decisions/0001). Everything below the ``HttpBroker`` class works with ``S3Grant`` only, so
adopting core's real shape means changing ``HttpBroker._request``/``_parse`` and nothing else.

A grant carries its own resolved location (endpoint, bucket, key prefix): the provider
(booth-storage) is the only thing that knows how a ``{backendId, path}`` pair (ADR 0045) maps onto a
real bucket, so nothing here constructs that mapping itself.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

READ, READWRITE = "read", "readwrite"


class BrokerError(Exception):
    """The broker (or the provider behind it) refused or failed. ``status`` is the HTTP status when
    there was one; ``scope_unsupported`` is the contract's "can't scope this narrowly" refusal."""

    def __init__(self, message: str, status: int = 0, scope_unsupported: bool = False) -> None:
        super().__init__(message)
        self.status = status
        self.scope_unsupported = scope_unsupported


@dataclass(frozen=True)
class S3Grant:
    """One issued, short-lived, scoped object-storage credential. ``repr`` never shows the secret:
    a grant ends up in tracebacks and debugger output, and the contract says the value is never
    logged anywhere."""

    lease_id: str
    backend_id: str
    path: str
    access: str
    expires_at: float
    endpoint: str
    region: str
    bucket: str
    key_prefix: str
    path_style: bool
    access_key_id: str
    secret_access_key: str = field(repr=False)
    session_token: str = field(default="", repr=False)

    @property
    def root_uri(self) -> str:
        """The ``s3://`` URI this grant covers — used only to tell whether a table location falls
        inside it, never to reach storage without the grant."""
        prefix = self.key_prefix.strip("/")
        return f"s3://{self.bucket}/{prefix}" if prefix else f"s3://{self.bucket}"

    def covers(self, uri: str) -> bool:
        root = self.root_uri
        return uri == root or uri.startswith(root + "/")

    def expires_within(self, seconds: float, now: float | None = None) -> bool:
        return (now if now is not None else time.time()) >= self.expires_at - seconds

    def fileio_properties(self) -> dict[str, str]:
        """PyIceberg FileIO properties for this grant (and nothing else)."""
        props = {
            "s3.endpoint": self.endpoint,
            "s3.region": self.region,
            "s3.access-key-id": self.access_key_id,
            "s3.secret-access-key": self.secret_access_key,
            "s3.path-style-access": "true" if self.path_style else "false",
        }
        if self.session_token:
            props["s3.session-token"] = self.session_token
        return props


class Broker:
    """What this module needs from ADR 0080: an ``s3`` credential for one ``{backendId, path}``."""

    def issue_s3(
        self,
        workspace: str,
        backend_id: str,
        path: str,
        access: str,
        ttl_seconds: int,
        static_key: bool = False,
    ) -> S3Grant:  # pragma: no cover - interface
        """``static_key=True`` asks for a key pair with no session token. Lakekeeper can only hold
        that form as a warehouse credential (its API has no session-token field), so the provider
        must either issue it or refuse — see docs/decisions/0001."""
        raise NotImplementedError


class HttpBroker(Broker):
    """booth-core's broker over HTTP, authenticated as the caller (its own bearer token), so the
    broker's audit trail names who actually asked (ADR 0080)."""

    def __init__(self, url: str, token: Callable[[], str], opener=None, timeout: float = 30) -> None:
        if not url:
            raise ValueError("no credential broker URL is configured")
        self._url = url
        self._token = token
        self._open = opener or urllib.request.urlopen
        self._timeout = timeout

    def issue_s3(self, workspace, backend_id, path, access, ttl_seconds, static_key=False) -> S3Grant:
        if access not in (READ, READWRITE):
            raise ValueError(f"unknown access mode {access!r}")
        body = {
            "kind": "s3",
            "scope": {"backendId": backend_id, "path": path, "access": access},
            "ttlSeconds": int(ttl_seconds),
            "options": {"sessionToken": "forbidden" if static_key else "allowed"},
        }
        doc = self._request(workspace, body)
        return _parse(doc, backend_id, path, access)

    def _request(self, workspace: str, body: dict) -> dict:
        req = urllib.request.Request(self._url, data=json.dumps(body).encode(), method="POST")
        req.add_header("Authorization", "Bearer " + self._token())
        req.add_header("X-Workspace", workspace)
        req.add_header("Content-Type", "application/json")
        try:
            with self._open(req, timeout=self._timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            detail, code = _error_detail(e)
            unsupported = e.code == 422 and code == "scope_not_supported"
            raise BrokerError(f"credential broker refused an s3 credential (HTTP {e.code}): {detail}", e.code, unsupported) from None
        except (urllib.error.URLError, OSError) as e:
            raise BrokerError(f"couldn't reach the credential broker: {e}") from None


def _error_detail(e: urllib.error.HTTPError) -> tuple[str, str]:
    try:
        doc = json.loads(e.read().decode("utf-8", "replace"))
    except (ValueError, OSError):
        return "", ""
    if not isinstance(doc, dict):
        return "", ""
    return str(doc.get("message") or doc.get("error") or ""), str(doc.get("error") or "")


def _parse(doc: dict, backend_id: str, path: str, access: str) -> S3Grant:
    """Turn a broker response into a grant, refusing anything that isn't exactly what was asked
    for: a provider must refuse rather than widen (credential-broker.md), and this side checks the
    echo rather than trusting it blindly."""
    try:
        cred = doc["credential"]
        scope = doc.get("scope") or {}
        if doc.get("kind") != "s3":
            raise BrokerError(f"broker returned a {doc.get('kind')!r} credential for an s3 request")
        if (scope.get("backendId"), _norm(scope.get("path", "")), scope.get("access")) != (backend_id, _norm(path), access):
            raise BrokerError("broker returned a credential for a different scope than requested; refusing to use it")
        return S3Grant(
            lease_id=str(doc["leaseId"]),
            backend_id=backend_id,
            path=_norm(path),
            access=access,
            expires_at=_timestamp(doc["expiresAt"]),
            endpoint=str(cred["endpoint"]),
            region=str(cred.get("region") or "us-east-1"),
            bucket=str(cred["bucket"]),
            key_prefix=str(cred.get("keyPrefix") or "").strip("/"),
            path_style=bool(cred.get("pathStyle", True)),
            access_key_id=str(cred["accessKeyId"]),
            secret_access_key=str(cred["secretAccessKey"]),
            session_token=str(cred.get("sessionToken") or ""),
        )
    except (KeyError, TypeError, ValueError) as e:
        raise BrokerError(f"malformed credential-broker response (missing or invalid {e})") from None


def _norm(path: str) -> str:
    return str(path).strip("/")


def _timestamp(value) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
