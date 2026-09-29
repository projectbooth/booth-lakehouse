"""In-process fakes for unit tests: a broker that records what it was asked, and a Lakekeeper
management API (httpx MockTransport) that records calls. The real ones are exercised in
tests/integration."""

from __future__ import annotations

import json
import time
import uuid

import httpx

from booth_lakehouse.broker import Broker, BrokerError, S3Grant


def grant(backend="lake", path="lakehouse", access="readwrite", ttl=3600, session_token="", prefix="acme-data") -> S3Grant:
    return S3Grant(
        lease_id=str(uuid.uuid4()), backend_id=backend, path=path, access=access, expires_at=time.time() + ttl,
        endpoint="http://minio:9000", region="us-east-1", bucket="lake", key_prefix=f"{prefix}/{path}".strip("/"),
        path_style=True, access_key_id="AKIA-" + uuid.uuid4().hex[:6], secret_access_key="SECRET-" + uuid.uuid4().hex,
        session_token=session_token,
    )


class FakeBroker(Broker):
    def __init__(self, fail: BrokerError | None = None, session_token: str = "") -> None:
        self.calls: list[dict] = []
        self.fail = fail
        self.session_token = session_token
        self.issued: list[S3Grant] = []

    def issue_s3(self, workspace, backend_id, path, access, ttl_seconds, static_key=False):
        self.calls.append({"workspace": workspace, "backend": backend_id, "path": path, "access": access, "ttl": ttl_seconds, "static": static_key})
        if self.fail:
            raise self.fail
        g = grant(backend_id, path, access, ttl_seconds, "" if static_key else self.session_token)
        self.issued.append(g)
        return g


class FakeLakekeeper:
    def __init__(self) -> None:
        self.warehouses: dict[str, dict] = {}
        self.calls: list[tuple[str, str, dict | None]] = []
        self.bootstrapped = False
        self.fail_create: tuple[int, str] | None = None

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        self.calls.append((request.method, request.url.path, body))
        p = request.url.path
        if p == "/health":
            return httpx.Response(200, json={"health": "ok"})
        if p == "/management/v1/info":
            return httpx.Response(200, json={"bootstrapped": self.bootstrapped})
        if p == "/management/v1/bootstrap":
            self.bootstrapped = True
            return httpx.Response(204)
        if p == "/management/v1/warehouse" and request.method == "GET":
            return httpx.Response(200, json={"warehouses": [{"warehouse-id": k, "name": v["warehouse-name"]} for k, v in self.warehouses.items()]})
        if p == "/management/v1/warehouse" and request.method == "POST":
            if self.fail_create:
                status, kind = self.fail_create
                return httpx.Response(status, json={"error": {"message": kind, "type": kind, "code": status}})
            wid = str(uuid.uuid4())
            self.warehouses[wid] = body
            return httpx.Response(201, json={"warehouse-id": wid})
        if p.endswith("/storage-credential"):
            wid = p.split("/")[4]
            self.warehouses[wid]["storage-credential"] = body["new-storage-credential"]
            return httpx.Response(200, json={})
        return httpx.Response(404, json={"error": {"message": "nope", "type": "NotFound", "code": 404}})

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)
