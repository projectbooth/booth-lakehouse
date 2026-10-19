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
        self.fail_profile = False
        self.undrop_conflict = False
        self.dropped: dict[str, list[dict]] = {}

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
        parts = p.split("/")
        if len(parts) == 5 and p.startswith("/management/v1/warehouse/") and request.method == "GET":
            if self.fail_profile:
                return httpx.Response(503, json={"error": {"message": "Lakekeeper is unwell", "type": "ServiceUnavailable", "code": 503}})
            wh = self.warehouses[parts[4]]
            return httpx.Response(200, json={"warehouse-id": parts[4], "name": wh["warehouse-name"], "delete-profile": wh.get("delete-profile", {"type": "hard"})})
        if p.endswith("/delete-profile") and request.method == "POST":
            self.warehouses[parts[4]]["delete-profile"] = body["delete-profile"]
            return httpx.Response(200, json={})
        if p.endswith("/deleted-tabulars") and request.method == "GET":
            return httpx.Response(200, json={"tabulars": self.dropped.get(parts[4], []), "next-page-token": None})
        if p.endswith("/deleted-tabulars/undrop") and request.method == "POST":
            if self.undrop_conflict:
                return httpx.Response(409, json={"error": {"message": "Tabular with the same name already exists in the namespace", "type": "AlreadyExistsException", "code": 409}})
            ids = {t["id"] for t in body["targets"]}
            self.dropped[parts[4]] = [t for t in self.dropped.get(parts[4], []) if t["id"] not in ids]
            return httpx.Response(204)
        return httpx.Response(404, json={"error": {"message": "nope", "type": "NotFound", "code": 404}})

    def drop(self, warehouse_id: str, namespace: list[str], name: str) -> str:
        """Record a soft-deleted table the way Lakekeeper lists it."""
        tid = str(uuid.uuid4())
        self.dropped.setdefault(warehouse_id, []).append({
            "id": tid, "name": name, "namespace": namespace, "typ": "table", "warehouse-id": warehouse_id,
            "created-at": "2026-10-01T00:00:00Z", "deleted-at": "2026-10-19T12:00:00Z", "expiration-date": "2026-10-26T12:00:00Z",
        })
        return tid

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)
