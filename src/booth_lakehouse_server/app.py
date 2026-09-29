"""booth-lakehouse's HTTP surface (reached through booth-core's gateway at ``/modules/lakehouse``).

- ``GET  /health``                     — the manifest's health path: 200 when Lakekeeper answers.
- ``GET  /api/warehouse``              — this workspace's warehouse (any role), 404 if none yet.
- ``PUT  /api/warehouse``              — owner: create it at ``{backendId, path}`` (ADR 0045).
- ``GET  /api/tables``                 — every table in this workspace's warehouse.
- ``GET  /api/tables/{ns}/{table}``    — one table: schema, location as ``{backendId, path}``,
                                         snapshots. This is the shape proposed for booth-catalog
                                         (docs/decisions/0002) — nothing is registered there yet.
- ``*    /iceberg/v1/...``             — the Iceberg REST catalog, authorized per ``proxy.py`` and
                                         forwarded to Lakekeeper.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import quote

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from booth_lakehouse.broker import Broker, HttpBroker

from . import proxy
from .config import Settings
from .identity import AuthError, Forbidden, Identity, Verifier, resolve
from .lakekeeper import Lakekeeper
from .store import MemoryStore, PostgresStore, Store
from .warehouses import WarehouseError, Warehouses, WorkloadTokens

log = logging.getLogger("booth_lakehouse")

# Request headers passed on to Lakekeeper. Everything else — Authorization, cookies, X-Booth-*,
# X-Iceberg-Access-Delegation — stops here.
_FORWARD_REQUEST = ("content-type", "accept", "idempotency-key", "user-agent", "x-client-version", "x-client-git-commit-short")
_FORWARD_RESPONSE = ("content-type", "etag")


@dataclass
class Components:
    verifier: Verifier
    store: Store
    lakekeeper: Lakekeeper
    warehouses: Warehouses
    renew_interval_seconds: int = 60


def components_from_settings(s: Settings) -> Components:
    if not s.issuers:
        raise ValueError("no trusted token issuer configured (BOOTH_OIDC_ISSUER_URL / BOOTH_WORKLOAD_ISSUER_URL)")
    if s.database_dsn:
        store: Store = PostgresStore(s.database_dsn)
    else:
        log.warning("DATABASE_DSN is empty: warehouse bindings are kept in memory and lost on restart (development only)")
        store = MemoryStore()
    lk = Lakekeeper(s.lakekeeper_url)
    broker_for: Callable[[Callable[[], str]], Broker] | None = (lambda tok: HttpBroker(s.broker_url, tok)) if s.broker_url else None
    minter = WorkloadTokens(s.workload_mint_url, s.workload_mint_credential) if s.workload_mint_url and s.workload_mint_credential else None
    wh = Warehouses(store, lk, broker_for, minter, s.warehouse_ttl_seconds, s.renew_margin_seconds)
    return Components(Verifier(s.issuers, s.groups_claim), store, lk, wh, s.renew_interval_seconds)


class WarehouseRequest(BaseModel):
    backendId: str
    path: str


def _iceberg_error(status: int, message: str) -> JSONResponse:
    kind = {400: "BadRequestException", 401: "NotAuthorizedException", 403: "ForbiddenException", 404: "NoSuchWarehouseException"}.get(status, "ServiceFailureException")
    return JSONResponse({"error": {"message": message, "type": kind, "code": status}}, status_code=status)


def create_app(c: Components, run_renewals: bool = True, catalog_transport: httpx.AsyncBaseTransport | None = None) -> FastAPI:
    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        task = asyncio.create_task(_renew_loop(c)) if run_renewals else None
        yield
        if task:
            task.cancel()

    app = FastAPI(title="booth-lakehouse", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    catalog = httpx.AsyncClient(base_url=c.lakekeeper.base_url + "/catalog", timeout=120, transport=catalog_transport)

    def identity(request: Request) -> Identity:
        auth = request.headers.get("authorization", "")
        if not auth.lower().startswith("bearer "):
            raise HTTPException(401, "a bearer token is required")
        token = auth[7:].strip()
        workspace = request.headers.get("x-booth-workspace") or request.headers.get("x-workspace", "")
        try:
            claims = c.verifier.verify(token)
            return resolve(claims, token, workspace, request.headers.get("x-booth-role", ""))
        except AuthError as e:
            raise HTTPException(401, str(e)) from None
        except Forbidden as e:
            raise HTTPException(403, str(e)) from None

    @app.get("/health")
    def health():
        ok = c.lakekeeper.healthy()
        return JSONResponse({"status": "ok" if ok else "unavailable", "lakekeeper": "ok" if ok else "unreachable"}, status_code=200 if ok else 503)

    @app.get("/api/warehouse")
    def get_warehouse(who: Identity = Depends(identity)):
        b = c.warehouses.get(who.workspace)
        if b is None:
            raise HTTPException(404, f"workspace {who.workspace!r} has no warehouse yet")
        return b.public()

    @app.put("/api/warehouse", status_code=201)
    def put_warehouse(body: WarehouseRequest, who: Identity = Depends(identity)):
        try:
            return c.warehouses.create(who, body.backendId, body.path).public()
        except Forbidden as e:
            raise HTTPException(403, str(e)) from None
        except WarehouseError as e:
            raise HTTPException(e.status, str(e)) from None

    def _binding(who: Identity):
        b = c.warehouses.get(who.workspace)
        if b is None:
            raise HTTPException(404, f"workspace {who.workspace!r} has no warehouse yet")
        return b

    async def _lk_json(path: str):
        resp = await catalog.get(path)
        if resp.status_code == 404:
            raise HTTPException(404, "not found")
        if resp.status_code >= 400:
            raise HTTPException(502, f"Lakekeeper returned {resp.status_code}")
        return resp.json()

    @app.get("/api/tables")
    async def list_tables(who: Identity = Depends(identity)):
        b = _binding(who)
        out = []
        nss = await _lk_json(f"/v1/{b.warehouse_id}/namespaces")
        for ns in nss.get("namespaces", []):
            enc = quote("\x1f".join(ns), safe="")
            tbls = await _lk_json(f"/v1/{b.warehouse_id}/namespaces/{enc}/tables")
            out.extend({"namespace": ".".join(t["namespace"]), "name": t["name"]} for t in tbls.get("identifiers", []))
        return {"items": out}

    @app.get("/api/tables/{namespace}/{table}")
    async def get_table(namespace: str, table: str, who: Identity = Depends(identity)):
        b = _binding(who)
        doc = await _lk_json(f"/v1/{b.warehouse_id}/namespaces/{quote(namespace, safe='')}/tables/{quote(table, safe='')}")
        return table_summary(b, namespace, table, doc["metadata"])

    @app.api_route("/iceberg/{rest:path}", methods=["GET", "HEAD", "POST", "DELETE", "PUT"])
    async def iceberg(request: Request, rest: str):
        try:
            who = identity(request)
        except HTTPException as e:
            return _iceberg_error(e.status_code, e.detail)
        b = c.warehouses.get(who.workspace)
        if b is None:
            return _iceberg_error(404, f"workspace {who.workspace!r} has no warehouse yet; a workspace owner creates one first")
        raw = request.scope.get("raw_path", b"").decode("latin-1") or request.url.path
        sub = raw.split("/iceberg", 1)[1] if "/iceberg" in raw else "/" + rest
        decision = proxy.authorize(request.method, sub, who.role, b.warehouse_id)
        if not decision.allowed:
            return _iceberg_error(decision.status, decision.reason)
        query = f"warehouse={quote(b.warehouse_name)}" if decision.is_config else request.scope.get("query_string", b"").decode("latin-1")
        headers = {k: v for k, v in request.headers.items() if k.lower() in _FORWARD_REQUEST}
        upstream = await catalog.request(request.method, sub + (f"?{query}" if query else ""), content=await request.body(), headers=headers)
        body = upstream.content
        if body and upstream.headers.get("content-type", "").startswith("application/json"):
            try:
                doc = json.loads(body)
            except ValueError:
                doc = None
            if isinstance(doc, dict):
                if decision.is_config:
                    doc = proxy.rewrite_config(doc)
                elif ("metadata" in doc and "config" in doc) or "storage-credentials" in doc:
                    doc = proxy.rewrite_table_response(doc)
                body = json.dumps(doc).encode()
        out_headers = {k: v for k, v in upstream.headers.items() if k.lower() in _FORWARD_RESPONSE}
        return Response(content=body, status_code=upstream.status_code, headers=out_headers)

    return app


def table_summary(b, namespace: str, table: str, md: dict) -> dict:
    """What a table *is*, platform-side: identity, schema, where it lives (as ``{backendId, path}``),
    history. The candidate shape for surfacing a table in booth-catalog (docs/decisions/0002)."""
    location = md.get("location", "")
    root = b.storage_root.rstrip("/")
    rel = location[len(root):].strip("/") if location.startswith(root) else None
    current = md.get("current-schema-id")
    schema = next((s for s in md.get("schemas", []) if s.get("schema-id") == current), {"fields": []})
    return {
        "namespace": namespace,
        "name": table,
        "tableUuid": md.get("table-uuid"),
        "formatVersion": md.get("format-version"),
        "location": {"backendId": b.backend_id, "path": f"{b.path}/{rel}".strip("/")} if rel is not None else None,
        "schema": [
            {"name": f["name"], "type": f["type"] if isinstance(f["type"], str) else f["type"].get("type"), "required": f.get("required", False), "doc": f.get("doc", "")}
            for f in schema.get("fields", [])
        ],
        "schemaId": current,
        "partitionSpec": next((s.get("fields", []) for s in md.get("partition-specs", []) if s.get("spec-id") == md.get("default-spec-id")), []),
        "currentSnapshotId": md.get("current-snapshot-id"),
        "snapshots": [
            {"snapshotId": s["snapshot-id"], "timestampMs": s["timestamp-ms"], "operation": (s.get("summary") or {}).get("operation")}
            for s in md.get("snapshots", [])
        ],
        "lastUpdatedMs": md.get("last-updated-ms"),
    }


async def _renew_loop(c: Components) -> None:
    while True:
        try:
            await asyncio.to_thread(c.warehouses.renew_due)
        except Exception as e:  # noqa: BLE001 - a dead loop means every warehouse silently expires
            log.warning("warehouse credential renewal pass failed: %s", e)
        await asyncio.sleep(c.renew_interval_seconds)


def main() -> None:  # pragma: no cover - process entry point
    import uvicorn

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    app = create_app(components_from_settings(Settings.from_env()))
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")
