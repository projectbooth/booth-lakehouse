"""booth-lakehouse's HTTP surface (reached through booth-core's gateway at ``/modules/lakehouse``).

- ``GET  /health``                     — the manifest's health path: 200 when Lakekeeper answers.
- ``GET  /api/warehouse``              — this workspace's warehouse (any role), 404 if none yet.
- ``PUT  /api/warehouse``              — owner: create it at ``{backendId, path}`` (ADR 0045).
- ``GET  /api/tables``                 — every table in this workspace's warehouse.
- ``GET  /api/admin/warehouses``       — the admin view (ADR 0093): warehouses with cheap status,
                                         own workspace for editors/owners, every workspace for a
                                         platform operator (ADR 0094, admin.py). Read-only.
- ``GET  /api/tables/{ns}/{table}``    — one table: schema, location as ``{backendId, path}``,
                                         snapshots. Minus snapshots, the ``table.*`` event payload
                                         booth-catalog registers (ADR 0085, events.py).
- ``GET  /api/dropped-tables``         — this workspace's dropped tables still restorable under the
                                         soft delete profile (any role), with when each was dropped
                                         and when it stops being restorable (ADR 0122 B).
- ``POST /api/dropped-tables/{id}/restore`` — owner: restore one; 409 if a table of that name
                                         exists now.
- ``*    /iceberg/v1/...``             — the Iceberg REST catalog, authorized per ``proxy.py`` and
                                         forwarded to Lakekeeper (table drops rewritten, ADR 0122 B).
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

from . import admin, proxy
from .config import Settings
from .deleteprofile import DeleteProfiles
from .events import NatsPublisher, TableEvents, run_forever
from .identity import OWNER, AuthError, Forbidden, Identity, Verifier, resolve
from .lakekeeper import Lakekeeper, LakekeeperError, delete_profile
from .store import MemoryStore, PostgresStore, Store
from .tables import TableNotFound, TableReader, TableRef, table_summary
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
    tables: TableReader
    renew_interval_seconds: int = 60
    events: TableEvents | None = None
    events_interval_seconds: float = 10
    profiles: DeleteProfiles | None = None
    profile_interval_seconds: float = 300


RESTORE_CONFLICT = "a table with this name exists now; rename or drop it first"


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
    profile = delete_profile(s.warehouse_delete_profile, s.warehouse_delete_expiration_seconds)
    log.info("warehouse delete profile: %s", profile)
    wh = Warehouses(store, lk, broker_for, minter, s.warehouse_ttl_seconds, s.renew_margin_seconds, delete_profile=profile)
    reader = TableReader(s.lakekeeper_url)
    events = None
    if s.events_url:
        events = TableEvents(
            store, reader, NatsPublisher(s.events_url, s.events_creds_file), s.events_update_min_gap_seconds,
            dropped=lambda wid: {t.id: t.dropped_at for t in lk.dropped_tables(wid)},  # a table's id is its Iceberg UUID
        )
    else:
        log.warning("BOOTH_EVENTS_URL is empty: no table.* events, so booth-catalog won't learn about tables (ADR 0085)")
    for i in s.issuers:
        log.info("oidc: verifying tokens with issuer=%s keys-from=%s", i.url, i.keys_from)
    return Components(
        Verifier(s.issuers, s.groups_claim), store, lk, wh, reader, s.renew_interval_seconds, events, s.events_interval_seconds,
        DeleteProfiles(lk, profile), s.delete_profile_reconcile_seconds,
    )


class WarehouseRequest(BaseModel):
    backendId: str
    path: str


def _iceberg_error(status: int, message: str) -> JSONResponse:
    kind = {400: "BadRequestException", 401: "NotAuthorizedException", 403: "ForbiddenException", 404: "NoSuchWarehouseException"}.get(status, "ServiceFailureException")
    return JSONResponse({"error": {"message": message, "type": kind, "code": status}}, status_code=status)


def create_app(c: Components, run_renewals: bool = True, catalog_transport: httpx.AsyncBaseTransport | None = None) -> FastAPI:
    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        tasks = []
        if run_renewals:
            tasks.append(asyncio.create_task(_renew_loop(c)))
        if c.events is not None:
            tasks.append(asyncio.create_task(run_forever(c.events, nudge, c.events_interval_seconds)))
        if c.profiles is not None:
            # In the background: Lakekeeper being down at startup must not keep the API from serving.
            tasks.append(asyncio.create_task(_profile_loop(c)))
        yield
        for t in tasks:
            t.cancel()
        if c.events is not None:
            await c.events.publisher.close()

    app = FastAPI(title="booth-lakehouse", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    nudge = asyncio.Event()  # set after a write through the proxy: publish table.* events soon
    catalog = httpx.AsyncClient(base_url=c.lakekeeper.base_url + "/catalog", timeout=120, transport=catalog_transport)

    def identity(request: Request) -> Identity:
        auth = request.headers.get("authorization", "")
        if not auth.lower().startswith("bearer "):
            raise HTTPException(401, "a bearer token is required")
        token = auth[7:].strip()
        workspace = request.headers.get("x-booth-workspace") or request.headers.get("x-workspace", "")
        try:
            claims = c.verifier.verify(token)
            who = resolve(claims, token, workspace, request.headers.get("x-booth-role", ""))
        except AuthError as e:
            raise HTTPException(401, str(e)) from None
        except Forbidden as e:
            raise HTTPException(403, str(e)) from None
        c.warehouses.note_member(who)  # candidates for unattended renewal (warehouses.py)
        return who

    @app.get("/health")
    def health():
        ok = c.lakekeeper.healthy()
        doc = {"status": "ok" if ok else "unavailable", "lakekeeper": "ok" if ok else "unreachable"}
        # Informational: a stalled publisher shouldn't take the catalog API down with it.
        doc["tableEvents"] = c.events.status if c.events is not None else "disabled"
        return JSONResponse(doc, status_code=200 if ok else 503)

    @app.get("/api/warehouse")
    def get_warehouse(who: Identity = Depends(identity)):
        b = c.warehouses.get(who.workspace)
        if b is None:
            raise HTTPException(404, f"workspace {who.workspace!r} has no warehouse yet")
        return b.public()

    @app.put("/api/warehouse", status_code=201)
    def put_warehouse(body: WarehouseRequest, who: Identity = Depends(identity)):
        try:
            b = c.warehouses.create(who, body.backendId, body.path)
            if c.profiles is not None:
                c.profiles.created(b)
            return b.public()
        except Forbidden as e:
            raise HTTPException(403, str(e)) from None
        except WarehouseError as e:
            raise HTTPException(e.status, str(e)) from None

    def _binding(who: Identity):
        b = c.warehouses.get(who.workspace)
        if b is None:
            raise HTTPException(404, f"workspace {who.workspace!r} has no warehouse yet")
        return b

    @app.get("/api/admin/warehouses")
    async def admin_warehouses(who: Identity = Depends(identity)):
        try:
            access = admin.decide(who)
        except Forbidden as e:
            raise HTTPException(403, str(e)) from None
        if access.scope == admin.ALL:
            bindings = await asyncio.to_thread(c.store.all)
        else:
            own = c.warehouses.get(who.workspace)
            bindings = [own] if own else []
        now = c.warehouses.clock()
        margin = c.warehouses.renew_margin_seconds
        rows = []
        for b in bindings:
            stats = await asyncio.to_thread(admin.table_stats, c.lakekeeper.http, b.warehouse_id)
            try:
                dropped = len(await asyncio.to_thread(c.lakekeeper.dropped_tables, b.warehouse_id))
            except (LakekeeperError, httpx.HTTPError, ValueError, KeyError):
                dropped = None  # degrade the one field, never the view (ADR 0093)
            stats = admin.without_dropped(stats, dropped)
            profile = c.profiles.status(b.workspace).public(c.profiles.desired) if c.profiles is not None else None
            rows.append(admin.row(b, now, margin, stats, profile))
        return {"scope": access.scope, "items": rows}

    @app.get("/api/dropped-tables")
    async def dropped_tables(who: Identity = Depends(identity)):
        b = _binding(who)
        try:
            items = await asyncio.to_thread(c.lakekeeper.dropped_tables, b.warehouse_id)
        except (LakekeeperError, httpx.HTTPError) as e:
            raise HTTPException(502, f"Lakekeeper: {e}") from None
        profile = c.profiles.status(b.workspace).public(c.profiles.desired) if c.profiles is not None else None
        return {"deleteProfile": profile, "items": [t.public() for t in sorted(items, key=lambda t: -t.dropped_at)]}

    @app.post("/api/dropped-tables/{table_id}/restore")
    async def restore_table(table_id: str, who: Identity = Depends(identity)):
        try:
            who.require(OWNER)
        except Forbidden as e:
            raise HTTPException(403, str(e)) from None
        b = _binding(who)
        try:
            # Only an id dropped from *this* workspace's warehouse: never act on one named by a caller
            # that isn't in the listing (and the listing answers the 404 clearly, expired or not).
            mine = {t.id: t for t in await asyncio.to_thread(c.lakekeeper.dropped_tables, b.warehouse_id)}
            t = mine.get(table_id)
            if t is None:
                raise HTTPException(404, "no restorable dropped table with that id in this workspace (it may have passed its restore window)")
            await asyncio.to_thread(c.lakekeeper.undrop_table, b.warehouse_id, table_id)
        except LakekeeperError as e:
            if e.status == 409:
                raise HTTPException(409, RESTORE_CONFLICT) from None
            raise HTTPException(502, str(e)) from None
        except httpx.HTTPError as e:
            raise HTTPException(502, f"Lakekeeper: {e}") from None
        log.info("table restored workspace=%s table=%s.%s id=%s by=%s", who.workspace, ".".join(t.namespace), t.name, table_id, who.subject)
        if c.events is not None:
            c.events.restored(who.workspace, table_id, t.dropped_at)  # its own message id (events.py)
        nudge.set()  # announce it to booth-catalog soon (table.created, events.py)
        return {"restored": t.public()}

    @app.get("/api/tables")
    async def list_tables(who: Identity = Depends(identity)):
        b = _binding(who)
        try:
            refs = await asyncio.to_thread(c.tables.tables, b.warehouse_id)
        except httpx.HTTPError as e:
            raise HTTPException(502, f"Lakekeeper: {e}") from None
        return {"items": [{"namespace": r.dotted_namespace, "name": r.name} for r in refs]}

    @app.get("/api/tables/{namespace}/{table}")
    async def get_table(namespace: str, table: str, who: Identity = Depends(identity)):
        b = _binding(who)
        ref = TableRef(tuple(namespace.split(".")), table)
        try:
            md = await asyncio.to_thread(c.tables.metadata, b.warehouse_id, ref)
        except TableNotFound:
            raise HTTPException(404, "no such table") from None
        except httpx.HTTPError as e:
            raise HTTPException(502, f"Lakekeeper: {e}") from None
        return table_summary(b, namespace, table, md)

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
        query = proxy.drop_query(request.method, sub, query)  # every table drop restorable (ADR 0122 B)
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
        if request.method not in ("GET", "HEAD") and upstream.status_code < 300:
            nudge.set()
        out_headers = {k: v for k, v in upstream.headers.items() if k.lower() in _FORWARD_RESPONSE}
        return Response(content=body, status_code=upstream.status_code, headers=out_headers)

    return app


# After a failed pass: retry within seconds, backing off, for about the first five minutes; then
# back to the normal interval. ADR 0122 addendum: a fresh install or restart must not leave a
# warehouse on hard (Lakekeeper's default) until the next 5-minute pass.
PROFILE_RETRY_SECONDS = (2, 5, 10, 30, 30, 30, 30, 30, 30, 30, 30, 30)


async def _profile_loop(c: Components) -> None:
    """ADR 0122 B: at startup, then every ``profile_interval_seconds``, keep every warehouse at the
    configured delete profile (deleteprofile.py). Failures are logged and shown in the admin view. The
    API and Lakekeeper often start together, so the first pass can find Lakekeeper not serving yet:
    a failed pass is retried after PROFILE_RETRY_SECONDS (2 s, 5 s, 10 s, then every 30 s), and only
    once those run out does it wait the full interval."""
    failures = 0
    while True:
        failed = True
        try:
            bindings = await asyncio.to_thread(c.store.all)
            failed = bool(await asyncio.to_thread(c.profiles.reconcile_all, bindings))
        except Exception as e:  # noqa: BLE001 - the loop must survive anything (e.g. the database down)
            log.warning("warehouse delete profile pass failed: %s", e)
        failures = failures + 1 if failed else 0
        wait = c.profile_interval_seconds
        if 0 < failures <= len(PROFILE_RETRY_SECONDS):
            wait = min(PROFILE_RETRY_SECONDS[failures - 1], wait)
        await asyncio.sleep(wait)


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
