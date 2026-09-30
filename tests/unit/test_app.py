"""The HTTP surface, in-process: real FastAPI app, real token verification, fake Lakekeeper. Checks
what the proxy actually forwards and returns, not just the pure rules in test_proxy.py."""

from __future__ import annotations

import json

import httpx
import pytest
from fastapi.testclient import TestClient

from booth_lakehouse_server.app import Components, create_app
from booth_lakehouse_server.lakekeeper import Lakekeeper
from booth_lakehouse_server.store import MemoryStore
from booth_lakehouse_server.tables import TableReader
from booth_lakehouse_server.warehouses import Warehouses

from .fakes import FakeBroker, FakeLakekeeper
from .keys import IDP, Signer, verifier

idp = Signer(IDP)


class Catalog:
    """Records what reached "Lakekeeper's" catalog API, and answers like it does."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    async def handle(self, request: httpx.Request) -> httpx.Response:
        return self.answer(request)

    def answer(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if path == "/catalog/v1/config":
            return httpx.Response(200, json={"overrides": {"uri": "http://lakekeeper:8181/catalog"}, "defaults": {"prefix": "WID"}})
        if path.endswith("/tables/daily"):
            return httpx.Response(200, json={
                "metadata-location": "s3://lake/acme-data/lakehouse/u1/metadata/1.json",
                "metadata": {"location": "s3://lake/acme-data/lakehouse/u1", "current-schema-id": 0, "format-version": 2,
                             "schemas": [{"schema-id": 0, "fields": [{"id": 1, "name": "amount", "type": "double", "required": False}]}],
                             "snapshots": [{"snapshot-id": 7, "timestamp-ms": 1, "summary": {"operation": "append"}}], "current-snapshot-id": 7},
                "config": {"s3.endpoint": "http://minio:9000", "s3.access-key-id": "LEAK"},
                "storage-credentials": [{"prefix": "s3://lake", "config": {"s3.secret-access-key": "LEAK"}}],
            })
        if path.endswith("/namespaces"):
            return httpx.Response(200, json={"namespaces": [["sales"]]})
        if path.endswith("/namespaces/sales/tables"):
            return httpx.Response(200, json={"identifiers": [{"namespace": ["sales"], "name": "daily"}]})
        return httpx.Response(404, json={"error": {"message": "no", "type": "NotFound", "code": 404}})


@pytest.fixture
def env():
    lk = FakeLakekeeper()
    lakekeeper = Lakekeeper("http://lk", transport=lk.transport())
    store = MemoryStore()
    broker = FakeBroker()
    wh = Warehouses(store, lakekeeper, lambda tok: broker, None)
    cat = Catalog()
    reader = TableReader("http://lk", transport=httpx.MockTransport(cat.answer))
    app = create_app(Components(verifier(idp), store, lakekeeper, wh, reader), run_renewals=False, catalog_transport=httpx.MockTransport(cat.handle))
    client = TestClient(app)
    client.store = store  # for assertions on what the app recorded
    owner = {"Authorization": "Bearer " + idp.token(sub="alice", groups=["/workspaces/acme/owner"]), "X-Workspace": "acme"}
    r = client.put("/api/warehouse", json={"backendId": "lake", "path": "lakehouse"}, headers=owner)
    assert r.status_code == 201, r.text
    wid = store.get("acme").warehouse_id
    return client, cat, wid, broker


def h(role="editor", ws="acme", **extra):
    return {"Authorization": "Bearer " + idp.token(groups=[f"/workspaces/{ws}/{role}"]), "X-Workspace": ws, **extra}


def test_health_follows_lakekeeper(env):
    client, *_ = env
    assert client.get("/health").json() == {"status": "ok", "lakekeeper": "ok", "tableEvents": "disabled"}


def test_warehouse_is_visible_to_any_member_and_never_carries_a_credential(env):
    client, _, _, broker = env
    doc = client.get("/api/warehouse", headers=h("viewer")).json()
    assert doc["storageRoot"] == "s3://lake/acme-data/lakehouse"
    assert broker.issued[0].secret_access_key not in json.dumps(doc) and broker.issued[0].access_key_id not in json.dumps(doc)
    acme_token_asking_for_beta = {**h("viewer"), "X-Workspace": "beta"}
    assert client.get("/api/warehouse", headers=acme_token_asking_for_beta).status_code == 403
    assert client.get("/api/warehouse", headers=h("viewer", "beta")).status_code == 404  # member, no warehouse yet


def test_config_is_forced_to_the_callers_warehouse_and_uri_is_removed(env):
    client, cat, _, _ = env
    r = client.get("/iceberg/v1/config?warehouse=booth-ws-other", headers=h())
    assert r.status_code == 200 and "uri" not in r.json()["overrides"]
    assert cat.requests[-1].url.query == b"warehouse=booth-ws-acme"


def test_only_safe_headers_reach_lakekeeper(env):
    client, cat, wid, _ = env
    client.get(f"/iceberg/v1/{wid}/namespaces", headers=h(**{"X-Iceberg-Access-Delegation": "vended-credentials", "Cookie": "c=1", "Idempotency-Key": "k"}))
    sent = {k.lower() for k in cat.requests[-1].headers}
    assert "authorization" not in sent and "x-iceberg-access-delegation" not in sent and "cookie" not in sent and "x-workspace" not in sent
    assert cat.requests[-1].headers["idempotency-key"] == "k"


def test_table_responses_are_scrubbed(env):
    client, _, wid, _ = env
    r = client.get(f"/iceberg/v1/{wid}/namespaces/sales/tables/daily", headers=h("viewer"))
    assert r.status_code == 200 and "LEAK" not in r.text and r.json()["metadata"]["current-snapshot-id"] == 7


def test_refusals_use_the_iceberg_error_shape_and_never_reach_lakekeeper(env):
    client, cat, wid, _ = env
    before = len(cat.requests)
    r = client.post(f"/iceberg/v1/{wid}/namespaces", json={"namespace": ["x"]}, headers=h("viewer"))
    assert r.status_code == 403 and r.json()["error"]["type"] == "ForbiddenException"
    assert client.get("/iceberg/v1/some-other-id/namespaces", headers=h()).status_code == 404
    assert client.get("/iceberg/v1/config", headers={"X-Workspace": "acme"}).status_code == 401
    assert client.get("/iceberg/v1/config", headers=h("viewer", **{"X-Booth-Role": "owner"})).status_code == 403
    assert len(cat.requests) == before


def test_a_workspace_without_a_warehouse_gets_a_clear_404(env):
    client, *_ = env
    r = client.get("/iceberg/v1/config", headers=h("editor", "beta"))
    assert r.status_code == 404 and "no warehouse yet" in r.json()["error"]["message"]


def test_table_summary_maps_location_to_backend_and_path(env):
    client, *_ = env
    doc = client.get("/api/tables/sales/daily", headers=h("viewer")).json()
    assert doc["location"] == {"backendId": "lake", "path": "lakehouse/u1"}
    assert doc["schema"] == [{"name": "amount", "type": "double", "required": False, "doc": ""}]
    assert doc["snapshots"] == [{"snapshotId": 7, "timestampMs": 1, "operation": "append"}]
    assert client.get("/api/tables", headers=h("viewer")).json() == {"items": [{"namespace": "sales", "name": "daily"}]}


def test_only_owner_creates_and_only_once(env):
    client, *_ = env
    assert client.put("/api/warehouse", json={"backendId": "lake", "path": "x"}, headers=h("editor")).status_code == 403
    assert client.put("/api/warehouse", json={"backendId": "lake", "path": "x"}, headers=h("owner")).status_code == 409


def test_verified_editors_and_owners_become_renewal_candidates(env):
    client, *_ = env
    for sub, role in (("bob", "editor"), ("vic", "viewer"), ("job:7", "editor")):
        h = {"Authorization": "Bearer " + idp.token(sub=sub, groups=[f"/workspaces/acme/{role}"]), "X-Workspace": "acme"}
        assert client.get("/api/warehouse", headers=h).status_code == 200
    # alice created the warehouse (an owner request), bob is an editor; viewers and runs aren't recorded.
    assert sorted(client.store.members("acme")) == ["alice", "bob"]
