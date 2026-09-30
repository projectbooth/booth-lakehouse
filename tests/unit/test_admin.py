"""The ADR 0093 admin view: who sees which warehouses, and the cheap status reported for each."""

from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

from booth_lakehouse_server import admin
from booth_lakehouse_server.app import Components, create_app
from booth_lakehouse_server.identity import Forbidden, Identity
from booth_lakehouse_server.lakekeeper import Lakekeeper
from booth_lakehouse_server.store import Binding, MemoryStore
from booth_lakehouse_server.tables import TableReader
from booth_lakehouse_server.warehouses import Warehouses

from .fakes import FakeBroker, FakeLakekeeper
from .keys import IDP, Signer, verifier

idp = Signer(IDP)


def binding(ws: str, expires: float = 10**10) -> Binding:
    return Binding(ws, "lake", "lakehouse", f"wid-{ws}", f"booth-ws-{ws}", f"s3://lake/{ws}-data/lakehouse", "alice", 1000.0, "l", expires)


# ---- pure decisions ----------------------------------------------------------------------------


@pytest.mark.parametrize("role,ws,ops,scope", [
    ("owner", "acme", frozenset(), admin.OWN),
    ("editor", "acme", frozenset(), admin.OWN),
    ("owner", "ops", frozenset({"ops"}), admin.ALL),
    ("editor", "ops", frozenset({"ops"}), admin.OWN),   # operator scope is owners only
    ("owner", "acme", frozenset({"ops"}), admin.OWN),   # an owner elsewhere is not an operator
])
def test_scope(role, ws, ops, scope):
    assert admin.decide(Identity("u", ws, role, "t"), ops).scope == scope


def test_viewers_are_refused():
    with pytest.raises(Forbidden):
        admin.decide(Identity("u", "ops", "viewer", "t"), frozenset({"ops"}))


def test_credential_status():
    b = binding("acme", expires=1000)
    assert admin.credential_status(b, now=0, renew_margin_seconds=120) == "ok"
    assert admin.credential_status(b, now=900, renew_margin_seconds=120) == "renewing"
    assert admin.credential_status(b, now=1000, renew_margin_seconds=120) == "expired"


def _client(handler) -> httpx.Client:
    return httpx.Client(base_url="http://lk", transport=httpx.MockTransport(handler))


def test_table_stats_is_one_request_and_takes_the_newest_entry():
    seen = []

    def handler(req):
        seen.append(req.url)
        return httpx.Response(200, json={"warehouse-ident": "w", "stats": [
            {"timestamp": "2026-09-30T10:00:00Z", "updated-at": "2026-09-30T09:40:00Z", "number-of-tables": 2, "number-of-views": 0},
            {"timestamp": "2026-09-30T11:00:00Z", "updated-at": "2026-09-30T10:05:00Z", "number-of-tables": 5, "number-of-views": 1},
        ]})

    assert admin.table_stats(_client(handler), "w") == {"tables": 5, "views": 1, "asOf": "2026-09-30T10:05:00Z"}
    assert len(seen) == 1 and seen[0].path == "/management/v1/warehouse/w/statistics"


def test_table_stats_degrades_to_none_and_empty_is_zero():
    assert admin.table_stats(_client(lambda r: httpx.Response(500)), "w") is None

    def boom(req):
        raise httpx.ConnectError("down")

    assert admin.table_stats(_client(boom), "w") is None
    assert admin.table_stats(_client(lambda r: httpx.Response(200, json={"stats": []})), "w") == {"tables": 0, "views": 0, "asOf": None}


# ---- the route, with real signed tokens ---------------------------------------------------------


@pytest.fixture
def client():
    lk = FakeLakekeeper()
    stats_calls: list[str] = []
    base = lk.handler

    def handler(req):
        if req.url.path.endswith("/statistics"):
            wid = req.url.path.split("/")[4]
            stats_calls.append(wid)
            if wid == "wid-beta":
                return httpx.Response(503)
            return httpx.Response(200, json={"stats": [{"timestamp": "t", "updated-at": "u", "number-of-tables": 3, "number-of-views": 0}]})
        return base(req)

    lakekeeper = Lakekeeper("http://lk", transport=httpx.MockTransport(handler))
    store = MemoryStore()
    for ws in ("acme", "beta", "ops"):
        store.insert(binding(ws))
    wh = Warehouses(store, lakekeeper, lambda tok: FakeBroker(), None, 300, 120)
    comps = Components(verifier(idp), store, lakekeeper, wh, TableReader("http://lk", transport=httpx.MockTransport(handler)), operator_workspaces=frozenset({"ops"}))
    c = TestClient(create_app(comps, run_renewals=False))
    c.stats_calls = stats_calls
    return c


def get(c, role, ws, sub="u1", **headers):
    h = {"Authorization": "Bearer " + idp.token(sub=sub, groups=[f"/workspaces/{ws}/{role}"]), "X-Workspace": ws, **headers}
    return c.get("/api/admin/warehouses", headers=h)


def test_an_editor_sees_only_their_own_workspace(client):
    r = get(client, "editor", "acme")
    assert r.status_code == 200
    doc = r.json()
    assert doc["scope"] == "workspace" and [i["workspace"] for i in doc["items"]] == ["acme"]
    item = doc["items"][0]
    assert item["location"] == {"backendId": "lake", "path": "lakehouse"} and item["tables"]["tables"] == 3
    assert item["credential"]["status"] == "ok" and item["createdBy"] == "alice"
    assert "secret" not in r.text.lower() and "lease" not in r.text.lower()


def test_an_operator_owner_sees_every_workspace_and_a_failing_stat_is_null_not_an_error(client):
    doc = get(client, "owner", "ops").json()
    assert doc["scope"] == "all"
    by_ws = {i["workspace"]: i for i in doc["items"]}
    assert set(by_ws) == {"acme", "beta", "ops"}
    assert by_ws["beta"]["tables"] is None and by_ws["acme"]["tables"]["tables"] == 3
    assert len(client.stats_calls) == 3  # one cheap call per warehouse, nothing else


def test_refusals(client):
    assert get(client, "viewer", "acme").status_code == 403
    assert client.get("/api/admin/warehouses", headers={"X-Workspace": "acme"}).status_code == 401
    # ADR 0041: an operator-workspace *editor* forging owner doesn't get the all-workspaces view.
    assert get(client, "editor", "ops", **{"X-Booth-Role": "owner"}).status_code == 403
    # A token for another workspace can't select this one.
    h = {"Authorization": "Bearer " + idp.token(groups=["/workspaces/beta/owner"]), "X-Workspace": "acme"}
    assert client.get("/api/admin/warehouses", headers=h).status_code == 403


def test_a_workspace_without_a_warehouse_is_an_empty_list(client):
    doc = get(client, "owner", "gamma").json()
    assert doc == {"scope": "workspace", "items": []}


def test_the_view_is_read_only(client):
    h = {"Authorization": "Bearer " + idp.token(groups=["/workspaces/ops/owner"]), "X-Workspace": "ops"}
    for method in ("post", "put", "patch", "delete"):
        assert getattr(client, method)("/api/admin/warehouses", headers=h).status_code == 405
