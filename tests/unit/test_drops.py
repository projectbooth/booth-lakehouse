"""ADR 0122 B in-process: the drop rewrite, the delete-profile reconciler, and the dropped-tables
routes, against the fake Lakekeeper. The real behaviour (files kept during the window, restore,
conflict) is proven against real Lakekeeper in tests/integration/test_drops.py."""

from __future__ import annotations

import logging

import httpx
import pytest
from fastapi.testclient import TestClient

from booth_lakehouse_server import proxy
from booth_lakehouse_server.app import RESTORE_CONFLICT, Components, create_app
from booth_lakehouse_server.config import Settings
from booth_lakehouse_server.deleteprofile import DeleteProfiles
from booth_lakehouse_server.lakekeeper import Lakekeeper, delete_profile, same_profile
from booth_lakehouse_server.store import MemoryStore
from booth_lakehouse_server.tables import TableReader
from booth_lakehouse_server.warehouses import Warehouses

from .fakes import FakeBroker, FakeLakekeeper
from .keys import IDP, Signer, verifier

idp = Signer(IDP)
SOFT = {"type": "soft", "expiration-seconds": 604800}
T = "/v1/WID/namespaces/nfl/tables/plays"


# ---- the proxy rule -------------------------------------------------------------------------------


@pytest.mark.parametrize("query", ["", "purgeRequested=false", "purgeRequested=False", "purgeRequested=true", "purgeRequested=false&purgeRequested=false"])
def test_a_table_drop_always_goes_out_as_a_purge(query):
    assert proxy.drop_query("DELETE", T, query) == "purgeRequested=true"


def test_force_is_removed_from_every_drop_so_nothing_skips_the_soft_profile():
    assert proxy.drop_query("DELETE", T, "force=true&purgeRequested=false") == "purgeRequested=true"
    assert proxy.drop_query("DELETE", "/v1/WID/namespaces/nfl", "recursive=true&force=true") == "recursive=true"
    assert proxy.drop_query("delete", "/v1/WID/namespaces/nfl/views/v", "Force=true") == ""


def test_other_requests_are_forwarded_unchanged():
    assert proxy.drop_query("GET", T, "snapshots=refs&force=true") == "snapshots=refs&force=true"
    assert proxy.drop_query("POST", T, "purgeRequested=false") == "purgeRequested=false"
    assert proxy.drop_query("DELETE", "/v1/WID/namespaces/nfl/tables/plays/metrics", "x=1") == "x=1"


# ---- the profile and its reconciler ---------------------------------------------------------------


def test_profiles_and_their_comparison():
    assert delete_profile("soft", 604800) == SOFT and delete_profile("hard", 0) == {"type": "hard"}
    with pytest.raises(ValueError):
        delete_profile("gentle", 1)
    assert same_profile({"type": "soft", "expiration-seconds": 604800}, SOFT)
    assert not same_profile({"type": "soft", "expiration-seconds": 30}, SOFT)
    assert not same_profile({"type": "hard"}, SOFT) and not same_profile(None, SOFT)


def test_settings_default_to_soft_for_seven_days():
    s = Settings.from_env({"LAKEKEEPER_URL": "http://lk"})
    assert (s.warehouse_delete_profile, s.warehouse_delete_expiration_seconds) == ("soft", 604800)
    with pytest.raises(ValueError):
        Settings.from_env({"LAKEKEEPER_URL": "http://lk", "WAREHOUSE_DELETE_PROFILE": "never"})


@pytest.fixture
def lk():
    fake = FakeLakekeeper()
    return fake, Lakekeeper("http://lk", transport=fake.transport())


def _existing_hard_warehouse(fake, lakekeeper, ws="home"):
    """A warehouse created before this setting existed: Lakekeeper's default, hard."""
    store, broker = MemoryStore(), FakeBroker()
    wh = Warehouses(store, lakekeeper, lambda tok: broker, None)
    from booth_lakehouse_server.identity import Identity

    b = wh.create(Identity(subject="alice", workspace=ws, role="owner", token="t"), "lake", "warehouse")
    assert fake.warehouses[b.warehouse_id].get("delete-profile") is None
    return store, b


def test_an_existing_hard_warehouse_is_switched_once_and_only_its_setting_changes(lk, caplog):
    fake, lakekeeper = lk
    store, b = _existing_hard_warehouse(fake, lakekeeper)
    before = dict(fake.warehouses[b.warehouse_id])
    profiles = DeleteProfiles(lakekeeper, SOFT, clock=lambda: 1000.0)
    fake.calls.clear()
    with caplog.at_level(logging.INFO):
        assert profiles.reconcile_all(store.all()) == []
    assert fake.warehouses[b.warehouse_id] == {**before, "delete-profile": SOFT}
    assert [(m, p) for m, p, _ in fake.calls] == [("GET", f"/management/v1/warehouse/{b.warehouse_id}"), ("POST", f"/management/v1/warehouse/{b.warehouse_id}/delete-profile")]
    assert "delete profile changed workspace=home" in caplog.text and "from=hard to=soft(604800s)" in caplog.text
    assert profiles.status("home").public(SOFT) == {"type": "soft", "expirationSeconds": 604800, "status": "ok", "desired": {"type": "soft", "expirationSeconds": 604800}, "changedAt": 1000.0}

    fake.calls.clear()  # idempotent: the next pass only looks
    profiles.reconcile_all(store.all())
    assert [m for m, _, _ in fake.calls] == ["GET"]


def test_a_failure_is_recorded_and_retried_never_raised(lk, caplog):
    fake, lakekeeper = lk
    store, _ = _existing_hard_warehouse(fake, lakekeeper)
    profiles = DeleteProfiles(lakekeeper, SOFT)
    fake.fail_profile = True
    with caplog.at_level(logging.WARNING):
        failures = profiles.reconcile_all(store.all())
    assert failures and failures[0][0] == "home" and "Lakekeeper is unwell" in failures[0][1]
    st = profiles.status("home").public(SOFT)
    assert st["status"] == "error" and "Lakekeeper is unwell" in st["error"]
    assert "couldn't check or apply soft(604800s) for workspace=home" in caplog.text
    fake.fail_profile = False
    assert profiles.reconcile_all(store.all()) == [] and profiles.status("home").status == "ok"


def test_a_new_warehouse_is_created_with_the_profile(lk):
    fake, lakekeeper = lk
    wh = Warehouses(MemoryStore(), lakekeeper, lambda tok: FakeBroker(), None, delete_profile=SOFT)
    from booth_lakehouse_server.identity import Identity

    b = wh.create(Identity(subject="alice", workspace="acme", role="owner", token="t"), "lake", "lakehouse")
    assert fake.warehouses[b.warehouse_id]["delete-profile"] == SOFT


# ---- the HTTP surface -----------------------------------------------------------------------------


class Catalog:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    async def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(204)


@pytest.fixture
def env(lk):
    fake, lakekeeper = lk
    store = MemoryStore()
    wh = Warehouses(store, lakekeeper, lambda tok: FakeBroker(), None, delete_profile=SOFT)
    profiles = DeleteProfiles(lakekeeper, SOFT)
    cat = Catalog()
    comps = Components(verifier(idp), store, lakekeeper, wh, TableReader("http://lk", transport=fake.transport()), profiles=profiles)
    client = TestClient(create_app(comps, run_renewals=False, catalog_transport=httpx.MockTransport(cat.handle)))
    client.comps = comps
    r = client.put("/api/warehouse", json={"backendId": "lake", "path": "lakehouse"}, headers=h("owner"))
    assert r.status_code == 201, r.text
    return client, fake, cat, store.get("acme").warehouse_id


def h(role="editor", ws="acme"):
    return {"Authorization": "Bearer " + idp.token(sub=f"{role}-1", groups=[f"/workspaces/{ws}/{role}"]), "X-Workspace": ws}


def test_drops_through_the_proxy_are_rewritten(env):
    client, _, cat, wid = env
    assert client.delete(f"/iceberg/v1/{wid}/namespaces/nfl/tables/plays?purgeRequested=false", headers=h()).status_code == 204
    assert cat.requests[-1].url.query == b"purgeRequested=true"
    client.delete(f"/iceberg/v1/{wid}/namespaces/nfl?recursive=true&force=true", headers=h())
    assert cat.requests[-1].url.query == b"recursive=true"


def test_dropped_tables_are_listed_newest_first_with_their_window(env):
    client, fake, _, wid = env
    fake.drop(wid, ["nfl"], "plays")
    r = client.get("/api/dropped-tables", headers=h("viewer"))
    assert r.status_code == 200
    doc = r.json()
    (item,) = doc["items"]
    assert (item["namespace"], item["name"]) == ("nfl", "plays")
    assert item["expiresAt"] - item["droppedAt"] == 7 * 24 * 3600
    assert doc["deleteProfile"]["type"] == "soft" and doc["deleteProfile"]["status"] == "ok"


def test_only_an_owner_restores_and_only_this_workspaces_tables(env):
    client, fake, _, wid = env
    tid = fake.drop(wid, ["nfl"], "plays")
    assert client.post(f"/api/dropped-tables/{tid}/restore", headers=h("editor")).status_code == 403
    other = fake.drop("someone-elses-warehouse", ["x"], "y")
    assert client.post(f"/api/dropped-tables/{other}/restore", headers=h("owner")).status_code == 404
    assert not any(p.endswith("/undrop") for _, p, _ in fake.calls)
    r = client.post(f"/api/dropped-tables/{tid}/restore", headers=h("owner"))
    assert r.status_code == 200 and r.json()["restored"]["name"] == "plays"
    assert fake.calls[-1][2] == {"targets": [{"type": "table", "id": tid}]}
    assert client.get("/api/dropped-tables", headers=h()).json()["items"] == []


def test_a_restore_onto_a_name_in_use_is_409_with_the_agreed_message(env):
    client, fake, _, wid = env
    tid = fake.drop(wid, ["nfl"], "plays")
    fake.undrop_conflict = True
    r = client.post(f"/api/dropped-tables/{tid}/restore", headers=h("owner"))
    assert r.status_code == 409 and r.json()["detail"] == RESTORE_CONFLICT == "a table with this name exists now; rename or drop it first"


def test_the_admin_view_shows_the_profile_and_a_reconcile_failure(env):
    client, fake, _, _ = env
    row = client.get("/api/admin/warehouses", headers=h("owner")).json()["items"][0]
    assert row["deleteProfile"]["type"] == "soft" and row["deleteProfile"]["status"] == "ok"
    fake.fail_profile = True
    client.comps.profiles.reconcile_all(client.comps.store.all())  # one pass of the background loop
    row = client.get("/api/admin/warehouses", headers=h("owner")).json()["items"][0]
    assert row["deleteProfile"]["status"] == "error" and "Lakekeeper is unwell" in row["deleteProfile"]["error"]
    assert client.get("/api/warehouse", headers=h()).status_code == 200  # the API keeps serving


def test_startup_survives_a_failing_reconcile_and_shows_it(lk):
    import time

    fake, lakekeeper = lk
    store, _ = _existing_hard_warehouse(fake, lakekeeper)
    fake.fail_profile = True
    profiles = DeleteProfiles(lakekeeper, SOFT)
    comps = Components(verifier(idp), store, lakekeeper, Warehouses(store, lakekeeper, None, None), TableReader("http://lk", transport=fake.transport()),
                       profiles=profiles, profile_interval_seconds=0.05)
    with TestClient(create_app(comps, run_renewals=False)) as client:  # runs the lifespan: the loop starts
        assert client.get("/health").status_code == 200
        deadline = time.time() + 5
        while profiles.status("home").status != "error" and time.time() < deadline:
            time.sleep(0.05)
        row = client.get("/api/admin/warehouses", headers=h("owner", "home")).json()["items"][0]
        assert row["deleteProfile"]["status"] == "error"
        fake.fail_profile = False  # the next pass fixes it
        deadline = time.time() + 5
        while profiles.status("home").status != "ok" and time.time() < deadline:
            time.sleep(0.05)
        assert fake.warehouses[store.get("home").warehouse_id]["delete-profile"] == SOFT


def test_admin_counts_leave_out_dropped_tables():
    from booth_lakehouse_server import admin

    assert admin.without_dropped({"tables": 5, "views": 1, "asOf": None}, 2) == {"tables": 3, "views": 1, "asOf": None, "dropped": 2}
    assert admin.without_dropped({"tables": 5, "views": 1, "asOf": None}, None) == {"tables": 5, "views": 1, "asOf": None}
    assert admin.without_dropped(None, 3) is None
