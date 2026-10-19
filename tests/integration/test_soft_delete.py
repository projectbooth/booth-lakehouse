"""ADR 0122 B against real Lakekeeper v0.13.6 and MinIO, through the real proxy: every drop is a
restorable soft delete whose files stay for the window; restore and its conflict; and switching an
existing ``hard`` warehouse that holds tables to ``soft`` leaves every table and file untouched.

Lakekeeper's task poll is 1 s in this stack (hack/docker-compose.yml), so waiting a few seconds is
long enough for a purge task to have run if one had been scheduled — under the old ``hard`` profile
these files would be gone by then (docs/design-drop-semantics.md)."""

from __future__ import annotations

import os
import time
import uuid

import httpx

from .conftest import FAKECORE, http, lakehouse, s3_root, token

LAKEKEEPER = os.environ.get("LAKEKEEPER_URL", "http://lakekeeper:8181")
SETTLE = 6  # > several task polls
WEEK = 7 * 24 * 3600
CONFLICT = "a table with this name exists now; rename or drop it first"


def api(method: str, path: str, tok: str, ws: str = "acme"):
    return http(method, f"{FAKECORE}/modules/lakehouse/api{path}", headers={"Authorization": "Bearer " + tok, "X-Workspace": ws})


def objects(location: str) -> dict[str, str]:
    """Every object under a table's location: key -> ETag."""
    bucket, _, prefix = location.removeprefix("s3://").partition("/")
    out, kw = {}, {"Bucket": bucket, "Prefix": prefix.rstrip("/") + "/"}
    while True:
        page = s3_root().list_objects_v2(**kw)
        out.update({o["Key"]: o["ETag"] for o in page.get("Contents", [])})
        if not page.get("IsTruncated"):
            return out
        kw["ContinuationToken"] = page["NextContinuationToken"]


def dropped(tok: str, ws: str = "acme") -> list[dict]:
    status, doc = api("GET", "/dropped-tables", tok, ws)
    assert status == 200, doc
    return doc["items"]


def one_dropped(tok: str, name: str, ws: str = "acme") -> dict:
    ns, _, t = name.rpartition(".")
    (item,) = [d for d in dropped(tok, ws) if (d["namespace"], d["name"]) == (ns, t)]
    return item


def test_every_kind_of_drop_keeps_the_files_and_is_restorable(acme, tokens):
    lh = lakehouse("acme", tokens["editor"])
    ns = f"drops_{uuid.uuid4().hex[:6]}"
    tables = {}
    for t in ("plain", "purged", "raw"):
        tbl = lh.create_table(f"{ns}.{t}", [{"v": 1}, {"v": 2}, {"v": 3}])
        loc = tbl.iceberg.location()
        tables[t] = (loc, objects(loc))
        assert tables[t][1], f"{t}: nothing written"

    lh.drop_table(f"{ns}.plain")  # PyIceberg's drop_table: sends purgeRequested=False
    lh.catalog.purge_table(f"{ns}.purged")  # purgeRequested=true
    # What Spark's plain DROP sends is no flag at all; and a client asking to skip the soft profile.
    prefix = http("GET", f"{FAKECORE}/modules/lakehouse/iceberg/v1/config", headers={"Authorization": "Bearer " + tokens["editor"], "X-Workspace": "acme"})[1]["defaults"]["prefix"]
    status, body = http("DELETE", f"{FAKECORE}/modules/lakehouse/iceberg/v1/{prefix}/namespaces/{ns}/tables/raw?force=true&purgeRequested=false",
                        headers={"Authorization": "Bearer " + tokens["editor"], "X-Workspace": "acme"})
    assert status == 204, body

    time.sleep(SETTLE)
    assert lh.tables(ns) == []
    for t, (loc, before) in tables.items():
        assert objects(loc) == before, f"{t}: files changed during the restore window"
        item = one_dropped(tokens["viewer"], f"{ns}.{t}")
        assert abs(item["expiresAt"] - item["droppedAt"] - WEEK) < 5, item

    # Restore: owner only, data back exactly, and writable again.
    tid = one_dropped(tokens["editor"], f"{ns}.plain")["id"]
    assert api("POST", f"/dropped-tables/{tid}/restore", tokens["editor"])[0] == 403
    status, doc = api("POST", f"/dropped-tables/{tid}/restore", tokens["owner"])
    assert status == 200, doc
    assert sorted(lh.read(f"{ns}.plain").column("v").to_pylist()) == [1, 2, 3]
    lh.append(f"{ns}.plain", [{"v": 4}])
    assert lh.read(f"{ns}.plain").num_rows == 4
    assert [d for d in dropped(tokens["editor"]) if d["id"] == tid] == []


def test_restoring_onto_a_name_in_use_is_refused_and_changes_nothing(acme, tokens):
    lh = lakehouse("acme", tokens["editor"])
    name = f"drops_{uuid.uuid4().hex[:6]}.c"
    lh.create_table(name, [{"v": 1}])
    lh.drop_table(name)
    old_id = one_dropped(tokens["editor"], name)["id"]
    lh.create_table(name, [{"v": 10}, {"v": 20}])  # the name is free once dropped
    status, doc = api("POST", f"/dropped-tables/{old_id}/restore", tokens["owner"])
    assert status == 409 and doc["detail"] == CONFLICT, (status, doc)
    assert sorted(lh.read(name).column("v").to_pylist()) == [10, 20]  # the live table is untouched
    assert one_dropped(tokens["editor"], name)["id"] == old_id  # and the dropped one still restorable


def test_the_admin_count_leaves_out_dropped_tables(acme, tokens):
    """Lakekeeper's statistics keep counting a soft-deleted table until its window ends; the admin
    view reports live tables, and the restorable dropped ones separately."""
    lh = lakehouse("acme", tokens["editor"])
    name = f"drops_{uuid.uuid4().hex[:6]}.counted"
    lh.create_table(name, [{"v": 1}])

    def counts():
        status, doc = api("GET", "/admin/warehouses", tokens["editor"])
        assert status == 200, doc
        return doc["items"][0]["tables"]

    before = counts()
    assert before["tables"] == len(lh.tables())
    lh.drop_table(name)
    after = counts()
    assert after["tables"] == before["tables"] - 1 == len(lh.tables())
    assert after["dropped"] == before["dropped"] + 1


def test_restoring_an_unknown_id_is_404(acme, tokens):
    status, _ = api("POST", f"/dropped-tables/{uuid.uuid4()}/restore", tokens["owner"])
    assert status == 404


def test_an_existing_hard_warehouse_with_tables_is_switched_without_touching_them():
    """The homelab's case: a warehouse created before ADR 0122 B (Lakekeeper's default, hard) that
    already holds tables. The API switches it to soft on its own; every table, row, metadata pointer
    and file is the same afterwards, and a drop is then restorable."""
    owner = token("erin-home", "/workspaces/home/owner")
    lh = lakehouse("home", owner)
    lh.create_warehouse("lake", "warehouse")
    lh.create_table("nfl.plays", [{"play": i, "yards": i * 3} for i in range(500)])
    lh.append("nfl.plays", [{"play": 500, "yards": 7}])
    lh.create_table("nfl.team_game_pbp", [{"game": g} for g in range(42)])
    root = lh.warehouse()["storageRoot"]

    # Make it what the homelab's warehouse is today: hard, as if created before this setting existed.
    wid = http("GET", f"{FAKECORE}/modules/lakehouse/iceberg/v1/config", headers={"Authorization": "Bearer " + owner, "X-Workspace": "home"})[1]["defaults"]["prefix"]
    lk = httpx.Client(base_url=LAKEKEEPER, timeout=30)
    assert lk.post(f"/management/v1/warehouse/{wid}/delete-profile", json={"delete-profile": {"type": "hard"}}).status_code < 300
    set_hard_at = time.time()

    def state():
        tables = {}
        for name in lh.tables("nfl"):
            t = lh.table(name).iceberg
            rows = lh.read(name)
            tables[name] = (t.metadata_location, t.metadata.current_snapshot_id, rows.num_rows, sorted(tuple(r.values()) for r in rows.to_pylist()))
        return tables, objects(root)

    before = state()
    assert set(before[0]) == {"nfl.plays", "nfl.team_game_pbp"} and before[0]["nfl.plays"][2] == 501

    deadline = time.time() + 30  # the compose API reconciles every 3 s
    while time.time() < deadline:
        profile = lk.get(f"/management/v1/warehouse/{wid}").json()["delete-profile"]
        if profile.get("type") == "soft":
            break
        time.sleep(1)
    assert profile == {"type": "soft", "expiration-seconds": WEEK}, profile

    assert state() == before  # tables, snapshots, rows, metadata pointers and every file unchanged
    status, view = api("GET", "/admin/warehouses", owner, "home")
    row = view["items"][0]["deleteProfile"]
    assert status == 200 and row["status"] == "ok" and row["type"] == "soft" and row["changedAt"] >= set_hard_at - 1, row

    # And a drop is now restorable, with its files kept.
    loc = lh.table("nfl.team_game_pbp").iceberg.location()
    files = objects(loc)
    lh.drop_table("nfl.team_game_pbp")
    time.sleep(SETTLE)
    assert objects(loc) == files
    tid = one_dropped(owner, "nfl.team_game_pbp", "home")["id"]
    assert api("POST", f"/dropped-tables/{tid}/restore", owner, "home")[0] == 200
    assert lh.read("nfl.team_game_pbp").num_rows == 42
