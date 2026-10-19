"""table.* events (ADR 0085) on a real NATS JetStream in core's JWT operator mode, with credentials
minted by booth-core's own code from this module's manifest (natsauth/gen.go). Read back the way
booth-catalog would: a durable pull consumer on BOOTH_EVENTS, with a subscriber credential."""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid

import nats
import pytest

from .conftest import API, FAKECORE, http, lakehouse

NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")
CREDS = os.path.join(os.path.dirname(__file__), "natsauth")


def _events(filter_subject: str, want, timeout: float = 45) -> list[dict]:
    """Messages on filter_subject (from the start of the stream) until ``want(messages)`` is true."""

    async def go():
        nc = await nats.connect(NATS_URL, user_credentials=os.path.join(CREDS, "catalog.creds"))
        js = nc.jetstream()
        sub = await js.pull_subscribe(filter_subject, durable=f"t-{uuid.uuid4().hex[:8]}", stream="BOOTH_EVENTS")
        got: list[dict] = []
        deadline = time.time() + timeout
        try:
            while time.time() < deadline and not want(got):
                try:
                    for m in await sub.fetch(50, timeout=2):
                        got.append({"subject": m.subject, **json.loads(m.data)})
                        await m.ack()
                except (nats.errors.TimeoutError, TimeoutError):  # nats-py raises either, depending on the path
                    pass
        finally:
            await nc.close()
        return got

    return asyncio.run(go())


def _for(uuid_: str):
    return lambda msgs: [m for m in msgs if (m.get("data") or {}).get("tableUuid") == uuid_]


def test_create_update_drop_are_published_for_catalog(acme, tokens):
    lh = lakehouse("acme", tokens["editor"])
    name = f"ev.t_{uuid.uuid4().hex[:8]}"
    tbl = lh.create_table(name, [{"id": 1, "label": "a"}])
    tid = str(tbl.iceberg.metadata.table_uuid)

    created = _for(tid)(_events("booth.acme.table.created", lambda ms: _for(tid)(ms)))
    assert len(created) == 1
    c = created[0]
    assert c["subject"] == "booth.acme.table.created" and c["workspace"] == "acme" and c["eventType"] == "table.created"
    assert c["publishedBy"] == "lakehouse"
    d = c["data"]
    ns, t = name.split(".")
    assert (d["namespace"], d["name"]) == (ns, t)
    assert d["location"] == {"backendId": tbl.location[0], "path": tbl.location[1]}  # ADR 0045 pair
    assert [f["name"] for f in d["schema"]] == ["id", "label"] and "snapshots" not in d
    first_snapshot = d["currentSnapshotId"]

    lh.append(name, [{"id": 2, "label": "b"}])
    updated = _for(tid)(_events("booth.acme.table.updated", lambda ms: _for(tid)(ms)))
    assert updated and updated[-1]["data"]["currentSnapshotId"] != first_snapshot

    lh.drop_table(name)
    deleted = _for(tid)(_events("booth.acme.table.deleted", lambda ms: _for(tid)(ms)))
    assert deleted and deleted[0]["data"] == {"tableUuid": tid, "namespace": ns, "name": t}


def test_a_burst_of_commits_is_debounced(acme, tokens):
    """The compose API runs with a 5 s per-table update gap: ten quick appends don't make ten events."""
    lh = lakehouse("acme", tokens["editor"])
    name = f"ev.burst_{uuid.uuid4().hex[:8]}"
    tbl = lh.create_table(name, [{"v": 0}])
    tid = str(tbl.iceberg.metadata.table_uuid)
    for i in range(1, 11):
        tbl.append([{"v": i}])
    final = lh.table(name).iceberg.metadata.current_snapshot_id
    # created or updated, whichever carried it: the first pass may already see the final snapshot.
    seen = _for(tid)(_events("booth.acme.table.*", lambda ms: any(m["data"].get("currentSnapshotId") == final for m in _for(tid)(ms))))
    assert seen[-1]["data"]["currentSnapshotId"] == final  # converges on the latest state
    assert seen[0]["eventType"] == "table.created"
    assert len(seen) < 11  # eleven commits, far fewer events


def test_each_workspace_publishes_on_its_own_subject(acme, tokens):
    beta = lakehouse("beta", tokens["beta_editor"])
    try:
        beta.warehouse()
    except Exception:
        lakehouse("beta", tokens["owner"]).create_warehouse("lake", "lakehouse")
    tbl = beta.create_table(f"ev.b_{uuid.uuid4().hex[:8]}", [{"v": 1}])
    tid = str(tbl.iceberg.metadata.table_uuid)
    msgs = _for(tid)(_events("booth.*.table.created", lambda ms: _for(tid)(ms)))
    assert [m["subject"] for m in msgs] == ["booth.beta.table.created"] and msgs[0]["workspace"] == "beta"


def test_the_lakehouse_credential_publishes_only_what_its_manifest_declares():
    """Core's grants for `events.publish: [table.*]`, enforced by the real server: another event
    type is refused, so this module can't impersonate, e.g., a dashboard module."""

    async def go():
        errors: list[Exception] = []

        async def on_error(e):
            errors.append(e)

        nc = await nats.connect(NATS_URL, user_credentials=os.path.join(CREDS, "lakehouse.creds"), error_cb=on_error)
        js = nc.jetstream()
        ok = await js.publish("booth.probe.table.updated", b'{"probe": true}', stream="BOOTH_EVENTS", headers={"Nats-Msg-Id": uuid.uuid4().hex})
        with pytest.raises((nats.errors.TimeoutError, nats.errors.NoRespondersError)):
            await js.publish("booth.acme.dashboard.created", b"{}", timeout=3)
        await nc.close()
        return ok, errors

    ok, errors = asyncio.run(go())
    assert ok.stream == "BOOTH_EVENTS"
    assert any("permissions violation" in str(e).lower() for e in errors)


def test_health_reports_the_publisher(acme):
    deadline = time.time() + 30
    while time.time() < deadline:
        status, doc = http("GET", f"{API}/health")
        if doc.get("tableEvents") == "ok":
            break
        time.sleep(1)
    assert doc["tableEvents"] == "ok"


# ADR 0122 addendum 2026-10-19: every drop and every restore has its own Nats-Msg-Id (events.py), so
# a restore of an unchanged table, or a second drop of the same table, inside JetStream's 2-minute
# duplicate window still reaches booth-catalog. (Before the fix these two tests were strict xfails.)


def _restore(tokens, name: str) -> str:
    ns, _, t = name.rpartition(".")
    h = {"Authorization": "Bearer " + tokens["owner"], "X-Workspace": "acme"}
    status, doc = http("GET", f"{FAKECORE}/modules/lakehouse/api/dropped-tables", headers=h)
    assert status == 200, doc
    (item,) = [d for d in doc["items"] if (d["namespace"], d["name"]) == (ns, t)]
    status, doc = http("POST", f"{FAKECORE}/modules/lakehouse/api/dropped-tables/{item['id']}/restore", headers=h)
    assert status == 200, doc
    return item["id"]


def _lifecycle(m: dict) -> tuple:
    return (m["subject"], m["eventType"], m["workspace"], m["publishedBy"])


def test_a_drop_and_a_restore_reach_catalog_under_soft_delete(acme, tokens):
    """ADR 0122 B: a soft-deleted table is no longer listed, so booth-catalog is told table.deleted
    (it tombstones the dataset); a restore brings the same table (same UUID) back, announced as
    table.created, which booth-catalog applies over the tombstone because it is published later."""
    lh = lakehouse("acme", tokens["editor"])
    name = f"ev.soft_{uuid.uuid4().hex[:8]}"
    tbl = lh.create_table(name, [{"id": 1}])
    tid = str(tbl.iceberg.metadata.table_uuid)
    ns, t = name.split(".")
    _events("booth.acme.table.created", lambda ms: _for(tid)(ms))

    lh.drop_table(name)
    deleted = _for(tid)(_events("booth.acme.table.deleted", lambda ms: _for(tid)(ms)))
    assert [_lifecycle(m) for m in deleted] == [("booth.acme.table.deleted", "table.deleted", "acme", "lakehouse")]
    assert deleted[0]["data"] == {"tableUuid": tid, "namespace": ns, "name": t}

    assert _restore(tokens, name) == tid  # Lakekeeper's table id is the Iceberg table UUID
    created = _for(tid)(_events("booth.acme.table.created", lambda ms: len(_for(tid)(ms)) >= 2))
    assert len(created) == 2, f"the restore was not announced: {len(created)} table.created for {tid}"
    again = created[-1]
    assert _lifecycle(again) == ("booth.acme.table.created", "table.created", "acme", "lakehouse")
    assert (again["data"]["namespace"], again["data"]["name"]) == (ns, t)
    assert again["publishedAt"] > deleted[0]["publishedAt"]  # what lets booth-catalog revive the tombstone


def test_a_second_drop_after_a_restore_is_announced_too(acme, tokens):
    """Drop, restore, drop again: the second table.deleted must reach catalog, or it keeps a live
    entry for a table that is gone."""
    lh = lakehouse("acme", tokens["editor"])
    name = f"ev.twice_{uuid.uuid4().hex[:8]}"
    tbl = lh.create_table(name, [{"id": 1}])
    tid = str(tbl.iceberg.metadata.table_uuid)
    _events("booth.acme.table.created", lambda ms: _for(tid)(ms))
    lh.drop_table(name)
    _events("booth.acme.table.deleted", lambda ms: _for(tid)(ms))
    _restore(tokens, name)
    _events("booth.acme.table.created", lambda ms: len(_for(tid)(ms)) >= 2, timeout=30)
    lh.drop_table(name)
    deleted = _for(tid)(_events("booth.acme.table.deleted", lambda ms: len(_for(tid)(ms)) >= 2))
    assert len(deleted) == 2, f"the second drop was not announced: {len(deleted)} table.deleted for {tid}"


CATALOG = os.environ.get("CATALOG_URL", "http://catalog:8080")


def _catalog_shows(tokens, name: str, tid: str) -> bool:
    """Whether booth-catalog (its released image, consuming these events) lists the table: it hides a
    tombstoned dataset from every read, so absent here is what a drop must lead to."""
    h = {"Authorization": "Bearer " + tokens["viewer"], "X-Booth-Workspace": "acme"}
    status, doc = http("GET", f"{CATALOG}/api/datasets?q={name}&limit=50", headers=h)
    assert status == 200, doc
    return any(d.get("format") == "iceberg" and (d.get("table") or {}).get("uuid") == tid for d in doc["items"])


def _until_catalog(tokens, name: str, tid: str, shown: bool, step: str, timeout: float = 30) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _catalog_shows(tokens, name, tid) == shown:
            return
        time.sleep(1)
    pytest.fail(f"after {step}: booth-catalog {'still does not list' if shown else 'still lists'} {name} ({tid})")


def test_drop_restore_drop_restore_inside_two_minutes_leaves_catalog_right_each_time(acme, tokens):
    """ADR 0122 addendum 2026-10-19: the same unchanged table dropped and restored twice inside
    JetStream's 2-minute duplicate window. booth-catalog must hide it after each drop and show it after
    each restore; before every operation had its own message id, the repeats were discarded."""
    deadline = time.time() + 60
    while True:
        status, doc = http("GET", f"{CATALOG}/healthz")
        if status == 200 and (doc.get("checks") or {}).get("tableEventBus") == "subscribed":
            break
        assert time.time() < deadline, f"booth-catalog never subscribed to table.*: {doc}"
        time.sleep(1)

    lh = lakehouse("acme", tokens["editor"])
    name = f"ev.cycle_{uuid.uuid4().hex[:8]}"
    tid = str(lh.create_table(name, [{"id": 1}]).iceberg.metadata.table_uuid)
    _until_catalog(tokens, name, tid, True, "create")

    start = time.time()
    for n in (1, 2):
        lh.drop_table(name)
        _until_catalog(tokens, name, tid, False, f"drop {n}")
        assert _restore(tokens, name) == tid
        _until_catalog(tokens, name, tid, True, f"restore {n}")
    assert time.time() - start < 120, "the cycle took longer than the duplicate window it is meant to exercise"
    assert lh.read(name).num_rows == 1  # and the restored table is the same table, data and all
