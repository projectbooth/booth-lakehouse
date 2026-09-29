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

from .conftest import API, http, lakehouse

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
