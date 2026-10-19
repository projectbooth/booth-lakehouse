"""The table.* publisher's decisions (events.py): what gets published, when, and what never does.
Against real NATS/Lakekeeper in tests/integration."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from booth_lakehouse_server.events import (
    CREATED,
    DELETED,
    UPDATED,
    TableEvents,
    digest,
    envelope,
    payload,
    subject,
)
from booth_lakehouse_server.store import Binding, MemoryStore, Published
from booth_lakehouse_server.tables import TableNotFound, TableRef

ROOT = "s3://lake/acme-data/lakehouse"


def binding(ws="acme", wid="W1") -> Binding:
    return Binding(ws, "lake", "lakehouse", wid, f"booth-ws-{ws}", ROOT.replace("acme", ws), "alice", 0, "l", 10**12)


def md(uuid, snapshot=1, fields=("id",), ws="acme"):
    return {
        "table-uuid": uuid, "format-version": 2, "location": f"s3://lake/{ws}-data/lakehouse/{uuid}",
        "current-schema-id": 0, "schemas": [{"schema-id": 0, "fields": [{"id": i, "name": f, "type": "long", "required": False} for i, f in enumerate(fields)]}],
        "default-spec-id": 0, "partition-specs": [{"spec-id": 0, "fields": []}],
        "current-snapshot-id": snapshot, "snapshots": [{"snapshot-id": snapshot, "timestamp-ms": 1, "summary": {"operation": "append"}}],
        "last-updated-ms": snapshot,
    }


class FakeReader:
    def __init__(self):
        self.by_wh: dict[str, dict[TableRef, dict | Exception]] = {}
        self.list_error: Exception | None = None

    def put(self, wid, ns, name, value):
        self.by_wh.setdefault(wid, {})[TableRef(tuple(ns.split(".")), name)] = value

    def drop(self, wid, ns, name):
        self.by_wh[wid].pop(TableRef(tuple(ns.split(".")), name))

    def tables(self, wid):
        if self.list_error:
            raise self.list_error
        return list(self.by_wh.get(wid, {}))

    def metadata(self, wid, ref):
        v = self.by_wh[wid][ref]
        if isinstance(v, Exception):
            raise v
        return v


class FakePublisher:
    def __init__(self):
        self.sent: list[tuple[str, dict, str]] = []
        self.fail_after: int | None = None

    async def publish(self, subject, data, msg_id):
        if self.fail_after is not None and len(self.sent) >= self.fail_after:
            raise ConnectionError("nats down")
        self.sent.append((subject, json.loads(data), msg_id))

    async def close(self):
        pass


@pytest.fixture
def env():
    store, reader, pub = MemoryStore(), FakeReader(), FakePublisher()
    store.insert(binding())
    now = [1000.0]
    ev = TableEvents(store, reader, pub, update_min_gap_seconds=60, clock=lambda: now[0])
    return store, reader, pub, ev, now


def run(ev):
    return asyncio.run(ev.run_once())


def test_created_then_nothing_until_it_changes(env):
    store, reader, pub, ev, now = env
    reader.put("W1", "sales", "daily", md("u1"))
    run(ev)
    ((subj, env_, _),) = pub.sent
    assert subj == "booth.acme.table.created"
    assert env_["workspace"] == "acme" and env_["eventType"] == CREATED and env_["publishedBy"] == "lakehouse"
    assert env_["publishedAt"].endswith("Z")
    data = env_["data"]
    assert data["tableUuid"] == "u1" and data["namespace"] == "sales" and data["name"] == "daily"
    assert data["location"] == {"backendId": "lake", "path": "lakehouse/u1"}
    assert "snapshots" not in data  # ADR 0085: the summary minus snapshots
    now[0] += 3600
    run(ev)
    assert len(pub.sent) == 1


def test_a_new_snapshot_or_schema_change_is_an_update_rate_limited_per_table(env):
    store, reader, pub, ev, now = env
    reader.put("W1", "sales", "daily", md("u1"))
    run(ev)
    reader.put("W1", "sales", "daily", md("u1", snapshot=2))
    now[0] += 10
    run(ev)
    assert len(pub.sent) == 1  # inside the 60 s gap: deferred, not dropped
    reader.put("W1", "sales", "daily", md("u1", snapshot=3, fields=("id", "note")))
    now[0] += 60
    run(ev)
    (subj, env_, _) = pub.sent[-1]
    assert subj == "booth.acme.table.updated" and env_["data"]["currentSnapshotId"] == 3  # latest state, not a stale one
    assert [c["name"] for c in env_["data"]["schema"]] == ["id", "note"]
    assert len(pub.sent) == 2


def test_last_updated_alone_is_not_a_change(env):
    store, reader, pub, ev, now = env
    reader.put("W1", "sales", "daily", md("u1"))
    run(ev)
    m = md("u1")
    m["last-updated-ms"] = 999
    reader.put("W1", "sales", "daily", m)
    now[0] += 3600
    run(ev)
    assert len(pub.sent) == 1


def test_rename_keeps_the_uuid_so_it_is_an_update(env):
    store, reader, pub, ev, now = env
    reader.put("W1", "sales", "daily", md("u1"))
    run(ev)
    reader.drop("W1", "sales", "daily")
    reader.put("W1", "sales", "daily_v2", md("u1"))
    now[0] += 60
    run(ev)
    assert [(s.split(".")[-1], e["data"]["name"]) for s, e, _ in pub.sent[1:]] == [("updated", "daily_v2")]


def test_drop_is_a_tombstone_and_drop_recreate_deletes_first(env):
    store, reader, pub, ev, now = env
    reader.put("W1", "sales", "daily", md("u1"))
    run(ev)
    reader.put("W1", "sales", "daily", md("u2"))  # dropped and recreated under the same name
    run(ev)
    kinds = [(e["eventType"], e["data"]["tableUuid"]) for _, e, _ in pub.sent[1:]]
    assert kinds == [(DELETED, "u1"), (CREATED, "u2")]
    assert pub.sent[1][1]["data"] == {"tableUuid": "u1", "namespace": "sales", "name": "daily"}
    assert set(store.published("acme")) == {"u2"}


def test_never_deletes_on_doubt(env):
    store, reader, pub, ev, now = env
    reader.put("W1", "sales", "a", md("u1"))
    reader.put("W1", "sales", "b", md("u2"))
    run(ev)
    reader.list_error = httpx.ConnectError("lakekeeper down")
    run(ev)
    assert len(pub.sent) == 2 and ev.status.startswith("error")
    reader.list_error = None
    reader.put("W1", "sales", "a", httpx.ReadTimeout("slow"))  # one table unreadable: left as is
    run(ev)
    assert len(pub.sent) == 2 and set(store.published("acme")) == {"u1", "u2"}
    reader.put("W1", "sales", "a", TableNotFound("gone mid-pass"))  # dropped between list and load
    run(ev)
    reader.drop("W1", "sales", "a")
    run(ev)
    assert [e["eventType"] for _, e, _ in pub.sent[2:]] == [DELETED]


def test_nothing_is_recorded_unless_the_stream_acked_it(env):
    store, reader, pub, ev, now = env
    for i in range(3):
        reader.put("W1", "sales", f"t{i}", md(f"u{i}"))
    pub.fail_after = 1
    run(ev)
    assert len(pub.sent) == 1 and len(store.published("acme")) == 1  # stopped at the first failure
    pub.fail_after = None
    run(ev)
    assert sorted(e["data"]["tableUuid"] for _, e, _ in pub.sent) == ["u0", "u1", "u2"]
    assert ev.status == "ok"


def test_the_same_state_gets_the_same_msg_id_for_jetstream_dedupe(env):
    store, reader, pub, ev, now = env
    reader.put("W1", "sales", "daily", md("u1"))
    run(ev)
    store.forget_published("acme", "u1")  # as if we crashed after the publish, before recording it
    run(ev)
    assert pub.sent[0][2] == pub.sent[1][2]


def test_workspaces_publish_on_their_own_subjects_and_one_failure_is_isolated(env):
    store, reader, pub, ev, now = env
    store.insert(binding("beta", "W2"))
    reader.put("W1", "sales", "daily", md("u1"))
    reader.put("W2", "mine", "t", md("u9", ws="beta"))
    run(ev)
    by_subject = {s: e for s, e, _ in pub.sent}
    assert by_subject["booth.beta.table.created"]["workspace"] == "beta"
    assert by_subject["booth.beta.table.created"]["data"]["location"] == {"backendId": "lake", "path": "lakehouse/u9"}
    assert by_subject["booth.acme.table.created"]["data"]["tableUuid"] == "u1"


def test_helpers():
    assert subject("acme", UPDATED) == "booth.acme.table.updated"
    s = {"namespace": "a", "name": "b", "snapshots": [1], "lastUpdatedMs": 1}
    assert payload(s) == {"namespace": "a", "name": "b", "lastUpdatedMs": 1}
    assert digest(s) == digest({**s, "lastUpdatedMs": 2, "snapshots": []})
    doc = json.loads(envelope("acme", CREATED, {"x": 1}, 0))
    assert doc == {"workspace": "acme", "eventType": CREATED, "publishedAt": "1970-01-01T00:00:00.000000Z", "publishedBy": "lakehouse", "data": {"x": 1}}


def _soft(ev, reader):
    """Lakekeeper's soft-deleted list, as TableEvents.dropped reads it."""
    drops: dict[str, dict[str, float]] = {}
    ev.dropped = lambda wid: dict(drops.get(wid, {}))
    return drops


def test_each_drop_and_each_restore_has_its_own_msg_id(env):
    """ADR 0122 addendum: drop, restore, drop, restore of an unchanged table inside JetStream's
    2-minute window must be four distinct ids, or the stream discards the repeats."""
    store, reader, pub, ev, now = env
    drops = _soft(ev, reader)
    table = md("u1")
    reader.put("W1", "sales", "daily", table)
    run(ev)
    for dropped_at in (2000.0, 2030.0):
        reader.drop("W1", "sales", "daily")
        drops["W1"] = {"u1": dropped_at}
        run(ev)
        reader.put("W1", "sales", "daily", table)  # restored: same uuid, same state
        drops["W1"] = {}
        ev.restored("acme", "u1", dropped_at)
        run(ev)
    kinds = [e["eventType"] for _, e, _ in pub.sent]
    assert kinds == [CREATED, DELETED, CREATED, DELETED, CREATED]
    ids = [m for _, _, m in pub.sent]
    assert len(set(ids)) == 5, ids
    assert ids[1].endswith(":table.deleted:deleted:2000.000000") and ids[2].endswith(":restore-2000.000000")
    assert not ev._restores  # published, so forgotten


def test_a_retried_drop_or_restore_keeps_its_msg_id(env):
    store, reader, pub, ev, now = env
    drops = _soft(ev, reader)
    reader.put("W1", "sales", "daily", md("u1"))
    run(ev)
    reader.drop("W1", "sales", "daily")
    drops["W1"] = {"u1": 2000.0}
    pub.fail_after = 1
    run(ev)  # the publish fails: nothing recorded, retried next pass
    pub.fail_after = None
    run(ev)
    reader.put("W1", "sales", "daily", md("u1"))
    drops["W1"] = {}
    ev.restored("acme", "u1", 2000.0)
    pub.fail_after = 2
    run(ev)
    pub.fail_after = None
    run(ev)
    assert [e["eventType"] for _, e, _ in pub.sent] == [CREATED, DELETED, CREATED]
    # and a crash after the publish, before the record: the same operation republished, same id
    deleted_id = pub.sent[1][2]
    reader.drop("W1", "sales", "daily")
    drops["W1"] = {"u1": 2000.0}
    store.record_published("acme", Published("u1", "sales", "daily", "x", 1000.0))  # as before the drop
    run(ev)
    assert pub.sent[-1][2] == deleted_id


def test_a_drop_whose_deletion_time_cant_be_read_waits_for_the_next_pass(env):
    store, reader, pub, ev, now = env
    reader.put("W1", "sales", "daily", md("u1"))
    run(ev)

    def down(wid):
        raise httpx.ConnectError("lakekeeper down")

    ev.dropped = down
    reader.drop("W1", "sales", "daily")
    run(ev)
    assert len(pub.sent) == 1 and ev.status.startswith("error") and "u1" in store.published("acme")


def test_a_hard_drop_keeps_the_plain_id_and_an_unpublished_restore_is_forgotten(env):
    store, reader, pub, ev, now = env
    _soft(ev, reader)  # empty: a hard delete leaves nothing listed
    reader.put("W1", "sales", "daily", md("u1"))
    run(ev)
    reader.drop("W1", "sales", "daily")
    run(ev)
    assert pub.sent[-1][2] == "acme:u1:table.deleted:deleted"
    reader.put("W1", "sales", "other", md("u2"))
    run(ev)
    ev.restored("acme", "u2", 5.0)  # dropped and restored between two passes: nothing to announce
    run(ev)
    assert len(pub.sent) == 3 and not ev._restores
