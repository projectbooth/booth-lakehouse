import logging

import pytest

from booth_lakehouse.broker import BrokerError
from booth_lakehouse_server.identity import Forbidden, Identity
from booth_lakehouse_server.lakekeeper import Lakekeeper, LakekeeperError, storage_credential
from booth_lakehouse_server.store import MemoryStore
from booth_lakehouse_server.warehouses import WarehouseError, Warehouses, normalize_path

from .fakes import FakeBroker, FakeLakekeeper, grant

OWNER = Identity("alice", "acme", "owner", "alice-token")


def make(broker=None, minted=None, clock=None):
    lk = FakeLakekeeper()
    b = broker or FakeBroker()
    tokens_seen: list[str] = []

    def broker_for(tok):
        tokens_seen.append(tok())
        return b

    def workload(ws, owner):
        if minted is not None:
            minted.append((ws, owner))
        return "workload-token"

    w = Warehouses(MemoryStore(), Lakekeeper("http://lk", transport=lk.transport()), broker_for, workload, 3600, 900)
    if clock:
        w.clock = clock
    return w, lk, b, tokens_seen


def test_owner_creates_a_warehouse_from_a_static_readwrite_grant_requested_as_themselves():
    w, lk, b, seen = make()
    binding = w.create(OWNER, "lake", "/lakehouse/")
    assert b.calls == [{"workspace": "acme", "backend": "lake", "path": "lakehouse", "access": "readwrite", "ttl": 3600, "static": True}]
    assert seen == ["alice-token"]  # the broker's audit names the owner
    (wh,) = lk.warehouses.values()
    assert wh["warehouse-name"] == "booth-ws-acme"
    prof = wh["storage-profile"]
    assert (prof["bucket"], prof["key-prefix"], prof["sts-enabled"], prof["remote-signing-enabled"]) == ("lake", "acme-data/lakehouse", False, False)
    assert wh["storage-credential"]["access-key-id"] == b.issued[0].access_key_id
    assert binding.storage_root == "s3://lake/acme-data/lakehouse" and binding.path == "lakehouse"
    assert lk.bootstrapped


def test_non_owners_are_refused_before_any_credential_is_requested():
    w, _, b, _ = make()
    with pytest.raises(Forbidden):
        w.create(Identity("bob", "acme", "editor", "t"), "lake", "x")
    assert b.calls == []


@pytest.mark.parametrize("path", ["", "/", "a/../b", "./x"])
def test_bad_paths_are_refused(path):
    with pytest.raises(WarehouseError) as e:
        normalize_path(path)
    assert e.value.status == 400


def test_second_warehouse_is_a_conflict():
    w, _, _, _ = make()
    w.create(OWNER, "lake", "lakehouse")
    with pytest.raises(WarehouseError) as e:
        w.create(OWNER, "lake", "other")
    assert e.value.status == 409


@pytest.mark.parametrize("err,status", [
    (BrokerError("x", 422, scope_unsupported=True), 422),
    (BrokerError("x", 403), 403),
    (BrokerError("x", 0), 502),
])
def test_broker_refusals_map_to_clear_statuses_and_create_nothing(err, status):
    w, lk, _, _ = make(broker=FakeBroker(fail=err))
    with pytest.raises(WarehouseError) as e:
        w.create(OWNER, "files", "x")
    assert e.value.status == status
    assert lk.warehouses == {} and w.get("acme") is None


def test_no_broker_configured_is_503():
    w, _, _, _ = make()
    w.broker_for = None
    with pytest.raises(WarehouseError) as e:
        w.create(OWNER, "lake", "x")
    assert e.value.status == 503 and "ADR 0080" in str(e.value)


def test_overlapping_storage_is_a_conflict():
    w, lk, _, _ = make()
    lk.fail_create = (400, "CreateWarehouseStorageProfileOverlap")
    with pytest.raises(WarehouseError) as e:
        w.create(OWNER, "lake", "x")
    assert e.value.status == 409 and "overlaps" in str(e.value)


def test_lakekeeper_cannot_be_given_a_session_token_credential():
    with pytest.raises(LakekeeperError, match="session-token"):
        storage_credential(grant(session_token="ST"))


def test_renewal_uses_a_workload_token_for_the_creator_and_updates_lakekeeper():
    now = [1_000_000.0]
    minted: list = []
    w, lk, b, seen = make(minted=minted, clock=lambda: now[0])
    first = w.create(OWNER, "lake", "lakehouse")
    b.issued.clear()
    now[0] = first.credential_expires_at - 100  # inside the 900 s margin
    assert w.renew_due() == []
    assert minted == [("acme", "alice")] and seen[-1] == "workload-token"
    (wh,) = lk.warehouses.values()
    assert wh["storage-credential"]["access-key-id"] == b.issued[0].access_key_id
    assert w.get("acme").lease_id == b.issued[0].lease_id != first.lease_id


def test_not_yet_due_is_left_alone_and_one_failure_doesnt_stop_others():
    w, _, b, _ = make()
    w.create(OWNER, "lake", "lakehouse")
    assert w.renew_due() == [] and len(b.calls) == 1  # far from expiry
    w.create(Identity("zed", "beta", "owner", "zt"), "lake", "beta-lh")
    w.renew_margin_seconds = 10**9
    b.fail = BrokerError("down", 0)
    failures = w.renew_due()
    assert sorted(ws for ws, _ in failures) == ["acme", "beta"]


def test_no_credential_value_is_ever_logged(caplog):
    caplog.set_level(logging.DEBUG)
    now = [1_000_000.0]
    w, _, b, _ = make(clock=lambda: now[0])
    first = w.create(OWNER, "lake", "lakehouse")
    now[0] = first.credential_expires_at
    w.renew_due()
    text = caplog.text
    assert "warehouse created" in text and "renewed" in text
    for g in b.issued:
        assert g.secret_access_key not in text and g.access_key_id not in text
    assert "alice-token" not in text


def test_lakekeeper_unreachable_during_renewal_is_a_failure_not_a_crash():
    import httpx

    now = [1_000_000.0]
    w, _, _, _ = make(clock=lambda: now[0])
    first = w.create(OWNER, "lake", "lakehouse")

    def down(request):
        raise httpx.ConnectError("connection refused")

    w.lakekeeper = Lakekeeper("http://lk", transport=httpx.MockTransport(down))
    now[0] = first.credential_expires_at
    assert [ws for ws, _ in w.renew_due()] == ["acme"]
    assert w.get("acme").lease_id == first.lease_id  # untouched, retried next pass
