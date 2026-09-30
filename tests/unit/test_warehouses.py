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


# ---- renewal on behalf of a current owner (ADR 0088) ---------------------------------------------


def renew_env(refuse=(), broker_403_for=()):
    """Warehouses whose minter refuses some owners, and whose broker refuses tokens minted for others."""
    from booth_lakehouse_server.warehouses import OwnerRefused

    now = [1_000_000.0]
    lk = FakeLakekeeper()
    b = FakeBroker()
    minted: list[str] = []

    def workload(ws, owner):
        minted.append(owner)
        if owner in refuse:
            raise OwnerRefused(f"not current: {owner}", 403)
        return f"token-for-{owner}"

    def broker_for(tok):
        if tok().removeprefix("token-for-") in broker_403_for:
            return FakeBroker(fail=BrokerError("viewer can't readwrite", 403))
        return b

    w = Warehouses(MemoryStore(), Lakekeeper("http://lk", transport=lk.transport()), broker_for, workload, 3600, 900)
    w.clock = lambda: now[0]
    first = w.create(OWNER, "lake", "lakehouse")
    now[0] = first.credential_expires_at  # due
    return w, minted, now, first


def person(sub, role="editor", ws="acme"):
    return Identity(sub, ws, role, "t")


def test_a_current_member_stands_in_when_the_creator_isnt_current():
    w, minted, now, first = renew_env(refuse={"alice"})
    w.note_member(person("bob"))
    now[0] += 1
    w.note_member(person("carol", "owner"))  # seen more recently than bob
    assert w.renew_due() == []
    assert minted == ["alice", "carol"]  # creator first, then most recently seen
    assert w.get("acme").lease_id != first.lease_id


def test_a_member_whose_live_role_dropped_is_skipped():
    """Core caps the minted token at the owner's live role; a demoted member gets a viewer token,
    which the broker refuses for read-write. That's "not this person", so try the next."""
    w, minted, now, _ = renew_env(refuse={"alice"}, broker_403_for={"bob"})
    w.note_member(person("dave"))
    now[0] += 1
    w.note_member(person("bob"))
    assert w.renew_due() == []
    assert minted == ["alice", "bob", "dave"]


def test_nobody_current_is_a_failure_and_attempts_are_bounded():
    from booth_lakehouse_server.warehouses import MAX_RENEWAL_CANDIDATES

    w, minted, now, first = renew_env(refuse={"alice"} | {f"u{i}" for i in range(30)})
    for i in range(30):
        now[0] += 1
        w.note_member(person(f"u{i}"))
    failures = w.renew_due()
    assert [ws for ws, _ in failures] == ["acme"] and "no current editor/owner" in failures[0][1]
    assert len(minted) == MAX_RENEWAL_CANDIDATES
    assert w.get("acme").lease_id == first.lease_id


def test_an_outage_is_not_mistaken_for_a_refusal():
    """Core unreachable (503) must not burn through every member; it stops and retries next pass."""
    w, minted, now, _ = renew_env()
    w.note_member(person("bob"))

    def down(ws, owner):
        minted.append(owner)
        raise WarehouseError("core down", 503)

    w.workload_token = down
    minted.clear()
    assert [ws for ws, _ in w.renew_due()] == ["acme"]
    assert minted == ["alice"]


def test_only_editors_and_owners_who_are_people_are_recorded_and_writes_are_throttled():
    w, _, now, _ = renew_env()
    w.note_member(person("vic", "viewer"))
    w.note_member(Identity("job:42", "acme", "editor", "t"))  # a pipeline run is never an owner
    w.note_member(person("bob"))
    assert w.store.members("acme") == ["bob"]
    seen = w.store._members["acme"]["bob"]
    now[0] += 10
    w.note_member(person("bob"))
    assert w.store._members["acme"]["bob"] == seen  # within the throttle window: no write
    now[0] += 600
    w.note_member(person("bob"))
    assert w.store._members["acme"]["bob"] > seen


def test_core_refusing_the_owner_is_an_owner_refusal():
    import io
    import urllib.error

    from booth_lakehouse_server.warehouses import OwnerRefused, WorkloadTokens

    def opener(status):
        def _open(req, timeout=None):
            raise urllib.error.HTTPError(req.full_url, status, "x", {}, io.BytesIO(b"{}"))
        return _open

    with pytest.raises(OwnerRefused):
        WorkloadTokens("http://core/api/internal/workload-tokens", "cred", opener(403))("acme", "alice")
    with pytest.raises(WarehouseError) as e:
        WorkloadTokens("http://core/api/internal/workload-tokens", "cred", opener(503))("acme", "alice")
    assert not isinstance(e.value, OwnerRefused)
