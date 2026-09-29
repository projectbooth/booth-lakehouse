"""Layer 3's cluster-free half: this module's real API image against a real Lakekeeper, its real
Postgres and real MinIO, driven through the real client library — no mocked catalog responses.

What is NOT real here, stated plainly: booth-core's broker (a stand-in at a provisional wire shape,
fakecore.py) and booth-storage (MinIO directly, standing in for an s3 backend booth-storage would
front). The credentials the stand-in issues are real MinIO credentials with real, enforced scope.
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import UTC, datetime

import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
import pytest
from pyiceberg.exceptions import ForbiddenError
from pyiceberg.types import StringType

from booth_lakehouse import READ, LakehouseError

from .conftest import API, EDITOR_SUB, FAKECORE, OWNER_SUB, audit, http, lakehouse, s3_root, token

GW = f"{FAKECORE}/modules/lakehouse"
GZIP_MAGIC = bytes([0x1F, 0x8B])


def _h(tok: str, ws: str = "acme", **extra) -> dict:
    return {"Authorization": "Bearer " + tok, "X-Workspace": ws, **extra}


def _name(prefix: str = "t") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


# ---- before any warehouse exists ---------------------------------------------------------------


def test_health_reflects_real_lakekeeper():
    status, doc = http("GET", f"{API}/health")
    assert status == 200 and doc["status"] == "ok" and doc["lakekeeper"] == "ok"


def test_no_warehouse_means_a_clear_error_not_a_crash(tokens):
    with pytest.raises(LakehouseError, match="no lakehouse warehouse yet"):
        lakehouse("acme", tokens["editor"]).warehouse()


def test_only_an_owner_can_create_a_warehouse(tokens):
    status, doc = http("PUT", f"{GW}/api/warehouse", {"backendId": "lake", "path": "lakehouse"}, _h(tokens["editor"]))
    assert status == 403 and "owner" in doc["detail"]


def test_a_backend_that_cant_scope_is_refused_not_widened(tokens):
    """credential-broker.md: a provider that can't scope narrowly refuses. The refusal surfaces as a
    422, and nothing is created."""
    status, doc = http("PUT", f"{GW}/api/warehouse", {"backendId": "files", "path": "lakehouse"}, _h(tokens["owner"]))
    assert status == 422 and "can't issue a credential scoped" in doc["detail"]
    assert http("GET", f"{GW}/api/warehouse", headers=_h(tokens["owner"]))[0] == 404


# ---- the warehouse -----------------------------------------------------------------------------


def test_owner_creates_the_warehouse_with_a_broker_credential(acme):
    assert acme["backendId"] == "lake" and acme["path"] == "lakehouse"
    assert acme["storageRoot"] == "s3://lake/acme-data/lakehouse"  # resolved by the provider, not by us
    assert acme["createdBy"] == OWNER_SUB
    issued = [a for a in audit() if a["workspace"] == "acme" and a["static"]]
    # Requested as the owner, scoped to exactly the chosen {backendId, path}, read-write, static key
    # (Lakekeeper can't hold a session token).
    assert issued[0]["subject"] == OWNER_SUB
    assert issued[0]["scope"] == {"backendId": "lake", "path": "lakehouse", "access": "readwrite"}
    assert "secretAccessKey" not in str(issued)


def test_a_second_warehouse_for_the_same_workspace_is_refused(acme, tokens):
    status, _ = http("PUT", f"{GW}/api/warehouse", {"backendId": "lake", "path": "elsewhere"}, _h(tokens["owner"]))
    assert status == 409


# ---- tables end to end -------------------------------------------------------------------------


def test_create_append_read_round_trip(acme, tokens):
    lh = lakehouse("acme", tokens["editor"])
    name = f"sales.{_name('daily')}"
    first = pd.DataFrame({"day": ["2026-09-01", "2026-09-02"], "amount": [10.5, 20.25], "at": pd.to_datetime(["2026-09-01T10:00:00", "2026-09-02T11:30:00"])})
    tbl = lh.create_table(name, first)
    lh.append(name, pd.DataFrame({"day": ["2026-09-03"], "amount": [5.0], "at": pd.to_datetime(["2026-09-03T09:00:00"])}))

    out = lh.read(name).to_pandas().sort_values("day").reset_index(drop=True)
    assert out["day"].tolist() == ["2026-09-01", "2026-09-02", "2026-09-03"]
    assert out["amount"].tolist() == [10.5, 20.25, 5.0]
    assert [s["operation"] for s in lh.table(name).snapshots()] == ["append", "append"]

    # The data really is in booth-storage's backend, under the warehouse's {backendId, path}.
    backend, path = tbl.location
    assert backend == "lake" and path.startswith("lakehouse/")
    keys = [o["Key"] for o in s3_root().list_objects_v2(Bucket="lake", Prefix=f"acme-data/{path}/").get("Contents", [])]
    assert any(k.endswith(".parquet") for k in keys) and any(".metadata.json" in k for k in keys)
    assert name in lh.tables()


def test_every_storage_credential_is_requested_as_the_caller_and_scoped_to_one_table(acme, tokens):
    lh = lakehouse("acme", tokens["editor"])
    name = f"audit.{_name()}"
    tbl = lh.create_table(name, [{"x": 1}])
    lh.read(name)
    _, path = tbl.location
    mine = [a for a in audit() if a["subject"] == EDITOR_SUB and a["scope"]["path"] == path]
    assert {a["scope"]["access"] for a in mine} == {"read", "readwrite"}
    assert all(not a["static"] for a in mine)  # engines get session credentials


def test_time_travel_reads_an_earlier_snapshot(acme, tokens):
    lh = lakehouse("acme", tokens["editor"])
    name = f"sales.{_name('tt')}"
    tbl = lh.create_table(name, [{"v": 1}])
    first_snapshot = tbl.refresh().snapshots()[0]["snapshotId"]
    lh.append(name, [{"v": 2}])
    assert sorted(lh.read(name).column("v").to_pylist()) == [1, 2]
    assert lh.read(name, snapshot_id=first_snapshot).column("v").to_pylist() == [1]


def test_schema_evolution_keeps_old_rows_readable(acme, tokens):
    lh = lakehouse("acme", tokens["editor"])
    name = f"sales.{_name('evo')}"
    tbl = lh.create_table(name, [{"id": 1}])
    with tbl.iceberg.update_schema() as upd:
        upd.add_column("note", StringType())
    tbl.refresh()
    tbl.append(pa.table({"id": pa.array([2], pa.int64()), "note": ["new"]}))
    rows = sorted(lh.read(name).to_pylist(), key=lambda r: r["id"])
    assert rows == [{"id": 1, "note": None}, {"id": 2, "note": "new"}]


def test_duckdb_can_query_a_table(acme, tokens):
    lh = lakehouse("acme", tokens["editor"])
    name = f"sales.{_name('dk')}"
    lh.create_table(name, [{"k": "a", "v": 1}, {"k": "a", "v": 2}, {"k": "b", "v": 5}])
    con = lh.table(name).duckdb(view="t")
    assert con.sql("select k, sum(v) from t group by k order by k").fetchall() == [("a", 3), ("b", 5)]


def test_table_summary_is_the_catalog_proposal_shape(acme, tokens):
    lh = lakehouse("acme", tokens["editor"])
    ns, t = "cat", _name()
    tbl = lh.create_table(f"{ns}.{t}", [{"id": 1, "label": "x"}])
    status, doc = http("GET", f"{GW}/api/tables/{ns}/{t}", headers=_h(tokens["viewer"]))
    assert status == 200
    assert doc["location"] == {"backendId": tbl.location[0], "path": tbl.location[1]}
    assert [(f["name"], f["type"]) for f in doc["schema"]] == [("id", "long"), ("label", "string")]
    assert len(doc["snapshots"]) == 1 and doc["formatVersion"] in (2, 3)
    listed = http("GET", f"{GW}/api/tables", headers=_h(tokens["viewer"]))[1]["items"]
    assert {"namespace": ns, "name": t} in listed


# ---- roles -------------------------------------------------------------------------------------


def test_viewer_reads_but_cannot_write(acme, tokens):
    name = f"sales.{_name('ro')}"
    lakehouse("acme", tokens["editor"]).create_table(name, [{"v": 1}])
    viewer = lakehouse("acme", tokens["viewer"])
    assert viewer.read(name).column("v").to_pylist() == [1]
    with pytest.raises(LakehouseError) as e:  # the broker refuses a read-write credential
        viewer.append(name, [{"v": 2}])
    assert e.value.status == 403
    with pytest.raises(ForbiddenError):  # and the catalog refuses the create itself
        viewer.create_table(f"sales.{_name('nope')}", [{"v": 1}])
    assert lakehouse("acme", tokens["editor"]).read(name).num_rows == 1


def test_a_forged_role_header_is_rejected(acme, tokens):
    """ADR 0041, calling the module directly (bypassing the gateway) with a stronger X-Booth-Role."""
    status, doc = http("GET", f"{API}/iceberg/v1/config", headers={"Authorization": "Bearer " + tokens["viewer"], "X-Booth-Workspace": "acme", "X-Booth-Role": "editor"})
    assert status == 403 and "exceeds" in doc["error"]["message"]


def test_no_token_is_401(acme):
    status, _ = http("GET", f"{API}/iceberg/v1/config", headers={"X-Booth-Workspace": "acme"})
    assert status == 401


# ---- storage scoping, enforced by the real object store ----------------------------------------


def test_a_table_grant_cannot_reach_another_table_or_write(acme, tokens):
    lh = lakehouse("acme", tokens["editor"])
    a = lh.create_table(f"scope.{_name('a')}", [{"v": 1}])
    b = lh.create_table(f"scope.{_name('b')}", [{"v": 2}])
    grant = lh._grant(a.iceberg.location(), READ)
    fs = pafs.S3FileSystem(access_key=grant.access_key_id, secret_key=grant.secret_access_key, session_token=grant.session_token,
                           endpoint_override=grant.endpoint, scheme="http", region="us-east-1")
    a_key = a.iceberg.metadata_location.removeprefix("s3://")
    b_key = b.iceberg.metadata_location.removeprefix("s3://")
    with fs.open_input_stream(a_key) as f:  # its own table: fine (Lakekeeper gzips metadata)
        assert f.read(2) == GZIP_MAGIC
    with pytest.raises(OSError):  # another table in the same warehouse: refused by MinIO
        fs.open_input_stream(b_key).read()
    with pytest.raises(OSError):  # a read grant can't write
        with fs.open_output_stream(a_key.rsplit("/", 1)[0] + "/intruder.txt") as f:
            f.write(b"x")


# ---- workspace isolation -----------------------------------------------------------------------


def _prefix(tok: str, ws: str) -> str:
    status, doc = http("GET", f"{GW}/iceberg/v1/config?warehouse=anything-else", headers=_h(tok, ws))
    assert status == 200
    return doc["defaults"]["prefix"]


def test_config_pins_the_callers_warehouse_and_hides_lakekeeper(acme, tokens):
    status, doc = http("GET", f"{GW}/iceberg/v1/config?warehouse=booth-ws-beta", headers=_h(tokens["editor"]))
    assert status == 200
    assert "uri" not in doc.get("overrides", {}) and "uri" not in doc.get("defaults", {})
    assert doc["defaults"]["prefix"] == _prefix(tokens["editor"], "acme")


def test_table_responses_carry_no_storage_settings_or_credentials(acme, tokens):
    lh = lakehouse("acme", tokens["editor"])
    ns, t = "leak", _name()
    lh.create_table(f"{ns}.{t}", [{"v": 1}])
    prefix = _prefix(tokens["editor"], "acme")
    status, doc = http("GET", f"{GW}/iceberg/v1/{prefix}/namespaces/{ns}/tables/{t}", headers=_h(tokens["editor"]))
    assert status == 200 and "metadata" in doc
    assert "storage-credentials" not in doc
    assert not [k for k in doc.get("config", {}) if k.startswith("s3.")]
    assert "lakekeeper" not in str(doc.get("config", {}))


def test_catalog_vended_credentials_are_refused(acme, tokens):
    lh = lakehouse("acme", tokens["editor"])
    ns, t = "vend", _name()
    lh.create_table(f"{ns}.{t}", [{"v": 1}])
    prefix = _prefix(tokens["editor"], "acme")
    status, _ = http("GET", f"{GW}/iceberg/v1/{prefix}/namespaces/{ns}/tables/{t}/credentials", headers=_h(tokens["editor"]))
    assert status == 403


def test_another_workspace_cannot_see_or_reach_acme(acme, tokens):
    ns, t = "iso", _name()
    lakehouse("acme", tokens["editor"]).create_table(f"{ns}.{t}", [{"secret": 42}])
    acme_prefix = _prefix(tokens["editor"], "acme")

    lakehouse("beta", tokens["owner"]).create_warehouse("lake", "lakehouse")
    beta = lakehouse("beta", tokens["beta_editor"])
    beta.create_table("mine.t", [{"v": 1}])
    assert beta.tables() == ["mine.t"]  # acme's tables don't show up

    # beta's token, aimed at acme's warehouse id directly: not found, whatever the route.
    for path in (f"/iceberg/v1/{acme_prefix}/namespaces", f"/iceberg/v1/{acme_prefix}/namespaces/{ns}/tables/{t}"):
        status, _ = http("GET", GW + path, headers=_h(tokens["beta_editor"], "beta"))
        assert status == 404, path
    # And a beta token can't act in acme at all.
    assert http("GET", f"{API}/api/tables", headers={"Authorization": "Bearer " + tokens["beta_editor"], "X-Booth-Workspace": "acme"})[0] == 403


def test_beta_warehouse_lives_in_betas_own_backend(acme, tokens):
    status, doc = http("GET", f"{GW}/api/warehouse", headers=_h(tokens["beta_editor"], "beta"))
    assert status == 200 and doc["storageRoot"] == "s3://lake/beta-data/lakehouse"


# ---- credential renewal -----------------------------------------------------------------------


def _warehouse_leases() -> list[dict]:
    return [a for a in audit() if a["workspace"] == "acme" and a["static"]]


def test_zz_warehouse_credential_is_renewed_as_the_modules_own_identity(acme, tokens):
    """The API renews inside a 930 s margin of a 960 s credential, i.e. ~30 s after creation. The
    renewal is requested with a workload token core minted for this module (no person present),
    for exactly the same scope, and Lakekeeper keeps working on the new credential (it writes table
    metadata with its own credential on every create)."""
    first = _warehouse_leases()[0]
    deadline = time.time() + 120
    renewals: list[dict] = []
    while time.time() < deadline and not renewals:
        renewals = [a for a in _warehouse_leases() if a["subject"] == "lakehouse:warehouse-acme"]
        time.sleep(2)
    assert renewals, "no renewal within 120 s"
    assert renewals[0]["scope"] == first["scope"] and renewals[0]["leaseId"] != first["leaseId"]
    name = f"renewed.{_name()}"
    lakehouse("acme", tokens["editor"]).create_table(name, [{"v": 1}])
    assert lakehouse("acme", tokens["editor"]).read(name).num_rows == 1


@pytest.mark.skipif(not os.environ.get("BOOTH_SLOW"), reason="waits ~16 min for a real credential to expire; BOOTH_SLOW=1 (nightly)")
def test_zzz_lakekeeper_outlives_its_original_credential(acme, tokens):
    first = _warehouse_leases()[0]
    wait = first["expiresAt"] - time.time() + 10
    if wait > 0:
        time.sleep(wait)
    assert datetime.now(UTC).timestamp() > first["expiresAt"]
    fresh = token(EDITOR_SUB, "/workspaces/acme/editor")  # the session's tokens expired while waiting
    name = f"outlived.{_name()}"
    lakehouse("acme", fresh).create_table(name, [{"v": 1}])
    assert lakehouse("acme", fresh).read(name).num_rows == 1


def test_zz_seed_for_restart_check(acme, tokens):
    """Seeds test_after_restart.py (hack/kind-integration.sh restarts the module, then checks it)."""
    lakehouse("acme", tokens["editor"]).create_table("persist.check", [{"v": 1}, {"v": 2}, {"v": 3}])
