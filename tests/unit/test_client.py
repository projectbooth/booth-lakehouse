"""The client library's own logic: where a table lives, which credential it asks for, when it renews.
The real read/write path is in tests/integration."""

from __future__ import annotations

import io
import json

import pyarrow as pa
import pytest

from booth_lakehouse import READ, READWRITE, Lakehouse, LakehouseError
from booth_lakehouse.lakehouse import RENEW_MARGIN_SECONDS, _as_arrow, _identifier

from .fakes import FakeBroker

WAREHOUSE = {"backendId": "lake", "path": "lakehouse", "storageRoot": "s3://lake/acme-data/lakehouse"}


class Opener:
    def __init__(self):
        self.requests = []

    def __call__(self, req, timeout=None):
        self.requests.append(req)
        return io.BytesIO(json.dumps(WAREHOUSE).encode())


def lh(broker=None):
    op = Opener()
    return Lakehouse("http://core/modules", "acme", "tok", broker=broker or FakeBroker(), opener=op), op


def test_api_calls_go_through_the_gateway_as_the_caller():
    client, op = lh()
    client.warehouse()
    req = op.requests[0]
    assert req.full_url == "http://core/modules/lakehouse/api/warehouse"
    assert req.get_header("Authorization") == "Bearer tok" and req.get_header("X-workspace") == "acme"


def test_a_table_location_maps_to_backend_and_path():
    client, _ = lh()
    assert client._location_ref("s3://lake/acme-data/lakehouse/0199-uuid") == ("lake", "lakehouse/0199-uuid")
    assert client._location_ref("s3://lake/acme-data/lakehouse") == ("lake", "lakehouse")


@pytest.mark.parametrize("loc", ["s3://lake/acme-data/lakehouse-other/x", "s3://lake/beta-data/lakehouse/x", "s3://other/acme-data/lakehouse/x"])
def test_a_location_outside_the_warehouse_is_never_requested(loc):
    broker = FakeBroker()
    client, _ = lh(broker)
    with pytest.raises(LakehouseError, match="outside"):
        client._grant(loc, READ)
    assert broker.calls == []


def test_grants_are_per_table_per_access_and_cached_until_near_expiry():
    broker = FakeBroker(session_token="ST")

    # FakeBroker's grants are rooted at acme-data/<path>, like the provider's resolution.
    client, _ = lh(broker)
    loc = "s3://lake/acme-data/lakehouse/t1"
    g1 = client._grant(loc, READ)
    assert client._grant(loc, READ) is g1 and len(broker.calls) == 1
    client._grant(loc, READWRITE)
    assert [c["access"] for c in broker.calls] == ["read", "readwrite"]
    assert broker.calls[0]["path"] == "lakehouse/t1" and broker.calls[0]["static"] is False
    object.__setattr__(g1, "expires_at", g1.expires_at - 10**6)  # now inside the renew margin
    assert client._grant(loc, READ) is not g1 and len(broker.calls) == 3
    assert RENEW_MARGIN_SECONDS > 0


def test_no_broker_is_a_clear_error():
    client = Lakehouse("http://core/modules", "acme", "tok", opener=Opener())
    with pytest.raises(LakehouseError, match="ADR 0080"):
        client._grant("s3://lake/acme-data/lakehouse/t1", READ)


def test_from_env_needs_a_token():
    with pytest.raises(LakehouseError, match="token"):
        Lakehouse.from_env({"BOOTH_GATEWAY_URL": "http://core/modules", "BOOTH_WORKSPACE": "acme"})
    c = Lakehouse.from_env({"BOOTH_GATEWAY_URL": "http://core/modules", "BOOTH_WORKSPACE": "acme", "BOOTH_TOKEN": "t", "BOOTH_CREDENTIAL_BROKER_URL": "http://core/api/credentials"})
    assert c.workspace == "acme"


def test_names_and_data_shapes():
    assert _identifier("sales.daily") == ("sales", "daily")
    for bad in ("daily", "a.b.c", ".x", "x."):
        with pytest.raises(LakehouseError):
            _identifier(bad)
    assert _as_arrow([{"a": 1}]).num_rows == 1
    import pandas as pd

    assert _as_arrow(pd.DataFrame({"a": [1, 2]}, index=[5, 6])).column_names == ["a"]  # no index column
    assert _as_arrow(pa.table({"a": [1]})).num_rows == 1
    with pytest.raises(TypeError):
        _as_arrow({"a": 1})
