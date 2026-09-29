"""Run by hack/kind-integration.sh after restarting both Deployments: a warehouse and its tables
survive a restart of the API and of Lakekeeper (state lives in the core-provisioned Postgres and in
object storage, not in either pod). Seeded by test_real_stack.py::test_zz_seed_for_restart_check."""

from __future__ import annotations

import os

import pytest

if os.environ.get("BOOTH_AFTER_RESTART") != "1":
    pytest.skip("only after hack/kind-integration.sh restarts the module", allow_module_level=True)

from .conftest import lakehouse as make  # noqa: E402
from .conftest import token  # noqa: E402


def test_warehouse_and_table_survive_a_restart():
    tok = token("bob-editor", "/workspaces/acme/editor")
    lh = make("acme", tok)
    assert lh.warehouse()["storageRoot"] == "s3://lake/acme-data/lakehouse"
    assert sorted(lh.read("persist.check").column("v").to_pylist()) == [1, 2, 3]
    lh.append("persist.check", [{"v": 4}])  # and still writable: Lakekeeper still holds a live credential
    assert lh.read("persist.check").num_rows == 4
