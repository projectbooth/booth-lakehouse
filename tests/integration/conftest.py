"""Shared fixtures for the real-dependency suite (see hack/docker-compose.yml). Skipped unless
BOOTH_INTEGRATION=1, which only the compose ``tests`` service sets."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

import boto3
import pytest

# Only the compose `tests` service and the kind Job set this; anywhere else there's nothing to talk to.
if os.environ.get("BOOTH_INTEGRATION") != "1":
    collect_ignore_glob = ["test_*.py"]

from booth_lakehouse import Lakehouse  # noqa: E402

API = os.environ.get("API_URL", "http://api:8080")
FAKECORE = os.environ.get("FAKECORE_URL", "http://fakecore:9090")
MINIO = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

OWNER_SUB, EDITOR_SUB, VIEWER_SUB = "alice-owner", "bob-editor", "carol-viewer"


def http(method: str, url: str, body=None, headers=None) -> tuple[int, dict | str]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            status = resp.status
    except urllib.error.HTTPError as e:
        raw, status = e.read(), e.code
    try:
        return status, json.loads(raw) if raw else {}
    except ValueError:
        return status, raw.decode("utf-8", "replace")


def token(sub: str, *groups: str) -> str:
    # 2 h: outlives the nightly test that waits ~16 min for a real credential to expire.
    status, doc = http("POST", f"{FAKECORE}/test/token", {"sub": sub, "groups": list(groups), "ttl": 7200})
    assert status == 200, doc
    return doc["token"]


def audit() -> list[dict]:
    return http("GET", f"{FAKECORE}/test/audit")[1]["items"]


def lakehouse(workspace: str, tok: str) -> Lakehouse:
    return Lakehouse(f"{FAKECORE}/modules", workspace, tok, broker_url=f"{FAKECORE}/api/credentials")


def s3_root():
    return boto3.client("s3", endpoint_url=MINIO, aws_access_key_id="booth-test", aws_secret_access_key="booth-test-secret", region_name="us-east-1")


@pytest.fixture(scope="session")
def tokens() -> dict[str, str]:
    return {
        "owner": token(OWNER_SUB, "/workspaces/acme/owner", "/workspaces/beta/owner"),
        "editor": token(EDITOR_SUB, "/workspaces/acme/editor"),
        "viewer": token(VIEWER_SUB, "/workspaces/acme/viewer"),
        "beta_editor": token("dave-beta", "/workspaces/beta/editor"),
    }


@pytest.fixture(scope="session")
def acme(tokens) -> dict:
    """acme's warehouse, created once per stack by its owner (the suite needs a fresh stack)."""
    lh = lakehouse("acme", tokens["owner"])
    status, _ = http("GET", f"{FAKECORE}/modules/lakehouse/api/warehouse", headers={"Authorization": "Bearer " + tokens["owner"], "X-Workspace": "acme"})
    assert status == 404, "run against a fresh stack: docker compose -f hack/docker-compose.yml down -v, then up again"
    return lh.create_warehouse("lake", "lakehouse")
