import pytest

from booth_lakehouse_server.proxy import RULES, authorize, rewrite_config, rewrite_table_response

WH = "58860ce2-bba1-11f1-b083-774406bd0ee6"
T = f"/v1/{WH}/namespaces/sales/tables/daily"


@pytest.mark.parametrize(
    "method,path",
    [("GET", "/v1/config"), ("GET", f"/v1/{WH}/namespaces"), ("HEAD", f"/v1/{WH}/namespaces/sales"), ("GET", T), ("HEAD", T),
     ("GET", f"/v1/{WH}/namespaces/sales/tables"), ("POST", f"{T}/metrics"), ("GET", f"/v1/{WH}/namespaces/sales/views/v")],
)
def test_viewer_can_read(method, path):
    assert authorize(method, path, "viewer", WH).allowed


@pytest.mark.parametrize(
    "method,path",
    [("POST", f"/v1/{WH}/namespaces"), ("DELETE", f"/v1/{WH}/namespaces/sales"), ("POST", f"/v1/{WH}/namespaces/sales/properties"),
     ("POST", f"/v1/{WH}/namespaces/sales/tables"), ("POST", T), ("DELETE", T), ("POST", f"/v1/{WH}/tables/rename"),
     ("POST", f"/v1/{WH}/transactions/commit"), ("POST", f"/v1/{WH}/namespaces/sales/views")],
)
def test_writes_need_editor(method, path):
    d = authorize(method, path, "viewer", WH)
    assert (d.allowed, d.status) == (False, 403)
    assert authorize(method, path, "editor", WH).allowed
    assert authorize(method, path, "owner", WH).allowed


def test_another_warehouse_is_not_found_for_every_route():
    other = "00000000-0000-0000-0000-000000000001"
    for method, path in [("GET", f"/v1/{other}/namespaces"), ("POST", f"/v1/{other}/namespaces/x/tables"), ("GET", f"/v1/{other}/namespaces/x/tables/y")]:
        d = authorize(method, path, "owner", WH)
        assert (d.allowed, d.status) == (False, 404), path


@pytest.mark.parametrize(
    "method,path,status",
    [("GET", f"{T}/credentials", 403), ("POST", f"/v1/{WH}/namespaces/sales/register", 403), ("POST", "/v1/oauth/tokens", 404),
     ("GET", "/management/v1/warehouse", 404), ("PUT", T, 404), ("GET", f"/v1/{WH}/namespaces/../../x", 400),
     ("GET", f"/v1/{WH}/namespaces/%2e%2e/x", 400), ("GET", f"/v1/{WH}//namespaces", 400), ("GET", f"/v1/{WH}/something-new", 404)],
)
def test_refused_for_everyone(method, path, status):
    d = authorize(method, path, "owner", WH)
    assert (d.allowed, d.status) == (False, status)


def test_an_encoded_slash_stays_inside_its_segment():
    # The raw path is what gets forwarded, so %2F can't smuggle an extra segment past the rules.
    assert authorize("GET", f"/v1/{WH}/namespaces/a%2Fb/tables/c", "viewer", WH).allowed
    assert not authorize("GET", f"/v1/{WH}%2Fnamespaces", "viewer", WH).allowed


def test_config_is_flagged_so_the_warehouse_can_be_forced():
    assert authorize("GET", "/v1/config", "viewer", WH).is_config


def test_every_rule_is_either_gated_or_deliberately_refused():
    assert {r.role for r in RULES} <= {"viewer", "editor", ""}


def test_config_rewrite_drops_uri_everywhere():
    out = rewrite_config({"overrides": {"uri": "http://lakekeeper:8181/catalog", "x": "1"}, "defaults": {"prefix": WH, "uri": "y"}, "endpoints": []})
    assert out == {"overrides": {"x": "1"}, "defaults": {"prefix": WH}, "endpoints": []}


def test_table_rewrite_strips_storage_settings_and_credentials():
    body = {
        "metadata-location": "s3://lake/x/metadata/00001.gz.metadata.json",
        "metadata": {"format-version": 2},
        "config": {"s3.endpoint": "http://minio:9000/", "s3.access-key-id": "AK", "region": "us-east-1", "client.region": "us-east-1",
                   "adls.sas-token.x": "t", "gcs.oauth2.token": "g", "token": "t", "rest-page-size": "100"},
        "storage-credentials": [{"prefix": "s3://", "config": {"s3.secret-access-key": "S"}}],
    }
    out = rewrite_table_response(body)
    assert out["config"] == {"rest-page-size": "100"}
    assert "storage-credentials" not in out and out["metadata"] == {"format-version": 2}
