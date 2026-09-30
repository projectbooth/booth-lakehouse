import io
import json
import urllib.error

import pytest

from booth_lakehouse.broker import BrokerError, HttpBroker


class Opener:
    def __init__(self, status=201, doc=None):
        self.status, self.doc, self.requests = status, doc, []

    def __call__(self, req, timeout=None):
        self.requests.append(req)
        if self.status >= 400:
            raise urllib.error.HTTPError(req.full_url, self.status, "x", {}, io.BytesIO(json.dumps(self.doc).encode()))
        return io.BytesIO(json.dumps(self.doc).encode())


def response(**over):
    doc = {
        "leaseId": "l-1", "kind": "s3", "expiresAt": "2026-09-28T12:00:00Z",
        "scope": {"backendId": "lake", "path": "lakehouse/t1", "access": "read"},
        "credential": {"accessKeyId": "AK", "secretAccessKey": "SK", "sessionToken": "ST", "endpoint": "http://minio:9000",
                       "region": "us-east-1", "bucket": "lake", "keyPrefix": "acme/lakehouse/t1", "pathStyle": True},
    }
    doc.update(over)
    return doc


def test_request_is_made_as_the_caller_with_mandatory_ttl():
    op = Opener(doc=response())
    g = HttpBroker("http://core/api/credentials", lambda: "caller-token", op).issue_s3("acme", "lake", "/lakehouse/t1/", "read", 900)
    req = op.requests[0]
    assert req.get_header("Authorization") == "Bearer caller-token" and req.get_header("X-workspace") == "acme"
    assert json.loads(req.data) == {
        "kind": "s3",
        "access": "read",  # top-level, as the broker authorizes on it (ADR 0088)
        "scope": {"backendId": "lake", "path": "/lakehouse/t1/", "access": "read"},
        "ttlSeconds": 900,
        "options": {"sessionToken": "allowed"},
    }
    assert (g.bucket, g.key_prefix, g.session_token, g.root_uri) == ("lake", "acme/lakehouse/t1", "ST", "s3://lake/acme/lakehouse/t1")
    assert g.fileio_properties()["s3.session-token"] == "ST"


def test_static_key_is_requested_explicitly():
    op = Opener(doc=response(scope={"backendId": "lake", "path": "lakehouse/t1", "access": "readwrite"}))
    HttpBroker("http://core/api/credentials", lambda: "t", op).issue_s3("acme", "lake", "lakehouse/t1", "readwrite", 60, static_key=True)
    sent = json.loads(op.requests[0].data)
    assert sent["options"] == {"sessionToken": "forbidden"} and sent["access"] == "readwrite"


@pytest.mark.parametrize("scope", [
    {"backendId": "lake", "path": "lakehouse", "access": "read"},          # wider path
    {"backendId": "other", "path": "lakehouse/t1", "access": "read"},      # different backend
    {"backendId": "lake", "path": "lakehouse/t1", "access": "readwrite"},  # more access than asked
])
def test_a_grant_for_a_different_scope_is_refused(scope):
    with pytest.raises(BrokerError, match="different scope"):
        HttpBroker("http://core/api/credentials", lambda: "t", Opener(doc=response(scope=scope))).issue_s3("acme", "lake", "lakehouse/t1", "read", 60)


def test_wrong_kind_and_malformed_responses_are_refused():
    with pytest.raises(BrokerError, match="postgres"):
        HttpBroker("http://core/api/credentials", lambda: "t", Opener(doc=response(kind="postgres"))).issue_s3("acme", "lake", "lakehouse/t1", "read", 60)
    partial = {"leaseId": "x", "kind": "s3", "scope": {"backendId": "lake", "path": "lakehouse/t1", "access": "read"}}
    with pytest.raises(BrokerError, match="malformed"):
        HttpBroker("http://core/api/credentials", lambda: "t", Opener(doc=partial)).issue_s3("acme", "lake", "lakehouse/t1", "read", 60)


def test_scope_not_supported_is_distinguishable():
    op = Opener(422, {"error": "scope_not_supported", "message": "filesystem backend"})
    with pytest.raises(BrokerError) as e:
        HttpBroker("http://core/api/credentials", lambda: "t", op).issue_s3("acme", "files", "x", "read", 60)
    assert e.value.scope_unsupported and e.value.status == 422
    op = Opener(403, {"error": "forbidden", "message": "viewer"})
    with pytest.raises(BrokerError) as e:
        HttpBroker("http://core/api/credentials", lambda: "t", op).issue_s3("acme", "lake", "x", "readwrite", 60)
    assert not e.value.scope_unsupported and e.value.status == 403


def test_a_grant_never_shows_its_secret():
    g = HttpBroker("http://core/api/credentials", lambda: "t", Opener(doc=response())).issue_s3("acme", "lake", "lakehouse/t1", "read", 60)
    assert "SK" not in repr(g) and "ST" not in repr(g)
    assert g.covers("s3://lake/acme/lakehouse/t1/data/x.parquet") and not g.covers("s3://lake/acme/lakehouse/t10/x")


def test_unconfigured_broker_is_an_error_not_a_silent_default():
    with pytest.raises(ValueError):
        HttpBroker("", lambda: "t")
