"""ADR 0108's key-fetch override (BOOTH_OIDC_JWKS_URL), against real HTTP servers: keys come from the
configured URL, `iss` is still checked exactly against the configured issuer, and discovery is
never contacted. Unset, behaviour is the existing discovery path (covered by every other test)."""

from __future__ import annotations

import base64
import json
import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from booth_lakehouse_server.app import components_from_settings
from booth_lakehouse_server.config import Settings
from booth_lakehouse_server.identity import AuthError, TrustedIssuer, Verifier

from .keys import Signer

# An https issuer nothing listens on: if anything tried discovery against it, verification would fail.
UNREACHABLE_ISSUER = "https://keycloak.booth.invalid/realms/booth"


def _b64(n: int) -> str:
    return base64.urlsafe_b64encode(n.to_bytes((n.bit_length() + 7) // 8, "big")).rstrip(b"=").decode()


def jwks_doc(signer: Signer, kid: str) -> dict:
    n = signer.key.public_key().public_numbers()
    return {"keys": [{"kty": "RSA", "kid": kid, "use": "sig", "alg": "RS256", "n": _b64(n.n), "e": _b64(n.e)}]}


@contextmanager
def http_server(routes: dict[str, dict]) -> Iterator[tuple[str, list[str]]]:
    """A real plain-http server on 127.0.0.1 serving JSON per path; yields (base URL, paths hit)."""
    hits: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            hits.append(self.path)
            doc = routes.get(self.path)
            body = json.dumps(doc).encode() if doc is not None else b"{}"
            self.send_response(200 if doc is not None else 404)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    try:
        yield f"http://127.0.0.1:{srv.server_address[1]}", hits
    finally:
        srv.shutdown()


def token(signer: Signer, iss: str, kid: str = "k1", **kw) -> str:
    import time

    import jwt

    now = int(time.time())
    claims = {"iss": iss, "sub": "alice", "iat": now, "exp": now + 300, "groups": ["/workspaces/acme/owner"], **kw}
    return jwt.encode(claims, signer.key, algorithm="RS256", headers={"kid": kid})


def test_keys_come_from_the_override_and_a_valid_token_verifies():
    signer = Signer(UNREACHABLE_ISSUER)
    with http_server({"/jwks": jwks_doc(signer, "k1")}) as (base, hits):
        v = Verifier([TrustedIssuer(UNREACHABLE_ISSUER, jwks_url=f"{base}/jwks")])
        claims = v.verify(token(signer, UNREACHABLE_ISSUER))
        assert claims.subject == "alice" and claims.issuer == UNREACHABLE_ISSUER
        assert hits == ["/jwks"]


def test_a_wrong_iss_with_the_same_key_is_rejected():
    signer = Signer(UNREACHABLE_ISSUER)
    with http_server({"/jwks": jwks_doc(signer, "k1")}) as (base, _):
        v = Verifier([TrustedIssuer(UNREACHABLE_ISSUER, jwks_url=f"{base}/jwks")])
        # Another issuer altogether, the in-cluster key URL's own host, and a near miss: all refused,
        # though every one is signed by the very key the JWKS URL serves.
        for iss in ("https://evil.example/realms/booth", base, UNREACHABLE_ISSUER.replace("https://", "http://"), UNREACHABLE_ISSUER + "x"):
            with pytest.raises(AuthError):
                v.verify(token(signer, iss))


def test_with_the_override_discovery_is_never_contacted():
    """The issuer here IS a reachable server with a discovery document — and it is never asked."""
    signer = Signer("placeholder")
    with http_server({"/jwks": jwks_doc(signer, "k1")}) as (keys_base, _):
        with http_server({"/.well-known/openid-configuration": {}}) as (issuer_base, discovery_hits):
            v = Verifier([TrustedIssuer(issuer_base, jwks_url=f"{keys_base}/jwks")])
            assert v.verify(token(signer, issuer_base)).subject == "alice"
            assert discovery_hits == []


def test_without_the_override_discovery_is_used_as_before():
    """Control for the test above: the same counting server, override unset — discovery is hit."""
    signer = Signer("placeholder")
    with http_server({}) as (keys_base, _):
        routes = {"/jwks": jwks_doc(signer, "k1")}
        with http_server(routes) as (issuer_base, hits):
            routes["/.well-known/openid-configuration"] = {"issuer": issuer_base, "jwks_uri": f"{issuer_base}/jwks"}
            v = Verifier([TrustedIssuer(issuer_base)])
            assert v.verify(token(signer, issuer_base)).subject == "alice"
            assert hits[0] == "/.well-known/openid-configuration"
    del keys_base


# ---- configuration -------------------------------------------------------------------------------


BASE_ENV = {"LAKEKEEPER_URL": "http://lakekeeper:8181"}


def test_jwks_url_without_an_issuer_is_a_startup_error():
    with pytest.raises(ValueError, match="BOOTH_OIDC_JWKS_URL is set but BOOTH_OIDC_ISSUER_URL is empty"):
        Settings.from_env({**BASE_ENV, "BOOTH_OIDC_JWKS_URL": "http://keycloak:8080/realms/booth/protocol/openid-connect/certs"})


def test_the_override_applies_to_the_oidc_issuer_only():
    s = Settings.from_env({
        **BASE_ENV,
        "BOOTH_OIDC_ISSUER_URL": UNREACHABLE_ISSUER,
        "BOOTH_OIDC_JWKS_URL": "http://keycloak:8080/certs",
        "BOOTH_WORKLOAD_ISSUER_URL": "http://booth-core:8080",
    })
    by_url = {i.url: i for i in s.issuers}
    assert by_url[UNREACHABLE_ISSUER].jwks_url == "http://keycloak:8080/certs"
    assert by_url["http://booth-core:8080"].jwks_url == ""


@pytest.mark.parametrize("value", [None, ""])
def test_unset_or_empty_is_ordinary_discovery(value):
    env = {**BASE_ENV, "BOOTH_OIDC_ISSUER_URL": UNREACHABLE_ISSUER}
    if value is not None:
        env["BOOTH_OIDC_JWKS_URL"] = value
    (issuer,) = Settings.from_env(env).issuers
    assert issuer == TrustedIssuer(UNREACHABLE_ISSUER, "")


def test_startup_logs_the_issuer_and_where_keys_come_from_once(caplog):
    caplog.set_level(logging.INFO)
    s = Settings.from_env({**BASE_ENV, "BOOTH_OIDC_ISSUER_URL": UNREACHABLE_ISSUER, "BOOTH_OIDC_JWKS_URL": "http://keycloak:8080/certs",
                           "BOOTH_WORKLOAD_ISSUER_URL": "http://booth-core:8080"})
    components_from_settings(s)
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("oidc:")]
    assert lines == [
        f"oidc: verifying tokens with issuer={UNREACHABLE_ISSUER} keys-from=http://keycloak:8080/certs",
        "oidc: verifying tokens with issuer=http://booth-core:8080 keys-from=discovery (http://booth-core:8080/.well-known/openid-configuration)",
    ]


# ---- the hand-rolled key set mirrors go-oidc's RemoteKeySet (booth-core) ---------------------------


def test_keys_are_cached_and_a_rotated_key_is_picked_up_by_one_refetch():
    old, new = Signer(UNREACHABLE_ISSUER), Signer(UNREACHABLE_ISSUER)
    routes = {"/jwks": jwks_doc(old, "k1")}
    with http_server(routes) as (base, hits):
        v = Verifier([TrustedIssuer(UNREACHABLE_ISSUER, jwks_url=f"{base}/jwks")])
        for _ in range(3):
            v.verify(token(old, UNREACHABLE_ISSUER, kid="k1"))
        assert hits == ["/jwks"]  # cached: one fetch for three tokens
        routes["/jwks"] = jwks_doc(new, "k2")  # the IdP rotates its key
        assert v.verify(token(new, UNREACHABLE_ISSUER, kid="k2")).subject == "alice"
        assert hits == ["/jwks", "/jwks"]  # exactly one refetch, triggered by the unknown kid


def test_an_unknown_key_id_is_refused_after_a_single_refetch():
    signer = Signer(UNREACHABLE_ISSUER)
    with http_server({"/jwks": jwks_doc(signer, "k1")}) as (base, hits):
        v = Verifier([TrustedIssuer(UNREACHABLE_ISSUER, jwks_url=f"{base}/jwks")])
        with pytest.raises(AuthError, match="no published signing key"):
            v.verify(token(Signer(UNREACHABLE_ISSUER), UNREACHABLE_ISSUER, kid="nope"))
        assert hits == ["/jwks"]


def test_keys_not_for_signing_are_skipped():
    signer = Signer(UNREACHABLE_ISSUER)
    doc = jwks_doc(signer, "k1")
    doc["keys"].insert(0, {**doc["keys"][0], "kid": "enc", "use": "enc"})
    doc["keys"].append({"kty": "unknown-kind", "kid": "junk"})
    with http_server({"/jwks": doc}) as (base, _):
        v = Verifier([TrustedIssuer(UNREACHABLE_ISSUER, jwks_url=f"{base}/jwks")])
        assert v.verify(token(signer, UNREACHABLE_ISSUER, kid="k1")).subject == "alice"
        with pytest.raises(AuthError):
            v.verify(token(signer, UNREACHABLE_ISSUER, kid="enc"))  # an encryption key is never a signing key
