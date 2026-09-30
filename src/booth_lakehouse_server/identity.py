"""Who is asking, and in which workspace: this module's side of core-platform-api.md "Auth enforcement".

Every request is re-verified here (signature, issuer, expiry) rather than trusted for having come
through the gateway, and the workspace role is derived from the token's own groups claim — a
forwarded ``X-Booth-Role`` stronger than that is rejected (ADR 0041). Same claim grammar, same
fail-closed rules and the same issuer dispatch as booth-notebooks' ``identity.py``.

One deliberate difference from booth-notebooks: a workload token (``sub`` = ``<kind>:<id>``, ADR
0056/0058) is accepted, because the expected callers here *are* workloads — a notebook kernel or a
pipeline task reading and writing tables. Core's workload issuer is simply one more trusted issuer.
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass

import httpx
import jwt
from jwt import PyJWKClient

OWNER, EDITOR, VIEWER = "owner", "editor", "viewer"
RANK = {OWNER: 3, EDITOR: 2, VIEWER: 1}

_GROUP_RE = re.compile(r"^/workspaces/([a-z0-9-]+)/(owner|editor|viewer)$")  # ADR 0025
WORKSPACE_RE = re.compile(r"^[a-z0-9-]+$")
# ADR 0058: a workload token's `sub` is `<kind>:<id>`, which no person's `sub` ever matches.
_WORKLOAD_SUBJECT_RE = re.compile(r"^[a-z][a-z0-9-]{0,31}:[A-Za-z0-9._:-]{1,200}$")

# Asymmetric algorithms only: never HS* (algorithm confusion against a public key) and never "none".
_ALGORITHMS = ["RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512"]


class AuthError(Exception):
    """401: the token itself is missing, malformed, untrusted or expired."""


class Forbidden(Exception):
    """403: a valid token that doesn't grant what the request needs."""


@dataclass(frozen=True)
class TrustedIssuer:
    url: str
    audience: str = ""


@dataclass(frozen=True)
class Claims:
    subject: str
    groups: tuple[str, ...]
    issuer: str


class _IssuerKeys:
    """Lazy OIDC discovery for one issuer, so the module starts (and passes health) even while an
    issuer is briefly unreachable."""

    def __init__(self, issuer: TrustedIssuer) -> None:
        self.issuer = issuer
        self._jwks: PyJWKClient | None = None
        self._lock = threading.Lock()

    def client(self) -> PyJWKClient:
        with self._lock:
            if self._jwks is None:
                with httpx.Client(timeout=10) as http:
                    resp = http.get(f"{self.issuer.url}/.well-known/openid-configuration")
                    resp.raise_for_status()
                    doc = resp.json()
                if doc.get("issuer", "").rstrip("/") != self.issuer.url:
                    raise AuthError("OIDC discovery document's issuer does not match the configured issuer")
                self._jwks = PyJWKClient(doc["jwks_uri"], cache_keys=True, lifespan=300)
            return self._jwks


class KeySource:
    def signing_key(self, issuer_url: str, raw_token: str):  # pragma: no cover - interface
        raise NotImplementedError


class DiscoveryKeySource(KeySource):
    def __init__(self, issuers: list[TrustedIssuer]) -> None:
        self._by_url = {i.url: _IssuerKeys(i) for i in issuers}

    def signing_key(self, issuer_url: str, raw_token: str):
        return self._by_url[issuer_url].client().get_signing_key_from_jwt(raw_token).key


class Verifier:
    """Dispatch by ``iss`` to a configured trusted issuer (ADR 0059's pattern); an untrusted ``iss``
    is rejected before any network call is made on its behalf."""

    def __init__(self, issuers: list[TrustedIssuer], groups_claim: str = "groups", keys: KeySource | None = None) -> None:
        if not issuers:
            raise ValueError("at least one trusted issuer is required")
        self._issuers = {i.url.rstrip("/"): TrustedIssuer(i.url.rstrip("/"), i.audience) for i in issuers}
        self._groups_claim = groups_claim or "groups"
        self._keys = keys or DiscoveryKeySource(list(self._issuers.values()))

    def verify(self, raw_token: str) -> Claims:
        try:
            unverified = jwt.decode(raw_token, options={"verify_signature": False})
        except jwt.PyJWTError as e:
            raise AuthError(f"malformed token: {e}") from e
        issuer = self._issuers.get(str(unverified.get("iss", "")).rstrip("/"))
        if issuer is None:
            raise AuthError("token issuer is not trusted by this module")
        try:
            key = self._keys.signing_key(issuer.url, raw_token)
            payload = jwt.decode(
                raw_token,
                key,
                algorithms=_ALGORITHMS,
                issuer=issuer.url,
                audience=issuer.audience or None,
                options={"require": ["exp", "iss", "sub"], "verify_aud": bool(issuer.audience)},
            )
        except (jwt.PyJWTError, httpx.HTTPError, KeyError, ValueError) as e:
            raise AuthError(f"token verification failed: {e}") from e
        raw_groups = payload.get(self._groups_claim)
        groups = tuple(g for g in raw_groups if isinstance(g, str)) if isinstance(raw_groups, list) else ()
        return Claims(subject=str(payload["sub"]), groups=groups, issuer=issuer.url)


def role_in_workspace(groups, workspace: str) -> str:
    best = ""
    for g in groups:
        m = _GROUP_RE.match(g)
        if m and m.group(1) == workspace and RANK[m.group(2)] > RANK.get(best, 0):
            best = m.group(2)
    return best


@dataclass(frozen=True)
class Identity:
    subject: str
    workspace: str
    role: str
    token: str  # the caller's own token, forwarded to the broker so its audit names the real requester

    @property
    def is_person(self) -> bool:
        return not _WORKLOAD_SUBJECT_RE.match(self.subject)

    def require(self, role: str) -> None:
        if RANK[self.role] < RANK[role]:
            raise Forbidden(f"this needs the {role} role in workspace {self.workspace!r}; you have {self.role}")

    def __repr__(self) -> str:  # never print the token
        return f"Identity(subject={self.subject!r}, workspace={self.workspace!r}, role={self.role!r})"


def resolve(claims: Claims, token: str, workspace: str, forwarded_role: str = "") -> Identity:
    """The whole authorization decision as a pure function. ``workspace`` is the gateway's
    ``X-Booth-Workspace`` (or the client's ``X-Workspace`` on a direct call) — safe to read because it
    only selects among workspaces the verified token itself grants a role in."""
    if not workspace or not WORKSPACE_RE.match(workspace):
        raise Forbidden("no (valid) active workspace on this request")
    granted = role_in_workspace(claims.groups, workspace)
    if not granted:
        raise Forbidden("your token grants no role in this workspace")
    forwarded = (forwarded_role or "").strip()
    if forwarded and RANK.get(forwarded, 99) > RANK[granted]:
        raise Forbidden("the forwarded role exceeds what your token grants in this workspace")
    role = forwarded if forwarded in RANK and RANK[forwarded] < RANK[granted] else granted
    return Identity(claims.subject, workspace, role, token)
