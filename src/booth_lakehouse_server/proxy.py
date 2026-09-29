"""Which Iceberg REST calls a caller may make, and what they see back — the workspace boundary.

Lakekeeper runs with no authentication of its own (allow-all authorizer) and is reachable only from
this module's API pod (chart NetworkPolicy). So everything that keeps one workspace out of another's
tables is decided here, as pure functions over (method, path, role, the workspace's warehouse id):

- ``GET /v1/config`` is always answered for the caller's own warehouse, whatever ``warehouse`` the
  client asked for.
- Every other route must carry the caller's own warehouse id as its ``{prefix}``. Anything else is
  refused as not found, so a caller can't probe other warehouses' ids.
- Reads (GET/HEAD, plus reporting scan metrics) need ``viewer``; everything that changes the catalog
  needs ``editor`` (the same split as booth-storage/booth-catalog, ADR 0038/0048).
- Refused for everyone, on purpose:
  * ``.../credentials`` and catalog-vended credentials generally — storage access comes only from the
    ADR 0080 broker, scoped per caller and per table, so the catalog never hands out a credential;
  * ``.../register`` — adopting an existing metadata file at an arbitrary location would let a
    table point at storage outside this warehouse; not needed for v0 and not safe to allow blindly;
  * ``/v1/oauth/tokens`` — platform tokens come from the platform, not the catalog;
  * anything not recognised (fail closed: a new Lakekeeper endpoint stays blocked until reviewed).

Responses are rewritten so a client keeps talking to this proxy and never learns Lakekeeper's
internal address or storage settings: ``uri`` is removed from the config overrides/defaults (a client
honours it and would otherwise go around the proxy — found against real Lakekeeper, which sets it to
its own base URI), and table responses lose their storage ``config`` keys and ``storage-credentials``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import unquote

from .identity import EDITOR, RANK, VIEWER

_SEG = r"[^/]+"


@dataclass(frozen=True)
class Rule:
    method: str
    pattern: re.Pattern[str]
    role: str  # "" = refused for everyone


def _r(method: str, template: str, role: str) -> Rule:
    regex = "^" + template.replace("{prefix}", f"(?P<prefix>{_SEG})").replace("{ns}", _SEG).replace("{t}", _SEG) + "$"
    return Rule(method, re.compile(regex), role)


# Iceberg REST catalog routes (the set real Lakekeeper 0.13 advertises in /v1/config "endpoints").
RULES: tuple[Rule, ...] = (
    _r("GET", "/v1/config", VIEWER),
    # namespaces
    _r("GET", "/v1/{prefix}/namespaces", VIEWER),
    _r("POST", "/v1/{prefix}/namespaces", EDITOR),
    _r("GET", "/v1/{prefix}/namespaces/{ns}", VIEWER),
    _r("HEAD", "/v1/{prefix}/namespaces/{ns}", VIEWER),
    _r("DELETE", "/v1/{prefix}/namespaces/{ns}", EDITOR),
    _r("POST", "/v1/{prefix}/namespaces/{ns}/properties", EDITOR),
    # tables
    _r("GET", "/v1/{prefix}/namespaces/{ns}/tables", VIEWER),
    _r("POST", "/v1/{prefix}/namespaces/{ns}/tables", EDITOR),
    _r("GET", "/v1/{prefix}/namespaces/{ns}/tables/{t}", VIEWER),
    _r("HEAD", "/v1/{prefix}/namespaces/{ns}/tables/{t}", VIEWER),
    _r("POST", "/v1/{prefix}/namespaces/{ns}/tables/{t}", EDITOR),
    _r("DELETE", "/v1/{prefix}/namespaces/{ns}/tables/{t}", EDITOR),
    _r("POST", "/v1/{prefix}/namespaces/{ns}/tables/{t}/metrics", VIEWER),
    _r("GET", "/v1/{prefix}/namespaces/{ns}/tables/{t}/credentials", ""),
    _r("POST", "/v1/{prefix}/namespaces/{ns}/register", ""),
    _r("POST", "/v1/{prefix}/tables/rename", EDITOR),
    _r("POST", "/v1/{prefix}/transactions/commit", EDITOR),
    # views
    _r("GET", "/v1/{prefix}/namespaces/{ns}/views", VIEWER),
    _r("POST", "/v1/{prefix}/namespaces/{ns}/views", EDITOR),
    _r("GET", "/v1/{prefix}/namespaces/{ns}/views/{t}", VIEWER),
    _r("HEAD", "/v1/{prefix}/namespaces/{ns}/views/{t}", VIEWER),
    _r("POST", "/v1/{prefix}/namespaces/{ns}/views/{t}", EDITOR),
    _r("DELETE", "/v1/{prefix}/namespaces/{ns}/views/{t}", EDITOR),
    _r("POST", "/v1/{prefix}/views/rename", EDITOR),
)


@dataclass(frozen=True)
class Decision:
    allowed: bool
    status: int = 200
    reason: str = ""
    is_config: bool = False


def authorize(method: str, path: str, role: str, warehouse_id: str) -> Decision:
    """``path`` is the *raw* (still percent-encoded) Iceberg REST path below the catalog root, e.g.
    ``/v1/<prefix>/namespaces`` — the same bytes that get forwarded, so what is authorized is exactly
    what Lakekeeper routes (an encoded ``/`` stays inside its segment either way)."""
    method = method.upper()
    if ".." in unquote(path) or "//" in path:
        return Decision(False, 400, "malformed path")
    path = path.rstrip("/") or "/"
    for rule in RULES:
        if rule.method != method:
            continue
        m = rule.pattern.match(path)
        if not m:
            continue
        if not rule.role:
            return Decision(False, 403, "this catalog operation is not available on this platform (storage access comes from the credential broker)")
        prefix = m.groupdict().get("prefix")
        if prefix is not None and prefix != warehouse_id:
            return Decision(False, 404, "no such warehouse")
        if RANK.get(role, 0) < RANK[rule.role]:
            return Decision(False, 403, f"this needs the {rule.role} role; you have {role or 'none'}")
        return Decision(True, is_config=path == "/v1/config")
    return Decision(False, 404, "not a supported catalog endpoint")


_STORAGE_KEY = re.compile(r"^(s3\.|adls\.|gcs\.|client\.|oss\.|azure\.)|^region$")


def rewrite_config(body: dict) -> dict:
    """``GET /v1/config``: drop any ``uri`` so the client stays on the proxy."""
    out = dict(body)
    for section in ("overrides", "defaults"):
        if isinstance(out.get(section), dict):
            out[section] = {k: v for k, v in out[section].items() if k != "uri"}
    return out


def rewrite_table_response(body: dict) -> dict:
    """A load/create-table response: keep the metadata, drop storage settings and any credentials."""
    out = dict(body)
    out.pop("storage-credentials", None)
    if isinstance(out.get("config"), dict):
        out["config"] = {k: v for k, v in out["config"].items() if not _STORAGE_KEY.match(k) and k != "token"}
    return out
