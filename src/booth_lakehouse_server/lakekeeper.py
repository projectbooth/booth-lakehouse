"""The slice of Lakekeeper's management API this module drives (checked against Lakekeeper 0.13.6's own
``management-openapi`` output, not remembered).

One Lakekeeper warehouse per workspace, named ``booth-ws-<workspace>``, in Lakekeeper's default
project. Its storage profile and credential come entirely from an ADR 0080 broker grant: the profile
from the grant's resolved location, the credential from its key pair. Lakekeeper's own STS vending and
remote signing are both switched off — engines get their storage access from the broker, never from
the catalog (see ``proxy.py``).

Lakekeeper's ``access-key`` credential has no session-token field, which is why the warehouse's grant
is requested with ``static_key=True`` (docs/decisions/0001).
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx

from booth_lakehouse.broker import S3Grant


class LakekeeperError(Exception):
    def __init__(self, message: str, status: int = 0, kind: str = "") -> None:
        super().__init__(message)
        self.status = status
        self.kind = kind


def warehouse_name(workspace: str) -> str:
    return f"booth-ws-{workspace}"


@dataclass(frozen=True)
class Warehouse:
    id: str
    name: str


def storage_profile(grant: S3Grant) -> dict:
    return {
        "type": "s3",
        "bucket": grant.bucket,
        "key-prefix": grant.key_prefix or None,
        "region": grant.region,
        "endpoint": grant.endpoint,
        "path-style-access": grant.path_style,
        "flavor": "s3-compat",
        "sts-enabled": False,
        "remote-signing-enabled": False,
    }


def storage_credential(grant: S3Grant) -> dict:
    if grant.session_token:
        # Lakekeeper would silently drop the token and then fail every storage call.
        raise LakekeeperError("Lakekeeper can't hold a session-token credential; the warehouse grant must be a static key pair")
    return {"type": "s3", "credential-type": "access-key", "access-key-id": grant.access_key_id, "secret-access-key": grant.secret_access_key}


class Lakekeeper:
    def __init__(self, base_url: str, transport: httpx.BaseTransport | None = None, timeout: float = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self._http = httpx.Client(base_url=self.base_url, transport=transport, timeout=timeout)

    def close(self) -> None:
        self._http.close()

    @property
    def http(self) -> httpx.Client:
        return self._http

    def _check(self, resp: httpx.Response, what: str) -> httpx.Response:
        if resp.status_code >= 400:
            kind, msg = "", resp.text[:300]
            try:
                err = resp.json().get("error", {})
                kind, msg = err.get("type", ""), err.get("message", msg)
            except (ValueError, AttributeError):
                pass
            raise LakekeeperError(f"Lakekeeper refused to {what}: {msg}", resp.status_code, kind)
        return resp

    def healthy(self) -> bool:
        try:
            resp = self._http.get("/health", timeout=5)
            return resp.status_code == 200 and resp.json().get("health") == "ok"
        except (httpx.HTTPError, ValueError):
            return False

    def ensure_bootstrapped(self) -> None:
        """First start only: Lakekeeper refuses management calls until bootstrapped. Idempotent."""
        info = self._check(self._http.get("/management/v1/info"), "report its status").json()
        if info.get("bootstrapped"):
            return
        resp = self._http.post("/management/v1/bootstrap", json={"accept-terms-of-use": True, "is-operator": True})
        if resp.status_code not in (200, 204) and not (resp.status_code == 400 and "already" in resp.text.lower()):
            self._check(resp, "bootstrap")

    def find_warehouse(self, name: str) -> Warehouse | None:
        doc = self._check(self._http.get("/management/v1/warehouse"), "list warehouses").json()
        for w in doc.get("warehouses", []):
            if w.get("name") == name:
                return Warehouse(w.get("warehouse-id") or w["id"], name)
        return None

    def create_warehouse(self, name: str, grant: S3Grant) -> Warehouse:
        body = {"warehouse-name": name, "storage-profile": storage_profile(grant), "storage-credential": storage_credential(grant)}
        doc = self._check(self._http.post("/management/v1/warehouse", json=body), "create the warehouse").json()
        return Warehouse(doc["warehouse-id"], name)

    def update_credential(self, warehouse_id: str, grant: S3Grant) -> None:
        body = {"new-storage-credential": storage_credential(grant)}
        self._check(self._http.post(f"/management/v1/warehouse/{warehouse_id}/storage-credential", json=body), "update the warehouse credential")

    def delete_warehouse(self, warehouse_id: str) -> None:
        self._check(self._http.delete(f"/management/v1/warehouse/{warehouse_id}"), "delete the warehouse")
