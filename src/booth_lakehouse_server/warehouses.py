"""A workspace's warehouse: created from an owner's chosen ``{backendId, path}``, kept alive by renewing
Lakekeeper's broker credential before it expires.

Creating one (owner only):
  1. ask the ADR 0080 broker — *as the owner making the request* — for a read-write, static-key ``s3``
     grant on exactly ``{backendId, path}``;
  2. create Lakekeeper's warehouse from that grant (profile from its resolved location, credential
     from its key pair); Lakekeeper validates by writing and reading a probe object with it;
  3. record the binding (never the credential).

Renewing: before the current grant expires, ask the broker for a fresh one and hand it to Lakekeeper's
``storage-credential`` endpoint (which exists for exactly this). No person is present then, so the
request is made with a workload token core mints for this module on the workspace's behalf (ADR 0056)
— with ``owner`` = whoever created the warehouse. That has a real limit, flagged in
docs/decisions/0001: core refuses to mint once that owner hasn't signed in for its recency window.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass

import httpx

from booth_lakehouse.broker import READWRITE, Broker, BrokerError, S3Grant

from .identity import OWNER, Identity
from .lakekeeper import Lakekeeper, LakekeeperError, warehouse_name
from .store import Binding, Conflict, Store

log = logging.getLogger(__name__)


class WarehouseError(Exception):
    def __init__(self, message: str, status: int) -> None:
        super().__init__(message)
        self.status = status


def normalize_path(path: str) -> str:
    """ADR 0037/0045's prefix-normalized form: ``/``-separated, no leading/trailing slash, no empty
    or dot segments."""
    parts = [p for p in str(path).strip().split("/") if p]
    if not parts or any(p in (".", "..") for p in parts):
        raise WarehouseError("a warehouse path must be a non-empty folder path within the backend", 400)
    return "/".join(parts)


class WorkloadTokens:
    """Core's workload-token minting (ADR 0056/0058, core-platform-api.md "Workload identity")."""

    def __init__(self, mint_url: str, credential: str, opener=None) -> None:
        self._url = mint_url
        self._credential = credential
        self._open = opener or urllib.request.urlopen

    def __call__(self, workspace: str, owner: str) -> str:
        body = {"workspace": workspace, "subject": f"lakehouse:warehouse-{workspace}", "roleCeiling": "editor", "owner": owner}
        req = urllib.request.Request(self._url, data=json.dumps(body).encode(), method="POST")
        req.add_header("Authorization", "Bearer " + self._credential)
        req.add_header("Content-Type", "application/json")
        try:
            with self._open(req, timeout=30) as resp:
                return json.loads(resp.read())["token"]
        except urllib.error.HTTPError as e:
            raise WarehouseError(f"core refused a workload token for renewing workspace {workspace!r}'s warehouse credential (HTTP {e.code})", 503) from None
        except (urllib.error.URLError, OSError, KeyError, ValueError) as e:
            raise WarehouseError(f"couldn't mint a workload token: {e}", 503) from None


@dataclass
class Warehouses:
    store: Store
    lakekeeper: Lakekeeper
    # (caller's token) -> Broker. The broker call is always made *as* someone, so its audit names them.
    broker_for: Callable[[Callable[[], str]], Broker] | None
    # (workspace, owner sub) -> token, for renewals with nobody present. None = renewals can't happen.
    workload_token: Callable[[str, str], str] | None = None
    ttl_seconds: int = 3600
    renew_margin_seconds: int = 900
    clock: Callable[[], float] = time.time

    def get(self, workspace: str) -> Binding | None:
        return self.store.get(workspace)

    def _grant(self, token: Callable[[], str], workspace: str, backend_id: str, path: str) -> S3Grant:
        if self.broker_for is None:
            raise WarehouseError(
                "this deployment has no credential broker configured, so a warehouse can't get storage access (ADR 0080)", 503
            )
        try:
            return self.broker_for(token).issue_s3(workspace, backend_id, path, READWRITE, self.ttl_seconds, static_key=True)
        except BrokerError as e:
            if e.scope_unsupported:
                raise WarehouseError(f"booth-storage can't issue a credential scoped to {backend_id}:{path} for this backend: {e}", 422) from None
            if e.status in (401, 403):
                raise WarehouseError(f"the credential broker refused access to {backend_id}:{path}: {e}", 403) from None
            raise WarehouseError(f"the credential broker failed: {e}", 502) from None

    def create(self, who: Identity, backend_id: str, path: str) -> Binding:
        who.require(OWNER)
        if not backend_id:
            raise WarehouseError("backendId is required", 400)
        path = normalize_path(path)
        if self.store.get(who.workspace):
            raise WarehouseError(f"workspace {who.workspace!r} already has a warehouse; moving one isn't supported in v0", 409)
        grant = self._grant(lambda: who.token, who.workspace, backend_id, path)
        name = warehouse_name(who.workspace)
        self.lakekeeper.ensure_bootstrapped()
        try:
            wh = self.lakekeeper.create_warehouse(name, grant)
        except LakekeeperError as e:
            if e.kind == "CreateWarehouseStorageProfileOverlap":
                raise WarehouseError(f"{backend_id}:{path} overlaps another workspace's warehouse; choose a separate folder", 409) from None
            if e.status == 409 or "already exists" in str(e).lower():
                # A previous attempt created it and failed before recording it (only this module can
                # reach Lakekeeper). Adopt it with the fresh credential.
                wh = self.lakekeeper.find_warehouse(name)
                if wh is None:
                    raise WarehouseError(str(e), 502) from None
                self.lakekeeper.update_credential(wh.id, grant)
            elif e.status == 400 or e.status == 422:
                raise WarehouseError(f"Lakekeeper couldn't use {backend_id}:{path} as a warehouse: {e}", 422) from None
            else:
                raise WarehouseError(str(e), 502) from None
        binding = Binding(
            workspace=who.workspace,
            backend_id=backend_id,
            path=path,
            warehouse_id=wh.id,
            warehouse_name=name,
            storage_root=grant.root_uri,
            created_by=who.subject,
            created_at=self.clock(),
            lease_id=grant.lease_id,
            credential_expires_at=grant.expires_at,
        )
        try:
            self.store.insert(binding)
        except Conflict:
            raise WarehouseError(f"workspace {who.workspace!r} already has a warehouse", 409) from None
        log.info("warehouse created workspace=%s backend=%s path=%s lease=%s expires=%s by=%s", who.workspace, backend_id, path, grant.lease_id, int(grant.expires_at), who.subject)
        return binding

    def renew(self, b: Binding) -> None:
        if self.workload_token is None:
            raise WarehouseError("renewing needs workload identity (workloadIdentity.enabled), which isn't configured", 503)
        token = self.workload_token(b.workspace, b.created_by)
        grant = self._grant(lambda: token, b.workspace, b.backend_id, b.path)
        if grant.root_uri != b.storage_root:
            raise WarehouseError(f"the broker's renewed credential covers {grant.root_uri}, not this warehouse's {b.storage_root}; not using it", 502)
        self.lakekeeper.update_credential(b.warehouse_id, grant)
        self.store.update_credential(b.workspace, grant.lease_id, grant.expires_at)
        log.info("warehouse credential renewed workspace=%s lease=%s expires=%s", b.workspace, grant.lease_id, int(grant.expires_at))

    def renew_due(self) -> list[tuple[str, str]]:
        """Renew every credential inside the margin. Returns ``(workspace, error)`` for failures;
        one workspace failing never stops the others."""
        failures = []
        for b in self.store.due(self.clock() + self.renew_margin_seconds):
            try:
                self.renew(b)
            except (WarehouseError, LakekeeperError, httpx.HTTPError) as e:
                # httpx.HTTPError: Lakekeeper unreachable mid-renewal. Retried on the next pass.
                log.warning("warehouse credential renewal failed workspace=%s: %s", b.workspace, e)
                failures.append((b.workspace, str(e)))
        return failures
