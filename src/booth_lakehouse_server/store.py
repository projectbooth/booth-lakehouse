"""Which warehouse belongs to which workspace, and where it lives — this module's only own state.

A binding records the ``{backendId, path}`` a workspace owner chose (ADR 0045), the Lakekeeper
warehouse created for it, and the lease/expiry of the broker credential Lakekeeper currently holds
(so the refresher knows what to renew). It never records a credential value: that lives only in
Lakekeeper's own encrypted secret store (docs/decisions/0001 on why that's acceptable here).

Stored in this module's core-provisioned database (ADR 0053), in its own ``booth_lakehouse`` schema so
it never collides with Lakekeeper's tables in the same database.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class Binding:
    workspace: str
    backend_id: str
    path: str
    warehouse_id: str
    warehouse_name: str
    storage_root: str
    created_by: str
    created_at: float
    lease_id: str
    credential_expires_at: float

    def public(self) -> dict:
        return {
            "workspace": self.workspace,
            "backendId": self.backend_id,
            "path": self.path,
            "warehouseName": self.warehouse_name,
            "storageRoot": self.storage_root,
            "createdBy": self.created_by,
            "createdAt": self.created_at,
        }


class Store:
    def get(self, workspace: str) -> Binding | None:  # pragma: no cover - interface
        raise NotImplementedError

    def insert(self, b: Binding) -> None:  # pragma: no cover - interface
        """Raises ``Conflict`` if the workspace already has one."""
        raise NotImplementedError

    def update_credential(self, workspace: str, lease_id: str, expires_at: float) -> None:  # pragma: no cover
        raise NotImplementedError

    def due(self, before: float) -> list[Binding]:  # pragma: no cover - interface
        raise NotImplementedError


class Conflict(Exception):
    pass


class MemoryStore(Store):
    def __init__(self) -> None:
        self._rows: dict[str, Binding] = {}
        self._lock = threading.Lock()

    def get(self, workspace):
        return self._rows.get(workspace)

    def insert(self, b):
        with self._lock:
            if b.workspace in self._rows:
                raise Conflict(b.workspace)
            self._rows[b.workspace] = b

    def update_credential(self, workspace, lease_id, expires_at):
        with self._lock:
            self._rows[workspace] = replace(self._rows[workspace], lease_id=lease_id, credential_expires_at=expires_at)

    def due(self, before):
        return [b for b in self._rows.values() if b.credential_expires_at < before]


_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS booth_lakehouse;
CREATE TABLE IF NOT EXISTS booth_lakehouse.bindings (
    workspace              text PRIMARY KEY,
    backend_id             text NOT NULL,
    path                   text NOT NULL,
    warehouse_id           text NOT NULL UNIQUE,
    warehouse_name         text NOT NULL UNIQUE,
    storage_root           text NOT NULL,
    created_by             text NOT NULL,
    created_at             double precision NOT NULL,
    lease_id               text NOT NULL,
    credential_expires_at  double precision NOT NULL
);
"""

_COLS = "workspace, backend_id, path, warehouse_id, warehouse_name, storage_root, created_by, created_at, lease_id, credential_expires_at"


class PostgresStore(Store):
    def __init__(self, dsn: str) -> None:
        import psycopg

        self._psycopg = psycopg
        self._dsn = dsn
        with self._conn() as c:
            c.execute(_SCHEMA)

    def _conn(self):
        return self._psycopg.connect(self._dsn, autocommit=True)

    def get(self, workspace):
        with self._conn() as c:
            row = c.execute(f"SELECT {_COLS} FROM booth_lakehouse.bindings WHERE workspace = %s", (workspace,)).fetchone()
        return Binding(*row) if row else None

    def insert(self, b):
        try:
            with self._conn() as c:
                c.execute(
                    f"INSERT INTO booth_lakehouse.bindings ({_COLS}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (b.workspace, b.backend_id, b.path, b.warehouse_id, b.warehouse_name, b.storage_root, b.created_by, b.created_at, b.lease_id, b.credential_expires_at),
                )
        except self._psycopg.errors.UniqueViolation:
            raise Conflict(b.workspace) from None

    def update_credential(self, workspace, lease_id, expires_at):
        with self._conn() as c:
            c.execute("UPDATE booth_lakehouse.bindings SET lease_id = %s, credential_expires_at = %s WHERE workspace = %s", (lease_id, expires_at, workspace))

    def due(self, before):
        with self._conn() as c:
            rows = c.execute(f"SELECT {_COLS} FROM booth_lakehouse.bindings WHERE credential_expires_at < %s", (before,)).fetchall()
        return [Binding(*r) for r in rows]
