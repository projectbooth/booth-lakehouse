"""This module's own state: which warehouse belongs to which workspace, and what booth-catalog has
last been told about each table.

A **binding** records the ``{backendId, path}`` a workspace owner chose (ADR 0045), the Lakekeeper
warehouse created for it, and the lease/expiry of the broker credential Lakekeeper currently holds
(so the refresher knows what to renew). A **member** row is an editor/owner person seen using the
module, so a renewal can name a current owner when the creator isn't (ADR 0088). It never records a
credential value: that lives only in
Lakekeeper's own encrypted secret store (ADR 0084, judgment call 4).

A **published** row is the last ``table.*`` event this module got a JetStream ack for, per table
(``events.py``): a digest of what was said, so the publisher emits only real changes, and when, so
updates to one table are rate-limited.

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


@dataclass(frozen=True)
class Published:
    table_uuid: str
    namespace: str
    name: str
    digest: str
    published_at: float


class Store:
    def get(self, workspace: str) -> Binding | None:  # pragma: no cover - interface
        raise NotImplementedError

    def all(self) -> list[Binding]:  # pragma: no cover - interface
        raise NotImplementedError

    def insert(self, b: Binding) -> None:  # pragma: no cover - interface
        """Raises ``Conflict`` if the workspace already has one."""
        raise NotImplementedError

    def update_credential(self, workspace: str, lease_id: str, expires_at: float) -> None:  # pragma: no cover
        raise NotImplementedError

    def due(self, before: float) -> list[Binding]:  # pragma: no cover - interface
        raise NotImplementedError

    def published(self, workspace: str) -> dict[str, Published]:  # pragma: no cover - interface
        raise NotImplementedError

    def record_published(self, workspace: str, p: Published) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def forget_published(self, workspace: str, table_uuid: str) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def note_member(self, workspace: str, subject: str, role: str, seen_at: float) -> None:  # pragma: no cover
        raise NotImplementedError

    def members(self, workspace: str) -> list[str]:  # pragma: no cover - interface
        """Editors/owners seen in the workspace, most recently seen first."""
        raise NotImplementedError


class Conflict(Exception):
    pass


class MemoryStore(Store):
    def __init__(self) -> None:
        self._rows: dict[str, Binding] = {}
        self._published: dict[str, dict[str, Published]] = {}
        self._members: dict[str, dict[str, float]] = {}
        self._lock = threading.Lock()

    def get(self, workspace):
        return self._rows.get(workspace)

    def all(self):
        return list(self._rows.values())

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

    def published(self, workspace):
        return dict(self._published.get(workspace, {}))

    def record_published(self, workspace, p):
        with self._lock:
            self._published.setdefault(workspace, {})[p.table_uuid] = p

    def forget_published(self, workspace, table_uuid):
        with self._lock:
            self._published.get(workspace, {}).pop(table_uuid, None)

    def note_member(self, workspace, subject, role, seen_at):
        with self._lock:
            self._members.setdefault(workspace, {})[subject] = seen_at

    def members(self, workspace):
        seen = self._members.get(workspace, {})
        return sorted(seen, key=seen.get, reverse=True)


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
CREATE TABLE IF NOT EXISTS booth_lakehouse.published_tables (
    workspace     text NOT NULL,
    table_uuid    text NOT NULL,
    namespace     text NOT NULL,
    name          text NOT NULL,
    digest        text NOT NULL,
    published_at  double precision NOT NULL,
    PRIMARY KEY (workspace, table_uuid)
);
CREATE TABLE IF NOT EXISTS booth_lakehouse.members (
    workspace  text NOT NULL,
    subject    text NOT NULL,
    role       text NOT NULL,
    seen_at    double precision NOT NULL,
    PRIMARY KEY (workspace, subject)
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

    def all(self):
        with self._conn() as c:
            return [Binding(*r) for r in c.execute(f"SELECT {_COLS} FROM booth_lakehouse.bindings ORDER BY workspace").fetchall()]

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

    def published(self, workspace):
        with self._conn() as c:
            rows = c.execute(
                "SELECT table_uuid, namespace, name, digest, published_at FROM booth_lakehouse.published_tables WHERE workspace = %s", (workspace,)
            ).fetchall()
        return {r[0]: Published(*r) for r in rows}

    def record_published(self, workspace, p):
        with self._conn() as c:
            c.execute(
                """INSERT INTO booth_lakehouse.published_tables (workspace, table_uuid, namespace, name, digest, published_at)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (workspace, table_uuid) DO UPDATE
                   SET namespace = EXCLUDED.namespace, name = EXCLUDED.name, digest = EXCLUDED.digest, published_at = EXCLUDED.published_at""",
                (workspace, p.table_uuid, p.namespace, p.name, p.digest, p.published_at),
            )

    def forget_published(self, workspace, table_uuid):
        with self._conn() as c:
            c.execute("DELETE FROM booth_lakehouse.published_tables WHERE workspace = %s AND table_uuid = %s", (workspace, table_uuid))

    def note_member(self, workspace, subject, role, seen_at):
        with self._conn() as c:
            c.execute(
                """INSERT INTO booth_lakehouse.members (workspace, subject, role, seen_at) VALUES (%s, %s, %s, %s)
                   ON CONFLICT (workspace, subject) DO UPDATE SET role = EXCLUDED.role, seen_at = EXCLUDED.seen_at""",
                (workspace, subject, role, seen_at),
            )

    def members(self, workspace):
        with self._conn() as c:
            rows = c.execute("SELECT subject FROM booth_lakehouse.members WHERE workspace = %s ORDER BY seen_at DESC LIMIT 50", (workspace,)).fetchall()
        return [r[0] for r in rows]
