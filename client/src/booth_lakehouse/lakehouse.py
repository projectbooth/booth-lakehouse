"""Iceberg tables in a Project Booth workspace, without the plumbing.

    from booth_lakehouse import Lakehouse
    lh = Lakehouse.from_env()
    lh.create_table("sales.daily", df)         # schema from the data; creates the namespace too
    lh.append("sales.daily", more_rows)
    lh.read("sales.daily").to_pandas()
    lh.tables()                                # ["sales.daily", ...]

Three things happen underneath, none of which a caller has to arrange:

1. **The catalog** is booth-lakehouse's Iceberg REST endpoint behind booth-core's gateway
   (``<gateway>/lakehouse/iceberg``), called with the caller's own platform token and
   ``X-Workspace``. booth-lakehouse pins every request to that workspace's warehouse and enforces
   the caller's role (viewer reads, editor writes).
2. **Storage access** comes from the ADR 0080 credential broker: before touching a table's files this
   asks for an ``s3`` credential scoped to exactly that table's ``{backendId, path}`` (ADR 0045) —
   read-only for reads, read-write for writes — as the caller, so the broker's audit trail names who
   actually read or wrote. Credentials are renewed shortly before they expire.
3. **Writes go through PyIceberg** (ADR 0079). Reads do too; ``duckdb()`` hands the result to DuckDB
   for SQL.

The catalog never hands out storage credentials itself, and whatever storage settings it attaches to
a table are ignored here: the only storage access this library ever uses is a broker grant.
"""

from __future__ import annotations

import json
import os
import threading
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any

from .broker import READ, READWRITE, Broker, BrokerError, HttpBroker, S3Grant

# How long a requested storage credential lives, and how close to expiry one is replaced. The
# broker may cap the TTL lower (ADR 0080 requires a strict ceiling); the margin keeps a read or a
# commit from starting on a credential that dies mid-flight.
DEFAULT_TTL_SECONDS = 900
RENEW_MARGIN_SECONDS = 120


class LakehouseError(Exception):
    def __init__(self, message: str, status: int = 0) -> None:
        super().__init__(message)
        self.status = status


def _as_arrow(data: Any):
    """pyarrow Table from a pyarrow Table, a pandas DataFrame, or a list of row dicts."""
    import pyarrow as pa

    if isinstance(data, pa.Table):
        return data
    if isinstance(data, pa.RecordBatch):
        return pa.Table.from_batches([data])
    if isinstance(data, list):
        return pa.Table.from_pylist(data)
    try:
        import pandas as pd
    except ImportError:  # pragma: no cover - pandas is optional
        pd = None
    if pd is not None and isinstance(data, pd.DataFrame):
        return pa.Table.from_pandas(data, preserve_index=False)
    raise TypeError(f"can't write a {type(data).__name__}; pass a pyarrow Table, a pandas DataFrame or a list of dicts")


def _identifier(name: str) -> tuple[str, str]:
    parts = name.split(".")
    if len(parts) != 2 or not all(parts):
        raise LakehouseError(f"table names are 'namespace.table' (got {name!r})")
    return parts[0], parts[1]


class Lakehouse:
    def __init__(
        self,
        gateway_url: str,
        workspace: str,
        token: Callable[[], str] | str,
        broker: Broker | None = None,
        broker_url: str = "",
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        opener=None,
    ) -> None:
        if not gateway_url or not workspace:
            raise LakehouseError("a gateway URL and a workspace are required")
        self.workspace = workspace
        self._base = gateway_url.rstrip("/") + "/lakehouse"
        self._token = (lambda: token) if isinstance(token, str) else token
        self._open = opener or urllib.request.urlopen
        self._ttl = ttl_seconds
        self._broker = broker or (HttpBroker(broker_url, self._token, opener) if broker_url else None)
        self._catalog = None
        self._warehouse: dict | None = None
        self._grants: dict[tuple[str, str], S3Grant] = {}
        self._lock = threading.Lock()
        # pandas timestamps are nanosecond; Iceberg v2 stores microseconds. Downcast on write rather
        # than refuse every DataFrame with a datetime column. Respects an explicit caller setting.
        os.environ.setdefault("PYICEBERG_DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE", "true")

    @classmethod
    def from_env(cls, env=None, **kwargs) -> Lakehouse:
        """Configured from the environment a platform workload already has: ``BOOTH_GATEWAY_URL``
        (``<core>/modules``), ``BOOTH_WORKSPACE``, ``BOOTH_CREDENTIAL_BROKER_URL``, and a token from
        ``BOOTH_TOKEN`` or — inside a booth-notebooks kernel — the notebook's own platform token."""
        env = os.environ if env is None else env
        token: Callable[[], str] | str | None = env.get("BOOTH_TOKEN") or _notebook_token()
        if not token:
            raise LakehouseError("no platform token: set BOOTH_TOKEN, or run inside a Project Booth notebook")
        return cls(
            env.get("BOOTH_GATEWAY_URL", ""),
            env.get("BOOTH_WORKSPACE", ""),
            token,
            broker_url=env.get("BOOTH_CREDENTIAL_BROKER_URL", ""),
            **kwargs,
        )

    # ---- booth-lakehouse's own API -----------------------------------------------------------

    def _api(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self._base + path, data=data, method=method)
        req.add_header("Authorization", "Bearer " + self._token())
        req.add_header("X-Workspace", self.workspace)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with self._open(req, timeout=60) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            try:
                detail = json.loads(e.read()).get("detail", "")
            except (ValueError, AttributeError, OSError):
                detail = ""
            raise LakehouseError(f"booth-lakehouse returned HTTP {e.code} for {method} {path}: {detail}", e.code) from None
        except (urllib.error.URLError, OSError) as e:
            raise LakehouseError(f"couldn't reach booth-lakehouse through the gateway: {e}") from None

    def warehouse(self, refresh: bool = False) -> dict:
        """This workspace's warehouse: where its tables live, as ``{backendId, path}``."""
        if self._warehouse is None or refresh:
            try:
                self._warehouse = self._api("GET", "/api/warehouse")
            except LakehouseError as e:
                if e.status == 404:
                    raise LakehouseError(
                        f"workspace {self.workspace!r} has no lakehouse warehouse yet; a workspace owner "
                        "creates one by choosing a storage location for it", 404
                    ) from None
                raise
        return self._warehouse

    def create_warehouse(self, backend_id: str, path: str) -> dict:
        """Owner only: put this workspace's tables at ``{backend_id, path}`` in booth-storage."""
        self._warehouse = self._api("PUT", "/api/warehouse", {"backendId": backend_id, "path": path})
        return self._warehouse

    # ---- the catalog --------------------------------------------------------------------------

    @property
    def catalog(self):
        """The PyIceberg ``RestCatalog`` for this workspace — the escape hatch for anything this
        class doesn't wrap. Tables loaded from it directly have no storage access; use ``table()``."""
        if self._catalog is None:
            from pyiceberg.catalog.rest import RestCatalog

            self._catalog = RestCatalog(
                f"booth-{self.workspace}",
                **{
                    "uri": self._base + "/iceberg",
                    "header.X-Workspace": self.workspace,
                    # Storage access comes from the broker, never the catalog (see module docstring).
                    "header.X-Iceberg-Access-Delegation": "",
                    "auth": {"type": "custom", "impl": "booth_lakehouse.auth.GatewayTokenAuth", "custom": {"token": self._token}},
                },
            )
        return self._catalog

    def tables(self, namespace: str | None = None) -> list[str]:
        namespaces = [namespace] if namespace else [ns[0] for ns in self.catalog.list_namespaces()]
        out = []
        for ns in namespaces:
            out.extend(".".join(ident) for ident in self.catalog.list_tables(ns))
        return sorted(out)

    def table(self, name: str) -> LakehouseTable:
        return LakehouseTable(self, self.catalog.load_table(_identifier(name)))

    def create_table(self, name: str, data: Any = None, schema=None, partition_by: str | None = None) -> LakehouseTable:
        """Create ``namespace.table`` from ``data`` (its schema, then its rows) or an explicit
        ``schema`` (a pyarrow or PyIceberg schema). The namespace is created if missing."""
        from pyiceberg.exceptions import NamespaceAlreadyExistsError

        ns, _ = _identifier(name)
        arrow = _as_arrow(data) if data is not None else None
        if schema is None:
            if arrow is None:
                raise LakehouseError("create_table needs data or a schema")
            schema = arrow.schema
        try:
            self.catalog.create_namespace(ns)
        except NamespaceAlreadyExistsError:
            pass
        tbl = LakehouseTable(self, self.catalog.create_table(_identifier(name), schema=schema))
        if partition_by:
            tbl.partition_by(partition_by)
        if arrow is not None and arrow.num_rows:
            tbl.append(arrow)
        return tbl

    def append(self, name: str, data: Any) -> LakehouseTable:
        tbl = self.table(name)
        tbl.append(data)
        return tbl

    def read(self, name: str, columns: list[str] | None = None, where: str | None = None, snapshot_id: int | None = None):
        return self.table(name).read(columns=columns, where=where, snapshot_id=snapshot_id)

    def drop_table(self, name: str) -> None:
        self.catalog.drop_table(_identifier(name))

    # ---- storage credentials -------------------------------------------------------------------

    def _location_ref(self, location: str) -> tuple[str, str]:
        """A table's ``s3://`` location as the ``{backendId, path}`` pair it lives at (ADR 0045)."""
        wh = self.warehouse()
        root = wh["storageRoot"].rstrip("/")
        if location != root and not location.startswith(root + "/"):
            raise LakehouseError(f"table location {location} is outside this workspace's warehouse ({root}); refusing to request access to it")
        rel = location[len(root):].strip("/")
        base = wh["path"].strip("/")
        return wh["backendId"], f"{base}/{rel}".strip("/") if rel else base

    def _grant(self, location: str, access: str) -> S3Grant:
        if self._broker is None:
            raise LakehouseError(
                "no credential broker is configured (BOOTH_CREDENTIAL_BROKER_URL): reading or writing table "
                "data needs a short-lived storage credential from booth-core's broker (ADR 0080)"
            )
        backend, path = self._location_ref(location)
        key = (path, access)
        with self._lock:
            grant = self._grants.get(key)
            if grant is None or grant.expires_within(RENEW_MARGIN_SECONDS):
                try:
                    grant = self._broker.issue_s3(self.workspace, backend, path, access, self._ttl)
                except BrokerError as e:
                    raise LakehouseError(str(e), e.status) from None
                if not grant.covers(location):
                    raise LakehouseError("the broker's credential doesn't cover this table's location; refusing to use it")
                self._grants[key] = grant
            return grant


class LakehouseTable:
    """A PyIceberg table plus the storage access to use it. ``iceberg`` is the underlying table for
    anything not wrapped here (its FileIO is replaced before every wrapped operation)."""

    def __init__(self, lakehouse: Lakehouse, table) -> None:
        self._lh = lakehouse
        self.iceberg = table

    @property
    def name(self) -> str:
        return ".".join(self.iceberg.name())

    @property
    def schema(self):
        return self.iceberg.schema()

    @property
    def location(self) -> tuple[str, str]:
        """``(backendId, path)`` of this table in booth-storage."""
        return self._lh._location_ref(self.iceberg.location())

    def _with_access(self, access: str):
        from pyiceberg.io import load_file_io
        from pyiceberg.table import Table

        grant = self._lh._grant(self.iceberg.location(), access)
        t = self.iceberg
        self.iceberg = Table(t._identifier, t.metadata, t.metadata_location, load_file_io(grant.fileio_properties(), t.metadata_location), t.catalog)
        return self.iceberg

    def refresh(self) -> LakehouseTable:
        self.iceberg = self.iceberg.catalog.load_table(self.iceberg._identifier)
        return self

    def append(self, data: Any) -> LakehouseTable:
        self._with_access(READWRITE).append(_as_arrow(data))
        return self

    def overwrite(self, data: Any) -> LakehouseTable:
        self._with_access(READWRITE).overwrite(_as_arrow(data))
        return self

    def partition_by(self, column: str) -> LakehouseTable:
        from pyiceberg.transforms import IdentityTransform

        with self.iceberg.update_spec() as spec:
            spec.add_field(column, IdentityTransform(), column)
        return self

    def read(self, columns: list[str] | None = None, where: str | None = None, snapshot_id: int | None = None):
        """A pyarrow Table (``.to_pandas()`` for a DataFrame). ``where`` is a PyIceberg row filter
        string, e.g. ``"amount > 10"``; ``snapshot_id`` reads the table as it was at that snapshot."""
        kwargs: dict[str, Any] = {}
        if columns:
            kwargs["selected_fields"] = tuple(columns)
        if where:
            kwargs["row_filter"] = where
        if snapshot_id is not None:
            kwargs["snapshot_id"] = snapshot_id
        return self._with_access(READ).scan(**kwargs).to_arrow()

    def to_pandas(self, **kwargs):
        return self.read(**kwargs).to_pandas()

    def duckdb(self, connection=None, view: str | None = None, **kwargs):
        """This table's rows registered as a DuckDB view (named after the table unless ``view`` is
        given), for SQL. Returns the connection."""
        import duckdb

        con = connection or duckdb.connect()
        con.register(view or self.iceberg.name()[-1], self.read(**kwargs))
        return con

    def snapshots(self) -> list[dict]:
        return [
            {"snapshotId": s.snapshot_id, "timestampMs": s.timestamp_ms, "operation": s.summary.operation.value if s.summary else None}
            for s in self.iceberg.metadata.snapshots
        ]


def _notebook_token() -> Callable[[], str] | None:
    """Inside a booth-notebooks kernel, reuse the notebook's own platform token source (its
    ``booth`` package mints and refreshes it). Reaches into that package's session object, which is
    not a public API yet — see docs/decisions/0001's note for booth-notebooks."""
    try:
        import booth  # type: ignore[import-not-found]
    except ImportError:
        return None
    http = getattr(getattr(booth, "_default", None), "_http", None)
    fn = getattr(http, "token", None)
    return fn if callable(fn) else None
