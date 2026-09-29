"""Reading what tables a warehouse holds, straight from Lakekeeper's catalog API (internal; never the
proxy), and describing one platform-side.

Shared by ``GET /api/tables*`` and the ``table.*`` event publisher (``events.py``), so the summary a
person sees and the payload booth-catalog receives (ADR 0085) are the same thing.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote

import httpx

# Iceberg REST encodes a multi-level namespace as its levels joined by the unit separator.
_SEP = "\x1f"


class TableNotFound(Exception):
    pass


@dataclass(frozen=True)
class TableRef:
    namespace: tuple[str, ...]
    name: str

    @property
    def dotted_namespace(self) -> str:
        return ".".join(self.namespace)


def _ns(levels: tuple[str, ...]) -> str:
    return quote(_SEP.join(levels), safe="")


class TableReader:
    def __init__(self, lakekeeper_url: str, transport: httpx.BaseTransport | None = None, timeout: float = 30) -> None:
        self._http = httpx.Client(base_url=lakekeeper_url.rstrip("/") + "/catalog", transport=transport, timeout=timeout)

    def _get(self, path: str, params: dict | None = None) -> dict:
        resp = self._http.get(path, params=params)
        if resp.status_code == 404:
            raise TableNotFound(path)
        resp.raise_for_status()
        return resp.json()

    def _paged(self, path: str, key: str, params: dict | None = None) -> list:
        out, token = [], None
        while True:
            doc = self._get(path, {**(params or {}), **({"pageToken": token} if token else {})})
            out.extend(doc.get(key, []))
            token = doc.get("next-page-token")
            if not token:
                return out

    def namespaces(self, warehouse_id: str) -> list[tuple[str, ...]]:
        """Every namespace, nested ones included (a client can create ``a.b`` with PyIceberg)."""
        found, frontier = [], [()]
        while frontier:
            parent = frontier.pop()
            params = {"parent": _SEP.join(parent)} if parent else None
            for ns in self._paged(f"/v1/{warehouse_id}/namespaces", "namespaces", params):
                levels = tuple(ns)
                if levels not in found:
                    found.append(levels)
                    frontier.append(levels)
        return found

    def tables(self, warehouse_id: str) -> list[TableRef]:
        out = []
        for ns in self.namespaces(warehouse_id):
            for ident in self._paged(f"/v1/{warehouse_id}/namespaces/{_ns(ns)}/tables", "identifiers"):
                out.append(TableRef(tuple(ident["namespace"]), ident["name"]))
        return out

    def metadata(self, warehouse_id: str, ref: TableRef) -> dict:
        return self._get(f"/v1/{warehouse_id}/namespaces/{_ns(ref.namespace)}/tables/{quote(ref.name, safe='')}")["metadata"]


def table_summary(b, namespace: str, table: str, md: dict) -> dict:
    """What a table *is*, platform-side: identity, schema, where it lives (as ``{backendId, path}``,
    ADR 0045), history. Minus ``snapshots``, this is the ``table.created``/``table.updated`` payload
    booth-catalog registers as an ``iceberg``-format dataset (ADR 0085)."""
    location = md.get("location", "")
    root = b.storage_root.rstrip("/")
    rel = location[len(root):].strip("/") if location == root or location.startswith(root + "/") else None
    current = md.get("current-schema-id")
    schema = next((s for s in md.get("schemas", []) if s.get("schema-id") == current), {"fields": []})
    return {
        "namespace": namespace,
        "name": table,
        "tableUuid": md.get("table-uuid"),
        "formatVersion": md.get("format-version"),
        "location": {"backendId": b.backend_id, "path": f"{b.path}/{rel}".strip("/")} if rel is not None else None,
        "schema": [
            {"name": f["name"], "type": f["type"] if isinstance(f["type"], str) else f["type"].get("type"), "required": f.get("required", False), "doc": f.get("doc", "")}
            for f in schema.get("fields", [])
        ],
        "schemaId": current,
        "partitionSpec": next((s.get("fields", []) for s in md.get("partition-specs", []) if s.get("spec-id") == md.get("default-spec-id")), []),
        "currentSnapshotId": md.get("current-snapshot-id"),
        "snapshots": [
            {"snapshotId": s["snapshot-id"], "timestampMs": s["timestamp-ms"], "operation": (s.get("summary") or {}).get("operation")}
            for s in md.get("snapshots", [])
        ],
        "lastUpdatedMs": md.get("last-updated-ms"),
    }
