"""The read-only admin view's data (ADR 0093): which workspaces have a warehouse, and its cheap status.

**Who sees what** (ADR 0093 scoping, ADR 0094 identification):

- a **platform operator** — ``/platform/operator`` in the caller's own verified groups claim — sees
  every workspace's warehouse, whatever their role in the workspace they're acting in (operator status
  is orthogonal to workspace roles, ADR 0094);
- otherwise a caller sees **their own active workspace's** warehouse, if they're its owner or editor
  (viewers already see the bare binding via ``GET /api/warehouse``; this is a Manage view);
- **fail-closed**: with nobody holding the claim, nobody sees another tenant's warehouse.

**Cheap status only**, per ADR 0093: everything comes from this module's own store except the table
count, which is one call to Lakekeeper's per-warehouse statistics (maintained by Lakekeeper on every
change), never a listing of namespaces and tables. A failure there degrades that one field to
``null``, never the view.

Read-only by construction: nothing here, and no route using it, changes or deletes anything.
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx

from .identity import EDITOR, OWNER, Forbidden, Identity
from .store import Binding

ALL, OWN = "all", "workspace"


@dataclass(frozen=True)
class Access:
    scope: str  # ALL or OWN


def decide(who: Identity) -> Access:
    if who.is_operator:
        return Access(ALL)
    if who.role in (OWNER, EDITOR):
        return Access(OWN)
    raise Forbidden("the lakehouse admin view needs the editor or owner role in this workspace")


def credential_status(b: Binding, now: float, renew_margin_seconds: float) -> str:
    """``ok`` / ``renewing`` (inside the renewal margin — the next pass replaces it) / ``expired``
    (renewal has been failing: tables can't be written or committed until it succeeds)."""
    if b.credential_expires_at <= now:
        return "expired"
    if b.credential_expires_at - now <= renew_margin_seconds:
        return "renewing"
    return "ok"


def table_stats(lakekeeper: httpx.Client, warehouse_id: str) -> dict | None:
    """Tables/views from Lakekeeper's statistics: one request, the newest entry. ``None`` on failure."""
    try:
        resp = lakekeeper.get(f"/management/v1/warehouse/{warehouse_id}/statistics", params={"page_size": 1}, timeout=5)
        resp.raise_for_status()
        entries = resp.json().get("stats") or []
    except (httpx.HTTPError, ValueError, AttributeError):
        return None
    if not entries:
        return {"tables": 0, "views": 0, "asOf": None}
    newest = max(entries, key=lambda e: e.get("timestamp", ""))
    return {"tables": newest.get("number-of-tables"), "views": newest.get("number-of-views"), "asOf": newest.get("updated-at") or newest.get("timestamp")}


def without_dropped(stats: dict | None, dropped: int | None) -> dict | None:
    """Lakekeeper's statistics count a soft-deleted table until its window ends (its trigger ignores
    soft deletes: migration ``20250410024101_fix_warehouse_stats.sql``), so under ADR 0122 B's soft
    profile a dropped table would stay in the count for 7 days. Report live tables, and the
    restorable dropped ones separately. ``dropped`` None (couldn't list them) = the count as given."""
    if stats is None or dropped is None or stats.get("tables") is None:
        return stats
    return {**stats, "tables": max(stats["tables"] - dropped, 0), "dropped": dropped}


def row(b: Binding, now: float, renew_margin_seconds: float, stats: dict | None, delete_profile: dict | None = None) -> dict:
    """``deleteProfile``: what a table drop does in this warehouse as last seen by the reconciler
    (``deleteprofile.py``, ADR 0122 B), with ``status`` ``error`` and the message if it couldn't be
    checked or applied."""
    return {
        "workspace": b.workspace,
        "warehouseName": b.warehouse_name,
        "location": {"backendId": b.backend_id, "path": b.path},
        "storageRoot": b.storage_root,
        "createdAt": b.created_at,
        "createdBy": b.created_by,
        "credential": {"status": credential_status(b, now, renew_margin_seconds), "expiresAt": b.credential_expires_at},
        "tables": stats,
        "deleteProfile": delete_profile,
    }
