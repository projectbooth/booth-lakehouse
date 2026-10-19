"""Every warehouse's delete profile, kept at the chart's ``warehouse.deleteProfile`` (ADR 0122 B).

With Lakekeeper's ``soft`` profile a dropped table is only marked deleted: its files stay, it can be
restored for the window (7 days by default), and only then are the catalog entry and its files
removed. ``hard`` removes it at once. The proxy forces ``purgeRequested=true`` on every table drop
(``proxy.py``), so under ``soft`` every drop is restorable and nothing is left behind afterwards.

**Reconciled, not just set at creation.** A new warehouse is created with the profile; every existing
one (including any created before this setting existed, such as a homelab's first warehouse) is
checked at startup and every ``interval`` after, and changed only if it differs. Idempotent: a
warehouse already at the profile gets one GET and nothing else. Changing the profile is a single
warehouse setting in Lakekeeper: no table, snapshot or file is touched, and tables dropped earlier
keep the window they were dropped with.

**Never stops the API.** A failure is logged and recorded per workspace; the admin view shows it
(``status`` ``error`` with the message) and the next pass retries.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import httpx

from .lakekeeper import Lakekeeper, LakekeeperError, same_profile
from .store import Binding

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProfileStatus:
    status: str  # "ok" (at the configured profile), "error" (couldn't check or change it), "pending" (not checked yet)
    actual: dict | None  # the profile Lakekeeper last reported, None if never read
    error: str = ""
    changed_at: float | None = None  # when this process last changed it

    def public(self, desired: dict) -> dict:
        shown = self.actual or {}
        out = {
            "type": shown.get("type"),
            "expirationSeconds": shown.get("expiration-seconds"),
            "status": self.status,
            "desired": {"type": desired["type"], "expirationSeconds": desired.get("expiration-seconds")},
        }
        if self.error:
            out["error"] = self.error
        if self.changed_at is not None:
            out["changedAt"] = self.changed_at
        return out


@dataclass
class DeleteProfiles:
    lakekeeper: Lakekeeper
    desired: dict
    clock: Callable[[], float] = time.time
    _status: dict[str, ProfileStatus] = field(default_factory=dict)

    def status(self, workspace: str) -> ProfileStatus:
        return self._status.get(workspace, ProfileStatus("pending", None))

    def created(self, b: Binding) -> None:
        """A warehouse this process just created with the desired profile."""
        self._status[b.workspace] = ProfileStatus("ok", dict(self.desired))

    def reconcile(self, b: Binding) -> ProfileStatus:
        prev = self._status.get(b.workspace)
        try:
            actual = self.lakekeeper.delete_profile(b.warehouse_id)
            if same_profile(actual, self.desired):
                st = ProfileStatus("ok", actual, changed_at=prev.changed_at if prev else None)
            else:
                self.lakekeeper.set_delete_profile(b.warehouse_id, self.desired)
                log.info(
                    "warehouse delete profile changed workspace=%s warehouse=%s from=%s to=%s (no table or file touched; applies to later drops)",
                    b.workspace, b.warehouse_id, _show(actual), _show(self.desired),
                )
                st = ProfileStatus("ok", dict(self.desired), changed_at=self.clock())
        except (LakekeeperError, httpx.HTTPError, ValueError) as e:
            log.warning("warehouse delete profile: couldn't check or apply %s for workspace=%s warehouse=%s: %s", _show(self.desired), b.workspace, b.warehouse_id, e)
            st = ProfileStatus("error", prev.actual if prev else None, error=str(e)[:300], changed_at=prev.changed_at if prev else None)
        self._status[b.workspace] = st
        return st

    def reconcile_all(self, bindings: list[Binding]) -> list[tuple[str, str]]:
        """One pass over every warehouse; ``(workspace, error)`` for each that failed. One warehouse's
        failure never stops the others."""
        failures = []
        for b in bindings:
            st = self.reconcile(b)
            if st.status == "error":
                failures.append((b.workspace, st.error))
        return failures


def _show(profile: dict | None) -> str:
    if not profile:
        return "unset"
    if profile.get("type") == "soft":
        return f"soft({profile.get('expiration-seconds')}s)"
    return str(profile.get("type"))
