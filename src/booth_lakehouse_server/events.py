"""``table.created`` / ``table.updated`` / ``table.deleted`` on the event bus, for booth-catalog (ADR 0085).

**A reconciler, not fire-on-write.** Each pass lists every warehouse's tables from Lakekeeper and
compares them with what this module last got a JetStream ack for (``store.published``):

- a table not published before → ``table.created``;
- a published table whose schema, snapshot, location, partitioning or name changed → ``table.updated``
  (a rename keeps the table's UUID, so it is an update, not a delete + create);
- a published table no longer listed → ``table.deleted`` (a tombstone: ``tableUuid``, namespace, name).

Why this shape rather than publishing from the proxy as writes pass through: it can't lose an event to
a crash between a commit and its publish (state is in Postgres, recorded only after the ack, so
delivery is at-least-once, de-duplicated by JetStream's ``Nats-Msg-Id``), it sees changes that didn't
come through the proxy (Lakekeeper's own expiry tasks), and it debounces by construction.

**Debouncing.** ``created``/``deleted`` go out on the next pass. ``updated`` for one table goes out at
most once per ``update_min_gap_seconds`` (default 60): a burst of appends becomes one event carrying the
latest state, never a stale one. Writes through the proxy nudge an early pass.

**Never deletes on doubt.** If a warehouse's listing fails, that workspace is skipped for the pass. If
one table's metadata can't be read, it is left exactly as last published.

Envelope and subject: ADR 0026 (``booth.<workspace>.table.<verb>`` on ``BOOTH_EVENTS``). booth-catalog
treats the subject as authoritative and drops an envelope that disagrees with it, so both are built
from the same binding here.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx

from .store import Binding, Published, Store
from .tables import TableNotFound, TableReader, table_summary

log = logging.getLogger(__name__)

STREAM = "BOOTH_EVENTS"
PUBLISHER = "lakehouse"
CREATED, UPDATED, DELETED = "table.created", "table.updated", "table.deleted"

# What makes two summaries "the same table state" for booth-catalog. Not lastUpdatedMs: Lakekeeper
# bumps it for changes (properties) that nothing registered in the catalog reflects.
_DIGEST_FIELDS = ("namespace", "name", "formatVersion", "location", "schema", "schemaId", "partitionSpec", "currentSnapshotId")


def subject(workspace: str, event_type: str) -> str:
    return f"booth.{workspace}.{event_type}"


def payload(summary: dict) -> dict:
    """The ADR 0085 payload: the table summary minus its snapshot history."""
    return {k: v for k, v in summary.items() if k != "snapshots"}


def digest(summary: dict) -> str:
    return hashlib.sha256(json.dumps({k: summary.get(k) for k in _DIGEST_FIELDS}, sort_keys=True).encode()).hexdigest()


def envelope(workspace: str, event_type: str, data: dict, now: float) -> bytes:
    published_at = datetime.fromtimestamp(now, UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return json.dumps({"workspace": workspace, "eventType": event_type, "publishedAt": published_at, "publishedBy": PUBLISHER, "data": data}).encode()


class Publisher:
    async def publish(self, subject: str, data: bytes, msg_id: str) -> None:  # pragma: no cover - interface
        """Returns only once the stream has stored it; raises otherwise."""
        raise NotImplementedError

    async def close(self) -> None:  # pragma: no cover - interface
        pass


class NatsPublisher(Publisher):
    """JetStream publish with the module's core-issued credential (ADR 0050, Secret
    ``booth-event-bus-credentials``). Connects lazily and reconnects after a failure."""

    def __init__(self, url: str, creds_file: str = "") -> None:
        self._url = url
        self._creds = creds_file
        self._nc = None
        self._js = None

    async def _connect(self):
        import nats

        opts = {"servers": [self._url], "name": "booth-lakehouse", "connect_timeout": 5, "max_reconnect_attempts": 2}
        if self._creds:
            opts["user_credentials"] = self._creds
        self._nc = await nats.connect(**opts)
        self._js = self._nc.jetstream()

    async def publish(self, subject, data, msg_id):
        if self._nc is None or self._nc.is_closed:
            await self._connect()
        try:
            await self._js.publish(subject, data, timeout=10, stream=STREAM, headers={"Nats-Msg-Id": msg_id})
        except Exception:
            await self.close()
            raise

    async def close(self):
        if self._nc is not None and not self._nc.is_closed:
            try:
                await self._nc.close()
            except Exception:  # noqa: BLE001 - closing a broken connection
                pass
        self._nc = self._js = None


@dataclass
class TableEvents:
    store: Store
    reader: TableReader
    publisher: Publisher
    update_min_gap_seconds: float = 60
    clock: Callable[[], float] = time.time
    status: str = field(default="starting")

    def _observe(self, b: Binding) -> tuple[dict[str, dict], set[tuple[str, str]]]:
        """Current tables by UUID, and the (namespace, name) of any listed table whose metadata
        couldn't be read this pass. Raises if the listing itself fails."""
        current: dict[str, dict] = {}
        unreadable: set[tuple[str, str]] = set()
        for ref in self.reader.tables(b.warehouse_id):
            try:
                md = self.reader.metadata(b.warehouse_id, ref)
            except TableNotFound:
                continue  # dropped between listing and loading: absent next pass
            except (httpx.HTTPError, KeyError, ValueError) as e:
                log.warning("table.* events: can't read %s.%s in workspace=%s this pass: %s", ref.dotted_namespace, ref.name, b.workspace, e)
                unreadable.add((ref.dotted_namespace, ref.name))
                continue
            summary = table_summary(b, ref.dotted_namespace, ref.name, md)
            if summary.get("tableUuid"):
                current[summary["tableUuid"]] = summary
        return current, unreadable

    def plan(self, b: Binding, current: dict[str, dict], unreadable: set[tuple[str, str]]) -> list[tuple[str, str, dict]]:
        """``(event_type, table_uuid, data)`` to publish, deletes first (a dropped-and-recreated
        name is then never momentarily registered twice)."""
        now = self.clock()
        prev = self.store.published(b.workspace)
        deletes, creates, updates = [], [], []
        for uuid, p in prev.items():
            if uuid not in current and (p.namespace, p.name) not in unreadable:
                deletes.append((DELETED, uuid, {"tableUuid": uuid, "namespace": p.namespace, "name": p.name}))
        for uuid, s in current.items():
            p = prev.get(uuid)
            if p is None:
                creates.append((CREATED, uuid, payload(s)))
            elif p.digest != digest(s) and now - p.published_at >= self.update_min_gap_seconds:
                updates.append((UPDATED, uuid, payload(s)))
        return deletes + creates + updates

    async def reconcile(self, b: Binding) -> list[tuple[str, str]]:
        current, unreadable = await asyncio.to_thread(self._observe, b)
        done = []
        for event_type, uuid, data in self.plan(b, current, unreadable):
            now = self.clock()
            d = digest(current[uuid]) if uuid in current else "deleted"
            # Same state → same id, so a republish after a crash (before the record below) is
            # de-duplicated by JetStream rather than delivered twice.
            await self.publisher.publish(subject(b.workspace, event_type), envelope(b.workspace, event_type, data, now), f"{b.workspace}:{uuid}:{event_type}:{d}")
            if event_type == DELETED:
                await asyncio.to_thread(self.store.forget_published, b.workspace, uuid)
            else:
                await asyncio.to_thread(self.store.record_published, b.workspace, Published(uuid, data["namespace"], data["name"], d, now))
            log.info("published %s workspace=%s table=%s.%s uuid=%s", event_type, b.workspace, data["namespace"], data["name"], uuid)
            done.append((event_type, uuid))
        return done

    async def run_once(self) -> list[tuple[str, str, str]]:
        """One pass over every workspace. A failure in one workspace doesn't stop the others; the
        first publish failure in a workspace stops that workspace's pass (order is kept)."""
        out, errors = [], []
        for b in await asyncio.to_thread(self.store.all):
            try:
                out.extend((b.workspace, t, u) for t, u in await self.reconcile(b))
            except Exception as e:  # noqa: BLE001 - one workspace's failure must not stop the rest
                log.warning("table.* events: pass failed for workspace=%s: %s", b.workspace, e)
                errors.append(f"{b.workspace}: {e}")
        self.status = "ok" if not errors else "error: " + "; ".join(errors)[:300]
        return out


async def run_forever(events: TableEvents, nudge: asyncio.Event, interval_seconds: float, settle_seconds: float = 2) -> None:
    """A pass every ``interval_seconds``, or sooner after a nudge (a write through the proxy),
    waiting ``settle_seconds`` so a burst of commits is seen as one change."""
    while True:
        try:
            await asyncio.wait_for(nudge.wait(), timeout=interval_seconds)
            await asyncio.sleep(settle_seconds)
        except TimeoutError:
            pass
        nudge.clear()
        try:
            await events.run_once()
        except Exception as e:  # noqa: BLE001 - the loop must survive anything
            events.status = f"error: {e}"
            log.warning("table.* events pass failed: %s", e)
